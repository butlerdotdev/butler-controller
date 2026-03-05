/*
Copyright 2026 The Butler Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package imagesync

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

var vmiImageGVR = schema.GroupVersionResource{
	Group:    "harvesterhci.io",
	Version:  "v1beta1",
	Resource: "virtualmachineimages",
}

// reconcileHarvester handles image sync for Harvester providers.
// Direct mode: creates a VirtualMachineImage with sourceType=download.
// Proxy mode: not yet implemented (requires CDI upload proxy).
func (r *Reconciler) reconcileHarvester(ctx context.Context, is *butlerv1alpha1.ImageSync, pc *butlerv1alpha1.ProviderConfig, imageName, artifactURL string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Get Harvester credentials (kubeconfig)
	creds, err := r.getProviderCredentials(ctx, pc)
	if err != nil {
		return r.setFailed(ctx, is, "CredentialsError", fmt.Sprintf("failed to get Harvester credentials: %v", err))
	}

	kubeconfigData, ok := creds["kubeconfig"]
	if !ok {
		return r.setFailed(ctx, is, "CredentialsError", "Harvester credentials secret missing 'kubeconfig' key")
	}

	dynClient, err := newHarvesterDynamicClient(kubeconfigData)
	if err != nil {
		return r.setFailed(ctx, is, "ClientError", fmt.Sprintf("failed to create Harvester client: %v", err))
	}

	namespace := "default"
	if pc.Spec.Harvester != nil && pc.Spec.Harvester.Namespace != "" {
		namespace = pc.Spec.Harvester.Namespace
	}

	// Check if VMI already exists
	existing, err := dynClient.Resource(vmiImageGVR).Namespace(namespace).Get(ctx, imageName, metav1.GetOptions{})
	if err == nil {
		// VMI exists — check its status
		return r.checkHarvesterVMIStatus(ctx, is, existing, namespace, imageName)
	}

	// VMI doesn't exist — create it
	if is.Spec.TransferMode == butlerv1alpha1.TransferModeProxy {
		return r.setFailed(ctx, is, "UnsupportedTransferMode",
			"proxy transfer mode for Harvester is not yet implemented; use direct mode")
	}

	logger.Info("creating Harvester VirtualMachineImage", "name", imageName, "url", artifactURL)

	vmi := buildHarvesterVMI(imageName, namespace, artifactURL, is.Spec.DisplayName)
	_, err = dynClient.Resource(vmiImageGVR).Namespace(namespace).Create(ctx, vmi, metav1.CreateOptions{})
	if err != nil {
		return r.setFailed(ctx, is, "CreateImageFailed", fmt.Sprintf("failed to create VirtualMachineImage: %v", err))
	}

	// Transition to Downloading (Harvester CDI pulls the image)
	is.SetPhase(butlerv1alpha1.ImageSyncPhaseDownloading)
	is.Status.ObservedGeneration = is.Generation
	meta.SetStatusCondition(&is.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeProgressing,
		Status:             metav1.ConditionTrue,
		Reason:             butlerv1alpha1.ReasonImageDownloading,
		Message:            fmt.Sprintf("Harvester is downloading image from %s", artifactURL),
		ObservedGeneration: is.Generation,
	})
	if err := r.Status().Update(ctx, is); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// pollHarvesterImage checks the status of a Harvester VirtualMachineImage.
func (r *Reconciler) pollHarvesterImage(ctx context.Context, is *butlerv1alpha1.ImageSync, pc *butlerv1alpha1.ProviderConfig, imageName string) (ctrl.Result, error) {
	creds, err := r.getProviderCredentials(ctx, pc)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	kubeconfigData, ok := creds["kubeconfig"]
	if !ok {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	dynClient, err := newHarvesterDynamicClient(kubeconfigData)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	namespace := "default"
	if pc.Spec.Harvester != nil && pc.Spec.Harvester.Namespace != "" {
		namespace = pc.Spec.Harvester.Namespace
	}

	vmi, err := dynClient.Resource(vmiImageGVR).Namespace(namespace).Get(ctx, imageName, metav1.GetOptions{})
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	return r.checkHarvesterVMIStatus(ctx, is, vmi, namespace, imageName)
}

// checkHarvesterVMIStatus inspects VMI conditions and transitions phases.
func (r *Reconciler) checkHarvesterVMIStatus(ctx context.Context, is *butlerv1alpha1.ImageSync, vmi *unstructured.Unstructured, namespace, imageName string) (ctrl.Result, error) {
	conditions, found, _ := unstructured.NestedSlice(vmi.Object, "status", "conditions")
	if !found {
		// No conditions yet — still importing
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	for _, c := range conditions {
		condMap, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(condMap, "type")
		condStatus, _, _ := unstructured.NestedString(condMap, "status")
		message, _, _ := unstructured.NestedString(condMap, "message")

		if condType == "Imported" && condStatus == "True" {
			// Image is ready
			providerRef := fmt.Sprintf("%s/%s", namespace, imageName)
			return r.setReady(ctx, is, providerRef)
		}

		if condType == "Imported" && condStatus == "False" {
			reason, _, _ := unstructured.NestedString(condMap, "reason")
			if reason == "ImportFailed" || reason == "Failed" {
				return r.setFailed(ctx, is, "HarvesterImportFailed",
					fmt.Sprintf("VirtualMachineImage import failed: %s", message))
			}
		}

		// Harvester CDI retry limit exceeded — terminal failure
		if condType == "RetryLimitExceeded" && condStatus == "True" {
			return r.setFailed(ctx, is, "HarvesterRetryLimitExceeded",
				fmt.Sprintf("VirtualMachineImage download failed after retries: %s", message))
		}

		// Harvester CDI initialization failed
		if condType == "Initialized" && condStatus == "False" {
			reason, _, _ := unstructured.NestedString(condMap, "reason")
			if reason == "Failed" || reason == "ImportFailed" {
				return r.setFailed(ctx, is, "HarvesterInitFailed",
					fmt.Sprintf("VirtualMachineImage initialization failed: %s", message))
			}
		}
	}

	// Still importing — keep polling
	if is.Status.Phase == butlerv1alpha1.ImageSyncPhasePending {
		is.SetPhase(butlerv1alpha1.ImageSyncPhaseDownloading)
		is.Status.ObservedGeneration = is.Generation
		if err := r.Status().Update(ctx, is); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// buildHarvesterVMI constructs a VirtualMachineImage with sourceType=download.
func buildHarvesterVMI(name, namespace, downloadURL, displayName string) *unstructured.Unstructured {
	if displayName == "" {
		displayName = name
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "harvesterhci.io/v1beta1",
			"kind":       "VirtualMachineImage",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
				"labels": map[string]interface{}{
					"app.kubernetes.io/managed-by": "butler",
				},
				"annotations": map[string]interface{}{
					"harvesterhci.io/image-download-url": downloadURL,
				},
			},
			"spec": map[string]interface{}{
				"displayName": displayName,
				"sourceType":  "download",
				"url":         downloadURL,
			},
		},
	}
}

// newHarvesterDynamicClient creates a dynamic client for a Harvester cluster.
func newHarvesterDynamicClient(kubeconfigData []byte) (dynamic.Interface, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfigData)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST config: %w", err)
	}
	return dynamic.NewForConfig(restConfig)
}
