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

// Package stewardstatus provides a controller that synchronizes Steward TenantControlPlane
// status to StewardControlPlane for CAPI compatibility.
//
// Problem: The capi-steward provider sometimes fails to properly synchronize status
// from the Steward TenantControlPlane to the StewardControlPlane resource, causing
// CAPI to not recognize the control plane as ready.
//
// Solution: Watch TenantControlPlane resources and patch the corresponding
// StewardControlPlane status when the TCP is ready.
package stewardstatus

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	TenantControlPlaneGVK = schema.GroupVersionKind{
		Group:   "steward.butlerlabs.dev",
		Version: "v1alpha1",
		Kind:    "TenantControlPlane",
	}

	StewardControlPlaneGVK = schema.GroupVersionKind{
		Group:   "controlplane.cluster.x-k8s.io",
		Version: "v1alpha1",
		Kind:    "StewardControlPlane",
	}
)

// Reconciler watches Steward TenantControlPlane resources and syncs status to StewardControlPlane.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=steward.butlerlabs.dev,resources=tenantcontrolplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=steward.butlerlabs.dev,resources=tenantcontrolplanes/status,verbs=get
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=stewardcontrolplanes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=stewardcontrolplanes/status,verbs=get;update;patch

// Reconcile handles TenantControlPlane status synchronization.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Get the TenantControlPlane
	tcp := &unstructured.Unstructured{}
	tcp.SetGroupVersionKind(TenantControlPlaneGVK)

	if err := r.Get(ctx, req.NamespacedName, tcp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Check if TCP is ready
	tcpReady, version, err := r.isTCPReady(tcp)
	if err != nil {
		logger.Error(err, "failed to check TCP status")
		return ctrl.Result{}, err
	}

	if !tcpReady {
		logger.V(1).Info("TenantControlPlane not yet ready", "name", req.Name, "namespace", req.Namespace)
		return ctrl.Result{}, nil
	}

	// Find the corresponding StewardControlPlane
	// The KCP typically has the same name as the TCP
	kcp := &unstructured.Unstructured{}
	kcp.SetGroupVersionKind(StewardControlPlaneGVK)

	if err := r.Get(ctx, req.NamespacedName, kcp); err != nil {
		if apierrors.IsNotFound(err) {
			// KCP doesn't exist yet, that's fine
			logger.V(1).Info("StewardControlPlane not found", "name", req.Name, "namespace", req.Namespace)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Check if KCP status already reflects ready state
	kcpReady, err := r.isSCPReady(kcp)
	if err != nil {
		logger.Error(err, "failed to check KCP status")
		return ctrl.Result{}, err
	}

	if kcpReady {
		logger.V(1).Info("StewardControlPlane already shows ready", "name", req.Name)
		return ctrl.Result{}, nil
	}

	// Patch KCP status to reflect TCP ready state
	logger.Info("patching StewardControlPlane status to ready",
		"name", req.Name,
		"namespace", req.Namespace,
		"version", version)

	patch := client.RawPatch(types.MergePatchType, []byte(fmt.Sprintf(`{
		"status": {
			"replicas": 1,
			"readyReplicas": 1,
			"updatedReplicas": 1,
			"version": "%s",
			"ready": true,
			"initialized": true
		}
	}`, version)))

	if err := r.Status().Patch(ctx, kcp, patch); err != nil {
		logger.Error(err, "failed to patch StewardControlPlane status")
		return ctrl.Result{}, err
	}

	logger.Info("StewardControlPlane status patched successfully",
		"name", req.Name,
		"namespace", req.Namespace)

	return ctrl.Result{}, nil
}

// isTCPReady checks if a TenantControlPlane is ready.
func (r *Reconciler) isTCPReady(tcp *unstructured.Unstructured) (bool, string, error) {
	status, found, err := unstructured.NestedMap(tcp.Object, "status")
	if err != nil {
		return false, "", err
	}
	if !found {
		return false, "", nil
	}

	kubernetesStatus, found, err := unstructured.NestedMap(status, "kubernetes")
	if err != nil {
		return false, "", err
	}
	if !found {
		return false, "", nil
	}

	// Get version
	versionInfo, found, err := unstructured.NestedMap(kubernetesStatus, "version")
	if err != nil {
		return false, "", err
	}

	var version string
	if found {
		if v, ok := versionInfo["version"].(string); ok {
			version = v
		}
	}

	// Get status (should be "Ready")
	kubeStatus, found, err := unstructured.NestedString(kubernetesStatus, "status")
	if err != nil {
		return false, version, err
	}

	return found && kubeStatus == "Ready", version, nil
}

// isSCPReady checks if a StewardControlPlane is already showing ready status.
func (r *Reconciler) isSCPReady(kcp *unstructured.Unstructured) (bool, error) {
	readyReplicas, found, err := unstructured.NestedInt64(kcp.Object, "status", "readyReplicas")
	if err != nil {
		return false, err
	}
	if !found || readyReplicas == 0 {
		return false, nil
	}

	ready, found, err := unstructured.NestedBool(kcp.Object, "status", "ready")
	if err != nil {
		return false, err
	}

	return found && ready, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Watch TenantControlPlane resources
	tcp := &unstructured.Unstructured{}
	tcp.SetGroupVersionKind(TenantControlPlaneGVK)

	return ctrl.NewControllerManagedBy(mgr).
		For(tcp).
		Named("stewardstatus").
		Complete(r)
}
