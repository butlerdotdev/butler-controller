/*
Copyright 2026 Butler Labs.

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

package kamajistatus

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Reconciler reconciles KamajiControlPlane status.
//
// The CAPI Harvester provider has a bug where it doesn't sync the
// KamajiControlPlane status properly, causing CAPI to not recognize
// when the control plane is ready. This controller watches
// KamajiControlPlane resources and patches the status to make it
// compatible with what CAPI expects.
//
// This is a workaround for a known integration issue. Once this is
// fixed in the CAPI Kamaji or Harvester provider, this controller
// can be removed.
//
// What it does:
//  1. Watch TenantControlPlane (Kamaji's CRD) resources
//  2. When Kamaji reports the TCP is ready, patch the CAPI Cluster
//     status to reflect the control plane is ready
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

var (
	// TenantControlPlaneGVK is the GroupVersionKind for Kamaji's TenantControlPlane.
	TenantControlPlaneGVK = schema.GroupVersionKind{
		Group:   "kamaji.clastix.io",
		Version: "v1alpha1",
		Kind:    "TenantControlPlane",
	}
)

// +kubebuilder:rbac:groups=kamaji.clastix.io,resources=tenantcontrolplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups=kamaji.clastix.io,resources=tenantcontrolplanes/status,verbs=get
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters/status,verbs=get;update;patch

// Reconcile handles KamajiControlPlane status synchronization.
//
// The reconciliation:
// 1. Get the TenantControlPlane resource
// 2. Check if Kamaji reports it as ready
// 3. Find the corresponding CAPI Cluster
// 4. Patch the Cluster status if needed
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the TenantControlPlane (using unstructured since we don't import Kamaji types)
	tcp := &unstructured.Unstructured{}
	tcp.SetGroupVersionKind(TenantControlPlaneGVK)

	if err := r.Get(ctx, req.NamespacedName, tcp); err != nil {
		if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "unable to fetch TenantControlPlane")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	logger.Info("reconciling TenantControlPlane status",
		"name", tcp.GetName(),
		"namespace", tcp.GetNamespace())

	// Check if the TCP is ready
	if !r.isTCPReady(tcp) {
		logger.V(1).Info("TenantControlPlane not ready yet")
		return ctrl.Result{}, nil
	}

	// Get the control plane endpoint
	endpoint, err := r.getControlPlaneEndpoint(tcp)
	if err != nil {
		logger.V(1).Info("could not extract control plane endpoint", "error", err)
		return ctrl.Result{}, nil
	}

	logger.Info("TenantControlPlane is ready",
		"endpoint", endpoint)

	// TODO: Patch the corresponding CAPI Cluster status
	// This requires finding the Cluster that references this TCP
	// and updating its status.controlPlaneReady = true

	return ctrl.Result{}, nil
}

// isTCPReady checks if the TenantControlPlane is ready.
func (r *Reconciler) isTCPReady(tcp *unstructured.Unstructured) bool {
	// Check status.kubernetesResources.version.status == "Ready"
	// or other relevant conditions
	status, found, err := unstructured.NestedMap(tcp.Object, "status")
	if err != nil || !found {
		return false
	}

	// Check for Ready condition
	conditions, found, err := unstructured.NestedSlice(status, "conditions")
	if err != nil || !found {
		return false
	}

	for _, c := range conditions {
		condition, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if condition["type"] == "Ready" && condition["status"] == "True" {
			return true
		}
	}

	return false
}

// getControlPlaneEndpoint extracts the control plane endpoint from the TCP.
func (r *Reconciler) getControlPlaneEndpoint(tcp *unstructured.Unstructured) (string, error) {
	// Extract from status.controlPlaneEndpoint or similar
	endpoint, found, err := unstructured.NestedString(tcp.Object, "status", "controlPlaneEndpoint")
	if err != nil || !found {
		return "", err
	}
	return endpoint, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Watch TenantControlPlane resources
	tcp := &unstructured.Unstructured{}
	tcp.SetGroupVersionKind(TenantControlPlaneGVK)

	return ctrl.NewControllerManagedBy(mgr).
		For(tcp).
		Named("kamajistatus").
		Complete(r)
}
