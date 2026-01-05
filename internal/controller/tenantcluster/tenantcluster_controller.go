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

package tenantcluster

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/capi"
)

const (
	// ButlerConfigSingletonName is the required name for ButlerConfig
	ButlerConfigSingletonName = "butler"

	// Condition reasons
	ReasonValidating             = "Validating"
	ReasonValidationFailed       = "ValidationFailed"
	ReasonTeamNotFound           = "TeamNotFound"
	ReasonTeamRequired           = "TeamRequired"
	ReasonNamespaceMismatch      = "NamespaceMismatch"
	ReasonProvisioningNS         = "ProvisioningNamespace"
	ReasonNamespaceReady         = "NamespaceReady"
	ReasonInfraProvisioning      = "InfrastructureProvisioning"
	ReasonControlPlaneReady      = "ControlPlaneReady"
	ReasonWorkersProvisioning    = "WorkersProvisioning"
	ReasonWorkersReady           = "WorkersReady"
	ReasonProviderConfigNotFound = "ProviderConfigNotFound"
	ReasonCAPIResourceError      = "CAPIResourceError"
	ReasonReady                  = "Ready"
)

// Reconciler reconciles a TenantCluster object.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=teams,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=butlerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=providerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machinedeployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machinedeployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=kubeadmconfigtemplates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=kamajicontrolplanes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=kamajicontrolplanes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvesterclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvestermachinetemplates,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles TenantCluster reconciliation.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the TenantCluster
	tc := &butlerv1alpha1.TenantCluster{}
	if err := r.Get(ctx, req.NamespacedName, tc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("reconciling TenantCluster",
		"name", tc.Name,
		"namespace", tc.Namespace,
		"phase", tc.Status.Phase)

	// Handle deletion
	if !tc.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, tc)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(tc, butlerv1alpha1.FinalizerTenantCluster) {
		controllerutil.AddFinalizer(tc, butlerv1alpha1.FinalizerTenantCluster)
		if err := r.Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Initialize status if needed
	if tc.Status.Phase == "" {
		tc.Status.Phase = butlerv1alpha1.TenantClusterPhasePending
		if err := r.Status().Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Get ButlerConfig for multi-tenancy mode
	butlerConfig, err := r.getButlerConfig(ctx)
	if err != nil {
		logger.Error(err, "failed to get ButlerConfig")
		return r.setFailedStatus(ctx, tc, "ConfigError", "Failed to get ButlerConfig: "+err.Error())
	}

	// Validation
	if err := r.validateTenantCluster(ctx, tc, butlerConfig); err != nil {
		logger.Error(err, "validation failed")
		return r.setFailedStatus(ctx, tc, ReasonValidationFailed, err.Error())
	}

	// Tenant Namespace
	if err := r.reconcileTenantNamespace(ctx, tc, butlerConfig); err != nil {
		logger.Error(err, "failed to reconcile tenant namespace")
		return r.setFailedStatus(ctx, tc, "NamespaceError", err.Error())
	}

	// Update phase to Provisioning if still Pending
	if tc.Status.Phase == butlerv1alpha1.TenantClusterPhasePending {
		tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseProvisioning
		if err := r.Status().Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Infrastructure (CAPI resources)
	result, err := r.reconcileInfrastructure(ctx, tc)
	if err != nil {
		logger.Error(err, "failed to reconcile infrastructure")
		return r.setFailedStatus(ctx, tc, ReasonCAPIResourceError, err.Error())
	}
	if result.Requeue || result.RequeueAfter > 0 {
		// Update status before returning
		if err := r.Status().Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
		return result, nil
	}

	// Update status
	tc.Status.ObservedGeneration = tc.Generation
	if err := r.Status().Update(ctx, tc); err != nil {
		return ctrl.Result{}, err
	}

	// Requeue based on phase
	return ctrl.Result{RequeueAfter: r.calculateRequeueInterval(tc)}, nil
}

// reconcileInfrastructure handles Phase 3 - creating CAPI resources.
func (r *Reconciler) reconcileInfrastructure(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling infrastructure", "tenantNamespace", tc.Status.TenantNamespace)

	// Get ProviderConfig - determines which infrastructure provider to use
	providerConfig, err := r.getProviderConfig(ctx, tc)
	if err != nil {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionInfrastructureReady,
			metav1.ConditionFalse, ReasonProviderConfigNotFound, err.Error())
		return ctrl.Result{}, err
	}

	// Build CAPI resources
	builder := capi.NewBuilder(tc, providerConfig, tc.Status.TenantNamespace)
	resourceSet, err := builder.Build()
	if err != nil {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionInfrastructureReady,
			metav1.ConditionFalse, ReasonCAPIResourceError, err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to build CAPI resources: %w", err)
	}

	// NOTE: We intentionally do NOT set owner references on CAPI resources.
	// OwnerReferences only work within the same namespace, but CAPI resources
	// are created in the tenant namespace while TenantCluster lives in a different
	// namespace. Cross-namespace owner refs cause immediate garbage collection.
	// Cleanup is handled via the TenantCluster finalizer which deletes the tenant namespace.

	// Create or update each resource
	for _, resource := range resourceSet.AllResources() {
		if resource == nil {
			continue
		}

		existing := &unstructured.Unstructured{}
		existing.SetGroupVersionKind(resource.GroupVersionKind())

		err := r.Get(ctx, types.NamespacedName{
			Name:      resource.GetName(),
			Namespace: resource.GetNamespace(),
		}, existing)

		if apierrors.IsNotFound(err) {
			logger.Info("creating CAPI resource",
				"kind", resource.GetKind(),
				"name", resource.GetName(),
				"namespace", resource.GetNamespace())

			if err := r.Create(ctx, resource); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to create %s %s: %w",
					resource.GetKind(), resource.GetName(), err)
			}
		} else if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get %s %s: %w",
				resource.GetKind(), resource.GetName(), err)
		} else {
			logger.V(1).Info("CAPI resource already exists",
				"kind", resource.GetKind(),
				"name", resource.GetName())
		}
	}

	// Check infrastructure status
	infraReady, controlPlaneReady, workersReady, err := r.checkInfrastructureStatus(ctx, tc)
	if err != nil {
		logger.Error(err, "failed to check infrastructure status")
		// Don't fail, just requeue
	}

	patched, err := r.handleKamajiHarvesterCompatibility(ctx, tc)
	if err != nil {
		logger.Error(err, "failed to handle Kamaji compatibility")
	}
	if patched {
		logger.Info("patched KamajiControlPlane status for Harvester compatibility")
		// Requeue immediately to pick up the new status
		return ctrl.Result{Requeue: true}, nil
	}

	// Update conditions based on status
	if infraReady {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionInfrastructureReady,
			metav1.ConditionTrue, "InfrastructureReady", "Infrastructure cluster is ready")
	} else {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionInfrastructureReady,
			metav1.ConditionFalse, ReasonInfraProvisioning, "Infrastructure cluster is provisioning")
	}

	if controlPlaneReady {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionControlPlaneReady,
			metav1.ConditionTrue, ReasonControlPlaneReady, "Control plane is ready")
	} else {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionControlPlaneReady,
			metav1.ConditionFalse, ReasonInfraProvisioning, "Control plane is provisioning")
	}

	if workersReady {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionWorkersReady,
			metav1.ConditionTrue, ReasonWorkersReady, "Workers are ready")

		// All infrastructure is ready - Phase 3 complete
		logger.Info("infrastructure ready, proceeding to addon installation")
		// Phase 4 (addons) would go here
	} else {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionWorkersReady,
			metav1.ConditionFalse, ReasonWorkersProvisioning, "Workers are provisioning")
	}

	// If not fully ready, requeue
	if !infraReady || !controlPlaneReady || !workersReady {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// getProviderConfig retrieves the ProviderConfig for the tenant cluster.
// ProviderConfig is a namespaced resource stored in butler-system namespace.
// The lookup order is:
// 1. TenantCluster.spec.providerConfigRef (name)
// 2. ButlerConfig.spec.defaultProviderConfigRef (name)
// 3. ProviderConfig named "default"
// 4. First available ProviderConfig
func (r *Reconciler) getProviderConfig(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (*butlerv1alpha1.ProviderConfig, error) {
	// Default namespace for ProviderConfigs
	const defaultProviderNamespace = "butler-system"

	// If providerConfigRef is specified on TenantCluster, use that
	if tc.Spec.ProviderConfigRef != nil && tc.Spec.ProviderConfigRef.Name != "" {
		pc := &butlerv1alpha1.ProviderConfig{}
		if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.ProviderConfigRef.Name, Namespace: defaultProviderNamespace}, pc); err != nil {
			return nil, fmt.Errorf("failed to get ProviderConfig %s/%s: %w", defaultProviderNamespace, tc.Spec.ProviderConfigRef.Name, err)
		}
		return pc, nil
	}

	// Try to get default from ButlerConfig
	butlerConfig := &butlerv1alpha1.ButlerConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: ButlerConfigSingletonName}, butlerConfig); err == nil {
		// ButlerConfig exists, check for defaultProviderConfigRef
		if butlerConfig.Spec.DefaultProviderConfigRef != nil && butlerConfig.Spec.DefaultProviderConfigRef.Name != "" {
			pc := &butlerv1alpha1.ProviderConfig{}
			if err := r.Get(ctx, types.NamespacedName{Name: butlerConfig.Spec.DefaultProviderConfigRef.Name, Namespace: defaultProviderNamespace}, pc); err != nil {
				return nil, fmt.Errorf("failed to get default ProviderConfig %s/%s from ButlerConfig: %w",
					defaultProviderNamespace, butlerConfig.Spec.DefaultProviderConfigRef.Name, err)
			}
			return pc, nil
		}
	}

	// Look for a ProviderConfig named "default" in butler-system namespace
	pc := &butlerv1alpha1.ProviderConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: "default", Namespace: defaultProviderNamespace}, pc); err == nil {
		return pc, nil
	}

	// If no default found, use the first available ProviderConfig in butler-system
	pcList := &butlerv1alpha1.ProviderConfigList{}
	if err := r.List(ctx, pcList, client.InNamespace(defaultProviderNamespace)); err != nil {
		return nil, fmt.Errorf("failed to list ProviderConfigs: %w", err)
	}

	if len(pcList.Items) > 0 {
		return &pcList.Items[0], nil
	}

	return nil, fmt.Errorf("no ProviderConfig available in %s namespace", defaultProviderNamespace)
}

// checkInfrastructureStatus checks the status of CAPI resources.
func (r *Reconciler) checkInfrastructureStatus(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (infraReady, cpReady, workersReady bool, err error) {
	logger := log.FromContext(ctx)

	// Check Cluster status
	cluster := &unstructured.Unstructured{}
	cluster.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.ClusterAPIGroup,
		Version: capi.ClusterAPIVersion,
		Kind:    "Cluster",
	})
	if err := r.Get(ctx, types.NamespacedName{
		Name:      tc.Name,
		Namespace: tc.Status.TenantNamespace,
	}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return false, false, false, nil
		}
		return false, false, false, err
	}

	// Check cluster phase
	phase, found, _ := unstructured.NestedString(cluster.Object, "status", "phase")
	if found {
		logger.V(1).Info("cluster phase", "phase", phase)
		infraReady = phase == "Provisioned"
	}

	// Check control plane status
	cpReadyVal, found, _ := unstructured.NestedBool(cluster.Object, "status", "controlPlaneReady")
	if found {
		cpReady = cpReadyVal
	}

	// Check infrastructure status
	infraReadyVal, found, _ := unstructured.NestedBool(cluster.Object, "status", "infrastructureReady")
	if found && !infraReady {
		infraReady = infraReadyVal
	}

	// Check MachineDeployment status for workers
	md := &unstructured.Unstructured{}
	md.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.ClusterAPIGroup,
		Version: capi.ClusterAPIVersion,
		Kind:    "MachineDeployment",
	})
	if err := r.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("%s-workers", tc.Name),
		Namespace: tc.Status.TenantNamespace,
	}, md); err != nil {
		if apierrors.IsNotFound(err) {
			return infraReady, cpReady, false, nil
		}
		return infraReady, cpReady, false, err
	}

	// Check replicas vs readyReplicas
	replicas, _, _ := unstructured.NestedInt64(md.Object, "status", "replicas")
	readyReplicas, _, _ := unstructured.NestedInt64(md.Object, "status", "readyReplicas")

	logger.V(1).Info("MachineDeployment status",
		"replicas", replicas,
		"readyReplicas", readyReplicas)

	if replicas > 0 && readyReplicas == replicas {
		workersReady = true
	}

	return infraReady, cpReady, workersReady, nil
}

// handleKamajiHarvesterCompatibility works around the Kamaji + Harvester compatibility issue.
func (r *Reconciler) handleKamajiHarvesterCompatibility(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (bool, error) {
	logger := log.FromContext(ctx)

	// Get KamajiControlPlane
	kcp := &unstructured.Unstructured{}
	kcp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.ControlPlaneAPIGroup,
		Version: capi.ControlPlaneAPIVersion,
		Kind:    "KamajiControlPlane",
	})
	if err := r.Get(ctx, types.NamespacedName{
		Name:      tc.Name,
		Namespace: tc.Status.TenantNamespace,
	}, kcp); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	// Check for "unsupported infrastructure provider" condition
	conditions, found, _ := unstructured.NestedSlice(kcp.Object, "status", "conditions")
	if !found {
		return false, nil
	}

	// Get TenantControlPlane to check status and get LoadBalancer IP
	tcp := &unstructured.Unstructured{}
	tcp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "kamaji.clastix.io",
		Version: "v1alpha1",
		Kind:    "TenantControlPlane",
	})
	if err := r.Get(ctx, types.NamespacedName{
		Name:      tc.Name,
		Namespace: tc.Status.TenantNamespace,
	}, tcp); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	// Extract LoadBalancer IP from TenantControlPlane service status
	serviceStatus, found, _ := unstructured.NestedMap(tcp.Object, "status", "kubernetesResources", "service")
	var loadBalancerIP string
	if found {
		ingress, found, _ := unstructured.NestedSlice(serviceStatus, "loadBalancer", "ingress")
		if found && len(ingress) > 0 {
			if ingressEntry, ok := ingress[0].(map[string]interface{}); ok {
				loadBalancerIP, _, _ = unstructured.NestedString(ingressEntry, "ip")
			}
		}
	}

	// Always check and patch Cluster controlPlaneEndpoint if needed
	// This must happen even if KCP status is already patched
	if loadBalancerIP != "" {
		cluster := &unstructured.Unstructured{}
		cluster.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   capi.ClusterAPIGroup,
			Version: capi.ClusterAPIVersion,
			Kind:    "Cluster",
		})
		if err := r.Get(ctx, types.NamespacedName{
			Name:      tc.Name,
			Namespace: tc.Status.TenantNamespace,
		}, cluster); err == nil {
			currentHost, _, _ := unstructured.NestedString(cluster.Object, "spec", "controlPlaneEndpoint", "host")
			if currentHost != loadBalancerIP {
				logger.Info("patching Cluster controlPlaneEndpoint",
					"currentHost", currentHost, "newHost", loadBalancerIP)

				if err := unstructured.SetNestedField(cluster.Object, loadBalancerIP, "spec", "controlPlaneEndpoint", "host"); err != nil {
					logger.Error(err, "failed to set controlPlaneEndpoint.host")
				} else if err := unstructured.SetNestedField(cluster.Object, int64(6443), "spec", "controlPlaneEndpoint", "port"); err != nil {
					logger.Error(err, "failed to set controlPlaneEndpoint.port")
				} else if err := r.Update(ctx, cluster); err != nil {
					logger.Error(err, "failed to update Cluster controlPlaneEndpoint")
				} else {
					logger.Info("successfully patched Cluster controlPlaneEndpoint", "host", loadBalancerIP)
					// Requeue to let CAPI propagate the change
					return true, nil
				}
			}
		}
	}

	// Check if KCP status already patched - if so, we're done
	initialized, _, _ := unstructured.NestedBool(kcp.Object, "status", "initialized")
	ready, _, _ := unstructured.NestedBool(kcp.Object, "status", "ready")
	updatedReplicas, _, _ := unstructured.NestedInt64(kcp.Object, "status", "updatedReplicas")
	version, _, _ := unstructured.NestedString(kcp.Object, "status", "version")
	if initialized && ready && updatedReplicas >= 1 && version != "" {
		return false, nil
	}

	hasUnsupportedProviderError := false
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(cond, "type")
		message, _, _ := unstructured.NestedString(cond, "message")
		if condType == "InfrastructureClusterPatched" && message == "unsupported infrastructure provider" {
			hasUnsupportedProviderError = true
			break
		}
	}

	if !hasUnsupportedProviderError {
		return false, nil
	}

	logger.Info("detected Kamaji unsupported infrastructure provider error, patching KamajiControlPlane status")

	// Check TenantControlPlane deployment is ready before patching KCP
	deploymentStatus, deployFound, _ := unstructured.NestedMap(tcp.Object, "status", "kubernetesResources", "deployment")
	if !deployFound {
		logger.V(1).Info("TenantControlPlane deployment status not found")
		return false, nil
	}

	readyReplicas, _, _ := unstructured.NestedInt64(deploymentStatus, "readyReplicas")
	if readyReplicas < 1 {
		logger.V(1).Info("TenantControlPlane deployment not ready yet", "readyReplicas", readyReplicas)
		return false, nil
	}

	// TenantControlPlane is ready but KamajiControlPlane isn't marked initialized
	// due to the unsupported infrastructure provider error. Patch it.
	logger.Info("TenantControlPlane is ready, patching KamajiControlPlane status",
		"readyReplicas", readyReplicas)

	// Patch KamajiControlPlane status
	if err := unstructured.SetNestedField(kcp.Object, true, "status", "initialized"); err != nil {
		return false, fmt.Errorf("failed to set initialized: %w", err)
	}
	if err := unstructured.SetNestedField(kcp.Object, true, "status", "ready"); err != nil {
		return false, fmt.Errorf("failed to set ready: %w", err)
	}
	if err := unstructured.SetNestedField(kcp.Object, int64(1), "status", "replicas"); err != nil {
		return false, fmt.Errorf("failed to set replicas: %w", err)
	}
	if err := unstructured.SetNestedField(kcp.Object, int64(1), "status", "readyReplicas"); err != nil {
		return false, fmt.Errorf("failed to set readyReplicas: %w", err)
	}
	if err := unstructured.SetNestedField(kcp.Object, int64(1), "status", "updatedReplicas"); err != nil {
		return false, fmt.Errorf("failed to set updatedReplicas: %w", err)
	}
	if err := unstructured.SetNestedField(kcp.Object, tc.Spec.KubernetesVersion, "status", "version"); err != nil {
		return false, fmt.Errorf("failed to set version: %w", err)
	}

	if err := r.Status().Update(ctx, kcp); err != nil {
		return false, fmt.Errorf("failed to update KamajiControlPlane status: %w", err)
	}

	return true, nil
}

// getButlerConfig retrieves the ButlerConfig singleton.
func (r *Reconciler) getButlerConfig(ctx context.Context) (*butlerv1alpha1.ButlerConfig, error) {
	config := &butlerv1alpha1.ButlerConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: ButlerConfigSingletonName}, config); err != nil {
		if apierrors.IsNotFound(err) {
			// Return a default config if not found
			return &butlerv1alpha1.ButlerConfig{
				Spec: butlerv1alpha1.ButlerConfigSpec{
					MultiTenancy: butlerv1alpha1.MultiTenancyConfig{
						Mode: butlerv1alpha1.MultiTenancyModeDisabled,
					},
					DefaultNamespace: "butler-tenants",
				},
			}, nil
		}
		return nil, err
	}
	return config, nil
}

// validateTenantCluster validates the TenantCluster based on multi-tenancy mode.
func (r *Reconciler) validateTenantCluster(ctx context.Context, tc *butlerv1alpha1.TenantCluster, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)
	mode := config.Spec.MultiTenancy.Mode

	logger.V(1).Info("validating TenantCluster", "mode", mode)

	switch mode {
	case butlerv1alpha1.MultiTenancyModeEnforced:
		return r.validateEnforcedMode(ctx, tc)

	case butlerv1alpha1.MultiTenancyModeOptional:
		return r.validateOptionalMode(ctx, tc, config)

	case butlerv1alpha1.MultiTenancyModeDisabled:
		return r.validateDisabledMode(ctx, tc, config)

	default:
		// Default to disabled if mode is empty
		return r.validateDisabledMode(ctx, tc, config)
	}
}

// validateEnforcedMode validates when multi-tenancy is enforced.
// - teamRef is required
// - Team must exist and be Ready
// - TenantCluster namespace must match Team's namespace
func (r *Reconciler) validateEnforcedMode(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	logger := log.FromContext(ctx)

	// teamRef is required in Enforced mode
	if tc.Spec.TeamRef == nil || tc.Spec.TeamRef.Name == "" {
		return fmt.Errorf("teamRef is required when multi-tenancy mode is Enforced")
	}

	// Get the Team
	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("team %q not found", tc.Spec.TeamRef.Name)
		}
		return fmt.Errorf("failed to get team: %w", err)
	}

	// Team must be Ready
	if team.Status.Phase != butlerv1alpha1.TeamPhaseReady {
		return fmt.Errorf("team %q is not ready (phase: %s)", team.Name, team.Status.Phase)
	}

	// TenantCluster namespace must match Team's namespace
	expectedNamespace := team.Status.Namespace
	if expectedNamespace == "" {
		expectedNamespace = team.Name // Team namespace is same as Team name
	}

	if tc.Namespace != expectedNamespace {
		return fmt.Errorf("TenantCluster must be in team namespace %q, got %q", expectedNamespace, tc.Namespace)
	}

	logger.V(1).Info("enforced mode validation passed", "team", team.Name)
	return nil
}

// validateOptionalMode validates when multi-tenancy is optional.
// - teamRef is optional
// - If provided, Team must exist and namespace must match
// - If not provided, TenantCluster must be in default namespace
func (r *Reconciler) validateOptionalMode(ctx context.Context, tc *butlerv1alpha1.TenantCluster, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	if tc.Spec.TeamRef != nil && tc.Spec.TeamRef.Name != "" {
		// teamRef provided - validate like Enforced mode
		team := &butlerv1alpha1.Team{}
		if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("team %q not found", tc.Spec.TeamRef.Name)
			}
			return fmt.Errorf("failed to get team: %w", err)
		}

		expectedNamespace := team.Status.Namespace
		if expectedNamespace == "" {
			expectedNamespace = team.Name
		}

		if tc.Namespace != expectedNamespace {
			return fmt.Errorf("TenantCluster must be in team namespace %q when teamRef is set, got %q", expectedNamespace, tc.Namespace)
		}

		logger.V(1).Info("optional mode validation passed with team", "team", team.Name)
	} else {
		// No teamRef - must be in default namespace
		defaultNS := config.Spec.DefaultNamespace
		if defaultNS == "" {
			defaultNS = "butler-tenants"
		}

		if tc.Namespace != defaultNS {
			return fmt.Errorf("TenantCluster without teamRef must be in default namespace %q, got %q", defaultNS, tc.Namespace)
		}

		logger.V(1).Info("optional mode validation passed without team", "namespace", tc.Namespace)
	}

	return nil
}

// validateDisabledMode validates when multi-tenancy is disabled.
// - TenantCluster must be in default namespace
// - teamRef is ignored
func (r *Reconciler) validateDisabledMode(ctx context.Context, tc *butlerv1alpha1.TenantCluster, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	defaultNS := config.Spec.DefaultNamespace
	if defaultNS == "" {
		defaultNS = "butler-tenants"
	}

	if tc.Namespace != defaultNS {
		return fmt.Errorf("TenantCluster must be in default namespace %q when multi-tenancy is disabled, got %q", defaultNS, tc.Namespace)
	}

	logger.V(1).Info("disabled mode validation passed", "namespace", tc.Namespace)
	return nil
}

// reconcileTenantNamespace creates the tenant namespace for CAPI/Kamaji resources.
func (r *Reconciler) reconcileTenantNamespace(ctx context.Context, tc *butlerv1alpha1.TenantCluster, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	// Generate tenant namespace name if not already set
	if tc.Status.TenantNamespace == "" {
		tc.Status.TenantNamespace = generateTenantNamespace(tc)
	}

	nsName := tc.Status.TenantNamespace
	logger.Info("reconciling tenant namespace", "namespace", nsName)

	// Create the namespace
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: nsName,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, ns, func() error {
		// Set labels
		if ns.Labels == nil {
			ns.Labels = make(map[string]string)
		}
		ns.Labels[butlerv1alpha1.LabelManagedBy] = "butler"
		ns.Labels[butlerv1alpha1.LabelTenant] = tc.Name
		ns.Labels[butlerv1alpha1.LabelSourceNamespace] = tc.Namespace
		ns.Labels[butlerv1alpha1.LabelSourceName] = tc.Name

		// Add team label if team is specified
		if tc.Spec.TeamRef != nil && tc.Spec.TeamRef.Name != "" {
			ns.Labels[butlerv1alpha1.LabelTeam] = tc.Spec.TeamRef.Name
		}

		// PodSecurity labels for CAPI namespaces
		ns.Labels["pod-security.kubernetes.io/enforce"] = "privileged"
		ns.Labels["pod-security.kubernetes.io/audit"] = "privileged"
		ns.Labels["pod-security.kubernetes.io/warn"] = "privileged"

		// Set annotations
		if ns.Annotations == nil {
			ns.Annotations = make(map[string]string)
		}
		ns.Annotations[butlerv1alpha1.AnnotationDescription] = fmt.Sprintf("Tenant namespace for TenantCluster %s/%s", tc.Namespace, tc.Name)

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to create/update tenant namespace: %w", err)
	}

	if op != controllerutil.OperationResultNone {
		logger.Info("tenant namespace reconciled", "namespace", nsName, "operation", op)
	}

	// Create RoleBinding for team access if team is specified
	if tc.Spec.TeamRef != nil && tc.Spec.TeamRef.Name != "" {
		if err := r.reconcileTeamRoleBinding(ctx, tc, nsName); err != nil {
			return fmt.Errorf("failed to create team RoleBinding: %w", err)
		}
	}

	return nil
}

// reconcileTeamRoleBinding creates a RoleBinding in the tenant namespace for team access.
func (r *Reconciler) reconcileTeamRoleBinding(ctx context.Context, tc *butlerv1alpha1.TenantCluster, nsName string) error {
	logger := log.FromContext(ctx)

	// Get the Team to find its users/groups
	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
		return err
	}

	rbName := fmt.Sprintf("butler-team-%s-access", team.Name)

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rbName,
			Namespace: nsName,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, rb, func() error {
		// Set labels
		if rb.Labels == nil {
			rb.Labels = make(map[string]string)
		}
		rb.Labels[butlerv1alpha1.LabelManagedBy] = "butler"
		rb.Labels[butlerv1alpha1.LabelTeam] = team.Name
		rb.Labels[butlerv1alpha1.LabelTenant] = tc.Name

		// RoleRef - give admin access to tenant namespace
		rb.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "admin", // Use built-in admin ClusterRole
		}

		// Build subjects from team users and groups
		var subjects []rbacv1.Subject

		for _, user := range team.Spec.Access.Users {
			subjects = append(subjects, rbacv1.Subject{
				Kind:     rbacv1.UserKind,
				APIGroup: rbacv1.GroupName,
				Name:     user.Name,
			})
		}

		for _, group := range team.Spec.Access.Groups {
			subjects = append(subjects, rbacv1.Subject{
				Kind:     rbacv1.GroupKind,
				APIGroup: rbacv1.GroupName,
				Name:     group.Name,
			})
		}

		rb.Subjects = subjects
		return nil
	})

	if err != nil {
		return err
	}

	if op != controllerutil.OperationResultNone {
		logger.Info("team RoleBinding reconciled", "name", rbName, "namespace", nsName, "operation", op)
	}

	return nil
}

// handleDeletion handles TenantCluster cleanup.
func (r *Reconciler) handleDeletion(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("handling TenantCluster deletion", "name", tc.Name, "namespace", tc.Namespace)

	// Update phase
	tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseDeleting
	if err := r.Status().Update(ctx, tc); err != nil {
		// Ignore conflict errors during deletion
		if !apierrors.IsConflict(err) {
			return ctrl.Result{}, err
		}
	}

	// Delete tenant namespace (this will cascade delete all CAPI resources in it)
	if tc.Status.TenantNamespace != "" {
		ns := &corev1.Namespace{}
		err := r.Get(ctx, types.NamespacedName{Name: tc.Status.TenantNamespace}, ns)
		if err == nil {
			logger.Info("deleting tenant namespace", "namespace", tc.Status.TenantNamespace)
			if err := r.Delete(ctx, ns); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			// Requeue to wait for namespace deletion
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		// Namespace is gone
		logger.Info("tenant namespace deleted", "namespace", tc.Status.TenantNamespace)
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(tc, butlerv1alpha1.FinalizerTenantCluster)
	if err := r.Update(ctx, tc); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("TenantCluster deletion complete", "name", tc.Name)
	return ctrl.Result{}, nil
}

// setFailedStatus updates status to Failed phase with error message.
func (r *Reconciler) setFailedStatus(ctx context.Context, tc *butlerv1alpha1.TenantCluster, reason, message string) (ctrl.Result, error) {
	tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseFailed
	tc.Status.ObservedGeneration = tc.Generation

	now := metav1.Now()
	tc.Status.LastTransitionTime = &now

	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionReady,
		metav1.ConditionFalse, reason, message)

	if err := r.Status().Update(ctx, tc); err != nil {
		return ctrl.Result{}, err
	}

	// Requeue with backoff for failed status
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// setCondition sets a condition on the TenantCluster.
func (r *Reconciler) setCondition(tc *butlerv1alpha1.TenantCluster, condType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&tc.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: tc.Generation,
	})
}

// calculateRequeueInterval returns the appropriate requeue duration based on phase and age.
func (r *Reconciler) calculateRequeueInterval(tc *butlerv1alpha1.TenantCluster) time.Duration {
	// Not ready - requeue quickly
	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady {
		return 30 * time.Second
	}

	// Ready - use tiered intervals based on age
	if tc.Status.LastTransitionTime == nil {
		return 1 * time.Minute
	}

	age := time.Since(tc.Status.LastTransitionTime.Time)
	switch {
	case age < 1*time.Hour:
		return 1 * time.Minute
	case age < 24*time.Hour:
		return 5 * time.Minute
	default:
		return 15 * time.Minute
	}
}

// generateTenantNamespace generates a unique namespace name for tenant resources.
func generateTenantNamespace(tc *butlerv1alpha1.TenantCluster) string {
	// Use first 8 characters of UID for uniqueness
	uid := string(tc.UID)
	if len(uid) > 8 {
		uid = uid[:8]
	}
	return fmt.Sprintf("%s-%s", tc.Name, uid)
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.TenantCluster{}).
		Owns(&corev1.Namespace{}).
		Owns(&corev1.Secret{}).
		Owns(&rbacv1.RoleBinding{}).
		Named("tenantcluster").
		Complete(r)
}
