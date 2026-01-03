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
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// Reconciler reconciles a TenantCluster object.
//
// TenantCluster represents a complete Kubernetes cluster managed by Butler.
// This controller is responsible for the full cluster lifecycle:
//
// Infrastructure Phase:
// - Create tenant namespace (e.g., "production-a7b8c9")
// - Create CAPI Cluster and provider-specific resources (HarvesterCluster)
// - Create KamajiControlPlane for hosted control plane
// - Create MachineDeployment for worker nodes
// - Wait for infrastructure to be ready
//
// Addon Phase:
// - Install CNI (Cilium)
// - Install LoadBalancer (MetalLB) with memberlist secret prereq
// - Install optional addons (cert-manager, storage, ingress)
// - Bootstrap GitOps if configured (Flux)
//
// Status Phase:
// - Query tenant cluster state via Kamaji-hosted API
// - Update conditions and observed state
// - Calculate requeue interval based on cluster age (tiered refresh)
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
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machinedeployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvesterclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvestermachinetemplates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=kubeadmconfigtemplates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kamaji.clastix.io,resources=tenantcontrolplanes,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles TenantCluster reconciliation.
//
// The reconciliation loop follows phases:
// 1. Validation - Check team membership (if Enforced mode), resource limits
// 2. Infrastructure - Create/update CAPI and Kamaji resources
// 3. Addons - Install configured addons (monotonic - only add, never remove)
// 4. Status - Update observed state from tenant cluster
//
// Requeue intervals are tiered based on cluster phase and age:
// - Provisioning: 30 seconds (user is waiting)
// - Ready (< 1 hour): 1 minute
// - Ready (< 24 hours): 5 minutes
// - Ready (> 24 hours): 15 minutes
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the TenantCluster instance
	tc := &butlerv1alpha1.TenantCluster{}
	if err := r.Get(ctx, req.NamespacedName, tc); err != nil {
		if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "unable to fetch TenantCluster")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	logger.Info("reconciling TenantCluster",
		"name", tc.Name,
		"namespace", tc.Namespace,
		"phase", tc.Status.Phase,
		"kubernetesVersion", tc.Spec.KubernetesVersion)

	// Handle deletion
	if !tc.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, tc)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(tc, butlerv1alpha1.FinalizerTenantCluster) {
		controllerutil.AddFinalizer(tc, butlerv1alpha1.FinalizerTenantCluster)
		if err := r.Update(ctx, tc); err != nil {
			logger.Error(err, "failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Phase 1: Validation
	if err := r.validateTenantCluster(ctx, tc); err != nil {
		logger.Error(err, "validation failed")
		return r.updateStatusFailed(ctx, tc, err)
	}

	// Phase 2: Infrastructure
	if err := r.reconcileInfrastructure(ctx, tc); err != nil {
		logger.Error(err, "infrastructure reconciliation failed")
		return r.updateStatusFailed(ctx, tc, err)
	}

	// Phase 3: Addons (only if infrastructure is ready)
	if r.isInfrastructureReady(tc) {
		if err := r.reconcileAddons(ctx, tc); err != nil {
			logger.Error(err, "addon reconciliation failed")
			return r.updateStatusFailed(ctx, tc, err)
		}
	}

	// Phase 4: Status update
	return r.updateStatusSuccess(ctx, tc)
}

// validateTenantCluster validates the TenantCluster spec.
func (r *Reconciler) validateTenantCluster(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("validating TenantCluster")

	// TODO: Implement validation
	// 1. Check if multi-tenancy mode requires Team
	// 2. Validate Team exists (if teamRef is set)
	// 3. Validate namespace matches Team (if Enforced mode)
	// 4. Validate resource limits
	// 5. Validate ProviderConfig exists

	return nil
}

// reconcileInfrastructure creates/updates CAPI and Kamaji resources.
func (r *Reconciler) reconcileInfrastructure(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("reconciling infrastructure")

	// TODO: Implement infrastructure reconciliation
	// 1. Generate tenant namespace name (e.g., "{name}-{hash}")
	// 2. Create tenant namespace with labels
	// 3. Create RoleBinding for team access
	// 4. Create CAPI Cluster
	// 5. Create provider-specific cluster (HarvesterCluster)
	// 6. Create KamajiControlPlane (or use CAPI Kamaji integration)
	// 7. Create MachineDeployment
	// 8. Create HarvesterMachineTemplate
	// 9. Create KubeadmConfigTemplate (with Rocky Linux preKubeadmCommands)
	// 10. Wait for control plane to be ready
	// 11. Wait for workers to join

	return nil
}

// reconcileAddons installs configured addons.
func (r *Reconciler) reconcileAddons(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("reconciling addons")

	// TODO: Implement addon reconciliation
	// This follows the monotonic model - only install, never remove
	//
	// 1. Get kubeconfig for tenant cluster (from Kamaji secret)
	// 2. Create client to tenant cluster
	// 3. Check which addons are already installed
	// 4. Install missing addons in order:
	//    - CNI (Cilium) first - required for networking
	//    - LoadBalancer (MetalLB) - create memberlist secret first
	//    - cert-manager
	//    - Storage (Longhorn/LINSTOR)
	//    - Ingress (Traefik)
	//    - GitOps (Flux) - if configured, bootstrap and hand off
	// 5. Update observed addon state

	return nil
}

// isInfrastructureReady checks if infrastructure is ready for addons.
func (r *Reconciler) isInfrastructureReady(tc *butlerv1alpha1.TenantCluster) bool {
	// TODO: Check conditions
	// - ControlPlaneReady
	// - At least one worker ready
	return false
}

// handleDeletion handles TenantCluster cleanup.
func (r *Reconciler) handleDeletion(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("handling TenantCluster deletion", "name", tc.Name)

	// TODO: Implement deletion logic
	// 1. Delete CAPI Cluster (cascades to machines, VMs)
	// 2. Delete Kamaji resources
	// 3. Wait for resources to be gone
	// 4. Delete tenant namespace
	// 5. Remove finalizer

	// For now, just remove finalizer
	controllerutil.RemoveFinalizer(tc, butlerv1alpha1.FinalizerTenantCluster)
	if err := r.Update(ctx, tc); err != nil {
		logger.Error(err, "failed to remove finalizer")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// updateStatusFailed updates status for a failed reconciliation.
func (r *Reconciler) updateStatusFailed(ctx context.Context, tc *butlerv1alpha1.TenantCluster, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseFailed
	// TODO: Update conditions with error details

	if updateErr := r.Status().Update(ctx, tc); updateErr != nil {
		logger.Error(updateErr, "failed to update status")
		return ctrl.Result{}, updateErr
	}

	// Requeue with backoff
	return ctrl.Result{RequeueAfter: 30 * time.Second}, err
}

// updateStatusSuccess updates status and calculates requeue interval.
func (r *Reconciler) updateStatusSuccess(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// TODO: Update observed state from tenant cluster
	// TODO: Update conditions

	if err := r.Status().Update(ctx, tc); err != nil {
		logger.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	// Calculate requeue interval (tiered refresh)
	requeueAfter := r.calculateRequeueInterval(tc)
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// calculateRequeueInterval returns the appropriate requeue duration.
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
	uidSuffix := string(tc.UID)[:8]
	return fmt.Sprintf("%s-%s", tc.Name, uidSuffix)
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.TenantCluster{}).
		Owns(&corev1.Namespace{}).
		Owns(&corev1.Secret{}).
		Owns(&rbacv1.RoleBinding{}).
		// TODO: Add watches for CAPI resources when implementing full reconciliation
		// Owns(&clusterv1.Cluster{}).
		// Owns(&clusterv1.MachineDeployment{}).
		Named("tenantcluster").
		Complete(r)
}
