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

package tenantaddon

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// Reconciler reconciles a TenantAddon object.
//
// TenantAddon represents an addon to be installed in a TenantCluster.
// Unlike addons in TenantCluster.spec.addons (which are monotonic/install-only),
// TenantAddons support full lifecycle management including removal.
//
// Key behaviors:
// - Waits for target TenantCluster to be Ready before installing
// - Respects DependsOn for addon ordering
// - Supports both Butler built-in addons and custom Helm charts
// - Deletion of TenantAddon CR triggers addon removal from cluster
// - Uses Helm SDK for installation/upgrade/removal
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantaddons,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantaddons/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantaddons/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile handles TenantAddon reconciliation.
//
// The reconciliation loop:
// 1. Check if TenantAddon is being deleted
//   - If yes, uninstall addon from target cluster and remove finalizer
//
// 2. Add finalizer if not present
// 3. Wait for target TenantCluster to be Ready
// 4. Wait for dependencies (DependsOn) to be Ready
// 5. Install or upgrade addon
// 6. Update status with Helm release info
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the TenantAddon instance
	addon := &butlerv1alpha1.TenantAddon{}
	if err := r.Get(ctx, req.NamespacedName, addon); err != nil {
		if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "unable to fetch TenantAddon")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	logger.Info("reconciling TenantAddon",
		"name", addon.Name,
		"namespace", addon.Namespace,
		"cluster", addon.Spec.ClusterRef.Name,
		"addon", addon.Spec.Addon,
		"phase", addon.Status.Phase)

	// Handle deletion
	if !addon.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, addon)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(addon, butlerv1alpha1.FinalizerTenantAddon) {
		controllerutil.AddFinalizer(addon, butlerv1alpha1.FinalizerTenantAddon)
		if err := r.Update(ctx, addon); err != nil {
			logger.Error(err, "failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Check if target cluster is ready
	cluster, err := r.getTargetCluster(ctx, addon)
	if err != nil {
		logger.Error(err, "failed to get target cluster")
		return r.updateStatusPending(ctx, addon, "WaitingForCluster", "Target cluster not found")
	}

	if cluster.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady {
		logger.Info("waiting for target cluster to be ready")
		return r.updateStatusPending(ctx, addon, "WaitingForCluster", "Target cluster is not ready")
	}

	// Check dependencies
	if !r.dependenciesMet(ctx, addon) {
		logger.Info("waiting for dependencies")
		return r.updateStatusPending(ctx, addon, "WaitingForDependencies", "Dependencies not yet ready")
	}

	// Install or upgrade addon
	if err := r.installOrUpgrade(ctx, addon, cluster); err != nil {
		logger.Error(err, "failed to install/upgrade addon")
		return r.updateStatusFailed(ctx, addon, err)
	}

	return r.updateStatusInstalled(ctx, addon)
}

// getTargetCluster retrieves the TenantCluster referenced by the addon.
func (r *Reconciler) getTargetCluster(ctx context.Context, addon *butlerv1alpha1.TenantAddon) (*butlerv1alpha1.TenantCluster, error) {
	cluster := &butlerv1alpha1.TenantCluster{}
	key := client.ObjectKey{
		Name:      addon.Spec.ClusterRef.Name,
		Namespace: addon.Namespace,
	}
	if err := r.Get(ctx, key, cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

// dependenciesMet checks if all dependencies are satisfied.
func (r *Reconciler) dependenciesMet(ctx context.Context, addon *butlerv1alpha1.TenantAddon) bool {
	logger := log.FromContext(ctx)

	for _, dep := range addon.Spec.DependsOn {
		depAddon := &butlerv1alpha1.TenantAddon{}
		key := client.ObjectKey{
			Name:      dep.Name,
			Namespace: addon.Namespace,
		}
		if err := r.Get(ctx, key, depAddon); err != nil {
			logger.V(1).Info("dependency not found", "dependency", dep.Name)
			return false
		}
		if depAddon.Status.Phase != butlerv1alpha1.TenantAddonPhaseInstalled {
			logger.V(1).Info("dependency not ready", "dependency", dep.Name, "phase", depAddon.Status.Phase)
			return false
		}
	}
	return true
}

// installOrUpgrade installs or upgrades the addon.
func (r *Reconciler) installOrUpgrade(ctx context.Context, addon *butlerv1alpha1.TenantAddon, cluster *butlerv1alpha1.TenantCluster) error {
	logger := log.FromContext(ctx)
	logger.V(1).Info("installing/upgrading addon")

	// TODO: Implement addon installation
	// 1. Get kubeconfig for tenant cluster
	// 2. Create Helm client for tenant cluster
	// 3. Determine if this is a Butler built-in addon or custom Helm chart
	// 4. Install or upgrade Helm release
	// 5. Wait for release to be healthy

	return nil
}

// handleDeletion handles TenantAddon cleanup.
func (r *Reconciler) handleDeletion(ctx context.Context, addon *butlerv1alpha1.TenantAddon) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("handling TenantAddon deletion", "name", addon.Name)

	// TODO: Implement addon removal
	// 1. Get kubeconfig for tenant cluster
	// 2. Create Helm client for tenant cluster
	// 3. Uninstall Helm release

	// Remove finalizer to allow deletion
	controllerutil.RemoveFinalizer(addon, butlerv1alpha1.FinalizerTenantAddon)
	if err := r.Update(ctx, addon); err != nil {
		logger.Error(err, "failed to remove finalizer")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// updateStatusPending updates status to pending with a reason.
func (r *Reconciler) updateStatusPending(ctx context.Context, addon *butlerv1alpha1.TenantAddon, reason, message string) (ctrl.Result, error) {
	addon.Status.Phase = butlerv1alpha1.TenantAddonPhasePending
	addon.Status.Message = message
	// TODO: Update conditions

	if err := r.Status().Update(ctx, addon); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// updateStatusFailed updates status to failed.
func (r *Reconciler) updateStatusFailed(ctx context.Context, addon *butlerv1alpha1.TenantAddon, err error) (ctrl.Result, error) {
	addon.Status.Phase = butlerv1alpha1.TenantAddonPhaseFailed
	addon.Status.Message = err.Error()
	// TODO: Update conditions

	if updateErr := r.Status().Update(ctx, addon); updateErr != nil {
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{RequeueAfter: 1 * time.Minute}, err
}

// updateStatusInstalled updates status to installed.
func (r *Reconciler) updateStatusInstalled(ctx context.Context, addon *butlerv1alpha1.TenantAddon) (ctrl.Result, error) {
	addon.Status.Phase = butlerv1alpha1.TenantAddonPhaseInstalled
	addon.Status.InstalledVersion = addon.Spec.Version
	addon.Status.Message = ""
	// TODO: Update conditions and HelmRelease status

	if err := r.Status().Update(ctx, addon); err != nil {
		return ctrl.Result{}, err
	}
	// Check health periodically
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.TenantAddon{}).
		Named("tenantaddon").
		Complete(r)
}
