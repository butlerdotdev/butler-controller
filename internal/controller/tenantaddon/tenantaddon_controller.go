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
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/addons"
)

const (
	ReasonAddonNotFound   = "AddonNotFound"
	ReasonClusterNotFound = "ClusterNotFound"
	ReasonClusterNotReady = "ClusterNotReady"
	ReasonKubeconfigError = "KubeconfigError"
	ReasonInstallFailed   = "InstallFailed"
	ReasonInstalled       = "Installed"
)

// Reconciler reconciles a TenantAddon object.
type Reconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Installer *addons.Installer
	APIReader client.Reader // Uncached reader for cross-type lookups
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantaddons,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantaddons/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantaddons/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=addondefinitions,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the TenantAddon instance
	addon := &butlerv1alpha1.TenantAddon{}
	if err := r.Get(ctx, req.NamespacedName, addon); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("reconciling TenantAddon",
		"name", addon.Name,
		"namespace", addon.Namespace,
		"addon", addon.Spec.Addon,
		"phase", addon.Status.Phase)

	// Handle deletion
	if !addon.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, req, addon)
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

	// Get addon definition
	addonDef, err := r.getAddonDefinition(ctx, addon)
	if err != nil {
		logger.Error(err, "failed to get AddonDefinition")
		return r.setFailedStatus(ctx, req, ReasonAddonNotFound, err.Error())
	}

	// Get target cluster
	cluster, err := r.getTargetCluster(ctx, addon)
	if err != nil {
		logger.Error(err, "failed to get target cluster")
		return r.setPendingStatus(ctx, req, ReasonClusterNotFound, "Waiting for cluster: "+err.Error())
	}

	// Check if cluster is ready
	if cluster.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady {
		logger.V(1).Info("waiting for cluster to be ready", "clusterPhase", cluster.Status.Phase)
		return r.setPendingStatus(ctx, req, ReasonClusterNotReady,
			fmt.Sprintf("Cluster is %s, waiting for Ready", cluster.Status.Phase))
	}

	// Get kubeconfig for tenant cluster
	kubeconfig, err := r.getTenantKubeconfig(ctx, cluster)
	if err != nil {
		logger.Error(err, "failed to get kubeconfig")
		return r.setFailedStatus(ctx, req, ReasonKubeconfigError, err.Error())
	}

	// Determine version
	version := addon.Spec.Version
	if version == "" {
		version = addonDef.Spec.Chart.DefaultVersion
	}

	if addon.Status.Phase == butlerv1alpha1.TenantAddonPhaseInstalled &&
		addon.Status.InstalledVersion == version {
		logger.V(1).Info("addon already installed", "version", version)
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	// Update status to installing (re-fetch first to avoid conflict)
	if addon.Status.Phase != butlerv1alpha1.TenantAddonPhaseInstalled {
		if err := r.Get(ctx, req.NamespacedName, addon); err != nil {
			return ctrl.Result{}, err
		}
		addon.Status.Phase = butlerv1alpha1.TenantAddonPhaseInstalling
		addon.Status.Message = "Installing addon"
		if err := r.Status().Update(ctx, addon); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Install using shared Installer
	logger.Info("installing addon",
		"chart", addonDef.Spec.Chart.Name,
		"repository", addonDef.Spec.Chart.Repository,
		"version", version,
		"namespace", addonDef.GetNamespace(),
		"release", addonDef.GetReleaseName())

	if err := r.Installer.InstallChart(ctx, kubeconfig, addons.ChartInstallOptions{
		RepoName:    "butler-" + addon.Spec.Addon,
		RepoURL:     addonDef.Spec.Chart.Repository,
		ChartName:   addonDef.Spec.Chart.Name,
		ReleaseName: addonDef.GetReleaseName(),
		Namespace:   addonDef.GetNamespace(),
		Version:     version,
		Values:      nil, // TODO: merge addon.Spec.Values with addonDef.Spec.Defaults.Values
	}); err != nil {
		logger.Error(err, "failed to install addon")
		return r.setFailedStatus(ctx, req, ReasonInstallFailed, err.Error())
	}

	// Re-fetch to get latest resourceVersion after helm install
	if err := r.Get(ctx, req.NamespacedName, addon); err != nil {
		return ctrl.Result{}, err
	}

	// Update status to installed
	addon.Status.Phase = butlerv1alpha1.TenantAddonPhaseInstalled
	addon.Status.InstalledVersion = version
	addon.Status.Message = ""
	addon.Status.ObservedGeneration = addon.Generation
	now := metav1.Now()
	addon.Status.LastTransitionTime = &now
	addon.Status.HelmRelease = &butlerv1alpha1.HelmReleaseStatus{
		Name:      addonDef.GetReleaseName(),
		Namespace: addonDef.GetNamespace(),
		Status:    "deployed",
	}

	if err := r.Status().Update(ctx, addon); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("addon installed successfully",
		"release", addonDef.GetReleaseName(),
		"namespace", addonDef.GetNamespace(),
		"version", version)

	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// getAddonDefinition retrieves the AddonDefinition for this addon.
func (r *Reconciler) getAddonDefinition(ctx context.Context, addon *butlerv1alpha1.TenantAddon) (*butlerv1alpha1.AddonDefinition, error) {
	if addon.Spec.Addon == "" {
		return nil, fmt.Errorf("addon name not specified")
	}

	addonDef := &butlerv1alpha1.AddonDefinition{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: addon.Spec.Addon}, addonDef); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("AddonDefinition %q not found", addon.Spec.Addon)
		}
		return nil, fmt.Errorf("failed to get AddonDefinition: %w", err)
	}

	return addonDef, nil
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

// getTenantKubeconfig retrieves the kubeconfig for the tenant cluster.
// Uses the same pattern as TenantCluster controller.
func (r *Reconciler) getTenantKubeconfig(ctx context.Context, cluster *butlerv1alpha1.TenantCluster) ([]byte, error) {
	// Prefer kubeconfigSecretRef if set
	var secretName string
	if cluster.Status.KubeconfigSecretRef != nil && cluster.Status.KubeconfigSecretRef.Name != "" {
		secretName = cluster.Status.KubeconfigSecretRef.Name
	} else {
		// Fallback to convention: <cluster-name>-admin-kubeconfig
		secretName = fmt.Sprintf("%s-admin-kubeconfig", cluster.Name)
	}

	secret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{
		Name:      secretName,
		Namespace: cluster.Status.TenantNamespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig secret %s/%s: %w",
			cluster.Status.TenantNamespace, secretName, err)
	}

	// Try common kubeconfig key names
	for _, key := range []string{"admin.conf", "value", "kubeconfig"} {
		if data, ok := secret.Data[key]; ok {
			return data, nil
		}
	}

	return nil, fmt.Errorf("kubeconfig not found in secret (tried: admin.conf, value, kubeconfig)")
}

// handleDeletion handles TenantAddon deletion.
func (r *Reconciler) handleDeletion(ctx context.Context, req ctrl.Request, addon *butlerv1alpha1.TenantAddon) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("handling TenantAddon deletion", "name", addon.Name, "namespace", addon.Namespace)

	// Update phase to Deleting
	if addon.Status.Phase != butlerv1alpha1.TenantAddonPhaseDeleting {
		addon.Status.Phase = butlerv1alpha1.TenantAddonPhaseDeleting
		if err := r.Status().Update(ctx, addon); err != nil {
			if !apierrors.IsConflict(err) {
				return ctrl.Result{}, err
			}
		}
	}

	// Get cluster
	cluster, err := r.getTargetCluster(ctx, addon)
	if err != nil {
		// Cluster gone, just remove finalizer
		logger.Info("target cluster not found, skipping uninstall")
		// Re-fetch before removing finalizer
		if err := r.Get(ctx, req.NamespacedName, addon); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(addon, butlerv1alpha1.FinalizerTenantAddon)
		return ctrl.Result{}, r.Update(ctx, addon)
	}

	// Only uninstall if cluster is ready
	if cluster.Status.Phase == butlerv1alpha1.TenantClusterPhaseReady {
		kubeconfig, err := r.getTenantKubeconfig(ctx, cluster)
		if err != nil {
			logger.Error(err, "failed to get kubeconfig for uninstall, proceeding with finalizer removal")
		} else {
			// Get addon definition for release name
			addonDef, _ := r.getAddonDefinition(ctx, addon)
			releaseName := addon.Name
			namespace := addon.Name
			if addonDef != nil {
				releaseName = addonDef.GetReleaseName()
				namespace = addonDef.GetNamespace()
			}

			logger.Info("uninstalling helm release", "release", releaseName, "namespace", namespace)
			if err := r.Installer.UninstallChart(ctx, kubeconfig, releaseName, namespace); err != nil {
				// "not found" is expected if already uninstalled
				if strings.Contains(err.Error(), "not found") {
					logger.Info("helm release already removed", "release", releaseName)
				} else {
					logger.Error(err, "helm uninstall failed, proceeding with finalizer removal")
				}
			}
		}
	} else {
		logger.Info("cluster not ready, skipping uninstall", "clusterPhase", cluster.Status.Phase)
	}

	// Re-fetch before removing finalizer to get latest resourceVersion
	if err := r.Get(ctx, req.NamespacedName, addon); err != nil {
		if apierrors.IsNotFound(err) {
			// Already deleted, nothing to do
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(addon, butlerv1alpha1.FinalizerTenantAddon)
	if err := r.Update(ctx, addon); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("TenantAddon deletion complete", "name", addon.Name)
	return ctrl.Result{}, nil
}

// setFailedStatus sets the addon status to Failed.
func (r *Reconciler) setFailedStatus(ctx context.Context, req ctrl.Request, reason, message string) (ctrl.Result, error) {
	addon := &butlerv1alpha1.TenantAddon{}
	if err := r.Get(ctx, req.NamespacedName, addon); err != nil {
		return ctrl.Result{}, err
	}

	addon.Status.Phase = butlerv1alpha1.TenantAddonPhaseFailed
	addon.Status.Message = message
	addon.Status.ObservedGeneration = addon.Generation
	now := metav1.Now()
	addon.Status.LastTransitionTime = &now

	if err := r.Status().Update(ctx, addon); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
}

// setPendingStatus sets the addon status to Pending.
func (r *Reconciler) setPendingStatus(ctx context.Context, req ctrl.Request, reason, message string) (ctrl.Result, error) {
	addon := &butlerv1alpha1.TenantAddon{}
	if err := r.Get(ctx, req.NamespacedName, addon); err != nil {
		return ctrl.Result{}, err
	}

	addon.Status.Phase = butlerv1alpha1.TenantAddonPhasePending
	addon.Status.Message = message
	addon.Status.ObservedGeneration = addon.Generation
	now := metav1.Now()
	addon.Status.LastTransitionTime = &now

	if err := r.Status().Update(ctx, addon); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(),
		&butlerv1alpha1.AddonDefinition{},
		"metadata.name",
		func(o client.Object) []string {
			return []string{o.GetName()}
		}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.TenantAddon{}).
		Named("tenantaddon").
		Complete(r)
}
