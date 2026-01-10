/*
Copyright 2024 The Butler Authors.

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

package managementaddon

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/addons"
)

const (
	finalizerName = "managementaddon.butler.butlerlabs.dev/finalizer"
)

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=managementaddons,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=managementaddons/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=managementaddons/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=addondefinitions,verbs=get;list;watch

// Reconciler reconciles a ManagementAddon object
type Reconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Installer  *addons.Installer
	APIReader  client.Reader // Uncached reader for AddonDefinition lookups
	Kubeconfig []byte        // Kubeconfig for management cluster (set from main.go)
}

// Reconcile handles ManagementAddon reconciliation
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the ManagementAddon
	addon := &butlerv1alpha1.ManagementAddon{}
	if err := r.Get(ctx, req.NamespacedName, addon); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion
	if !addon.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, addon)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(addon, finalizerName) {
		controllerutil.AddFinalizer(addon, finalizerName)
		if err := r.Update(ctx, addon); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Skip if paused
	if addon.Spec.Paused {
		logger.V(1).Info("addon reconciliation is paused")
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	// Get AddonDefinition
	addonDef, err := r.getAddonDefinition(ctx, addon)
	if err != nil {
		logger.Error(err, "failed to get AddonDefinition")
		return r.updateStatus(ctx, addon, butlerv1alpha1.ManagementAddonPhaseFailed, "", err.Error())
	}

	// Determine version to install
	version := addon.Spec.Version
	if version == "" {
		version = addonDef.Spec.Chart.DefaultVersion
	}

	// Short-circuit if already installed at target version
	if addon.Status.Phase == butlerv1alpha1.ManagementAddonPhaseInstalled &&
		addon.Status.InstalledVersion == version {
		logger.V(1).Info("addon already installed", "version", version)
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	// Update status to installing
	if addon.Status.Phase != butlerv1alpha1.ManagementAddonPhaseInstalling {
		if _, err := r.updateStatus(ctx, addon, butlerv1alpha1.ManagementAddonPhaseInstalling, "", "Installing addon"); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Get namespace from AddonDefinition defaults
	namespace := addon.Name + "-system" // fallback
	if addonDef.Spec.Defaults != nil && addonDef.Spec.Defaults.Namespace != "" {
		namespace = addonDef.Spec.Defaults.Namespace
	}

	// Get kubeconfig for management cluster
	kubeconfig, err := r.getKubeconfig()
	if err != nil {
		logger.Error(err, "failed to get kubeconfig")
		return r.updateStatus(ctx, addon, butlerv1alpha1.ManagementAddonPhaseFailed, "", fmt.Sprintf("Failed to get kubeconfig: %v", err))
	}

	// Build install options matching ChartInstallOptions struct
	opts := addons.ChartInstallOptions{
		RepoName:    "butler-" + addon.Spec.Addon,
		RepoURL:     addonDef.Spec.Chart.Repository,
		ChartName:   addonDef.Spec.Chart.Name,
		ReleaseName: addon.Name,
		Namespace:   namespace,
		Version:     version,
		Values:      make(map[string]string),
		Timeout:     "10m",
	}

	// Install chart to management cluster
	logger.Info("installing addon", "chart", opts.ChartName, "version", version, "namespace", namespace)

	if err := r.Installer.InstallChart(ctx, kubeconfig, opts); err != nil {
		logger.Error(err, "failed to install chart")
		return r.updateStatus(ctx, addon, butlerv1alpha1.ManagementAddonPhaseFailed, "", fmt.Sprintf("Install failed: %v", err))
	}

	logger.Info("addon installed successfully", "release", addon.Name, "namespace", namespace, "version", version)

	// Update status to installed
	addon.Status.HelmRelease = &butlerv1alpha1.HelmReleaseStatus{
		Name:      addon.Name,
		Namespace: namespace,
	}

	return r.updateStatus(ctx, addon, butlerv1alpha1.ManagementAddonPhaseInstalled, version, "Addon installed successfully")
}

// handleDeletion handles the deletion of a ManagementAddon
func (r *Reconciler) handleDeletion(ctx context.Context, addon *butlerv1alpha1.ManagementAddon) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(addon, finalizerName) {
		return ctrl.Result{}, nil
	}

	// Update status to uninstalling
	addon.Status.Phase = butlerv1alpha1.ManagementAddonPhaseUninstalling
	addon.Status.Message = "Uninstalling addon"
	if err := r.Status().Update(ctx, addon); err != nil {
		logger.V(1).Info("failed to update status during deletion", "error", err)
	}

	// Get kubeconfig
	kubeconfig, err := r.getKubeconfig()
	if err != nil {
		logger.Error(err, "failed to get kubeconfig, proceeding with finalizer removal")
	} else {
		// Get AddonDefinition for namespace info
		addonDef, err := r.getAddonDefinition(ctx, addon)
		if err != nil {
			logger.Info("AddonDefinition not found during deletion, proceeding with finalizer removal")
		} else {
			// Get namespace
			namespace := addon.Name + "-system"
			if addonDef.Spec.Defaults != nil && addonDef.Spec.Defaults.Namespace != "" {
				namespace = addonDef.Spec.Defaults.Namespace
			}

			// Uninstall Helm release from management cluster
			if err := r.Installer.UninstallChart(ctx, kubeconfig, addon.Name, namespace); err != nil {
				logger.Error(err, "helm uninstall failed, proceeding with finalizer removal")
			} else {
				logger.Info("addon uninstalled successfully", "release", addon.Name)
			}
		}
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(addon, finalizerName)
	if err := r.Update(ctx, addon); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// getAddonDefinition retrieves the AddonDefinition for this addon
func (r *Reconciler) getAddonDefinition(ctx context.Context, addon *butlerv1alpha1.ManagementAddon) (*butlerv1alpha1.AddonDefinition, error) {
	addonDef := &butlerv1alpha1.AddonDefinition{}
	// Use APIReader for uncached lookup (AddonDefinitions are cluster-scoped and not watched)
	if err := r.APIReader.Get(ctx, client.ObjectKey{Name: addon.Spec.Addon}, addonDef); err != nil {
		return nil, fmt.Errorf("AddonDefinition %q not found: %w", addon.Spec.Addon, err)
	}
	return addonDef, nil
}

// getKubeconfig returns the kubeconfig for the management cluster
func (r *Reconciler) getKubeconfig() ([]byte, error) {
	// If kubeconfig was set explicitly (e.g., from --kubeconfig flag), use it
	if r.Kubeconfig != nil {
		return r.Kubeconfig, nil
	}

	// Try to build in-cluster kubeconfig
	return r.buildInClusterKubeconfig()
}

// buildInClusterKubeconfig constructs a kubeconfig from in-cluster service account
func (r *Reconciler) buildInClusterKubeconfig() ([]byte, error) {
	tokenPath := "/var/run/secrets/kubernetes.io/serviceaccount/token"
	caPath := "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

	// Check if we're running in-cluster
	if _, err := os.Stat(tokenPath); os.IsNotExist(err) {
		// Not in-cluster, try KUBECONFIG env or default
		kubeconfigPath := os.Getenv("KUBECONFIG")
		if kubeconfigPath == "" {
			homeDir, _ := os.UserHomeDir()
			kubeconfigPath = homeDir + "/.kube/config"
		}
		return os.ReadFile(kubeconfigPath)
	}

	// Read in-cluster credentials
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read service account token: %w", err)
	}

	caCert, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA cert: %w", err)
	}

	// Get API server address
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, fmt.Errorf("KUBERNETES_SERVICE_HOST or KUBERNETES_SERVICE_PORT not set")
	}

	// Build kubeconfig YAML with embedded CA cert
	kubeconfig := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: %s
    server: https://%s:%s
  name: in-cluster
contexts:
- context:
    cluster: in-cluster
    user: in-cluster
  name: in-cluster
current-context: in-cluster
users:
- name: in-cluster
  user:
    token: %s
`, base64Encode(caCert), host, port, string(token))

	return []byte(kubeconfig), nil
}

// base64Encode encodes bytes to base64
func base64Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// updateStatus updates the ManagementAddon status
func (r *Reconciler) updateStatus(ctx context.Context, addon *butlerv1alpha1.ManagementAddon, phase butlerv1alpha1.ManagementAddonPhase, installedVersion, message string) (ctrl.Result, error) {
	addon.Status.Phase = phase
	addon.Status.Message = message
	addon.Status.ObservedGeneration = addon.Generation

	if installedVersion != "" {
		addon.Status.InstalledVersion = installedVersion
	}

	if err := r.Status().Update(ctx, addon); err != nil {
		return ctrl.Result{}, err
	}

	if phase == butlerv1alpha1.ManagementAddonPhaseFailed {
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.ManagementAddon{}).
		Named("managementaddon").
		Complete(r)
}
