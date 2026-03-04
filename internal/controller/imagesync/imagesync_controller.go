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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const (
	// ButlerConfigSingletonName is the expected name of the ButlerConfig singleton.
	ButlerConfigSingletonName = "butler"

	// Requeue intervals.
	requeueShort = 15 * time.Second
	requeueLong  = 60 * time.Second
	requeueReady = 10 * time.Minute
)

// Reconciler reconciles an ImageSync object.
// This controller manages the full lifecycle of syncing an image from the
// Butler Image Factory to an infrastructure provider. It handles finalizers,
// phase management, and dispatches to provider-specific fulfillment logic
// (Harvester VirtualMachineImage, Nutanix Prism Central image API).
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=imagesyncs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=imagesyncs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=imagesyncs/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=providerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=butlerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	is := &butlerv1alpha1.ImageSync{}
	if err := r.Get(ctx, req.NamespacedName, is); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.V(1).Info("reconciling ImageSync", "name", is.Name, "phase", is.Status.Phase)

	// Handle deletion
	if !is.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, is)
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(is, butlerv1alpha1.FinalizerImageSync) {
		controllerutil.AddFinalizer(is, butlerv1alpha1.FinalizerImageSync)
		if err := r.Update(ctx, is); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Set initial phase if not set
	if is.Status.Phase == "" {
		is.SetPhase(butlerv1alpha1.ImageSyncPhasePending)
		is.Status.ObservedGeneration = is.Generation
		if err := r.Status().Update(ctx, is); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Fetch ProviderConfig
	pc, err := r.getProviderConfig(ctx, is)
	if err != nil {
		return r.setFailed(ctx, is, "ProviderConfigNotFound", err.Error())
	}

	// Fetch ButlerConfig for factory URL
	bc, err := r.getButlerConfig(ctx)
	if err != nil {
		return r.setFailed(ctx, is, "ButlerConfigNotFound", err.Error())
	}

	if !bc.IsImageFactoryConfigured() {
		return r.setFailed(ctx, is, "ImageFactoryNotConfigured", "ButlerConfig.spec.imageFactory is not configured")
	}

	// Dispatch to phase handler
	switch is.Status.Phase {
	case butlerv1alpha1.ImageSyncPhasePending:
		return r.reconcilePending(ctx, is, pc, bc)
	case butlerv1alpha1.ImageSyncPhaseBuilding:
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	case butlerv1alpha1.ImageSyncPhaseDownloading, butlerv1alpha1.ImageSyncPhaseUploading:
		return r.reconcileInProgress(ctx, is, pc, bc)
	case butlerv1alpha1.ImageSyncPhaseFailed:
		return ctrl.Result{RequeueAfter: requeueLong}, nil
	case butlerv1alpha1.ImageSyncPhaseReady:
		return ctrl.Result{RequeueAfter: requeueReady}, nil
	}

	return ctrl.Result{}, nil
}

// reconcilePending handles the Pending phase: checks if the image already
// exists on the provider, and if not, initiates the transfer.
func (r *Reconciler) reconcilePending(ctx context.Context, is *butlerv1alpha1.ImageSync, pc *butlerv1alpha1.ProviderConfig, bc *butlerv1alpha1.ButlerConfig) (ctrl.Result, error) {
	factoryURL := bc.GetImageFactoryURL()
	artifactURL := buildArtifactURL(factoryURL, is.Spec.FactoryRef, is.Spec.Format)
	imageName := buildProviderImageName(is)

	// Store artifact URL in status
	is.Status.ArtifactURL = artifactURL

	switch pc.Spec.Provider {
	case butlerv1alpha1.ProviderTypeHarvester:
		return r.reconcileHarvester(ctx, is, pc, imageName, artifactURL)
	case butlerv1alpha1.ProviderTypeNutanix:
		apiKey, _ := r.getFactoryAPIKey(ctx, bc)
		return r.reconcileNutanix(ctx, is, pc, bc, imageName, artifactURL, apiKey)
	default:
		return r.setFailed(ctx, is, "UnsupportedProvider",
			fmt.Sprintf("image sync not supported for provider type %q", pc.Spec.Provider))
	}
}

// reconcileInProgress checks the status of an in-progress image sync.
func (r *Reconciler) reconcileInProgress(ctx context.Context, is *butlerv1alpha1.ImageSync, pc *butlerv1alpha1.ProviderConfig, bc *butlerv1alpha1.ButlerConfig) (ctrl.Result, error) {
	imageName := buildProviderImageName(is)

	switch pc.Spec.Provider {
	case butlerv1alpha1.ProviderTypeHarvester:
		return r.pollHarvesterImage(ctx, is, pc, imageName)
	case butlerv1alpha1.ProviderTypeNutanix:
		return r.pollNutanixImage(ctx, is, pc, imageName)
	default:
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
}

// handleDeletion removes the finalizer. Provider image cleanup is best-effort.
func (r *Reconciler) handleDeletion(ctx context.Context, is *butlerv1alpha1.ImageSync) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(is, butlerv1alpha1.FinalizerImageSync) {
		return ctrl.Result{}, nil
	}

	logger.Info("removing ImageSync finalizer", "name", is.Name)

	// Re-fetch to avoid conflicts
	if err := r.Get(ctx, types.NamespacedName{Name: is.Name, Namespace: is.Namespace}, is); err != nil {
		return ctrl.Result{}, err
	}

	controllerutil.RemoveFinalizer(is, butlerv1alpha1.FinalizerImageSync)
	return ctrl.Result{}, r.Update(ctx, is)
}

// setFailed sets the ImageSync to Failed phase with reason and message.
func (r *Reconciler) setFailed(ctx context.Context, is *butlerv1alpha1.ImageSync, reason, message string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(fmt.Errorf("%s", message), "image sync failed", "reason", reason)

	is.SetFailure(reason, message)
	is.Status.ObservedGeneration = is.Generation
	meta.SetStatusCondition(&is.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: is.Generation,
	})

	if err := r.Status().Update(ctx, is); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueLong}, nil
}

// setReady transitions the ImageSync to Ready phase.
func (r *Reconciler) setReady(ctx context.Context, is *butlerv1alpha1.ImageSync, providerImageRef string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("image sync ready", "providerImageRef", providerImageRef)

	is.SetPhase(butlerv1alpha1.ImageSyncPhaseReady)
	is.Status.ProviderImageRef = providerImageRef
	is.Status.ObservedGeneration = is.Generation
	is.Status.FailureReason = ""
	is.Status.FailureMessage = ""
	meta.SetStatusCondition(&is.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             butlerv1alpha1.ReasonImageReady,
		Message:            fmt.Sprintf("Image synced to provider: %s", providerImageRef),
		ObservedGeneration: is.Generation,
	})

	if err := r.Status().Update(ctx, is); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueReady}, nil
}

// getProviderConfig fetches the ProviderConfig referenced by the ImageSync.
func (r *Reconciler) getProviderConfig(ctx context.Context, is *butlerv1alpha1.ImageSync) (*butlerv1alpha1.ProviderConfig, error) {
	pc := &butlerv1alpha1.ProviderConfig{}
	ns := is.Spec.ProviderConfigRef.Namespace
	if ns == "" {
		ns = is.Namespace
	}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      is.Spec.ProviderConfigRef.Name,
		Namespace: ns,
	}, pc); err != nil {
		return nil, fmt.Errorf("failed to get ProviderConfig %s/%s: %w", ns, is.Spec.ProviderConfigRef.Name, err)
	}
	return pc, nil
}

// getButlerConfig fetches the singleton ButlerConfig.
func (r *Reconciler) getButlerConfig(ctx context.Context) (*butlerv1alpha1.ButlerConfig, error) {
	bc := &butlerv1alpha1.ButlerConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: ButlerConfigSingletonName}, bc); err != nil {
		return nil, fmt.Errorf("failed to get ButlerConfig: %w", err)
	}
	return bc, nil
}

// getFactoryAPIKey retrieves the factory API key from the credentials Secret.
func (r *Reconciler) getFactoryAPIKey(ctx context.Context, bc *butlerv1alpha1.ButlerConfig) (string, error) {
	if bc.Spec.ImageFactory == nil || bc.Spec.ImageFactory.CredentialsRef == nil {
		return "", nil
	}

	ref := bc.Spec.ImageFactory.CredentialsRef
	secret, err := r.getSecret(ctx, ref.Name, ref.Namespace)
	if err != nil {
		return "", err
	}

	key := ref.Key
	if key == "" {
		key = "apiKey"
	}

	apiKey, ok := secret[key]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %s/%s", key, ref.Namespace, ref.Name)
	}
	return string(apiKey), nil
}

// getSecret fetches a Secret and returns its data map.
func (r *Reconciler) getSecret(ctx context.Context, name, namespace string) (map[string][]byte, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, secret); err != nil {
		return nil, fmt.Errorf("failed to get secret %s/%s: %w", namespace, name, err)
	}
	return secret.Data, nil
}

// getProviderCredentials fetches provider credentials from the referenced Secret.
func (r *Reconciler) getProviderCredentials(ctx context.Context, pc *butlerv1alpha1.ProviderConfig) (map[string][]byte, error) {
	ns := pc.Spec.CredentialsRef.Namespace
	if ns == "" {
		ns = pc.Namespace
	}
	return r.getSecret(ctx, pc.Spec.CredentialsRef.Name, ns)
}

// buildArtifactURL constructs the factory download URL for an image.
// Uses the Talos Image Factory URL format: {factory}/image/{schematic}/{version}/{platform}-{arch}.{format}
// The platform is "nocloud" for KubeVirt/cloud-init targets (Harvester, Nutanix).
func buildArtifactURL(factoryURL string, ref butlerv1alpha1.ImageFactoryRef, format string) string {
	factoryURL = strings.TrimSuffix(factoryURL, "/")
	if format == "" {
		format = "qcow2"
	}
	arch := ref.Arch
	if arch == "" {
		arch = "amd64"
	}
	platform := ref.Platform
	if platform == "" {
		platform = "nocloud"
	}
	return fmt.Sprintf("%s/image/%s/%s/%s-%s.%s", factoryURL, ref.SchematicID, ref.Version, platform, arch, format)
}

// buildProviderImageName generates a deterministic image name for the provider.
func buildProviderImageName(is *butlerv1alpha1.ImageSync) string {
	if is.Spec.DisplayName != "" {
		return sanitizeName(is.Spec.DisplayName)
	}
	// Build from schematic + version: talos-v1-12-4-amd64-butler
	version := strings.ReplaceAll(is.Spec.FactoryRef.Version, ".", "-")
	arch := is.Spec.FactoryRef.Arch
	if arch == "" {
		arch = "amd64"
	}
	schematicPrefix := is.Spec.FactoryRef.SchematicID
	if len(schematicPrefix) > 8 {
		schematicPrefix = schematicPrefix[:8]
	}
	name := fmt.Sprintf("talos-%s-%s-%s-butler", version, arch, schematicPrefix)
	return sanitizeName(name)
}

// sanitizeName converts a string into a valid Kubernetes resource name.
func sanitizeName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, " ", "-")
	name = strings.ReplaceAll(name, "_", "-")
	// Truncate to 63 chars (K8s label limit)
	if len(name) > 63 {
		name = name[:63]
	}
	// Trim trailing hyphens
	name = strings.TrimRight(name, "-")
	return name
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.ImageSync{}).
		Named("imagesync").
		Complete(r)
}
