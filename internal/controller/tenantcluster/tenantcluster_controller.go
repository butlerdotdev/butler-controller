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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/addons"
	"github.com/butlerdotdev/butler-controller/internal/tenant"
)

const (
	ButlerConfigSingletonName = "butler"

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

type Reconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Installer               *addons.Installer
	ClientManager           *tenant.ClientManager
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=teams,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=butlerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=providerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantaddons,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machinedeployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machinedeployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=kubeadmconfigtemplates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=stewardcontrolplanes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=stewardcontrolplanes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvesterclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvestermachinetemplates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	reconcileStart := time.Now()

	tc := &butlerv1alpha1.TenantCluster{}
	if err := r.Get(ctx, req.NamespacedName, tc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	defer func() {
		reconcileDuration.WithLabelValues(tc.Namespace, tc.Name).Observe(time.Since(reconcileStart).Seconds())
		recordPhase(tc.Namespace, tc.Name, string(tc.Status.Phase))
	}()

	logger.Info("reconciling TenantCluster",
		"name", tc.Name,
		"namespace", tc.Namespace,
		"phase", tc.Status.Phase)

	if !tc.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, tc)
	}

	if !controllerutil.ContainsFinalizer(tc, butlerv1alpha1.FinalizerTenantCluster) {
		controllerutil.AddFinalizer(tc, butlerv1alpha1.FinalizerTenantCluster)
		if err := r.Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if tc.Status.Phase == "" {
		tc.Status.Phase = butlerv1alpha1.TenantClusterPhasePending
		if err := r.Status().Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	butlerConfig, err := r.getButlerConfig(ctx)
	if err != nil {
		logger.Error(err, "failed to get ButlerConfig")
		return r.setFailedStatus(ctx, tc, "ConfigError", "Failed to get ButlerConfig: "+err.Error())
	}

	if err := r.promoteOwnerAnnotation(ctx, tc); err != nil {
		logger.Error(err, "failed to promote owner annotation")
		return ctrl.Result{}, err
	}

	// Resolve team/env ClusterDefaults for observability. v1 does not
	// apply them to spec because the apiserver has already stamped
	// kubebuilder defaults by the time reconciliation runs; see
	// resolveTeamAndEnvDefaults for the full note. A future mutating
	// webhook will apply them before the schema stamp.
	if d := r.resolveTeamAndEnvDefaults(ctx, tc); d != nil {
		logger.V(1).Info("resolved team/env defaults (declarative-only in v1)",
			"team", tc.Spec.TeamRef.Name,
			"env", tc.Labels[butlerv1alpha1.LabelEnvironment],
			"kubernetesVersion", d.KubernetesVersion,
			"defaultAddons", d.DefaultAddons,
		)
	}

	if err := r.validateTenantCluster(ctx, tc, butlerConfig); err != nil {
		logger.Error(err, "validation failed")
		return r.setFailedStatus(ctx, tc, ReasonValidationFailed, err.Error())
	}

	if err := r.reconcileTenantNamespace(ctx, tc, butlerConfig); err != nil {
		logger.Error(err, "failed to reconcile tenant namespace")
		return r.setFailedStatus(ctx, tc, "NamespaceError", err.Error())
	}

	if tc.Status.Phase == butlerv1alpha1.TenantClusterPhasePending {
		tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseProvisioning
		r.Recorder.Eventf(tc, corev1.EventTypeNormal, "ClusterProvisioning",
			"Infrastructure provisioning started for %s", tc.Name)
		if err := r.Status().Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Resolve ProviderConfig for image sync (needed before infrastructure reconciliation)
	providerConfig, pcErr := r.getProviderConfig(ctx, tc)
	if pcErr != nil {
		logger.V(1).Info("ProviderConfig not yet available, skipping image sync", "error", pcErr)
	}

	// Reconcile image sync before infrastructure — ensures the OS image is available
	// on the provider before VMs are created. The resolved provider image ref is injected
	// into the in-memory TC spec so the CAPI builder picks it up.
	if providerConfig != nil {
		providerImageRef, imageSyncErr := r.reconcileImageSync(ctx, tc, providerConfig, butlerConfig)
		if imageSyncErr != nil {
			logger.Info("image sync not yet ready, waiting", "error", imageSyncErr)
			if err := r.Status().Update(ctx, tc); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}

		// Apply provider image ref to in-memory TC spec for the builder
		if providerImageRef != "" {
			switch providerConfig.Spec.Provider {
			case butlerv1alpha1.ProviderTypeHarvester:
				if tc.Spec.InfrastructureOverride == nil {
					tc.Spec.InfrastructureOverride = &butlerv1alpha1.InfrastructureOverride{}
				}
				if tc.Spec.InfrastructureOverride.Harvester == nil {
					tc.Spec.InfrastructureOverride.Harvester = &butlerv1alpha1.HarvesterOverride{}
				}
				tc.Spec.InfrastructureOverride.Harvester.ImageName = providerImageRef
			case butlerv1alpha1.ProviderTypeNutanix:
				if tc.Spec.InfrastructureOverride == nil {
					tc.Spec.InfrastructureOverride = &butlerv1alpha1.InfrastructureOverride{}
				}
				if tc.Spec.InfrastructureOverride.Nutanix == nil {
					tc.Spec.InfrastructureOverride.Nutanix = &butlerv1alpha1.NutanixOverride{}
				}
				tc.Spec.InfrastructureOverride.Nutanix.ImageUUID = providerImageRef
			case butlerv1alpha1.ProviderTypeProxmox:
				// Proxmox uses TemplateID (int) — image sync returns a string ref.
				// Provider controller is responsible for setting the correct template.
				logger.V(1).Info("Proxmox image ref from ImageSync not applied (TemplateID is int)",
					"providerImageRef", providerImageRef)
			}
		}
	}

	result, err := r.reconcileInfrastructure(ctx, tc, butlerConfig, providerConfig)
	if err != nil {
		logger.Error(err, "failed to reconcile infrastructure")
		return r.setFailedStatus(ctx, tc, ReasonCAPIResourceError, err.Error())
	}
	if result.Requeue || result.RequeueAfter > 0 {
		if err := r.Status().Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
		return result, nil
	}

	// Steady-state operations for Ready and Installing clusters.
	// Non-fatal: failures are aggregated into a degraded condition rather than
	// failing the reconcile. The cluster remains Ready but operators can see
	// which background operations need attention.
	var degraded []string

	if tc.Status.Phase == butlerv1alpha1.TenantClusterPhaseReady {
		if err := r.reconcileAutoEnrollObservability(ctx, tc, butlerConfig); err != nil {
			logger.Error(err, "failed to auto-enroll observability agents")
			degraded = append(degraded, "observability sync failed ("+truncateError(err)+")")
		}
	}

	if err := r.reconcileMachineDeploymentReplicas(ctx, tc, butlerConfig); err != nil {
		logger.Error(err, "failed to reconcile MachineDeployment replicas")
		degraded = append(degraded, "replica sync failed ("+truncateError(err)+")")
	}

	if err := r.reconcileNodeProviderIDs(ctx, tc, butlerConfig); err != nil {
		logger.Error(err, "failed to reconcile node providerIDs")
		degraded = append(degraded, "providerID sync failed ("+truncateError(err)+")")
	}

	if err := r.reconcileStewardControlPlane(ctx, tc, butlerConfig); err != nil {
		logger.Error(err, "failed to reconcile StewardControlPlane")
		degraded = append(degraded, "control plane sync failed ("+truncateError(err)+")")
	}

	if err := r.reconcileMachineTemplate(ctx, tc); err != nil {
		logger.Error(err, "failed to reconcile MachineTemplate")
		degraded = append(degraded, "machine template sync failed ("+truncateError(err)+")")
	}

	if err := r.reconcileElasticIPAM(ctx, tc, butlerConfig, providerConfig); err != nil {
		logger.Error(err, "elastic IPAM reconciliation failed")
		degraded = append(degraded, "elastic IPAM failed ("+truncateError(err)+")")
	}

	if isTalosCluster(tc) {
		if err := r.reconcileTalosBootstrap(ctx, tc, butlerConfig); err != nil {
			logger.Error(err, "failed to reconcile Talos bootstrap")
			degraded = append(degraded, "talos bootstrap failed ("+truncateError(err)+")")
		}
		if _, err := r.reconcileTalosApplyConfig(ctx, tc); err != nil {
			logger.Error(err, "failed to apply Talos config to workers")
			degraded = append(degraded, "talos config apply failed ("+truncateError(err)+")")
		}
		if err := r.reconcileTalosconfig(ctx, tc); err != nil {
			logger.Error(err, "failed to create talosconfig Secret")
			degraded = append(degraded, "talosconfig sync failed ("+truncateError(err)+")")
		}
	}

	if tc.Status.Phase == butlerv1alpha1.TenantClusterPhaseReady {
		if len(degraded) > 0 {
			msg := fmt.Sprintf("%d operations degraded: %s", len(degraded), strings.Join(degraded, ", "))
			if len(msg) > 1024 {
				msg = msg[:1021] + "..."
			}
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionReady,
				metav1.ConditionTrue, "ReconcileDegraded", msg)
			r.Recorder.Eventf(tc, corev1.EventTypeWarning, "ReconcileDegraded", msg)
		} else {
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionReady,
				metav1.ConditionTrue, "ReconcileSucceeded", "All operations healthy")
		}
	}

	tc.Status.ObservedGeneration = tc.Generation
	if err := r.Status().Update(ctx, tc); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: r.calculateRequeueInterval(tc)}, nil
}

func (r *Reconciler) setFailedStatus(ctx context.Context, tc *butlerv1alpha1.TenantCluster, reason, message string) (ctrl.Result, error) {
	tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseFailed
	tc.Status.ObservedGeneration = tc.Generation

	now := metav1.Now()
	tc.Status.LastTransitionTime = &now

	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionReady, metav1.ConditionFalse, reason, message)
	r.Recorder.Eventf(tc, corev1.EventTypeWarning, "ClusterFailed", "%s: %s", reason, message)

	if err := r.Status().Update(ctx, tc); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *Reconciler) setCondition(tc *butlerv1alpha1.TenantCluster, condType string, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&tc.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: tc.Generation,
	})
}

func truncateError(err error) string {
	s := err.Error()
	if len(s) > 100 {
		return s[:97] + "..."
	}
	return s
}

func (r *Reconciler) calculateRequeueInterval(tc *butlerv1alpha1.TenantCluster) time.Duration {
	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady {
		return 30 * time.Second
	}

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

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.TenantCluster{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&corev1.Namespace{}).
		Owns(&corev1.Secret{}).
		Owns(&rbacv1.RoleBinding{}).
		Named("tenantcluster").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrentReconciles,
		}).
		Complete(r)
}
