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
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"net/url"
	"strings"
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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/addons"
	"github.com/butlerdotdev/butler-controller/internal/capi"
	"github.com/butlerdotdev/butler-controller/internal/talos"
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
	Scheme    *runtime.Scheme
	Installer *addons.Installer
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
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=kamajicontrolplanes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=kamajicontrolplanes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvesterclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvestermachinetemplates,verbs=get;list;watch;create;update;patch;delete

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

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
		if err := r.Status().Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
	}

	result, err := r.reconcileInfrastructure(ctx, tc, butlerConfig)
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

	// Auto-enroll observability agents for Ready clusters
	if tc.Status.Phase == butlerv1alpha1.TenantClusterPhaseReady {
		if err := r.reconcileAutoEnrollObservability(ctx, tc, butlerConfig); err != nil {
			logger.Error(err, "failed to auto-enroll observability agents")
		}
	}

	// Sync MachineDeployment replicas for Ready clusters
	// This handles scaling operations after initial provisioning
	if err := r.reconcileMachineDeploymentReplicas(ctx, tc); err != nil {
		logger.Error(err, "failed to reconcile MachineDeployment replicas")
		// Don't fail the reconcile, just log - scaling is not critical path
	}

	// Elastic IPAM: grow/shrink LB IP allocations for Ready clusters
	if err := r.reconcileElasticIPAM(ctx, tc, butlerConfig); err != nil {
		logger.Error(err, "elastic IPAM reconciliation failed")
		// Non-fatal: don't fail the reconcile
	}

	// For Talos clusters, run apply-config and talosconfig as a safety net for Ready
	// clusters (e.g. new workers from scaling). The primary apply-config happens in
	// reconcileInfrastructure before addon installation.
	if isTalosCluster(tc) {
		if err := r.reconcileTalosBootstrap(ctx, tc, butlerConfig); err != nil {
			logger.Error(err, "failed to reconcile Talos bootstrap")
		}
		if _, err := r.reconcileTalosApplyConfig(ctx, tc); err != nil {
			logger.Error(err, "failed to apply Talos config to workers")
		}
		if err := r.reconcileTalosconfig(ctx, tc); err != nil {
			logger.Error(err, "failed to create talosconfig Secret")
		}
	}

	tc.Status.ObservedGeneration = tc.Generation
	if err := r.Status().Update(ctx, tc); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: r.calculateRequeueInterval(tc)}, nil
}

func (r *Reconciler) reconcileInfrastructure(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Already ready, nothing to do
	if tc.Status.Phase == butlerv1alpha1.TenantClusterPhaseReady {
		return ctrl.Result{}, nil
	}

	logger.Info("reconciling infrastructure", "tenantNamespace", tc.Status.TenantNamespace)

	providerConfig, err := r.getProviderConfig(ctx, tc)
	if err != nil {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionInfrastructureReady,
			metav1.ConditionFalse, ReasonProviderConfigNotFound, err.Error())
		return ctrl.Result{}, err
	}

	// Validate provider scope access
	if err := r.validateProviderAccess(ctx, tc, providerConfig); err != nil {
		return ctrl.Result{}, err
	}

	// Reconcile IPAM allocation (if provider has network.mode=ipam)
	ipamReady, err := r.reconcileIPAllocation(ctx, tc, providerConfig)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ipamReady {
		logger.Info("waiting for IPAM allocation")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Build CAPI resources with ButlerConfig for platform-level settings
	// ButlerConfig provides ControlPlaneExposure settings (LoadBalancer/Ingress/Gateway mode)
	// which enables auto-enabling tcp-proxy for non-LoadBalancer modes
	builder := capi.NewBuilder(tc, providerConfig, tc.Status.TenantNamespace).
		WithButlerConfig(butlerConfig)

	// For Ingress/Gateway modes, get the Ingress controller IP for worker /etc/hosts
	// CRITICAL: If Ingress/Gateway mode is enabled but the Ingress controller doesn't have
	// an external IP yet, we MUST wait. Otherwise, KubeadmConfigTemplate will be created
	// without the /etc/hosts entry, and workers won't be able to resolve the API server hostname.
	if butlerConfig != nil {
		mode := butlerConfig.GetControlPlaneExposureMode()
		hostname := butlerConfig.GetControlPlaneExposureHostname()
		logger.V(1).Info("ButlerConfig exposure settings",
			"mode", mode,
			"hostname", hostname)
		if mode == butlerv1alpha1.ControlPlaneExposureModeIngress ||
			mode == butlerv1alpha1.ControlPlaneExposureModeGateway {
			ingressIP, err := r.getExposureIP(ctx, butlerConfig)
			if err != nil {
				logger.Error(err, "failed to get exposure IP")
				r.setCondition(tc, butlerv1alpha1.TenantClusterConditionInfrastructureReady,
					metav1.ConditionFalse, ReasonInfraProvisioning, "Waiting for exposure endpoint")
				if err := r.Status().Update(ctx, tc); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
			if ingressIP == "" {
				logger.Info("Ingress/Gateway mode requires external IP, waiting")
				r.setCondition(tc, butlerv1alpha1.TenantClusterConditionInfrastructureReady,
					metav1.ConditionFalse, ReasonInfraProvisioning, "Waiting for exposure endpoint external IP")
				if err := r.Status().Update(ctx, tc); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
			logger.Info("Ingress/Gateway mode detected, setting ingressIP on builder", "ingressIP", ingressIP)
			builder.WithIngressIP(ingressIP)
		}
	}

	resourceSet, err := builder.Build()
	if err != nil {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionInfrastructureReady,
			metav1.ConditionFalse, ReasonCAPIResourceError, err.Error())
		return ctrl.Result{}, fmt.Errorf("failed to build CAPI resources: %w", err)
	}

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

	infraReady, controlPlaneReady, workersReady, err := r.checkInfrastructureStatus(ctx, tc)
	if err != nil {
		logger.Error(err, "failed to check infrastructure status")
	}

	patched, err := r.handleKamajiHarvesterCompatibility(ctx, tc, butlerConfig)
	if err != nil {
		logger.Error(err, "failed to handle Kamaji compatibility")
	}
	if patched {
		logger.Info("patched StewardControlPlane status for Harvester compatibility")
		return ctrl.Result{Requeue: true}, nil
	}

	if infraReady {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionInfrastructureReady,
			metav1.ConditionTrue, "InfrastructureReady", "Infrastructure cluster is ready")
	} else {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionInfrastructureReady,
			metav1.ConditionFalse, ReasonInfraProvisioning, "Infrastructure cluster is provisioning")
	}

	if workersReady {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionWorkersReady,
			metav1.ConditionTrue, ReasonWorkersReady, "Workers are ready")
	} else {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionWorkersReady,
			metav1.ConditionFalse, ReasonWorkersProvisioning, "Workers are provisioning")
	}

	if controlPlaneReady {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionControlPlaneReady,
			metav1.ConditionTrue, ReasonControlPlaneReady, "Control plane is ready")

		// For Talos clusters, create the bootstrap Secret and apply config to workers
		// as soon as CP is ready. Bootstrap MUST happen here because CAPI Machines
		// reference the Secret via dataSecretName and block until it exists.
		// Config must be applied BEFORE addon installation — workers need to leave
		// maintenance mode and join the cluster before Cilium and other addons can
		// schedule pods on them.
		if isTalosCluster(tc) {
			if err := r.reconcileTalosBootstrap(ctx, tc, butlerConfig); err != nil {
				logger.Error(err, "failed to reconcile Talos bootstrap")
			}
			allApplied, err := r.reconcileTalosApplyConfig(ctx, tc)
			if err != nil {
				logger.Error(err, "failed to apply Talos config to workers")
			}
			if !allApplied {
				logger.Info("waiting for Talos config to be applied to all workers before installing addons")
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}
		}

		// Install addons after workers have config applied (Talos) or as soon as
		// control plane is accessible (non-Talos). Skip if already ready.
		if tc.Status.Phase == butlerv1alpha1.TenantClusterPhaseReady {
			return ctrl.Result{}, nil
		}

		// Check if control plane is accessible by trying to get kubeconfig
		_, err := r.getTenantKubeconfig(ctx, tc, butlerConfig)
		if err == nil {
			logger.Info("control plane accessible, proceeding to addon installation")
			return r.reconcileAddons(ctx, tc, butlerConfig)
		}
		logger.V(1).Info("waiting for control plane kubeconfig", "error", err)
	} else {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionControlPlaneReady,
			metav1.ConditionFalse, ReasonInfraProvisioning, "Control plane is provisioning")
	}

	if !infraReady || !controlPlaneReady || !workersReady {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// reconcileAddons installs required addons for a functional tenant cluster.
// These are infrastructure requirements, not optional features.
// Addons are installed monotonically - they are added but never removed via spec changes.
func (r *Reconciler) reconcileAddons(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseInstalling {
		tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseInstalling
		if err := r.Status().Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
	}

	kubeconfigData, err := r.getTenantKubeconfig(ctx, tc, butlerConfig)
	if err != nil {
		logger.Error(err, "failed to get tenant kubeconfig")
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	var addonStatuses []butlerv1alpha1.AddonStatus

	// 1. CNI - REQUIRED, nodes won't be Ready without it
	ciliumVersion := addons.DefaultCiliumVersion
	if tc.Spec.Addons.CNI != nil && tc.Spec.Addons.CNI.Version != "" {
		ciliumVersion = tc.Spec.Addons.CNI.Version
	}

	// Extract API server endpoint from kubeconfig (used for default/LoadBalancer mode)
	apiServerHost, apiServerPort := extractAPIServerEndpoint(kubeconfigData)

	// Determine if we need hostAlias for Ingress/Gateway mode
	// When using Ingress mode with TLS passthrough, Cilium must connect to the
	// hostname (not IP) so SNI is sent for routing. But CoreDNS needs CNI to start,
	// creating a chicken-and-egg problem. We solve this with hostAlias.
	var ingressIP string
	if butlerConfig != nil {
		mode := butlerConfig.GetControlPlaneExposureMode()
		if mode == butlerv1alpha1.ControlPlaneExposureModeIngress ||
			mode == butlerv1alpha1.ControlPlaneExposureModeGateway {
			// Get the exposure IP (Gateway or Ingress controller) to add as hostAlias
			ip, err := r.getExposureIP(ctx, butlerConfig)
			if err != nil {
				logger.Error(err, "failed to get exposure IP")
			}
			ingressIP = ip

			// For Ingress/Gateway mode, we need the EXTERNAL hostname (from admin.conf)
			// not the internal .svc endpoint (from admin.svc). The external hostname
			// is needed for proper SNI routing in TLS passthrough.
			externalKubeconfig, err := r.getExternalKubeconfig(ctx, tc)
			if err == nil && externalKubeconfig != nil {
				apiServerHost, apiServerPort = extractAPIServerEndpoint(externalKubeconfig)
			}
			logger.Info("Ingress mode detected", "ingressIP", ingressIP, "externalHost", apiServerHost)
		}
	}

	ciliumCfg := addons.CiliumConfig{
		Version:       ciliumVersion,
		APIServerHost: apiServerHost,
		APIServerPort: apiServerPort,
		IngressIP:     ingressIP,
	}

	logger.Info("installing Cilium CNI", "version", ciliumVersion,
		"apiServerHost", apiServerHost, "ingressIP", ingressIP)

	if err := r.Installer.InstallCilium(ctx, kubeconfigData, ciliumCfg); err != nil {
		logger.Error(err, "failed to install Cilium")
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionAddonsReady,
			metav1.ConditionFalse, "CNIInstallFailed", err.Error())
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
		Name: "cilium", Version: ciliumVersion, Status: "Healthy", ManagedBy: "butler",
	})

	// 2. cert-manager - needed by many other components
	certManagerVersion := addons.DefaultCertManagerVersion
	if tc.Spec.Addons.CertManager != nil && tc.Spec.Addons.CertManager.Version != "" {
		certManagerVersion = tc.Spec.Addons.CertManager.Version
	}
	logger.Info("installing cert-manager", "version", certManagerVersion)
	if err := r.Installer.InstallCertManager(ctx, kubeconfigData, certManagerVersion); err != nil {
		logger.Error(err, "failed to install cert-manager")
	} else {
		addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
			Name: "cert-manager", Version: certManagerVersion, Status: "Healthy", ManagedBy: "butler",
		})
	}

	// 3. Longhorn - storage
	longhornVersion := addons.DefaultLonghornVersion
	if tc.Spec.Addons.Storage != nil && tc.Spec.Addons.Storage.Version != "" {
		longhornVersion = tc.Spec.Addons.Storage.Version
	}
	logger.Info("installing Longhorn", "version", longhornVersion)
	if err := r.Installer.InstallLonghorn(ctx, kubeconfigData, longhornVersion); err != nil {
		logger.Error(err, "failed to install Longhorn")
	} else {
		addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
			Name: "longhorn", Version: longhornVersion, Status: "Healthy", ManagedBy: "butler",
		})
	}

	// 4. MetalLB - LoadBalancer services
	metallbVersion := addons.DefaultMetalLBVersion
	if tc.Spec.Addons.LoadBalancer != nil && tc.Spec.Addons.LoadBalancer.Version != "" {
		metallbVersion = tc.Spec.Addons.LoadBalancer.Version
	}
	var poolStart, poolEnd string
	// Check IPAM allocation first, fall back to manual loadBalancerPool
	if tc.Status.LBAllocationRef != nil {
		lbAlloc := &butlerv1alpha1.IPAllocation{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      tc.Status.LBAllocationRef.Name,
			Namespace: "butler-system",
		}, lbAlloc); err == nil && lbAlloc.Status.Phase == butlerv1alpha1.IPAllocationPhaseAllocated {
			poolStart = lbAlloc.Status.StartAddress
			poolEnd = lbAlloc.Status.EndAddress
		}
	}
	if poolStart == "" && tc.Spec.Networking.LoadBalancerPool != nil {
		poolStart = tc.Spec.Networking.LoadBalancerPool.Start
		poolEnd = tc.Spec.Networking.LoadBalancerPool.End
	}
	logger.Info("installing MetalLB", "version", metallbVersion, "poolStart", poolStart, "poolEnd", poolEnd)
	if err := r.Installer.InstallMetalLB(ctx, kubeconfigData, metallbVersion, poolStart, poolEnd); err != nil {
		logger.Error(err, "failed to install MetalLB")
	} else {
		addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
			Name: "metallb", Version: metallbVersion, Status: "Healthy", ManagedBy: "butler",
		})
	}

	// 5. Traefik - Ingress
	traefikVersion := addons.DefaultTraefikVersion
	if tc.Spec.Addons.Ingress != nil && tc.Spec.Addons.Ingress.Version != "" {
		traefikVersion = tc.Spec.Addons.Ingress.Version
	}
	logger.Info("installing Traefik", "version", traefikVersion)
	if err := r.Installer.InstallTraefik(ctx, kubeconfigData, traefikVersion); err != nil {
		logger.Error(err, "failed to install Traefik")
	} else {
		addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
			Name: "traefik", Version: traefikVersion, Status: "Healthy", ManagedBy: "butler",
		})
	}

	// Update observed state
	if tc.Status.ObservedState == nil {
		tc.Status.ObservedState = &butlerv1alpha1.ObservedClusterState{}
	}
	tc.Status.ObservedState.Addons = addonStatuses

	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionAddonsReady,
		metav1.ConditionTrue, "AddonsInstalled", "All addons installed successfully")

	// Set kubeconfig secret reference for TenantAddon controller
	tc.Status.KubeconfigSecretRef = &butlerv1alpha1.LocalObjectReference{
		Name: fmt.Sprintf("%s-admin-kubeconfig", tc.Name),
	}

	tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseReady
	now := metav1.Now()
	tc.Status.LastTransitionTime = &now

	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionReady,
		metav1.ConditionTrue, ReasonReady, "Cluster is ready")

	logger.Info("TenantCluster is ready", "name", tc.Name)

	return ctrl.Result{}, nil
}

// reconcileAutoEnrollObservability creates TenantAddon resources for observability
// agents when autoEnroll is enabled in ButlerConfig and a pipeline is configured.
// Each agent (vector, prometheus, otel) is independently controlled.
func (r *Reconciler) reconcileAutoEnrollObservability(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	if butlerConfig == nil || butlerConfig.Spec.Observability == nil {
		return nil
	}

	obs := butlerConfig.Spec.Observability
	if obs.Collection == nil || obs.Collection.AutoEnroll == nil {
		return nil
	}

	if obs.Pipeline == nil || obs.Pipeline.ClusterRef == nil {
		return nil
	}

	// Don't install agents on the pipeline cluster itself
	pipeRef := obs.Pipeline.ClusterRef
	if pipeRef.Name == tc.Name && pipeRef.Namespace == tc.Namespace {
		return nil
	}

	enroll := obs.Collection.AutoEnroll

	if enroll.VectorAgent && obs.Pipeline.LogEndpoint != "" {
		if err := r.ensureAutoEnrolledAddon(ctx, tc, "vector-agent", buildVectorAgentValues(obs.Pipeline)); err != nil {
			logger.Error(err, "failed to auto-enroll vector-agent")
		}
	}

	if enroll.Prometheus && obs.Pipeline.MetricEndpoint != "" {
		if err := r.ensureAutoEnrolledAddon(ctx, tc, "prometheus-operator", buildPrometheusValues(obs.Pipeline)); err != nil {
			logger.Error(err, "failed to auto-enroll prometheus-operator")
		}
	}

	if enroll.OtelCollector && obs.Pipeline.TraceEndpoint != "" {
		if err := r.ensureAutoEnrolledAddon(ctx, tc, "otel-collector", buildOtelCollectorValues(obs.Pipeline)); err != nil {
			logger.Error(err, "failed to auto-enroll otel-collector")
		}
	}

	return nil
}

// ensureAutoEnrolledAddon creates a TenantAddon if it doesn't already exist.
func (r *Reconciler) ensureAutoEnrolledAddon(ctx context.Context, tc *butlerv1alpha1.TenantCluster, addonDefName string, values *butlerv1alpha1.ExtensionValues) error {
	logger := log.FromContext(ctx)
	addonName := fmt.Sprintf("%s-%s", tc.Name, addonDefName)

	existing := &butlerv1alpha1.TenantAddon{}
	err := r.Get(ctx, client.ObjectKey{Name: addonName, Namespace: tc.Namespace}, existing)
	if err == nil {
		return nil // already exists
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check TenantAddon %s: %w", addonName, err)
	}

	addon := &butlerv1alpha1.TenantAddon{
		ObjectMeta: metav1.ObjectMeta{
			Name:      addonName,
			Namespace: tc.Namespace,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy:              "butler",
				butlerv1alpha1.LabelTeam:                   tc.Labels[butlerv1alpha1.LabelTeam],
				"butler.butlerlabs.dev/cluster":            tc.Name,
				"butler.butlerlabs.dev/addon-definition":   addonDefName,
				"butler.butlerlabs.dev/auto-enrolled":      "true",
			},
		},
		Spec: butlerv1alpha1.TenantAddonSpec{
			ClusterRef: butlerv1alpha1.LocalObjectReference{Name: tc.Name},
			Addon:      addonDefName,
			Version:    "", // use AddonDefinition default
			Values:     values,
		},
	}

	if err := controllerutil.SetControllerReference(tc, addon, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference: %w", err)
	}

	logger.Info("auto-enrolling observability addon",
		"addon", addonName, "cluster", tc.Name)

	if err := r.Create(ctx, addon); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create TenantAddon %s: %w", addonName, err)
	}

	return nil
}

// buildVectorAgentValues configures the vector-agent sink to forward to the pipeline aggregator.
func buildVectorAgentValues(pipeline *butlerv1alpha1.ObservabilityPipelineConfig) *butlerv1alpha1.ExtensionValues {
	if pipeline == nil || pipeline.LogEndpoint == "" {
		return nil
	}

	host := pipeline.LogEndpoint
	if u, err := url.Parse(pipeline.LogEndpoint); err == nil && u.Hostname() != "" {
		host = u.Hostname()
	}

	values := map[string]interface{}{
		"customConfig": map[string]interface{}{
			"sinks": map[string]interface{}{
				"aggregator": map[string]interface{}{
					"address": fmt.Sprintf("%s:6000", host),
				},
			},
		},
	}

	raw, err := json.Marshal(values)
	if err != nil {
		return nil
	}
	return &butlerv1alpha1.ExtensionValues{Raw: raw}
}

// buildPrometheusValues configures remote-write to the pipeline metric endpoint.
func buildPrometheusValues(pipeline *butlerv1alpha1.ObservabilityPipelineConfig) *butlerv1alpha1.ExtensionValues {
	if pipeline == nil || pipeline.MetricEndpoint == "" {
		return nil
	}

	values := map[string]interface{}{
		"prometheus": map[string]interface{}{
			"prometheusSpec": map[string]interface{}{
				"remoteWrite": []map[string]interface{}{
					{"url": pipeline.MetricEndpoint},
				},
			},
		},
	}

	raw, err := json.Marshal(values)
	if err != nil {
		return nil
	}
	return &butlerv1alpha1.ExtensionValues{Raw: raw}
}

// buildOtelCollectorValues configures the OTLP exporter to the pipeline trace endpoint.
func buildOtelCollectorValues(pipeline *butlerv1alpha1.ObservabilityPipelineConfig) *butlerv1alpha1.ExtensionValues {
	if pipeline == nil || pipeline.TraceEndpoint == "" {
		return nil
	}

	values := map[string]interface{}{
		"config": map[string]interface{}{
			"exporters": map[string]interface{}{
				"otlp": map[string]interface{}{
					"endpoint": pipeline.TraceEndpoint,
					"tls": map[string]interface{}{
						"insecure": true,
					},
				},
			},
		},
	}

	raw, err := json.Marshal(values)
	if err != nil {
		return nil
	}
	return &butlerv1alpha1.ExtensionValues{Raw: raw}
}

func (r *Reconciler) getTenantKubeconfig(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) ([]byte, error) {
	secretName := fmt.Sprintf("%s-admin-kubeconfig", tc.Name)

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: tc.Status.TenantNamespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig secret: %w", err)
	}

	// Determine which kubeconfig key to use based on control plane exposure mode
	// For Ingress/Gateway modes, use admin.svc (internal service endpoint) since the external
	// hostname isn't resolvable from within the management cluster
	kubeconfigKey := "admin.conf"
	if butlerConfig != nil {
		mode := butlerConfig.GetControlPlaneExposureMode()
		if mode == butlerv1alpha1.ControlPlaneExposureModeIngress ||
			mode == butlerv1alpha1.ControlPlaneExposureModeGateway {
			kubeconfigKey = "admin.svc"
		}
	}

	kubeconfigData, ok := secret.Data[kubeconfigKey]
	if !ok {
		// Fallback to admin.conf if preferred key doesn't exist
		kubeconfigData, ok = secret.Data["admin.conf"]
		if !ok {
			return nil, fmt.Errorf("kubeconfig secret missing %s and admin.conf keys", kubeconfigKey)
		}
	}

	return kubeconfigData, nil
}

// getExternalKubeconfig returns the kubeconfig with the external hostname (admin.conf).
// This is used for Cilium configuration in Ingress/Gateway mode to ensure proper SNI is sent.
func (r *Reconciler) getExternalKubeconfig(ctx context.Context, tc *butlerv1alpha1.TenantCluster) ([]byte, error) {
	secretName := fmt.Sprintf("%s-admin-kubeconfig", tc.Name)

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: tc.Status.TenantNamespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig secret: %w", err)
	}

	// admin.conf contains the external hostname for Ingress/Gateway mode
	kubeconfigData, ok := secret.Data["admin.conf"]
	if !ok {
		return nil, fmt.Errorf("kubeconfig secret missing admin.conf key")
	}

	return kubeconfigData, nil
}

func (r *Reconciler) getProviderConfig(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (*butlerv1alpha1.ProviderConfig, error) {
	const defaultProviderNamespace = "butler-system"

	if tc.Spec.ProviderConfigRef != nil && tc.Spec.ProviderConfigRef.Name != "" {
		ns := tc.Spec.ProviderConfigRef.Namespace
		if ns == "" {
			ns = defaultProviderNamespace
		}
		pc := &butlerv1alpha1.ProviderConfig{}
		if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.ProviderConfigRef.Name, Namespace: ns}, pc); err != nil {
			return nil, fmt.Errorf("failed to get ProviderConfig %s/%s: %w", ns, tc.Spec.ProviderConfigRef.Name, err)
		}
		return pc, nil
	}

	butlerConfig := &butlerv1alpha1.ButlerConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: ButlerConfigSingletonName}, butlerConfig); err == nil {
		if butlerConfig.Spec.DefaultProviderConfigRef != nil && butlerConfig.Spec.DefaultProviderConfigRef.Name != "" {
			pc := &butlerv1alpha1.ProviderConfig{}
			if err := r.Get(ctx, types.NamespacedName{Name: butlerConfig.Spec.DefaultProviderConfigRef.Name, Namespace: defaultProviderNamespace}, pc); err != nil {
				return nil, fmt.Errorf("failed to get default ProviderConfig %s/%s from ButlerConfig: %w",
					defaultProviderNamespace, butlerConfig.Spec.DefaultProviderConfigRef.Name, err)
			}
			return pc, nil
		}
	}

	pc := &butlerv1alpha1.ProviderConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: "default", Namespace: defaultProviderNamespace}, pc); err == nil {
		return pc, nil
	}

	pcList := &butlerv1alpha1.ProviderConfigList{}
	if err := r.List(ctx, pcList, client.InNamespace(defaultProviderNamespace)); err != nil {
		return nil, fmt.Errorf("failed to list ProviderConfigs: %w", err)
	}

	if len(pcList.Items) > 0 {
		return &pcList.Items[0], nil
	}

	return nil, fmt.Errorf("no ProviderConfig available in %s namespace", defaultProviderNamespace)
}

// checkInfrastructureStatus checks the status of the CAPI Cluster and MachineDeployment.
// It updates the TenantCluster status with current worker node counts.
func (r *Reconciler) checkInfrastructureStatus(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (infraReady, cpReady, workersReady bool, err error) {
	logger := log.FromContext(ctx)

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

	phase, found, _ := unstructured.NestedString(cluster.Object, "status", "phase")
	if found {
		logger.V(1).Info("cluster phase", "phase", phase)
		infraReady = phase == "Provisioned"
	}

	cpReadyVal, found, _ := unstructured.NestedBool(cluster.Object, "status", "controlPlaneReady")
	if found {
		cpReady = cpReadyVal
	}

	infraReadyVal, found, _ := unstructured.NestedBool(cluster.Object, "status", "infrastructureReady")
	if found && !infraReady {
		infraReady = infraReadyVal
	}

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

	replicas, _, _ := unstructured.NestedInt64(md.Object, "status", "replicas")
	readyReplicas, _, _ := unstructured.NestedInt64(md.Object, "status", "readyReplicas")

	logger.V(1).Info("MachineDeployment status", "replicas", replicas, "readyReplicas", readyReplicas)

	// Update TenantCluster status with worker counts
	desiredReplicas := int64(tc.Spec.Workers.Replicas)
	if desiredReplicas < 1 {
		desiredReplicas = 1
	}
	tc.Status.WorkerNodesReady = int32(readyReplicas)
	tc.Status.WorkerNodesDesired = int32(desiredReplicas)

	// Workers are ready when at least one replica is ready
	if readyReplicas > 0 {
		workersReady = true
	}

	return infraReady, cpReady, workersReady, nil
}

// reconcileMachineDeploymentReplicas syncs worker count from TenantCluster spec to MachineDeployment.
// This is called on every reconcile to ensure spec.workers.replicas changes are propagated
// to the underlying CAPI MachineDeployment, enabling cluster scaling operations.
// It also updates the TenantCluster status with current worker counts.
func (r *Reconciler) reconcileMachineDeploymentReplicas(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	logger := log.FromContext(ctx)

	// Only sync when cluster is Ready or Installing
	// During Provisioning, initial creation handles replicas
	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady &&
		tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseInstalling {
		return nil
	}

	// Need tenant namespace to find MachineDeployment
	if tc.Status.TenantNamespace == "" {
		return nil
	}

	// Get the MachineDeployment
	mdName := fmt.Sprintf("%s-workers", tc.Name)
	md := &unstructured.Unstructured{}
	md.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.ClusterAPIGroup,
		Version: capi.ClusterAPIVersion,
		Kind:    "MachineDeployment",
	})

	if err := r.Get(ctx, types.NamespacedName{
		Namespace: tc.Status.TenantNamespace,
		Name:      mdName,
	}, md); err != nil {
		if apierrors.IsNotFound(err) {
			// Not created yet, will be handled by initial provisioning
			return nil
		}
		return fmt.Errorf("failed to get MachineDeployment: %w", err)
	}

	// Get current spec replicas and desired replicas from TenantCluster
	currentSpecReplicas, _, _ := unstructured.NestedInt64(md.Object, "spec", "replicas")
	desiredReplicas := int64(tc.Spec.Workers.Replicas)

	// Default to 1 if not specified (matches builder.go behavior)
	if desiredReplicas < 1 {
		desiredReplicas = 1
	}

	// Update TenantCluster status with worker counts from MachineDeployment
	readyReplicas, _, _ := unstructured.NestedInt64(md.Object, "status", "readyReplicas")
	tc.Status.WorkerNodesReady = int32(readyReplicas)
	tc.Status.WorkerNodesDesired = int32(desiredReplicas)

	// Check if MachineDeployment spec needs to be scaled
	if currentSpecReplicas == desiredReplicas {
		// Already in sync, nothing more to do
		return nil
	}

	logger.Info("scaling MachineDeployment",
		"name", mdName,
		"from", currentSpecReplicas,
		"to", desiredReplicas)

	// Patch MachineDeployment replicas
	patch := []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, desiredReplicas))

	if err := r.Patch(ctx, md, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("failed to patch MachineDeployment replicas: %w", err)
	}

	logger.Info("MachineDeployment scaled successfully",
		"name", mdName,
		"replicas", desiredReplicas)

	return nil
}

func (r *Reconciler) handleKamajiHarvesterCompatibility(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) (bool, error) {
	logger := log.FromContext(ctx)

	kcp := &unstructured.Unstructured{}
	kcp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.ControlPlaneAPIGroup,
		Version: capi.ControlPlaneAPIVersion,
		Kind:    "StewardControlPlane",
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

	conditions, found, _ := unstructured.NestedSlice(kcp.Object, "status", "conditions")
	if !found {
		return false, nil
	}

	tcp := &unstructured.Unstructured{}
	tcp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "steward.butlerlabs.dev",
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

	// Determine the control plane endpoint based on exposure mode
	var endpointHost string
	var endpointPort int64

	exposureMode := butlerv1alpha1.ControlPlaneExposureModeLoadBalancer
	if butlerConfig != nil {
		exposureMode = butlerConfig.GetControlPlaneExposureMode()
	}

	switch exposureMode {
	case butlerv1alpha1.ControlPlaneExposureModeIngress, butlerv1alpha1.ControlPlaneExposureModeGateway:
		// For Ingress/Gateway modes, use the generated hostname
		// Ingress uses port 443 (HTTPS), Gateway uses port 6443 (kube-apiserver listener)
		if butlerConfig != nil {
			hostnamePattern := butlerConfig.GetControlPlaneExposureHostname()
			if hostnamePattern != "" {
				// Generate tenant-specific hostname: "clustername.namespace.k8s.example.com" from "*.k8s.example.com"
				base := strings.TrimPrefix(hostnamePattern, "*.")
				endpointHost = fmt.Sprintf("%s.%s.%s", tc.Name, tc.Status.TenantNamespace, base)
				if exposureMode == butlerv1alpha1.ControlPlaneExposureModeGateway {
					endpointPort = 6443
				} else {
					endpointPort = 443
				}
				logger.V(1).Info("using Ingress/Gateway hostname for controlPlaneEndpoint",
					"mode", exposureMode, "hostname", endpointHost)
			}
		}
	default:
		// LoadBalancer mode: get IP from service status, port 6443
		serviceStatus, found, _ := unstructured.NestedMap(tcp.Object, "status", "kubernetesResources", "service")
		if found {
			ingress, found, _ := unstructured.NestedSlice(serviceStatus, "loadBalancer", "ingress")
			if found && len(ingress) > 0 {
				if ingressEntry, ok := ingress[0].(map[string]interface{}); ok {
					endpointHost, _, _ = unstructured.NestedString(ingressEntry, "ip")
					endpointPort = 6443
					logger.V(1).Info("using LoadBalancer IP for controlPlaneEndpoint",
						"ip", endpointHost)
				}
			}
		}
	}

	// Patch Cluster controlPlaneEndpoint if we have a valid endpoint
	if endpointHost != "" {
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
			currentPort, _, _ := unstructured.NestedInt64(cluster.Object, "spec", "controlPlaneEndpoint", "port")
			if currentHost != endpointHost || currentPort != endpointPort {
				logger.Info("patching Cluster controlPlaneEndpoint", "currentHost", currentHost, "newHost", endpointHost, "currentPort", currentPort, "newPort", endpointPort)

				if err := unstructured.SetNestedField(cluster.Object, endpointHost, "spec", "controlPlaneEndpoint", "host"); err != nil {
					logger.Error(err, "failed to set controlPlaneEndpoint.host")
				} else if err := unstructured.SetNestedField(cluster.Object, endpointPort, "spec", "controlPlaneEndpoint", "port"); err != nil {
					logger.Error(err, "failed to set controlPlaneEndpoint.port")
				} else if err := r.Update(ctx, cluster); err != nil {
					logger.Error(err, "failed to update Cluster controlPlaneEndpoint")
				} else {
					logger.Info("successfully patched Cluster controlPlaneEndpoint", "host", endpointHost, "port", endpointPort)
					return true, nil
				}
			}
		}
	}

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

	logger.Info("detected Kamaji unsupported infrastructure provider error, patching StewardControlPlane status")

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

	logger.Info("TenantControlPlane is ready, patching StewardControlPlane status", "readyReplicas", readyReplicas)

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
		return false, fmt.Errorf("failed to update StewardControlPlane status: %w", err)
	}

	return true, nil
}

func (r *Reconciler) getButlerConfig(ctx context.Context) (*butlerv1alpha1.ButlerConfig, error) {
	config := &butlerv1alpha1.ButlerConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: ButlerConfigSingletonName}, config); err != nil {
		if apierrors.IsNotFound(err) {
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
		return r.validateDisabledMode(ctx, tc, config)
	}
}

func (r *Reconciler) validateEnforcedMode(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	logger := log.FromContext(ctx)

	if tc.Spec.TeamRef == nil || tc.Spec.TeamRef.Name == "" {
		return fmt.Errorf("teamRef is required when multi-tenancy mode is Enforced")
	}

	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("team %q not found", tc.Spec.TeamRef.Name)
		}
		return fmt.Errorf("failed to get team: %w", err)
	}

	if team.Status.Phase != butlerv1alpha1.TeamPhaseReady {
		return fmt.Errorf("team %q is not ready (phase: %s)", team.Name, team.Status.Phase)
	}

	expectedNamespace := team.Status.Namespace
	if expectedNamespace == "" {
		expectedNamespace = team.Name
	}

	if tc.Namespace != expectedNamespace {
		return fmt.Errorf("TenantCluster must be in team namespace %q, got %q", expectedNamespace, tc.Namespace)
	}

	logger.V(1).Info("enforced mode validation passed", "team", team.Name)
	return nil
}

func (r *Reconciler) validateOptionalMode(ctx context.Context, tc *butlerv1alpha1.TenantCluster, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	if tc.Spec.TeamRef != nil && tc.Spec.TeamRef.Name != "" {
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

func (r *Reconciler) reconcileTenantNamespace(ctx context.Context, tc *butlerv1alpha1.TenantCluster, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	if tc.Status.TenantNamespace == "" {
		tc.Status.TenantNamespace = generateTenantNamespace(tc)
	}

	nsName := tc.Status.TenantNamespace
	logger.Info("reconciling tenant namespace", "namespace", nsName)

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: nsName},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, ns, func() error {
		if ns.Labels == nil {
			ns.Labels = make(map[string]string)
		}
		ns.Labels[butlerv1alpha1.LabelManagedBy] = "butler"
		ns.Labels[butlerv1alpha1.LabelTenant] = tc.Name
		ns.Labels[butlerv1alpha1.LabelSourceNamespace] = tc.Namespace
		ns.Labels[butlerv1alpha1.LabelSourceName] = tc.Name

		if tc.Spec.TeamRef != nil && tc.Spec.TeamRef.Name != "" {
			ns.Labels[butlerv1alpha1.LabelTeam] = tc.Spec.TeamRef.Name
		}

		ns.Labels["pod-security.kubernetes.io/enforce"] = "privileged"
		ns.Labels["pod-security.kubernetes.io/audit"] = "privileged"
		ns.Labels["pod-security.kubernetes.io/warn"] = "privileged"

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

	if tc.Spec.TeamRef != nil && tc.Spec.TeamRef.Name != "" {
		if err := r.reconcileTeamRoleBinding(ctx, tc, nsName); err != nil {
			return fmt.Errorf("failed to create team RoleBinding: %w", err)
		}
	}

	return nil
}

func (r *Reconciler) reconcileTeamRoleBinding(ctx context.Context, tc *butlerv1alpha1.TenantCluster, nsName string) error {
	logger := log.FromContext(ctx)

	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
		return err
	}

	rbName := fmt.Sprintf("butler-team-%s-access", team.Name)

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: rbName, Namespace: nsName},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, rb, func() error {
		if rb.Labels == nil {
			rb.Labels = make(map[string]string)
		}
		rb.Labels[butlerv1alpha1.LabelManagedBy] = "butler"
		rb.Labels[butlerv1alpha1.LabelTeam] = team.Name
		rb.Labels[butlerv1alpha1.LabelTenant] = tc.Name

		rb.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "admin",
		}

		var subjects []rbacv1.Subject
		for _, user := range team.Spec.Access.Users {
			subjects = append(subjects, rbacv1.Subject{
				Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: user.Name,
			})
		}
		for _, group := range team.Spec.Access.Groups {
			subjects = append(subjects, rbacv1.Subject{
				Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: group.Name,
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

func (r *Reconciler) handleDeletion(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("handling TenantCluster deletion", "name", tc.Name, "namespace", tc.Namespace)

	tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseDeleting
	if err := r.Status().Update(ctx, tc); err != nil {
		if !apierrors.IsConflict(err) {
			return ctrl.Result{}, err
		}
	}

	// Clean up IPAllocations before deleting namespace
	r.cleanupIPAllocations(ctx, tc)

	if tc.Status.TenantNamespace != "" {
		ns := &corev1.Namespace{}
		err := r.Get(ctx, types.NamespacedName{Name: tc.Status.TenantNamespace}, ns)
		if err == nil {
			logger.Info("deleting tenant namespace", "namespace", tc.Status.TenantNamespace)
			if err := r.Delete(ctx, ns); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		logger.Info("tenant namespace deleted", "namespace", tc.Status.TenantNamespace)
	}

	controllerutil.RemoveFinalizer(tc, butlerv1alpha1.FinalizerTenantCluster)
	if err := r.Update(ctx, tc); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("TenantCluster deletion complete", "name", tc.Name)
	return ctrl.Result{}, nil
}

func (r *Reconciler) setFailedStatus(ctx context.Context, tc *butlerv1alpha1.TenantCluster, reason, message string) (ctrl.Result, error) {
	tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseFailed
	tc.Status.ObservedGeneration = tc.Generation

	now := metav1.Now()
	tc.Status.LastTransitionTime = &now

	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionReady, metav1.ConditionFalse, reason, message)

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

func generateTenantNamespace(tc *butlerv1alpha1.TenantCluster) string {
	uid := string(tc.UID)
	if len(uid) > 8 {
		uid = uid[:8]
	}
	return fmt.Sprintf("%s-%s", tc.Name, uid)
}

func extractAPIServerEndpoint(kubeconfig []byte) (string, string) {
	// Parse kubeconfig to get server URL
	var kc struct {
		Clusters []struct {
			Cluster struct {
				Server string `yaml:"server"`
			} `yaml:"cluster"`
		} `yaml:"clusters"`
	}
	if err := yaml.Unmarshal(kubeconfig, &kc); err != nil || len(kc.Clusters) == 0 {
		return "kubernetes.default.svc.cluster.local", "443" // fallback
	}

	// Parse URL like https://10.40.0.111:6443
	server := kc.Clusters[0].Cluster.Server
	server = strings.TrimPrefix(server, "https://")
	server = strings.TrimPrefix(server, "http://")

	parts := strings.Split(server, ":")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return parts[0], "6443"
}

// getIngressControllerIP returns the external IP of the Ingress controller service.
// For Ingress/Gateway modes, worker VMs need this IP to resolve the API server hostname
// before DNS is available (via /etc/hosts entry).
func (r *Reconciler) getIngressControllerIP(ctx context.Context, butlerConfig *butlerv1alpha1.ButlerConfig) (string, error) {
	logger := log.FromContext(ctx)

	// Determine the controller type to find the right service
	controllerType := "traefik" // default
	if butlerConfig != nil && butlerConfig.Spec.ControlPlaneExposure != nil {
		if ct := butlerConfig.Spec.ControlPlaneExposure.ControllerType; ct != "" {
			controllerType = ct
		}
	}

	// Map controller type to namespace and service name patterns
	// Each ingress controller has different naming conventions
	var namespace, serviceName string
	switch controllerType {
	case "traefik":
		namespace = "traefik"
		serviceName = "traefik"
	case "nginx":
		namespace = "ingress-nginx"
		serviceName = "ingress-nginx-controller"
	case "haproxy":
		namespace = "haproxy-controller"
		serviceName = "haproxy-kubernetes-ingress"
	default:
		// For generic or unknown types, try traefik as fallback
		namespace = "traefik"
		serviceName = "traefik"
	}

	// Look up the service
	svc := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: namespace}, svc); err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("ingress controller service not found",
				"namespace", namespace, "service", serviceName, "controllerType", controllerType)
			return "", nil
		}
		return "", fmt.Errorf("failed to get ingress controller service: %w", err)
	}

	// Extract external IP from LoadBalancer status
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		logger.V(1).Info("ingress controller service is not LoadBalancer type",
			"type", svc.Spec.Type, "namespace", namespace, "service", serviceName)
		return "", nil
	}

	for _, ingress := range svc.Status.LoadBalancer.Ingress {
		if ingress.IP != "" {
			logger.V(1).Info("found ingress controller IP",
				"ip", ingress.IP, "namespace", namespace, "service", serviceName)
			return ingress.IP, nil
		}
		if ingress.Hostname != "" {
			// Some cloud providers use hostname instead of IP
			logger.V(1).Info("found ingress controller hostname (no IP)",
				"hostname", ingress.Hostname, "namespace", namespace, "service", serviceName)
			return ingress.Hostname, nil
		}
	}

	logger.V(1).Info("ingress controller service has no external IP yet",
		"namespace", namespace, "service", serviceName)
	return "", nil
}

// getGatewayIP retrieves the external IP from a Gateway resource status.
// For Gateway API exposure mode, the external IP is on the Gateway resource
// rather than an Ingress controller Service.
func (r *Reconciler) getGatewayIP(ctx context.Context, butlerConfig *butlerv1alpha1.ButlerConfig) (string, error) {
	logger := log.FromContext(ctx)

	gatewayRef := butlerConfig.GetControlPlaneExposureGatewayRef()
	if gatewayRef == "" {
		return "", fmt.Errorf("Gateway mode requires gatewayRef in ButlerConfig")
	}

	parts := strings.Split(gatewayRef, "/")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid gatewayRef format: %s, expected namespace/name", gatewayRef)
	}
	namespace, name := parts[0], parts[1]

	gw := &unstructured.Unstructured{}
	gw.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "gateway.networking.k8s.io",
		Version: "v1",
		Kind:    "Gateway",
	})

	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, gw); err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("Gateway resource not found", "namespace", namespace, "name", name)
			return "", nil
		}
		return "", fmt.Errorf("failed to get Gateway %s/%s: %w", namespace, name, err)
	}

	addresses, found, err := unstructured.NestedSlice(gw.Object, "status", "addresses")
	if err != nil || !found || len(addresses) == 0 {
		logger.V(1).Info("Gateway has no addresses in status yet", "namespace", namespace, "name", name)
		return "", nil
	}

	firstAddr, ok := addresses[0].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid address format in Gateway %s/%s status", namespace, name)
	}

	ip, found, err := unstructured.NestedString(firstAddr, "value")
	if err != nil || !found || ip == "" {
		logger.V(1).Info("Gateway has no IP value in address", "namespace", namespace, "name", name)
		return "", nil
	}

	logger.V(1).Info("found Gateway IP", "ip", ip, "namespace", namespace, "name", name)
	return ip, nil
}

// getExposureIP returns the external IP for control plane exposure based on mode.
// For Gateway mode, reads the Gateway resource status. For Ingress mode, reads the
// Ingress controller Service status.
func (r *Reconciler) getExposureIP(ctx context.Context, butlerConfig *butlerv1alpha1.ButlerConfig) (string, error) {
	mode := butlerConfig.GetControlPlaneExposureMode()
	if mode == butlerv1alpha1.ControlPlaneExposureModeGateway {
		return r.getGatewayIP(ctx, butlerConfig)
	}
	return r.getIngressControllerIP(ctx, butlerConfig)
}

// reconcileTalosApplyConfig applies Talos machine configs to CAPI Machines in maintenance mode.
// It discovers Machine IPs, checks whether config was already applied (via annotation),
// and runs talosctl apply-config --insecure for each unconfigured Machine.
// reconcileTalosApplyConfig applies Talos machine config to CAPI Machines via talosctl.
// Returns (allApplied, error) where allApplied is true when all Machines with IPs
// have their config applied (or are already configured).
func (r *Reconciler) reconcileTalosApplyConfig(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (bool, error) {
	logger := log.FromContext(ctx)

	tenantNS := tc.Status.TenantNamespace
	if tenantNS == "" {
		return false, nil
	}

	// Read bootstrap Secret to get the machine config
	secretName := fmt.Sprintf("%s-talos-bootstrap", tc.Name)
	bootstrapSecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: tenantNS}, bootstrapSecret); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil // Bootstrap Secret not ready yet
		}
		return false, fmt.Errorf("failed to get bootstrap Secret: %w", err)
	}

	machineConfig := bootstrapSecret.Data["value"]
	if len(machineConfig) == 0 {
		return false, fmt.Errorf("bootstrap Secret %s has empty machine config", secretName)
	}

	// Get CAPI Machine addresses
	machines, err := talos.GetMachineAddresses(ctx, r.Client, tc.Name, tenantNS)
	if err != nil {
		return false, fmt.Errorf("getting Machine addresses: %w", err)
	}
	if len(machines) == 0 {
		logger.Info("waiting for CAPI Machines to get IP addresses")
		return false, nil
	}

	// Apply config to each Machine that hasn't been configured yet
	talosClient := talos.NewClient()
	var failCount, pendingCount int
	for _, machine := range machines {
		if machine.Annotations != nil && machine.Annotations["butler.butlerlabs.dev/talos-config-applied"] == "true" {
			continue
		}

		pendingCount++
		logger.Info("applying Talos config to Machine", "machine", machine.Name, "ip", machine.IP)
		if err := talosClient.ApplyConfig(ctx, machine.IP, machineConfig); err != nil {
			// "certificate required" means the node left maintenance mode and already has config.
			// Treat as success and annotate.
			if strings.Contains(err.Error(), "certificate required") {
				logger.Info("node already configured (requires TLS client cert), marking as applied",
					"machine", machine.Name, "ip", machine.IP)
			} else {
				logger.Error(err, "failed to apply Talos config", "machine", machine.Name, "ip", machine.IP)
				failCount++
				continue
			}
		}

		// Annotate the Machine to mark config as applied
		if err := r.annotateMachine(ctx, machine.Name, tenantNS); err != nil {
			logger.Error(err, "failed to annotate Machine after config apply", "machine", machine.Name)
		}
		pendingCount--
	}

	if failCount > 0 && failCount == len(machines) {
		return false, fmt.Errorf("failed to apply Talos config to all %d Machines", failCount)
	}

	allApplied := pendingCount == 0 && failCount == 0
	return allApplied, nil
}

// annotateMachine adds the talos-config-applied annotation to a CAPI Machine.
func (r *Reconciler) annotateMachine(ctx context.Context, machineName, namespace string) error {
	machine := &unstructured.Unstructured{}
	machine.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.ClusterAPIGroup,
		Version: capi.ClusterAPIVersion,
		Kind:    "Machine",
	})
	if err := r.Get(ctx, types.NamespacedName{Name: machineName, Namespace: namespace}, machine); err != nil {
		return err
	}

	annotations := machine.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations["butler.butlerlabs.dev/talos-config-applied"] = "true"
	machine.SetAnnotations(annotations)

	return r.Update(ctx, machine)
}

// reconcileTalosconfig creates a talosconfig Secret for CLI access to worker nodes.
// This requires admin client credentials from the trustd-creds Secret (generated by steward).
func (r *Reconciler) reconcileTalosconfig(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	logger := log.FromContext(ctx)

	tenantNS := tc.Status.TenantNamespace
	talosconfigName := fmt.Sprintf("%s-talosconfig", tc.Name)

	// Check if talosconfig Secret already exists
	existing := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: talosconfigName, Namespace: tenantNS}, existing); err == nil {
		return nil // Already exists
	}

	// Read trustd credentials
	trustdCredsName := fmt.Sprintf("%s-trustd-creds", tc.Name)
	trustdCreds := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: trustdCredsName, Namespace: tenantNS}, trustdCreds); err != nil {
		return fmt.Errorf("failed to get trustd credentials: %w", err)
	}

	adminCert := trustdCreds.Data["admin.crt"]
	adminKey := trustdCreds.Data["admin.key"]
	caCert := trustdCreds.Data["os-ca.crt"]

	if len(adminCert) == 0 || len(adminKey) == 0 {
		logger.V(1).Info("admin credentials not available in trustd-creds, skipping talosconfig")
		return nil // Steward hasn't been updated yet
	}

	// Determine the endpoint for the talosconfig
	var endpoint string
	machines, err := talos.GetMachineAddresses(ctx, r.Client, tc.Name, tenantNS)
	if err == nil && len(machines) > 0 {
		endpoint = machines[0].IP
	}
	if endpoint == "" {
		return nil // No machines with IPs yet
	}

	// Generate talosconfig YAML
	talosconfigData, err := talos.GenerateTalosconfig(talos.TalosconfigInput{
		ContextName: tc.Name,
		Endpoints:   []string{endpoint},
		CACert:      caCert,
		AdminCert:   adminCert,
		AdminKey:    adminKey,
	})
	if err != nil {
		return fmt.Errorf("generating talosconfig: %w", err)
	}

	// Create the Secret
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      talosconfigName,
			Namespace: tenantNS,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy: "butler",
				butlerv1alpha1.LabelTenant:    tc.Name,
			},
		},
		Data: map[string][]byte{
			"talosconfig": talosconfigData,
		},
	}

	// Skip owner reference — TC is in a different namespace than the Secret.
	// Labels track ownership instead.

	if err := r.Create(ctx, secret); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("creating talosconfig Secret: %w", err)
	}

	logger.Info("created talosconfig Secret", "name", talosconfigName, "namespace", tenantNS)

	return nil
}

// isTalosCluster returns true if the TenantCluster uses Talos OS for workers.
func isTalosCluster(tc *butlerv1alpha1.TenantCluster) bool {
	return talos.IsTalosCluster(tc)
}

// effectiveServiceType determines the actual Kubernetes Service type for the TCP.
// Gateway/Ingress modes default to ClusterIP, LoadBalancer mode defaults to LoadBalancer.
// The TC's spec.controlPlane.serviceType can override any default.
func (r *Reconciler) effectiveServiceType(tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) string {
	if tc.Spec.ControlPlane.ServiceType != "" {
		return tc.Spec.ControlPlane.ServiceType
	}
	if butlerConfig != nil {
		mode := butlerConfig.GetControlPlaneExposureMode()
		if mode == butlerv1alpha1.ControlPlaneExposureModeIngress ||
			mode == butlerv1alpha1.ControlPlaneExposureModeGateway {
			return "ClusterIP"
		}
	}
	return "LoadBalancer"
}

// reconcileTalosBootstrap generates the Talos machine config and creates the bootstrap
// Secret that CAPI's dataSecretName mechanism reads. This is called after the TCP is
// ready and the control plane is accessible.
//
// The Secret contains a Talos v1alpha1 worker machine config with:
//   - machine.token: trustd token for apid auth with steward-trustd
//   - cluster.token: kubeadm bootstrap token for kubelet TLS bootstrapping
//   - machine.ca.crt: OS CA cert for trusting steward-trustd
//   - cluster.ca.crt: K8s cluster CA for trusting the API server
func (r *Reconciler) reconcileTalosBootstrap(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	secretName := fmt.Sprintf("%s-talos-bootstrap", tc.Name)
	tenantNS := tc.Status.TenantNamespace

	// Check if bootstrap Secret already exists
	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: tenantNS}, existing)
	if err == nil {
		logger.V(1).Info("Talos bootstrap Secret already exists", "name", secretName)
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check for bootstrap Secret: %w", err)
	}

	// Read trustd credentials from the management cluster
	trustdCredsName := fmt.Sprintf("%s-trustd-creds", tc.Name)
	trustdCreds := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: trustdCredsName, Namespace: tenantNS}, trustdCreds); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("trustd credentials not ready yet, waiting", "name", trustdCredsName)
			return nil
		}
		return fmt.Errorf("failed to get trustd credentials: %w", err)
	}

	machineToken := string(trustdCreds.Data["token"])
	osCACert := string(trustdCreds.Data["os-ca.crt"])
	if machineToken == "" || osCACert == "" {
		return fmt.Errorf("trustd credentials missing token or os-ca.crt")
	}

	// Read cluster CA from the management cluster
	caSecretName := fmt.Sprintf("%s-ca", tc.Name)
	caSecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: caSecretName, Namespace: tenantNS}, caSecret); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("cluster CA not ready yet, waiting", "name", caSecretName)
			return nil
		}
		return fmt.Errorf("failed to get cluster CA: %w", err)
	}

	clusterCACert := string(caSecret.Data["ca.crt"])
	if clusterCACert == "" {
		return fmt.Errorf("cluster CA Secret missing ca.crt")
	}

	// Determine control plane endpoint based on service type.
	// When using LoadBalancer, use the Service's external IP directly so that
	// both the API server (port 6443) and trustd (port 50001) are reachable
	// on the same address. Gateway mode TLS passthrough on multiple ports
	// for the same hostname has known issues (cilium/cilium#42898).
	var controlPlaneEndpoint string

	effectiveServiceType := r.effectiveServiceType(tc, butlerConfig)
	if effectiveServiceType == "LoadBalancer" {
		svc := &corev1.Service{}
		if err := r.Get(ctx, types.NamespacedName{Name: tc.Name, Namespace: tenantNS}, svc); err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("TCP Service not found yet, waiting")
				return nil
			}
			return fmt.Errorf("failed to get TCP Service: %w", err)
		}
		var lbIP string
		for _, ingress := range svc.Status.LoadBalancer.Ingress {
			if ingress.IP != "" {
				lbIP = ingress.IP
				break
			}
			if ingress.Hostname != "" {
				lbIP = ingress.Hostname
				break
			}
		}
		if lbIP == "" {
			logger.Info("TCP Service LoadBalancer IP not assigned yet, waiting")
			return nil
		}
		controlPlaneEndpoint = fmt.Sprintf("https://%s:6443", lbIP)
		logger.Info("using LoadBalancer IP for Talos control plane endpoint", "ip", lbIP)
	} else {
		// Gateway/Ingress mode — use the TCP status endpoint (hostname-based)
		tcp := &unstructured.Unstructured{}
		tcp.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "steward.butlerlabs.dev",
			Version: "v1alpha1",
			Kind:    "TenantControlPlane",
		})
		if err := r.Get(ctx, types.NamespacedName{Name: tc.Name, Namespace: tenantNS}, tcp); err != nil {
			return fmt.Errorf("failed to get TenantControlPlane: %w", err)
		}
		tcpEndpoint, found, _ := unstructured.NestedString(tcp.Object, "status", "controlPlaneEndpoint")
		if !found || tcpEndpoint == "" {
			logger.Info("TCP controlPlaneEndpoint not set yet, waiting")
			return nil
		}
		controlPlaneEndpoint = talos.EndpointFromTCPStatus(tcpEndpoint)
		logger.Info("using Gateway endpoint for Talos control plane endpoint", "endpoint", controlPlaneEndpoint)
	}

	// Get tenant admin kubeconfig to create bootstrap token in tenant API server
	kubeconfigData, err := r.getTenantKubeconfig(ctx, tc, butlerConfig)
	if err != nil {
		logger.Info("tenant kubeconfig not available yet, waiting", "error", err)
		return nil
	}

	// Create kubernetes client from kubeconfig
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfigData)
	if err != nil {
		return fmt.Errorf("failed to create REST config from kubeconfig: %w", err)
	}

	tenantClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to create tenant kubernetes client: %w", err)
	}

	// Reuse existing bootstrap token if one was created by a previous attempt,
	// otherwise generate a new one. This ensures idempotency when the reconciler
	// succeeds in creating the token but fails before creating the bootstrap Secret.
	bootstrapToken, err := talos.FindExistingBootstrapToken(ctx, tenantClient)
	if err != nil {
		return fmt.Errorf("failed to check for existing bootstrap token: %w", err)
	}

	if bootstrapToken != "" {
		logger.Info("reusing existing kubeadm bootstrap token in tenant API server")
	} else {
		bootstrapToken, err = talos.GenerateBootstrapToken()
		if err != nil {
			return fmt.Errorf("failed to generate bootstrap token: %w", err)
		}

		if err := talos.CreateBootstrapTokenSecret(ctx, tenantClient, bootstrapToken); err != nil {
			return fmt.Errorf("failed to create bootstrap token in tenant: %w", err)
		}

		logger.Info("created kubeadm bootstrap token in tenant API server")
	}

	// Get networking CIDRs
	podCIDR := "10.244.0.0/16"
	serviceCIDR := "10.96.0.0/12"
	if tc.Spec.Networking.PodCIDR != "" {
		podCIDR = tc.Spec.Networking.PodCIDR
	}
	if tc.Spec.Networking.ServiceCIDR != "" {
		serviceCIDR = tc.Spec.Networking.ServiceCIDR
	}

	// Get Talos-specific config
	installDisk := "/dev/vda"
	var installerImage string
	if tc.Spec.Workers.MachineTemplate.OS.Talos != nil {
		if tc.Spec.Workers.MachineTemplate.OS.Talos.InstallDisk != "" {
			installDisk = tc.Spec.Workers.MachineTemplate.OS.Talos.InstallDisk
		}
		installerImage = tc.Spec.Workers.MachineTemplate.OS.Talos.InstallerImage
	}

	// Generate Talos machine config
	input := talos.MachineConfigInput{
		ClusterName:          tc.Name,
		ControlPlaneEndpoint: controlPlaneEndpoint,
		ClusterCACert:        clusterCACert,
		MachineToken:         machineToken,
		BootstrapToken:       bootstrapToken,
		OSCACert:             osCACert,
		PodCIDR:              podCIDR,
		ServiceCIDR:          serviceCIDR,
		InstallDisk:          installDisk,
		InstallerImage:       installerImage,
	}

	machineConfigData, err := talos.GenerateWorkerConfig(input)
	if err != nil {
		return fmt.Errorf("failed to generate Talos machine config: %w", err)
	}

	// Create the bootstrap Secret in the tenant namespace on the management cluster.
	// CAPI reads this via dataSecretName on the MachineDeployment.
	bootstrapSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: tenantNS,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy: "butler",
				butlerv1alpha1.LabelTenant:    tc.Name,
			},
		},
		Data: map[string][]byte{
			"value": machineConfigData,
		},
	}

	// Skip owner reference — TC is in butler-tenants namespace but bootstrap Secret
	// is in the tenant namespace. Cross-namespace owner refs are disallowed.
	// Labels track ownership instead.

	if err := r.Create(ctx, bootstrapSecret); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create Talos bootstrap Secret: %w", err)
	}

	logger.Info("created Talos bootstrap Secret",
		"name", secretName,
		"namespace", tenantNS,
		"endpoint", controlPlaneEndpoint)

	return nil
}

// validateProviderAccess checks if the TenantCluster's team has access to the ProviderConfig.
func (r *Reconciler) validateProviderAccess(ctx context.Context, tc *butlerv1alpha1.TenantCluster, pc *butlerv1alpha1.ProviderConfig) error {
	// Platform-scoped providers are accessible to all teams
	if pc.Spec.Scope == nil || pc.Spec.Scope.Type == "" || pc.Spec.Scope.Type == butlerv1alpha1.ProviderConfigScopePlatform {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
			metav1.ConditionTrue, butlerv1alpha1.ReasonReady, "platform-scoped provider")
		return r.validateProviderLimits(ctx, tc, pc)
	}

	// Team-scoped: verify the TC's team matches the PC's team
	if pc.Spec.Scope.TeamRef == nil {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
			metav1.ConditionFalse, butlerv1alpha1.ReasonProviderAccessDenied,
			"provider is team-scoped but has no teamRef")
		return fmt.Errorf("provider %s is team-scoped but has no teamRef", pc.Name)
	}

	if tc.Spec.TeamRef == nil {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
			metav1.ConditionFalse, butlerv1alpha1.ReasonProviderAccessDenied,
			"cluster has no teamRef but provider is team-scoped")
		return fmt.Errorf("cluster has no teamRef but provider %s is team-scoped to %s",
			pc.Name, pc.Spec.Scope.TeamRef.Name)
	}

	if tc.Spec.TeamRef.Name != pc.Spec.Scope.TeamRef.Name {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
			metav1.ConditionFalse, butlerv1alpha1.ReasonProviderAccessDenied,
			fmt.Sprintf("team %s does not have access to provider %s (scoped to %s)",
				tc.Spec.TeamRef.Name, pc.Name, pc.Spec.Scope.TeamRef.Name))
		return fmt.Errorf("team %s does not have access to provider %s (scoped to %s)",
			tc.Spec.TeamRef.Name, pc.Name, pc.Spec.Scope.TeamRef.Name)
	}

	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
		metav1.ConditionTrue, butlerv1alpha1.ReasonReady,
		fmt.Sprintf("team %s has access to provider %s", tc.Spec.TeamRef.Name, pc.Name))

	// Enforce provider limits (defense-in-depth when webhooks are disabled)
	return r.validateProviderLimits(ctx, tc, pc)
}

// validateProviderLimits checks maxClustersPerTeam and maxNodesPerTeam limits from ProviderConfig.
func (r *Reconciler) validateProviderLimits(ctx context.Context, tc *butlerv1alpha1.TenantCluster, pc *butlerv1alpha1.ProviderConfig) error {
	if pc.Spec.Limits == nil {
		return nil
	}

	if pc.Spec.Limits.MaxClustersPerTeam != nil {
		maxClusters := *pc.Spec.Limits.MaxClustersPerTeam
		var tcList butlerv1alpha1.TenantClusterList
		if err := r.List(ctx, &tcList, client.InNamespace(tc.Namespace)); err != nil {
			return fmt.Errorf("failed to list TenantClusters: %w", err)
		}

		var existingCount int32
		for i := range tcList.Items {
			if tcList.Items[i].Name != tc.Name {
				existingCount++
			}
		}

		if existingCount >= maxClusters {
			msg := fmt.Sprintf("team namespace %q has %d cluster(s), provider %q limits to %d",
				tc.Namespace, existingCount, pc.Name, maxClusters)
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
				metav1.ConditionFalse, butlerv1alpha1.ReasonQuotaExceeded, msg)
			return fmt.Errorf("quota exceeded: %s", msg)
		}
	}

	if pc.Spec.Limits.MaxNodesPerTeam != nil {
		maxNodes := *pc.Spec.Limits.MaxNodesPerTeam
		var tcList butlerv1alpha1.TenantClusterList
		if err := r.List(ctx, &tcList, client.InNamespace(tc.Namespace)); err != nil {
			return fmt.Errorf("failed to list TenantClusters: %w", err)
		}

		totalNodes := tc.Spec.Workers.Replicas
		for i := range tcList.Items {
			if tcList.Items[i].Name != tc.Name {
				totalNodes += tcList.Items[i].Spec.Workers.Replicas
			}
		}

		if totalNodes > maxNodes {
			msg := fmt.Sprintf("total worker replicas (%d) exceeds provider %q per-team limit (%d)",
				totalNodes, pc.Name, maxNodes)
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
				metav1.ConditionFalse, butlerv1alpha1.ReasonQuotaExceeded, msg)
			return fmt.Errorf("quota exceeded: %s", msg)
		}
	}

	return nil
}

// reconcileIPAllocation creates or checks LB IPAllocation for IPAM mode providers.
// Returns true if IPAM is ready (allocated or not needed), false if waiting.
func (r *Reconciler) reconcileIPAllocation(ctx context.Context, tc *butlerv1alpha1.TenantCluster, pc *butlerv1alpha1.ProviderConfig) (bool, error) {
	logger := log.FromContext(ctx)

	// Skip IPAM if not configured
	if pc.Spec.Network == nil || pc.Spec.Network.Mode != "ipam" {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionNetworkReady,
			metav1.ConditionTrue, butlerv1alpha1.ReasonReady, "cloud networking mode")
		return true, nil
	}

	if len(pc.Spec.Network.PoolRefs) == 0 {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionNetworkReady,
			metav1.ConditionFalse, butlerv1alpha1.ReasonNetworkNotReady,
			"IPAM mode but no poolRefs configured on provider")
		return false, fmt.Errorf("IPAM mode but no poolRefs configured on provider %s", pc.Name)
	}

	// Check if LB allocation already exists
	if tc.Status.LBAllocationRef != nil {
		alloc := &butlerv1alpha1.IPAllocation{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      tc.Status.LBAllocationRef.Name,
			Namespace: "butler-system",
		}, alloc); err != nil {
			if apierrors.IsNotFound(err) {
				// Allocation was deleted, clear ref and retry
				tc.Status.LBAllocationRef = nil
			} else {
				return false, err
			}
		} else {
			switch alloc.Status.Phase {
			case butlerv1alpha1.IPAllocationPhaseAllocated:
				r.setCondition(tc, butlerv1alpha1.TenantClusterConditionNetworkReady,
					metav1.ConditionTrue, butlerv1alpha1.ReasonReady,
					fmt.Sprintf("LB IPs allocated: %s", alloc.Status.CIDR))
				return true, nil
			case butlerv1alpha1.IPAllocationPhasePending:
				r.setCondition(tc, butlerv1alpha1.TenantClusterConditionNetworkReady,
					metav1.ConditionFalse, butlerv1alpha1.ReasonReconciling,
					"waiting for LB IP allocation")
				return false, nil
			case butlerv1alpha1.IPAllocationPhaseFailed:
				// Delete failed allocation and try next pool
				logger.Info("LB allocation failed, cleaning up", "allocation", alloc.Name)
				if err := r.Delete(ctx, alloc); err != nil && !apierrors.IsNotFound(err) {
					return false, err
				}
				tc.Status.LBAllocationRef = nil
			}
		}
	}

	// Create new allocation — try pools in priority order
	lbCount := r.getInitialLBPoolSize(tc, pc)

	// Enforce quota on initial allocation
	if pc.Spec.Network.QuotaPerTenant != nil && pc.Spec.Network.QuotaPerTenant.MaxLoadBalancerIPs != nil {
		maxLB := *pc.Spec.Network.QuotaPerTenant.MaxLoadBalancerIPs
		if lbCount > maxLB {
			lbCount = maxLB
		}
	}

	allocName := fmt.Sprintf("%s-%s-lb", tc.Namespace, tc.Name)

	for _, poolRef := range pc.Spec.Network.PoolRefs {
		// Check pool capacity
		pool := &butlerv1alpha1.NetworkPool{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      poolRef.Name,
			Namespace: "butler-system",
		}, pool); err != nil {
			logger.V(1).Info("pool not found, trying next", "pool", poolRef.Name)
			continue
		}

		if pool.Status.AvailableIPs < lbCount {
			logger.V(1).Info("pool has insufficient capacity, trying next",
				"pool", poolRef.Name, "available", pool.Status.AvailableIPs, "needed", lbCount)
			continue
		}

		// Create IPAllocation
		alloc := &butlerv1alpha1.IPAllocation{
			ObjectMeta: metav1.ObjectMeta{
				Name:      allocName,
				Namespace: "butler-system",
				Labels: map[string]string{
					butlerv1alpha1.LabelNetworkPool:    poolRef.Name,
					butlerv1alpha1.LabelTeam:           tc.Namespace,
					butlerv1alpha1.LabelTenant:         tc.Name,
					butlerv1alpha1.LabelAllocationType: "loadbalancer",
				},
			},
			Spec: butlerv1alpha1.IPAllocationSpec{
				PoolRef:          butlerv1alpha1.LocalObjectReference{Name: poolRef.Name},
				TenantClusterRef: butlerv1alpha1.NamespacedObjectReference{Name: tc.Name, Namespace: tc.Namespace},
				Type:             butlerv1alpha1.IPAllocationTypeLoadBalancer,
				Count:            &lbCount,
			},
		}

		if err := r.Create(ctx, alloc); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Re-fetch and check status
				existing := &butlerv1alpha1.IPAllocation{}
				if getErr := r.Get(ctx, types.NamespacedName{Name: allocName, Namespace: "butler-system"}, existing); getErr == nil {
					tc.Status.LBAllocationRef = &butlerv1alpha1.LocalObjectReference{Name: allocName}
					return false, nil
				}
			}
			return false, fmt.Errorf("failed to create IPAllocation: %w", err)
		}

		logger.Info("created LB IPAllocation", "allocation", allocName, "pool", poolRef.Name, "count", lbCount)
		tc.Status.LBAllocationRef = &butlerv1alpha1.LocalObjectReference{Name: allocName}
		return false, nil // Wait for allocation to be fulfilled
	}

	// All pools exhausted
	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionNetworkReady,
		metav1.ConditionFalse, butlerv1alpha1.ReasonPoolExhausted,
		"all configured pools are exhausted")
	return false, nil
}

func (r *Reconciler) getInitialLBPoolSize(tc *butlerv1alpha1.TenantCluster, pc *butlerv1alpha1.ProviderConfig) int32 {
	// TC override takes priority
	if tc.Spec.Networking.LBPoolSize != nil {
		return *tc.Spec.Networking.LBPoolSize
	}
	if pc.Spec.Network != nil && pc.Spec.Network.LoadBalancer != nil {
		lb := pc.Spec.Network.LoadBalancer
		if lb.AllocationMode == "elastic" {
			if lb.InitialPoolSize != nil {
				return *lb.InitialPoolSize
			}
			return 2
		}
		if lb.DefaultPoolSize != nil {
			return *lb.DefaultPoolSize
		}
	}
	return 8
}

func (r *Reconciler) isElasticIPAM(pc *butlerv1alpha1.ProviderConfig) bool {
	return pc.Spec.Network != nil &&
		pc.Spec.Network.Mode == "ipam" &&
		pc.Spec.Network.LoadBalancer != nil &&
		pc.Spec.Network.LoadBalancer.AllocationMode == "elastic"
}

func (r *Reconciler) getGrowthIncrement(pc *butlerv1alpha1.ProviderConfig) int32 {
	if pc.Spec.Network != nil && pc.Spec.Network.LoadBalancer != nil && pc.Spec.Network.LoadBalancer.GrowthIncrement != nil {
		return *pc.Spec.Network.LoadBalancer.GrowthIncrement
	}
	return 2
}

// listLBAllocations returns all LB IPAllocations for a given tenant cluster.
func (r *Reconciler) listLBAllocations(ctx context.Context, tc *butlerv1alpha1.TenantCluster) ([]butlerv1alpha1.IPAllocation, error) {
	allocList := &butlerv1alpha1.IPAllocationList{}
	if err := r.List(ctx, allocList, client.InNamespace("butler-system"),
		client.MatchingLabels{
			butlerv1alpha1.LabelTeam:           tc.Namespace,
			butlerv1alpha1.LabelTenant:         tc.Name,
			butlerv1alpha1.LabelAllocationType: "loadbalancer",
		}); err != nil {
		return nil, err
	}
	return allocList.Items, nil
}

// countUsedLBIPs counts the number of LoadBalancer services with assigned IPs on a tenant cluster.
func (r *Reconciler) countUsedLBIPs(ctx context.Context, kubeconfig []byte) (int32, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return 0, fmt.Errorf("failed to build REST config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return 0, fmt.Errorf("failed to create clientset: %w", err)
	}

	svcList, err := clientset.CoreV1().Services("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to list services: %w", err)
	}

	var count int32
	for _, svc := range svcList.Items {
		if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
			continue
		}
		for _, ing := range svc.Status.LoadBalancer.Ingress {
			if ing.IP != "" {
				count++
				break
			}
		}
	}
	return count, nil
}

// reconcileElasticIPAM handles dynamic LB IP allocation growth and shrink for Ready clusters.
func (r *Reconciler) reconcileElasticIPAM(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) error {
	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady {
		return nil
	}

	logger := log.FromContext(ctx)

	pc, err := r.getProviderConfig(ctx, tc)
	if err != nil {
		return nil // Can't get provider, skip silently
	}

	if !r.isElasticIPAM(pc) {
		return nil
	}

	// Get all LB allocations for this tenant
	allocs, err := r.listLBAllocations(ctx, tc)
	if err != nil {
		return fmt.Errorf("failed to list LB allocations: %w", err)
	}

	if len(allocs) == 0 {
		return nil // No allocations yet, initial allocation handles this
	}

	// Count total allocated and available IPs
	var totalAllocated int32
	for _, alloc := range allocs {
		if alloc.Status.Phase == butlerv1alpha1.IPAllocationPhaseAllocated {
			if alloc.Spec.Count != nil {
				totalAllocated += *alloc.Spec.Count
			}
		}
	}

	// Count used IPs from tenant cluster
	kubeconfig, err := r.getTenantKubeconfig(ctx, tc, butlerConfig)
	if err != nil {
		logger.V(1).Info("cannot get tenant kubeconfig for elastic IPAM, skipping", "error", err)
		return nil
	}

	usedIPs, err := r.countUsedLBIPs(ctx, kubeconfig)
	if err != nil {
		logger.V(1).Info("cannot count used LB IPs, skipping", "error", err)
		return nil
	}

	availableIPs := totalAllocated - usedIPs
	growthIncrement := r.getGrowthIncrement(pc)

	logger.V(1).Info("elastic IPAM status",
		"totalAllocated", totalAllocated, "usedIPs", usedIPs,
		"availableIPs", availableIPs, "growthIncrement", growthIncrement)

	// Grow: if fewer than 1 IP is available
	if availableIPs < 1 {
		// Check quota
		if pc.Spec.Network.QuotaPerTenant != nil && pc.Spec.Network.QuotaPerTenant.MaxLoadBalancerIPs != nil {
			maxLB := *pc.Spec.Network.QuotaPerTenant.MaxLoadBalancerIPs
			if totalAllocated+growthIncrement > maxLB {
				logger.Info("elastic IPAM growth blocked by quota",
					"totalAllocated", totalAllocated, "maxLB", maxLB)
				return nil
			}
		}

		// Find next allocation index
		nextIdx := len(allocs)
		allocName := fmt.Sprintf("%s-%s-lb-%d", tc.Namespace, tc.Name, nextIdx)

		// Find a pool with capacity
		for _, poolRef := range pc.Spec.Network.PoolRefs {
			pool := &butlerv1alpha1.NetworkPool{}
			if err := r.Get(ctx, types.NamespacedName{
				Name: poolRef.Name, Namespace: "butler-system",
			}, pool); err != nil {
				continue
			}
			if pool.Status.AvailableIPs < growthIncrement {
				continue
			}

			alloc := &butlerv1alpha1.IPAllocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      allocName,
					Namespace: "butler-system",
					Labels: map[string]string{
						butlerv1alpha1.LabelNetworkPool:    poolRef.Name,
						butlerv1alpha1.LabelTeam:           tc.Namespace,
						butlerv1alpha1.LabelTenant:         tc.Name,
						butlerv1alpha1.LabelAllocationType: "loadbalancer",
					},
				},
				Spec: butlerv1alpha1.IPAllocationSpec{
					PoolRef:          butlerv1alpha1.LocalObjectReference{Name: poolRef.Name},
					TenantClusterRef: butlerv1alpha1.NamespacedObjectReference{Name: tc.Name, Namespace: tc.Namespace},
					Type:             butlerv1alpha1.IPAllocationTypeLoadBalancer,
					Count:            &growthIncrement,
				},
			}

			if err := r.Create(ctx, alloc); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return nil // Already created
				}
				return fmt.Errorf("failed to create growth allocation: %w", err)
			}
			logger.Info("elastic IPAM: created growth allocation",
				"allocation", allocName, "pool", poolRef.Name, "count", growthIncrement)
			break
		}
	}

	// Shrink: if we have more than 1 allocation and enough spare IPs
	if len(allocs) > 1 && availableIPs > growthIncrement {
		// Find the newest allocation that is fully unused and older than 10 minutes
		var shrinkCandidate *butlerv1alpha1.IPAllocation
		for i := len(allocs) - 1; i >= 1; i-- { // Skip index 0 (initial allocation)
			alloc := &allocs[i]
			if alloc.Status.Phase != butlerv1alpha1.IPAllocationPhaseAllocated {
				continue
			}
			if alloc.Spec.Count == nil {
				continue
			}
			// Only shrink allocations older than 10 minutes to avoid thrashing
			if time.Since(alloc.CreationTimestamp.Time) < 10*time.Minute {
				continue
			}
			shrinkCandidate = alloc
			break
		}

		if shrinkCandidate != nil {
			logger.Info("elastic IPAM: shrinking unused allocation",
				"allocation", shrinkCandidate.Name, "availableIPs", availableIPs)
			if err := r.Delete(ctx, shrinkCandidate); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete shrink allocation: %w", err)
			}
		}
	}

	// Update MetalLB pool with all current allocated ranges
	if err := r.updateMetalLBPool(ctx, tc, butlerConfig, allocs); err != nil {
		logger.Error(err, "failed to update MetalLB pool")
	}

	return nil
}

// updateMetalLBPool configures MetalLB on the tenant cluster with all allocated IP ranges.
func (r *Reconciler) updateMetalLBPool(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig, allocs []butlerv1alpha1.IPAllocation) error {
	var ranges []string
	for _, alloc := range allocs {
		if alloc.Status.Phase != butlerv1alpha1.IPAllocationPhaseAllocated {
			continue
		}
		if alloc.Status.StartAddress != "" && alloc.Status.EndAddress != "" {
			ranges = append(ranges, fmt.Sprintf("%s-%s", alloc.Status.StartAddress, alloc.Status.EndAddress))
		} else if alloc.Status.CIDR != "" {
			ranges = append(ranges, alloc.Status.CIDR)
		}
	}

	if len(ranges) == 0 {
		return nil
	}

	kubeconfig, err := r.getTenantKubeconfig(ctx, tc, butlerConfig)
	if err != nil {
		return err
	}

	return r.Installer.UpdateMetalLBPool(ctx, kubeconfig, ranges)
}

// cleanupIPAllocations deletes IPAllocations referenced by the TenantCluster
// and any additional elastic allocations discovered by labels.
func (r *Reconciler) cleanupIPAllocations(ctx context.Context, tc *butlerv1alpha1.TenantCluster) {
	logger := log.FromContext(ctx)

	// Delete referenced allocations
	refs := []*butlerv1alpha1.LocalObjectReference{tc.Status.IPAllocationRef, tc.Status.LBAllocationRef}
	deleted := make(map[string]bool)
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		alloc := &butlerv1alpha1.IPAllocation{}
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: "butler-system"}, alloc); err != nil {
			if !apierrors.IsNotFound(err) {
				logger.Error(err, "failed to get IPAllocation for cleanup", "name", ref.Name)
			}
			continue
		}
		if err := r.Delete(ctx, alloc); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete IPAllocation", "name", ref.Name)
		} else {
			logger.Info("deleted IPAllocation during cleanup", "name", ref.Name)
		}
		deleted[ref.Name] = true
	}

	// Also clean up any elastic growth allocations discovered by labels
	allocList := &butlerv1alpha1.IPAllocationList{}
	if err := r.List(ctx, allocList, client.InNamespace("butler-system"),
		client.MatchingLabels{
			butlerv1alpha1.LabelTeam:   tc.Namespace,
			butlerv1alpha1.LabelTenant: tc.Name,
		}); err != nil {
		logger.Error(err, "failed to list IPAllocations for cleanup")
		return
	}
	for i := range allocList.Items {
		alloc := &allocList.Items[i]
		if deleted[alloc.Name] {
			continue
		}
		if err := r.Delete(ctx, alloc); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete elastic IPAllocation", "name", alloc.Name)
		} else {
			logger.Info("deleted elastic IPAllocation during cleanup", "name", alloc.Name)
		}
	}
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.TenantCluster{}).
		Owns(&corev1.Namespace{}).
		Owns(&corev1.Secret{}).
		Owns(&rbacv1.RoleBinding{}).
		Named("tenantcluster").
		WithOptions(controller.Options{
			// Allow multiple TenantClusters to be reconciled in parallel.
			// This is critical for multi-tenant scenarios where creating
			// multiple clusters simultaneously should not block each other.
			MaxConcurrentReconciles: 5,
		}).
		Complete(r)
}
