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
	"gopkg.in/yaml.v3"
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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/addons"
	"github.com/butlerdotdev/butler-controller/internal/capi"
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

	result, err := r.reconcileInfrastructure(ctx, tc)
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

	// Sync MachineDeployment replicas for Ready clusters
	// This handles scaling operations after initial provisioning
	if err := r.reconcileMachineDeploymentReplicas(ctx, tc); err != nil {
		logger.Error(err, "failed to reconcile MachineDeployment replicas")
		// Don't fail the reconcile, just log - scaling is not critical path
	}

	tc.Status.ObservedGeneration = tc.Generation
	if err := r.Status().Update(ctx, tc); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: r.calculateRequeueInterval(tc)}, nil
}

func (r *Reconciler) reconcileInfrastructure(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (ctrl.Result, error) {
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

	builder := capi.NewBuilder(tc, providerConfig, tc.Status.TenantNamespace)
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

	patched, err := r.handleKamajiHarvesterCompatibility(ctx, tc)
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

	if controlPlaneReady {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionControlPlaneReady,
			metav1.ConditionTrue, ReasonControlPlaneReady, "Control plane is ready")
	} else {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionControlPlaneReady,
			metav1.ConditionFalse, ReasonInfraProvisioning, "Control plane is provisioning")
	}

	if workersReady {

		// Skip installation if already ready
		if tc.Status.Phase == butlerv1alpha1.TenantClusterPhaseReady {
			return ctrl.Result{}, nil
		}

		// Check if control plane is accessible by trying to get kubeconfig
		_, err := r.getTenantKubeconfig(ctx, tc)
		if err == nil {
			logger.Info("control plane accessible and workers provisioned, proceeding to addon installation")
			return r.reconcileAddons(ctx, tc)
		}
		logger.V(1).Info("waiting for control plane kubeconfig", "error", err)
	}

	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionWorkersReady,
		metav1.ConditionFalse, ReasonWorkersProvisioning, "Workers are provisioning")

	if !infraReady || !controlPlaneReady || !workersReady {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// reconcileAddons installs required addons for a functional tenant cluster.
// These are infrastructure requirements, not optional features.
// Addons are installed monotonically - they are added but never removed via spec changes.
func (r *Reconciler) reconcileAddons(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseInstalling {
		tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseInstalling
		if err := r.Status().Update(ctx, tc); err != nil {
			return ctrl.Result{}, err
		}
	}

	kubeconfigData, err := r.getTenantKubeconfig(ctx, tc)
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
	logger.Info("installing Cilium CNI", "version", ciliumVersion)
	// Extract API server endpoint from kubeconfig
	apiServerHost, apiServerPort := extractAPIServerEndpoint(kubeconfigData)
	if err := r.Installer.InstallCilium(ctx, kubeconfigData, ciliumVersion, apiServerHost, apiServerPort); err != nil {
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
	if tc.Spec.Networking.LoadBalancerPool != nil {
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

func (r *Reconciler) getTenantKubeconfig(ctx context.Context, tc *butlerv1alpha1.TenantCluster) ([]byte, error) {
	secretName := fmt.Sprintf("%s-admin-kubeconfig", tc.Name)

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: tc.Status.TenantNamespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig secret: %w", err)
	}

	kubeconfigData, ok := secret.Data["admin.conf"]
	if !ok {
		return nil, fmt.Errorf("kubeconfig secret missing admin.conf key")
	}

	return kubeconfigData, nil
}

func (r *Reconciler) getProviderConfig(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (*butlerv1alpha1.ProviderConfig, error) {
	const defaultProviderNamespace = "butler-system"

	if tc.Spec.ProviderConfigRef != nil && tc.Spec.ProviderConfigRef.Name != "" {
		pc := &butlerv1alpha1.ProviderConfig{}
		if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.ProviderConfigRef.Name, Namespace: defaultProviderNamespace}, pc); err != nil {
			return nil, fmt.Errorf("failed to get ProviderConfig %s/%s: %w", defaultProviderNamespace, tc.Spec.ProviderConfigRef.Name, err)
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

	if replicas > 0 {
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

func (r *Reconciler) handleKamajiHarvesterCompatibility(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (bool, error) {
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
				logger.Info("patching Cluster controlPlaneEndpoint", "currentHost", currentHost, "newHost", loadBalancerIP)

				if err := unstructured.SetNestedField(cluster.Object, loadBalancerIP, "spec", "controlPlaneEndpoint", "host"); err != nil {
					logger.Error(err, "failed to set controlPlaneEndpoint.host")
				} else if err := unstructured.SetNestedField(cluster.Object, int64(6443), "spec", "controlPlaneEndpoint", "port"); err != nil {
					logger.Error(err, "failed to set controlPlaneEndpoint.port")
				} else if err := r.Update(ctx, cluster); err != nil {
					logger.Error(err, "failed to update Cluster controlPlaneEndpoint")
				} else {
					logger.Info("successfully patched Cluster controlPlaneEndpoint", "host", loadBalancerIP)
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

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.TenantCluster{}).
		Owns(&corev1.Namespace{}).
		Owns(&corev1.Secret{}).
		Owns(&rbacv1.RoleBinding{}).
		Named("tenantcluster").
		Complete(r)
}
