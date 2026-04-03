// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/capi"

	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

func (r *Reconciler) reconcileInfrastructure(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig, providerConfig *butlerv1alpha1.ProviderConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Already ready, nothing to do
	if tc.Status.Phase == butlerv1alpha1.TenantClusterPhaseReady {
		return ctrl.Result{}, nil
	}

	logger.Info("reconciling infrastructure", "tenantNamespace", tc.Status.TenantNamespace)

	if providerConfig == nil {
		var err error
		providerConfig, err = r.getProviderConfig(ctx, tc)
		if err != nil {
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionInfrastructureReady,
				metav1.ConditionFalse, ReasonProviderConfigNotFound, err.Error())
			return ctrl.Result{}, err
		}
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
				if apierrors.IsAlreadyExists(err) {
					logger.V(1).Info("CAPI resource created by concurrent reconcile",
						"kind", resource.GetKind(),
						"name", resource.GetName())
				} else {
					return ctrl.Result{}, fmt.Errorf("failed to create %s %s: %w",
						resource.GetKind(), resource.GetName(), err)
				}
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

				patch := []byte(fmt.Sprintf(
					`{"spec":{"controlPlaneEndpoint":{"host":%q,"port":%d}}}`,
					endpointHost, endpointPort,
				))
				if err := r.Patch(ctx, cluster, client.RawPatch(types.MergePatchType, patch)); err != nil {
					logger.Error(err, "failed to patch Cluster controlPlaneEndpoint")
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

// reconcileImageSync ensures the OS image is synced to the infrastructure provider
// via the Butler Image Factory. It creates an ImageSync resource if needed, deduplicates
// across TenantClusters sharing the same schematic+version+provider, and returns the
// provider-specific image reference once the sync is complete.
//
// Returns:
//   - providerImageRef: the provider-specific image reference (e.g., "default/talos-v1.12.4-amd64")
//     Empty string if no sync is needed (no schematic configured or auto-sync disabled).
//   - error: non-nil triggers a requeue (image sync in progress, failed, or creation error).
func (r *Reconciler) reconcileImageSync(ctx context.Context, tc *butlerv1alpha1.TenantCluster, pc *butlerv1alpha1.ProviderConfig, bc *butlerv1alpha1.ButlerConfig) (string, error) {
	logger := log.FromContext(ctx)

	// Resolve schematicID: TC spec takes precedence, then ButlerConfig default
	schematicID := tc.Spec.Workers.MachineTemplate.OS.SchematicID
	if schematicID == "" && bc != nil && bc.Spec.ImageFactory != nil {
		schematicID = bc.Spec.ImageFactory.DefaultSchematicID
	}
	if schematicID == "" {
		// No schematic configured — no image sync needed
		return "", nil
	}

	// Image factory must be configured if a schematicID is set
	if bc == nil || !bc.IsImageFactoryConfigured() {
		return "", fmt.Errorf("schematicID %q is set but Image Factory is not configured in ButlerConfig", schematicID)
	}

	// Check if auto-sync is enabled
	if !bc.IsAutoSyncEnabled() {
		logger.V(1).Info("image auto-sync disabled, skipping ImageSync creation")
		return "", nil
	}

	// Resolve image version, architecture, and platform
	version := tc.Spec.Workers.MachineTemplate.OS.Version
	if version == "" {
		version = "9.5" // default OS version from kubebuilder marker
	}
	// Architecture defaults to amd64 — all current Butler providers target amd64.
	// When multi-arch support is added, this should read from MachineTemplate or OS spec.
	arch := "amd64"
	// Platform is the artifact name prefix in the factory URL.
	// For Butler Image Factory: use the OS type (talos, flatcar, kairos, bottlerocket).
	// For Siderolabs factory (factory.talos.dev): use "nocloud" (or metal, vmware, etc.).
	platform := string(tc.Spec.Workers.MachineTemplate.OS.Type)
	if platform == "" {
		platform = "nocloud"
	}

	// Truncate schematicID to 63 chars for label value (Kubernetes label limit)
	labelSchematicID := schematicID
	if len(labelSchematicID) > 63 {
		labelSchematicID = labelSchematicID[:63]
	}

	// Build label selector for deduplication
	matchLabels := map[string]string{
		butlerv1alpha1.LabelSchematicID:    labelSchematicID,
		butlerv1alpha1.LabelImageVersion:   version,
		butlerv1alpha1.LabelProviderConfig: pc.Name,
		butlerv1alpha1.LabelImageArch:      arch,
	}

	// List existing ImageSyncs matching labels in the TC's namespace
	var existingList butlerv1alpha1.ImageSyncList
	if err := r.List(ctx, &existingList,
		client.InNamespace(tc.Namespace),
		client.MatchingLabels(matchLabels),
	); err != nil {
		return "", fmt.Errorf("failed to list ImageSyncs: %w", err)
	}

	if len(existingList.Items) > 0 {
		is := &existingList.Items[0]

		switch {
		case is.IsReady():
			logger.V(1).Info("ImageSync is ready", "name", is.Name, "providerImageRef", is.Status.ProviderImageRef)
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionImageReady,
				metav1.ConditionTrue, "ImageReady", "OS image synced to provider")
			tc.Status.ImageSyncRef = &butlerv1alpha1.LocalObjectReference{Name: is.Name}
			return is.Status.ProviderImageRef, nil

		case is.IsFailed():
			reason := is.Status.FailureReason
			if reason == "" {
				reason = "ImageSyncFailed"
			}
			message := is.Status.FailureMessage
			if message == "" {
				message = "Image sync failed"
			}
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionImageReady,
				metav1.ConditionFalse, reason, message)
			tc.Status.ImageSyncRef = &butlerv1alpha1.LocalObjectReference{Name: is.Name}
			return "", fmt.Errorf("ImageSync %s failed: %s", is.Name, message)

		default:
			// In progress (Pending, Building, Downloading, Uploading)
			logger.Info("ImageSync in progress", "name", is.Name, "phase", is.Status.Phase)
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionImageReady,
				metav1.ConditionFalse, "ImageSyncInProgress",
				fmt.Sprintf("Image sync %s is %s", is.Name, is.Status.Phase))
			tc.Status.ImageSyncRef = &butlerv1alpha1.LocalObjectReference{Name: is.Name}
			return "", fmt.Errorf("ImageSync %s is in progress (phase: %s)", is.Name, is.Status.Phase)
		}
	}

	// No existing ImageSync found — create one

	// Build a short, DNS-safe name
	schematicPrefix := schematicID
	if len(schematicPrefix) > 8 {
		schematicPrefix = schematicPrefix[:8]
	}
	// Sanitize version for DNS label (replace dots with dashes, strip leading 'v')
	sanitizedVersion := strings.ReplaceAll(version, ".", "-")
	sanitizedVersion = strings.TrimPrefix(sanitizedVersion, "v")
	imageSyncName := fmt.Sprintf("%s-%s-%s", tc.Name, schematicPrefix, sanitizedVersion)
	// Ensure name is DNS-safe (max 253 chars for object names, but keep it reasonable)
	if len(imageSyncName) > 63 {
		imageSyncName = imageSyncName[:63]
	}

	// Resolve provider config reference
	pcRef := butlerv1alpha1.ProviderReference{
		Name: pc.Name,
	}
	if pc.Namespace != "" && pc.Namespace != tc.Namespace {
		pcRef.Namespace = pc.Namespace
	}

	is := &butlerv1alpha1.ImageSync{
		ObjectMeta: metav1.ObjectMeta{
			Name:      imageSyncName,
			Namespace: tc.Namespace,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy:       "butler",
				butlerv1alpha1.LabelTenant:          tc.Name,
				butlerv1alpha1.LabelSchematicID:     labelSchematicID,
				butlerv1alpha1.LabelImageVersion:    version,
				butlerv1alpha1.LabelProviderConfig:  pc.Name,
				butlerv1alpha1.LabelImageArch:       arch,
			},
		},
		Spec: butlerv1alpha1.ImageSyncSpec{
			FactoryRef: butlerv1alpha1.ImageFactoryRef{
				SchematicID: schematicID,
				Version:     version,
				Arch:        arch,
				Platform:    platform,
			},
			ProviderConfigRef: pcRef,
			Format:            "qcow2",
			TransferMode:      butlerv1alpha1.TransferModeDirect,
		},
	}

	// Set owner reference for garbage collection
	if err := controllerutil.SetControllerReference(tc, is, r.Scheme); err != nil {
		return "", fmt.Errorf("failed to set owner reference on ImageSync: %w", err)
	}

	logger.Info("creating ImageSync", "name", is.Name, "schematicID", schematicID, "version", version)
	if err := r.Create(ctx, is); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Race condition — another reconcile created it. Requeue to pick it up.
			return "", fmt.Errorf("ImageSync %s was just created, requeueing", imageSyncName)
		}
		return "", fmt.Errorf("failed to create ImageSync %s: %w", imageSyncName, err)
	}

	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionImageReady,
		metav1.ConditionFalse, "ImageSyncPending", "Image sync created, waiting for provider to process")
	tc.Status.ImageSyncRef = &butlerv1alpha1.LocalObjectReference{Name: is.Name}

	return "", fmt.Errorf("ImageSync %s created, waiting for completion", imageSyncName)
}
