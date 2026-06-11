// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"time"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/addons"
)

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
	var failedAddons []string

	// The local provider runs a single-node toy tenant on CAPD. Cilium (CNI) and
	// MetalLB (the control plane LoadBalancer IP) are load-bearing and stay; Longhorn
	// and Traefik are unnecessary and only add load to a constrained node.
	providerType, _ := resolveProviderType(ctx, r.Client, tc)
	isLocal := providerType == string(butlerv1alpha1.ProviderTypeLocal)

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
		failedAddons = append(failedAddons, "cert-manager")
		r.Recorder.Eventf(tc, corev1.EventTypeWarning, "AddonInstallFailed",
			"cert-manager %s installation failed: %s", certManagerVersion, truncateError(err))
		addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
			Name: "cert-manager", Version: certManagerVersion, Status: "Failed", ManagedBy: "butler",
		})
	} else {
		addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
			Name: "cert-manager", Version: certManagerVersion, Status: "Healthy", ManagedBy: "butler",
		})
	}

	// 3. Longhorn - storage (skipped for local; kind's default StorageClass suffices)
	longhornVersion := addons.DefaultLonghornVersion
	if tc.Spec.Addons.Storage != nil && tc.Spec.Addons.Storage.Version != "" {
		longhornVersion = tc.Spec.Addons.Storage.Version
	}
	if isLocal {
		logger.Info("skipping Longhorn installation (local provider)")
	} else if err := r.Installer.InstallLonghorn(ctx, kubeconfigData, longhornVersion); err != nil {
		logger.Error(err, "failed to install Longhorn")
		failedAddons = append(failedAddons, "longhorn")
		r.Recorder.Eventf(tc, corev1.EventTypeWarning, "AddonInstallFailed",
			"longhorn %s installation failed: %s", longhornVersion, truncateError(err))
		addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
			Name: "longhorn", Version: longhornVersion, Status: "Failed", ManagedBy: "butler",
		})
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
	poolStart, poolEnd := r.resolveMetalLBPool(ctx, tc)
	logger.Info("installing MetalLB", "version", metallbVersion, "poolStart", poolStart, "poolEnd", poolEnd)
	if err := r.Installer.InstallMetalLB(ctx, kubeconfigData, metallbVersion, poolStart, poolEnd); err != nil {
		logger.Error(err, "failed to install MetalLB")
		failedAddons = append(failedAddons, "metallb")
		r.Recorder.Eventf(tc, corev1.EventTypeWarning, "AddonInstallFailed",
			"metallb %s installation failed: %s", metallbVersion, truncateError(err))
		addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
			Name: "metallb", Version: metallbVersion, Status: "Failed", ManagedBy: "butler",
		})
	} else {
		addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
			Name: "metallb", Version: metallbVersion, Status: "Healthy", ManagedBy: "butler",
		})
	}

	// 5. Traefik - Ingress (optional; skipped for local)
	if tc.Spec.Addons.Ingress.IsIngressEnabled() && !isLocal {
		traefikVersion := addons.DefaultTraefikVersion
		if tc.Spec.Addons.Ingress != nil && tc.Spec.Addons.Ingress.Version != "" {
			traefikVersion = tc.Spec.Addons.Ingress.Version
		}
		logger.Info("installing Traefik", "version", traefikVersion)
		if err := r.Installer.InstallTraefik(ctx, kubeconfigData, traefikVersion); err != nil {
			logger.Error(err, "failed to install Traefik")
			failedAddons = append(failedAddons, "traefik")
			r.Recorder.Eventf(tc, corev1.EventTypeWarning, "AddonInstallFailed",
				"traefik %s installation failed: %s", traefikVersion, truncateError(err))
			addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
				Name: "traefik", Version: traefikVersion, Status: "Failed", ManagedBy: "butler",
			})
		} else {
			addonStatuses = append(addonStatuses, butlerv1alpha1.AddonStatus{
				Name: "traefik", Version: traefikVersion, Status: "Healthy", ManagedBy: "butler",
			})
		}
	} else {
		logger.Info("skipping Traefik installation (disabled by spec)")
	}

	// Update observed state
	if tc.Status.ObservedState == nil {
		tc.Status.ObservedState = &butlerv1alpha1.ObservedClusterState{}
	}
	tc.Status.ObservedState.Addons = addonStatuses

	if len(failedAddons) > 0 {
		msg := fmt.Sprintf("failed to install: %s", strings.Join(failedAddons, ", "))
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionAddonsReady,
			metav1.ConditionFalse, ReasonAddonInstallFailed, msg)
	} else {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionAddonsReady,
			metav1.ConditionTrue, "AddonsInstalled", "All addons installed successfully")
	}

	// Set kubeconfig secret reference for TenantAddon controller
	tc.Status.KubeconfigSecretRef = &butlerv1alpha1.LocalObjectReference{
		Name: fmt.Sprintf("%s-admin-kubeconfig", tc.Name),
	}

	// Transition to Ready even with addon failures -- the cluster is functional
	// and the steady-state retry loop will attempt to heal failed addons.
	tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseReady
	now := metav1.Now()
	tc.Status.LastTransitionTime = &now

	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionReady,
		metav1.ConditionTrue, ReasonReady, "Cluster is ready")
	r.Recorder.Eventf(tc, corev1.EventTypeNormal, "ClusterReady",
		"Cluster %s is ready with %d workers", tc.Name, tc.Spec.Workers.Replicas)

	logger.Info("TenantCluster is ready", "name", tc.Name)

	return ctrl.Result{}, nil
}

// resolveMetalLBPool determines the MetalLB IP pool range for a tenant cluster.
// It checks the IPAM allocation first, then falls back to the manual loadBalancerPool spec.
func (r *Reconciler) resolveMetalLBPool(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (string, string) {
	var poolStart, poolEnd string
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
	return poolStart, poolEnd
}

// reconcileAddonHealth checks for failed or missing platform addons on a Ready
// cluster and attempts to reinstall them. An addon needs retry if it is recorded
// as Failed in observedState OR if it is expected by the spec but absent from
// observedState entirely (e.g., it failed silently during initial install before
// failure tracking was added). Returns true if any addons still need retry,
// which signals the caller to cap the requeue interval.
func (r *Reconciler) reconcileAddonHealth(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) (bool, error) {
	logger := log.FromContext(ctx)

	// Build the expected non-CNI addons list from the spec. Cilium is excluded
	// because its failure is fatal during initial install and it requires
	// CiliumConfig (not just a version string).
	type expectedAddon struct {
		name    string
		version string
	}

	certManagerVersion := addons.DefaultCertManagerVersion
	if tc.Spec.Addons.CertManager != nil && tc.Spec.Addons.CertManager.Version != "" {
		certManagerVersion = tc.Spec.Addons.CertManager.Version
	}
	longhornVersion := addons.DefaultLonghornVersion
	if tc.Spec.Addons.Storage != nil && tc.Spec.Addons.Storage.Version != "" {
		longhornVersion = tc.Spec.Addons.Storage.Version
	}
	metallbVersion := addons.DefaultMetalLBVersion
	if tc.Spec.Addons.LoadBalancer != nil && tc.Spec.Addons.LoadBalancer.Version != "" {
		metallbVersion = tc.Spec.Addons.LoadBalancer.Version
	}

	// Local toy tenants skip Longhorn and Traefik (see reconcileAddons); only
	// cert-manager and MetalLB are expected alongside the required Cilium.
	providerType, _ := resolveProviderType(ctx, r.Client, tc)
	isLocal := providerType == string(butlerv1alpha1.ProviderTypeLocal)

	expected := []expectedAddon{
		{"cert-manager", certManagerVersion},
		{"metallb", metallbVersion},
	}
	if !isLocal {
		expected = append(expected, expectedAddon{"longhorn", longhornVersion})
	}
	if tc.Spec.Addons.Ingress.IsIngressEnabled() && !isLocal {
		traefikVersion := addons.DefaultTraefikVersion
		if tc.Spec.Addons.Ingress != nil && tc.Spec.Addons.Ingress.Version != "" {
			traefikVersion = tc.Spec.Addons.Ingress.Version
		}
		expected = append(expected, expectedAddon{"traefik", traefikVersion})
	}

	// Index observed state by addon name for lookup.
	observed := make(map[string]string) // name -> status
	if tc.Status.ObservedState != nil {
		for _, a := range tc.Status.ObservedState.Addons {
			observed[a.Name] = a.Status
		}
	}

	// Collect addons that need retry: Failed in observedState OR absent entirely.
	var needsRetry []expectedAddon
	for _, ea := range expected {
		status, present := observed[ea.name]
		if !present || status == "Failed" {
			needsRetry = append(needsRetry, ea)
		}
	}
	if len(needsRetry) == 0 {
		return false, nil
	}

	kubeconfigData, err := r.getTenantKubeconfig(ctx, tc, butlerConfig)
	if err != nil {
		return true, fmt.Errorf("addon health: failed to get tenant kubeconfig: %w", err)
	}

	var stillFailed []string

	for _, addon := range needsRetry {
		var installErr error
		switch addon.name {
		case "cert-manager":
			installErr = r.Installer.InstallCertManager(ctx, kubeconfigData, addon.version)
		case "longhorn":
			installErr = r.Installer.InstallLonghorn(ctx, kubeconfigData, addon.version)
		case "metallb":
			poolStart, poolEnd := r.resolveMetalLBPool(ctx, tc)
			installErr = r.Installer.InstallMetalLB(ctx, kubeconfigData, addon.version, poolStart, poolEnd)
		case "traefik":
			installErr = r.Installer.InstallTraefik(ctx, kubeconfigData, addon.version)
		}

		if installErr != nil {
			logger.Error(installErr, "addon retry failed", "addon", addon.name)
			stillFailed = append(stillFailed, addon.name)
		} else {
			logger.Info("addon retry succeeded", "addon", addon.name)
			r.Recorder.Eventf(tc, corev1.EventTypeNormal, "AddonRetrySucceeded",
				"%s installed successfully on retry", addon.name)
		}
		// Update or append observedState entry.
		r.upsertAddonStatus(tc, addon.name, addon.version, installErr)
	}

	// Update AddonsReady condition
	if len(stillFailed) > 0 {
		msg := fmt.Sprintf("failed to install: %s", strings.Join(stillFailed, ", "))
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionAddonsReady,
			metav1.ConditionFalse, ReasonAddonInstallFailed, msg)
	} else {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionAddonsReady,
			metav1.ConditionTrue, "AddonsInstalled", "All addons installed successfully")
	}

	return len(stillFailed) > 0, nil
}

// upsertAddonStatus updates an existing addon entry in observedState or appends
// a new one if the addon was absent.
func (r *Reconciler) upsertAddonStatus(tc *butlerv1alpha1.TenantCluster, name, version string, installErr error) {
	status := "Healthy"
	if installErr != nil {
		status = "Failed"
	}

	if tc.Status.ObservedState == nil {
		tc.Status.ObservedState = &butlerv1alpha1.ObservedClusterState{}
	}

	for i := range tc.Status.ObservedState.Addons {
		if tc.Status.ObservedState.Addons[i].Name == name {
			tc.Status.ObservedState.Addons[i].Status = status
			tc.Status.ObservedState.Addons[i].Version = version
			return
		}
	}
	// Absent: append new entry.
	tc.Status.ObservedState.Addons = append(tc.Status.ObservedState.Addons, butlerv1alpha1.AddonStatus{
		Name: name, Version: version, Status: status, ManagedBy: "butler",
	})
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

	// Resolve provider type once for all builders that need cluster identification.
	providerType, err := resolveProviderType(ctx, r.Client, tc)
	if err != nil {
		logger.Error(err, "failed to resolve provider type", "cluster", tc.Name)
	}

	if enroll.VectorAgent && obs.Pipeline.LogEndpoint != "" {
		if err := r.ensureAutoEnrolledAddon(ctx, tc, "vector-agent", buildVectorAgentValues(obs.Pipeline, tc, providerType)); err != nil {
			logger.Error(err, "failed to auto-enroll vector-agent")
		}
	}

	if enroll.Prometheus && obs.Pipeline.MetricEndpoint != "" {
		if err := r.ensureAutoEnrolledAddon(ctx, tc, "prometheus-operator", buildPrometheusValues(obs.Pipeline, tc.Name)); err != nil {
			logger.Error(err, "failed to auto-enroll prometheus-operator")
		}
	}

	if enroll.OtelCollector && obs.Pipeline.TraceEndpoint != "" {
		if err := r.ensureAutoEnrolledAddon(ctx, tc, "otel-collector", buildOtelCollectorValues(obs.Pipeline, tc, providerType)); err != nil {
			logger.Error(err, "failed to auto-enroll otel-collector")
		}
	}

	return nil
}

// ensureAutoEnrolledAddon creates a TenantAddon if it doesn't already exist,
// or updates its values if the desired configuration has changed (e.g., a
// pipeline endpoint was modified in ButlerConfig).
func (r *Reconciler) ensureAutoEnrolledAddon(ctx context.Context, tc *butlerv1alpha1.TenantCluster, addonDefName string, values *butlerv1alpha1.ExtensionValues) error {
	logger := log.FromContext(ctx)
	addonName := fmt.Sprintf("%s-%s", tc.Name, addonDefName)

	existing := &butlerv1alpha1.TenantAddon{}
	err := r.Get(ctx, client.ObjectKey{Name: addonName, Namespace: tc.Namespace}, existing)
	if err == nil {
		return r.updateAutoEnrolledAddonIfNeeded(ctx, existing, values)
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check TenantAddon %s: %w", addonName, err)
	}

	addon := &butlerv1alpha1.TenantAddon{
		ObjectMeta: metav1.ObjectMeta{
			Name:      addonName,
			Namespace: tc.Namespace,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy:            "butler",
				butlerv1alpha1.LabelTeam:                 tc.Labels[butlerv1alpha1.LabelTeam],
				"butler.butlerlabs.dev/cluster":          tc.Name,
				"butler.butlerlabs.dev/addon-definition": addonDefName,
				"butler.butlerlabs.dev/auto-enrolled":    "true",
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

// updateAutoEnrolledAddonIfNeeded compares the desired values with the existing
// TenantAddon and updates if they differ. Only auto-enrolled addons are updated;
// manually created TenantAddons (missing the auto-enrolled label) are left alone.
func (r *Reconciler) updateAutoEnrolledAddonIfNeeded(ctx context.Context, existing *butlerv1alpha1.TenantAddon, desired *butlerv1alpha1.ExtensionValues) error {
	if existing.Labels["butler.butlerlabs.dev/auto-enrolled"] != "true" {
		return nil
	}

	existingRaw := rawOrEmpty(existing.Spec.Values)
	desiredRaw := rawOrEmpty(desired)
	if bytes.Equal(existingRaw, desiredRaw) {
		return nil
	}

	logger := log.FromContext(ctx)
	logger.Info("updating auto-enrolled addon values",
		"addon", existing.Name, "cluster", existing.Spec.ClusterRef.Name)

	existing.Spec.Values = desired
	if err := r.Update(ctx, existing); err != nil {
		return fmt.Errorf("failed to update TenantAddon %s: %w", existing.Name, err)
	}
	return nil
}

// rawOrEmpty returns the Raw bytes from an ExtensionValues, or an empty slice if nil.
func rawOrEmpty(v *butlerv1alpha1.ExtensionValues) []byte {
	if v == nil {
		return nil
	}
	return v.Raw
}

// buildVectorAgentValues configures the vector-agent sink to forward to the
// pipeline aggregator and injects cluster identification fields into the VRL
// remap transform so log events carry source cluster metadata.
//
// Only transforms and sink URI are set here. The remaining sink fields (type,
// inputs, encoding) come from the AddonDefinition default via recursive deep
// merge in tenantaddon_controller.go. The default's sinks.aggregator.inputs
// references ["enrich_metadata"], ensuring events flow through the transform
// before reaching the sink.
func buildVectorAgentValues(pipeline *butlerv1alpha1.ObservabilityPipelineConfig, tc *butlerv1alpha1.TenantCluster, providerType string) *butlerv1alpha1.ExtensionValues {
	if pipeline == nil || pipeline.LogEndpoint == "" {
		return nil
	}

	values := map[string]interface{}{
		"customConfig": map[string]interface{}{
			"transforms": map[string]interface{}{
				"enrich_metadata": map[string]interface{}{
					"type":   "remap",
					"inputs": []string{"kubernetes_logs"},
					"source": buildVectorRemapSource(tc, providerType),
				},
			},
			"sinks": map[string]interface{}{
				"aggregator": map[string]interface{}{
					"uri": pipeline.LogEndpoint,
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

// buildPrometheusValues configures remote-write to the pipeline metric endpoint
// and sets externalLabels.cluster so metrics are identifiable by source cluster
// in the backend.
func buildPrometheusValues(pipeline *butlerv1alpha1.ObservabilityPipelineConfig, clusterName string) *butlerv1alpha1.ExtensionValues {
	if pipeline == nil || pipeline.MetricEndpoint == "" {
		return nil
	}

	values := map[string]interface{}{
		"prometheus": map[string]interface{}{
			"prometheusSpec": map[string]interface{}{
				"externalLabels": map[string]interface{}{
					"cluster": clusterName,
				},
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

// buildOtelCollectorValues configures the OTLP exporter to the pipeline trace
// endpoint and injects cluster identification as OTLP resource attributes via
// a resource processor.
func buildOtelCollectorValues(pipeline *butlerv1alpha1.ObservabilityPipelineConfig, tc *butlerv1alpha1.TenantCluster, providerType string) *butlerv1alpha1.ExtensionValues {
	if pipeline == nil || pipeline.TraceEndpoint == "" {
		return nil
	}

	attrs := buildOtelResourceAttributes(tc, providerType)

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
			"processors": map[string]interface{}{
				"resource": map[string]interface{}{
					"attributes": attrs,
				},
			},
			"service": map[string]interface{}{
				"pipelines": map[string]interface{}{
					"traces": map[string]interface{}{
						"receivers":  []string{"otlp"},
						"processors": []string{"resource", "memory_limiter", "batch"},
						"exporters":  []string{"otlp"},
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

// buildOtelResourceAttributes produces the OTLP resource attribute upsert list
// for cluster identification. Uses semconv keys where applicable.
func buildOtelResourceAttributes(tc *butlerv1alpha1.TenantCluster, providerType string) []map[string]interface{} {
	attrs := []map[string]interface{}{
		{"key": "k8s.cluster.name", "value": tc.Name, "action": "upsert"},
		{"key": "k8s.namespace.name", "value": tc.Namespace, "action": "upsert"},
		{"key": "k8s.cluster.uid", "value": string(tc.UID), "action": "upsert"},
	}
	if env := tc.Labels[butlerv1alpha1.LabelEnvironment]; env != "" {
		attrs = append(attrs, map[string]interface{}{
			"key": "butler.environment", "value": env, "action": "upsert",
		})
	}
	if providerType != "" {
		attrs = append(attrs, map[string]interface{}{
			"key": "butler.provider_type", "value": providerType, "action": "upsert",
		})
	}
	if pc := tc.Labels[butlerv1alpha1.LabelProviderConfig]; pc != "" {
		attrs = append(attrs, map[string]interface{}{
			"key": "butler.provider_config", "value": pc, "action": "upsert",
		})
	}
	return attrs
}

// resolveProviderType looks up the canonical provider type (e.g., "nutanix",
// "harvester") from the TenantCluster's ProviderConfigRef. Returns "" if the
// ref is nil or empty. Uses the controller-runtime cached client, so cost is
// bounded by informer cache, not API server round-trips.
//
// Accepts client.Reader (not client.Client) to signal read-only intent and
// simplify test setup with fake.NewClientBuilder.
func resolveProviderType(ctx context.Context, c client.Reader, tc *butlerv1alpha1.TenantCluster) (string, error) {
	if tc.Spec.ProviderConfigRef == nil || tc.Spec.ProviderConfigRef.Name == "" {
		return "", nil
	}

	ns := tc.Spec.ProviderConfigRef.Namespace
	if ns == "" {
		ns = "butler-system"
	}

	pc := &butlerv1alpha1.ProviderConfig{}
	if err := c.Get(ctx, client.ObjectKey{Name: tc.Spec.ProviderConfigRef.Name, Namespace: ns}, pc); err != nil {
		return "", fmt.Errorf("get ProviderConfig %s/%s: %w", ns, tc.Spec.ProviderConfigRef.Name, err)
	}
	return string(pc.Spec.Provider), nil
}

// buildVectorRemapSource produces the VRL source for the Vector agent's
// enrich_metadata transform. Required fields (host, service, namespace, cluster,
// tenant_namespace, cluster_uid) are always emitted. Optional fields
// (environment, provider_type, provider_config) are omitted when their source
// value is empty, avoiding empty-string Loki labels.
func buildVectorRemapSource(tc *butlerv1alpha1.TenantCluster, providerType string) string {
	var b strings.Builder
	b.WriteString(".host = .kubernetes.pod_node_name\n")
	b.WriteString(".service = .kubernetes.pod_name\n")
	b.WriteString(".namespace = .kubernetes.pod_namespace\n")
	fmt.Fprintf(&b, ".cluster = %s\n", strconv.Quote(tc.Name))
	fmt.Fprintf(&b, ".tenant_namespace = %s\n", strconv.Quote(tc.Namespace))
	fmt.Fprintf(&b, ".cluster_uid = %s\n", strconv.Quote(string(tc.UID)))

	if env := tc.Labels[butlerv1alpha1.LabelEnvironment]; env != "" {
		fmt.Fprintf(&b, ".environment = %s\n", strconv.Quote(env))
	}
	if providerType != "" {
		fmt.Fprintf(&b, ".provider_type = %s\n", strconv.Quote(providerType))
	}
	if pc := tc.Labels[butlerv1alpha1.LabelProviderConfig]; pc != "" {
		fmt.Fprintf(&b, ".provider_config = %s\n", strconv.Quote(pc))
	}
	return b.String()
}
