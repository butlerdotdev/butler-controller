// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"encoding/json"
	"fmt"

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
	r.Recorder.Eventf(tc, corev1.EventTypeNormal, "ClusterReady",
		"Cluster %s is ready with %d workers", tc.Name, tc.Spec.Workers.Replicas)

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

	values := map[string]interface{}{
		"customConfig": map[string]interface{}{
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
