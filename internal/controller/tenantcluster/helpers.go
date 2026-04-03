// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/tenant"
)

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

// getTenantClient returns a cached Kubernetes client for the tenant cluster.
// Falls back to ad-hoc client creation if the ClientManager is not configured.
func (r *Reconciler) getTenantClient(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (*tenant.TenantClient, error) {
	if r.ClientManager == nil {
		return nil, fmt.Errorf("tenant ClientManager not configured")
	}
	if tc.Status.TenantNamespace == "" {
		return nil, fmt.Errorf("tenant namespace not yet assigned")
	}
	return r.ClientManager.GetClient(ctx, tc.Status.TenantNamespace, tc.Name)
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

func generateTenantNamespace(tc *butlerv1alpha1.TenantCluster) string {
	uid := string(tc.UID)
	if len(uid) > 8 {
		uid = uid[:8]
	}
	return fmt.Sprintf("%s-%s", tc.Name, uid)
}
