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

package butlerconfig

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const (
	// SingletonName is the required name for the ButlerConfig resource.
	// Only one ButlerConfig should exist in the cluster.
	SingletonName = "butler"

	// Condition types for ButlerConfig
	ConditionConfigured     = "Configured"
	ConditionNamespaceReady = "NamespaceReady"
	ConditionGatewayReady   = "GatewayReady"
)

// Reconciler reconciles a ButlerConfig object
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=butlerconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=butlerconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=butlerconfigs/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=teams,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways/status,verbs=get

// Reconcile handles ButlerConfig reconciliation
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the ButlerConfig
	config := &butlerv1alpha1.ButlerConfig{}
	if err := r.Get(ctx, req.NamespacedName, config); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("reconciling ButlerConfig", "name", config.Name)

	// Validate singleton name
	if config.Name != SingletonName {
		logger.Error(nil, "ButlerConfig must be named 'butler'", "actual", config.Name)
		r.setInvalidNameCondition(ctx, config)
		return ctrl.Result{}, nil
	}

	// Reconcile default namespace if mode is Optional or Disabled
	if err := r.reconcileDefaultNamespace(ctx, config); err != nil {
		logger.Error(err, "failed to reconcile default namespace")
		r.setFailedCondition(ctx, config, "NamespaceError", err.Error())
		return ctrl.Result{}, err
	}

	// Reconcile Gateway if configured
	if err := r.reconcileGateway(ctx, config); err != nil {
		logger.Error(err, "failed to reconcile Gateway")
		r.setFailedCondition(ctx, config, "GatewayError", err.Error())
		return ctrl.Result{}, err
	}

	// Count teams and clusters for status
	if err := r.reconcileStatus(ctx, config); err != nil {
		logger.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	logger.Info("ButlerConfig reconciliation complete", "name", config.Name,
		"mode", config.Spec.MultiTenancy.Mode,
		"teamCount", config.Status.TeamCount,
		"clusterCount", config.Status.ClusterCount)

	// Requeue periodically to update counts and check Gateway status
	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

// reconcileDefaultNamespace creates the default namespace if needed
func (r *Reconciler) reconcileDefaultNamespace(ctx context.Context, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	// Only create default namespace for Optional or Disabled modes
	mode := config.Spec.MultiTenancy.Mode
	if mode == butlerv1alpha1.MultiTenancyModeEnforced {
		logger.V(1).Info("skipping default namespace creation in Enforced mode")
		meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
			Type:               ConditionNamespaceReady,
			Status:             metav1.ConditionTrue,
			Reason:             "NotRequired",
			Message:            "Default namespace not required in Enforced mode",
			ObservedGeneration: config.Generation,
		})
		return nil
	}

	// Get the default namespace name
	namespaceName := config.Spec.DefaultNamespace
	if namespaceName == "" {
		namespaceName = "butler-tenants"
	}

	// Create or update the namespace
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespaceName,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, namespace, func() error {
		if namespace.Labels == nil {
			namespace.Labels = make(map[string]string)
		}
		namespace.Labels[butlerv1alpha1.LabelManagedBy] = "butler"

		if namespace.Annotations == nil {
			namespace.Annotations = make(map[string]string)
		}
		namespace.Annotations[butlerv1alpha1.AnnotationDescription] = "Butler default namespace for tenant clusters"

		return nil
	})

	if err != nil {
		meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
			Type:               ConditionNamespaceReady,
			Status:             metav1.ConditionFalse,
			Reason:             "CreateFailed",
			Message:            err.Error(),
			ObservedGeneration: config.Generation,
		})
		return err
	}

	if op != controllerutil.OperationResultNone {
		logger.Info("default namespace reconciled", "namespace", namespaceName, "operation", op)
	}

	meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
		Type:               ConditionNamespaceReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Ready",
		Message:            "Default namespace exists: " + namespaceName,
		ObservedGeneration: config.Generation,
	})

	return nil
}

// reconcileGateway creates or updates the Butler-managed Gateway resource
// for control plane exposure when Gateway mode is configured.
func (r *Reconciler) reconcileGateway(ctx context.Context, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	// Check if Gateway is configured
	if !config.IsGatewayConfigured() {
		logger.V(1).Info("Gateway not configured, skipping Gateway reconciliation")

		// Clear Gateway status if it was previously set
		if config.Status.Gateway != nil {
			config.Status.Gateway = nil
		}

		// Remove GatewayReady condition if present
		meta.RemoveStatusCondition(&config.Status.Conditions, ConditionGatewayReady)
		return nil
	}

	// Ensure the Gateway namespace exists
	if err := r.ensureGatewayNamespace(ctx, config); err != nil {
		return fmt.Errorf("failed to ensure Gateway namespace: %w", err)
	}

	// Build the Gateway resource
	gateway := r.buildGateway(config)

	// Create or update the Gateway
	existingGateway := &gatewayv1.Gateway{}
	err := r.Get(ctx, client.ObjectKey{
		Name:      gateway.Name,
		Namespace: gateway.Namespace,
	}, existingGateway)

	if apierrors.IsNotFound(err) {
		// Create new Gateway
		logger.Info("creating Gateway", "name", gateway.Name, "namespace", gateway.Namespace)
		if err := r.Create(ctx, gateway); err != nil {
			meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
				Type:               ConditionGatewayReady,
				Status:             metav1.ConditionFalse,
				Reason:             "CreateFailed",
				Message:            err.Error(),
				ObservedGeneration: config.Generation,
			})
			return fmt.Errorf("failed to create Gateway: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to get Gateway: %w", err)
	} else {
		// Update existing Gateway if spec changed
		if r.gatewayNeedsUpdate(existingGateway, gateway) {
			logger.Info("updating Gateway", "name", gateway.Name, "namespace", gateway.Namespace)
			existingGateway.Spec = gateway.Spec
			existingGateway.Labels = gateway.Labels
			if err := r.Update(ctx, existingGateway); err != nil {
				return fmt.Errorf("failed to update Gateway: %w", err)
			}
		}
		gateway = existingGateway
	}

	// Update Gateway status in ButlerConfig
	if err := r.updateGatewayStatus(ctx, config, gateway); err != nil {
		return fmt.Errorf("failed to update Gateway status: %w", err)
	}

	return nil
}

// ensureGatewayNamespace ensures the Gateway namespace exists
func (r *Reconciler) ensureGatewayNamespace(ctx context.Context, config *butlerv1alpha1.ButlerConfig) error {
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: config.GetGatewayNamespace(),
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, namespace, func() error {
		if namespace.Labels == nil {
			namespace.Labels = make(map[string]string)
		}
		namespace.Labels[butlerv1alpha1.LabelManagedBy] = "butler"
		return nil
	})

	return err
}

// buildGateway constructs the Gateway resource for control plane exposure.
// The Gateway uses TLS passthrough mode to route traffic based on SNI hostname
// to the appropriate TenantControlPlane service.
func (r *Reconciler) buildGateway(config *butlerv1alpha1.ButlerConfig) *gatewayv1.Gateway {
	domain := config.GetGatewayDomain()
	wildcardHostname := gatewayv1.Hostname("*." + domain)

	// Build annotations from config + defaults
	annotations := make(map[string]string)
	if config.Spec.ControlPlane != nil && config.Spec.ControlPlane.Gateway != nil {
		for k, v := range config.Spec.ControlPlane.Gateway.Annotations {
			annotations[k] = v
		}
	}

	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      config.GetGatewayName(),
			Namespace: config.GetGatewayNamespace(),
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy: "butler",
			},
			Annotations: annotations,
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: gatewayv1.ObjectName(config.GetGatewayClassName()),
			Listeners: []gatewayv1.Listener{
				// Listener for kube-apiserver traffic (port 6443)
				{
					Name:     "kube-apiserver",
					Port:     gatewayv1.PortNumber(6443),
					Protocol: gatewayv1.TLSProtocolType,
					Hostname: &wildcardHostname,
					TLS: &gatewayv1.GatewayTLSConfig{
						Mode: ptr.To(gatewayv1.TLSModePassthrough),
					},
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Kinds: []gatewayv1.RouteGroupKind{
							{
								Group: ptr.To(gatewayv1.Group("gateway.networking.k8s.io")),
								Kind:  "TLSRoute",
							},
						},
						Namespaces: &gatewayv1.RouteNamespaces{
							From: ptr.To(gatewayv1.NamespacesFromAll),
						},
					},
				},
				// Listener for konnectivity-server traffic (port 8132)
				{
					Name:     "konnectivity-server",
					Port:     gatewayv1.PortNumber(8132),
					Protocol: gatewayv1.TLSProtocolType,
					Hostname: &wildcardHostname,
					TLS: &gatewayv1.GatewayTLSConfig{
						Mode: ptr.To(gatewayv1.TLSModePassthrough),
					},
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Kinds: []gatewayv1.RouteGroupKind{
							{
								Group: ptr.To(gatewayv1.Group("gateway.networking.k8s.io")),
								Kind:  "TLSRoute",
							},
						},
						Namespaces: &gatewayv1.RouteNamespaces{
							From: ptr.To(gatewayv1.NamespacesFromAll),
						},
					},
				},
			},
		},
	}

	return gateway
}

// gatewayNeedsUpdate checks if the existing Gateway needs to be updated
func (r *Reconciler) gatewayNeedsUpdate(existing, desired *gatewayv1.Gateway) bool {
	// Check GatewayClassName
	if existing.Spec.GatewayClassName != desired.Spec.GatewayClassName {
		return true
	}

	// Check listener count
	if len(existing.Spec.Listeners) != len(desired.Spec.Listeners) {
		return true
	}

	// Check each listener's essential fields
	for i, desiredListener := range desired.Spec.Listeners {
		existingListener := existing.Spec.Listeners[i]

		if existingListener.Name != desiredListener.Name ||
			existingListener.Port != desiredListener.Port ||
			existingListener.Protocol != desiredListener.Protocol {
			return true
		}

		// Check hostname
		if (existingListener.Hostname == nil) != (desiredListener.Hostname == nil) {
			return true
		}
		if existingListener.Hostname != nil && desiredListener.Hostname != nil &&
			*existingListener.Hostname != *desiredListener.Hostname {
			return true
		}

		// Check TLS mode
		if existingListener.TLS != nil && desiredListener.TLS != nil {
			if (existingListener.TLS.Mode == nil) != (desiredListener.TLS.Mode == nil) {
				return true
			}
			if existingListener.TLS.Mode != nil && desiredListener.TLS.Mode != nil &&
				*existingListener.TLS.Mode != *desiredListener.TLS.Mode {
				return true
			}
		}
	}

	return false
}

// updateGatewayStatus updates the ButlerConfig status with Gateway information
func (r *Reconciler) updateGatewayStatus(ctx context.Context, config *butlerv1alpha1.ButlerConfig, gateway *gatewayv1.Gateway) error {
	logger := log.FromContext(ctx)

	// Initialize Gateway status
	if config.Status.Gateway == nil {
		config.Status.Gateway = &butlerv1alpha1.GatewayStatus{}
	}

	// Count listeners
	config.Status.Gateway.ListenerCount = int32(len(gateway.Spec.Listeners))

	// Count TenantClusters using Gateway mode
	tenantCount, err := r.countGatewayTenants(ctx, config)
	if err != nil {
		logger.Error(err, "failed to count Gateway tenants")
	} else {
		config.Status.Gateway.TenantCount = tenantCount
	}

	// Extract Gateway address from status
	gatewayReady := false
	var gatewayAddress string

	if len(gateway.Status.Addresses) > 0 {
		addr := gateway.Status.Addresses[0]
		if addr.Value != "" {
			gatewayAddress = addr.Value
			gatewayReady = true
		}
	}

	config.Status.Gateway.Address = gatewayAddress
	config.Status.Gateway.Ready = gatewayReady

	// Set condition and message based on readiness
	if gatewayReady {
		config.Status.Gateway.Message = fmt.Sprintf("Gateway ready at %s with %d listeners",
			gatewayAddress, config.Status.Gateway.ListenerCount)

		meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
			Type:               ConditionGatewayReady,
			Status:             metav1.ConditionTrue,
			Reason:             "Ready",
			Message:            config.Status.Gateway.Message,
			ObservedGeneration: config.Generation,
		})
	} else {
		config.Status.Gateway.Message = "Waiting for Gateway to be assigned an address"

		meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
			Type:               ConditionGatewayReady,
			Status:             metav1.ConditionFalse,
			Reason:             "WaitingForAddress",
			Message:            config.Status.Gateway.Message,
			ObservedGeneration: config.Generation,
		})
	}

	return nil
}

// countGatewayTenants counts TenantClusters using Gateway exposure mode
func (r *Reconciler) countGatewayTenants(ctx context.Context, config *butlerv1alpha1.ButlerConfig) (int32, error) {
	clusterList := &butlerv1alpha1.TenantClusterList{}
	if err := r.List(ctx, clusterList); err != nil {
		return 0, err
	}

	var count int32
	defaultMode := config.GetDefaultExposureMode()

	for _, tc := range clusterList.Items {
		effectiveMode := tc.Spec.ControlPlane.ExposureMode
		if effectiveMode == "" {
			effectiveMode = defaultMode
		}

		if effectiveMode == butlerv1alpha1.ControlPlaneExposureModeGateway {
			count++
		}
	}

	return count, nil
}

// reconcileStatus updates team and cluster counts
func (r *Reconciler) reconcileStatus(ctx context.Context, config *butlerv1alpha1.ButlerConfig) error {
	// Count teams
	teamList := &butlerv1alpha1.TeamList{}
	if err := r.List(ctx, teamList); err != nil {
		return err
	}
	config.Status.TeamCount = int32(len(teamList.Items))

	// Count clusters across all namespaces
	clusterList := &butlerv1alpha1.TenantClusterList{}
	if err := r.List(ctx, clusterList); err != nil {
		return err
	}
	config.Status.ClusterCount = int32(len(clusterList.Items))

	// Set observed generation
	config.Status.ObservedGeneration = config.Generation

	// Set configured condition
	meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
		Type:               ConditionConfigured,
		Status:             metav1.ConditionTrue,
		Reason:             "ConfigValid",
		Message:            "ButlerConfig is valid and active",
		ObservedGeneration: config.Generation,
	})

	// Update status
	return r.Status().Update(ctx, config)
}

// setInvalidNameCondition sets a condition for invalid config name
func (r *Reconciler) setInvalidNameCondition(ctx context.Context, config *butlerv1alpha1.ButlerConfig) {
	meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
		Type:               ConditionConfigured,
		Status:             metav1.ConditionFalse,
		Reason:             "InvalidName",
		Message:            "ButlerConfig must be named 'butler', got: " + config.Name,
		ObservedGeneration: config.Generation,
	})
	_ = r.Status().Update(ctx, config)
}

// setFailedCondition sets a failed condition
func (r *Reconciler) setFailedCondition(ctx context.Context, config *butlerv1alpha1.ButlerConfig, reason, message string) {
	meta.SetStatusCondition(&config.Status.Conditions, metav1.Condition{
		Type:               ConditionConfigured,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: config.Generation,
	})
	_ = r.Status().Update(ctx, config)
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.ButlerConfig{}).
		Owns(&gatewayv1.Gateway{}).
		Named("butlerconfig").
		Complete(r)
}
