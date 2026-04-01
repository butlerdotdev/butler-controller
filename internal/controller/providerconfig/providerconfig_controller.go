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

package providerconfig

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const (
	ConditionCredentialsValid = "CredentialsValid"
	ConditionPoolAvailable    = "PoolAvailable"
)

// expectedSecretKeys defines the required Secret keys per provider type.
var expectedSecretKeys = map[butlerv1alpha1.ProviderType][]string{
	butlerv1alpha1.ProviderTypeHarvester: {"kubeconfig"},
	butlerv1alpha1.ProviderTypeNutanix:   {"username", "password"},
	butlerv1alpha1.ProviderTypeProxmox:   {"username", "password"},
	butlerv1alpha1.ProviderTypeAzure:     {"clientID", "clientSecret", "tenantID"},
	butlerv1alpha1.ProviderTypeAWS:       {"accessKeyID", "secretAccessKey"},
	butlerv1alpha1.ProviderTypeGCP:       {"serviceAccountKey"},
}

// Reconciler reconciles a ProviderConfig object.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=providerconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=providerconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=providerconfigs/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=networkpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=teams,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pc := &butlerv1alpha1.ProviderConfig{}
	if err := r.Get(ctx, req.NamespacedName, pc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("reconciling ProviderConfig", "name", pc.Name, "provider", pc.Spec.Provider)

	// Handle deletion
	if !pc.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, pc)
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig) {
		controllerutil.AddFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig)
		if err := r.Update(ctx, pc); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Validate credentials
	credentialsValid := r.validateCredentials(ctx, pc)

	// Validate scope if team-scoped
	if pc.Spec.Scope != nil && pc.Spec.Scope.Type == butlerv1alpha1.ProviderConfigScopeTeam {
		r.validateTeamScope(ctx, pc)
	}

	// Check pool availability if IPAM mode
	if pc.Spec.Network != nil && pc.Spec.Network.Mode == "ipam" {
		r.checkPoolAvailability(ctx, pc)
	}

	// Set overall ready status
	now := metav1.Now()
	pc.Status.Ready = credentialsValid
	pc.Status.LastProbeTime = &now

	if err := r.Status().Update(ctx, pc); err != nil {
		return ctrl.Result{}, err
	}

	// Update Prometheus metrics
	scope := "platform"
	if pc.Spec.Scope != nil && pc.Spec.Scope.Type != "" {
		scope = string(pc.Spec.Scope.Type)
	}
	readyVal := float64(0)
	if credentialsValid {
		readyVal = 1
	}
	providerReady.WithLabelValues(pc.Name, pc.Namespace, string(pc.Spec.Provider), scope).Set(readyVal)

	if pc.Status.Capacity != nil {
		providerAvailableIPs.WithLabelValues(pc.Name, pc.Namespace).Set(float64(pc.Status.Capacity.AvailableIPs))
		providerEstimatedTenants.WithLabelValues(pc.Name, pc.Namespace).Set(float64(pc.Status.Capacity.EstimatedTenants))
	}

	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

func (r *Reconciler) handleDeletion(ctx context.Context, pc *butlerv1alpha1.ProviderConfig) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig) {
		return ctrl.Result{}, nil
	}

	// Check for TenantClusters referencing this provider
	tcList := &butlerv1alpha1.TenantClusterList{}
	if err := r.List(ctx, tcList); err != nil {
		return ctrl.Result{}, err
	}

	refCount := 0
	for _, tc := range tcList.Items {
		if tc.Spec.ProviderConfigRef != nil && tc.Spec.ProviderConfigRef.Name == pc.Name {
			refCount++
		}
	}

	if refCount > 0 {
		logger.Info("blocking ProviderConfig deletion, TenantClusters reference it", "count", refCount)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	latest := &butlerv1alpha1.ProviderConfig{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pc), latest); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if controllerutil.ContainsFinalizer(latest, butlerv1alpha1.FinalizerProviderConfig) {
		controllerutil.RemoveFinalizer(latest, butlerv1alpha1.FinalizerProviderConfig)
		if err := r.Update(ctx, latest); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) validateCredentials(ctx context.Context, pc *butlerv1alpha1.ProviderConfig) bool {
	secretNS := pc.Spec.CredentialsRef.Namespace
	if secretNS == "" {
		secretNS = pc.Namespace
	}

	secret := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      pc.Spec.CredentialsRef.Name,
		Namespace: secretNS,
	}, secret)

	if err != nil {
		msg := fmt.Sprintf("credentials secret not found: %v", err)
		if apierrors.IsNotFound(err) {
			msg = fmt.Sprintf("credentials secret %s/%s not found", secretNS, pc.Spec.CredentialsRef.Name)
		}
		meta.SetStatusCondition(&pc.Status.Conditions, metav1.Condition{
			Type:               ConditionCredentialsValid,
			Status:             metav1.ConditionFalse,
			Reason:             butlerv1alpha1.ReasonCredentialsInvalid,
			Message:            msg,
			ObservedGeneration: pc.Generation,
		})
		pc.Status.Validated = false
		r.Recorder.Eventf(pc, "Warning", "CredentialsInvalid", "%s", msg)
		return false
	}

	// Check expected keys
	keys := expectedSecretKeys[pc.Spec.Provider]
	var missing []string
	for _, key := range keys {
		if _, ok := secret.Data[key]; !ok {
			missing = append(missing, key)
		}
	}

	if len(missing) > 0 {
		msg := fmt.Sprintf("secret missing required keys: %v", missing)
		meta.SetStatusCondition(&pc.Status.Conditions, metav1.Condition{
			Type:               ConditionCredentialsValid,
			Status:             metav1.ConditionFalse,
			Reason:             butlerv1alpha1.ReasonCredentialsInvalid,
			Message:            msg,
			ObservedGeneration: pc.Generation,
		})
		pc.Status.Validated = false
		r.Recorder.Eventf(pc, "Warning", "CredentialsInvalid", "%s", msg)
		return false
	}

	now := metav1.Now()
	meta.SetStatusCondition(&pc.Status.Conditions, metav1.Condition{
		Type:               ConditionCredentialsValid,
		Status:             metav1.ConditionTrue,
		Reason:             butlerv1alpha1.ReasonReady,
		Message:            "credentials validated",
		ObservedGeneration: pc.Generation,
	})
	pc.Status.Validated = true
	pc.Status.LastValidationTime = &now
	return true
}

func (r *Reconciler) validateTeamScope(ctx context.Context, pc *butlerv1alpha1.ProviderConfig) {
	if pc.Spec.Scope.TeamRef == nil {
		meta.SetStatusCondition(&pc.Status.Conditions, metav1.Condition{
			Type:               butlerv1alpha1.ConditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             butlerv1alpha1.ReasonValidationFailed,
			Message:            "scope.type is 'team' but teamRef is not set",
			ObservedGeneration: pc.Generation,
		})
		return
	}

	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, types.NamespacedName{Name: pc.Spec.Scope.TeamRef.Name}, team); err != nil {
		meta.SetStatusCondition(&pc.Status.Conditions, metav1.Condition{
			Type:               butlerv1alpha1.ConditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             butlerv1alpha1.ReasonValidationFailed,
			Message:            fmt.Sprintf("referenced team %q not found", pc.Spec.Scope.TeamRef.Name),
			ObservedGeneration: pc.Generation,
		})
	}
}

func (r *Reconciler) checkPoolAvailability(ctx context.Context, pc *butlerv1alpha1.ProviderConfig) {
	if len(pc.Spec.Network.PoolRefs) == 0 {
		meta.SetStatusCondition(&pc.Status.Conditions, metav1.Condition{
			Type:               ConditionPoolAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             butlerv1alpha1.ReasonValidationFailed,
			Message:            "network.mode is 'ipam' but no poolRefs configured",
			ObservedGeneration: pc.Generation,
		})
		return
	}

	totalAvailable := int32(0)
	for _, poolRef := range pc.Spec.Network.PoolRefs {
		pool := &butlerv1alpha1.NetworkPool{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      poolRef.Name,
			Namespace: pc.Namespace,
		}, pool); err != nil {
			continue
		}
		totalAvailable += pool.Status.AvailableIPs
	}

	// Estimate capacity
	nodesPerTenant := int32(5)
	lbPerTenant := int32(8)
	if pc.Spec.Network.LoadBalancer != nil && pc.Spec.Network.LoadBalancer.DefaultPoolSize != nil {
		lbPerTenant = *pc.Spec.Network.LoadBalancer.DefaultPoolSize
	}
	perTenant := nodesPerTenant + lbPerTenant
	estimatedTenants := int32(0)
	if perTenant > 0 {
		estimatedTenants = totalAvailable / perTenant
	}

	pc.Status.Capacity = &butlerv1alpha1.ProviderCapacity{
		AvailableIPs:     totalAvailable,
		EstimatedTenants: estimatedTenants,
	}

	if totalAvailable > 0 {
		meta.SetStatusCondition(&pc.Status.Conditions, metav1.Condition{
			Type:               ConditionPoolAvailable,
			Status:             metav1.ConditionTrue,
			Reason:             butlerv1alpha1.ReasonPoolAvailable,
			Message:            fmt.Sprintf("%d IPs available (~%d tenants)", totalAvailable, estimatedTenants),
			ObservedGeneration: pc.Generation,
		})
	} else {
		meta.SetStatusCondition(&pc.Status.Conditions, metav1.Condition{
			Type:               ConditionPoolAvailable,
			Status:             metav1.ConditionFalse,
			Reason:             butlerv1alpha1.ReasonPoolExhausted,
			Message:            "all configured pools are exhausted",
			ObservedGeneration: pc.Generation,
		})
		r.Recorder.Event(pc, "Warning", "PoolsExhausted", "all configured network pools are exhausted")
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.ProviderConfig{}).
		Watches(&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				// Find ProviderConfigs referencing this secret
				pcList := &butlerv1alpha1.ProviderConfigList{}
				if err := mgr.GetClient().List(ctx, pcList); err != nil {
					return nil
				}
				var requests []reconcile.Request
				for _, pc := range pcList.Items {
					secretNS := pc.Spec.CredentialsRef.Namespace
					if secretNS == "" {
						secretNS = pc.Namespace
					}
					if obj.GetName() == pc.Spec.CredentialsRef.Name && obj.GetNamespace() == secretNS {
						requests = append(requests, reconcile.Request{
							NamespacedName: types.NamespacedName{
								Name:      pc.Name,
								Namespace: pc.Namespace,
							},
						})
					}
				}
				return requests
			}),
		).
		Named("providerconfig").
		Complete(r)
}
