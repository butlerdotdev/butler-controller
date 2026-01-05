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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const (
	SingletonName = "butler"

	// Condition types
	ConditionConfigured     = "Configured"
	ConditionNamespaceReady = "NamespaceReady"
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

	// Count teams and clusters for status
	if err := r.reconcileStatus(ctx, config); err != nil {
		logger.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	logger.Info("ButlerConfig reconciliation complete", "name", config.Name,
		"mode", config.Spec.MultiTenancy.Mode,
		"teamCount", config.Status.TeamCount,
		"clusterCount", config.Status.ClusterCount)

	// Requeue periodically to update counts
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
		namespaceName = "butler-tenants" // Default value
	}

	// Create or update the namespace
	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespaceName,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, namespace, func() error {
		// Set labels
		if namespace.Labels == nil {
			namespace.Labels = make(map[string]string)
		}
		namespace.Labels[butlerv1alpha1.LabelManagedBy] = "butler"

		// Set annotations
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
		Named("butlerconfig").
		Complete(r)
}
