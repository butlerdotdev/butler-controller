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

package team

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const (
	// roleBindingPrefix is the prefix for RoleBinding names created by this controller.
	roleBindingPrefix = "butler-team-"

	// clusterRoleAdmin is the ClusterRole for admin users.
	clusterRoleAdmin = "cluster-admin"

	// clusterRoleMember is the ClusterRole for member users.
	clusterRoleMember = "edit"

	// requeueInterval is the default requeue interval for status updates.
	requeueInterval = 30 * time.Second
)

// Reconciler reconciles a Team object.
//
// The TeamReconciler is responsible for:
// - Creating a namespace for each Team (namespace name = Team name)
// - Creating RoleBindings for users and groups defined in the Team
// - Tracking TenantCluster count and resource usage
// - Preventing deletion while TenantClusters exist
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=teams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=teams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=teams/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete

// Reconcile is the main reconciliation loop for Team resources.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Team instance
	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, req.NamespacedName, team); err != nil {
		if apierrors.IsNotFound(err) {
			// Team was deleted before we could process it
			logger.V(1).Info("Team not found, likely deleted")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "unable to fetch Team")
		return ctrl.Result{}, err
	}

	logger.Info("reconciling Team", "name", team.Name, "phase", team.Status.Phase)

	// Handle deletion
	if !team.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, team)
	}

	// Ensure finalizer is present
	if !controllerutil.ContainsFinalizer(team, butlerv1alpha1.FinalizerTeam) {
		logger.V(1).Info("adding finalizer")
		controllerutil.AddFinalizer(team, butlerv1alpha1.FinalizerTeam)
		if err := r.Update(ctx, team); err != nil {
			logger.Error(err, "failed to add finalizer")
			return ctrl.Result{}, err
		}
		// Requeue to continue reconciliation with finalizer in place
		return ctrl.Result{Requeue: true}, nil
	}

	// Set initial phase if not set
	if team.Status.Phase == "" {
		team.Status.Phase = butlerv1alpha1.TeamPhasePending
		if err := r.Status().Update(ctx, team); err != nil {
			logger.Error(err, "failed to set initial phase")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Reconcile namespace
	if err := r.reconcileNamespace(ctx, team); err != nil {
		return r.setFailedCondition(ctx, team, "NamespaceReconcileFailed", err)
	}

	// Reconcile RBAC
	if err := r.reconcileRBAC(ctx, team); err != nil {
		return r.setFailedCondition(ctx, team, "RBACReconcileFailed", err)
	}

	// Update status with cluster count and resource usage
	if err := r.reconcileStatus(ctx, team); err != nil {
		logger.Error(err, "failed to update status")
		return ctrl.Result{}, err
	}

	logger.Info("Team reconciliation complete", "name", team.Name, "phase", team.Status.Phase)

	// Requeue periodically to refresh cluster counts
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// reconcileDelete handles Team deletion.
func (r *Reconciler) reconcileDelete(ctx context.Context, team *butlerv1alpha1.Team) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling Team deletion", "name", team.Name)

	// Update phase to Terminating
	if team.Status.Phase != butlerv1alpha1.TeamPhaseTerminating {
		team.Status.Phase = butlerv1alpha1.TeamPhaseTerminating
		if err := r.Status().Update(ctx, team); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Check if any TenantClusters still exist in this Team's namespace
	clusterCount, err := r.countTenantClusters(ctx, team.Name)
	if err != nil {
		logger.Error(err, "failed to count TenantClusters")
		return ctrl.Result{}, err
	}

	if clusterCount > 0 {
		logger.Info("cannot delete Team with existing TenantClusters",
			"name", team.Name, "clusterCount", clusterCount)

		// Set condition to indicate why deletion is blocked
		meta.SetStatusCondition(&team.Status.Conditions, metav1.Condition{
			Type:    butlerv1alpha1.TeamConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  "DeletionBlocked",
			Message: fmt.Sprintf("Cannot delete Team: %d TenantCluster(s) still exist", clusterCount),
		})
		if err := r.Status().Update(ctx, team); err != nil {
			return ctrl.Result{}, err
		}

		// Requeue to check again
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Delete the namespace (which will cascade delete RoleBindings)
	namespace := &corev1.Namespace{}
	err = r.Get(ctx, types.NamespacedName{Name: team.Name}, namespace)
	if err == nil {
		// Namespace exists
		if namespace.DeletionTimestamp.IsZero() {
			// Not being deleted yet, delete it
			logger.Info("deleting Team namespace", "namespace", team.Name)
			if err := r.Delete(ctx, namespace); err != nil && !apierrors.IsNotFound(err) {
				logger.Error(err, "failed to delete namespace")
				return ctrl.Result{}, err
			}
			// Requeue to wait for namespace deletion to start
			return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
		}
		// Namespace is terminating, wait for it to be fully deleted
		logger.V(1).Info("waiting for namespace to be deleted", "namespace", team.Name)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	} else if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	// Namespace is gone, remove finalizer
	logger.Info("removing finalizer", "name", team.Name)
	controllerutil.RemoveFinalizer(team, butlerv1alpha1.FinalizerTeam)
	if err := r.Update(ctx, team); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("Team deletion complete", "name", team.Name)
	return ctrl.Result{}, nil
}

// reconcileNamespace ensures the Team's namespace exists with proper labels.
func (r *Reconciler) reconcileNamespace(ctx context.Context, team *butlerv1alpha1.Team) error {
	logger := log.FromContext(ctx)

	namespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: team.Name,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, namespace, func() error {
		// Set labels
		if namespace.Labels == nil {
			namespace.Labels = make(map[string]string)
		}
		namespace.Labels[butlerv1alpha1.LabelTeam] = team.Name
		namespace.Labels[butlerv1alpha1.LabelManagedBy] = "butler"

		// Set annotations
		if namespace.Annotations == nil {
			namespace.Annotations = make(map[string]string)
		}
		if team.Spec.Description != "" {
			namespace.Annotations[butlerv1alpha1.AnnotationDescription] = team.Spec.Description
		}

		return nil
	})

	if err != nil {
		logger.Error(err, "failed to reconcile namespace")
		meta.SetStatusCondition(&team.Status.Conditions, metav1.Condition{
			Type:    butlerv1alpha1.TeamConditionNamespaceReady,
			Status:  metav1.ConditionFalse,
			Reason:  butlerv1alpha1.ReasonFailed,
			Message: fmt.Sprintf("Failed to create namespace: %v", err),
		})
		return err
	}

	if op != controllerutil.OperationResultNone {
		logger.Info("namespace reconciled", "namespace", team.Name, "operation", op)
	}

	// Update status with namespace
	team.Status.Namespace = team.Name

	// Set condition
	meta.SetStatusCondition(&team.Status.Conditions, metav1.Condition{
		Type:    butlerv1alpha1.TeamConditionNamespaceReady,
		Status:  metav1.ConditionTrue,
		Reason:  butlerv1alpha1.ReasonReady,
		Message: "Namespace exists and is configured",
	})

	return nil
}

// reconcileRBAC ensures RoleBindings exist for all users and groups.
func (r *Reconciler) reconcileRBAC(ctx context.Context, team *butlerv1alpha1.Team) error {
	logger := log.FromContext(ctx)

	// Track which RoleBindings we expect to exist
	expectedBindings := make(map[string]bool)

	// Create RoleBindings for users
	for _, user := range team.Spec.Access.Users {
		bindingName := fmt.Sprintf("%suser-%s", roleBindingPrefix, sanitizeName(user.Name))
		expectedBindings[bindingName] = true

		if err := r.ensureRoleBinding(ctx, team, bindingName, rbacv1.UserKind, user.Name, user.Role); err != nil {
			return err
		}
	}

	// Create RoleBindings for groups
	for _, group := range team.Spec.Access.Groups {
		bindingName := fmt.Sprintf("%sgroup-%s", roleBindingPrefix, sanitizeName(group.Name))
		expectedBindings[bindingName] = true

		if err := r.ensureRoleBinding(ctx, team, bindingName, rbacv1.GroupKind, group.Name, group.Role); err != nil {
			return err
		}
	}

	// Clean up stale RoleBindings
	if err := r.cleanupStaleRoleBindings(ctx, team, expectedBindings); err != nil {
		logger.Error(err, "failed to cleanup stale RoleBindings")
		// Non-fatal, continue
	}

	// Set condition
	meta.SetStatusCondition(&team.Status.Conditions, metav1.Condition{
		Type:   butlerv1alpha1.TeamConditionRBACReady,
		Status: metav1.ConditionTrue,
		Reason: butlerv1alpha1.ReasonReady,
		Message: fmt.Sprintf("RBAC configured: %d user(s), %d group(s)",
			len(team.Spec.Access.Users), len(team.Spec.Access.Groups)),
	})

	return nil
}

// ensureRoleBinding creates or updates a RoleBinding for a user or group.
func (r *Reconciler) ensureRoleBinding(
	ctx context.Context,
	team *butlerv1alpha1.Team,
	name string,
	subjectKind string,
	subjectName string,
	role butlerv1alpha1.TeamRole,
) error {
	logger := log.FromContext(ctx)

	// Determine ClusterRole based on TeamRole
	clusterRole := clusterRoleMember
	if role == butlerv1alpha1.TeamRoleAdmin {
		clusterRole = clusterRoleAdmin
	}

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: team.Name,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, rb, func() error {
		// Set labels
		if rb.Labels == nil {
			rb.Labels = make(map[string]string)
		}
		rb.Labels[butlerv1alpha1.LabelTeam] = team.Name
		rb.Labels[butlerv1alpha1.LabelManagedBy] = "butler"

		// Set subjects
		rb.Subjects = []rbacv1.Subject{
			{
				Kind:     subjectKind,
				Name:     subjectName,
				APIGroup: rbacv1.GroupName,
			},
		}

		// Set role reference
		rb.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     clusterRole,
		}

		return nil
	})

	if err != nil {
		logger.Error(err, "failed to reconcile RoleBinding",
			"name", name, "subject", subjectName, "role", role)
		return err
	}

	if op != controllerutil.OperationResultNone {
		logger.V(1).Info("RoleBinding reconciled",
			"name", name, "subject", subjectName, "role", role, "operation", op)
	}

	return nil
}

// cleanupStaleRoleBindings removes RoleBindings that are no longer needed.
func (r *Reconciler) cleanupStaleRoleBindings(
	ctx context.Context,
	team *butlerv1alpha1.Team,
	expectedBindings map[string]bool,
) error {
	logger := log.FromContext(ctx)

	// List all RoleBindings in the namespace managed by Butler
	rbList := &rbacv1.RoleBindingList{}
	if err := r.List(ctx, rbList,
		client.InNamespace(team.Name),
		client.MatchingLabels{
			butlerv1alpha1.LabelManagedBy: "butler",
			butlerv1alpha1.LabelTeam:      team.Name,
		},
	); err != nil {
		return err
	}

	// Delete RoleBindings that are no longer expected
	for _, rb := range rbList.Items {
		if !expectedBindings[rb.Name] {
			logger.Info("deleting stale RoleBinding", "name", rb.Name)
			if err := r.Delete(ctx, &rb); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	return nil
}

// reconcileStatus updates the Team status with cluster count and resource usage.
func (r *Reconciler) reconcileStatus(ctx context.Context, team *butlerv1alpha1.Team) error {
	// Count TenantClusters
	clusterCount, err := r.countTenantClusters(ctx, team.Name)
	if err != nil {
		return err
	}
	team.Status.ClusterCount = clusterCount

	// Calculate resource usage
	resourceUsage, err := r.calculateResourceUsage(ctx, team.Name)
	if err != nil {
		return err
	}
	team.Status.ResourceUsage = resourceUsage

	// Update observed generation
	team.Status.ObservedGeneration = team.Generation

	// Set overall Ready condition
	namespaceReady := meta.IsStatusConditionTrue(team.Status.Conditions, butlerv1alpha1.TeamConditionNamespaceReady)
	rbacReady := meta.IsStatusConditionTrue(team.Status.Conditions, butlerv1alpha1.TeamConditionRBACReady)

	if namespaceReady && rbacReady {
		team.Status.Phase = butlerv1alpha1.TeamPhaseReady
		meta.SetStatusCondition(&team.Status.Conditions, metav1.Condition{
			Type:    butlerv1alpha1.TeamConditionReady,
			Status:  metav1.ConditionTrue,
			Reason:  butlerv1alpha1.ReasonReady,
			Message: "Team is ready",
		})
	} else {
		team.Status.Phase = butlerv1alpha1.TeamPhasePending
		meta.SetStatusCondition(&team.Status.Conditions, metav1.Condition{
			Type:    butlerv1alpha1.TeamConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  butlerv1alpha1.ReasonReconciling,
			Message: "Team is not yet ready",
		})
	}

	return r.Status().Update(ctx, team)
}

// countTenantClusters returns the number of TenantClusters in the Team's namespace.
func (r *Reconciler) countTenantClusters(ctx context.Context, namespace string) (int32, error) {
	clusterList := &butlerv1alpha1.TenantClusterList{}
	if err := r.List(ctx, clusterList, client.InNamespace(namespace)); err != nil {
		return 0, err
	}
	return int32(len(clusterList.Items)), nil
}

// calculateResourceUsage calculates the total resource usage for all TenantClusters.
func (r *Reconciler) calculateResourceUsage(ctx context.Context, namespace string) (*butlerv1alpha1.ResourceUsage, error) {
	clusterList := &butlerv1alpha1.TenantClusterList{}
	if err := r.List(ctx, clusterList, client.InNamespace(namespace)); err != nil {
		return nil, err
	}

	usage := &butlerv1alpha1.ResourceUsage{
		Clusters: int32(len(clusterList.Items)),
	}

	var totalWorkers int32
	for _, cluster := range clusterList.Items {
		totalWorkers += cluster.Spec.Workers.Replicas
		// TODO: Calculate CPU and Memory from MachineTemplate when needed
	}
	usage.Workers = totalWorkers

	return usage, nil
}

// setFailedCondition sets a failed condition and returns an appropriate result.
func (r *Reconciler) setFailedCondition(
	ctx context.Context,
	team *butlerv1alpha1.Team,
	reason string,
	err error,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "reconciliation failed", "reason", reason)

	team.Status.Phase = butlerv1alpha1.TeamPhaseFailed
	meta.SetStatusCondition(&team.Status.Conditions, metav1.Condition{
		Type:    butlerv1alpha1.TeamConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: err.Error(),
	})

	if updateErr := r.Status().Update(ctx, team); updateErr != nil {
		logger.Error(updateErr, "failed to update status after error")
	}

	// Return error to trigger backoff retry
	return ctrl.Result{}, err
}

// sanitizeName converts a name to a valid Kubernetes resource name component.
// It handles email addresses, AD group DNs, etc.
func sanitizeName(name string) string {
	// Simple sanitization: replace invalid characters with hyphens
	// and truncate to a reasonable length
	result := make([]byte, 0, len(name))
	for _, c := range []byte(name) {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			result = append(result, c)
		} else if c >= 'A' && c <= 'Z' {
			result = append(result, c+32) // lowercase
		} else {
			result = append(result, '-')
		}
	}

	// Remove leading/trailing hyphens and collapse multiple hyphens
	s := string(result)
	for len(s) > 0 && s[0] == '-' {
		s = s[1:]
	}
	for len(s) > 0 && s[len(s)-1] == '-' {
		s = s[:len(s)-1]
	}

	// Truncate to 63 chars (Kubernetes limit)
	if len(s) > 63 {
		s = s[:63]
	}

	// Ensure it doesn't end with a hyphen after truncation
	for len(s) > 0 && s[len(s)-1] == '-' {
		s = s[:len(s)-1]
	}

	return s
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.Team{}).
		Owns(&corev1.Namespace{}).
		Owns(&rbacv1.RoleBinding{}).
		Named("team").
		Complete(r)
}
