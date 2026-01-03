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

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// Reconciler reconciles a Team object.
//
// Team represents a group of users who share access to TenantClusters.
// When a Team is created, this controller:
// 1. Creates a namespace with the same name as the Team
// 2. Creates RoleBindings for users and groups defined in Team.spec.access
// 3. Updates Team status with namespace name and phase
//
// When a Team is deleted (with finalizer):
// 1. Ensures all TenantClusters in the namespace are deleted
// 2. Deletes the namespace
// 3. Removes the finalizer
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=teams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=teams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=teams/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters,verbs=get;list;watch

// Reconcile handles Team reconciliation.
//
// The reconciliation loop:
// 1. Check if Team is being deleted (has deletionTimestamp)
//   - If yes, run cleanup logic and remove finalizer
//
// 2. Add finalizer if not present
// 3. Create namespace if it doesn't exist
// 4. Create/update RoleBindings for access control
// 5. Count TenantClusters in namespace
// 6. Update status (phase, namespace, cluster count)
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Team instance
	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, req.NamespacedName, team); err != nil {
		if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "unable to fetch Team")
			return ctrl.Result{}, err
		}
		// Team not found - likely deleted
		return ctrl.Result{}, nil
	}

	logger.Info("reconciling Team",
		"name", team.Name,
		"phase", team.Status.Phase,
		"displayName", team.Spec.DisplayName)

	// Handle deletion
	if !team.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, team)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(team, butlerv1alpha1.FinalizerTeam) {
		controllerutil.AddFinalizer(team, butlerv1alpha1.FinalizerTeam)
		if err := r.Update(ctx, team); err != nil {
			logger.Error(err, "failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// TODO: Implement reconciliation logic
	// 1. Ensure namespace exists
	// 2. Create/update RoleBindings
	// 3. Count TenantClusters
	// 4. Update status

	return ctrl.Result{}, nil
}

// handleDeletion handles Team cleanup when deleted.
func (r *Reconciler) handleDeletion(ctx context.Context, team *butlerv1alpha1.Team) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("handling Team deletion", "name", team.Name)

	// TODO: Implement deletion logic
	// 1. Check for remaining TenantClusters
	// 2. Delete namespace
	// 3. Remove finalizer

	// Remove finalizer to allow deletion
	controllerutil.RemoveFinalizer(team, butlerv1alpha1.FinalizerTeam)
	if err := r.Update(ctx, team); err != nil {
		logger.Error(err, "failed to remove finalizer")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
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
