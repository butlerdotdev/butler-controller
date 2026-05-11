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

package user

import (
	"context"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const (
	// fieldManager is the SSA field manager identity for User.status.teams
	// and User.status.teamsResolvedAt. Distinct from butler-server's
	// "butler-server/user-auth" field manager which owns status.lastSeenGroups.
	fieldManager = "butler-controller/user-teams"
)

// Reconciler resolves team membership for User CRDs by matching each
// user's status.lastSeenGroups against Team CRD access configuration.
// Writes status.teams and status.teamsResolvedAt via SSA.
type Reconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Recorder                record.EventRecorder
	MaxConcurrentReconciles int
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=users,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=users/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=teams,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	trigger := triggerFromContext(ctx, req)

	user := &butlerv1alpha1.User{}
	if err := r.Get(ctx, req.NamespacedName, user); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !user.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	start := time.Now()

	// List all Team CRDs.
	teamList := &butlerv1alpha1.TeamList{}
	if err := r.List(ctx, teamList); err != nil {
		resolveTotal.WithLabelValues(trigger, "error").Inc()
		return ctrl.Result{}, fmt.Errorf("listing teams: %w", err)
	}

	// Resolve team memberships. Manual membership (spec.access.users) is
	// always resolved. Group-based membership (spec.access.groups) is only
	// resolved when lastSeenGroups is populated (user has logged in via SSO).
	resolved := resolveTeamMemberships(
		user.Spec.Email,
		user.Status.LastSeenGroups,
		teamList.Items,
	)

	duration := time.Since(start).Seconds()
	resolveDuration.WithLabelValues(trigger).Observe(duration)
	resolveTotal.WithLabelValues(trigger, "success").Inc()

	// Conditional write: skip the patch when resolved teams match the
	// current status. This prevents unnecessary writes and ensures
	// teamsResolvedAt only advances when membership actually changes.
	if teamMembershipsEqual(resolved, user.Status.Teams) {
		writeTotal.WithLabelValues("noop").Inc()
		logger.V(1).Info("team membership unchanged, skipping write",
			"user", user.Name,
			"email", user.Spec.Email,
			"teams", formatMemberships(resolved),
			"trigger", trigger,
		)
		return ctrl.Result{}, nil
	}

	// Build the SSA patch with only the fields this controller owns.
	now := metav1.Now()
	patch := &butlerv1alpha1.User{
		TypeMeta: metav1.TypeMeta{
			APIVersion: butlerv1alpha1.GroupVersion.String(),
			Kind:       "User",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: user.Name,
		},
	}
	patch.Status.Teams = resolved
	patch.Status.TeamsResolvedAt = &now

	if err := r.Status().Patch(ctx, patch, client.Apply,
		client.FieldOwner(fieldManager),
		client.ForceOwnership,
	); err != nil {
		writeTotal.WithLabelValues("error").Inc()
		return ctrl.Result{}, fmt.Errorf("patching user status: %w", err)
	}

	writeTotal.WithLabelValues("success").Inc()

	// Emit a Kubernetes event for audit trail.
	r.Recorder.Eventf(user, "Normal", "TeamMembershipResolved",
		"Resolved team membership: [%s]. Trigger: %s. Previous: [%s]. Groups matched: [%s]",
		formatMemberships(resolved),
		trigger,
		formatMemberships(user.Status.Teams),
		strings.Join(user.Status.LastSeenGroups, ", "),
	)

	logger.Info("updated team membership",
		"user", user.Name,
		"email", user.Spec.Email,
		"teams", formatMemberships(resolved),
		"previous", formatMemberships(user.Status.Teams),
		"trigger", trigger,
	)

	return ctrl.Result{}, nil
}

// SetupWithManager registers the controller with the manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	maxConcurrent := r.MaxConcurrentReconciles
	if maxConcurrent <= 0 {
		maxConcurrent = 5
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.User{}).
		Watches(&butlerv1alpha1.Team{},
			handler.EnqueueRequestsFromMapFunc(r.mapTeamToUsers),
		).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrent}).
		Named("user-teams").
		Complete(r)
}

// mapTeamToUsers enqueues all User CRDs when a Team CRD changes. The
// reconciler then re-resolves each user's membership against current Team
// state. Stale entries are corrected naturally because the resolution
// function is pure: same inputs always produce the same output.
//
// Optimization note: at scale (>1000 users), this mapper can be refined
// to use a field indexer on status.lastSeenGroups to only enqueue users
// whose groups intersect with the changed Team's configured groups. At
// current scale (<100 users), enqueue-all completes in <1s and the
// complexity is not justified.
func (r *Reconciler) mapTeamToUsers(ctx context.Context, _ client.Object) []reconcile.Request {
	userList := &butlerv1alpha1.UserList{}
	if err := r.List(ctx, userList); err != nil {
		log.FromContext(ctx).Error(err, "failed to list Users for Team mapper")
		return nil
	}

	requests := make([]reconcile.Request, len(userList.Items))
	for i, user := range userList.Items {
		requests[i] = reconcile.Request{
			NamespacedName: client.ObjectKey{Name: user.Name},
		}
	}
	return requests
}

// triggerFromContext determines the reconciliation trigger for audit and
// metric labeling. When a Team CRD changes and the mapper enqueues users,
// the reconcile request is indistinguishable from a direct User CRD watch
// trigger at the Reconcile() level. Both produce the same correct result
// because the resolution function is pure. The trigger classification
// here is best-effort for observability, not a correctness concern.
func triggerFromContext(_ context.Context, _ ctrl.Request) string {
	// controller-runtime does not propagate the source of the reconcile
	// request into the context. Default to "resync" as the catch-all.
	// Login-driven triggers (lastSeenGroups change) and team-change
	// triggers (Team CRD change) both appear as "resync" at this level.
	// A future refinement could use annotations or controller-runtime
	// source tracking to distinguish these, but the correctness of the
	// resolution is not affected.
	return "resync"
}

// formatMemberships produces a human-readable string of team memberships
// for log messages and audit events.
func formatMemberships(memberships []butlerv1alpha1.UserTeamMembership) string {
	parts := make([]string, len(memberships))
	for i, m := range memberships {
		parts[i] = fmt.Sprintf("%s(%s)", m.Name, m.Role)
	}
	return strings.Join(parts, ", ")
}

// RecordUserMetrics updates the population gauges and stale_age metric.
// Called periodically by the controller (e.g., via a separate goroutine
// or as part of a reconcile pass that processes all users).
func RecordUserMetrics(users []butlerv1alpha1.User) {
	var total, sso, internal, withGroups, withoutGroups int
	var maxStale float64

	now := time.Now()

	for i := range users {
		u := &users[i]
		total++

		if u.IsSSO() {
			sso++
		} else {
			internal++
		}

		if len(u.Status.LastSeenGroups) > 0 {
			withGroups++
		} else {
			withoutGroups++
		}

		// Compute stale_age: time since last successful teams resolution.
		// Falls back to creationTimestamp for never-resolved users, producing
		// worst-case stale_age that surfaces in monitoring alerts.
		var resolvedAt time.Time
		if u.Status.TeamsResolvedAt != nil {
			resolvedAt = u.Status.TeamsResolvedAt.Time
		} else {
			resolvedAt = u.CreationTimestamp.Time
		}
		age := now.Sub(resolvedAt).Seconds()
		if age > maxStale {
			maxStale = age
		}
	}

	userCount.WithLabelValues("total").Set(float64(total))
	userCount.WithLabelValues("sso").Set(float64(sso))
	userCount.WithLabelValues("internal").Set(float64(internal))
	userCount.WithLabelValues("with_groups").Set(float64(withGroups))
	userCount.WithLabelValues("without_groups").Set(float64(withoutGroups))
	staleAge.Set(maxStale)
}
