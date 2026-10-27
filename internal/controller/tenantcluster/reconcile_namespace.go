// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

func (r *Reconciler) reconcileTenantNamespace(ctx context.Context, tc *butlerv1alpha1.TenantCluster, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	if tc.Status.TenantNamespace == "" {
		tc.Status.TenantNamespace = generateTenantNamespace(tc)
	}

	nsName := tc.Status.TenantNamespace
	logger.Info("reconciling tenant namespace", "namespace", nsName)

	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: nsName},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, ns, func() error {
		if ns.Labels == nil {
			ns.Labels = make(map[string]string)
		}
		ns.Labels[butlerv1alpha1.LabelManagedBy] = "butler"
		ns.Labels[butlerv1alpha1.LabelTenant] = tc.Name
		ns.Labels[butlerv1alpha1.LabelSourceNamespace] = tc.Namespace
		ns.Labels[butlerv1alpha1.LabelSourceName] = tc.Name

		if tc.Spec.TeamRef != nil && tc.Spec.TeamRef.Name != "" {
			ns.Labels[butlerv1alpha1.LabelTeam] = tc.Spec.TeamRef.Name
		}

		ns.Labels["pod-security.kubernetes.io/enforce"] = "privileged"
		ns.Labels["pod-security.kubernetes.io/audit"] = "privileged"
		ns.Labels["pod-security.kubernetes.io/warn"] = "privileged"

		if ns.Annotations == nil {
			ns.Annotations = make(map[string]string)
		}
		ns.Annotations[butlerv1alpha1.AnnotationDescription] = fmt.Sprintf("Tenant namespace for TenantCluster %s/%s", tc.Namespace, tc.Name)

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to create/update tenant namespace: %w", err)
	}

	if op != controllerutil.OperationResultNone {
		logger.Info("tenant namespace reconciled", "namespace", nsName, "operation", op)
	}

	if tc.Spec.TeamRef != nil && tc.Spec.TeamRef.Name != "" {
		if err := r.reconcileTeamRoleBinding(ctx, tc, nsName); err != nil {
			return fmt.Errorf("failed to create team RoleBinding: %w", err)
		}
	}

	return nil
}

// teamRoleBindingSpec describes one per-role RoleBinding that grants team
// members access to a tenant namespace.
type teamRoleBindingSpec struct {
	role        butlerv1alpha1.TeamRole
	clusterRole string
}

// teamRoleBindingSpecs lists the per-role RoleBindings created in a tenant
// namespace. The mapping is least-privilege and mirrors the team namespace
// mapping in the team controller:
//
//   - admin    -> ClusterRole "admin" (can read the admin kubeconfig Secret)
//   - operator -> ClusterRole "edit"
//   - viewer   -> ClusterRole "view" (read-only, cannot read Secrets)
var teamRoleBindingSpecs = []teamRoleBindingSpec{
	{role: butlerv1alpha1.TeamRoleAdmin, clusterRole: "admin"},
	{role: butlerv1alpha1.TeamRoleOperator, clusterRole: "edit"},
	{role: butlerv1alpha1.TeamRoleViewer, clusterRole: "view"},
}

// normalizeTeamRole maps unknown or empty roles to viewer, matching the API
// default (+kubebuilder:default="viewer"), so an unrecognized value can never
// widen access.
func normalizeTeamRole(role butlerv1alpha1.TeamRole) butlerv1alpha1.TeamRole {
	switch role {
	case butlerv1alpha1.TeamRoleAdmin, butlerv1alpha1.TeamRoleOperator:
		return role
	default:
		return butlerv1alpha1.TeamRoleViewer
	}
}

// legacyTeamRoleBindingName is the name of the pre-least-privilege RoleBinding
// that bound every team member to ClusterRole "admin" in the tenant namespace.
func legacyTeamRoleBindingName(teamName string) string {
	return fmt.Sprintf("butler-team-%s-access", teamName)
}

// teamRoleBindingName returns the per-role RoleBinding name for a team.
func teamRoleBindingName(teamName string, role butlerv1alpha1.TeamRole) string {
	return fmt.Sprintf("butler-team-%s-%s-access", teamName, role)
}

// teamSubjectsByRole groups the team's users and groups by normalized role.
func teamSubjectsByRole(team *butlerv1alpha1.Team) map[butlerv1alpha1.TeamRole][]rbacv1.Subject {
	out := make(map[butlerv1alpha1.TeamRole][]rbacv1.Subject)
	for _, user := range team.Spec.Access.Users {
		role := normalizeTeamRole(user.Role)
		out[role] = append(out[role], rbacv1.Subject{
			Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: user.Name,
		})
	}
	for _, group := range team.Spec.Access.Groups {
		role := normalizeTeamRole(group.Role)
		out[role] = append(out[role], rbacv1.Subject{
			Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: group.Name,
		})
	}
	return out
}

// reconcileTeamRoleBinding grants team members access to the tenant namespace
// with one RoleBinding per Butler team role, each containing only the subjects
// holding that role. Bindings for roles with no subjects are removed, as is the
// legacy all-members "butler-team-<team>-access" binding.
func (r *Reconciler) reconcileTeamRoleBinding(ctx context.Context, tc *butlerv1alpha1.TenantCluster, nsName string) error {
	logger := log.FromContext(ctx)

	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
		return err
	}

	subjectsByRole := teamSubjectsByRole(team)
	expected := make(map[string]bool)

	for _, spec := range teamRoleBindingSpecs {
		rbName := teamRoleBindingName(team.Name, spec.role)
		subjects := subjectsByRole[spec.role]
		if len(subjects) == 0 {
			if err := r.deleteRoleBindingIfExists(ctx, nsName, rbName); err != nil {
				return err
			}
			continue
		}
		expected[rbName] = true
		if err := r.ensureTeamRoleBinding(ctx, tc, team, nsName, rbName, spec.clusterRole, subjects); err != nil {
			return err
		}
	}

	// Remove the legacy all-members admin binding by name (it may predate labels).
	if err := r.deleteRoleBindingIfExists(ctx, nsName, legacyTeamRoleBindingName(team.Name)); err != nil {
		return err
	}

	// Remove any other stale team bindings this controller created in the
	// tenant namespace (label-selected).
	rbList := &rbacv1.RoleBindingList{}
	if err := r.List(ctx, rbList,
		client.InNamespace(nsName),
		client.MatchingLabels{
			butlerv1alpha1.LabelManagedBy: "butler",
			butlerv1alpha1.LabelTeam:      team.Name,
			butlerv1alpha1.LabelTenant:    tc.Name,
		},
	); err != nil {
		return err
	}
	for i := range rbList.Items {
		rb := &rbList.Items[i]
		if expected[rb.Name] || !strings.HasPrefix(rb.Name, "butler-team-") {
			continue
		}
		logger.Info("deleting stale team RoleBinding", "name", rb.Name, "namespace", nsName)
		if err := r.Delete(ctx, rb); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

// ensureTeamRoleBinding creates or updates a per-role RoleBinding. RoleBinding
// roleRef is immutable, so a binding whose roleRef differs is deleted and
// recreated.
func (r *Reconciler) ensureTeamRoleBinding(
	ctx context.Context,
	tc *butlerv1alpha1.TenantCluster,
	team *butlerv1alpha1.Team,
	nsName, rbName, clusterRole string,
	subjects []rbacv1.Subject,
) error {
	logger := log.FromContext(ctx)

	roleRef := rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName,
		Kind:     "ClusterRole",
		Name:     clusterRole,
	}

	labels := map[string]string{
		butlerv1alpha1.LabelManagedBy: "butler",
		butlerv1alpha1.LabelTeam:      team.Name,
		butlerv1alpha1.LabelTenant:    tc.Name,
	}

	existing := &rbacv1.RoleBinding{}
	err := r.Get(ctx, types.NamespacedName{Name: rbName, Namespace: nsName}, existing)
	switch {
	case err == nil && existing.RoleRef == roleRef:
		needsUpdate := !equality.Semantic.DeepEqual(existing.Subjects, subjects)
		if existing.Labels == nil {
			existing.Labels = make(map[string]string)
		}
		for k, v := range labels {
			if existing.Labels[k] != v {
				existing.Labels[k] = v
				needsUpdate = true
			}
		}
		if !needsUpdate {
			return nil
		}
		existing.Subjects = subjects
		if err := r.Update(ctx, existing); err != nil {
			return err
		}
		logger.Info("team RoleBinding updated", "name", rbName, "namespace", nsName,
			"clusterRole", clusterRole, "subjects", len(subjects))
		return nil
	case err == nil:
		logger.Info("team RoleBinding roleRef changed, deleting for recreation",
			"name", rbName, "namespace", nsName, "oldRole", existing.RoleRef.Name, "newRole", clusterRole)
		if err := r.Delete(ctx, existing); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	case !apierrors.IsNotFound(err):
		return err
	}

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: rbName, Namespace: nsName, Labels: labels},
		RoleRef:    roleRef,
		Subjects:   subjects,
	}
	if err := r.Create(ctx, rb); err != nil {
		// AlreadyExists after a delete means the old object is still terminating
		// or the cache is stale; return the error so the reconcile is retried.
		return err
	}
	logger.Info("team RoleBinding created", "name", rbName, "namespace", nsName,
		"clusterRole", clusterRole, "subjects", len(subjects))
	return nil
}

// deleteRoleBindingIfExists deletes the named RoleBinding, ignoring NotFound.
func (r *Reconciler) deleteRoleBindingIfExists(ctx context.Context, nsName, name string) error {
	rb := &rbacv1.RoleBinding{}
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: nsName}, rb); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	log.FromContext(ctx).Info("deleting team RoleBinding", "name", name, "namespace", nsName)
	if err := r.Delete(ctx, rb); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
