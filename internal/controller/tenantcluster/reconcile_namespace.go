// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

func (r *Reconciler) reconcileTeamRoleBinding(ctx context.Context, tc *butlerv1alpha1.TenantCluster, nsName string) error {
	logger := log.FromContext(ctx)

	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
		return err
	}

	rbName := fmt.Sprintf("butler-team-%s-access", team.Name)

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: rbName, Namespace: nsName},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, rb, func() error {
		if rb.Labels == nil {
			rb.Labels = make(map[string]string)
		}
		rb.Labels[butlerv1alpha1.LabelManagedBy] = "butler"
		rb.Labels[butlerv1alpha1.LabelTeam] = team.Name
		rb.Labels[butlerv1alpha1.LabelTenant] = tc.Name

		rb.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "admin",
		}

		var subjects []rbacv1.Subject
		for _, user := range team.Spec.Access.Users {
			subjects = append(subjects, rbacv1.Subject{
				Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: user.Name,
			})
		}
		for _, group := range team.Spec.Access.Groups {
			subjects = append(subjects, rbacv1.Subject{
				Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: group.Name,
			})
		}
		rb.Subjects = subjects
		return nil
	})

	if err != nil {
		return err
	}

	if op != controllerutil.OperationResultNone {
		logger.Info("team RoleBinding reconciled", "name", rbName, "namespace", nsName, "operation", op)
	}

	return nil
}
