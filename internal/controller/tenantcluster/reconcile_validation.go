// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// ownerIdentity returns the authoritative owner email for a
// TenantCluster. AnnotationOwner (set by the controller) is preferred;
// AnnotationCreatorEmail (set by butler-server) is a fallback that
// closes the race between webhook admission and controller reconcile
// when sibling counts would otherwise miss a just-created cluster.
func ownerIdentity(tc *butlerv1alpha1.TenantCluster) string {
	if tc.Annotations == nil {
		return ""
	}
	if v := tc.Annotations[butlerv1alpha1.AnnotationOwner]; v != "" {
		return v
	}
	return tc.Annotations[butlerv1alpha1.AnnotationCreatorEmail]
}

func (r *Reconciler) validateTenantCluster(ctx context.Context, tc *butlerv1alpha1.TenantCluster, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)
	mode := config.Spec.MultiTenancy.Mode

	logger.V(1).Info("validating TenantCluster", "mode", mode)

	switch mode {
	case butlerv1alpha1.MultiTenancyModeEnforced:
		return r.validateEnforcedMode(ctx, tc)
	case butlerv1alpha1.MultiTenancyModeOptional:
		return r.validateOptionalMode(ctx, tc, config)
	case butlerv1alpha1.MultiTenancyModeDisabled:
		return r.validateDisabledMode(ctx, tc, config)
	default:
		return r.validateDisabledMode(ctx, tc, config)
	}
}

func (r *Reconciler) validateEnforcedMode(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	logger := log.FromContext(ctx)

	if tc.Spec.TeamRef == nil || tc.Spec.TeamRef.Name == "" {
		return fmt.Errorf("teamRef is required when multi-tenancy mode is Enforced")
	}

	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("team %q not found", tc.Spec.TeamRef.Name)
		}
		return fmt.Errorf("failed to get team: %w", err)
	}

	if team.Status.Phase != butlerv1alpha1.TeamPhaseReady {
		return fmt.Errorf("team %q is not ready (phase: %s)", team.Name, team.Status.Phase)
	}

	expectedNamespace := team.Status.Namespace
	if expectedNamespace == "" {
		expectedNamespace = team.Name
	}

	if tc.Namespace != expectedNamespace {
		return fmt.Errorf("TenantCluster must be in team namespace %q, got %q", expectedNamespace, tc.Namespace)
	}

	logger.V(1).Info("enforced mode validation passed", "team", team.Name)
	return nil
}

func (r *Reconciler) validateOptionalMode(ctx context.Context, tc *butlerv1alpha1.TenantCluster, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	if tc.Spec.TeamRef != nil && tc.Spec.TeamRef.Name != "" {
		team := &butlerv1alpha1.Team{}
		if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("team %q not found", tc.Spec.TeamRef.Name)
			}
			return fmt.Errorf("failed to get team: %w", err)
		}

		expectedNamespace := team.Status.Namespace
		if expectedNamespace == "" {
			expectedNamespace = team.Name
		}

		if tc.Namespace != expectedNamespace {
			return fmt.Errorf("TenantCluster must be in team namespace %q when teamRef is set, got %q", expectedNamespace, tc.Namespace)
		}

		logger.V(1).Info("optional mode validation passed with team", "team", team.Name)
	} else {
		defaultNS := config.Spec.DefaultNamespace
		if defaultNS == "" {
			defaultNS = "butler-tenants"
		}

		if tc.Namespace != defaultNS {
			return fmt.Errorf("TenantCluster without teamRef must be in default namespace %q, got %q", defaultNS, tc.Namespace)
		}

		logger.V(1).Info("optional mode validation passed without team", "namespace", tc.Namespace)
	}

	return nil
}

func (r *Reconciler) validateDisabledMode(ctx context.Context, tc *butlerv1alpha1.TenantCluster, config *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	defaultNS := config.Spec.DefaultNamespace
	if defaultNS == "" {
		defaultNS = "butler-tenants"
	}

	if tc.Namespace != defaultNS {
		return fmt.Errorf("TenantCluster must be in default namespace %q when multi-tenancy is disabled, got %q", defaultNS, tc.Namespace)
	}

	logger.V(1).Info("disabled mode validation passed", "namespace", tc.Namespace)
	return nil
}

// validateProviderAccess checks if the TenantCluster's team has access to the ProviderConfig.
func (r *Reconciler) validateProviderAccess(ctx context.Context, tc *butlerv1alpha1.TenantCluster, pc *butlerv1alpha1.ProviderConfig) error {
	// Platform-scoped providers are accessible to all teams
	if pc.Spec.Scope == nil || pc.Spec.Scope.Type == "" || pc.Spec.Scope.Type == butlerv1alpha1.ProviderConfigScopePlatform {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
			metav1.ConditionTrue, butlerv1alpha1.ReasonReady, "platform-scoped provider")
		return r.validateProviderLimits(ctx, tc, pc)
	}

	// Team-scoped: verify the TC's team matches the PC's team
	if pc.Spec.Scope.TeamRef == nil {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
			metav1.ConditionFalse, butlerv1alpha1.ReasonProviderAccessDenied,
			"provider is team-scoped but has no teamRef")
		return fmt.Errorf("provider %s is team-scoped but has no teamRef", pc.Name)
	}

	if tc.Spec.TeamRef == nil {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
			metav1.ConditionFalse, butlerv1alpha1.ReasonProviderAccessDenied,
			"cluster has no teamRef but provider is team-scoped")
		return fmt.Errorf("cluster has no teamRef but provider %s is team-scoped to %s",
			pc.Name, pc.Spec.Scope.TeamRef.Name)
	}

	if tc.Spec.TeamRef.Name != pc.Spec.Scope.TeamRef.Name {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
			metav1.ConditionFalse, butlerv1alpha1.ReasonProviderAccessDenied,
			fmt.Sprintf("team %s does not have access to provider %s (scoped to %s)",
				tc.Spec.TeamRef.Name, pc.Name, pc.Spec.Scope.TeamRef.Name))
		return fmt.Errorf("team %s does not have access to provider %s (scoped to %s)",
			tc.Spec.TeamRef.Name, pc.Name, pc.Spec.Scope.TeamRef.Name)
	}

	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionProviderAccessGranted,
		metav1.ConditionTrue, butlerv1alpha1.ReasonReady,
		fmt.Sprintf("team %s has access to provider %s", tc.Spec.TeamRef.Name, pc.Name))

	// Enforce provider limits (defense-in-depth when webhooks are disabled)
	return r.validateProviderLimits(ctx, tc, pc)
}

// validateProviderLimits checks maxClustersPerTeam and maxNodesPerTeam limits from ProviderConfig,
// then checks team-level resource quotas as defense-in-depth when webhooks are disabled.
func (r *Reconciler) validateProviderLimits(ctx context.Context, tc *butlerv1alpha1.TenantCluster, pc *butlerv1alpha1.ProviderConfig) error {
	if pc.Spec.Limits != nil {
		if pc.Spec.Limits.MaxClustersPerTeam != nil {
			maxClusters := *pc.Spec.Limits.MaxClustersPerTeam
			var tcList butlerv1alpha1.TenantClusterList
			if err := r.List(ctx, &tcList, client.InNamespace(tc.Namespace)); err != nil {
				return fmt.Errorf("failed to list TenantClusters: %w", err)
			}

			var existingCount int32
			for i := range tcList.Items {
				if tcList.Items[i].Name != tc.Name {
					existingCount++
				}
			}

			if existingCount >= maxClusters {
				msg := fmt.Sprintf("team namespace %q has %d cluster(s), provider %q limits to %d",
					tc.Namespace, existingCount, pc.Name, maxClusters)
				r.setCondition(tc, butlerv1alpha1.TenantClusterConditionQuotaSatisfied,
					metav1.ConditionFalse, butlerv1alpha1.ReasonQuotaExceeded, msg)
				return fmt.Errorf("quota exceeded: %s", msg)
			}
		}

		if pc.Spec.Limits.MaxNodesPerTeam != nil {
			maxNodes := *pc.Spec.Limits.MaxNodesPerTeam
			var tcList butlerv1alpha1.TenantClusterList
			if err := r.List(ctx, &tcList, client.InNamespace(tc.Namespace)); err != nil {
				return fmt.Errorf("failed to list TenantClusters: %w", err)
			}

			totalNodes := tc.Spec.Workers.Replicas
			for i := range tcList.Items {
				if tcList.Items[i].Name != tc.Name {
					totalNodes += tcList.Items[i].Spec.Workers.Replicas
				}
			}

			if totalNodes > maxNodes {
				msg := fmt.Sprintf("total worker replicas (%d) exceeds provider %q per-team limit (%d)",
					totalNodes, pc.Name, maxNodes)
				r.setCondition(tc, butlerv1alpha1.TenantClusterConditionQuotaSatisfied,
					metav1.ConditionFalse, butlerv1alpha1.ReasonQuotaExceeded, msg)
				return fmt.Errorf("quota exceeded: %s", msg)
			}
		}
	}

	// Check team-level resource quotas (defense-in-depth when webhooks are disabled)
	if err := r.validateTeamQuotas(ctx, tc); err != nil {
		return err
	}

	// All quota paths passed: record a positive condition so operators
	// can observe the current quota state. A lingering False from a
	// prior reconcile would otherwise persist until overwritten.
	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionQuotaSatisfied,
		metav1.ConditionTrue, butlerv1alpha1.ReasonReady, "within all quotas")
	return nil
}

// validateTeamQuotas checks Team.spec.resourceLimits for maxClusters, maxTotalNodes,
// and maxNodesPerCluster. CPU/Memory/Storage are checked by the webhook.
func (r *Reconciler) validateTeamQuotas(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	if tc.Spec.TeamRef == nil {
		return nil
	}

	logger := log.FromContext(ctx)

	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
		logger.V(1).Info("could not fetch team for quota check", "team", tc.Spec.TeamRef.Name, "error", err)
		return nil // Don't block if team can't be fetched
	}

	limits := team.Spec.ResourceLimits
	if limits == nil {
		return nil
	}

	// Per-cluster node limit
	if limits.MaxNodesPerCluster != nil {
		if tc.Spec.Workers.Replicas > *limits.MaxNodesPerCluster {
			msg := fmt.Sprintf("worker replicas (%d) exceeds team %q per-cluster node limit (%d)",
				tc.Spec.Workers.Replicas, team.Name, *limits.MaxNodesPerCluster)
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionQuotaSatisfied,
				metav1.ConditionFalse, butlerv1alpha1.ReasonQuotaExceeded, msg)
			return fmt.Errorf("quota exceeded: %s", msg)
		}
	}

	var tcList butlerv1alpha1.TenantClusterList
	if err := r.List(ctx, &tcList, client.InNamespace(tc.Namespace)); err != nil {
		return fmt.Errorf("failed to list TenantClusters: %w", err)
	}

	// Cluster count limit
	if limits.MaxClusters != nil {
		var existingCount int32
		for i := range tcList.Items {
			if tcList.Items[i].Name != tc.Name {
				existingCount++
			}
		}
		if existingCount+1 > *limits.MaxClusters {
			msg := fmt.Sprintf("team %q has %d cluster(s), team quota limits to %d",
				team.Name, existingCount, *limits.MaxClusters)
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionQuotaSatisfied,
				metav1.ConditionFalse, butlerv1alpha1.ReasonQuotaExceeded, msg)
			return fmt.Errorf("quota exceeded: %s", msg)
		}
	}

	// Total nodes limit
	if limits.MaxTotalNodes != nil {
		totalNodes := tc.Spec.Workers.Replicas
		for i := range tcList.Items {
			if tcList.Items[i].Name != tc.Name {
				totalNodes += tcList.Items[i].Spec.Workers.Replicas
			}
		}
		if totalNodes > *limits.MaxTotalNodes {
			msg := fmt.Sprintf("total worker nodes (%d) would exceed team %q node quota (%d)",
				totalNodes, team.Name, *limits.MaxTotalNodes)
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionQuotaSatisfied,
				metav1.ConditionFalse, butlerv1alpha1.ReasonQuotaExceeded, msg)
			return fmt.Errorf("quota exceeded: %s", msg)
		}
	}

	return r.validateEnvironmentQuotas(ctx, tc, team, tcList.Items)
}

// validateEnvironmentQuotas applies env-scoped defense-in-depth checks
// when the TenantCluster carries a butler.butlerlabs.dev/environment
// label that matches a Team.spec.environments[] entry. Two gates per
// ADR-009 v1 scope: env MaxClusters and per-member MaxClustersPerMember.
// MaxTotalNodes is part of the embedded TeamResourceLimits shape but is
// not enforced at env scope in v1. Team-total is enforced by the caller.
func (r *Reconciler) validateEnvironmentQuotas(ctx context.Context, tc *butlerv1alpha1.TenantCluster, team *butlerv1alpha1.Team, siblings []butlerv1alpha1.TenantCluster) error {
	if len(team.Spec.Environments) == 0 {
		return nil
	}

	envName := tc.Labels[butlerv1alpha1.LabelEnvironment]
	if envName == "" {
		return nil
	}

	var env *butlerv1alpha1.EnvironmentSpec
	for i := range team.Spec.Environments {
		if team.Spec.Environments[i].Name == envName {
			env = &team.Spec.Environments[i]
			break
		}
	}
	if env == nil || env.Limits == nil {
		return nil
	}

	logger := log.FromContext(ctx)

	var envSiblings []butlerv1alpha1.TenantCluster
	for i := range siblings {
		if siblings[i].Name == tc.Name {
			continue
		}
		if siblings[i].Labels[butlerv1alpha1.LabelEnvironment] == envName {
			envSiblings = append(envSiblings, siblings[i])
		}
	}

	if env.Limits.MaxClusters != nil && *env.Limits.MaxClusters > 0 {
		if int32(len(envSiblings))+1 > *env.Limits.MaxClusters {
			msg := fmt.Sprintf("environment %q has %d cluster(s), env quota limits to %d",
				envName, len(envSiblings), *env.Limits.MaxClusters)
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionQuotaSatisfied,
				metav1.ConditionFalse, butlerv1alpha1.ReasonEnvQuotaExceeded, msg)
			return fmt.Errorf("env quota exceeded: %s", msg)
		}
	}

	if env.Limits.MaxClustersPerMember != nil && *env.Limits.MaxClustersPerMember > 0 {
		owner := ownerIdentity(tc)
		if owner == "" {
			logger.V(1).Info("per-member cap set but TenantCluster has no owner annotation; skipping cap enforcement",
				"env", envName, "tc", tc.Name)
			return nil
		}
		var ownerCount int32
		for i := range envSiblings {
			if strings.EqualFold(ownerIdentity(&envSiblings[i]), owner) {
				ownerCount++
			}
		}
		if ownerCount+1 > *env.Limits.MaxClustersPerMember {
			msg := fmt.Sprintf("member %q owns %d cluster(s) in environment %q, per-member cap is %d",
				owner, ownerCount, envName, *env.Limits.MaxClustersPerMember)
			r.setCondition(tc, butlerv1alpha1.TenantClusterConditionQuotaSatisfied,
				metav1.ConditionFalse, butlerv1alpha1.ReasonPerMemberCapExceeded, msg)
			return fmt.Errorf("per-member cap exceeded: %s", msg)
		}
	}

	return nil
}
