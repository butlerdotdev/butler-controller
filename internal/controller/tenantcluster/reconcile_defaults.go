// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// promoteOwnerAnnotation copies the butler.butlerlabs.dev/creator-email
// annotation (set by butler-server on creates it handles) to the
// butler.butlerlabs.dev/owner annotation. The owner annotation is the
// authoritative source for per-member cap accounting and CAPI resource
// ownership tracking; the creator-email annotation is the raw input,
// and the owner annotation is what downstream code reads. Stored as
// annotations rather than labels because email addresses contain "@",
// which is not valid in a Kubernetes label value. No-op when the
// creator-email annotation is absent or the owner annotation is
// already set to the same value.
func (r *Reconciler) promoteOwnerAnnotation(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	if tc.Annotations == nil {
		return nil
	}
	email := tc.Annotations[butlerv1alpha1.AnnotationCreatorEmail]
	if email == "" {
		return nil
	}
	if tc.Annotations[butlerv1alpha1.AnnotationOwner] == email {
		return nil
	}
	tc.Annotations[butlerv1alpha1.AnnotationOwner] = email
	if err := r.Update(ctx, tc); err != nil {
		return fmt.Errorf("failed to set owner annotation: %w", err)
	}
	log.FromContext(ctx).V(1).Info("promoted owner annotation from creator-email", "owner", email)
	return nil
}

// resolveTeamAndEnvDefaults returns the effective ClusterDefaults for a
// TenantCluster by layering the env entry's defaults over the team-level
// defaults. Returns nil when neither side defines defaults or when the
// referenced team is missing or unreachable (treat as no defaults, not
// an error, to avoid blocking reconciliation on a transient cache miss).
//
// Application note: ClusterDefaults is declarative-only in v1. The
// TenantClusterSpec fields that ClusterDefaults targets all carry
// kubebuilder defaults (CPU=4, Memory=16Gi, DiskSize=100Gi) or are
// +kubebuilder:validation:Required (KubernetesVersion, Workers.Replicas),
// so the apiserver stamps values before the reconciler sees them.
// Consumers of this resolver are limited to logging and future paths
// that bypass the schema defaulting. A mutating webhook is the correct
// place to apply these before the apiserver stamp; tracked as a
// follow-on.
func (r *Reconciler) resolveTeamAndEnvDefaults(ctx context.Context, tc *butlerv1alpha1.TenantCluster) *butlerv1alpha1.ClusterDefaults {
	if tc.Spec.TeamRef == nil || tc.Spec.TeamRef.Name == "" {
		return nil
	}

	logger := log.FromContext(ctx)

	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
		if !apierrors.IsNotFound(err) {
			logger.V(1).Info("could not fetch team for defaults resolution", "team", tc.Spec.TeamRef.Name, "error", err)
		}
		return nil
	}

	return mergeClusterDefaults(team, tc.Labels[butlerv1alpha1.LabelEnvironment])
}

// mergeClusterDefaults returns an effective ClusterDefaults by layering
// env.ClusterDefaults over team.ClusterDefaults. Env fields win; team
// fields fill the remaining gaps. Nil when neither side defines defaults.
// Exposed as a package function (not a method) so a future mutating
// webhook can call it without a reconciler instance.
func mergeClusterDefaults(team *butlerv1alpha1.Team, envName string) *butlerv1alpha1.ClusterDefaults {
	var envDefaults *butlerv1alpha1.ClusterDefaults
	if envName != "" {
		for i := range team.Spec.Environments {
			if team.Spec.Environments[i].Name == envName {
				envDefaults = team.Spec.Environments[i].ClusterDefaults
				break
			}
		}
	}

	teamDefaults := team.Spec.ClusterDefaults
	if envDefaults == nil && teamDefaults == nil {
		return nil
	}

	merged := &butlerv1alpha1.ClusterDefaults{}
	if teamDefaults != nil {
		teamDefaults.DeepCopyInto(merged)
	}
	if envDefaults == nil {
		return merged
	}

	if envDefaults.KubernetesVersion != "" {
		merged.KubernetesVersion = envDefaults.KubernetesVersion
	}
	if envDefaults.WorkerCount != nil {
		v := *envDefaults.WorkerCount
		merged.WorkerCount = &v
	}
	if envDefaults.WorkerCPU != nil {
		v := *envDefaults.WorkerCPU
		merged.WorkerCPU = &v
	}
	if envDefaults.WorkerMemoryGi != nil {
		v := *envDefaults.WorkerMemoryGi
		merged.WorkerMemoryGi = &v
	}
	if envDefaults.WorkerDiskGi != nil {
		v := *envDefaults.WorkerDiskGi
		merged.WorkerDiskGi = &v
	}
	// DefaultAddons merge: append env additions onto the team set and
	// deduplicate while preserving first-seen order. Team addons come
	// first, env-only addons come after. Replace-semantics would
	// surprise operators who expect env to be additive; scalar fields
	// still use env-wins-on-conflict because lists need the combine.
	merged.DefaultAddons = mergeAddons(merged.DefaultAddons, envDefaults.DefaultAddons)
	return merged
}

// mergeAddons concatenates two addon name lists, dropping duplicates
// while preserving first-seen order. Returns nil when both inputs are
// empty so the output matches Go's zero-value idiom.
func mergeAddons(a, b []string) []string {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, x := range a {
		if _, ok := seen[x]; ok {
			continue
		}
		seen[x] = struct{}{}
		out = append(out, x)
	}
	for _, x := range b {
		if _, ok := seen[x]; ok {
			continue
		}
		seen[x] = struct{}{}
		out = append(out, x)
	}
	return out
}
