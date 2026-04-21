// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
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

// applyTeamAndEnvDefaults mutates the in-memory TenantCluster spec to fill
// unset fields from Team.spec.environments[].clusterDefaults and
// Team.spec.clusterDefaults. Env defaults win over Team defaults on the
// same field, user-set fields on the TC win over both. See ADR-009.
//
// Applied to the in-memory spec only; never written back to the cluster,
// matching the existing image-sync override pattern in the reconciler.
func (r *Reconciler) applyTeamAndEnvDefaults(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	if tc.Spec.TeamRef == nil || tc.Spec.TeamRef.Name == "" {
		return nil
	}

	logger := log.FromContext(ctx)

	team := &butlerv1alpha1.Team{}
	if err := r.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get team for defaults: %w", err)
	}

	effective := mergeClusterDefaults(team, tc.Labels[butlerv1alpha1.LabelEnvironment])
	if effective == nil {
		return nil
	}

	applyClusterDefaults(&tc.Spec, effective)
	logger.V(1).Info("applied team/env defaults", "team", team.Name, "env", tc.Labels[butlerv1alpha1.LabelEnvironment])
	return nil
}

// mergeClusterDefaults returns an effective ClusterDefaults by layering
// env.ClusterDefaults over team.ClusterDefaults. Env fields win; team
// fields fill the remaining gaps. Nil when neither side defines defaults.
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

// applyClusterDefaults fills zero-valued fields on spec from defaults.
// User-set fields on the spec are never overwritten. Schema-level
// kubebuilder defaults (applied by the API server) normally leave
// numeric fields non-zero, so in practice this function only fills
// gaps on paths that skipped the schema defaults (e.g. older clients).
func applyClusterDefaults(spec *butlerv1alpha1.TenantClusterSpec, d *butlerv1alpha1.ClusterDefaults) {
	if d == nil {
		return
	}

	if spec.KubernetesVersion == "" && d.KubernetesVersion != "" {
		spec.KubernetesVersion = d.KubernetesVersion
	}

	if spec.Workers.Replicas == 0 && d.WorkerCount != nil {
		spec.Workers.Replicas = *d.WorkerCount
	}

	mt := &spec.Workers.MachineTemplate
	if mt.CPU == 0 && d.WorkerCPU != nil {
		mt.CPU = *d.WorkerCPU
	}
	if mt.Memory.IsZero() && d.WorkerMemoryGi != nil {
		mt.Memory = resource.MustParse(fmt.Sprintf("%dGi", *d.WorkerMemoryGi))
	}
	if mt.DiskSize.IsZero() && d.WorkerDiskGi != nil {
		mt.DiskSize = resource.MustParse(fmt.Sprintf("%dGi", *d.WorkerDiskGi))
	}
}
