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

// promoteOwnerLabel promotes the butler.butlerlabs.dev/creator-email
// annotation (set by butler-server on creates it handles) to the
// butler.butlerlabs.dev/owner label. The label feeds per-member cap
// accounting and CAPI resource ownership tracking. Writes back to the
// cluster when the label was missing. No-op when the annotation is
// absent (kubectl-direct creates) or the label is already set.
func (r *Reconciler) promoteOwnerLabel(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	if tc.Annotations == nil {
		return nil
	}
	email := tc.Annotations[butlerv1alpha1.AnnotationCreatorEmail]
	if email == "" {
		return nil
	}
	if tc.Labels[butlerv1alpha1.LabelOwner] == email {
		return nil
	}
	if tc.Labels == nil {
		tc.Labels = map[string]string{}
	}
	tc.Labels[butlerv1alpha1.LabelOwner] = email
	if err := r.Update(ctx, tc); err != nil {
		return fmt.Errorf("failed to set owner label: %w", err)
	}
	log.FromContext(ctx).V(1).Info("promoted owner label from creator-email annotation", "owner", email)
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
	if len(envDefaults.DefaultAddons) > 0 {
		merged.DefaultAddons = append([]string(nil), envDefaults.DefaultAddons...)
	}
	return merged
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
