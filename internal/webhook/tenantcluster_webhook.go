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

package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	authnv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	policypkg "github.com/butlerdotdev/butler-api/pkg/policy"
)

const defaultProviderConfigNamespace = "butler-system"

// TenantClusterValidator validates TenantCluster resources on admission.
//
// Client is the cached, manager-backed client used for high-volume reads
// (TenantCluster list for sibling counts); staleness up to 1s is
// tolerable because webhook decisions only need approximate counts.
//
// APIReader is an uncached reader used for reads whose staleness would
// flip an admission decision: Team spec (env list, limits, access).
// controller-runtime's cached client can lag apiserver writes by a
// reconcile tick, which is enough to miss a just-applied env rename or
// access-block edit. The Team admission webhook for "resourceLimits
// changed, needs platform admin" wants the current answer, not a 1s-
// old one.
type TenantClusterValidator struct {
	Client    client.Client
	APIReader client.Reader
}

// +kubebuilder:webhook:path=/validate-butler-butlerlabs-dev-v1alpha1-tenantcluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=butler.butlerlabs.dev,resources=tenantclusters,verbs=create;update,versions=v1alpha1,name=vtenantcluster.kb.io,admissionReviewVersions=v1

// Handle implements admission.Handler. Dispatches to handleCreate or
// handleUpdate based on the operation. Uses the raw admission.Handler
// pattern (not CustomValidator) so the handler can read UserInfo from
// the admission request. UserInfo is required to validate that the
// creator-email annotation matches the requesting identity; this is
// the only defense against a kubectl-direct caller spoofing another
// user's MaxClustersPerMember cap by claiming a different email.
func (v *TenantClusterValidator) Handle(ctx context.Context, req admission.Request) admission.Response {
	log.FromContext(ctx).Info("admission request received",
		"op", string(req.Operation),
		"namespace", req.Namespace,
		"name", req.Name,
		"userInfoUsername", req.UserInfo.Username,
		"userInfoGroups", req.UserInfo.Groups,
	)
	switch req.Operation {
	case admissionv1.Create:
		return v.handleCreate(ctx, req)
	case admissionv1.Update:
		return v.handleUpdate(ctx, req)
	default:
		return admission.Allowed("")
	}
}

func (v *TenantClusterValidator) handleCreate(ctx context.Context, req admission.Request) admission.Response {
	tc := &butlerv1alpha1.TenantCluster{}
	if err := json.Unmarshal(req.Object.Raw, tc); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode TenantCluster: %w", err))
	}
	if _, err := v.validateCreateUpdate(ctx, tc, nil, req.UserInfo); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("")
}

func (v *TenantClusterValidator) handleUpdate(ctx context.Context, req admission.Request) admission.Response {
	oldTC := &butlerv1alpha1.TenantCluster{}
	if err := json.Unmarshal(req.OldObject.Raw, oldTC); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode old TenantCluster: %w", err))
	}
	tc := &butlerv1alpha1.TenantCluster{}
	if err := json.Unmarshal(req.Object.Raw, tc); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode new TenantCluster: %w", err))
	}
	if errs := validateEnvironmentLabelImmutability(oldTC, tc); len(errs) > 0 {
		return admission.Denied(errs.ToAggregate().Error())
	}
	if _, err := v.validateCreateUpdate(ctx, tc, oldTC, req.UserInfo); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("")
}

// validateEnvironmentLabelImmutability enforces that an env-label change
// on update carries the AnnotationMigrationOperation annotation set to
// "true". Direct kubectl edits of the label are rejected. Additions and
// removals count as changes. A no-op update (label unchanged) passes
// through. See ADR-009 Phased Migration for the operator model.
func validateEnvironmentLabelImmutability(oldTC, newTC *butlerv1alpha1.TenantCluster) field.ErrorList {
	var errs field.ErrorList
	oldEnv := ""
	if oldTC.Labels != nil {
		oldEnv = oldTC.Labels[butlerv1alpha1.LabelEnvironment]
	}
	newEnv := ""
	if newTC.Labels != nil {
		newEnv = newTC.Labels[butlerv1alpha1.LabelEnvironment]
	}
	if oldEnv == newEnv {
		return errs
	}
	migration := ""
	if newTC.Annotations != nil {
		migration = newTC.Annotations[butlerv1alpha1.AnnotationMigrationOperation]
	}
	if migration != "true" {
		errs = append(errs, field.Forbidden(
			field.NewPath("metadata", "labels").Key(butlerv1alpha1.LabelEnvironment),
			fmt.Sprintf(
				"env label changes require the %q annotation set to \"true\"; use `butleradm env migrate`",
				butlerv1alpha1.AnnotationMigrationOperation,
			),
		))
	}
	return errs
}

// validateCreateUpdate contains shared validation logic for create and
// update operations. oldTC is nil on create and non-nil on update; the
// update path carries it through so validateEnvironment can grandfather
// pre-existing unlabeled TenantClusters whose team later defined
// environments (ADR-009 phased migration, phase 2).
func (v *TenantClusterValidator) validateCreateUpdate(ctx context.Context, tc, oldTC *butlerv1alpha1.TenantCluster, userInfo authnv1.UserInfo) (admission.Warnings, error) {
	var allErrs field.ErrorList

	// Aggregate (count- and sum-based) quota gates are create-time
	// guards that simulate adding this TC to the namespace total. On
	// update the cluster already exists and the admitted count is
	// stable; re-running the simulation treats the update as an extra
	// addition and deadlocks routine operations (scale, annotate,
	// deletionTimestamp propagation, finalizer removal) after a quota
	// reduction. Per-cluster caps (MaxNodesPerCluster) remain on both
	// paths because they gate THIS TC's own spec and are legitimate
	// to re-check on scale. See ADR-009: "Cluster creation validates
	// three gates in order."
	isCreate := oldTC == nil

	// Environment label and per-env checks are independent of
	// providerConfigRef; a TC without a provider config still belongs
	// to a team and a team may define environments. Run env validation
	// first so the env-label-required and creator-email identity gates
	// fire regardless of whether the provider-access path runs.
	if tc.Spec.TeamRef != nil {
		envErrs, err := v.validateEnvironment(ctx, tc, oldTC, userInfo)
		if err != nil {
			return nil, err
		}
		allErrs = append(allErrs, envErrs...)
	}

	if tc.Spec.ProviderConfigRef == nil {
		if len(allErrs) > 0 {
			return nil, allErrs.ToAggregate()
		}
		return nil, nil
	}

	// Resolve the ProviderConfig namespace.
	pcNamespace := tc.Spec.ProviderConfigRef.Namespace
	if pcNamespace == "" {
		pcNamespace = defaultProviderConfigNamespace
	}

	// Fetch the referenced ProviderConfig.
	pc := &butlerv1alpha1.ProviderConfig{}
	pcKey := types.NamespacedName{
		Name:      tc.Spec.ProviderConfigRef.Name,
		Namespace: pcNamespace,
	}
	if err := v.Client.Get(ctx, pcKey, pc); err != nil {
		if apierrors.IsNotFound(err) {
			allErrs = append(allErrs, field.NotFound(
				field.NewPath("spec", "providerConfigRef", "name"),
				tc.Spec.ProviderConfigRef.Name,
			))
			return nil, allErrs.ToAggregate()
		}
		return nil, fmt.Errorf("failed to fetch ProviderConfig %s/%s: %w", pcNamespace, tc.Spec.ProviderConfigRef.Name, err)
	}

	// Validate team scope if the ProviderConfig is team-scoped.
	if pc.Spec.Scope != nil && pc.Spec.Scope.Type == butlerv1alpha1.ProviderConfigScopeTeam {
		if pc.Spec.Scope.TeamRef != nil {
			tcTeamName := ""
			if tc.Spec.TeamRef != nil {
				tcTeamName = tc.Spec.TeamRef.Name
			}
			if tcTeamName != pc.Spec.Scope.TeamRef.Name {
				allErrs = append(allErrs, field.Forbidden(
					field.NewPath("spec", "providerConfigRef"),
					fmt.Sprintf(
						"ProviderConfig %q is scoped to team %q, but TenantCluster references team %q",
						pc.Name, pc.Spec.Scope.TeamRef.Name, tcTeamName,
					),
				))
			}
		}
	}

	// ADR-018: enforce ClusterCreationPolicy at admission time. Runs
	// after PC fetch because provider type drives field-map resolution.
	policyErrs, err := v.validatePolicy(ctx, tc, pc.Spec.Provider)
	if err != nil {
		return nil, err
	}
	allErrs = append(allErrs, policyErrs...)

	// Enforce per-team cluster limit. Create-only: on update the TC is
	// already counted and re-simulation would deadlock operations when
	// the cap is reduced below existing usage.
	if isCreate && pc.Spec.Limits != nil && pc.Spec.Limits.MaxClustersPerTeam != nil {
		maxClusters := *pc.Spec.Limits.MaxClustersPerTeam

		var tcList butlerv1alpha1.TenantClusterList
		if err := v.Client.List(ctx, &tcList, client.InNamespace(tc.Namespace)); err != nil {
			return nil, fmt.Errorf("failed to list TenantClusters in namespace %s: %w", tc.Namespace, err)
		}

		// Count existing clusters, excluding self on update.
		existingCount := int32(0)
		for i := range tcList.Items {
			if tcList.Items[i].Name != tc.Name {
				existingCount++
			}
		}

		if existingCount >= maxClusters {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec"),
				fmt.Sprintf(
					"team namespace %q already has %d cluster(s); ProviderConfig %q limits to %d",
					tc.Namespace, existingCount, pc.Name, maxClusters,
				),
			))
		}
	}

	// Enforce per-team node limit. Create-only (aggregate, see comment
	// on MaxClustersPerTeam above).
	if isCreate && pc.Spec.Limits != nil && pc.Spec.Limits.MaxNodesPerTeam != nil {
		maxNodes := *pc.Spec.Limits.MaxNodesPerTeam

		var tcList butlerv1alpha1.TenantClusterList
		if err := v.Client.List(ctx, &tcList, client.InNamespace(tc.Namespace)); err != nil {
			return nil, fmt.Errorf("failed to list TenantClusters in namespace %s: %w", tc.Namespace, err)
		}

		// Sum worker replicas across all TCs in the namespace, excluding self.
		totalNodes := tc.Spec.Workers.Replicas
		for i := range tcList.Items {
			if tcList.Items[i].Name != tc.Name {
				totalNodes += tcList.Items[i].Spec.Workers.Replicas
			}
		}

		if totalNodes > maxNodes {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec", "workers", "replicas"),
				fmt.Sprintf(
					"total worker replicas (%d) would exceed ProviderConfig %q per-team node limit (%d)",
					totalNodes, pc.Name, maxNodes,
				),
			))
		}
	}

	// Enforce team-level CPU, Memory, and Storage quotas.
	if tc.Spec.TeamRef != nil {
		team := &butlerv1alpha1.Team{}
		if err := v.APIReader.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err == nil {
			if limits := team.Spec.ResourceLimits; limits != nil {
				// List existing clusters for resource accounting
				var tcList butlerv1alpha1.TenantClusterList
				if err := v.Client.List(ctx, &tcList, client.InNamespace(tc.Namespace)); err == nil {
					mt := tc.Spec.Workers.MachineTemplate
					replicas := tc.Spec.Workers.Replicas

					// Cluster count check (create-only; on update the TC is
					// already counted and re-simulation deadlocks on quota
					// reduction).
					if isCreate && limits.MaxClusters != nil {
						existingCount := int32(0)
						for i := range tcList.Items {
							if tcList.Items[i].Name != tc.Name {
								existingCount++
							}
						}
						// Adding this cluster would make existingCount+1
						if existingCount+1 > *limits.MaxClusters {
							allErrs = append(allErrs, field.Forbidden(
								field.NewPath("spec"),
								fmt.Sprintf(
									"team %q already has %d cluster(s); team quota limits to %d",
									team.Name, existingCount, *limits.MaxClusters,
								),
							))
						}
					}

					// Total nodes check (create-only, aggregate)
					if isCreate && limits.MaxTotalNodes != nil {
						totalNodes := replicas
						for i := range tcList.Items {
							if tcList.Items[i].Name != tc.Name {
								totalNodes += tcList.Items[i].Spec.Workers.Replicas
							}
						}
						if totalNodes > *limits.MaxTotalNodes {
							allErrs = append(allErrs, field.Forbidden(
								field.NewPath("spec", "workers", "replicas"),
								fmt.Sprintf(
									"total worker nodes (%d) would exceed team %q node quota (%d)",
									totalNodes, team.Name, *limits.MaxTotalNodes,
								),
							))
						}
					}

					// Per-cluster node limit check
					if limits.MaxNodesPerCluster != nil {
						if replicas > *limits.MaxNodesPerCluster {
							allErrs = append(allErrs, field.Forbidden(
								field.NewPath("spec", "workers", "replicas"),
								fmt.Sprintf(
									"worker replicas (%d) exceeds team %q per-cluster node limit (%d)",
									replicas, team.Name, *limits.MaxNodesPerCluster,
								),
							))
						}
					}

					// CPU quota check (create-only, aggregate)
					if isCreate && limits.MaxCPUCores != nil && !limits.MaxCPUCores.IsZero() {
						requestedCPU := resource.NewQuantity(int64(mt.CPU)*int64(replicas), resource.DecimalSI)
						currentCPU := resource.NewQuantity(0, resource.DecimalSI)
						for i := range tcList.Items {
							if tcList.Items[i].Name != tc.Name {
								other := tcList.Items[i]
								otherCPU := resource.NewQuantity(
									int64(other.Spec.Workers.MachineTemplate.CPU)*int64(other.Spec.Workers.Replicas),
									resource.DecimalSI,
								)
								currentCPU.Add(*otherCPU)
							}
						}
						totalCPU := currentCPU.DeepCopy()
						totalCPU.Add(*requestedCPU)
						if totalCPU.Cmp(*limits.MaxCPUCores) > 0 {
							allErrs = append(allErrs, field.Forbidden(
								field.NewPath("spec", "workers"),
								fmt.Sprintf(
									"total CPU (%s) would exceed team %q CPU quota (%s)",
									totalCPU.String(), team.Name, limits.MaxCPUCores.String(),
								),
							))
						}
					}

					// Memory quota check (create-only, aggregate)
					if isCreate && limits.MaxMemory != nil && !limits.MaxMemory.IsZero() {
						requestedMemory := resource.NewQuantity(0, resource.BinarySI)
						for i := int32(0); i < replicas; i++ {
							requestedMemory.Add(mt.Memory)
						}
						currentMemory := resource.NewQuantity(0, resource.BinarySI)
						for i := range tcList.Items {
							if tcList.Items[i].Name != tc.Name {
								other := tcList.Items[i]
								for j := int32(0); j < other.Spec.Workers.Replicas; j++ {
									currentMemory.Add(other.Spec.Workers.MachineTemplate.Memory)
								}
							}
						}
						totalMemory := currentMemory.DeepCopy()
						totalMemory.Add(*requestedMemory)
						if totalMemory.Cmp(*limits.MaxMemory) > 0 {
							allErrs = append(allErrs, field.Forbidden(
								field.NewPath("spec", "workers"),
								fmt.Sprintf(
									"total memory (%s) would exceed team %q memory quota (%s)",
									totalMemory.String(), team.Name, limits.MaxMemory.String(),
								),
							))
						}
					}

					// Storage quota check (create-only, aggregate)
					if isCreate && limits.MaxStorage != nil && !limits.MaxStorage.IsZero() {
						requestedStorage := resource.NewQuantity(0, resource.BinarySI)
						for i := int32(0); i < replicas; i++ {
							requestedStorage.Add(mt.DiskSize)
						}
						currentStorage := resource.NewQuantity(0, resource.BinarySI)
						for i := range tcList.Items {
							if tcList.Items[i].Name != tc.Name {
								other := tcList.Items[i]
								for j := int32(0); j < other.Spec.Workers.Replicas; j++ {
									currentStorage.Add(other.Spec.Workers.MachineTemplate.DiskSize)
								}
							}
						}
						totalStorage := currentStorage.DeepCopy()
						totalStorage.Add(*requestedStorage)
						if totalStorage.Cmp(*limits.MaxStorage) > 0 {
							allErrs = append(allErrs, field.Forbidden(
								field.NewPath("spec", "workers"),
								fmt.Sprintf(
									"total storage (%s) would exceed team %q storage quota (%s)",
									totalStorage.String(), team.Name, limits.MaxStorage.String(),
								),
							))
						}
					}
				}
			}
		}
	}

	if len(allErrs) > 0 {
		return nil, allErrs.ToAggregate()
	}

	return nil, nil
}

// validateEnvironment applies the environment-label, per-env quota, and
// MaxClustersPerMember rules. Called only when tc.Spec.TeamRef is set.
// userInfo is used to validate that the creator-email annotation (when
// present and MaxClustersPerMember is engaged) matches the requesting
// identity, preventing a kubectl-direct caller from claiming another
// user's cap room. Email comparison is case-insensitive per common
// MTA practice (RFC 5321 allows CS local-parts but discourages it).
//
// oldTC is nil on create and non-nil on update. On update, a pre-
// existing TenantCluster that lacked the env label is grandfathered:
// operators can continue to scale/update/patch without adding a label,
// matching the ADR-009 phase-2 contract that existing unlabeled
// clusters "continue to work" after the team gains environments.
// Likewise, if the cluster's env label references an env that has
// since been removed from the team spec, unchanged-label updates are
// tolerated so the cluster can still be scaled or deleted. Migration
// to a valid env requires the standard migration-operation annotation.
func (v *TenantClusterValidator) validateEnvironment(ctx context.Context, tc, oldTC *butlerv1alpha1.TenantCluster, userInfo authnv1.UserInfo) (field.ErrorList, error) {
	var allErrs field.ErrorList

	team := &butlerv1alpha1.Team{}
	if err := v.APIReader.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err != nil {
		if apierrors.IsNotFound(err) {
			return allErrs, nil
		}
		return nil, fmt.Errorf("fetch team %q: %w", tc.Spec.TeamRef.Name, err)
	}

	// Phase 1: team has no environments; label absence is allowed.
	if len(team.Spec.Environments) == 0 {
		return allErrs, nil
	}

	envLabel := ""
	if tc.Labels != nil {
		envLabel = tc.Labels[butlerv1alpha1.LabelEnvironment]
	}

	// Phase 2: team has environments; label is required on create and
	// must match a team env. On update, a TenantCluster that was
	// unlabeled before the team gained environments is grandfathered.
	if envLabel == "" {
		if oldTC != nil && (oldTC.Labels == nil || oldTC.Labels[butlerv1alpha1.LabelEnvironment] == "") {
			return allErrs, nil
		}
		allErrs = append(allErrs, field.Required(
			field.NewPath("metadata", "labels").Key(butlerv1alpha1.LabelEnvironment),
			fmt.Sprintf("team %q defines environments; TenantClusters must carry this label set to one of the team's env names", team.Name),
		))
		return allErrs, nil
	}

	var targetEnv *butlerv1alpha1.EnvironmentSpec
	for i := range team.Spec.Environments {
		if team.Spec.Environments[i].Name == envLabel {
			targetEnv = &team.Spec.Environments[i]
			break
		}
	}
	if targetEnv == nil {
		// On update with unchanged env label, tolerate a dangling
		// reference so operators can still scale or delete clusters
		// whose env was removed from the team. A changed label is
		// gated by the immutability check above plus the migration
		// annotation, so this tolerance does not enable bypass.
		if oldTC != nil && oldTC.Labels[butlerv1alpha1.LabelEnvironment] == envLabel {
			return allErrs, nil
		}
		names := make([]string, 0, len(team.Spec.Environments))
		for i := range team.Spec.Environments {
			names = append(names, team.Spec.Environments[i].Name)
		}
		allErrs = append(allErrs, field.NotSupported(
			field.NewPath("metadata", "labels").Key(butlerv1alpha1.LabelEnvironment),
			envLabel,
			names,
		))
		return allErrs, nil
	}

	if targetEnv.Limits == nil {
		return allErrs, nil
	}

	// List all TenantClusters in the team namespace so we can project them
	// by environment for per-env quota accounting.
	var tcList butlerv1alpha1.TenantClusterList
	if err := v.Client.List(ctx, &tcList, client.InNamespace(tc.Namespace)); err != nil {
		return nil, fmt.Errorf("list tenant clusters in %q: %w", tc.Namespace, err)
	}

	// Count same-env siblings (exclude self on update). v1 env-level
	// enforcement is cluster count plus per-member cap; CPU/memory/
	// storage/node caps stay on Team.spec.resourceLimits.
	sameEnvCount := int32(0)
	for i := range tcList.Items {
		other := &tcList.Items[i]
		if other.Name == tc.Name {
			continue
		}
		if other.Labels == nil || other.Labels[butlerv1alpha1.LabelEnvironment] != envLabel {
			continue
		}
		sameEnvCount++
	}

	lim := targetEnv.Limits
	isCreate := oldTC == nil

	// Env cluster-count cap. Create-only: the TC already counts in
	// same-env siblings after admission; re-simulating on update would
	// deadlock operations (scale, annotate, deletionTimestamp, finalizer
	// removal) when an admin reduces the cap below current usage. Zero
	// means no cap (aligned with MaxClustersPerMember semantics).
	if isCreate && lim.MaxClusters != nil && *lim.MaxClusters > 0 && sameEnvCount+1 > *lim.MaxClusters {
		allErrs = append(allErrs, field.Forbidden(
			field.NewPath("spec"),
			fmt.Sprintf(
				"environment %q in team %q already has %d cluster(s); env quota limits to %d",
				envLabel, team.Name, sameEnvCount, *lim.MaxClusters,
			),
		))
	}

	// MaxClustersPerMember enforcement. The creator's email is carried on
	// the incoming TenantCluster via the AnnotationCreatorEmail annotation
	// set by butler-server on API-path creates. On CREATE the annotation
	// must match the requesting identity; on UPDATE the annotation was
	// already vetted at create time and must stay unchanged (immutability
	// check below). The cap simulation (ownedByCreator+1 > max) is
	// create-only for the same reason as the env cluster cap above;
	// update-path identity match is also skipped and replaced by
	// annotation immutability.
	if lim.MaxClustersPerMember != nil && *lim.MaxClustersPerMember > 0 {
		creator := ""
		if tc.Annotations != nil {
			creator = tc.Annotations[butlerv1alpha1.AnnotationCreatorEmail]
		}
		oldCreator := ""
		if !isCreate && oldTC.Annotations != nil {
			oldCreator = oldTC.Annotations[butlerv1alpha1.AnnotationCreatorEmail]
		}
		if creator == "" && isCreate {
			allErrs = append(allErrs, field.Required(
				field.NewPath("metadata", "annotations").Key(butlerv1alpha1.AnnotationCreatorEmail),
				fmt.Sprintf(
					"environment %q sets maxClustersPerMember; TenantClusters must carry the %s annotation (set automatically by butler-server; kubectl-direct creates must supply it explicitly)",
					envLabel, butlerv1alpha1.AnnotationCreatorEmail,
				),
			))
		} else if isCreate && !strings.EqualFold(creator, userInfo.Username) {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("metadata", "annotations").Key(butlerv1alpha1.AnnotationCreatorEmail),
				creator,
				fmt.Sprintf(
					"%s must match the requesting identity %q; environment %q enforces per-member caps and cannot accept an annotation claiming a different user",
					butlerv1alpha1.AnnotationCreatorEmail, userInfo.Username, envLabel,
				),
			))
		} else if !isCreate && creator != oldCreator {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("metadata", "annotations").Key(butlerv1alpha1.AnnotationCreatorEmail),
				fmt.Sprintf(
					"%s is immutable; cannot change from %q to %q",
					butlerv1alpha1.AnnotationCreatorEmail, oldCreator, creator,
				),
			))
		} else if isCreate {
			ownedByCreator := int32(0)
			for i := range tcList.Items {
				other := &tcList.Items[i]
				if other.Name == tc.Name {
					continue
				}
				if other.Labels == nil || other.Labels[butlerv1alpha1.LabelEnvironment] != envLabel {
					continue
				}
				otherOwner := ""
				if other.Annotations != nil {
					otherOwner = other.Annotations[butlerv1alpha1.AnnotationOwner]
					if otherOwner == "" {
						otherOwner = other.Annotations[butlerv1alpha1.AnnotationCreatorEmail]
					}
				}
				if strings.EqualFold(otherOwner, creator) {
					ownedByCreator++
				}
			}
			if ownedByCreator+1 > *lim.MaxClustersPerMember {
				allErrs = append(allErrs, field.Forbidden(
					field.NewPath("metadata", "annotations").Key(butlerv1alpha1.AnnotationCreatorEmail),
					fmt.Sprintf(
						"user %q already owns %d cluster(s) in environment %q; env limits to %d per member",
						creator, ownedByCreator, envLabel, *lim.MaxClustersPerMember,
					),
				))
			}
		}
	}

	return allErrs, nil
}

// validatePolicy resolves applicable ClusterCreationPolicy resources for
// the TC's (team, environment, provider) tuple and rejects when the TC
// spec violates a pin or allowList rule. default and recommended modes
// are presentation-only and do not enforce here. See ADR-018 Decision
// section 7.
func (v *TenantClusterValidator) validatePolicy(ctx context.Context, tc *butlerv1alpha1.TenantCluster, providerType butlerv1alpha1.ProviderType) (field.ErrorList, error) {
	var errs field.ErrorList

	teamName := ""
	if tc.Spec.TeamRef != nil {
		teamName = tc.Spec.TeamRef.Name
	}
	envName := tc.Labels[butlerv1alpha1.LabelEnvironment]

	policies := &butlerv1alpha1.ClusterCreationPolicyList{}
	if err := v.Client.List(ctx, policies); err != nil {
		return nil, fmt.Errorf("list ClusterCreationPolicy: %w", err)
	}
	if len(policies.Items) == 0 {
		return errs, nil
	}

	resCtx := policypkg.ResolutionContext{
		TeamName:        teamName,
		EnvironmentName: envName,
		ProviderType:    providerType,
	}
	rules, sources := policypkg.ResolveWithSources(resCtx, policies.Items)
	for optType, rule := range rules {
		ok, fieldPath, msg := policypkg.ValidateAgainstRule(tc, providerType, optType, rule, sources[optType])
		if !ok {
			path := field.NewPath(fieldPath)
			errs = append(errs, field.Forbidden(path, msg))
		}
	}
	return errs, nil
}

// SetupWebhookWithManager registers the TenantCluster validating webhook
// with the manager. Uses the raw admission.Handler path (not
// CustomValidator) so the handler can read UserInfo from the admission
// request; this is required to verify the creator-email annotation
// against the requesting identity.
func (v *TenantClusterValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	v.Client = mgr.GetClient()
	v.APIReader = mgr.GetAPIReader()
	mgr.GetWebhookServer().Register(
		"/validate-butler-butlerlabs-dev-v1alpha1-tenantcluster",
		&admission.Webhook{Handler: v},
	)
	return nil
}
