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
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const defaultProviderConfigNamespace = "butler-system"

// TenantClusterValidator validates TenantCluster resources on admission.
type TenantClusterValidator struct {
	Client client.Client
}

// +kubebuilder:webhook:path=/validate-butler-butlerlabs-dev-v1alpha1-tenantcluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=butler.butlerlabs.dev,resources=tenantclusters,verbs=create;update,versions=v1alpha1,name=vtenantcluster.kb.io,admissionReviewVersions=v1

// ValidateCreate validates a TenantCluster on creation.
func (v *TenantClusterValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	tc, ok := obj.(*butlerv1alpha1.TenantCluster)
	if !ok {
		return nil, fmt.Errorf("expected TenantCluster, got %T", obj)
	}

	return v.validateCreateUpdate(ctx, tc)
}

// ValidateUpdate validates a TenantCluster on update.
func (v *TenantClusterValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	tc, ok := newObj.(*butlerv1alpha1.TenantCluster)
	if !ok {
		return nil, fmt.Errorf("expected TenantCluster, got %T", newObj)
	}

	return v.validateCreateUpdate(ctx, tc)
}

// ValidateDelete validates a TenantCluster on deletion.
func (v *TenantClusterValidator) ValidateDelete(_ context.Context, _ runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

// validateCreateUpdate contains shared validation logic for create and update operations.
func (v *TenantClusterValidator) validateCreateUpdate(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (admission.Warnings, error) {
	var allErrs field.ErrorList

	if tc.Spec.ProviderConfigRef == nil {
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

	// Enforce per-team cluster limit.
	if pc.Spec.Limits != nil && pc.Spec.Limits.MaxClustersPerTeam != nil {
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

	// Enforce per-team node limit.
	if pc.Spec.Limits != nil && pc.Spec.Limits.MaxNodesPerTeam != nil {
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
		if err := v.Client.Get(ctx, types.NamespacedName{Name: tc.Spec.TeamRef.Name}, team); err == nil {
			if limits := team.Spec.ResourceLimits; limits != nil {
				// List existing clusters for resource accounting
				var tcList butlerv1alpha1.TenantClusterList
				if err := v.Client.List(ctx, &tcList, client.InNamespace(tc.Namespace)); err == nil {
					mt := tc.Spec.Workers.MachineTemplate
					replicas := tc.Spec.Workers.Replicas

					// CPU quota check
					if limits.MaxCPUCores != nil && !limits.MaxCPUCores.IsZero() {
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

					// Memory quota check
					if limits.MaxMemory != nil && !limits.MaxMemory.IsZero() {
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

					// Storage quota check
					if limits.MaxStorage != nil && !limits.MaxStorage.IsZero() {
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

// SetupWebhookWithManager registers the TenantCluster validating webhook with the manager.
func (v *TenantClusterValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	v.Client = mgr.GetClient()
	return ctrl.NewWebhookManagedBy(mgr).
		For(&butlerv1alpha1.TenantCluster{}).
		WithValidator(v).
		Complete()
}
