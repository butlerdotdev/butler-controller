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

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// ProviderConfigValidator validates ProviderConfig resources on admission.
type ProviderConfigValidator struct {
	Client client.Client
}

// +kubebuilder:webhook:path=/validate-butler-butlerlabs-dev-v1alpha1-providerconfig,mutating=false,failurePolicy=fail,sideEffects=None,groups=butler.butlerlabs.dev,resources=providerconfigs,verbs=create;update;delete,versions=v1alpha1,name=vproviderconfig.kb.io,admissionReviewVersions=v1

// ValidateCreate validates a ProviderConfig on creation.
func (v *ProviderConfigValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	pc, ok := obj.(*butlerv1alpha1.ProviderConfig)
	if !ok {
		return nil, fmt.Errorf("expected ProviderConfig, got %T", obj)
	}

	return v.validateCreateUpdate(pc)
}

// ValidateUpdate validates a ProviderConfig on update.
func (v *ProviderConfigValidator) ValidateUpdate(_ context.Context, _, newObj runtime.Object) (admission.Warnings, error) {
	pc, ok := newObj.(*butlerv1alpha1.ProviderConfig)
	if !ok {
		return nil, fmt.Errorf("expected ProviderConfig, got %T", newObj)
	}

	return v.validateCreateUpdate(pc)
}

// ValidateDelete validates a ProviderConfig on deletion.
func (v *ProviderConfigValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	pc, ok := obj.(*butlerv1alpha1.ProviderConfig)
	if !ok {
		return nil, fmt.Errorf("expected ProviderConfig, got %T", obj)
	}

	// Check for TenantClusters that reference this ProviderConfig.
	var tcList butlerv1alpha1.TenantClusterList
	if err := v.Client.List(ctx, &tcList); err != nil {
		return nil, fmt.Errorf("failed to list TenantClusters: %w", err)
	}

	for i := range tcList.Items {
		tc := &tcList.Items[i]
		if tc.Spec.ProviderConfigRef == nil {
			continue
		}

		refNamespace := tc.Spec.ProviderConfigRef.Namespace
		if refNamespace == "" {
			refNamespace = defaultProviderConfigNamespace
		}

		if tc.Spec.ProviderConfigRef.Name == pc.Name && refNamespace == pc.Namespace {
			return nil, field.ErrorList{field.Forbidden(
				field.NewPath("metadata", "name"),
				fmt.Sprintf(
					"cannot delete ProviderConfig %q: TenantCluster %s/%s still references it",
					pc.Name, tc.Namespace, tc.Name,
				),
			)}.ToAggregate()
		}
	}

	return nil, nil
}

// validateCreateUpdate contains shared validation logic for create and update operations.
func (v *ProviderConfigValidator) validateCreateUpdate(pc *butlerv1alpha1.ProviderConfig) (admission.Warnings, error) {
	var allErrs field.ErrorList

	// Validate provider-specific configuration exists.
	switch pc.Spec.Provider {
	case butlerv1alpha1.ProviderTypeAzure:
		if pc.Spec.Azure == nil {
			allErrs = append(allErrs, field.Required(
				field.NewPath("spec", "azure"),
				"azure configuration is required when provider is \"azure\"",
			))
		} else {
			if pc.Spec.Azure.SubscriptionID == "" {
				allErrs = append(allErrs, field.Required(
					field.NewPath("spec", "azure", "subscriptionID"),
					"subscriptionID is required for Azure provider",
				))
			}
			if pc.Spec.Azure.ResourceGroup == "" {
				allErrs = append(allErrs, field.Required(
					field.NewPath("spec", "azure", "resourceGroup"),
					"resourceGroup is required for Azure provider",
				))
			}
		}

	case butlerv1alpha1.ProviderTypeAWS:
		if pc.Spec.AWS == nil {
			allErrs = append(allErrs, field.Required(
				field.NewPath("spec", "aws"),
				"aws configuration is required when provider is \"aws\"",
			))
		} else {
			if pc.Spec.AWS.Region == "" {
				allErrs = append(allErrs, field.Required(
					field.NewPath("spec", "aws", "region"),
					"region is required for AWS provider",
				))
			}
		}

	case butlerv1alpha1.ProviderTypeGCP:
		if pc.Spec.GCP == nil {
			allErrs = append(allErrs, field.Required(
				field.NewPath("spec", "gcp"),
				"gcp configuration is required when provider is \"gcp\"",
			))
		} else {
			if pc.Spec.GCP.ProjectID == "" {
				allErrs = append(allErrs, field.Required(
					field.NewPath("spec", "gcp", "projectID"),
					"projectID is required for GCP provider",
				))
			}
			if pc.Spec.GCP.Region == "" {
				allErrs = append(allErrs, field.Required(
					field.NewPath("spec", "gcp", "region"),
					"region is required for GCP provider",
				))
			}
		}
	}

	// Validate scope configuration.
	if pc.Spec.Scope != nil && pc.Spec.Scope.Type == butlerv1alpha1.ProviderConfigScopeTeam {
		if pc.Spec.Scope.TeamRef == nil {
			allErrs = append(allErrs, field.Required(
				field.NewPath("spec", "scope", "teamRef"),
				"teamRef is required when scope type is \"team\"",
			))
		}
	}

	// Validate network configuration.
	if pc.Spec.Network != nil && pc.Spec.Network.Mode == "ipam" {
		// IPAM mode requires at least one pool reference.
		if len(pc.Spec.Network.PoolRefs) == 0 {
			allErrs = append(allErrs, field.Required(
				field.NewPath("spec", "network", "poolRefs"),
				"at least one poolRef is required when network mode is \"ipam\"",
			))
		}

		// IPAM mode is not supported for cloud providers.
		if isCloudProvider(pc.Spec.Provider) {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec", "network", "mode"),
				"IPAM mode is not supported for cloud providers",
			))
		}
	}

	if len(allErrs) > 0 {
		return nil, allErrs.ToAggregate()
	}

	return nil, nil
}

// isCloudProvider returns true if the provider type is a public cloud provider.
func isCloudProvider(provider butlerv1alpha1.ProviderType) bool {
	switch provider {
	case butlerv1alpha1.ProviderTypeAzure,
		butlerv1alpha1.ProviderTypeAWS,
		butlerv1alpha1.ProviderTypeGCP:
		return true
	default:
		return false
	}
}

// SetupWebhookWithManager registers the ProviderConfig validating webhook with the manager.
func (v *ProviderConfigValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	v.Client = mgr.GetClient()
	return ctrl.NewWebhookManagedBy(mgr).
		For(&butlerv1alpha1.ProviderConfig{}).
		WithValidator(v).
		Complete()
}
