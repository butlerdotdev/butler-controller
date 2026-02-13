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
	"github.com/butlerdotdev/butler-controller/internal/ipam"
)

// NetworkPoolValidator validates NetworkPool resources on admission.
type NetworkPoolValidator struct {
	Client client.Client
}

// +kubebuilder:webhook:path=/validate-butler-butlerlabs-dev-v1alpha1-networkpool,mutating=false,failurePolicy=fail,sideEffects=None,groups=butler.butlerlabs.dev,resources=networkpools,verbs=create;update;delete,versions=v1alpha1,name=vnetworkpool.kb.io,admissionReviewVersions=v1

// ValidateCreate validates a NetworkPool on creation.
func (v *NetworkPoolValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	pool, ok := obj.(*butlerv1alpha1.NetworkPool)
	if !ok {
		return nil, fmt.Errorf("expected NetworkPool, got %T", obj)
	}

	return v.validatePoolSpec(pool)
}

// ValidateUpdate validates a NetworkPool on update.
func (v *NetworkPoolValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	oldPool, ok := oldObj.(*butlerv1alpha1.NetworkPool)
	if !ok {
		return nil, fmt.Errorf("expected NetworkPool for old object, got %T", oldObj)
	}

	newPool, ok := newObj.(*butlerv1alpha1.NetworkPool)
	if !ok {
		return nil, fmt.Errorf("expected NetworkPool for new object, got %T", newObj)
	}

	// Validate the new spec.
	warnings, err := v.validatePoolSpec(newPool)
	if err != nil {
		return warnings, err
	}

	// Validate that the new CIDR contains the old CIDR to prevent shrinking
	// below existing allocations.
	if newPool.Spec.CIDR != oldPool.Spec.CIDR {
		var allErrs field.ErrorList

		contained, cidrErr := ipam.ValidateCIDRContains(newPool.Spec.CIDR, oldPool.Spec.CIDR)
		if cidrErr != nil {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec", "cidr"),
				newPool.Spec.CIDR,
				fmt.Sprintf("failed to validate CIDR containment: %v", cidrErr),
			))
		} else if !contained {
			allErrs = append(allErrs, field.Forbidden(
				field.NewPath("spec", "cidr"),
				fmt.Sprintf(
					"new CIDR %q does not contain the previous CIDR %q; shrinking a pool may orphan existing allocations",
					newPool.Spec.CIDR, oldPool.Spec.CIDR,
				),
			))
		}

		if len(allErrs) > 0 {
			return nil, allErrs.ToAggregate()
		}
	}

	return nil, nil
}

// ValidateDelete validates a NetworkPool on deletion.
func (v *NetworkPoolValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	pool, ok := obj.(*butlerv1alpha1.NetworkPool)
	if !ok {
		return nil, fmt.Errorf("expected NetworkPool, got %T", obj)
	}

	// Check for active IPAllocations that reference this pool.
	var allocList butlerv1alpha1.IPAllocationList
	if err := v.Client.List(ctx, &allocList, client.MatchingLabels{
		butlerv1alpha1.LabelNetworkPool: pool.Name,
	}); err != nil {
		return nil, fmt.Errorf("failed to list IPAllocations for pool %q: %w", pool.Name, err)
	}

	for i := range allocList.Items {
		if allocList.Items[i].Status.Phase == butlerv1alpha1.IPAllocationPhaseAllocated {
			return nil, field.ErrorList{field.Forbidden(
				field.NewPath("metadata", "name"),
				fmt.Sprintf(
					"cannot delete NetworkPool %q: IPAllocation %q in namespace %q is still in phase %s",
					pool.Name,
					allocList.Items[i].Name,
					allocList.Items[i].Namespace,
					butlerv1alpha1.IPAllocationPhaseAllocated,
				),
			)}.ToAggregate()
		}
	}

	return nil, nil
}

// validatePoolSpec validates the internal consistency of a NetworkPool spec using the ipam package.
func (v *NetworkPoolValidator) validatePoolSpec(pool *butlerv1alpha1.NetworkPool) (admission.Warnings, error) {
	// Collect reserved CIDRs.
	reservedCIDRs := make([]string, 0, len(pool.Spec.Reserved))
	for _, r := range pool.Spec.Reserved {
		reservedCIDRs = append(reservedCIDRs, r.CIDR)
	}

	// Determine allocation range.
	allocStart := ""
	allocEnd := ""
	if pool.Spec.TenantAllocation != nil {
		allocStart = pool.Spec.TenantAllocation.Start
		allocEnd = pool.Spec.TenantAllocation.End
	}

	errs := ipam.ValidatePoolSpec(pool.Spec.CIDR, reservedCIDRs, allocStart, allocEnd)
	if len(errs) > 0 {
		var allErrs field.ErrorList
		for _, errMsg := range errs {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec"),
				pool.Spec.CIDR,
				errMsg,
			))
		}
		return nil, allErrs.ToAggregate()
	}

	return nil, nil
}

// SetupWebhookWithManager registers the NetworkPool validating webhook with the manager.
func (v *NetworkPoolValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	v.Client = mgr.GetClient()
	return ctrl.NewWebhookManagedBy(mgr).
		For(&butlerv1alpha1.NetworkPool{}).
		WithValidator(v).
		Complete()
}
