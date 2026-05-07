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
	"net"

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
func (v *NetworkPoolValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
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

	// Validate tenant allocation range narrowing: if the range was narrowed,
	// ensure no existing IPAllocations have addresses outside the new range.
	if err := v.validateTenantAllocationNarrowing(ctx, oldPool, newPool); err != nil {
		return nil, err
	}

	return nil, nil
}

// validateTenantAllocationNarrowing rejects narrowing of the tenant allocation
// ranges when existing IPAllocations have addresses outside the proposed ranges.
// Handles all narrowing shapes: range removal, range shrink, range split,
// and Start/End-to-Ranges migration.
func (v *NetworkPoolValidator) validateTenantAllocationNarrowing(ctx context.Context, oldPool, newPool *butlerv1alpha1.NetworkPool) error {
	if newPool.Spec.TenantAllocation == nil {
		return nil
	}

	newRanges := newPool.Spec.TenantAllocation.GetEffectiveRanges()
	if len(newRanges) == 0 {
		return nil // no ranges = entire CIDR, can't narrow
	}

	// Parse new ranges into uint32 pairs for fast lookup.
	type rangeBounds struct {
		start, end uint32
	}
	newBounds := make([]rangeBounds, 0, len(newRanges))
	for _, r := range newRanges {
		sIP := net.ParseIP(r.Start).To4()
		eIP := net.ParseIP(r.End).To4()
		if sIP == nil || eIP == nil {
			return nil // IP validation is done by validatePoolSpec
		}
		newBounds = append(newBounds, rangeBounds{
			start: ipam.IPToUint32(sIP),
			end:   ipam.IPToUint32(eIP),
		})
	}

	// ipWithinRanges checks if an IP falls within any of the new ranges.
	ipWithinRanges := func(ipUint uint32) bool {
		for _, b := range newBounds {
			if ipUint >= b.start && ipUint <= b.end {
				return true
			}
		}
		return false
	}

	// List all IPAllocations referencing this pool.
	var allocList butlerv1alpha1.IPAllocationList
	if err := v.Client.List(ctx, &allocList, client.MatchingLabels{
		butlerv1alpha1.LabelNetworkPool: newPool.Name,
	}); err != nil {
		return fmt.Errorf("failed to list IPAllocations for pool %q: %w", newPool.Name, err)
	}

	var allErrs field.ErrorList
	for i := range allocList.Items {
		alloc := &allocList.Items[i]
		if alloc.Status.Phase != butlerv1alpha1.IPAllocationPhaseAllocated {
			continue
		}

		// Check each address in the allocation.
		for _, addr := range alloc.Status.Addresses {
			ip := net.ParseIP(addr).To4()
			if ip == nil {
				continue
			}
			if !ipWithinRanges(ipam.IPToUint32(ip)) {
				allErrs = append(allErrs, field.Forbidden(
					field.NewPath("spec", "tenantAllocation"),
					fmt.Sprintf(
						"cannot narrow tenant allocation ranges: IPAllocation %q has address %s outside the proposed ranges. Migrate or delete affected allocations first",
						alloc.Name, addr,
					),
				))
				break // one violation per allocation is enough
			}
		}

		// Also check start/end addresses for allocations that may not have
		// Addresses populated (belt and suspenders).
		if len(alloc.Status.Addresses) == 0 && alloc.Status.StartAddress != "" {
			startIP := net.ParseIP(alloc.Status.StartAddress).To4()
			endIP := net.ParseIP(alloc.Status.EndAddress).To4()
			if startIP != nil && endIP != nil {
				sUint := ipam.IPToUint32(startIP)
				eUint := ipam.IPToUint32(endIP)
				if !ipWithinRanges(sUint) || !ipWithinRanges(eUint) {
					allErrs = append(allErrs, field.Forbidden(
						field.NewPath("spec", "tenantAllocation"),
						fmt.Sprintf(
							"cannot narrow tenant allocation ranges: IPAllocation %q has addresses %s-%s outside the proposed ranges. Migrate or delete affected allocations first",
							alloc.Name, alloc.Status.StartAddress, alloc.Status.EndAddress,
						),
					))
				}
			}
		}
	}

	if len(allErrs) > 0 {
		return allErrs.ToAggregate()
	}
	return nil
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
// Returns deprecation warnings when Start/End are used instead of Ranges.
func (v *NetworkPoolValidator) validatePoolSpec(pool *butlerv1alpha1.NetworkPool) (admission.Warnings, error) {
	// Collect reserved CIDRs.
	reservedCIDRs := make([]string, 0, len(pool.Spec.Reserved))
	for _, r := range pool.Spec.Reserved {
		reservedCIDRs = append(reservedCIDRs, r.CIDR)
	}

	// Resolve effective ranges and build deprecation warnings.
	var warnings admission.Warnings
	var ranges []ipam.AllocatedRange
	if pool.Spec.TenantAllocation != nil {
		ta := pool.Spec.TenantAllocation
		hasStartEnd := ta.Start != "" && ta.End != ""
		hasRanges := len(ta.Ranges) > 0

		if hasStartEnd && hasRanges {
			warnings = append(warnings,
				"Start/End are ignored when Ranges is present; remove them")
		} else if hasStartEnd && !hasRanges {
			warnings = append(warnings, fmt.Sprintf(
				"Start/End fields are deprecated; migrate to Ranges. Equivalent configuration:\n"+
					"  tenantAllocation:\n"+
					"    ranges:\n"+
					"    - start: %s\n"+
					"      end: %s", ta.Start, ta.End))
		}

		for _, r := range ta.GetEffectiveRanges() {
			ranges = append(ranges, ipam.AllocatedRange{Start: r.Start, End: r.End})
		}
	}

	errs := ipam.ValidatePoolSpec(pool.Spec.CIDR, ranges, reservedCIDRs)
	if len(errs) > 0 {
		var allErrs field.ErrorList
		for _, errMsg := range errs {
			allErrs = append(allErrs, field.Invalid(
				field.NewPath("spec"),
				pool.Spec.CIDR,
				errMsg,
			))
		}
		return warnings, allErrs.ToAggregate()
	}

	return warnings, nil
}

// SetupWebhookWithManager registers the NetworkPool validating webhook with the manager.
func (v *NetworkPoolValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	v.Client = mgr.GetClient()
	return ctrl.NewWebhookManagedBy(mgr).
		For(&butlerv1alpha1.NetworkPool{}).
		WithValidator(v).
		Complete()
}
