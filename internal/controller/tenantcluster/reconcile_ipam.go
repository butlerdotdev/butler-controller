// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/tenant"
)

// reconcileIPAllocation creates or checks LB IPAllocation for IPAM mode providers.
// Returns true if IPAM is ready (allocated or not needed), false if waiting.
func (r *Reconciler) reconcileIPAllocation(ctx context.Context, tc *butlerv1alpha1.TenantCluster, pc *butlerv1alpha1.ProviderConfig) (bool, error) {
	logger := log.FromContext(ctx)

	// Skip IPAM if not configured
	if pc.Spec.Network == nil || pc.Spec.Network.Mode != "ipam" {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionNetworkReady,
			metav1.ConditionTrue, butlerv1alpha1.ReasonReady, "cloud networking mode")
		return true, nil
	}

	if len(pc.Spec.Network.PoolRefs) == 0 {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionNetworkReady,
			metav1.ConditionFalse, butlerv1alpha1.ReasonNetworkNotReady,
			"IPAM mode but no poolRefs configured on provider")
		return false, fmt.Errorf("IPAM mode but no poolRefs configured on provider %s", pc.Name)
	}

	// Check if LB allocation already exists
	if tc.Status.LBAllocationRef != nil {
		alloc := &butlerv1alpha1.IPAllocation{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      tc.Status.LBAllocationRef.Name,
			Namespace: "butler-system",
		}, alloc); err != nil {
			if apierrors.IsNotFound(err) {
				// Allocation was deleted, clear ref and retry
				tc.Status.LBAllocationRef = nil
			} else {
				return false, err
			}
		} else {
			switch alloc.Status.Phase {
			case butlerv1alpha1.IPAllocationPhaseAllocated:
				r.setCondition(tc, butlerv1alpha1.TenantClusterConditionNetworkReady,
					metav1.ConditionTrue, butlerv1alpha1.ReasonReady,
					fmt.Sprintf("LB IPs allocated: %s", alloc.Status.CIDR))
				return true, nil
			case butlerv1alpha1.IPAllocationPhasePending:
				r.setCondition(tc, butlerv1alpha1.TenantClusterConditionNetworkReady,
					metav1.ConditionFalse, butlerv1alpha1.ReasonReconciling,
					"waiting for LB IP allocation")
				return false, nil
			case butlerv1alpha1.IPAllocationPhaseFailed:
				// Delete failed allocation and try next pool
				logger.Info("LB allocation failed, cleaning up", "allocation", alloc.Name)
				if err := r.Delete(ctx, alloc); err != nil && !apierrors.IsNotFound(err) {
					return false, err
				}
				tc.Status.LBAllocationRef = nil
			}
		}
	}

	// Create new allocation — try pools in priority order
	lbCount := r.getInitialLBPoolSize(tc, pc)

	// Enforce quota on initial allocation
	if pc.Spec.Network.QuotaPerTenant != nil && pc.Spec.Network.QuotaPerTenant.MaxLoadBalancerIPs != nil {
		maxLB := *pc.Spec.Network.QuotaPerTenant.MaxLoadBalancerIPs
		if lbCount > maxLB {
			lbCount = maxLB
		}
	}

	allocName := fmt.Sprintf("%s-%s-lb", tc.Namespace, tc.Name)

	for _, poolRef := range pc.Spec.Network.PoolRefs {
		// Check pool capacity
		pool := &butlerv1alpha1.NetworkPool{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      poolRef.Name,
			Namespace: "butler-system",
		}, pool); err != nil {
			logger.V(1).Info("pool not found, trying next", "pool", poolRef.Name)
			continue
		}

		if pool.Status.AvailableIPs < lbCount {
			logger.V(1).Info("pool has insufficient capacity, trying next",
				"pool", poolRef.Name, "available", pool.Status.AvailableIPs, "needed", lbCount)
			continue
		}

		// Create IPAllocation
		alloc := &butlerv1alpha1.IPAllocation{
			ObjectMeta: metav1.ObjectMeta{
				Name:      allocName,
				Namespace: "butler-system",
				Labels: map[string]string{
					butlerv1alpha1.LabelNetworkPool:    poolRef.Name,
					butlerv1alpha1.LabelTeam:           tc.Namespace,
					butlerv1alpha1.LabelTenant:         tc.Name,
					butlerv1alpha1.LabelAllocationType: "loadbalancer",
				},
			},
			Spec: butlerv1alpha1.IPAllocationSpec{
				PoolRef:          butlerv1alpha1.LocalObjectReference{Name: poolRef.Name},
				TenantClusterRef: butlerv1alpha1.NamespacedObjectReference{Name: tc.Name, Namespace: tc.Namespace},
				Type:             butlerv1alpha1.IPAllocationTypeLoadBalancer,
				Count:            &lbCount,
			},
		}

		if err := r.Create(ctx, alloc); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Re-fetch and check status
				existing := &butlerv1alpha1.IPAllocation{}
				if getErr := r.Get(ctx, types.NamespacedName{Name: allocName, Namespace: "butler-system"}, existing); getErr == nil {
					tc.Status.LBAllocationRef = &butlerv1alpha1.LocalObjectReference{Name: allocName}
					return false, nil
				}
			}
			return false, fmt.Errorf("failed to create IPAllocation: %w", err)
		}

		logger.Info("created LB IPAllocation", "allocation", allocName, "pool", poolRef.Name, "count", lbCount)
		tc.Status.LBAllocationRef = &butlerv1alpha1.LocalObjectReference{Name: allocName}
		return false, nil // Wait for allocation to be fulfilled
	}

	// All pools exhausted
	r.setCondition(tc, butlerv1alpha1.TenantClusterConditionNetworkReady,
		metav1.ConditionFalse, butlerv1alpha1.ReasonPoolExhausted,
		"all configured pools are exhausted")
	return false, nil
}

// reconcileElasticIPAM handles dynamic LB IP allocation growth and shrink for Ready clusters.
func (r *Reconciler) reconcileElasticIPAM(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) error {
	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady {
		return nil
	}

	logger := log.FromContext(ctx)

	pc, err := r.getProviderConfig(ctx, tc)
	if err != nil {
		return nil // Can't get provider, skip silently
	}

	if !r.isElasticIPAM(pc) {
		return nil
	}

	// Get all LB allocations for this tenant
	allocs, err := r.listLBAllocations(ctx, tc)
	if err != nil {
		return fmt.Errorf("failed to list LB allocations: %w", err)
	}

	if len(allocs) == 0 {
		return nil // No allocations yet, initial allocation handles this
	}

	// Count total allocated and available IPs
	var totalAllocated int32
	for _, alloc := range allocs {
		if alloc.Status.Phase == butlerv1alpha1.IPAllocationPhaseAllocated {
			if alloc.Spec.Count != nil {
				totalAllocated += *alloc.Spec.Count
			}
		}
	}

	// Count used IPs from tenant cluster
	tenantClient, err := r.getTenantClient(ctx, tc)
	if err != nil {
		logger.V(1).Info("cannot get tenant client for elastic IPAM, skipping", "error", err)
		return nil
	}

	usedIPs, err := r.countUsedLBIPs(ctx, tenantClient)
	if err != nil {
		logger.V(1).Info("cannot count used LB IPs, skipping", "error", err)
		return nil
	}

	availableIPs := totalAllocated - usedIPs
	growthIncrement := r.getGrowthIncrement(pc)

	logger.V(1).Info("elastic IPAM status",
		"totalAllocated", totalAllocated, "usedIPs", usedIPs,
		"availableIPs", availableIPs, "growthIncrement", growthIncrement)

	// Grow: if fewer than 1 IP is available
	if availableIPs < 1 {
		// Check quota
		if pc.Spec.Network.QuotaPerTenant != nil && pc.Spec.Network.QuotaPerTenant.MaxLoadBalancerIPs != nil {
			maxLB := *pc.Spec.Network.QuotaPerTenant.MaxLoadBalancerIPs
			if totalAllocated+growthIncrement > maxLB {
				logger.Info("elastic IPAM growth blocked by quota",
					"totalAllocated", totalAllocated, "maxLB", maxLB)
				return nil
			}
		}

		// Find next allocation index
		nextIdx := len(allocs)
		allocName := fmt.Sprintf("%s-%s-lb-%d", tc.Namespace, tc.Name, nextIdx)

		// Find a pool with capacity
		for _, poolRef := range pc.Spec.Network.PoolRefs {
			pool := &butlerv1alpha1.NetworkPool{}
			if err := r.Get(ctx, types.NamespacedName{
				Name: poolRef.Name, Namespace: "butler-system",
			}, pool); err != nil {
				continue
			}
			if pool.Status.AvailableIPs < growthIncrement {
				continue
			}

			alloc := &butlerv1alpha1.IPAllocation{
				ObjectMeta: metav1.ObjectMeta{
					Name:      allocName,
					Namespace: "butler-system",
					Labels: map[string]string{
						butlerv1alpha1.LabelNetworkPool:    poolRef.Name,
						butlerv1alpha1.LabelTeam:           tc.Namespace,
						butlerv1alpha1.LabelTenant:         tc.Name,
						butlerv1alpha1.LabelAllocationType: "loadbalancer",
					},
				},
				Spec: butlerv1alpha1.IPAllocationSpec{
					PoolRef:          butlerv1alpha1.LocalObjectReference{Name: poolRef.Name},
					TenantClusterRef: butlerv1alpha1.NamespacedObjectReference{Name: tc.Name, Namespace: tc.Namespace},
					Type:             butlerv1alpha1.IPAllocationTypeLoadBalancer,
					Count:            &growthIncrement,
				},
			}

			if err := r.Create(ctx, alloc); err != nil {
				if apierrors.IsAlreadyExists(err) {
					return nil // Already created
				}
				return fmt.Errorf("failed to create growth allocation: %w", err)
			}
			logger.Info("elastic IPAM: created growth allocation",
				"allocation", allocName, "pool", poolRef.Name, "count", growthIncrement)
			break
		}
	}

	// Shrink: if we have more than 1 allocation and enough spare IPs
	if len(allocs) > 1 && availableIPs > growthIncrement {
		// Find the newest allocation that is fully unused and older than 10 minutes
		var shrinkCandidate *butlerv1alpha1.IPAllocation
		for i := len(allocs) - 1; i >= 1; i-- { // Skip index 0 (initial allocation)
			alloc := &allocs[i]
			if alloc.Status.Phase != butlerv1alpha1.IPAllocationPhaseAllocated {
				continue
			}
			if alloc.Spec.Count == nil {
				continue
			}
			// Only shrink allocations older than 10 minutes to avoid thrashing
			if time.Since(alloc.CreationTimestamp.Time) < 10*time.Minute {
				continue
			}
			shrinkCandidate = alloc
			break
		}

		if shrinkCandidate != nil {
			logger.Info("elastic IPAM: shrinking unused allocation",
				"allocation", shrinkCandidate.Name, "availableIPs", availableIPs)
			if err := r.Delete(ctx, shrinkCandidate); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete shrink allocation: %w", err)
			}
		}
	}

	// Update MetalLB pool with all current allocated ranges
	if err := r.updateMetalLBPool(ctx, tc, butlerConfig, allocs); err != nil {
		logger.Error(err, "failed to update MetalLB pool")
	}

	return nil
}

func (r *Reconciler) getInitialLBPoolSize(tc *butlerv1alpha1.TenantCluster, pc *butlerv1alpha1.ProviderConfig) int32 {
	// TC override takes priority
	if tc.Spec.Networking.LBPoolSize != nil {
		return *tc.Spec.Networking.LBPoolSize
	}
	if pc.Spec.Network != nil && pc.Spec.Network.LoadBalancer != nil {
		lb := pc.Spec.Network.LoadBalancer
		if lb.AllocationMode == "elastic" {
			if lb.InitialPoolSize != nil {
				return *lb.InitialPoolSize
			}
			return 2
		}
		if lb.DefaultPoolSize != nil {
			return *lb.DefaultPoolSize
		}
	}
	return 8
}

func (r *Reconciler) isElasticIPAM(pc *butlerv1alpha1.ProviderConfig) bool {
	return pc.Spec.Network != nil &&
		pc.Spec.Network.Mode == "ipam" &&
		pc.Spec.Network.LoadBalancer != nil &&
		pc.Spec.Network.LoadBalancer.AllocationMode == "elastic"
}

func (r *Reconciler) getGrowthIncrement(pc *butlerv1alpha1.ProviderConfig) int32 {
	if pc.Spec.Network != nil && pc.Spec.Network.LoadBalancer != nil && pc.Spec.Network.LoadBalancer.GrowthIncrement != nil {
		return *pc.Spec.Network.LoadBalancer.GrowthIncrement
	}
	return 2
}

// listLBAllocations returns all LB IPAllocations for a given tenant cluster.
func (r *Reconciler) listLBAllocations(ctx context.Context, tc *butlerv1alpha1.TenantCluster) ([]butlerv1alpha1.IPAllocation, error) {
	allocList := &butlerv1alpha1.IPAllocationList{}
	if err := r.List(ctx, allocList, client.InNamespace("butler-system"),
		client.MatchingLabels{
			butlerv1alpha1.LabelTeam:           tc.Namespace,
			butlerv1alpha1.LabelTenant:         tc.Name,
			butlerv1alpha1.LabelAllocationType: "loadbalancer",
		}); err != nil {
		return nil, err
	}
	return allocList.Items, nil
}

// countUsedLBIPs counts the number of LoadBalancer services with assigned IPs on a tenant cluster.
func (r *Reconciler) countUsedLBIPs(ctx context.Context, tc *tenant.TenantClient) (int32, error) {
	svcList, err := tc.Clientset.CoreV1().Services("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to list services: %w", err)
	}

	var count int32
	for _, svc := range svcList.Items {
		if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
			continue
		}
		for _, ing := range svc.Status.LoadBalancer.Ingress {
			if ing.IP != "" {
				count++
				break
			}
		}
	}
	return count, nil
}

// updateMetalLBPool configures MetalLB on the tenant cluster with all allocated IP ranges.
func (r *Reconciler) updateMetalLBPool(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig, allocs []butlerv1alpha1.IPAllocation) error {
	var ranges []string
	for _, alloc := range allocs {
		if alloc.Status.Phase != butlerv1alpha1.IPAllocationPhaseAllocated {
			continue
		}
		if alloc.Status.StartAddress != "" && alloc.Status.EndAddress != "" {
			ranges = append(ranges, fmt.Sprintf("%s-%s", alloc.Status.StartAddress, alloc.Status.EndAddress))
		} else if alloc.Status.CIDR != "" {
			ranges = append(ranges, alloc.Status.CIDR)
		}
	}

	if len(ranges) == 0 {
		return nil
	}

	kubeconfig, err := r.getTenantKubeconfig(ctx, tc, butlerConfig)
	if err != nil {
		return err
	}

	return r.Installer.UpdateMetalLBPool(ctx, kubeconfig, ranges)
}

// cleanupIPAllocations deletes IPAllocations referenced by the TenantCluster
// and any additional elastic allocations discovered by labels.
func (r *Reconciler) cleanupIPAllocations(ctx context.Context, tc *butlerv1alpha1.TenantCluster) {
	logger := log.FromContext(ctx)

	// Delete referenced allocations
	refs := []*butlerv1alpha1.LocalObjectReference{tc.Status.IPAllocationRef, tc.Status.LBAllocationRef}
	deleted := make(map[string]bool)
	for _, ref := range refs {
		if ref == nil {
			continue
		}
		alloc := &butlerv1alpha1.IPAllocation{}
		if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: "butler-system"}, alloc); err != nil {
			if !apierrors.IsNotFound(err) {
				logger.Error(err, "failed to get IPAllocation for cleanup", "name", ref.Name)
			}
			continue
		}
		if err := r.Delete(ctx, alloc); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete IPAllocation", "name", ref.Name)
		} else {
			logger.Info("deleted IPAllocation during cleanup", "name", ref.Name)
		}
		deleted[ref.Name] = true
	}

	// Also clean up any elastic growth allocations discovered by labels
	allocList := &butlerv1alpha1.IPAllocationList{}
	if err := r.List(ctx, allocList, client.InNamespace("butler-system"),
		client.MatchingLabels{
			butlerv1alpha1.LabelTeam:   tc.Namespace,
			butlerv1alpha1.LabelTenant: tc.Name,
		}); err != nil {
		logger.Error(err, "failed to list IPAllocations for cleanup")
		return
	}
	for i := range allocList.Items {
		alloc := &allocList.Items[i]
		if deleted[alloc.Name] {
			continue
		}
		if err := r.Delete(ctx, alloc); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete elastic IPAllocation", "name", alloc.Name)
		} else {
			logger.Info("deleted elastic IPAllocation during cleanup", "name", alloc.Name)
		}
	}
}
