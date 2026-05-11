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

package networkpool

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/ipam"
)

const (
	// Capacity event reasons
	reasonCapacityWarning   = "PoolCapacityWarning"
	reasonCapacityCritical  = "PoolCapacityCritical"
	reasonCapacityExhausted = "PoolCapacityExhausted"
	reasonCapacityRecovered = "PoolCapacityRecovered"

	// capacityEventInterval is the minimum time between repeated events
	// of the same tier for the same pool.
	capacityEventInterval = 10 * time.Minute
)

// Capacity condition types. These are set on NetworkPool.Status.Conditions
// to surface utilization state to integrations (kubectl, ArgoCD, Flux, etc.).
// Intended for promotion to butler-api once the thresholds are validated.
const (
	ConditionCapacityWarning   = "CapacityWarning"
	ConditionCapacityCritical  = "CapacityCritical"
	ConditionCapacityExhausted = "CapacityExhausted"
)

// Reconciler reconciles a NetworkPool object.
// It is the sole allocator -- it processes Pending IPAllocations.
type Reconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	// lastEventTimes tracks when each capacity event tier was last emitted
	// per pool, for rate limiting. Key format: "poolName/reason".
	// Populated lazily; safe to leave zero-valued.
	lastEventTimes sync.Map
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=networkpools,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=networkpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=networkpools/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=ipallocations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=ipallocations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pool := &butlerv1alpha1.NetworkPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("reconciling NetworkPool", "name", pool.Name, "namespace", pool.Namespace)

	// Handle deletion
	if !pool.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, pool)
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(pool, butlerv1alpha1.FinalizerNetworkPool) {
		controllerutil.AddFinalizer(pool, butlerv1alpha1.FinalizerNetworkPool)
		if err := r.Update(ctx, pool); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Validate CIDR math
	errs := r.validatePool(pool)
	if len(errs) > 0 {
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               butlerv1alpha1.ConditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             butlerv1alpha1.ReasonValidationFailed,
			Message:            fmt.Sprintf("validation errors: %v", errs),
			ObservedGeneration: pool.Generation,
		})
		pool.Status.ObservedGeneration = pool.Generation
		if err := r.Status().Update(ctx, pool); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// List all IPAllocations for this pool
	allocList := &butlerv1alpha1.IPAllocationList{}
	if err := r.List(ctx, allocList,
		client.InNamespace(pool.Namespace),
		client.MatchingFields{"spec.poolRef.name": pool.Name},
	); err != nil {
		return ctrl.Result{}, err
	}

	// Garbage collect orphaned allocations (TenantCluster deleted but allocation remains)
	if gcCount := r.gcOrphanedAllocations(ctx, pool, allocList); gcCount > 0 {
		logger.Info("garbage collected orphaned allocations", "count", gcCount)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Process pending allocations (FIFO by creation timestamp)
	pendingProcessed := r.processPendingAllocations(ctx, pool, allocList)

	// Compute status from all allocations
	if err := r.updatePoolStatus(ctx, pool, allocList); err != nil {
		return ctrl.Result{}, err
	}

	// Requeue faster if we processed pending allocations (re-check for new ones)
	if pendingProcessed > 0 {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

func (r *Reconciler) handleDeletion(ctx context.Context, pool *butlerv1alpha1.NetworkPool) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(pool, butlerv1alpha1.FinalizerNetworkPool) {
		return ctrl.Result{}, nil
	}

	// Block deletion if allocated IPAllocations exist
	allocList := &butlerv1alpha1.IPAllocationList{}
	if err := r.List(ctx, allocList,
		client.InNamespace(pool.Namespace),
		client.MatchingFields{"spec.poolRef.name": pool.Name},
	); err != nil {
		return ctrl.Result{}, err
	}

	activeCount := 0
	for _, alloc := range allocList.Items {
		if alloc.Status.Phase == butlerv1alpha1.IPAllocationPhaseAllocated {
			activeCount++
		}
	}

	if activeCount > 0 {
		logger.Info("blocking NetworkPool deletion, active allocations exist", "count", activeCount)
		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               butlerv1alpha1.ConditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             butlerv1alpha1.ReasonDeleting,
			Message:            fmt.Sprintf("waiting for %d active allocations to be released", activeCount),
			ObservedGeneration: pool.Generation,
		})
		if err := r.Status().Update(ctx, pool); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	latest := &butlerv1alpha1.NetworkPool{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(pool), latest); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if controllerutil.ContainsFinalizer(latest, butlerv1alpha1.FinalizerNetworkPool) {
		controllerutil.RemoveFinalizer(latest, butlerv1alpha1.FinalizerNetworkPool)
		if err := r.Update(ctx, latest); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) validatePool(pool *butlerv1alpha1.NetworkPool) []string {
	var reservedCIDRs []string
	for _, reserved := range pool.Spec.Reserved {
		reservedCIDRs = append(reservedCIDRs, reserved.CIDR)
	}

	var ranges []ipam.AllocatedRange
	if pool.Spec.TenantAllocation != nil {
		for _, r := range pool.Spec.TenantAllocation.GetEffectiveRanges() {
			ranges = append(ranges, ipam.AllocatedRange{Start: r.Start, End: r.End})
		}
	}

	return ipam.ValidatePoolSpec(pool.Spec.CIDR, ranges, reservedCIDRs)
}

func (r *Reconciler) processPendingAllocations(ctx context.Context, pool *butlerv1alpha1.NetworkPool, allocList *butlerv1alpha1.IPAllocationList) int {
	logger := log.FromContext(ctx)

	// Separate pending, allocated, and failed
	var pending []butlerv1alpha1.IPAllocation
	var allocated []butlerv1alpha1.IPAllocation
	for _, alloc := range allocList.Items {
		switch alloc.Status.Phase {
		case butlerv1alpha1.IPAllocationPhaseAllocated:
			allocated = append(allocated, alloc)
		case butlerv1alpha1.IPAllocationPhasePending, "":
			pending = append(pending, alloc)
		case butlerv1alpha1.IPAllocationPhaseFailed:
			// Re-process failed allocations (event-driven retry)
			pending = append(pending, alloc)
		}
	}

	if len(pending) == 0 {
		return 0
	}

	// Sort pending by creation timestamp (FIFO)
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].CreationTimestamp.Before(&pending[j].CreationTimestamp)
	})

	// Build pool state from current allocations
	poolState := r.buildPoolState(pool, allocated)
	processed := 0

	for i := range pending {
		alloc := &pending[i]
		count := r.getAllocationCount(pool, alloc)

		var result *ipam.AllocationResult
		var err error
		if alloc.Spec.PinnedRange != nil {
			// Pinned allocation: validate and assign the requested range
			result, err = ipam.AllocatePinnedRange(poolState, alloc.Spec.PinnedRange.StartAddress, alloc.Spec.PinnedRange.EndAddress)
		} else {
			result, err = ipam.AllocateRange(poolState, count)
		}
		if err != nil {
			logger.Info("allocation failed", "allocation", alloc.Name, "error", err)
			now := metav1.Now()
			alloc.Status.Phase = butlerv1alpha1.IPAllocationPhaseFailed
			meta.SetStatusCondition(&alloc.Status.Conditions, metav1.Condition{
				Type:               butlerv1alpha1.ConditionTypeReady,
				Status:             metav1.ConditionFalse,
				Reason:             butlerv1alpha1.ReasonPoolExhausted,
				Message:            err.Error(),
				ObservedGeneration: alloc.Generation,
				LastTransitionTime: now,
			})
			if updateErr := r.Status().Update(ctx, alloc); updateErr != nil {
				logger.Error(updateErr, "failed to update IPAllocation status", "allocation", alloc.Name)
			}
			r.Recorder.Eventf(pool, "Warning", "AllocationFailed",
				"Failed to allocate IPs for %s: %v", alloc.Name, err)
			allocationProcessed.WithLabelValues(pool.Name, pool.Namespace, "failed").Inc()
			continue
		}

		// Allocation succeeded
		now := metav1.Now()
		alloc.Status.Phase = butlerv1alpha1.IPAllocationPhaseAllocated
		alloc.Status.StartAddress = result.Start
		alloc.Status.EndAddress = result.End
		alloc.Status.CIDR = result.CIDR
		alloc.Status.Addresses = result.Addresses
		alloc.Status.AllocatedCount = result.Count
		alloc.Status.AllocatedAt = &now
		alloc.Status.AllocatedBy = "networkpool-controller"
		alloc.Status.ObservedGeneration = alloc.Generation

		meta.SetStatusCondition(&alloc.Status.Conditions, metav1.Condition{
			Type:               butlerv1alpha1.ConditionTypeReady,
			Status:             metav1.ConditionTrue,
			Reason:             butlerv1alpha1.ReasonReady,
			Message:            fmt.Sprintf("allocated %d IPs: %s", result.Count, result.CIDR),
			ObservedGeneration: alloc.Generation,
			LastTransitionTime: now,
		})

		if updateErr := r.Status().Update(ctx, alloc); updateErr != nil {
			logger.Error(updateErr, "failed to update IPAllocation status", "allocation", alloc.Name)
			continue
		}

		logger.Info("allocated IPs", "allocation", alloc.Name, "start", result.Start, "end", result.End, "count", result.Count)
		r.Recorder.Eventf(pool, "Normal", "IPsAllocated",
			"Allocated %d IPs (%s - %s) for %s", result.Count, result.Start, result.End, alloc.Name)
		allocationProcessed.WithLabelValues(pool.Name, pool.Namespace, "success").Inc()

		// Update pool state with new allocation for next iteration
		poolState.ExistingAllocs = append(poolState.ExistingAllocs, ipam.AllocatedRange{
			Start: result.Start,
			End:   result.End,
		})
		processed++
	}

	return processed
}

func (r *Reconciler) buildPoolState(pool *butlerv1alpha1.NetworkPool, allocated []butlerv1alpha1.IPAllocation) ipam.PoolState {
	state := ipam.PoolState{}

	// Set allocatable ranges from GetEffectiveRanges or fall back to full CIDR.
	if pool.Spec.TenantAllocation != nil {
		effectiveRanges := pool.Spec.TenantAllocation.GetEffectiveRanges()
		for _, r := range effectiveRanges {
			state.AllocatableRanges = append(state.AllocatableRanges, ipam.AllocatedRange{
				Start: r.Start,
				End:   r.End,
			})
		}
	}
	if len(state.AllocatableRanges) == 0 {
		// Use full CIDR range
		start, end, _, err := ipam.ParseCIDR(pool.Spec.CIDR)
		if err != nil {
			return state
		}
		state.AllocatableRanges = []ipam.AllocatedRange{
			{Start: start.String(), End: end.String()},
		}
	}

	// Add reserved CIDRs
	for _, reserved := range pool.Spec.Reserved {
		state.ReservedCIDRs = append(state.ReservedCIDRs, reserved.CIDR)
	}

	// Add existing allocations
	for _, alloc := range allocated {
		if alloc.Status.StartAddress != "" && alloc.Status.EndAddress != "" {
			state.ExistingAllocs = append(state.ExistingAllocs, ipam.AllocatedRange{
				Start: alloc.Status.StartAddress,
				End:   alloc.Status.EndAddress,
			})
		}
	}

	return state
}

func (r *Reconciler) getAllocationCount(pool *butlerv1alpha1.NetworkPool, alloc *butlerv1alpha1.IPAllocation) int32 {
	if alloc.Spec.Count != nil {
		return *alloc.Spec.Count
	}

	// Use pool defaults
	if pool.Spec.TenantAllocation != nil {
		switch alloc.Spec.Type {
		case butlerv1alpha1.IPAllocationTypeNodes:
			if pool.Spec.TenantAllocation.Defaults.NodesPerTenant > 0 {
				return pool.Spec.TenantAllocation.Defaults.NodesPerTenant
			}
		case butlerv1alpha1.IPAllocationTypeLoadBalancer:
			if pool.Spec.TenantAllocation.Defaults.LBPoolPerTenant > 0 {
				return pool.Spec.TenantAllocation.Defaults.LBPoolPerTenant
			}
		}
	}

	// Fallback defaults
	switch alloc.Spec.Type {
	case butlerv1alpha1.IPAllocationTypeNodes:
		return 5
	case butlerv1alpha1.IPAllocationTypeLoadBalancer:
		return 8
	}
	return 1
}

func (r *Reconciler) updatePoolStatus(ctx context.Context, pool *butlerv1alpha1.NetworkPool, allocList *butlerv1alpha1.IPAllocationList) error {
	// Build pool state for capacity calculation
	var allocated []butlerv1alpha1.IPAllocation
	allocationCount := int32(0)
	for _, alloc := range allocList.Items {
		if alloc.Status.Phase == butlerv1alpha1.IPAllocationPhaseAllocated {
			allocated = append(allocated, alloc)
			allocationCount++
		}
	}

	poolState := r.buildPoolState(pool, allocated)

	totalIPs, allocatedIPs, availableIPs, err := ipam.PoolCapacityInfo(poolState)
	if err != nil {
		return fmt.Errorf("failed to compute pool capacity: %w", err)
	}

	fragInfo, err := ipam.ComputeFragmentation(poolState)
	if err != nil {
		return fmt.Errorf("failed to compute fragmentation: %w", err)
	}

	pool.Status.TotalIPs = totalIPs
	pool.Status.AllocatedIPs = allocatedIPs
	pool.Status.AvailableIPs = availableIPs
	pool.Status.AllocationCount = allocationCount
	pool.Status.FragmentationPercent = &fragInfo.FragmentationPercent
	pool.Status.LargestFreeBlock = fragInfo.LargestFreeBlock
	pool.Status.ObservedGeneration = pool.Generation

	// Compute pool-wide capacity fields.
	poolSizeIPs, err := ipam.ComputePoolSizeIPs(pool.Spec.CIDR)
	if err == nil {
		pool.Status.PoolSizeIPs = poolSizeIPs
	}

	var reservedCIDRs []string
	for _, r := range pool.Spec.Reserved {
		reservedCIDRs = append(reservedCIDRs, r.CIDR)
	}
	reservedIPs, err := ipam.ComputeReservedIPs(reservedCIDRs)
	if err == nil {
		pool.Status.ReservedIPs = reservedIPs
	}

	var tenantRanges []ipam.AllocatedRange
	if pool.Spec.TenantAllocation != nil {
		for _, r := range pool.Spec.TenantAllocation.GetEffectiveRanges() {
			tenantRanges = append(tenantRanges, ipam.AllocatedRange{Start: r.Start, End: r.End})
		}
	}
	unmanagedRanges, unmanagedScopeIPs, err := ipam.ComputeUnmanagedRanges(pool.Spec.CIDR, reservedCIDRs, tenantRanges)
	if err == nil {
		pool.Status.UnmanagedScopeIPs = unmanagedScopeIPs
		pool.Status.UnmanagedRanges = make([]butlerv1alpha1.AllocationRange, 0, len(unmanagedRanges))
		for _, ur := range unmanagedRanges {
			pool.Status.UnmanagedRanges = append(pool.Status.UnmanagedRanges, butlerv1alpha1.AllocationRange{
				Start: ur.Start,
				End:   ur.End,
			})
		}
	}

	pool.Status.InfrastructureConsumedIPs = int32(len(pool.Status.InfrastructureAllocations))

	meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             butlerv1alpha1.ReasonReady,
		Message:            fmt.Sprintf("%d/%d IPs available (%d allocations)", availableIPs, totalIPs, allocationCount),
		ObservedGeneration: pool.Generation,
	})

	// Set capacity tier conditions based on utilization thresholds.
	// Each condition is True when utilization exceeds its threshold.
	if totalIPs > 0 {
		usagePercent := float64(allocatedIPs) / float64(totalIPs) * 100
		setCapacityConditions(pool, usagePercent, allocatedIPs, totalIPs)
	}

	// Update Prometheus metrics
	labels := []string{pool.Name, pool.Namespace}
	poolTotalIPs.WithLabelValues(labels...).Set(float64(totalIPs))
	poolAllocatedIPs.WithLabelValues(labels...).Set(float64(allocatedIPs))
	poolAvailableIPs.WithLabelValues(labels...).Set(float64(availableIPs))
	poolAllocationCount.WithLabelValues(labels...).Set(float64(allocationCount))
	poolFragmentationPercent.WithLabelValues(labels...).Set(float64(fragInfo.FragmentationPercent))
	poolLargestFreeBlock.WithLabelValues(labels...).Set(float64(fragInfo.LargestFreeBlock))

	// Emit tiered capacity events with rate limiting (one per tier per 10 minutes)
	if totalIPs > 0 {
		usagePercent := float64(allocatedIPs) / float64(totalIPs) * 100
		r.emitCapacityEvents(pool, usagePercent, allocatedIPs, totalIPs)
	}

	return r.Status().Update(ctx, pool)
}

// emitCapacityEvents emits tiered capacity events with rate limiting.
// Thresholds: Warning at 70%, Critical at 85%, Exhausted at 95%.
// Emits a recovery event when utilization drops below all thresholds
// after a previous warning was emitted.
func (r *Reconciler) emitCapacityEvents(pool *butlerv1alpha1.NetworkPool, usagePercent float64, allocated, total int32) {
	type tier struct {
		threshold float64
		reason    string
		message   string
	}

	tiers := []tier{
		{95, reasonCapacityExhausted, "NetworkPool is over 95%% utilized (%d/%d IPs allocated)"},
		{85, reasonCapacityCritical, "NetworkPool is over 85%% utilized (%d/%d IPs allocated)"},
		{70, reasonCapacityWarning, "NetworkPool is over 70%% utilized (%d/%d IPs allocated)"},
	}

	emitted := false
	for _, t := range tiers {
		if usagePercent >= t.threshold {
			if r.shouldEmitEvent(pool.Name, t.reason) {
				r.Recorder.Eventf(pool, "Warning", t.reason,
					t.message, allocated, total)
			}
			emitted = true
			break // Only emit the highest matching tier
		}
	}

	// If no tier matched and we previously emitted a warning, emit recovery
	if !emitted && r.hasPreviousEvent(pool.Name) {
		if r.shouldEmitEvent(pool.Name, reasonCapacityRecovered) {
			r.Recorder.Eventf(pool, "Normal", reasonCapacityRecovered,
				"NetworkPool utilization recovered below 70%% (%d/%d IPs allocated)", allocated, total)
		}
	}
}

// shouldEmitEvent returns true if enough time has passed since the last event
// of the given reason for the given pool. Updates the timestamp on emit.
func (r *Reconciler) shouldEmitEvent(poolName, reason string) bool {
	key := poolName + "/" + reason
	now := time.Now()

	if v, ok := r.lastEventTimes.Load(key); ok {
		if last, ok := v.(time.Time); ok && now.Sub(last) < capacityEventInterval {
			return false
		}
	}

	r.lastEventTimes.Store(key, now)
	return true
}

// hasPreviousEvent returns true if any capacity warning tier was previously
// emitted for this pool (used to decide whether to emit a recovery event).
func (r *Reconciler) hasPreviousEvent(poolName string) bool {
	for _, reason := range []string{reasonCapacityWarning, reasonCapacityCritical, reasonCapacityExhausted} {
		if _, ok := r.lastEventTimes.Load(poolName + "/" + reason); ok {
			return true
		}
	}
	return false
}

// setCapacityConditions sets CapacityWarning, CapacityCritical, and
// CapacityExhausted conditions based on pool utilization thresholds.
func setCapacityConditions(pool *butlerv1alpha1.NetworkPool, usagePercent float64, allocated, total int32) {
	msg := fmt.Sprintf("Pool utilization is %.0f%% (%d/%d IPs)", usagePercent, allocated, total)

	thresholds := []struct {
		condType  string
		threshold float64
	}{
		{ConditionCapacityWarning, 70},
		{ConditionCapacityCritical, 85},
		{ConditionCapacityExhausted, 95},
	}

	for _, t := range thresholds {
		status := metav1.ConditionFalse
		reason := "UtilizationBelowThreshold"
		if usagePercent >= t.threshold {
			status = metav1.ConditionTrue
			reason = "UtilizationAboveThreshold"
		}

		meta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               t.condType,
			Status:             status,
			Reason:             reason,
			Message:            msg,
			ObservedGeneration: pool.Generation,
		})
	}
}

// gcOrphanedAllocations deletes IPAllocations whose tenantClusterRef points
// to a TenantCluster that no longer exists. Returns the number of allocations deleted.
func (r *Reconciler) gcOrphanedAllocations(ctx context.Context, pool *butlerv1alpha1.NetworkPool, allocList *butlerv1alpha1.IPAllocationList) int {
	logger := log.FromContext(ctx)
	deleted := 0

	for i := range allocList.Items {
		alloc := &allocList.Items[i]

		// Only GC allocated entries — pending/failed are transient
		if alloc.Status.Phase != butlerv1alpha1.IPAllocationPhaseAllocated {
			continue
		}

		// Skip allocations without a tenant cluster ref
		ref := alloc.Spec.TenantClusterRef
		if ref.Name == "" || ref.Namespace == "" {
			continue
		}

		// Check if the referenced TenantCluster still exists
		tc := &butlerv1alpha1.TenantCluster{}
		err := r.Get(ctx, client.ObjectKey{Name: ref.Name, Namespace: ref.Namespace}, tc)
		if err == nil {
			continue // TC exists, allocation is valid
		}
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to check TenantCluster existence", "name", ref.Name, "namespace", ref.Namespace)
			continue
		}

		// TenantCluster is gone — delete the orphaned allocation
		logger.Info("garbage collecting orphaned IPAllocation",
			"allocation", alloc.Name, "tenant", ref.Name, "namespace", ref.Namespace)
		if err := r.Delete(ctx, alloc); err != nil && !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to delete orphaned IPAllocation", "name", alloc.Name)
			continue
		}
		r.Recorder.Eventf(pool, "Normal", "OrphanedAllocationGC",
			"Garbage collected IPAllocation %s (TenantCluster %s/%s no longer exists)",
			alloc.Name, ref.Namespace, ref.Name)
		deleted++
	}

	return deleted
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Register field indexer for efficient IPAllocation lookups by pool name
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &butlerv1alpha1.IPAllocation{},
		"spec.poolRef.name", func(obj client.Object) []string {
			alloc, ok := obj.(*butlerv1alpha1.IPAllocation)
			if !ok {
				return nil
			}
			return []string{alloc.Spec.PoolRef.Name}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.NetworkPool{}).
		Watches(&butlerv1alpha1.IPAllocation{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				alloc, ok := obj.(*butlerv1alpha1.IPAllocation)
				if !ok {
					return nil
				}
				return []reconcile.Request{
					{NamespacedName: client.ObjectKey{
						Name:      alloc.Spec.PoolRef.Name,
						Namespace: alloc.Namespace,
					}},
				}
			}),
		).
		Named("networkpool").
		Complete(r)
}
