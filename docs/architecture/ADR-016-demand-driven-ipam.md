# ADR-016: Demand-Driven IPAM

## Status

Accepted

## Date

2026-05-03

## Context

Butler's on-prem IPAM system (ADR-008) has been in production for several months on a deployment with 8 tenant clusters. The initial allocation path works correctly, but the elastic growth/shrink cycle has a fundamental flaw: it oscillates indefinitely, creating and deleting IPAllocations every reconcile interval on 5 of 8 production tenant clusters.

### The phantom growth cycle

The elastic IPAM logic in `reconcileElasticIPAM` computes available IPs as:

```
availableIPs = totalAllocated - platformCount - tenantCount
```

Growth fires when `availableIPs < 1`. Shrink fires when `availableIPs >= growthIncrement`. With `growthIncrement=1` and all allocated IPs in use (e.g., 2 IPs allocated, 1 platform LB service + 1 tenant LB service), `availableIPs = 0` triggers growth. After the growth allocation is fulfilled, `availableIPs = 1`, which equals `growthIncrement`, so shrink triggers. After shrink, `availableIPs = 0` again. This repeats every reconcile interval (1-15 minutes depending on cluster age), creating continuous IPAllocation churn, unnecessary Kubernetes events, and wasted controller cycles.

A 1Gi memory limit on the controller absorbs the burst allocation patterns this cycle creates, but the underlying bug persists.

### Foundation work

Several infrastructure improvements shipped in butler-controller v0.18.0 (#84, #85, #86, #87) to address gaps without changing allocation behavior:

- **IPAllocation watch on TenantCluster controller** (#84): Eliminated the 1-15 minute propagation delay for IPAllocation state changes. The TenantCluster controller now reacts to allocation changes within seconds rather than waiting for timer-based requeue.
- **Dynamic client for MetalLB sync** (#85): Replaced the kubectl subprocess with server-side apply via a dynamic client. Adds timeout, retry with exponential backoff, and read-back verification.
- **Tiered capacity events** (#86): Rate-limited events at 70%, 85%, and 95% utilization with recovery events. Replaces the previous behavior of emitting an event every 60 seconds above 80%.
- **Capacity status conditions** (#87): Always-present `CapacityWarning`, `CapacityCritical`, and `CapacityExhausted` conditions on NetworkPool, consumable by kubectl, ArgoCD, Flux, and butler-console.

These improvements addressed observability and propagation speed. This ADR addresses the root cause: the allocation trigger model.

### Root problem

The current system makes growth and shrink decisions based on speculative accounting. It mixes management-side state (IPAllocation.Spec.Count) with tenant-side state (LB Service counts) and triggers allocations based on arithmetic projections rather than observed demand. When the arithmetic has no stable equilibrium (as with `growthIncrement=1`), the system oscillates.

The analysis behind this ADR is documented in the IPAM Redesign Plan, which includes a full current-state inventory of all IPAM components, a catalog of 10 known problems with severity and frequency assessments, functional and non-functional requirements, architectural premises, and five architecture decisions with recommended options. This ADR synthesizes the allocation trigger, authority model, and state recovery decisions from that plan into a design that will guide Phase 2 implementation.

## Decision

Three architectural decisions, each addressing a distinct failure mode.

### 1. Allocation trigger model: demand-driven

**Replace speculative arithmetic with observed demand.**

Growth fires when at least one LB Service on the tenant cluster is in Pending state without an externalIP (age > 30 seconds, to avoid racing with MetalLB assignment). This is a direct signal that the tenant needs more IPs, not an accounting projection.

Shrink fires when allocated IPs have no matching LB Service for longer than a configurable grace period (default 10 minutes). This means the tenant has more IPs than it is using, sustained over time.

The current speculative computation (`availableIPs = totalAllocated - platformCount - tenantCount`) is removed entirely. No more growth based on projections; growth is triggered by observed demand on the tenant cluster.

**Batch assessment.** A single reconcile counts all Pending LB Services and creates enough growth allocations to cover all of them. This prevents a burst of 5 new services from creating 5 separate growth allocations across 5 reconcile cycles.

**Demand signal source.** The TenantCluster controller already reads tenant LB Service state via `countLBIPs`. The change is in what drives the growth/shrink decision, not in how tenant state is read. The cross-cluster poll of tenant LB Services is consistent with Butler's existing architecture for addon installation, MetalLB sync, and every other management-to-tenant operation Butler performs.

### 2. Authority model: management authoritative, tenant derived

**IPAllocation CRs on the management cluster are the desired state.** MetalLB IPAddressPools on tenant clusters are projections of management state. If they disagree, the controller corrects the MetalLB pool to match management state. Manual edits to `default-pool` on tenants are reverted on next reconcile.

This follows the standard Kubernetes pattern: desired state in CRs, controllers reconcile actual state to match.

**Tenant LB Services drive demand assessment but not authority over IP ranges.** The tenant says "I need IPs" (via a Pending Service); management decides which IPs and tells the tenant (via MetalLB pool update). The decision of which IPs to allocate, from which pool, subject to which quotas, remains entirely on the management side.

**Drift correction.** On every MetalLB sync, the controller computes the expected pool state from IPAllocations and applies it to the tenant. If the tenant pool has been manually edited, the edit is overwritten. Operators who need custom MetalLB pools should create separate IPAddressPool resources with different names, not modify `default-pool`.

The mechanism for this already exists: #85 introduced server-side apply with the `butler-controller/ipam` field manager and `Force: true`, which overwrites fields managed by other actors. This ADR formalizes that behavior as the authority model.

### 3. State recovery: full reconciliation from source with startup sweep

**Every reconcile rebuilds state from the Kubernetes API.** The controller lists IPAllocations, reads tenant Service inventory, rebuilds the bitmap. No cached state persists between reconciles. This is the existing pattern and remains unchanged.

**Startup sweep.** On controller startup, queue an immediate reconcile for all TenantClusters with elastic IPAM enabled rather than waiting for individual requeue timers to fire. For a mature cluster with a 15-minute requeue interval, this means MetalLB state is verified within minutes of a controller restart rather than up to 15 minutes later.

The startup sweep is rate-limited via the existing `MaxConcurrentReconciles` setting (default 5 for TenantCluster). This prevents a thundering herd against the API server.

## Implementation Plan

The demand-driven IPAM changes ship as four sequential PRs in butler-controller. Each builds on the previous but is independently deployable.

**Tenant LB Service inventory** (#89): Extends the existing tenant client with a structured Service inventory. Plumbing only, no behavior change. Required before the demand-driven growth trigger.

**Demand-driven growth** (#90): Replaces the `availableIPs < 1` trigger with "any LB Service Pending without externalIP." Uses the Service inventory from #89. Batch assessment: count all Pending Services, allocate enough IPs for all in one growth event. The old shrink logic stays temporarily.

**Demand-driven shrink** (#91): Replaces arithmetic shrink with "allocated IPs with no matching Service for sustained grace period." This eliminates the phantom growth cycle entirely. Configurable grace period (default 10 minutes).

**Migration hardening**: Ensures existing allocations created under speculative allocation remain valid under demand-driven allocation. Adds PinnedRange protection to the shrink path. Existing clusters are not disrupted during rollout. Idempotent on re-run.

## Validation Strategy

### Before merge

Local validation against the Company 1 management cluster: scale the deployed controller to 0 replicas, run the local controller binary with `--leader-elect=false`, validate, then restore the deployed controller.

Specific validation criteria:

- Zero phantom `-lb-N` allocations created during the validation window. If no growth allocations are created when no LB Services are Pending, the demand-driven trigger is working.
- The 8 existing tenant clusters with their current Allocated state must not be disrupted. All existing IPAllocations remain in Allocated phase. MetalLB pools on tenants remain unchanged.
- Growth fires when it should: create a test LB Service on a tenant (type=LoadBalancer), verify a growth allocation appears within 1-2 reconcile cycles.
- Shrink fires when it should: delete the test LB Service, verify the growth allocation is reclaimed after the grace period.

### After deploy

- **Phantom cycle indicator**: zero `-lb-N` allocations created over a 24-hour period (excluding legitimate demand). This is the primary success metric.
- **IPAllocation count**: stable with no churn. The count should only change when LB Services are created or deleted on tenant clusters.
- **Growth on demand**: create a test LB Service on a tenant, verify growth allocation appears within 1-2 reconcile cycles (30 seconds to 2 minutes).
- **Shrink on surplus**: delete a test LB Service, verify allocation reclaimed after the grace period (10+ minutes).

## Rollback

Each demand-driven IPAM PR ships independently. If the growth trigger (#90) or shrink logic (#91) causes problems, revert to butler-controller v0.18.0. The v0.18.0 behavior (speculative arithmetic with the phantom cycle) resumes, absorbed by the 1Gi memory limit.

Existing allocations remain functional under v0.18.0. Allocations created under speculative allocation are valid CRs under demand-driven allocation. Rolling back does not require reverse migration.

If pure demand-driven shrink proves operationally risky, the documented fallback is hybrid: demand-driven growth with arithmetic shrink using an improved dead zone. See Alternatives Considered below.

## Consequences

### Positive

- **Phantom growth cycle eliminated.** No speculative growth means no speculative shrink. The oscillation disappears entirely.
- **Allocation accuracy improves.** Growth fires on actual demand (a Service needs an IP), not on accounting projections that can have no stable equilibrium.
- **Single source of truth with clear authority.** Management-side IPAllocations are authoritative. Tenant-side MetalLB pools are derived. No ambiguity about which state wins on disagreement.
- **Controller stability improves.** No more cascade of phantom IPAllocation creates/deletes hitting the reclaim grace period simultaneously. Allocation count is stable at rest.
- **Pool exhaustion becomes visible earlier.** Capacity conditions (#86, #87) surface utilization tiers before exhaustion. Combined with demand-driven allocation (which only allocates when needed), pools last longer because speculative allocations no longer waste IPs.

### Negative

- **Tenant API server is a hot dependency for growth decisions.** If a tenant's API server is unreachable, growth stalls for that tenant. The current system already requires tenant API access for `countLBIPs`, so this is the same failure mode, not a new one. Other tenants are unaffected (failure isolation per the requirements).
- **Manual edits to `default-pool` are reverted.** Operators who manually configure tenant MetalLB pools will see their edits overwritten on next reconcile. This must be documented. Operators needing custom pools should create additional IPAddressPool resources with different names.
- **Migration requires careful sequencing.** Existing allocations created under speculative logic must transition cleanly to demand-driven logic without disrupting Ready clusters.
- **Polling latency for growth.** Growth latency depends on the TenantCluster reconcile interval. For a mature cluster (>24h), the worst case before a Pending Service is detected is the requeue interval. The IPAllocation watch (#84) accelerates follow-up reconciles after the growth allocation is fulfilled, but the initial detection still depends on the timer. If this proves unacceptable, a dedicated cross-cluster Service watch could reduce latency further (future work).

### Deferred

- **Multi-pool selection priority**: Proper priority-ordered pool selection for growth allocations when ProviderConfig references multiple pools.
- **Read-back verification with event emission**: Extends the logged-only MetalLB verification from #85 to emit a Kubernetes event on drift detection.
- **Pool exhaustion queueing**: Admission queueing for new TenantClusters when pools are near capacity. Deferred unless pool exhaustion becomes a recurring operational problem despite tiered advance warning from capacity conditions.
- **Drift detection checkpointing**: Persistent snapshots of expected-vs-actual state per tenant. Deferred unless operator-driven drift becomes a recurring support issue.
- **IPv6 support**: All current on-prem providers use IPv4. Explicitly out of scope.
- **Optional butler-ipam-metrics addon**: Prometheus-specific metrics endpoint, PrometheusRule, ServiceMonitor, and Grafana dashboard. Opt-in addon for operators using prometheus-operator. Butler core remains stack-agnostic.

## Alternatives Considered

### Allocation trigger: hybrid (demand-driven growth, arithmetic shrink)

Growth is demand-driven (same as the decision above), but shrink uses improved arithmetic with a dead zone: release allocations when allocated IPs exceed in-use IPs by more than a configurable margin, sustained over a grace period. This avoids the phantom cycle by ensuring the shrink threshold is far enough from the growth threshold that they cannot oscillate.

**Why not chosen as the primary approach:** The dead zone fixes the symptom (oscillation) but preserves the split source of truth for shrink decisions. Management-side accounting still mixes with tenant-side data. Pure demand-driven eliminates the speculative-allocation category of bug entirely.

**Retained as fallback:** If pure demand-driven shrink proves operationally risky (e.g., tenant-side Service inventory is unreliable for shrink decisions), hybrid is the documented fallback. The growth side is identical; only the shrink trigger changes.

### Authority model: tenant authoritative, management derived

Tenant cluster MetalLB pools and LB Services are the source of truth. Management-side IPAllocations are derived from observed tenant state. Reconciliation flows tenant to management.

**Why rejected:** Inverts the platform model. Tenants should not drive platform-level decisions about IP allocation. Management cannot enforce quotas if tenants can manually configure MetalLB beyond Butler's intent. Pool exhaustion is detected after the fact rather than prevented. Pool capacity calculation requires aggregating state from N tenant clusters every reconcile, scaling as O(N) API calls per pool reconcile.

### Authority model: bidirectional with conflict resolution

Both management and tenant can be modified independently. Reconciliation detects conflicts and applies a resolution policy.

**Why rejected:** Distributed consensus for IP allocation is over-engineered for this problem. Conflict resolution policies are notoriously hard to get right and have surprising edge cases. The Kubernetes-native pattern (desired state in CRs, controller reconciles actual to match desired) is simpler and proven.

## References

- [ADR-008: Enterprise Networking and IPAM](ADR-008-enterprise-networking-ipam.md): Established the PVC/PV pattern, bitmap allocator, and NetworkPool/IPAllocation CRDs that this ADR builds on.
- [butler-controller#83](https://github.com/butlerdotdev/butler-controller/issues/83): Tracking issue for the IPAM redesign.
- Foundation PRs: [#84](https://github.com/butlerdotdev/butler-controller/pull/84) (IPAllocation watch), [#85](https://github.com/butlerdotdev/butler-controller/pull/85) (dynamic MetalLB client), [#86](https://github.com/butlerdotdev/butler-controller/pull/86) (tiered events), [#87](https://github.com/butlerdotdev/butler-controller/pull/87) (capacity conditions).
- Demand-driven PRs: [#89](https://github.com/butlerdotdev/butler-controller/pull/89) (Service inventory), [#90](https://github.com/butlerdotdev/butler-controller/pull/90) (demand-driven growth), [#91](https://github.com/butlerdotdev/butler-controller/pull/91) (demand-driven shrink).
