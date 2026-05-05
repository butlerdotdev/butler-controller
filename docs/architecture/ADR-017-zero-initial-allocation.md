# ADR-017: Zero Initial Allocation for Ingress-Disabled Elastic Tenants

## Status

Proposed

## Date

2026-05-03

## Context

Butler's IPAM system (ADR-008) allocates a block of LB IPs to every tenant during the Provisioning phase. For elastic-mode providers (ADR-016), the default initial allocation is 2 IPs. This allocation happens unconditionally in `getInitialLBPoolSize`, which checks TenantCluster overrides, ProviderConfig settings, and global defaults, but never checks `spec.addons.ingress.enabled`.

When a tenant cluster has `ingress.enabled: false`, no ingress controller is installed, no platform LB Service is created, and no LB IPs are consumed. Yet 2 or more IPs are allocated from the shared NetworkPool and sit idle indefinitely. Initial allocations are labeled `AllocationRoleInitial` and are never released by elastic shrink.

When tenants are configured without ingress controllers, the default initial allocation creates idle IPs. At scale, this reduces effective pool capacity and limits tenant density.

The demand-driven IPAM model (ADR-016) provides the mechanism to resolve this: growth allocations fire when LB Services are Pending without an external IP. If initial allocation is zero for ingress-disabled tenants, the elastic growth path can create allocations when demand materializes -- whether the operator enables ingress later, or tenant workloads create their own LB Services.

### Constraint: the zero-allocation guard

`reconcileElasticIPAM` has an early return: `if len(allocs) == 0 { return nil }`. Its purpose was to avoid elastic growth running before the initial allocation path has had a chance to execute during Provisioning. However, it also permanently blocks demand-driven growth for any Ready tenant with zero allocations. Since `reconcileElasticIPAM` already gates itself on `tc.Status.Phase == Ready`, the guard is redundant for its stated purpose and harmful for the ingress-toggle use case.

## Decision

### 1. Implicit zero from ingress configuration (elastic mode only)

In elastic IPAM mode, `getInitialLBPoolSize` returns 0 when `spec.addons.ingress.IsIngressEnabled()` returns false and no explicit `spec.networking.lbPoolSize` override is set.

The TenantCluster `lbPoolSize` override takes precedence. This preserves the escape hatch for operators who disable the default ingress controller but still want pre-allocated LB IPs for custom services or alternative ingress implementations.

Static IPAM mode is not affected. Static mode has no growth path, so zero initial allocation would leave a tenant permanently without LB IPs. The zero-from-ingress optimization applies only to elastic mode where the demand-driven growth mechanism can allocate IPs when demand materializes.

### 2. Zero-count short circuit in initial allocation

When `getInitialLBPoolSize` returns 0, `reconcileIPAllocation` marks `NetworkReady` as true immediately and skips IPAllocation creation entirely. This allows provisioning to proceed without creating an empty allocation object.

### 3. Guard removal in elastic growth

The `if len(allocs) == 0 { return nil }` guard in `reconcileElasticIPAM` is removed. The existing Ready phase gate prevents elastic growth from racing with initial allocation during Provisioning. A Ready cluster with zero allocations and zero LB demand gets a no-op reconcile (no growth triggers, no shrink targets, MetalLB sync returns early on empty ranges).

### 4. No CRD schema changes

The `minimum: 1` constraints on `lbPoolSize` (TenantCluster) and `initialPoolSize` (ProviderConfig) remain unchanged. The zero is computed implicitly by the controller from existing configuration, not set by the user. No butler-api changes, webhook changes, or CRD regeneration required.

## Implementation Plan

All changes are confined to a single file (`internal/controller/tenantcluster/reconcile_ipam.go`) plus tests.

**`getInitialLBPoolSize`**: Insert an ingress check after the TC override but before the ProviderConfig lookup. Checks `r.isElasticIPAM(pc)` and `!tc.Spec.Addons.Ingress.IsIngressEnabled()`.

**`reconcileIPAllocation`**: Add a zero-count check after `lbCount := r.getInitialLBPoolSize(tc, pc)`. When `lbCount == 0`, set the `NetworkReady` condition to true and return.

**`reconcileElasticIPAM`**: Remove lines 195-197 (the `len(allocs) == 0` early return).

## Validation Strategy

### Before merge

Unit tests covering:

- `getInitialLBPoolSize` returns 0 for ingress-disabled elastic tenants
- `getInitialLBPoolSize` respects TC override even when ingress is disabled
- `getInitialLBPoolSize` returns the normal default for static-mode tenants with ingress disabled
- `reconcileIPAllocation` returns ready with no IPAllocation created when pool size is 0
- `reconcileElasticIPAM` does not return early on zero allocations for Ready clusters

Full existing test suite passes without regression.

### After deploy

- Ingress-disabled tenants provisioned after the change have zero IPAllocations and `NetworkReady=True`.
- Existing tenants (including those with ingress disabled and existing allocations) are unaffected. Allocations remain in place.
- Toggling ingress to enabled on a previously-disabled tenant results in Traefik installation, a Pending LB Service, and a demand-driven growth allocation within one reconcile cycle.

## Rollback

Revert the three changes in `reconcile_ipam.go`. The old guard returns and all tenants behave as before. No IPAllocations are affected because the change only prevents creation during Provisioning and removes a guard that was blocking growth for zero-allocation tenants. Reverting restores the previous behavior without data migration.

## Consequences

### Positive

- Ingress-disabled tenants on elastic providers no longer consume LB IPs from shared pools at provisioning time.
- No operator action required. Existing `ingress.enabled: false` configuration automatically results in zero initial allocation.
- Backward compatible. All existing tenants (including those with ingress disabled and existing allocations) retain their current allocations and behavior.
- The ingress-toggle use case works through the demand-driven growth path. A tenant that enables ingress after provisioning gets IPs allocated via elastic growth when the LB Service appears.

### Negative

- When ingress is enabled on a previously-disabled Ready cluster, there is a latency window (up to one reconcile interval plus the 30-second pending threshold) before Traefik gets an IP. This is acceptable for a configuration change but should be visible to operators via the standard capacity events.
- The removal of the `len(allocs) == 0` guard is a behavioral change for an edge case that may not have been exercised: a Ready cluster with zero allocations due to a bug or manual deletion. Previously this was silently ignored; now it may trigger growth allocations if there are Pending LB Services. This is the correct behavior.

### Deferred

- Deallocation when ingress is disabled on an existing tenant. Currently Butler does not uninstall addons when their spec is toggled off. Implementing IP release on disable requires an explicit uninstall path and is separate, riskier work.
- IPv6 pool support. Out of scope (consistent with ADR-016).

## Alternatives Considered

### Explicit `initialPoolSize: 0` (Path B)

Add support for setting `initialPoolSize: 0` on ProviderConfig (or `lbPoolSize: 0` on TenantCluster) by removing the `minimum: 1` CRD validation constraint. Requires CRD schema changes in butler-api, webhook validation updates, CRD regeneration, and operator documentation.

Rejected because it adds complexity without proportional benefit. The implicit approach requires zero operator action -- ingress-disabled tenants already express "no platform LB services needed" through their configuration. Requiring a separate explicit zero adds operational burden and a new configuration to document and maintain.

### Both implicit and explicit (Path C)

Combine implicit zero from ingress configuration with explicit `initialPoolSize: 0` support.

Rejected because the explicit override is already available via the TC `lbPoolSize` field (which takes precedence in the implementation). Adding a separate zero-capable `initialPoolSize` path duplicates functionality that `lbPoolSize` already provides.

## References

- [ADR-008: Enterprise Networking and IPAM](ADR-008-enterprise-networking-ipam.md): Established the NetworkPool/IPAllocation CRD model and initial allocation path.
- [ADR-016: Demand-Driven IPAM](ADR-016-demand-driven-ipam.md): Replaced speculative allocation triggers with demand-driven growth and shrink. Provides the growth mechanism that enables zero initial allocation.
