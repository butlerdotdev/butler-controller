# ADR-008: Enterprise Networking and IPAM

## Status

Accepted

## Context

Butler's networking model was entirely manual: platform admins specified LoadBalancer IP ranges per TenantCluster in `spec.networking.loadBalancerPool`. This works for small deployments but breaks down at scale:

- **No IP tracking**: With 50+ tenants, admins resort to spreadsheets to avoid IP conflicts
- **No provider scoping**: All ProviderConfigs are platform-wide, no access control per team
- **No quota enforcement**: Teams can consume unlimited resources
- **No cloud provider support**: Only on-prem providers (Harvester, Nutanix, Proxmox)

We needed automated IPAM for on-prem providers, provider scoping and limits, and a foundation for cloud provider support.

## Decision

### IPAM Architecture

We follow the **PVC/PV provisioner pattern** from Kubernetes core:

1. **NetworkPool** (cluster-scoped pool definition) is the "StorageClass"
2. **IPAllocation** (individual allocation record) is the "PVC/PV"
3. **NetworkPool controller** is the sole allocator (single writer per pool)

This prevents double-allocation by design. Controller-runtime's work queue serialization ensures only one reconcile runs per NetworkPool at a time. No distributed locks needed.

### Allocation Algorithm

Best-fit allocation using a bitmap:

1. Build bitmap of the entire allocatable range
2. Mark reserved CIDRs and existing allocations as used
3. Find all contiguous free blocks
4. Select the smallest block that fits the request (best-fit)

Best-fit minimizes fragmentation compared to first-fit by preventing large blocks from being consumed by small requests.

### Pinned Allocations

For migration scenarios, IPAllocations can specify a `pinnedRange` with exact start/end addresses. The allocator validates the range is within the pool, not reserved, and not already allocated.

### Provider Scoping

ProviderConfigs support `scope: {type: platform|team, teamRef: ...}`. Platform-scoped providers are available to all teams. Team-scoped providers restrict access to the referenced team. Enforcement happens in both the admission webhook (fail-fast) and the controller reconcile loop (defense-in-depth).

### Provider Limits

ProviderConfigs support `limits: {maxClustersPerTeam, maxNodesPerTeam}`. Enforced at admission time and during reconciliation.

## Alternatives Considered

### IPAM as a separate service (rejected)

Running IPAM as a gRPC service adds operational complexity without benefit. The Kubernetes API already provides persistence, watches, and RBAC. A controller-based approach is simpler and follows established patterns (cert-manager, MetalLB).

### Annotation-based IP tracking (rejected)

Storing allocations as annotations on NetworkPool would create a single large object that's rewritten on every change. At 500+ tenants, this becomes a performance bottleneck. Separate IPAllocation resources scale better and enable standard `kubectl get ipa` queries.

### First-fit allocation (rejected)

First-fit is simpler but leads to fragmentation. After several allocate/free cycles, large free blocks get split by small allocations. Best-fit keeps large blocks available for large requests.

## Consequences

### Positive
- Zero manual IP management for new deployments
- Existing deployments migrate incrementally (all new fields are optional)
- Prometheus metrics expose pool utilization for alerting
- Events provide audit trail for IP allocation/deallocation
- Pinned allocations enable migration from legacy manual setup

### Negative
- NetworkPool controller is a single point of allocation (by design for safety, but if it crashes, new allocations stall until leader election recovers)
- Pool capacity warnings emit every reconcile cycle (60s) when above threshold. Rate limiting should be added if noisy.

### Metrics Emitted
- `butler_network_pool_total_ips`: total IPs per pool
- `butler_network_pool_allocated_ips`: allocated IPs per pool
- `butler_network_pool_available_ips`: available IPs per pool
- `butler_network_pool_fragmentation_percent`: free space fragmentation
- `butler_ip_allocation_processed_total`: allocation counter by result
- `butler_provider_config_ready`: provider readiness gauge
- `butler_provider_config_available_ips`: available IPs per provider

### Events Emitted
- `PoolCapacityWarning`: pool > 80% utilized
- `PoolCapacityDanger`: pool > 90% utilized
- `PoolExhausted`: pool 100% utilized
- `IPsAllocated`: successful IP allocation
- `AllocationFailed`: failed allocation attempt
- `CredentialsInvalid`: provider credential validation failed
- `PoolsExhausted`: all pools for a provider exhausted
