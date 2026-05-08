# NetworkPool Infrastructure Allocations — Investigation

## 1. Current State

### NetworkPool CRD types (butler-api)

`butler-api/api/v1alpha1/networkpool_types.go` defines `NetworkPoolStatus` with
capacity-centric fields only: `TotalIPs`, `AllocatedIPs`, `AvailableIPs`,
`AllocationCount`, `FragmentationPercent`, `LargestFreeBlock`, and
`ObservedGeneration`. Conditions cover `Ready`, `CapacityWarning`,
`CapacityCritical`, and `CapacityExhausted`.

There is no field tracking which IPs within reserved ranges are in use by
infrastructure services, nor any mechanism to expose the consuming workload
identity. The status describes the tenant allocation bitmap; the reserved
portion of the pool is invisible from a utilization standpoint.

### Address Map rendering (butler-console)

`IPAddressMap.tsx` defines six `BlockStatus` values: `free`, `reserved`,
`allocated-nodes`, `allocated-lb`, `mixed`, `gateway`. The `classifyIP()`
function checks reserved ranges and tenant allocation ranges. Every IP in a
reserved CIDR is classified as a single `reserved` status — there is no
distinction between "reserved and occupied by a MetalLB Service" and "reserved
and available."

When a user drills into a reserved `/24` block on the Address Map, all 256
cells are the same neutral gray. The tooltip shows "Reserved" with the CIDR
description but no service or workload context.

### MetalLB CRDs on Corteva crop cluster

The management cluster runs MetalLB v0.14.9 in L2 mode. One `IPAddressPool`
named `default-pool` exists in `metallb-system` with two address ranges:

- `10.92.90.7-10.92.90.31` (25 IPs)
- `10.92.91.192-10.92.91.250` (59 IPs)

Both ranges overlap with the NetworkPool's reserved CIDRs. This is by design:
reserved ranges are excluded from tenant allocation, and a subset is given to
MetalLB for management-plane load balancers.

An `L2Advertisement` named `default` advertises `default-pool`. No BGP
configuration exists.

### Assignment state

18 LoadBalancer Services are assigned IPs, all within the first range
(`10.92.90.7` through `10.92.90.24`). Services include Traefik, monitoring
stack components, cert-manager webhook, and platform infrastructure. Each
Service carries the annotation `metallb.io/ip-allocated-from-pool: default-pool`
and has `.status.loadBalancer.ingress[0].ip` with `ipMode: VIP`.

The second range (`10.92.91.192-10.92.91.250`) has zero assignments — all 59
IPs are available to MetalLB but unused.

## 2. Capability Gaps

### Gap A: Reserved infrastructure usage

The Address Map cannot show which reserved IPs are consumed by management-plane
Services. All reserved IPs render identically regardless of whether MetalLB has
assigned them. On the crop cluster, 18 of 84 reserved IPs are actively in use,
but the UI shows 84 homogeneous gray cells.

The platform operator cannot answer "which reserved IPs are still available for
new infrastructure Services?" without running kubectl on the cluster. This
defeats the purpose of the console's Address Map as a single-pane IPAM view.

### Gap B: DHCP allocations

On networks with DHCP scopes (common in enterprise environments), reserved
ranges may contain IPs leased by DHCP to non-Kubernetes devices. Butler has no
mechanism to discover or track DHCP leases. This is an acknowledged limitation
that requires integration with external IPAM systems (e.g., Infoblox, PHPIPAM)
and is out of scope for this work item. The investigation focuses exclusively
on Gap A.

## 3. Architectural Options

### Option A: butler-server reads MetalLB directly

butler-server would query each tenant cluster's MetalLB `IPAddressPool` and
cross-reference Service IPs at request time, returning enriched data in the
`GET /api/networks/:namespace/pools/:name` response.

**Rejected.** This violates ADR-002 (CRDs-as-API). butler-server is a stateless
gateway that reads CRDs from the management cluster. It has no cross-cluster
client, no kubeconfig material for tenant clusters, and adding that capability
would break the clean separation between the API gateway and the reconciliation
plane. It would also create a latency dependency on tenant cluster availability.

More fundamentally, even the management-cluster MetalLB state lives on the
management cluster itself, not on tenant clusters — but the principle holds:
aggregation logic belongs in butler-controller, surfaced via CRD status.

### Option B: butler-controller reconciler (chosen)

A new reconciler in butler-controller watches Services and MetalLB
`IPAddressPool` CRs on the management cluster, cross-references their assigned
IPs against NetworkPool reserved ranges, and writes an
`InfrastructureAllocations` slice to `NetworkPoolStatus`. butler-server returns
this data passively via its existing `GET` handler (it already returns full
`pool.Object`). butler-console reads the new status field and renders reserved
IPs with occupancy distinction.

This follows the established pattern: controller aggregates, CRD carries state,
server proxies, console renders.

## 4. CRD Change

Add to `NetworkPoolStatus`:

```go
// InfrastructureAllocation represents a single infrastructure IP consumed
// from a reserved range by a management-cluster resource (e.g., a MetalLB
// load balancer Service).
type InfrastructureAllocation struct {
    // IP is the individual IP address allocated to infrastructure.
    // +kubebuilder:validation:Required
    // +kubebuilder:validation:Pattern=`^(\d{1,3}\.){3}\d{1,3}$`
    IP string `json:"ip"`

    // Source identifies the infrastructure system that owns this IP.
    // Examples: "metallb", "dhcp", "static".
    // +kubebuilder:validation:Required
    Source string `json:"source"`

    // ServiceRef references the Kubernetes Service consuming this IP.
    // Only populated when Source is "metallb".
    // +optional
    ServiceRef *NamespacedObjectReference `json:"serviceRef,omitempty"`
}
```

No `Description` field on the per-allocation entry. The description belongs to
the reserved range (`NetworkPool.spec.reserved[].description`) and should not
be duplicated on every individual IP. The console joins on CIDR membership at
render time to display the reserved range description alongside each
infrastructure allocation.

Add field to `NetworkPoolStatus`:

```go
// InfrastructureAllocations lists IPs within reserved ranges that are
// consumed by management-plane infrastructure (MetalLB Services, etc.).
// Populated by the infrastructure-allocation reconciler.
// +optional
// +listType=map
// +listMapKey=ip
InfrastructureAllocations []InfrastructureAllocation `json:"infrastructureAllocations,omitempty"`
```

`ServiceRef` uses `NamespacedObjectReference` (defined in `common_types.go`),
consistent with `IPAllocationSpec.TenantClusterRef`. This allows the console to
render the Service name/namespace without additional API calls.

The `+listType=map` with `+listMapKey=ip` enables SSA merge semantics on the
IP key, preventing duplicate entries during concurrent reconciles.

## 5. butler-controller Reconciler Design

### Separate reconciler vs. extending NetworkPool reconciler

The existing NetworkPool reconciler (`networkpool_controller.go`) is the IPAM
allocator. Its reconcile loop processes pending IPAllocations, computes tenant
capacity, and manages finalizers. Adding MetalLB cross-referencing would
conflate two concerns (tenant allocation vs. infrastructure discovery) and
make the reconcile loop harder to reason about.

A separate reconciler for infrastructure allocations is cleaner. It has its own
watch triggers, its own RBAC annotations, and its own failure mode. If MetalLB
is not installed, the reconciler is a no-op. If it fails, tenant allocation
continues unaffected.

### Reconciler structure

```
internal/controller/networkpool/infra_allocation_controller.go
```

Reconciler struct:

```go
type InfraAllocationReconciler struct {
    client.Client
    Scheme *runtime.Scheme
}
```

### Watches and predicates

Primary: `For(&butlerv1alpha1.NetworkPool{})` — reconcile when pool spec
changes (reserved ranges modified).

Secondary watches via `Watches()`:

1. **`corev1.Service`** — filtered with a custom predicate that compares old
   and new `status.loadBalancer.ingress` slices and only enqueues when the
   slice changes. A generation predicate is incorrect here: Service generation
   bumps only on spec changes, but the trigger we care about is the IP being
   assigned or revoked, which lives in status. The custom predicate must
   compare `event.ObjectOld.Status.LoadBalancer.Ingress` to
   `event.ObjectNew.Status.LoadBalancer.Ingress` and return true only when
   they differ.

   The `EnqueueRequestsFromMapFunc` mapper lists all NetworkPools (cheap — there
   are few per management cluster, typically 1-5) and enqueues every pool whose
   reserved ranges contain the Service's LB IP. This is approach (a) from the
   design options: full list of NetworkPools per mapper call. Approach (b),
   field-indexing NetworkPool reserved CIDRs, adds complexity without
   measurable benefit at current scale. If we ever exceed ~50 NetworkPools per
   management cluster, revisit the mapper strategy.

2. **MetalLB `IPAddressPool`** (optional, future) — if MetalLB IPAddressPool
   address ranges change, the pool of possible infrastructure IPs changes. For
   the initial implementation, this is not needed because the MetalLB pool
   ranges are a subset of NetworkPool reserved ranges, which are already
   watched via the primary trigger.

### 5a. Service deletion handling

When a Service that previously held an IP is deleted, the mapper triggers on
the delete event and enqueues the relevant NetworkPool(s). The reconcile loop
recomputes the desired `InfrastructureAllocations` slice from current LB
Services only — it never reads from prior status. The deleted Service's IP
naturally drops out of the desired slice, and the status patch removes the
stale entry. Unit test required: "Service deleted, IP entry removed from
status."

### 5b. IP move handling

If a Service's LB IP changes (e.g., MetalLB reassigns), both the old and new
NetworkPool need to be enqueued. The mapper sees only the new state in a
typical watch Update event via `event.ObjectNew`. Mitigation: in the mapper,
access both `event.ObjectOld` and `event.ObjectNew`, extract IPs from both,
and enqueue all NetworkPools whose reserved ranges contain either the old or
new IP. This ensures the old pool removes the stale entry and the new pool
adds the new entry. Unit test required: "Service IP changes, old pool drops
entry, new pool gains entry."

### 5c. IPs outside reserved ranges

When a Service's LB IP falls outside all reserved ranges of all NetworkPools,
the reconciler produces no status entry for that IP. This is expected — normal
LB Services that consume IPs from non-reserved pool segments (tenant
allocations) are already tracked by the tenant IPAM system. The reconciler
does not error, does not log a warning, and produces no status entry. This is
the common case for tenant-facing Services.

### 5d. Interaction with existing NetworkPool reconciler

Both reconcilers patch `NetworkPool.status`. They write to different fields:
the existing reconciler writes capacity fields (`TotalIPs`, `AllocatedIPs`,
`AvailableIPs`, etc.) and conditions; the new reconciler writes
`InfrastructureAllocations` only. At the field level they do not conflict,
but they will race on the resource version.

The existing NetworkPool reconciler uses plain `r.Status().Update()` (not
SSA). It does not set a field manager. The new reconciler should use SSA
with a distinct field manager name
(`"butler-controller/infra-allocation"`) to claim ownership of only the
`InfrastructureAllocations` field. This eliminates resource version
conflicts — SSA does not require a resource version, and field ownership
prevents one controller from accidentally clearing another's fields.

Confirm the existing controller's update mechanism in the implementation PR
and verify that the two controllers do not interfere. If the existing
controller is later migrated to SSA, it should use a different field manager
name (e.g., `"butler-controller/networkpool"`).

### Reconcile loop

1. Fetch the NetworkPool.
2. Parse all reserved ranges into IP sets.
3. List all Services in the cluster with `spec.type=LoadBalancer`.
4. For each Service with a `.status.loadBalancer.ingress[].ip`:
   a. Check if the IP falls within any reserved range.
   b. If yes, build an `InfrastructureAllocation` entry with `Source: "metallb"`
      and `ServiceRef` pointing to the Service.
   c. If the IP does not fall within any reserved range, skip silently (5c).
5. Sort entries by IP (deterministic output, enables diff comparison).
6. Compare desired slice to current `pool.Status.InfrastructureAllocations`.
   If equal, skip the patch entirely. Status writes are the most common cause
   of hot loops — the no-op case must be explicitly tested. Unit test required:
   "no change in LB Services, no status patch issued."
7. If different, patch `NetworkPoolStatus.InfrastructureAllocations` via SSA
   with field manager `"butler-controller/infra-allocation"`.

### Field indexer

Since the number of LB Services on a management cluster is typically small
(10-50), a full list + in-memory filter is acceptable for the initial
implementation. No field indexer on Services is needed.

### RBAC additions

```go
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
```

The existing `role.yaml` does not include Services. This is the only RBAC
addition needed — MetalLB IPAddressPool CRs are read via the dynamic client
in `installer.go`, but for this reconciler we only need core Services since
the IP is sourced from `Service.status.loadBalancer.ingress`.

### Registration in main.go

```go
// Infrastructure allocation controller. Discovers reserved IP usage
if err = (&networkpool.InfraAllocationReconciler{
    Client: mgr.GetClient(),
    Scheme: mgr.GetScheme(),
}).SetupWithManager(mgr); err != nil {
    setupLog.Error(err, "unable to create controller", "controller", "InfraAllocation")
    os.Exit(1)
}
```

## 6. butler-api Types Update

Changes in `butler-api/api/v1alpha1/networkpool_types.go`:

1. Add `InfrastructureAllocation` struct (see section 4 — no Description field).
2. Add `InfrastructureAllocations` field to `NetworkPoolStatus`.
3. Run `make generate` (controller-gen) to regenerate `zz_generated.deepcopy.go`.
   Verify the generated deepcopy for `InfrastructureAllocation` correctly
   handles the `*NamespacedObjectReference` pointer field (must nil-check and
   copy, not shallow-assign).
4. Run `make manifests` to regenerate CRD YAML.
5. Tag new butler-api release (minor bump since this is an additive status field).
6. Update `go.mod` in butler-controller to reference new butler-api version.
7. Copy regenerated CRDs to butler-crds (version bump and chart release).

The change is purely additive — existing controllers and consumers that do not
read `InfrastructureAllocations` are unaffected. No migration is needed;
clusters running older butler-controller versions will have `null` for the
field, which serializes as omitted JSON.

## 7. butler-console Rendering Changes

### New BlockStatus values

Add two new statuses to `IPAddressMap.tsx`:

```typescript
type BlockStatus =
  | 'free'
  | 'reserved'
  | 'reserved-occupied'   // new: reserved IP in use by infrastructure
  | 'reserved-available'  // new: reserved IP not in use
  | 'allocated-nodes'
  | 'allocated-lb'
  | 'mixed'
  | 'gateway'
```

When `infrastructureAllocations` is available in the pool status, `classifyIP()`
splits the current `reserved` status:
- IP is in reserved range AND in `infrastructureAllocations` → `reserved-occupied`
- IP is in reserved range AND NOT in `infrastructureAllocations` → `reserved-available`

When `infrastructureAllocations` is absent (older controller), fall back to
the current `reserved` status (undifferentiated gray).

### 7a. Empty state for Infrastructure Usage panel

When `infrastructureAllocations` is present but empty (controller deployed,
zero LB Services with IPs in reserved ranges), the panel renders with
explanatory text: "No infrastructure services currently consume reserved IPs
in this pool." This follows the pattern used by the Reserved Ranges card,
which renders "No reserved ranges" rather than disappearing. Operators need
to see that the feature is active and reporting "none" — disappearing implies
the feature is missing or broken.

### 7b. Older controller compatibility

Two distinct fallback cases, each with an explicit unit test:

1. `infrastructureAllocations: undefined` (controller predates this feature):
   renders identically to today. All reserved IPs show as undifferentiated
   `reserved` status. No Infrastructure Usage panel. Test verifies rendering
   matches the pre-feature baseline.

2. `infrastructureAllocations: []` (controller deployed, no entries): reserved
   IPs show as `reserved-available`. Infrastructure Usage panel renders with
   empty-state text. Test verifies the panel appears and the status split is
   active (all reserved IPs are `reserved-available`, not `reserved`).

### Color scheme — two alternates for review

Amber/orange is rejected for `reserved-occupied`: it reads as a warning state
in the console (CapacityWarning condition uses amber, mixed allocation status
uses amber). Reserved-occupied is informational, not a warning. Two alternates
are proposed for visual validation in the console before committing:

**Alternate A: Teal fill on neutral base**

| Status | Style | Rationale |
|--------|-------|-----------|
| `reserved-occupied` | `bg-teal-600/40 border-teal-500/30` | Distinct from amber (warning), blue (nodes), purple (LB). Teal is unused in the current palette. Reads as "infrastructure in use" without alarm. |
| `reserved-available` | `bg-neutral-700/40 border-neutral-600/30` | Same as current `reserved`. |

**Alternate B: Neutral with density marker**

| Status | Style | Rationale |
|--------|-------|-----------|
| `reserved-occupied` | `bg-neutral-600/60 border-neutral-500/40` + small inner dot (`bg-neutral-300`) | Darker neutral than `reserved-available`, with a centered dot to indicate occupancy. Avoids introducing a new hue entirely. Subtler visual distinction — the dot provides the signal without competing with allocation colors. |
| `reserved-available` | `bg-neutral-700/40 border-neutral-600/30` | Same as current `reserved`. |

Decision: defer to visual validation. Both alternates will be implemented as
toggleable options during the console development phase. Screenshots of both
against the crop cluster's Address Map (which has 18 occupied out of 84
reserved) will be captured and reviewed before the console release.

### Tooltip enrichment

When hovering over a `reserved-occupied` IP, the tooltip shows:
- IP address
- Status: "Reserved (In Use)"
- Service: `{namespace}/{name}`
- Reserved range description (joined from `pool.spec.reserved[]` by CIDR
  membership, not from the allocation entry)

### New prop on IPAddressMap

```typescript
interface IPAddressMapProps {
    cidr: string
    reserved?: Array<{ cidr: string; description?: string }>
    allocations: Array<{...}>
    infrastructureAllocations?: Array<{  // new
        ip: string
        source: string
        serviceRef?: { name: string; namespace: string }
    }>
}
```

`NetworkPoolDetailPage.tsx` passes `pool.status?.infrastructureAllocations`
as the new prop.

### Infrastructure Usage panel wireframe

The panel sits between "Reserved Ranges" and "Tenant Allocation Defaults" in
the two-column grid on `NetworkPoolDetailPage.tsx`:

```
┌─ Infrastructure Usage ──────────────────────────────────────────┐
│                                                                  │
│  IP               Source     Service                             │
│  ────────────     ───────    ────────────────────────────         │
│  10.92.90.7       metallb    metallb-system/traefik              │
│  10.92.90.8       metallb    monitoring/vmagent-lb               │
│  10.92.90.9       metallb    monitoring/vmselect-lb              │
│  10.92.90.10      metallb    cert-manager/cert-manager-webhook   │
│  ...                                                             │
│  10.92.90.24      metallb    butler-system/butler-console        │
│                                                                  │
│  ─────────────────────────────────────────────────────────────── │
│  18 of 84 reserved IPs in use (21%)                             │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

Columns: IP (monospace, left-aligned), Source (badge-style, e.g., teal pill
with "metallb"), Service (namespace/name, plain text for system namespaces).

Footer: summary line showing count of occupied vs total reserved IPs with
percentage.

Empty state (7a):

```
┌─ Infrastructure Usage ──────────────────────────────────────────┐
│                                                                  │
│  No infrastructure services currently consume reserved IPs       │
│  in this pool.                                                   │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### NetworkLayoutBar impact

`NetworkLayoutBar.tsx` and `computePoolLayout` are not affected. The layout bar
already renders reserved segments correctly. Infrastructure occupancy is a
drill-down concern, not a layout-bar concern. The layout bar shows the macro
structure (gateway, reserved, tenant-allocated, tenant-available, unassigned);
the Address Map shows the micro structure (individual IPs with their status).

## 8. butlerlabs-docs Additions

### Concepts page update

The existing IPAM concepts page (`docs/concepts/networking/ipam.md`) should add
a subsection explaining that reserved ranges can contain infrastructure
allocations, and that butler-controller automatically discovers MetalLB LB
Service IPs within reserved ranges.

### Admin guide update

The NetworkPool admin guide (`docs/admin/network-pools.md`) should add:

1. A section on the Infrastructure Usage panel in the console UI.
2. An explanation of what "reserved-occupied" vs "reserved-available" means.
3. A note that DHCP allocations are not tracked and remain a known limitation.

### API reference update

If auto-generated from CRD markers: regeneration covers it. If manually
maintained: add `InfrastructureAllocation` type and the new status field.

## 9. LOC Estimate

| Repository | Scope | Estimated LOC |
|---|---|---|
| butler-api | `InfrastructureAllocation` type + `NetworkPoolStatus` field | ~35 |
| butler-api | Generated deepcopy changes | ~30 (auto) |
| butler-crds | CRD YAML regeneration | ~60 (auto) |
| butler-controller | `infra_allocation_controller.go` + envtest coverage | ~400-500 |
| butler-controller | RBAC additions | ~5 |
| butler-controller | `main.go` registration | ~10 |
| butler-console | `IPAddressMap.tsx` status split + rendering | ~80 |
| butler-console | `NetworkPoolDetailPage.tsx` infrastructure panel | ~60 |
| butler-console | Types update + unit tests (7b compatibility) | ~40 |
| butlerlabs-docs | Concept + admin guide updates | ~80 |
| **Total** | | **~800-900** |

The heaviest item is the controller reconciler + envtest coverage, which
includes tests for: no-op status skip, Service deletion, IP move, IPs outside
reserved ranges, and the basic happy path.

## 10. Implementation Order

1. **butler-api**: Add `InfrastructureAllocation` type and `NetworkPoolStatus`
   field. Tag release. This unblocks both the controller and console work.

   **Gate**: butler-api must be tagged and pushed before the controller branch
   is even created. The controller PR cannot reference an unreleased butler-api
   version in `go.mod` — `go mod tidy` will fail if the tag doesn't exist in
   the remote. Open the butler-api PR, merge, tag, push tag, verify the tag is
   fetchable via `go list -m`, then proceed.

2. **butler-crds**: Regenerate CRDs from updated butler-api. Tag chart release.
   Management clusters need the updated CRD before the controller can write to
   the new status field.

3. **butler-controller**: Implement `InfraAllocationReconciler`, add RBAC, add
   tests, register in `main.go`. Tag release. Depends on butler-api being
   released so `go.mod` can reference the new types.

4. **butler-console**: Add `reserved-occupied` / `reserved-available` rendering,
   infrastructure panel, tooltip enrichment. Tag release. Can start in parallel
   with the controller work (types are known from butler-api), but the visual
   validation requires the controller to be deployed and populating the status.

5. **butlerlabs-docs**: Update concept and admin pages. Can start any time after
   butler-api types are finalized.

6. **Corteva GitOps bumps**: Strict ordering with verification gates:

   a. **butler-crds chart bump** — update HelmRelease to new chart version
      containing the `InfrastructureAllocations` CRD field. Wait for Flux
      reconciliation. Verify: `kubectl explain networkpool.status.infrastructureAllocations`
      must return the field definition. Do not proceed until this verification
      passes — if the controller deploys before the CRD has the new field, the
      API server silently drops the status writes.

   b. **butler-controller image bump** — update HelmRelease to new controller
      image containing the `InfraAllocationReconciler`. Wait for Flux
      reconciliation and pod rollout. Verify: `kubectl get networkpool -o json`
      shows `infrastructureAllocations` populated in status.

   c. **butler-console image bump** — update HelmRelease to new console image
      with reserved-occupied rendering. Wait for Flux reconciliation and pod
      rollout. Visual validation at butler-crop.phibred.com.

### Rationale

The ordering follows the dependency graph: types → CRDs → controller →
console → docs → deployment. butler-api and butler-crds must be released
before the controller can compile and before the CRD on-cluster allows the new
status field. The console can be developed in parallel using the known type
shapes but cannot be visually validated until the controller is deployed and
producing data. The explicit gates between Corteva GitOps bumps prevent silent
data loss from CRD/controller version skew.
