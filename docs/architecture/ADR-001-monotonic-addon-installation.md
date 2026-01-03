# ADR-001: Monotonic Addon Installation

## Status

Accepted

## Context

Butler installs addons (CNI, load balancer, storage, etc.) into tenant clusters. We needed to decide how to handle addon lifecycle when users modify `TenantCluster.spec.addons`:

1. **Full sync**: Butler ensures cluster state matches spec exactly (add and remove)
2. **Monotonic**: Butler only adds addons, never removes them
3. **Immutable**: Addons are fixed at cluster creation, no changes allowed

The key concern is safety. Removing addons is destructive:
- Removing Longhorn deletes PersistentVolumes and data
- Removing MetalLB orphans LoadBalancer service IPs
- Removing Cilium breaks all cluster networking

Additionally, users may adopt GitOps (Flux/ArgoCD) to manage their clusters. Full sync would cause Butler and GitOps tools to fight over addon state.

## Decision

We implement monotonic addon installation for `TenantCluster.spec.addons`. Butler only **adds** addons, never **removes** them. The set of Butler-installed addons can only grow or stay the same.

Timeline example:
```
T0: TenantCluster created with [cilium, metallb, longhorn] → Butler installs all
T1: User adds traefik to spec → Butler installs traefik
T2: User removes longhorn from spec → Butler does NOTHING (no removal)
T3: User manually removes longhorn (kubectl/helm) → Butler updates status only
```

For explicit addon removal, users can:
1. Use `TenantAddon` CR (deletion removes the addon)
2. Manually uninstall via kubectl/helm
3. Use GitOps tooling

## Consequences

### Positive

- Safe by default: no accidental data loss from spec changes
- GitOps compatible: Butler doesn't fight with Flux/ArgoCD
- Clear mental model: `spec.addons` = "things I want Butler to ensure exist"
- Matches Crossplane's adoption policies pattern
- Allows gradual migration to GitOps without disruption

### Negative

- Users cannot declaratively remove addons via TenantCluster spec
- Status may show addons that are no longer in spec
- Requires TenantAddon CR or manual intervention for removal

### Neutral

- Consistent with infrastructure-as-code principle of "create, don't destroy"
- Similar to how Terraform handles `prevent_destroy` lifecycle rules
- Users who need full lifecycle management can use TenantAddon CRs
