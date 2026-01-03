# ADR-007: Hybrid Addon Management Model

## Status

Accepted

## Context

Butler manages addons in tenant clusters. We needed to decide how users specify and manage addons:

1. **Inline only**: All addons defined in `TenantCluster.spec.addons`
2. **Separate CRs only**: All addons as individual `TenantAddon` CRs
3. **Hybrid**: Initial addons inline, additional/managed addons as CRs

Considerations:
- Initial addons (CNI, load balancer) are essential and should be simple to specify
- Post-creation addons may need more configuration and lifecycle management
- Some addons should never be removed (monotonic), others need full lifecycle
- GitOps users may want to manage addons via Flux/ArgoCD

## Decision

We implement a hybrid model with two addon management mechanisms:

### TenantCluster.spec.addons (Bootstrap Addons)
- Specified inline in TenantCluster spec
- Installed at cluster creation time
- **Monotonic**: Butler only adds, never removes (see ADR-001)
- Best for: CNI, load balancer, cert-manager, storage
- Simple: version pinning, basic customization

```yaml
spec:
  addons:
    cni:
      provider: cilium
      version: "1.16.0"
    loadBalancer:
      provider: metallb
      version: "0.14.5"
```

### TenantAddon CR (Managed Addons)
- Separate namespaced resource
- CR lifecycle = addon lifecycle (deletion removes addon)
- Can be created anytime after cluster is ready
- **Full lifecycle**: Add, upgrade, remove
- Best for: Optional addons, custom Helm charts, team-specific tools

```yaml
apiVersion: butler.butlerlabs.dev/v1alpha1
kind: TenantAddon
metadata:
  name: production-argocd
  namespace: platform-engineering
spec:
  clusterRef:
    name: production
  addon: argocd
  version: "2.10.0"
```

Key insight: `TenantCluster.spec.addons` answers "what should exist", while `TenantAddon` answers "manage this addon's full lifecycle".

## Consequences

### Positive

- Simple path for common case (essential addons inline)
- Full control when needed (TenantAddon)
- Clear semantic difference between bootstrap and managed addons
- Monotonic safety for critical addons (CNI, storage)
- GitOps friendly: TenantAddon CRs work well with Flux/ArgoCD

### Negative

- Two ways to manage addons may confuse users initially
- Documentation must clearly explain when to use which
- Status reporting differs between the two mechanisms

### Neutral

- Users can choose the model that fits their workflow
- Migration between models is possible but not automatic
- Pattern similar to Kubernetes core resources vs operators
