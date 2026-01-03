# ADR-005: Kamaji Integration Controllers

## Status

Accepted

## Context

Butler uses Kamaji for hosted control planes and CAPI for infrastructure management. These components were not designed to work together out of the box, resulting in integration gaps:

1. **Kubeconfig format mismatch**: Kamaji creates kubeconfig secrets with key `admin.conf`, but CAPI Harvester provider expects key `value`
2. **Status synchronization**: CAPI doesn't automatically recognize when Kamaji's TenantControlPlane is ready
3. **Control plane endpoint**: CAPI expects control plane endpoint in a specific status field

We needed to decide how to bridge these gaps:

1. **Upstream fixes**: Contribute fixes to Kamaji and CAPI providers
2. **Wrapper/shim controllers**: Additional controllers to translate between systems
3. **Fork and patch**: Maintain our own forks with fixes
4. **Monkeypatch at runtime**: Use mutating webhooks to fix resources

## Decision

We implement dedicated integration controllers in butler-controller to bridge the gaps:

### KamajiSecretReconciler
- Watches Secrets with label `kamaji.clastix.io/component: admin-kubeconfig`
- Creates a copy with key `value` (CAPI-compatible format)
- Updates copy when source changes

### KamajiStatusReconciler
- Watches Kamaji `TenantControlPlane` resources
- Patches corresponding CAPI `Cluster` status when control plane is ready
- Ensures CAPI recognizes control plane health

These controllers are:
- Clearly documented as workarounds
- Designed to be removable when upstream fixes land
- Isolated from core business logic

## Consequences

### Positive

- Butler works with current upstream releases (no forks required)
- Integration issues are contained in dedicated controllers
- Can be removed cleanly when upstream fixes are available
- Doesn't block progress on upstream timelines
- Clear separation: workarounds don't pollute main controllers

### Negative

- Additional code to maintain
- Must track upstream changes that might conflict
- Users may hit issues if controllers aren't deployed
- Technical debt until upstream fixes land

### Neutral

- Common pattern in Kubernetes ecosystem (operators often bridge gaps)
- Upstream contributions should still be pursued
- Controllers can evolve into proper integration points if pattern proves useful
