# ADR-002: Multi-Tenancy Platform Modes

## Status

Accepted

## Context

Butler targets both enterprise deployments (multi-billion dollar companies with governance requirements) and simpler use cases (demos, home labs, single-team organizations). We needed to decide how to handle multi-tenancy:

1. **Always enforced**: Teams required for all deployments
2. **Always optional**: Teams available but never required
3. **Configurable modes**: Platform admin chooses enforcement level

Enterprise deployments require:
- Clear team ownership for audit trails
- Resource quotas per team
- RBAC boundaries between teams
- Integration with AD/OIDC groups

Simple deployments want:
- Minimal configuration
- No team management overhead
- Quick time to first cluster

## Decision

We implement three platform-wide multi-tenancy modes configured via `ButlerConfig`:

### Enforced (Enterprise Default)
- Teams must exist before creating TenantClusters
- TenantCluster must be in Team's namespace
- butlerctl requires `--team` or prompts for selection
- Validation webhook rejects TenantClusters in non-team namespaces
- Provides clean audit trail

### Optional (Flexible)
- Teams can be created and used
- TenantCluster can exist in `defaultNamespace` without team
- Good for mixed environments and gradual adoption

### Disabled (Simple/Demo)
- No Team CRD functionality
- All TenantClusters in `defaultNamespace`
- Simplest experience for demos, home labs, single-team use

```yaml
apiVersion: butler.butlerlabs.dev/v1alpha1
kind: ButlerConfig
metadata:
  name: butler
spec:
  multiTenancy:
    mode: Enforced  # or Optional, Disabled
  defaultNamespace: butler-tenants
```

The mode is platform-level, not per-tenant, ensuring consistent behavior.

## Consequences

### Positive

- Enterprises get required governance out of the box
- Simple deployments aren't burdened with team management
- Clear upgrade path from Disabled → Optional → Enforced
- Platform admin has full control over enforcement
- All CRDs support full multi-tenancy (no technical debt)

### Negative

- Three code paths to maintain and test
- Documentation must cover all modes
- Users migrating between modes may need cluster moves

### Neutral

- Mode selection is a day-1 decision (can be changed but requires migration)
- Matches enterprise software patterns (editions/tiers)
- Similar to how Kubernetes itself handles RBAC (can run without it for testing)
