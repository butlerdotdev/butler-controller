# ADR-003: Team Namespace Model

## Status

Accepted

## Context

Butler needs to organize resources by team while providing proper isolation. We needed to decide how Teams relate to namespaces:

1. **Team references existing namespace**: Admin creates namespace, then Team references it
2. **Team creates namespace (same name)**: Team CR creates namespace with identical name
3. **Team creates namespace (generated name)**: Team CR creates namespace with generated name
4. **Multiple namespaces per Team**: Team can span multiple namespaces

Additionally, TenantCluster creates CAPI/Kamaji resources that need their own namespace to avoid conflicts and match Kamaji's expectations.

## Decision

We implement a two-level namespace model:

### Team Namespace
- Team CR is cluster-scoped
- Team controller creates a namespace with the **same name** as the Team
- TenantCluster CRs live in the Team's namespace
- Users interact with this namespace

### Tenant Namespace (per TenantCluster)
- Generated name: `{cluster-name}-{uid-prefix}` (e.g., `production-a7b8c9`)
- Contains CAPI Cluster, MachineDeployment, KamajiControlPlane
- Contains Kamaji pods (kube-apiserver, controller-manager, etc.)
- Butler auto-creates RoleBindings for team access
- Labels connect back to source Team and TenantCluster

```
Team/acme (cluster-scoped)
    │
    └── Creates: Namespace/acme (team namespace)
                    │
                    ├── TenantCluster/production
                    │       └── Creates: Namespace/production-a7b8c9 (tenant namespace)
                    │
                    └── TenantCluster/staging
                            └── Creates: Namespace/staging-b8c9d0 (tenant namespace)
```

Labels on tenant namespace:
```yaml
labels:
  butler.butlerlabs.dev/team: acme
  butler.butlerlabs.dev/tenant: production
  butler.butlerlabs.dev/source-namespace: acme
  butler.butlerlabs.dev/source-name: production
```

## Consequences

### Positive

- Simple mapping: Team name = namespace name
- Clean separation between user-facing (team) and internal (tenant) namespaces
- Matches Kamaji's expectation of namespace-per-tenant
- RBAC boundaries are clear
- Labels enable easy querying across all team resources
- Team members get automatic access to tenant namespaces via RoleBindings

### Negative

- Two namespaces per TenantCluster increases namespace count
- Team names must be valid namespace names
- Cannot have Team named same as existing namespace

### Neutral

- Pattern matches Cluster API's namespace-per-cluster model
- Tenant namespace cleanup happens automatically with TenantCluster deletion
- Team namespace cleanup requires explicit Team deletion
