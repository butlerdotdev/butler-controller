# Butler Controller Architecture

This document provides detailed architecture documentation for butler-controller.

## Overview

butler-controller is the core component responsible for tenant cluster lifecycle management in the Butler platform. It implements the Kubernetes controller pattern to watch Butler custom resources and reconcile them to the desired state.

## Design Principles

1. **Kubernetes-Native**: All operations are expressed as CRDs and reconciled by controllers
2. **Infrastructure Agnostic**: Works with any CAPI-supported infrastructure provider
3. **GitOps Ready**: Supports declarative management via Flux or ArgoCD
4. **Enterprise Multi-Tenancy**: Team-based isolation with RBAC and quotas
5. **Monotonic Addon Installation**: Safe addon management that only adds, never removes

## Component Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Management Cluster                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                      butler-controller                            │   │
│  ├──────────────────────────────────────────────────────────────────┤   │
│  │                                                                   │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │   │
│  │  │ ButlerConfig    │  │ Team            │  │ TenantCluster   │   │   │
│  │  │ Reconciler      │  │ Reconciler      │  │ Reconciler      │   │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘   │   │
│  │                                                                   │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │   │
│  │  │ TenantAddon     │  │ KamajiSecret    │  │ KamajiStatus    │   │   │
│  │  │ Reconciler      │  │ Reconciler      │  │ Reconciler      │   │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘   │   │
│  │                                                                   │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐             │
│  │  Cluster API   │  │    Kamaji      │  │ Infrastructure │             │
│  │                │  │                │  │   Provider     │             │
│  └────────────────┘  └────────────────┘  └────────────────┘             │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Controllers

### ButlerConfigReconciler

**Purpose**: Manages platform-wide configuration singleton.

**Watches**: ButlerConfig

**Responsibilities**:
- Validates singleton exists (name must be "butler")
- Creates default namespace for Disabled/Optional modes
- Tracks aggregate counts (teams, clusters)
- Propagates configuration changes

### TeamReconciler

**Purpose**: Manages team namespaces and RBAC.

**Watches**: Team, Namespace, RoleBinding

**Responsibilities**:
- Creates namespace with same name as Team
- Creates RoleBindings for users and groups
- Enforces resource limits
- Tracks cluster count per team
- Handles cleanup on deletion (finalizer)

**Reconciliation Flow**:
```
Team Created
    │
    ▼
Add Finalizer
    │
    ▼
Create Namespace (team name)
    │
    ▼
Create RoleBindings
├── For each user in spec.access.users
└── For each group in spec.access.groups
    │
    ▼
Update Status
├── namespace: <created namespace>
├── phase: Ready
└── clusterCount: <count>
```

### TenantClusterReconciler

**Purpose**: Full tenant cluster lifecycle management.

**Watches**: TenantCluster, Namespace, Secret, CAPI Cluster, MachineDeployment

**Responsibilities**:
- Validates team membership (Enforced mode)
- Creates tenant namespace for CAPI resources
- Creates CAPI Cluster and infrastructure resources
- Creates KamajiControlPlane for hosted control plane
- Creates MachineDeployment for worker nodes
- Installs addons (monotonic - only add, never remove)
- Updates observed state from tenant cluster

**Reconciliation Phases**:

```
┌─────────────────────────────────────────────────────────────────┐
│                    TenantCluster Reconciliation                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Phase 1: Validation                                             │
│  ├── Check multi-tenancy mode requirements                       │
│  ├── Validate Team membership (if Enforced)                      │
│  ├── Validate resource limits                                    │
│  └── Validate ProviderConfig exists                              │
│                                                                  │
│  Phase 2: Infrastructure                                         │
│  ├── Generate tenant namespace name                              │
│  ├── Create tenant namespace with labels                         │
│  ├── Create CAPI Cluster                                         │
│  ├── Create provider-specific cluster (HarvesterCluster)         │
│  ├── Create KamajiControlPlane                                   │
│  ├── Create MachineDeployment                                    │
│  ├── Create HarvesterMachineTemplate                             │
│  └── Create KubeadmConfigTemplate                                │
│                                                                  │
│  Phase 3: Addons (when infrastructure ready)                     │
│  ├── Install CNI (Cilium) - required for networking              │
│  ├── Install LoadBalancer (MetalLB) - create memberlist first    │
│  ├── Install cert-manager                                        │
│  ├── Install Storage (Longhorn)                                  │
│  ├── Install Ingress (Traefik)                                   │
│  └── Bootstrap GitOps (Flux) - if configured                     │
│                                                                  │
│  Phase 4: Status                                                 │
│  ├── Query tenant cluster state                                  │
│  ├── Update conditions                                           │
│  └── Calculate requeue interval (tiered refresh)                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Tiered Status Refresh**:
| Cluster State | Requeue Interval |
|---------------|------------------|
| Provisioning | 30 seconds |
| Ready (< 1 hour) | 1 minute |
| Ready (< 24 hours) | 5 minutes |
| Ready (> 24 hours) | 15 minutes |

### TenantAddonReconciler

**Purpose**: Post-creation addon lifecycle management.

**Watches**: TenantAddon

**Responsibilities**:
- Waits for target TenantCluster to be Ready
- Respects DependsOn for addon ordering
- Installs addons using Helm SDK
- Removes addons when TenantAddon CR is deleted

**Key Difference from TenantCluster.spec.addons**:
- `TenantCluster.spec.addons`: Monotonic (only add, never remove)
- `TenantAddon`: Full lifecycle (CR deletion = addon removal)

### KamajiSecretReconciler

**Purpose**: Translates kubeconfig format for CAPI compatibility.

**Watches**: Secrets with label `kamaji.clastix.io/component: admin-kubeconfig`

**Responsibilities**:
- Watches Kamaji kubeconfig secrets
- Creates CAPI-compatible copy (key: "value" instead of "admin.conf")
- Handles updates when kubeconfig rotates

**Why Needed**: Kamaji creates kubeconfig secrets with key "admin.conf", but CAPI Harvester provider expects key "value".

### KamajiStatusReconciler

**Purpose**: Synchronizes Kamaji status to CAPI.

**Watches**: TenantControlPlane (Kamaji CRD)

**Responsibilities**:
- Watches TenantControlPlane resources
- Patches CAPI Cluster status when control plane is ready

**Why Needed**: Workaround for Kamaji/CAPI integration where status isn't automatically synced.

## Multi-Tenancy Model

### Platform Modes

```
ButlerConfig.spec.multiTenancy.mode:

┌──────────────────────────────────────────────────────────────────┐
│  Enforced (Enterprise Default)                                    │
├──────────────────────────────────────────────────────────────────┤
│  • Teams required before creating TenantClusters                  │
│  • TenantCluster must be in Team's namespace                      │
│  • butlerctl requires --team                                      │
│  • Clean audit trail                                              │
│  • Recommended for: Enterprise production                         │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│  Optional (Flexible)                                              │
├──────────────────────────────────────────────────────────────────┤
│  • Teams can be used but not required                             │
│  • TenantCluster can exist in default namespace                   │
│  • Good for mixed environments, gradual adoption                  │
│  • Recommended for: Growing organizations                         │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│  Disabled (Simple)                                                │
├──────────────────────────────────────────────────────────────────┤
│  • No Team functionality                                          │
│  • All TenantClusters in default namespace                        │
│  • Simplest experience                                            │
│  • Recommended for: Demos, home labs, single-user                 │
└──────────────────────────────────────────────────────────────────┘
```

### Namespace Model

```
CLUSTER-SCOPED:
  Team/acme  ─────────────────────────────┐
                                          │ Creates
NAMESPACED:                               ▼
  acme/  ◄──────────────────── Team namespace
  │
  ├── Labels: butler.butlerlabs.dev/team: acme
  ├── RBAC: RoleBinding for team members
  │
  └── Contains:
      ├── TenantCluster/production ────────┐
      │                                    │ Creates
      ├── TenantCluster/staging            ▼
      │
      └── TenantAddon/production-argocd

  production-a7b8c9/  ◄──────── Tenant namespace (auto-generated)
  │
  ├── Labels:
  │   ├── butler.butlerlabs.dev/team: acme
  │   ├── butler.butlerlabs.dev/tenant: production
  │   └── butler.butlerlabs.dev/source: acme/production
  │
  └── Contains:
      ├── Cluster/production (CAPI)
      ├── MachineDeployment/production-workers
      ├── KamajiControlPlane/production
      └── Secret/production-kubeconfig
```

## Addon Management

### Monotonic Model (TenantCluster.spec.addons)

Butler only **adds** addons, never **removes** them. The set of Butler-installed addons can only grow or stay the same.

**Timeline Example**:
```
T0: TenantCluster created with [cilium, metallb, longhorn]
    → Butler installs all three

T1: User adds traefik to spec.addons
    → Butler installs traefik

T2: User removes longhorn from spec.addons
    → Butler does NOTHING (addon remains)

T3: User manually removes longhorn (kubectl delete)
    → Butler updates status only
```

**Rationale**:
1. **Safety**: Removing addons is destructive (PVCs, IPs, networking)
2. **GitOps Compatibility**: Prevents fight between Butler and Flux/ArgoCD
3. **Clear Mental Model**: spec.addons = "ensure these exist"

### Full Lifecycle Model (TenantAddon)

TenantAddon CR lifecycle = addon lifecycle. Deleting the CR removes the addon.

```yaml
apiVersion: butler.butlerlabs.dev/v1alpha1
kind: TenantAddon
metadata:
  name: production-redis
  namespace: acme
spec:
  clusterRef:
    name: production
  helm:
    repository: https://charts.bitnami.com/bitnami
    chart: redis
    version: "18.0.0"
```

Deleting this CR will uninstall Redis from the production cluster.

## RBAC Model

### ClusterRoles

| Role | Scope | Permissions |
|------|-------|-------------|
| butler-platform-admin | Cluster | Full access to Teams, ButlerConfig; view all clusters |
| butler-team-admin | Namespace | Full access to TenantClusters, TenantAddons in team namespace |
| butler-team-member | Namespace | Create/view TenantClusters; limited deletion |

### RoleBinding Generation

When a Team is created, butler-controller automatically creates RoleBindings:

```yaml
# Generated by TeamReconciler
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: acme-team-admin
  namespace: acme
roleRef:
  kind: ClusterRole
  name: butler-team-admin
subjects:
- kind: User
  name: admin@example.com
- kind: Group
  name: platform-engineers
```

## Integration Points

### Cluster API

Butler creates standard CAPI resources:
- `Cluster`
- `MachineDeployment`
- Provider-specific resources (HarvesterCluster, HarvesterMachineTemplate)

### Kamaji

Butler uses Kamaji for hosted control planes:
- Control plane pods run in management cluster
- No dedicated control plane VMs needed
- Reduces resource requirements significantly

### Infrastructure Providers

Butler works with CAPI infrastructure providers:
- Harvester (primary)
- Nutanix (planned)
- Proxmox (planned)
- Public clouds via CAPI providers (planned)

## Error Handling

### Reconciliation Errors

Errors are recorded in status conditions with detailed messages:

```yaml
status:
  conditions:
  - type: Ready
    status: "False"
    reason: ProviderError
    message: "Failed to create HarvesterCluster: insufficient resources"
```

### Requeue Strategy

| Error Type | Requeue After |
|------------|---------------|
| Transient (network, API) | 30 seconds |
| Resource not ready | 1 minute |
| Validation error | No requeue (user must fix) |
| Provider error | 5 minutes with backoff |

## Future Enhancements

1. **Validation Webhooks**: Move validation from reconciler to admission webhook
2. **Resource Watches**: Watch tenant cluster resources for real-time status
3. **Cluster Migration**: Support moving clusters between teams
4. **Self-Service Teams**: Kyverno policies for team creation
5. **Cost Tracking**: Resource usage and cost allocation per team
