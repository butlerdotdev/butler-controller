# ADR-004: Kamaji Hosted Control Planes

## Status

Accepted

## Context

Tenant clusters need Kubernetes control planes. We needed to decide how to provision them:

1. **Dedicated VMs**: Traditional 3-node control plane VMs per cluster
2. **Hosted control planes (Kamaji)**: Control plane pods in management cluster
3. **Managed Kubernetes**: Use cloud provider's managed control plane (EKS, AKS, GKE)
4. **k3s/k0s embedded**: Lightweight control plane on worker nodes

Key considerations:
- Resource efficiency: Control plane VMs are expensive
- Network access: API server must be reachable from workers
- Multi-tenancy: Control planes must be isolated
- On-premises focus: Cannot rely on cloud-managed services

## Decision

We use Kamaji for hosted control planes. Kamaji runs tenant control plane components (kube-apiserver, kube-controller-manager, kube-scheduler) as pods in the management cluster.

Benefits for Butler:
- **Resource efficiency**: No dedicated control plane VMs (significant cost savings)
- **Local API access**: Control plane is in management cluster, accessible via konnectivity
- **Fast provisioning**: No VM boot time for control plane
- **Simplified networking**: Workers connect to management cluster's network
- **Multi-tenant by design**: Kamaji isolates tenants via namespaces

Architecture:
```
Management Cluster
├── Kamaji Operator
├── Tenant Control Plane Pods (per TenantCluster)
│   ├── kube-apiserver
│   ├── kube-controller-manager
│   └── kube-scheduler
└── Konnectivity Server (tunnels to workers)

Tenant Workers (VMs)
├── kubelet
├── kube-proxy (or Cilium)
└── Konnectivity Agent (connects to management cluster)
```

Butler creates a `TenantControlPlane` CR (Kamaji's CRD) for each TenantCluster. Kamaji handles:
- Control plane pod scheduling
- etcd storage (shared or dedicated)
- Certificate management
- Kubeconfig generation

## Consequences

### Positive

- 3x fewer VMs per cluster (workers only)
- Faster cluster provisioning (no control plane VM boot)
- Centralized control plane management and monitoring
- Easier backup/restore (etcd in management cluster)
- Natural fit for on-premises where VM resources are constrained

### Negative

- Management cluster becomes critical path for all tenant clusters
- Kamaji is a dependency we don't control
- Network connectivity requirements (konnectivity)
- Integration workarounds needed (see ADR-005)

### Neutral

- Matches the managed Kubernetes model (EKS, GKE, AKS all host control planes)
- Kamaji is CNCF sandbox project with active community
- Can fall back to dedicated VMs if needed (CAPI supports both)
