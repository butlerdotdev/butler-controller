# ADR-006: Tiered Status Refresh Strategy

## Status

Accepted

## Context

Butler needs to keep TenantCluster status up-to-date with the actual state of tenant clusters (node count, addon health, etc.). This requires querying tenant cluster APIs.

We needed to decide how frequently to refresh status:

1. **Fixed interval**: Same interval for all clusters (e.g., every 30 seconds)
2. **Event-driven only**: Only update on watched resource changes
3. **Tiered by age/phase**: Different intervals based on cluster state
4. **On-demand only**: User explicitly requests refresh

Considerations:
- Provisioning clusters need frequent updates (user is waiting)
- Stable clusters change rarely
- Too frequent polling wastes resources
- Too infrequent polling shows stale data
- At scale, hundreds of clusters polling frequently is problematic

## Decision

We implement tiered status refresh based on cluster phase and age:

| Cluster State | Requeue Interval | Rationale |
|---------------|------------------|-----------|
| Provisioning/Installing | 30 seconds | User is actively waiting |
| Ready (< 1 hour) | 1 minute | Recently created, may have issues |
| Ready (1-24 hours) | 5 minutes | Settling period |
| Ready (> 24 hours) | 15 minutes | Stable, changes are rare |

Implementation:
```go
func calculateRequeueAfter(tc *TenantCluster) time.Duration {
    if tc.Status.Phase != "Ready" {
        return 30 * time.Second
    }
    age := time.Since(tc.Status.LastTransitionTime)
    switch {
    case age < 1*time.Hour:
        return 1 * time.Minute
    case age < 24*time.Hour:
        return 5 * time.Minute
    default:
        return 15 * time.Minute
    }
}
```

On-demand refresh available via:
```bash
butlerctl cluster status production --refresh
```

Future enhancement: Add watches for critical tenant resources (Nodes, key Deployments) while keeping tiered polling for detailed status.

## Consequences

### Positive

- Good UX during provisioning (fast feedback)
- Efficient at scale (stable clusters poll infrequently)
- Resource usage scales with actual activity
- Simple to implement and understand
- On-demand escape hatch for immediate refresh

### Negative

- Status may be up to 15 minutes stale for stable clusters
- Age calculation adds minor complexity
- Different users may see different update frequencies

### Neutral

- Can tune intervals based on operational experience
- Watches can be added later for more real-time updates
- Pattern is common in Kubernetes controllers (exponential backoff)
