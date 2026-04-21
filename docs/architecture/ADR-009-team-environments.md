# ADR-009: Team Environments

## Status

Proposed

## Date

2026-04-20

## Context

Teams in Butler are flat. A `Team` owns a namespace, carries an `Access` block (users and groups with roles), optional `ResourceLimits`, and optional `ClusterDefaults`. `TenantCluster` resources reference the team through `spec.teamRef`. All of a team's clusters sit side by side with no operator-visible segmentation.

Real organizations run clusters in tiered lifecycles: development, staging, production, per-engineer sandboxes, and team-wide utility environments (integration, demo). Taxonomies vary by org. A 5-person startup uses `dev` and `prod`. A Fortune 500 uses `ci-us-east-dev-03`. Butler today has no way to express any of this as platform state, which means:

- Operators cannot apply different resource caps per environment (dev gets 15%, prod gets 80%).
- RBAC cannot vary per environment; a team operator has the same privileges everywhere.
- Personal sandboxes require manual one-off clusters with no enforcement of per-user limits.
- kubectl queries for environment-scoped operations require convention-based labels that no CRD validates.

Constraints binding the solution:

- **ADR-003 (CRDs-as-API)**: Platform state is expressed as CRDs. Environments must be schema-visible, not side-channel configuration.
- **No breaking changes**: Existing teams and clusters continue to work. Operators adopt environments when ready.
- **Operator ergonomics**: kubectl selectors and `butlerctl` flags must stay simple. Complex namespace topologies are deferred.
- **Existing quota enforcement**: Team-level quota is already enforced at the TenantCluster admission webhook. Env-level quota extends rather than replaces that path.

## Decision

Add an `Environments` field to `TeamSpec` as a list of typed entries. Require a label on `TenantCluster` once the parent team defines any environment. Extend existing admission paths for validation and quota.

### EnvironmentSpec

```go
type EnvironmentSpec struct {
    // Name is the environment name, operator-chosen. Free-form string.
    // Used as the value of the butler.butlerlabs.dev/environment label
    // on TenantClusters that belong to this environment.
    Name string `json:"name"`

    // Limits optionally caps the environment's usage within the team
    // ceiling. Unset means no environment-level cap; team-level cap
    // still applies.
    // +optional
    Limits *EnvironmentLimits `json:"limits,omitempty"`

    // Access elevates team members within this environment. Additive
    // only: the block cannot reduce a team-level role. Team admins
    // retain admin regardless of what Access contains.
    // +optional
    Access *TeamAccess `json:"access,omitempty"`

    // ClusterDefaults apply when a TenantCluster is created in this
    // environment and does not set the field itself. Environment
    // defaults win over team-level ClusterDefaults on conflicts.
    // +optional
    ClusterDefaults *ClusterDefaults `json:"clusterDefaults,omitempty"`
}

type EnvironmentLimits struct {
    TeamResourceLimits `json:",inline"`

    // MaxClustersPerMember caps how many TenantClusters each individual
    // team member can own in this environment. Zero or unset means no
    // cap. Enables personal sandbox patterns without a first-class
    // sandbox type.
    // +optional
    // +kubebuilder:validation:Minimum=0
    MaxClustersPerMember *int32 `json:"maxClustersPerMember,omitempty"`
}
```

`TeamResourceLimits`, `TeamAccess`, and `ClusterDefaults` are reused verbatim from `butler-api/api/v1alpha1/common_types.go` and `team_types.go`. No new quota surface is introduced.

### Label on TenantCluster

New label: `butler.butlerlabs.dev/environment`. Set at create time; immutable after. Value must match a `spec.environments[].name` on the parent team. Label absence is allowed only when the parent team has no environments defined; once the team defines any environment, the label is required on every new TenantCluster in that team.

### RBAC: additive-only inheritance

Team-level roles flow through to every environment unchanged. The environment's optional `access` block can elevate a member within that environment; it cannot reduce a member's team-level role. Consequences:

- A team admin is admin on every environment by construction.
- A team operator stays at operator in an environment unless elevated to admin by the env's access block.
- A team viewer stays at viewer unless elevated.
- Personal sandboxes (described in the Patterns section) rely on the composition of `access` scoped to one member plus `MaxClustersPerMember: 1`. Team admins still have admin on a personal sandbox; only the owning user can create clusters in it.

### Quota model: team ceiling, env caps below

- `Team.spec.resourceLimits` is the absolute team ceiling. Platform-admin-editable only.
- `Team.spec.environments[].limits` are optional sub-caps. Team-admin-editable (platform admin can also edit).
- Sum of env caps has no required relationship to team total. Over-provisioning across environments is legal and sometimes desirable for flexibility.
- Cluster creation validates three gates in order:
  1. Team total (always).
  2. Environment limits (if set on the target environment).
  3. `MaxClustersPerMember` (if set on the target environment).
- Team total always wins. Env caps restrict further; they never expand past the team total because the team-total gate runs first.

**Worked example**

```
Team: acme
  resourceLimits.maxClusters: 20              (platform-admin-set ceiling)
  environments:
    - name: prod
      limits.maxClusters: 10                  (team-admin-set)
    - name: dev
      limits.maxClusters: 15                  (team-admin-set)
    - name: sandbox
      limits: {}                              (uncapped within team total)

Sum of env caps: 10 + 15 = 25. Legal; env caps do not sum to team total.

Scenarios:
  - 8 clusters in prod, 7 in dev, 2 in sandbox = 17 total.
    Creating a new dev cluster: dev cap check (7 of 15, ok);
    team total check (17 of 20, 1 slot, ok). Allowed.
  - 9 prod + 9 dev + 2 sandbox = 20 total. Creation in any env
    rejected because team total at ceiling. Env caps irrelevant.
  - 10 prod + 4 dev + 3 sandbox = 17. Creation in prod rejected
    because prod cap reached; dev and sandbox still have room
    within team total (17 of 20, 3 slots).
```

### Mutation authority: split access

Two new admission-webhook gates on `Team` updates:

- `spec.resourceLimits` edits require platform admin. Team admins cannot raise their own ceiling.
- `spec.environments[].limits` edits require team admin on the Team being edited (or platform admin).

Platform-admin detection runs two paths:

1. **Primary (User CRD)**: the webhook reads `AdmissionRequest.UserInfo.Username` (email), lists `User` CRDs, and when one matches by `spec.email` the caller's `spec.isPlatformAdmin` value is authoritative. This handles callers mapped to a Butler User.
2. **Fallback (SubjectAccessReview)**: when no User CRD matches the caller (kubectl-direct operators holding the admin kubeconfig, automation service accounts not onboarded as Butler Users), the webhook submits a `SubjectAccessReview` for `verb=* group=butler.butlerlabs.dev resource=teams`. Only subjects bound to the `butler-cli-platform-admin` ClusterRole receive `allowed=true`.

The SAR path requires the butler-controller ServiceAccount to carry `create` on `subjectaccessreviews.authorization.k8s.io`. The butler-charts PR on branch `feat/butler-controller-sar-permission` adds that grant to the butler-controller chart; without it, the SAR request errors with a 500 and callers without a User CRD cannot pass the platform-admin check. The chart PR must land alongside this ADR.

Team-admin detection: read the Team's own `spec.access.users[].role` and `spec.access.groups[].role` for `admin` matching the requesting user (by email or by group membership).

### Enforcement locations

- **New `Team` admission webhook** (`butler-controller/internal/webhook/team_webhook.go`): validates the mutation-authority split on Team create and update.
- **Extended `TenantCluster` admission webhook** (`butler-controller/internal/webhook/tenantcluster_webhook.go`): validates the `butler.butlerlabs.dev/environment` label, env-level quota, and `MaxClustersPerMember` on create.
- **`TenantCluster` reconciler** (`butler-controller/internal/controller/tenantcluster/reconcile_validation.go`): defense-in-depth copies of the env quota and per-member checks. Sets condition reasons `ReasonEnvQuotaExceeded` and `ReasonPerMemberCapExceeded`.
- **butler-server session middleware** (`butler-server/internal/auth/middleware.go`): reads `X-Butler-Environment` request header and populates `SelectedEnvironment` and `SelectedEnvironmentRole` on `UserSession`.

## Patterns

Operators compose the three existing primitives (env `access`, env `limits`, `MaxClustersPerMember`) to achieve the common shapes.

### Personal sandbox

```yaml
environments:
  - name: alice-sandbox
    access:
      users:
        - name: alice@example.com
          role: operator
    limits:
      maxClustersPerMember: 1
```

Outcome: only `alice@example.com` (plus team admins by inheritance) can create clusters in this environment. Alice can own exactly one cluster in it at a time. Other team operators and viewers have their team-level roles on this environment but cannot create (they are not elevated to operator on it because the access block does not list them; inheritance gives them their team role, which for operators means they can still view but not create because `MaxClustersPerMember: 1` applies to them as well if they ever tried).

### Restricted-access production

```yaml
environments:
  - name: prod
    access:
      users:
        - name: sre-lead@example.com
          role: admin
    limits:
      maxClusters: 10
```

Outcome: SRE lead is admin on prod. Team admins are admin on prod by inheritance. Team operators keep their operator role on prod (inheritance). Team viewers keep viewer. Nothing is reduced.

### Team-wide environment (no overrides)

```yaml
environments:
  - name: dev
    limits:
      maxClusters: 15
```

Outcome: team roles flow through unchanged. Env caps clusters at 15.

### Shared utility environment (elevated subset)

```yaml
environments:
  - name: integration
    access:
      users:
        - name: ci-bot@example.com
          role: admin
      groups:
        - name: platform-ops
          role: admin
```

Outcome: the CI service account and the `platform-ops` group are admins on the integration env. Team-level roles still apply (additive).

## Phased Migration

Three phases with explicit detection criteria.

### Phase 1: baseline

- No Team has `spec.environments`. No TenantCluster carries the `butler.butlerlabs.dev/environment` label.
- Webhook behavior: label absent is allowed.
- Operator-visible behavior: nothing changes.
- Detection of phase completion: not applicable; phase 1 is the pre-adoption state.

### Phase 2: per-team opt-in

- A team defines one or more environments by editing `spec.environments`.
- Webhook behavior: from the moment a team has any `spec.environments`, new `TenantCluster` create requests in that team's namespace are rejected if the env label is absent or does not match an entry in `spec.environments[].name`. Existing TenantClusters without the label continue to work.
- Operator-visible behavior: new clusters require the `--environment` flag (or the equivalent label on a kubectl-created manifest). Existing clusters are unaffected until they are re-reconciled or updated.
- Detection of phase completion (for a given team): all existing TenantClusters in the team namespace carry the env label.

### Phase 3: bulk backfill

- `butleradm env migrate` walks a team's existing unlabeled TenantClusters and applies the env label interactively (one at a time with operator confirmation) or in bulk (`--all-to <env>`).
- Operator-visible behavior: after migration, env quota accounting reflects the backfilled clusters; previously unaccounted clusters now count against their target env's cap.
- Detection of phase completion: `kubectl get tc -n team-<name> -l '!butler.butlerlabs.dev/environment'` returns empty.

## Alternatives Considered

### Standalone Environment CRD

Make `Environment` its own cluster-scoped resource with a `teamRef` to its owning team. Separate lifecycle, status, events.

Rejected for v1: overbuilds before a real need for independent lifecycle, status, or reconciliation emerges. Promotion from `Team.spec.environments[]` to a standalone CRD stays viable if and when that need appears. The schema shape of `EnvironmentSpec` does not change between the embedded and standalone forms; migration would be a copy-and-rename.

### Type enum on EnvironmentSpec

`type: standard | personal | shared`, each with distinct controller semantics (TTL, production-target blocking, per-user scoping).

Rejected: the distinguishing behaviors each collapsed during design review. Personal sandbox TTL was removed because operators lose work. Production-target blocking folded into a deferred GitOps feature. Per-user scoping is expressed through `access` composition. When every behavior a type value gates disappears, the enum itself should disappear. Operators compose the effect from `access` and `MaxClustersPerMember`.

### Labels-only without Team schema

Keep `Team` unchanged; express environments as a convention-based label on `TenantCluster`.

Rejected: no schema validation target, no quota enforcement surface, no RBAC integration point. Operators end up with typo-prone labels and no way to express per-env caps.

### Fixed enum of environment names

`environment: dev | stage | prod | sandbox` with predefined names.

Rejected: organizational taxonomies vary. A 5-person startup and a Fortune 500 cannot share a fixed list. The freeform `name` field keeps the schema permissive while `type` semantics (since removed) could have captured behavior differences.

### Sum-to-total quota constraint

Require `sum(env.limits) <= team.resourceLimits` as a webhook invariant.

Rejected: overly rigid. Operators often want flexibility: prod capped at 10 and dev capped at 15 with a team total of 20 is useful because it lets a team flex between environments as workloads shift. Sum-to-total blocks this. The team total is the enforced ceiling at cluster-creation time; summing at schema-validation time is the wrong gate.

### Per-env sub-namespace in v1

Each environment gets `team-<name>-<env>` as its own namespace.

Rejected for v1: enables per-env NetworkPolicies and ResourceQuotas naturally, but the required changes (namespace creation per env, RBAC resolution via namespace hierarchy, list handlers querying multiple namespaces per team, migration tool moving CRDs across namespaces) are out of proportion to the v1 value. Label-only is simpler and leaves the sub-namespace model available as a future ADR if per-env network isolation becomes a requirement.

## Consequences

### Positive

- Operators can express real org taxonomies (dev, stage, prod, custom names, personal sandboxes, shared utilities) as platform state.
- Per-env quotas are enforced at admission time with defense-in-depth at reconcile time. The existing team-quota webhook is extended, not replaced.
- Platform admins retain control of the team ceiling. Team admins get self-service control of per-env allocation within that ceiling. The split access model matches typical platform-team relationships.
- RBAC inheritance is additive-only, which is the safer default. Operators cannot accidentally reduce a team member's access by editing an env.
- `MaxClustersPerMember` enables personal sandbox patterns without a first-class type.
- Existing teams and clusters continue to work without intervention. Adoption is per-team, opt-in, incremental.

### Negative

- RBAC resolution now has two levels (team, env). The session middleware must resolve both per request, and `checkClusterAccess` must consult env scope when a cluster has the label.
- Quota accounting has two sums per team (team total, per-env total) plus per-member counts when `MaxClustersPerMember` is set. Webhook validation cost grows with team cluster count.
- The new `Team` admission webhook introduces a cross-resource dependency: Team mutations require a User CRD lookup (or SubjectAccessReview) to verify the requesting identity's role. Adds an admission-time round-trip to the API server.
- A new label must appear on TenantClusters. Existing automation that creates `TenantCluster` resources directly (GitOps, CI) must be updated to set the label once the owning team defines environments.

### Owner-label edge cases for `MaxClustersPerMember`

- **kubectl-created TenantClusters without creator-email annotation**: butler-server sets `butler.butlerlabs.dev/creator-email` on creates it handles, and the controller promotes this annotation to a `butler.butlerlabs.dev/owner` label on the TenantCluster. Clusters created directly via kubectl (bypassing butler-server) carry no annotation. When the target env has `MaxClustersPerMember` set, the admission webhook rejects the create with a message pointing operators at the required annotation or label. kubectl-path creates must set the annotation explicitly. CLI reference documents the annotation name.
- **Pre-existing TenantClusters without owner label**: treated as owned by no one for `MaxClustersPerMember` accounting. They do not count against any member's cap. `butleradm env migrate` can backfill the owner label from audit logs when the creator is recoverable; otherwise the label stays blank and the cluster remains unaccounted for member-cap purposes until an operator sets it manually.

### Deferred

- **Promotion workflow**: `butlerctl cluster promote --from dev --to prod` (clone TenantCluster spec across envs). Does not require schema changes; lands later as a CLI convenience.
- **Per-env GitOps directory conventions**: hint in docs, not enforced. A v2 may formalize `platform/<team>/<env>/cluster-<name>.yaml`.
- **Cross-env NetworkPolicies**: requires per-env sub-namespaces. Tracked as a future ADR if per-env network isolation becomes a requirement.
- **Per-env sub-namespaces**: deferred in favor of label-only v1. See alternatives.
- **GitOps production-target flag**: scrapped for v1. No production concept at the platform layer. Re-opens if a customer requires it.

## Open Questions

No open questions at publication. All design tensions were resolved during review. The five items below are follow-ons rather than blockers.

- Session revocation on env `access` change: when an operator removes a member from an env's access block, existing sessions for that member retain their old role until the next session re-resolution. Current behavior for team-level access is the same. Not addressed here.
- Metrics: env label should appear on controller-exported metrics (cluster count, provisioning latency, etc.). Implementation detail for a follow-up PR.
- Audit log: env label and env-mutation events should appear in the audit trail. Implementation detail for the audit PR.
- `butleradm env migrate` backfill source: the migration tool can read creator-email from the Butler audit log. The audit log retention window is configurable; clusters older than the window have no recoverable creator.
- Printer columns on Team: whether to surface env count and env utilization in `kubectl get teams` output. Implementation detail for step 2.

## References

- [ADR-002: Multi-Tenancy Platform Modes](./ADR-002-multi-tenancy-modes.md)
- [ADR-003: Team Namespace Model](./ADR-003-team-namespace-model.md)
- [ADR-007: Hybrid Addon Management](./ADR-007-hybrid-addon-management.md)
- `butler-api/api/v1alpha1/team_types.go` (current `TeamSpec`, `TeamAccess`, `ClusterDefaults`)
- `butler-api/api/v1alpha1/common_types.go:114-187` (`TeamResourceLimits`)
- `butler-api/api/v1alpha1/user_types.go:82` (`User.spec.isPlatformAdmin`)
- `butler-controller/internal/webhook/tenantcluster_webhook.go:42-330` (existing team-quota enforcement)
- `butler-server/internal/auth/session.go:35-280` (session model, role helpers)
- `butler-server/internal/auth/users.go:666` (platform-admin resolution)
- `butler-server/internal/auth/serviceaccount.go:53` (`butler-cli-platform-admin` ClusterRole)
- `butler-controller/internal/capi/builder.go:178-190` (label propagation to CAPI resources)
