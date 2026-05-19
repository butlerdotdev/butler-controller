# ADR-018: ClusterCreationPolicy

## Status

Proposed

## Date

2026-05-18

## Context

The create-cluster modal in `butler-console` populates four option dropdowns from butler-server endpoints registered at `butler-server/internal/api/router.go:430-435`:

- `GET /providers/{ns}/{name}/images` (`internal/api/handlers/providers.go:1432`)
- `GET /providers/{ns}/{name}/networks` (`internal/api/handlers/providers.go:1630`)
- `GET /providers/{ns}/{name}/clusters` (`internal/api/handlers/providers.go:1961`, Nutanix only)
- `GET /providers/{ns}/{name}/storage-containers` (`internal/api/handlers/providers.go:2093`, Nutanix only)

Each endpoint queries the live provider and returns every entry the provider exposes. The modal's only filter is what the operator's eyes can do. An operator on Nutanix sees every image in Prism Central, every subnet in the configured Prism cluster, every storage container the credentials can list. The Harvester path is the same shape against the Harvester CRDs.

Admins cannot constrain those lists. A team operator can pick any image even when policy says "only the three vetted Talos images are allowed for production." Scalar defaults exist on `Team.spec.clusterDefaults` and `Team.spec.environments[].clusterDefaults` (`butler-api/api/v1alpha1/team_types.go:68-180`), with apply-via-mutating-webhook tracked as a v1.1 follow-on per ADR-009. Provider option lists have no analogous surface at all.

Any policy that filters only at the modal is bypassed by `kubectl apply -f tenantcluster.yaml`. The TenantCluster admission webhook (`butler-controller/internal/webhook/tenantcluster_webhook.go:60-440`) already enforces team quota, provider scope, and environment validity at admission. Policy enforcement must extend that same path so kubectl-direct callers cannot bypass.

Binding constraints:

- **ADR-003 (CRDs-as-API)**: policy state is a CRD, not external configuration.
- **ADR-009 coexistence**: `Team.spec.clusterDefaults` keeps owning scalar soft defaults (Kubernetes version, worker count, worker CPU, worker memory, worker disk). This ADR adds the option-list surface as a peer. No deprecation.
- **Provider-agnostic**: the policy CRD references option types by string name. Per-provider code stays in `butler-server/internal/api/handlers/providers.go` and the provider packages; the policy resolution layer never branches on provider type.
- **Closed option-type set in v1**: the four types the butler-server handlers expose today (`image`, `network`, `cluster`, `storageContainer`) form the closed set. No `ListOptionTypes` provider method yet.
- **Operator ergonomics**: a team operator opening the modal sees the curated list with no extra clicks. kubectl-direct callers get an admission-time rejection that names the policy.

## Decision

Add a new cluster-scoped CRD `ClusterCreationPolicy` in the `butler.butlerlabs.dev` group. The policy CRD declares scope (cluster-wide, team, or team-and-environment), the providers it applies to, and a map of option-type modes (`pin`, `allowList`, `default`, `recommended`) over the four canonical option types. butler-server applies the resolved policy to option-list responses. The TenantCluster admission webhook validates create and update requests against the same resolved policy for defense in depth.

### 1. CRD shape

```go
// ClusterCreationPolicySpec defines admin-curated constraints over the
// create-cluster option lists (subnets, images, storage containers,
// clusters) and is enforced at both butler-server list time and at
// TenantCluster admission. Scalar soft defaults remain on
// Team.spec.clusterDefaults (see ADR-009); this CRD owns option-list
// curation only.
type ClusterCreationPolicySpec struct {
    // Scope selects which TenantCluster create requests this policy
    // applies to. Exactly one of clusterWide, team, teamAndEnvironment
    // must be set. Validated by the policy admission webhook.
    Scope PolicyScope `json:"scope"`

    // TargetProviders names the ProviderType values this policy filters.
    // Empty list means all providers. Values must match ProviderType.
    TargetProviders []ProviderType `json:"targetProviders,omitempty"`

    // Options maps option-type name to its curation rule. Closed set in
    // v1: image, network, cluster, storageContainer.
    Options map[OptionType]OptionRule `json:"options,omitempty"`
}

// PolicyScope discriminator: exactly one of clusterWide, team, or
// teamAndEnvironment must be set. Enforced via CEL XValidation in the
// policy admission webhook (see Decision section 7).
type PolicyScope struct {
    ClusterWide        *ClusterWideScope     `json:"clusterWide,omitempty"`
    Team               *TeamScope            `json:"team,omitempty"`
    TeamAndEnvironment *TeamEnvironmentScope `json:"teamAndEnvironment,omitempty"`
}

type ClusterWideScope struct{}

type TeamScope struct {
    TeamRef LocalObjectReference `json:"teamRef"`
}

type TeamEnvironmentScope struct {
    TeamRef         LocalObjectReference `json:"teamRef"`
    EnvironmentName string               `json:"environmentName"`
}

// +kubebuilder:validation:Enum=image;network;cluster;storageContainer
type OptionType string

const (
    OptionTypeImage            OptionType = "image"
    OptionTypeNetwork          OptionType = "network"
    OptionTypeCluster          OptionType = "cluster"
    OptionTypeStorageContainer OptionType = "storageContainer"
)

type OptionRule struct {
    Mode              OptionMode `json:"mode"`
    Values            []string   `json:"values,omitempty"`
    Default           string     `json:"default,omitempty"`
    RecommendedReason string     `json:"recommendedReason,omitempty"`
}

// +kubebuilder:validation:Enum=pin;allowList;default;recommended
type OptionMode string

const (
    OptionModePin         OptionMode = "pin"
    OptionModeAllowList   OptionMode = "allowList"
    OptionModeDefault     OptionMode = "default"
    OptionModeRecommended OptionMode = "recommended"
)
```

Each ClusterCreationPolicy contains zero or more OptionRule entries within its `Options` map, one rule per option type. A policy is the unit of authorship and RBAC; a rule is the unit of resolution.

Resource attributes: `+kubebuilder:resource:scope=Cluster`, shortname `ccp`. The `spec.scope` discriminator carves team boundaries; the resource itself is not namespaced.

### 2. Coexistence with `Team.spec.clusterDefaults`

`Team.spec.clusterDefaults` and `Team.spec.environments[].clusterDefaults` keep their existing shape (`butler-api/api/v1alpha1/team_types.go:151-180`): `KubernetesVersion`, `WorkerCount`, `WorkerCPU`, `WorkerMemoryGi`, `WorkerDiskGi`, `DefaultAddons`. These are scalar soft defaults. ADR-009 tracks apply-via-mutating-webhook for these fields as a v1.1 follow-on; this ADR does not change that work.

`ClusterCreationPolicy` covers a different field shape: option-list curation. Subnets, images, storage containers, and Nutanix clusters are sets of provider-returned entries identified by ID. A policy filters or decorates those sets with `pin`, `allowList`, `default`, `recommended` modes. The two surfaces ship side by side; an admin who wants to default `workerCPU=4` uses `Team.spec.clusterDefaults`, and an admin who wants to allow-list three image UUIDs uses ClusterCreationPolicy.

### 3. Standalone CRD, not embedded

ADR-009 embedded environments in `Team.spec.environments` and noted promotion to a standalone CRD stays viable if independent lifecycle emerges. This ADR chooses standalone for one reason: cluster-wide policies cannot be expressed embedded. A rule like "no team can use image X" needs a global statement. Duplicating that rule across every team's spec produces drift; inventing a separate cluster-wide carve-out reproduces a standalone CRD by another name.

Lifecycle also differs from Team. Platform admins author policies; team admins do not. Cluster-wide rules require platform-level authority by definition. A team admin authoring a cluster-wide policy would be exercising scope beyond their team. Team admins can author policies scoped to their own team via `spec.scope.team`, but the lifecycle and RBAC surface for those policies stays distinct from team self-service operations on `Team.spec` itself. Putting policy fields under Team would mean either gating those fields by an additional role check inside the Team admission webhook or risking team admins accidentally clearing policy. A separate CRD with its own webhook gives policy its own RBAC surface.

### 4. Closed option-type set in v1

The CRD `OptionType` enum is `image | network | cluster | storageContainer`. Naming choices and current butler-server coverage:

- `image`: every provider with an image dropdown returns this. Nutanix and Harvester both implemented.
- `network`: Nutanix calls these subnets in the Prism API; Harvester calls these NetworkAttachmentDefinitions. The CRD uses one canonical name across providers.
- `cluster`: Nutanix only today; a Prism Central account can carry multiple Prism Element clusters. Harvester has no analogue; a policy targeting `cluster` against a Harvester provider has no effect.
- `storageContainer`: Nutanix only today; same shape as `cluster`.

Adding a new option type beyond this four requires butler-server handler changes (a new `Listfoo` handler plus a new route). Authoring a policy against an unrecognized option type returns a CRD validation error at write time via the kubebuilder enum constraint. `ListOptionTypes` per provider is logged as a follow-on (see Alternatives).

### 5. Mode semantics

- **pin**: exactly one acceptable value for this option. `Values` must have length 1. The modal renders the pinned entry as a single read-only selection. Admission rejects any TenantCluster create or update whose corresponding spec field references a different value. Use `allowList` with one entry when you want a single-value enforcing rule that the admin may later widen; use `pin` when the single-value semantics are deliberate.
- **allowList**: the acceptable values for this option. The modal renders the allow-listed subset. `Default` (when set) is pre-selected. Admission rejects values outside `Values`.
- **default**: presentation only. The modal renders the full provider response. `Default` is pre-selected. Admission does not enforce.
- **recommended**: presentation only. The modal renders the full provider response. Entries in `Values` are badged and sorted first. `RecommendedReason` (when set) appears in the badge tooltip. Admission does not enforce.

Summary of mode behaviors:

| Mode | Filters dropdown | Pre-selects default | Badges entries | Admission enforces |
|---|---|---|---|---|
| pin | Yes (single value) | Renders read-only | No | Yes |
| allowList | Yes | When default set | No | Yes |
| default | No | Yes | No | No |
| recommended | No | No | Yes | No |

### 6. Resolution order

The resolution context is the tuple `(team, environment, providerConfig.provider)` derived from the operator session for list-time resolution, and from the TenantCluster spec for admission-time resolution.

Steps:

1. **Gather candidates.** List every ClusterCreationPolicy where `spec.targetProviders` is empty or contains the current provider AND `spec.scope` matches the context. `clusterWide` always matches. `team` matches when `teamRef.name == team.name`. `teamAndEnvironment` matches when both `teamRef.name` and `environmentName` match.
2. **Bin by specificity tier.** Tier 1 is `teamAndEnvironment`. Tier 2 is `team`. Tier 3 is `clusterWide`.
3. **Resolve per option type.** Walk tiers most-specific first. The first tier that contains at least one rule for a given option type provides the effective rule for that option type. Modes do not stack across tiers; specificity wins.
4. **Intra-tier conflict.** If two policies in the same tier define rules for the same option type for the same matching context, the policy admission webhook rejects the second at write time. Conflict detection is scoped to overlapping `targetProviders` AND shared option-type keys: two policies in the same tier targeting different providers do not conflict, and two policies in the same tier targeting the same providers but defining rules for disjoint option-type keys do not conflict. The resolution layer is therefore single-policy-per-tier-per-option-type at read time. Read-time conflict resolution would force a choice between union (the broadest combined rule), intersection (the strictest combined rule), or strictest-wins (always pin if any conflicting rule pins). Each option is surprising to operators who authored their policy expecting a specific behavior. Write-time rejection makes the conflict explicit at the author's terminal, with the conflicting policy named.
5. **Apply the resolved rule.** butler-server applies the rule when serving option-list responses. The admission webhook applies the rule when validating TenantCluster spec.
6. **No effective rule.** When no tier contains a rule for an option type, the provider response passes through unchanged. This matches today's behavior.

Worked example:

```
Context: team=acme, env=prod, provider=nutanix

Policies in cluster:
  - ccp/global-no-deprecated-images
      scope.clusterWide: {}
      targetProviders: [nutanix]
      options:
        image:
          mode: allowList
          values: [img-rocky-9, img-talos-1.7, img-talos-1.8]
  - ccp/acme-prod-network-pin
      scope.teamAndEnvironment: {teamRef: acme, environmentName: prod}
      targetProviders: [nutanix]
      options:
        network:
          mode: pin
          values: [net-prod-vlan-200]

Resolved rules for (acme, prod, nutanix):
  image:            clusterWide -> allowList [img-rocky-9, img-talos-1.7, img-talos-1.8]
  network:          teamAndEnv  -> pin       [net-prod-vlan-200]
  cluster:          no rule, pass through
  storageContainer: no rule, pass through

Modal renders:
  image dropdown:            3 entries
  network dropdown:          1 entry
  cluster dropdown:          full provider response
  storageContainer dropdown: full provider response
```

A cluster-wide pin and a team allow-list for the same option type resolve in favor of the team. The cluster-wide rule is the umbrella; the team carve-out is the intentional override. Operators who need to widen for one team author a team-scoped policy; operators who need to narrow for one team do the same.

### 7. Enforcement locations

Five surfaces, mirroring ADR-009's "Enforcement locations" structure.

- **butler-server option-list handlers** (`butler-server/internal/api/handlers/providers.go`). New shared package `butler-server/internal/api/policy/` provides `ApplyOptionRule(ctx, optionType, items, resCtx)`. Each of the four list handlers calls `policy.Apply...` after the existing `switch providerType` block populates the result, before `writeJSON`. Resolution context comes from `UserSession.SelectedTeam` and `UserSession.SelectedEnvironment` already populated by `butler-server/internal/auth/middleware.go:84` (X-Butler-Environment header). No new request header is required.
- **butler-server admin endpoints for policy CRUD** (`butler-server/internal/api/handlers/admin_policies.go`). Five thin dynamic-client wrappers following the Team admin endpoint pattern: `GET /admin/policies`, `GET /admin/policies/:name`, `POST /admin/policies`, `PUT /admin/policies/:name`, `DELETE /admin/policies/:name`. Platform admin role required. Admission-webhook rejection messages on Create and Update are unwrapped into a structured 400 response so the console can render the validation error inline against the offending field. The webhook validates structure regardless of whether the input came from UI, GitOps, or kubectl. All three paths converge at the same validation surface.
- **Policy CRD admission webhook** (new file `butler-controller/internal/webhook/clustercreationpolicy_webhook.go`). Validates the CRD itself on create and update:
  - Exactly-one-of on `spec.scope` (CEL `XValidation` rule).
  - Referenced team exists (`APIReader.Get` against `butler-api/api/v1alpha1.Team`).
  - Referenced environment exists on the team when scope is `teamAndEnvironment`.
  - No same-tier same-option-type conflict with an existing policy in the same context (scan + reject with the conflicting policy name).
  - `pin` and `allowList` require non-empty `values`; `default` requires non-empty `default`; `recommended` requires non-empty `values`.
  - Provider-entry existence is a soft warning, not a hard reject. Provider lookups at admission time would couple admission to provider availability; a provider outage would block legitimate policy authoring. Stale references are surfaced by a status reconciler (deferred).
- **TenantCluster admission webhook** (`butler-controller/internal/webhook/tenantcluster_webhook.go`). Add `validatePolicy` called from `validateCreateUpdate` (line 155) after the env validation block at line 181. Shared resolution code lives in new `butler-controller/internal/policy/` package (`resolve.go`). Provider-field dispatch (how to read the image ID from `tc.Spec.InfrastructureOverride.Nutanix` vs `.Harvester`) lives in `butler-controller/internal/policy/fieldmap.go`. For example, the image option type maps to `tc.Spec.InfrastructureOverride.Nutanix.ImageUUID` for Nutanix providers and `tc.Spec.InfrastructureOverride.Harvester.ImageName` for Harvester providers. The fieldmap is the only per-provider code path the resolver touches. The webhook rejects with the policy name and the violating field path.
- **butler-console create-cluster modal** (`butler-console/src/pages/CreateClusterPage.tsx`). The console does not re-resolve policy. It consumes the server-applied response. Three touches: render the `recommended` badge and re-sort when the response carries policy metadata; pre-select the `default` ID; show "curated by policy: <name>" under the affected dropdown so the operator understands why the list is shorter. New typings in `butler-console/src/api/`.
- **butler-console admin authoring pages** (`butler-console/src/pages/admin/policies/`). Three pages compose the admin authoring workflow: a list view (`/admin/policies`) with status indicators, a create form (`/admin/policies/new`) with scope picker, provider multi-select, and per-option-type mode and value editors, and a view/edit/delete page (`/admin/policies/:name`). UI authoring posts to the butler-server admin endpoints described above; validation messages from the policy admission webhook surface directly in the UI against the offending field.
- **butler-charts CRD sync** (`butler-charts/charts/butler-crds/`). The CRD generated from `butler-api` lands as a YAML manifest. Standard CRD chart version bump. No values templating; the CRD is data, not configuration.

Response envelope change: each of the four list endpoints returns `{<items>: [...], policy?: {name, mode, default?, recommendedReason?}}`. Existing API consumers that ignore unknown fields keep working.

### 8. Validation webhook on the policy CRD itself

The policy webhook runs on create and update of `ClusterCreationPolicy`. It rejects policies that are structurally malformed before the resolution layer ever sees them. Operators get the error at policy authoring time, not at modal open time or admission time. This matches Butler's defense-in-depth pattern for ADR-009 (Team mutation authority split runs at Team-write time, not when a TenantCluster is created).

Conflict detection runs as part of this webhook. When a new policy would land in the same tier as an existing policy with overlapping `targetProviders` and at least one shared option-type key, the webhook returns `Forbidden` and names the conflicting policy. This keeps the read-time resolution layer single-rule-per-tier-per-option-type.

## Patterns

Four scenarios composed from the primitives above.

### Production network pin

```yaml
apiVersion: butler.butlerlabs.dev/v1alpha1
kind: ClusterCreationPolicy
metadata:
  name: acme-prod-network-pin
spec:
  scope:
    teamAndEnvironment:
      teamRef:
        name: acme
      environmentName: prod
  targetProviders: [nutanix]
  options:
    network:
      mode: pin
      values: [net-prod-vlan-200]
```

Outcome: the network dropdown for any cluster being created in `acme/prod` on a Nutanix provider shows exactly one entry. kubectl-direct creates that reference a different subnet are rejected at admission with `ClusterCreationPolicy "acme-prod-network-pin" pins network to [net-prod-vlan-200]`.

### Approved-images allow-list (team scope)

```yaml
apiVersion: butler.butlerlabs.dev/v1alpha1
kind: ClusterCreationPolicy
metadata:
  name: acme-approved-images
spec:
  scope:
    team:
      teamRef:
        name: acme
  targetProviders: [nutanix]
  options:
    image:
      mode: allowList
      values:
        - img-rocky-9-hardened
        - img-talos-1.7
        - img-talos-1.8
      default: img-talos-1.8
```

Outcome: every environment under `acme` sees only the three vetted images, with Talos 1.8 pre-selected. Cluster-wide image policies apply only to teams that have no team-scoped image rule (specificity wins).

### Recommended storage container (cluster-wide soft default)

```yaml
apiVersion: butler.butlerlabs.dev/v1alpha1
kind: ClusterCreationPolicy
metadata:
  name: global-recommended-storage
spec:
  scope:
    clusterWide: {}
  targetProviders: [nutanix]
  options:
    storageContainer:
      mode: recommended
      values: [sc-ssd-prod-001]
      recommendedReason: "SSD tier with prod-grade replication"
```

Outcome: every Nutanix storage-container dropdown shows the full list, with `sc-ssd-prod-001` badged and sorted to the top. No admission rejection; operators can still pick a different container.

### Default storage container

```yaml
apiVersion: butler.butlerlabs.dev/v1alpha1
kind: ClusterCreationPolicy
metadata:
  name: global-default-storage
spec:
  scope:
    clusterWide: {}
  targetProviders: [nutanix]
  options:
    storageContainer:
      mode: default
      default: sc-ssd-prod-001
```

Outcome: every Nutanix storage-container dropdown shows the full provider response with `sc-ssd-prod-001` pre-selected. Operators can pick any other container; admission does not enforce. The pattern fits soft defaults that aim for operator convenience without restricting choice.

### Deprecated-image block plus team carve-out

```yaml
apiVersion: butler.butlerlabs.dev/v1alpha1
kind: ClusterCreationPolicy
metadata:
  name: global-image-deny-rocky-8
spec:
  scope:
    clusterWide: {}
  targetProviders: [nutanix]
  options:
    image:
      mode: allowList
      values: [img-rocky-9, img-talos-1.7, img-talos-1.8, img-flatcar-current]
---
apiVersion: butler.butlerlabs.dev/v1alpha1
kind: ClusterCreationPolicy
metadata:
  name: team-legacy-rocky-8-extension
spec:
  scope:
    team:
      teamRef:
        name: team-legacy
  targetProviders: [nutanix]
  options:
    image:
      mode: allowList
      values: [img-rocky-8, img-rocky-9, img-talos-1.8]
```

Outcome: team-legacy operators see the four-item team list (including the deprecated Rocky 8). Every other team sees the cluster-wide four-item list (which excludes Rocky 8). The team policy wins for team-legacy because team is more specific than cluster-wide.

## Alternatives Considered

### Embedded in Team.spec.environments

Push the policy fields into `EnvironmentSpec.options` on each environment, mirroring how `EnvironmentSpec.ClusterDefaults` lives there today (`butler-api/api/v1alpha1/team_types.go:118-122`).

Rejected: cluster-wide policies cannot be expressed. "No team can use deprecated image X" needs a global statement; embedding requires either duplicating the rule across every team (drift) or inventing a separate cluster-wide carve-out (which is the standalone CRD by another name). ADR-009 chose embedded for environments because environments have no cross-team form; this case is the opposite. Lifecycle also differs: platform admins write policies, team admins do not.

### Apply at the provider layer

Move policy resolution into the provider packages (`butler-provider-nutanix`, `butler-provider-harvester`) so each provider filters its own option responses.

Rejected: every provider has to re-implement the same resolution logic. The policy CRD becomes part of the provider contract surface, which couples providers to a CRD shape they otherwise do not see. Keeping the resolution in `butler-server/internal/api/policy/` keeps providers stateless about policy. They return raw lists; butler-server filters.

### Extend ClusterDefaults with a mode field

Add `mode` to each field on `ClusterDefaults` so it can express pin, allowList, default, recommended over the scalar fields too.

Rejected: conflates two axes. Scalar fields (Kubernetes version, worker count) have one value to default and do not need allow-listing. List fields (image, network) do. A `mode` field that ignores itself on scalars is dead schema; a uniform shape that expands every scalar into a `{mode, value}` tuple is operator-hostile for fields like `workerCount: 3`. ADR-009 already established `ClusterDefaults` as the scalar surface; this ADR adds the list surface as a peer.

### ListOptionTypes per provider in v1

Add a provider-method `ListOptionTypes` to `butler-provider-nutanix`, `butler-provider-harvester`, and friends so the closed set is provider-declared, not hardcoded in butler-server.

Rejected for v1: requires cross-repo work across every provider package, and the four option types in butler-server's handlers are already a closed set in practice (`butler-server/internal/api/handlers/providers.go:1432, 1630, 1961, 2093`). The hardcoded set matches the current architecture. Promotion to a provider method stays open as a follow-on ADR when a new option type appears.

## Consequences

### Positive

- Admins curate option lists per scope (cluster-wide, team, team-and-env) without per-team YAML duplication.
- Operators see shorter, vetted dropdowns and a clear "curated by policy" indicator when the list is filtered.
- kubectl-direct creates are validated against the same policy at admission. No console-only enforcement gap.
- Provider-agnostic resolution. New providers inherit policy support as long as they return `{id, name}` shaped option entries.
- Coexists cleanly with `Team.spec.clusterDefaults` (ADR-009). No deprecation, no schema churn on the existing field.
- Closed option-type set in v1 keeps the CRD shape small and matches butler-server's current handler surface.
- UI, GitOps, and kubectl all author policies against the same validation surface. Operators choose the workflow that fits their team; policies remain auditable and reviewable in any path.

### Negative

- New CRD to document, version, and chart. Adds a v1alpha1 type to butler-api and a CRD chart row to `butler-charts/charts/butler-crds/`.
- Resolution latency at list time: butler-server uses controller-runtime's cached client to List policies, so resolution adds a memory lookup plus an in-process loop per modal open rather than an etcd hit. For N policies and K option types, the resolver runs at most N times K rule evaluations per modal open. K is bounded at four in v1 and N at the cluster's policy count, so the operation budget stays small in practice.
- Misconfigured policies can block legitimate operators. Mitigation: the policy admission webhook validates structure at write time (exactly-one-of on scope, non-empty values on pin and allowList, no same-tier conflicts). It does not validate provider-entry existence; a policy that pins to an image UUID the provider no longer exposes produces an empty list. A follow-on status reconciler surfaces stale references as a condition on the policy.
- Modal response envelope changes shape: a new optional `policy` block on each list endpoint. Console types update; existing API consumers ignore unknown fields.

### Deferred

- `ListOptionTypes` provider method. Lets the CRD enum become provider-declared instead of butler-server-declared. Promotion path documented in Alternatives.
- Cross-team policy inheritance (a policy on team A also applies to teams B and C). Not requested; would be a `parentTeamRef` or label-selector mechanic in a future iteration.
- Provider-entry-existence reconciler that sets `status.staleReferences` on the policy when pinned or allow-listed IDs no longer exist in the provider response.
- Audit log of policy-applied filtering decisions at the create-cluster path. Implementation detail for the audit PR.

## Open Questions

No open questions block this ADR. The five items below are follow-ons.

- **Multi-team operator conflict resolution**. When one operator is in teams A and B and opens the modal, the modal already requires team selection before any provider-list endpoint fires. v1 disposition: resolution uses `UserSession.SelectedTeam` only. No multi-team union semantics in v1.
- **Provider-specific metadata filtering**. The v1 CRD filters on `{id, name}` only because that is what every provider response shares (`butler-server/internal/api/handlers/providers.go:1424-1429` `ImageInfo`; analogous shapes for the other three). Filtering on VLAN (Nutanix network), OS (image), or replication factor (storage container) would require either per-provider schema extensions in the policy CRD or a uniform metadata-map shape on provider responses. v1 disposition: deferred; no metadata filtering.
- **Stale-reference handling**. A policy that pins to an image UUID the provider has since removed produces an empty modal list. v1 disposition: soft-warn via status; the follow-on reconciler writes the condition.
- **Audit verbosity**. Every option-list response that runs through policy filtering is a decision point. Logging every one is noisy. Logging only admission rejections is too narrow. v1 disposition: logs admission rejections only; verbose mode deferred to the audit PR.
- **Orphan policy scope**. When a Team CRD is renamed or deleted, a `scope.team.teamRef` pointing at the removed name falls through (policy effectively unscoped, never matches anything). v1 disposition: the status reconciler surfaces the orphan via a stale-reference condition. No cascade delete in v1.

## References

- [ADR-002: Multi-Tenancy Platform Modes](./ADR-002-multi-tenancy-modes.md)
- [ADR-003: Team Namespace Model](./ADR-003-team-namespace-model.md)
- [ADR-009: Team Environments](./ADR-009-team-environments.md)
- `butler-api/api/v1alpha1/team_types.go:43-180` (TeamSpec, EnvironmentSpec, ClusterDefaults; the coexisting field shape)
- `butler-api/api/v1alpha1/providerconfig_types.go:23-105` (ProviderType enum; source for `targetProviders` values)
- `butler-api/api/v1alpha1/tenantcluster_types.go:65-122` (TenantClusterSpec; fields the admission validator compares against policy)
- `butler-server/internal/api/router.go:430-435` (provider list route registrations)
- `butler-server/internal/api/handlers/providers.go:1432-1484` (ListImages; post-filter insertion point)
- `butler-server/internal/api/handlers/providers.go:1630-1746` (ListNetworks; analogous insertion point)
- `butler-server/internal/api/handlers/providers.go:1961-2007` (ListClusters; Nutanix-only)
- `butler-server/internal/api/handlers/providers.go:2093-2139` (ListStorageContainers; Nutanix-only)
- `butler-server/internal/auth/middleware.go:84` (X-Butler-Environment session context)
- `butler-controller/internal/webhook/tenantcluster_webhook.go:150-181` (validateCreateUpdate; insertion point for validatePolicy)
- `butler-controller/internal/webhook/tenantcluster_webhook.go:467` (validateEnvironment; pattern to mirror)
- `butler-console/src/pages/CreateClusterPage.tsx` (create-cluster modal; consumes the `policy` block)
