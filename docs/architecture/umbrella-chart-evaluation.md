# Butler umbrella chart scope evaluation

| | |
|---|---|
| **Status** | Draft evaluation (not a ratified ADR) |
| **Date** | 2026-04-23 |
| **Scope** | Should Butler ship as a Helm umbrella chart, and if so, which components belong inside? |
| **Outcome** | Recommended: ship a `butler` umbrella bundling butler-crds + butler-controller + butler-console + butler-addons + steward-crds + steward + capi-steward. Provider charts and butler-portal stay external. |

This evaluation is a draft; promotion to `ADR-014-*` (wherever placed) is a future decision once the recommendation stabilizes and the open questions close.

## 1. Context

Butler currently ships as independent Helm charts. Customers upgrade by coordinating version bumps across multiple HelmReleases.

The proposal under evaluation: a Butler umbrella chart that bundles a subset of components with pinned dependency versions, so customers upgrade via a single umbrella version bump.

Two customers run Butler today:
- butler-beta, a dev/validation cluster observable this session via `~/.butler/butler-beta-kubeconfig`
- Company 1, not observable this session (no kubeconfig on this machine)

The question this document answers: which components belong in the umbrella, and what does the umbrella design look like given how Butler actually works? Every claim is backed by a file path, a command output, or a `unobserved this session` marker.

## 2. Phase 1: component inventory

Nine charts live under `butler-charts/charts/`. Two additional charts (`steward`, `steward-crds`) live in the steward repo at `steward/charts/`. Three repos the prompt mentioned are not charts and are called out separately.

### 2.1 Charts (11 total)

| Chart | Version / appVersion | Component kind | Workloads deployed | Chart deps | External prereqs | Container image |
|---|---|---|---|---|---|---|
| butler-addons | 0.3.0 / v1alpha1 | RBAC + CR instances only | ButlerConfig/User/AddonDefinition CRs, ConfigMap | none | none | n/a |
| butler-bootstrap | 0.1.0 / 0.1.0 | long-running controller | Deployment, SA, Service, RBAC | none | none (CRDs assumed) | ghcr.io/butlerdotdev/butler-bootstrap |
| butler-console | 0.6.2 / 0.6.0 | long-running dual service (API + SPA) | Deployment x2 (server, frontend), SA, Services, RBAC, Secret, PDB, Ingress | none | cert-manager for OIDC optional, Flux CRs (read) | ghcr.io/butlerdotdev/butler-server + butler-console |
| butler-controller | 0.12.0 / 0.15.0 | long-running controller + webhooks | Deployment, SA, Service, RBAC, PDB, ServiceMonitor, ValidatingWebhookConfiguration, cert-manager Certificate + ClusterIssuer | none | cert-manager | ghcr.io/butlerdotdev/butler-controller |
| butler-crds | 0.13.0 / v1alpha1 | CRD-only | 17 CustomResourceDefinitions (in `templates/`, not `crds/`) | none | none | n/a |
| butler-provider-harvester | 0.3.0 / 0.1.0 | long-running controller | Deployment, SA, Service, RBAC | none | none | ghcr.io/butlerdotdev/butler-provider-harvester |
| butler-provider-nutanix | 0.3.0 / 0.1.0 | long-running controller | Deployment, SA, Service, RBAC | none | none | ghcr.io/butlerdotdev/butler-provider-nutanix |
| butler-provider-proxmox | 0.1.0 / 0.1.0 | long-running controller | Deployment, SA, Service, RBAC | none | none | ghcr.io/butlerdotdev/butler-provider-proxmox |
| steward-etcd | 0.15.0 / 3.5.17 | long-running stateful service (subchart-only) | StatefulSet, SA, Services, ConfigMap, Secret, PVCs, ServiceMonitor, pre-install Job | none | cert-manager (TLS between etcd peers) | quay.io/coreos/etcd |
| steward (in steward repo) | 0.3.2 / 0.3.1 | long-running controller + webhooks | Deployment (manager), optional Deployment (kubeconfig-generator), SA, Services, RBAC, Mutating + ValidatingWebhookConfiguration, cert-manager Certificate + Issuer | **steward-etcd 0.15.0** (conditional on `steward-etcd.deploy`) | cert-manager; Gateway API + Traefik optional | ghcr.io/butlerdotdev/steward |
| steward-crds (in steward repo) | 0.0.0+latest / latest | CRD-only | 3 CRDs: TenantControlPlane, DataStore, KubeconfigGenerator | none | none | n/a |

### 2.2 17 Butler CRDs shipped by `butler-crds` chart

AddonDefinition, ButlerConfig, ClusterBootstrap, IdentityProvider, ImageSync, IPAllocation, LoadBalancerRequest, MachineRequest, ManagementAddon, NetworkPool, ProviderConfig, Team, TenantAddon, TenantCluster, User, Workspace, WorkspaceTemplate.

### 2.3 Non-chart repos mentioned in the prompt

These were listed in the proposal framing but are not Helm charts. The umbrella scope decision does not apply to them directly.

| Repo | What it is | Relevance to umbrella |
|---|---|---|
| butler-cli | Go binary (two commands: `butleradm`, `butlerctl`); distributed as binaries and `go install` | Not a chart. Customers install directly; unaffected by umbrella decisions. |
| butler-portal | Separate product built on Backstage (node/yarn repo, no Chart.yaml) | Not a chart. Own product line. Out of umbrella scope. |
| capi-steward | Go operator (CAPI control plane provider). Deployed to butler-beta as Deployment `capi-steward-controller-manager` in `steward-system` via kubectl; no Helm chart in any repo observed this session | Currently deployed via raw kubectl manifests on butler-beta. Included IN the umbrella scope (see §5.1), which requires creating a capi-steward Helm chart as a prerequisite workstream. |
| butler-api | Go types module; no runtime; no chart | Not a chart. Coupling-only via `go.mod` imports. |

### 2.4 Cross-chart observations

- Only one chart declares a Chart-level dependency: `steward` depends on `steward-etcd 0.15.0` conditionally.
- All other charts are self-contained. CRD and RBAC dependencies between Butler charts are implicit (install order matters; no chart-level `dependencies:` declarations).
- butler-console and butler-controller do not depend on one another as charts; they both assume `butler-crds` is installed first.
- butler-console's RBAC grants read on `steward.butlerlabs.dev/tenantcontrolplanes` and patch on Steward's management Deployment for rotation tracking (file: `butler-charts/charts/butler-console/templates/rbac.yaml`). This is a one-way runtime coupling: butler-server reads Steward state but does not import Steward Go types.

## 3. Phase 2: lifecycle and coupling

### 3.1 Go module coupling (butler-api pin audit)

| Repo | butler-api pin | Notes |
|---|---|---|
| butler-server | v0.10.0 | latest |
| butler-controller | v0.10.0 | latest |
| butler-bootstrap | v0.9.4 | one minor behind |
| butler-provider-aws | v0.9.4 | |
| butler-provider-gcp | v0.9.4 | |
| butler-provider-azure | v0.9.4 | |
| butler-provider-harvester | v0.9.4 | |
| butler-provider-nutanix | v0.9.4 | |
| butler-cli | not imported (commented in go.mod) | uses embedded YAML manifests |
| steward | not imported | independent Go module with its own API group `steward.butlerlabs.dev` |

**Release coupling pattern:** a CRD addition in butler-api forces 6 repos to bump (butler-server, butler-controller, butler-bootstrap, 5 providers). butler-cli syncs manifests manually. Steward is entirely isolated.

**Version skew risk:** five providers and butler-bootstrap lag on v0.9.4. If butler-api ships a v0.11.0 CRD addition, the lag widens until each laggard independently bumps.

### 3.2 Runtime coupling (CRD consumers)

butler-controller and butler-server both read Steward's `TenantControlPlane` via unstructured dynamic client, not generated Go types. This means Steward can rev its internal implementation freely; Butler only breaks when the TCP CRD's observed GVR or load-bearing status fields change.

### 3.3 Webhook shippers (cert-manager coupling)

Charts and repos that ship admission webhooks and therefore require cert-manager on the management cluster:

- butler-controller (ValidatingWebhook on Team, TenantCluster, NetworkPool, ProviderConfig)
- steward (Mutating + Validating on TenantControlPlane and DataStore; write-permission enforcement; migration freeze)
- Each provider repo lists a ValidatingWebhookConfiguration in its `config/default/`, but the published charts here do not currently deploy those webhooks. That gap is out of scope for this evaluation.

### 3.4 Steward identity and release cadence

The crux of the umbrella scope decision. Evidence gathered:

- **Steward README** positions Steward as "the Kubernetes Control Plane Manager" and explicitly targets CNCF Sandbox status by approximately 2027. It is not marketed as "Butler's internal control plane."
- **Community governance** is called out in the README: "Stable releases published to public registries," "community-governed roadmap and governance."
- **Adopters list** and Slack channel (`#steward` on kubernetes.slack.com) indicate external users beyond Butler Labs.
- **CAPI provider** (`capi-steward`) lets any CAPI workflow consume Steward without Butler. It is not Butler-exclusive by design.
- **Release cadence**: steward tags advance on their own schedule (v0.1.0 → v0.4.0 across its history), independent of butler-controller's cadence (v0.5.0 → v0.15.0). Commits in butler-controller that bump `butler-api` are dependency updates, not coordinated releases with Steward. The release tag chronology shows no lockstep.
- **Zero lockstep-forcing events in history**: Steward has released without corresponding butler-controller releases. butler-controller has released without Steward bumps. Both consume `butler-api` types but neither forces the other.

**Independent conclusion on Steward identity:** Steward is a distinct product under the Butler Labs organization with its own release cadence, its own user base (including non-Butler users via CAPI), and an explicit CNCF trajectory. Those facts are true and remain true regardless of whether the Butler umbrella depends on Steward. The umbrella's chart dependency is an operator install-path decision; it does not alter Steward's standalone chart at `steward/charts/steward`, its tag cadence, its community governance, or its CNCF submission. The kube-prometheus-stack pattern (umbrella chart depending on a pinned Prometheus version) is the canonical precedent: Prometheus remains its own product with its own cadence, and the umbrella holds a pinned dependency for operators installing the stack. Steward's distinct product existence and the Butler umbrella bundling Steward as a dependency are independent questions; both answers are yes. See §5.1 for the scope decision and §7.2 for the counterargument pass.

### 3.5 Other product identities

- **capi-steward**: tightly scoped (CAPI glue for Steward). Same verdict as Steward: IN the umbrella as a chart dependency, subject to creating a capi-steward Helm chart first. Standalone product existence (where relevant) is unaffected.
- **butler-portal**: separate product line (Internal Developer Platform built on Backstage). Not relevant to this umbrella.
- **butler-cli**: client-side tool; out of scope for chart bundling.
- **butler-providers**: provider-per-repo design means each provider is installable independently. Product positioning is multi-provider, but deployed reality (see Phase 3) is per-customer choice.

## 4. Phase 3: runtime observation

### 4.1 butler-beta (observed this session)

Helm releases present on butler-beta, `helm list -A`:

| Namespace | Release | Chart | Managed by | Notes |
|---|---|---|---|---|
| butler-system | butler-addons | butler-addons-0.1.0 | manual helm | **stale**: current chart is 0.3.0 |
| butler-system | butler-crds | butler-crds-0.11.0 | manual helm | **stale**: current chart is 0.13.0 |
| butler-system | butler-console | butler-console-0.6.2 | Flux HelmRelease | current |
| butler-system | butler-controller | butler-controller-0.12.0 | Flux HelmRelease | current |
| steward-system | steward | steward-0.3.2 | manual helm | current |
| cert-manager | cert-manager | cert-manager-v1.16.2 | prerequisite | |
| kube-system | cilium | cilium-1.17.0 | prerequisite | |
| envoy-gateway-system | eg | gateway-helm-v1.3.0 | prerequisite | |
| longhorn-system | longhorn | longhorn-1.7.2 | prerequisite | |
| metallb-system | metallb | metallb-0.15.3 | prerequisite | |
| traefik | traefik | traefik-34.3.0 | prerequisite | |

Deployments in butler-system: butler-console-frontend, butler-console-server, butler-controller.
Deployments in steward-system: capi-steward-controller-manager (kubectl-managed, no Helm release), steward. StatefulSet: steward-etcd.
Deployments in flux-system: 6 Flux controllers (helm-controller, source-controller, image-automation-controller, image-reflector-controller, kustomize-controller, notification-controller).

**Butler charts NOT deployed on butler-beta:**
- butler-bootstrap (image exists; bootstrap performed via butler-cli instead)
- butler-provider-harvester, butler-provider-nutanix, butler-provider-proxmox (no on-prem provisioning on this management cluster)

### 4.2 Company 1 (unobserved this session)

Company 1 kubeconfig not on this machine; `~/.butler/` does not contain a matching file. No runtime observation available. Marked `unobserved this session`.

Company 1's GitOps repo was not inspected per session constraints.

### 4.3 Runtime vs inventory reconciliation

- 9 Butler charts exist; 4 are deployed on butler-beta (`butler-addons`, `butler-crds`, `butler-console`, `butler-controller`).
- Steward (+ bundled steward-etcd StatefulSet + sibling capi-steward Deployment) adds one more Helm release plus kubectl-managed capi-steward.
- Customer operational surface on butler-beta today: **4 Butler Helm releases** + 1 Steward release + 6 prerequisite charts. Providers and bootstrap chart unused.

The observed pattern supports the premise of the umbrella proposal on one dimension: the 4 Butler-side releases move in tight coupling (CRDs + controller + console + addons) and would reduce to 1 HelmRelease under an umbrella.

## 5. Phase 4: umbrella scope recommendation

For each component, IN or OUT of the umbrella with evidence-based reasoning. Criteria weighted: operational-burden reduction, release-cadence fit, product-identity distortion risk, Helm dependency-resolution issues, migration cost.

### 5.1 Per-component decisions

| Component | IN / OUT | Reasoning |
|---|---|---|
| butler-crds | IN | Tight coupling to butler-api. Every controller release needs the CRDs it was built against. Bundling collapses the current two-step manual upgrade (butler-crds: 0.11.0 → 0.13.0, then controller) into one umbrella version bump. Stale butler-crds on butler-beta is direct evidence of the operational cost of separation. |
| butler-controller | IN | Core reconciler for Butler CRDs. Releases track butler-api pins; moves in lockstep with butler-crds. Primary upgrade target. |
| butler-console | IN | Ships butler-server as well as the SPA. butler-server also pins butler-api v0.10.0 alongside butler-controller. Customer expectation is that UI and controller upgrade together. Not bundling forces customers to coordinate a console chart bump with a controller chart bump every release. |
| butler-addons | IN | Installs ButlerConfig singleton, init User, AddonDefinition catalog. Semantically tied to `butler-crds` (ButlerConfig is a CRD, AddonDefinitions reference the catalog the controller reads). Tiny RBAC+CR chart; trivial to include; keeps the "install Butler" story a single operation. |
| butler-bootstrap | **OPEN** (lean OUT) | Chart exists at 0.1.0 and has not iterated. butler-beta does not deploy it. butler-cli performs the bootstrap role on butler-beta today. Chart may be legacy. Requires product-side clarification (Phase 8). Default position until clarified: OUT. |
| butler-provider-harvester | OUT | Per-customer choice. butler-beta runs zero provider charts. Bundling all providers would install 3 controller Deployments and 3 credential Secrets surfaces on every management cluster regardless of use. Customers install the provider they run. |
| butler-provider-nutanix | OUT | Same as harvester. |
| butler-provider-proxmox | OUT | Same as harvester. Chart is at 0.1.0 and provider is still maturing. |
| butler-provider-{aws,azure,gcp} | OUT | No charts in butler-charts yet (providers distributed as Go repos only). When charts appear, same verdict: per-customer choice. |
| steward | IN | Every Butler management cluster runs Steward. There is no observed customer installing Butler without Steward. Bundling matches the install reality; not bundling forces operators to reason about two products' compatibility matrices to install one. Steward's standalone chart at `steward/charts/steward` remains for Steward's own (non-Butler) users; the umbrella's pinned dependency is a separate artifact with zero effect on Steward's release cadence, community, or CNCF trajectory. Same pattern as kube-prometheus-stack depending on Prometheus. |
| steward-crds | IN | Same reasoning as butler-crds: CRDs must install before the controller. Tight coupling. |
| steward-etcd | IN (transitively) | Subchart of steward via the existing `dependencies:` block. Included automatically when steward is IN; no separate decision. |
| capi-steward | IN | Butler's CAPI workflows depend on capi-steward; no observed scenario where Butler is installed without it. Requires creating a Helm chart as a prerequisite workstream (current deployment is via kubectl manifests). |
| butler-portal | OUT | Separate product line. |
| butler-cli | N/A | Not a chart. |

### 5.2 Umbrella scope summary

**IN (7 subcharts):** butler-crds, butler-controller, butler-console, butler-addons, steward-crds, steward, capi-steward. steward-etcd ships transitively as a subchart of steward.

**Pending decision (1 subchart):** butler-bootstrap. Default OUT pending clarification. See Phase 8.

**OUT:** all provider charts (per-customer choice; see §5.1), butler-portal (separate product line), butler-cli (not a chart).

**Prerequisite workstream:** no capi-steward Helm chart exists today. Session 0 of the implementation plan (§8) must create one before the umbrella can bundle it.

### 5.3 External prerequisites (documented, not bundled)

- **cert-manager** (required by butler-controller and steward webhooks)
- **CAPI core + infrastructure providers** (when tenant provisioning is active; version requirements owned by butler-controller)
- **MetalLB or equivalent LoadBalancer** (platform-dependent)
- **Gateway API CRDs** (if Steward's Gateway exposure mode is used)

Steward is no longer listed as an external prereq: it is bundled as a subchart (§5.1). The umbrella's Steward subchart version pin replaces the previously-proposed external compatibility matrix.

### 5.4 Compatibility matrix shape

The umbrella chart's README ships a compatibility matrix covering the external pieces the umbrella does not bundle:

```
Umbrella version -> Kubernetes version range -> CAPI version range -> cert-manager version range
```

Compatibility is documented, not runtime-enforced. Platform-level versions outside the documented range are the customer's risk. Steward is no longer part of this matrix because the umbrella pins its subchart version directly.

## 6. Phase 5: umbrella design details

### 6.1 Chart name and location

**Recommendation: `butler` at `butler-charts/charts/butler/`.**

The umbrella is the customer-facing product surface. Naming it `butler-platform` or similar muddies branding: Artifact Hub lists it alongside `butler-controller`, `butler-console`, etc., and "Butler" is what customers install. Consistency with kube-prometheus-stack's naming pattern (umbrella chart uses the product name, subcharts carry the component suffix).

Alternatives considered:
- `butler-platform`: adds a disambiguation layer that confuses rather than clarifies. Rejected.
- `butler-stack`: signals bundling but breaks the naming symmetry with subcharts. Rejected.

### 6.2 Dependency model

**Recommendation: OCI registry dependencies, single source (`oci://ghcr.io/butlerdotdev/charts`).**

Chart.yaml dependencies block:

```yaml
dependencies:
  - name: butler-crds
    version: "0.13.0"
    repository: "oci://ghcr.io/butlerdotdev/charts"
  - name: butler-controller
    version: "0.12.0"
    repository: "oci://ghcr.io/butlerdotdev/charts"
  - name: butler-console
    version: "0.6.2"
    repository: "oci://ghcr.io/butlerdotdev/charts"
  - name: butler-addons
    version: "0.3.0"
    repository: "oci://ghcr.io/butlerdotdev/charts"
  - name: steward-crds
    version: "0.0.0+latest"
    repository: "oci://ghcr.io/butlerdotdev/charts"
  - name: steward
    version: "0.3.2"
    repository: "oci://ghcr.io/butlerdotdev/charts"
  - name: capi-steward
    version: "0.1.0"  # pending chart creation
    repository: "oci://ghcr.io/butlerdotdev/charts"
```

The four butler-* subcharts already publish to this OCI registry via `.github/workflows/release.yaml`. steward and steward-crds publish to the same registry via the steward repo's release workflow. capi-steward has no chart today; creating one is a prerequisite workstream (§8). Umbrella builds consume already-released subchart versions, which forces a subchart to publish before the umbrella can bundle it; an acceptable discipline.

Alternatives considered:
- **Local-path (`file://../butler-controller`)**: easier dev iteration, but breaks `helm dependency update` for release artifacts and prevents consumers from pinning exact subchart SHAs. Rejected for release builds. Acceptable for local development via a `Chart.dev.yaml` override pattern if needed.
- **Mixed local + OCI**: worst of both. Rejected.

### 6.3 Global values design

Settings that apply across multiple subcharts belong at `global.*`:

- `global.imageRegistry`: mirror destination for air-gapped or private-registry deployments
- `global.imagePullSecrets`: list of ImagePullSecret names applied to every pod
- `global.tlsIssuerRef`: cert-manager issuer kind + name for webhook certs (butler-controller + butler-console Ingress)
- `global.logLevel`: default log level inherited by subcharts that honor it
- `global.butler.namespace`: system namespace (default `butler-system`)

Settings that are subchart-scoped stay in their subchart's values tree under the subchart's key:

- `butler-controller.replicas`, `butler-controller.resources`, etc.
- `butler-console.server.auth.jwtSecret`
- `butler-crds.crds.*` toggles

Subchart values.yaml is wired to accept `global.*` via the Helm subchart global pattern (`{{ .Values.global.imageRegistry | default .Values.image.registry }}`).

### 6.4 Versioning

**Recommendation: independent semver for the umbrella, subchart versions pinned in Chart.yaml.**

Kube-prometheus-stack model. Umbrella `butler 1.0.0` pins exact subchart versions. A subchart bump that adds a feature triggers an umbrella minor bump. A subchart patch that fixes a bug triggers an umbrella patch bump. Umbrella major bumps are reserved for breaking changes at the umbrella API (values schema incompatibility, removal of a subchart).

Subchart releases continue on their own cadence. The umbrella can lag a release or two without penalty.

### 6.5 Release coordination

- Subchart teams release independently as today.
- The umbrella release workflow triggers on either (a) manual tag `butler-v*` or (b) automated PR when any subchart publishes a new version that is semver-compatible with the umbrella's declared range.
- Umbrella CI runs `helm lint`, `helm template`, and a smoke install on KinD before publishing.

### 6.6 Migration strategy for existing customers

Two paths. The recommendation is path (b) for both butler-beta and Company 1.

#### (a) Destructive reinstall

1. Helm uninstall each individual chart (preserving CRDs via `helm.sh/resource-policy: keep` on CRDs).
2. Helm install `butler` umbrella.

Risks: brief outage, reliance on per-resource keep annotations, operator must know which secrets survive uninstall.

#### (b) In-place Helm 3 release adoption (recommended)

Helm 3 supports adopting existing resources into a new release via annotations:

```
meta.helm.sh/release-name: butler
meta.helm.sh/release-namespace: butler-system
app.kubernetes.io/managed-by: Helm
```

Migration script per customer:

1. `kubectl annotate/label` every resource owned by the existing Helm releases (butler-crds, butler-controller, butler-console, butler-addons, steward-crds, steward) plus the kubectl-managed capi-steward resources with the umbrella release-name/namespace. The steward release lives in `steward-system`; the umbrella needs to span both namespaces (or the umbrella values set per-subchart namespaces to preserve placement).
2. `helm uninstall` the pre-existing releases with `--no-hooks` (resources are already relabeled; Helm leaves them alone). capi-steward kubectl manifests are deleted without cascade (labels already moved).
3. `helm install butler oci://ghcr.io/butlerdotdev/charts/butler --version 1.0.0 -n butler-system --take-ownership`.

`--take-ownership` is the Helm 3.18+ flag for adopting pre-existing resources. Alternative for older Helm: the upgrade path uses `helm upgrade --install` with a new release name and relies on the relabeling step.

Validation: a scratch cluster reproduces the adoption procedure before butler-beta sees it. butler-beta is the next target after the scratch run. Company 1 follows butler-beta.

## 7. Phase 6: counterarguments

Every meaningful Phase 4 and Phase 5 decision gets stress-tested.

### 7.1 Decision: butler-crds goes in the umbrella

**Counterargument:** Helm CRD lifecycle is a well-known footgun. CRDs placed in `templates/` are deleted on chart uninstall, potentially taking CR data with them. kube-prometheus-stack deliberately keeps CRDs in a `crds/` directory to avoid this. Bundling butler-crds as an umbrella subchart repeats the risk.

**Response:** The risk exists today regardless of the umbrella. butler-crds chart already places CRDs in `templates/` (not `crds/`) because the chart author wanted Helm templating and install-order control. Uninstall already deletes CRDs. Bundling does not change the risk profile; it only relocates where the footgun lives. Mitigation is a `helm.sh/resource-policy: keep` annotation on every CRD, which the umbrella can enforce uniformly across the subchart's CRD templates.

**Survives:** NO. Addressed.

### 7.2 Decision: Steward (and the Steward family) is IN the umbrella

**Counterargument:** Steward is a distinct product with its own CNCF trajectory, its own community, and its own standalone Helm chart. Bundling it under the Butler umbrella might imply Butler "owns" Steward or couple Steward's release cadence to Butler's.

**Response:** Bundling is a chart dependency, not a product claim. Steward's standalone chart at `steward/charts/steward` continues to exist for Steward's own users and continues to release on its own cadence. The Butler umbrella depending on Steward at a pinned version is the kube-prometheus-stack pattern: a curated stack with a pinned subchart dependency. Prometheus's identity, release cadence, and CNCF status are unaffected by being bundled in kube-prometheus-stack; Steward's identity is unaffected by being bundled in butler. Operators installing Butler management clusters always need Steward, so the umbrella matches that reality. The cadence question that the earlier draft raised is a misread: the umbrella can lag Steward releases (it pins a version and bumps when ready), and Steward can release without waiting for the umbrella. Neither forces the other.

**Survives:** NO. Addressed.

### 7.3 Decision: providers stay out of the umbrella

**Counterargument:** Butler's product narrative is multi-cloud + on-prem, driven by the provider family. Carving providers out of the umbrella signals they are "optional" and weakens the pitch. Customers who run multiple providers get the same coordination tax the umbrella was supposed to eliminate.

**Response:** The narrative concern is real. Evidence from deployed state says the narrative hasn't yet hit the ground: butler-beta deploys zero provider charts. Until multi-provider deployments become the observed customer shape, bundling all six providers forces deploying 6 Deployments, 6 RBAC surfaces, and 6 credential Secrets paths on every management cluster, most of which go unused. A future "butler-providers" meta-umbrella could bundle the subset a customer actually needs, but that adds a layer without a clear win today.

**Survives:** PARTIALLY. If, once multiple customers are running 2+ providers, the coordination tax materializes, revisit. For this decision, the evidence points OUT.

### 7.4 Decision: butler-bootstrap stays out (pending clarification)

**Counterargument:** A chart exists; someone presumably uses it. Default-OUT might break a workflow we cannot see.

**Response:** butler-beta does not deploy the chart. The chart has not iterated past 0.1.0. butler-cli performs the bootstrap role on observed customers. Either the chart is legacy (drop it) or it serves a workflow not represented in either observed customer (Company 1 unobserved). This is genuinely a Phase 8 question the evaluation cannot answer alone.

**Survives:** YES. Flagged for user decision (§9).

### 7.5 Decision: umbrella name is `butler` (not `butler-platform`)

**Counterargument:** Customers already install charts named `butler-*`. A chart named `butler` is ambiguous: is it the whole thing, or is it a specific component someone forgot to name?

**Response:** Chart names are not global. A chart named `butler` under OCI repository `ghcr.io/butlerdotdev/charts/butler` is distinct from `butler-controller`, and the name prefix `butler-` on subcharts clearly signals they are subcharts of `butler`. This is how kube-prometheus-stack and every major umbrella does it. `butler-platform` adds syllables and does not clarify.

**Survives:** NO. Addressed. Name stays `butler`.

### 7.6 Decision: OCI-only dependency resolution (no local-path)

**Counterargument:** Local development is painful when every dependency bump requires publishing to OCI first. Local-path dependencies make iteration fast.

**Response:** Local iteration can use a dev-only values or an out-of-band `helm dependency build` against a local Helm repo if needed. Release builds must use OCI so customers can pin exact versions and reproduce builds. Mixing local and OCI in released artifacts breaks reproducibility.

**Survives:** NO.

## 8. Phase 7: implementation plan

Six sessions. Each session is surgical and validates on butler-beta or a KinD scratch cluster before moving on.

| Session | Scope | Validation | Depends on |
|---|---|---|---|
| 0 | Create a capi-steward Helm chart (prereq workstream; capi-steward is currently deployed via raw kubectl manifests). Publish to the same OCI registry used by the other Butler charts. | `helm install` on a KinD cluster reproduces the kubectl-managed Deployment + RBAC. | none |
| 1 | Scaffold `butler-charts/charts/butler/Chart.yaml` with all 7 subchart dependencies at current versions. Minimal `values.yaml`. `helm template` smoke test. | `helm lint` + `helm template` pass locally. No push. | Session 0; Phase 8 resolution of butler-bootstrap inclusion |
| 2 | Introduce `global.*` values. Rewire each subchart's values.yaml to honor globals via the Helm subchart global pattern. Requires PRs into butler-crds/controller/console/addons and steward/capi-steward to accept globals. | `helm template` shows expected rendering with a single `global.imageRegistry` override. Subchart individual installs still work (backward compatible). | Session 1 |
| 3 | Release `butler 1.0.0-rc0` to OCI. Install on a KinD scratch cluster via `helm install`. | Full KinD smoke test: CRDs install, controller + console + steward + capi-steward pods Ready, ButlerConfig and a test TenantControlPlane reconcile. | Session 2 |
| 4 | Migration harness: write and test the Helm 3 release adoption script on the KinD scratch cluster that previously held the seven individual releases (four butler-* + steward-crds + steward + a capi-steward Helm release converted from kubectl manifests). Prove it re-adopts without resource churn. | Re-adopt dry-run, apply, confirm `helm list` shows only `butler` and `helm get manifest butler` matches the union of the previous releases. | Session 3 |
| 5 | butler-beta cutover. Apply the migration script, including the conversion of the currently-kubectl-managed capi-steward Deployment into the umbrella. Consolidate Flux HelmReleases on butler-beta. | butler-beta stays green: Management GitOps tab loads, cluster create works, webhooks enforce, TenantControlPlane reconciliation unchanged. | Session 4 |
| 6 | Company 1 cutover per their change window. | Per Company 1 harness (to be defined when the window is scheduled). | Session 5 + Company 1 availability |

**Total calendar estimate:** 3-5 development weeks assuming one session per ~2 work days plus butler-beta validation windows. Company 1 cutover timing depends on their change window.

## 9. Phase 8: open questions

1. **butler-bootstrap chart status.** Is the chart still intended for use, or has butler-cli fully replaced it? Default: OUT of umbrella until clarified. If kept, re-evaluate inclusion.
2. **butler-providers meta-umbrella.** If multi-provider deployments become common, is a `butler-providers` sub-umbrella worth building that bundles only the providers a specific customer needs? Not today; worth a future decision point.
3. **Document location and ADR asymmetry.** ADR-009, ADR-012 live in butler-controller. ADR-013 lives in butler-server. ADR-010 and ADR-011 are referenced elsewhere but not observed in butler-controller's docs/architecture this session; they likely live in butler-server and butler-cli respectively. If this evaluation promotes to ADR-014, where does it land: butler-controller (consistency with the charter ADRs), butler-charts (consistency with the subject matter), or a new butler-umbrella docs tree (consistency with cross-repo architecture)? Worth resolving before the next ADR lands anywhere.
4. **capi-steward Helm chart creation.** The umbrella scope includes capi-steward, but no Helm chart exists today (deployed via kubectl manifests on butler-beta). Session 0 of the implementation plan creates the chart. Open question: does the chart live in butler-charts (with the other operator charts) or in the capi-steward repo (matching steward's `charts/steward` pattern)? Recommend the latter for consistency with steward's repo layout.
5. **butler-cli integration.** Today, `butleradm` bootstrap installs individual charts. Post-umbrella, does the CLI install the umbrella by default? That's a butler-cli PR; not an umbrella decision, but a dependency of any "install Butler" story refresh.
6. **Company 1 kubeconfig parity.** This evaluation could not validate against Company 1. Before the Session 5-6 cutover sequence, get Company 1 runtime state into scope (shared kubeconfig or a one-time snapshot via their operator) to avoid migrating blind.

## 10. Critical files and evidence sources

- `butler-charts/charts/*/Chart.yaml`, `values.yaml`, `templates/`: the 9 charts inventoried
- `steward/charts/steward/Chart.yaml`, `steward/charts/steward-crds/Chart.yaml`
- `butler-api/go.mod` and every consumer's `go.mod` for version pins
- `butler-controller/docs/architecture/ADR-009-team-environments.md`, `ADR-012-admission-webhook-deployment.md` (format precedent)
- `butler-server/docs/architecture/ADR-013-websocket-authentication.md` (location asymmetry evidence)
- `KUBECONFIG=~/.butler/butler-beta-kubeconfig helm list -A` (runtime baseline)
- `KUBECONFIG=~/.butler/butler-beta-kubeconfig kubectl get crd | grep -E "butler|steward|cluster.x-k8s.io"` (CRD surface)
- `steward/README.md`, Butler Labs site + docs references to Steward (product-identity evidence)

## 11. Summary

Ship a Butler umbrella bundling seven subcharts: the butler core four (`butler-crds + butler-controller + butler-console + butler-addons`) plus the Steward family (`steward-crds + steward + capi-steward`). `steward-etcd` comes along transitively as steward's subchart. Keep the provider family OUT (per-customer choice), butler-portal OUT (separate product line), butler-cli N/A (not a chart). butler-bootstrap remains open pending product clarification.

Document external prerequisites (cert-manager, CAPI core + providers, MetalLB, Gateway API) rather than bundling them. Use Helm 3 release adoption for in-place migration of butler-beta and Company 1.

Migration runs as a seven-session plan (Session 0 creates the capi-steward Helm chart as a prerequisite; Sessions 1-6 scaffold, release, validate, cut over butler-beta, and cut over Company 1). butler-beta gates the Company 1 cutover.

The umbrella proposal is correct in its core premise: every Butler management cluster runs the same set of components, and collapsing seven HelmReleases (plus kubectl-managed capi-steward) to one is a measurable operational win. butler-beta is currently sitting on stale butler-addons 0.1.0 and butler-crds 0.11.0 because the human-driven, per-chart upgrade loop has gaps; the same gap is latent wherever a customer has to coordinate Steward and Butler version matrices by hand.

Steward's distinct product existence (standalone chart, independent release cadence, CNCF Sandbox trajectory, community governance) is preserved by keeping its standalone chart at `steward/charts/steward`. The umbrella's pinned dependency on Steward is a separate artifact that does not affect Steward's identity or cadence; it is the kube-prometheus-stack pattern applied to Butler.

Revisit in a future session once butler-bootstrap's status closes, once the capi-steward chart is created (§8 Session 0), and once multi-provider customer deployments are observed.
