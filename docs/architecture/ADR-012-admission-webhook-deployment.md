# ADR-012: Admission Webhook Deployment

## Status

Proposed

## Date

2026-04-23

## Context

ADR-009 team environments, ADR-008 enterprise networking, and the ProviderConfig guard rails each add admission webhooks to `butler-controller`. The webhook handlers are implemented and unit-tested; they live in `internal/webhook/`:

- `vtenantcluster.kb.io`: enforces env label requirements, per-env quota, per-member cap, provider-config team scope.
- `vteam.kb.io`: enforces the ADR-009 authority split (platform-admin on `spec.resourceLimits`, team-admin on `spec.environments[].limits`, additive-only env access membership).
- `vnetworkpool.kb.io`: validates CIDR math, prevents CIDR shrinking, blocks deletion when allocations exist.
- `vproviderconfig.kb.io`: validates provider-specific config, team scope, network mode.

Prior to this ADR there was no `ValidatingWebhookConfiguration` deployed on any production cluster. The handlers registered with the manager but no traffic reached them, so every gate was silently unenforced. The butler-beta harness pattern deployed ad-hoc namespace-scoped webhook configs during development; nothing existed for steady-state production.

The `butler-charts/charts/butler-controller/` chart has partial scaffolding (`templates/webhook.yaml`, `templates/certificate.yaml`, values-gated via `controller.webhooksEnabled`) but:

- The `vteam.kb.io` entry is missing from the chart (three webhooks ship, not four).
- The chart's `certManager.issuerRef` fallback references a `selfsigned-issuer` ClusterIssuer that isn't guaranteed to exist on the target cluster.

## Decision

Deploy all four admission webhooks via the `butler-charts/butler-controller` chart, gated by a single operator toggle, with cert-manager-issued serving certs and cluster-wide scope. Fill the `vteam.kb.io` gap, add an optional self-contained ClusterIssuer, and commit to `failurePolicy: Fail` as the steady-state behavior.

### Webhook set

Four `ValidatingWebhookConfiguration` entries, one per handler, each mirroring the kubebuilder marker on the Go source. Paths, operations, `sideEffects: None`, and `admissionReviewVersions: [v1]` are fixed by the handler; the chart reproduces them verbatim.

### failurePolicy: Fail

All four webhooks use `failurePolicy: Fail`. The alternative, `Ignore`, defeats the security gate the webhooks exist to enforce:

- A dropped `vteam.kb.io` request under `Ignore` lets a team admin raise `spec.resourceLimits`, inverting ADR-009's authority model.
- A dropped `vtenantcluster.kb.io` request under `Ignore` lets per-member caps leak.
- Operator-visible downtime on the webhook produces an operator-visible 500, which the operator can diagnose and unblock. Silent bypass produces undetectable security regressions.

Rollback when the webhook itself is misbehaving: see the Rollback procedure below.

### namespaceSelector: cluster-wide

The webhooks apply to all namespaces without a `namespaceSelector` filter. Rationale:

- Teams are cluster-scoped. The Team webhook must fire regardless of namespace context.
- TenantClusters live in team-owned namespaces. These namespaces are not pre-labeled with butler-system markers; they're named after the team and managed by the Team controller.
- A scoped selector would require every team namespace to opt in. An un-opted-in namespace silently bypasses enforcement. That inverts the trust model: the default state must be fail-safe (enforced), not fail-open (skipped).

Cluster-wide with `failurePolicy: Fail` is the fail-safe choice.

### cert-manager integration

cert-manager is assumed to be installed on every Butler-managed cluster. `butler-bootstrap` installs it by default; butler-beta runs it today. The chart does not depend on cert-manager via `Chart.yaml` because clusters without webhooks (pre-opt-in, simple dev clusters) don't need cert-manager at all.

Cert shape:

- `Certificate` spec with 1-year duration, 30-day renewal window.
- Secret name matches the webhook Service's TLS directory convention.
- `cert-manager.io/inject-ca-from` annotation on the `ValidatingWebhookConfiguration` auto-populates `caBundle`.
- `issuerRef` defaults to `ClusterIssuer/selfsigned-issuer`. Operators with their own PKI override via values.

### Optional self-signed ClusterIssuer

Clusters that don't have a pre-provisioned `selfsigned-issuer` ClusterIssuer need one for the cert to become Ready. The chart provides an optional template gated by `certManager.installSelfSignedIssuer: false` (default off). Operators enabling webhooks on a fresh cluster set both `controller.webhooksEnabled: true` and `certManager.installSelfSignedIssuer: true` in one Helm upgrade.

The toggle defaults off to avoid stomping on operators who already manage issuers through a separate path (Step CA, external PKI).

### Upgrade path

Enablement is an opt-in values override:

```yaml
controller:
  webhooksEnabled: true
certManager:
  installSelfSignedIssuer: true
```

No in-place data migration. Re-deploying the chart with the flag on:

1. Creates the webhook Service + Certificate + (optional) ClusterIssuer.
2. cert-manager issues the cert.
3. cert-manager populates `caBundle` on the `ValidatingWebhookConfiguration`.
4. Admission traffic starts flowing to the controller's webhook server.

### Rollback procedure

A misbehaving webhook (cert expired, controller crashloop, unintended rule catch) blocks Team and TenantCluster mutations. Restore pre-ADR-012 behavior without editing code:

```bash
helm upgrade butler-controller \
  oci://ghcr.io/butlerdotdev/charts/butler-controller \
  --reuse-values \
  --set controller.webhooksEnabled=false \
  -n butler-system
```

The next Helm sync removes the `ValidatingWebhookConfiguration`, the `Certificate`, and the Service. Controller continues serving its reconciliation responsibilities; admission gates go dormant until webhooks are re-enabled.

## Consequences

### Positive

- ADR-009 authority split and ADR-008 networking guards become enforced.
- Cluster-wide + fail-safe default prevents silent bypass.
- Single-toggle enable/disable keeps incident response procedural.
- Self-contained issuer option removes the external-prereq gotcha that would otherwise block fresh-cluster installs.

### Negative

- cert-manager becomes a hard prerequisite when webhooks are enabled.
- A webhook outage blocks all Team/TC mutations cluster-wide. Mitigated by the documented rollback and by controller health being part of standard Butler operator monitoring.

### Deferred

- Mutating webhooks (none today; if the controller ever needs to inject defaults server-side, the ADR will be amended with a `MutatingWebhookConfiguration` section).
- Per-scope `namespaceSelector` overrides for operators who need to exempt specific namespaces. Not requested today; can be added as a values override without ADR revision.

## Maintenance

The chart's `webhook.yaml` mirrors kubebuilder markers on the controller source. Drift between the two is the failure mode that shipped the missing `vteam.kb.io` entry before this ADR. To detect drift at review time, cross-check:

```bash
# Paths registered in butler-controller source (kubebuilder markers):
grep -rE "kubebuilder:webhook" \
  butler-controller/internal/webhook/ \
  | awk -F'path=' '{print $2}' | awk -F',' '{print $1}' \
  | sort -u

# Paths rendered by the chart (helm template first; yq can't parse raw
# Helm sources because they contain Go template directives):
helm template butler-controller butler-charts/charts/butler-controller \
    --set controller.webhooksEnabled=true \
  | yq 'select(.kind == "ValidatingWebhookConfiguration").webhooks[].clientConfig.service.path' \
  | sort -u
```

Both commands must produce identical output. Any mismatch means a webhook exists in one place but not the other; the PR adding the handler must also touch the chart (or vice versa).

The same snippet is documented in `butler-charts/CLAUDE.md` under "Webhook registration drift" so future chart PRs carry the check forward.

## References

- ADR-009 Team Environments: admission handler authority split
- ADR-008 Enterprise Networking and IPAM: NetworkPool webhook
- `butler-controller/internal/webhook/`: webhook handler implementations and kubebuilder markers
- `butler-charts/charts/butler-controller/templates/webhook.yaml`: chart-side webhook configuration
- `butler-charts/charts/butler-controller/templates/certificate.yaml`: cert-manager integration
