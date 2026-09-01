# ADR-019: ProviderConfig credential trust-bundle validation warning

## Status

Proposed

## Date

2026-05-22

## Context

butler-controller v0.28.0 supports declarative CA trust injection for Nutanix tenant clusters:

- `internal/controller/tenantcluster/helpers.go:47` reads `caBundle := string(secret.Data["ca.pem"])` from the ProviderConfig's `credentialsRef` Secret.
- `internal/capi/builder.go:731-736` conditionally writes `prismCentral.additionalTrustBundle` into the NutanixCluster CR spec when the bundle is non-empty:

```go
if b.nutanixCreds != nil && b.nutanixCreds.CABundle != "" {
    prismCentral := spec["prismCentral"].(map[string]interface{})
    prismCentral["additionalTrustBundle"] = map[string]interface{}{
        "kind": "String",
        "data": b.nutanixCreds.CABundle,
    }
}
```

The mechanism is correct: when an operator's Prism Central endpoint is served by a corp-internal-CA-signed cert, they add the CA chain as `ca.pem` to the credentials Secret, and butler-controller wires it into every NutanixCluster CR it generates.

The mechanism is also invisible. The `ca.pem` Secret key is mentioned only in source code. README and operator-facing docs describe `username` and `password` only. An operator who bootstraps a fresh mgmt cluster against an internal-CA-signed Prism Central, with `insecure: false` (the default and correct production setting), can:

1. Create a `nutanix-credentials` Secret with the documented `username` and `password` keys.
2. Apply a ProviderConfig referencing that Secret with `endpoint: https://...` and `insecure: false`.
3. Successfully provision the Butler control plane, TenantCluster, NutanixCluster, and CAPI Cluster/MachineDeployment.
4. Watch the first worker NutanixMachine sit in Pending forever.

The error surfaces only in `capx-controller-manager` logs after the operator drills past butler-controller and into the CAPI Nutanix provider, where it appears as:

```
"error occurred while getting nutanix prism client from cache"
"Get \"https://<prism>:9440/api/nutanix/v3/users/me\":
 tls: failed to verify certificate: x509: certificate signed by unknown authority"
```

That error names the symptom (TLS verification failed) but not the cause (the operator's `nutanix-credentials` Secret is missing the `ca.pem` key that butler-controller would have used to populate `additionalTrustBundle`). Operators have to read butler-controller source to discover the contract. The recently-investigated seed mgmt cluster incident hit this exact path; the diagnostic chain to find the cause took longer than the actual fix.

This is a silent failure mode. butler-controller has the inputs to predict it at the moment it builds the NutanixCluster spec (it knows the endpoint scheme, the `insecure` value, and whether `caBundle` is empty), but emits no signal when those inputs combine into a guaranteed downstream TLS failure.

### Binding constraints

- **No new CRD field**: the Secret-key contract is already the API surface; adding a `ProviderConfig.spec.nutanix.caBundleRef` field would be a parallel mechanism, not the right shape. Validation belongs against the existing contract.
- **No behavior change for working setups**: configurations that already work (either `ca.pem` present, or `insecure: true`, or HTTP endpoints) must see no new warnings, no status changes, no log noise.
- **Operator-facing surface**: the warning must reach an operator who is looking at the resources they can see (TenantCluster status conditions and/or ProviderConfig status), not only butler-controller container logs.
- **Provider-specific**: Nutanix is the only provider that consumes `ca.pem` from the credentials Secret today. AWS, Azure, GCP, Harvester, Proxmox have different credential shapes and TLS configs; their validation, if needed, belongs in their own per-provider helpers.
- **Cheap to evaluate**: the check runs on every TenantCluster reconcile; it must be a pure-Go conditional on already-fetched data with no extra API calls.

## Decision

Add a `MissingTrustBundle` warning condition emitted at the point butler-controller resolves Nutanix credentials, surfaced on the **TenantCluster status** (where the operator is already looking when provisioning fails) and logged at `WARN` level with the exact remediation in the message.

### 1. Where the check fires

In `internal/controller/tenantcluster/reconcile_infrastructure.go`, after `getNutanixCredentials` returns and before `WithNutanixCredentials(...)` is called on the builder. The condition is evaluated against the resolved ProviderConfig + credentials struct, both already in scope.

### 2. Condition predicate

The warning fires if and only if **all** of:

- `pc.Spec.Provider == "nutanix"` (Nutanix-specific; other providers don't consume `ca.pem`)
- `pc.Spec.Nutanix.Endpoint` parses as HTTPS (`https://` scheme)
- `pc.Spec.Nutanix.Insecure == false` (operator wants verification)
- `creds.CABundle == ""` (no `ca.pem` key in the credentials Secret)

If any condition is false, no warning. In particular: HTTP endpoints, `insecure: true`, and Secrets with non-empty `ca.pem` produce no warning. Working setups stay silent.

### 3. Warning surfaces

Two outputs, both required:

**a. TenantCluster status condition.** Add a new condition type `NutanixCredentialsTrustBundle` to `TenantCluster.status.conditions` with:

- `status: False`
- `reason: MissingTrustBundle`
- `message: "Prism Central endpoint <endpoint> is HTTPS with insecure=false, but credentials Secret <namespace>/<name> has no ca.pem key. Worker VM provisioning will fail TLS verification at the CAPI Nutanix provider. Add the Prism CA chain as the ca.pem key of the credentials Secret."`

The condition is set during the infrastructure-reconcile phase. When the operator adds `ca.pem` and the next reconcile sees `creds.CABundle != ""`, the controller removes the condition (or sets `status: True, reason: TrustBundlePresent`).

**b. Structured log line.** Logged once per reconcile that detects the condition, at `WARN`:

```
{"level":"warn","msg":"Nutanix credentials Secret missing ca.pem; tenant TLS verification against Prism will fail",
 "tenantcluster":"<ns>/<name>","providerconfig":"<name>","endpoint":"<endpoint>",
 "credentialsSecret":"<ns>/<name>","remediation":"Add the Prism CA chain as the ca.pem key of the credentials Secret."}
```

Operator-readable via `kubectl describe tenantcluster <name>` (status condition) and via butler-controller logs (structured warning).

### 4. What the warning does NOT do

- It does not block the TenantCluster reconcile. The CR continues through its normal lifecycle; the NutanixCluster CR is still created without `additionalTrustBundle`; capx still attempts provisioning and still fails downstream. The warning surfaces the cause early; it doesn't change the failure timing.
- It does not auto-populate `ca.pem`. The bundle's source (corp CA, vendor CA, public CA) is operator policy, not something butler-controller can infer.
- It does not validate that a provided `ca.pem` actually verifies the Prism endpoint's cert. That check would require fetching the live Prism cert and a TLS handshake, neither of which belong in the reconciler hot path.

### 5. Test plan

Unit tests in `internal/controller/tenantcluster/`:

- `TestMissingTrustBundleWarning_HTTPSInsecureFalseNoCABundle_EmitsWarning`: positive case; condition added, log emitted.
- `TestMissingTrustBundleWarning_HTTPSInsecureFalseWithCABundle_NoWarning`: ca.pem present; no condition, no log.
- `TestMissingTrustBundleWarning_HTTPSInsecureTrue_NoWarning`: operator chose to skip verification; no warning.
- `TestMissingTrustBundleWarning_HTTPEndpoint_NoWarning`: plaintext endpoint; no warning.
- `TestMissingTrustBundleWarning_NonNutanixProvider_NoWarning`: AWS/Azure/etc. provider configs don't reach this code path.
- `TestMissingTrustBundleWarning_ConditionRemovedOnRemediation`: condition is cleared on a subsequent reconcile where `creds.CABundle != ""`.

The condition state transitions are also captured by integration tests in the TenantCluster suite if such suite exists.

### 6. Backward compatibility

No API field changes. The new TenantCluster status condition is additive; existing consumers ignore unknown condition types per Kubernetes convention. Existing Secrets, ProviderConfigs, and TenantClusters require no migration. Operators with working configurations see no change.

### 7. Documentation update (paired follow-up)

This ADR is paired with a documentation change in the `butler-controller` operator/bootstrap docs explicitly documenting the `ca.pem` Secret key:

> ### Nutanix credentials Secret
>
> The Secret referenced by `ProviderConfig.spec.credentialsRef` must contain `username` and `password` keys. If your Prism Central endpoint is served by an internal-CA-signed certificate and your ProviderConfig has `insecure: false` (the default and recommended production setting), you must also include a `ca.pem` key containing the Prism CA chain in PEM format. Without it, butler-controller will emit a `NutanixCredentialsTrustBundle` warning on the TenantCluster status and tenant worker VM provisioning will fail TLS verification at the CAPI Nutanix provider.
>
> Example:
>
> ```bash
> kubectl create secret generic nutanix-credentials \
>   --namespace=butler-system \
>   --from-literal=username=<user> \
>   --from-literal=password=<pass> \
>   --from-file=ca.pem=<corp-ca-chain.pem>
> ```

The documentation update lands in the same PR as the warning code.

## Consequences

### Positive

- The silent failure mode named in the Context section becomes a loud warning on the first reconcile. Mean time to diagnose drops from "read source code" to "read status condition".
- Operators who hit the warning get the exact remediation in the message: which Secret to edit, which key to add, what content goes in it.
- The check is cheap (one struct field, one string comparison, one URL parse on already-fetched data) and runs per reconcile, so it stays in sync with the actual condition.
- Working configurations are silent: no new noise for setups that already have `ca.pem`, use `insecure: true`, or use HTTP endpoints.
- Documentation makes the contract first-class instead of implementation-detail.

### Negative

- One new TenantCluster status condition type to maintain.
- Operators using `insecure: true` who later flip to `insecure: false` without adding `ca.pem` will see the warning; this is correct behavior but is one more thing for them to handle in the migration.

### Neutral

- The actual provisioning failure timing does not change. The warning is informational; the downstream capx TLS error still happens. Operators who want to act on the warning before the downstream failure can; operators who don't will see the same downstream failure they would have without this change, just preceded by a clear status condition naming the cause.
