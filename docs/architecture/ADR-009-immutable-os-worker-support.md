# ADR-009: Immutable OS Worker Node Support

## Status

Proposed

## Context

Butler currently supports Talos Linux for tenant worker nodes. Talos required significant integration effort:

- Custom `steward-trustd` for mutual TLS between management and worker nodes
- Factory images with pre-baked configuration
- Two-phase bootstrap (machine config apply via talosctl, then addon installation)
- ~500+ lines of Talos-specific code in butler-controller (`internal/talos/`)
- Custom CAPI bootstrap flow (dataSecretName injection, not CABPK)

Many users prefer kubeadm-compatible immutable OSes that can join hosted control planes using standard Kubernetes bootstrap tokens. Since Steward (our Kamaji fork) generates standard kubeadm bootstrap tokens and cluster-info ConfigMaps, kubeadm-based OSes should work with far less integration effort.

We evaluated three immutable OSes: Flatcar Container Linux, Kairos, and Bottlerocket.

## Litmus Test Results

### Test Setup

- **Management cluster**: butler-beta (Talos Linux, Steward 0.3.1)
- **Test TCP**: trustd-test at 10.40.0.214:6443 (K8s v1.30.2, LoadBalancer mode)
- **Infrastructure**: Harvester (KubeVirt VMs on bare metal)
- **Join method**: Standard kubeadm bootstrap token flow via Steward

### Flatcar Container Linux - PASS

**Result**: Successfully joined Steward TCP as a worker node.

| Property | Value |
|----------|-------|
| OS | Flatcar Container Linux 4459.2.3 (Oklo) |
| Kernel | 6.12.66-flatcar |
| Container Runtime | containerd 2.0.7 |
| Kubelet | v1.30.2 |
| Join Mechanism | kubeadm join via coreos-cloudinit cloud-config |
| Config Format | `#cloud-config` (write_files + coreos.units) |
| CSR Approval | Auto-approved by Steward bootstrap token RBAC |
| Time to Join | ~2 minutes (including binary download) |

**Key findings**:

1. **Must use cloud-config on KubeVirt, not Ignition**: KubeVirt presents platform as "qemu", causing Flatcar's Ignition to read from `fw_cfg` instead of the config-2 drive. The coreos-cloudinit `#cloud-config` format works via `cloudInitConfigDrive`.

2. **Read-only `/usr`**: Flatcar's `/usr` partition is immutable. Binaries must go to `/opt/bin/` with `export PATH="/opt/bin:$PATH"`.

3. **No bundled kubeadm/kubelet**: Must download static binaries from `dl.k8s.io` and set up kubelet systemd service manually.

4. **kubeadm API version**: K8s 1.30.x uses `v1beta3` JoinConfiguration (not v1beta4).

**Integration effort for Butler**: Low. Generate a cloud-config template with:
- kubeadm join config (endpoint, token, CA hash from Steward TCP)
- kubelet/kubeadm binary download script
- kubelet systemd service setup
- Kernel module and sysctl configuration

### Kairos - CONDITIONAL PASS

**Result**: Proven to work with Kamaji (Steward's upstream) by Clastix. Hands-on testing on Harvester confirmed the core ISO is insufficient — a custom ISO with the kubeadm image baked in is required. Manual join validated that the underlying OS (Ubuntu 22.04) supports kubeadm join once containerd and networking are present.

| Property | Value |
|----------|-------|
| Join Mechanism | kubeadm join via provider-kubeadm (when kubeadm image is installed) |
| Config Format | `#cloud-config` YAML with `cluster:` section |
| Bootstrap | provider-kubeadm reads cloud-config, runs kubeadm join |
| Image Format | Container image → ISO via auroraboot (custom build required) |
| Core ISO | Too minimal — no containerd, no kubeadm, no kubelet |
| Network | `systemd-networkd` with no default `.network` file for physical NICs |

**Key findings**:

1. **Core ISO is insufficient**: The Kairos Ubuntu core ISO contains only a bootloader, systemd, and kairos-agent. No containerd, kubeadm, or kubelet. The `install.image` directive (which pulls the kubeadm container image during install) fails because network isn't available when the install agent starts on KubeVirt.

2. **Custom ISO build required**: Must use `auroraboot build-iso` to bake the Clastix `core-ubuntu-22-lts-kubeadm:1.31.1` image into an ISO. This image includes containerd + kubeadm + kubelet + provider-kubeadm. The Clastix image (Kairos v2.4.x) needs `KAIROS_ARCH="amd64"` patched into `/etc/os-release` for auroraboot compatibility.

3. **systemd-networkd needs configuration**: Kairos Ubuntu core uses systemd-networkd but ships no `.network` files for physical interfaces. KubeVirt presents NICs as `enp1s0` (predictable naming, not `eth0`). A `.network` file matching `Name=enp*` with `DHCP=yes` must be baked into the image or delivered via cloud-config.

4. **Boot stages don't execute on core**: The Kairos cloud-config `stages.boot` commands are not executed on the installed core system despite the config being persisted to `/oem/90_custom.yaml`. The kairos-agent stage executor may not be enabled as a systemd service in the core image.

5. **Proven kamaji-kairos integration**: When the kubeadm image IS properly installed, the join mechanism works — Clastix validated this. Uses `cluster.control_plane_host`, `cluster.cluster_token`, and `cluster.role: worker` in cloud-config with full `joinConfiguration` block.

6. **Alpine ISO not viable**: Alpine-based Kairos ISOs use OpenRC (no systemd, no auto-DHCP), making them incompatible with the install flow on KubeVirt.

**Integration effort for Butler**: Medium. Requires:
- Image build pipeline (auroraboot on Linux CI runner) to produce ISOs with kubeadm baked in
- systemd-networkd config for target platform NIC naming
- Cloud-config template generation for the `cluster:` section
- Image hosting and distribution (similar to Talos Factory model)

### Bottlerocket - BLOCKED

**Result**: Cannot test on Harvester. Multiple blockers.

| Property | Value |
|----------|-------|
| Join Mechanism | kubelet TLS bootstrapping (no kubeadm) |
| Config Format | TOML user-data |
| Metal Variant | Discontinued after K8s 1.29 |
| VMware Variant | OVA format, requires guestinfo interface |

**Blockers**:

1. **Metal variant discontinued**: AWS stopped publishing `metal-k8s-*` variants for K8s 1.29+ because the metal variant required too many hardware drivers, conflicting with Bottlerocket's minimalist philosophy.

2. **VMware variant incompatible with KubeVirt**: The VMware variant reads TOML config from VMware's guestinfo interface, not from cloud-init config drive. KubeVirt doesn't expose guestinfo.

3. **No generic VM variant**: Bottlerocket only supports AWS, VMware, and (deprecated) bare metal platforms. No generic QEMU/KVM variant exists.

4. **Custom build required**: A custom Bottlerocket variant targeting KubeVirt would need to be built from source with appropriate platform support (SMBIOS or config drive reading).

**Integration effort for Butler**: High. Would require:
- Custom Bottlerocket variant build for KubeVirt
- TOML config generation (different from cloud-config/Ignition)
- Provider-specific user-data delivery mechanism
- No CAPI bootstrap provider exists (would need custom)

## Decision

### Priority Order for Butler Integration

1. **Flatcar Container Linux** (Recommended first)
   - Confirmed working with Steward
   - Standard kubeadm join, familiar cloud-config format
   - CAPI support via CABPK with Ignition format (for non-KubeVirt providers)
   - Cloud-config format for KubeVirt/Harvester
   - Mature ecosystem (Kinvolk/Microsoft backing)

2. **Kairos** (Second priority)
   - Proven to work with Kamaji/Steward (Clastix reference + partial hands-on validation)
   - Good cloud-config integration via `cluster:` section
   - **Requires custom ISO build pipeline** (auroraboot + patched Clastix image)
   - Core ISO is too minimal for kubeadm — must bake in kubeadm container image
   - systemd-networkd needs platform-specific NIC config
   - Strong immutability model (A/B partition updates)

3. **Bottlerocket** (Defer)
   - Cannot run on Harvester without custom build
   - AWS-only in practice (VMware variant is niche)
   - Revisit if/when a generic QEMU variant or CAPI provider appears

### Implementation Changes Needed

**butler-api** (`tenantcluster_types.go`):
- `OSTypeFlatcar` already exists (line 51) — add `OSTypeKairos`
- Add OS-specific config fields to `WorkerSpec` (image reference, cloud-config overrides)

**butler-controller** (`internal/capi/builder.go`):
- Extend `buildKubeadmConfigTemplate()` to generate Flatcar cloud-config (currently only Rocky Linux)
- Add Kairos cloud-config generation as alternative
- Both use standard kubeadm join — share the same Steward bootstrap token flow

**butler-controller** (`internal/controller/tenantcluster/`):
- Add OS-aware bootstrap config selection in worker reconciliation
- Flatcar: cloud-config with binary download + kubeadm join
- Kairos: cloud-config with `cluster:` section
- Both skip the Talos-specific code path entirely

**No changes needed** in Steward, capi-steward, or butler-server. The bootstrap token flow is OS-agnostic.

## Consequences

### Positive

- Users get choice of worker OS without vendor lock-in to Talos
- Flatcar and Kairos use standard kubeadm join — dramatically simpler than Talos integration
- No Steward changes needed (standard kubeadm bootstrap tokens work as-is)
- CAPI bootstrap provider (CABPK) supports both Flatcar and Kairos formats
- Reduced operational complexity for teams already familiar with kubeadm

### Negative

- Multiple OS support increases testing matrix (3 OSes x N providers)
- Image management varies per OS (Flatcar: qcow2/raw, Kairos: ISO/raw, Talos: factory images)
- Cloud-config generation differs per OS (Flatcar: coreos-cloudinit, Kairos: provider-kubeadm, Talos: machine config)
- Bottlerocket deferred — users wanting AWS-optimized immutable OS must wait

### Risks

- Flatcar cloud-config on KubeVirt is a workaround (Ignition doesn't work on QEMU platform) — may need revisiting if KubeVirt adds Ignition support
- Kairos image building adds a pipeline dependency (auroraboot or equivalent)
- Per-OS CSR approval behavior needs validation at scale (Steward auto-approves bootstrap CSRs but kubelet-serving CSRs may differ)
