# Immutable OS Litmus Test: Worker Nodes on Steward TCPs

**Date**: 2026-02-16
**Environment**: butler-beta management cluster, Harvester (KubeVirt)
**TCP**: trustd-test at 10.40.0.214:6443 (K8s v1.30.2, LoadBalancer mode)

## Prerequisites

### Bootstrap Token Creation

Steward creates bootstrap tokens as standard Kubernetes Secrets of type
`bootstrap.kubernetes.io/token`. For this test, we created one manually:

```bash
# Extract tenant admin kubeconfig
kubectl get secret trustd-test-admin-kubeconfig -n trustd-test \
  -o jsonpath='{.data.admin\.conf}' | base64 -d > /tmp/trustd-test-tenant.kubeconfig

# Verify cluster-info ConfigMap exists (created by Steward's BootstrapToken phase)
kubectl --kubeconfig /tmp/trustd-test-tenant.kubeconfig get configmap cluster-info -n kube-public

# Create bootstrap token
kubectl --kubeconfig /tmp/trustd-test-tenant.kubeconfig create -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: bootstrap-token-13c53e
  namespace: kube-system
type: bootstrap.kubernetes.io/token
data:
  token-id: MTNjNTNl
  token-secret: ZmI3ZDZjMTFiMWVhOWQ4ZA==
  usage-bootstrap-authentication: dHJ1ZQ==
  auth-extra-groups: c3lzdGVtOmJvb3RzdHJhcHBlcnM6a3ViZWFkbTpkZWZhdWx0LW5vZGUtdG9rZW4=
EOF
```

Token: `13c53e.fb7d6c11b1ea9d8d`
CA Hash: `sha256:68439a2b73940beb3ce6ce06266d0764240a831066a5b31b7b85324550719761`

### Verified RBAC for Bootstrap

```bash
# These ClusterRoleBindings exist on the TCP (created by Steward):
kubectl get clusterrolebinding | grep bootstrap
# kubeadm:kubelet-bootstrap
# kubeadm:node-autoapprove-bootstrap
# kubeadm:node-autoapprove-certificate-rotation
```

---

## Test 1: Flatcar Container Linux

### Image Upload

Downloaded `flatcar_production_qemu_image.img` from Flatcar stable channel (4459.2.3).
Uploaded to Harvester as VM image `flatcar-stable` (image-mdgbs).

### Attempt 1: Ignition (FAILED)

Created Butane YAML config, transpiled to Ignition JSON. Applied via `cloudInitConfigDrive`.

**Result**: Ignition not processed. Flatcar detected platform as `qemu` and read Ignition
from `/sys/firmware/qemu_fw_cfg/...` instead of the config-2 drive.

```
# From VM journal:
ignition[788]: fetching Ignition config from "system" platform
# Platform "qemu" reads from fw_cfg, not config-2 drive
```

Tried setting `ignition.platform.id=openstack` via KubeVirt `firmware.kernelBoot.kernelArgs`,
but KubeVirt rejects kernel args without an external kernel:
`kernel arguments cannot be provided without an external kernel`

### Attempt 2: Cloud-config (SUCCESS)

Switched to coreos-cloudinit `#cloud-config` format with `cloudInitConfigDrive`.

**Iteration 2a** - kubeadm API version error:
```
experimental API spec: "kubeadm.k8s.io/v1beta4" is not allowed
```
Fix: Changed to `kubeadm.k8s.io/v1beta3`

**Iteration 2b** - Read-only filesystem:
```
ln: failed to create symbolic link '/usr/local/bin/kubelet': Read-only file system
```
Fix: Removed symlink commands. Flatcar's `/usr` is immutable. Use `/opt/bin` + `export PATH`.

**Iteration 2c** - SUCCESS:

Cloud-config with:
- `write_files:` for sysctl, kernel modules, kubeadm join config, setup script
- `coreos.units:` for oneshot systemd service
- Script downloads kubeadm/kubelet/kubectl v1.30.2 to `/opt/bin/`
- Waits for API server health, then runs `kubeadm join`

### VM Spec (Harvester)

```yaml
dataVolumeTemplates:
  - metadata:
      name: flatcar-litmus-test-rootdisk
      annotations:
        harvesterhci.io/imageId: default/image-mdgbs
    spec:
      source:
        blank: {}
      storage:
        accessModes: [ReadWriteMany]
        resources:
          requests:
            storage: 40Gi
        storageClassName: longhorn-image-mdgbs
        volumeMode: Block  # REQUIRED - Harvester images use Block mode
```

Key: Must use `storage` (not `pvc`), `volumeMode: Block`, and `storageClassName: longhorn-image-*`.

### Result

```
$ kubectl --kubeconfig /tmp/trustd-test-tenant.kubeconfig get nodes
NAME                  STATUS     ROLES    AGE    VERSION
flatcar-litmus-test   NotReady   <none>   114s   v1.30.2

$ kubectl --kubeconfig /tmp/trustd-test-tenant.kubeconfig get csr
NAME        AGE    SIGNERNAME                                    REQUESTOR                 CONDITION
csr-s48fg   114s   kubernetes.io/kube-apiserver-client-kubelet   system:bootstrap:13c53e   Approved,Issued
```

Node info:
- OS Image: Flatcar Container Linux by Kinvolk 4459.2.3 (Oklo)
- Kernel: 6.12.66-flatcar
- Container Runtime: containerd://2.0.7
- Kubelet: v1.30.2

Node is NotReady because no CNI is installed (expected for bare TCP).

### Config Files

See `immutable-os-test/flatcar-cloud-config.yaml` (working config)
See `immutable-os-test/flatcar-butane.yaml` (Ignition config, does NOT work on KubeVirt)

---

## Test 2: Kairos

### Prior Art

Clastix proved Kairos + Kamaji works via `clastix/kamaji-kairos` GitHub repo.
Since Steward is a Kamaji fork with identical kubeadm bootstrap machinery, this carries over.

### Image Preparation

No pre-built qcow2 exists for Kairos with kubeadm. The intended flow is:
1. Boot from a Kairos core ISO (e.g., `kairos-ubuntu-22.04-core-amd64-generic-v3.7.2.iso`)
2. Cloud-config `install.image` directive pulls a kubeadm container image during install
3. System reboots into the kubeadm image with containerd + kubeadm + provider-kubeadm

Alternative: Use `auroraboot build-iso` to bake the kubeadm image into the ISO itself.

### Attempt 1: Alpine Core ISO (FAILED)

Downloaded `kairos-core-alpine-v3.7.2.iso` from Kairos releases. Created VM with
CD-ROM boot + empty rootdisk.

**Result**: ISO installed to disk but `install.image` pull failed — Alpine core
has no auto-DHCP (uses OpenRC, not systemd). Without network during install, the
3.4GB kubeadm container image can't be pulled. Installed system was bare Alpine
with no kubeadm, no systemctl.

### Attempt 2: Ubuntu Core ISO (PARTIAL)

Uploaded `kairos-ubuntu-22.04-core-amd64-generic-v3.7.2.iso` to Harvester.
Ubuntu has systemd and was expected to auto-DHCP during install.

**Result**: Cloud-config was correctly read and persisted to `/oem/90_custom.yaml`
(verified on installed system). However:

1. **`install.image` pull still failed**: The Kairos install agent runs before
   systemd-networkd configures the interface. Tried `stages.before-install` with
   a network wait loop — still didn't work.

2. **No DHCP after install**: Kairos Ubuntu core uses `systemd-networkd` but ships
   **zero `.network` files for physical interfaces**. Without a config file matching
   the NIC, systemd-networkd doesn't request DHCP.

3. **Interface naming**: KubeVirt presents the NIC as `enp1s0` (predictable naming),
   not `eth0`. Network configs must use `Name=enp*` or similar glob.

4. **Core image is too minimal**: No containerd, no kubeadm, no kubelet. The core
   ISO is just a bootloader + systemd + kairos-agent. Everything else was supposed
   to come from the `install.image` container.

5. **Boot stages don't execute**: The `stages.boot` commands in the cloud-config are
   NOT executed after install. `journalctl | grep kairos` shows no agent activity.
   The installed core image either doesn't have the stage executor service enabled
   or the agent isn't configured to run boot stages.

### Attempt 3: Manual Join from Core (SUCCESS)

After installing from Ubuntu core ISO, manually configured the system via SSH:

```bash
# 1. Write systemd-networkd DHCP config (the root cause fix)
sudo tee /etc/systemd/network/20-dhcp.network <<'EOF'
[Match]
Name=enp*

[Network]
DHCP=yes
EOF
sudo systemctl restart systemd-networkd
# Got IP 10.40.1.208 via DHCP

# 2. Download kubeadm, kubelet, crictl
sudo curl -sSL https://dl.k8s.io/release/v1.30.2/bin/linux/amd64/kubeadm -o /usr/local/bin/kubeadm
sudo curl -sSL https://dl.k8s.io/release/v1.30.2/bin/linux/amd64/kubelet -o /usr/local/bin/kubelet
sudo chmod +x /usr/local/bin/kubeadm /usr/local/bin/kubelet
sudo curl -Lo /tmp/crictl.tar.gz https://github.com/kubernetes-sigs/cri-tools/releases/download/v1.30.0/crictl-v1.30.0-linux-amd64.tar.gz
sudo tar -xzf /tmp/crictl.tar.gz -C /usr/local/bin

# 3. Enable ip_forward
sudo sysctl -w net.ipv4.ip_forward=1
```

**Blocker**: `kubeadm join` failed with `containerd.sock: no such file or directory`.
The core image does not include containerd. This is the definitive finding — the
core ISO is insufficient for kubeadm workloads.

### auroraboot ISO Build (BLOCKED)

Attempted to build a custom ISO with the Clastix kubeadm image baked in:

```bash
docker run --rm -v /tmp/kairos-output:/output \
  quay.io/kairos/auroraboot:latest \
  build-iso -o /output "quay.io/clastix/core-ubuntu-22-lts-kubeadm:1.31.1"
```

**Failures**:
1. **OOM with 4GB Docker**: The 3.4GB container image extraction exceeds Docker Desktop's
   default memory. Required 8GB+ VM.
2. **Missing `KAIROS_ARCH`**: The Clastix image is based on Kairos v2.4.1 and lacks
   `KAIROS_ARCH`/`KAIROS_TARGETARCH` in `/etc/os-release`. auroraboot (both v0.14.0
   and v0.19.4) requires this field for EFI image creation.
3. **Patching workaround attempted**: Built a patched Docker image adding the missing
   fields, extracted rootfs, tried `dir:` source — rsync permission issues in container.

**Resolution**: auroraboot build must run on a Linux machine (not Docker Desktop) with
the Clastix image patched to include `KAIROS_ARCH="amd64"` in `/etc/os-release`. A
GitHub Actions workflow on a Linux runner is the correct approach.

### Assessment

Kairos CAN work with Steward but **requires a custom ISO built with auroraboot**.
The core ISO is too minimal (no containerd, no kubeadm). The `install.image` pull
mechanism doesn't work reliably in KubeVirt because:
- Network isn't available when the install agent starts
- The Kairos agent's boot stages don't execute on the installed core system

When the kubeadm image IS properly installed (via a pre-built ISO), the join mechanism
is identical to standard kubeadm — proven by the Clastix kamaji-kairos reference.

**Butler Image Factory**: To support Kairos in production, Butler would need an image
build pipeline that:
1. Takes the Clastix kubeadm container image
2. Patches `KAIROS_ARCH` into `/etc/os-release`
3. Adds the systemd-networkd DHCP config for the target platform's NIC naming
4. Runs `auroraboot build-iso` to produce a bootable ISO
5. Publishes to an image registry (e.g., `ghcr.io/butlerdotdev/images/`)

### Config Files

See `immutable-os-test/kairos-cloud-config.yaml` (final cloud-config with all findings)

---

## Test 3: Bottlerocket

### Blockers

1. **Metal variant discontinued**: Bottlerocket stopped publishing `metal-k8s-*` variants
   after K8s 1.29 (GitHub issue #3794). The metal variant required too many hardware
   drivers, conflicting with Bottlerocket's minimalist philosophy.

2. **VMware variant incompatible with KubeVirt**: The VMware variant produces OVA files
   and reads TOML config from VMware's guestinfo interface. KubeVirt doesn't expose
   guestinfo — it uses cloud-init config drives.

3. **No generic QEMU/KVM variant**: Only AWS, VMware, and (deprecated) metal platforms.

### Theoretical Approach

If a compatible image existed:
```toml
[settings.kubernetes]
api-server = "https://10.40.0.214:6443"
cluster-certificate = "<base64-CA-cert>"
cluster-name = "trustd-test"
authentication-mode = "tls"
bootstrap-token = "13c53e.fb7d6c11b1ea9d8d"
```

Bottlerocket uses kubelet TLS bootstrapping directly (no kubeadm binary needed).
This is simpler conceptually but requires TOML config delivery, which is platform-specific.

### Assessment

Cannot test on Harvester without building a custom Bottlerocket variant with KubeVirt
platform support. This is a legitimate litmus test finding — Bottlerocket requires
provider-specific work for each target platform.

### Config Files

See `immutable-os-test/bottlerocket-user-data.toml` (theoretical config)

---

## Comparison Summary

| Criterion | Talos | Flatcar | Kairos | Bottlerocket |
|-----------|-------|---------|--------|--------------|
| Join mechanism | trustd + talosctl | kubeadm join | kubeadm join (provider) | kubelet TLS bootstrap |
| Config format | Talos machine config | cloud-config / Ignition | cloud-config YAML | TOML |
| Steward integration | Custom (workerBootstrap) | Standard kubeadm | Standard kubeadm | Standard (kubelet direct) |
| CAPI bootstrap | dataSecretName (custom) | CABPK (cloud-init/Ignition) | CABPK (cloud-init) | None (custom needed) |
| kubeadm binary needed | No | Yes (via static download) | Yes (bundled in image) | No |
| Container runtime | containerd (bundled) | containerd (bundled) | containerd (bundled) | containerd (bundled) |
| Tested on Harvester | Yes (production) | Yes (this test) | Partially (manual join, no containerd in core) | No (cannot) |
| Integration effort | High (done) | Low | Medium (custom ISO build pipeline) | High |
| Image availability | Factory images | qcow2 (stable channel) | ISO (custom build required via auroraboot) | OVA (VMware only) |
| User data delivery | Machine config API | cloud-init config drive | cloud-init config drive | guestinfo / partition |
| Container runtime | containerd (bundled) | containerd (bundled) | containerd (in kubeadm image, NOT core) | containerd (bundled) |
| Network config needed | N/A (Talos manages) | None (auto) | Yes (systemd-networkd .network file for enp*) | N/A |
