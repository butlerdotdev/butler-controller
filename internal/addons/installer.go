/*
Copyright 2026 The Butler Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package addons

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Default addon versions
const (
	DefaultCiliumVersion      = "1.17.0"
	DefaultCertManagerVersion = "v1.16.2"
	DefaultLonghornVersion    = "1.7.2"
	DefaultMetalLBVersion     = "0.14.9"
	DefaultTraefikVersion     = "34.3.0"
)

// Installer handles addon installations on tenant clusters.
// Tenant clusters use Kamaji hosted control planes, so Cilium must be
// configured to reach the API server via kubernetes.default.svc.cluster.local
// rather than localhost:7445 used on management clusters.
type Installer struct {
	kubectlPath string
	helmPath    string
}

// ChartInstallOptions contains options for installing a Helm chart.
type ChartInstallOptions struct {
	RepoName    string            // e.g., "butler-velero"
	RepoURL     string            // e.g., "https://vmware-tanzu.github.io/helm-charts"
	ChartName   string            // e.g., "velero"
	ReleaseName string            // e.g., "velero"
	Namespace   string            // e.g., "velero"
	Version     string            // e.g., "7.2.1"
	Values      map[string]string // Optional helm --set values
	ValuesJSON  []byte            // Optional JSON values written to temp file and passed via --values
	Timeout     string            // Optional, defaults to "10m"
}

// NewInstaller creates a new tenant addon installer.
func NewInstaller() *Installer {
	return &Installer{
		kubectlPath: "kubectl",
		helmPath:    "helm",
	}
}

func (i *Installer) writeKubeconfig(kubeconfig []byte) (string, func(), error) {
	f, err := os.CreateTemp("", "kubeconfig-*")
	if err != nil {
		return "", nil, err
	}
	if _, err := f.Write(kubeconfig); err != nil {
		f.Close()
		os.Remove(f.Name())
		return "", nil, err
	}
	f.Close()
	return f.Name(), func() { os.Remove(f.Name()) }, nil
}

func (i *Installer) runHelm(ctx context.Context, kubeconfigPath string, args ...string) error {
	var fullArgs []string

	if len(args) > 0 && args[0] == "repo" {
		fullArgs = args
	} else {
		fullArgs = append(args, "--kubeconfig", kubeconfigPath)
	}

	cmd := exec.CommandContext(ctx, i.helmPath, fullArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("helm failed: %w, output: %s", err, string(output))
	}
	return nil
}

func (i *Installer) runKubectl(ctx context.Context, kubeconfigPath string, args ...string) error {
	fullArgs := append([]string{"--kubeconfig", kubeconfigPath}, args...)
	cmd := exec.CommandContext(ctx, i.kubectlPath, fullArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl failed: %w, output: %s", err, string(output))
	}
	return nil
}

func (i *Installer) ensurePrivilegedNamespace(ctx context.Context, kubeconfigPath string, namespace string) error {
	logger := log.FromContext(ctx)

	i.runKubectl(ctx, kubeconfigPath, "create", "namespace", namespace)

	if err := i.runKubectl(ctx, kubeconfigPath, "label", "namespace", namespace,
		"pod-security.kubernetes.io/enforce=privileged",
		"pod-security.kubernetes.io/enforce-version=latest",
		"pod-security.kubernetes.io/warn=privileged",
		"pod-security.kubernetes.io/warn-version=latest",
		"pod-security.kubernetes.io/audit=privileged",
		"pod-security.kubernetes.io/audit-version=latest",
		"--overwrite"); err != nil {
		logger.Error(err, "failed to label namespace as privileged", "namespace", namespace)
		return err
	}

	return nil
}

// CiliumConfig contains the configuration for installing Cilium.
type CiliumConfig struct {
	// Version is the Cilium version to install.
	Version string
	// APIServerHost is the hostname or IP of the API server.
	APIServerHost string
	// APIServerPort is the port of the API server.
	APIServerPort string
	// IngressIP is the IP address of the Ingress controller (for hostAlias).
	// Only used when using Ingress mode with a hostname.
	IngressIP string
}

// InstallCilium installs Cilium CNI configured for hosted control planes.
//
// For Ingress/Gateway mode (hostname-based API access):
// Uses "Template-Patch-Apply" pattern to solve the DNS bootstrap problem.
// CoreDNS needs CNI (Cilium) to run, but Cilium pods need DNS to resolve
// the API server hostname. We solve this by:
// 1. Using `helm template` to render manifests (no cluster contact)
// 2. Patching hostAliases directly into the rendered manifests
// 3. Applying with `kubectl apply` - pods start with hostAliases from the beginning
// 4. Waiting for rollout completion
//
// This ensures pods NEVER attempt to resolve the hostname without hostAliases,
// avoiding progressDeadlineSeconds failures.
//
// For LoadBalancer mode: Cilium connects directly to the LoadBalancer IP,
// no hostname resolution needed, so we use standard `helm upgrade --install --wait`.
func (i *Installer) InstallCilium(ctx context.Context, kubeconfig []byte, cfg CiliumConfig) error {
	logger := log.FromContext(ctx)

	kubeconfigPath, cleanup, err := i.writeKubeconfig(kubeconfig)
	if err != nil {
		return err
	}
	defer cleanup()

	version := cfg.Version
	if version == "" {
		version = DefaultCiliumVersion
	}

	// Determine if we need hostAlias (Ingress/Gateway mode with hostname)
	needsHostAlias := cfg.IngressIP != "" && !isIPAddress(cfg.APIServerHost)

	logger.Info("installing Cilium for tenant cluster",
		"version", version,
		"apiServerHost", cfg.APIServerHost,
		"apiServerPort", cfg.APIServerPort,
		"ingressIP", cfg.IngressIP,
		"needsHostAlias", needsHostAlias)

	if err := i.ensurePrivilegedNamespace(ctx, kubeconfigPath, "kube-system"); err != nil {
		return fmt.Errorf("failed to ensure kube-system namespace: %w", err)
	}

	if err := i.runHelm(ctx, kubeconfigPath, "repo", "add", "cilium", "https://helm.cilium.io/"); err != nil {
		logger.V(1).Info("helm repo add failed (may already exist)", "error", err)
	}
	if err := i.runHelm(ctx, kubeconfigPath, "repo", "update"); err != nil {
		logger.V(1).Info("helm repo update failed", "error", err)
	}

	baseArgs := []string{
		"--version", version,
		"--namespace", "kube-system",
		"--set", "ipam.mode=kubernetes",
		"--set", "kubeProxyReplacement=true",
		"--set", "securityContext.capabilities.ciliumAgent={CHOWN,KILL,NET_ADMIN,NET_RAW,IPC_LOCK,SYS_ADMIN,SYS_RESOURCE,DAC_OVERRIDE,FOWNER,SETGID,SETUID}",
		"--set", "securityContext.capabilities.cleanCiliumState={NET_ADMIN,SYS_ADMIN,SYS_RESOURCE}",
		"--set", "cgroup.autoMount.enabled=false",
		"--set", "cgroup.hostRoot=/sys/fs/cgroup",
		"--set", "hubble.relay.enabled=true",
		"--set", "hubble.ui.enabled=true",
		"--set", fmt.Sprintf("k8sServiceHost=%s", cfg.APIServerHost),
		"--set", fmt.Sprintf("k8sServicePort=%s", cfg.APIServerPort),
	}

	if needsHostAlias {
		// Ingress/Gateway mode: Template-Patch-Apply pattern
		logger.Info("using Template-Patch-Apply pattern for Ingress/Gateway mode")

		if err := i.installCiliumWithHostAlias(ctx, kubeconfigPath, baseArgs, cfg.APIServerHost, cfg.IngressIP); err != nil {
			return fmt.Errorf("failed to install Cilium with hostAlias: %w", err)
		}
	} else {
		// LoadBalancer mode: Standard helm install with --wait
		args := append([]string{"upgrade", "--install", "cilium", "cilium/cilium"}, baseArgs...)
		args = append(args, "--wait", "--timeout", "10m")

		if err := i.runHelm(ctx, kubeconfigPath, args...); err != nil {
			return fmt.Errorf("failed to install Cilium: %w", err)
		}
	}

	logger.Info("Cilium installed successfully")
	return nil
}

// installCiliumWithHostAlias uses helm template + kubectl apply to install Cilium
// with hostAliases pre-configured. This ensures pods start with hostAliases from
// the very first attempt, avoiding DNS resolution failures.
func (i *Installer) installCiliumWithHostAlias(ctx context.Context, kubeconfigPath string, helmArgs []string, hostname, ip string) error {
	logger := log.FromContext(ctx)

	// Step 1: Render manifests with helm template
	logger.Info("rendering Cilium manifests with helm template")
	templateArgs := append([]string{"template", "cilium", "cilium/cilium"}, helmArgs...)

	cmd := exec.CommandContext(ctx, i.helmPath, templateArgs...)
	manifestBytes, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("helm template failed: %w, stderr: %s", err, string(exitErr.Stderr))
		}
		return fmt.Errorf("helm template failed: %w", err)
	}

	// Step 2: Patch manifests to add hostAliases
	logger.Info("patching manifests with hostAliases", "hostname", hostname, "ip", ip)
	patchedManifests := i.patchManifestsWithHostAlias(string(manifestBytes), hostname, ip)

	// Step 3: Write patched manifests to temp file
	manifestFile, err := os.CreateTemp("", "cilium-manifests-*.yaml")
	if err != nil {
		return fmt.Errorf("failed to create temp manifest file: %w", err)
	}
	defer os.Remove(manifestFile.Name())

	if _, err := manifestFile.WriteString(patchedManifests); err != nil {
		manifestFile.Close()
		return fmt.Errorf("failed to write manifests: %w", err)
	}
	manifestFile.Close()

	// Step 4: Apply manifests with kubectl apply
	logger.Info("applying Cilium manifests with kubectl apply")
	if err := i.runKubectl(ctx, kubeconfigPath,
		"apply", "-f", manifestFile.Name(), "--server-side", "--force-conflicts",
	); err != nil {
		return fmt.Errorf("failed to apply Cilium manifests: %w", err)
	}

	// Step 5: Wait for rollout
	logger.Info("waiting for Cilium rollout")
	if err := i.waitForCiliumReady(ctx, kubeconfigPath); err != nil {
		return fmt.Errorf("Cilium rollout failed: %w", err)
	}

	return nil
}

// patchManifestsWithHostAlias injects hostAliases into DaemonSet and Deployment specs.
// This is a simple string-based patch that adds hostAliases to pod specs.
func (i *Installer) patchManifestsWithHostAlias(manifests, hostname, ip string) string {
	// The hostAliases block to inject (indented for pod spec)
	hostAliasesBlock := fmt.Sprintf(`      hostAliases:
      - hostnames:
        - %s
        ip: %s
`, hostname, ip)

	// Pattern to find the serviceAccountName line in pod specs and inject hostAliases before it
	// This works because serviceAccountName is typically in the pod spec alongside hostAliases
	result := manifests

	// Patch all Cilium-related service accounts that might need API server access
	// Note: helm template outputs quoted service account names like "cilium"
	serviceAccounts := []string{
		"cilium",          // Main Cilium agent DaemonSet
		"cilium-envoy",    // Cilium Envoy DaemonSet
		"cilium-operator", // Cilium Operator Deployment
		"hubble-relay",    // Hubble Relay Deployment
		"hubble-ui",       // Hubble UI Deployment
	}

	for _, sa := range serviceAccounts {
		// Try quoted format first (helm template output)
		result = strings.Replace(result,
			fmt.Sprintf("      serviceAccountName: \"%s\"\n", sa),
			hostAliasesBlock+fmt.Sprintf("      serviceAccountName: \"%s\"\n", sa),
			-1)
		// Also try unquoted format (for compatibility)
		result = strings.Replace(result,
			fmt.Sprintf("      serviceAccountName: %s\n", sa),
			hostAliasesBlock+fmt.Sprintf("      serviceAccountName: %s\n", sa),
			-1)
	}

	return result
}

// isIPAddress returns true if the given string is an IP address.
func isIPAddress(s string) bool {
	return net.ParseIP(s) != nil
}

// waitForCiliumReady waits for Cilium DaemonSet and Operator Deployment to be ready.
// For operator: waits until at least 1 pod is Ready (handles single-node clusters where
// only 1 of 2 replicas can be scheduled due to pod anti-affinity).
// For agent: uses DaemonSet rollout status which works correctly for node-scoped resources.
func (i *Installer) waitForCiliumReady(ctx context.Context, kubeconfigPath string) error {
	logger := log.FromContext(ctx)

	// Wait for DaemonSet rollout (this works correctly for DaemonSets)
	logger.Info("waiting for Cilium agent DaemonSet")
	if err := i.runKubectl(ctx, kubeconfigPath,
		"rollout", "status", "daemonset/cilium",
		"-n", "kube-system",
		"--timeout=10m",
	); err != nil {
		return fmt.Errorf("Cilium DaemonSet rollout failed: %w", err)
	}

	// For the operator, wait until deployment has Available=True condition
	// This checks MinimumReplicasAvailable which is what we care about
	// (not Progressing which may have stale ProgressDeadlineExceeded)
	logger.Info("waiting for Cilium Operator to be Available")
	if err := i.runKubectl(ctx, kubeconfigPath,
		"wait", "--for=condition=Available", "deployment/cilium-operator",
		"-n", "kube-system",
		"--timeout=5m",
	); err != nil {
		return fmt.Errorf("Cilium Operator not available: %w", err)
	}

	logger.Info("Cilium is Ready")
	return nil
}

// InstallCertManager installs cert-manager for TLS certificate management.
func (i *Installer) InstallCertManager(ctx context.Context, kubeconfig []byte, version string) error {
	logger := log.FromContext(ctx)

	kubeconfigPath, cleanup, err := i.writeKubeconfig(kubeconfig)
	if err != nil {
		return err
	}
	defer cleanup()

	if version == "" {
		version = DefaultCertManagerVersion
	}

	logger.Info("installing cert-manager", "version", version)

	if err := i.ensurePrivilegedNamespace(ctx, kubeconfigPath, "cert-manager"); err != nil {
		return fmt.Errorf("failed to prepare cert-manager namespace: %w", err)
	}

	if err := i.runHelm(ctx, kubeconfigPath, "repo", "add", "jetstack", "https://charts.jetstack.io"); err != nil {
		logger.V(1).Info("helm repo add failed (may already exist)", "error", err)
	}
	if err := i.runHelm(ctx, kubeconfigPath, "repo", "update"); err != nil {
		logger.V(1).Info("helm repo update failed", "error", err)
	}

	args := []string{
		"upgrade", "--install", "cert-manager", "jetstack/cert-manager",
		"--namespace", "cert-manager",
		"--version", version,
		"--set", "crds.enabled=true",
		"--wait",
		"--timeout", "5m",
	}

	if err := i.runHelm(ctx, kubeconfigPath, args...); err != nil {
		return fmt.Errorf("failed to install cert-manager: %w", err)
	}

	logger.Info("cert-manager installed successfully")
	return nil
}

// InstallLonghorn installs Longhorn distributed storage.
func (i *Installer) InstallLonghorn(ctx context.Context, kubeconfig []byte, version string) error {
	logger := log.FromContext(ctx)

	kubeconfigPath, cleanup, err := i.writeKubeconfig(kubeconfig)
	if err != nil {
		return err
	}
	defer cleanup()

	if version == "" {
		version = DefaultLonghornVersion
	}

	logger.Info("installing Longhorn", "version", version)

	if err := i.ensurePrivilegedNamespace(ctx, kubeconfigPath, "longhorn-system"); err != nil {
		return fmt.Errorf("failed to prepare longhorn-system namespace: %w", err)
	}

	if err := i.runHelm(ctx, kubeconfigPath, "repo", "add", "longhorn", "https://charts.longhorn.io"); err != nil {
		logger.V(1).Info("helm repo add failed (may already exist)", "error", err)
	}
	if err := i.runHelm(ctx, kubeconfigPath, "repo", "update"); err != nil {
		logger.V(1).Info("helm repo update failed", "error", err)
	}

	args := []string{
		"upgrade", "--install", "longhorn", "longhorn/longhorn",
		"--version", version,
		"--namespace", "longhorn-system",
		"--set", "defaultSettings.defaultReplicaCount=2",
		"--set", "defaultSettings.defaultDataPath=/var/lib/longhorn",
		"--wait",
		"--timeout", "10m",
	}

	if err := i.runHelm(ctx, kubeconfigPath, args...); err != nil {
		return fmt.Errorf("failed to install Longhorn: %w", err)
	}

	logger.Info("Longhorn installed successfully")
	return nil
}

// InstallMetalLB installs MetalLB load balancer and configures the IP pool.
func (i *Installer) InstallMetalLB(ctx context.Context, kubeconfig []byte, version string, poolStart string, poolEnd string) error {
	logger := log.FromContext(ctx)

	kubeconfigPath, cleanup, err := i.writeKubeconfig(kubeconfig)
	if err != nil {
		return err
	}
	defer cleanup()

	if version == "" {
		version = DefaultMetalLBVersion
	}

	logger.Info("installing MetalLB", "version", version)

	if err := i.ensurePrivilegedNamespace(ctx, kubeconfigPath, "metallb-system"); err != nil {
		return fmt.Errorf("failed to prepare metallb-system namespace: %w", err)
	}

	if err := i.runHelm(ctx, kubeconfigPath, "repo", "add", "metallb", "https://metallb.github.io/metallb"); err != nil {
		logger.V(1).Info("helm repo add failed (may already exist)", "error", err)
	}
	if err := i.runHelm(ctx, kubeconfigPath, "repo", "update"); err != nil {
		logger.V(1).Info("helm repo update failed", "error", err)
	}

	args := []string{
		"upgrade", "--install", "metallb", "metallb/metallb",
		"--namespace", "metallb-system",
		"--version", version,
		"--wait",
		"--timeout", "5m",
	}

	if err := i.runHelm(ctx, kubeconfigPath, args...); err != nil {
		return fmt.Errorf("failed to install MetalLB: %w", err)
	}

	if poolStart != "" && poolEnd != "" {
		addressRange := fmt.Sprintf("%s-%s", poolStart, poolEnd)
		logger.Info("configuring MetalLB IP pool", "range", addressRange)

		poolManifest := fmt.Sprintf(`apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: default-pool
  namespace: metallb-system
spec:
  addresses:
    - %s
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: default
  namespace: metallb-system
spec:
  ipAddressPools:
    - default-pool
`, addressRange)

		tmpFile, err := os.CreateTemp("", "metallb-pool-*.yaml")
		if err != nil {
			return fmt.Errorf("failed to create temp file: %w", err)
		}
		defer os.Remove(tmpFile.Name())

		if _, err := tmpFile.WriteString(poolManifest); err != nil {
			return fmt.Errorf("failed to write pool manifest: %w", err)
		}
		tmpFile.Close()

		if err := i.runKubectl(ctx, kubeconfigPath, "apply", "-f", tmpFile.Name()); err != nil {
			return fmt.Errorf("failed to apply MetalLB pool: %w", err)
		}
	}

	logger.Info("MetalLB installed successfully")
	return nil
}

// UpdateMetalLBPool updates the MetalLB IPAddressPool with the given address ranges.
// This is used by elastic IPAM to configure multiple address ranges.
func (i *Installer) UpdateMetalLBPool(ctx context.Context, kubeconfig []byte, ranges []string) error {
	logger := log.FromContext(ctx)

	kubeconfigPath, cleanup, err := i.writeKubeconfig(kubeconfig)
	if err != nil {
		return err
	}
	defer cleanup()

	// Build addresses YAML list
	var addressLines string
	for _, r := range ranges {
		addressLines += fmt.Sprintf("    - %s\n", r)
	}

	poolManifest := fmt.Sprintf(`apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: default-pool
  namespace: metallb-system
spec:
  addresses:
%s---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: default
  namespace: metallb-system
spec:
  ipAddressPools:
    - default-pool
`, addressLines)

	tmpFile, err := os.CreateTemp("", "metallb-pool-*.yaml")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(poolManifest); err != nil {
		return fmt.Errorf("failed to write pool manifest: %w", err)
	}
	tmpFile.Close()

	if err := i.runKubectl(ctx, kubeconfigPath, "apply", "-f", tmpFile.Name()); err != nil {
		return fmt.Errorf("failed to apply MetalLB pool: %w", err)
	}

	logger.Info("updated MetalLB IP pool", "ranges", ranges)
	return nil
}

// InstallTraefik installs Traefik ingress controller.
func (i *Installer) InstallTraefik(ctx context.Context, kubeconfig []byte, version string) error {
	logger := log.FromContext(ctx)

	kubeconfigPath, cleanup, err := i.writeKubeconfig(kubeconfig)
	if err != nil {
		return err
	}
	defer cleanup()

	if version == "" {
		version = DefaultTraefikVersion
	}

	logger.Info("installing Traefik", "version", version)

	if err := i.ensurePrivilegedNamespace(ctx, kubeconfigPath, "traefik"); err != nil {
		return fmt.Errorf("failed to prepare traefik namespace: %w", err)
	}

	if err := i.runHelm(ctx, kubeconfigPath, "repo", "add", "traefik", "https://traefik.github.io/charts"); err != nil {
		logger.V(1).Info("helm repo add failed (may already exist)", "error", err)
	}
	if err := i.runHelm(ctx, kubeconfigPath, "repo", "update"); err != nil {
		logger.V(1).Info("helm repo update failed", "error", err)
	}

	args := []string{
		"upgrade", "--install", "traefik", "traefik/traefik",
		"--namespace", "traefik",
		"--version", version,
		"--set", "ingressClass.enabled=true",
		"--set", "ingressClass.isDefaultClass=true",
		"--set", "service.type=LoadBalancer",
		"--wait",
		"--timeout", "2m",
	}

	if err := i.runHelm(ctx, kubeconfigPath, args...); err != nil {
		return fmt.Errorf("failed to install Traefik: %w", err)
	}

	logger.Info("Traefik installed successfully")
	return nil
}

// InstallChart installs or upgrades a Helm chart.
// This is the generic method used by TenantAddon controller.
func (i *Installer) InstallChart(ctx context.Context, kubeconfig []byte, opts ChartInstallOptions) error {
	logger := log.FromContext(ctx)

	kubeconfigPath, cleanup, err := i.writeKubeconfig(kubeconfig)
	if err != nil {
		return err
	}
	defer cleanup()

	logger.Info("installing chart",
		"chart", opts.ChartName,
		"release", opts.ReleaseName,
		"namespace", opts.Namespace,
		"version", opts.Version)

	// Ensure namespace exists with privileged PSA
	if err := i.ensurePrivilegedNamespace(ctx, kubeconfigPath, opts.Namespace); err != nil {
		return fmt.Errorf("failed to prepare namespace %s: %w", opts.Namespace, err)
	}

	// Add helm repo
	if err := i.runHelm(ctx, kubeconfigPath, "repo", "add", opts.RepoName, opts.RepoURL, "--force-update"); err != nil {
		logger.V(1).Info("helm repo add failed (may already exist)", "error", err)
	}

	// Update repos
	if err := i.runHelm(ctx, kubeconfigPath, "repo", "update"); err != nil {
		logger.V(1).Info("helm repo update failed", "error", err)
	}

	// Build args
	timeout := opts.Timeout
	if timeout == "" {
		timeout = "10m"
	}

	args := []string{
		"upgrade", "--install", opts.ReleaseName, opts.RepoName + "/" + opts.ChartName,
		"--namespace", opts.Namespace,
		"--create-namespace",
		"--version", opts.Version,
		"--wait",
		"--timeout", timeout,
	}

	// Add JSON values file if provided
	if len(opts.ValuesJSON) > 0 {
		valuesFile, err := os.CreateTemp("", "helm-values-*.json")
		if err != nil {
			return fmt.Errorf("failed to create values file: %w", err)
		}
		defer os.Remove(valuesFile.Name())
		if _, err := valuesFile.Write(opts.ValuesJSON); err != nil {
			valuesFile.Close()
			return fmt.Errorf("failed to write values file: %w", err)
		}
		valuesFile.Close()
		args = append(args, "--values", valuesFile.Name())
	}

	// Add any custom values
	for k, v := range opts.Values {
		args = append(args, "--set", fmt.Sprintf("%s=%s", k, v))
	}

	if err := i.runHelm(ctx, kubeconfigPath, args...); err != nil {
		return fmt.Errorf("failed to install chart %s: %w", opts.ChartName, err)
	}

	logger.Info("chart installed successfully", "release", opts.ReleaseName)
	return nil
}

// UninstallChart uninstalls a Helm release.
func (i *Installer) UninstallChart(ctx context.Context, kubeconfig []byte, releaseName, namespace string) error {
	logger := log.FromContext(ctx)

	kubeconfigPath, cleanup, err := i.writeKubeconfig(kubeconfig)
	if err != nil {
		return err
	}
	defer cleanup()

	logger.Info("uninstalling chart", "release", releaseName, "namespace", namespace)

	if err := i.runHelm(ctx, kubeconfigPath, "uninstall", releaseName, "--namespace", namespace); err != nil {
		return fmt.Errorf("failed to uninstall release %s: %w", releaseName, err)
	}

	logger.Info("chart uninstalled successfully", "release", releaseName)
	return nil
}
