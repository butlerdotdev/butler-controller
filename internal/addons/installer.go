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
	"os"
	"os/exec"

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

// InstallCilium installs Cilium CNI configured for hosted control planes.
func (i *Installer) InstallCilium(ctx context.Context, kubeconfig []byte, version string, apiServerHost string, apiServerPort string) error {
	logger := log.FromContext(ctx)

	kubeconfigPath, cleanup, err := i.writeKubeconfig(kubeconfig)
	if err != nil {
		return err
	}
	defer cleanup()

	if version == "" {
		version = DefaultCiliumVersion
	}

	logger.Info("installing Cilium for tenant cluster", "version", version)

	i.ensurePrivilegedNamespace(ctx, kubeconfigPath, "kube-system")

	if err := i.runHelm(ctx, kubeconfigPath, "repo", "add", "cilium", "https://helm.cilium.io/"); err != nil {
		logger.V(1).Info("helm repo add failed (may already exist)", "error", err)
	}
	if err := i.runHelm(ctx, kubeconfigPath, "repo", "update"); err != nil {
		logger.V(1).Info("helm repo update failed", "error", err)
	}

	args := []string{
		"upgrade", "--install", "cilium", "cilium/cilium",
		"--version", version,
		"--namespace", "kube-system",
		"--set", "ipam.mode=kubernetes",
		"--set", "kubeProxyReplacement=true",
		"--set", "securityContext.capabilities.ciliumAgent={CHOWN,KILL,NET_ADMIN,NET_RAW,IPC_LOCK,SYS_ADMIN,SYS_RESOURCE,DAC_OVERRIDE,FOWNER,SETGID,SETUID}",
		"--set", "securityContext.capabilities.cleanCiliumState={NET_ADMIN,SYS_ADMIN,SYS_RESOURCE}",
		"--set", "cgroup.autoMount.enabled=false",
		"--set", "cgroup.hostRoot=/sys/fs/cgroup",
		"--set", "k8sServiceHost=kubernetes.default.svc.cluster.local",
		"--set", "k8sServicePort=443",
		"--set", "hubble.relay.enabled=true",
		"--set", "hubble.ui.enabled=true",
		"--set", fmt.Sprintf("k8sServiceHost=%s", apiServerHost),
		"--set", fmt.Sprintf("k8sServicePort=%s", apiServerPort),
		"--wait",
		"--timeout", "10m",
	}

	if err := i.runHelm(ctx, kubeconfigPath, args...); err != nil {
		return fmt.Errorf("failed to install Cilium: %w", err)
	}

	logger.Info("Cilium installed successfully")
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
		"--timeout", "5m",
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
