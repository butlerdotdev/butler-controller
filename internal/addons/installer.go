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

// Package addons provides utilities for installing and managing cluster addons.
package addons

import (
	"context"

	"k8s.io/client-go/rest"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// Installer handles addon installation for tenant clusters.
//
// It uses the Helm SDK to install, upgrade, and remove addons.
// For Butler built-in addons, it knows the chart repository and
// default values. For custom addons, it uses the HelmChartSpec.
//
// Installation order matters for some addons:
// 1. CNI (Cilium) - required for networking
// 2. LoadBalancer (MetalLB) - needs memberlist secret created first
// 3. cert-manager - may be required by other addons
// 4. Storage (Longhorn) - needs iscsi on nodes
// 5. Ingress (Traefik)
// 6. GitOps (Flux) - bootstraps last, then hands off
type Installer struct {
	// BuiltinCharts maps addon names to chart info.
	BuiltinCharts map[string]ChartInfo
}

// ChartInfo describes a Helm chart.
type ChartInfo struct {
	Repository    string
	ChartName     string
	DefaultValues map[string]interface{}
}

// NewInstaller creates a new addon installer.
func NewInstaller() *Installer {
	return &Installer{
		BuiltinCharts: map[string]ChartInfo{
			"cilium": {
				Repository: "https://helm.cilium.io/",
				ChartName:  "cilium",
			},
			"metallb": {
				Repository: "https://metallb.github.io/metallb",
				ChartName:  "metallb",
			},
			"cert-manager": {
				Repository: "https://charts.jetstack.io",
				ChartName:  "cert-manager",
				DefaultValues: map[string]interface{}{
					"installCRDs": true,
				},
			},
			"longhorn": {
				Repository: "https://charts.longhorn.io",
				ChartName:  "longhorn",
			},
			"traefik": {
				Repository: "https://traefik.github.io/charts",
				ChartName:  "traefik",
			},
		},
	}
}

// Install installs an addon into the tenant cluster.
func (i *Installer) Install(ctx context.Context, restConfig *rest.Config, addon AddonSpec) error {
	// TODO: Implement using Helm SDK
	// 1. Create Helm action configuration with tenant REST config
	// 2. Check if release already exists
	// 3. If exists, upgrade if version changed
	// 4. If not exists, install
	// 5. Wait for release to be healthy
	return nil
}

// Uninstall removes an addon from the tenant cluster.
func (i *Installer) Uninstall(ctx context.Context, restConfig *rest.Config, releaseName, namespace string) error {
	// TODO: Implement using Helm SDK
	// 1. Create Helm action configuration
	// 2. Uninstall release
	return nil
}

// IsInstalled checks if an addon is installed.
func (i *Installer) IsInstalled(ctx context.Context, restConfig *rest.Config, releaseName, namespace string) (bool, string, error) {
	// TODO: Implement using Helm SDK
	// Returns: installed, version, error
	return false, "", nil
}

// AddonSpec is a simplified addon specification.
type AddonSpec struct {
	Name        string
	Version     string
	Namespace   string
	ReleaseName string
	Values      map[string]interface{}
	Chart       *CustomChart
}

// CustomChart represents a non-builtin Helm chart.
type CustomChart struct {
	Repository string
	ChartName  string
}

// InstallCNI installs the CNI addon.
func (i *Installer) InstallCNI(ctx context.Context, restConfig *rest.Config, spec *butlerv1alpha1.CNISpec) error {
	// Cilium-specific installation
	return i.Install(ctx, restConfig, AddonSpec{
		Name:        "cilium",
		Version:     spec.Version,
		Namespace:   "kube-system",
		ReleaseName: "cilium",
		// TODO: Merge default values with spec.Values
	})
}

// InstallLoadBalancer installs the load balancer addon.
func (i *Installer) InstallLoadBalancer(ctx context.Context, restConfig *rest.Config, spec *butlerv1alpha1.LoadBalancerSpec) error {
	// TODO: Create memberlist secret first
	return i.Install(ctx, restConfig, AddonSpec{
		Name:        "metallb",
		Version:     spec.Version,
		Namespace:   "metallb-system",
		ReleaseName: "metallb",
	})
}

// CreateMetalLBPrerequisites creates resources needed before MetalLB install.
func (i *Installer) CreateMetalLBPrerequisites(ctx context.Context, restConfig *rest.Config) error {
	// TODO: Create memberlist secret
	// The secret needs to be created before MetalLB starts
	return nil
}
