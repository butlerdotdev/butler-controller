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

// Package flux provides utilities for bootstrapping FluxCD in tenant clusters.
package flux

import (
	"context"

	"k8s.io/client-go/rest"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// Bootstrapper handles FluxCD bootstrap for tenant clusters.
//
// The GitOps handoff model:
// 1. Butler installs addons imperatively (Helm)
// 2. Butler bootstraps Flux with user's credentials
// 3. Butler generates HelmRelease/Kustomization manifests representing installed state
// 4. Butler commits manifests to user's Git repository
// 5. Flux takes over ongoing management
// 6. Butler switches to Observe/GitOps mode (no more spec.addons changes)
type Bootstrapper struct {
}

// NewBootstrapper creates a new Flux bootstrapper.
func NewBootstrapper() *Bootstrapper {
	return &Bootstrapper{}
}

// Bootstrap installs and configures FluxCD.
func (b *Bootstrapper) Bootstrap(ctx context.Context, restConfig *rest.Config, spec *butlerv1alpha1.GitOpsSpec) error {
	// TODO: Implement Flux bootstrap
	// 1. Install Flux controllers (source-controller, kustomize-controller, helm-controller)
	// 2. Create GitRepository pointing to user's repo
	// 3. Create Kustomization for the cluster path
	return nil
}

// GenerateManifests creates GitOps manifests for installed addons.
//
// This converts the imperatively-installed addons into declarative
// HelmRelease resources that Flux can manage going forward.
func (b *Bootstrapper) GenerateManifests(installedAddons []AddonInfo) ([]byte, error) {
	// TODO: Generate YAML manifests
	// For each addon:
	// - Create HelmRepository if needed
	// - Create HelmRelease with values
	// - Organize in cluster-specific path structure
	return nil, nil
}

// CommitManifests commits generated manifests to the Git repository.
func (b *Bootstrapper) CommitManifests(ctx context.Context, repoSpec *butlerv1alpha1.GitRepositorySpec, manifests []byte, clusterPath string) error {
	// TODO: Implement Git operations
	// 1. Clone repository
	// 2. Create/update files at clusterPath
	// 3. Commit with message
	// 4. Push
	return nil
}

// AddonInfo describes an installed addon for manifest generation.
type AddonInfo struct {
	Name        string
	Version     string
	Namespace   string
	ReleaseName string
	Repository  string
	ChartName   string
	Values      map[string]interface{}
}

// IsBootstrapped checks if Flux is already bootstrapped.
func (b *Bootstrapper) IsBootstrapped(ctx context.Context, restConfig *rest.Config) (bool, error) {
	// Check if flux-system namespace exists and has running pods
	return false, nil
}
