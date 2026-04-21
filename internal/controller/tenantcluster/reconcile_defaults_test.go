// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

func int32Ptr(v int32) *int32 { return &v }

func TestMergeClusterDefaults_EnvWinsOverTeam(t *testing.T) {
	team := &butlerv1alpha1.Team{
		Spec: butlerv1alpha1.TeamSpec{
			ClusterDefaults: &butlerv1alpha1.ClusterDefaults{
				KubernetesVersion: "v1.30.0",
				WorkerCPU:         int32Ptr(2),
				WorkerMemoryGi:    int32Ptr(8),
				WorkerDiskGi:      int32Ptr(40),
			},
			Environments: []butlerv1alpha1.EnvironmentSpec{
				{
					Name: "prod",
					ClusterDefaults: &butlerv1alpha1.ClusterDefaults{
						KubernetesVersion: "v1.31.0",
						WorkerCPU:         int32Ptr(8),
					},
				},
			},
		},
	}

	merged := mergeClusterDefaults(team, "prod")
	if merged == nil {
		t.Fatal("expected non-nil merged defaults")
	}
	if merged.KubernetesVersion != "v1.31.0" {
		t.Errorf("expected env KubernetesVersion to win: got %q", merged.KubernetesVersion)
	}
	if merged.WorkerCPU == nil || *merged.WorkerCPU != 8 {
		t.Errorf("expected env WorkerCPU=8 to win: got %v", merged.WorkerCPU)
	}
	if merged.WorkerMemoryGi == nil || *merged.WorkerMemoryGi != 8 {
		t.Errorf("expected team WorkerMemoryGi=8 to fill gap: got %v", merged.WorkerMemoryGi)
	}
	if merged.WorkerDiskGi == nil || *merged.WorkerDiskGi != 40 {
		t.Errorf("expected team WorkerDiskGi=40 to fill gap: got %v", merged.WorkerDiskGi)
	}
}

func TestMergeClusterDefaults_NoEnvMatch(t *testing.T) {
	team := &butlerv1alpha1.Team{
		Spec: butlerv1alpha1.TeamSpec{
			ClusterDefaults: &butlerv1alpha1.ClusterDefaults{WorkerCPU: int32Ptr(4)},
			Environments: []butlerv1alpha1.EnvironmentSpec{
				{Name: "prod", ClusterDefaults: &butlerv1alpha1.ClusterDefaults{WorkerCPU: int32Ptr(8)}},
			},
		},
	}

	merged := mergeClusterDefaults(team, "dev")
	if merged == nil || merged.WorkerCPU == nil || *merged.WorkerCPU != 4 {
		t.Errorf("expected team defaults when env name does not match: got %v", merged)
	}
}

func TestMergeClusterDefaults_NoDefaults(t *testing.T) {
	team := &butlerv1alpha1.Team{Spec: butlerv1alpha1.TeamSpec{}}
	if merged := mergeClusterDefaults(team, ""); merged != nil {
		t.Errorf("expected nil when neither team nor env defines defaults: got %v", merged)
	}
}

func TestApplyClusterDefaults_FillsZeroFields(t *testing.T) {
	spec := &butlerv1alpha1.TenantClusterSpec{
		Workers: butlerv1alpha1.WorkersSpec{
			MachineTemplate: butlerv1alpha1.MachineTemplateSpec{},
		},
	}
	defaults := &butlerv1alpha1.ClusterDefaults{
		KubernetesVersion: "v1.31.0",
		WorkerCount:       int32Ptr(3),
		WorkerCPU:         int32Ptr(4),
		WorkerMemoryGi:    int32Ptr(16),
		WorkerDiskGi:      int32Ptr(50),
	}

	applyClusterDefaults(spec, defaults)

	if spec.KubernetesVersion != "v1.31.0" {
		t.Errorf("expected KubernetesVersion filled: got %q", spec.KubernetesVersion)
	}
	if spec.Workers.Replicas != 3 {
		t.Errorf("expected Replicas=3: got %d", spec.Workers.Replicas)
	}
	if spec.Workers.MachineTemplate.CPU != 4 {
		t.Errorf("expected CPU=4: got %d", spec.Workers.MachineTemplate.CPU)
	}
	if spec.Workers.MachineTemplate.Memory.Cmp(resource.MustParse("16Gi")) != 0 {
		t.Errorf("expected Memory=16Gi: got %s", spec.Workers.MachineTemplate.Memory.String())
	}
	if spec.Workers.MachineTemplate.DiskSize.Cmp(resource.MustParse("50Gi")) != 0 {
		t.Errorf("expected DiskSize=50Gi: got %s", spec.Workers.MachineTemplate.DiskSize.String())
	}
}

func TestApplyClusterDefaults_DoesNotOverwriteUserSetValues(t *testing.T) {
	spec := &butlerv1alpha1.TenantClusterSpec{
		KubernetesVersion: "v1.29.0",
		Workers: butlerv1alpha1.WorkersSpec{
			Replicas: 5,
			MachineTemplate: butlerv1alpha1.MachineTemplateSpec{
				CPU:      2,
				Memory:   resource.MustParse("4Gi"),
				DiskSize: resource.MustParse("20Gi"),
			},
		},
	}
	defaults := &butlerv1alpha1.ClusterDefaults{
		KubernetesVersion: "v1.31.0",
		WorkerCount:       int32Ptr(10),
		WorkerCPU:         int32Ptr(8),
		WorkerMemoryGi:    int32Ptr(32),
		WorkerDiskGi:      int32Ptr(100),
	}

	applyClusterDefaults(spec, defaults)

	if spec.KubernetesVersion != "v1.29.0" {
		t.Errorf("expected user KubernetesVersion preserved: got %q", spec.KubernetesVersion)
	}
	if spec.Workers.Replicas != 5 {
		t.Errorf("expected user Replicas=5 preserved: got %d", spec.Workers.Replicas)
	}
	if spec.Workers.MachineTemplate.CPU != 2 {
		t.Errorf("expected user CPU=2 preserved: got %d", spec.Workers.MachineTemplate.CPU)
	}
	if spec.Workers.MachineTemplate.Memory.Cmp(resource.MustParse("4Gi")) != 0 {
		t.Errorf("expected user Memory=4Gi preserved: got %s", spec.Workers.MachineTemplate.Memory.String())
	}
}

func TestApplyClusterDefaults_NilDefaults(t *testing.T) {
	spec := &butlerv1alpha1.TenantClusterSpec{}
	applyClusterDefaults(spec, nil)
	if spec.KubernetesVersion != "" {
		t.Errorf("expected spec unchanged when defaults are nil")
	}
}

func TestMergeClusterDefaults_DefaultAddonsAppendDedupe(t *testing.T) {
	team := &butlerv1alpha1.Team{
		Spec: butlerv1alpha1.TeamSpec{
			ClusterDefaults: &butlerv1alpha1.ClusterDefaults{
				DefaultAddons: []string{"cilium", "cert-manager", "traefik"},
			},
			Environments: []butlerv1alpha1.EnvironmentSpec{
				{
					Name: "prod",
					ClusterDefaults: &butlerv1alpha1.ClusterDefaults{
						DefaultAddons: []string{"traefik", "grafana", "loki"},
					},
				},
			},
		},
	}

	merged := mergeClusterDefaults(team, "prod")
	if merged == nil {
		t.Fatal("expected non-nil merged defaults")
	}
	want := []string{"cilium", "cert-manager", "traefik", "grafana", "loki"}
	if len(merged.DefaultAddons) != len(want) {
		t.Fatalf("expected %d addons, got %d: %v", len(want), len(merged.DefaultAddons), merged.DefaultAddons)
	}
	for i, name := range want {
		if merged.DefaultAddons[i] != name {
			t.Errorf("addon %d: want %q, got %q", i, name, merged.DefaultAddons[i])
		}
	}
}

func TestMergeAddons_EmptyInputs(t *testing.T) {
	if got := mergeAddons(nil, nil); got != nil {
		t.Errorf("expected nil for empty inputs, got %v", got)
	}
	if got := mergeAddons([]string{"a"}, nil); len(got) != 1 || got[0] != "a" {
		t.Errorf("expected [a], got %v", got)
	}
	if got := mergeAddons(nil, []string{"b"}); len(got) != 1 || got[0] != "b" {
		t.Errorf("expected [b], got %v", got)
	}
}
