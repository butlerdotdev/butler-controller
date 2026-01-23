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

package capi

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

func TestNewBuilder(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "test-ns")
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "tenant-ns")

	if builder.tc != tc {
		t.Error("expected tc to be set")
	}
	if builder.providerConfig != pc {
		t.Error("expected providerConfig to be set")
	}
	if builder.namespace != "tenant-ns" {
		t.Errorf("expected namespace 'tenant-ns', got '%s'", builder.namespace)
	}
}

func TestBuildHarvesterResources(t *testing.T) {
	tc := newTestTenantCluster("my-tenant", "default")
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "my-tenant-abc12345")

	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	// Verify all resources are created
	if rs.Cluster == nil {
		t.Error("expected Cluster to be created")
	}
	if rs.InfrastructureCluster == nil {
		t.Error("expected InfrastructureCluster to be created")
	}
	if rs.ControlPlane == nil {
		t.Error("expected ControlPlane to be created")
	}
	if rs.MachineDeployment == nil {
		t.Error("expected MachineDeployment to be created")
	}
	if rs.MachineTemplate == nil {
		t.Error("expected MachineTemplate to be created")
	}
	if rs.BootstrapConfigTemplate == nil {
		t.Error("expected BootstrapConfigTemplate to be created")
	}
}

func TestBuildCluster(t *testing.T) {
	tc := newTestTenantCluster("prod-cluster", "team-alpha")
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "prod-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	cluster := rs.Cluster

	// Check GVK
	if cluster.GetAPIVersion() != "cluster.x-k8s.io/v1beta1" {
		t.Errorf("expected apiVersion 'cluster.x-k8s.io/v1beta1', got '%s'", cluster.GetAPIVersion())
	}
	if cluster.GetKind() != "Cluster" {
		t.Errorf("expected kind 'Cluster', got '%s'", cluster.GetKind())
	}

	// Check name and namespace
	if cluster.GetName() != "prod-cluster" {
		t.Errorf("expected name 'prod-cluster', got '%s'", cluster.GetName())
	}
	if cluster.GetNamespace() != "prod-cluster-12345678" {
		t.Errorf("expected namespace 'prod-cluster-12345678', got '%s'", cluster.GetNamespace())
	}

	// Check labels
	labels := cluster.GetLabels()
	if labels[butlerv1alpha1.LabelManagedBy] != "butler" {
		t.Errorf("expected managed-by label 'butler', got '%s'", labels[butlerv1alpha1.LabelManagedBy])
	}
	if labels["cluster.x-k8s.io/cluster-name"] != "prod-cluster" {
		t.Errorf("expected cluster-name label 'prod-cluster', got '%s'", labels["cluster.x-k8s.io/cluster-name"])
	}

	// Check spec references
	spec := cluster.Object["spec"].(map[string]interface{})

	cpRef := spec["controlPlaneRef"].(map[string]interface{})
	if cpRef["kind"] != "StewardControlPlane" {
		t.Errorf("expected controlPlaneRef.kind 'StewardControlPlane', got '%s'", cpRef["kind"])
	}

	infraRef := spec["infrastructureRef"].(map[string]interface{})
	if infraRef["kind"] != "HarvesterCluster" {
		t.Errorf("expected infrastructureRef.kind 'HarvesterCluster', got '%s'", infraRef["kind"])
	}
}

func TestBuildHarvesterCluster(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	pc := newTestProviderConfig("harvester")
	pc.Spec.Harvester.Namespace = "default"
	pc.Spec.CredentialsRef.Name = "harvester-creds"
	pc.Spec.CredentialsRef.Namespace = "butler-system"

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	hc := rs.InfrastructureCluster

	// Check GVK
	if hc.GetAPIVersion() != "infrastructure.cluster.x-k8s.io/v1alpha1" {
		t.Errorf("expected apiVersion 'infrastructure.cluster.x-k8s.io/v1alpha1', got '%s'", hc.GetAPIVersion())
	}
	if hc.GetKind() != "HarvesterCluster" {
		t.Errorf("expected kind 'HarvesterCluster', got '%s'", hc.GetKind())
	}

	// Check spec
	spec := hc.Object["spec"].(map[string]interface{})
	if spec["targetNamespace"] != "default" {
		t.Errorf("expected targetNamespace 'default', got '%v'", spec["targetNamespace"])
	}

	// Check loadBalancerConfig (required by HarvesterCluster CRD)
	lbConfig, ok := spec["loadBalancerConfig"].(map[string]interface{})
	if !ok {
		t.Error("expected loadBalancerConfig to be present")
	} else if lbConfig["ipamType"] != "dhcp" {
		t.Errorf("expected loadBalancerConfig.ipamType 'dhcp', got '%v'", lbConfig["ipamType"])
	}

	identitySecret := spec["identitySecret"].(map[string]interface{})
	if identitySecret["name"] != "harvester-creds" {
		t.Errorf("expected identitySecret.name 'harvester-creds', got '%v'", identitySecret["name"])
	}
	if identitySecret["namespace"] != "butler-system" {
		t.Errorf("expected identitySecret.namespace 'butler-system', got '%v'", identitySecret["namespace"])
	}
}

func TestBuildStewardControlPlane(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	tc.Spec.KubernetesVersion = "v1.30.2"
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	kcp := rs.ControlPlane

	// Check GVK
	if kcp.GetAPIVersion() != "controlplane.cluster.x-k8s.io/v1alpha1" {
		t.Errorf("expected apiVersion 'controlplane.cluster.x-k8s.io/v1alpha1', got '%s'", kcp.GetAPIVersion())
	}
	if kcp.GetKind() != "StewardControlPlane" {
		t.Errorf("expected kind 'StewardControlPlane', got '%s'", kcp.GetKind())
	}

	// Check spec
	spec := kcp.Object["spec"].(map[string]interface{})
	if spec["version"] != "v1.30.2" {
		t.Errorf("expected version 'v1.30.2', got '%v'", spec["version"])
	}
	if spec["replicas"].(int64) != 1 {
		t.Errorf("expected replicas 1, got '%v'", spec["replicas"])
	}
	if spec["dataStoreName"] != "default" {
		t.Errorf("expected dataStoreName 'default', got '%v'", spec["dataStoreName"])
	}
}

func TestBuildMachineDeployment(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	tc.Spec.Workers.Replicas = 3
	tc.Spec.KubernetesVersion = "v1.30.2"
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	md := rs.MachineDeployment

	// Check GVK
	if md.GetAPIVersion() != "cluster.x-k8s.io/v1beta1" {
		t.Errorf("expected apiVersion 'cluster.x-k8s.io/v1beta1', got '%s'", md.GetAPIVersion())
	}
	if md.GetKind() != "MachineDeployment" {
		t.Errorf("expected kind 'MachineDeployment', got '%s'", md.GetKind())
	}

	// Check name
	if md.GetName() != "test-cluster-workers" {
		t.Errorf("expected name 'test-cluster-workers', got '%s'", md.GetName())
	}

	// Check spec
	spec := md.Object["spec"].(map[string]interface{})
	if spec["clusterName"] != "test-cluster" {
		t.Errorf("expected clusterName 'test-cluster', got '%v'", spec["clusterName"])
	}
	if spec["replicas"].(int64) != 3 {
		t.Errorf("expected replicas 3, got '%v'", spec["replicas"])
	}

	// Check template references
	template := spec["template"].(map[string]interface{})
	templateSpec := template["spec"].(map[string]interface{})

	bootstrap := templateSpec["bootstrap"].(map[string]interface{})
	configRef := bootstrap["configRef"].(map[string]interface{})
	if configRef["kind"] != "KubeadmConfigTemplate" {
		t.Errorf("expected bootstrap.configRef.kind 'KubeadmConfigTemplate', got '%v'", configRef["kind"])
	}

	infraRef := templateSpec["infrastructureRef"].(map[string]interface{})
	if infraRef["kind"] != "HarvesterMachineTemplate" {
		t.Errorf("expected infrastructureRef.kind 'HarvesterMachineTemplate', got '%v'", infraRef["kind"])
	}
}

func TestBuildKubeadmConfigTemplate(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	tc.Spec.KubernetesVersion = "v1.30.2"
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	kct := rs.BootstrapConfigTemplate

	// Check GVK
	if kct.GetAPIVersion() != "bootstrap.cluster.x-k8s.io/v1beta1" {
		t.Errorf("expected apiVersion 'bootstrap.cluster.x-k8s.io/v1beta1', got '%s'", kct.GetAPIVersion())
	}
	if kct.GetKind() != "KubeadmConfigTemplate" {
		t.Errorf("expected kind 'KubeadmConfigTemplate', got '%s'", kct.GetKind())
	}

	// Check preKubeadmCommands exist and contain expected content
	spec := kct.Object["spec"].(map[string]interface{})
	template := spec["template"].(map[string]interface{})
	templateSpec := template["spec"].(map[string]interface{})

	preKubeadmCommands, ok := templateSpec["preKubeadmCommands"].([]interface{})
	if !ok {
		t.Fatal("expected preKubeadmCommands to be a slice")
	}

	// Verify some essential commands are present
	hasOverlay := false
	hasContainerd := false
	hasKubelet := false
	hasIscsi := false

	for _, cmd := range preKubeadmCommands {
		cmdStr := cmd.(string)
		if cmdStr == "modprobe overlay" {
			hasOverlay = true
		}
		if cmdStr == "dnf install -y containerd.io" {
			hasContainerd = true
		}
		if cmdStr == "systemctl enable kubelet" {
			hasKubelet = true
		}
		if cmdStr == "dnf install -y iscsi-initiator-utils nfs-utils" {
			hasIscsi = true
		}
	}

	if !hasOverlay {
		t.Error("expected preKubeadmCommands to contain 'modprobe overlay'")
	}
	if !hasContainerd {
		t.Error("expected preKubeadmCommands to contain containerd install")
	}
	if !hasKubelet {
		t.Error("expected preKubeadmCommands to contain kubelet enable")
	}
	if !hasIscsi {
		t.Error("expected preKubeadmCommands to contain iscsi-initiator-utils install (Longhorn dep)")
	}

	// Check files
	files, ok := templateSpec["files"].([]interface{})
	if !ok {
		t.Fatal("expected files to be a slice")
	}
	if len(files) == 0 {
		t.Error("expected at least one file in KubeadmConfigTemplate")
	}
}

func TestBuildHarvesterMachineTemplate(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	// Note: Workers.Resources doesn't exist in butler-api, using defaults (4 CPU, 8Gi)
	pc := newTestProviderConfig("harvester")
	pc.Spec.Harvester.NetworkName = "default/vlan40-workloads"
	pc.Spec.Harvester.ImageName = "default/rocky-9-generic-cloud"
	pc.Spec.Harvester.StorageClassName = "longhorn"

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	hmt := rs.MachineTemplate

	// Check GVK
	if hmt.GetAPIVersion() != "infrastructure.cluster.x-k8s.io/v1alpha1" {
		t.Errorf("expected apiVersion 'infrastructure.cluster.x-k8s.io/v1alpha1', got '%s'", hmt.GetAPIVersion())
	}
	if hmt.GetKind() != "HarvesterMachineTemplate" {
		t.Errorf("expected kind 'HarvesterMachineTemplate', got '%s'", hmt.GetKind())
	}

	// Check name
	if hmt.GetName() != "test-cluster-worker" {
		t.Errorf("expected name 'test-cluster-worker', got '%s'", hmt.GetName())
	}

	// Check spec - using defaults (4 CPU, 8Gi memory)
	spec := hmt.Object["spec"].(map[string]interface{})
	template := spec["template"].(map[string]interface{})
	templateSpec := template["spec"].(map[string]interface{})

	if templateSpec["cpu"].(int64) != 4 {
		t.Errorf("expected cpu 4 (default), got '%v'", templateSpec["cpu"])
	}
	if templateSpec["memory"] != "8192Mi" {
		t.Errorf("expected memory '8192Mi' (default), got '%v'", templateSpec["memory"])
	}
	if templateSpec["sshUser"] != "rocky" {
		t.Errorf("expected sshUser 'rocky', got '%v'", templateSpec["sshUser"])
	}
	// sshKeyPair is required by HarvesterMachineTemplate CRD
	if templateSpec["sshKeyPair"] != "default" {
		t.Errorf("expected sshKeyPair 'default', got '%v'", templateSpec["sshKeyPair"])
	}

	// Check networks - should be a list of strings
	networks := templateSpec["networks"].([]interface{})
	if len(networks) != 1 {
		t.Fatalf("expected 1 network, got %d", len(networks))
	}
	networkName := networks[0].(string)
	if networkName != "default/vlan40-workloads" {
		t.Errorf("expected network 'default/vlan40-workloads', got '%v'", networkName)
	}

	// Check volumes - should have volumeType
	volumes := templateSpec["volumes"].([]interface{})
	if len(volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(volumes))
	}
	volume := volumes[0].(map[string]interface{})
	if volume["volumeType"] != "image" {
		t.Errorf("expected volumeType 'image', got '%v'", volume["volumeType"])
	}
	if volume["imageName"] != "default/rocky-9-generic-cloud" {
		t.Errorf("expected imageName 'default/rocky-9-generic-cloud', got '%v'", volume["imageName"])
	}
}

func TestExtractMinorVersion(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"v1.30.2", "v1.30"},
		{"1.30.2", "v1.30"},
		{"v1.29.0", "v1.29"},
		{"v1.28", "v1.28"},
		{"1.30", "v1.30"},
	}

	for _, test := range tests {
		result := extractMinorVersion(test.input)
		if result != test.expected {
			t.Errorf("extractMinorVersion(%s) = %s, expected %s", test.input, result, test.expected)
		}
	}
}

func TestSetOwnerReferences(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	tc.UID = types.UID("12345678-1234-1234-1234-123456789012")
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	ownerRef := builder.GetOwnerReference()
	rs.SetOwnerReferences(ownerRef)

	// Check all resources have owner reference
	for _, resource := range rs.AllResources() {
		if resource == nil {
			continue
		}
		refs := resource.GetOwnerReferences()
		if len(refs) != 1 {
			t.Errorf("%s should have exactly 1 owner reference, got %d", resource.GetKind(), len(refs))
			continue
		}
		if refs[0].UID != tc.UID {
			t.Errorf("%s owner reference UID mismatch", resource.GetKind())
		}
	}
}

func TestAllResources(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	resources := rs.AllResources()

	// Should have 6 resources in the correct order
	if len(resources) != 6 {
		t.Errorf("expected 6 resources, got %d", len(resources))
	}

	// Verify order is correct for creation
	expectedOrder := []string{
		"HarvesterCluster",
		"KubeadmConfigTemplate",
		"HarvesterMachineTemplate",
		"StewardControlPlane",
		"MachineDeployment",
		"Cluster",
	}

	for i, r := range resources {
		if r.GetKind() != expectedOrder[i] {
			t.Errorf("resource %d should be %s, got %s", i, expectedOrder[i], r.GetKind())
		}
	}
}

func TestUnsupportedProvider(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	pc := newTestProviderConfig("nutanix")

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	_, err := builder.Build()

	if err == nil {
		t.Error("expected error for unsupported provider")
	}
}

func TestCommonLabels(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "team-alpha")
	tc.Spec.TeamRef = &butlerv1alpha1.LocalObjectReference{Name: "team-alpha"}
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	labels := builder.commonLabels()

	if labels[butlerv1alpha1.LabelManagedBy] != "butler" {
		t.Error("expected managed-by label")
	}
	if labels[butlerv1alpha1.LabelTenant] != "test-cluster" {
		t.Error("expected tenant label")
	}
	if labels[butlerv1alpha1.LabelTeam] != "team-alpha" {
		t.Error("expected team label when teamRef is set")
	}
}

// Helper functions to create test objects

func newTestTenantCluster(name, namespace string) *butlerv1alpha1.TenantCluster {
	return &butlerv1alpha1.TenantCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "butler.butlerlabs.dev/v1alpha1",
			Kind:       "TenantCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID("test-uid-12345678"),
		},
		Spec: butlerv1alpha1.TenantClusterSpec{
			KubernetesVersion: "v1.30.2",
			Workers: butlerv1alpha1.WorkersSpec{
				Replicas: 1,
			},
		},
	}
}

func newTestProviderConfig(providerType string) *butlerv1alpha1.ProviderConfig {
	return &butlerv1alpha1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-provider",
		},
		Spec: butlerv1alpha1.ProviderConfigSpec{
			Provider: butlerv1alpha1.ProviderType(providerType),
			CredentialsRef: butlerv1alpha1.SecretReference{
				Name:      "provider-creds",
				Namespace: "butler-system",
			},
			Harvester: &butlerv1alpha1.HarvesterProviderConfig{
				Namespace:        "default",
				NetworkName:      "default/vlan40-workloads",
				ImageName:        "default/rocky-9-generic-cloud",
				StorageClassName: "longhorn",
			},
		},
	}
}
