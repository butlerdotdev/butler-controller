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

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

func TestBuildLocalResources(t *testing.T) {
	tc := newTestTenantCluster("toy", "team-demo")
	pc := newTestProviderConfig("local")

	rs, err := NewBuilder(tc, pc, "toy-abc12345").Build()
	if err != nil {
		t.Fatalf("failed to build local resources: %v", err)
	}

	// Every resource except the credential secret is created (CAPD needs no creds).
	if rs.Cluster == nil || rs.InfrastructureCluster == nil || rs.ControlPlane == nil ||
		rs.MachineDeployment == nil || rs.MachineTemplate == nil || rs.BootstrapConfigTemplate == nil {
		t.Fatal("expected all CAPD resources to be created")
	}
	if rs.CredentialSecret != nil {
		t.Error("local provider must not create a credential secret")
	}

	// Infrastructure cluster is a DockerCluster on the v1beta1 infra contract.
	if got := rs.InfrastructureCluster.GetKind(); got != "DockerCluster" {
		t.Errorf("InfrastructureCluster kind = %q, want DockerCluster", got)
	}
	if got := rs.InfrastructureCluster.GetAPIVersion(); got != "infrastructure.cluster.x-k8s.io/v1beta1" {
		t.Errorf("DockerCluster apiVersion = %q, want infrastructure.cluster.x-k8s.io/v1beta1", got)
	}

	// Machine template is a DockerMachineTemplate; customImage derives from the k8s version.
	if got := rs.MachineTemplate.GetKind(); got != "DockerMachineTemplate" {
		t.Errorf("MachineTemplate kind = %q, want DockerMachineTemplate", got)
	}
	img, _, _ := unstructured.NestedString(rs.MachineTemplate.Object, "spec", "template", "spec", "customImage")
	if img != "kindest/node:v1.30.2" {
		t.Errorf("customImage = %q, want kindest/node:v1.30.2", img)
	}

	// Bootstrap is a kubeadm join template (CAPD-specific): containerd criSocket + eviction-hard,
	// and NONE of the Rocky Linux package-install preKubeadmCommands.
	criSocket, _, _ := unstructured.NestedString(rs.BootstrapConfigTemplate.Object,
		"spec", "template", "spec", "joinConfiguration", "nodeRegistration", "criSocket")
	if criSocket != "unix:///var/run/containerd/containerd.sock" {
		t.Errorf("criSocket = %q, want containerd socket", criSocket)
	}
	if _, found, _ := unstructured.NestedSlice(rs.BootstrapConfigTemplate.Object, "spec", "template", "spec", "preKubeadmCommands"); found {
		t.Error("CAPD bootstrap must not carry Rocky Linux preKubeadmCommands")
	}

	// MachineDeployment uses configRef (kubeadm), not a Talos dataSecretName.
	bootstrap, _, _ := unstructured.NestedMap(rs.MachineDeployment.Object, "spec", "template", "spec", "bootstrap")
	if _, hasConfigRef := bootstrap["configRef"]; !hasConfigRef {
		t.Error("MachineDeployment bootstrap must use configRef for the local (kubeadm) path")
	}
	if _, hasDataSecret := bootstrap["dataSecretName"]; hasDataSecret {
		t.Error("local MachineDeployment must not use dataSecretName (that is the Talos path)")
	}
}

func TestLocalNodeImageOverride(t *testing.T) {
	tc := newTestTenantCluster("toy", "team-demo")
	pc := newTestProviderConfig("local")
	pc.Spec.Local = &butlerv1alpha1.LocalProviderConfig{KindNodeImage: "kindest/node:v1.29.4"}

	rs, err := NewBuilder(tc, pc, "toy-abc12345").Build()
	if err != nil {
		t.Fatalf("failed to build: %v", err)
	}
	img, _, _ := unstructured.NestedString(rs.MachineTemplate.Object, "spec", "template", "spec", "customImage")
	if img != "kindest/node:v1.29.4" {
		t.Errorf("customImage = %q, want override kindest/node:v1.29.4", img)
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
	if infraRef["kind"] != "KubevirtCluster" {
		t.Errorf("expected infrastructureRef.kind 'KubevirtCluster', got '%s'", infraRef["kind"])
	}
}

func TestBuildKubevirtCluster(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	pc := newTestProviderConfig("harvester")
	pc.Spec.CredentialsRef.Name = "harvester-creds"
	pc.Spec.CredentialsRef.Namespace = "butler-system"

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	kc := rs.InfrastructureCluster

	if kc.GetAPIVersion() != "infrastructure.cluster.x-k8s.io/v1alpha1" {
		t.Errorf("expected apiVersion 'infrastructure.cluster.x-k8s.io/v1alpha1', got '%s'", kc.GetAPIVersion())
	}
	if kc.GetKind() != "KubevirtCluster" {
		t.Errorf("expected kind 'KubevirtCluster', got '%s'", kc.GetKind())
	}

	spec := kc.Object["spec"].(map[string]interface{})

	secretRef := spec["infraClusterSecretRef"].(map[string]interface{})
	if secretRef["name"] != "harvester-creds" {
		t.Errorf("expected infraClusterSecretRef.name 'harvester-creds', got '%v'", secretRef["name"])
	}
	if secretRef["namespace"] != "butler-system" {
		t.Errorf("expected infraClusterSecretRef.namespace 'butler-system', got '%v'", secretRef["namespace"])
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
	// Replicas omitted on the TenantCluster must be omitted downstream so the
	// provider (Steward / capi-steward) applies its own default.
	if v, ok := spec["replicas"]; ok {
		t.Errorf("expected replicas to be omitted when unset on the TenantCluster, got '%v'", v)
	}
	if spec["dataStoreName"] != "default" {
		t.Errorf("expected dataStoreName 'default', got '%v'", spec["dataStoreName"])
	}
}

// TestBuildStewardControlPlane_Replicas proves the provider-owned replicas
// contract at the point where Butler builds the StewardControlPlane spec:
// an omitted (nil) value is omitted downstream so the provider chooses its
// own default; explicit values are copied exactly; and an explicit zero is
// preserved (never synthesized from nil).
func TestBuildStewardControlPlane_Replicas(t *testing.T) {
	ptrI32 := func(v int32) *int32 { return &v }
	pc := newTestProviderConfig("harvester")

	scpSpec := func(t *testing.T, r *int32) map[string]interface{} {
		t.Helper()
		tc := newTestTenantCluster("cp-replicas", "default")
		tc.Spec.ControlPlane.Replicas = r
		rs, err := NewBuilder(tc, pc, "cp-replicas-12345678").Build()
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		return rs.ControlPlane.Object["spec"].(map[string]interface{})
	}

	t.Run("omitted is omitted downstream", func(t *testing.T) {
		if v, ok := scpSpec(t, nil)["replicas"]; ok {
			t.Fatalf("replicas must be omitted when unset, got %v", v)
		}
	})

	for _, n := range []int32{1, 2, 3} {
		n := n
		t.Run("explicit preserved", func(t *testing.T) {
			got, ok := scpSpec(t, ptrI32(n))["replicas"]
			if !ok {
				t.Fatalf("replicas %d must be present", n)
			}
			if got.(int64) != int64(n) {
				t.Fatalf("replicas = %v, want %d", got, n)
			}
		})
	}

	t.Run("explicit zero preserved, never from nil", func(t *testing.T) {
		got, ok := scpSpec(t, ptrI32(0))["replicas"]
		if !ok || got.(int64) != 0 {
			t.Fatalf("explicit zero must emit 0, got present=%v val=%v", ok, got)
		}
	})
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
	if infraRef["kind"] != "KubevirtMachineTemplate" {
		t.Errorf("expected infrastructureRef.kind 'KubevirtMachineTemplate', got '%v'", infraRef["kind"])
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

func TestBuildKubevirtMachineTemplate(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	pc := newTestProviderConfig("harvester")
	pc.Spec.Harvester.NetworkName = "default/vlan40-workloads"
	pc.Spec.Harvester.ImageName = "default/rocky-9-generic-cloud"
	pc.Spec.Harvester.StorageClassName = "longhorn"

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	kvmt := rs.MachineTemplate

	if kvmt.GetAPIVersion() != "infrastructure.cluster.x-k8s.io/v1alpha1" {
		t.Errorf("expected apiVersion 'infrastructure.cluster.x-k8s.io/v1alpha1', got '%s'", kvmt.GetAPIVersion())
	}
	if kvmt.GetKind() != "KubevirtMachineTemplate" {
		t.Errorf("expected kind 'KubevirtMachineTemplate', got '%s'", kvmt.GetKind())
	}
	if kvmt.GetName() != "test-cluster-worker" {
		t.Errorf("expected name 'test-cluster-worker', got '%s'", kvmt.GetName())
	}

	// KubevirtMachineTemplate has virtualMachineTemplate nested under spec.template.spec
	spec := kvmt.Object["spec"].(map[string]interface{})
	template := spec["template"].(map[string]interface{})
	templateSpec := template["spec"].(map[string]interface{})

	vmTemplate, ok := templateSpec["virtualMachineTemplate"].(map[string]interface{})
	if !ok {
		t.Fatal("expected virtualMachineTemplate in spec")
	}

	vmSpec := vmTemplate["spec"].(map[string]interface{})
	if vmSpec["runStrategy"] != "Always" {
		t.Errorf("expected runStrategy 'Always', got '%v'", vmSpec["runStrategy"])
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

	// AllResources returns 7 slots (includes CredentialSecret which is nil for Harvester)
	if len(resources) != 7 {
		t.Fatalf("expected 7 resource slots, got %d", len(resources))
	}

	// Count non-nil resources (Harvester has no CredentialSecret)
	var nonNil []*unstructured.Unstructured
	for _, r := range resources {
		if r != nil {
			nonNil = append(nonNil, r)
		}
	}
	if len(nonNil) != 6 {
		t.Errorf("expected 6 non-nil resources, got %d", len(nonNil))
	}

	// Verify non-nil resource kinds (order: InfraCluster, Bootstrap, MachineTemplate, CP, MD, Cluster)
	expectedKinds := []string{
		"KubevirtCluster",
		"KubeadmConfigTemplate",
		"KubevirtMachineTemplate",
		"StewardControlPlane",
		"MachineDeployment",
		"Cluster",
	}

	for i, r := range nonNil {
		if r.GetKind() != expectedKinds[i] {
			t.Errorf("resource %d should be %s, got %s", i, expectedKinds[i], r.GetKind())
		}
	}
}

func TestResourcesWithoutMachineDeployment_ExcludesMD(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "test-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	resources := rs.ResourcesWithoutMachineDeployment()

	// Should have 6 slots (no MachineDeployment)
	if len(resources) != 6 {
		t.Fatalf("expected 6 resource slots, got %d", len(resources))
	}

	// Verify no MachineDeployment in the list
	for _, r := range resources {
		if r != nil && r.GetKind() == "MachineDeployment" {
			t.Error("ResourcesWithoutMachineDeployment must not contain MachineDeployment")
		}
	}

	// Verify MachineDeployment is still accessible directly on the ResourceSet
	if rs.MachineDeployment == nil {
		t.Error("MachineDeployment should still be present on the ResourceSet")
	}
	if rs.MachineDeployment.GetKind() != "MachineDeployment" {
		t.Errorf("expected MachineDeployment kind, got %s", rs.MachineDeployment.GetKind())
	}
}

func TestTalosNutanixBuild_HasMachineDeploymentWithDataSecretName(t *testing.T) {
	tc := newTestTenantCluster("talos-cluster", "default")
	tc.Spec.Workers.MachineTemplate.OS.Type = butlerv1alpha1.OSTypeTalos

	pc := newTestProviderConfig("nutanix")
	pc.Spec.Nutanix = &butlerv1alpha1.NutanixProviderConfig{
		Endpoint:    "https://prism.example.com",
		Port:        9440,
		ClusterUUID: "cluster-uuid-1234",
		SubnetUUID:  "subnet-uuid-5678",
	}

	builder := NewBuilder(tc, pc, "talos-cluster-12345678").
		WithNutanixCredentials("admin", "password", "")

	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	// Builder still produces MachineDeployment — it's the reconciler that defers creation
	if rs.MachineDeployment == nil {
		t.Fatal("expected MachineDeployment to be built")
	}

	// Talos clusters use dataSecretName, not configRef
	md := rs.MachineDeployment
	spec := md.Object["spec"].(map[string]interface{})
	template := spec["template"].(map[string]interface{})
	templateSpec := template["spec"].(map[string]interface{})
	bootstrap := templateSpec["bootstrap"].(map[string]interface{})

	if _, hasConfigRef := bootstrap["configRef"]; hasConfigRef {
		t.Error("Talos cluster should not have bootstrap.configRef")
	}

	secretName, ok := bootstrap["dataSecretName"].(string)
	if !ok {
		t.Fatal("expected bootstrap.dataSecretName for Talos cluster")
	}
	if secretName != "talos-cluster-talos-bootstrap" {
		t.Errorf("expected dataSecretName 'talos-cluster-talos-bootstrap', got '%s'", secretName)
	}

	// No BootstrapConfigTemplate for Talos
	if rs.BootstrapConfigTemplate != nil {
		t.Error("Talos cluster should not have BootstrapConfigTemplate")
	}
}

func TestTalosNutanixBuild_ResourcesWithoutMD(t *testing.T) {
	tc := newTestTenantCluster("talos-cluster", "default")
	tc.Spec.Workers.MachineTemplate.OS.Type = butlerv1alpha1.OSTypeTalos

	pc := newTestProviderConfig("nutanix")
	pc.Spec.Nutanix = &butlerv1alpha1.NutanixProviderConfig{
		Endpoint:    "https://prism.example.com",
		Port:        9440,
		ClusterUUID: "cluster-uuid-1234",
		SubnetUUID:  "subnet-uuid-5678",
	}

	builder := NewBuilder(tc, pc, "talos-cluster-12345678").
		WithNutanixCredentials("admin", "password", "")

	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	// Phase 1 resources (without MachineDeployment)
	phase1 := rs.ResourcesWithoutMachineDeployment()

	// Count non-nil resources
	var phase1NonNil []*unstructured.Unstructured
	for _, r := range phase1 {
		if r != nil {
			phase1NonNil = append(phase1NonNil, r)
		}
	}

	// Nutanix Talos: CredentialSecret, NutanixCluster, NutanixMachineTemplate,
	// StewardControlPlane, Cluster (no BootstrapConfigTemplate for Talos)
	if len(phase1NonNil) != 5 {
		t.Errorf("expected 5 non-nil phase 1 resources for Nutanix Talos, got %d", len(phase1NonNil))
		for _, r := range phase1NonNil {
			t.Logf("  - %s", r.GetKind())
		}
	}

	// MachineDeployment still on the struct for phase 2
	if rs.MachineDeployment == nil {
		t.Error("MachineDeployment must still be present on ResourceSet for phase 2 creation")
	}
}

func TestNonTalosHarvesterBuild_AllResourcesUnchanged(t *testing.T) {
	// Ensure the fix doesn't change behavior for non-Talos clusters
	tc := newTestTenantCluster("kubeadm-cluster", "default")
	// OS.Type defaults to empty (non-Talos)
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "kubeadm-cluster-12345678")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build resources: %v", err)
	}

	all := rs.AllResources()
	withoutMD := rs.ResourcesWithoutMachineDeployment()

	// AllResources should have MachineDeployment
	hasMD := false
	for _, r := range all {
		if r != nil && r.GetKind() == "MachineDeployment" {
			hasMD = true
			break
		}
	}
	if !hasMD {
		t.Error("AllResources for non-Talos cluster must include MachineDeployment")
	}

	// ResourcesWithoutMachineDeployment should NOT have it
	for _, r := range withoutMD {
		if r != nil && r.GetKind() == "MachineDeployment" {
			t.Error("ResourcesWithoutMachineDeployment must not contain MachineDeployment")
		}
	}

	// Non-Talos should have BootstrapConfigTemplate (kubeadm)
	if rs.BootstrapConfigTemplate == nil {
		t.Error("Non-Talos cluster should have BootstrapConfigTemplate")
	}

	// Non-Talos bootstrap uses configRef, not dataSecretName
	md := rs.MachineDeployment
	spec := md.Object["spec"].(map[string]interface{})
	template := spec["template"].(map[string]interface{})
	templateSpec := template["spec"].(map[string]interface{})
	bootstrap := templateSpec["bootstrap"].(map[string]interface{})

	if _, hasConfigRef := bootstrap["configRef"]; !hasConfigRef {
		t.Error("Non-Talos cluster should have bootstrap.configRef")
	}
	if _, hasSecret := bootstrap["dataSecretName"]; hasSecret {
		t.Error("Non-Talos cluster should not have bootstrap.dataSecretName")
	}
}

func TestUnsupportedProvider(t *testing.T) {
	tc := newTestTenantCluster("test-cluster", "default")
	pc := newTestProviderConfig("proxmox")

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
	if _, ok := labels[butlerv1alpha1.LabelEnvironment]; ok {
		t.Error("expected no env label when TC is unlabeled")
	}
}

func TestCommonLabelsAndAnnotations_EnvAndOwnerPropagated(t *testing.T) {
	tc := newTestTenantCluster("sandbox-cluster", "team-alpha")
	tc.Spec.TeamRef = &butlerv1alpha1.LocalObjectReference{Name: "team-alpha"}
	tc.Labels = map[string]string{
		butlerv1alpha1.LabelEnvironment: "dev",
	}
	tc.Annotations = map[string]string{
		butlerv1alpha1.AnnotationOwner: "alice@example.com",
	}
	pc := newTestProviderConfig("harvester")

	b := NewBuilder(tc, pc, "ns")

	labels := b.commonLabels()
	if labels[butlerv1alpha1.LabelEnvironment] != "dev" {
		t.Errorf("expected env label dev, got %q", labels[butlerv1alpha1.LabelEnvironment])
	}

	ann := b.commonAnnotations()
	if ann[butlerv1alpha1.AnnotationOwner] != "alice@example.com" {
		t.Errorf("expected owner annotation alice@example.com, got %q", ann[butlerv1alpha1.AnnotationOwner])
	}

	cluster := &unstructured.Unstructured{}
	b.applyCommonMetadata(cluster)
	if cluster.GetLabels()[butlerv1alpha1.LabelEnvironment] != "dev" {
		t.Error("expected env label propagated to resource")
	}
	if cluster.GetAnnotations()[butlerv1alpha1.AnnotationOwner] != "alice@example.com" {
		t.Error("expected owner annotation propagated to resource")
	}
}

func TestResolveControlPlaneResources_NoConfig(t *testing.T) {
	tc := newTestTenantCluster("test", "default")
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "ns")
	result := builder.resolveControlPlaneResources()

	if result != nil {
		t.Error("expected nil when no ButlerConfig and no TC resources")
	}
}

func TestResolveControlPlaneResources_ButlerConfigOnly(t *testing.T) {
	tc := newTestTenantCluster("test", "default")
	pc := newTestProviderConfig("harvester")
	bc := newTestButlerConfig()
	bc.Spec.DefaultControlPlaneResources = &butlerv1alpha1.ControlPlaneResourcesSpec{
		APIServer: &butlerv1alpha1.ComponentResources{
			Requests: &butlerv1alpha1.ResourceQuantities{
				CPU:    quantityPtr("100m"),
				Memory: quantityPtr("256Mi"),
			},
		},
	}

	builder := NewBuilder(tc, pc, "ns").WithButlerConfig(bc)
	result := builder.resolveControlPlaneResources()

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.APIServer == nil {
		t.Fatal("expected APIServer to be set")
	}
	if result.APIServer.Requests.CPU.String() != "100m" {
		t.Errorf("expected CPU 100m, got %s", result.APIServer.Requests.CPU.String())
	}
}

func TestResolveControlPlaneResources_TCOnly(t *testing.T) {
	tc := newTestTenantCluster("test", "default")
	tc.Spec.ControlPlane.Resources = &butlerv1alpha1.ControlPlaneResourcesSpec{
		Scheduler: &butlerv1alpha1.ComponentResources{
			Requests: &butlerv1alpha1.ResourceQuantities{
				CPU: quantityPtr("50m"),
			},
		},
	}
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "ns")
	result := builder.resolveControlPlaneResources()

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Scheduler == nil {
		t.Fatal("expected Scheduler to be set")
	}
	if result.Scheduler.Requests.CPU.String() != "50m" {
		t.Errorf("expected CPU 50m, got %s", result.Scheduler.Requests.CPU.String())
	}
	if result.APIServer != nil {
		t.Error("expected APIServer to be nil")
	}
}

func TestResolveControlPlaneResources_MergeOverride(t *testing.T) {
	tc := newTestTenantCluster("test", "default")
	tc.Spec.ControlPlane.Resources = &butlerv1alpha1.ControlPlaneResourcesSpec{
		APIServer: &butlerv1alpha1.ComponentResources{
			Requests: &butlerv1alpha1.ResourceQuantities{
				CPU:    quantityPtr("500m"),
				Memory: quantityPtr("1Gi"),
			},
		},
		// ControllerManager NOT set — should inherit from ButlerConfig
	}

	pc := newTestProviderConfig("harvester")
	bc := newTestButlerConfig()
	bc.Spec.DefaultControlPlaneResources = &butlerv1alpha1.ControlPlaneResourcesSpec{
		APIServer: &butlerv1alpha1.ComponentResources{
			Requests: &butlerv1alpha1.ResourceQuantities{
				CPU:    quantityPtr("100m"),
				Memory: quantityPtr("256Mi"),
			},
		},
		ControllerManager: &butlerv1alpha1.ComponentResources{
			Requests: &butlerv1alpha1.ResourceQuantities{
				CPU: quantityPtr("50m"),
			},
		},
	}

	builder := NewBuilder(tc, pc, "ns").WithButlerConfig(bc)
	result := builder.resolveControlPlaneResources()

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// APIServer should be overridden by TC
	if result.APIServer.Requests.CPU.String() != "500m" {
		t.Errorf("expected APIServer CPU 500m (TC override), got %s", result.APIServer.Requests.CPU.String())
	}

	// ControllerManager should inherit from ButlerConfig
	if result.ControllerManager == nil {
		t.Fatal("expected ControllerManager to inherit from ButlerConfig")
	}
	if result.ControllerManager.Requests.CPU.String() != "50m" {
		t.Errorf("expected ControllerManager CPU 50m (BC default), got %s", result.ControllerManager.Requests.CPU.String())
	}
}

func TestResolveControlPlaneResources_PartialOverride(t *testing.T) {
	tc := newTestTenantCluster("test", "default")
	tc.Spec.ControlPlane.Resources = &butlerv1alpha1.ControlPlaneResourcesSpec{
		Scheduler: &butlerv1alpha1.ComponentResources{
			Requests: &butlerv1alpha1.ResourceQuantities{
				CPU: quantityPtr("200m"),
			},
		},
	}

	pc := newTestProviderConfig("harvester")
	bc := newTestButlerConfig()
	bc.Spec.DefaultControlPlaneResources = &butlerv1alpha1.ControlPlaneResourcesSpec{
		APIServer: &butlerv1alpha1.ComponentResources{
			Requests: &butlerv1alpha1.ResourceQuantities{
				CPU: quantityPtr("100m"),
			},
		},
		Scheduler: &butlerv1alpha1.ComponentResources{
			Requests: &butlerv1alpha1.ResourceQuantities{
				CPU: quantityPtr("25m"),
			},
		},
	}

	builder := NewBuilder(tc, pc, "ns").WithButlerConfig(bc)
	result := builder.resolveControlPlaneResources()

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// APIServer from ButlerConfig (not overridden)
	if result.APIServer == nil || result.APIServer.Requests.CPU.String() != "100m" {
		t.Error("expected APIServer CPU 100m from ButlerConfig")
	}

	// Scheduler from TC (overridden)
	if result.Scheduler == nil || result.Scheduler.Requests.CPU.String() != "200m" {
		t.Error("expected Scheduler CPU 200m from TC override")
	}
}

func TestComponentResourceMap(t *testing.T) {
	tc := newTestTenantCluster("test", "default")
	pc := newTestProviderConfig("harvester")
	builder := NewBuilder(tc, pc, "ns")

	cr := &butlerv1alpha1.ComponentResources{
		Requests: &butlerv1alpha1.ResourceQuantities{
			CPU:    quantityPtr("100m"),
			Memory: quantityPtr("256Mi"),
		},
		Limits: &butlerv1alpha1.ResourceQuantities{
			CPU:    quantityPtr("2"),
			Memory: quantityPtr("1Gi"),
		},
	}

	result := builder.componentResourceMap(cr)
	resMap, ok := result["resources"].(map[string]interface{})
	if !ok {
		t.Fatal("expected resources key in result")
	}

	reqMap, ok := resMap["requests"].(map[string]interface{})
	if !ok {
		t.Fatal("expected requests in resources")
	}
	if reqMap["cpu"] != "100m" {
		t.Errorf("expected cpu 100m, got %v", reqMap["cpu"])
	}
	if reqMap["memory"] != "256Mi" {
		t.Errorf("expected memory 256Mi, got %v", reqMap["memory"])
	}

	limMap, ok := resMap["limits"].(map[string]interface{})
	if !ok {
		t.Fatal("expected limits in resources")
	}
	if limMap["cpu"] != "2" {
		t.Errorf("expected cpu 2, got %v", limMap["cpu"])
	}
	if limMap["memory"] != "1Gi" {
		t.Errorf("expected memory 1Gi, got %v", limMap["memory"])
	}
}

func TestSCPIncludesResources(t *testing.T) {
	tc := newTestTenantCluster("test", "default")
	pc := newTestProviderConfig("harvester")
	bc := newTestButlerConfig()
	bc.Spec.DefaultControlPlaneResources = &butlerv1alpha1.ControlPlaneResourcesSpec{
		APIServer: &butlerv1alpha1.ComponentResources{
			Requests: &butlerv1alpha1.ResourceQuantities{
				CPU:    quantityPtr("100m"),
				Memory: quantityPtr("256Mi"),
			},
		},
	}

	builder := NewBuilder(tc, pc, "test-ns").WithButlerConfig(bc)
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build: %v", err)
	}

	spec := rs.ControlPlane.Object["spec"].(map[string]interface{})

	// Should have apiServer key with resources
	apiServer, ok := spec["apiServer"].(map[string]interface{})
	if !ok {
		t.Fatal("expected apiServer key in SCP spec")
	}
	resMap, ok := apiServer["resources"].(map[string]interface{})
	if !ok {
		t.Fatal("expected resources in apiServer")
	}
	reqMap := resMap["requests"].(map[string]interface{})
	if reqMap["cpu"] != "100m" {
		t.Errorf("expected cpu 100m, got %v", reqMap["cpu"])
	}
}

func TestSCPNoResourcesWhenNotConfigured(t *testing.T) {
	tc := newTestTenantCluster("test", "default")
	pc := newTestProviderConfig("harvester")

	builder := NewBuilder(tc, pc, "test-ns")
	rs, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build: %v", err)
	}

	spec := rs.ControlPlane.Object["spec"].(map[string]interface{})

	// Should NOT have apiServer, controllerManager, or scheduler keys
	if _, ok := spec["apiServer"]; ok {
		t.Error("expected no apiServer key when resources not configured")
	}
	if _, ok := spec["controllerManager"]; ok {
		t.Error("expected no controllerManager key when resources not configured")
	}
	if _, ok := spec["scheduler"]; ok {
		t.Error("expected no scheduler key when resources not configured")
	}
}

func quantityPtr(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

func newTestButlerConfig() *butlerv1alpha1.ButlerConfig {
	return &butlerv1alpha1.ButlerConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: "butler",
		},
		Spec: butlerv1alpha1.ButlerConfigSpec{},
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
