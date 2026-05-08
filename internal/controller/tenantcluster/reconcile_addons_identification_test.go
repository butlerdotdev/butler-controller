// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

func TestBuildVectorRemapSource(t *testing.T) {
	tests := []struct {
		name         string
		tc           *butlerv1alpha1.TenantCluster
		providerType string
		wantPresent  []string
		wantAbsent   []string
	}{
		{
			name: "all optional fields populated",
			tc: &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "my-cluster", Namespace: "team-x", UID: "uid-999",
					Labels: map[string]string{
						butlerv1alpha1.LabelEnvironment:    "prd",
						butlerv1alpha1.LabelProviderConfig: "nutanix-corp",
					},
				},
			},
			providerType: "nutanix",
			wantPresent: []string{
				`.cluster = "my-cluster"`,
				`.tenant_namespace = "team-x"`,
				`.cluster_uid = "uid-999"`,
				`.environment = "prd"`,
				`.provider_type = "nutanix"`,
				`.provider_config = "nutanix-corp"`,
			},
		},
		{
			name: "environment absent",
			tc: &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c1", Namespace: "ns1", UID: "u1",
					Labels: map[string]string{
						butlerv1alpha1.LabelProviderConfig: "harvester-lab",
					},
				},
			},
			providerType: "harvester",
			wantPresent:  []string{`.cluster = "c1"`, `.provider_type = "harvester"`},
			wantAbsent:   []string{".environment"},
		},
		{
			name: "provider type empty",
			tc: &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c2", Namespace: "ns2", UID: "u2",
					Labels: map[string]string{
						butlerv1alpha1.LabelEnvironment: "dev",
					},
				},
			},
			providerType: "",
			wantPresent:  []string{`.environment = "dev"`},
			wantAbsent:   []string{".provider_type", ".provider_config"},
		},
		{
			name: "all optional fields absent",
			tc: &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "bare", Namespace: "default", UID: "u3",
				},
			},
			providerType: "",
			wantPresent: []string{
				`.cluster = "bare"`,
				`.tenant_namespace = "default"`,
				`.cluster_uid = "u3"`,
				".host = .kubernetes.pod_node_name",
			},
			wantAbsent: []string{".environment", ".provider_type", ".provider_config"},
		},
		{
			name: "special characters in cluster name",
			tc: &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: `cluster-with-"quotes"`, Namespace: "ns", UID: "u4",
				},
			},
			providerType: "",
			wantPresent:  []string{`"cluster-with-\"quotes\""`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildVectorRemapSource(tt.tc, tt.providerType)
			for _, s := range tt.wantPresent {
				if !strings.Contains(got, s) {
					t.Errorf("expected VRL to contain %q\ngot:\n%s", s, got)
				}
			}
			for _, s := range tt.wantAbsent {
				if strings.Contains(got, s) {
					t.Errorf("expected VRL to NOT contain %q\ngot:\n%s", s, got)
				}
			}
		})
	}
}

func TestResolveProviderType(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	nutanixPC := &butlerv1alpha1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nutanix-jhh",
			Namespace: "butler-system",
		},
		Spec: butlerv1alpha1.ProviderConfigSpec{
			Provider: butlerv1alpha1.ProviderTypeNutanix,
			CredentialsRef: butlerv1alpha1.SecretReference{
				Name:      "nutanix-creds",
				Namespace: "butler-system",
			},
		},
	}

	tests := []struct {
		name    string
		tc      *butlerv1alpha1.TenantCluster
		objects []client.Object
		want    string
		wantErr bool
	}{
		{
			name: "nil ProviderConfigRef",
			tc: &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
			},
			want: "",
		},
		{
			name: "empty ProviderConfigRef name",
			tc: &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
				Spec: butlerv1alpha1.TenantClusterSpec{
					ProviderConfigRef: &butlerv1alpha1.ProviderReference{Name: ""},
				},
			},
			want: "",
		},
		{
			name: "valid ref resolves to nutanix",
			tc: &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
				Spec: butlerv1alpha1.TenantClusterSpec{
					ProviderConfigRef: &butlerv1alpha1.ProviderReference{
						Name:      "nutanix-jhh",
						Namespace: "butler-system",
					},
				},
			},
			objects: []client.Object{nutanixPC},
			want:    "nutanix",
		},
		{
			name: "defaults namespace to butler-system",
			tc: &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
				Spec: butlerv1alpha1.TenantClusterSpec{
					ProviderConfigRef: &butlerv1alpha1.ProviderReference{
						Name: "nutanix-jhh",
					},
				},
			},
			objects: []client.Object{nutanixPC},
			want:    "nutanix",
		},
		{
			name: "missing ProviderConfig returns error",
			tc: &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
				Spec: butlerv1alpha1.TenantClusterSpec{
					ProviderConfigRef: &butlerv1alpha1.ProviderReference{
						Name:      "does-not-exist",
						Namespace: "butler-system",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fakeclient.NewClientBuilder().WithScheme(scheme)
			if len(tt.objects) > 0 {
				builder = builder.WithObjects(tt.objects...)
			}
			cl := builder.Build()

			got, err := resolveProviderType(context.Background(), cl, tt.tc)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("providerType = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildVectorAgentValues_IncludesTransform(t *testing.T) {
	pipeline := &butlerv1alpha1.ObservabilityPipelineConfig{
		LogEndpoint: "http://10.0.0.1:8080",
	}
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "obs-perf-dev",
			Namespace: "team-obs",
			UID:       "abc-def-123",
			Labels: map[string]string{
				butlerv1alpha1.LabelEnvironment:    "dev",
				butlerv1alpha1.LabelProviderConfig: "nutanix-jhh",
			},
		},
	}

	ev := buildVectorAgentValues(pipeline, tc, "nutanix")
	if ev == nil {
		t.Fatal("expected non-nil ExtensionValues")
	}

	var vals map[string]interface{}
	if err := json.Unmarshal(ev.Raw, &vals); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	cfg := vals["customConfig"].(map[string]interface{})
	transform := cfg["transforms"].(map[string]interface{})["enrich_metadata"].(map[string]interface{})
	if transform["type"] != "remap" {
		t.Errorf("transform type = %v, want remap", transform["type"])
	}

	source := transform["source"].(string)
	for _, want := range []string{
		`.cluster = "obs-perf-dev"`,
		`.tenant_namespace = "team-obs"`,
		`.cluster_uid = "abc-def-123"`,
		`.environment = "dev"`,
		`.provider_type = "nutanix"`,
		`.provider_config = "nutanix-jhh"`,
		`.host = .kubernetes.pod_node_name`,
		`.service = .kubernetes.pod_name`,
		`.namespace = .kubernetes.pod_namespace`,
	} {
		if !strings.Contains(source, want) {
			t.Errorf("VRL source missing %q\ngot: %s", want, source)
		}
	}
}

func TestBuildVectorAgentValues_NilPipelineReturnsNil(t *testing.T) {
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns", UID: "u"},
	}
	if ev := buildVectorAgentValues(nil, tc, ""); ev != nil {
		t.Error("expected nil for nil pipeline")
	}
}

func TestBuildVectorAgentValues_EmptyEndpointReturnsNil(t *testing.T) {
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns", UID: "u"},
	}
	if ev := buildVectorAgentValues(&butlerv1alpha1.ObservabilityPipelineConfig{}, tc, ""); ev != nil {
		t.Error("expected nil for empty log endpoint")
	}
}

func TestBuildOtelCollectorValues_WithClusterIdentification(t *testing.T) {
	pipeline := &butlerv1alpha1.ObservabilityPipelineConfig{
		TraceEndpoint: "10.0.0.5:4317",
	}
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "trace-cluster", Namespace: "team-t", UID: "uid-t",
			Labels: map[string]string{
				butlerv1alpha1.LabelEnvironment:    "prd",
				butlerv1alpha1.LabelProviderConfig: "nutanix-jhh",
			},
		},
	}

	ev := buildOtelCollectorValues(pipeline, tc, "nutanix")
	if ev == nil {
		t.Fatal("expected non-nil ExtensionValues")
	}

	var vals map[string]interface{}
	if err := json.Unmarshal(ev.Raw, &vals); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	cfg := vals["config"].(map[string]interface{})

	endpoint := cfg["exporters"].(map[string]interface{})["otlp"].(map[string]interface{})["endpoint"]
	if endpoint != "10.0.0.5:4317" {
		t.Errorf("endpoint = %v, want 10.0.0.5:4317", endpoint)
	}

	proc := cfg["processors"].(map[string]interface{})["resource"].(map[string]interface{})
	attrs := proc["attributes"].([]interface{})

	attrMap := make(map[string]string)
	for _, a := range attrs {
		m := a.(map[string]interface{})
		attrMap[m["key"].(string)] = m["value"].(string)
	}

	expected := map[string]string{
		"k8s.cluster.name":       "trace-cluster",
		"k8s.namespace.name":     "team-t",
		"k8s.cluster.uid":        "uid-t",
		"butler.environment":     "prd",
		"butler.provider_type":   "nutanix",
		"butler.provider_config": "nutanix-jhh",
	}
	for k, v := range expected {
		if attrMap[k] != v {
			t.Errorf("attribute %q = %q, want %q", k, attrMap[k], v)
		}
	}

	tracesPipeline := cfg["service"].(map[string]interface{})["pipelines"].(map[string]interface{})["traces"].(map[string]interface{})
	procs := tracesPipeline["processors"].([]interface{})
	found := false
	for _, p := range procs {
		if p == "resource" {
			found = true
		}
	}
	if !found {
		t.Error("traces pipeline missing 'resource' processor")
	}
}

func TestBuildOtelCollectorValues_OptionalFieldsOmitted(t *testing.T) {
	pipeline := &butlerv1alpha1.ObservabilityPipelineConfig{
		TraceEndpoint: "10.0.0.5:4317",
	}
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bare-cluster", Namespace: "ns", UID: "uid-bare",
		},
	}

	ev := buildOtelCollectorValues(pipeline, tc, "")
	if ev == nil {
		t.Fatal("expected non-nil ExtensionValues")
	}

	var vals map[string]interface{}
	if err := json.Unmarshal(ev.Raw, &vals); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	cfg := vals["config"].(map[string]interface{})
	proc := cfg["processors"].(map[string]interface{})["resource"].(map[string]interface{})
	attrs := proc["attributes"].([]interface{})

	if len(attrs) != 3 {
		t.Errorf("expected 3 attributes, got %d", len(attrs))
		for _, a := range attrs {
			m := a.(map[string]interface{})
			t.Logf("  %s = %s", m["key"], m["value"])
		}
	}

	for _, a := range attrs {
		m := a.(map[string]interface{})
		key := m["key"].(string)
		if strings.HasPrefix(key, "butler.") {
			t.Errorf("optional attribute %q should not be present when source is empty", key)
		}
	}
}

func TestBuildOtelCollectorValues_NilPipelineReturnsNil(t *testing.T) {
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns", UID: "u"},
	}
	if ev := buildOtelCollectorValues(nil, tc, ""); ev != nil {
		t.Error("expected nil for nil pipeline")
	}
}

func TestBuildOtelCollectorValues_EmptyEndpointReturnsNil(t *testing.T) {
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns", UID: "u"},
	}
	if ev := buildOtelCollectorValues(&butlerv1alpha1.ObservabilityPipelineConfig{}, tc, ""); ev != nil {
		t.Error("expected nil for empty trace endpoint")
	}
}
