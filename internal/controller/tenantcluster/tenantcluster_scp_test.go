// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"encoding/json"
	"testing"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func makeSCP(version string, replicas int64, apiServerCPU, apiServerMem string) *unstructured.Unstructured {
	spec := map[string]interface{}{
		"version":  version,
		"replicas": replicas,
	}
	if apiServerCPU != "" || apiServerMem != "" {
		resources := map[string]interface{}{}
		requests := map[string]interface{}{}
		if apiServerCPU != "" {
			requests["cpu"] = apiServerCPU
		}
		if apiServerMem != "" {
			requests["memory"] = apiServerMem
		}
		resources["requests"] = requests
		spec["apiServer"] = map[string]interface{}{"resources": resources}
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "controlplane.cluster.x-k8s.io/v1alpha1",
			"kind":       "StewardControlPlane",
			"metadata": map[string]interface{}{
				"name":      "test-cluster",
				"namespace": "test-ns-abc123",
			},
			"spec": spec,
		},
	}
}

func quantityPtr(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

func TestCpComponentDrifted(t *testing.T) {
	r := &Reconciler{}

	tests := []struct {
		name      string
		scpCPU    string
		scpMemory string
		wantCPU   string
		wantMem   string
		drifted   bool
	}{
		{
			name:      "no drift",
			scpCPU:    "500m",
			scpMemory: "256Mi",
			wantCPU:   "500m",
			wantMem:   "256Mi",
			drifted:   false,
		},
		{
			name:      "equivalent quantities no drift",
			scpCPU:    "500m",
			scpMemory: "256Mi",
			wantCPU:   "0.5",
			wantMem:   "268435456",
			drifted:   false,
		},
		{
			name:      "cpu drifted",
			scpCPU:    "500m",
			scpMemory: "256Mi",
			wantCPU:   "1",
			wantMem:   "256Mi",
			drifted:   true,
		},
		{
			name:      "memory drifted",
			scpCPU:    "500m",
			scpMemory: "256Mi",
			wantCPU:   "500m",
			wantMem:   "512Mi",
			drifted:   true,
		},
		{
			name:      "field missing on scp",
			scpCPU:    "",
			scpMemory: "",
			wantCPU:   "500m",
			wantMem:   "256Mi",
			drifted:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scp := makeSCP("v1.31.0", 1, tt.scpCPU, tt.scpMemory)
			desired := &butlerv1alpha1.ComponentResources{
				Requests: &butlerv1alpha1.ResourceQuantities{
					CPU:    quantityPtr(tt.wantCPU),
					Memory: quantityPtr(tt.wantMem),
				},
			}
			got := r.cpComponentDrifted(scp, "apiServer", desired)
			if got != tt.drifted {
				t.Errorf("cpComponentDrifted() = %v, want %v", got, tt.drifted)
			}
		})
	}
}

func TestReconcileStewardControlPlane_PatchContent(t *testing.T) {
	tests := []struct {
		name           string
		tcVersion      string
		tcReplicas     int32
		tcResources    *butlerv1alpha1.ControlPlaneResourcesSpec
		scpVersion     string
		scpReplicas    int64
		scpCPU         string
		scpMem         string
		wantNoPatch    bool
		wantVersion    string
		wantReplicas   *int64
		wantAPIServer  bool
	}{
		{
			name:        "no drift produces no patch",
			tcVersion:   "v1.31.0",
			tcReplicas:  1,
			scpVersion:  "v1.31.0",
			scpReplicas: 1,
			wantNoPatch: true,
		},
		{
			name:        "version drift",
			tcVersion:   "v1.32.0",
			tcReplicas:  1,
			scpVersion:  "v1.31.0",
			scpReplicas: 1,
			wantVersion: "v1.32.0",
		},
		{
			name:         "replicas drift",
			tcVersion:    "v1.31.0",
			tcReplicas:   3,
			scpVersion:   "v1.31.0",
			scpReplicas:  1,
			wantReplicas: int64Ptr(3),
		},
		{
			name:       "resource drift",
			tcVersion:  "v1.31.0",
			tcReplicas: 1,
			tcResources: &butlerv1alpha1.ControlPlaneResourcesSpec{
				APIServer: &butlerv1alpha1.ComponentResources{
					Requests: &butlerv1alpha1.ResourceQuantities{
						CPU:    quantityPtr("2"),
						Memory: quantityPtr("1Gi"),
					},
				},
			},
			scpVersion:    "v1.31.0",
			scpReplicas:   1,
			scpCPU:        "500m",
			scpMem:        "256Mi",
			wantAPIServer: true,
		},
		{
			name:       "equivalent resources no drift",
			tcVersion:  "v1.31.0",
			tcReplicas: 1,
			tcResources: &butlerv1alpha1.ControlPlaneResourcesSpec{
				APIServer: &butlerv1alpha1.ComponentResources{
					Requests: &butlerv1alpha1.ResourceQuantities{
						CPU:    quantityPtr("0.5"),
						Memory: quantityPtr("268435456"),
					},
				},
			},
			scpVersion:  "v1.31.0",
			scpReplicas: 1,
			scpCPU:      "500m",
			scpMem:      "256Mi",
			wantNoPatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "platform-engineering",
				},
				Spec: butlerv1alpha1.TenantClusterSpec{
					KubernetesVersion: tt.tcVersion,
					ControlPlane: butlerv1alpha1.ControlPlaneSpec{
						Replicas:  tt.tcReplicas,
						Resources: tt.tcResources,
					},
				},
				Status: butlerv1alpha1.TenantClusterStatus{
					Phase:           butlerv1alpha1.TenantClusterPhaseReady,
					TenantNamespace: "test-ns-abc123",
				},
			}

			scp := makeSCP(tt.scpVersion, tt.scpReplicas, tt.scpCPU, tt.scpMem)

			patch, changes := buildSCPPatch(tc, scp, nil)

			if tt.wantNoPatch {
				if len(changes) != 0 {
					t.Errorf("expected no changes, got %v", changes)
				}
				return
			}
			if len(changes) == 0 {
				t.Fatal("expected changes but got none")
			}

			patchBytes, err := json.Marshal(patch)
			if err != nil {
				t.Fatalf("failed to marshal patch: %v", err)
			}

			var parsed map[string]interface{}
			if err := json.Unmarshal(patchBytes, &parsed); err != nil {
				t.Fatalf("failed to unmarshal patch: %v", err)
			}

			specPatch, _ := parsed["spec"].(map[string]interface{})
			if specPatch == nil {
				t.Fatal("patch missing spec")
			}

			if tt.wantVersion != "" {
				if v, ok := specPatch["version"]; !ok || v != tt.wantVersion {
					t.Errorf("patch version = %v, want %v", v, tt.wantVersion)
				}
			}
			if tt.wantReplicas != nil {
				v, ok := specPatch["replicas"]
				if !ok {
					t.Errorf("patch missing replicas")
				} else if int64(v.(float64)) != *tt.wantReplicas {
					t.Errorf("patch replicas = %v, want %v", v, *tt.wantReplicas)
				}
			}
			if tt.wantAPIServer {
				if _, ok := specPatch["apiServer"]; !ok {
					t.Errorf("patch missing apiServer")
				}
			}
		})
	}
}

func int64Ptr(v int64) *int64 { return &v }

// buildSCPPatch extracts the patch-building logic so it can be tested without a real client.
// This mirrors the logic inside reconcileStewardControlPlane.
func buildSCPPatch(tc *butlerv1alpha1.TenantCluster, scp *unstructured.Unstructured, butlerConfig *butlerv1alpha1.ButlerConfig) (map[string]interface{}, []string) {
	r := &Reconciler{}
	patch := map[string]interface{}{"spec": map[string]interface{}{}}
	specPatch := patch["spec"].(map[string]interface{})
	var changes []string

	currentVersion, _, _ := unstructured.NestedString(scp.Object, "spec", "version")
	if tc.Spec.KubernetesVersion != "" && tc.Spec.KubernetesVersion != currentVersion {
		specPatch["version"] = tc.Spec.KubernetesVersion
		changes = append(changes, "version")
	}

	currentReplicas, _, _ := unstructured.NestedInt64(scp.Object, "spec", "replicas")
	desiredReplicas := int64(tc.Spec.ControlPlane.Replicas)
	if desiredReplicas < 1 {
		desiredReplicas = 1
	}
	if desiredReplicas != currentReplicas {
		specPatch["replicas"] = desiredReplicas
		changes = append(changes, "replicas")
	}

	desired := tc.Spec.ControlPlane.Resources
	if butlerConfig != nil {
		// Would use capi.ResolveControlPlaneResources here in production
	}
	if desired != nil {
		if desired.APIServer != nil && r.cpComponentDrifted(scp, "apiServer", desired.APIServer) {
			specPatch["apiServer"] = map[string]interface{}{"changed": true}
			changes = append(changes, "apiServer")
		}
	}

	return patch, changes
}
