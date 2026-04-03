// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package kamajistatus

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestIsTCPReady(t *testing.T) {
	r := &Reconciler{}

	tests := []struct {
		name        string
		tcp         map[string]interface{}
		wantReady   bool
		wantVersion string
	}{
		{
			"ready with version",
			map[string]interface{}{
				"status": map[string]interface{}{
					"kubernetes": map[string]interface{}{
						"status":  "Ready",
						"version": map[string]interface{}{"version": "v1.31.0"},
					},
				},
			},
			true, "v1.31.0",
		},
		{
			"not ready",
			map[string]interface{}{
				"status": map[string]interface{}{
					"kubernetes": map[string]interface{}{
						"status": "Provisioning",
					},
				},
			},
			false, "",
		},
		{
			"no status",
			map[string]interface{}{},
			false, "",
		},
		{
			"no kubernetes status",
			map[string]interface{}{
				"status": map[string]interface{}{},
			},
			false, "",
		},
		{
			"ready without version info",
			map[string]interface{}{
				"status": map[string]interface{}{
					"kubernetes": map[string]interface{}{
						"status": "Ready",
					},
				},
			},
			true, "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tcp := &unstructured.Unstructured{Object: tt.tcp}
			gotReady, gotVersion, err := r.isTCPReady(tcp)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotReady != tt.wantReady {
				t.Errorf("ready = %v, want %v", gotReady, tt.wantReady)
			}
			if gotVersion != tt.wantVersion {
				t.Errorf("version = %q, want %q", gotVersion, tt.wantVersion)
			}
		})
	}
}

func TestIsSCPReady(t *testing.T) {
	r := &Reconciler{}

	tests := []struct {
		name      string
		kcp       map[string]interface{}
		wantReady bool
	}{
		{
			"ready",
			map[string]interface{}{
				"status": map[string]interface{}{
					"readyReplicas": int64(1),
					"ready":         true,
				},
			},
			true,
		},
		{
			"not ready - zero replicas",
			map[string]interface{}{
				"status": map[string]interface{}{
					"readyReplicas": int64(0),
					"ready":         false,
				},
			},
			false,
		},
		{
			"not ready - missing ready field",
			map[string]interface{}{
				"status": map[string]interface{}{
					"readyReplicas": int64(1),
				},
			},
			false,
		},
		{
			"no status",
			map[string]interface{}{},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kcp := &unstructured.Unstructured{Object: tt.kcp}
			gotReady, err := r.isSCPReady(kcp)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotReady != tt.wantReady {
				t.Errorf("ready = %v, want %v", gotReady, tt.wantReady)
			}
		})
	}
}
