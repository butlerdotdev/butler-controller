// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package stewardsecret

import "testing"

func TestExtractClusterName(t *testing.T) {
	tests := []struct {
		name       string
		secretName string
		expected   string
	}{
		{"standard format", "my-cluster-admin-kubeconfig", "my-cluster"},
		{"dashes in name", "prod-us-east-1-admin-kubeconfig", "prod-us-east-1"},
		{"short name", "a-admin-kubeconfig", "a"},
		{"no suffix match", "my-cluster-kubeconfig", ""},
		{"just the suffix", "-admin-kubeconfig", ""},
		{"empty string", "", ""},
		{"partial suffix", "my-cluster-admin-kubeconfigx", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractClusterName(tt.secretName)
			if got != tt.expected {
				t.Errorf("extractClusterName(%q) = %q, want %q", tt.secretName, got, tt.expected)
			}
		})
	}
}
