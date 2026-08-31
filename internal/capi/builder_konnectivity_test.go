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
	"encoding/json"
	"strings"
	"testing"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// TestBuildStewardControlPlane_Konnectivity verifies konnectivity resources
// (a Steward addon, not a top-level control plane component) are written into
// the StewardControlPlane addons block only when set, so capi-steward can carry
// them to the TenantControlPlane's konnectivity-server.
func TestBuildStewardControlPlane_Konnectivity(t *testing.T) {
	pc := newTestProviderConfig("harvester")
	qty := func(s string) *resource.Quantity { q := resource.MustParse(s); return &q }

	addonsFor := func(t *testing.T, k *butlerv1alpha1.ComponentResources) map[string]interface{} {
		t.Helper()
		tc := newTestTenantCluster("cp-konn", "default")
		tc.Spec.ControlPlane.Resources = &butlerv1alpha1.ControlPlaneResourcesSpec{Konnectivity: k}
		rs, err := NewBuilder(tc, pc, "cp-konn-12345678").Build()
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		spec := rs.ControlPlane.Object["spec"].(map[string]interface{})
		return spec["addons"].(map[string]interface{})
	}

	t.Run("omitted keeps konnectivity addon empty", func(t *testing.T) {
		konn, _ := addonsFor(t, nil)["konnectivity"].(map[string]interface{})
		if len(konn) != 0 {
			t.Fatalf("konnectivity addon should be empty when unset, got %v", konn)
		}
	})

	t.Run("explicit maps to addons.konnectivity.server.resources", func(t *testing.T) {
		addons := addonsFor(t, &butlerv1alpha1.ComponentResources{
			Requests: &butlerv1alpha1.ResourceQuantities{CPU: qty("10m"), Memory: qty("32Mi")},
		})
		b, _ := json.Marshal(addons["konnectivity"])
		s := string(b)
		if !strings.Contains(s, `"server"`) || !strings.Contains(s, `"resources"`) ||
			!strings.Contains(s, "32Mi") || !strings.Contains(s, "10m") {
			t.Fatalf("konnectivity resources not mapped into addons: %s", s)
		}
	})
}
