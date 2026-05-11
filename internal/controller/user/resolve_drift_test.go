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

package user

import (
	"testing"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-api/testdata/resolution"
)

// TestResolutionDrift asserts that butler-controller's resolution logic
// produces output matching the shared fixtures in butler-api. The
// butler-server has an equivalent test. Drift between the two
// implementations surfaces as a test failure in whichever component
// diverged from the fixture expectations.
func TestResolutionDrift(t *testing.T) {
	fixtures := resolution.LoadAll()
	if len(fixtures) == 0 {
		t.Fatal("no fixtures loaded")
	}

	for _, f := range fixtures {
		t.Run(f.Name, func(t *testing.T) {
			// Build typed Team objects from fixture input.
			teams := make([]butlerv1alpha1.Team, len(f.Input.Teams))
			for i, ft := range f.Input.Teams {
				teams[i].Name = ft.Name
				for _, u := range ft.Access.Users {
					teams[i].Spec.Access.Users = append(teams[i].Spec.Access.Users,
						butlerv1alpha1.TeamUser{
							Name: u.Name,
							Role: butlerv1alpha1.TeamRole(u.Role),
						})
				}
				for _, g := range ft.Access.Groups {
					teams[i].Spec.Access.Groups = append(teams[i].Spec.Access.Groups,
						butlerv1alpha1.TeamGroup{
							Name: g.Name,
							Role: butlerv1alpha1.TeamRole(g.Role),
						})
				}
			}

			// Run resolution.
			result := resolveTeamMemberships(f.Input.Email, f.Input.LastSeenGroups, teams)

			// Compare against fixture expectations.
			if len(result) != len(f.Expected) {
				t.Fatalf("membership count: got %d, want %d\n  got:  %v\n  want: %v",
					len(result), len(f.Expected), formatResult(result), f.Expected)
			}
			for i := range result {
				if result[i].Name != f.Expected[i].Name || result[i].Role != f.Expected[i].Role {
					t.Errorf("membership[%d]: got %s(%s), want %s(%s)",
						i, result[i].Name, result[i].Role,
						f.Expected[i].Name, f.Expected[i].Role)
				}
			}
		})
	}
}

func formatResult(memberships []butlerv1alpha1.UserTeamMembership) []resolution.ExpectedMembership {
	out := make([]resolution.ExpectedMembership, len(memberships))
	for i, m := range memberships {
		out[i] = resolution.ExpectedMembership{Name: m.Name, Role: m.Role}
	}
	return out
}
