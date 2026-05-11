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
)

func TestNormalizeGroupName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"platform-engineering", "platform-engineering"},
		{"Platform-Engineering", "platform-engineering"},
		{"platform-engineering@butlerlabs.dev", "platform-engineering"},
		{"CN=DevOps,OU=Groups,DC=corp,DC=example,DC=com", "devops"},
		{"cn=DevOps,OU=Groups,DC=corp,DC=example,DC=com", "devops"},
		{"7f2ed032-9e36-433a-acef-ccc19524b2a5", "7f2ed032-9e36-433a-acef-ccc19524b2a5"},
		{"  spacey-group  ", "spacey-group"},
		{"", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := normalizeGroupName(tt.input)
			if got != tt.want {
				t.Errorf("normalizeGroupName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestBuildGroupLookupSet(t *testing.T) {
	t.Run("nil groups", func(t *testing.T) {
		got := buildGroupLookupSet(nil)
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("empty groups", func(t *testing.T) {
		got := buildGroupLookupSet([]string{})
		if got != nil {
			t.Errorf("expected nil, got %v", got)
		}
	})

	t.Run("uuid groups", func(t *testing.T) {
		got := buildGroupLookupSet([]string{"7F2ED032-9E36-433A-ACEF-CCC19524B2A5"})
		if !got["7f2ed032-9e36-433a-acef-ccc19524b2a5"] {
			t.Error("expected lowercased UUID in set")
		}
	})

	t.Run("email-style groups", func(t *testing.T) {
		got := buildGroupLookupSet([]string{"devops@company.com"})
		if !got["devops@company.com"] {
			t.Error("expected lowercased original in set")
		}
		if !got["devops"] {
			t.Error("expected normalized (domain-stripped) in set")
		}
	})
}

func TestMatchGroup(t *testing.T) {
	tests := []struct {
		name       string
		configured string
		userGroups []string
		want       bool
	}{
		{
			name:       "exact uuid match",
			configured: "7f2ed032-9e36-433a-acef-ccc19524b2a5",
			userGroups: []string{"7f2ed032-9e36-433a-acef-ccc19524b2a5"},
			want:       true,
		},
		{
			name:       "case insensitive uuid",
			configured: "7F2ED032-9E36-433A-ACEF-CCC19524B2A5",
			userGroups: []string{"7f2ed032-9e36-433a-acef-ccc19524b2a5"},
			want:       true,
		},
		{
			name:       "email-style match via normalization",
			configured: "devops",
			userGroups: []string{"devops@company.com"},
			want:       true,
		},
		{
			name:       "ldap dn match",
			configured: "CN=DevOps,OU=Groups,DC=corp,DC=example,DC=com",
			userGroups: []string{"devops"},
			want:       true,
		},
		{
			name:       "no match",
			configured: "platform-engineering",
			userGroups: []string{"devops"},
			want:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupSet := buildGroupLookupSet(tt.userGroups)
			got := matchGroup(tt.configured, groupSet)
			if got != tt.want {
				t.Errorf("matchGroup(%q, %v) = %v, want %v", tt.configured, tt.userGroups, got, tt.want)
			}
		})
	}
}

func TestResolveTeamMemberships(t *testing.T) {
	teams := []butlerv1alpha1.Team{
		{
			Spec: butlerv1alpha1.TeamSpec{
				Access: butlerv1alpha1.TeamAccess{
					Users: []butlerv1alpha1.TeamUser{
						{Name: "alice@example.com", Role: "admin"},
					},
					Groups: []butlerv1alpha1.TeamGroup{
						{
							Name: "7f2ed032-9e36-433a-acef-ccc19524b2a5",
							Role: "viewer",
						},
					},
				},
			},
		},
		{
			Spec: butlerv1alpha1.TeamSpec{
				Access: butlerv1alpha1.TeamAccess{
					Groups: []butlerv1alpha1.TeamGroup{
						{
							Name: "7f2ed032-9e36-433a-acef-ccc19524b2a5",
							Role: "admin",
						},
					},
				},
			},
		},
	}
	teams[0].Name = "alpha"
	teams[1].Name = "beta"

	t.Run("manual membership only", func(t *testing.T) {
		result := resolveTeamMemberships("alice@example.com", nil, teams)
		if len(result) != 1 {
			t.Fatalf("expected 1 membership, got %d", len(result))
		}
		if result[0].Name != "alpha" || result[0].Role != "admin" {
			t.Errorf("expected alpha(admin), got %s(%s)", result[0].Name, result[0].Role)
		}
	})

	t.Run("group membership only", func(t *testing.T) {
		result := resolveTeamMemberships(
			"bob@example.com",
			[]string{"7f2ed032-9e36-433a-acef-ccc19524b2a5"},
			teams,
		)
		if len(result) != 2 {
			t.Fatalf("expected 2 memberships, got %d", len(result))
		}
		// Sorted by name: alpha, beta
		if result[0].Name != "alpha" || result[0].Role != "viewer" {
			t.Errorf("expected alpha(viewer), got %s(%s)", result[0].Name, result[0].Role)
		}
		if result[1].Name != "beta" || result[1].Role != "admin" {
			t.Errorf("expected beta(admin), got %s(%s)", result[1].Name, result[1].Role)
		}
	})

	t.Run("manual and group on same team, highest role wins", func(t *testing.T) {
		result := resolveTeamMemberships(
			"alice@example.com",
			[]string{"7f2ed032-9e36-433a-acef-ccc19524b2a5"},
			teams,
		)
		if len(result) != 2 {
			t.Fatalf("expected 2 memberships, got %d", len(result))
		}
		// alpha: manual=admin, group=viewer -> admin wins
		if result[0].Name != "alpha" || result[0].Role != "admin" {
			t.Errorf("expected alpha(admin), got %s(%s)", result[0].Name, result[0].Role)
		}
		// beta: group=admin
		if result[1].Name != "beta" || result[1].Role != "admin" {
			t.Errorf("expected beta(admin), got %s(%s)", result[1].Name, result[1].Role)
		}
	})

	t.Run("no memberships", func(t *testing.T) {
		result := resolveTeamMemberships("nobody@example.com", nil, teams)
		if len(result) != 0 {
			t.Errorf("expected 0 memberships, got %d", len(result))
		}
	})

	t.Run("empty groups, manual only", func(t *testing.T) {
		result := resolveTeamMemberships("alice@example.com", []string{}, teams)
		if len(result) != 1 {
			t.Fatalf("expected 1 membership, got %d", len(result))
		}
		if result[0].Name != "alpha" || result[0].Role != "admin" {
			t.Errorf("expected alpha(admin), got %s(%s)", result[0].Name, result[0].Role)
		}
	})

	t.Run("case insensitive email match", func(t *testing.T) {
		result := resolveTeamMemberships("ALICE@Example.COM", nil, teams)
		if len(result) != 1 {
			t.Fatalf("expected 1 membership, got %d", len(result))
		}
		if result[0].Name != "alpha" {
			t.Errorf("expected alpha, got %s", result[0].Name)
		}
	})
}

func TestResolveTeamMembershipsDeterministic(t *testing.T) {
	// Teams named in reverse order to exercise sorting.
	teams := []butlerv1alpha1.Team{
		{
			Spec: butlerv1alpha1.TeamSpec{
				Access: butlerv1alpha1.TeamAccess{
					Users: []butlerv1alpha1.TeamUser{
						{Name: "user@example.com", Role: "viewer"},
					},
				},
			},
		},
		{
			Spec: butlerv1alpha1.TeamSpec{
				Access: butlerv1alpha1.TeamAccess{
					Users: []butlerv1alpha1.TeamUser{
						{Name: "user@example.com", Role: "admin"},
					},
				},
			},
		},
		{
			Spec: butlerv1alpha1.TeamSpec{
				Access: butlerv1alpha1.TeamAccess{
					Users: []butlerv1alpha1.TeamUser{
						{Name: "user@example.com", Role: "operator"},
					},
				},
			},
		},
	}
	teams[0].Name = "zeta"
	teams[1].Name = "alpha"
	teams[2].Name = "middle"

	// Run resolution multiple times. Map iteration order is
	// non-deterministic, but the output must always be sorted.
	var first []butlerv1alpha1.UserTeamMembership
	for i := 0; i < 50; i++ {
		result := resolveTeamMemberships("user@example.com", nil, teams)
		if first == nil {
			first = result
		}
		if !teamMembershipsEqual(result, first) {
			t.Fatalf("iteration %d produced different output: %v vs %v", i, result, first)
		}
	}

	// Verify sorted order.
	if first[0].Name != "alpha" || first[1].Name != "middle" || first[2].Name != "zeta" {
		t.Errorf("expected [alpha, middle, zeta], got [%s, %s, %s]",
			first[0].Name, first[1].Name, first[2].Name)
	}
}

func TestTeamMembershipsEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b []butlerv1alpha1.UserTeamMembership
		want bool
	}{
		{
			name: "both nil",
			a:    nil,
			b:    nil,
			want: true,
		},
		{
			name: "both empty",
			a:    []butlerv1alpha1.UserTeamMembership{},
			b:    []butlerv1alpha1.UserTeamMembership{},
			want: true,
		},
		{
			name: "nil vs empty",
			a:    nil,
			b:    []butlerv1alpha1.UserTeamMembership{},
			want: true,
		},
		{
			name: "identical",
			a:    []butlerv1alpha1.UserTeamMembership{{Name: "alpha", Role: "admin"}},
			b:    []butlerv1alpha1.UserTeamMembership{{Name: "alpha", Role: "admin"}},
			want: true,
		},
		{
			name: "different role",
			a:    []butlerv1alpha1.UserTeamMembership{{Name: "alpha", Role: "admin"}},
			b:    []butlerv1alpha1.UserTeamMembership{{Name: "alpha", Role: "viewer"}},
			want: false,
		},
		{
			name: "different count",
			a:    []butlerv1alpha1.UserTeamMembership{{Name: "alpha", Role: "admin"}},
			b:    []butlerv1alpha1.UserTeamMembership{{Name: "alpha", Role: "admin"}, {Name: "beta", Role: "viewer"}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := teamMembershipsEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("teamMembershipsEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRoleLevel(t *testing.T) {
	if roleLevel("admin") <= roleLevel("operator") {
		t.Error("admin should outrank operator")
	}
	if roleLevel("operator") <= roleLevel("viewer") {
		t.Error("operator should outrank viewer")
	}
	if roleLevel("viewer") <= roleLevel("unknown") {
		t.Error("viewer should outrank unknown")
	}
	if roleLevel("unknown") != 0 {
		t.Errorf("unknown role should be 0, got %d", roleLevel("unknown"))
	}
}

func TestDefaultRole(t *testing.T) {
	// When a Team CRD has a user with no explicit role, the resolution
	// should default to viewer.
	teams := []butlerv1alpha1.Team{
		{
			Spec: butlerv1alpha1.TeamSpec{
				Access: butlerv1alpha1.TeamAccess{
					Users: []butlerv1alpha1.TeamUser{
						{Name: "user@example.com"}, // no Role set
					},
				},
			},
		},
	}
	teams[0].Name = "team-a"

	result := resolveTeamMemberships("user@example.com", nil, teams)
	if len(result) != 1 {
		t.Fatalf("expected 1 membership, got %d", len(result))
	}
	if result[0].Role != "viewer" {
		t.Errorf("expected default role viewer, got %s", result[0].Role)
	}
}
