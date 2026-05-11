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
	"sort"
	"strings"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// Role constants mirror butler-server's role hierarchy.
const (
	roleAdmin    = "admin"
	roleOperator = "operator"
	roleViewer   = "viewer"
)

// roleLevel returns the numeric privilege level for a role string.
// Higher values indicate higher privileges. Unknown roles return 0.
func roleLevel(role string) int {
	switch role {
	case roleAdmin:
		return 3
	case roleOperator:
		return 2
	case roleViewer:
		return 1
	default:
		return 0
	}
}

// resolveTeamMemberships computes a user's team memberships from all Team
// CRDs, combining manual membership (spec.access.users) and group-based
// membership (spec.access.groups matched against the user's lastSeenGroups).
//
// The output is deterministic: sorted by team name ascending. When a user
// matches both manual and group access on the same team, the highest role
// wins. This matches the semantics of butler-server's ResolveTeams().
func resolveTeamMemberships(
	email string,
	lastSeenGroups []string,
	teams []butlerv1alpha1.Team,
) []butlerv1alpha1.UserTeamMembership {
	memberships := make(map[string]string) // team name -> highest role

	groupSet := buildGroupLookupSet(lastSeenGroups)

	for i := range teams {
		team := &teams[i]

		// Check manual membership (spec.access.users).
		for _, u := range team.Spec.Access.Users {
			if !strings.EqualFold(u.Name, email) {
				continue
			}
			role := string(u.Role)
			if role == "" {
				role = roleViewer
			}
			addMembership(memberships, team.Name, role)
		}

		// Check group-based membership (spec.access.groups).
		if len(groupSet) > 0 {
			for _, g := range team.Spec.Access.Groups {
				if !matchGroup(g.Name, groupSet) {
					continue
				}
				role := string(g.Role)
				if role == "" {
					role = roleViewer
				}
				addMembership(memberships, team.Name, role)
			}
		}
	}

	// Convert to sorted slice for deterministic output.
	result := make([]butlerv1alpha1.UserTeamMembership, 0, len(memberships))
	for name, role := range memberships {
		result = append(result, butlerv1alpha1.UserTeamMembership{
			Name: name,
			Role: role,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// addMembership adds or upgrades a team membership. The highest role wins
// when a user matches through multiple paths (manual + group, or multiple
// groups with different roles on the same team).
func addMembership(memberships map[string]string, teamName, role string) {
	existing, ok := memberships[teamName]
	if !ok || roleLevel(role) > roleLevel(existing) {
		memberships[teamName] = role
	}
}

// normalizeGroupName extracts the base group name without domain suffix.
// Mirrors butler-server's normalizeGroupName for matching consistency.
//
// Examples:
//   - "platform-engineering@butlerlabs.dev" -> "platform-engineering"
//   - "platform-engineering" -> "platform-engineering"
//   - "CN=DevOps,OU=Groups,DC=corp,DC=example,DC=com" -> "devops"
//   - "7f2ed032-9e36-433a-acef-ccc19524b2a5" -> "7f2ed032-9e36-433a-acef-ccc19524b2a5"
func normalizeGroupName(group string) string {
	group = strings.TrimSpace(group)

	// Handle LDAP DN format (CN=GroupName,OU=...).
	if strings.HasPrefix(strings.ToUpper(group), "CN=") {
		parts := strings.Split(group, ",")
		if len(parts) > 0 {
			cnPart := parts[0]
			if idx := strings.Index(cnPart, "="); idx != -1 {
				group = cnPart[idx+1:]
			}
		}
	}

	// Handle email-style groups (group@domain.com).
	if idx := strings.LastIndex(group, "@"); idx != -1 {
		group = group[:idx]
	}

	return strings.ToLower(group)
}

// buildGroupLookupSet creates an O(1) lookup set from a user's IdP groups.
// Stores both the original (lowercased) and normalized (domain-stripped)
// forms for flexible matching. Mirrors butler-server's buildGroupLookupSet.
func buildGroupLookupSet(idpGroups []string) map[string]bool {
	if len(idpGroups) == 0 {
		return nil
	}
	groupSet := make(map[string]bool, len(idpGroups)*2)
	for _, g := range idpGroups {
		lower := strings.ToLower(g)
		groupSet[lower] = true

		normalized := normalizeGroupName(g)
		if normalized != lower {
			groupSet[normalized] = true
		}
	}
	return groupSet
}

// matchGroup checks if a configured group name matches any entry in the
// user's group lookup set. Uses both normalized and exact (lowercased)
// comparisons. Mirrors butler-server's groupMatches/matchGroup.
func matchGroup(configuredGroup string, groupSet map[string]bool) bool {
	if groupSet[normalizeGroupName(configuredGroup)] {
		return true
	}
	if groupSet[strings.ToLower(configuredGroup)] {
		return true
	}
	return false
}

// teamMembershipsEqual returns true if two sorted membership slices are
// identical. Both slices must be sorted by team name (as produced by
// resolveTeamMemberships).
func teamMembershipsEqual(a, b []butlerv1alpha1.UserTeamMembership) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Role != b[i].Role {
			return false
		}
	}
	return true
}
