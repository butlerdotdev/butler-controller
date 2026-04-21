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

package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	admissionv1 "k8s.io/api/admission/v1"
	authzv1 "k8s.io/api/authorization/v1"
	authnv1 "k8s.io/api/authentication/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// PlatformAdminClusterRole is the ClusterRole bound to platform admins for
// kubectl access. Mirrors butler-server/internal/auth/serviceaccount.go's
// CLIClusterRolePlatformAdmin constant.
const PlatformAdminClusterRole = "butler-cli-platform-admin"

// TeamValidator validates Team mutations and enforces the platform-admin /
// team-admin split on ResourceLimits and Environments[].Limits fields.
//
// spec.resourceLimits is the team's absolute ceiling and may only be set or
// modified by a platform admin. Team admins cannot raise their own ceiling.
//
// spec.environments[].limits are per-environment sub-caps within that ceiling
// and may be modified by a team admin of the team being edited (platform
// admins can modify them as well).
//
// The check runs on both create and update. On create, any resourceLimits or
// env-limits present on the incoming Team require platform admin, because no
// team admin can exist for a team that does not yet exist.
type TeamValidator struct {
	Client client.Client
}

// +kubebuilder:webhook:path=/validate-butler-butlerlabs-dev-v1alpha1-team,mutating=false,failurePolicy=fail,sideEffects=None,groups=butler.butlerlabs.dev,resources=teams,verbs=create;update,versions=v1alpha1,name=vteam.kb.io,admissionReviewVersions=v1

// Handle implements admission.Handler. Dispatches to handleCreate or
// handleUpdate based on the operation.
func (v *TeamValidator) Handle(ctx context.Context, req admission.Request) admission.Response {
	switch req.Operation {
	case admissionv1.Create:
		return v.handleCreate(ctx, req)
	case admissionv1.Update:
		return v.handleUpdate(ctx, req)
	default:
		return admission.Allowed("")
	}
}

func (v *TeamValidator) handleCreate(ctx context.Context, req admission.Request) admission.Response {
	team := &butlerv1alpha1.Team{}
	if err := json.Unmarshal(req.Object.Raw, team); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode Team: %w", err))
	}

	// ResourceLimits on a new team: platform admin only.
	if team.Spec.ResourceLimits != nil {
		ok, err := v.isPlatformAdmin(ctx, req.UserInfo)
		if err != nil {
			return admission.Errored(http.StatusInternalServerError, fmt.Errorf("resolve platform admin: %w", err))
		}
		if !ok {
			return admission.Denied(fmt.Sprintf(
				"spec.resourceLimits may only be set by platform admins; user %q is not a platform admin",
				req.UserInfo.Username,
			))
		}
	}

	// Any environment with limits on a brand-new team requires platform admin,
	// since there is no team admin yet for a team that does not exist.
	for i, env := range team.Spec.Environments {
		if env.Limits != nil {
			ok, err := v.isPlatformAdmin(ctx, req.UserInfo)
			if err != nil {
				return admission.Errored(http.StatusInternalServerError, fmt.Errorf("resolve platform admin: %w", err))
			}
			if !ok {
				return admission.Denied(fmt.Sprintf(
					"spec.environments[%d].limits on a new team may only be set by platform admins; user %q is not a platform admin",
					i, req.UserInfo.Username,
				))
			}
			break
		}
	}

	return admission.Allowed("")
}

func (v *TeamValidator) handleUpdate(ctx context.Context, req admission.Request) admission.Response {
	oldTeam := &butlerv1alpha1.Team{}
	if err := json.Unmarshal(req.OldObject.Raw, oldTeam); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode old Team: %w", err))
	}
	newTeam := &butlerv1alpha1.Team{}
	if err := json.Unmarshal(req.Object.Raw, newTeam); err != nil {
		return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode new Team: %w", err))
	}

	// ResourceLimits diff: platform admin only.
	if !reflect.DeepEqual(oldTeam.Spec.ResourceLimits, newTeam.Spec.ResourceLimits) {
		ok, err := v.isPlatformAdmin(ctx, req.UserInfo)
		if err != nil {
			return admission.Errored(http.StatusInternalServerError, fmt.Errorf("resolve platform admin: %w", err))
		}
		if !ok {
			return admission.Denied(fmt.Sprintf(
				"spec.resourceLimits may only be modified by platform admins; user %q is not a platform admin",
				req.UserInfo.Username,
			))
		}
	}

	// Environments[].Limits diff: team admin on this team, or platform admin.
	if environmentLimitsChanged(oldTeam.Spec.Environments, newTeam.Spec.Environments) {
		ok, err := v.isTeamAdminOrPlatformAdmin(ctx, req.UserInfo, newTeam)
		if err != nil {
			return admission.Errored(http.StatusInternalServerError, fmt.Errorf("resolve team admin: %w", err))
		}
		if !ok {
			return admission.Denied(fmt.Sprintf(
				"spec.environments[].limits may only be modified by team admins of %q or platform admins; user %q is neither",
				newTeam.Name, req.UserInfo.Username,
			))
		}
	}

	return admission.Allowed("")
}

// environmentLimitsChanged reports whether any environment's Limits block
// differs between old and new. Addition, removal, and mutation all count.
func environmentLimitsChanged(old, new []butlerv1alpha1.EnvironmentSpec) bool {
	oldByName := make(map[string]*butlerv1alpha1.EnvironmentLimits, len(old))
	for i := range old {
		oldByName[old[i].Name] = old[i].Limits
	}
	newByName := make(map[string]*butlerv1alpha1.EnvironmentLimits, len(new))
	for i := range new {
		newByName[new[i].Name] = new[i].Limits
	}
	names := make(map[string]struct{}, len(oldByName)+len(newByName))
	for n := range oldByName {
		names[n] = struct{}{}
	}
	for n := range newByName {
		names[n] = struct{}{}
	}
	for n := range names {
		if !reflect.DeepEqual(oldByName[n], newByName[n]) {
			return true
		}
	}
	return false
}

// isPlatformAdmin returns true when the user is a platform admin. Primary
// path: list User CRDs and match on Spec.Email == user.Username; the
// matching User's Spec.IsPlatformAdmin is the authoritative answer. The
// SAR fallback fires only when no User CRD matches the caller's username,
// which is the case for kubectl-direct operators holding the admin
// kubeconfig but not mapped to a User CRD.
func (v *TeamValidator) isPlatformAdmin(ctx context.Context, user authnv1.UserInfo) (bool, error) {
	userList := &butlerv1alpha1.UserList{}
	if err := v.Client.List(ctx, userList); err == nil {
		for i := range userList.Items {
			u := &userList.Items[i]
			if u.Spec.Email == user.Username {
				return u.Spec.IsPlatformAdmin, nil
			}
		}
	}
	// No matching User CRD; fall back to the K8s authz model via SAR.
	// The butler-cli-platform-admin ClusterRole grants enumerated verbs
	// (get,list,watch,create,update,patch,delete) on teams, not "*", so
	// the SAR must check a specific representative verb. "update" reflects
	// the mutation the webhook is actually gating and matches the verb set
	// of every ClusterRole that legitimately represents platform admin.
	sar := &authzv1.SubjectAccessReview{
		Spec: authzv1.SubjectAccessReviewSpec{
			User:   user.Username,
			Groups: user.Groups,
			ResourceAttributes: &authzv1.ResourceAttributes{
				Verb:     "update",
				Group:    "butler.butlerlabs.dev",
				Resource: "teams",
			},
		},
	}
	if err := v.Client.Create(ctx, sar); err != nil {
		return false, fmt.Errorf("submit SubjectAccessReview: %w", err)
	}
	return sar.Status.Allowed, nil
}

// isTeamAdminOrPlatformAdmin returns true when the user is a platform admin
// or when the user is listed as an admin (directly or via group membership)
// on the given team's Spec.Access.
func (v *TeamValidator) isTeamAdminOrPlatformAdmin(ctx context.Context, user authnv1.UserInfo, team *butlerv1alpha1.Team) (bool, error) {
	platformAdmin, err := v.isPlatformAdmin(ctx, user)
	if err != nil {
		return false, err
	}
	if platformAdmin {
		return true, nil
	}
	for _, tu := range team.Spec.Access.Users {
		if tu.Name == user.Username && tu.Role == butlerv1alpha1.TeamRoleAdmin {
			return true, nil
		}
	}
	userGroups := make(map[string]struct{}, len(user.Groups))
	for _, g := range user.Groups {
		userGroups[g] = struct{}{}
	}
	for _, tg := range team.Spec.Access.Groups {
		if tg.Role != butlerv1alpha1.TeamRoleAdmin {
			continue
		}
		if _, ok := userGroups[tg.Name]; ok {
			return true, nil
		}
	}
	return false, nil
}

// SetupWebhookWithManager registers the Team validating webhook with the
// manager. Uses the raw admission.Handler path (not CustomValidator) so the
// handler can read UserInfo from the admission request.
func (v *TeamValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	v.Client = mgr.GetClient()
	mgr.GetWebhookServer().Register(
		"/validate-butler-butlerlabs-dev-v1alpha1-team",
		&admission.Webhook{Handler: v},
	)
	return nil
}
