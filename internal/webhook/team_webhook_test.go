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

// SAR fallback path coverage limitation: the isPlatformAdmin helper
// falls back to a SubjectAccessReview when no User CRD matches the
// caller. The SAR path is exercised only on live clusters where the
// controller's ServiceAccount carries create on
// subjectaccessreviews.authorization.k8s.io (granted by the
// butler-controller chart as of
// butler-charts/feat/butler-controller-sar-permission). The fake
// client in controller-runtime cannot emulate the apiserver's
// synchronous SAR handling: SARs are write-only resources that the
// apiserver answers in-line without persistence, while the fake
// client tries to store them and rejects empty metadata.name. Tests
// in this file therefore cover the User CRD primary path only. The
// SAR fallback must be validated via envtest or a real cluster.
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	authnv1 "k8s.io/api/authentication/v1"
	authzv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

func teamScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := butlerv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add butler scheme: %v", err)
	}
	if err := authzv1.AddToScheme(s); err != nil {
		t.Fatalf("add authorization scheme: %v", err)
	}
	return s
}

func newTeamWithEnvs(name string, envs ...butlerv1alpha1.EnvironmentSpec) *butlerv1alpha1.Team {
	return &butlerv1alpha1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: butlerv1alpha1.TeamSpec{
			Access: butlerv1alpha1.TeamAccess{
				Users: []butlerv1alpha1.TeamUser{
					{Name: "team-admin@example.com", Role: butlerv1alpha1.TeamRoleAdmin},
					{Name: "team-operator@example.com", Role: butlerv1alpha1.TeamRoleOperator},
				},
			},
			Environments: envs,
		},
	}
}

func marshalTeam(t *testing.T, team *butlerv1alpha1.Team) []byte {
	t.Helper()
	b, err := json.Marshal(team)
	if err != nil {
		t.Fatalf("marshal team: %v", err)
	}
	return b
}

// clientWithUsers returns a fake client seeded with User CRDs so the
// isPlatformAdmin primary path (User CRD lookup) can resolve. The SAR
// fallback path runs against the fake client and will return Allowed=false
// by default (the fake client stores the SAR with its zero-valued Status),
// which is the correct outcome for any caller who is not already a
// platform admin via the User CRD path.
func clientWithUsers(s *runtime.Scheme, users []butlerv1alpha1.User, initial ...client.Object) client.Client {
	objs := make([]client.Object, 0, len(initial)+len(users))
	for i := range users {
		u := users[i]
		objs = append(objs, &u)
	}
	objs = append(objs, initial...)
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

// newAdmissionRequest constructs an admission.Request for a Team operation.
// oldTeam is ignored on create.
func newAdmissionRequest(t *testing.T, op admissionv1.Operation, username string, groups []string, newTeam, oldTeam *butlerv1alpha1.Team) admission.Request {
	t.Helper()
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: op,
			UserInfo: authnv1.UserInfo{
				Username: username,
				Groups:   groups,
			},
			Object:    runtime.RawExtension{Raw: marshalTeam(t, newTeam)},
			OldObject: runtime.RawExtension{},
		},
	}
	if oldTeam != nil {
		req.OldObject = runtime.RawExtension{Raw: marshalTeam(t, oldTeam)}
	}
	return req
}

func assertAllowed(t *testing.T, resp admission.Response) {
	t.Helper()
	if !resp.Allowed {
		t.Fatalf("expected allowed, got denied: %v", resp.Result)
	}
}

func assertDenied(t *testing.T, resp admission.Response, wantSubstr string) {
	t.Helper()
	if resp.Allowed {
		t.Fatalf("expected denied, got allowed")
	}
	if wantSubstr == "" {
		return
	}
	msg := ""
	if resp.Result != nil {
		msg = resp.Result.Message
	}
	if !strings.Contains(msg, wantSubstr) {
		t.Fatalf("expected denial message containing %q, got %q", wantSubstr, msg)
	}
}

func platformAdminUser() butlerv1alpha1.User {
	return butlerv1alpha1.User{
		ObjectMeta: metav1.ObjectMeta{Name: "platform-admin"},
		Spec: butlerv1alpha1.UserSpec{
			Email:           "platform-admin@example.com",
			IsPlatformAdmin: true,
		},
	}
}

func regularUser(email string) butlerv1alpha1.User {
	return butlerv1alpha1.User{
		ObjectMeta: metav1.ObjectMeta{Name: strings.Split(email, "@")[0]},
		Spec: butlerv1alpha1.UserSpec{
			Email:           email,
			IsPlatformAdmin: false,
		},
	}
}

// --- CREATE TESTS ---

func TestTeamWebhook_CreateWithoutLimits_Allowed(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{regularUser("team-admin@example.com")})
	v := &TeamValidator{Client: c, APIReader: c}

	team := &butlerv1alpha1.Team{ObjectMeta: metav1.ObjectMeta{Name: "acme"}}
	req := newAdmissionRequest(t, admissionv1.Create, "team-admin@example.com", nil, team, nil)

	assertAllowed(t, v.Handle(context.Background(), req))
}

func TestTeamWebhook_CreateWithResourceLimits_PlatformAdmin_Allowed(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{platformAdminUser()})
	v := &TeamValidator{Client: c, APIReader: c}

	maxClusters := int32(20)
	team := &butlerv1alpha1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "acme"},
		Spec: butlerv1alpha1.TeamSpec{
			ResourceLimits: &butlerv1alpha1.TeamResourceLimits{
				MaxClusters: &maxClusters,
			},
		},
	}
	req := newAdmissionRequest(t, admissionv1.Create, "platform-admin@example.com", nil, team, nil)

	assertAllowed(t, v.Handle(context.Background(), req))
}

func TestTeamWebhook_CreateWithResourceLimits_NonPlatformAdmin_Denied(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{regularUser("bob@example.com")})
	v := &TeamValidator{Client: c, APIReader: c}

	maxClusters := int32(20)
	team := &butlerv1alpha1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "acme"},
		Spec: butlerv1alpha1.TeamSpec{
			ResourceLimits: &butlerv1alpha1.TeamResourceLimits{
				MaxClusters: &maxClusters,
			},
		},
	}
	req := newAdmissionRequest(t, admissionv1.Create, "bob@example.com", nil, team, nil)

	assertDenied(t, v.Handle(context.Background(), req), "platform admin")
}

func TestTeamWebhook_CreateWithEnvLimits_NonPlatformAdmin_Denied(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{regularUser("bob@example.com")})
	v := &TeamValidator{Client: c, APIReader: c}

	maxClusters := int32(5)
	team := &butlerv1alpha1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "acme"},
		Spec: butlerv1alpha1.TeamSpec{
			Environments: []butlerv1alpha1.EnvironmentSpec{
				{
					Name: "prod",
					Limits: &butlerv1alpha1.EnvironmentLimits{
						MaxClusters: &maxClusters,
					},
				},
			},
		},
	}
	req := newAdmissionRequest(t, admissionv1.Create, "bob@example.com", nil, team, nil)

	assertDenied(t, v.Handle(context.Background(), req), "platform admin")
}

// --- UPDATE TESTS (the plan's four authorization scenarios) ---

// Scenario 5: team admin modifying team.spec.resourceLimits -> denied.
func TestTeamWebhook_UpdateResourceLimits_TeamAdmin_Denied(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{regularUser("team-admin@example.com")})
	v := &TeamValidator{Client: c, APIReader: c}

	oldMax := int32(10)
	newMax := int32(20)
	oldTeam := newTeamWithEnvs("acme")
	oldTeam.Spec.ResourceLimits = &butlerv1alpha1.TeamResourceLimits{MaxClusters: &oldMax}
	newTeam := oldTeam.DeepCopy()
	newTeam.Spec.ResourceLimits = &butlerv1alpha1.TeamResourceLimits{MaxClusters: &newMax}

	req := newAdmissionRequest(t, admissionv1.Update, "team-admin@example.com", nil, newTeam, oldTeam)

	assertDenied(t, v.Handle(context.Background(), req), "platform admin")
}

// Scenario 6: team admin modifying team.spec.environments[].limits -> allowed.
func TestTeamWebhook_UpdateEnvLimits_TeamAdmin_Allowed(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{regularUser("team-admin@example.com")})
	v := &TeamValidator{Client: c, APIReader: c}

	oldMax := int32(10)
	newMax := int32(15)
	oldTeam := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "prod",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClusters: &oldMax,
		},
	})
	newTeam := oldTeam.DeepCopy()
	newTeam.Spec.Environments[0].Limits.MaxClusters = &newMax

	req := newAdmissionRequest(t, admissionv1.Update, "team-admin@example.com", nil, newTeam, oldTeam)

	assertAllowed(t, v.Handle(context.Background(), req))
}

// Scenario 7: team operator modifying team.spec.environments[].limits -> denied.
func TestTeamWebhook_UpdateEnvLimits_TeamOperator_Denied(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{regularUser("team-operator@example.com")})
	v := &TeamValidator{Client: c, APIReader: c}

	oldMax := int32(10)
	newMax := int32(15)
	oldTeam := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "prod",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClusters: &oldMax,
		},
	})
	newTeam := oldTeam.DeepCopy()
	newTeam.Spec.Environments[0].Limits.MaxClustersPerMember = &newMax

	req := newAdmissionRequest(t, admissionv1.Update, "team-operator@example.com", nil, newTeam, oldTeam)

	assertDenied(t, v.Handle(context.Background(), req), "team admin")
}

// Scenario 8: platform admin modifying both fields -> allowed in one request.
func TestTeamWebhook_UpdateBothAsPlatformAdmin_Allowed(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{platformAdminUser()})
	v := &TeamValidator{Client: c, APIReader: c}

	oldTeamMax := int32(10)
	newTeamMax := int32(25)
	oldEnvMax := int32(5)
	newEnvMax := int32(12)

	oldTeam := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "prod",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClusters: &oldEnvMax,
		},
	})
	oldTeam.Spec.ResourceLimits = &butlerv1alpha1.TeamResourceLimits{MaxClusters: &oldTeamMax}

	newTeam := oldTeam.DeepCopy()
	newTeam.Spec.ResourceLimits.MaxClusters = &newTeamMax
	newTeam.Spec.Environments[0].Limits.MaxClusters = &newEnvMax

	req := newAdmissionRequest(t, admissionv1.Update, "platform-admin@example.com", nil, newTeam, oldTeam)

	assertAllowed(t, v.Handle(context.Background(), req))
}

// --- other coverage ---

func TestTeamWebhook_UpdateNoLimitsChange_Allowed(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{regularUser("team-operator@example.com")})
	v := &TeamValidator{Client: c, APIReader: c}

	oldTeam := newTeamWithEnvs("acme")
	oldTeam.Spec.DisplayName = "Acme Corp"
	newTeam := oldTeam.DeepCopy()
	newTeam.Spec.Description = "new description"

	req := newAdmissionRequest(t, admissionv1.Update, "team-operator@example.com", nil, newTeam, oldTeam)

	assertAllowed(t, v.Handle(context.Background(), req))
}

// environmentLimitsChanged unit-test covers addition, removal, and mutation.
func TestEnvironmentLimitsChanged(t *testing.T) {
	m5 := int32(5)
	m10 := int32(10)

	tests := []struct {
		name string
		old  []butlerv1alpha1.EnvironmentSpec
		new  []butlerv1alpha1.EnvironmentSpec
		want bool
	}{
		{
			name: "no envs either side",
			old:  nil, new: nil, want: false,
		},
		{
			name: "add env without limits",
			old:  nil,
			new:  []butlerv1alpha1.EnvironmentSpec{{Name: "prod"}},
			want: false,
		},
		{
			name: "add env with limits",
			old:  nil,
			new: []butlerv1alpha1.EnvironmentSpec{{Name: "prod", Limits: &butlerv1alpha1.EnvironmentLimits{
				MaxClusters: &m5,
			}}},
			want: true,
		},
		{
			name: "remove env with limits",
			old: []butlerv1alpha1.EnvironmentSpec{{Name: "prod", Limits: &butlerv1alpha1.EnvironmentLimits{
				MaxClusters: &m5,
			}}},
			new:  nil,
			want: true,
		},
		{
			name: "limit value changed",
			old: []butlerv1alpha1.EnvironmentSpec{{Name: "prod", Limits: &butlerv1alpha1.EnvironmentLimits{
				MaxClusters: &m5,
			}}},
			new: []butlerv1alpha1.EnvironmentSpec{{Name: "prod", Limits: &butlerv1alpha1.EnvironmentLimits{
				MaxClusters: &m10,
			}}},
			want: true,
		},
		{
			name: "access changed, limits unchanged",
			old: []butlerv1alpha1.EnvironmentSpec{{Name: "prod", Limits: &butlerv1alpha1.EnvironmentLimits{
				MaxClusters: &m5,
			}}},
			new: []butlerv1alpha1.EnvironmentSpec{{Name: "prod",
				Access: &butlerv1alpha1.TeamAccess{Users: []butlerv1alpha1.TeamUser{{Name: "u@x", Role: butlerv1alpha1.TeamRoleAdmin}}},
				Limits: &butlerv1alpha1.EnvironmentLimits{
					MaxClusters: &m5,
				},
			}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := environmentLimitsChanged(tt.old, tt.new)
			if got != tt.want {
				t.Fatalf("environmentLimitsChanged = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- isPlatformAdmin error surfacing (finding #8) ---

func TestTeamWebhook_UserListError_Surfaced(t *testing.T) {
	s := teamScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, client client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*butlerv1alpha1.UserList); ok {
					return fmt.Errorf("simulated apiserver flake")
				}
				return client.List(ctx, list, opts...)
			},
		}).
		Build()
	v := &TeamValidator{Client: c, APIReader: c}

	maxClusters := int32(20)
	team := &butlerv1alpha1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "acme"},
		Spec: butlerv1alpha1.TeamSpec{
			ResourceLimits: &butlerv1alpha1.TeamResourceLimits{MaxClusters: &maxClusters},
		},
	}
	req := newAdmissionRequest(t, admissionv1.Create, "any@example.com", nil, team, nil)

	resp := v.Handle(context.Background(), req)
	if resp.Allowed {
		t.Fatal("expected error/denial, got allowed")
	}
	// admission.Errored sets Result.Code != 0 (HTTP 500 here) and a
	// message. A transient List error must not silently fall through
	// to the SAR path; the whole point of the fix is to fail loud.
	if resp.Result == nil || resp.Result.Code == 0 {
		t.Fatalf("expected errored response, got %+v", resp)
	}
	if !strings.Contains(resp.Result.Message, "simulated apiserver flake") {
		t.Errorf("expected underlying error surfaced, got %q", resp.Result.Message)
	}
}

// --- Env access membership enforcement (finding #6) ---

func TestTeamWebhook_EnvAccessUser_MatchesTeam_Allowed(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{platformAdminUser()})
	v := &TeamValidator{Client: c, APIReader: c}

	team := &butlerv1alpha1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "acme"},
		Spec: butlerv1alpha1.TeamSpec{
			Access: butlerv1alpha1.TeamAccess{
				Users: []butlerv1alpha1.TeamUser{
					{Name: "alice@example.com", Role: butlerv1alpha1.TeamRoleOperator},
				},
			},
			Environments: []butlerv1alpha1.EnvironmentSpec{
				{
					Name: "prod",
					Access: &butlerv1alpha1.TeamAccess{
						Users: []butlerv1alpha1.TeamUser{
							{Name: "alice@example.com", Role: butlerv1alpha1.TeamRoleAdmin},
						},
					},
				},
			},
		},
	}
	req := newAdmissionRequest(t, admissionv1.Create, "platform-admin@example.com", nil, team, nil)
	assertAllowed(t, v.Handle(context.Background(), req))
}

func TestTeamWebhook_EnvAccessUser_NotInTeam_Denied(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{platformAdminUser()})
	v := &TeamValidator{Client: c, APIReader: c}

	team := &butlerv1alpha1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "acme"},
		Spec: butlerv1alpha1.TeamSpec{
			Access: butlerv1alpha1.TeamAccess{
				Users: []butlerv1alpha1.TeamUser{
					{Name: "alice@example.com", Role: butlerv1alpha1.TeamRoleOperator},
				},
			},
			Environments: []butlerv1alpha1.EnvironmentSpec{
				{
					Name: "prod",
					Access: &butlerv1alpha1.TeamAccess{
						Users: []butlerv1alpha1.TeamUser{
							{Name: "outsider@example.com", Role: butlerv1alpha1.TeamRoleAdmin},
						},
					},
				},
			},
		},
	}
	req := newAdmissionRequest(t, admissionv1.Create, "platform-admin@example.com", nil, team, nil)
	assertDenied(t, v.Handle(context.Background(), req), "is not a team member")
}

func TestTeamWebhook_EnvAccessGroup_NotInTeam_Denied(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{platformAdminUser()})
	v := &TeamValidator{Client: c, APIReader: c}

	team := &butlerv1alpha1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "acme"},
		Spec: butlerv1alpha1.TeamSpec{
			Access: butlerv1alpha1.TeamAccess{
				Groups: []butlerv1alpha1.TeamGroup{
					{Name: "team-devs", Role: butlerv1alpha1.TeamRoleOperator},
				},
			},
			Environments: []butlerv1alpha1.EnvironmentSpec{
				{
					Name: "prod",
					Access: &butlerv1alpha1.TeamAccess{
						Groups: []butlerv1alpha1.TeamGroup{
							{Name: "outsider-group", Role: butlerv1alpha1.TeamRoleAdmin},
						},
					},
				},
			},
		},
	}
	req := newAdmissionRequest(t, admissionv1.Create, "platform-admin@example.com", nil, team, nil)
	assertDenied(t, v.Handle(context.Background(), req), "is not a team group")
}

func TestTeamWebhook_EnvAccessUser_CaseInsensitive_Allowed(t *testing.T) {
	s := teamScheme(t)
	c := clientWithUsers(s, []butlerv1alpha1.User{platformAdminUser()})
	v := &TeamValidator{Client: c, APIReader: c}

	team := &butlerv1alpha1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: "acme"},
		Spec: butlerv1alpha1.TeamSpec{
			Access: butlerv1alpha1.TeamAccess{
				Users: []butlerv1alpha1.TeamUser{
					{Name: "Alice@Example.COM", Role: butlerv1alpha1.TeamRoleOperator},
				},
			},
			Environments: []butlerv1alpha1.EnvironmentSpec{
				{
					Name: "prod",
					Access: &butlerv1alpha1.TeamAccess{
						Users: []butlerv1alpha1.TeamUser{
							{Name: "alice@example.com", Role: butlerv1alpha1.TeamRoleAdmin},
						},
					},
				},
			},
		},
	}
	req := newAdmissionRequest(t, admissionv1.Create, "platform-admin@example.com", nil, team, nil)
	assertAllowed(t, v.Handle(context.Background(), req))
}
