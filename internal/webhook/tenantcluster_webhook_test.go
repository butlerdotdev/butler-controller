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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

func buildTC(name, namespace, envLabel, owner string, replicas int32) *butlerv1alpha1.TenantCluster {
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{},
		},
		Spec: butlerv1alpha1.TenantClusterSpec{
			TeamRef: &butlerv1alpha1.LocalObjectReference{Name: "acme"},
			Workers: butlerv1alpha1.WorkersSpec{
				Replicas: replicas,
				MachineTemplate: butlerv1alpha1.MachineTemplateSpec{CPU: 2},
			},
		},
	}
	if envLabel != "" {
		tc.Labels[butlerv1alpha1.LabelEnvironment] = envLabel
	}
	if owner != "" {
		tc.Labels[butlerv1alpha1.LabelOwner] = owner
	}
	return tc
}

func validateEnvOnly(t *testing.T, c client.Client, tc *butlerv1alpha1.TenantCluster) error {
	t.Helper()
	v := &TenantClusterValidator{Client: c}
	errs, err := v.validateEnvironment(context.Background(), tc)
	if err != nil {
		return err
	}
	if len(errs) > 0 {
		return errs.ToAggregate()
	}
	return nil
}

func TestTCWebhook_TeamWithoutEnvs_LabelAbsent_Allowed(t *testing.T) {
	s := teamScheme(t)
	team := newTeamWithEnvs("acme") // no envs
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	tc := buildTC("tc-1", "team-acme", "", "", 3)
	if err := validateEnvOnly(t, c, tc); err != nil {
		t.Fatalf("expected allowed, got error: %v", err)
	}
}

func TestTCWebhook_TeamWithEnvs_LabelAbsent_Denied(t *testing.T) {
	s := teamScheme(t)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{Name: "prod"})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	tc := buildTC("tc-1", "team-acme", "", "", 3)
	err := validateEnvOnly(t, c, tc)
	if err == nil || !strings.Contains(err.Error(), "defines environments") {
		t.Fatalf("expected denial about missing env label, got %v", err)
	}
}

func TestTCWebhook_TeamWithEnvs_LabelNotMatching_Denied(t *testing.T) {
	s := teamScheme(t)
	team := newTeamWithEnvs("acme",
		butlerv1alpha1.EnvironmentSpec{Name: "prod"},
		butlerv1alpha1.EnvironmentSpec{Name: "dev"},
	)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	tc := buildTC("tc-1", "team-acme", "staging", "", 3)
	err := validateEnvOnly(t, c, tc)
	if err == nil {
		t.Fatal("expected denial for non-matching env label")
	}
}

func TestTCWebhook_TeamWithEnvs_LabelMatches_NoLimits_Allowed(t *testing.T) {
	s := teamScheme(t)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{Name: "prod"})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	tc := buildTC("tc-1", "team-acme", "prod", "", 3)
	if err := validateEnvOnly(t, c, tc); err != nil {
		t.Fatalf("expected allowed, got error: %v", err)
	}
}

func TestTCWebhook_EnvCapExceeded_Denied(t *testing.T) {
	s := teamScheme(t)
	maxClusters := int32(2)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "prod",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			TeamResourceLimits: butlerv1alpha1.TeamResourceLimits{MaxClusters: &maxClusters},
		},
	})
	existing1 := buildTC("tc-existing-1", "team-acme", "prod", "u1", 3)
	existing2 := buildTC("tc-existing-2", "team-acme", "prod", "u2", 3)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team, existing1, existing2).Build()

	tc := buildTC("tc-new", "team-acme", "prod", "", 3)
	err := validateEnvOnly(t, c, tc)
	if err == nil || !strings.Contains(err.Error(), "env quota") {
		t.Fatalf("expected env cap denial, got %v", err)
	}
}

func TestTCWebhook_EnvCapHasRoom_OtherEnvsIgnored(t *testing.T) {
	s := teamScheme(t)
	// prod cap 2; prod has 1; dev has 5. The dev clusters do not count.
	maxClusters := int32(2)
	team := newTeamWithEnvs("acme",
		butlerv1alpha1.EnvironmentSpec{
			Name: "prod",
			Limits: &butlerv1alpha1.EnvironmentLimits{
				TeamResourceLimits: butlerv1alpha1.TeamResourceLimits{MaxClusters: &maxClusters},
			},
		},
		butlerv1alpha1.EnvironmentSpec{Name: "dev"},
	)
	prodExisting := buildTC("prod-1", "team-acme", "prod", "u1", 3)
	devExisting1 := buildTC("dev-1", "team-acme", "dev", "u1", 3)
	devExisting2 := buildTC("dev-2", "team-acme", "dev", "u1", 3)
	devExisting3 := buildTC("dev-3", "team-acme", "dev", "u1", 3)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team, prodExisting, devExisting1, devExisting2, devExisting3).Build()

	tc := buildTC("tc-new", "team-acme", "prod", "", 3)
	if err := validateEnvOnly(t, c, tc); err != nil {
		t.Fatalf("expected allowed, got error: %v", err)
	}
}

func TestTCWebhook_MaxClustersPerMember_MissingCreatorAnnotation_Denied(t *testing.T) {
	s := teamScheme(t)
	maxPerMember := int32(1)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "sandbox",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClustersPerMember: &maxPerMember,
		},
	})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	tc := buildTC("tc-new", "team-acme", "sandbox", "", 1)
	// No creator-email annotation set.
	err := validateEnvOnly(t, c, tc)
	if err == nil || !strings.Contains(err.Error(), "maxClustersPerMember") {
		t.Fatalf("expected missing-annotation denial, got %v", err)
	}
}

func TestTCWebhook_MaxClustersPerMember_AlreadyAtCap_Denied(t *testing.T) {
	s := teamScheme(t)
	maxPerMember := int32(1)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "sandbox",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClustersPerMember: &maxPerMember,
		},
	})
	existing := buildTC("sb-1", "team-acme", "sandbox", "alice@example.com", 1)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team, existing).Build()

	tc := buildTC("sb-2", "team-acme", "sandbox", "", 1)
	tc.Annotations = map[string]string{
		butlerv1alpha1.AnnotationCreatorEmail: "alice@example.com",
	}
	err := validateEnvOnly(t, c, tc)
	if err == nil || !strings.Contains(err.Error(), "per member") {
		t.Fatalf("expected per-member cap denial, got %v", err)
	}
}

func TestTCWebhook_MaxClustersPerMember_UnderCap_Allowed(t *testing.T) {
	s := teamScheme(t)
	maxPerMember := int32(2)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "sandbox",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClustersPerMember: &maxPerMember,
		},
	})
	// alice owns 1 already; the cap is 2, so a second cluster is allowed.
	existing := buildTC("sb-1", "team-acme", "sandbox", "alice@example.com", 1)
	// bob's clusters in the same env do not count against alice.
	bobsCluster := buildTC("sb-b1", "team-acme", "sandbox", "bob@example.com", 1)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team, existing, bobsCluster).Build()

	tc := buildTC("sb-2", "team-acme", "sandbox", "", 1)
	tc.Annotations = map[string]string{
		butlerv1alpha1.AnnotationCreatorEmail: "alice@example.com",
	}
	if err := validateEnvOnly(t, c, tc); err != nil {
		t.Fatalf("expected allowed, got error: %v", err)
	}
}
