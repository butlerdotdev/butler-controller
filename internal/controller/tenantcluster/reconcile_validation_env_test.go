// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

func envValidationScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := butlerv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to add butler scheme: %v", err)
	}
	return s
}

func buildEnvTC(name, ns, env, owner string, replicas int32) *butlerv1alpha1.TenantCluster {
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{},
		},
		Spec: butlerv1alpha1.TenantClusterSpec{
			TeamRef: &butlerv1alpha1.LocalObjectReference{Name: "acme"},
			Workers: butlerv1alpha1.WorkersSpec{Replicas: replicas},
		},
	}
	if env != "" {
		tc.Labels[butlerv1alpha1.LabelEnvironment] = env
	}
	if owner != "" {
		tc.Labels[butlerv1alpha1.LabelOwner] = owner
	}
	return tc
}

func teamWithEnv(name, envName string, limits *butlerv1alpha1.EnvironmentLimits) *butlerv1alpha1.Team {
	return &butlerv1alpha1.Team{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: butlerv1alpha1.TeamSpec{
			Environments: []butlerv1alpha1.EnvironmentSpec{
				{Name: envName, Limits: limits},
			},
		},
	}
}

func TestValidateEnvironmentQuotas_NoEnvs_NoOp(t *testing.T) {
	s := envValidationScheme(t)
	team := &butlerv1alpha1.Team{ObjectMeta: metav1.ObjectMeta{Name: "acme"}}
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := &Reconciler{Client: c, Scheme: s}

	tc := buildEnvTC("new", "team-acme", "", "", 1)
	if err := r.validateEnvironmentQuotas(context.Background(), tc, team, nil); err != nil {
		t.Fatalf("expected no-op, got %v", err)
	}
}

func TestValidateEnvironmentQuotas_EnvClusterCapExceeded(t *testing.T) {
	s := envValidationScheme(t)
	team := teamWithEnv("acme", "prod", &butlerv1alpha1.EnvironmentLimits{
		TeamResourceLimits: butlerv1alpha1.TeamResourceLimits{MaxClusters: int32Ptr(2)},
	})
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := &Reconciler{Client: c, Scheme: s}

	siblings := []butlerv1alpha1.TenantCluster{
		*buildEnvTC("a", "team-acme", "prod", "", 1),
		*buildEnvTC("b", "team-acme", "prod", "", 1),
	}
	tc := buildEnvTC("new", "team-acme", "prod", "", 1)

	err := r.validateEnvironmentQuotas(context.Background(), tc, team, siblings)
	if err == nil || !strings.Contains(err.Error(), "env quota exceeded") {
		t.Fatalf("expected env quota denial, got %v", err)
	}
	reason := conditionReason(tc, butlerv1alpha1.TenantClusterConditionQuotaSatisfied)
	if reason != butlerv1alpha1.ReasonEnvQuotaExceeded {
		t.Errorf("expected condition reason %s, got %s", butlerv1alpha1.ReasonEnvQuotaExceeded, reason)
	}
}

func TestValidateEnvironmentQuotas_DifferentEnvNotCounted(t *testing.T) {
	s := envValidationScheme(t)
	team := teamWithEnv("acme", "prod", &butlerv1alpha1.EnvironmentLimits{
		TeamResourceLimits: butlerv1alpha1.TeamResourceLimits{MaxClusters: int32Ptr(2)},
	})
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := &Reconciler{Client: c, Scheme: s}

	siblings := []butlerv1alpha1.TenantCluster{
		*buildEnvTC("a", "team-acme", "prod", "", 1),
		*buildEnvTC("b", "team-acme", "dev", "", 1),
	}
	tc := buildEnvTC("new", "team-acme", "prod", "", 1)

	if err := r.validateEnvironmentQuotas(context.Background(), tc, team, siblings); err != nil {
		t.Fatalf("expected allow (only 1 prod sibling), got %v", err)
	}
}

func TestValidateEnvironmentQuotas_PerMemberCapExceeded(t *testing.T) {
	s := envValidationScheme(t)
	team := teamWithEnv("acme", "dev", &butlerv1alpha1.EnvironmentLimits{
		MaxClustersPerMember: int32Ptr(1),
	})
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := &Reconciler{Client: c, Scheme: s}

	siblings := []butlerv1alpha1.TenantCluster{
		*buildEnvTC("a", "team-acme", "dev", "alice@example.com", 1),
	}
	tc := buildEnvTC("new", "team-acme", "dev", "alice@example.com", 1)

	err := r.validateEnvironmentQuotas(context.Background(), tc, team, siblings)
	if err == nil || !strings.Contains(err.Error(), "per-member cap exceeded") {
		t.Fatalf("expected per-member cap denial, got %v", err)
	}
	reason := conditionReason(tc, butlerv1alpha1.TenantClusterConditionQuotaSatisfied)
	if reason != butlerv1alpha1.ReasonPerMemberCapExceeded {
		t.Errorf("expected condition reason %s, got %s", butlerv1alpha1.ReasonPerMemberCapExceeded, reason)
	}
}

func TestValidateEnvironmentQuotas_PerMemberCapMissingOwner_SkippedNotDenied(t *testing.T) {
	s := envValidationScheme(t)
	team := teamWithEnv("acme", "dev", &butlerv1alpha1.EnvironmentLimits{
		MaxClustersPerMember: int32Ptr(1),
	})
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := &Reconciler{Client: c, Scheme: s}

	tc := buildEnvTC("new", "team-acme", "dev", "", 1)
	if err := r.validateEnvironmentQuotas(context.Background(), tc, team, nil); err != nil {
		t.Fatalf("expected skip when owner unknown, got %v", err)
	}
}

func TestValidateEnvironmentQuotas_DifferentOwnerNotCounted(t *testing.T) {
	s := envValidationScheme(t)
	team := teamWithEnv("acme", "dev", &butlerv1alpha1.EnvironmentLimits{
		MaxClustersPerMember: int32Ptr(1),
	})
	c := fake.NewClientBuilder().WithScheme(s).Build()
	r := &Reconciler{Client: c, Scheme: s}

	siblings := []butlerv1alpha1.TenantCluster{
		*buildEnvTC("a", "team-acme", "dev", "bob@example.com", 1),
	}
	tc := buildEnvTC("new", "team-acme", "dev", "alice@example.com", 1)

	if err := r.validateEnvironmentQuotas(context.Background(), tc, team, siblings); err != nil {
		t.Fatalf("expected allow (bob owns 1, alice owns 0), got %v", err)
	}
}

func conditionReason(tc *butlerv1alpha1.TenantCluster, condType string) string {
	for _, c := range tc.Status.Conditions {
		if c.Type == condType {
			return c.Reason
		}
	}
	return ""
}
