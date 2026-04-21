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
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

func buildTC(name, namespace, envLabel, owner string, replicas int32) *butlerv1alpha1.TenantCluster {
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
		Spec: butlerv1alpha1.TenantClusterSpec{
			TeamRef: &butlerv1alpha1.LocalObjectReference{Name: "acme"},
			Workers: butlerv1alpha1.WorkersSpec{
				Replicas:        replicas,
				MachineTemplate: butlerv1alpha1.MachineTemplateSpec{CPU: 2},
			},
		},
	}
	if envLabel != "" {
		tc.Labels[butlerv1alpha1.LabelEnvironment] = envLabel
	}
	if owner != "" {
		tc.Annotations[butlerv1alpha1.AnnotationOwner] = owner
	}
	return tc
}

func validateEnvOnly(t *testing.T, c client.Client, tc *butlerv1alpha1.TenantCluster) error {
	t.Helper()
	return validateEnvAs(t, c, tc, nil, authnv1.UserInfo{})
}

func validateEnvAs(t *testing.T, c client.Client, tc, oldTC *butlerv1alpha1.TenantCluster, user authnv1.UserInfo) error {
	t.Helper()
	v := &TenantClusterValidator{Client: c, APIReader: c}
	errs, err := v.validateEnvironment(context.Background(), tc, oldTC, user)
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
			MaxClusters: &maxClusters,
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
				MaxClusters: &maxClusters,
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
	err := validateEnvAs(t, c, tc, nil, authnv1.UserInfo{Username: "alice@example.com"})
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
	if err := validateEnvAs(t, c, tc, nil, authnv1.UserInfo{Username: "alice@example.com"}); err != nil {
		t.Fatalf("expected allowed, got error: %v", err)
	}
}

// --- Creator-email spoof defense (finding #2) ---

func TestTCWebhook_CreatorEmailSpoof_Denied(t *testing.T) {
	s := teamScheme(t)
	maxPerMember := int32(1)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "sandbox",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClustersPerMember: &maxPerMember,
		},
	})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	tc := buildTC("sb-2", "team-acme", "sandbox", "", 1)
	tc.Annotations = map[string]string{
		butlerv1alpha1.AnnotationCreatorEmail: "victim@example.com",
	}
	err := validateEnvAs(t, c, tc, nil, authnv1.UserInfo{Username: "attacker@example.com"})
	if err == nil || !strings.Contains(err.Error(), "must match") {
		t.Fatalf("expected spoof denial, got %v", err)
	}
}

func TestTCWebhook_CreatorEmailCaseInsensitive_Allowed(t *testing.T) {
	s := teamScheme(t)
	maxPerMember := int32(2)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "sandbox",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClustersPerMember: &maxPerMember,
		},
	})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	tc := buildTC("sb-1", "team-acme", "sandbox", "", 1)
	tc.Annotations = map[string]string{
		butlerv1alpha1.AnnotationCreatorEmail: "Alice@Example.COM",
	}
	if err := validateEnvAs(t, c, tc, nil, authnv1.UserInfo{Username: "alice@example.com"}); err != nil {
		t.Fatalf("expected case-insensitive match to allow, got %v", err)
	}
}

func TestTCWebhook_CreatorEmailAnnotationIgnored_NoPerMemberCap(t *testing.T) {
	s := teamScheme(t)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name:   "sandbox",
		Limits: &butlerv1alpha1.EnvironmentLimits{}, // no MaxClustersPerMember
	})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	tc := buildTC("sb-1", "team-acme", "sandbox", "", 1)
	tc.Annotations = map[string]string{
		butlerv1alpha1.AnnotationCreatorEmail: "does-not-match@elsewhere.com",
	}
	// Per-member cap not engaged, so the annotation-identity match is not
	// checked. Mismatched annotation passes through.
	if err := validateEnvAs(t, c, tc, nil, authnv1.UserInfo{Username: "someone@example.com"}); err != nil {
		t.Fatalf("expected allowed when MaxClustersPerMember is unset, got %v", err)
	}
}

// --- Environment label immutability on update ---

// withMigrationAnnotation returns a shallow copy of tc with the
// migration-operation annotation set to "true".
func withMigrationAnnotation(tc *butlerv1alpha1.TenantCluster) *butlerv1alpha1.TenantCluster {
	out := tc.DeepCopy()
	if out.Annotations == nil {
		out.Annotations = map[string]string{}
	}
	out.Annotations[butlerv1alpha1.AnnotationMigrationOperation] = "true"
	return out
}

func TestEnvLabelImmutability_Unchanged_Allowed(t *testing.T) {
	old := buildTC("tc", "team-acme", "prod", "", 3)
	new := buildTC("tc", "team-acme", "prod", "", 5) // replicas differ; env label does not
	errs := validateEnvironmentLabelImmutability(old, new)
	if len(errs) != 0 {
		t.Fatalf("expected no errors, got: %v", errs.ToAggregate())
	}
}

func TestEnvLabelImmutability_Changed_NoAnnotation_Denied(t *testing.T) {
	old := buildTC("tc", "team-acme", "prod", "", 3)
	new := buildTC("tc", "team-acme", "dev", "", 3)
	errs := validateEnvironmentLabelImmutability(old, new)
	if len(errs) == 0 {
		t.Fatal("expected denial for env label change without migration annotation")
	}
	if !strings.Contains(errs.ToAggregate().Error(), "migration-operation") {
		t.Fatalf("expected message to cite migration-operation annotation, got: %v", errs.ToAggregate())
	}
}

func TestEnvLabelImmutability_Changed_WithAnnotation_Allowed(t *testing.T) {
	old := buildTC("tc", "team-acme", "prod", "", 3)
	new := withMigrationAnnotation(buildTC("tc", "team-acme", "dev", "", 3))
	errs := validateEnvironmentLabelImmutability(old, new)
	if len(errs) != 0 {
		t.Fatalf("expected allowed with migration annotation, got: %v", errs.ToAggregate())
	}
}

func TestEnvLabelImmutability_Removed_NoAnnotation_Denied(t *testing.T) {
	old := buildTC("tc", "team-acme", "prod", "", 3)
	new := buildTC("tc", "team-acme", "", "", 3)
	errs := validateEnvironmentLabelImmutability(old, new)
	if len(errs) == 0 {
		t.Fatal("expected denial for env label removal without migration annotation")
	}
}

func TestEnvLabelImmutability_Added_NoAnnotation_Denied(t *testing.T) {
	old := buildTC("tc", "team-acme", "", "", 3)
	new := buildTC("tc", "team-acme", "prod", "", 3)
	errs := validateEnvironmentLabelImmutability(old, new)
	if len(errs) == 0 {
		t.Fatal("expected denial for env label addition without migration annotation")
	}
}

func TestEnvLabelImmutability_Added_WithAnnotation_Allowed(t *testing.T) {
	old := buildTC("tc", "team-acme", "", "", 3)
	new := withMigrationAnnotation(buildTC("tc", "team-acme", "prod", "", 3))
	errs := validateEnvironmentLabelImmutability(old, new)
	if len(errs) != 0 {
		t.Fatalf("expected allowed migration-path add, got: %v", errs.ToAggregate())
	}
}

// --- Phase-2 grandfathering + env-deletion tolerance (fresh-review findings) ---

func TestTCWebhook_Phase2_PreExistingUnlabeled_UpdateAllowed(t *testing.T) {
	s := teamScheme(t)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{Name: "prod"})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	// TC pre-dates the env addition: old and new both carry no env label.
	oldTC := buildTC("legacy", "team-acme", "", "", 3)
	newTC := buildTC("legacy", "team-acme", "", "", 5) // scaled workers
	if err := validateEnvAs(t, c, newTC, oldTC, authnv1.UserInfo{}); err != nil {
		t.Fatalf("expected grandfathered update to pass, got %v", err)
	}
}

func TestTCWebhook_Phase2_PreExistingUnlabeled_CreateDenied(t *testing.T) {
	s := teamScheme(t)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{Name: "prod"})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	// A brand-new TC without the label is rejected even after envs exist.
	tc := buildTC("new-no-label", "team-acme", "", "", 3)
	err := validateEnvAs(t, c, tc, nil, authnv1.UserInfo{})
	if err == nil || !strings.Contains(err.Error(), "defines environments") {
		t.Fatalf("expected new-create denial, got %v", err)
	}
}

func TestTCWebhook_EnvRemoved_UnchangedLabelUpdate_Allowed(t *testing.T) {
	s := teamScheme(t)
	// Team has "dev" but no longer has "prod"; a TC carrying env=prod
	// survives an unchanged-label update so it can be scaled or deleted.
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{Name: "dev"})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	oldTC := buildTC("orphan", "team-acme", "prod", "", 3)
	newTC := buildTC("orphan", "team-acme", "prod", "", 5) // scale
	if err := validateEnvAs(t, c, newTC, oldTC, authnv1.UserInfo{}); err != nil {
		t.Fatalf("expected dangling-env update tolerated, got %v", err)
	}
}

func TestTCWebhook_EnvRemoved_NewCreate_Denied(t *testing.T) {
	s := teamScheme(t)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{Name: "dev"})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()

	tc := buildTC("new-prod", "team-acme", "prod", "", 3)
	err := validateEnvAs(t, c, tc, nil, authnv1.UserInfo{})
	if err == nil {
		t.Fatal("expected denial for new create referencing removed env")
	}
}

// --- Admission-entrypoint tests (fix for review-round-3 finding 2) ---
//
// Prior tests invoked validateEnvironment / validateEnvironmentLabelImmutability
// directly, skipping the Handle() dispatch, JSON decode, create-vs-update
// branching, and validateCreateUpdate's ProviderConfigRef gating. Round 2's
// live-validation finding (env short-circuited on missing providerConfigRef)
// and round 3's deadlock finding (per-member cap on UPDATE) both slipped
// past unit tests because of this gap. These tests go through v.Handle with
// full admission.Request payloads.

func newTCAdmissionRequest(t *testing.T, op admissionv1.Operation, username string, newTC, oldTC *butlerv1alpha1.TenantCluster) admission.Request {
	t.Helper()
	newRaw, err := json.Marshal(newTC)
	if err != nil {
		t.Fatalf("marshal newTC: %v", err)
	}
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: op,
			UserInfo:  authnv1.UserInfo{Username: username},
			Object:    runtime.RawExtension{Raw: newRaw},
		},
	}
	if oldTC != nil {
		oldRaw, err := json.Marshal(oldTC)
		if err != nil {
			t.Fatalf("marshal oldTC: %v", err)
		}
		req.OldObject = runtime.RawExtension{Raw: oldRaw}
	}
	return req
}

// The deadlock fix: reducing per-member cap below current ownership must not
// block updates to existing resources. Without the isCreate gate, this test
// fails because ownedByCreator+1 > cap rejects the update.
func TestTCWebhook_Handle_UpdateAfterPerMemberCapReduced_Allowed(t *testing.T) {
	s := teamScheme(t)
	maxPerMember := int32(1)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "sandbox",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClustersPerMember: &maxPerMember,
		},
	})
	// Alice already owns two TCs in sandbox (from before cap was reduced).
	a1 := buildTC("sb-a1", "team-acme", "sandbox", "alice@example.com", 1)
	a1.Annotations[butlerv1alpha1.AnnotationCreatorEmail] = "alice@example.com"
	a2 := buildTC("sb-a2", "team-acme", "sandbox", "alice@example.com", 1)
	a2.Annotations[butlerv1alpha1.AnnotationCreatorEmail] = "alice@example.com"
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team, a1, a2).Build()
	v := &TenantClusterValidator{Client: c, APIReader: c}

	// Routine update: scale sb-a1 workers from 1 to 3 (no label/annotation
	// change; per-cluster and per-member identities stay as-is).
	newA1 := a1.DeepCopy()
	newA1.Spec.Workers.Replicas = 3

	req := newTCAdmissionRequest(t, admissionv1.Update, "alice@example.com", newA1, a1)
	assertAllowed(t, v.Handle(context.Background(), req))
}

// The deletion chain: apiserver converts DELETE-with-finalizer into an
// UPDATE that sets deletionTimestamp, and the controller's finalizer
// removal is another UPDATE. Neither can be rejected by a cap simulation
// even if the cap has been reduced below current usage.
func TestTCWebhook_Handle_FinalizerRemovalUnderReducedCap_Allowed(t *testing.T) {
	s := teamScheme(t)
	maxPerMember := int32(1)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "sandbox",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClustersPerMember: &maxPerMember,
		},
	})
	a1 := buildTC("sb-a1", "team-acme", "sandbox", "alice@example.com", 1)
	a1.Annotations[butlerv1alpha1.AnnotationCreatorEmail] = "alice@example.com"
	a1.Finalizers = []string{"butler.butlerlabs.dev/tenantcluster"}
	a2 := buildTC("sb-a2", "team-acme", "sandbox", "alice@example.com", 1)
	a2.Annotations[butlerv1alpha1.AnnotationCreatorEmail] = "alice@example.com"
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team, a1, a2).Build()
	v := &TenantClusterValidator{Client: c, APIReader: c}

	// Controller removes the finalizer; reconciler's update call runs as
	// the controller SA, which is not alice but still must succeed.
	newA1 := a1.DeepCopy()
	newA1.Finalizers = nil

	req := newTCAdmissionRequest(t, admissionv1.Update, "system:serviceaccount:butler-system:butler-controller", newA1, a1)
	assertAllowed(t, v.Handle(context.Background(), req))
}

// Env cluster-count cap must also skip on update; same deadlock shape.
func TestTCWebhook_Handle_UpdateAfterEnvClusterCapReduced_Allowed(t *testing.T) {
	s := teamScheme(t)
	maxClusters := int32(1)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "prod",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClusters: &maxClusters,
		},
	})
	existing1 := buildTC("p1", "team-acme", "prod", "u1", 1)
	existing2 := buildTC("p2", "team-acme", "prod", "u2", 1)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team, existing1, existing2).Build()
	v := &TenantClusterValidator{Client: c, APIReader: c}

	// Scale existing1; env now has 2 clusters against a cap of 1.
	newEx := existing1.DeepCopy()
	newEx.Spec.Workers.Replicas = 3
	req := newTCAdmissionRequest(t, admissionv1.Update, "anyone@example.com", newEx, existing1)
	assertAllowed(t, v.Handle(context.Background(), req))
}

// Create path still denies spoofed creator-email. Goes through Handle so
// admission.Request parsing + validateCreateUpdate dispatch are exercised.
func TestTCWebhook_Handle_Create_SpoofedCreatorEmail_Denied(t *testing.T) {
	s := teamScheme(t)
	maxPerMember := int32(1)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name: "sandbox",
		Limits: &butlerv1alpha1.EnvironmentLimits{
			MaxClustersPerMember: &maxPerMember,
		},
	})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()
	v := &TenantClusterValidator{Client: c, APIReader: c}

	tc := buildTC("sb-x", "team-acme", "sandbox", "", 1)
	tc.Annotations[butlerv1alpha1.AnnotationCreatorEmail] = "victim@example.com"
	req := newTCAdmissionRequest(t, admissionv1.Create, "attacker@example.com", tc, nil)
	assertDenied(t, v.Handle(context.Background(), req), "must match")
}

// Env label immutability on a label-change UPDATE without the migration
// annotation. Round 2 tested this via the helper directly; the entrypoint
// path adds the JSON decode and handleUpdate dispatch.
func TestTCWebhook_Handle_Update_EnvLabelChangedNoMigration_Denied(t *testing.T) {
	s := teamScheme(t)
	team := newTeamWithEnvs("acme",
		butlerv1alpha1.EnvironmentSpec{Name: "prod"},
		butlerv1alpha1.EnvironmentSpec{Name: "dev"},
	)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()
	v := &TenantClusterValidator{Client: c, APIReader: c}

	old := buildTC("tc", "team-acme", "prod", "", 1)
	new := buildTC("tc", "team-acme", "dev", "", 1)
	req := newTCAdmissionRequest(t, admissionv1.Update, "any@example.com", new, old)
	assertDenied(t, v.Handle(context.Background(), req), "migration-operation")
}

// TC without providerConfigRef must still run env validation; the
// create path must not short-circuit before reaching the env-label
// check. Caught in round 2 by live validation; now covered at unit level.
func TestTCWebhook_Handle_Create_NoProviderConfig_MissingEnvLabel_Denied(t *testing.T) {
	s := teamScheme(t)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{Name: "prod"})
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team).Build()
	v := &TenantClusterValidator{Client: c, APIReader: c}

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "no-pc", Namespace: "team-acme",
			Labels: map[string]string{}, Annotations: map[string]string{},
		},
		Spec: butlerv1alpha1.TenantClusterSpec{
			TeamRef: &butlerv1alpha1.LocalObjectReference{Name: "acme"},
			Workers: butlerv1alpha1.WorkersSpec{Replicas: 1},
		},
	}
	req := newTCAdmissionRequest(t, admissionv1.Create, "any@example.com", tc, nil)
	assertDenied(t, v.Handle(context.Background(), req), "defines environments")
}

// MaxClusters=0 must be treated as "no cap", matching MaxClustersPerMember.
func TestTCWebhook_Handle_Create_EnvMaxClustersZero_Allowed(t *testing.T) {
	s := teamScheme(t)
	zero := int32(0)
	team := newTeamWithEnvs("acme", butlerv1alpha1.EnvironmentSpec{
		Name:   "prod",
		Limits: &butlerv1alpha1.EnvironmentLimits{MaxClusters: &zero},
	})
	existing := buildTC("p1", "team-acme", "prod", "", 1)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(team, existing).Build()
	v := &TenantClusterValidator{Client: c, APIReader: c}

	tc := buildTC("p2", "team-acme", "prod", "", 1)
	req := newTCAdmissionRequest(t, admissionv1.Create, "any@example.com", tc, nil)
	assertAllowed(t, v.Handle(context.Background(), req))
}
