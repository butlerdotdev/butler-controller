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

	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// ClusterCreationPolicyValidator validates ClusterCreationPolicy
// resources on admission. See ADR-018 Decision section 7 and 8.
//
// Structural validation (exactly-one-of on spec.scope, enum values on
// OptionMode and OptionType) is enforced by the CRD schema before the
// webhook runs. This webhook layers referential integrity checks,
// mode-specific value requirements with operator-facing error messages,
// and intra-tier conflict detection that the schema cannot express.
//
// Provider-entry existence is intentionally not validated here. A
// provider outage at admission time would block legitimate policy
// authoring; stale references are surfaced by the deferred status
// reconciler instead (ADR-018 Deferred).
type ClusterCreationPolicyValidator struct {
	Client    client.Client
	APIReader client.Reader
}

// +kubebuilder:webhook:path=/validate-butler-butlerlabs-dev-v1alpha1-clustercreationpolicy,mutating=false,failurePolicy=fail,sideEffects=None,groups=butler.butlerlabs.dev,resources=clustercreationpolicies,verbs=create;update,versions=v1alpha1,name=vclustercreationpolicy.kb.io,admissionReviewVersions=v1

// Handle implements admission.Handler.
func (v *ClusterCreationPolicyValidator) Handle(ctx context.Context, req admission.Request) admission.Response {
	log.FromContext(ctx).Info("ccp admission request received",
		"op", string(req.Operation),
		"name", req.Name,
		"user", req.UserInfo.Username,
	)
	switch req.Operation {
	case admissionv1.Create, admissionv1.Update:
		policy := &butlerv1alpha1.ClusterCreationPolicy{}
		if err := json.Unmarshal(req.Object.Raw, policy); err != nil {
			return admission.Errored(http.StatusBadRequest, fmt.Errorf("decode ClusterCreationPolicy: %w", err))
		}
		if err := v.validate(ctx, policy); err != nil {
			return admission.Denied(err.Error())
		}
		return admission.Allowed("")
	default:
		return admission.Allowed("")
	}
}

// validate runs the four referential and structural checks. Returns nil
// when the policy is admissible.
func (v *ClusterCreationPolicyValidator) validate(ctx context.Context, policy *butlerv1alpha1.ClusterCreationPolicy) error {
	var errs field.ErrorList

	errs = append(errs, v.validateScopeReferences(ctx, policy)...)
	errs = append(errs, validateOptionRules(policy)...)
	conflictErrs, err := v.detectIntraTierConflict(ctx, policy)
	if err != nil {
		return fmt.Errorf("scan existing policies for conflict: %w", err)
	}
	errs = append(errs, conflictErrs...)

	if len(errs) == 0 {
		return nil
	}
	return errs.ToAggregate()
}

// validateScopeReferences checks that any referenced Team and
// environment exist. Uses APIReader (uncached) so a just-created Team
// is visible.
func (v *ClusterCreationPolicyValidator) validateScopeReferences(ctx context.Context, policy *butlerv1alpha1.ClusterCreationPolicy) field.ErrorList {
	var errs field.ErrorList
	scope := policy.Spec.Scope

	if scope.Team != nil {
		team := &butlerv1alpha1.Team{}
		if err := v.APIReader.Get(ctx, types.NamespacedName{Name: scope.Team.TeamRef.Name}, team); err != nil {
			if apierrors.IsNotFound(err) {
				errs = append(errs, field.NotFound(
					field.NewPath("spec", "scope", "team", "teamRef", "name"),
					scope.Team.TeamRef.Name,
				))
			} else {
				errs = append(errs, field.InternalError(
					field.NewPath("spec", "scope", "team", "teamRef", "name"),
					fmt.Errorf("fetch team: %w", err),
				))
			}
		}
		return errs
	}

	if scope.TeamAndEnvironment != nil {
		team := &butlerv1alpha1.Team{}
		teamPath := field.NewPath("spec", "scope", "teamAndEnvironment")
		if err := v.APIReader.Get(ctx, types.NamespacedName{Name: scope.TeamAndEnvironment.TeamRef.Name}, team); err != nil {
			if apierrors.IsNotFound(err) {
				errs = append(errs, field.NotFound(
					teamPath.Child("teamRef", "name"),
					scope.TeamAndEnvironment.TeamRef.Name,
				))
				return errs
			}
			errs = append(errs, field.InternalError(
				teamPath.Child("teamRef", "name"),
				fmt.Errorf("fetch team: %w", err),
			))
			return errs
		}
		envFound := false
		for _, env := range team.Spec.Environments {
			if env.Name == scope.TeamAndEnvironment.EnvironmentName {
				envFound = true
				break
			}
		}
		if !envFound {
			errs = append(errs, field.NotFound(
				teamPath.Child("environmentName"),
				scope.TeamAndEnvironment.EnvironmentName,
			))
		}
	}
	return errs
}

// validateOptionRules enforces the mode-specific value requirements that
// produce better operator-facing error messages than CRD schema CEL.
// Per ADR-018 Decision section 7.
func validateOptionRules(policy *butlerv1alpha1.ClusterCreationPolicy) field.ErrorList {
	var errs field.ErrorList
	for optType, rule := range policy.Spec.Options {
		path := field.NewPath("spec", "options").Key(string(optType))
		switch rule.Mode {
		case butlerv1alpha1.OptionModePin:
			if len(rule.Values) != 1 {
				errs = append(errs, field.Invalid(
					path.Child("values"),
					rule.Values,
					fmt.Sprintf("mode \"pin\" requires exactly one value, got %d", len(rule.Values)),
				))
			}
		case butlerv1alpha1.OptionModeAllowList:
			if len(rule.Values) == 0 {
				errs = append(errs, field.Required(
					path.Child("values"),
					"at least one value is required when mode is \"allowList\"",
				))
			}
		case butlerv1alpha1.OptionModeDefault:
			if rule.Default == "" {
				errs = append(errs, field.Required(
					path.Child("default"),
					"default value is required when mode is \"default\"",
				))
			}
		case butlerv1alpha1.OptionModeRecommended:
			if len(rule.Values) == 0 {
				errs = append(errs, field.Required(
					path.Child("values"),
					"at least one value is required when mode is \"recommended\"",
				))
			}
		}
	}
	return errs
}

// detectIntraTierConflict scans existing policies for a conflict with
// the policy being admitted. Conflict scope per ADR-018 Decision section
// 6 step 4: same tier (clusterWide vs team vs teamAndEnvironment),
// overlapping targetProviders, AND shared option-type keys.
func (v *ClusterCreationPolicyValidator) detectIntraTierConflict(ctx context.Context, policy *butlerv1alpha1.ClusterCreationPolicy) (field.ErrorList, error) {
	var errs field.ErrorList
	existing := &butlerv1alpha1.ClusterCreationPolicyList{}
	if err := v.Client.List(ctx, existing); err != nil {
		return nil, err
	}
	tier := tierOfScope(policy.Spec.Scope)
	for i := range existing.Items {
		other := &existing.Items[i]
		if other.Name == policy.Name {
			continue
		}
		if tierOfScope(other.Spec.Scope) != tier {
			continue
		}
		if !sameScopeTarget(policy.Spec.Scope, other.Spec.Scope) {
			continue
		}
		if !providersOverlap(policy.Spec.TargetProviders, other.Spec.TargetProviders) {
			continue
		}
		shared := sharedOptionTypes(policy.Spec.Options, other.Spec.Options)
		if len(shared) == 0 {
			continue
		}
		errs = append(errs, field.Forbidden(
			field.NewPath("spec", "options"),
			fmt.Sprintf("conflicts with ClusterCreationPolicy %q on option types %v at the same tier", other.Name, shared),
		))
	}
	return errs, nil
}

type scopeTier int

const (
	stClusterWide scopeTier = iota + 1
	stTeam
	stTeamAndEnv
)

func tierOfScope(s butlerv1alpha1.PolicyScope) scopeTier {
	switch {
	case s.TeamAndEnvironment != nil:
		return stTeamAndEnv
	case s.Team != nil:
		return stTeam
	case s.ClusterWide != nil:
		return stClusterWide
	}
	return 0
}

func sameScopeTarget(a, b butlerv1alpha1.PolicyScope) bool {
	switch {
	case a.ClusterWide != nil && b.ClusterWide != nil:
		return true
	case a.Team != nil && b.Team != nil:
		return a.Team.TeamRef.Name == b.Team.TeamRef.Name
	case a.TeamAndEnvironment != nil && b.TeamAndEnvironment != nil:
		return a.TeamAndEnvironment.TeamRef.Name == b.TeamAndEnvironment.TeamRef.Name &&
			a.TeamAndEnvironment.EnvironmentName == b.TeamAndEnvironment.EnvironmentName
	}
	return false
}

func providersOverlap(a, b []butlerv1alpha1.ProviderType) bool {
	if len(a) == 0 || len(b) == 0 {
		return true
	}
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
}

func sharedOptionTypes(a, b map[butlerv1alpha1.OptionType]butlerv1alpha1.OptionRule) []butlerv1alpha1.OptionType {
	var shared []butlerv1alpha1.OptionType
	for k := range a {
		if _, ok := b[k]; ok {
			shared = append(shared, k)
		}
	}
	return shared
}

// SetupWebhookWithManager registers the ClusterCreationPolicy validating
// webhook with the manager.
func (v *ClusterCreationPolicyValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	v.Client = mgr.GetClient()
	v.APIReader = mgr.GetAPIReader()
	mgr.GetWebhookServer().Register(
		"/validate-butler-butlerlabs-dev-v1alpha1-clustercreationpolicy",
		&admission.Webhook{Handler: v},
	)
	return nil
}
