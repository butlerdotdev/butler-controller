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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// TestValidateOptionRules_UnknownOptionType verifies the CRD schema gap
// where kubebuilder Enum on OptionType applies to map values but not to
// map keys. The webhook closes that gap.
func TestValidateOptionRules_UnknownOptionType(t *testing.T) {
	cases := []struct {
		name        string
		policy      *butlerv1alpha1.ClusterCreationPolicy
		wantErr     bool
		errContains string
	}{
		{
			name: "known option type passes",
			policy: &butlerv1alpha1.ClusterCreationPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "ok-image-pin"},
				Spec: butlerv1alpha1.ClusterCreationPolicySpec{
					Scope: butlerv1alpha1.PolicyScope{
						PlatformWide: &butlerv1alpha1.PlatformWideScope{},
					},
					TargetProviders: []butlerv1alpha1.ProviderType{butlerv1alpha1.ProviderTypeNutanix},
					Options: map[butlerv1alpha1.OptionType]butlerv1alpha1.OptionRule{
						butlerv1alpha1.OptionTypeImage: {
							Mode:   butlerv1alpha1.OptionModePin,
							Values: []string{"img-uuid"},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "unknown option type rejected",
			policy: &butlerv1alpha1.ClusterCreationPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "bad-unknown-type"},
				Spec: butlerv1alpha1.ClusterCreationPolicySpec{
					Scope: butlerv1alpha1.PolicyScope{
						PlatformWide: &butlerv1alpha1.PlatformWideScope{},
					},
					TargetProviders: []butlerv1alpha1.ProviderType{butlerv1alpha1.ProviderTypeNutanix},
					Options: map[butlerv1alpha1.OptionType]butlerv1alpha1.OptionRule{
						butlerv1alpha1.OptionType("unknownOptionType"): {
							Mode:   butlerv1alpha1.OptionModePin,
							Values: []string{"v"},
						},
					},
				},
			},
			wantErr:     true,
			errContains: "Unsupported value",
		},
		{
			name: "pin with two values rejected",
			policy: &butlerv1alpha1.ClusterCreationPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "bad-pin-two-values"},
				Spec: butlerv1alpha1.ClusterCreationPolicySpec{
					Scope: butlerv1alpha1.PolicyScope{
						PlatformWide: &butlerv1alpha1.PlatformWideScope{},
					},
					Options: map[butlerv1alpha1.OptionType]butlerv1alpha1.OptionRule{
						butlerv1alpha1.OptionTypeImage: {
							Mode:   butlerv1alpha1.OptionModePin,
							Values: []string{"a", "b"},
						},
					},
				},
			},
			wantErr:     true,
			errContains: "exactly one value",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			errs := validateOptionRules(tc.policy)
			if tc.wantErr {
				if len(errs) == 0 {
					t.Fatalf("expected validation error, got none")
				}
				if tc.errContains != "" && !strings.Contains(errs.ToAggregate().Error(), tc.errContains) {
					t.Errorf("error message %q does not contain %q", errs.ToAggregate().Error(), tc.errContains)
				}
				return
			}
			if len(errs) > 0 {
				t.Fatalf("expected no error, got: %s", errs.ToAggregate().Error())
			}
		})
	}
}
