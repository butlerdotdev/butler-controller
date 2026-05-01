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
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

func TestValidateUpdate_TenantAllocationNarrowing(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	// IPAllocation with addresses inside the old range but outside the proposed new range.
	allocInRange := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-a-test-lb",
			Namespace: "butler-system",
			Labels: map[string]string{
				butlerv1alpha1.LabelNetworkPool: "pool-1",
			},
		},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.5",
			EndAddress:   "10.0.0.6",
			Addresses:    []string{"10.0.0.5", "10.0.0.6"},
		},
	}

	tests := []struct {
		name      string
		allocs    []butlerv1alpha1.IPAllocation
		oldStart  string
		oldEnd    string
		newStart  string
		newEnd    string
		wantErr   bool
		errSubstr string
	}{
		{
			name:     "narrowing with allocation outside new range rejects",
			allocs:   []butlerv1alpha1.IPAllocation{*allocInRange},
			oldStart: "10.0.0.1",
			oldEnd:   "10.0.0.10",
			newStart: "10.0.0.7",
			newEnd:   "10.0.0.10",
			wantErr:  true,
			// 10.0.0.5 is outside 10.0.0.7-10.0.0.10
			errSubstr: "cannot narrow tenant allocation range",
		},
		{
			name:     "widening is allowed",
			allocs:   []butlerv1alpha1.IPAllocation{*allocInRange},
			oldStart: "10.0.0.3",
			oldEnd:   "10.0.0.8",
			newStart: "10.0.0.1",
			newEnd:   "10.0.0.10",
			wantErr:  false,
		},
		{
			name:     "same range is allowed",
			allocs:   []butlerv1alpha1.IPAllocation{*allocInRange},
			oldStart: "10.0.0.1",
			oldEnd:   "10.0.0.10",
			newStart: "10.0.0.1",
			newEnd:   "10.0.0.10",
			wantErr:  false,
		},
		{
			name:     "narrowing with all allocations inside new range is allowed",
			allocs:   []butlerv1alpha1.IPAllocation{*allocInRange},
			oldStart: "10.0.0.1",
			oldEnd:   "10.0.0.20",
			newStart: "10.0.0.1",
			newEnd:   "10.0.0.10",
			wantErr:  false,
		},
		{
			name:     "narrowing with no allocations is allowed",
			allocs:   nil,
			oldStart: "10.0.0.1",
			oldEnd:   "10.0.0.20",
			newStart: "10.0.0.10",
			newEnd:   "10.0.0.15",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			for i := range tt.allocs {
				builder = builder.WithObjects(&tt.allocs[i])
			}
			cl := builder.Build()

			v := &NetworkPoolValidator{Client: cl}

			oldPool := &butlerv1alpha1.NetworkPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool-1", Namespace: "butler-system"},
				Spec: butlerv1alpha1.NetworkPoolSpec{
					CIDR: "10.0.0.0/24",
					TenantAllocation: &butlerv1alpha1.TenantAllocationConfig{
						Start: tt.oldStart,
						End:   tt.oldEnd,
					},
				},
			}
			newPool := &butlerv1alpha1.NetworkPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool-1", Namespace: "butler-system"},
				Spec: butlerv1alpha1.NetworkPoolSpec{
					CIDR: "10.0.0.0/24",
					TenantAllocation: &butlerv1alpha1.TenantAllocationConfig{
						Start: tt.newStart,
						End:   tt.newEnd,
					},
				},
			}

			_, err := v.ValidateUpdate(context.Background(), oldPool, newPool)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errSubstr != "" && !strings.Contains(err.Error(), tt.errSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errSubstr)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestValidateUpdate_TenantAllocationNarrowing_StartEndFallback(t *testing.T) {
	// Test the fallback path where Addresses is empty but StartAddress/EndAddress
	// are populated.
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	alloc := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-a-test-lb",
			Namespace: "butler-system",
			Labels: map[string]string{
				butlerv1alpha1.LabelNetworkPool: "pool-1",
			},
		},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.5",
			EndAddress:   "10.0.0.6",
			// Addresses is empty — exercises the fallback path
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(alloc).Build()
	v := &NetworkPoolValidator{Client: cl}

	oldPool := &butlerv1alpha1.NetworkPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-1", Namespace: "butler-system"},
		Spec: butlerv1alpha1.NetworkPoolSpec{
			CIDR: "10.0.0.0/24",
			TenantAllocation: &butlerv1alpha1.TenantAllocationConfig{
				Start: "10.0.0.1",
				End:   "10.0.0.10",
			},
		},
	}
	newPool := &butlerv1alpha1.NetworkPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-1", Namespace: "butler-system"},
		Spec: butlerv1alpha1.NetworkPoolSpec{
			CIDR: "10.0.0.0/24",
			TenantAllocation: &butlerv1alpha1.TenantAllocationConfig{
				Start: "10.0.0.7",
				End:   "10.0.0.10",
			},
		},
	}

	_, err := v.ValidateUpdate(context.Background(), oldPool, newPool)
	if err == nil {
		t.Fatal("expected error for narrowing with allocation outside range, got nil")
	}
	if !strings.Contains(err.Error(), "cannot narrow tenant allocation range") {
		t.Errorf("error %q does not contain expected message", err.Error())
	}
}

func TestValidateUpdate_TenantAllocationNarrowing_PendingSkipped(t *testing.T) {
	// Pending allocations should not block range narrowing.
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	alloc := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-a-test-lb",
			Namespace: "butler-system",
			Labels: map[string]string{
				butlerv1alpha1.LabelNetworkPool: "pool-1",
			},
		},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhasePending,
			StartAddress: "10.0.0.5",
			EndAddress:   "10.0.0.6",
			Addresses:    []string{"10.0.0.5", "10.0.0.6"},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(alloc).Build()
	v := &NetworkPoolValidator{Client: cl}

	oldPool := &butlerv1alpha1.NetworkPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-1", Namespace: "butler-system"},
		Spec: butlerv1alpha1.NetworkPoolSpec{
			CIDR: "10.0.0.0/24",
			TenantAllocation: &butlerv1alpha1.TenantAllocationConfig{
				Start: "10.0.0.1",
				End:   "10.0.0.10",
			},
		},
	}
	newPool := &butlerv1alpha1.NetworkPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-1", Namespace: "butler-system"},
		Spec: butlerv1alpha1.NetworkPoolSpec{
			CIDR: "10.0.0.0/24",
			TenantAllocation: &butlerv1alpha1.TenantAllocationConfig{
				Start: "10.0.0.7",
				End:   "10.0.0.10",
			},
		},
	}

	_, err := v.ValidateUpdate(context.Background(), oldPool, newPool)
	if err != nil {
		t.Errorf("pending allocation should not block narrowing, got error: %v", err)
	}
}
