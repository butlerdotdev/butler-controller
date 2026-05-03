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

package networkpool

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

func TestSetCapacityConditions(t *testing.T) {
	tests := []struct {
		name         string
		usage        float64
		allocated    int32
		total        int32
		wantWarning  metav1.ConditionStatus
		wantCritical metav1.ConditionStatus
		wantExhaust  metav1.ConditionStatus
	}{
		{
			name:         "below all thresholds",
			usage:        50,
			allocated:    50,
			total:        100,
			wantWarning:  metav1.ConditionFalse,
			wantCritical: metav1.ConditionFalse,
			wantExhaust:  metav1.ConditionFalse,
		},
		{
			name:         "at warning threshold",
			usage:        70,
			allocated:    70,
			total:        100,
			wantWarning:  metav1.ConditionTrue,
			wantCritical: metav1.ConditionFalse,
			wantExhaust:  metav1.ConditionFalse,
		},
		{
			name:         "between warning and critical",
			usage:        80,
			allocated:    80,
			total:        100,
			wantWarning:  metav1.ConditionTrue,
			wantCritical: metav1.ConditionFalse,
			wantExhaust:  metav1.ConditionFalse,
		},
		{
			name:         "at critical threshold",
			usage:        85,
			allocated:    85,
			total:        100,
			wantWarning:  metav1.ConditionTrue,
			wantCritical: metav1.ConditionTrue,
			wantExhaust:  metav1.ConditionFalse,
		},
		{
			name:         "at exhausted threshold",
			usage:        95,
			allocated:    95,
			total:        100,
			wantWarning:  metav1.ConditionTrue,
			wantCritical: metav1.ConditionTrue,
			wantExhaust:  metav1.ConditionTrue,
		},
		{
			name:         "fully exhausted",
			usage:        100,
			allocated:    100,
			total:        100,
			wantWarning:  metav1.ConditionTrue,
			wantCritical: metav1.ConditionTrue,
			wantExhaust:  metav1.ConditionTrue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := &butlerv1alpha1.NetworkPool{
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
			}

			setCapacityConditions(pool, tt.usage, tt.allocated, tt.total)

			checkCondition(t, pool, ConditionCapacityWarning, tt.wantWarning)
			checkCondition(t, pool, ConditionCapacityCritical, tt.wantCritical)
			checkCondition(t, pool, ConditionCapacityExhausted, tt.wantExhaust)
		})
	}
}

func TestSetCapacityConditions_TransitionsCorrectly(t *testing.T) {
	pool := &butlerv1alpha1.NetworkPool{
		ObjectMeta: metav1.ObjectMeta{Generation: 1},
	}

	// Start above critical
	setCapacityConditions(pool, 90, 90, 100)
	checkCondition(t, pool, ConditionCapacityWarning, metav1.ConditionTrue)
	checkCondition(t, pool, ConditionCapacityCritical, metav1.ConditionTrue)
	checkCondition(t, pool, ConditionCapacityExhausted, metav1.ConditionFalse)

	// Drop below all thresholds
	setCapacityConditions(pool, 50, 50, 100)
	checkCondition(t, pool, ConditionCapacityWarning, metav1.ConditionFalse)
	checkCondition(t, pool, ConditionCapacityCritical, metav1.ConditionFalse)
	checkCondition(t, pool, ConditionCapacityExhausted, metav1.ConditionFalse)
}

func TestSetCapacityConditions_Reasons(t *testing.T) {
	pool := &butlerv1alpha1.NetworkPool{
		ObjectMeta: metav1.ObjectMeta{Generation: 1},
	}

	setCapacityConditions(pool, 90, 90, 100)

	warning := meta.FindStatusCondition(pool.Status.Conditions, ConditionCapacityWarning)
	if warning == nil {
		t.Fatal("CapacityWarning condition not found")
	}
	if warning.Reason != "UtilizationAboveThreshold" {
		t.Errorf("warning reason = %q, want UtilizationAboveThreshold", warning.Reason)
	}

	exhausted := meta.FindStatusCondition(pool.Status.Conditions, ConditionCapacityExhausted)
	if exhausted == nil {
		t.Fatal("CapacityExhausted condition not found")
	}
	if exhausted.Reason != "UtilizationBelowThreshold" {
		t.Errorf("exhausted reason = %q, want UtilizationBelowThreshold", exhausted.Reason)
	}
}

func checkCondition(t *testing.T, pool *butlerv1alpha1.NetworkPool, condType string, want metav1.ConditionStatus) {
	t.Helper()
	cond := meta.FindStatusCondition(pool.Status.Conditions, condType)
	if cond == nil {
		t.Fatalf("condition %q not found", condType)
	}
	if cond.Status != want {
		t.Errorf("condition %q status = %v, want %v", condType, cond.Status, want)
	}
}
