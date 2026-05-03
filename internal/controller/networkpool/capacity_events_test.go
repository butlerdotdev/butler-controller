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
	"time"
)

func TestShouldEmitEvent_FirstCall(t *testing.T) {
	r := &Reconciler{}
	if !r.shouldEmitEvent("pool-a", reasonCapacityWarning) {
		t.Fatal("first call should always emit")
	}
}

func TestShouldEmitEvent_RateLimited(t *testing.T) {
	r := &Reconciler{}

	// First call emits
	if !r.shouldEmitEvent("pool-a", reasonCapacityWarning) {
		t.Fatal("first call should emit")
	}

	// Immediate second call is rate limited
	if r.shouldEmitEvent("pool-a", reasonCapacityWarning) {
		t.Fatal("second call within interval should be rate limited")
	}
}

func TestShouldEmitEvent_DifferentPoolsIndependent(t *testing.T) {
	r := &Reconciler{}

	if !r.shouldEmitEvent("pool-a", reasonCapacityWarning) {
		t.Fatal("pool-a first call should emit")
	}
	if !r.shouldEmitEvent("pool-b", reasonCapacityWarning) {
		t.Fatal("pool-b first call should emit (independent of pool-a)")
	}
}

func TestShouldEmitEvent_DifferentTiersIndependent(t *testing.T) {
	r := &Reconciler{}

	if !r.shouldEmitEvent("pool-a", reasonCapacityWarning) {
		t.Fatal("warning should emit")
	}
	if !r.shouldEmitEvent("pool-a", reasonCapacityCritical) {
		t.Fatal("critical should emit (independent of warning)")
	}
}

func TestShouldEmitEvent_EmitsAfterInterval(t *testing.T) {
	r := &Reconciler{}

	// Seed with a timestamp in the past
	key := "pool-a/" + reasonCapacityWarning
	r.lastEventTimes.Store(key, time.Now().Add(-11*time.Minute))

	if !r.shouldEmitEvent("pool-a", reasonCapacityWarning) {
		t.Fatal("should emit after interval has passed")
	}
}

func TestHasPreviousEvent_NoPrior(t *testing.T) {
	r := &Reconciler{}
	if r.hasPreviousEvent("pool-a") {
		t.Fatal("should return false with no prior events")
	}
}

func TestHasPreviousEvent_AfterWarning(t *testing.T) {
	r := &Reconciler{}
	r.shouldEmitEvent("pool-a", reasonCapacityWarning)
	if !r.hasPreviousEvent("pool-a") {
		t.Fatal("should return true after a warning was emitted")
	}
}

func TestHasPreviousEvent_AfterCritical(t *testing.T) {
	r := &Reconciler{}
	r.shouldEmitEvent("pool-a", reasonCapacityCritical)
	if !r.hasPreviousEvent("pool-a") {
		t.Fatal("should return true after a critical was emitted")
	}
}

func TestHasPreviousEvent_RecoveryDoesNotCount(t *testing.T) {
	r := &Reconciler{}
	r.shouldEmitEvent("pool-a", reasonCapacityRecovered)
	if r.hasPreviousEvent("pool-a") {
		t.Fatal("recovery events should not count as previous warning events")
	}
}
