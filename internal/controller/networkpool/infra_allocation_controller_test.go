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
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// ── Helpers ──

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

func newLBService(name, namespace, ip string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{{IP: ip}},
			},
		},
	}
}

func newPool(name, ns, cidr string, reserved []butlerv1alpha1.ReservedRange) *butlerv1alpha1.NetworkPool {
	return &butlerv1alpha1.NetworkPool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: butlerv1alpha1.NetworkPoolSpec{
			CIDR:     cidr,
			Reserved: reserved,
		},
	}
}

type patchCapture struct {
	called bool
	pool   *butlerv1alpha1.NetworkPool
}

// reconcileWith wraps a fake client with an interceptor that counts
// SubResourcePatch calls via the patchCapture flag. Use this for any test
// that needs to assert a reconciler's no-op patch guard works correctly.
//
// Asserting absence of state change in the fake client is not sufficient:
// a reconciler can patch with the same desired value and still be looping.
// Counting patch calls proves the comparison-before-patch path is correct.
func reconcileWith(t *testing.T, pool *butlerv1alpha1.NetworkPool, services ...*corev1.Service) (ctrl.Result, patchCapture) {
	t.Helper()
	s := testScheme()
	objs := []client.Object{pool}
	for _, svc := range services {
		objs = append(objs, svc)
	}
	fc := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&butlerv1alpha1.NetworkPool{}).
		Build()

	var capture patchCapture
	cl := interceptor.NewClient(fc, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			capture.called = true
			if p, ok := obj.(*butlerv1alpha1.NetworkPool); ok {
				capture.pool = p.DeepCopy()
			}
			return nil
		},
	})

	r := &InfraAllocationReconciler{Client: cl, Scheme: s}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: pool.Name, Namespace: pool.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	return result, capture
}

// ── extractLBIPs ──

func TestExtractLBIPs(t *testing.T) {
	tests := []struct {
		name string
		svc  *corev1.Service
		want []string
	}{
		{
			name: "single IP",
			svc:  newLBService("s", "ns", "10.0.0.1"),
			want: []string{"10.0.0.1"},
		},
		{
			name: "multiple IPs sorted",
			svc: &corev1.Service{
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{
							{IP: "10.0.0.3"},
							{IP: "10.0.0.1"},
						},
					},
				},
			},
			want: []string{"10.0.0.1", "10.0.0.3"},
		},
		{
			name: "no ingress",
			svc:  &corev1.Service{},
			want: nil,
		},
		{
			name: "hostname only, no IP",
			svc: &corev1.Service{
				Status: corev1.ServiceStatus{
					LoadBalancer: corev1.LoadBalancerStatus{
						Ingress: []corev1.LoadBalancerIngress{
							{Hostname: "example.com"},
						},
					},
				},
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractLBIPs(tt.svc)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("IP[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// ── lbIngressIPsEqual ──

func TestLBIngressIPsEqual(t *testing.T) {
	tests := []struct {
		name string
		old  *corev1.Service
		new  *corev1.Service
		want bool
	}{
		{
			name: "both empty",
			old:  &corev1.Service{},
			new:  &corev1.Service{},
			want: true,
		},
		{
			name: "same IPs",
			old:  newLBService("s", "ns", "10.0.0.1"),
			new:  newLBService("s", "ns", "10.0.0.1"),
			want: true,
		},
		{
			name: "different IPs",
			old:  newLBService("s", "ns", "10.0.0.1"),
			new:  newLBService("s", "ns", "10.0.0.2"),
			want: false,
		},
		{
			name: "IP added",
			old:  &corev1.Service{},
			new:  newLBService("s", "ns", "10.0.0.1"),
			want: false,
		},
		{
			name: "IP removed",
			old:  newLBService("s", "ns", "10.0.0.1"),
			new:  &corev1.Service{},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lbIngressIPsEqual(tt.old, tt.new)
			if got != tt.want {
				t.Errorf("lbIngressIPsEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ── Predicate tests ──

func TestLBIngressChangedPredicate_Create(t *testing.T) {
	pred := lbIngressChangedPredicate{}
	tests := []struct {
		name string
		svc  *corev1.Service
		want bool
	}{
		{
			name: "LB with ingress IP",
			svc:  newLBService("s", "ns", "10.0.0.1"),
			want: true,
		},
		{
			name: "LB without ingress",
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "s"},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
			},
			want: false,
		},
		{
			name: "ClusterIP service",
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "s"},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
			},
			want: false,
		},
		{
			name: "NodePort service",
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "s"},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeNodePort},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.Create(event.CreateEvent{Object: tt.svc})
			if got != tt.want {
				t.Errorf("Create() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLBIngressChangedPredicate_Update(t *testing.T) {
	pred := lbIngressChangedPredicate{}
	tests := []struct {
		name string
		old  *corev1.Service
		new  *corev1.Service
		want bool
	}{
		{
			name: "IP assigned",
			old: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "s"},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
			},
			new:  newLBService("s", "ns", "10.0.0.1"),
			want: true,
		},
		{
			name: "IP unchanged",
			old:  newLBService("s", "ns", "10.0.0.1"),
			new:  newLBService("s", "ns", "10.0.0.1"),
			want: false,
		},
		{
			name: "IP changed",
			old:  newLBService("s", "ns", "10.0.0.1"),
			new:  newLBService("s", "ns", "10.0.0.2"),
			want: true,
		},
		{
			name: "IP removed",
			old:  newLBService("s", "ns", "10.0.0.1"),
			new: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "s"},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
			},
			want: true,
		},
		{
			name: "spec-only change, no ingress change",
			old:  newLBService("s", "ns", "10.0.0.1"),
			new:  newLBService("s", "ns", "10.0.0.1"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.Update(event.UpdateEvent{ObjectOld: tt.old, ObjectNew: tt.new})
			if got != tt.want {
				t.Errorf("Update() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLBIngressChangedPredicate_Delete(t *testing.T) {
	pred := lbIngressChangedPredicate{}
	tests := []struct {
		name string
		svc  *corev1.Service
		want bool
	}{
		{
			name: "LB with ingress IP",
			svc:  newLBService("s", "ns", "10.0.0.1"),
			want: true,
		},
		{
			name: "LB without ingress",
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "s"},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
			},
			want: false,
		},
		{
			name: "non-LB service",
			svc: &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "s"},
				Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.Delete(event.DeleteEvent{Object: tt.svc})
			if got != tt.want {
				t.Errorf("Delete() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ── Reconcile: no-op patch guard ──

func TestReconcile_NoOpWhenAllocationsUnchanged(t *testing.T) {
	pool := newPool("pool", "default", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{{CIDR: "10.0.0.0/28"}})
	pool.Status.InfrastructureAllocations = []butlerv1alpha1.InfrastructureAllocation{
		{
			IP:     "10.0.0.5",
			Source: SourceMetalLB,
			ServiceRef: &butlerv1alpha1.NamespacedObjectReference{
				Name: "svc", Namespace: "default",
			},
		},
	}

	_, capture := reconcileWith(t, pool, newLBService("svc", "default", "10.0.0.5"))

	if capture.called {
		t.Error("expected no status patch when allocations are unchanged")
	}
}

func TestReconcile_NoOpWhenBothEmpty(t *testing.T) {
	pool := newPool("pool", "default", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{{CIDR: "10.0.0.0/28"}})

	// ClusterIP service produces no allocations
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
	}

	_, capture := reconcileWith(t, pool,
		&corev1.Service{
			ObjectMeta: svc.ObjectMeta,
			Spec:       svc.Spec,
		})

	if capture.called {
		t.Error("expected no status patch when both desired and current are empty")
	}
}

// ── Reconcile: pool edge cases ──

func TestReconcile_PoolNotFound(t *testing.T) {
	s := testScheme()
	fc := fake.NewClientBuilder().WithScheme(s).Build()
	r := &InfraAllocationReconciler{Client: fc, Scheme: s}

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("expected nil error for not-found, got: %v", err)
	}
	if result.Requeue {
		t.Error("expected no requeue for not-found pool")
	}
}

func TestReconcile_PoolDeleting(t *testing.T) {
	now := metav1.Now()
	pool := newPool("pool", "default", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{{CIDR: "10.0.0.0/28"}})
	pool.DeletionTimestamp = &now
	pool.Finalizers = []string{"test-finalizer"}

	_, capture := reconcileWith(t, pool, newLBService("svc", "default", "10.0.0.5"))

	if capture.called {
		t.Error("expected no status patch for deleting pool")
	}
}

func TestReconcile_NoReservedRanges(t *testing.T) {
	pool := newPool("pool", "default", "10.0.0.0/24", nil)

	_, capture := reconcileWith(t, pool, newLBService("svc", "default", "10.0.0.5"))

	if capture.called {
		t.Error("expected no status patch when pool has no reserved ranges")
	}
}

func TestReconcile_ClearsAllocationsWhenReservedRangesRemoved(t *testing.T) {
	pool := newPool("pool", "default", "10.0.0.0/24", nil)
	pool.Status.InfrastructureAllocations = []butlerv1alpha1.InfrastructureAllocation{
		{IP: "10.0.0.5", Source: SourceMetalLB},
	}

	_, capture := reconcileWith(t, pool)

	if !capture.called {
		t.Fatal("expected status patch to clear stale allocations")
	}
	if capture.pool != nil && len(capture.pool.Status.InfrastructureAllocations) != 0 {
		t.Errorf("expected empty allocations, got %d", len(capture.pool.Status.InfrastructureAllocations))
	}
}

// ── Reconcile: IP-in-CIDR membership ──

func TestReconcile_IPInReservedRange(t *testing.T) {
	pool := newPool("pool", "default", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{{CIDR: "10.0.0.0/28"}})

	_, capture := reconcileWith(t, pool, newLBService("svc", "default", "10.0.0.5"))

	if !capture.called {
		t.Fatal("expected status patch")
	}
	allocs := capture.pool.Status.InfrastructureAllocations
	if len(allocs) != 1 {
		t.Fatalf("expected 1 allocation, got %d", len(allocs))
	}
	if allocs[0].IP != "10.0.0.5" {
		t.Errorf("IP = %q, want 10.0.0.5", allocs[0].IP)
	}
	if allocs[0].Source != SourceMetalLB {
		t.Errorf("Source = %q, want %q", allocs[0].Source, SourceMetalLB)
	}
	if allocs[0].ServiceRef == nil {
		t.Fatal("expected ServiceRef to be set")
	}
	if allocs[0].ServiceRef.Name != "svc" || allocs[0].ServiceRef.Namespace != "default" {
		t.Errorf("ServiceRef = %v, want default/svc", allocs[0].ServiceRef)
	}
}

func TestReconcile_IPOutsideReservedRange(t *testing.T) {
	pool := newPool("pool", "default", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{{CIDR: "10.0.0.0/28"}}) // .0-.15

	// IP 10.0.0.100 is outside the /28 range
	_, capture := reconcileWith(t, pool, newLBService("svc", "default", "10.0.0.100"))

	if capture.called {
		t.Error("expected no status patch for IP outside reserved range")
	}
}

func TestReconcile_MultipleRangesAndServices(t *testing.T) {
	pool := newPool("pool", "default", "10.0.0.0/16",
		[]butlerv1alpha1.ReservedRange{
			{CIDR: "10.0.0.0/28"},   // .0-.15
			{CIDR: "10.0.1.0/28"},   // .0-.15 in second octet
		})

	svcs := []*corev1.Service{
		newLBService("svc-a", "ns1", "10.0.0.5"),   // in first range
		newLBService("svc-b", "ns2", "10.0.1.10"),  // in second range
		newLBService("svc-c", "ns3", "10.0.2.1"),   // outside both ranges
	}

	_, capture := reconcileWith(t, pool, svcs...)

	if !capture.called {
		t.Fatal("expected status patch")
	}
	allocs := capture.pool.Status.InfrastructureAllocations
	if len(allocs) != 2 {
		t.Fatalf("expected 2 allocations, got %d", len(allocs))
	}
	// Sorted numerically: 10.0.0.5 < 10.0.1.10
	if allocs[0].IP != "10.0.0.5" {
		t.Errorf("allocs[0].IP = %q, want 10.0.0.5", allocs[0].IP)
	}
	if allocs[1].IP != "10.0.1.10" {
		t.Errorf("allocs[1].IP = %q, want 10.0.1.10", allocs[1].IP)
	}
}

func TestReconcile_NumericIPSort(t *testing.T) {
	pool := newPool("pool", "default", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{{CIDR: "10.0.0.0/24"}})

	// IPs that would sort wrong lexicographically: "10.0.0.10" < "10.0.0.2"
	svcs := []*corev1.Service{
		newLBService("svc-a", "ns", "10.0.0.10"),
		newLBService("svc-b", "ns", "10.0.0.2"),
		newLBService("svc-c", "ns", "10.0.0.1"),
	}

	_, capture := reconcileWith(t, pool, svcs...)

	if !capture.called {
		t.Fatal("expected status patch")
	}
	allocs := capture.pool.Status.InfrastructureAllocations
	if len(allocs) != 3 {
		t.Fatalf("expected 3 allocations, got %d", len(allocs))
	}
	wantOrder := []string{"10.0.0.1", "10.0.0.2", "10.0.0.10"}
	for i, want := range wantOrder {
		if allocs[i].IP != want {
			t.Errorf("allocs[%d].IP = %q, want %q (numeric sort)", i, allocs[i].IP, want)
		}
	}
}

func TestReconcile_ClusterIPServiceIgnored(t *testing.T) {
	pool := newPool("pool", "default", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{{CIDR: "10.0.0.0/24"}})

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
	}

	_, capture := reconcileWith(t, pool, svc)

	if capture.called {
		t.Error("expected no patch for ClusterIP service")
	}
}

func TestReconcile_LBServiceWithNoIngress(t *testing.T) {
	pool := newPool("pool", "default", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{{CIDR: "10.0.0.0/24"}})

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
		// No status.loadBalancer.ingress
	}

	_, capture := reconcileWith(t, pool, svc)

	if capture.called {
		t.Error("expected no patch for LB service without ingress IP")
	}
}

func TestReconcile_ClearsAllocationsWhenServiceRemoved(t *testing.T) {
	pool := newPool("pool", "default", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{{CIDR: "10.0.0.0/28"}})
	pool.Status.InfrastructureAllocations = []butlerv1alpha1.InfrastructureAllocation{
		{
			IP:     "10.0.0.5",
			Source: SourceMetalLB,
			ServiceRef: &butlerv1alpha1.NamespacedObjectReference{
				Name: "deleted-svc", Namespace: "default",
			},
		},
	}

	// No services exist. The deleted service's allocation should be cleared.
	_, capture := reconcileWith(t, pool)

	if !capture.called {
		t.Fatal("expected status patch to clear stale allocation")
	}
	if capture.pool != nil && len(capture.pool.Status.InfrastructureAllocations) != 0 {
		t.Errorf("expected 0 allocations after service removal, got %d",
			len(capture.pool.Status.InfrastructureAllocations))
	}
}

// TestReconcile_ServiceIPChanged exercises the IP-move scenario: a
// LoadBalancer Service changes its assigned IP from one reserved CIDR to a
// different one. The mapper enqueues all NetworkPools (see
// mapServiceToNetworkPools), and each pool's Reconcile recomputes desired
// allocations from current Service state. The old IP entry drops and the
// new IP entry appears.
//
// Reconcilers that watch secondary K8s resources with mutable identity
// (Service IPs, claim refs, owner references, etc.) should test this
// scenario explicitly. Create and Delete events alone are not sufficient
// to exercise the comparison path on Update.
func TestReconcile_ServiceIPChanged(t *testing.T) {
	// Pool with two distinct reserved CIDRs.
	pool := newPool("pool", "default", "10.0.0.0/16",
		[]butlerv1alpha1.ReservedRange{
			{CIDR: "10.0.0.0/28"}, // range A: .0-.15
			{CIDR: "10.0.1.0/28"}, // range B: .0-.15
		})

	svc := newLBService("moving-svc", "default", "10.0.0.5") // starts in range A

	s := testScheme()
	fc := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(pool, svc).
		WithStatusSubresource(&butlerv1alpha1.NetworkPool{}).
		Build()

	var captures []patchCapture
	cl := interceptor.NewClient(fc, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if p, ok := obj.(*butlerv1alpha1.NetworkPool); ok {
				captures = append(captures, patchCapture{called: true, pool: p.DeepCopy()})
			}
			return nil
		},
	})

	r := &InfraAllocationReconciler{Client: cl, Scheme: s}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "pool", Namespace: "default"}}

	// First reconcile: Service IP 10.0.0.5 is in range A.
	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("first reconcile error: %v", err)
	}
	if len(captures) != 1 {
		t.Fatalf("expected 1 patch after first reconcile, got %d", len(captures))
	}
	if captures[0].pool.Status.InfrastructureAllocations[0].IP != "10.0.0.5" {
		t.Errorf("first reconcile: IP = %q, want 10.0.0.5",
			captures[0].pool.Status.InfrastructureAllocations[0].IP)
	}

	// Simulate IP move: update the Service in the fake client to use an IP
	// in range B. This models MetalLB reassigning the IP.
	if err := fc.Get(context.Background(), types.NamespacedName{Name: "moving-svc", Namespace: "default"}, svc); err != nil {
		t.Fatalf("failed to get service: %v", err)
	}
	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "10.0.1.3"}}
	if err := fc.Status().Update(context.Background(), svc); err != nil {
		t.Fatalf("failed to update service status: %v", err)
	}

	// Update the pool's current status to match what the first reconcile
	// wrote, so the no-op guard sees the stale state.
	if err := fc.Get(context.Background(), types.NamespacedName{Name: "pool", Namespace: "default"}, pool); err != nil {
		t.Fatalf("failed to get pool: %v", err)
	}
	pool.Status.InfrastructureAllocations = captures[0].pool.Status.InfrastructureAllocations
	if err := fc.Status().Update(context.Background(), pool); err != nil {
		t.Fatalf("failed to update pool status: %v", err)
	}

	// Second reconcile: Service IP moved to 10.0.1.3 in range B.
	captures = nil
	_, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("second reconcile error: %v", err)
	}
	if len(captures) != 1 {
		t.Fatalf("expected 1 patch after second reconcile (IP changed), got %d", len(captures))
	}
	allocs := captures[0].pool.Status.InfrastructureAllocations
	if len(allocs) != 1 {
		t.Fatalf("expected 1 allocation after IP move, got %d", len(allocs))
	}
	if allocs[0].IP != "10.0.1.3" {
		t.Errorf("after IP move: IP = %q, want 10.0.1.3", allocs[0].IP)
	}
	if allocs[0].ServiceRef.Name != "moving-svc" {
		t.Errorf("after IP move: ServiceRef.Name = %q, want moving-svc", allocs[0].ServiceRef.Name)
	}
}

func TestReconcile_SourceConstant(t *testing.T) {
	if SourceMetalLB != "metallb" {
		t.Errorf("SourceMetalLB = %q, want %q", SourceMetalLB, "metallb")
	}
	if SourceNode != "node" {
		t.Errorf("SourceNode = %q, want %q", SourceNode, "node")
	}
}

// ── Node helpers ──

func newNode(name, ip string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: ip},
			},
		},
	}
}

func newPoolWithTenant(name, ns, cidr string, reserved []butlerv1alpha1.ReservedRange, tenantRanges []butlerv1alpha1.AllocationRange) *butlerv1alpha1.NetworkPool {
	pool := newPool(name, ns, cidr, reserved)
	if len(tenantRanges) > 0 {
		pool.Spec.TenantAllocation = &butlerv1alpha1.TenantAllocationConfig{
			Ranges: tenantRanges,
		}
	}
	return pool
}

// reconcileWithNodes is like reconcileWith but also accepts Nodes.
func reconcileWithNodes(t *testing.T, pool *butlerv1alpha1.NetworkPool, services []*corev1.Service, nodes []*corev1.Node) (ctrl.Result, patchCapture) {
	t.Helper()
	s := testScheme()
	objs := []client.Object{pool}
	for _, svc := range services {
		objs = append(objs, svc)
	}
	for _, node := range nodes {
		objs = append(objs, node)
	}
	fc := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&butlerv1alpha1.NetworkPool{}).
		Build()

	var capture patchCapture
	cl := interceptor.NewClient(fc, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			capture.called = true
			if p, ok := obj.(*butlerv1alpha1.NetworkPool); ok {
				capture.pool = p.DeepCopy()
			}
			return nil
		},
	})

	r := &InfraAllocationReconciler{Client: cl, Scheme: s}
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: pool.Name, Namespace: pool.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	return result, capture
}

// ── Node tracking tests ──

func TestReconcile_NodeInPoolCIDR(t *testing.T) {
	pool := newPoolWithTenant("test-pool", "butler-system", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{
			{CIDR: "10.0.0.1/32"},
		},
		[]butlerv1alpha1.AllocationRange{
			{Start: "10.0.0.32", End: "10.0.0.63"},
		},
	)
	nodes := []*corev1.Node{
		newNode("worker-01", "10.0.0.80"),
		newNode("worker-02", "10.0.0.81"),
	}

	_, capture := reconcileWithNodes(t, pool, nil, nodes)
	if !capture.called {
		t.Fatal("expected patch to be called")
	}
	allocs := capture.pool.Status.InfrastructureAllocations
	if len(allocs) != 2 {
		t.Fatalf("expected 2 allocations, got %d", len(allocs))
	}
	for _, a := range allocs {
		if a.Source != SourceNode {
			t.Errorf("expected source %q, got %q", SourceNode, a.Source)
		}
		if a.NodeRef == nil {
			t.Errorf("expected nodeRef for IP %s", a.IP)
		}
	}
	if capture.pool.Status.InfrastructureConsumedIPs != 2 {
		t.Errorf("InfrastructureConsumedIPs = %d, want 2", capture.pool.Status.InfrastructureConsumedIPs)
	}
}

func TestReconcile_NodeInTenantRange_Excluded(t *testing.T) {
	pool := newPoolWithTenant("test-pool", "butler-system", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{
			{CIDR: "10.0.0.1/32"},
		},
		[]butlerv1alpha1.AllocationRange{
			{Start: "10.0.0.32", End: "10.0.0.63"},
		},
	)
	// Node IP falls within tenant range -- should be excluded
	nodes := []*corev1.Node{
		newNode("tenant-node", "10.0.0.40"),
	}

	_, capture := reconcileWithNodes(t, pool, nil, nodes)
	if capture.called {
		t.Fatal("expected no patch (node IP in tenant range should be excluded)")
	}
}

func TestReconcile_NodeAndServiceOverlap(t *testing.T) {
	pool := newPoolWithTenant("test-pool", "butler-system", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{
			{CIDR: "10.0.0.1/32"},
		},
		[]butlerv1alpha1.AllocationRange{
			{Start: "10.0.0.32", End: "10.0.0.63"},
		},
	)
	// Both a Service and a Node claim 10.0.0.1
	services := []*corev1.Service{
		newLBService("traefik", "traefik", "10.0.0.1"),
	}
	nodes := []*corev1.Node{
		newNode("cp-01", "10.0.0.1"),
	}

	_, capture := reconcileWithNodes(t, pool, services, nodes)
	if !capture.called {
		t.Fatal("expected patch to be called")
	}
	allocs := capture.pool.Status.InfrastructureAllocations
	if len(allocs) != 1 {
		t.Fatalf("expected 1 allocation (merged), got %d", len(allocs))
	}
	a := allocs[0]
	if a.IP != "10.0.0.1" {
		t.Errorf("IP = %q, want 10.0.0.1", a.IP)
	}
	if a.Source != SourceMetalLB {
		t.Errorf("Source = %q, want %q (Service takes precedence)", a.Source, SourceMetalLB)
	}
	if a.ServiceRef == nil || a.ServiceRef.Name != "traefik" {
		t.Errorf("expected ServiceRef to reference traefik")
	}
	if a.NodeRef == nil || a.NodeRef.Name != "cp-01" {
		t.Errorf("expected NodeRef to reference cp-01")
	}
}

func TestReconcile_NodeOutsidePoolCIDR_Excluded(t *testing.T) {
	pool := newPoolWithTenant("test-pool", "butler-system", "10.0.0.0/24",
		[]butlerv1alpha1.ReservedRange{
			{CIDR: "10.0.0.1/32"},
		},
		nil,
	)
	// Node IP not in pool CIDR at all
	nodes := []*corev1.Node{
		newNode("other-node", "192.168.1.100"),
	}

	_, capture := reconcileWithNodes(t, pool, nil, nodes)
	if capture.called {
		t.Fatal("expected no patch (node IP outside pool CIDR)")
	}
}

// ── Node predicate tests ──

func TestNodeIPChangedPredicate_Create(t *testing.T) {
	pred := nodeIPChangedPredicate{}
	tests := []struct {
		name string
		node *corev1.Node
		want bool
	}{
		{
			name: "node with IP",
			node: newNode("n", "10.0.0.1"),
			want: true,
		},
		{
			name: "node without addresses",
			node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n"}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.Create(event.CreateEvent{Object: tt.node})
			if got != tt.want {
				t.Errorf("Create() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeIPChangedPredicate_Update(t *testing.T) {
	pred := nodeIPChangedPredicate{}
	tests := []struct {
		name string
		old  *corev1.Node
		new  *corev1.Node
		want bool
	}{
		{
			name: "IP unchanged",
			old:  newNode("n", "10.0.0.1"),
			new:  newNode("n", "10.0.0.1"),
			want: false,
		},
		{
			name: "IP changed",
			old:  newNode("n", "10.0.0.1"),
			new:  newNode("n", "10.0.0.2"),
			want: true,
		},
		{
			name: "IP added",
			old:  &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n"}},
			new:  newNode("n", "10.0.0.1"),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.Update(event.UpdateEvent{ObjectOld: tt.old, ObjectNew: tt.new})
			if got != tt.want {
				t.Errorf("Update() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeIPChangedPredicate_Delete(t *testing.T) {
	pred := nodeIPChangedPredicate{}
	tests := []struct {
		name string
		node *corev1.Node
		want bool
	}{
		{
			name: "node with IP",
			node: newNode("n", "10.0.0.1"),
			want: true,
		},
		{
			name: "node without IP",
			node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n"}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.Delete(event.DeleteEvent{Object: tt.node})
			if got != tt.want {
				t.Errorf("Delete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractNodeIPs(t *testing.T) {
	tests := []struct {
		name string
		node *corev1.Node
		want []string
	}{
		{
			name: "single IP",
			node: newNode("n", "10.0.0.1"),
			want: []string{"10.0.0.1"},
		},
		{
			name: "no addresses",
			node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n"}},
			want: nil,
		},
		{
			name: "multiple address types",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "n"},
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{
						{Type: corev1.NodeInternalIP, Address: "10.0.0.1"},
						{Type: corev1.NodeExternalIP, Address: "1.2.3.4"},
						{Type: corev1.NodeHostName, Address: "node-1"},
					},
				},
			},
			want: []string{"10.0.0.1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractNodeIPs(tt.node)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("IP[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}
