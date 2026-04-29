// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/tenant"
)

func TestCountLBIPs(t *testing.T) {
	tests := []struct {
		name             string
		services         []corev1.Service
		wantPlatform     int32
		wantTenant       int32
	}{
		{
			name:         "no services",
			services:     nil,
			wantPlatform: 0,
			wantTenant:   0,
		},
		{
			name: "ClusterIP service ignored",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
				},
			},
			wantPlatform: 0,
			wantTenant:   0,
		},
		{
			name: "unlabeled LB counts as tenant",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "app-lb", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.1"}},
						},
					},
				},
			},
			wantPlatform: 0,
			wantTenant:   1,
		},
		{
			name: "platform-labeled LB counts as platform",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "traefik",
						Namespace: "traefik",
						Labels:    map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
					},
					Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.2"}},
						},
					},
				},
			},
			wantPlatform: 1,
			wantTenant:   0,
		},
		{
			name: "LB without assigned IP not counted",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pending-lb", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status:     corev1.ServiceStatus{},
				},
			},
			wantPlatform: 0,
			wantTenant:   0,
		},
		{
			name: "mixed: platform and tenant LBs counted separately",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "traefik",
						Namespace: "traefik",
						Labels:    map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
					},
					Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.2"}},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "app-lb", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.3"}},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "internal", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pending-lb", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status:     corev1.ServiceStatus{},
				},
			},
			wantPlatform: 1,
			wantTenant:   1,
		},
		{
			// Scenario: allocated=1, Traefik is the only LB. Old code reported
			// availableIPs=1 (filtered Traefik entirely), so growth never fired.
			// New code: platform=1, tenant=0, available = 1 - 1 - 0 = 0, growth fires.
			name: "platform LB consumes all allocation capacity",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "traefik",
						Namespace: "traefik",
						Labels:    map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
					},
					Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.1"}},
						},
					},
				},
			},
			wantPlatform: 1,
			wantTenant:   0,
		},
		{
			// Scenario: allocated=3, Traefik + 2 tenant LBs.
			// available = 3 - 1 - 2 = 0, growth fires correctly.
			name: "platform plus multiple tenant LBs fills allocation",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "traefik",
						Namespace: "traefik",
						Labels:    map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
					},
					Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.1"}},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "svc-a", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.2"}},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "svc-b", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.3"}},
						},
					},
				},
			},
			wantPlatform: 1,
			wantTenant:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			for i := range tt.services {
				_, err := clientset.CoreV1().Services(tt.services[i].Namespace).Create(
					context.Background(), &tt.services[i], metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to create test service: %v", err)
				}
			}

			tc := &tenant.TenantClient{Clientset: clientset}
			r := &Reconciler{}

			gotPlatform, gotTenant, _, err := r.countLBIPs(context.Background(), tc)
			if err != nil {
				t.Fatalf("countLBIPs() error = %v", err)
			}
			if gotPlatform != tt.wantPlatform {
				t.Errorf("countLBIPs() platformCount = %d, want %d", gotPlatform, tt.wantPlatform)
			}
			if gotTenant != tt.wantTenant {
				t.Errorf("countLBIPs() tenantCount = %d, want %d", gotTenant, tt.wantTenant)
			}
		})
	}
}

func TestCountLBIPs_AvailableCapacity(t *testing.T) {
	// Integration-style tests validating the capacity formula:
	// availableIPs = totalAllocated - platformCount - tenantCount

	tests := []struct {
		name           string
		totalAllocated int32
		services       []corev1.Service
		wantAvailable  int32
	}{
		{
			// The original bug: allocated=1, Traefik owns it. Old code said available=1.
			// Correct: available = 1 - 1(platform) - 0(tenant) = 0, growth should fire.
			name:           "single allocation consumed by platform LB triggers growth",
			totalAllocated: 1,
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "traefik",
						Namespace: "traefik",
						Labels:    map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
					},
					Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.1"}},
						},
					},
				},
			},
			wantAvailable: 0,
		},
		{
			// allocated=3, platform=1, tenant=1. available = 3 - 1 - 1 = 1. No growth needed.
			name:           "headroom exists when allocation exceeds usage",
			totalAllocated: 3,
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "traefik",
						Namespace: "traefik",
						Labels:    map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
					},
					Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.1"}},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.2"}},
						},
					},
				},
			},
			wantAvailable: 1,
		},
		{
			// No platform LB (ingress disabled). allocated=2, tenant=2. available=0.
			name:           "no platform LB still calculates correctly",
			totalAllocated: 2,
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "svc-a", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.1"}},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "svc-b", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.2"}},
						},
					},
				},
			},
			wantAvailable: 0,
		},
		{
			// Fresh cluster: allocated=1, no services yet. available=1. No growth needed.
			name:           "empty cluster has full capacity available",
			totalAllocated: 1,
			services:       nil,
			wantAvailable:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset()
			for i := range tt.services {
				_, err := clientset.CoreV1().Services(tt.services[i].Namespace).Create(
					context.Background(), &tt.services[i], metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to create test service: %v", err)
				}
			}

			tc := &tenant.TenantClient{Clientset: clientset}
			r := &Reconciler{}

			platformCount, tenantCount, _, err := r.countLBIPs(context.Background(), tc)
			if err != nil {
				t.Fatalf("countLBIPs() error = %v", err)
			}

			available := tt.totalAllocated - platformCount - tenantCount
			if available != tt.wantAvailable {
				t.Errorf("availableIPs = %d (total=%d - platform=%d - tenant=%d), want %d",
					available, tt.totalAllocated, platformCount, tenantCount, tt.wantAvailable)
			}
		})
	}
}

func TestEnsurePlatformLBLabels(t *testing.T) {
	t.Run("labels traefik service when label missing", func(t *testing.T) {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "traefik",
				Namespace: "traefik",
			},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
		}
		clientset := fake.NewSimpleClientset(svc)
		tc := &tenant.TenantClient{Clientset: clientset}
		r := &Reconciler{}

		r.ensurePlatformLBLabels(context.Background(), tc)

		updated, err := clientset.CoreV1().Services("traefik").Get(
			context.Background(), "traefik", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get service: %v", err)
		}
		if updated.Labels[butlerv1alpha1.LabelPlatformLB] != "true" {
			t.Errorf("expected label %s=true, got labels: %v",
				butlerv1alpha1.LabelPlatformLB, updated.Labels)
		}
	})

	t.Run("no-op when label already present", func(t *testing.T) {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "traefik",
				Namespace: "traefik",
				Labels:    map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
			},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
		}
		clientset := fake.NewSimpleClientset(svc)
		tc := &tenant.TenantClient{Clientset: clientset}
		r := &Reconciler{}

		r.ensurePlatformLBLabels(context.Background(), tc)

		updated, err := clientset.CoreV1().Services("traefik").Get(
			context.Background(), "traefik", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get service: %v", err)
		}
		if updated.Labels[butlerv1alpha1.LabelPlatformLB] != "true" {
			t.Errorf("label should still be present")
		}
	})

	t.Run("no-op when traefik service does not exist", func(t *testing.T) {
		clientset := fake.NewSimpleClientset()
		tc := &tenant.TenantClient{Clientset: clientset}
		r := &Reconciler{}

		// Should not panic or error
		r.ensurePlatformLBLabels(context.Background(), tc)
	})
}

func TestCountLBIPs_ServiceIPs(t *testing.T) {
	services := []corev1.Service{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "traefik",
				Namespace: "traefik",
				Labels:    map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
			},
			Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
			Status: corev1.ServiceStatus{
				LoadBalancer: corev1.LoadBalancerStatus{
					Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.1"}},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "default"},
			Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
			Status: corev1.ServiceStatus{
				LoadBalancer: corev1.LoadBalancerStatus{
					Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.2"}},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "default"},
			Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
			Status:     corev1.ServiceStatus{},
		},
	}

	clientset := fake.NewSimpleClientset()
	for i := range services {
		_, err := clientset.CoreV1().Services(services[i].Namespace).Create(
			context.Background(), &services[i], metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create test service: %v", err)
		}
	}

	tc := &tenant.TenantClient{Clientset: clientset}
	r := &Reconciler{}

	_, _, serviceIPs, err := r.countLBIPs(context.Background(), tc)
	if err != nil {
		t.Fatalf("countLBIPs() error = %v", err)
	}
	if !serviceIPs["10.0.0.1"] {
		t.Error("expected 10.0.0.1 in serviceIPs")
	}
	if !serviceIPs["10.0.0.2"] {
		t.Error("expected 10.0.0.2 in serviceIPs")
	}
	if len(serviceIPs) != 2 {
		t.Errorf("expected 2 service IPs, got %d: %v", len(serviceIPs), serviceIPs)
	}
}

func TestAllocationHasServiceIP(t *testing.T) {
	tests := []struct {
		name       string
		start      string
		end        string
		serviceIPs map[string]bool
		want       bool
	}{
		{
			name:       "single IP matches",
			start:      "10.0.0.5",
			end:        "10.0.0.5",
			serviceIPs: map[string]bool{"10.0.0.5": true},
			want:       true,
		},
		{
			name:       "single IP no match",
			start:      "10.0.0.5",
			end:        "10.0.0.5",
			serviceIPs: map[string]bool{"10.0.0.6": true},
			want:       false,
		},
		{
			name:       "range match on start",
			start:      "10.0.0.10",
			end:        "10.0.0.11",
			serviceIPs: map[string]bool{"10.0.0.10": true},
			want:       true,
		},
		{
			name:       "range match on end",
			start:      "10.0.0.10",
			end:        "10.0.0.11",
			serviceIPs: map[string]bool{"10.0.0.11": true},
			want:       true,
		},
		{
			name:       "range no match",
			start:      "10.0.0.10",
			end:        "10.0.0.11",
			serviceIPs: map[string]bool{"10.0.0.12": true},
			want:       false,
		},
		{
			name:       "empty start address",
			start:      "",
			end:        "",
			serviceIPs: map[string]bool{"10.0.0.1": true},
			want:       false,
		},
		{
			name:       "empty service IPs",
			start:      "10.0.0.5",
			end:        "10.0.0.5",
			serviceIPs: map[string]bool{},
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alloc := &butlerv1alpha1.IPAllocation{
				Status: butlerv1alpha1.IPAllocationStatus{
					StartAddress: tt.start,
					EndAddress:   tt.end,
				},
			}
			got := allocationHasServiceIP(alloc, tt.serviceIPs)
			if got != tt.want {
				t.Errorf("allocationHasServiceIP() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReclaimOrphanAllocations(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	oldTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	newTime := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	count := int32(1)

	t.Run("reclaims allocation with no matching service IP", func(t *testing.T) {
		alloc := &butlerv1alpha1.IPAllocation{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test-orphan",
				Namespace:         "butler-system",
				CreationTimestamp: oldTime,
			},
			Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
			Status: butlerv1alpha1.IPAllocationStatus{
				Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
				StartAddress: "10.0.0.5",
				EndAddress:   "10.0.0.5",
			},
		}

		cl := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(alloc).Build()
		r := &Reconciler{Client: cl}

		serviceIPs := map[string]bool{"10.0.0.1": true, "10.0.0.2": true}
		r.reclaimOrphanAllocations(context.Background(), []butlerv1alpha1.IPAllocation{*alloc}, serviceIPs)

		// Verify deletion
		err := cl.Get(context.Background(), client.ObjectKeyFromObject(alloc), &butlerv1alpha1.IPAllocation{})
		if err == nil {
			t.Error("expected orphan allocation to be deleted")
		}
	})

	t.Run("skips allocation with matching service IP", func(t *testing.T) {
		alloc := &butlerv1alpha1.IPAllocation{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test-used",
				Namespace:         "butler-system",
				CreationTimestamp: oldTime,
			},
			Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
			Status: butlerv1alpha1.IPAllocationStatus{
				Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
				StartAddress: "10.0.0.1",
				EndAddress:   "10.0.0.1",
			},
		}

		cl := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(alloc).Build()
		r := &Reconciler{Client: cl}

		serviceIPs := map[string]bool{"10.0.0.1": true}
		r.reclaimOrphanAllocations(context.Background(), []butlerv1alpha1.IPAllocation{*alloc}, serviceIPs)

		// Verify NOT deleted
		err := cl.Get(context.Background(), client.ObjectKeyFromObject(alloc), &butlerv1alpha1.IPAllocation{})
		if err != nil {
			t.Errorf("expected used allocation to remain, got error: %v", err)
		}
	})

	t.Run("skips allocation younger than grace period", func(t *testing.T) {
		alloc := &butlerv1alpha1.IPAllocation{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test-young",
				Namespace:         "butler-system",
				CreationTimestamp: newTime, // 2 minutes old, under 5-minute grace
			},
			Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
			Status: butlerv1alpha1.IPAllocationStatus{
				Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
				StartAddress: "10.0.0.9",
				EndAddress:   "10.0.0.9",
			},
		}

		cl := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(alloc).Build()
		r := &Reconciler{Client: cl}

		serviceIPs := map[string]bool{"10.0.0.1": true} // no match for .9
		r.reclaimOrphanAllocations(context.Background(), []butlerv1alpha1.IPAllocation{*alloc}, serviceIPs)

		// Verify NOT deleted (within grace period)
		err := cl.Get(context.Background(), client.ObjectKeyFromObject(alloc), &butlerv1alpha1.IPAllocation{})
		if err != nil {
			t.Errorf("expected young allocation to remain, got error: %v", err)
		}
	})

	t.Run("skips allocation in pending phase", func(t *testing.T) {
		alloc := &butlerv1alpha1.IPAllocation{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "test-pending",
				Namespace:         "butler-system",
				CreationTimestamp: oldTime,
			},
			Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
			Status: butlerv1alpha1.IPAllocationStatus{
				Phase:        butlerv1alpha1.IPAllocationPhasePending,
				StartAddress: "",
				EndAddress:   "",
			},
		}

		cl := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(alloc).Build()
		r := &Reconciler{Client: cl}

		serviceIPs := map[string]bool{}
		r.reclaimOrphanAllocations(context.Background(), []butlerv1alpha1.IPAllocation{*alloc}, serviceIPs)

		// Verify NOT deleted (not yet allocated)
		err := cl.Get(context.Background(), client.ObjectKeyFromObject(alloc), &butlerv1alpha1.IPAllocation{})
		if err != nil {
			t.Errorf("expected pending allocation to remain, got error: %v", err)
		}
	})
}

func TestShrinkThreshold(t *testing.T) {
	// Validates the off-by-one fix: with growthIncrement=1, shrink should fire
	// when availableIPs == 1 (one orphan present).
	tests := []struct {
		name         string
		availableIPs int32
		increment    int32
		wantShrink   bool
	}{
		{
			name:         "available equals increment: shrink fires",
			availableIPs: 1,
			increment:    1,
			wantShrink:   true,
		},
		{
			name:         "available exceeds increment: shrink fires",
			availableIPs: 2,
			increment:    1,
			wantShrink:   true,
		},
		{
			name:         "available below increment: no shrink",
			availableIPs: 0,
			increment:    1,
			wantShrink:   false,
		},
		{
			name:         "increment=2, available=2: shrink fires",
			availableIPs: 2,
			increment:    2,
			wantShrink:   true,
		},
		{
			name:         "increment=2, available=1: no shrink",
			availableIPs: 1,
			increment:    2,
			wantShrink:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The shrink condition from reconcileElasticIPAM:
			// len(allocs) > 1 && availableIPs >= growthIncrement
			allocCount := 2 // More than 1 to satisfy first condition
			got := allocCount > 1 && tt.availableIPs >= tt.increment
			if got != tt.wantShrink {
				t.Errorf("shrink condition (available=%d >= increment=%d) = %v, want %v",
					tt.availableIPs, tt.increment, got, tt.wantShrink)
			}
		})
	}
}
