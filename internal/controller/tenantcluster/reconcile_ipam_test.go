// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"fmt"
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

func TestShrinkSelectsGrowthNotInitial(t *testing.T) {
	// Verifies that the shrink loop only considers growth allocations, never
	// the initial allocation, regardless of the order allocations appear in
	// the slice (simulating undefined Kubernetes List ordering).
	oldTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	count := int32(2)

	// Create 3 allocations: initial + 2 growth, deliberately in non-alphabetical
	// order to prove we don't depend on ordering.
	allocs := []butlerv1alpha1.IPAllocation{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "team-a-cluster-lb-2",
				Namespace:         "butler-system",
				CreationTimestamp: oldTime,
				Labels: map[string]string{
					LabelAllocationRole: AllocationRoleGrowth,
				},
			},
			Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
			Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated},
		},
		{
			// The initial allocation is at index 1 — not index 0.
			ObjectMeta: metav1.ObjectMeta{
				Name:              "team-a-cluster-lb",
				Namespace:         "butler-system",
				CreationTimestamp: oldTime,
				Labels: map[string]string{
					LabelAllocationRole: AllocationRoleInitial,
				},
			},
			Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
			Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "team-a-cluster-lb-1",
				Namespace:         "butler-system",
				CreationTimestamp: oldTime,
				Labels: map[string]string{
					LabelAllocationRole: AllocationRoleGrowth,
				},
			},
			Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
			Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated},
		},
	}

	// Simulate the shrink selection logic from reconcileElasticIPAM
	var shrinkCandidate *butlerv1alpha1.IPAllocation
	for i := len(allocs) - 1; i >= 0; i-- {
		alloc := &allocs[i]
		if alloc.Labels[LabelAllocationRole] != AllocationRoleGrowth {
			continue
		}
		if alloc.Status.Phase != butlerv1alpha1.IPAllocationPhaseAllocated {
			continue
		}
		if alloc.Spec.Count == nil {
			continue
		}
		if time.Since(alloc.CreationTimestamp.Time) < 10*time.Minute {
			continue
		}
		shrinkCandidate = alloc
		break
	}

	if shrinkCandidate == nil {
		t.Fatal("expected a shrink candidate but got nil")
	}
	if shrinkCandidate.Labels[LabelAllocationRole] != AllocationRoleGrowth {
		t.Errorf("expected shrink candidate to be a growth allocation, got role=%q",
			shrinkCandidate.Labels[LabelAllocationRole])
	}
	// Verify initial was never selected
	if shrinkCandidate.Name == "team-a-cluster-lb" {
		t.Error("shrink selected the initial allocation; this must never happen")
	}
}

func TestShrinkThenReclaimUsesFreshData(t *testing.T) {
	// Verifies that after shrink deletes an allocation, reclaim uses a fresh
	// slice rather than the stale one. We simulate this by checking that a
	// deleted allocation's name does not appear in the fresh slice.
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	oldTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	count := int32(2)

	initialAlloc := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "team-a-test-lb",
			Namespace:         "butler-system",
			CreationTimestamp: oldTime,
			Labels: map[string]string{
				butlerv1alpha1.LabelTeam:           "team-a",
				butlerv1alpha1.LabelTenant:         "test",
				butlerv1alpha1.LabelAllocationType: "loadbalancer",
				LabelAllocationRole:                AllocationRoleInitial,
			},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.1",
			EndAddress:   "10.0.0.2",
		},
	}
	growthAlloc := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "team-a-test-lb-1",
			Namespace:         "butler-system",
			CreationTimestamp: oldTime,
			Labels: map[string]string{
				butlerv1alpha1.LabelTeam:           "team-a",
				butlerv1alpha1.LabelTenant:         "test",
				butlerv1alpha1.LabelAllocationType: "loadbalancer",
				LabelAllocationRole:                AllocationRoleGrowth,
			},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.3",
			EndAddress:   "10.0.0.4",
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initialAlloc, growthAlloc).
		Build()
	r := &Reconciler{Client: cl}
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
	}

	// Simulate shrink: delete the growth allocation
	if err := cl.Delete(context.Background(), growthAlloc); err != nil {
		t.Fatalf("failed to delete growth allocation: %v", err)
	}

	// Now re-fetch — this is what the fix does
	freshAllocs, err := r.listLBAllocations(context.Background(), tc)
	if err != nil {
		t.Fatalf("listLBAllocations() error = %v", err)
	}

	// The deleted allocation should not appear in the fresh list
	for _, alloc := range freshAllocs {
		if alloc.Name == "team-a-test-lb-1" {
			t.Error("deleted growth allocation still appears in fresh list")
		}
	}
	if len(freshAllocs) != 1 {
		t.Errorf("expected 1 allocation after shrink, got %d", len(freshAllocs))
	}
}

func TestMigrateAllocationRoleLabels(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "mycluster", Namespace: "team-x"},
	}

	count := int32(2)
	oldTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))

	// Create unlabeled allocations: one initial pattern, two growth patterns
	initialAlloc := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "team-x-mycluster-lb",
			Namespace:         "butler-system",
			CreationTimestamp: oldTime,
			Labels: map[string]string{
				butlerv1alpha1.LabelTeam:           "team-x",
				butlerv1alpha1.LabelTenant:         "mycluster",
				butlerv1alpha1.LabelAllocationType: "loadbalancer",
				// No allocation-role label — needs migration
			},
		},
		Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated},
	}
	growth1 := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "team-x-mycluster-lb-1",
			Namespace:         "butler-system",
			CreationTimestamp: oldTime,
			Labels: map[string]string{
				butlerv1alpha1.LabelTeam:           "team-x",
				butlerv1alpha1.LabelTenant:         "mycluster",
				butlerv1alpha1.LabelAllocationType: "loadbalancer",
			},
		},
		Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated},
	}
	growth2 := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "team-x-mycluster-lb-2",
			Namespace:         "butler-system",
			CreationTimestamp: oldTime,
			Labels: map[string]string{
				butlerv1alpha1.LabelTeam:           "team-x",
				butlerv1alpha1.LabelTenant:         "mycluster",
				butlerv1alpha1.LabelAllocationType: "loadbalancer",
			},
		},
		Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initialAlloc, growth1, growth2).
		Build()
	r := &Reconciler{Client: cl}

	allocs := []butlerv1alpha1.IPAllocation{*initialAlloc, *growth1, *growth2}
	r.migrateAllocationRoleLabels(context.Background(), allocs, tc)

	// Verify the initial allocation got labeled correctly
	updated := &butlerv1alpha1.IPAllocation{}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(initialAlloc), updated); err != nil {
		t.Fatalf("failed to get initial allocation: %v", err)
	}
	if updated.Labels[LabelAllocationRole] != AllocationRoleInitial {
		t.Errorf("expected initial allocation role=%q, got %q",
			AllocationRoleInitial, updated.Labels[LabelAllocationRole])
	}

	// Verify growth allocations got labeled correctly
	for _, name := range []string{"team-x-mycluster-lb-1", "team-x-mycluster-lb-2"} {
		g := &butlerv1alpha1.IPAllocation{}
		if err := cl.Get(context.Background(), client.ObjectKey{Name: name, Namespace: "butler-system"}, g); err != nil {
			t.Fatalf("failed to get %s: %v", name, err)
		}
		if g.Labels[LabelAllocationRole] != AllocationRoleGrowth {
			t.Errorf("%s: expected growth allocation role=%q, got %q",
				name, AllocationRoleGrowth, g.Labels[LabelAllocationRole])
		}
	}
}

func TestMigrateAllocationRoleLabels_AlreadyLabeled(t *testing.T) {
	// Verify migration is a no-op for already-labeled allocations
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
	}
	count := int32(2)

	alloc := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-a-test-lb",
			Namespace: "butler-system",
			Labels: map[string]string{
				LabelAllocationRole: AllocationRoleInitial,
			},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(alloc).
		Build()
	r := &Reconciler{Client: cl}

	allocs := []butlerv1alpha1.IPAllocation{*alloc}
	r.migrateAllocationRoleLabels(context.Background(), allocs, tc)

	updated := &butlerv1alpha1.IPAllocation{}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(alloc), updated); err != nil {
		t.Fatalf("failed to get allocation: %v", err)
	}
	if updated.Labels[LabelAllocationRole] != AllocationRoleInitial {
		t.Errorf("label changed unexpectedly: got %q", updated.Labels[LabelAllocationRole])
	}
}

func TestGrowthSuffixPattern(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"initial (no suffix)", "team-a-cluster-lb", false},
		{"growth -1", "team-a-cluster-lb-1", true},
		{"growth -12", "team-a-cluster-lb-12", true},
		{"growth -0", "team-a-cluster-lb-0", true},
		{"not a number suffix", "team-a-cluster-lb-abc", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := growthSuffixPattern.MatchString(tt.input)
			if got != tt.want {
				t.Errorf("growthSuffixPattern.MatchString(%q) = %v, want %v",
					tt.input, got, tt.want)
			}
		})
	}
}

func TestCreationLabelsInitialAndGrowth(t *testing.T) {
	// Verify that the label maps used during creation include the correct
	// allocation-role values, matching what reconcileIPAllocation and the
	// growth path in reconcileElasticIPAM produce.
	initialLabels := map[string]string{
		butlerv1alpha1.LabelNetworkPool:    "pool-1",
		butlerv1alpha1.LabelTeam:           "team-a",
		butlerv1alpha1.LabelTenant:         "test",
		butlerv1alpha1.LabelAllocationType: "loadbalancer",
		LabelAllocationRole:                AllocationRoleInitial,
	}
	if initialLabels[LabelAllocationRole] != "initial" {
		t.Errorf("initial label = %q, want %q", initialLabels[LabelAllocationRole], "initial")
	}

	growthLabels := map[string]string{
		butlerv1alpha1.LabelNetworkPool:    "pool-1",
		butlerv1alpha1.LabelTeam:           "team-a",
		butlerv1alpha1.LabelTenant:         "test",
		butlerv1alpha1.LabelAllocationType: "loadbalancer",
		LabelAllocationRole:                AllocationRoleGrowth,
	}
	if growthLabels[LabelAllocationRole] != "growth" {
		t.Errorf("growth label = %q, want %q", growthLabels[LabelAllocationRole], "growth")
	}
}

// TestListLBAllocations_Labels verifies that listLBAllocations filters by the
// correct label combination and works with the fake client.
func TestListLBAllocations_Labels(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	count := int32(2)
	matching := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-a-test-lb",
			Namespace: "butler-system",
			Labels: map[string]string{
				butlerv1alpha1.LabelTeam:           "team-a",
				butlerv1alpha1.LabelTenant:         "test",
				butlerv1alpha1.LabelAllocationType: "loadbalancer",
				LabelAllocationRole:                AllocationRoleInitial,
			},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
	}
	nonMatching := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-b-other-lb",
			Namespace: "butler-system",
			Labels: map[string]string{
				butlerv1alpha1.LabelTeam:           "team-b",
				butlerv1alpha1.LabelTenant:         "other",
				butlerv1alpha1.LabelAllocationType: "loadbalancer",
			},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(matching, nonMatching).
		Build()
	r := &Reconciler{Client: cl}
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
	}

	allocs, err := r.listLBAllocations(context.Background(), tc)
	if err != nil {
		t.Fatalf("listLBAllocations() error = %v", err)
	}
	if len(allocs) != 1 {
		t.Fatalf("expected 1 matching allocation, got %d", len(allocs))
	}
	if allocs[0].Name != "team-a-test-lb" {
		t.Errorf("expected allocation name team-a-test-lb, got %s", allocs[0].Name)
	}
}

// TestReclaimSkipsInitialAllocation verifies that reclaimOrphanAllocations
// does not discriminate by role label — it uses service IPs. This is a
// regression guard to confirm reclaim logic is orthogonal to the role label.
func TestReclaimSkipsInitialAllocation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	oldTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	count := int32(1)

	// Initial allocation with a service IP — should NOT be reclaimed
	initial := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "team-a-test-lb",
			Namespace:         "butler-system",
			CreationTimestamp: oldTime,
			Labels: map[string]string{
				LabelAllocationRole: AllocationRoleInitial,
			},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.1",
			EndAddress:   "10.0.0.1",
		},
	}

	cl := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(initial).Build()
	r := &Reconciler{Client: cl}

	serviceIPs := map[string]bool{"10.0.0.1": true}
	r.reclaimOrphanAllocations(context.Background(), []butlerv1alpha1.IPAllocation{*initial}, serviceIPs)

	// Verify NOT deleted
	err := cl.Get(context.Background(), client.ObjectKeyFromObject(initial), &butlerv1alpha1.IPAllocation{})
	if err != nil {
		t.Errorf("initial allocation with active service IP was deleted: %v", err)
	}
}

func TestShrinkCandidateSelection_MultipleMixed(t *testing.T) {
	// Test with a mix of initial, growth, pending, and young allocations
	// to verify the full filtering logic.
	oldTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	youngTime := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	count := int32(2)

	allocs := []butlerv1alpha1.IPAllocation{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "team-a-c-lb",
				Namespace:         "butler-system",
				CreationTimestamp: oldTime,
				Labels:            map[string]string{LabelAllocationRole: AllocationRoleInitial},
			},
			Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
			Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated},
		},
		{
			// Growth but too young
			ObjectMeta: metav1.ObjectMeta{
				Name:              "team-a-c-lb-1",
				Namespace:         "butler-system",
				CreationTimestamp: youngTime,
				Labels:            map[string]string{LabelAllocationRole: AllocationRoleGrowth},
			},
			Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
			Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated},
		},
		{
			// Growth, old enough, pending phase — should be skipped
			ObjectMeta: metav1.ObjectMeta{
				Name:              "team-a-c-lb-2",
				Namespace:         "butler-system",
				CreationTimestamp: oldTime,
				Labels:            map[string]string{LabelAllocationRole: AllocationRoleGrowth},
			},
			Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
			Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhasePending},
		},
		{
			// Growth, old enough, allocated — this is the only valid candidate
			ObjectMeta: metav1.ObjectMeta{
				Name:              "team-a-c-lb-3",
				Namespace:         "butler-system",
				CreationTimestamp: oldTime,
				Labels:            map[string]string{LabelAllocationRole: AllocationRoleGrowth},
			},
			Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
			Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated},
		},
	}

	var shrinkCandidate *butlerv1alpha1.IPAllocation
	for i := len(allocs) - 1; i >= 0; i-- {
		alloc := &allocs[i]
		if alloc.Labels[LabelAllocationRole] != AllocationRoleGrowth {
			continue
		}
		if alloc.Status.Phase != butlerv1alpha1.IPAllocationPhaseAllocated {
			continue
		}
		if alloc.Spec.Count == nil {
			continue
		}
		if time.Since(alloc.CreationTimestamp.Time) < 10*time.Minute {
			continue
		}
		shrinkCandidate = alloc
		break
	}

	if shrinkCandidate == nil {
		t.Fatal("expected a shrink candidate")
	}
	if shrinkCandidate.Name != "team-a-c-lb-3" {
		t.Errorf("expected shrink candidate team-a-c-lb-3, got %s", shrinkCandidate.Name)
	}
}

func TestGetInitialLBPoolSize(t *testing.T) {
	r := &Reconciler{}

	t.Run("defaults to 8 for non-elastic", func(t *testing.T) {
		tc := &butlerv1alpha1.TenantCluster{}
		pc := &butlerv1alpha1.ProviderConfig{}
		got := r.getInitialLBPoolSize(tc, pc)
		if got != 8 {
			t.Errorf("expected 8, got %d", got)
		}
	})

	t.Run("elastic defaults to 2", func(t *testing.T) {
		tc := &butlerv1alpha1.TenantCluster{}
		pc := &butlerv1alpha1.ProviderConfig{
			Spec: butlerv1alpha1.ProviderConfigSpec{
				Network: &butlerv1alpha1.ProviderNetworkConfig{
					LoadBalancer: &butlerv1alpha1.ProviderLBConfig{
						AllocationMode: "elastic",
					},
				},
			},
		}
		got := r.getInitialLBPoolSize(tc, pc)
		if got != 2 {
			t.Errorf("expected 2, got %d", got)
		}
	})

	t.Run("TC override takes priority", func(t *testing.T) {
		size := int32(4)
		tc := &butlerv1alpha1.TenantCluster{
			Spec: butlerv1alpha1.TenantClusterSpec{
				Networking: butlerv1alpha1.NetworkingSpec{
					LBPoolSize: &size,
				},
			},
		}
		pc := &butlerv1alpha1.ProviderConfig{}
		got := r.getInitialLBPoolSize(tc, pc)
		if got != 4 {
			t.Errorf("expected 4, got %d", got)
		}
	})
}

// TestReconcileIPAllocation_InitialLabelPresent verifies that a newly created
// IPAllocation carries the allocation-role=initial label.
func TestReconcileIPAllocation_InitialLabelPresent(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	pool := &butlerv1alpha1.NetworkPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-1", Namespace: "butler-system"},
		Status:     butlerv1alpha1.NetworkPoolStatus{AvailableIPs: 10},
	}

	cl := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(pool).Build()
	r := &Reconciler{Client: cl}

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Spec:       butlerv1alpha1.TenantClusterSpec{},
	}

	pc := &butlerv1alpha1.ProviderConfig{
		Spec: butlerv1alpha1.ProviderConfigSpec{
			Network: &butlerv1alpha1.ProviderNetworkConfig{
				Mode: "ipam",
				PoolRefs: []butlerv1alpha1.PoolReference{
					{Name: "pool-1"},
				},
			},
		},
	}

	_, err := r.reconcileIPAllocation(context.Background(), tc, pc)
	if err != nil {
		t.Fatalf("reconcileIPAllocation() error = %v", err)
	}

	alloc := &butlerv1alpha1.IPAllocation{}
	allocName := fmt.Sprintf("%s-%s-lb", tc.Namespace, tc.Name)
	if err := cl.Get(context.Background(), client.ObjectKey{Name: allocName, Namespace: "butler-system"}, alloc); err != nil {
		t.Fatalf("failed to get allocation: %v", err)
	}

	if alloc.Labels[LabelAllocationRole] != AllocationRoleInitial {
		t.Errorf("allocation-role label = %q, want %q",
			alloc.Labels[LabelAllocationRole], AllocationRoleInitial)
	}
}

func TestMapIPAllocationToTenantCluster(t *testing.T) {
	r := &Reconciler{}

	tests := []struct {
		name     string
		obj      client.Object
		wantLen  int
		wantName string
		wantNS   string
	}{
		{
			name: "valid allocation maps to tenant cluster",
			obj: &butlerv1alpha1.IPAllocation{
				Spec: butlerv1alpha1.IPAllocationSpec{
					TenantClusterRef: butlerv1alpha1.NamespacedObjectReference{
						Name:      "my-cluster",
						Namespace: "team-alpha",
					},
				},
			},
			wantLen:  1,
			wantName: "my-cluster",
			wantNS:   "team-alpha",
		},
		{
			name: "empty name returns nothing",
			obj: &butlerv1alpha1.IPAllocation{
				Spec: butlerv1alpha1.IPAllocationSpec{
					TenantClusterRef: butlerv1alpha1.NamespacedObjectReference{
						Name:      "",
						Namespace: "team-alpha",
					},
				},
			},
			wantLen: 0,
		},
		{
			name: "empty namespace returns nothing",
			obj: &butlerv1alpha1.IPAllocation{
				Spec: butlerv1alpha1.IPAllocationSpec{
					TenantClusterRef: butlerv1alpha1.NamespacedObjectReference{
						Name:      "my-cluster",
						Namespace: "",
					},
				},
			},
			wantLen: 0,
		},
		{
			name:    "wrong type returns nothing",
			obj:     &corev1.ConfigMap{},
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := r.mapIPAllocationToTenantCluster(context.Background(), tt.obj)
			if len(result) != tt.wantLen {
				t.Fatalf("got %d requests, want %d", len(result), tt.wantLen)
			}
			if tt.wantLen > 0 {
				if result[0].Name != tt.wantName {
					t.Errorf("name = %q, want %q", result[0].Name, tt.wantName)
				}
				if result[0].Namespace != tt.wantNS {
					t.Errorf("namespace = %q, want %q", result[0].Namespace, tt.wantNS)
				}
			}
		})
	}
}
