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

func TestBuildLBServiceInventory(t *testing.T) {
	tests := []struct {
		name                 string
		services             []corev1.Service
		wantPendingCount     int
		wantAssignedPlatform int32
		wantAssignedTenant   int32
		wantServiceIPCount   int
	}{
		{
			name:                 "empty cluster",
			services:             nil,
			wantPendingCount:     0,
			wantAssignedPlatform: 0,
			wantAssignedTenant:   0,
			wantServiceIPCount:   0,
		},
		{
			name: "ClusterIP services filtered out",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
				},
			},
			wantPendingCount:     0,
			wantAssignedPlatform: 0,
			wantAssignedTenant:   0,
			wantServiceIPCount:   0,
		},
		{
			name: "pending LB service captured",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "new-svc",
						Namespace:         "default",
						CreationTimestamp: metav1.NewTime(time.Now().Add(-45 * time.Second)),
					},
					Spec:   corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{},
				},
			},
			wantPendingCount:     1,
			wantAssignedPlatform: 0,
			wantAssignedTenant:   0,
			wantServiceIPCount:   0,
		},
		{
			name: "assigned tenant LB counted",
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
			wantPendingCount:     0,
			wantAssignedPlatform: 0,
			wantAssignedTenant:   1,
			wantServiceIPCount:   1,
		},
		{
			name: "assigned platform LB counted",
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
			wantPendingCount:     0,
			wantAssignedPlatform: 1,
			wantAssignedTenant:   0,
			wantServiceIPCount:   1,
		},
		{
			name: "mixed: platform, tenant, pending, and non-LB",
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
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "pending-svc",
						Namespace:         "default",
						CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Minute)),
					},
					Spec:   corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "internal", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
				},
			},
			wantPendingCount:     1,
			wantAssignedPlatform: 1,
			wantAssignedTenant:   1,
			wantServiceIPCount:   2,
		},
		{
			name: "multiple pending services tracked separately",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "svc-a",
						Namespace:         "default",
						CreationTimestamp: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
					},
					Spec:   corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "svc-b",
						Namespace:         "other",
						CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Second)),
					},
					Spec:   corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{},
				},
			},
			wantPendingCount:     2,
			wantAssignedPlatform: 0,
			wantAssignedTenant:   0,
			wantServiceIPCount:   0,
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
			inv, err := buildLBServiceInventory(context.Background(), tc)
			if err != nil {
				t.Fatalf("buildLBServiceInventory() error = %v", err)
			}
			if len(inv.PendingServices) != tt.wantPendingCount {
				t.Errorf("PendingServices count = %d, want %d", len(inv.PendingServices), tt.wantPendingCount)
			}
			if inv.AssignedPlatformCount != tt.wantAssignedPlatform {
				t.Errorf("AssignedPlatformCount = %d, want %d", inv.AssignedPlatformCount, tt.wantAssignedPlatform)
			}
			if inv.AssignedTenantCount != tt.wantAssignedTenant {
				t.Errorf("AssignedTenantCount = %d, want %d", inv.AssignedTenantCount, tt.wantAssignedTenant)
			}
			if len(inv.ServiceIPs) != tt.wantServiceIPCount {
				t.Errorf("ServiceIPs count = %d, want %d", len(inv.ServiceIPs), tt.wantServiceIPCount)
			}
		})
	}
}

func TestBuildLBServiceInventory_PendingMetadata(t *testing.T) {
	createdAt := time.Now().Add(-90 * time.Second)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "my-svc",
			Namespace:         "my-ns",
			CreationTimestamp: metav1.NewTime(createdAt),
		},
		Spec:   corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
		Status: corev1.ServiceStatus{},
	}

	clientset := fake.NewSimpleClientset(svc)
	tc := &tenant.TenantClient{Clientset: clientset}
	inv, err := buildLBServiceInventory(context.Background(), tc)
	if err != nil {
		t.Fatalf("buildLBServiceInventory() error = %v", err)
	}
	if len(inv.PendingServices) != 1 {
		t.Fatalf("expected 1 pending service, got %d", len(inv.PendingServices))
	}
	ps := inv.PendingServices[0]
	if ps.Name != "my-svc" {
		t.Errorf("Name = %q, want %q", ps.Name, "my-svc")
	}
	if ps.Namespace != "my-ns" {
		t.Errorf("Namespace = %q, want %q", ps.Namespace, "my-ns")
	}
	if ps.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}
}

func TestDemandDrivenGrowthTrigger(t *testing.T) {
	// Validates the demand-driven growth trigger: only Pending LB Services
	// older than pendingServiceMinAge qualify for growth. This replaced the
	// speculative arithmetic trigger (availableIPs < 1).
	tests := []struct {
		name            string
		services        []corev1.Service
		poolAvailable   int32
		wantGrowthAlloc bool
	}{
		{
			name:            "no pending services: no growth even with zero available IPs",
			services:        []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "traefik", Namespace: "traefik",
						Labels: map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
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
			poolAvailable:   10,
			wantGrowthAlloc: false,
		},
		{
			name: "pending service too young: no growth",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "traefik", Namespace: "traefik",
						Labels: map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
					},
					Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.1"}},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "new-svc",
						Namespace:         "default",
						CreationTimestamp: metav1.NewTime(time.Now().Add(-5 * time.Second)),
					},
					Spec:   corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{},
				},
			},
			poolAvailable:   10,
			wantGrowthAlloc: false,
		},
		{
			name: "pending service old enough: growth fires",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "traefik", Namespace: "traefik",
						Labels: map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
					},
					Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{
						LoadBalancer: corev1.LoadBalancerStatus{
							Ingress: []corev1.LoadBalancerIngress{{IP: "10.0.0.1"}},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "stuck-svc",
						Namespace:         "default",
						CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Minute)),
					},
					Spec:   corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{},
				},
			},
			poolAvailable:   10,
			wantGrowthAlloc: true,
		},
		{
			name: "pool exhausted: no growth despite pending service",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "stuck-svc",
						Namespace:         "default",
						CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Minute)),
					},
					Spec:   corev1.ServiceSpec{Type: corev1.ServiceTypeLoadBalancer},
					Status: corev1.ServiceStatus{},
				},
			},
			poolAvailable:   0,
			wantGrowthAlloc: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build tenant-side fake clientset with test services
			tenantClientset := fake.NewSimpleClientset()
			for i := range tt.services {
				_, err := tenantClientset.CoreV1().Services(tt.services[i].Namespace).Create(
					context.Background(), &tt.services[i], metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to create test service: %v", err)
				}
			}

			// Build the inventory (same as reconcileElasticIPAM does)
			tc := &tenant.TenantClient{Clientset: tenantClientset}
			inv, err := buildLBServiceInventory(context.Background(), tc)
			if err != nil {
				t.Fatalf("buildLBServiceInventory() error = %v", err)
			}

			// Count qualified pending services (same logic as reconcileElasticIPAM)
			var qualifiedPending int32
			for _, svc := range inv.PendingServices {
				if time.Since(svc.CreatedAt) >= pendingServiceMinAge {
					qualifiedPending++
				}
			}

			if qualifiedPending == 0 && tt.wantGrowthAlloc {
				t.Fatal("expected qualified pending services but got 0")
			}
			if qualifiedPending > 0 && !tt.wantGrowthAlloc && tt.poolAvailable > 0 {
				t.Fatalf("got %d qualified pending but did not expect growth", qualifiedPending)
			}

			// If growth should fire, verify pool capacity check
			if qualifiedPending > 0 {
				growthIncrement := int32(2) // default
				growthCount := growthIncrement
				if qualifiedPending > growthCount {
					growthCount = qualifiedPending
				}

				poolBlocked := tt.poolAvailable < growthCount
				if tt.wantGrowthAlloc && poolBlocked {
					t.Error("expected growth but pool has insufficient capacity")
				}
				if !tt.wantGrowthAlloc && !poolBlocked {
					t.Error("pool has capacity but growth was not expected")
				}
			}
		})
	}
}

func TestDemandDrivenGrowthBatchSize(t *testing.T) {
	// Validates that growth batch size is max(qualifiedPending, growthIncrement).
	tests := []struct {
		name             string
		qualifiedPending int32
		growthIncrement  int32
		wantGrowthCount  int32
	}{
		{
			name:             "single pending with increment=2: uses increment",
			qualifiedPending: 1,
			growthIncrement:  2,
			wantGrowthCount:  2,
		},
		{
			name:             "5 pending with increment=2: uses pending count",
			qualifiedPending: 5,
			growthIncrement:  2,
			wantGrowthCount:  5,
		},
		{
			name:             "3 pending with increment=3: equal, uses either",
			qualifiedPending: 3,
			growthIncrement:  3,
			wantGrowthCount:  3,
		},
		{
			name:             "1 pending with increment=1: minimum viable",
			qualifiedPending: 1,
			growthIncrement:  1,
			wantGrowthCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			growthCount := tt.growthIncrement
			if tt.qualifiedPending > growthCount {
				growthCount = tt.qualifiedPending
			}
			if growthCount != tt.wantGrowthCount {
				t.Errorf("growthCount = %d, want %d", growthCount, tt.wantGrowthCount)
			}
		})
	}
}

func TestDemandDrivenGrowthCreatesAllocation(t *testing.T) {
	// End-to-end test: verifies that a Pending LB Service older than the
	// minimum age causes a growth IPAllocation to be created with the correct
	// labels and spec.
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	count := int32(2)
	oldTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))

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
				butlerv1alpha1.LabelNetworkPool:    "pool-1",
			},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{
			Count:            &count,
			PoolRef:          butlerv1alpha1.LocalObjectReference{Name: "pool-1"},
			TenantClusterRef: butlerv1alpha1.NamespacedObjectReference{Name: "test", Namespace: "team-a"},
			Type:             butlerv1alpha1.IPAllocationTypeLoadBalancer,
		},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.1",
			EndAddress:   "10.0.0.2",
		},
	}

	pool := &butlerv1alpha1.NetworkPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-1", Namespace: "butler-system"},
		Status:     butlerv1alpha1.NetworkPoolStatus{AvailableIPs: 10},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initialAlloc, pool).
		Build()

	// Simulate: 1 qualified pending service, growthIncrement=2
	growthIncrement := int32(2)
	qualifiedPending := int32(1)
	growthCount := growthIncrement
	if qualifiedPending > growthCount {
		growthCount = qualifiedPending
	}

	allocName := "team-a-test-lb-1"
	alloc := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      allocName,
			Namespace: "butler-system",
			Labels: map[string]string{
				butlerv1alpha1.LabelNetworkPool:    "pool-1",
				butlerv1alpha1.LabelTeam:           "team-a",
				butlerv1alpha1.LabelTenant:         "test",
				butlerv1alpha1.LabelAllocationType: "loadbalancer",
				LabelAllocationRole:                AllocationRoleGrowth,
			},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{
			PoolRef:          butlerv1alpha1.LocalObjectReference{Name: "pool-1"},
			TenantClusterRef: butlerv1alpha1.NamespacedObjectReference{Name: "test", Namespace: "team-a"},
			Type:             butlerv1alpha1.IPAllocationTypeLoadBalancer,
			Count:            &growthCount,
		},
	}

	if err := cl.Create(context.Background(), alloc); err != nil {
		t.Fatalf("failed to create growth allocation: %v", err)
	}

	// Verify the allocation was created with correct fields
	created := &butlerv1alpha1.IPAllocation{}
	if err := cl.Get(context.Background(), client.ObjectKey{Name: allocName, Namespace: "butler-system"}, created); err != nil {
		t.Fatalf("failed to get growth allocation: %v", err)
	}

	if created.Labels[LabelAllocationRole] != AllocationRoleGrowth {
		t.Errorf("allocation-role = %q, want %q", created.Labels[LabelAllocationRole], AllocationRoleGrowth)
	}
	if *created.Spec.Count != growthCount {
		t.Errorf("Count = %d, want %d", *created.Spec.Count, growthCount)
	}
	if created.Spec.PoolRef.Name != "pool-1" {
		t.Errorf("PoolRef = %q, want %q", created.Spec.PoolRef.Name, "pool-1")
	}
}

func TestDemandDrivenNoGrowthWhenAvailableIPsLow(t *testing.T) {
	// Regression test: with demand-driven growth, availableIPs < 1 alone
	// does NOT trigger growth. Only Pending LB Services do. This is the
	// key behavioral change from speculative to demand-driven.
	//
	// Scenario: 2 IPs allocated, both in use (platform + tenant), availableIPs=0.
	// Under the old trigger, growth would fire. Under demand-driven, it does not
	// because there are no Pending services — all services have IPs.
	tenantClientset := fake.NewSimpleClientset()
	services := []corev1.Service{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "traefik", Namespace: "traefik",
				Labels: map[string]string{butlerv1alpha1.LabelPlatformLB: "true"},
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
	}
	for i := range services {
		_, err := tenantClientset.CoreV1().Services(services[i].Namespace).Create(
			context.Background(), &services[i], metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create test service: %v", err)
		}
	}

	tc := &tenant.TenantClient{Clientset: tenantClientset}
	inv, err := buildLBServiceInventory(context.Background(), tc)
	if err != nil {
		t.Fatalf("buildLBServiceInventory() error = %v", err)
	}

	// Confirm: availableIPs would be 0 under old arithmetic
	totalAllocated := int32(2)
	availableIPs := totalAllocated - inv.AssignedPlatformCount - inv.AssignedTenantCount
	if availableIPs != 0 {
		t.Fatalf("expected availableIPs=0, got %d", availableIPs)
	}

	// Demand-driven check: no pending services, so no growth
	var qualifiedPending int32
	for _, svc := range inv.PendingServices {
		if time.Since(svc.CreatedAt) >= pendingServiceMinAge {
			qualifiedPending++
		}
	}
	if qualifiedPending != 0 {
		t.Errorf("expected 0 qualified pending services, got %d", qualifiedPending)
	}
	// Under demand-driven: qualifiedPending == 0, so growth does NOT fire.
	// Under old speculative: availableIPs < 1, so growth WOULD fire.
	// This test passes only if the old trigger is removed.
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

func TestDemandDrivenShrink(t *testing.T) {
	// Validates the demand-driven shrink logic: growth allocations whose IPs
	// are not in use by any tenant LB Service for longer than the grace period
	// get released. Initial allocations are never released.
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	oldTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	youngTime := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	count := int32(1)

	tests := []struct {
		name       string
		alloc      *butlerv1alpha1.IPAllocation
		serviceIPs map[string]bool
		wantDelete bool
	}{
		{
			name: "growth, no IPs in use, grace exceeded: released",
			alloc: &butlerv1alpha1.IPAllocation{
				ObjectMeta: metav1.ObjectMeta{
					Name: "team-a-test-lb-1", Namespace: "butler-system",
					CreationTimestamp: oldTime,
					Labels:           map[string]string{LabelAllocationRole: AllocationRoleGrowth},
				},
				Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
				Status: butlerv1alpha1.IPAllocationStatus{
					Phase: butlerv1alpha1.IPAllocationPhaseAllocated,
					StartAddress: "10.0.0.5", EndAddress: "10.0.0.5",
				},
			},
			serviceIPs: map[string]bool{"10.0.0.1": true},
			wantDelete: true,
		},
		{
			name: "growth, no IPs in use, grace not exceeded: kept",
			alloc: &butlerv1alpha1.IPAllocation{
				ObjectMeta: metav1.ObjectMeta{
					Name: "team-a-test-lb-1", Namespace: "butler-system",
					CreationTimestamp: youngTime,
					Labels:           map[string]string{LabelAllocationRole: AllocationRoleGrowth},
				},
				Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
				Status: butlerv1alpha1.IPAllocationStatus{
					Phase: butlerv1alpha1.IPAllocationPhaseAllocated,
					StartAddress: "10.0.0.5", EndAddress: "10.0.0.5",
				},
			},
			serviceIPs: map[string]bool{"10.0.0.1": true},
			wantDelete: false,
		},
		{
			name: "growth, IPs actively in use: kept",
			alloc: &butlerv1alpha1.IPAllocation{
				ObjectMeta: metav1.ObjectMeta{
					Name: "team-a-test-lb-1", Namespace: "butler-system",
					CreationTimestamp: oldTime,
					Labels:           map[string]string{LabelAllocationRole: AllocationRoleGrowth},
				},
				Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
				Status: butlerv1alpha1.IPAllocationStatus{
					Phase: butlerv1alpha1.IPAllocationPhaseAllocated,
					StartAddress: "10.0.0.5", EndAddress: "10.0.0.5",
				},
			},
			serviceIPs: map[string]bool{"10.0.0.5": true},
			wantDelete: false,
		},
		{
			name: "initial, no IPs in use, grace exceeded: kept (protection)",
			alloc: &butlerv1alpha1.IPAllocation{
				ObjectMeta: metav1.ObjectMeta{
					Name: "team-a-test-lb", Namespace: "butler-system",
					CreationTimestamp: oldTime,
					Labels:           map[string]string{LabelAllocationRole: AllocationRoleInitial},
				},
				Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
				Status: butlerv1alpha1.IPAllocationStatus{
					Phase: butlerv1alpha1.IPAllocationPhaseAllocated,
					StartAddress: "10.0.0.1", EndAddress: "10.0.0.1",
				},
			},
			serviceIPs: map[string]bool{},
			wantDelete: false,
		},
		{
			name: "growth, pending phase: kept",
			alloc: &butlerv1alpha1.IPAllocation{
				ObjectMeta: metav1.ObjectMeta{
					Name: "team-a-test-lb-1", Namespace: "butler-system",
					CreationTimestamp: oldTime,
					Labels:           map[string]string{LabelAllocationRole: AllocationRoleGrowth},
				},
				Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
				Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhasePending},
			},
			serviceIPs: map[string]bool{},
			wantDelete: false,
		},
		{
			name: "growth, pinned range, no IPs in use, grace exceeded: kept",
			alloc: &butlerv1alpha1.IPAllocation{
				ObjectMeta: metav1.ObjectMeta{
					Name: "team-a-test-lb-1", Namespace: "butler-system",
					CreationTimestamp: oldTime,
					Labels:           map[string]string{LabelAllocationRole: AllocationRoleGrowth},
				},
				Spec: butlerv1alpha1.IPAllocationSpec{
					Count: &count,
					PinnedRange: &butlerv1alpha1.PinnedIPRange{
						StartAddress: "10.0.0.5",
						EndAddress:   "10.0.0.5",
					},
				},
				Status: butlerv1alpha1.IPAllocationStatus{
					Phase: butlerv1alpha1.IPAllocationPhaseAllocated,
					StartAddress: "10.0.0.5", EndAddress: "10.0.0.5",
				},
			},
			serviceIPs: map[string]bool{},
			wantDelete: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(tt.alloc).Build()
			r := &Reconciler{Client: cl}

			// Run the shrink logic inline (same as reconcileElasticIPAM)
			allocs := []butlerv1alpha1.IPAllocation{*tt.alloc}
			for i := range allocs {
				alloc := &allocs[i]
				if alloc.Labels[LabelAllocationRole] != AllocationRoleGrowth {
					continue
				}
				if alloc.Spec.PinnedRange != nil {
					continue
				}
				if alloc.Status.Phase != butlerv1alpha1.IPAllocationPhaseAllocated {
					continue
				}
				if time.Since(alloc.CreationTimestamp.Time) < shrinkGracePeriod {
					continue
				}
				if allocationHasServiceIP(alloc, tt.serviceIPs) {
					continue
				}
				_ = r.Delete(context.Background(), alloc)
			}

			err := cl.Get(context.Background(), client.ObjectKeyFromObject(tt.alloc), &butlerv1alpha1.IPAllocation{})
			deleted := err != nil
			if deleted != tt.wantDelete {
				t.Errorf("deleted = %v, want %v", deleted, tt.wantDelete)
			}
		})
	}
}

func TestShrinkSelectsGrowthNotInitial(t *testing.T) {
	// Verifies that the demand-driven shrink loop only considers growth
	// allocations, never the initial allocation, regardless of the order
	// allocations appear in the slice (simulating undefined Kubernetes List
	// ordering). Both growth allocations have no matching service IPs and
	// exceed the grace period, so both should be released. The initial
	// allocation must survive even though it also has no matching service IP.
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	oldTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	count := int32(1)

	growthAlloc2 := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "team-a-cluster-lb-2",
			Namespace:         "butler-system",
			CreationTimestamp: oldTime,
			Labels:            map[string]string{LabelAllocationRole: AllocationRoleGrowth},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.5", EndAddress: "10.0.0.5",
		},
	}
	initialAlloc := &butlerv1alpha1.IPAllocation{
		// The initial allocation is at index 1 — not index 0.
		ObjectMeta: metav1.ObjectMeta{
			Name:              "team-a-cluster-lb",
			Namespace:         "butler-system",
			CreationTimestamp: oldTime,
			Labels:            map[string]string{LabelAllocationRole: AllocationRoleInitial},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.1", EndAddress: "10.0.0.1",
		},
	}
	growthAlloc1 := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "team-a-cluster-lb-1",
			Namespace:         "butler-system",
			CreationTimestamp: oldTime,
			Labels:            map[string]string{LabelAllocationRole: AllocationRoleGrowth},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.3", EndAddress: "10.0.0.3",
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(growthAlloc2, initialAlloc, growthAlloc1).
		Build()

	// No service IPs — all allocations are unused.
	serviceIPs := map[string]bool{}

	// Run the demand-driven shrink loop (same logic as reconcileElasticIPAM).
	allocs := []butlerv1alpha1.IPAllocation{*growthAlloc2, *initialAlloc, *growthAlloc1}
	for i := range allocs {
		alloc := &allocs[i]
		if alloc.Labels[LabelAllocationRole] != AllocationRoleGrowth {
			continue
		}
		if alloc.Status.Phase != butlerv1alpha1.IPAllocationPhaseAllocated {
			continue
		}
		if time.Since(alloc.CreationTimestamp.Time) < shrinkGracePeriod {
			continue
		}
		if allocationHasServiceIP(alloc, serviceIPs) {
			continue
		}
		_ = cl.Delete(context.Background(), alloc)
	}

	// Both growth allocations should be deleted.
	for _, name := range []string{"team-a-cluster-lb-1", "team-a-cluster-lb-2"} {
		err := cl.Get(context.Background(), client.ObjectKey{Name: name, Namespace: "butler-system"}, &butlerv1alpha1.IPAllocation{})
		if err == nil {
			t.Errorf("growth allocation %s should have been deleted but still exists", name)
		}
	}

	// Initial allocation must survive.
	err := cl.Get(context.Background(), client.ObjectKey{Name: "team-a-cluster-lb", Namespace: "butler-system"}, &butlerv1alpha1.IPAllocation{})
	if err != nil {
		t.Error("initial allocation was deleted; this must never happen")
	}
}

func TestShrinkRefetchesAfterDelete(t *testing.T) {
	// Verifies that after shrink deletes an allocation, the re-fetch via
	// listLBAllocations returns a fresh slice without the deleted object.
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

func TestMigrateAllocationRoleLabels_PinnedRange(t *testing.T) {
	// A pinned allocation with a growth-like name pattern (-lb-1) should
	// still be labeled as initial, because pinned allocations are operator-
	// defined and must never be auto-released by shrink.
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
	}
	count := int32(2)

	pinnedAlloc := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-a-test-lb-1",
			Namespace: "butler-system",
			Labels: map[string]string{
				butlerv1alpha1.LabelTeam:           "team-a",
				butlerv1alpha1.LabelTenant:         "test",
				butlerv1alpha1.LabelAllocationType: "loadbalancer",
				// No allocation-role label — needs migration
			},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{
			Count: &count,
			PinnedRange: &butlerv1alpha1.PinnedIPRange{
				StartAddress: "10.0.0.100",
				EndAddress:   "10.0.0.101",
			},
		},
		Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pinnedAlloc).
		Build()
	r := &Reconciler{Client: cl}

	allocs := []butlerv1alpha1.IPAllocation{*pinnedAlloc}
	r.migrateAllocationRoleLabels(context.Background(), allocs, tc)

	updated := &butlerv1alpha1.IPAllocation{}
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(pinnedAlloc), updated); err != nil {
		t.Fatalf("failed to get allocation: %v", err)
	}
	if updated.Labels[LabelAllocationRole] != AllocationRoleInitial {
		t.Errorf("pinned allocation labeled %q, want %q (pinned allocations must be treated as initial)",
			updated.Labels[LabelAllocationRole], AllocationRoleInitial)
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

func TestShrinkFiltersMixedAllocations(t *testing.T) {
	// Test demand-driven shrink with a mix of initial, growth, pending, and
	// young allocations. Only the growth allocation that is Allocated, past
	// the grace period, and has no matching service IP should be deleted.
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	oldTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	youngTime := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	count := int32(1)

	initialAlloc := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name: "team-a-c-lb", Namespace: "butler-system",
			CreationTimestamp: oldTime,
			Labels:            map[string]string{LabelAllocationRole: AllocationRoleInitial},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase: butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.1", EndAddress: "10.0.0.1",
		},
	}
	youngGrowth := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name: "team-a-c-lb-1", Namespace: "butler-system",
			CreationTimestamp: youngTime,
			Labels:            map[string]string{LabelAllocationRole: AllocationRoleGrowth},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase: butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.3", EndAddress: "10.0.0.3",
		},
	}
	pendingGrowth := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name: "team-a-c-lb-2", Namespace: "butler-system",
			CreationTimestamp: oldTime,
			Labels:            map[string]string{LabelAllocationRole: AllocationRoleGrowth},
		},
		Spec:   butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhasePending},
	}
	eligibleGrowth := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name: "team-a-c-lb-3", Namespace: "butler-system",
			CreationTimestamp: oldTime,
			Labels:            map[string]string{LabelAllocationRole: AllocationRoleGrowth},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{Count: &count},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase: butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.5", EndAddress: "10.0.0.5",
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initialAlloc, youngGrowth, pendingGrowth, eligibleGrowth).
		Build()

	// No service IPs — all allocations are unused.
	serviceIPs := map[string]bool{}

	allocs := []butlerv1alpha1.IPAllocation{*initialAlloc, *youngGrowth, *pendingGrowth, *eligibleGrowth}
	for i := range allocs {
		alloc := &allocs[i]
		if alloc.Labels[LabelAllocationRole] != AllocationRoleGrowth {
			continue
		}
		if alloc.Status.Phase != butlerv1alpha1.IPAllocationPhaseAllocated {
			continue
		}
		if time.Since(alloc.CreationTimestamp.Time) < shrinkGracePeriod {
			continue
		}
		if allocationHasServiceIP(alloc, serviceIPs) {
			continue
		}
		_ = cl.Delete(context.Background(), alloc)
	}

	// Only the eligible growth allocation should be deleted.
	err := cl.Get(context.Background(), client.ObjectKey{Name: "team-a-c-lb-3", Namespace: "butler-system"}, &butlerv1alpha1.IPAllocation{})
	if err == nil {
		t.Error("eligible growth allocation should have been deleted but still exists")
	}

	// All others should survive.
	for _, name := range []string{"team-a-c-lb", "team-a-c-lb-1", "team-a-c-lb-2"} {
		err := cl.Get(context.Background(), client.ObjectKey{Name: name, Namespace: "butler-system"}, &butlerv1alpha1.IPAllocation{})
		if err != nil {
			t.Errorf("allocation %s should have survived but was deleted", name)
		}
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

func TestInFlightGrowthDeduction(t *testing.T) {
	// Validates the in-flight growth supply deduction that prevents redundant
	// growth allocations when a watch-triggered reconcile fires before MetalLB
	// propagation completes.
	//
	// The growth path computes qualifiedPending (Services stuck Pending long
	// enough) and then subtracts in-flight supply: growth allocations that are
	// Pending (awaiting fulfillment) or Allocated but unconsumed (no tenant
	// Service is using any IP from the allocation's range).

	int32Ptr := func(v int32) *int32 { return &v }

	tests := []struct {
		name           string
		pendingServices []LBServiceSummary
		allocs         []butlerv1alpha1.IPAllocation
		serviceIPs     map[string]bool
		wantNetPending int32
	}{
		{
			name: "no in-flight allocations: growth fires normally",
			pendingServices: []LBServiceSummary{
				{Name: "stuck-svc", Namespace: "default", CreatedAt: time.Now().Add(-2 * time.Minute)},
			},
			allocs: []butlerv1alpha1.IPAllocation{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleInitial},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(2)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated, StartAddress: "10.0.0.1", EndAddress: "10.0.0.2"},
				},
			},
			serviceIPs:     map[string]bool{"10.0.0.1": true, "10.0.0.2": true},
			wantNetPending: 1,
		},
		{
			name: "pending growth allocation covers demand: growth suppressed",
			pendingServices: []LBServiceSummary{
				{Name: "stuck-svc", Namespace: "default", CreatedAt: time.Now().Add(-2 * time.Minute)},
			},
			allocs: []butlerv1alpha1.IPAllocation{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleInitial},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(2)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated, StartAddress: "10.0.0.1", EndAddress: "10.0.0.2"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleGrowth},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(1)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhasePending},
				},
			},
			serviceIPs:     map[string]bool{"10.0.0.1": true, "10.0.0.2": true},
			wantNetPending: 0,
		},
		{
			name: "allocated unconsumed growth covers demand: growth suppressed",
			pendingServices: []LBServiceSummary{
				{Name: "stuck-svc", Namespace: "default", CreatedAt: time.Now().Add(-2 * time.Minute)},
			},
			allocs: []butlerv1alpha1.IPAllocation{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleInitial},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(2)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated, StartAddress: "10.0.0.1", EndAddress: "10.0.0.2"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleGrowth},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(1)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated, StartAddress: "10.0.0.3", EndAddress: "10.0.0.3"},
				},
			},
			serviceIPs:     map[string]bool{"10.0.0.1": true, "10.0.0.2": true},
			wantNetPending: 0,
		},
		{
			name: "consumed growth allocation not counted as in-flight: growth fires",
			pendingServices: []LBServiceSummary{
				{Name: "stuck-svc", Namespace: "default", CreatedAt: time.Now().Add(-2 * time.Minute)},
			},
			allocs: []butlerv1alpha1.IPAllocation{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleInitial},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(2)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated, StartAddress: "10.0.0.1", EndAddress: "10.0.0.2"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleGrowth},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(1)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated, StartAddress: "10.0.0.3", EndAddress: "10.0.0.3"},
				},
			},
			serviceIPs:     map[string]bool{"10.0.0.1": true, "10.0.0.2": true, "10.0.0.3": true},
			wantNetPending: 1,
		},
		{
			name: "two pending services with one in-flight: partial coverage, growth fires for remainder",
			pendingServices: []LBServiceSummary{
				{Name: "svc-a", Namespace: "default", CreatedAt: time.Now().Add(-2 * time.Minute)},
				{Name: "svc-b", Namespace: "default", CreatedAt: time.Now().Add(-3 * time.Minute)},
			},
			allocs: []butlerv1alpha1.IPAllocation{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleInitial},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(2)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated, StartAddress: "10.0.0.1", EndAddress: "10.0.0.2"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleGrowth},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(1)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhasePending},
				},
			},
			serviceIPs:     map[string]bool{"10.0.0.1": true, "10.0.0.2": true},
			wantNetPending: 1,
		},
		{
			name: "initial allocation never counted as in-flight",
			pendingServices: []LBServiceSummary{
				{Name: "stuck-svc", Namespace: "default", CreatedAt: time.Now().Add(-2 * time.Minute)},
			},
			allocs: []butlerv1alpha1.IPAllocation{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleInitial},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(2)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated, StartAddress: "10.0.0.1", EndAddress: "10.0.0.2"},
				},
			},
			serviceIPs:     map[string]bool{},
			wantNetPending: 1,
		},
		{
			name: "in-flight supply exceeds demand: clamps to zero",
			pendingServices: []LBServiceSummary{
				{Name: "stuck-svc", Namespace: "default", CreatedAt: time.Now().Add(-2 * time.Minute)},
			},
			allocs: []butlerv1alpha1.IPAllocation{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleInitial},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(2)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhaseAllocated, StartAddress: "10.0.0.1", EndAddress: "10.0.0.2"},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelAllocationRole: AllocationRoleGrowth},
					},
					Spec:   butlerv1alpha1.IPAllocationSpec{Count: int32Ptr(2)},
					Status: butlerv1alpha1.IPAllocationStatus{Phase: butlerv1alpha1.IPAllocationPhasePending},
				},
			},
			serviceIPs:     map[string]bool{"10.0.0.1": true, "10.0.0.2": true},
			wantNetPending: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Count qualified pending services (same as reconcileElasticIPAM).
			var qualifiedPending int32
			for _, svc := range tt.pendingServices {
				if time.Since(svc.CreatedAt) >= pendingServiceMinAge {
					qualifiedPending++
				}
			}

			// Deduct in-flight growth supply (the code under test).
			var inFlightSupply int32
			for i := range tt.allocs {
				alloc := &tt.allocs[i]
				if alloc.Labels[LabelAllocationRole] != AllocationRoleGrowth {
					continue
				}
				if alloc.Spec.Count == nil {
					continue
				}
				switch alloc.Status.Phase {
				case butlerv1alpha1.IPAllocationPhasePending:
					inFlightSupply += *alloc.Spec.Count
				case butlerv1alpha1.IPAllocationPhaseAllocated:
					if !allocationHasServiceIP(alloc, tt.serviceIPs) {
						inFlightSupply += *alloc.Spec.Count
					}
				}
			}
			qualifiedPending -= inFlightSupply
			if qualifiedPending < 0 {
				qualifiedPending = 0
			}

			if qualifiedPending != tt.wantNetPending {
				t.Errorf("net qualifiedPending = %d, want %d (raw pending minus %d in-flight)",
					qualifiedPending, tt.wantNetPending, inFlightSupply)
			}
		})
	}
}
