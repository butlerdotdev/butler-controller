// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

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

			gotPlatform, gotTenant, err := r.countLBIPs(context.Background(), tc)
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

			platformCount, tenantCount, err := r.countLBIPs(context.Background(), tc)
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
