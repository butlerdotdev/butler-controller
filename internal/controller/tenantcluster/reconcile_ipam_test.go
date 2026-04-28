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

func TestCountUsedLBIPs(t *testing.T) {
	tests := []struct {
		name     string
		services []corev1.Service
		expected int32
	}{
		{
			name:     "no services",
			services: nil,
			expected: 0,
		},
		{
			name: "ClusterIP service ignored",
			services: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
					Spec:       corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
				},
			},
			expected: 0,
		},
		{
			name: "LB service without label counted",
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
			expected: 1,
		},
		{
			name: "LB service with platform-lb label excluded",
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
			expected: 0,
		},
		{
			name: "mixed services: only unlabeled LBs with IPs counted",
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
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objects []corev1.Service
			objects = append(objects, tt.services...)

			clientset := fake.NewSimpleClientset()
			for i := range objects {
				_, err := clientset.CoreV1().Services(objects[i].Namespace).Create(
					context.Background(), &objects[i], metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("failed to create test service: %v", err)
				}
			}

			tc := &tenant.TenantClient{Clientset: clientset}
			r := &Reconciler{}

			got, err := r.countUsedLBIPs(context.Background(), tc)
			if err != nil {
				t.Fatalf("countUsedLBIPs() error = %v", err)
			}
			if got != tt.expected {
				t.Errorf("countUsedLBIPs() = %d, want %d", got, tt.expected)
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

		// Verify no update was issued (label still present, no error)
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
