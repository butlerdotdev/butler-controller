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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

var _ = Describe("InfraAllocationReconciler", func() {
	const (
		timeout  = 15 * time.Second
		interval = 250 * time.Millisecond

		testNS   = "default"
		poolCIDR = "10.50.0.0/16"
	)

	cleanupInfraTest := func(poolName string, svcNames ...string) {
		pool := &butlerv1alpha1.NetworkPool{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNS}, pool); err == nil {
			_ = k8sClient.Delete(ctx, pool)
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNS}, pool)
			}, timeout, interval).ShouldNot(Succeed())
		}
		for _, name := range svcNames {
			svc := &corev1.Service{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNS}, svc); err == nil {
				_ = k8sClient.Delete(ctx, svc)
			}
		}
	}

	Context("when a LB Service IP falls within a reserved range", func() {
		poolName := "infra-test-pool"
		svcName := "infra-test-svc"

		AfterEach(func() {
			cleanupInfraTest(poolName, svcName)
		})

		It("should populate InfrastructureAllocations", func() {
			pool := &butlerv1alpha1.NetworkPool{
				ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: testNS},
				Spec: butlerv1alpha1.NetworkPoolSpec{
					CIDR: poolCIDR,
					Reserved: []butlerv1alpha1.ReservedRange{
						{CIDR: "10.50.0.0/28", Description: "management infra"},
					},
					TenantAllocation: &butlerv1alpha1.TenantAllocationConfig{
						Ranges: []butlerv1alpha1.AllocationRange{
							{Start: "10.50.1.0", End: "10.50.1.255"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: testNS},
				Spec: corev1.ServiceSpec{
					Type:  corev1.ServiceTypeLoadBalancer,
					Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP}},
				},
			}
			Expect(k8sClient.Create(ctx, svc)).To(Succeed())

			// Simulate MetalLB assigning an IP by updating the status
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: svcName, Namespace: testNS}, svc); err != nil {
					return err
				}
				svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{
					{IP: "10.50.0.5"},
				}
				return k8sClient.Status().Update(ctx, svc)
			}, timeout, interval).Should(Succeed())

			// Wait for the reconciler to discover the IP and populate allocations
			Eventually(func(g Gomega) {
				updated := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNS}, updated)).To(Succeed())
				g.Expect(updated.Status.InfrastructureAllocations).To(HaveLen(1))
				g.Expect(updated.Status.InfrastructureAllocations[0].IP).To(Equal("10.50.0.5"))
				g.Expect(updated.Status.InfrastructureAllocations[0].Source).To(Equal(SourceMetalLB))
				g.Expect(updated.Status.InfrastructureAllocations[0].ServiceRef).NotTo(BeNil())
				g.Expect(updated.Status.InfrastructureAllocations[0].ServiceRef.Name).To(Equal(svcName))
				g.Expect(updated.Status.InfrastructureAllocations[0].ServiceRef.Namespace).To(Equal(testNS))
			}, timeout, interval).Should(Succeed())
		})
	})

	Context("when a LB Service is deleted", func() {
		poolName := "infra-delete-pool"
		svcName := "infra-delete-svc"

		AfterEach(func() {
			cleanupInfraTest(poolName, svcName)
		})

		It("should remove the allocation entry", func() {
			pool := &butlerv1alpha1.NetworkPool{
				ObjectMeta: metav1.ObjectMeta{Name: poolName, Namespace: testNS},
				Spec: butlerv1alpha1.NetworkPoolSpec{
					CIDR: poolCIDR,
					Reserved: []butlerv1alpha1.ReservedRange{
						{CIDR: "10.50.0.0/28"},
					},
					TenantAllocation: &butlerv1alpha1.TenantAllocationConfig{
						Ranges: []butlerv1alpha1.AllocationRange{
							{Start: "10.50.1.0", End: "10.50.1.255"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: svcName, Namespace: testNS},
				Spec: corev1.ServiceSpec{
					Type:  corev1.ServiceTypeLoadBalancer,
					Ports: []corev1.ServicePort{{Port: 80, Protocol: corev1.ProtocolTCP}},
				},
			}
			Expect(k8sClient.Create(ctx, svc)).To(Succeed())

			// Assign IP
			Eventually(func() error {
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: svcName, Namespace: testNS}, svc); err != nil {
					return err
				}
				svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "10.50.0.3"}}
				return k8sClient.Status().Update(ctx, svc)
			}, timeout, interval).Should(Succeed())

			// Wait for allocation to appear
			Eventually(func(g Gomega) {
				updated := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNS}, updated)).To(Succeed())
				g.Expect(updated.Status.InfrastructureAllocations).To(HaveLen(1))
			}, timeout, interval).Should(Succeed())

			// Delete the service
			Expect(k8sClient.Delete(ctx, svc)).To(Succeed())

			// Wait for allocation to be removed
			Eventually(func(g Gomega) {
				updated := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNS}, updated)).To(Succeed())
				g.Expect(updated.Status.InfrastructureAllocations).To(BeEmpty())
			}, timeout, interval).Should(Succeed())
		})
	})
})
