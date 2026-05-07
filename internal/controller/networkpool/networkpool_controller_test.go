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
	"fmt"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/types"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

var _ = Describe("NetworkPool Controller", func() {
	const (
		timeout  = 10 * time.Second
		interval = 100 * time.Millisecond

		testNamespace = "default"
		testCIDR      = "10.40.0.0/24"
		allocStart    = "10.40.0.128"
		allocEnd      = "10.40.0.255"
	)

	// newStandardTenantAllocation returns the tenantAllocation config used across most tests.
	newStandardTenantAllocation := func() *butlerv1alpha1.TenantAllocationConfig {
		return &butlerv1alpha1.TenantAllocationConfig{
			Ranges: []butlerv1alpha1.AllocationRange{
				{Start: allocStart, End: allocEnd},
			},
		}
	}

	// cleanupPool deletes the pool and waits for it to disappear. It first deletes
	// all IPAllocations referencing the pool to unblock the finalizer.
	cleanupPool := func(poolName string) {
		allocList := &butlerv1alpha1.IPAllocationList{}
		_ = k8sClient.List(ctx, allocList)
		for i := range allocList.Items {
			alloc := &allocList.Items[i]
			if alloc.Spec.PoolRef.Name == poolName && alloc.Namespace == testNamespace {
				_ = k8sClient.Delete(ctx, alloc)
			}
		}

		// Wait for all allocations to be fully deleted
		Eventually(func() int {
			list := &butlerv1alpha1.IPAllocationList{}
			_ = k8sClient.List(ctx, list)
			count := 0
			for _, a := range list.Items {
				if a.Spec.PoolRef.Name == poolName && a.Namespace == testNamespace {
					count++
				}
			}
			return count
		}, timeout, interval).Should(Equal(0))

		pool := &butlerv1alpha1.NetworkPool{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, pool); err == nil {
			_ = k8sClient.Delete(ctx, pool)
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, pool)
			}, timeout, interval).ShouldNot(Succeed())
		}
	}

	Describe("Pool Status Math", func() {
		const poolName = "pool-status-math"

		AfterEach(func() {
			cleanupPool(poolName)
		})

		It("should compute correct IP counts from CIDR and tenantAllocation range", func() {
			pool := newNetworkPool(poolName, testNamespace, testCIDR, newStandardTenantAllocation())
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			// 10.40.0.128 to 10.40.0.255 = 128 IPs
			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Status.TotalIPs).To(Equal(int32(128)))
				g.Expect(fetched.Status.AvailableIPs).To(Equal(int32(128)))
				g.Expect(fetched.Status.AllocatedIPs).To(Equal(int32(0)))
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Single Allocation", func() {
		const poolName = "pool-single-alloc"
		const allocName = "alloc-single"

		AfterEach(func() {
			cleanupPool(poolName)
		})

		It("should allocate IPs from a pending IPAllocation", func() {
			pool := newNetworkPool(poolName, testNamespace, testCIDR, newStandardTenantAllocation())
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			// Wait for pool to be ready
			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Status.TotalIPs).To(Equal(int32(128)))
			}, timeout, interval).Should(Succeed())

			alloc := newIPAllocation(allocName, testNamespace, poolName, butlerv1alpha1.IPAllocationTypeLoadBalancer, int32Ptr(8))
			Expect(k8sClient.Create(ctx, alloc)).To(Succeed())

			// Wait for allocation to reach Allocated phase
			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.IPAllocation{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: allocName, Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal(butlerv1alpha1.IPAllocationPhaseAllocated))
			}, timeout, interval).Should(Succeed())

			// Verify allocation details
			fetchedAlloc := &butlerv1alpha1.IPAllocation{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: allocName, Namespace: testNamespace}, fetchedAlloc)).To(Succeed())

			Expect(fetchedAlloc.Status.StartAddress).NotTo(BeEmpty())
			Expect(fetchedAlloc.Status.EndAddress).NotTo(BeEmpty())
			Expect(fetchedAlloc.Status.AllocatedCount).To(Equal(int32(8)))
			Expect(fetchedAlloc.Status.Addresses).To(HaveLen(8))
			Expect(fetchedAlloc.Status.AllocatedAt).NotTo(BeNil())
			Expect(fetchedAlloc.Status.AllocatedBy).To(Equal("networkpool-controller"))

			// Verify pool status updated
			Eventually(func(g Gomega) {
				fetchedPool := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetchedPool)).To(Succeed())
				g.Expect(fetchedPool.Status.AllocatedIPs).To(Equal(int32(8)))
				g.Expect(fetchedPool.Status.AvailableIPs).To(Equal(int32(120)))
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Multiple Non-Overlapping Allocations", func() {
		const poolName = "pool-multi-alloc"

		AfterEach(func() {
			cleanupPool(poolName)
		})

		It("should allocate non-overlapping ranges for multiple IPAllocations", func() {
			pool := newNetworkPool(poolName, testNamespace, testCIDR, newStandardTenantAllocation())
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			// Wait for pool to be ready
			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Status.TotalIPs).To(Equal(int32(128)))
			}, timeout, interval).Should(Succeed())

			// Create 3 allocations sequentially
			allocNames := []string{"alloc-multi-1", "alloc-multi-2", "alloc-multi-3"}
			for _, name := range allocNames {
				alloc := newIPAllocation(name, testNamespace, poolName, butlerv1alpha1.IPAllocationTypeLoadBalancer, int32Ptr(8))
				Expect(k8sClient.Create(ctx, alloc)).To(Succeed())
			}

			// Wait for all 3 to reach Allocated phase
			for _, name := range allocNames {
				Eventually(func(g Gomega) {
					fetched := &butlerv1alpha1.IPAllocation{}
					g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, fetched)).To(Succeed())
					g.Expect(fetched.Status.Phase).To(Equal(butlerv1alpha1.IPAllocationPhaseAllocated))
				}, timeout, interval).Should(Succeed())
			}

			// Collect allocated ranges and verify no overlap
			type ipRange struct {
				start uint32
				end   uint32
			}
			var ranges []ipRange
			for _, name := range allocNames {
				fetched := &butlerv1alpha1.IPAllocation{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, fetched)).To(Succeed())
				Expect(fetched.Status.StartAddress).NotTo(BeEmpty())
				Expect(fetched.Status.EndAddress).NotTo(BeEmpty())
				Expect(fetched.Status.AllocatedCount).To(Equal(int32(8)))

				startIP := net.ParseIP(fetched.Status.StartAddress).To4()
				endIP := net.ParseIP(fetched.Status.EndAddress).To4()
				Expect(startIP).NotTo(BeNil())
				Expect(endIP).NotTo(BeNil())

				s := uint32(startIP[0])<<24 | uint32(startIP[1])<<16 | uint32(startIP[2])<<8 | uint32(startIP[3])
				e := uint32(endIP[0])<<24 | uint32(endIP[1])<<16 | uint32(endIP[2])<<8 | uint32(endIP[3])
				ranges = append(ranges, ipRange{start: s, end: e})
			}

			// Verify no ranges overlap
			for i := 0; i < len(ranges); i++ {
				for j := i + 1; j < len(ranges); j++ {
					overlaps := ranges[i].start <= ranges[j].end && ranges[j].start <= ranges[i].end
					Expect(overlaps).To(BeFalse(), fmt.Sprintf(
						"ranges %d and %d overlap: [%d-%d] vs [%d-%d]",
						i, j, ranges[i].start, ranges[i].end, ranges[j].start, ranges[j].end,
					))
				}
			}

			// Verify pool status: 24 IPs allocated, 104 available
			Eventually(func(g Gomega) {
				fetchedPool := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetchedPool)).To(Succeed())
				g.Expect(fetchedPool.Status.AllocatedIPs).To(Equal(int32(24)))
				g.Expect(fetchedPool.Status.AvailableIPs).To(Equal(int32(104)))
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Pool Exhaustion", func() {
		const poolName = "pool-exhaustion"

		AfterEach(func() {
			cleanupPool(poolName)
		})

		It("should fail allocation when the pool is exhausted", func() {
			// Small pool: 10.40.0.128 to 10.40.0.135 = 8 IPs
			smallAlloc := &butlerv1alpha1.TenantAllocationConfig{
				Start: "10.40.0.128",
				End:   "10.40.0.135",
			}
			pool := newNetworkPool(poolName, testNamespace, testCIDR, smallAlloc)
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			// Wait for pool to be ready
			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Status.TotalIPs).To(Equal(int32(8)))
			}, timeout, interval).Should(Succeed())

			// First allocation: request 8 IPs, should succeed
			alloc1 := newIPAllocation("alloc-exhaust-1", testNamespace, poolName, butlerv1alpha1.IPAllocationTypeLoadBalancer, int32Ptr(8))
			Expect(k8sClient.Create(ctx, alloc1)).To(Succeed())

			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.IPAllocation{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "alloc-exhaust-1", Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal(butlerv1alpha1.IPAllocationPhaseAllocated))
			}, timeout, interval).Should(Succeed())

			// Second allocation: request 4 more IPs, should fail (pool exhausted)
			alloc2 := newIPAllocation("alloc-exhaust-2", testNamespace, poolName, butlerv1alpha1.IPAllocationTypeLoadBalancer, int32Ptr(4))
			Expect(k8sClient.Create(ctx, alloc2)).To(Succeed())

			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.IPAllocation{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "alloc-exhaust-2", Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal(butlerv1alpha1.IPAllocationPhaseFailed))
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Allocation Cleanup", func() {
		const poolName = "pool-cleanup"
		const allocName = "alloc-cleanup"

		AfterEach(func() {
			cleanupPool(poolName)
		})

		It("should release IPs and update pool status when an IPAllocation is deleted", func() {
			pool := newNetworkPool(poolName, testNamespace, testCIDR, newStandardTenantAllocation())
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			// Wait for pool to be ready
			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Status.TotalIPs).To(Equal(int32(128)))
			}, timeout, interval).Should(Succeed())

			alloc := newIPAllocation(allocName, testNamespace, poolName, butlerv1alpha1.IPAllocationTypeLoadBalancer, int32Ptr(8))
			Expect(k8sClient.Create(ctx, alloc)).To(Succeed())

			// Wait for allocation to succeed
			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.IPAllocation{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: allocName, Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal(butlerv1alpha1.IPAllocationPhaseAllocated))
			}, timeout, interval).Should(Succeed())

			// Verify pool has 8 allocated
			Eventually(func(g Gomega) {
				fetchedPool := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetchedPool)).To(Succeed())
				g.Expect(fetchedPool.Status.AllocatedIPs).To(Equal(int32(8)))
			}, timeout, interval).Should(Succeed())

			// Delete the IPAllocation
			fetchedAlloc := &butlerv1alpha1.IPAllocation{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: allocName, Namespace: testNamespace}, fetchedAlloc)).To(Succeed())
			Expect(k8sClient.Delete(ctx, fetchedAlloc)).To(Succeed())

			// The IPAllocation controller sets releasedAt and removes the finalizer,
			// then the object is garbage collected. Verify pool returns to full capacity.
			Eventually(func(g Gomega) {
				fetchedPool := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetchedPool)).To(Succeed())
				g.Expect(fetchedPool.Status.AllocatedIPs).To(Equal(int32(0)))
				g.Expect(fetchedPool.Status.AvailableIPs).To(Equal(int32(128)))
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Deletion Protection", func() {
		const poolName = "pool-deletion-protection"
		const allocName = "alloc-deletion-protection"

		AfterEach(func() {
			cleanupPool(poolName)
		})

		It("should add a finalizer to the NetworkPool", func() {
			pool := newNetworkPool(poolName, testNamespace, testCIDR, newStandardTenantAllocation())
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			// Wait for the finalizer to be added by the controller
			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Finalizers).To(ContainElement(butlerv1alpha1.FinalizerNetworkPool))
			}, timeout, interval).Should(Succeed())
		})

		It("should block pool deletion while allocated IPAllocations exist", func() {
			pool := newNetworkPool(poolName, testNamespace, testCIDR, newStandardTenantAllocation())
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			// Wait for pool to be ready
			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.NetworkPool{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Status.TotalIPs).To(Equal(int32(128)))
			}, timeout, interval).Should(Succeed())

			// Create an allocation
			alloc := newIPAllocation(allocName, testNamespace, poolName, butlerv1alpha1.IPAllocationTypeLoadBalancer, int32Ptr(8))
			Expect(k8sClient.Create(ctx, alloc)).To(Succeed())

			Eventually(func(g Gomega) {
				fetched := &butlerv1alpha1.IPAllocation{}
				g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: allocName, Namespace: testNamespace}, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal(butlerv1alpha1.IPAllocationPhaseAllocated))
			}, timeout, interval).Should(Succeed())

			// Attempt to delete the pool (should be blocked by finalizer)
			fetchedPool := &butlerv1alpha1.NetworkPool{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetchedPool)).To(Succeed())
			Expect(k8sClient.Delete(ctx, fetchedPool)).To(Succeed())

			// Pool should still exist (deletion blocked by finalizer)
			Consistently(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetchedPool)
			}, 2*time.Second, interval).Should(Succeed())

			// Verify the pool has a DeletionTimestamp set but is not yet deleted
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetchedPool)).To(Succeed())
			Expect(fetchedPool.DeletionTimestamp).NotTo(BeNil())
			Expect(fetchedPool.Finalizers).To(ContainElement(butlerv1alpha1.FinalizerNetworkPool))

			// Delete the allocation to unblock pool deletion
			fetchedAlloc := &butlerv1alpha1.IPAllocation{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: allocName, Namespace: testNamespace}, fetchedAlloc)).To(Succeed())
			Expect(k8sClient.Delete(ctx, fetchedAlloc)).To(Succeed())

			// Pool should now be fully deleted
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: poolName, Namespace: testNamespace}, fetchedPool)
			}, timeout, interval).ShouldNot(Succeed())
		})
	})
})
