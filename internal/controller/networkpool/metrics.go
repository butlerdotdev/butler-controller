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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	poolTotalIPs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_network_pool_total_ips",
			Help: "Total number of IPs in a NetworkPool (excludes reserved).",
		},
		[]string{"pool", "namespace"},
	)

	poolAllocatedIPs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_network_pool_allocated_ips",
			Help: "Number of allocated IPs in a NetworkPool.",
		},
		[]string{"pool", "namespace"},
	)

	poolAvailableIPs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_network_pool_available_ips",
			Help: "Number of available IPs in a NetworkPool.",
		},
		[]string{"pool", "namespace"},
	)

	poolAllocationCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_network_pool_allocation_count",
			Help: "Number of active IPAllocations from a NetworkPool.",
		},
		[]string{"pool", "namespace"},
	)

	poolFragmentationPercent = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_network_pool_fragmentation_percent",
			Help: "Fragmentation percentage of free space in a NetworkPool (0-100).",
		},
		[]string{"pool", "namespace"},
	)

	poolLargestFreeBlock = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_network_pool_largest_free_block",
			Help: "Size of the largest contiguous free block in a NetworkPool.",
		},
		[]string{"pool", "namespace"},
	)

	allocationProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "butler_ip_allocation_processed_total",
			Help: "Total number of IP allocations processed, by result.",
		},
		[]string{"pool", "namespace", "result"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		poolTotalIPs,
		poolAllocatedIPs,
		poolAvailableIPs,
		poolAllocationCount,
		poolFragmentationPercent,
		poolLargestFreeBlock,
		allocationProcessed,
	)
}
