// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	reconcileDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "butler_tenant_cluster_reconcile_duration_seconds",
			Help:    "Duration of TenantCluster reconcile loops in seconds.",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30, 60},
		},
		[]string{"namespace", "name"},
	)

	clusterPhase = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_tenant_cluster_phase",
			Help: "Current phase of a TenantCluster. Value is 1 for the current phase, 0 for others.",
		},
		[]string{"namespace", "name", "phase"},
	)

)

func init() {
	metrics.Registry.MustRegister(
		reconcileDuration,
		clusterPhase,
	)
}

var allPhases = []string{"", "Pending", "Provisioning", "Installing", "Ready", "Failed", "Deleting"}

func recordPhase(namespace, name, phase string) {
	for _, p := range allPhases {
		val := float64(0)
		if p == phase {
			val = 1
		}
		clusterPhase.WithLabelValues(namespace, name, p).Set(val)
	}
}
