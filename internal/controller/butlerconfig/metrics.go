// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package butlerconfig

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	clusterCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_tenant_clusters",
			Help: "Total number of TenantClusters by phase.",
		},
		[]string{"phase"},
	)
)

func init() {
	metrics.Registry.MustRegister(clusterCount)
}
