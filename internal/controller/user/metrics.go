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

package user

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	resolveTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "butler_user_status_resolve_total",
			Help: "Total team membership resolution attempts for User CRDs.",
		},
		[]string{"trigger", "result"},
	)

	resolveDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "butler_user_status_resolve_duration_seconds",
			Help:    "Time to resolve team membership for a single User CRD.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		},
		[]string{"trigger"},
	)

	writeTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "butler_user_status_write_total",
			Help: "User status write outcomes. noop means resolved teams matched current status.",
		},
		[]string{"result"},
	)

	staleAge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "butler_user_status_stale_age_seconds",
			Help: "Maximum time since last successful status.teams resolution across all users. Falls back to creationTimestamp for never-resolved users.",
		},
	)

	userCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_user_count",
			Help: "User population breakdown by state.",
		},
		[]string{"state"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		resolveTotal,
		resolveDuration,
		writeTotal,
		staleAge,
		userCount,
	)
}
