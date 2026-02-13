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

package providerconfig

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	providerReady = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_provider_config_ready",
			Help: "Whether a ProviderConfig is ready (1=ready, 0=not ready).",
		},
		[]string{"provider", "namespace", "type", "scope"},
	)

	providerAvailableIPs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_provider_config_available_ips",
			Help: "Available IPs across all pools for an IPAM-mode ProviderConfig.",
		},
		[]string{"provider", "namespace"},
	)

	providerEstimatedTenants = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "butler_provider_config_estimated_tenants",
			Help: "Estimated number of tenants that can be provisioned by a ProviderConfig.",
		},
		[]string{"provider", "namespace"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		providerReady,
		providerAvailableIPs,
		providerEstimatedTenants,
	)
}
