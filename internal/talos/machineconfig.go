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

package talos

import (
	"encoding/base64"
	"fmt"
	"net"
	"strings"

	"gopkg.in/yaml.v3"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// IsTalosCluster returns true if the TenantCluster uses Talos OS for workers.
func IsTalosCluster(tc *butlerv1alpha1.TenantCluster) bool {
	return tc.Spec.Workers.MachineTemplate.OS.Type == butlerv1alpha1.OSTypeTalos
}

// MachineConfigInput contains all data needed to generate a Talos worker machine config.
type MachineConfigInput struct {
	// ClusterName is the name of the tenant cluster.
	ClusterName string

	// ControlPlaneEndpoint is the API server endpoint (e.g., "https://10.40.0.1:6443").
	ControlPlaneEndpoint string

	// ClusterCACert is the PEM-encoded Kubernetes cluster CA certificate.
	ClusterCACert string

	// MachineToken is the trustd authentication token (format: "butler.<hex>").
	// Used by Talos apid to authenticate with steward-trustd.
	MachineToken string

	// BootstrapToken is a kubeadm bootstrap token (format: "<6char>.<16char>").
	// Used by kubelet for TLS bootstrapping with the tenant API server.
	BootstrapToken string

	// OSCACert is the PEM-encoded OS CA certificate for trusting steward-trustd.
	OSCACert string

	// PodCIDR is the pod network CIDR (e.g., "10.244.0.0/16").
	PodCIDR string

	// ServiceCIDR is the service network CIDR (e.g., "10.96.0.0/12").
	ServiceCIDR string

	// InstallDisk is the disk device for Talos installation (e.g., "/dev/vda").
	InstallDisk string

	// InstallerImage is the Talos installer image reference.
	InstallerImage string

	// TimeServers are NTP servers for time synchronization.
	// Talos blocks kubelet startup until time is synced.
	TimeServers []string
}

// GenerateWorkerConfig generates a Talos v1alpha1 worker machine config YAML.
func GenerateWorkerConfig(input MachineConfigInput) ([]byte, error) {
	if err := validateInput(input); err != nil {
		return nil, fmt.Errorf("invalid input: %w", err)
	}

	clusterDNS, err := deriveClusterDNS(input.ServiceCIDR)
	if err != nil {
		return nil, fmt.Errorf("failed to derive cluster DNS: %w", err)
	}

	config := buildConfig(input, clusterDNS)

	data, err := yaml.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal machine config: %w", err)
	}

	return data, nil
}

func validateInput(input MachineConfigInput) error {
	if input.ClusterName == "" {
		return fmt.Errorf("clusterName is required")
	}
	if input.ControlPlaneEndpoint == "" {
		return fmt.Errorf("controlPlaneEndpoint is required")
	}
	if input.ClusterCACert == "" {
		return fmt.Errorf("clusterCACert is required")
	}
	if input.MachineToken == "" {
		return fmt.Errorf("machineToken is required")
	}
	if input.BootstrapToken == "" {
		return fmt.Errorf("bootstrapToken is required")
	}
	if input.OSCACert == "" {
		return fmt.Errorf("osCACert is required")
	}
	if input.PodCIDR == "" {
		return fmt.Errorf("podCIDR is required")
	}
	if input.ServiceCIDR == "" {
		return fmt.Errorf("serviceCIDR is required")
	}
	if input.InstallDisk == "" {
		return fmt.Errorf("installDisk is required")
	}
	return nil
}

func buildConfig(input MachineConfigInput, clusterDNS string) map[string]interface{} {
	osCABase64 := base64.StdEncoding.EncodeToString([]byte(input.OSCACert))
	clusterCABase64 := base64.StdEncoding.EncodeToString([]byte(input.ClusterCACert))

	machine := map[string]interface{}{
		"type":  "worker",
		"token": input.MachineToken,
		"ca": map[string]interface{}{
			"crt": osCABase64,
		},
		"certSANs": []string{},
		"kubelet": map[string]interface{}{
			"clusterDNS": []string{clusterDNS},
		},
		"network": map[string]interface{}{},
	}

	install := map[string]interface{}{
		"disk": input.InstallDisk,
	}
	if input.InstallerImage != "" {
		install["image"] = input.InstallerImage
	}
	machine["install"] = install

	if len(input.TimeServers) > 0 {
		machine["time"] = map[string]interface{}{
			"servers": input.TimeServers,
		}
	}

	cluster := map[string]interface{}{
		"controlPlane": map[string]interface{}{
			"endpoint": input.ControlPlaneEndpoint,
		},
		"clusterName": input.ClusterName,
		"ca": map[string]interface{}{
			"crt": clusterCABase64,
		},
		"network": map[string]interface{}{
			"dnsDomain":      "cluster.local",
			"podSubnets":     []string{input.PodCIDR},
			"serviceSubnets": []string{input.ServiceCIDR},
		},
		"token": input.BootstrapToken,
		"discovery": map[string]interface{}{
			"enabled": false,
		},
	}

	return map[string]interface{}{
		"version": "v1alpha1",
		"persist": true,
		"machine": machine,
		"cluster": cluster,
	}
}

// deriveClusterDNS computes the cluster DNS IP from the service CIDR.
// By convention, this is the 10th IP in the service subnet (e.g., 10.96.0.10).
func deriveClusterDNS(serviceCIDR string) (string, error) {
	_, ipNet, err := net.ParseCIDR(serviceCIDR)
	if err != nil {
		return "", fmt.Errorf("invalid service CIDR %q: %w", serviceCIDR, err)
	}

	ip := ipNet.IP.To4()
	if ip == nil {
		// IPv6 — use the 10th address
		ip = ipNet.IP.To16()
		ip[len(ip)-1] = 10
		return ip.String(), nil
	}

	// IPv4 — add 10 to the network address
	ip[3] += 10
	return ip.String(), nil
}

// EndpointFromTCPStatus converts a TCP status endpoint ("host:port") to
// the format Talos expects ("https://host:port").
func EndpointFromTCPStatus(endpoint string) string {
	if strings.HasPrefix(endpoint, "https://") || strings.HasPrefix(endpoint, "http://") {
		return endpoint
	}
	return "https://" + endpoint
}
