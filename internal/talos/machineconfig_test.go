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
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func validInput() MachineConfigInput {
	return MachineConfigInput{
		ClusterName:          "test-cluster",
		ControlPlaneEndpoint: "https://10.40.0.1:6443",
		ClusterCACert:        "-----BEGIN CERTIFICATE-----\nfakecacert\n-----END CERTIFICATE-----\n",
		MachineToken:         "butler.abcdef0123456789abcdef0123456789",
		BootstrapToken:       "abc123.0123456789abcdef",
		OSCACert:             "-----BEGIN CERTIFICATE-----\nfakeoscacert\n-----END CERTIFICATE-----\n",
		PodCIDR:              "10.244.0.0/16",
		ServiceCIDR:          "10.96.0.0/12",
		InstallDisk:          "/dev/vda",
		InstallerImage:       "factory.talos.dev/installer/abc:v1.9.3",
	}
}

func TestGenerateWorkerConfig(t *testing.T) {
	input := validInput()

	data, err := GenerateWorkerConfig(input)
	if err != nil {
		t.Fatalf("GenerateWorkerConfig() error = %v", err)
	}

	var config map[string]interface{}
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatalf("failed to unmarshal output: %v", err)
	}

	// Check top-level fields
	if v := config["version"]; v != "v1alpha1" {
		t.Errorf("version = %v, want v1alpha1", v)
	}
	if v := config["persist"]; v != true {
		t.Errorf("persist = %v, want true", v)
	}

	// Check machine section
	machine, ok := config["machine"].(map[string]interface{})
	if !ok {
		t.Fatal("machine section missing or wrong type")
	}
	if v := machine["type"]; v != "worker" {
		t.Errorf("machine.type = %v, want worker", v)
	}
	if v := machine["token"]; v != input.MachineToken {
		t.Errorf("machine.token = %v, want %v", v, input.MachineToken)
	}

	// Check machine.ca.crt is base64-encoded OS CA
	machineCA, ok := machine["ca"].(map[string]interface{})
	if !ok {
		t.Fatal("machine.ca missing")
	}
	expectedOSCA := base64.StdEncoding.EncodeToString([]byte(input.OSCACert))
	if v := machineCA["crt"]; v != expectedOSCA {
		t.Error("machine.ca.crt does not match base64-encoded OS CA")
	}

	// Check cluster section
	cluster, ok := config["cluster"].(map[string]interface{})
	if !ok {
		t.Fatal("cluster section missing or wrong type")
	}
	if v := cluster["clusterName"]; v != input.ClusterName {
		t.Errorf("cluster.clusterName = %v, want %v", v, input.ClusterName)
	}
	if v := cluster["token"]; v != input.BootstrapToken {
		t.Errorf("cluster.token = %v, want %v (bootstrap token)", v, input.BootstrapToken)
	}

	// Check cluster.ca.crt is base64-encoded cluster CA
	clusterCA, ok := cluster["ca"].(map[string]interface{})
	if !ok {
		t.Fatal("cluster.ca missing")
	}
	expectedClusterCA := base64.StdEncoding.EncodeToString([]byte(input.ClusterCACert))
	if v := clusterCA["crt"]; v != expectedClusterCA {
		t.Error("cluster.ca.crt does not match base64-encoded cluster CA")
	}

	// Check control plane endpoint
	cp, ok := cluster["controlPlane"].(map[string]interface{})
	if !ok {
		t.Fatal("cluster.controlPlane missing")
	}
	if v := cp["endpoint"]; v != input.ControlPlaneEndpoint {
		t.Errorf("cluster.controlPlane.endpoint = %v, want %v", v, input.ControlPlaneEndpoint)
	}

	// Check discovery disabled
	discovery, ok := cluster["discovery"].(map[string]interface{})
	if !ok {
		t.Fatal("cluster.discovery missing")
	}
	if v := discovery["enabled"]; v != false {
		t.Errorf("cluster.discovery.enabled = %v, want false", v)
	}

	// Check machine.token != cluster.token (different tokens)
	if machine["token"] == cluster["token"] {
		t.Error("machine.token and cluster.token should be different (trustd vs kubeadm bootstrap)")
	}
}

func TestGenerateWorkerConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*MachineConfigInput)
		wantErr string
	}{
		{
			name:    "missing cluster name",
			modify:  func(i *MachineConfigInput) { i.ClusterName = "" },
			wantErr: "clusterName",
		},
		{
			name:    "missing control plane endpoint",
			modify:  func(i *MachineConfigInput) { i.ControlPlaneEndpoint = "" },
			wantErr: "controlPlaneEndpoint",
		},
		{
			name:    "missing machine token",
			modify:  func(i *MachineConfigInput) { i.MachineToken = "" },
			wantErr: "machineToken",
		},
		{
			name:    "missing bootstrap token",
			modify:  func(i *MachineConfigInput) { i.BootstrapToken = "" },
			wantErr: "bootstrapToken",
		},
		{
			name:    "missing OS CA cert",
			modify:  func(i *MachineConfigInput) { i.OSCACert = "" },
			wantErr: "osCACert",
		},
		{
			name:    "missing service CIDR",
			modify:  func(i *MachineConfigInput) { i.ServiceCIDR = "" },
			wantErr: "serviceCIDR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := validInput()
			tt.modify(&input)

			_, err := GenerateWorkerConfig(input)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want containing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestDeriveClusterDNS(t *testing.T) {
	tests := []struct {
		cidr    string
		want    string
		wantErr bool
	}{
		{"10.96.0.0/12", "10.96.0.10", false},
		{"10.0.0.0/16", "10.0.0.10", false},
		{"172.16.0.0/16", "172.16.0.10", false},
		{"invalid", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.cidr, func(t *testing.T) {
			got, err := deriveClusterDNS(tt.cidr)
			if (err != nil) != tt.wantErr {
				t.Errorf("deriveClusterDNS(%q) error = %v, wantErr %v", tt.cidr, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("deriveClusterDNS(%q) = %v, want %v", tt.cidr, got, tt.want)
			}
		})
	}
}

func TestEndpointFromTCPStatus(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"10.40.0.1:6443", "https://10.40.0.1:6443"},
		{"api.example.com:6443", "https://api.example.com:6443"},
		{"https://10.40.0.1:6443", "https://10.40.0.1:6443"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := EndpointFromTCPStatus(tt.input)
			if got != tt.want {
				t.Errorf("EndpointFromTCPStatus(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestGenerateWorkerConfig_NoInstallerImage(t *testing.T) {
	input := validInput()
	input.InstallerImage = ""

	data, err := GenerateWorkerConfig(input)
	if err != nil {
		t.Fatalf("GenerateWorkerConfig() error = %v", err)
	}

	var config map[string]interface{}
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	machine := config["machine"].(map[string]interface{})
	install := machine["install"].(map[string]interface{})

	if _, ok := install["image"]; ok {
		t.Error("install.image should not be set when InstallerImage is empty")
	}
	if v := install["disk"]; v != "/dev/vda" {
		t.Errorf("install.disk = %v, want /dev/vda", v)
	}
}
