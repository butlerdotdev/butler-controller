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
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MachineInfo contains the name, IP, and annotations of a CAPI Machine.
type MachineInfo struct {
	Name        string
	IP          string
	Annotations map[string]string
}

// GetMachineAddresses lists CAPI Machine objects for a cluster and extracts their IP addresses.
// Returns a slice of MachineInfo with name, IP, and annotations for each Machine that has an address.
// Machines without addresses are skipped (they may not be provisioned yet).
func GetMachineAddresses(ctx context.Context, c client.Client, clusterName, namespace string) ([]MachineInfo, error) {
	machineList := &unstructured.UnstructuredList{}
	machineList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "cluster.x-k8s.io",
		Version: "v1beta1",
		Kind:    "MachineList",
	})

	selector := labels.SelectorFromSet(labels.Set{
		"cluster.x-k8s.io/cluster-name": clusterName,
	})

	if err := c.List(ctx, machineList, &client.ListOptions{
		Namespace:     namespace,
		LabelSelector: selector,
	}); err != nil {
		return nil, fmt.Errorf("listing CAPI Machines: %w", err)
	}

	var machines []MachineInfo
	for _, m := range machineList.Items {
		name := m.GetName()
		annotations := m.GetAnnotations()

		ip := extractMachineIP(m.Object)
		if ip == "" {
			continue
		}

		machines = append(machines, MachineInfo{
			Name:        name,
			IP:          ip,
			Annotations: annotations,
		})
	}

	return machines, nil
}

// extractMachineIP extracts the IP address from a CAPI Machine's status.addresses.
// Prefers InternalIP, falls back to ExternalIP.
func extractMachineIP(obj map[string]interface{}) string {
	addresses, found, _ := unstructured.NestedSlice(obj, "status", "addresses")
	if !found {
		return ""
	}

	var fallbackIP string
	for _, addr := range addresses {
		addrMap, ok := addr.(map[string]interface{})
		if !ok {
			continue
		}

		addrType, _ := addrMap["type"].(string)
		addrValue, _ := addrMap["address"].(string)
		if addrValue == "" {
			continue
		}

		if addrType == "InternalIP" {
			return addrValue
		}
		if addrType == "ExternalIP" && fallbackIP == "" {
			fallbackIP = addrValue
		}
	}

	return fallbackIP
}
