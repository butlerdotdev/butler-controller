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

// Package capi provides builders for Cluster API resources.
// This package uses unstructured resources to avoid tight coupling to specific
// CAPI provider versions while maintaining type safety through well-defined builders.
package capi

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// API Group/Version constants for CAPI resources.
const (
	ClusterAPIGroup   = "cluster.x-k8s.io"
	ClusterAPIVersion = "v1beta1"

	InfrastructureAPIGroup   = "infrastructure.cluster.x-k8s.io"
	InfrastructureAPIVersion = "v1alpha1"

	ControlPlaneAPIGroup   = "controlplane.cluster.x-k8s.io"
	ControlPlaneAPIVersion = "v1alpha1"

	BootstrapAPIGroup   = "bootstrap.cluster.x-k8s.io"
	BootstrapAPIVersion = "v1beta1"
)

// Default values for machine specs when not specified in TenantCluster
const (
	DefaultWorkerCPU      = 4
	DefaultWorkerMemoryMB = 8192 // 8Gi in MB
	DefaultWorkerDiskGB   = 40
)

// ResourceSet contains all CAPI resources needed for a tenant cluster.
type ResourceSet struct {
	Cluster                 *unstructured.Unstructured
	InfrastructureCluster   *unstructured.Unstructured
	ControlPlane            *unstructured.Unstructured
	MachineDeployment       *unstructured.Unstructured
	MachineTemplate         *unstructured.Unstructured
	BootstrapConfigTemplate *unstructured.Unstructured
	CredentialSecret        *unstructured.Unstructured
}

// Builder constructs CAPI resources for a TenantCluster.
type Builder struct {
	tc             *butlerv1alpha1.TenantCluster
	providerConfig *butlerv1alpha1.ProviderConfig
	namespace      string
	nutanixCreds   *NutanixCredentials
}

// NutanixCredentials holds Nutanix Prism Central credentials.
type NutanixCredentials struct {
	Username string
	Password string
}

// NewBuilder creates a new CAPI resource builder.
func NewBuilder(tc *butlerv1alpha1.TenantCluster, pc *butlerv1alpha1.ProviderConfig, namespace string) *Builder {
	return &Builder{
		tc:             tc,
		providerConfig: pc,
		namespace:      namespace,
	}
}

// WithNutanixCredentials sets the Nutanix credentials for CAPX secret generation.
// Must be called before Build() when using Nutanix provider.
func (b *Builder) WithNutanixCredentials(username, password string) *Builder {
	b.nutanixCreds = &NutanixCredentials{
		Username: username,
		Password: password,
	}
	return b
}

// Build constructs all CAPI resources for the tenant cluster.
func (b *Builder) Build() (*ResourceSet, error) {
	// Determine provider type and delegate to appropriate builder
	switch b.providerConfig.Spec.Provider {
	case butlerv1alpha1.ProviderTypeHarvester:
		return b.buildHarvesterResources()
	case butlerv1alpha1.ProviderTypeNutanix:
		return b.buildNutanixResources()
	default:
		return nil, fmt.Errorf("unsupported provider type: %s", b.providerConfig.Spec.Provider)
	}
}

// buildHarvesterResources constructs CAPI resources for Harvester provider.
// Uses KubeVirt CAPI provider (capk) for proper CPU/memory control.
func (b *Builder) buildHarvesterResources() (*ResourceSet, error) {
	clusterName := b.tc.Name

	// Build infrastructure cluster (KubevirtCluster)
	infraCluster := b.buildKubevirtCluster(clusterName)

	// Build control plane (KamajiControlPlane - unchanged)
	controlPlane := b.buildKamajiControlPlane(clusterName)

	// Build top-level Cluster
	cluster := b.buildCluster(clusterName, infraCluster, controlPlane)

	// Build machine template (KubevirtMachineTemplate)
	machineTemplate := b.buildKubevirtMachineTemplate(clusterName)

	// Build bootstrap config template
	bootstrapTemplate := b.buildKubeadmConfigTemplate(clusterName)

	// Build machine deployment
	machineDeployment := b.buildMachineDeployment(clusterName, machineTemplate, bootstrapTemplate)

	return &ResourceSet{
		Cluster:                 cluster,
		InfrastructureCluster:   infraCluster,
		ControlPlane:            controlPlane,
		MachineDeployment:       machineDeployment,
		MachineTemplate:         machineTemplate,
		BootstrapConfigTemplate: bootstrapTemplate,
	}, nil
}

// buildCluster constructs the top-level CAPI Cluster resource.
func (b *Builder) buildCluster(name string, infraRef, cpRef *unstructured.Unstructured) *unstructured.Unstructured {
	cluster := &unstructured.Unstructured{}
	cluster.SetAPIVersion(fmt.Sprintf("%s/%s", ClusterAPIGroup, ClusterAPIVersion))
	cluster.SetKind("Cluster")
	cluster.SetName(name)
	cluster.SetNamespace(b.namespace)
	cluster.SetLabels(b.commonLabels())

	// Get network CIDRs from spec or use defaults
	podCIDR := "10.244.0.0/16"
	serviceCIDR := "10.96.0.0/12"
	if b.tc.Spec.Networking.PodCIDR != "" {
		podCIDR = b.tc.Spec.Networking.PodCIDR
	}
	if b.tc.Spec.Networking.ServiceCIDR != "" {
		serviceCIDR = b.tc.Spec.Networking.ServiceCIDR
	}

	spec := map[string]interface{}{
		"clusterNetwork": map[string]interface{}{
			"pods": map[string]interface{}{
				"cidrBlocks": []interface{}{podCIDR},
			},
			"services": map[string]interface{}{
				"cidrBlocks": []interface{}{serviceCIDR},
			},
		},
		"controlPlaneRef": map[string]interface{}{
			"apiVersion": cpRef.GetAPIVersion(),
			"kind":       cpRef.GetKind(),
			"name":       cpRef.GetName(),
			"namespace":  cpRef.GetNamespace(),
		},
		"infrastructureRef": map[string]interface{}{
			"apiVersion": infraRef.GetAPIVersion(),
			"kind":       infraRef.GetKind(),
			"name":       infraRef.GetName(),
			"namespace":  infraRef.GetNamespace(),
		},
	}

	cluster.Object["spec"] = spec
	return cluster
}

// buildKubevirtCluster constructs the KubevirtCluster infrastructure resource.
// Uses capk (cluster-api-provider-kubevirt) instead of caphv for proper CPU/memory control.
func (b *Builder) buildKubevirtCluster(name string) *unstructured.Unstructured {
	kvCluster := &unstructured.Unstructured{}
	kvCluster.SetAPIVersion(fmt.Sprintf("%s/%s", InfrastructureAPIGroup, InfrastructureAPIVersion))
	kvCluster.SetKind("KubevirtCluster")
	kvCluster.SetName(name)
	kvCluster.SetNamespace(b.namespace)
	kvCluster.SetLabels(b.commonLabels())

	// Get credentials reference namespace, default to butler-system
	credsNS := b.providerConfig.Spec.CredentialsRef.Namespace
	if credsNS == "" {
		credsNS = "butler-system"
	}

	// Get the key within the secret, default to "kubeconfig"
	credsKey := b.providerConfig.Spec.CredentialsRef.Key
	if credsKey == "" {
		credsKey = "kubeconfig"
	}

	spec := map[string]interface{}{
		// Reference to Harvester kubeconfig for capk to create VMs
		"infraClusterSecretRef": map[string]interface{}{
			"name":       b.providerConfig.Spec.CredentialsRef.Name,
			"namespace":  credsNS,
			"apiVersion": "v1",
			"kind":       "Secret",
		},
		// Control plane service template - Kamaji handles the actual control plane
		// but capk needs this for cluster readiness
		"controlPlaneServiceTemplate": map[string]interface{}{
			"spec": map[string]interface{}{
				"type": "ClusterIP",
			},
		},
	}

	kvCluster.Object["spec"] = spec
	return kvCluster
}

// buildKamajiControlPlane constructs the KamajiControlPlane resource.
func (b *Builder) buildKamajiControlPlane(name string) *unstructured.Unstructured {
	kcp := &unstructured.Unstructured{}
	kcp.SetAPIVersion(fmt.Sprintf("%s/%s", ControlPlaneAPIGroup, ControlPlaneAPIVersion))
	kcp.SetKind("KamajiControlPlane")
	kcp.SetName(name)
	kcp.SetNamespace(b.namespace)
	kcp.SetLabels(b.commonLabels())

	// Get control plane replicas from spec
	replicas := int64(1)
	if b.tc.Spec.ControlPlane.Replicas > 0 {
		replicas = int64(b.tc.Spec.ControlPlane.Replicas)
	}

	// Get datastore name
	dataStoreName := "default"
	if b.tc.Spec.ControlPlane.DataStoreRef != nil && b.tc.Spec.ControlPlane.DataStoreRef.Name != "" {
		dataStoreName = b.tc.Spec.ControlPlane.DataStoreRef.Name
	}

	// Get service type
	serviceType := "LoadBalancer"
	if b.tc.Spec.ControlPlane.ServiceType != "" {
		serviceType = b.tc.Spec.ControlPlane.ServiceType
	}

	// See: https://kamaji.clastix.io/cluster-api/kamaji-control-plane-provider/
	spec := map[string]interface{}{
		"version":       b.tc.Spec.KubernetesVersion,
		"replicas":      replicas,
		"dataStoreName": dataStoreName,

		// Addons - Kamaji manages these automatically
		// konnectivity is enabled by default, version matched automatically
		"addons": map[string]interface{}{
			"coreDNS":      map[string]interface{}{},
			"kubeProxy":    map[string]interface{}{},
			"konnectivity": map[string]interface{}{},
		},

		// Kubelet configuration for Rocky Linux / systemd
		"kubelet": map[string]interface{}{
			"cgroupfs": "systemd",
			"preferredAddressTypes": []interface{}{
				"InternalIP",
				"ExternalIP",
				"Hostname",
			},
		},

		// Network configuration
		"network": map[string]interface{}{
			"serviceType": serviceType,
		},
	}

	// Add certSANs if specified
	if len(b.tc.Spec.ControlPlane.CertSANs) > 0 {
		certSANs := make([]interface{}, len(b.tc.Spec.ControlPlane.CertSANs))
		for i, san := range b.tc.Spec.ControlPlane.CertSANs {
			certSANs[i] = san
		}
		spec["network"].(map[string]interface{})["certSANs"] = certSANs
	}

	kcp.Object["spec"] = spec
	return kcp
}

// buildKubevirtMachineTemplate constructs the KubevirtMachineTemplate resource.
// Uses full KubeVirt VM spec for proper CPU topology and resource limits.
func (b *Builder) buildKubevirtMachineTemplate(name string) *unstructured.Unstructured {
	kvmt := &unstructured.Unstructured{}
	kvmt.SetAPIVersion(fmt.Sprintf("%s/%s", InfrastructureAPIGroup, InfrastructureAPIVersion))
	kvmt.SetKind("KubevirtMachineTemplate")
	kvmt.SetName(fmt.Sprintf("%s-worker", name))
	kvmt.SetNamespace(b.namespace)
	kvmt.SetLabels(b.commonLabels())

	harvesterSpec := b.providerConfig.Spec.Harvester

	// Get target namespace in Harvester for VMs
	targetNS := harvesterSpec.Namespace
	if targetNS == "" {
		targetNS = "default"
	}

	// Check infrastructureOverride first (per-cluster override)
	if b.tc.Spec.InfrastructureOverride != nil &&
		b.tc.Spec.InfrastructureOverride.Harvester != nil &&
		b.tc.Spec.InfrastructureOverride.Harvester.Namespace != "" {
		targetNS = b.tc.Spec.InfrastructureOverride.Harvester.Namespace
	}
	// Network name for Multus - must be namespace/name format
	networkName := harvesterSpec.NetworkName
	if networkName == "" {
		networkName = "default/vlan40-workloads"
	}

	// Check infrastructureOverride first
	if b.tc.Spec.InfrastructureOverride != nil &&
		b.tc.Spec.InfrastructureOverride.Harvester != nil &&
		b.tc.Spec.InfrastructureOverride.Harvester.NetworkName != "" {
		networkName = b.tc.Spec.InfrastructureOverride.Harvester.NetworkName
	}

	// Image name - Harvester image ID format namespace/name
	imageName := harvesterSpec.ImageName
	if imageName == "" {
		imageName = "default/image-b8c6x"
	}
	// Check infrastructureOverride first (per-cluster override takes precedence)
	if b.tc.Spec.InfrastructureOverride != nil &&
		b.tc.Spec.InfrastructureOverride.Harvester != nil &&
		b.tc.Spec.InfrastructureOverride.Harvester.ImageName != "" {
		imageName = b.tc.Spec.InfrastructureOverride.Harvester.ImageName
	} else if b.tc.Spec.Workers.MachineTemplate.OS.ImageRef != "" {
		// Fall back to OS.ImageRef
		imageName = b.tc.Spec.Workers.MachineTemplate.OS.ImageRef
	}

	// Parse image ID for storage class name
	_, imageID := parseImageNamespace(imageName)

	// Storage class derived from image name (Harvester pattern: longhorn-<image-id>)
	storageClassName := fmt.Sprintf("longhorn-%s", imageID)

	// Get credentials reference for infraClusterSecretRef
	credsNS := b.providerConfig.Spec.CredentialsRef.Namespace
	if credsNS == "" {
		credsNS = "butler-system"
	}

	// Get machine specs from TenantCluster
	cpu, memoryStr, diskStr := b.getMachineSpecs()

	// Disk name for the root volume
	diskName := fmt.Sprintf("%s-worker-disk", name)

	spec := map[string]interface{}{
		"template": map[string]interface{}{
			"spec": map[string]interface{}{
				// Reference to Harvester kubeconfig
				"infraClusterSecretRef": map[string]interface{}{
					"name":      b.providerConfig.Spec.CredentialsRef.Name,
					"namespace": credsNS,
				},
				// Full KubeVirt VirtualMachine template
				"virtualMachineTemplate": map[string]interface{}{
					"metadata": map[string]interface{}{
						"namespace": targetNS,
						"labels": map[string]interface{}{
							"harvesterhci.io/creator":          "harvester",
							"butler.butlerlabs.dev/managed-by": "butler",
							"butler.butlerlabs.dev/tenant":     b.tc.Name,
						},
					},
					"spec": map[string]interface{}{
						"runStrategy": "Always",
						// DataVolumeTemplates for disk provisioning
						// Use blank source + annotation for Harvester backing image
						"dataVolumeTemplates": []interface{}{
							map[string]interface{}{
								"metadata": map[string]interface{}{
									"name": diskName,
									"annotations": map[string]interface{}{
										"harvesterhci.io/imageId": imageName,
									},
								},
								"spec": map[string]interface{}{
									// Blank source passes KubeVirt validation
									// Harvester's storage class + annotation handles actual image
									"source": map[string]interface{}{
										"blank": map[string]interface{}{},
									},
									"storage": map[string]interface{}{
										"storageClassName": storageClassName,
										"accessModes": []interface{}{
											"ReadWriteMany",
										},
										"volumeMode": "Block",
										"resources": map[string]interface{}{
											"requests": map[string]interface{}{
												"storage": diskStr,
											},
										},
									},
								},
							},
						},
						"template": map[string]interface{}{
							"spec": map[string]interface{}{
								"domain": map[string]interface{}{
									// Proper CPU topology - cores only, no multiplication
									"cpu": map[string]interface{}{
										"cores":   cpu,
										"sockets": int64(1),
										"threads": int64(1),
									},
									// Resource requests AND limits for proper display
									"resources": map[string]interface{}{
										"requests": map[string]interface{}{
											"memory": memoryStr,
										},
										"limits": map[string]interface{}{
											"memory": memoryStr,
											"cpu":    fmt.Sprintf("%d", cpu),
										},
									},
									"devices": map[string]interface{}{
										"disks": []interface{}{
											map[string]interface{}{
												"name": "rootdisk",
												"disk": map[string]interface{}{
													"bus": "virtio",
												},
											},
										},
										"interfaces": []interface{}{
											map[string]interface{}{
												"name":   "default",
												"bridge": map[string]interface{}{},
											},
										},
									},
									"machine": map[string]interface{}{
										"type": "q35",
									},
								},
								"networks": []interface{}{
									map[string]interface{}{
										"name": "default",
										"multus": map[string]interface{}{
											"networkName": networkName,
										},
									},
								},
								"volumes": []interface{}{
									map[string]interface{}{
										"name": "rootdisk",
										"dataVolume": map[string]interface{}{
											"name": diskName,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	kvmt.Object["spec"] = spec
	return kvmt
}

// buildNutanixResources constructs CAPI resources for Nutanix provider.
// Uses CAPX (cluster-api-provider-nutanix) with Kamaji hosted control planes.
func (b *Builder) buildNutanixResources() (*ResourceSet, error) {
	// Validate Nutanix credentials are provided
	if b.nutanixCreds == nil {
		return nil, fmt.Errorf("Nutanix credentials not provided; call WithNutanixCredentials() before Build()")
	}
	if b.nutanixCreds.Username == "" || b.nutanixCreds.Password == "" {
		return nil, fmt.Errorf("Nutanix username and password are required")
	}

	clusterName := b.tc.Name

	// Build credential secret first (NutanixCluster references it)
	credentialSecret := b.buildNutanixCredentialSecret(clusterName)

	// Build infrastructure cluster (NutanixCluster)
	infraCluster := b.buildNutanixCluster(clusterName)

	// Build control plane (KamajiControlPlane - shared across providers)
	controlPlane := b.buildKamajiControlPlane(clusterName)

	// Build top-level Cluster
	cluster := b.buildCluster(clusterName, infraCluster, controlPlane)

	// Build machine template (NutanixMachineTemplate)
	machineTemplate := b.buildNutanixMachineTemplate(clusterName)

	// Build bootstrap config template (shared - Rocky Linux)
	bootstrapTemplate := b.buildKubeadmConfigTemplate(clusterName)

	// Build machine deployment (shared)
	machineDeployment := b.buildMachineDeployment(clusterName, machineTemplate, bootstrapTemplate)

	return &ResourceSet{
		Cluster:                 cluster,
		InfrastructureCluster:   infraCluster,
		ControlPlane:            controlPlane,
		MachineDeployment:       machineDeployment,
		MachineTemplate:         machineTemplate,
		BootstrapConfigTemplate: bootstrapTemplate,
		CredentialSecret:        credentialSecret,
	}, nil
}

// buildNutanixCluster constructs the NutanixCluster infrastructure resource.
func (b *Builder) buildNutanixCluster(name string) *unstructured.Unstructured {
	nxCluster := &unstructured.Unstructured{}
	// CAPX uses v1beta1, not v1alpha1 like capk
	nxCluster.SetAPIVersion(fmt.Sprintf("%s/v1beta1", InfrastructureAPIGroup))
	nxCluster.SetKind("NutanixCluster")
	nxCluster.SetName(name)
	nxCluster.SetNamespace(b.namespace)
	nxCluster.SetLabels(b.commonLabels())

	nutanixSpec := b.providerConfig.Spec.Nutanix

	// Parse endpoint to get address without scheme
	address := nutanixSpec.Endpoint
	address = strings.TrimPrefix(address, "https://")
	address = strings.TrimPrefix(address, "http://")

	// Get port, default to 9440
	port := int64(9440)
	if nutanixSpec.Port > 0 {
		port = int64(nutanixSpec.Port)
	}

	// Credential secret name follows pattern: <cluster-name>-nutanix-creds
	credSecretName := fmt.Sprintf("%s-nutanix-creds", name)

	spec := map[string]interface{}{
		// Control plane endpoint - will be patched by Kamaji when LoadBalancer gets IP
		"controlPlaneEndpoint": map[string]interface{}{
			"host": "",
			"port": int64(6443),
		},
		// Prism Central configuration
		"prismCentral": map[string]interface{}{
			"address":  address,
			"port":     port,
			"insecure": nutanixSpec.Insecure,
			"credentialRef": map[string]interface{}{
				"kind":      "Secret",
				"name":      credSecretName,
				"namespace": b.namespace,
			},
		},
	}

	nxCluster.Object["spec"] = spec
	return nxCluster
}

// buildNutanixMachineTemplate constructs the NutanixMachineTemplate resource.
func (b *Builder) buildNutanixMachineTemplate(name string) *unstructured.Unstructured {
	nxmt := &unstructured.Unstructured{}
	// CAPX uses v1beta1
	nxmt.SetAPIVersion(fmt.Sprintf("%s/v1beta1", InfrastructureAPIGroup))
	nxmt.SetKind("NutanixMachineTemplate")
	nxmt.SetName(fmt.Sprintf("%s-worker", name))
	nxmt.SetNamespace(b.namespace)
	nxmt.SetLabels(b.commonLabels())

	nutanixSpec := b.providerConfig.Spec.Nutanix

	// Get machine specs from TenantCluster
	cpu, memoryStr, diskStr := b.getMachineSpecs()

	// CAPX uses vcpuSockets * vcpusPerSocket for total vCPUs
	vcpuSockets := cpu
	vcpusPerSocket := int64(1)

	// Determine image UUID - TenantCluster spec takes precedence over ProviderConfig
	imageUUID := nutanixSpec.ImageUUID
	if b.tc.Spec.InfrastructureOverride != nil &&
		b.tc.Spec.InfrastructureOverride.Nutanix != nil &&
		b.tc.Spec.InfrastructureOverride.Nutanix.ImageUUID != "" {
		imageUUID = b.tc.Spec.InfrastructureOverride.Nutanix.ImageUUID
	} else if b.tc.Spec.Workers.MachineTemplate.OS.ImageRef != "" {
		imageUUID = b.tc.Spec.Workers.MachineTemplate.OS.ImageRef
	}

	spec := map[string]interface{}{
		"template": map[string]interface{}{
			"spec": map[string]interface{}{
				"bootType":       "legacy",
				"vcpuSockets":    vcpuSockets,
				"vcpusPerSocket": vcpusPerSocket,
				"memorySize":     memoryStr,
				"systemDiskSize": diskStr,
				// Image reference by UUID
				"image": map[string]interface{}{
					"type": "uuid",
					"uuid": imageUUID,
				},
				// Prism Element cluster reference by UUID
				"cluster": map[string]interface{}{
					"type": "uuid",
					"uuid": nutanixSpec.ClusterUUID,
				},
				// Subnet reference by UUID
				"subnet": []interface{}{
					map[string]interface{}{
						"type": "uuid",
						"uuid": nutanixSpec.SubnetUUID,
					},
				},
			},
		},
	}

	nxmt.Object["spec"] = spec
	return nxmt
}

// buildNutanixCredentialSecret constructs the Secret containing Nutanix credentials
// in the format required by CAPX (JSON array with basic_auth type).
func (b *Builder) buildNutanixCredentialSecret(name string) *unstructured.Unstructured {
	secret := &unstructured.Unstructured{}
	secret.SetAPIVersion("v1")
	secret.SetKind("Secret")
	secret.SetName(fmt.Sprintf("%s-nutanix-creds", name))
	secret.SetNamespace(b.namespace)
	secret.SetLabels(b.commonLabels())

	// CAPX expects credentials in this specific JSON format
	credentialsJSON := fmt.Sprintf(`[{"type":"basic_auth","data":{"prismCentral":{"username":"%s","password":"%s"}}}]`,
		b.nutanixCreds.Username,
		b.nutanixCreds.Password,
	)

	secret.Object["type"] = "Opaque"
	secret.Object["stringData"] = map[string]interface{}{
		"credentials": credentialsJSON,
	}

	return secret
}

// parseImageNamespace extracts namespace and image ID from namespace/name format.
// e.g., "default/image-b8c6x" -> ("default", "image-b8c6x")
func parseImageNamespace(imageName string) (string, string) {
	parts := strings.Split(imageName, "/")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "default", imageName
}

// getMachineSpecs extracts CPU, Memory, and Disk from TenantCluster spec.
// Returns values formatted for HarvesterMachineTemplate.
func (b *Builder) getMachineSpecs() (cpu int64, memory string, disk string) {
	// Defaults
	cpu = DefaultWorkerCPU
	memoryMB := DefaultWorkerMemoryMB
	diskGB := DefaultWorkerDiskGB

	// Check if MachineTemplate is specified
	mt := b.tc.Spec.Workers.MachineTemplate

	// CPU - direct int32 field
	if mt.CPU > 0 {
		cpu = int64(mt.CPU)
	}

	// Memory - resource.Quantity, need to convert to string for Harvester
	// Harvester expects format like "8Gi" or "8192Mi"
	if !mt.Memory.IsZero() {
		// Get value in bytes and convert to Gi for cleaner output
		memBytes := mt.Memory.Value()
		memGi := memBytes / (1024 * 1024 * 1024)
		if memGi > 0 {
			memory = fmt.Sprintf("%dGi", memGi)
		} else {
			// Fall back to Mi if less than 1Gi
			memMi := memBytes / (1024 * 1024)
			memory = fmt.Sprintf("%dMi", memMi)
		}
	} else {
		// Use default in Mi format
		memory = fmt.Sprintf("%dMi", memoryMB)
	}

	// DiskSize - resource.Quantity, convert to string for Harvester volumeSize
	if !mt.DiskSize.IsZero() {
		diskBytes := mt.DiskSize.Value()
		diskGiB := diskBytes / (1024 * 1024 * 1024)
		if diskGiB > 0 {
			disk = fmt.Sprintf("%dGi", diskGiB)
		} else {
			// Unlikely but handle sub-Gi disks
			diskMi := diskBytes / (1024 * 1024)
			disk = fmt.Sprintf("%dMi", diskMi)
		}
	} else {
		disk = fmt.Sprintf("%dGi", diskGB)
	}

	return cpu, memory, disk
}

// buildKubeadmConfigTemplate constructs the KubeadmConfigTemplate with Rocky Linux bootstrap.
func (b *Builder) buildKubeadmConfigTemplate(name string) *unstructured.Unstructured {
	kct := &unstructured.Unstructured{}
	kct.SetAPIVersion(fmt.Sprintf("%s/%s", BootstrapAPIGroup, BootstrapAPIVersion))
	kct.SetKind("KubeadmConfigTemplate")
	kct.SetName(fmt.Sprintf("%s-worker", name))
	kct.SetNamespace(b.namespace)
	kct.SetLabels(b.commonLabels())

	k8sVersion := b.tc.Spec.KubernetesVersion
	k8sMinorVersion := extractMinorVersion(k8sVersion)

	spec := map[string]interface{}{
		"template": map[string]interface{}{
			"spec": map[string]interface{}{
				"joinConfiguration": map[string]interface{}{
					"nodeRegistration": map[string]interface{}{
						"imagePullPolicy": "IfNotPresent",
					},
				},
				"preKubeadmCommands": b.buildRockyLinuxBootstrapCommands(k8sMinorVersion),
				"files":              b.buildBootstrapFiles(k8sMinorVersion),
			},
		},
	}

	kct.Object["spec"] = spec
	return kct
}

// buildRockyLinuxBootstrapCommands returns the preKubeadmCommands for Rocky Linux k8s node bootstrap.
func (b *Builder) buildRockyLinuxBootstrapCommands(k8sMinorVersion string) []interface{} {
	return []interface{}{
		// Kernel modules for container networking
		"modprobe overlay",
		"modprobe br_netfilter",

		// Persist kernel modules
		"echo 'overlay' >> /etc/modules-load.d/k8s.conf",
		"echo 'br_netfilter' >> /etc/modules-load.d/k8s.conf",

		// Sysctl params for k8s networking
		"sysctl --system",

		// Install containerd from Docker repo
		"dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo",
		"dnf install -y containerd.io",

		// Configure containerd with systemd cgroup driver
		"containerd config default | tee /etc/containerd/config.toml",
		"sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml",
		"systemctl enable --now containerd",

		// Install kubeadm, kubelet, kubectl from official Kubernetes repo
		fmt.Sprintf(`cat <<EOF | tee /etc/yum.repos.d/kubernetes.repo
[kubernetes]
name=Kubernetes
baseurl=https://pkgs.k8s.io/core:/stable:/%s/rpm/
enabled=1
gpgcheck=1
gpgkey=https://pkgs.k8s.io/core:/stable:/%s/rpm/repodata/repomd.xml.key
exclude=kubelet kubeadm kubectl cri-tools kubernetes-cni
EOF`, k8sMinorVersion, k8sMinorVersion),

		"dnf install -y kubelet kubeadm kubectl --disableexcludes=kubernetes",
		"systemctl enable kubelet",

		// Install iSCSI tools for Longhorn storage (required dependency)
		"dnf install -y iscsi-initiator-utils nfs-utils",
		"systemctl enable iscsid --now",

		// Disable swap (required for kubelet)
		"swapoff -a",
		"sed -i '/ swap / s/^/#/' /etc/fstab",

		// Disable SELinux (or set to permissive) for k8s compatibility
		"setenforce 0 || true",
		"sed -i 's/^SELINUX=enforcing$/SELINUX=permissive/' /etc/selinux/config",

		// Disable firewalld (CNI manages networking)
		"systemctl disable --now firewalld || true",
	}
}

// buildBootstrapFiles returns additional files to create during bootstrap.
func (b *Builder) buildBootstrapFiles(k8sMinorVersion string) []interface{} {
	return []interface{}{
		// Sysctl configuration for k8s networking
		map[string]interface{}{
			"path":        "/etc/sysctl.d/k8s.conf",
			"permissions": "0644",
			"content": `net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
`,
		},
	}
}

// buildMachineDeployment constructs the MachineDeployment resource.
func (b *Builder) buildMachineDeployment(name string, machineTemplate, bootstrapTemplate *unstructured.Unstructured) *unstructured.Unstructured {
	md := &unstructured.Unstructured{}
	md.SetAPIVersion(fmt.Sprintf("%s/%s", ClusterAPIGroup, ClusterAPIVersion))
	md.SetKind("MachineDeployment")
	md.SetName(fmt.Sprintf("%s-workers", name))
	md.SetNamespace(b.namespace)
	md.SetLabels(b.commonLabels())

	replicas := int64(b.tc.Spec.Workers.Replicas)
	if replicas < 1 {
		replicas = 1
	}

	spec := map[string]interface{}{
		"clusterName": name,
		"replicas":    replicas,
		"selector": map[string]interface{}{
			"matchLabels": map[string]interface{}{
				"cluster.x-k8s.io/cluster-name":    name,
				"cluster.x-k8s.io/deployment-name": fmt.Sprintf("%s-workers", name),
			},
		},
		"template": map[string]interface{}{
			"metadata": map[string]interface{}{
				"labels": map[string]interface{}{
					"cluster.x-k8s.io/cluster-name":    name,
					"cluster.x-k8s.io/deployment-name": fmt.Sprintf("%s-workers", name),
				},
			},
			"spec": map[string]interface{}{
				"clusterName": name,
				"version":     b.tc.Spec.KubernetesVersion,
				"bootstrap": map[string]interface{}{
					"configRef": map[string]interface{}{
						"apiVersion": bootstrapTemplate.GetAPIVersion(),
						"kind":       bootstrapTemplate.GetKind(),
						"name":       bootstrapTemplate.GetName(),
						"namespace":  bootstrapTemplate.GetNamespace(),
					},
				},
				"infrastructureRef": map[string]interface{}{
					"apiVersion": machineTemplate.GetAPIVersion(),
					"kind":       machineTemplate.GetKind(),
					"name":       machineTemplate.GetName(),
					"namespace":  machineTemplate.GetNamespace(),
				},
			},
		},
	}

	md.Object["spec"] = spec
	return md
}

// commonLabels returns labels applied to all CAPI resources.
func (b *Builder) commonLabels() map[string]string {
	labels := map[string]string{
		butlerv1alpha1.LabelManagedBy:       "butler",
		butlerv1alpha1.LabelTenant:          b.tc.Name,
		butlerv1alpha1.LabelSourceNamespace: b.tc.Namespace,
		butlerv1alpha1.LabelSourceName:      b.tc.Name,
		"cluster.x-k8s.io/cluster-name":     b.tc.Name,
	}

	if b.tc.Spec.TeamRef != nil && b.tc.Spec.TeamRef.Name != "" {
		labels[butlerv1alpha1.LabelTeam] = b.tc.Spec.TeamRef.Name
	}

	return labels
}

// extractMinorVersion extracts the minor version from a Kubernetes version string.
func extractMinorVersion(version string) string {
	v := version
	if len(v) > 0 && v[0] != 'v' {
		v = "v" + v
	}

	// Count dots to determine if we have major.minor or major.minor.patch
	dotCount := 0
	for _, c := range v {
		if c == '.' {
			dotCount++
		}
	}

	// If only one dot (major.minor), return as-is
	if dotCount <= 1 {
		return v
	}

	// Two or more dots (major.minor.patch), find the last dot and truncate
	lastDot := -1
	for i := len(v) - 1; i >= 0; i-- {
		if v[i] == '.' {
			lastDot = i
			break
		}
	}

	if lastDot > 0 {
		return v[:lastDot]
	}
	return v
}

// GetOwnerReference returns an OwnerReference for the TenantCluster.
func (b *Builder) GetOwnerReference() metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         b.tc.APIVersion,
		Kind:               b.tc.Kind,
		Name:               b.tc.Name,
		UID:                b.tc.UID,
		Controller:         ptrBool(true),
		BlockOwnerDeletion: ptrBool(true),
	}
}

func ptrBool(b bool) *bool {
	return &b
}

// SetOwnerReferences sets owner references on all resources in the set.
func (rs *ResourceSet) SetOwnerReferences(ref metav1.OwnerReference) {
	resources := []*unstructured.Unstructured{
		rs.Cluster,
		rs.InfrastructureCluster,
		rs.ControlPlane,
		rs.MachineDeployment,
		rs.MachineTemplate,
		rs.BootstrapConfigTemplate,
		rs.CredentialSecret,
	}

	for _, r := range resources {
		if r != nil {
			r.SetOwnerReferences([]metav1.OwnerReference{ref})
		}
	}
}

// AllResources returns all resources as a slice for iteration.
// Order matters: credential secret must exist before NutanixCluster references it.
func (rs *ResourceSet) AllResources() []*unstructured.Unstructured {
	return []*unstructured.Unstructured{
		rs.CredentialSecret,        // Create credentials first (CAPX needs this)
		rs.InfrastructureCluster,   // Create infrastructure cluster
		rs.BootstrapConfigTemplate, // Then bootstrap config
		rs.MachineTemplate,         // Then machine template
		rs.ControlPlane,            // Then control plane
		rs.MachineDeployment,       // Then machine deployment
		rs.Cluster,                 // Finally the top-level cluster (references others)
	}
}
