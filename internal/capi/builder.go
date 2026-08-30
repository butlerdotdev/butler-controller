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
	"github.com/butlerdotdev/butler-controller/internal/talos"
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
	butlerConfig   *butlerv1alpha1.ButlerConfig
	namespace      string
	nutanixCreds   *NutanixCredentials
	ingressIP      string // External IP for Ingress/Gateway mode TLS passthrough
}

// NutanixCredentials holds Nutanix Prism Central credentials.
type NutanixCredentials struct {
	Username string
	Password string
	CABundle string // PEM-encoded CA chain for Prism Central TLS verification
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
func (b *Builder) WithNutanixCredentials(username, password, caBundle string) *Builder {
	b.nutanixCreds = &NutanixCredentials{
		Username: username,
		Password: password,
		CABundle: caBundle,
	}
	return b
}

// WithButlerConfig sets the ButlerConfig for platform-level settings.
// This enables reading ControlPlaneExposure for tcp-proxy auto-enablement.
func (b *Builder) WithButlerConfig(bc *butlerv1alpha1.ButlerConfig) *Builder {
	b.butlerConfig = bc
	return b
}

// WithIngressIP sets the external Ingress IP for TLS passthrough.
// This is used for Ingress/Gateway modes to configure worker /etc/hosts.
func (b *Builder) WithIngressIP(ip string) *Builder {
	b.ingressIP = ip
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
	case butlerv1alpha1.ProviderTypeLocal:
		return b.buildLocalResources()
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

	// Build control plane (StewardControlPlane - unchanged)
	controlPlane := b.buildStewardControlPlane(clusterName)

	// Build top-level Cluster
	cluster := b.buildCluster(clusterName, infraCluster, controlPlane)

	// Build machine template (KubevirtMachineTemplate)
	machineTemplate := b.buildKubevirtMachineTemplate(clusterName)

	// Build bootstrap config template (skip for Talos - uses dataSecretName instead)
	var bootstrapTemplate *unstructured.Unstructured
	if !b.isTalosCluster() {
		bootstrapTemplate = b.buildKubeadmConfigTemplate(clusterName)
	}

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

// buildLocalResources constructs CAPI resources for the local provider.
// Worker nodes run as containers via the Cluster API Docker provider (CAPD) on a
// kind-based management cluster. The control plane is a Steward TenantControlPlane
// (pods), identical to every other provider. The local provider is always kubeadm
// (never Talos), so the bootstrap template is always created and the MachineDeployment
// is created in one pass (the non-Talos 1-phase path in reconcile_infrastructure).
func (b *Builder) buildLocalResources() (*ResourceSet, error) {
	clusterName := b.tc.Name

	// Infrastructure cluster (DockerCluster).
	infraCluster := b.buildDockerCluster(clusterName)

	// Control plane (StewardControlPlane - shared across providers).
	controlPlane := b.buildStewardControlPlane(clusterName)

	// Top-level Cluster.
	cluster := b.buildCluster(clusterName, infraCluster, controlPlane)

	// Machine template (DockerMachineTemplate).
	machineTemplate := b.buildDockerMachineTemplate(clusterName)

	// Bootstrap config template (CAPD-specific minimal kubeadm join).
	bootstrapTemplate := b.buildCAPDKubeadmConfigTemplate(clusterName)

	// Machine deployment (shared).
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

// buildDockerCluster constructs the DockerCluster infrastructure resource (CAPD).
// CAPD uses the v1beta1 infrastructure contract, like CAPX.
func (b *Builder) buildDockerCluster(name string) *unstructured.Unstructured {
	dc := &unstructured.Unstructured{}
	dc.SetAPIVersion(fmt.Sprintf("%s/v1beta1", InfrastructureAPIGroup))
	dc.SetKind("DockerCluster")
	dc.SetName(name)
	dc.SetNamespace(b.namespace)
	b.applyCommonMetadata(dc)

	// Control plane endpoint is owned by Steward, not CAPD: the StewardControlPlane
	// reports the endpoint once its Service has an address, mirroring the CAPX pattern
	// (empty host, patched later). CAPD would otherwise provision an HAProxy load
	// balancer container for the control plane, which is unnecessary here because the
	// control plane is the external Steward TenantControlPlane.
	// NOTE: whether CAPD honors a pre-set endpoint or insists on its own LB must be
	// validated live when CAPD is installed on the management cluster (PR 2.3).
	spec := map[string]interface{}{
		"controlPlaneEndpoint": map[string]interface{}{
			"host": "",
			"port": int64(6443),
		},
	}

	dc.Object["spec"] = spec
	return dc
}

// buildDockerMachineTemplate constructs the DockerMachineTemplate for worker nodes (CAPD).
// CAPD runs each node as a container from a kindest/node image whose tag must match the
// tenant Kubernetes version.
func (b *Builder) buildDockerMachineTemplate(name string) *unstructured.Unstructured {
	dmt := &unstructured.Unstructured{}
	dmt.SetAPIVersion(fmt.Sprintf("%s/v1beta1", InfrastructureAPIGroup))
	dmt.SetKind("DockerMachineTemplate")
	dmt.SetName(fmt.Sprintf("%s-worker", name))
	dmt.SetNamespace(b.namespace)
	b.applyCommonMetadata(dmt)

	spec := map[string]interface{}{
		"template": map[string]interface{}{
			"spec": map[string]interface{}{
				"customImage": b.localNodeImage(),
			},
		},
	}

	dmt.Object["spec"] = spec
	return dmt
}

// buildCAPDKubeadmConfigTemplate constructs a minimal kubeadm join template for CAPD nodes.
// Unlike buildKubeadmConfigTemplate (Rocky Linux), CAPD nodes boot from a pre-baked
// kindest/node image where containerd, kubelet, and kubeadm already exist, so there are no
// package-install preKubeadmCommands. The eviction-hard override is required because kind
// nodes share the host filesystem and the default eviction thresholds would otherwise fire
// immediately and keep the node NotReady.
func (b *Builder) buildCAPDKubeadmConfigTemplate(name string) *unstructured.Unstructured {
	kct := &unstructured.Unstructured{}
	kct.SetAPIVersion(fmt.Sprintf("%s/%s", BootstrapAPIGroup, BootstrapAPIVersion))
	kct.SetKind("KubeadmConfigTemplate")
	kct.SetName(fmt.Sprintf("%s-worker", name))
	kct.SetNamespace(b.namespace)
	b.applyCommonMetadata(kct)

	spec := map[string]interface{}{
		"template": map[string]interface{}{
			"spec": map[string]interface{}{
				"joinConfiguration": map[string]interface{}{
					"nodeRegistration": map[string]interface{}{
						"criSocket": "unix:///var/run/containerd/containerd.sock",
						"kubeletExtraArgs": map[string]interface{}{
							"eviction-hard": "nodefs.available<0%,nodefs.inodesFree<0%,imagefs.available<0%",
						},
					},
				},
			},
		},
	}

	kct.Object["spec"] = spec
	return kct
}

// localNodeImage returns the kindest/node image for tenant worker containers. It honors an
// explicit override on the local ProviderConfig and otherwise derives the tag from the
// tenant Kubernetes version.
func (b *Builder) localNodeImage() string {
	if b.providerConfig.Spec.Local != nil && b.providerConfig.Spec.Local.KindNodeImage != "" {
		return b.providerConfig.Spec.Local.KindNodeImage
	}
	version := b.tc.Spec.KubernetesVersion
	if !strings.HasPrefix(version, "v") {
		version = "v" + version
	}
	return "kindest/node:" + version
}

// buildCluster constructs the top-level CAPI Cluster resource.
func (b *Builder) buildCluster(name string, infraRef, cpRef *unstructured.Unstructured) *unstructured.Unstructured {
	cluster := &unstructured.Unstructured{}
	cluster.SetAPIVersion(fmt.Sprintf("%s/%s", ClusterAPIGroup, ClusterAPIVersion))
	cluster.SetKind("Cluster")
	cluster.SetName(name)
	cluster.SetNamespace(b.namespace)
	b.applyCommonMetadata(cluster)

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
	b.applyCommonMetadata(kvCluster)

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
		// Control plane service template - Steward handles the actual control plane
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

// buildStewardControlPlane constructs the StewardControlPlane resource.
func (b *Builder) buildStewardControlPlane(name string) *unstructured.Unstructured {
	kcp := &unstructured.Unstructured{}
	kcp.SetAPIVersion(fmt.Sprintf("%s/%s", ControlPlaneAPIGroup, ControlPlaneAPIVersion))
	kcp.SetKind("StewardControlPlane")
	kcp.SetName(name)
	kcp.SetNamespace(b.namespace)
	b.applyCommonMetadata(kcp)

	// Get datastore name
	dataStoreName := "default"
	if b.tc.Spec.ControlPlane.DataStoreRef != nil && b.tc.Spec.ControlPlane.DataStoreRef.Name != "" {
		dataStoreName = b.tc.Spec.ControlPlane.DataStoreRef.Name
	}

	// Determine exposure mode and service type from ButlerConfig (platform-level)
	// Default to LoadBalancer if ButlerConfig not provided
	exposureMode := butlerv1alpha1.ControlPlaneExposureModeLoadBalancer
	var exposureHostname string
	var gatewayRef string
	var ingressClassName string
	var controllerType string

	if b.butlerConfig != nil {
		exposureMode = b.butlerConfig.GetControlPlaneExposureMode()
		exposureHostname = b.butlerConfig.GetControlPlaneExposureHostname()
		gatewayRef = b.butlerConfig.GetControlPlaneExposureGatewayRef()
		ingressClassName = b.butlerConfig.GetControlPlaneExposureIngressClassName()
		controllerType = b.butlerConfig.GetControlPlaneExposureControllerType()
	}

	// Map exposure mode to service type
	serviceType := "LoadBalancer"
	if exposureMode == butlerv1alpha1.ControlPlaneExposureModeIngress ||
		exposureMode == butlerv1alpha1.ControlPlaneExposureModeGateway {
		serviceType = "ClusterIP"
	}

	// TenantCluster serviceType override (should be rare, but allowed)
	if b.tc.Spec.ControlPlane.ServiceType != "" {
		serviceType = b.tc.Spec.ControlPlane.ServiceType
	}

	// Build addons map - always include core addons
	addons := map[string]interface{}{
		"coreDNS":      map[string]interface{}{},
		"kubeProxy":    map[string]interface{}{},
		"konnectivity": map[string]interface{}{},
	}

	// Enable workerBootstrap for Talos workers
	if b.tc.Spec.Workers.MachineTemplate.OS.Type == butlerv1alpha1.OSTypeTalos {
		addons["workerBootstrap"] = map[string]interface{}{
			"provider": "talos",
			"talos": map[string]interface{}{
				"port": int64(50001),
			},
		}
	}

	// AUTO-ENABLE tcp-proxy for non-LoadBalancer modes
	// tcp-proxy rewrites kubernetes.default.svc EndpointSlice for in-cluster API access
	if exposureMode == butlerv1alpha1.ControlPlaneExposureModeIngress ||
		exposureMode == butlerv1alpha1.ControlPlaneExposureModeGateway {
		tcpProxyConfig := map[string]interface{}{}

		// Add hostAliases for DNS resolution before CoreDNS is ready
		// tcp-proxy uses hostNetwork and needs to resolve the API server hostname
		if b.ingressIP != "" && exposureHostname != "" {
			tenantHostname := b.generateTenantHostname(exposureHostname)
			// Konnectivity uses a parallel hostname by replacing ".k8s." with ".konnectivity."
			konnectivityHostname := strings.Replace(tenantHostname, ".k8s.", ".konnectivity.", 1)

			tcpProxyConfig["hostAliases"] = []interface{}{
				map[string]interface{}{
					"ip":        b.ingressIP,
					"hostnames": []interface{}{tenantHostname, konnectivityHostname},
				},
			}
		}

		addons["tcpProxy"] = tcpProxyConfig
	}

	// Build network configuration
	network := map[string]interface{}{
		"serviceType": serviceType,
	}

	// Configure Ingress mode
	if exposureMode == butlerv1alpha1.ControlPlaneExposureModeIngress && exposureHostname != "" {
		tenantHostname := b.generateTenantHostname(exposureHostname)
		ingressConfig := map[string]interface{}{
			"hostname": tenantHostname,
		}
		if ingressClassName != "" {
			ingressConfig["className"] = ingressClassName
		}
		if controllerType != "" {
			ingressConfig["controllerType"] = controllerType
		}
		network["ingress"] = ingressConfig
	}

	// Configure Gateway mode
	if exposureMode == butlerv1alpha1.ControlPlaneExposureModeGateway && exposureHostname != "" {
		tenantHostname := b.generateTenantHostname(exposureHostname)
		gatewayConfig := map[string]interface{}{
			"hostname": tenantHostname,
		}
		if gatewayRef != "" {
			// Parse gatewayRef "namespace/name" format
			gatewayConfig["parentRefs"] = b.buildGatewayParentRefs(gatewayRef)
		}
		network["gateway"] = gatewayConfig
	}

	// See: https://steward.butlerlabs.dev/cluster-api/kamaji-control-plane-provider/
	spec := map[string]interface{}{
		"version":       b.tc.Spec.KubernetesVersion,
		"dataStoreName": dataStoreName,
		"addons":        addons,

		// Kubelet configuration - Talos uses cgroupfs, Rocky/Flatcar use systemd
		"kubelet": map[string]interface{}{
			"cgroupfs": b.kubeletCgroupDriver(),
			"preferredAddressTypes": []interface{}{
				"InternalIP",
				"ExternalIP",
				"Hostname",
			},
		},

		// Network configuration
		"network": network,
	}

	// Control plane replicas: preserve provider ownership. When the tenant
	// omits replicas (nil), leave the field unset downstream so Steward /
	// capi-steward applies its own default. Only set it when explicit.
	if b.tc.Spec.ControlPlane.Replicas != nil {
		spec["replicas"] = int64(*b.tc.Spec.ControlPlane.Replicas)
	}

	// Add control plane resources if configured (ButlerConfig defaults + TenantCluster overrides)
	resources := b.resolveControlPlaneResources()
	if resources != nil {
		if resources.APIServer != nil {
			spec["apiServer"] = b.componentResourceMap(resources.APIServer)
		}
		if resources.ControllerManager != nil {
			spec["controllerManager"] = b.componentResourceMap(resources.ControllerManager)
		}
		if resources.Scheduler != nil {
			spec["scheduler"] = b.componentResourceMap(resources.Scheduler)
		}
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

// generateTenantHostname creates a tenant-specific hostname from a wildcard pattern.
// Pattern "*.k8s.example.com" becomes "clustername.namespace.k8s.example.com"
func (b *Builder) generateTenantHostname(pattern string) string {
	base := strings.TrimPrefix(pattern, "*.")
	return fmt.Sprintf("%s.%s.%s", b.tc.Name, b.namespace, base)
}

// buildGatewayParentRefs constructs Gateway API parentRefs from a "namespace/name" reference.
func (b *Builder) buildGatewayParentRefs(gatewayRef string) []interface{} {
	parts := strings.Split(gatewayRef, "/")
	if len(parts) != 2 {
		return nil
	}
	return []interface{}{
		map[string]interface{}{
			"group":     "gateway.networking.k8s.io",
			"kind":      "Gateway",
			"namespace": parts[0],
			"name":      parts[1],
		},
	}
}

// buildKubevirtMachineTemplate constructs the KubevirtMachineTemplate resource.
// Uses full KubeVirt VM spec for proper CPU topology and resource limits.
func (b *Builder) buildKubevirtMachineTemplate(name string) *unstructured.Unstructured {
	kvmt := &unstructured.Unstructured{}
	kvmt.SetAPIVersion(fmt.Sprintf("%s/%s", InfrastructureAPIGroup, InfrastructureAPIVersion))
	kvmt.SetKind("KubevirtMachineTemplate")
	kvmt.SetName(fmt.Sprintf("%s-worker", name))
	kvmt.SetNamespace(b.namespace)
	b.applyCommonMetadata(kvmt)

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
// Uses CAPX (cluster-api-provider-nutanix) with Steward hosted control planes.
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

	// Build control plane (StewardControlPlane - shared across providers)
	controlPlane := b.buildStewardControlPlane(clusterName)

	// Build top-level Cluster
	cluster := b.buildCluster(clusterName, infraCluster, controlPlane)

	// Build machine template (NutanixMachineTemplate)
	machineTemplate := b.buildNutanixMachineTemplate(clusterName)

	// Build bootstrap config template (skip for Talos - uses dataSecretName instead)
	var bootstrapTemplate *unstructured.Unstructured
	if !b.isTalosCluster() {
		bootstrapTemplate = b.buildKubeadmConfigTemplate(clusterName)
	}

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
	b.applyCommonMetadata(nxCluster)

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
		// Control plane endpoint - will be patched by Steward when LoadBalancer gets IP
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

	// Add CA trust bundle for internal PKI (e.g. corporate CAs)
	if b.nutanixCreds != nil && b.nutanixCreds.CABundle != "" {
		prismCentral := spec["prismCentral"].(map[string]interface{})
		prismCentral["additionalTrustBundle"] = map[string]interface{}{
			"kind": "String",
			"data": b.nutanixCreds.CABundle,
		}
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
	b.applyCommonMetadata(nxmt)

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
	b.applyCommonMetadata(secret)

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

// GetMachineSpecs extracts CPU, Memory, and Disk from TenantCluster spec with defaults.
func GetMachineSpecs(tc *butlerv1alpha1.TenantCluster) (cpu int64, memory string, disk string) {
	cpu = DefaultWorkerCPU
	memoryMB := DefaultWorkerMemoryMB
	diskGB := DefaultWorkerDiskGB

	mt := tc.Spec.Workers.MachineTemplate

	if mt.CPU > 0 {
		cpu = int64(mt.CPU)
	}

	if !mt.Memory.IsZero() {
		memBytes := mt.Memory.Value()
		memGi := memBytes / (1024 * 1024 * 1024)
		if memGi > 0 {
			memory = fmt.Sprintf("%dGi", memGi)
		} else {
			memMi := memBytes / (1024 * 1024)
			memory = fmt.Sprintf("%dMi", memMi)
		}
	} else {
		memory = fmt.Sprintf("%dMi", memoryMB)
	}

	if !mt.DiskSize.IsZero() {
		diskBytes := mt.DiskSize.Value()
		diskGiB := diskBytes / (1024 * 1024 * 1024)
		if diskGiB > 0 {
			disk = fmt.Sprintf("%dGi", diskGiB)
		} else {
			diskMi := diskBytes / (1024 * 1024)
			disk = fmt.Sprintf("%dMi", diskMi)
		}
	} else {
		disk = fmt.Sprintf("%dGi", diskGB)
	}

	return cpu, memory, disk
}

func (b *Builder) getMachineSpecs() (cpu int64, memory string, disk string) {
	return GetMachineSpecs(b.tc)
}

// buildKubeadmConfigTemplate constructs the KubeadmConfigTemplate with Rocky Linux bootstrap.
func (b *Builder) buildKubeadmConfigTemplate(name string) *unstructured.Unstructured {
	kct := &unstructured.Unstructured{}
	kct.SetAPIVersion(fmt.Sprintf("%s/%s", BootstrapAPIGroup, BootstrapAPIVersion))
	kct.SetKind("KubeadmConfigTemplate")
	kct.SetName(fmt.Sprintf("%s-worker", name))
	kct.SetNamespace(b.namespace)
	b.applyCommonMetadata(kct)

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
	commands := []interface{}{}

	// For Ingress/Gateway modes, add /etc/hosts entry first (before any k8s commands)
	// This allows the worker to resolve the control plane hostname via the Ingress IP
	// Note: We use /tmp/ because it always exists, unlike /etc/hosts.d/ which cloud-init may not create
	if b.needsIngressHostsEntry() {
		commands = append(commands, "cat /tmp/ingress-hosts >> /etc/hosts")
	}

	// Kernel modules for container networking
	commands = append(commands,
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
	)

	return commands
}

// buildBootstrapFiles returns additional files to create during bootstrap.
func (b *Builder) buildBootstrapFiles(k8sMinorVersion string) []interface{} {
	files := []interface{}{
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

	// For Ingress/Gateway modes, add /etc/hosts entries to resolve control plane hostnames
	// Both the API server hostname and konnectivity hostname need to resolve to the Ingress IP
	// Note: We use /tmp/ because it always exists - cloud-init may not create /etc/hosts.d/
	if b.needsIngressHostsEntry() {
		hostname := b.generateTenantHostname(b.butlerConfig.GetControlPlaneExposureHostname())
		// Konnectivity uses a parallel hostname by replacing ".k8s." with ".konnectivity."
		konnectivityHostname := strings.Replace(hostname, ".k8s.", ".konnectivity.", 1)
		files = append(files, map[string]interface{}{
			"path":        "/tmp/ingress-hosts",
			"permissions": "0644",
			"content":     fmt.Sprintf("%s %s\n%s %s\n", b.ingressIP, hostname, b.ingressIP, konnectivityHostname),
		})
	}

	return files
}

// kubeletCgroupDriver returns the cgroup driver for the OS type.
// Talos uses cgroupfs; Rocky Linux and Flatcar use systemd.
func (b *Builder) kubeletCgroupDriver() string {
	if b.tc.Spec.Workers.MachineTemplate.OS.Type == butlerv1alpha1.OSTypeTalos {
		return "cgroupfs"
	}
	return "systemd"
}

// isTalosCluster returns true if the TenantCluster uses Talos OS.
func (b *Builder) isTalosCluster() bool {
	return talos.IsTalosCluster(b.tc)
}

// needsIngressHostsEntry returns true if the cluster uses Ingress/Gateway mode
// and needs a hosts entry to resolve the control plane hostname.
func (b *Builder) needsIngressHostsEntry() bool {
	if b.butlerConfig == nil || b.ingressIP == "" {
		return false
	}
	mode := b.butlerConfig.GetControlPlaneExposureMode()
	return mode == butlerv1alpha1.ControlPlaneExposureModeIngress ||
		mode == butlerv1alpha1.ControlPlaneExposureModeGateway
}

// ResolveControlPlaneResources merges ButlerConfig defaults with TenantCluster overrides.
// Returns nil if no resources are configured (BestEffort QoS).
func ResolveControlPlaneResources(tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) *butlerv1alpha1.ControlPlaneResourcesSpec {
	var result *butlerv1alpha1.ControlPlaneResourcesSpec
	if butlerConfig != nil {
		result = butlerConfig.GetDefaultControlPlaneResources()
	}

	tcResources := tc.Spec.ControlPlane.Resources
	if tcResources == nil {
		return result
	}

	if result == nil {
		return tcResources
	}

	merged := result.DeepCopy()
	if tcResources.APIServer != nil {
		merged.APIServer = tcResources.APIServer
	}
	if tcResources.ControllerManager != nil {
		merged.ControllerManager = tcResources.ControllerManager
	}
	if tcResources.Scheduler != nil {
		merged.Scheduler = tcResources.Scheduler
	}
	return merged
}

func (b *Builder) resolveControlPlaneResources() *butlerv1alpha1.ControlPlaneResourcesSpec {
	return ResolveControlPlaneResources(b.tc, b.butlerConfig)
}

// ComponentResourceMap converts a ComponentResources to an unstructured map
// matching the StewardControlPlane's ControlPlaneComponent shape.
func ComponentResourceMap(cr *butlerv1alpha1.ComponentResources) map[string]interface{} {
	result := map[string]interface{}{}
	resMap := map[string]interface{}{}

	if cr.Requests != nil {
		reqMap := map[string]interface{}{}
		if cr.Requests.CPU != nil {
			reqMap["cpu"] = cr.Requests.CPU.String()
		}
		if cr.Requests.Memory != nil {
			reqMap["memory"] = cr.Requests.Memory.String()
		}
		if len(reqMap) > 0 {
			resMap["requests"] = reqMap
		}
	}

	if cr.Limits != nil {
		limMap := map[string]interface{}{}
		if cr.Limits.CPU != nil {
			limMap["cpu"] = cr.Limits.CPU.String()
		}
		if cr.Limits.Memory != nil {
			limMap["memory"] = cr.Limits.Memory.String()
		}
		if len(limMap) > 0 {
			resMap["limits"] = limMap
		}
	}

	if len(resMap) > 0 {
		result["resources"] = resMap
	}
	return result
}

func (b *Builder) componentResourceMap(cr *butlerv1alpha1.ComponentResources) map[string]interface{} {
	return ComponentResourceMap(cr)
}

// buildMachineDeployment constructs the MachineDeployment resource.
func (b *Builder) buildMachineDeployment(name string, machineTemplate, bootstrapTemplate *unstructured.Unstructured) *unstructured.Unstructured {
	md := &unstructured.Unstructured{}
	md.SetAPIVersion(fmt.Sprintf("%s/%s", ClusterAPIGroup, ClusterAPIVersion))
	md.SetKind("MachineDeployment")
	md.SetName(fmt.Sprintf("%s-workers", name))
	md.SetNamespace(b.namespace)
	b.applyCommonMetadata(md)

	replicas := int64(b.tc.Spec.Workers.Replicas)
	if replicas < 1 {
		replicas = 1
	}

	// Build bootstrap config: use configRef for kubeadm, dataSecretName for Talos
	var bootstrap map[string]interface{}
	if bootstrapTemplate != nil {
		bootstrap = map[string]interface{}{
			"configRef": map[string]interface{}{
				"apiVersion": bootstrapTemplate.GetAPIVersion(),
				"kind":       bootstrapTemplate.GetKind(),
				"name":       bootstrapTemplate.GetName(),
				"namespace":  bootstrapTemplate.GetNamespace(),
			},
		}
	} else {
		// Talos: use dataSecretName — CAPX reads the Secret directly.
		// IMPORTANT: The Secret MUST exist before the MachineDeployment is created.
		// CAPX (Nutanix) treats a missing Secret as a terminal CreateError on the
		// NutanixMachine, which is NOT retryable. The reconciler defers
		// MachineDeployment creation for Talos clusters until after the bootstrap
		// secret is created by reconcileTalosBootstrap.
		bootstrap = map[string]interface{}{
			"dataSecretName": fmt.Sprintf("%s-talos-bootstrap", name),
		}
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
				"bootstrap":   bootstrap,
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

// applyCommonMetadata sets the common labels and common annotations
// on a CAPI resource. Owner identity (email) is stored on the
// AnnotationOwner annotation rather than a label because email values
// violate the Kubernetes label-value character class.
func (b *Builder) applyCommonMetadata(obj *unstructured.Unstructured) {
	obj.SetLabels(b.commonLabels())
	if ann := b.commonAnnotations(); len(ann) > 0 {
		obj.SetAnnotations(ann)
	}
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

	if env := b.tc.Labels[butlerv1alpha1.LabelEnvironment]; env != "" {
		labels[butlerv1alpha1.LabelEnvironment] = env
	}

	return labels
}

// commonAnnotations returns annotations applied to all CAPI resources
// whose values cannot fit in a label (for example email addresses,
// which contain "@" and fail label-value validation). The owner
// annotation is read from the TenantCluster's own annotations; the
// reconciler sets it from the creator-email annotation before CAPI
// resources are built.
func (b *Builder) commonAnnotations() map[string]string {
	if b.tc.Annotations == nil {
		return nil
	}
	owner := b.tc.Annotations[butlerv1alpha1.AnnotationOwner]
	if owner == "" {
		return nil
	}
	return map[string]string{butlerv1alpha1.AnnotationOwner: owner}
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

// ResourcesWithoutMachineDeployment returns all resources except the MachineDeployment.
// Used for Talos clusters where the MachineDeployment must be deferred until the
// bootstrap secret exists — CAPX treats a missing dataSecretName target as a terminal
// CreateError on the NutanixMachine, which is not retryable.
func (rs *ResourceSet) ResourcesWithoutMachineDeployment() []*unstructured.Unstructured {
	return []*unstructured.Unstructured{
		rs.CredentialSecret,
		rs.InfrastructureCluster,
		rs.BootstrapConfigTemplate,
		rs.MachineTemplate,
		rs.ControlPlane,
		rs.Cluster,
	}
}
