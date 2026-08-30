// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/capi"
)

// reconcileMachineDeploymentReplicas syncs worker count from TenantCluster spec to MachineDeployment.
// This is called on every reconcile to ensure spec.workers.replicas changes are propagated
// to the underlying CAPI MachineDeployment, enabling cluster scaling operations.
// It also updates the TenantCluster status with current worker counts and the WorkersReady condition.
func (r *Reconciler) reconcileMachineDeploymentReplicas(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	// Only sync when cluster is Ready or Installing
	// During Provisioning, initial creation handles replicas
	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady &&
		tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseInstalling {
		return nil
	}

	// Need tenant namespace to find MachineDeployment
	if tc.Status.TenantNamespace == "" {
		return nil
	}

	// Get the MachineDeployment
	mdName := fmt.Sprintf("%s-workers", tc.Name)
	md := &unstructured.Unstructured{}
	md.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.ClusterAPIGroup,
		Version: capi.ClusterAPIVersion,
		Kind:    "MachineDeployment",
	})

	if err := r.Get(ctx, types.NamespacedName{
		Namespace: tc.Status.TenantNamespace,
		Name:      mdName,
	}, md); err != nil {
		if apierrors.IsNotFound(err) {
			// Not created yet, will be handled by initial provisioning
			return nil
		}
		return fmt.Errorf("failed to get MachineDeployment: %w", err)
	}

	// Get current spec replicas and desired replicas from TenantCluster
	currentSpecReplicas, _, _ := unstructured.NestedInt64(md.Object, "spec", "replicas")
	desiredReplicas := int64(tc.Spec.Workers.Replicas)

	// Default to 1 if not specified (matches builder.go behavior)
	if desiredReplicas < 1 {
		desiredReplicas = 1
	}

	// Update TenantCluster status with worker counts from MachineDeployment
	readyReplicas, _, _ := unstructured.NestedInt64(md.Object, "status", "readyReplicas")

	// CAPI MachineDeployment readyReplicas can be 0 even when workers are actually running,
	// because CAPI can't match Machine→Node without spec.providerID (e.g. Talos, Flatcar on KubeVirt).
	// When the cluster is already Ready, fall back to querying the tenant API server directly.
	// To avoid unnecessary API calls at scale, skip the query when the previous reconcile already
	// found all workers ready (status matches desired). Only re-query on first check or during scaling.
	if readyReplicas == 0 && tc.Status.Phase == butlerv1alpha1.TenantClusterPhaseReady {
		previousReady := tc.Status.WorkerNodesReady
		if previousReady > 0 && previousReady == int32(desiredReplicas) {
			// Workers fully converged last time — reuse cached count
			readyReplicas = int64(previousReady)
		} else {
			// First check, scaling in progress, or workers not yet converged — query tenant API
			if nodeCount, err := r.countTenantReadyNodes(ctx, tc, butlerConfig); err == nil && nodeCount > 0 {
				readyReplicas = int64(nodeCount)
				logger.V(1).Info("using tenant node count as CAPI readyReplicas fallback",
					"nodeCount", nodeCount)
			}
		}
	}

	tc.Status.WorkerNodesReady = int32(readyReplicas)
	tc.Status.WorkerNodesDesired = int32(desiredReplicas)

	// Update WorkersReady condition based on current worker counts
	if tc.Status.WorkerNodesReady >= tc.Status.WorkerNodesDesired && tc.Status.WorkerNodesDesired > 0 {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionWorkersReady,
			metav1.ConditionTrue, ReasonWorkersReady,
			fmt.Sprintf("All %d workers ready", tc.Status.WorkerNodesReady))
	} else {
		r.setCondition(tc, butlerv1alpha1.TenantClusterConditionWorkersReady,
			metav1.ConditionFalse, ReasonWorkersProvisioning,
			fmt.Sprintf("Workers provisioning: %d/%d ready", tc.Status.WorkerNodesReady, tc.Status.WorkerNodesDesired))
	}

	// Check if MachineDeployment spec needs to be scaled
	if currentSpecReplicas == desiredReplicas {
		// Already in sync, nothing more to do
		return nil
	}

	logger.Info("scaling MachineDeployment",
		"name", mdName,
		"from", currentSpecReplicas,
		"to", desiredReplicas)

	// Patch MachineDeployment replicas
	patch := []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, desiredReplicas))

	if err := r.Patch(ctx, md, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("failed to patch MachineDeployment replicas: %w", err)
	}

	r.Recorder.Eventf(tc, corev1.EventTypeNormal, "ClusterScaling",
		"Scaled workers from %d to %d", currentSpecReplicas, desiredReplicas)

	logger.Info("MachineDeployment scaled successfully",
		"name", mdName,
		"replicas", desiredReplicas)

	return nil
}

// countTenantReadyNodes queries the tenant cluster's API server to count Ready worker nodes.
// This is a fallback for when CAPI readyReplicas is unreliable (e.g. no providerID on nodes).
// Returns 0 without error if the tenant cluster is unreachable (non-fatal).
func (r *Reconciler) countTenantReadyNodes(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) (int32, error) {
	tenantClient, err := r.getTenantClient(ctx, tc)
	if err != nil {
		return 0, nil // non-fatal: kubeconfig not available yet
	}

	nodes, err := tenantClient.Clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, nil // non-fatal: tenant cluster may be unreachable
	}

	var readyCount int32
	for _, node := range nodes.Items {
		// Skip control plane nodes (only count workers)
		if _, isMaster := node.Labels["node-role.kubernetes.io/master"]; isMaster {
			continue
		}
		if _, isCP := node.Labels["node-role.kubernetes.io/control-plane"]; isCP {
			continue
		}
		for _, cond := range node.Status.Conditions {
			if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionTrue {
				readyCount++
				break
			}
		}
	}

	return readyCount, nil
}

// reconcileNodeProviderIDs patches spec.providerID on tenant cluster Nodes to match
// the CAPI Machine providerID. Without this, CAPI cannot populate Machine.status.nodeRef
// and MachineDeployment rolling updates stall. Affects Talos and Flatcar workers on
// KubeVirt and Nutanix where the kubelet does not set providerID natively.
// No-op for nodes that already have providerID set (including cloud provider nodes).
func (r *Reconciler) reconcileNodeProviderIDs(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) error {
	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady {
		return nil
	}
	if tc.Status.TenantNamespace == "" {
		return nil
	}

	logger := log.FromContext(ctx)

	// Short-circuit: if CAPI MachineDeployment already reports all replicas ready,
	// providerIDs are already set (CAPI needs nodeRef to count ready replicas).
	mdName := fmt.Sprintf("%s-workers", tc.Name)
	md := &unstructured.Unstructured{}
	md.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.ClusterAPIGroup,
		Version: capi.ClusterAPIVersion,
		Kind:    "MachineDeployment",
	})
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: tc.Status.TenantNamespace,
		Name:      mdName,
	}, md); err != nil {
		return nil
	}
	mdDesired, _, _ := unstructured.NestedInt64(md.Object, "spec", "replicas")
	mdReady, _, _ := unstructured.NestedInt64(md.Object, "status", "readyReplicas")
	mdTotal, _, _ := unstructured.NestedInt64(md.Object, "status", "replicas")
	// Only short-circuit when all replicas are ready AND no extra replicas exist
	// (rolling updates temporarily create more replicas than desired).
	if mdDesired > 0 && mdReady == mdDesired && mdTotal == mdDesired {
		return nil
	}

	machineList := &unstructured.UnstructuredList{}
	machineList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.ClusterAPIGroup,
		Version: capi.ClusterAPIVersion,
		Kind:    "MachineList",
	})
	if err := r.List(ctx, machineList, client.InNamespace(tc.Status.TenantNamespace),
		client.MatchingLabels{"cluster.x-k8s.io/cluster-name": tc.Name}); err != nil {
		return fmt.Errorf("failed to list Machines: %w", err)
	}

	// Build IP -> providerID map. Skip Machines in Deleting/Failed phase and
	// detect duplicate IPs (can happen during rolling updates) to avoid wrong assignments.
	ipToProviderID := make(map[string]string)
	duplicateIPs := make(map[string]bool)
	for _, machine := range machineList.Items {
		phase, _, _ := unstructured.NestedString(machine.Object, "status", "phase")
		if phase == "Deleting" || phase == "Failed" {
			continue
		}
		providerID, _, _ := unstructured.NestedString(machine.Object, "spec", "providerID")
		if providerID == "" {
			continue
		}
		addresses, _, _ := unstructured.NestedSlice(machine.Object, "status", "addresses")
		for _, addr := range addresses {
			addrMap, ok := addr.(map[string]interface{})
			if !ok {
				continue
			}
			addrType, _ := addrMap["type"].(string)
			addrValue, _ := addrMap["address"].(string)
			if addrType != "InternalIP" || addrValue == "" {
				continue
			}
			if _, exists := ipToProviderID[addrValue]; exists {
				duplicateIPs[addrValue] = true
				continue
			}
			ipToProviderID[addrValue] = providerID
		}
	}
	for ip := range duplicateIPs {
		logger.V(1).Info("skipping ambiguous IP shared by multiple Machines", "ip", ip)
		delete(ipToProviderID, ip)
	}

	if len(ipToProviderID) == 0 {
		return nil
	}

	tenantClient, err := r.getTenantClient(ctx, tc)
	if err != nil {
		return fmt.Errorf("failed to get tenant client: %w", err)
	}

	nodes, err := tenantClient.Clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list tenant nodes: %w", err)
	}

	for i := range nodes.Items {
		node := &nodes.Items[i]
		if node.Spec.ProviderID != "" {
			continue
		}

		var nodeIP string
		for _, addr := range node.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP {
				nodeIP = addr.Address
				break
			}
		}
		if nodeIP == "" {
			continue
		}

		providerID, ok := ipToProviderID[nodeIP]
		if !ok {
			continue
		}

		patch := []byte(fmt.Sprintf(`{"spec":{"providerID":%q}}`, providerID))
		if _, err := tenantClient.Clientset.CoreV1().Nodes().Patch(ctx, node.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}); err != nil {
			logger.Info("failed to set providerID on tenant node", "node", node.Name, "providerID", providerID, "error", err)
			continue
		}
		logger.Info("set providerID on tenant node", "node", node.Name, "providerID", providerID)
	}

	return nil
}

// reconcileStewardControlPlane patches the StewardControlPlane when the TenantCluster
// spec diverges from the live SCP (version, replicas, CP resources).
func (r *Reconciler) reconcileStewardControlPlane(ctx context.Context, tc *butlerv1alpha1.TenantCluster, butlerConfig *butlerv1alpha1.ButlerConfig) error {
	logger := log.FromContext(ctx)

	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady &&
		tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseInstalling {
		return nil
	}
	if tc.Status.TenantNamespace == "" {
		return nil
	}

	scp := &unstructured.Unstructured{}
	scp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.ControlPlaneAPIGroup,
		Version: capi.ControlPlaneAPIVersion,
		Kind:    "StewardControlPlane",
	})
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: tc.Status.TenantNamespace,
		Name:      tc.Name,
	}, scp); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get StewardControlPlane: %w", err)
	}

	patch := map[string]interface{}{"spec": map[string]interface{}{}}
	specPatch := patch["spec"].(map[string]interface{})
	var changes []string

	// Version drift
	currentVersion, _, _ := unstructured.NestedString(scp.Object, "spec", "version")
	if tc.Spec.KubernetesVersion != "" && tc.Spec.KubernetesVersion != currentVersion {
		specPatch["version"] = tc.Spec.KubernetesVersion
		changes = append(changes, fmt.Sprintf("version %s->%s", currentVersion, tc.Spec.KubernetesVersion))
	}

	// Replicas drift. Only patch if the TenantCluster explicitly sets replicas.
	// When it is omitted (nil), leave the SCP replicas alone so the provider
	// default is preserved; patching it would fight that default.
	if tc.Spec.ControlPlane.Replicas != nil {
		currentReplicas, _, _ := unstructured.NestedInt64(scp.Object, "spec", "replicas")
		desiredReplicas := int64(*tc.Spec.ControlPlane.Replicas)
		if desiredReplicas != currentReplicas {
			specPatch["replicas"] = desiredReplicas
			changes = append(changes, fmt.Sprintf("replicas %d->%d", currentReplicas, desiredReplicas))
		}
	}

	// CP resource drift
	desired := capi.ResolveControlPlaneResources(tc, butlerConfig)
	if desired != nil {
		if desired.APIServer != nil {
			if cpComponentDrifted(scp, "apiServer", desired.APIServer) {
				specPatch["apiServer"] = capi.ComponentResourceMap(desired.APIServer)
				changes = append(changes, "apiServer resources")
			}
		}
		if desired.ControllerManager != nil {
			if cpComponentDrifted(scp, "controllerManager", desired.ControllerManager) {
				specPatch["controllerManager"] = capi.ComponentResourceMap(desired.ControllerManager)
				changes = append(changes, "controllerManager resources")
			}
		}
		if desired.Scheduler != nil {
			if cpComponentDrifted(scp, "scheduler", desired.Scheduler) {
				specPatch["scheduler"] = capi.ComponentResourceMap(desired.Scheduler)
				changes = append(changes, "scheduler resources")
			}
		}
	}

	if len(changes) == 0 {
		return nil
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal SCP patch: %w", err)
	}

	logger.Info("updating StewardControlPlane", "cluster", tc.Name, "changes", changes)
	return r.Patch(ctx, scp, client.RawPatch(types.MergePatchType, patchBytes))
}

func cpComponentDrifted(scp *unstructured.Unstructured, component string, desired *butlerv1alpha1.ComponentResources) bool {
	check := func(path []string, want *resource.Quantity) bool {
		if want == nil {
			return false
		}
		cur, ok, _ := unstructured.NestedString(scp.Object, path...)
		if !ok {
			return true
		}
		curQ, err := resource.ParseQuantity(cur)
		if err != nil {
			return true
		}
		return curQ.Cmp(*want) != 0
	}

	if desired.Requests != nil {
		if check([]string{"spec", component, "resources", "requests", "cpu"}, desired.Requests.CPU) {
			return true
		}
		if check([]string{"spec", component, "resources", "requests", "memory"}, desired.Requests.Memory) {
			return true
		}
	}
	if desired.Limits != nil {
		if check([]string{"spec", component, "resources", "limits", "cpu"}, desired.Limits.CPU) {
			return true
		}
		if check([]string{"spec", component, "resources", "limits", "memory"}, desired.Limits.Memory) {
			return true
		}
	}
	return false
}

// reconcileMachineTemplate detects CPU/memory/disk changes in the TenantCluster spec
// and performs a rolling update by creating a new immutable MachineTemplate and updating
// the MachineDeployment's infrastructureRef to point to it.
func (r *Reconciler) reconcileMachineTemplate(ctx context.Context, tc *butlerv1alpha1.TenantCluster) error {
	logger := log.FromContext(ctx)

	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady &&
		tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseInstalling {
		return nil
	}
	if tc.Status.TenantNamespace == "" {
		return nil
	}

	// Get the MachineDeployment
	mdName := fmt.Sprintf("%s-workers", tc.Name)
	md := &unstructured.Unstructured{}
	md.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.ClusterAPIGroup,
		Version: capi.ClusterAPIVersion,
		Kind:    "MachineDeployment",
	})
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: tc.Status.TenantNamespace,
		Name:      mdName,
	}, md); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get MachineDeployment: %w", err)
	}

	// Get current infrastructureRef from MachineDeployment
	infraRefName, _, _ := unstructured.NestedString(md.Object,
		"spec", "template", "spec", "infrastructureRef", "name")
	infraRefKind, _, _ := unstructured.NestedString(md.Object,
		"spec", "template", "spec", "infrastructureRef", "kind")
	infraRefAPIVersion, _, _ := unstructured.NestedString(md.Object,
		"spec", "template", "spec", "infrastructureRef", "apiVersion")
	if infraRefName == "" || infraRefKind == "" {
		return nil
	}

	// Get the current MachineTemplate
	currentTmpl := &unstructured.Unstructured{}
	currentTmpl.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   capi.InfrastructureAPIGroup,
		Version: strings.TrimPrefix(infraRefAPIVersion, capi.InfrastructureAPIGroup+"/"),
		Kind:    infraRefKind,
	})
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: tc.Status.TenantNamespace,
		Name:      infraRefName,
	}, currentTmpl); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get current MachineTemplate: %w", err)
	}

	desiredCPU, desiredMemory, desiredDisk := capi.GetMachineSpecs(tc)

	var currentCPU int64
	var currentMemory, currentDisk string

	switch infraRefKind {
	case "KubevirtMachineTemplate":
		currentCPU, _, _ = unstructured.NestedInt64(currentTmpl.Object,
			"spec", "template", "spec", "virtualMachineTemplate", "spec",
			"template", "spec", "domain", "cpu", "cores")
		currentMemory, _, _ = unstructured.NestedString(currentTmpl.Object,
			"spec", "template", "spec", "virtualMachineTemplate", "spec",
			"template", "spec", "domain", "resources", "requests", "memory")
		dvTemplates, ok, _ := unstructured.NestedSlice(currentTmpl.Object,
			"spec", "template", "spec", "virtualMachineTemplate", "spec",
			"dataVolumeTemplates")
		if ok && len(dvTemplates) > 0 {
			if dvt, ok := dvTemplates[0].(map[string]interface{}); ok {
				currentDisk, _, _ = unstructured.NestedString(dvt,
					"spec", "storage", "resources", "requests", "storage")
			}
		}

	case "NutanixMachineTemplate":
		currentCPU, _, _ = unstructured.NestedInt64(currentTmpl.Object,
			"spec", "template", "spec", "vcpuSockets")
		currentMemory, _, _ = unstructured.NestedString(currentTmpl.Object,
			"spec", "template", "spec", "memorySize")
		currentDisk, _, _ = unstructured.NestedString(currentTmpl.Object,
			"spec", "template", "spec", "systemDiskSize")

	default:
		logger.Info("machine template rolling update not supported for provider",
			"kind", infraRefKind, "cluster", tc.Name)
		return nil
	}

	if currentCPU == desiredCPU && currentMemory == desiredMemory && currentDisk == desiredDisk {
		return nil
	}

	logger.Info("machine template change detected",
		"cluster", tc.Name,
		"currentCPU", currentCPU, "desiredCPU", desiredCPU,
		"currentMemory", currentMemory, "desiredMemory", desiredMemory,
		"currentDisk", currentDisk, "desiredDisk", desiredDisk)

	// Determine new template name — increment version suffix
	newName := infraRefName + "-v2"
	// Check if -v2 already exists, if so try -v3, etc.
	for i := 2; i <= 20; i++ {
		candidate := fmt.Sprintf("%s-worker-v%d", tc.Name, i)
		check := &unstructured.Unstructured{}
		check.SetGroupVersionKind(currentTmpl.GroupVersionKind())
		err := r.Get(ctx, types.NamespacedName{
			Namespace: tc.Status.TenantNamespace,
			Name:      candidate,
		}, check)
		if apierrors.IsNotFound(err) {
			newName = candidate
			break
		}
		if err != nil {
			return fmt.Errorf("failed to check template name %s: %w", candidate, err)
		}
		// Already exists — check if MachineDeployment already points to it
		if infraRefName == candidate {
			// Already using this one, need next version
			continue
		}
	}

	// Deep copy the current template and update specs
	newTmpl := currentTmpl.DeepCopy()
	newTmpl.SetName(newName)
	newTmpl.SetResourceVersion("")
	newTmpl.SetUID("")
	newTmpl.SetCreationTimestamp(metav1.Time{})
	// Remove managedFields to avoid conflicts
	newTmpl.SetManagedFields(nil)

	var setErrs []error
	setField := func(obj map[string]interface{}, val interface{}, fields ...string) {
		if err := unstructured.SetNestedField(obj, val, fields...); err != nil {
			setErrs = append(setErrs, err)
		}
	}

	switch infraRefKind {
	case "KubevirtMachineTemplate":
		vmBase := []string{"spec", "template", "spec", "virtualMachineTemplate", "spec", "template", "spec", "domain"}
		setField(newTmpl.Object, desiredCPU, append(vmBase, "cpu", "cores")...)
		setField(newTmpl.Object, desiredMemory, append(vmBase, "resources", "requests", "memory")...)
		setField(newTmpl.Object, desiredMemory, append(vmBase, "resources", "limits", "memory")...)
		setField(newTmpl.Object, fmt.Sprintf("%d", desiredCPU), append(vmBase, "resources", "limits", "cpu")...)

		dvTemplates, ok, _ := unstructured.NestedSlice(newTmpl.Object,
			"spec", "template", "spec", "virtualMachineTemplate", "spec", "dataVolumeTemplates")
		if ok && len(dvTemplates) > 0 {
			if dvt, ok := dvTemplates[0].(map[string]interface{}); ok {
				setField(dvt, desiredDisk, "spec", "storage", "resources", "requests", "storage")
				dvTemplates[0] = dvt
				setField(newTmpl.Object, dvTemplates,
					"spec", "template", "spec", "virtualMachineTemplate", "spec", "dataVolumeTemplates")
			}
		}

	case "NutanixMachineTemplate":
		setField(newTmpl.Object, desiredCPU, "spec", "template", "spec", "vcpuSockets")
		setField(newTmpl.Object, desiredMemory, "spec", "template", "spec", "memorySize")
		setField(newTmpl.Object, desiredDisk, "spec", "template", "spec", "systemDiskSize")
	}

	if len(setErrs) > 0 {
		return fmt.Errorf("failed to set fields on new MachineTemplate: %v", setErrs[0])
	}

	// Create the new template
	if err := r.Create(ctx, newTmpl); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Race condition — already created, just update MD
		} else {
			return fmt.Errorf("failed to create new MachineTemplate %s: %w", newName, err)
		}
	} else {
		logger.Info("created new MachineTemplate",
			"name", newName,
			"cpu", desiredCPU,
			"memory", desiredMemory)
	}

	// Update MachineDeployment to point to the new template
	patch := []byte(fmt.Sprintf(`{"spec":{"template":{"spec":{"infrastructureRef":{"name":"%s"}}}}}`, newName))
	if err := r.Patch(ctx, md, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("failed to update MachineDeployment infrastructureRef: %w", err)
	}

	logger.Info("updated MachineDeployment to new MachineTemplate",
		"machineDeployment", mdName,
		"newTemplate", newName)

	return nil
}
