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

package networkpool

import (
	"bytes"
	"context"
	"net"
	"reflect"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/ipam"
)

const (
	// SourceMetalLB identifies IPs consumed by MetalLB LoadBalancer Services.
	SourceMetalLB = "metallb"

	// SourceNode identifies IPs used as Node InternalIPs within the pool CIDR.
	SourceNode = "node"

	// SourceMachine identifies IPs used by CAPI Machine VMs within the pool CIDR.
	SourceMachine = "machine"

	// infraAllocationFieldManager is the SSA field manager identity for
	// NetworkPool.status.infrastructureAllocations.
	//
	// Multiple writers patch NetworkPool.status. Each uses a distinct field
	// manager to prevent SSA conflicts:
	//   butler-controller/infra-allocation: this reconciler, owns infrastructureAllocations
	//   butler-controller/ipam (installer.go): MetalLB IPAddressPool on tenant clusters
	//   (existing NetworkPool reconciler): plain Update for capacity fields and conditions
	//
	// If the existing NetworkPool reconciler is migrated to SSA, it must adopt
	// a distinct field manager (e.g., butler-controller/networkpool-allocation).
	// Sharing a field manager across writers causes silent conflict resolution.
	infraAllocationFieldManager = "butler-controller/infra-allocation"
)

// InfraAllocationReconciler discovers reserved IP usage by management-cluster
// LoadBalancer Services and writes InfrastructureAllocations to NetworkPool status.
type InfraAllocationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines,verbs=get;list;watch

func (r *InfraAllocationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pool := &butlerv1alpha1.NetworkPool{}
	if err := r.Get(ctx, req.NamespacedName, pool); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !pool.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Parse pool CIDR for containment checks on Nodes.
	_, poolNet, err := net.ParseCIDR(pool.Spec.CIDR)
	if err != nil {
		logger.V(1).Info("skipping pool with invalid CIDR", "cidr", pool.Spec.CIDR, "error", err)
		return ctrl.Result{}, nil
	}

	// Parse reserved ranges into net.IPNet for Service containment checks.
	var reservedNets []*net.IPNet
	for _, reserved := range pool.Spec.Reserved {
		_, network, err := net.ParseCIDR(reserved.CIDR)
		if err != nil {
			logger.V(1).Info("skipping invalid reserved CIDR", "cidr", reserved.CIDR, "error", err)
			continue
		}
		reservedNets = append(reservedNets, network)
	}

	// Parse tenant allocation ranges (Nodes in these ranges are tracked by
	// the IPAM allocator, not the infra reconciler).
	var tenantRanges []ipam.AllocatedRange
	if pool.Spec.TenantAllocation != nil {
		for _, r := range pool.Spec.TenantAllocation.GetEffectiveRanges() {
			tenantRanges = append(tenantRanges, ipam.AllocatedRange{Start: r.Start, End: r.End})
		}
	}

	// Build a map keyed by IP for merging Service, Node, and Machine facts.
	type infraEntry struct {
		source     string
		serviceRef *butlerv1alpha1.NamespacedObjectReference
		nodeRef    *butlerv1alpha1.ClusterObjectReference
		machineRef *butlerv1alpha1.NamespacedObjectReference
	}
	entries := make(map[string]*infraEntry)

	// Discover LB Service IPs in reserved ranges.
	svcList := &corev1.ServiceList{}
	if err := r.List(ctx, svcList); err != nil {
		return ctrl.Result{}, err
	}
	for i := range svcList.Items {
		svc := &svcList.Items[i]
		if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
			continue
		}
		for _, ingress := range svc.Status.LoadBalancer.Ingress {
			if ingress.IP == "" {
				continue
			}
			ip := net.ParseIP(ingress.IP)
			if ip == nil {
				continue
			}
			for _, network := range reservedNets {
				if network.Contains(ip) {
					entries[ingress.IP] = &infraEntry{
						source: SourceMetalLB,
						serviceRef: &butlerv1alpha1.NamespacedObjectReference{
							Name:      svc.Name,
							Namespace: svc.Namespace,
						},
					}
					break
				}
			}
		}
	}

	// Discover Node InternalIPs within the pool CIDR (outside tenant ranges).
	nodeList := &corev1.NodeList{}
	if err := r.List(ctx, nodeList); err != nil {
		return ctrl.Result{}, err
	}
	for i := range nodeList.Items {
		node := &nodeList.Items[i]
		for _, addr := range node.Status.Addresses {
			if addr.Type != corev1.NodeInternalIP {
				continue
			}
			ip := net.ParseIP(addr.Address)
			if ip == nil {
				continue
			}
			if !poolNet.Contains(ip) {
				continue
			}
			// Skip IPs within tenant allocation ranges.
			if ipInTenantRanges(addr.Address, tenantRanges) {
				continue
			}
			nodeRef := &butlerv1alpha1.ClusterObjectReference{
				Name: node.Name,
			}
			if existing, ok := entries[addr.Address]; ok {
				// IP already claimed by a Service: enrich with NodeRef.
				existing.nodeRef = nodeRef
			} else {
				entries[addr.Address] = &infraEntry{
					source:  SourceNode,
					nodeRef: nodeRef,
				}
			}
		}
	}

	// Discover CAPI Machine IPs within the pool CIDR (outside tenant ranges).
	// Machines represent tenant cluster VMs whose IPs are assigned by the
	// infrastructure provider rather than by Butler.
	machineList := &unstructured.UnstructuredList{}
	machineList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "cluster.x-k8s.io",
		Version: "v1beta1",
		Kind:    "MachineList",
	})
	if err := r.List(ctx, machineList); err != nil {
		logger.V(1).Info("unable to list CAPI Machines, skipping machine tracking", "error", err)
	} else {
		for i := range machineList.Items {
			m := &machineList.Items[i]
			for _, ipStr := range extractMachineIPs(m) {
				ip := net.ParseIP(ipStr)
				if ip == nil {
					continue
				}
				if !poolNet.Contains(ip) {
					continue
				}
				if ipInTenantRanges(ipStr, tenantRanges) {
					continue
				}
				machineRef := &butlerv1alpha1.NamespacedObjectReference{
					Name:      m.GetName(),
					Namespace: m.GetNamespace(),
				}
				if existing, ok := entries[ipStr]; ok {
					// IP already claimed by a Node or Service: enrich with MachineRef.
					existing.machineRef = machineRef
				} else {
					entries[ipStr] = &infraEntry{
						source:     SourceMachine,
						machineRef: machineRef,
					}
				}
			}
		}
	}

	// Convert map to sorted slice.
	var desired []butlerv1alpha1.InfrastructureAllocation
	for ip, entry := range entries {
		desired = append(desired, butlerv1alpha1.InfrastructureAllocation{
			IP:         ip,
			Source:     entry.source,
			ServiceRef: entry.serviceRef,
			NodeRef:    entry.nodeRef,
			MachineRef: entry.machineRef,
		})
	}
	sort.Slice(desired, func(i, j int) bool {
		a := net.ParseIP(desired[i].IP).To4()
		b := net.ParseIP(desired[j].IP).To4()
		if a == nil || b == nil {
			return desired[i].IP < desired[j].IP
		}
		return bytes.Compare(a, b) < 0
	})

	// No-op guard: skip the patch if nothing changed.
	if len(desired) == 0 && len(pool.Status.InfrastructureAllocations) == 0 {
		return ctrl.Result{}, nil
	}
	if reflect.DeepEqual(desired, pool.Status.InfrastructureAllocations) {
		return ctrl.Result{}, nil
	}

	logger.Info("updating infrastructure allocations", "count", len(desired))
	return r.patchInfraAllocations(ctx, pool, desired)
}

// ipInTenantRanges returns true if the given IP falls within any tenant
// allocation range.
func ipInTenantRanges(ipStr string, ranges []ipam.AllocatedRange) bool {
	ip := net.ParseIP(ipStr).To4()
	if ip == nil {
		return false
	}
	ipN := ipam.IPToUint32(ip)
	for _, r := range ranges {
		startIP := net.ParseIP(r.Start).To4()
		endIP := net.ParseIP(r.End).To4()
		if startIP == nil || endIP == nil {
			continue
		}
		if ipN >= ipam.IPToUint32(startIP) && ipN <= ipam.IPToUint32(endIP) {
			return true
		}
	}
	return false
}

// patchInfraAllocations patches only the InfrastructureAllocations field
// on NetworkPool status via SSA. Other status fields (TotalIPs, AllocatedIPs,
// conditions, etc.) are untouched. They are managed by the existing
// NetworkPool reconciler through plain Update.
func (r *InfraAllocationReconciler) patchInfraAllocations(
	ctx context.Context,
	pool *butlerv1alpha1.NetworkPool,
	desired []butlerv1alpha1.InfrastructureAllocation,
) (ctrl.Result, error) {
	patch := &butlerv1alpha1.NetworkPool{
		TypeMeta: metav1.TypeMeta{
			APIVersion: butlerv1alpha1.GroupVersion.String(),
			Kind:       "NetworkPool",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      pool.Name,
			Namespace: pool.Namespace,
		},
	}
	patch.Status.InfrastructureAllocations = desired
	patch.Status.InfrastructureConsumedIPs = int32(len(desired))

	if err := r.Status().Patch(ctx, patch, client.Apply,
		client.FieldOwner(infraAllocationFieldManager),
		client.ForceOwnership,
	); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// machineGVK is the GroupVersionKind for CAPI Machine resources.
var machineGVK = schema.GroupVersionKind{
	Group:   "cluster.x-k8s.io",
	Version: "v1beta1",
	Kind:    "Machine",
}

// SetupWithManager registers the controller with the manager.
func (r *InfraAllocationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	machine := &unstructured.Unstructured{}
	machine.SetGroupVersionKind(machineGVK)

	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.NetworkPool{}).
		Watches(&corev1.Service{},
			handler.EnqueueRequestsFromMapFunc(r.mapServiceToNetworkPools),
			builder.WithPredicates(lbIngressChangedPredicate{}),
		).
		Watches(&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(r.mapNodeToNetworkPools),
			builder.WithPredicates(nodeIPChangedPredicate{}),
		).
		Watches(machine,
			handler.EnqueueRequestsFromMapFunc(r.mapMachineToNetworkPools),
			builder.WithPredicates(machineIPChangedPredicate{}),
		).
		Named("infra-allocation").
		Complete(r)
}

// mapServiceToNetworkPools enqueues every NetworkPool when a LoadBalancer
// Service event fires. The reconciler then recomputes desired infrastructure
// allocations from current cluster state, so stale entries are dropped
// naturally regardless of which IP changed or which pool previously
// contained it.
//
// This is correct by construction: any IP move (Service IP changes from
// CIDR-A to CIDR-B across pools) reconciles both pools without requiring
// the mapper to inspect old vs new state on Update events.
//
// Cost: one Reconcile per NetworkPool per LB Service event. Acceptable at
// typical management-cluster scale (1-50 pools, 10-50 LB Services).
// Revisit if pool count approaches 100+.
func (r *InfraAllocationReconciler) mapServiceToNetworkPools(ctx context.Context, _ client.Object) []reconcile.Request {
	poolList := &butlerv1alpha1.NetworkPoolList{}
	if err := r.List(ctx, poolList); err != nil {
		log.FromContext(ctx).Error(err, "failed to list NetworkPools for Service mapper")
		return nil
	}

	requests := make([]reconcile.Request, len(poolList.Items))
	for i, pool := range poolList.Items {
		requests[i] = reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      pool.Name,
				Namespace: pool.Namespace,
			},
		}
	}
	return requests
}

// lbIngressChangedPredicate filters Service events to only those where the
// LoadBalancer ingress IP set changed. Generation predicates are wrong here:
// Service.metadata.generation only bumps on spec changes, but LB IP assignment
// is a status change that does not increment generation.
type lbIngressChangedPredicate struct {
	predicate.Funcs
}

func (lbIngressChangedPredicate) Create(e event.CreateEvent) bool {
	svc, ok := e.Object.(*corev1.Service)
	if !ok {
		return false
	}
	return svc.Spec.Type == corev1.ServiceTypeLoadBalancer &&
		len(svc.Status.LoadBalancer.Ingress) > 0
}

func (lbIngressChangedPredicate) Update(e event.UpdateEvent) bool {
	oldSvc, ok := e.ObjectOld.(*corev1.Service)
	if !ok {
		return false
	}
	newSvc, ok := e.ObjectNew.(*corev1.Service)
	if !ok {
		return false
	}
	return !lbIngressIPsEqual(oldSvc, newSvc)
}

func (lbIngressChangedPredicate) Delete(e event.DeleteEvent) bool {
	svc, ok := e.Object.(*corev1.Service)
	if !ok {
		return false
	}
	return svc.Spec.Type == corev1.ServiceTypeLoadBalancer &&
		len(svc.Status.LoadBalancer.Ingress) > 0
}

// lbIngressIPsEqual returns true if both Services have the same set of
// LoadBalancer ingress IPs.
func lbIngressIPsEqual(old, new *corev1.Service) bool {
	oldIPs := extractLBIPs(old)
	newIPs := extractLBIPs(new)
	if len(oldIPs) != len(newIPs) {
		return false
	}
	for i := range oldIPs {
		if oldIPs[i] != newIPs[i] {
			return false
		}
	}
	return true
}

// extractLBIPs returns the sorted list of LoadBalancer ingress IPs from a Service.
func extractLBIPs(svc *corev1.Service) []string {
	var ips []string
	for _, ingress := range svc.Status.LoadBalancer.Ingress {
		if ingress.IP != "" {
			ips = append(ips, ingress.IP)
		}
	}
	sort.Strings(ips)
	return ips
}

// mapNodeToNetworkPools enqueues every NetworkPool when a Node's InternalIP
// changes. Same fan-out strategy as mapServiceToNetworkPools.
func (r *InfraAllocationReconciler) mapNodeToNetworkPools(ctx context.Context, _ client.Object) []reconcile.Request {
	poolList := &butlerv1alpha1.NetworkPoolList{}
	if err := r.List(ctx, poolList); err != nil {
		log.FromContext(ctx).Error(err, "failed to list NetworkPools for Node mapper")
		return nil
	}

	requests := make([]reconcile.Request, len(poolList.Items))
	for i, pool := range poolList.Items {
		requests[i] = reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      pool.Name,
				Namespace: pool.Namespace,
			},
		}
	}
	return requests
}

// nodeIPChangedPredicate filters Node events to only those where the
// InternalIP address set changed.
type nodeIPChangedPredicate struct {
	predicate.Funcs
}

func (nodeIPChangedPredicate) Create(e event.CreateEvent) bool {
	node, ok := e.Object.(*corev1.Node)
	if !ok {
		return false
	}
	return len(extractNodeIPs(node)) > 0
}

func (nodeIPChangedPredicate) Update(e event.UpdateEvent) bool {
	oldNode, ok := e.ObjectOld.(*corev1.Node)
	if !ok {
		return false
	}
	newNode, ok := e.ObjectNew.(*corev1.Node)
	if !ok {
		return false
	}
	return !nodeIPsEqual(oldNode, newNode)
}

func (nodeIPChangedPredicate) Delete(e event.DeleteEvent) bool {
	node, ok := e.Object.(*corev1.Node)
	if !ok {
		return false
	}
	return len(extractNodeIPs(node)) > 0
}

// nodeIPsEqual returns true if both Nodes have the same set of InternalIPs.
func nodeIPsEqual(old, new *corev1.Node) bool {
	oldIPs := extractNodeIPs(old)
	newIPs := extractNodeIPs(new)
	if len(oldIPs) != len(newIPs) {
		return false
	}
	for i := range oldIPs {
		if oldIPs[i] != newIPs[i] {
			return false
		}
	}
	return true
}

// extractNodeIPs returns the sorted list of InternalIP addresses from a Node.
func extractNodeIPs(node *corev1.Node) []string {
	var ips []string
	for _, addr := range node.Status.Addresses {
		if addr.Type == corev1.NodeInternalIP && addr.Address != "" {
			ips = append(ips, addr.Address)
		}
	}
	sort.Strings(ips)
	return ips
}

// mapMachineToNetworkPools enqueues every NetworkPool when a CAPI Machine's
// IP changes. Same fan-out strategy as mapServiceToNetworkPools.
func (r *InfraAllocationReconciler) mapMachineToNetworkPools(ctx context.Context, _ client.Object) []reconcile.Request {
	poolList := &butlerv1alpha1.NetworkPoolList{}
	if err := r.List(ctx, poolList); err != nil {
		log.FromContext(ctx).Error(err, "failed to list NetworkPools for Machine mapper")
		return nil
	}

	requests := make([]reconcile.Request, len(poolList.Items))
	for i, pool := range poolList.Items {
		requests[i] = reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      pool.Name,
				Namespace: pool.Namespace,
			},
		}
	}
	return requests
}

// machineIPChangedPredicate filters CAPI Machine events to only those where
// the InternalIP address set changed. Works with unstructured objects.
type machineIPChangedPredicate struct {
	predicate.Funcs
}

func (machineIPChangedPredicate) Create(e event.CreateEvent) bool {
	return len(extractMachineIPs(e.Object)) > 0
}

func (machineIPChangedPredicate) Update(e event.UpdateEvent) bool {
	return !stringSlicesEqual(extractMachineIPs(e.ObjectOld), extractMachineIPs(e.ObjectNew))
}

func (machineIPChangedPredicate) Delete(e event.DeleteEvent) bool {
	return len(extractMachineIPs(e.Object)) > 0
}

// stringSlicesEqual returns true if both slices have the same elements.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// extractMachineIPs returns the sorted list of InternalIP addresses from a
// CAPI Machine resource. The Machine's status.addresses follows the same
// schema as Node status.addresses: [{type, address}, ...].
func extractMachineIPs(obj client.Object) []string {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil
	}
	addrs, found, err := unstructured.NestedSlice(u.Object, "status", "addresses")
	if err != nil || !found {
		return nil
	}
	var ips []string
	for _, a := range addrs {
		addrMap, ok := a.(map[string]interface{})
		if !ok {
			continue
		}
		addrType, _ := addrMap["type"].(string)
		address, _ := addrMap["address"].(string)
		if addrType == "InternalIP" && address != "" {
			ips = append(ips, address)
		}
	}
	sort.Strings(ips)
	return ips
}
