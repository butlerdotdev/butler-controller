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
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const (
	// SourceMetalLB identifies IPs consumed by MetalLB LoadBalancer Services.
	SourceMetalLB = "metallb"

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

	// Parse reserved ranges into net.IPNet for containment checks.
	var reservedNets []*net.IPNet
	for _, reserved := range pool.Spec.Reserved {
		_, network, err := net.ParseCIDR(reserved.CIDR)
		if err != nil {
			logger.V(1).Info("skipping invalid reserved CIDR", "cidr", reserved.CIDR, "error", err)
			continue
		}
		reservedNets = append(reservedNets, network)
	}

	if len(reservedNets) == 0 {
		if len(pool.Status.InfrastructureAllocations) > 0 {
			return r.patchInfraAllocations(ctx, pool, nil)
		}
		return ctrl.Result{}, nil
	}

	// List all Services cluster-wide.
	svcList := &corev1.ServiceList{}
	if err := r.List(ctx, svcList); err != nil {
		return ctrl.Result{}, err
	}

	// Build desired InfrastructureAllocations from LB Services whose IPs
	// fall within reserved ranges.
	var desired []butlerv1alpha1.InfrastructureAllocation
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
					desired = append(desired, butlerv1alpha1.InfrastructureAllocation{
						IP:     ingress.IP,
						Source: SourceMetalLB,
						ServiceRef: &butlerv1alpha1.NamespacedObjectReference{
							Name:      svc.Name,
							Namespace: svc.Namespace,
						},
					})
					break
				}
			}
		}
	}

	// Sort by numeric IP for deterministic output.
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

// patchInfraAllocations patches only the InfrastructureAllocations field
// on NetworkPool status via SSA. Other status fields (TotalIPs, AllocatedIPs,
// conditions, etc.) are untouched — they are managed by the existing
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

	if err := r.Status().Patch(ctx, patch, client.Apply,
		client.FieldOwner(infraAllocationFieldManager),
		client.ForceOwnership,
	); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers the controller with the manager.
func (r *InfraAllocationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.NetworkPool{}).
		Watches(&corev1.Service{},
			handler.EnqueueRequestsFromMapFunc(r.mapServiceToNetworkPools),
			builder.WithPredicates(lbIngressChangedPredicate{}),
		).
		Named("infra-allocation").
		Complete(r)
}

// mapServiceToNetworkPools enqueues all NetworkPools when a LB Service's
// ingress IPs change. There are typically 1-5 pools per management cluster,
// so a full list is cheaper than maintaining a CIDR-based index. Enqueuing
// all pools also correctly handles IP moves: both the old pool (which loses
// the IP) and the new pool (which gains it) get reconciled.
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
