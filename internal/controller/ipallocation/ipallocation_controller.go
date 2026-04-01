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

package ipallocation

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// Reconciler reconciles an IPAllocation object.
// This is a thin controller — validation, finalizers, and deletion cleanup.
// Allocation itself is handled by the NetworkPool controller.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=ipallocations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=ipallocations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=ipallocations/finalizers,verbs=update

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	alloc := &butlerv1alpha1.IPAllocation{}
	if err := r.Get(ctx, req.NamespacedName, alloc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.V(1).Info("reconciling IPAllocation", "name", alloc.Name, "phase", alloc.Status.Phase)

	// Handle deletion
	if !alloc.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, alloc)
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(alloc, butlerv1alpha1.FinalizerIPAllocation) {
		controllerutil.AddFinalizer(alloc, butlerv1alpha1.FinalizerIPAllocation)
		if err := r.Update(ctx, alloc); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Set initial phase if not set
	if alloc.Status.Phase == "" {
		alloc.Status.Phase = butlerv1alpha1.IPAllocationPhasePending
		alloc.Status.ObservedGeneration = alloc.Generation
		if err := r.Status().Update(ctx, alloc); err != nil {
			return ctrl.Result{}, err
		}
		// Return immediately — the NetworkPool controller will pick this up
		return ctrl.Result{}, nil
	}

	switch alloc.Status.Phase {
	case butlerv1alpha1.IPAllocationPhasePending:
		// Waiting for NetworkPool controller to fulfill
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	case butlerv1alpha1.IPAllocationPhaseFailed:
		// Backstop retry — primary retry is event-driven via NetworkPool controller
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	case butlerv1alpha1.IPAllocationPhaseAllocated:
		// Healthy — periodic check
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	return ctrl.Result{}, nil
}

func (r *Reconciler) handleDeletion(ctx context.Context, alloc *butlerv1alpha1.IPAllocation) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(alloc, butlerv1alpha1.FinalizerIPAllocation) {
		return ctrl.Result{}, nil
	}

	// Set released timestamp for audit trail
	if alloc.Status.ReleasedAt == nil {
		now := metav1.Now()
		alloc.Status.ReleasedAt = &now
		alloc.Status.Phase = butlerv1alpha1.IPAllocationPhaseReleased
		if err := r.Status().Update(ctx, alloc); err != nil {
			return ctrl.Result{}, err
		}
	}

	logger.Info("releasing IPAllocation", "name", alloc.Name,
		"start", alloc.Status.StartAddress, "end", alloc.Status.EndAddress)

	latest := &butlerv1alpha1.IPAllocation{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(alloc), latest); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if controllerutil.ContainsFinalizer(latest, butlerv1alpha1.FinalizerIPAllocation) {
		controllerutil.RemoveFinalizer(latest, butlerv1alpha1.FinalizerIPAllocation)
		if err := r.Update(ctx, latest); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.IPAllocation{}).
		Named("ipallocation").
		Complete(r)
}
