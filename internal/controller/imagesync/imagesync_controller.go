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

package imagesync

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// Reconciler reconciles an ImageSync object.
// This is a thin lifecycle controller — finalizers, phase initialization, and
// periodic requeue. The actual image build/download/upload is handled by the
// provider controller that watches ImageSync resources.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=imagesyncs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=imagesyncs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=imagesyncs/finalizers,verbs=update

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	is := &butlerv1alpha1.ImageSync{}
	if err := r.Get(ctx, req.NamespacedName, is); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.V(1).Info("reconciling ImageSync", "name", is.Name, "phase", is.Status.Phase)

	// Handle deletion
	if !is.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, is)
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(is, butlerv1alpha1.FinalizerImageSync) {
		controllerutil.AddFinalizer(is, butlerv1alpha1.FinalizerImageSync)
		if err := r.Update(ctx, is); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Set initial phase if not set
	if is.Status.Phase == "" {
		is.Status.Phase = butlerv1alpha1.ImageSyncPhasePending
		is.Status.ObservedGeneration = is.Generation
		if err := r.Status().Update(ctx, is); err != nil {
			return ctrl.Result{}, err
		}
		// Return immediately — the provider controller will pick this up
		return ctrl.Result{}, nil
	}

	switch is.Status.Phase {
	case butlerv1alpha1.ImageSyncPhasePending,
		butlerv1alpha1.ImageSyncPhaseBuilding,
		butlerv1alpha1.ImageSyncPhaseDownloading,
		butlerv1alpha1.ImageSyncPhaseUploading:
		// In-progress — periodic check
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	case butlerv1alpha1.ImageSyncPhaseFailed:
		// Backstop retry — primary retry is event-driven via provider controller
		return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
	case butlerv1alpha1.ImageSyncPhaseReady:
		// Healthy — periodic check
		return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
	}

	return ctrl.Result{}, nil
}

func (r *Reconciler) handleDeletion(ctx context.Context, is *butlerv1alpha1.ImageSync) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(is, butlerv1alpha1.FinalizerImageSync) {
		return ctrl.Result{}, nil
	}

	logger.Info("removing ImageSync finalizer", "name", is.Name)

	controllerutil.RemoveFinalizer(is, butlerv1alpha1.FinalizerImageSync)
	return ctrl.Result{}, r.Update(ctx, is)
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.ImageSync{}).
		Named("imagesync").
		Complete(r)
}
