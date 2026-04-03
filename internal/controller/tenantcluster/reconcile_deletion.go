// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/capi"
)

func (r *Reconciler) handleDeletion(ctx context.Context, tc *butlerv1alpha1.TenantCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("handling TenantCluster deletion", "name", tc.Name, "namespace", tc.Namespace)

	tc.Status.Phase = butlerv1alpha1.TenantClusterPhaseDeleting
	r.Recorder.Eventf(tc, corev1.EventTypeNormal, "ClusterDeleting", "Cluster %s deletion started", tc.Name)
	if err := r.Status().Update(ctx, tc); err != nil {
		if !apierrors.IsConflict(err) {
			return ctrl.Result{}, err
		}
	}

	// Evict cached tenant client before cleanup
	if r.ClientManager != nil && tc.Status.TenantNamespace != "" {
		r.ClientManager.RemoveClient(tc.Status.TenantNamespace, tc.Name)
	}

	// Clean up IPAllocations before deleting namespace
	r.cleanupIPAllocations(ctx, tc)

	if tc.Status.TenantNamespace != "" {
		// Delete the CAPI Cluster first so the CAPI controller can remove its
		// finalizer while dependent resources (KubevirtCluster, StewardControlPlane)
		// still exist. Deleting the namespace first cascade-deletes those resources,
		// leaving the CAPI Cluster with a finalizer that can never be removed.
		capiCluster := &unstructured.Unstructured{}
		capiCluster.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   capi.ClusterAPIGroup,
			Version: capi.ClusterAPIVersion,
			Kind:    "Cluster",
		})
		if err := r.Get(ctx, types.NamespacedName{
			Name:      tc.Name,
			Namespace: tc.Status.TenantNamespace,
		}, capiCluster); err == nil {
			if capiCluster.GetDeletionTimestamp().IsZero() {
				logger.Info("deleting CAPI Cluster before namespace", "cluster", tc.Name)
				if err := r.Delete(ctx, capiCluster); err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
			}
			// CAPI Cluster still exists (finalizer pending), wait for CAPI controller to clean up
			logger.V(1).Info("waiting for CAPI Cluster deletion", "cluster", tc.Name)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}

		// CAPI Cluster is gone, safe to delete the namespace
		ns := &corev1.Namespace{}
		err := r.Get(ctx, types.NamespacedName{Name: tc.Status.TenantNamespace}, ns)
		if err == nil {
			logger.Info("deleting tenant namespace", "namespace", tc.Status.TenantNamespace)
			if err := r.Delete(ctx, ns); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		logger.Info("tenant namespace deleted", "namespace", tc.Status.TenantNamespace)
	}

	latest := &butlerv1alpha1.TenantCluster{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(tc), latest); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if controllerutil.ContainsFinalizer(latest, butlerv1alpha1.FinalizerTenantCluster) {
		controllerutil.RemoveFinalizer(latest, butlerv1alpha1.FinalizerTenantCluster)
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

	logger.Info("TenantCluster deletion complete", "name", tc.Name)
	return ctrl.Result{}, nil
}
