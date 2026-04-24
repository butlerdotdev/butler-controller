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

// Package stewardsecret provides a controller that translates Steward kubeconfig secrets
// to the format expected by Cluster API.
//
// Problem: Steward creates kubeconfig secrets with key "admin.conf", but CAPI expects "value".
// Solution: Watch Steward secrets and create/update corresponding CAPI-compatible secrets.
package stewardsecret

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	// StewardAdminKubeconfigLabel is the label that identifies Steward admin kubeconfig secrets.
	StewardAdminKubeconfigLabel = "steward.butlerlabs.dev/component"
	StewardAdminKubeconfigValue = "admin-kubeconfig"

	// StewardSourceKey is the key Steward uses for the kubeconfig.
	StewardSourceKey = "admin.conf"

	// CAPITargetKey is the key CAPI expects for the kubeconfig.
	CAPITargetKey = "value"

	// CAPIClusterNameLabel is the standard CAPI label for cluster name.
	CAPIClusterNameLabel = "cluster.x-k8s.io/cluster-name"

	// ButlerManagedLabel marks resources managed by Butler.
	ButlerManagedLabel = "app.kubernetes.io/managed-by"
)

// Reconciler watches Steward kubeconfig secrets and creates CAPI-compatible versions.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch

// Reconcile handles Steward secret translation.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Get the Steward admin-kubeconfig secret
	stewardSecret := &corev1.Secret{}
	if err := r.Get(ctx, req.NamespacedName, stewardSecret); err != nil {
		if apierrors.IsNotFound(err) {
			// Secret was deleted, CAPI secret will be garbage collected via owner reference
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Skip if not a Steward admin kubeconfig secret
	if stewardSecret.Labels[StewardAdminKubeconfigLabel] != StewardAdminKubeconfigValue {
		return ctrl.Result{}, nil
	}

	// Extract kubeconfig from Steward secret
	kubeconfigData, ok := stewardSecret.Data[StewardSourceKey]
	if !ok {
		logger.V(1).Info("Steward secret missing admin.conf key", "secret", req.NamespacedName)
		return ctrl.Result{}, nil
	}

	// Derive cluster name from secret name (format: {cluster-name}-admin-kubeconfig)
	clusterName := extractClusterName(stewardSecret.Name)
	if clusterName == "" {
		logger.Info("could not extract cluster name from secret", "secret", stewardSecret.Name)
		return ctrl.Result{}, nil
	}

	// Create or update CAPI-compatible secret
	capiSecretName := fmt.Sprintf("%s-kubeconfig", clusterName)
	capiSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      capiSecretName,
			Namespace: req.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, capiSecret, func() error {
		// CAPI expects kubeconfig secrets to have this type.
		// Must be set on create; Secret type is immutable after creation.
		if capiSecret.CreationTimestamp.IsZero() {
			capiSecret.Type = corev1.SecretType("cluster.x-k8s.io/secret")
		}

		// Set labels
		if capiSecret.Labels == nil {
			capiSecret.Labels = make(map[string]string)
		}
		capiSecret.Labels[CAPIClusterNameLabel] = clusterName
		capiSecret.Labels[ButlerManagedLabel] = "butler"
		capiSecret.Labels["butler.butlerlabs.dev/secret-type"] = "kubeconfig"

		// Set data with CAPI-expected key
		capiSecret.Data = map[string][]byte{
			CAPITargetKey: kubeconfigData,
		}

		// Set owner reference to the original Steward secret
		// This ensures cleanup when the Steward secret is deleted
		return controllerutil.SetOwnerReference(stewardSecret, capiSecret, r.Scheme)
	})

	if err != nil {
		logger.Error(err, "failed to create/update CAPI kubeconfig secret",
			"secret", capiSecretName,
			"namespace", req.Namespace)
		return ctrl.Result{}, err
	}

	if op != controllerutil.OperationResultNone {
		logger.Info("CAPI kubeconfig secret reconciled",
			"secret", capiSecretName,
			"namespace", req.Namespace,
			"operation", op,
			"cluster", clusterName)
	}

	return ctrl.Result{}, nil
}

// extractClusterName extracts the cluster name from a Steward admin-kubeconfig secret name.
// Format: {cluster-name}-admin-kubeconfig -> cluster-name
func extractClusterName(secretName string) string {
	const suffix = "-admin-kubeconfig"
	if strings.HasSuffix(secretName, suffix) {
		return strings.TrimSuffix(secretName, suffix)
	}
	return ""
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Only watch secrets with the Steward admin-kubeconfig label
	labelPredicate := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		labels := obj.GetLabels()
		return labels[StewardAdminKubeconfigLabel] == StewardAdminKubeconfigValue
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Secret{}, builder.WithPredicates(labelPredicate)).
		Named("stewardsecret").
		Complete(r)
}
