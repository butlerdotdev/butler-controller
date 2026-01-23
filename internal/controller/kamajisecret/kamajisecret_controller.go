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

// Package kamajisecret provides a controller that translates Kamaji kubeconfig secrets
// to the format expected by Cluster API.
//
// Problem: Kamaji creates kubeconfig secrets with key "admin.conf", but CAPI expects "value".
// Solution: Watch Kamaji secrets and create/update corresponding CAPI-compatible secrets.
package kamajisecret

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
	// StewardAdminKubeconfigLabel is the label that identifies Kamaji admin kubeconfig secrets.
	StewardAdminKubeconfigLabel = "steward.butlerlabs.dev/component"
	KamajiAdminKubeconfigValue = "admin-kubeconfig"

	// KamajiSourceKey is the key Kamaji uses for the kubeconfig.
	KamajiSourceKey = "admin.conf"

	// CAPITargetKey is the key CAPI expects for the kubeconfig.
	CAPITargetKey = "value"

	// CAPIClusterNameLabel is the standard CAPI label for cluster name.
	CAPIClusterNameLabel = "cluster.x-k8s.io/cluster-name"

	// ButlerManagedLabel marks resources managed by Butler.
	ButlerManagedLabel = "app.kubernetes.io/managed-by"
)

// Reconciler watches Kamaji kubeconfig secrets and creates CAPI-compatible versions.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch

// Reconcile handles Kamaji secret translation.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Get the Kamaji secret
	kamajiSecret := &corev1.Secret{}
	if err := r.Get(ctx, req.NamespacedName, kamajiSecret); err != nil {
		if apierrors.IsNotFound(err) {
			// Secret was deleted, CAPI secret will be garbage collected via owner reference
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Skip if not a Kamaji admin kubeconfig secret
	if kamajiSecret.Labels[StewardAdminKubeconfigLabel] != KamajiAdminKubeconfigValue {
		return ctrl.Result{}, nil
	}

	// Extract kubeconfig from Kamaji secret
	kubeconfigData, ok := kamajiSecret.Data[KamajiSourceKey]
	if !ok {
		logger.V(1).Info("Kamaji secret missing admin.conf key", "secret", req.NamespacedName)
		return ctrl.Result{}, nil
	}

	// Derive cluster name from secret name (format: {cluster-name}-admin-kubeconfig)
	clusterName := extractClusterName(kamajiSecret.Name)
	if clusterName == "" {
		logger.Info("could not extract cluster name from secret", "secret", kamajiSecret.Name)
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

		// Set owner reference to the original Kamaji secret
		// This ensures cleanup when the Kamaji secret is deleted
		return controllerutil.SetOwnerReference(kamajiSecret, capiSecret, r.Scheme)
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

// extractClusterName extracts the cluster name from a Kamaji secret name.
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
	// Only watch secrets with the Kamaji admin-kubeconfig label
	labelPredicate := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		labels := obj.GetLabels()
		return labels[StewardAdminKubeconfigLabel] == KamajiAdminKubeconfigValue
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Secret{}, builder.WithPredicates(labelPredicate)).
		Named("kamajisecret").
		Complete(r)
}
