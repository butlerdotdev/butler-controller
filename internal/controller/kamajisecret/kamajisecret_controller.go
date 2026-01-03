/*
Copyright 2026 Butler Labs.

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

package kamajisecret

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Reconciler reconciles Kamaji kubeconfig Secrets.
//
// Kamaji creates kubeconfig secrets with the key "admin.conf", but CAPI
// expects the key "value". This controller watches for Kamaji-created
// kubeconfig secrets and creates a CAPI-compatible copy.
//
// This is a workaround for a known integration issue between Kamaji and
// the CAPI Harvester provider. Once this is fixed upstream, this controller
// can be removed or simplified.
//
// How it works:
// 1. Watch Secrets with label "kamaji.clastix.io/component: admin-kubeconfig"
// 2. When found, create a new Secret with the same kubeconfig but key "value"
// 3. The CAPI Harvester provider uses this translated secret
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	// KamajiKubeconfigLabel identifies Kamaji admin kubeconfig secrets.
	KamajiKubeconfigLabel = "kamaji.clastix.io/component"
	KamajiKubeconfigValue = "admin-kubeconfig"

	// KamajiSourceKey is the key Kamaji uses in the secret.
	KamajiSourceKey = "admin.conf"

	// CAPITargetKey is the key CAPI expects.
	CAPITargetKey = "value"

	// TranslatedSecretSuffix is added to create the CAPI-compatible secret name.
	TranslatedSecretSuffix = "-capi-kubeconfig"
)

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch

// Reconcile handles Kamaji kubeconfig secret translation.
//
// The reconciliation:
// 1. Check if this is a Kamaji admin-kubeconfig secret
// 2. Extract kubeconfig from "admin.conf" key
// 3. Create/update a new secret with "value" key
// 4. Add labels for discoverability
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the Secret
	secret := &corev1.Secret{}
	if err := r.Get(ctx, req.NamespacedName, secret); err != nil {
		if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "unable to fetch Secret")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Verify this is a Kamaji kubeconfig secret
	if !isKamajiKubeconfigSecret(secret) {
		return ctrl.Result{}, nil
	}

	logger.Info("reconciling Kamaji kubeconfig secret",
		"name", secret.Name,
		"namespace", secret.Namespace)

	// Get the kubeconfig data
	kubeconfigData, ok := secret.Data[KamajiSourceKey]
	if !ok {
		logger.V(1).Info("secret does not contain admin.conf key, skipping")
		return ctrl.Result{}, nil
	}

	// Create or update the CAPI-compatible secret
	if err := r.ensureCAPISecret(ctx, secret, kubeconfigData); err != nil {
		logger.Error(err, "failed to ensure CAPI secret")
		return ctrl.Result{}, err
	}

	logger.Info("CAPI-compatible kubeconfig secret ensured",
		"translatedSecret", secret.Name+TranslatedSecretSuffix)

	return ctrl.Result{}, nil
}

// isKamajiKubeconfigSecret checks if the secret is a Kamaji admin kubeconfig.
func isKamajiKubeconfigSecret(secret *corev1.Secret) bool {
	if secret.Labels == nil {
		return false
	}
	return secret.Labels[KamajiKubeconfigLabel] == KamajiKubeconfigValue
}

// ensureCAPISecret creates or updates the CAPI-compatible secret.
func (r *Reconciler) ensureCAPISecret(ctx context.Context, source *corev1.Secret, kubeconfig []byte) error {
	logger := log.FromContext(ctx)

	targetName := source.Name + TranslatedSecretSuffix

	// Check if target secret exists
	target := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{
		Name:      targetName,
		Namespace: source.Namespace,
	}, target)

	if client.IgnoreNotFound(err) != nil {
		return err
	}

	if err != nil {
		// Secret doesn't exist, create it
		target = &corev1.Secret{
			ObjectMeta: ctrl.ObjectMeta{
				Name:      targetName,
				Namespace: source.Namespace,
				Labels: map[string]string{
					"butler.butlerlabs.dev/managed-by": "butler-controller",
					"butler.butlerlabs.dev/source":     source.Name,
					"cluster.x-k8s.io/cluster-name":    extractClusterName(source),
				},
			},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{
				CAPITargetKey: kubeconfig,
			},
		}

		if err := r.Create(ctx, target); err != nil {
			return err
		}
		logger.Info("created CAPI-compatible secret", "name", targetName)
		return nil
	}

	// Secret exists, update if needed
	if string(target.Data[CAPITargetKey]) != string(kubeconfig) {
		target.Data[CAPITargetKey] = kubeconfig
		if err := r.Update(ctx, target); err != nil {
			return err
		}
		logger.Info("updated CAPI-compatible secret", "name", targetName)
	}

	return nil
}

// extractClusterName extracts the cluster name from the Kamaji secret.
func extractClusterName(secret *corev1.Secret) string {
	// Kamaji secrets are typically named like "{cluster-name}-admin-kubeconfig"
	// Try to extract the cluster name from labels first
	if name, ok := secret.Labels["kamaji.clastix.io/name"]; ok {
		return name
	}
	// Fallback: use the namespace as a hint
	return secret.Namespace
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Secret{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(object client.Object) bool {
			// Only watch secrets with Kamaji kubeconfig label
			secret, ok := object.(*corev1.Secret)
			if !ok {
				return false
			}
			return isKamajiKubeconfigSecret(secret)
		})).
		Named("kamajisecret").
		Complete(r)
}
