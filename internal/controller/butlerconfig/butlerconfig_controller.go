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

package butlerconfig

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// Reconciler reconciles a ButlerConfig object.
//
// ButlerConfig is a singleton resource that configures platform-wide settings.
// Only one ButlerConfig named "butler" should exist in the cluster.
//
// Responsibilities:
// - Validate the ButlerConfig singleton exists (or create default)
// - Create default namespace if Disabled mode
// - Update aggregate counts (teams, clusters) in status
// - Propagate configuration changes to other controllers
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=butlerconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=butlerconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=butlerconfigs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch;create

// Reconcile handles ButlerConfig reconciliation.
//
// The reconciliation loop:
// 1. Ensure singleton exists (name must be "butler")
// 2. Validate configuration
// 3. Create default namespace if mode is Disabled/Optional
// 4. Count teams and clusters for status
// 5. Update status conditions
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Only process the singleton "butler" config
	if req.Name != "butler" {
		logger.V(1).Info("ignoring non-singleton ButlerConfig", "name", req.Name)
		return ctrl.Result{}, nil
	}

	// Fetch the ButlerConfig instance
	config := &butlerv1alpha1.ButlerConfig{}
	if err := r.Get(ctx, req.NamespacedName, config); err != nil {
		if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "unable to fetch ButlerConfig")
			return ctrl.Result{}, err
		}
		// ButlerConfig not found - this is expected on first run
		// Could create a default one here, or let the operator handle it
		logger.Info("ButlerConfig not found, waiting for creation")
		return ctrl.Result{}, nil
	}

	logger.Info("reconciling ButlerConfig",
		"mode", config.Spec.MultiTenancy.Mode,
		"defaultNamespace", config.Spec.DefaultNamespace)

	// TODO: Implement reconciliation logic
	// 1. Validate configuration
	// 2. Ensure default namespace exists (if mode != Enforced)
	// 3. Count teams and clusters
	// 4. Update status

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.ButlerConfig{}).
		Named("butlerconfig").
		Complete(r)
}
