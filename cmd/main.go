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

package main

import (
	"flag"
	"os"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"

	"github.com/butlerdotdev/butler-controller/internal/addons"
	"github.com/butlerdotdev/butler-controller/internal/controller/butlerconfig"
	"github.com/butlerdotdev/butler-controller/internal/controller/imagesync"
	"github.com/butlerdotdev/butler-controller/internal/controller/ipallocation"
	"github.com/butlerdotdev/butler-controller/internal/controller/kamajisecret"
	"github.com/butlerdotdev/butler-controller/internal/controller/kamajistatus"
	"github.com/butlerdotdev/butler-controller/internal/controller/managementaddon"
	"github.com/butlerdotdev/butler-controller/internal/controller/networkpool"
	"github.com/butlerdotdev/butler-controller/internal/controller/providerconfig"
	"github.com/butlerdotdev/butler-controller/internal/controller/team"
	"github.com/butlerdotdev/butler-controller/internal/controller/tenantaddon"
	"github.com/butlerdotdev/butler-controller/internal/controller/tenantcluster"
	"github.com/butlerdotdev/butler-controller/internal/controller/workspace"
	"github.com/butlerdotdev/butler-controller/internal/webhook"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	// Register core Kubernetes types
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(rbacv1.AddToScheme(scheme))

	// Register Butler API types
	utilruntime.Must(butlerv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var probeAddr string
	var enableLeaderElection bool
	var enableKamajiControllers bool
	var enableWebhooks bool

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&enableKamajiControllers, "enable-kamaji-controllers", true,
		"Enable Kamaji integration controllers (KamajiSecret, KamajiStatus). "+
			"Disable if not using Kamaji for hosted control planes.")
	flag.BoolVar(&enableWebhooks, "enable-webhooks", false,
		"Enable admission webhooks for TenantCluster, NetworkPool, and ProviderConfig. "+
			"Requires cert-manager for TLS certificate management.")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "butler-controller.butlerlabs.dev",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Read kubeconfig bytes for ManagementAddon controller's Helm operations.
	// Uses KUBECONFIG env var - same source controller-runtime uses.
	// When running in-cluster, this will be empty and the controller will
	// build an in-cluster kubeconfig from the service account.
	var kubeconfigBytes []byte
	if kubeconfigPath := os.Getenv("KUBECONFIG"); kubeconfigPath != "" {
		var readErr error
		kubeconfigBytes, readErr = os.ReadFile(kubeconfigPath)
		if readErr != nil {
			setupLog.Error(readErr, "failed to read kubeconfig file", "path", kubeconfigPath)
			os.Exit(1)
		}
		setupLog.Info("loaded kubeconfig for management addon operations", "path", kubeconfigPath)
	}

	// ButlerConfig controller. Validates platform configuration
	if err = (&butlerconfig.Reconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ButlerConfig")
		os.Exit(1)
	}

	// Team controller. Manages team namespaces and RBAC
	if err = (&team.Reconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Team")
		os.Exit(1)
	}

	// TenantCluster controller. Orchestrates CAPI resources
	if err = (&tenantcluster.Reconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Installer: addons.NewInstaller(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "TenantCluster")
		os.Exit(1)
	}

	// TenantAddon controller. Installs optional addons via TenantAddon CRs
	if err = (&tenantaddon.Reconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Installer: addons.NewInstaller(),
		APIReader: mgr.GetAPIReader(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "TenantAddon")
		os.Exit(1)
	}

	// ManagementAddon controller. Installs addons on the management cluster
	if err = (&managementaddon.Reconciler{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		Installer:  addons.NewInstaller(),
		APIReader:  mgr.GetAPIReader(),
		Kubeconfig: kubeconfigBytes,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ManagementAddon")
		os.Exit(1)
	}

	// NetworkPool controller. IPAM allocator — sole writer for IP allocations
	if err = (&networkpool.Reconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("networkpool-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "NetworkPool")
		os.Exit(1)
	}

	// IPAllocation controller. Thin controller for lifecycle management
	if err = (&ipallocation.Reconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "IPAllocation")
		os.Exit(1)
	}

	// ImageSync controller. Fulfills image syncs from factory to providers
	if err = (&imagesync.Reconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ImageSync")
		os.Exit(1)
	}

	// ProviderConfig controller. Health checks and capacity reporting
	if err = (&providerconfig.Reconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("providerconfig-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ProviderConfig")
		os.Exit(1)
	}

	// Workspace controller. Manages cloud development environments in tenant clusters
	if err = (&workspace.Reconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Workspace")
		os.Exit(1)
	}

	// Admission webhooks
	if enableWebhooks {
		if err = (&webhook.TenantClusterValidator{}).SetupWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "TenantCluster")
			os.Exit(1)
		}
		if err = (&webhook.NetworkPoolValidator{}).SetupWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "NetworkPool")
			os.Exit(1)
		}
		if err = (&webhook.ProviderConfigValidator{}).SetupWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook", "webhook", "ProviderConfig")
			os.Exit(1)
		}
		setupLog.Info("admission webhooks enabled")
	} else {
		setupLog.Info("admission webhooks disabled")
	}

	// Kamaji integration controllers
	if enableKamajiControllers {
		// KamajiSecret controller. Translates kubeconfig format
		if err = (&kamajisecret.Reconciler{
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "KamajiSecret")
			os.Exit(1)
		}

		// KamajiStatus controller
		if err = (&kamajistatus.Reconciler{
			Client: mgr.GetClient(),
			Scheme: mgr.GetScheme(),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create controller", "controller", "KamajiStatus")
			os.Exit(1)
		}
		setupLog.Info("Kamaji integration controllers enabled")
	} else {
		setupLog.Info("Kamaji integration controllers disabled")
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager",
		"controllers", []string{"ButlerConfig", "Team", "TenantCluster", "TenantAddon", "ManagementAddon", "NetworkPool", "IPAllocation", "ImageSync", "ProviderConfig", "Workspace", "KamajiSecret", "KamajiStatus"})

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
