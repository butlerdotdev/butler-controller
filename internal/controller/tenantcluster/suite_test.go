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

package tenantcluster

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

var (
	cfg       *rest.Config
	k8sClient client.Client
	testEnv   *envtest.Environment
	ctx       context.Context
	cancel    context.CancelFunc
)

func TestTenantCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "TenantCluster Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "..", "..", "butler-api", "config", "crd", "bases"),
		},
		ErrorIfCRDPathMissing: true,
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	err = butlerv1alpha1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// Start the controller manager with metrics disabled to avoid port conflicts
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme.Scheme,
		Metrics: metricsserver.Options{
			BindAddress: "0", // Disable metrics server in tests
		},
	})
	Expect(err).NotTo(HaveOccurred())

	err = (&Reconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("tenantcluster-controller"),
	}).SetupWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	go func() {
		defer GinkgoRecover()
		err = mgr.Start(ctx)
		Expect(err).NotTo(HaveOccurred())
	}()

	// Wait for cache to sync
	time.Sleep(100 * time.Millisecond)
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})

// Helper to create a ButlerConfig
func newButlerConfig(mode butlerv1alpha1.MultiTenancyMode, defaultNS string) *butlerv1alpha1.ButlerConfig {
	return &butlerv1alpha1.ButlerConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: ButlerConfigSingletonName,
		},
		Spec: butlerv1alpha1.ButlerConfigSpec{
			MultiTenancy: butlerv1alpha1.MultiTenancyConfig{
				Mode: mode,
			},
			DefaultNamespace: defaultNS,
		},
	}
}

// Helper to create a Team
func newTeam(name string) *butlerv1alpha1.Team {
	return &butlerv1alpha1.Team{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: butlerv1alpha1.TeamSpec{
			DisplayName: name,
			Access: butlerv1alpha1.TeamAccess{
				Users: []butlerv1alpha1.TeamUser{
					{Name: "test@example.com", Role: butlerv1alpha1.TeamRoleAdmin},
				},
			},
		},
	}
}

// Helper to mark a Team as Ready
func markTeamReady(team *butlerv1alpha1.Team) {
	team.Status.Phase = butlerv1alpha1.TeamPhaseReady
	team.Status.Namespace = team.Name
}

// Helper to create a TenantCluster
func newTenantCluster(name, namespace string, teamRef *string) *butlerv1alpha1.TenantCluster {
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: butlerv1alpha1.TenantClusterSpec{
			KubernetesVersion: "v1.30.0",
			Workers: butlerv1alpha1.WorkersSpec{
				Replicas: 1,
			},
		},
	}
	if teamRef != nil {
		tc.Spec.TeamRef = &butlerv1alpha1.LocalObjectReference{Name: *teamRef}
	}
	return tc
}

// Helper to create a namespace
func newNamespace(name string) *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
}

// Helper to create a test ProviderConfig
func newProviderConfig(name, namespace string) *butlerv1alpha1.ProviderConfig {
	return &butlerv1alpha1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: butlerv1alpha1.ProviderConfigSpec{
			Provider: butlerv1alpha1.ProviderTypeHarvester,
			CredentialsRef: butlerv1alpha1.SecretReference{
				Name:      name + "-credentials",
				Namespace: namespace,
			},
			Harvester: &butlerv1alpha1.HarvesterProviderConfig{
				NetworkName: "default/vm-network",
				Namespace:   "default",
			},
		},
	}
}

// Helper to create a credentials Secret for ProviderConfig
func newProviderCredentialsSecret(name, namespace string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: map[string][]byte{
			"kubeconfig": []byte("apiVersion: v1\nkind: Config\nclusters: []\ncontexts: []\nusers: []"),
		},
	}
}
