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
	"context"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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
	ipallocationctrl "github.com/butlerdotdev/butler-controller/internal/controller/ipallocation"
)

var (
	cfg       *rest.Config
	k8sClient client.Client
	testEnv   *envtest.Environment
	ctx       context.Context
	cancel    context.CancelFunc
)

func TestNetworkPool(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "NetworkPool Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "..", "butler-api", "config", "crd", "bases")},
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

	// Register the NetworkPool controller (sole allocator for IPAllocations)
	err = (&Reconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr)
	Expect(err).NotTo(HaveOccurred())

	// Register the IPAllocation controller (finalizers, initial phase, deletion cleanup)
	err = (&ipallocationctrl.Reconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
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

// Helper to create a NetworkPool for testing.
func newNetworkPool(name, namespace, cidr string, tenantAllocation *butlerv1alpha1.TenantAllocationConfig) *butlerv1alpha1.NetworkPool {
	return &butlerv1alpha1.NetworkPool{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: butlerv1alpha1.NetworkPoolSpec{
			CIDR:             cidr,
			TenantAllocation: tenantAllocation,
		},
	}
}

// Helper to create an IPAllocation for testing.
func newIPAllocation(name, namespace, poolName string, allocType butlerv1alpha1.IPAllocationType, count *int32) *butlerv1alpha1.IPAllocation {
	return &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				butlerv1alpha1.LabelNetworkPool: poolName,
			},
		},
		Spec: butlerv1alpha1.IPAllocationSpec{
			PoolRef: butlerv1alpha1.LocalObjectReference{
				Name: poolName,
			},
			TenantClusterRef: butlerv1alpha1.NamespacedObjectReference{
				Name:      "test-cluster",
				Namespace: namespace,
			},
			Type:  allocType,
			Count: count,
		},
	}
}

// int32Ptr returns a pointer to the given int32 value.
func int32Ptr(v int32) *int32 {
	return &v
}
