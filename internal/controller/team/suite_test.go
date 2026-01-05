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

package team

import (
	"context"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

// Package-level variables accessible to all test files in this package.
// These are initialized in BeforeSuite and cleaned up in AfterSuite.
var (
	cfg            *rest.Config
	k8sClient      client.Client
	testEnv        *envtest.Environment
	ctx            context.Context
	cancel         context.CancelFunc
	testReconciler *Reconciler
)

// TestTeamController is the entry point for running the Ginkgo test suite.
// Run with: go test ./internal/controller/team/... -v
func TestTeamController(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Team Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths: []string{
			// Path to butler-api CRDs
			// Option 1: If butler-api is a sibling directory
			filepath.Join("..", "..", "..", "..", "butler-api", "config", "crd", "bases"),
			// Option 2: If using go mod vendor or module cache
			// You may need to adjust this path based on your setup
		},
		ErrorIfCRDPathMissing: false, // Set to true once CRDs are available
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	// Register Butler API types with the scheme
	err = butlerv1alpha1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	// Create the Kubernetes client for tests
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// Create the reconciler instance for tests
	testReconciler = &Reconciler{
		Client: k8sClient,
		Scheme: scheme.Scheme,
	}
})

var _ = AfterSuite(func() {
	cancel()
	By("tearing down the test environment")
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})
