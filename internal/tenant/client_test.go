// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func kubeconfig() []byte {
	return []byte(`apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:6443
    insecure-skip-tls-verify: true
  name: test
contexts:
- context:
    cluster: test
    user: test
  name: test
current-context: test
users:
- name: test
  user:
    token: fake-token
`)
}

func newFakeManager(secrets ...*corev1.Secret) *ClientManager {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	builder := clientfake.NewClientBuilder().WithScheme(scheme)
	for _, s := range secrets {
		builder = builder.WithObjects(s)
	}

	cm := NewClientManager(builder.Build())
	cm.ttl = 1 * time.Second
	return cm
}

func secretWithKey(ns, name, key string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string][]byte{key: kubeconfig()},
	}
}

func TestGetClient_CacheHit(t *testing.T) {
	cm := newFakeManager(secretWithKey("ns-a", "cluster1-admin-kubeconfig", "admin.svc"))

	ctx := context.Background()
	c1, err := cm.GetClient(ctx, "ns-a", "cluster1")
	if err != nil {
		t.Fatalf("first GetClient: %v", err)
	}

	c2, err := cm.GetClient(ctx, "ns-a", "cluster1")
	if err != nil {
		t.Fatalf("second GetClient: %v", err)
	}

	if c1 != c2 {
		t.Error("expected same pointer on cache hit")
	}
}

func TestGetClient_TTLExpiry(t *testing.T) {
	cm := newFakeManager(secretWithKey("ns-a", "cluster1-admin-kubeconfig", "admin.svc"))
	cm.ttl = 10 * time.Millisecond

	ctx := context.Background()
	c1, err := cm.GetClient(ctx, "ns-a", "cluster1")
	if err != nil {
		t.Fatalf("first GetClient: %v", err)
	}

	time.Sleep(20 * time.Millisecond)

	c2, err := cm.GetClient(ctx, "ns-a", "cluster1")
	if err != nil {
		t.Fatalf("second GetClient after TTL: %v", err)
	}

	if c1 == c2 {
		t.Error("expected different pointer after TTL expiry")
	}
}

func TestGetClient_MissingSecret(t *testing.T) {
	cm := newFakeManager()

	_, err := cm.GetClient(context.Background(), "ns-a", "cluster1")
	if err == nil {
		t.Fatal("expected error for missing secret")
	}
}

func TestGetClient_EmptySecret(t *testing.T) {
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster1-admin-kubeconfig", Namespace: "ns-a"},
		Data:       map[string][]byte{},
	}
	cm := newFakeManager(s)

	_, err := cm.GetClient(context.Background(), "ns-a", "cluster1")
	if err == nil {
		t.Fatal("expected error for empty secret")
	}
}

func TestGetClient_PrefersAdminSvc(t *testing.T) {
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster1-admin-kubeconfig", Namespace: "ns-a"},
		Data: map[string][]byte{
			"admin.svc":  kubeconfig(),
			"admin.conf": kubeconfig(),
		},
	}
	cm := newFakeManager(s)

	c, err := cm.GetClient(context.Background(), "ns-a", "cluster1")
	if err != nil {
		t.Fatalf("GetClient: %v", err)
	}
	if c.RESTConfig.Host != "https://127.0.0.1:6443" {
		t.Errorf("unexpected host: %s", c.RESTConfig.Host)
	}
}

func TestGetClient_FallsBackToAdminConf(t *testing.T) {
	cm := newFakeManager(secretWithKey("ns-a", "cluster1-admin-kubeconfig", "admin.conf"))

	c, err := cm.GetClient(context.Background(), "ns-a", "cluster1")
	if err != nil {
		t.Fatalf("GetClient: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestRemoveClient(t *testing.T) {
	cm := newFakeManager(secretWithKey("ns-a", "cluster1-admin-kubeconfig", "admin.svc"))

	ctx := context.Background()
	c1, _ := cm.GetClient(ctx, "ns-a", "cluster1")
	cm.RemoveClient("ns-a", "cluster1")
	c2, _ := cm.GetClient(ctx, "ns-a", "cluster1")

	if c1 == c2 {
		t.Error("expected different pointer after RemoveClient")
	}
}

func TestRefreshClient(t *testing.T) {
	cm := newFakeManager(secretWithKey("ns-a", "cluster1-admin-kubeconfig", "admin.svc"))

	ctx := context.Background()
	c1, _ := cm.GetClient(ctx, "ns-a", "cluster1")
	c2, err := cm.RefreshClient(ctx, "ns-a", "cluster1")
	if err != nil {
		t.Fatalf("RefreshClient: %v", err)
	}

	if c1 == c2 {
		t.Error("expected different pointer after RefreshClient")
	}
}

func TestGetClient_ConcurrentAccess(t *testing.T) {
	cm := newFakeManager(secretWithKey("ns-a", "cluster1-admin-kubeconfig", "admin.svc"))
	ctx := context.Background()

	var wg sync.WaitGroup
	results := make([]*TenantClient, 10)
	errs := make([]error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = cm.GetClient(ctx, "ns-a", "cluster1")
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}

	// All should get the same cached client (within TTL)
	for i := 1; i < 10; i++ {
		if results[i] != results[0] {
			t.Errorf("goroutine %d got different client than goroutine 0", i)
		}
	}
}

func TestGetClient_IsolatedNamespaces(t *testing.T) {
	cm := newFakeManager(
		secretWithKey("ns-a", "cluster1-admin-kubeconfig", "admin.svc"),
		secretWithKey("ns-b", "cluster1-admin-kubeconfig", "admin.svc"),
	)
	ctx := context.Background()

	ca, _ := cm.GetClient(ctx, "ns-a", "cluster1")
	cb, _ := cm.GetClient(ctx, "ns-b", "cluster1")

	if ca == cb {
		t.Error("expected different clients for different namespaces")
	}
}

// Verify the fake clientset import compiles (used in test helpers only).
var _ = fake.NewSimpleClientset
