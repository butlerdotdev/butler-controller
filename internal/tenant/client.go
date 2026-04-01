// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenant

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClientManager manages cached Kubernetes clients for tenant clusters.
// Tenant control planes are hosted in the management cluster via Steward,
// so connections use the internal ClusterIP service endpoint (admin.svc).
// Clients are cached by tenant namespace/name to avoid creating new HTTP/2
// connections on every reconcile cycle.
type ClientManager struct {
	mu      sync.RWMutex
	clients map[string]*TenantClient
	mgr     client.Client
}

// TenantClient wraps a cached connection to a tenant cluster.
type TenantClient struct {
	RESTConfig *rest.Config
	Clientset  kubernetes.Interface
}

// NewClientManager creates a new ClientManager.
func NewClientManager(mgr client.Client) *ClientManager {
	return &ClientManager{
		clients: make(map[string]*TenantClient),
		mgr:     mgr,
	}
}

// GetClient returns a cached client for the specified tenant cluster.
// On cache miss, it fetches the admin kubeconfig Secret from the tenant
// namespace, preferring the internal service endpoint (admin.svc) over
// the external endpoint (admin.conf).
func (m *ClientManager) GetClient(ctx context.Context, namespace, clusterName string) (*TenantClient, error) {
	key := namespace + "/" + clusterName

	m.mu.RLock()
	if c, ok := m.clients[key]; ok {
		m.mu.RUnlock()
		return c, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	if c, ok := m.clients[key]; ok {
		return c, nil
	}

	secretName := clusterName + "-admin-kubeconfig"
	secret := &corev1.Secret{}
	if err := m.mgr.Get(ctx, client.ObjectKey{Name: secretName, Namespace: namespace}, secret); err != nil {
		return nil, fmt.Errorf("fetching kubeconfig secret %s/%s: %w", namespace, secretName, err)
	}

	var kubeconfigData []byte
	for _, key := range []string{"admin.svc", "admin.conf"} {
		if data, ok := secret.Data[key]; ok && len(data) > 0 {
			kubeconfigData = data
			break
		}
	}
	if len(kubeconfigData) == 0 {
		return nil, fmt.Errorf("kubeconfig secret %s/%s has no admin.svc or admin.conf key", namespace, secretName)
	}

	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfigData)
	if err != nil {
		return nil, fmt.Errorf("parsing kubeconfig for %s/%s: %w", namespace, clusterName, err)
	}
	restConfig.Timeout = 10 * time.Second

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("creating clientset for %s/%s: %w", namespace, clusterName, err)
	}

	tc := &TenantClient{
		RESTConfig: restConfig,
		Clientset:  clientset,
	}
	m.clients[namespace+"/"+clusterName] = tc
	return tc, nil
}

// RemoveClient removes a cached client. Called when a tenant cluster is deleted.
func (m *ClientManager) RemoveClient(namespace, clusterName string) {
	m.mu.Lock()
	delete(m.clients, namespace+"/"+clusterName)
	m.mu.Unlock()
}

// RefreshClient forces a client refresh. Called when a kubeconfig Secret changes.
func (m *ClientManager) RefreshClient(ctx context.Context, namespace, clusterName string) (*TenantClient, error) {
	m.RemoveClient(namespace, clusterName)
	return m.GetClient(ctx, namespace, clusterName)
}
