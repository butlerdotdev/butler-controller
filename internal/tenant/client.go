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

// Package tenant provides utilities for managing connections to tenant clusters.
package tenant

import (
	"context"
	"sync"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClientManager manages Kubernetes clients for tenant clusters.
//
// Since Kamaji hosts tenant control planes in the management cluster,
// we can connect to tenant APIs locally. This manager caches clients
// to avoid recreating them on every reconciliation.
//
// Key features:
// - Caches clients by tenant cluster name
// - Automatically refreshes clients when kubeconfig changes
// - Cleans up clients when tenant clusters are deleted
// - Thread-safe for concurrent reconciliation
type ClientManager struct {
	mu      sync.RWMutex
	clients map[string]*TenantClient
	mgr     client.Client
}

// TenantClient wraps connections to a tenant cluster.
type TenantClient struct {
	// RESTConfig is the REST config for the tenant cluster.
	RESTConfig *rest.Config

	// Clientset is the Kubernetes clientset.
	Clientset kubernetes.Interface

	// Client is the controller-runtime client.
	Client client.Client
}

// NewClientManager creates a new ClientManager.
func NewClientManager(mgr client.Client) *ClientManager {
	return &ClientManager{
		clients: make(map[string]*TenantClient),
		mgr:     mgr,
	}
}

// GetClient returns a client for the specified tenant cluster.
// If a client doesn't exist or the kubeconfig has changed, a new one is created.
func (m *ClientManager) GetClient(ctx context.Context, namespace, name string) (*TenantClient, error) {
	key := namespace + "/" + name

	// Check cache first
	m.mu.RLock()
	if client, ok := m.clients[key]; ok {
		m.mu.RUnlock()
		return client, nil
	}
	m.mu.RUnlock()

	// Create new client
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if client, ok := m.clients[key]; ok {
		return client, nil
	}

	// TODO: Implement client creation
	// 1. Get kubeconfig secret for tenant cluster
	// 2. Parse kubeconfig
	// 3. Create REST config
	// 4. Create clientset and controller-runtime client
	// 5. Cache and return

	return nil, nil
}

// RemoveClient removes a cached client.
// Called when a tenant cluster is deleted.
func (m *ClientManager) RemoveClient(namespace, name string) {
	key := namespace + "/" + name
	m.mu.Lock()
	delete(m.clients, key)
	m.mu.Unlock()
}

// RefreshClient forces a client refresh.
// Called when a kubeconfig secret changes.
func (m *ClientManager) RefreshClient(ctx context.Context, namespace, name string) (*TenantClient, error) {
	m.RemoveClient(namespace, name)
	return m.GetClient(ctx, namespace, name)
}
