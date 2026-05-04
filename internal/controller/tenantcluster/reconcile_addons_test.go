// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/addons"
)

// mockInstaller implements PlatformAddonInstaller for testing.
type mockInstaller struct {
	installCertManagerErr error
	installLonghornErr    error
	installMetalLBErr     error
	installTraefikErr     error
	installCiliumErr      error
	updateMetalLBPoolErr  error

	certManagerCalls int
	longhornCalls    int
	metalLBCalls     int
	traefikCalls     int
}

func (m *mockInstaller) InstallCilium(_ context.Context, _ []byte, _ addons.CiliumConfig) error {
	return m.installCiliumErr
}

func (m *mockInstaller) InstallCertManager(_ context.Context, _ []byte, _ string) error {
	m.certManagerCalls++
	return m.installCertManagerErr
}

func (m *mockInstaller) InstallLonghorn(_ context.Context, _ []byte, _ string) error {
	m.longhornCalls++
	return m.installLonghornErr
}

func (m *mockInstaller) InstallMetalLB(_ context.Context, _ []byte, _, _, _ string) error {
	m.metalLBCalls++
	return m.installMetalLBErr
}

func (m *mockInstaller) InstallTraefik(_ context.Context, _ []byte, _ string) error {
	m.traefikCalls++
	return m.installTraefikErr
}

func (m *mockInstaller) UpdateMetalLBPool(_ context.Context, _ []byte, _ []string) error {
	return m.updateMetalLBPoolErr
}

func TestReconcileAddonHealth_NoFailedAddons(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			ObservedState: &butlerv1alpha1.ObservedClusterState{
				Addons: []butlerv1alpha1.AddonStatus{
					{Name: "cilium", Version: "1.17.0", Status: "Healthy", ManagedBy: "butler"},
					{Name: "cert-manager", Version: "v1.16.2", Status: "Healthy", ManagedBy: "butler"},
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	installer := &mockInstaller{}
	r := &Reconciler{
		Client:    cl,
		Scheme:    scheme,
		Installer: installer,
		Recorder:  record.NewFakeRecorder(10),
	}

	retryNeeded, err := r.reconcileAddonHealth(context.Background(), tc, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if retryNeeded {
		t.Error("expected retryNeeded=false when no addons are failed")
	}
	if installer.certManagerCalls != 0 {
		t.Errorf("expected 0 cert-manager calls, got %d", installer.certManagerCalls)
	}
}

func TestReconcileAddonHealth_NilObservedState(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status:     butlerv1alpha1.TenantClusterStatus{},
	}

	cl := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{
		Client:    cl,
		Scheme:    scheme,
		Installer: &mockInstaller{},
		Recorder:  record.NewFakeRecorder(10),
	}

	retryNeeded, err := r.reconcileAddonHealth(context.Background(), tc, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if retryNeeded {
		t.Error("expected retryNeeded=false with nil ObservedState")
	}
}

func TestReconcileAddonHealth_RetrySucceeds(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Simulate a cluster where cert-manager and longhorn initially failed.
	// The kubeconfig secret must exist for getTenantKubeconfig to succeed.
	kubeconfigSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-admin-kubeconfig",
			Namespace: "test-abc123",
		},
		Data: map[string][]byte{
			"admin.conf": []byte("fake-kubeconfig"),
		},
	}

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			TenantNamespace: "test-abc123",
			ObservedState: &butlerv1alpha1.ObservedClusterState{
				Addons: []butlerv1alpha1.AddonStatus{
					{Name: "cilium", Version: "1.17.0", Status: "Healthy", ManagedBy: "butler"},
					{Name: "cert-manager", Version: "v1.16.2", Status: "Failed", ManagedBy: "butler"},
					{Name: "longhorn", Version: "1.7.2", Status: "Failed", ManagedBy: "butler"},
					{Name: "metallb", Version: "0.14.9", Status: "Healthy", ManagedBy: "butler"},
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeconfigSecret).
		Build()

	recorder := record.NewFakeRecorder(10)
	installer := &mockInstaller{} // all installs succeed
	r := &Reconciler{
		Client:    cl,
		Scheme:    scheme,
		Installer: installer,
		Recorder:  recorder,
	}

	retryNeeded, err := r.reconcileAddonHealth(context.Background(), tc, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if retryNeeded {
		t.Error("expected retryNeeded=false when all retries succeed")
	}
	if installer.certManagerCalls != 1 {
		t.Errorf("expected 1 cert-manager call, got %d", installer.certManagerCalls)
	}
	if installer.longhornCalls != 1 {
		t.Errorf("expected 1 longhorn call, got %d", installer.longhornCalls)
	}
	if installer.metalLBCalls != 0 {
		t.Errorf("expected 0 metallb calls (was healthy), got %d", installer.metalLBCalls)
	}

	// Verify observed state was updated
	for _, a := range tc.Status.ObservedState.Addons {
		if a.Status == "Failed" {
			t.Errorf("addon %s still shows Failed after successful retry", a.Name)
		}
	}

	// Verify AddonsReady condition was set to True
	for _, c := range tc.Status.Conditions {
		if c.Type == butlerv1alpha1.TenantClusterConditionAddonsReady {
			if c.Status != metav1.ConditionTrue {
				t.Errorf("AddonsReady condition = %s, want True", c.Status)
			}
			return
		}
	}
	t.Error("AddonsReady condition not found")
}

func TestReconcileAddonHealth_RetryPartialFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	kubeconfigSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-admin-kubeconfig",
			Namespace: "test-abc123",
		},
		Data: map[string][]byte{
			"admin.conf": []byte("fake-kubeconfig"),
		},
	}

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			TenantNamespace: "test-abc123",
			ObservedState: &butlerv1alpha1.ObservedClusterState{
				Addons: []butlerv1alpha1.AddonStatus{
					{Name: "cert-manager", Version: "v1.16.2", Status: "Failed", ManagedBy: "butler"},
					{Name: "longhorn", Version: "1.7.2", Status: "Failed", ManagedBy: "butler"},
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeconfigSecret).
		Build()

	recorder := record.NewFakeRecorder(10)
	// cert-manager succeeds, longhorn still fails
	installer := &mockInstaller{
		installLonghornErr: fmt.Errorf("helm timeout"),
	}
	r := &Reconciler{
		Client:    cl,
		Scheme:    scheme,
		Installer: installer,
		Recorder:  recorder,
	}

	retryNeeded, err := r.reconcileAddonHealth(context.Background(), tc, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !retryNeeded {
		t.Error("expected retryNeeded=true when longhorn still fails")
	}

	// cert-manager should be Healthy, longhorn should still be Failed
	for _, a := range tc.Status.ObservedState.Addons {
		switch a.Name {
		case "cert-manager":
			if a.Status != "Healthy" {
				t.Errorf("cert-manager status = %s, want Healthy", a.Status)
			}
		case "longhorn":
			if a.Status != "Failed" {
				t.Errorf("longhorn status = %s, want Failed", a.Status)
			}
		}
	}

	// AddonsReady should be False
	for _, c := range tc.Status.Conditions {
		if c.Type == butlerv1alpha1.TenantClusterConditionAddonsReady {
			if c.Status != metav1.ConditionFalse {
				t.Errorf("AddonsReady condition = %s, want False", c.Status)
			}
			if c.Reason != ReasonAddonInstallFailed {
				t.Errorf("AddonsReady reason = %s, want %s", c.Reason, ReasonAddonInstallFailed)
			}
			return
		}
	}
	t.Error("AddonsReady condition not found")
}

func TestReconcileAddonHealth_MetalLBRetryUsesPoolResolution(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	kubeconfigSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-admin-kubeconfig",
			Namespace: "test-abc123",
		},
		Data: map[string][]byte{
			"admin.conf": []byte("fake-kubeconfig"),
		},
	}

	// MetalLB failed on initial install. The pool allocation exists.
	lbAlloc := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-a-test-lb",
			Namespace: "butler-system",
		},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.1",
			EndAddress:   "10.0.0.4",
		},
	}

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			TenantNamespace: "test-abc123",
			LBAllocationRef: &butlerv1alpha1.LocalObjectReference{
				Name: "team-a-test-lb",
			},
			ObservedState: &butlerv1alpha1.ObservedClusterState{
				Addons: []butlerv1alpha1.AddonStatus{
					{Name: "metallb", Version: "0.14.9", Status: "Failed", ManagedBy: "butler"},
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeconfigSecret, lbAlloc).
		Build()

	installer := &mockInstaller{}
	r := &Reconciler{
		Client:    cl,
		Scheme:    scheme,
		Installer: installer,
		Recorder:  record.NewFakeRecorder(10),
	}

	retryNeeded, err := r.reconcileAddonHealth(context.Background(), tc, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if retryNeeded {
		t.Error("expected retryNeeded=false after successful MetalLB retry")
	}
	if installer.metalLBCalls != 1 {
		t.Errorf("expected 1 metallb call, got %d", installer.metalLBCalls)
	}
}

func TestReconcileAddonHealth_SkipsUnknownAddons(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	kubeconfigSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-admin-kubeconfig",
			Namespace: "test-abc123",
		},
		Data: map[string][]byte{
			"admin.conf": []byte("fake-kubeconfig"),
		},
	}

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			TenantNamespace: "test-abc123",
			ObservedState: &butlerv1alpha1.ObservedClusterState{
				Addons: []butlerv1alpha1.AddonStatus{
					{Name: "unknown-addon", Version: "1.0.0", Status: "Failed", ManagedBy: "butler"},
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeconfigSecret).
		Build()

	installer := &mockInstaller{}
	r := &Reconciler{
		Client:    cl,
		Scheme:    scheme,
		Installer: installer,
		Recorder:  record.NewFakeRecorder(10),
	}

	retryNeeded, err := r.reconcileAddonHealth(context.Background(), tc, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Unknown addons are skipped, not counted as still-failed
	if retryNeeded {
		t.Error("expected retryNeeded=false for unknown addon")
	}
}

func TestResolveMetalLBPool_FromAllocation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	lbAlloc := &butlerv1alpha1.IPAllocation{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "team-a-test-lb",
			Namespace: "butler-system",
		},
		Status: butlerv1alpha1.IPAllocationStatus{
			Phase:        butlerv1alpha1.IPAllocationPhaseAllocated,
			StartAddress: "10.0.0.1",
			EndAddress:   "10.0.0.8",
		},
	}

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			LBAllocationRef: &butlerv1alpha1.LocalObjectReference{
				Name: "team-a-test-lb",
			},
		},
	}

	cl := fakeclient.NewClientBuilder().WithScheme(scheme).WithObjects(lbAlloc).Build()
	r := &Reconciler{Client: cl}

	start, end := r.resolveMetalLBPool(context.Background(), tc)
	if start != "10.0.0.1" {
		t.Errorf("poolStart = %q, want 10.0.0.1", start)
	}
	if end != "10.0.0.8" {
		t.Errorf("poolEnd = %q, want 10.0.0.8", end)
	}
}

func TestResolveMetalLBPool_FallbackToSpec(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Spec: butlerv1alpha1.TenantClusterSpec{
			Networking: butlerv1alpha1.NetworkingSpec{
				LoadBalancerPool: &butlerv1alpha1.IPPool{
					Start: "192.168.1.100",
					End:   "192.168.1.110",
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: cl}

	start, end := r.resolveMetalLBPool(context.Background(), tc)
	if start != "192.168.1.100" {
		t.Errorf("poolStart = %q, want 192.168.1.100", start)
	}
	if end != "192.168.1.110" {
		t.Errorf("poolEnd = %q, want 192.168.1.110", end)
	}
}

func TestResolveMetalLBPool_NoPool(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
	}

	cl := fakeclient.NewClientBuilder().WithScheme(scheme).Build()
	r := &Reconciler{Client: cl}

	start, end := r.resolveMetalLBPool(context.Background(), tc)
	if start != "" {
		t.Errorf("poolStart = %q, want empty", start)
	}
	if end != "" {
		t.Errorf("poolEnd = %q, want empty", end)
	}
}
