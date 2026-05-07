// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
	"github.com/butlerdotdev/butler-controller/internal/addons"
)

// mockInstaller implements AddonInstaller for testing.
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

// allHealthyAddons returns an observedState with all expected non-CNI addons as
// Healthy, matching what reconcileAddons would produce on a successful install.
func allHealthyAddons() *butlerv1alpha1.ObservedClusterState {
	return &butlerv1alpha1.ObservedClusterState{
		Addons: []butlerv1alpha1.AddonStatus{
			{Name: "cilium", Version: "1.17.0", Status: "Healthy", ManagedBy: "butler"},
			{Name: "cert-manager", Version: "v1.16.2", Status: "Healthy", ManagedBy: "butler"},
			{Name: "longhorn", Version: "1.7.2", Status: "Healthy", ManagedBy: "butler"},
			{Name: "metallb", Version: "0.14.9", Status: "Healthy", ManagedBy: "butler"},
			{Name: "traefik", Version: "34.3.0", Status: "Healthy", ManagedBy: "butler"},
		},
	}
}

func kubeconfigSecret(tcName, tenantNS string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tcName + "-admin-kubeconfig",
			Namespace: tenantNS,
		},
		Data: map[string][]byte{
			"admin.conf": []byte("fake-kubeconfig"),
		},
	}
}

func TestReconcileAddonHealth_NoFailedAddons(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			ObservedState: allHealthyAddons(),
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
		t.Error("expected retryNeeded=false when all addons are healthy")
	}
	if installer.certManagerCalls != 0 {
		t.Errorf("expected 0 cert-manager calls, got %d", installer.certManagerCalls)
	}
}

func TestReconcileAddonHealth_NilObservedState(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Nil observedState with a Ready cluster means all expected addons are
	// absent. Retry should be attempted for each one.
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			TenantNamespace: "test-abc123",
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeconfigSecret("test", "test-abc123")).
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
		t.Error("expected retryNeeded=false when all retries succeed")
	}
	// All 4 expected addons should be retried (ingress defaults to enabled).
	if installer.certManagerCalls != 1 {
		t.Errorf("expected 1 cert-manager call, got %d", installer.certManagerCalls)
	}
	if installer.longhornCalls != 1 {
		t.Errorf("expected 1 longhorn call, got %d", installer.longhornCalls)
	}
	if installer.metalLBCalls != 1 {
		t.Errorf("expected 1 metallb call, got %d", installer.metalLBCalls)
	}
	if installer.traefikCalls != 1 {
		t.Errorf("expected 1 traefik call, got %d", installer.traefikCalls)
	}
	// ObservedState should now contain all 4 addons.
	if tc.Status.ObservedState == nil {
		t.Fatal("observedState should not be nil after retry")
	}
	if len(tc.Status.ObservedState.Addons) != 4 {
		t.Errorf("expected 4 addons in observedState, got %d", len(tc.Status.ObservedState.Addons))
	}
}

func TestReconcileAddonHealth_RetrySucceeds(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

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
					{Name: "traefik", Version: "34.3.0", Status: "Healthy", ManagedBy: "butler"},
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeconfigSecret("test", "test-abc123")).
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
					{Name: "traefik", Version: "34.3.0", Status: "Healthy", ManagedBy: "butler"},
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeconfigSecret("test", "test-abc123")).
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
					{Name: "cilium", Version: "1.17.0", Status: "Healthy", ManagedBy: "butler"},
					{Name: "cert-manager", Version: "v1.16.2", Status: "Healthy", ManagedBy: "butler"},
					{Name: "longhorn", Version: "1.7.2", Status: "Healthy", ManagedBy: "butler"},
					{Name: "metallb", Version: "0.14.9", Status: "Failed", ManagedBy: "butler"},
					{Name: "traefik", Version: "34.3.0", Status: "Healthy", ManagedBy: "butler"},
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeconfigSecret("test", "test-abc123"), lbAlloc).
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

	// An unexpected addon is present but Failed. It should not be retried
	// (not in the expected list) and should not be removed from observedState.
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			TenantNamespace: "test-abc123",
			ObservedState: &butlerv1alpha1.ObservedClusterState{
				Addons: []butlerv1alpha1.AddonStatus{
					{Name: "cilium", Version: "1.17.0", Status: "Healthy", ManagedBy: "butler"},
					{Name: "cert-manager", Version: "v1.16.2", Status: "Healthy", ManagedBy: "butler"},
					{Name: "longhorn", Version: "1.7.2", Status: "Healthy", ManagedBy: "butler"},
					{Name: "metallb", Version: "0.14.9", Status: "Healthy", ManagedBy: "butler"},
					{Name: "traefik", Version: "34.3.0", Status: "Healthy", ManagedBy: "butler"},
					{Name: "unknown-addon", Version: "1.0.0", Status: "Failed", ManagedBy: "butler"},
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
		t.Error("expected retryNeeded=false — unknown addons are not retried")
	}

	// unknown-addon should still be in observedState, untouched
	found := false
	for _, a := range tc.Status.ObservedState.Addons {
		if a.Name == "unknown-addon" {
			found = true
			if a.Status != "Failed" {
				t.Errorf("unknown-addon status = %s, want Failed (untouched)", a.Status)
			}
		}
	}
	if !found {
		t.Error("unknown-addon was removed from observedState — it should be left in place")
	}
}

func TestReconcileAddonHealth_AbsentAddonRetrySucceeds(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Traefik is expected (ingress nil -> enabled) but absent from observedState.
	// This is the tempo-prd scenario: initial install failed silently before
	// failure tracking existed.
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			TenantNamespace: "test-abc123",
			ObservedState: &butlerv1alpha1.ObservedClusterState{
				Addons: []butlerv1alpha1.AddonStatus{
					{Name: "cilium", Version: "1.17.0", Status: "Healthy", ManagedBy: "butler"},
					{Name: "cert-manager", Version: "v1.16.2", Status: "Healthy", ManagedBy: "butler"},
					{Name: "longhorn", Version: "1.7.2", Status: "Healthy", ManagedBy: "butler"},
					{Name: "metallb", Version: "0.14.9", Status: "Healthy", ManagedBy: "butler"},
					// traefik absent
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeconfigSecret("test", "test-abc123")).
		Build()

	installer := &mockInstaller{} // traefik install succeeds
	recorder := record.NewFakeRecorder(10)
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
		t.Error("expected retryNeeded=false after successful retry of absent addon")
	}
	if installer.traefikCalls != 1 {
		t.Errorf("expected 1 traefik call for absent addon, got %d", installer.traefikCalls)
	}
	// Only traefik should be retried, not the healthy addons.
	if installer.certManagerCalls != 0 {
		t.Errorf("expected 0 cert-manager calls, got %d", installer.certManagerCalls)
	}

	// Traefik should now appear in observedState as Healthy.
	var traefikStatus string
	for _, a := range tc.Status.ObservedState.Addons {
		if a.Name == "traefik" {
			traefikStatus = a.Status
		}
	}
	if traefikStatus != "Healthy" {
		t.Errorf("traefik status = %q, want Healthy (should be appended to observedState)", traefikStatus)
	}

	// AddonsReady should be True
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

func TestReconcileAddonHealth_AbsentAddonRetryFails(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Traefik absent and retry will fail.
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			TenantNamespace: "test-abc123",
			ObservedState: &butlerv1alpha1.ObservedClusterState{
				Addons: []butlerv1alpha1.AddonStatus{
					{Name: "cilium", Version: "1.17.0", Status: "Healthy", ManagedBy: "butler"},
					{Name: "cert-manager", Version: "v1.16.2", Status: "Healthy", ManagedBy: "butler"},
					{Name: "longhorn", Version: "1.7.2", Status: "Healthy", ManagedBy: "butler"},
					{Name: "metallb", Version: "0.14.9", Status: "Healthy", ManagedBy: "butler"},
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeconfigSecret("test", "test-abc123")).
		Build()

	installer := &mockInstaller{
		installTraefikErr: fmt.Errorf("chart repo unreachable"),
	}
	recorder := record.NewFakeRecorder(10)
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
		t.Error("expected retryNeeded=true when absent addon retry fails")
	}

	// Traefik should now appear in observedState as Failed (appended).
	var traefikStatus string
	for _, a := range tc.Status.ObservedState.Addons {
		if a.Name == "traefik" {
			traefikStatus = a.Status
		}
	}
	if traefikStatus != "Failed" {
		t.Errorf("traefik status = %q, want Failed (should be appended with failed status)", traefikStatus)
	}

	// AddonsReady should be False with AddonInstallFailed reason
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

func TestReconcileAddonHealth_IngressDisabledSkipsTraefik(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// Ingress explicitly disabled. Traefik absent from observedState should
	// NOT trigger a retry.
	enabled := false
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Spec: butlerv1alpha1.TenantClusterSpec{
			Addons: butlerv1alpha1.AddonsSpec{
				Ingress: &butlerv1alpha1.IngressSpec{
					Enabled: &enabled,
				},
			},
		},
		Status: butlerv1alpha1.TenantClusterStatus{
			ObservedState: &butlerv1alpha1.ObservedClusterState{
				Addons: []butlerv1alpha1.AddonStatus{
					{Name: "cilium", Version: "1.17.0", Status: "Healthy", ManagedBy: "butler"},
					{Name: "cert-manager", Version: "v1.16.2", Status: "Healthy", ManagedBy: "butler"},
					{Name: "longhorn", Version: "1.7.2", Status: "Healthy", ManagedBy: "butler"},
					{Name: "metallb", Version: "0.14.9", Status: "Healthy", ManagedBy: "butler"},
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
		t.Error("expected retryNeeded=false — traefik not expected when ingress disabled")
	}
	if installer.traefikCalls != 0 {
		t.Errorf("expected 0 traefik calls, got %d", installer.traefikCalls)
	}
}

func TestReconcileAddonHealth_MixedFailedAndAbsent(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	// cert-manager is Failed, traefik is absent. Both should be retried.
	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "team-a"},
		Status: butlerv1alpha1.TenantClusterStatus{
			TenantNamespace: "test-abc123",
			ObservedState: &butlerv1alpha1.ObservedClusterState{
				Addons: []butlerv1alpha1.AddonStatus{
					{Name: "cilium", Version: "1.17.0", Status: "Healthy", ManagedBy: "butler"},
					{Name: "cert-manager", Version: "v1.16.2", Status: "Failed", ManagedBy: "butler"},
					{Name: "longhorn", Version: "1.7.2", Status: "Healthy", ManagedBy: "butler"},
					{Name: "metallb", Version: "0.14.9", Status: "Healthy", ManagedBy: "butler"},
					// traefik absent
				},
			},
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(kubeconfigSecret("test", "test-abc123")).
		Build()

	installer := &mockInstaller{} // all succeed
	recorder := record.NewFakeRecorder(10)
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
		t.Errorf("expected 1 cert-manager call (Failed), got %d", installer.certManagerCalls)
	}
	if installer.traefikCalls != 1 {
		t.Errorf("expected 1 traefik call (absent), got %d", installer.traefikCalls)
	}
	if installer.longhornCalls != 0 {
		t.Errorf("expected 0 longhorn calls (Healthy), got %d", installer.longhornCalls)
	}

	// Both should now be Healthy in observedState
	for _, a := range tc.Status.ObservedState.Addons {
		if a.Name == "cert-manager" && a.Status != "Healthy" {
			t.Errorf("cert-manager status = %s, want Healthy", a.Status)
		}
		if a.Name == "traefik" && a.Status != "Healthy" {
			t.Errorf("traefik status = %s, want Healthy", a.Status)
		}
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

func makeExtensionValues(t *testing.T, v map[string]interface{}) *butlerv1alpha1.ExtensionValues {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal values: %v", err)
	}
	return &butlerv1alpha1.ExtensionValues{Raw: raw}
}

func TestEnsureAutoEnrolledAddon_CreatesNew(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "team-a",
			UID:       "abc-123",
			Labels:    map[string]string{butlerv1alpha1.LabelTeam: "team-a"},
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc).
		Build()

	r := &Reconciler{Client: cl, Scheme: scheme}

	values := makeExtensionValues(t, map[string]interface{}{
		"customConfig": map[string]interface{}{
			"sinks": map[string]interface{}{
				"aggregator": map[string]interface{}{"uri": "http://10.0.0.1:8080"},
			},
		},
	})

	err := r.ensureAutoEnrolledAddon(context.Background(), tc, "vector-agent", values)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	created := &butlerv1alpha1.TenantAddon{}
	err = cl.Get(context.Background(), client.ObjectKey{Name: "test-cluster-vector-agent", Namespace: "team-a"}, created)
	if err != nil {
		t.Fatalf("TenantAddon not created: %v", err)
	}
	if created.Spec.Addon != "vector-agent" {
		t.Errorf("addon = %q, want vector-agent", created.Spec.Addon)
	}
	if created.Labels["butler.butlerlabs.dev/auto-enrolled"] != "true" {
		t.Error("missing auto-enrolled label")
	}
}

func TestEnsureAutoEnrolledAddon_UpdatesChangedValues(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "team-a",
			UID:       "abc-123",
			Labels:    map[string]string{butlerv1alpha1.LabelTeam: "team-a"},
		},
	}

	oldValues := makeExtensionValues(t, map[string]interface{}{
		"customConfig": map[string]interface{}{
			"sinks": map[string]interface{}{
				"aggregator": map[string]interface{}{"uri": "http://10.0.0.1:8080"},
			},
		},
	})

	existing := &butlerv1alpha1.TenantAddon{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-vector-agent",
			Namespace: "team-a",
			Labels: map[string]string{
				"butler.butlerlabs.dev/auto-enrolled": "true",
			},
		},
		Spec: butlerv1alpha1.TenantAddonSpec{
			ClusterRef: butlerv1alpha1.LocalObjectReference{Name: "test-cluster"},
			Addon:      "vector-agent",
			Values:     oldValues,
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, existing).
		Build()

	r := &Reconciler{Client: cl, Scheme: scheme}

	newValues := makeExtensionValues(t, map[string]interface{}{
		"customConfig": map[string]interface{}{
			"sinks": map[string]interface{}{
				"aggregator": map[string]interface{}{"uri": "http://10.0.0.2:8080"},
			},
		},
	})

	err := r.ensureAutoEnrolledAddon(context.Background(), tc, "vector-agent", newValues)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updated := &butlerv1alpha1.TenantAddon{}
	err = cl.Get(context.Background(), client.ObjectKey{Name: "test-cluster-vector-agent", Namespace: "team-a"}, updated)
	if err != nil {
		t.Fatalf("failed to get TenantAddon: %v", err)
	}

	var vals map[string]interface{}
	if err := json.Unmarshal(updated.Spec.Values.Raw, &vals); err != nil {
		t.Fatalf("failed to unmarshal updated values: %v", err)
	}

	uri := vals["customConfig"].(map[string]interface{})["sinks"].(map[string]interface{})["aggregator"].(map[string]interface{})["uri"]
	if uri != "http://10.0.0.2:8080" {
		t.Errorf("uri = %v, want http://10.0.0.2:8080", uri)
	}
}

func TestEnsureAutoEnrolledAddon_SkipsIdenticalValues(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "team-a",
			UID:       "abc-123",
			Labels:    map[string]string{butlerv1alpha1.LabelTeam: "team-a"},
		},
	}

	values := makeExtensionValues(t, map[string]interface{}{
		"customConfig": map[string]interface{}{
			"sinks": map[string]interface{}{
				"aggregator": map[string]interface{}{"uri": "http://10.0.0.1:8080"},
			},
		},
	})

	existing := &butlerv1alpha1.TenantAddon{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-vector-agent",
			Namespace: "team-a",
			Labels: map[string]string{
				"butler.butlerlabs.dev/auto-enrolled": "true",
			},
		},
		Spec: butlerv1alpha1.TenantAddonSpec{
			ClusterRef: butlerv1alpha1.LocalObjectReference{Name: "test-cluster"},
			Addon:      "vector-agent",
			Values:     values,
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, existing).
		Build()

	r := &Reconciler{Client: cl, Scheme: scheme}

	// Pass identical values — should not trigger an update.
	sameValues := makeExtensionValues(t, map[string]interface{}{
		"customConfig": map[string]interface{}{
			"sinks": map[string]interface{}{
				"aggregator": map[string]interface{}{"uri": "http://10.0.0.1:8080"},
			},
		},
	})

	err := r.ensureAutoEnrolledAddon(context.Background(), tc, "vector-agent", sameValues)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the resource version hasn't changed (no update was made).
	after := &butlerv1alpha1.TenantAddon{}
	_ = cl.Get(context.Background(), client.ObjectKey{Name: "test-cluster-vector-agent", Namespace: "team-a"}, after)
	if after.ResourceVersion != existing.ResourceVersion {
		t.Error("resource version changed — update was triggered for identical values")
	}
}

func TestEnsureAutoEnrolledAddon_SkipsManuallyCreated(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = butlerv1alpha1.AddToScheme(scheme)

	tc := &butlerv1alpha1.TenantCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "team-a",
			UID:       "abc-123",
			Labels:    map[string]string{butlerv1alpha1.LabelTeam: "team-a"},
		},
	}

	oldValues := makeExtensionValues(t, map[string]interface{}{
		"customConfig": map[string]interface{}{
			"sinks": map[string]interface{}{
				"aggregator": map[string]interface{}{"uri": "http://10.0.0.1:8080"},
			},
		},
	})

	// Manually created TenantAddon (no auto-enrolled label).
	existing := &butlerv1alpha1.TenantAddon{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-vector-agent",
			Namespace: "team-a",
			Labels:    map[string]string{},
		},
		Spec: butlerv1alpha1.TenantAddonSpec{
			ClusterRef: butlerv1alpha1.LocalObjectReference{Name: "test-cluster"},
			Addon:      "vector-agent",
			Values:     oldValues,
		},
	}

	cl := fakeclient.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(tc, existing).
		Build()

	r := &Reconciler{Client: cl, Scheme: scheme}

	newValues := makeExtensionValues(t, map[string]interface{}{
		"customConfig": map[string]interface{}{
			"sinks": map[string]interface{}{
				"aggregator": map[string]interface{}{"uri": "http://10.0.0.2:8080"},
			},
		},
	})

	err := r.ensureAutoEnrolledAddon(context.Background(), tc, "vector-agent", newValues)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Values should NOT be updated on a manually created addon.
	after := &butlerv1alpha1.TenantAddon{}
	_ = cl.Get(context.Background(), client.ObjectKey{Name: "test-cluster-vector-agent", Namespace: "team-a"}, after)

	var vals map[string]interface{}
	_ = json.Unmarshal(after.Spec.Values.Raw, &vals)
	uri := vals["customConfig"].(map[string]interface{})["sinks"].(map[string]interface{})["aggregator"].(map[string]interface{})["uri"]
	if uri != "http://10.0.0.1:8080" {
		t.Errorf("uri was updated to %v — should not update manually created addons", uri)
	}
}

func TestBuildPrometheusValues_IncludesExternalLabels(t *testing.T) {
	pipeline := &butlerv1alpha1.ObservabilityPipelineConfig{
		MetricEndpoint: "http://10.0.0.1:9000/insert/0/prometheus/api/v1/write",
	}

	ev := buildPrometheusValues(pipeline, "my-tenant-cluster")
	if ev == nil {
		t.Fatal("expected non-nil ExtensionValues")
	}

	var vals map[string]interface{}
	if err := json.Unmarshal(ev.Raw, &vals); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	promSpec := vals["prometheus"].(map[string]interface{})["prometheusSpec"].(map[string]interface{})

	// Check externalLabels.cluster
	extLabels, ok := promSpec["externalLabels"].(map[string]interface{})
	if !ok {
		t.Fatal("externalLabels missing from prometheusSpec")
	}
	if extLabels["cluster"] != "my-tenant-cluster" {
		t.Errorf("cluster label = %v, want my-tenant-cluster", extLabels["cluster"])
	}

	// Check remoteWrite URL is still present
	rw, ok := promSpec["remoteWrite"].([]interface{})
	if !ok || len(rw) == 0 {
		t.Fatal("remoteWrite missing from prometheusSpec")
	}
	rwEntry := rw[0].(map[string]interface{})
	if rwEntry["url"] != "http://10.0.0.1:9000/insert/0/prometheus/api/v1/write" {
		t.Errorf("remoteWrite url = %v, want the pipeline metric endpoint", rwEntry["url"])
	}
}

func TestBuildPrometheusValues_NilPipeline(t *testing.T) {
	ev := buildPrometheusValues(nil, "cluster-1")
	if ev != nil {
		t.Error("expected nil ExtensionValues for nil pipeline")
	}
}

func TestBuildPrometheusValues_EmptyEndpoint(t *testing.T) {
	pipeline := &butlerv1alpha1.ObservabilityPipelineConfig{
		MetricEndpoint: "",
	}
	ev := buildPrometheusValues(pipeline, "cluster-1")
	if ev != nil {
		t.Error("expected nil ExtensionValues for empty endpoint")
	}
}

func TestBuildVectorAgentValues_HTTPSinkURI(t *testing.T) {
	pipeline := &butlerv1alpha1.ObservabilityPipelineConfig{
		LogEndpoint: "http://10.0.0.1:8080",
	}

	ev := buildVectorAgentValues(pipeline)
	if ev == nil {
		t.Fatal("expected non-nil ExtensionValues")
	}

	var vals map[string]interface{}
	if err := json.Unmarshal(ev.Raw, &vals); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	uri := vals["customConfig"].(map[string]interface{})["sinks"].(map[string]interface{})["aggregator"].(map[string]interface{})["uri"]
	if uri != "http://10.0.0.1:8080" {
		t.Errorf("uri = %v, want http://10.0.0.1:8080", uri)
	}
}
