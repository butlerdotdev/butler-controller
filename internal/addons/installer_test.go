// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package addons

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallTraefikUsesSetStringForLabels(t *testing.T) {
	// Create a temp dir for the fake helm binary and its output
	tmpDir := t.TempDir()
	argsFile := filepath.Join(tmpDir, "helm-args")
	fakeHelm := filepath.Join(tmpDir, "helm")

	// Write a fake helm script that records all arguments
	script := "#!/bin/sh\necho \"$@\" >> " + argsFile + "\n"
	if err := os.WriteFile(fakeHelm, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}

	installer := &Installer{
		kubectlPath: filepath.Join(tmpDir, "kubectl"),
		helmPath:    fakeHelm,
	}

	// Create a fake kubectl that does nothing (for ensurePrivilegedNamespace)
	fakeKubectl := "#!/bin/sh\nexit 0\n"
	if err := os.WriteFile(installer.kubectlPath, []byte(fakeKubectl), 0755); err != nil {
		t.Fatal(err)
	}

	// Create a dummy kubeconfig
	kubeconfig := []byte("apiVersion: v1\nkind: Config\nclusters: []\ncontexts: []\nusers: []")

	err := installer.InstallTraefik(context.Background(), kubeconfig, "34.3.0")
	if err != nil {
		// The fake helm returns exit 0 so this should succeed
		t.Fatalf("InstallTraefik() error = %v", err)
	}

	recorded, err := os.ReadFile(argsFile)
	if err != nil {
		t.Fatalf("failed to read recorded args: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(recorded)), "\n")

	// Find the helm upgrade --install invocation (last line, after repo add and repo update)
	var installLine string
	for _, line := range lines {
		if strings.Contains(line, "upgrade --install") {
			installLine = line
			break
		}
	}
	if installLine == "" {
		t.Fatalf("no 'upgrade --install' invocation found in recorded args:\n%s", string(recorded))
	}

	// The commonLabels value must use --set-string, not --set
	if !strings.Contains(installLine, "--set-string commonLabels") {
		t.Errorf("expected --set-string for commonLabels, got:\n%s", installLine)
	}
	if strings.Contains(installLine, "--set commonLabels") {
		t.Errorf("found --set (not --set-string) for commonLabels, got:\n%s", installLine)
	}
}

func TestApplyWithRetry_SucceedsFirstAttempt(t *testing.T) {
	calls := 0
	err := applyWithRetry(context.Background(), func(_ context.Context) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestApplyWithRetry_SucceedsAfterRetries(t *testing.T) {
	calls := 0
	err := applyWithRetry(context.Background(), func(_ context.Context) error {
		calls++
		if calls < 3 {
			return fmt.Errorf("transient error")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestApplyWithRetry_ExhaustsRetries(t *testing.T) {
	calls := 0
	err := applyWithRetry(context.Background(), func(_ context.Context) error {
		calls++
		return fmt.Errorf("persistent error")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 4 {
		t.Fatalf("expected 4 calls (initial + 3 retries), got %d", calls)
	}
}

func TestApplyWithRetry_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := applyWithRetry(ctx, func(_ context.Context) error {
		calls++
		cancel()
		return fmt.Errorf("transient error")
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls > 2 {
		t.Fatalf("expected at most 2 calls after cancellation, got %d", calls)
	}
}
