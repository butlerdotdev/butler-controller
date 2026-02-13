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

package talos

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Client wraps talosctl for applying machine configs to nodes in maintenance mode.
type Client struct {
	TalosctlPath string
}

// NewClient creates a new talosctl client.
func NewClient() *Client {
	return &Client{TalosctlPath: "talosctl"}
}

// ApplyConfig applies a Talos machine config to a node using talosctl.
// The --insecure flag is always used because these are fresh VMs in maintenance mode.
func (c *Client) ApplyConfig(ctx context.Context, nodeIP string, configData []byte) error {
	logger := log.FromContext(ctx)

	configFile, err := os.CreateTemp("", "talos-config-*.yaml")
	if err != nil {
		return fmt.Errorf("creating temp config file: %w", err)
	}
	defer os.Remove(configFile.Name())

	if _, err := configFile.Write(configData); err != nil {
		configFile.Close()
		return fmt.Errorf("writing config file: %w", err)
	}
	configFile.Close()

	args := []string{
		"apply-config",
		"--nodes", nodeIP,
		"--file", configFile.Name(),
		"--insecure",
	}

	logger.V(1).Info("applying Talos config", "node", nodeIP)

	cmd := exec.CommandContext(ctx, c.TalosctlPath, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("talosctl apply-config failed for %s: %w, output: %s", nodeIP, err, strings.TrimSpace(string(output)))
	}

	return nil
}
