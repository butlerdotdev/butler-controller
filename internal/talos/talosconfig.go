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
	"encoding/base64"
	"fmt"

	"gopkg.in/yaml.v3"
)

// TalosconfigInput contains the parameters needed to generate a talosconfig.
type TalosconfigInput struct {
	ContextName string
	Endpoints   []string // Control plane endpoint(s)
	CACert      []byte   // PEM-encoded os-ca.crt
	AdminCert   []byte   // PEM-encoded admin.crt
	AdminKey    []byte   // PEM-encoded admin.key
}

// talosconfigContext represents a single context in the talosconfig.
type talosconfigContext struct {
	Endpoints []string `yaml:"endpoints"`
	CA        string   `yaml:"ca"`
	Crt       string   `yaml:"crt"`
	Key       string   `yaml:"key"`
}

// talosconfigFile represents the top-level talosconfig structure.
type talosconfigFile struct {
	Context  string                        `yaml:"context"`
	Contexts map[string]talosconfigContext  `yaml:"contexts"`
}

// GenerateTalosconfig generates a talosconfig YAML for CLI access to worker nodes.
func GenerateTalosconfig(input TalosconfigInput) ([]byte, error) {
	if input.ContextName == "" {
		return nil, fmt.Errorf("context name is required")
	}
	if len(input.Endpoints) == 0 {
		return nil, fmt.Errorf("at least one endpoint is required")
	}
	if len(input.CACert) == 0 || len(input.AdminCert) == 0 || len(input.AdminKey) == 0 {
		return nil, fmt.Errorf("CA cert, admin cert, and admin key are required")
	}

	cfg := talosconfigFile{
		Context: input.ContextName,
		Contexts: map[string]talosconfigContext{
			input.ContextName: {
				Endpoints: input.Endpoints,
				CA:        base64.StdEncoding.EncodeToString(input.CACert),
				Crt:       base64.StdEncoding.EncodeToString(input.AdminCert),
				Key:       base64.StdEncoding.EncodeToString(input.AdminKey),
			},
		},
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshaling talosconfig: %w", err)
	}

	return data, nil
}
