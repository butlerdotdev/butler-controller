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
	"regexp"
	"testing"
)

func TestGenerateBootstrapToken(t *testing.T) {
	token, err := GenerateBootstrapToken()
	if err != nil {
		t.Fatalf("GenerateBootstrapToken() error = %v", err)
	}

	// Token format: <6 alphanum>.<16 alphanum>
	pattern := regexp.MustCompile(`^[a-z0-9]{6}\.[a-z0-9]{16}$`)
	if !pattern.MatchString(token) {
		t.Errorf("token %q does not match expected format <6char>.<16char>", token)
	}
}

func TestGenerateBootstrapToken_Uniqueness(t *testing.T) {
	tokens := make(map[string]bool)
	for i := 0; i < 100; i++ {
		token, err := GenerateBootstrapToken()
		if err != nil {
			t.Fatalf("GenerateBootstrapToken() error = %v", err)
		}
		if tokens[token] {
			t.Errorf("duplicate token generated: %s", token)
		}
		tokens[token] = true
	}
}
