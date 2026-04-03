// Copyright 2026 The Butler Authors.
// SPDX-License-Identifier: Apache-2.0

package tenantcluster

import (
	"errors"
	"testing"
)

func TestTruncateError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{"short", errors.New("connection refused"), "connection refused"},
		{"exactly 100", errors.New("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{"over 100", errors.New("this error message is longer than one hundred characters and should be truncated to keep condition messages readable"), "this error message is longer than one hundred characters and should be truncated to keep conditio..."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateError(tt.err)
			if got != tt.expected {
				t.Errorf("truncateError() = %q (len %d), want %q (len %d)", got, len(got), tt.expected, len(tt.expected))
			}
		})
	}
}
