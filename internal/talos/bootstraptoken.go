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
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	bootstrapTokenChars = "abcdefghijklmnopqrstuvwxyz0123456789"
	tokenIDLength       = 6
	tokenSecretLength   = 16
)

// GenerateBootstrapToken generates a random kubeadm-format bootstrap token.
// Format: "<6-char-id>.<16-char-secret>" (e.g., "a1b2c3.abcdef0123456789").
func GenerateBootstrapToken() (string, error) {
	id, err := randomString(tokenIDLength)
	if err != nil {
		return "", fmt.Errorf("failed to generate token ID: %w", err)
	}

	secret, err := randomString(tokenSecretLength)
	if err != nil {
		return "", fmt.Errorf("failed to generate token secret: %w", err)
	}

	return id + "." + secret, nil
}

// CreateBootstrapTokenSecret creates a bootstrap token Secret in the tenant
// API server's kube-system namespace. The token has no TTL so it remains
// valid for static machine configs.
//
// Steward's soot PhaseBootstrapToken already creates the RBAC needed for
// bootstrap tokens to work (AllowBootstrapTokensToGetNodes, AllowBootstrapTokensToPostCSRs,
// AutoApproveNodeBootstrapTokens, AutoApproveNodeCertificateRotation).
// These RBAC rules apply to ALL bootstrap tokens via group-based ClusterRoleBindings.
func CreateBootstrapTokenSecret(ctx context.Context, client kubernetes.Interface, token string) error {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 || len(parts[0]) != tokenIDLength || len(parts[1]) != tokenSecretLength {
		return fmt.Errorf("invalid bootstrap token format: expected <6char>.<16char>")
	}

	tokenID := parts[0]
	tokenSecret := parts[1]
	secretName := "bootstrap-token-" + tokenID

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: "kube-system",
		},
		Type: corev1.SecretTypeBootstrapToken,
		StringData: map[string]string{
			"token-id":                       tokenID,
			"token-secret":                   tokenSecret,
			"usage-bootstrap-authentication": "true",
			"usage-bootstrap-signing":        "true",
			"auth-extra-groups":              "system:bootstrappers:kubeadm:default-node-token",
		},
	}

	_, err := client.CoreV1().Secrets("kube-system").Create(ctx, secret, metav1.CreateOptions{})
	if err != nil {
		if k8serrors.IsAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("failed to create bootstrap token secret: %w", err)
	}

	return nil
}

// FindExistingBootstrapToken checks the tenant API server for an existing bootstrap
// token Secret. Returns the token string if found, or empty string if none exists.
// This enables idempotent reconciliation when the reconciler creates a token but
// fails before creating the CAPI bootstrap Secret.
func FindExistingBootstrapToken(ctx context.Context, client kubernetes.Interface) (string, error) {
	secrets, err := client.CoreV1().Secrets("kube-system").List(ctx, metav1.ListOptions{
		FieldSelector: "type=" + string(corev1.SecretTypeBootstrapToken),
	})
	if err != nil {
		return "", fmt.Errorf("failed to list bootstrap token secrets: %w", err)
	}

	for _, s := range secrets.Items {
		if !strings.HasPrefix(s.Name, "bootstrap-token-") {
			continue
		}
		tokenID := string(s.Data["token-id"])
		tokenSecret := string(s.Data["token-secret"])
		if tokenID != "" && tokenSecret != "" {
			return tokenID + "." + tokenSecret, nil
		}
	}

	return "", nil
}

func randomString(length int) (string, error) {
	result := make([]byte, length)
	max := big.NewInt(int64(len(bootstrapTokenChars)))

	for i := range result {
		idx, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		result[i] = bootstrapTokenChars[idx.Int64()]
	}

	return string(result), nil
}
