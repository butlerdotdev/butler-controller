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

package imagesync

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const (
	nutanixAPIPath     = "/api/nutanix/v3"
	nutanixHTTPTimeout = 30 * time.Second
)

// reconcileNutanix handles image sync for Nutanix providers.
// Direct mode: creates an image with source_uri so Prism Central downloads directly.
// Proxy mode: not yet implemented.
func (r *Reconciler) reconcileNutanix(ctx context.Context, is *butlerv1alpha1.ImageSync, pc *butlerv1alpha1.ProviderConfig, bc *butlerv1alpha1.ButlerConfig, imageName, artifactURL, factoryAPIKey string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	client, err := r.newNutanixClient(ctx, pc)
	if err != nil {
		return r.setFailed(ctx, is, "CredentialsError", fmt.Sprintf("failed to create Nutanix client: %v", err))
	}

	// Check if image already exists
	existingUUID, err := client.getImageByName(ctx, imageName)
	if err != nil {
		return r.setFailed(ctx, is, "NutanixAPIError", fmt.Sprintf("failed to check existing image: %v", err))
	}
	if existingUUID != "" {
		logger.Info("image already exists on Nutanix", "uuid", existingUUID, "name", imageName)
		return r.setReady(ctx, is, existingUUID)
	}

	if is.Spec.TransferMode == butlerv1alpha1.TransferModeProxy {
		return r.setFailed(ctx, is, "UnsupportedTransferMode",
			"proxy transfer mode for Nutanix is not yet implemented; use direct mode")
	}

	// Direct mode: create image with source_uri
	logger.Info("creating Nutanix image via direct download", "name", imageName, "url", artifactURL)

	taskUUID, err := client.createImageDirect(ctx, imageName, artifactURL,
		fmt.Sprintf("Butler Image Factory: %s %s", is.Spec.FactoryRef.Version, is.Spec.FactoryRef.SchematicID[:8]))
	if err != nil {
		return r.setFailed(ctx, is, "CreateImageFailed", fmt.Sprintf("failed to create Nutanix image: %v", err))
	}

	// Store task UUID in status for polling
	is.SetPhase(butlerv1alpha1.ImageSyncPhaseDownloading)
	is.Status.ObservedGeneration = is.Generation
	// Store task UUID in failure reason temporarily (we don't have a dedicated field)
	// This is a pragmatic approach — the task UUID is needed for polling
	is.Status.FailureReason = "task:" + taskUUID
	is.Status.FailureMessage = ""
	meta.SetStatusCondition(&is.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.ConditionTypeProgressing,
		Status:             metav1.ConditionTrue,
		Reason:             butlerv1alpha1.ReasonImageDownloading,
		Message:            fmt.Sprintf("Nutanix Prism Central is downloading image (task: %s)", taskUUID),
		ObservedGeneration: is.Generation,
	})
	if err := r.Status().Update(ctx, is); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueShort}, nil
}

// pollNutanixImage checks the status of a Nutanix image task.
func (r *Reconciler) pollNutanixImage(ctx context.Context, is *butlerv1alpha1.ImageSync, pc *butlerv1alpha1.ProviderConfig, imageName string) (ctrl.Result, error) {
	client, err := r.newNutanixClient(ctx, pc)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// Check if we have a task UUID stored
	taskUUID := ""
	if strings.HasPrefix(is.Status.FailureReason, "task:") {
		taskUUID = strings.TrimPrefix(is.Status.FailureReason, "task:")
	}

	if taskUUID != "" {
		// Poll task status
		status, err := client.getTaskStatus(ctx, taskUUID)
		if err != nil {
			return ctrl.Result{RequeueAfter: requeueShort}, nil
		}

		switch status.Status {
		case "SUCCEEDED":
			// Find the image UUID from the task
			imageUUID := ""
			for _, ref := range status.EntityRefs {
				if ref.Kind == "image" {
					imageUUID = ref.UUID
					break
				}
			}
			if imageUUID == "" {
				// Try to look up by name
				imageUUID, _ = client.getImageByName(ctx, imageName)
			}
			if imageUUID == "" {
				return r.setFailed(ctx, is, "ImageUUIDNotFound", "task succeeded but image UUID not found")
			}
			// Clear the task tracking
			is.Status.FailureReason = ""
			return r.setReady(ctx, is, imageUUID)

		case "FAILED":
			is.Status.FailureReason = ""
			return r.setFailed(ctx, is, "NutanixTaskFailed",
				fmt.Sprintf("image creation task failed: %s", status.Message))
		}

		// Still running
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// No task UUID — try to find image by name
	imageUUID, err := client.getImageByName(ctx, imageName)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
	if imageUUID != "" {
		return r.setReady(ctx, is, imageUUID)
	}

	// Image not found and no task — something went wrong
	return r.setFailed(ctx, is, "ImageNotFound", "image not found on Nutanix and no active task")
}

// nutanixClient wraps HTTP calls to Prism Central.
type nutanixClient struct {
	httpClient *http.Client
	baseURL    string
	authHeader string
}

// newNutanixClient creates a Prism Central HTTP client from ProviderConfig.
func (r *Reconciler) newNutanixClient(ctx context.Context, pc *butlerv1alpha1.ProviderConfig) (*nutanixClient, error) {
	creds, err := r.getProviderCredentials(ctx, pc)
	if err != nil {
		return nil, err
	}

	username := string(creds["username"])
	password := string(creds["password"])
	if username == "" || password == "" {
		return nil, fmt.Errorf("Nutanix credentials missing username or password")
	}

	if pc.Spec.Nutanix == nil {
		return nil, fmt.Errorf("ProviderConfig missing nutanix configuration")
	}

	endpoint := strings.TrimSuffix(pc.Spec.Nutanix.Endpoint, "/")
	port := pc.Spec.Nutanix.Port
	if port == 0 {
		port = 9440
	}
	baseURL := fmt.Sprintf("%s:%d%s", endpoint, port, nutanixAPIPath)

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: pc.Spec.Nutanix.Insecure, //nolint:gosec // User-controlled config
		},
	}

	auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))

	return &nutanixClient{
		httpClient: &http.Client{Transport: transport, Timeout: nutanixHTTPTimeout},
		baseURL:    baseURL,
		authHeader: "Basic " + auth,
	}, nil
}

// createImageDirect creates a Nutanix image with source_uri for direct download.
func (c *nutanixClient) createImageDirect(ctx context.Context, name, sourceURL, description string) (string, error) {
	body := map[string]interface{}{
		"api_version": "3.1",
		"metadata": map[string]interface{}{
			"kind": "image",
		},
		"spec": map[string]interface{}{
			"name":        name,
			"description": description,
			"resources": map[string]interface{}{
				"image_type": "DISK_IMAGE",
				"source_uri": sourceURL,
			},
		},
	}

	resp, err := c.doRequest(ctx, "POST", "/images", body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("create image failed: status %d, body: %s", resp.StatusCode, string(respBody))
	}

	var createResp struct {
		Status struct {
			ExecutionContext struct {
				TaskUUID string `json:"task_uuid"`
			} `json:"execution_context"`
		} `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&createResp); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	return createResp.Status.ExecutionContext.TaskUUID, nil
}

// getImageByName finds a Nutanix image by name and returns its UUID.
func (c *nutanixClient) getImageByName(ctx context.Context, name string) (string, error) {
	body := map[string]interface{}{
		"kind":   "image",
		"filter": fmt.Sprintf("name==%s", name),
	}

	resp, err := c.doRequest(ctx, "POST", "/images/list", body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("list images failed: status %d", resp.StatusCode)
	}

	var listResp struct {
		Entities []struct {
			Metadata struct {
				UUID string `json:"uuid"`
			} `json:"metadata"`
			Status struct {
				Name string `json:"name"`
			} `json:"status"`
		} `json:"entities"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		return "", err
	}

	for _, e := range listResp.Entities {
		if e.Status.Name == name {
			return e.Metadata.UUID, nil
		}
	}
	return "", nil
}

type nutanixTaskStatus struct {
	Status     string
	Message    string
	EntityRefs []struct {
		Kind string
		UUID string
	}
}

// getTaskStatus checks a Nutanix task status.
func (c *nutanixClient) getTaskStatus(ctx context.Context, taskUUID string) (*nutanixTaskStatus, error) {
	resp, err := c.doRequest(ctx, "GET", "/tasks/"+taskUUID, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get task failed: status %d", resp.StatusCode)
	}

	var taskResp struct {
		Status              string `json:"status"`
		ProgressMessage     string `json:"progress_message"`
		EntityReferenceList []struct {
			Kind string `json:"kind"`
			UUID string `json:"uuid"`
		} `json:"entity_reference_list"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&taskResp); err != nil {
		return nil, err
	}

	result := &nutanixTaskStatus{
		Status:  taskResp.Status,
		Message: taskResp.ProgressMessage,
	}
	for _, ref := range taskResp.EntityReferenceList {
		result.EntityRefs = append(result.EntityRefs, struct {
			Kind string
			UUID string
		}{Kind: ref.Kind, UUID: ref.UUID})
	}
	return result, nil
}

// doRequest performs an HTTP request to Prism Central.
func (c *nutanixClient) doRequest(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	url := c.baseURL + path

	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}
		reqBody = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", c.authHeader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	return c.httpClient.Do(req)
}
