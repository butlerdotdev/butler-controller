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

package workspace

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

const (
	workspacesNamespace = "workspaces"
	sshPort             = 2222
	requeueInterval     = 5 * time.Minute
	requeueShort        = 5 * time.Second
)

// Reconciler reconciles Workspace resources.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=workspaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=workspaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=workspaces/finalizers,verbs=update
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=tenantclusters/status,verbs=get
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=butlerconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=butler.butlerlabs.dev,resources=users,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch workspace
	ws := &butlerv1alpha1.Workspace{}
	if err := r.Get(ctx, req.NamespacedName, ws); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	logger.Info("reconciling workspace",
		"name", ws.Name,
		"namespace", ws.Namespace,
		"phase", ws.Status.Phase)

	// Handle deletion
	if !ws.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, ws)
	}

	// Ensure finalizer
	if !controllerutil.ContainsFinalizer(ws, butlerv1alpha1.FinalizerWorkspace) {
		controllerutil.AddFinalizer(ws, butlerv1alpha1.FinalizerWorkspace)
		if err := r.Update(ctx, ws); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Initialize phase
	if ws.Status.Phase == "" {
		ws.Status.Phase = butlerv1alpha1.WorkspacePhasePending
		if err := r.Status().Update(ctx, ws); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Validate TenantCluster and workspaces config
	tc := &butlerv1alpha1.TenantCluster{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      ws.Spec.ClusterRef.Name,
		Namespace: ws.Namespace,
	}, tc); err != nil {
		if errors.IsNotFound(err) {
			return r.setFailed(ctx, ws, "ClusterNotFound",
				fmt.Sprintf("TenantCluster %q not found", ws.Spec.ClusterRef.Name))
		}
		return ctrl.Result{}, err
	}

	if tc.Spec.Workspaces == nil || !tc.Spec.Workspaces.Enabled {
		return r.setFailed(ctx, ws, "WorkspacesDisabled",
			fmt.Sprintf("Workspaces not enabled on cluster %q", tc.Name))
	}

	if tc.Status.Phase != butlerv1alpha1.TenantClusterPhaseReady {
		logger.Info("waiting for cluster to be ready", "cluster", tc.Name, "phase", tc.Status.Phase)
		meta.SetStatusCondition(&ws.Status.Conditions, metav1.Condition{
			Type:    butlerv1alpha1.WorkspaceConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  butlerv1alpha1.ReasonWaitingForDependencies,
			Message: fmt.Sprintf("Waiting for cluster %q to be ready (current: %s)", tc.Name, tc.Status.Phase),
		})
		if err := r.Status().Update(ctx, ws); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	// Check workspace quota
	if err := r.checkWorkspaceQuota(ctx, ws, tc); err != nil {
		return r.setFailed(ctx, ws, butlerv1alpha1.ReasonQuotaExceeded, err.Error())
	}

	// Get ButlerConfig for kubeconfig key selection
	butlerConfig := &butlerv1alpha1.ButlerConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: "butler"}, butlerConfig); err != nil {
		if !errors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		butlerConfig = nil
	}

	// Get tenant kubeconfig
	kubeconfigData, err := r.getTenantKubeconfig(ctx, tc, butlerConfig)
	if err != nil {
		logger.Error(err, "failed to get tenant kubeconfig")
		return ctrl.Result{RequeueAfter: requeueInterval}, nil
	}

	// Create tenant clientset
	tenantClient, err := r.buildTenantClient(kubeconfigData)
	if err != nil {
		return r.setFailed(ctx, ws, "TenantClientError", err.Error())
	}

	// Handle stopped workspace resume
	if ws.Status.Phase == butlerv1alpha1.WorkspacePhaseStopped {
		return r.handleStoppedWorkspace(ctx, ws, tc, tenantClient)
	}

	// Handle auto-stop check
	if ws.Status.Phase == butlerv1alpha1.WorkspacePhaseRunning {
		return r.handleRunningWorkspace(ctx, ws, tc, tenantClient)
	}

	// Handle starting phase (resuming from stopped)
	if ws.Status.Phase == butlerv1alpha1.WorkspacePhaseStarting {
		return r.reconcileCreatePhase(ctx, ws, tc, tenantClient)
	}

	// Transition Pending → Creating
	if ws.Status.Phase == butlerv1alpha1.WorkspacePhasePending {
		ws.Status.Phase = butlerv1alpha1.WorkspacePhaseCreating
		if err := r.Status().Update(ctx, ws); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Creating phase — provision tenant resources
	if ws.Status.Phase == butlerv1alpha1.WorkspacePhaseCreating {
		return r.reconcileCreatePhase(ctx, ws, tc, tenantClient)
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// reconcileCreatePhase handles PVC, Pod, and SSH setup creation.
func (r *Reconciler) reconcileCreatePhase(ctx context.Context, ws *butlerv1alpha1.Workspace,
	tc *butlerv1alpha1.TenantCluster, tenantClient kubernetes.Interface) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Ensure workspaces namespace
	if err := r.ensureWorkspacesNamespace(ctx, tenantClient, tc); err != nil {
		logger.Error(err, "failed to ensure workspaces namespace")
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// Resolve SSH keys
	sshKeys, err := r.resolveSSHKeys(ctx, ws)
	if err != nil {
		return r.setFailed(ctx, ws, "SSHKeysRequired", err.Error())
	}

	// Create SSH authorized_keys secret on tenant cluster
	if err := r.ensureSSHKeysSecret(ctx, tenantClient, ws, sshKeys); err != nil {
		logger.Error(err, "failed to create SSH keys secret")
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// Copy Git credentials if any repository has a secretRef
	if hasGitCredentials(ws) {
		if err := r.copyGitCredentials(ctx, tenantClient, ws); err != nil {
			logger.Error(err, "failed to copy git credentials")
			return ctrl.Result{RequeueAfter: requeueShort}, nil
		}
	}

	// Create PVC
	pvcName := fmt.Sprintf("ws-%s", ws.Name)
	if err := r.ensurePVC(ctx, tenantClient, ws, pvcName); err != nil {
		logger.Error(err, "failed to create PVC")
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
	ws.Status.PVCName = pvcName
	meta.SetStatusCondition(&ws.Status.Conditions, metav1.Condition{
		Type:   butlerv1alpha1.WorkspaceConditionPVCReady,
		Status: metav1.ConditionTrue,
		Reason: butlerv1alpha1.ReasonReady,
	})

	// Resolve EnvFrom
	var envVars []corev1.EnvVar
	if ws.Spec.EnvFrom != nil {
		envVars, err = r.resolveEnvFrom(ctx, tenantClient, ws)
		if err != nil {
			logger.Error(err, "failed to resolve envFrom", "workload", ws.Spec.EnvFrom.Name)
			// Non-fatal — continue without env vars
		}
	}

	// Create Pod
	podName := fmt.Sprintf("ws-%s", ws.Name)
	if err := r.ensurePod(ctx, tenantClient, ws, podName, pvcName, envVars); err != nil {
		logger.Error(err, "failed to create pod")
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}
	ws.Status.PodName = podName

	// Check pod status
	pod, err := tenantClient.CoreV1().Pods(workspacesNamespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	if pod.Status.Phase != corev1.PodRunning {
		logger.Info("waiting for pod to be running", "pod", podName, "phase", pod.Status.Phase)
		meta.SetStatusCondition(&ws.Status.Conditions, metav1.Condition{
			Type:    butlerv1alpha1.WorkspaceConditionPodReady,
			Status:  metav1.ConditionFalse,
			Reason:  butlerv1alpha1.ReasonCreating,
			Message: fmt.Sprintf("Pod phase: %s", pod.Status.Phase),
		})
		if err := r.Status().Update(ctx, ws); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	// Pod is running
	meta.SetStatusCondition(&ws.Status.Conditions, metav1.Condition{
		Type:   butlerv1alpha1.WorkspaceConditionPodReady,
		Status: metav1.ConditionTrue,
		Reason: butlerv1alpha1.ReasonReady,
	})
	meta.SetStatusCondition(&ws.Status.Conditions, metav1.Condition{
		Type:   butlerv1alpha1.WorkspaceConditionSSHReady,
		Status: metav1.ConditionTrue,
		Reason: butlerv1alpha1.ReasonReady,
	})
	meta.SetStatusCondition(&ws.Status.Conditions, metav1.Condition{
		Type:   butlerv1alpha1.WorkspaceConditionReady,
		Status: metav1.ConditionTrue,
		Reason: butlerv1alpha1.ReasonReady,
	})

	ws.Status.Phase = butlerv1alpha1.WorkspacePhaseRunning
	ws.Status.ObservedGeneration = ws.Generation
	if err := r.Status().Update(ctx, ws); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("workspace is running", "name", ws.Name, "pod", podName)
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// handleRunningWorkspace checks connect/disconnect annotations and idle/auto-stop timeouts.
func (r *Reconciler) handleRunningWorkspace(ctx context.Context, ws *butlerv1alpha1.Workspace,
	tc *butlerv1alpha1.TenantCluster, tenantClient kubernetes.Interface) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Check if pod still exists
	podName := ws.Status.PodName
	if podName != "" {
		_, err := tenantClient.CoreV1().Pods(workspacesNamespace).Get(ctx, podName, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			logger.Info("pod no longer exists, marking as stopped", "pod", podName)
			ws.Status.Phase = butlerv1alpha1.WorkspacePhaseStopped
			ws.Status.PodName = ""
			ws.Status.Connected = false
			ws.Status.SSHEndpoint = ""
			ws.Status.ServiceName = ""
			if err := r.Status().Update(ctx, ws); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: requeueInterval}, nil
		}
	}

	annotations := ws.GetAnnotations()
	connectRequested := annotations != nil && annotations[butlerv1alpha1.AnnotationConnect] == "true"

	if connectRequested && !ws.Status.Connected {
		// Create SSH service
		return r.handleConnect(ctx, ws, tenantClient)
	}

	if !connectRequested && ws.Status.Connected {
		// Tear down SSH service
		return r.handleDisconnect(ctx, ws, tenantClient)
	}

	// Check idle timeout (connected workspace)
	if ws.Status.Connected {
		connectTimeStr := annotations[butlerv1alpha1.AnnotationConnectTime]
		if connectTimeStr != "" {
			connectTime, err := time.Parse(time.RFC3339, connectTimeStr)
			if err == nil {
				idleTimeout := 4 * time.Hour
				if ws.Spec.IdleTimeout != nil {
					idleTimeout = ws.Spec.IdleTimeout.Duration
				}
				elapsed := time.Since(connectTime)
				if elapsed > idleTimeout {
					logger.Info("idle timeout reached, disconnecting", "elapsed", elapsed, "timeout", idleTimeout)
					// Remove connect annotation to trigger disconnect
					delete(ws.Annotations, butlerv1alpha1.AnnotationConnect)
					delete(ws.Annotations, butlerv1alpha1.AnnotationConnectTime)
					if err := r.Update(ctx, ws); err != nil {
						return ctrl.Result{}, err
					}
					return ctrl.Result{Requeue: true}, nil
				}
				// Requeue when timeout expires
				remaining := idleTimeout - elapsed
				return ctrl.Result{RequeueAfter: remaining}, nil
			}
		}
	}

	// Check auto-stop (disconnected workspace)
	if !ws.Status.Connected && ws.Status.LastDisconnectTime != nil {
		autoStopAfter := 8 * time.Hour
		if ws.Spec.AutoStopAfter != nil {
			autoStopAfter = ws.Spec.AutoStopAfter.Duration
		}
		if autoStopAfter > 0 {
			elapsed := time.Since(ws.Status.LastDisconnectTime.Time)
			if elapsed > autoStopAfter {
				logger.Info("auto-stop reached, stopping workspace", "elapsed", elapsed, "timeout", autoStopAfter)
				// Delete pod, keep PVC
				if ws.Status.PodName != "" {
					err := tenantClient.CoreV1().Pods(workspacesNamespace).Delete(ctx, ws.Status.PodName, metav1.DeleteOptions{})
					if err != nil && !errors.IsNotFound(err) {
						return ctrl.Result{}, err
					}
				}
				ws.Status.Phase = butlerv1alpha1.WorkspacePhaseStopped
				ws.Status.PodName = ""
				meta.SetStatusCondition(&ws.Status.Conditions, metav1.Condition{
					Type:    butlerv1alpha1.WorkspaceConditionPodReady,
					Status:  metav1.ConditionFalse,
					Reason:  "Stopped",
					Message: "Pod stopped after inactivity",
				})
				if err := r.Status().Update(ctx, ws); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: requeueInterval}, nil
			}
			remaining := autoStopAfter - elapsed
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
	}

	// Check auto-delete for stopped workspaces
	if ws.Status.Phase == butlerv1alpha1.WorkspacePhaseStopped && ws.Status.LastDisconnectTime != nil {
		autoDeleteAfter := 720 * time.Hour // 30 days
		if tc.Spec.Workspaces != nil && tc.Spec.Workspaces.AutoDeleteAfter != nil {
			autoDeleteAfter = tc.Spec.Workspaces.AutoDeleteAfter.Duration
		}
		if autoDeleteAfter > 0 {
			elapsed := time.Since(ws.Status.LastDisconnectTime.Time)
			if elapsed > autoDeleteAfter {
				logger.Info("auto-delete reached, deleting workspace", "elapsed", elapsed)
				if err := r.Delete(ctx, ws); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, nil
			}
		}
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// handleStoppedWorkspace checks for connect requests on stopped workspaces and auto-delete.
func (r *Reconciler) handleStoppedWorkspace(ctx context.Context, ws *butlerv1alpha1.Workspace,
	tc *butlerv1alpha1.TenantCluster, tenantClient kubernetes.Interface) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	annotations := ws.GetAnnotations()
	connectRequested := annotations != nil && annotations[butlerv1alpha1.AnnotationConnect] == "true"

	// Resume if connect requested
	if connectRequested {
		logger.Info("resuming stopped workspace")
		ws.Status.Phase = butlerv1alpha1.WorkspacePhaseStarting
		if err := r.Status().Update(ctx, ws); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Check auto-delete
	if ws.Status.LastDisconnectTime != nil {
		autoDeleteAfter := 720 * time.Hour
		if tc.Spec.Workspaces != nil && tc.Spec.Workspaces.AutoDeleteAfter != nil {
			autoDeleteAfter = tc.Spec.Workspaces.AutoDeleteAfter.Duration
		}
		if autoDeleteAfter > 0 {
			elapsed := time.Since(ws.Status.LastDisconnectTime.Time)
			if elapsed > autoDeleteAfter {
				logger.Info("auto-delete reached for stopped workspace", "elapsed", elapsed)
				if err := r.Delete(ctx, ws); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, nil
			}
			remaining := autoDeleteAfter - elapsed
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// handleConnect creates the SSH service for the workspace.
func (r *Reconciler) handleConnect(ctx context.Context, ws *butlerv1alpha1.Workspace,
	tenantClient kubernetes.Interface) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	svcName := fmt.Sprintf("ws-%s-ssh", ws.Name)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: workspacesNamespace,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy:      "butler",
				butlerv1alpha1.LabelWorkspaceOwner: sanitizeLabel(ws.Spec.Owner),
			},
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Selector: map[string]string{
				"butler.butlerlabs.dev/workspace": ws.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "ssh",
					Port:       int32(sshPort),
					TargetPort: intstr.FromInt32(int32(sshPort)),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	_, err := tenantClient.CoreV1().Services(workspacesNamespace).Create(ctx, svc, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return ctrl.Result{}, fmt.Errorf("failed to create SSH service: %w", err)
	}

	// Record connect time
	if ws.Annotations == nil {
		ws.Annotations = make(map[string]string)
	}
	ws.Annotations[butlerv1alpha1.AnnotationConnectTime] = time.Now().UTC().Format(time.RFC3339)
	if err := r.Update(ctx, ws); err != nil {
		return ctrl.Result{}, err
	}

	// Check if service has external IP
	existingSvc, err := tenantClient.CoreV1().Services(workspacesNamespace).Get(ctx, svcName, metav1.GetOptions{})
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	var endpoint string
	if len(existingSvc.Status.LoadBalancer.Ingress) > 0 {
		ingress := existingSvc.Status.LoadBalancer.Ingress[0]
		if ingress.IP != "" {
			endpoint = fmt.Sprintf("%s:%d", ingress.IP, sshPort)
		} else if ingress.Hostname != "" {
			endpoint = fmt.Sprintf("%s:%d", ingress.Hostname, sshPort)
		}
	}

	if endpoint == "" {
		logger.Info("waiting for LoadBalancer IP", "service", svcName)
		return ctrl.Result{RequeueAfter: requeueShort}, nil
	}

	ws.Status.ServiceName = svcName
	ws.Status.SSHEndpoint = endpoint
	ws.Status.Connected = true
	now := metav1.Now()
	ws.Status.LastActivityTime = &now
	if err := r.Status().Update(ctx, ws); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("workspace connected", "endpoint", endpoint)
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// handleDisconnect tears down the SSH service.
func (r *Reconciler) handleDisconnect(ctx context.Context, ws *butlerv1alpha1.Workspace,
	tenantClient kubernetes.Interface) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if ws.Status.ServiceName != "" {
		err := tenantClient.CoreV1().Services(workspacesNamespace).Delete(ctx, ws.Status.ServiceName, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("failed to delete SSH service: %w", err)
		}
	}

	now := metav1.Now()
	ws.Status.ServiceName = ""
	ws.Status.SSHEndpoint = ""
	ws.Status.Connected = false
	ws.Status.LastDisconnectTime = &now
	if err := r.Status().Update(ctx, ws); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("workspace disconnected")
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

// handleDeletion cleans up tenant resources and removes the finalizer.
func (r *Reconciler) handleDeletion(ctx context.Context, ws *butlerv1alpha1.Workspace) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(ws, butlerv1alpha1.FinalizerWorkspace) {
		return ctrl.Result{}, nil
	}

	logger.Info("cleaning up workspace resources")

	// Get TenantCluster for kubeconfig
	tc := &butlerv1alpha1.TenantCluster{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      ws.Spec.ClusterRef.Name,
		Namespace: ws.Namespace,
	}, tc); err != nil {
		if errors.IsNotFound(err) {
			return r.removeFinalizer(ctx, ws)
		}
		return ctrl.Result{}, err
	}

	butlerConfig := &butlerv1alpha1.ButlerConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: "butler"}, butlerConfig); err != nil {
		if !errors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		butlerConfig = nil
	}

	kubeconfigData, err := r.getTenantKubeconfig(ctx, tc, butlerConfig)
	if err != nil {
		logger.Error(err, "failed to get tenant kubeconfig for cleanup, removing finalizer anyway")
		return r.removeFinalizer(ctx, ws)
	}

	tenantClient, err := r.buildTenantClient(kubeconfigData)
	if err != nil {
		logger.Error(err, "failed to build tenant client for cleanup, removing finalizer anyway")
		return r.removeFinalizer(ctx, ws)
	}

	// Delete service
	if ws.Status.ServiceName != "" {
		if err := tenantClient.CoreV1().Services(workspacesNamespace).Delete(ctx, ws.Status.ServiceName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "failed to delete service", "service", ws.Status.ServiceName)
		}
	}

	// Delete pod
	if ws.Status.PodName != "" {
		if err := tenantClient.CoreV1().Pods(workspacesNamespace).Delete(ctx, ws.Status.PodName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "failed to delete pod", "pod", ws.Status.PodName)
		}
	}

	// Delete PVC
	if ws.Status.PVCName != "" {
		if err := tenantClient.CoreV1().PersistentVolumeClaims(workspacesNamespace).Delete(ctx, ws.Status.PVCName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "failed to delete PVC", "pvc", ws.Status.PVCName)
		}
	}

	// Delete SSH keys secret
	sshSecretName := fmt.Sprintf("ws-%s-ssh-keys", ws.Name)
	if err := tenantClient.CoreV1().Secrets(workspacesNamespace).Delete(ctx, sshSecretName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		logger.Error(err, "failed to delete SSH keys secret")
	}

	// Delete copied Git credentials
	if hasGitCredentials(ws) {
		gitSecretName := fmt.Sprintf("ws-%s-git-credentials", ws.Name)
		if err := tenantClient.CoreV1().Secrets(workspacesNamespace).Delete(ctx, gitSecretName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "failed to delete git credentials secret")
		}
	}

	return r.removeFinalizer(ctx, ws)
}

// removeFinalizer re-fetches the Workspace, removes the finalizer, and handles conflicts.
func (r *Reconciler) removeFinalizer(ctx context.Context, ws *butlerv1alpha1.Workspace) (ctrl.Result, error) {
	latest := &butlerv1alpha1.Workspace{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(ws), latest); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if controllerutil.ContainsFinalizer(latest, butlerv1alpha1.FinalizerWorkspace) {
		controllerutil.RemoveFinalizer(latest, butlerv1alpha1.FinalizerWorkspace)
		if err := r.Update(ctx, latest); err != nil {
			if errors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			if errors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// --- Helper methods ---

func (r *Reconciler) getTenantKubeconfig(ctx context.Context, tc *butlerv1alpha1.TenantCluster,
	butlerConfig *butlerv1alpha1.ButlerConfig) ([]byte, error) {
	secretName := fmt.Sprintf("%s-admin-kubeconfig", tc.Name)

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: tc.Status.TenantNamespace,
	}, secret); err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig secret: %w", err)
	}

	kubeconfigKey := "admin.conf"
	if butlerConfig != nil {
		mode := butlerConfig.GetControlPlaneExposureMode()
		if mode == butlerv1alpha1.ControlPlaneExposureModeIngress ||
			mode == butlerv1alpha1.ControlPlaneExposureModeGateway {
			kubeconfigKey = "admin.svc"
		}
	}

	kubeconfigData, ok := secret.Data[kubeconfigKey]
	if !ok {
		kubeconfigData, ok = secret.Data["admin.conf"]
		if !ok {
			return nil, fmt.Errorf("kubeconfig secret missing %s and admin.conf keys", kubeconfigKey)
		}
	}

	return kubeconfigData, nil
}

func (r *Reconciler) buildTenantClient(kubeconfigData []byte) (kubernetes.Interface, error) {
	restConfig, err := clientcmd.RESTConfigFromKubeConfig(kubeconfigData)
	if err != nil {
		return nil, fmt.Errorf("failed to build REST config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create tenant clientset: %w", err)
	}
	return clientset, nil
}

func (r *Reconciler) checkWorkspaceQuota(ctx context.Context, ws *butlerv1alpha1.Workspace,
	tc *butlerv1alpha1.TenantCluster) error {
	if tc.Spec.Workspaces.MaxWorkspaces == nil || *tc.Spec.Workspaces.MaxWorkspaces == 0 {
		return nil
	}

	wsList := &butlerv1alpha1.WorkspaceList{}
	if err := r.List(ctx, wsList, client.InNamespace(ws.Namespace),
		client.MatchingFields{"spec.clusterRef.name": ws.Spec.ClusterRef.Name}); err != nil {
		// If index not available, list all and filter
		if err := r.List(ctx, wsList, client.InNamespace(ws.Namespace)); err != nil {
			return fmt.Errorf("failed to list workspaces: %w", err)
		}
	}

	count := int32(0)
	for _, existing := range wsList.Items {
		if existing.Spec.ClusterRef.Name == ws.Spec.ClusterRef.Name && existing.Name != ws.Name {
			count++
		}
	}

	if count >= *tc.Spec.Workspaces.MaxWorkspaces {
		return fmt.Errorf("workspace quota exceeded: %d/%d workspaces on cluster %q",
			count, *tc.Spec.Workspaces.MaxWorkspaces, tc.Name)
	}
	return nil
}

func (r *Reconciler) resolveSSHKeys(ctx context.Context, ws *butlerv1alpha1.Workspace) ([]string, error) {
	if len(ws.Spec.SSHPublicKeys) > 0 {
		return ws.Spec.SSHPublicKeys, nil
	}

	// Look up User CRD by owner email
	userList := &butlerv1alpha1.UserList{}
	if err := r.List(ctx, userList); err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}

	for _, user := range userList.Items {
		if user.Spec.Email == ws.Spec.Owner {
			if len(user.Spec.SSHKeys) > 0 {
				keys := make([]string, 0, len(user.Spec.SSHKeys))
				for _, entry := range user.Spec.SSHKeys {
					keys = append(keys, entry.PublicKey)
				}
				return keys, nil
			}
			break
		}
	}

	return nil, fmt.Errorf("no SSH keys configured for user %q: add keys in Settings or provide them in the workspace spec", ws.Spec.Owner)
}

func (r *Reconciler) ensureWorkspacesNamespace(ctx context.Context, tenantClient kubernetes.Interface,
	tc *butlerv1alpha1.TenantCluster) error {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: workspacesNamespace,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy: "butler",
			},
		},
	}

	_, err := tenantClient.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	// Create ResourceQuota if configured
	if tc.Spec.Workspaces != nil && tc.Spec.Workspaces.ResourceQuota != nil {
		rq := tc.Spec.Workspaces.ResourceQuota
		quota := &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "workspace-quota",
				Namespace: workspacesNamespace,
			},
			Spec: corev1.ResourceQuotaSpec{
				Hard: corev1.ResourceList{},
			},
		}
		if rq.MaxCPU != "" {
			quota.Spec.Hard[corev1.ResourceLimitsCPU] = resource.MustParse(rq.MaxCPU)
		}
		if rq.MaxMemory != "" {
			quota.Spec.Hard[corev1.ResourceLimitsMemory] = resource.MustParse(rq.MaxMemory)
		}
		if rq.MaxStorage != "" {
			quota.Spec.Hard[corev1.ResourceRequestsStorage] = resource.MustParse(rq.MaxStorage)
		}

		_, err := tenantClient.CoreV1().ResourceQuotas(workspacesNamespace).Create(ctx, quota, metav1.CreateOptions{})
		if err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create resource quota: %w", err)
		}
	}

	return nil
}

func (r *Reconciler) ensureSSHKeysSecret(ctx context.Context, tenantClient kubernetes.Interface,
	ws *butlerv1alpha1.Workspace, sshKeys []string) error {
	secretName := fmt.Sprintf("ws-%s-ssh-keys", ws.Name)
	authorizedKeys := strings.Join(sshKeys, "\n") + "\n"

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: workspacesNamespace,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy:      "butler",
				butlerv1alpha1.LabelWorkspaceOwner: sanitizeLabel(ws.Spec.Owner),
			},
		},
		Data: map[string][]byte{
			"authorized_keys": []byte(authorizedKeys),
		},
	}

	_, err := tenantClient.CoreV1().Secrets(workspacesNamespace).Create(ctx, secret, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		_, err = tenantClient.CoreV1().Secrets(workspacesNamespace).Update(ctx, secret, metav1.UpdateOptions{})
	}
	return err
}

func (r *Reconciler) copyGitCredentials(ctx context.Context, tenantClient kubernetes.Interface,
	ws *butlerv1alpha1.Workspace) error {
	// Collect all secret refs from repositories
	repos := resolveRepositories(ws)
	mergedData := make(map[string][]byte)
	var secretType corev1.SecretType

	for _, repo := range repos {
		if repo.SecretRef == nil {
			continue
		}
		srcSecret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      repo.SecretRef.Name,
			Namespace: ws.Namespace,
		}, srcSecret); err != nil {
			return fmt.Errorf("failed to get git credentials secret %q: %w", repo.SecretRef.Name, err)
		}
		for k, v := range srcSecret.Data {
			mergedData[k] = v
		}
		if secretType == "" {
			secretType = srcSecret.Type
		}
	}

	if len(mergedData) == 0 {
		return nil
	}

	dstSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("ws-%s-git-credentials", ws.Name),
			Namespace: workspacesNamespace,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy: "butler",
			},
		},
		Data: mergedData,
		Type: secretType,
	}

	_, err := tenantClient.CoreV1().Secrets(workspacesNamespace).Create(ctx, dstSecret, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		_, err = tenantClient.CoreV1().Secrets(workspacesNamespace).Update(ctx, dstSecret, metav1.UpdateOptions{})
	}
	return err
}

func (r *Reconciler) ensurePVC(ctx context.Context, tenantClient kubernetes.Interface,
	ws *butlerv1alpha1.Workspace, pvcName string) error {
	storageSize := resource.MustParse("10Gi")
	if ws.Spec.StorageSize != nil {
		storageSize = *ws.Spec.StorageSize
	}

	// Ensure a workspace-friendly StorageClass exists with 1 replica
	// so workspace PVCs work on single-node tenant clusters.
	scName := "longhorn-workspace"
	r.ensureWorkspaceStorageClass(ctx, tenantClient, scName)

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: workspacesNamespace,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy:      "butler",
				butlerv1alpha1.LabelWorkspaceOwner: sanitizeLabel(ws.Spec.Owner),
				"butler.butlerlabs.dev/workspace":  ws.Name,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &scName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageSize,
				},
			},
		},
	}

	_, err := tenantClient.CoreV1().PersistentVolumeClaims(workspacesNamespace).Create(ctx, pvc, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil // PVC already exists (e.g., resuming from stopped)
	}
	return err
}

func (r *Reconciler) ensureWorkspaceStorageClass(ctx context.Context, tenantClient kubernetes.Interface, name string) {
	_, err := tenantClient.StorageV1().StorageClasses().Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return // already exists
	}
	reclaimPolicy := corev1.PersistentVolumeReclaimDelete
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy: "butler",
			},
		},
		Provisioner:   "driver.longhorn.io",
		ReclaimPolicy: &reclaimPolicy,
		Parameters: map[string]string{
			"numberOfReplicas":    "1",
			"staleReplicaTimeout": "30",
		},
		AllowVolumeExpansion: boolPtr(true),
	}
	tenantClient.StorageV1().StorageClasses().Create(ctx, sc, metav1.CreateOptions{})
}

func boolPtr(b bool) *bool { return &b }

func (r *Reconciler) resolveEnvFrom(ctx context.Context, tenantClient kubernetes.Interface,
	ws *butlerv1alpha1.Workspace) ([]corev1.EnvVar, error) {
	envFrom := ws.Spec.EnvFrom
	ns := envFrom.Namespace
	if ns == "" {
		ns = "default"
	}

	var envVars []corev1.EnvVar

	switch envFrom.Kind {
	case "Deployment", "":
		deploy, err := tenantClient.AppsV1().Deployments(ns).Get(ctx, envFrom.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get deployment %s/%s: %w", ns, envFrom.Name, err)
		}
		containers := deploy.Spec.Template.Spec.Containers
		if len(containers) > 0 {
			container := containers[0]
			if envFrom.Container != "" {
				for _, c := range containers {
					if c.Name == envFrom.Container {
						container = c
						break
					}
				}
			}
			envVars = container.Env
		}
	case "StatefulSet":
		sts, err := tenantClient.AppsV1().StatefulSets(ns).Get(ctx, envFrom.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get statefulset %s/%s: %w", ns, envFrom.Name, err)
		}
		containers := sts.Spec.Template.Spec.Containers
		if len(containers) > 0 {
			container := containers[0]
			if envFrom.Container != "" {
				for _, c := range containers {
					if c.Name == envFrom.Container {
						container = c
						break
					}
				}
			}
			envVars = container.Env
		}
	}

	// Filter out env vars that reference secrets/configmaps (not portable)
	var filtered []corev1.EnvVar
	for _, ev := range envVars {
		if ev.ValueFrom == nil {
			filtered = append(filtered, ev)
		}
	}

	return filtered, nil
}

func (r *Reconciler) ensurePod(ctx context.Context, tenantClient kubernetes.Interface,
	ws *butlerv1alpha1.Workspace, podName, pvcName string, envVars []corev1.EnvVar) error {

	// Check if pod already exists
	_, err := tenantClient.CoreV1().Pods(workspacesNamespace).Get(ctx, podName, metav1.GetOptions{})
	if err == nil {
		return nil // Already exists
	}
	if !errors.IsNotFound(err) {
		return err
	}

	sshSecretName := fmt.Sprintf("ws-%s-ssh-keys", ws.Name)

	// Collect repositories from both spec.repository (single, legacy) and spec.repositories (multi).
	repos := resolveRepositories(ws)

	// Build init containers
	var initContainers []corev1.Container
	if len(repos) > 0 {
		hasGitCredentials := false
		for _, repo := range repos {
			if repo.SecretRef != nil {
				hasGitCredentials = true
				break
			}
		}

		// Build a shell script that clones all repos and generates .code-workspace file
		cloneScript := buildCloneScript(repos)

		volumeMounts := []corev1.VolumeMount{
			{Name: "workspace", MountPath: "/workspace"},
		}
		if hasGitCredentials {
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name: "git-credentials", MountPath: "/git-credentials", ReadOnly: true,
			})
		}

		initContainers = append(initContainers, corev1.Container{
			Name:         "git-clone",
			Image:        ws.Spec.Image,
			Command:      []string{"/bin/sh", "-c", cloneScript},
			VolumeMounts: volumeMounts,
		})
	}

	if ws.Spec.Dotfiles != nil {
		installCmd := ws.Spec.Dotfiles.InstallCommand
		if installCmd == "" {
			installCmd = `cd /home/dev/.dotfiles && for f in install.sh install bootstrap.sh bootstrap setup.sh setup; do [ -x "$f" ] && ./"$f" && exit 0; done; [ -f Makefile ] && make`
		}
		dotfilesCmd := fmt.Sprintf("git clone %s /home/dev/.dotfiles && %s", ws.Spec.Dotfiles.URL, installCmd)
		initContainers = append(initContainers, corev1.Container{
			Name:    "dotfiles",
			Image:   ws.Spec.Image,
			Command: []string{"/bin/sh", "-c", dotfilesCmd},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "workspace", MountPath: "/workspace"},
			},
		})
	}

	// Resource limits
	cpuStr := "2"
	memStr := "4Gi"
	if ws.Spec.Resources != nil {
		if ws.Spec.Resources.CPU != "" {
			cpuStr = ws.Spec.Resources.CPU
		}
		if ws.Spec.Resources.Memory != "" {
			memStr = ws.Spec.Resources.Memory
		}
	}

	// Volumes
	volumes := []corev1.Volume{
		{
			Name: "workspace",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		},
		{
			Name: "ssh-keys",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  sshSecretName,
					DefaultMode: int32Ptr(0600),
				},
			},
		},
	}

	// Add git credentials volume if any repo has a secretRef
	if hasGitCredentials(ws) {
		gitSecretName := fmt.Sprintf("ws-%s-git-credentials", ws.Name)
		volumes = append(volumes, corev1.Volume{
			Name: "git-credentials",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  gitSecretName,
					DefaultMode: int32Ptr(0600),
				},
			},
		})
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: workspacesNamespace,
			Labels: map[string]string{
				butlerv1alpha1.LabelManagedBy:      "butler",
				butlerv1alpha1.LabelWorkspaceOwner: sanitizeLabel(ws.Spec.Owner),
				"butler.butlerlabs.dev/workspace":  ws.Name,
			},
		},
		Spec: corev1.PodSpec{
			InitContainers: initContainers,
			Containers: []corev1.Container{
				{
					Name:    "workspace",
					Image:   ws.Spec.Image,
					Command: []string{"/bin/sh", "-c"},
					Args:    []string{buildEntrypointScript(ws)},
					Ports: []corev1.ContainerPort{
						{
							Name:          "ssh",
							ContainerPort: int32(sshPort),
							Protocol:      corev1.ProtocolTCP,
						},
					},
					Env: envVars,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("250m"),
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(cpuStr),
							corev1.ResourceMemory: resource.MustParse(memStr),
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "workspace", MountPath: "/workspace"},
						{Name: "ssh-keys", MountPath: "/tmp/ssh-keys", ReadOnly: true},
					},
				},
			},
			Volumes:       volumes,
			RestartPolicy: corev1.RestartPolicyAlways,
			SecurityContext: &corev1.PodSecurityContext{
				FSGroup: int64Ptr(1000),
			},
		},
	}

	_, err = tenantClient.CoreV1().Pods(workspacesNamespace).Create(ctx, pod, metav1.CreateOptions{})
	return err
}

func (r *Reconciler) setFailed(ctx context.Context, ws *butlerv1alpha1.Workspace,
	reason, message string) (ctrl.Result, error) {
	ws.Status.Phase = butlerv1alpha1.WorkspacePhaseFailed
	ws.Status.ObservedGeneration = ws.Generation

	meta.SetStatusCondition(&ws.Status.Conditions, metav1.Condition{
		Type:               butlerv1alpha1.WorkspaceConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: ws.Generation,
	})

	if err := r.Status().Update(ctx, ws); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&butlerv1alpha1.Workspace{}).
		Named("workspace").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 5,
		}).
		Complete(r)
}

// --- Utility functions ---

func sanitizeLabel(email string) string {
	s := strings.ReplaceAll(email, "@", "_at_")
	s = strings.ReplaceAll(s, ".", "_")
	if len(s) > 63 {
		s = s[:63]
	}
	return s
}

func int32Ptr(i int32) *int32 {
	return &i
}

func int64Ptr(i int64) *int64 {
	return &i
}

// resolveRepositories merges spec.repository (single, legacy) and spec.repositories (multi)
// into a single list. If both are set, the single repo is prepended.
func resolveRepositories(ws *butlerv1alpha1.Workspace) []butlerv1alpha1.WorkspaceRepository {
	var repos []butlerv1alpha1.WorkspaceRepository
	if ws.Spec.Repository != nil {
		repos = append(repos, *ws.Spec.Repository)
	}
	repos = append(repos, ws.Spec.Repositories...)
	return repos
}

// hasGitCredentials returns true if any repository in the workspace has a secretRef.
func hasGitCredentials(ws *butlerv1alpha1.Workspace) bool {
	for _, repo := range resolveRepositories(ws) {
		if repo.SecretRef != nil {
			return true
		}
	}
	return false
}

// repoNameFromURL extracts a directory name from a git URL.
// "https://github.com/butlerdotdev/butler-api" → "butler-api"
// "https://github.com/butlerdotdev/butler-api.git" → "butler-api"
func repoNameFromURL(url string) string {
	// Strip trailing .git
	url = strings.TrimSuffix(url, ".git")
	// Strip trailing slash
	url = strings.TrimSuffix(url, "/")
	// Get last path segment
	idx := strings.LastIndex(url, "/")
	if idx >= 0 && idx < len(url)-1 {
		return url[idx+1:]
	}
	return url
}

// buildCloneScript generates a shell script that:
// 1. Installs git if missing
// 2. Clones each repository to its target directory
// 3. Generates a .code-workspace file if multiple repos exist
func buildCloneScript(repos []butlerv1alpha1.WorkspaceRepository) string {
	var sb strings.Builder

	// Install git if missing
	sb.WriteString(`set -e
if ! command -v git >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then apt-get update -qq && apt-get install -y -qq git >/dev/null 2>&1
  elif command -v apk >/dev/null 2>&1; then apk add --no-cache git >/dev/null 2>&1; fi
fi
`)

	isMulti := len(repos) > 1

	// Clone each repo
	for _, repo := range repos {
		name := repo.Name
		if name == "" {
			name = repoNameFromURL(repo.URL)
		}
		branch := repo.Branch
		if branch == "" {
			branch = "main"
		}

		var targetDir string
		if isMulti {
			// Multi-repo: each gets its own directory under /workspace/
			targetDir = fmt.Sprintf("/workspace/%s", name)
		} else {
			// Single repo: clone to /workspace/src/ for backwards compat
			targetDir = "/workspace/src"
		}

		cloneCmd := fmt.Sprintf("git clone --branch %s %s %s", branch, repo.URL, targetDir)
		if repo.SecretRef != nil {
			cloneCmd = fmt.Sprintf("GIT_SSH_COMMAND='ssh -i /git-credentials/ssh-privatekey -o StrictHostKeyChecking=no' %s", cloneCmd)
		}
		// Skip if already cloned (PVC persists across pod restarts)
		// Use || true so a failed clone (e.g. private repo without credentials) doesn't kill the init container
		sb.WriteString(fmt.Sprintf("if [ ! -d \"%s/.git\" ]; then %s || echo 'WARNING: failed to clone %s'; fi\n", targetDir, cloneCmd, repo.URL))
	}

	// Generate .code-workspace file for multi-repo setups
	if isMulti {
		sb.WriteString(`# Generate VS Code multi-root workspace file
cat > /workspace/workspace.code-workspace << 'WSEOF'
{
  "folders": [
`)
		for i, repo := range repos {
			name := repo.Name
			if name == "" {
				name = repoNameFromURL(repo.URL)
			}
			comma := ","
			if i == len(repos)-1 {
				comma = ""
			}
			sb.WriteString(fmt.Sprintf("    { \"path\": \"%s\" }%s\n", name, comma))
		}
		sb.WriteString(`  ],
  "settings": {}
}
WSEOF
`)
	}

	return sb.String()
}

// buildEntrypointScript generates the main container entrypoint that:
// 1. Creates a dev user with uid 1000
// 2. Installs sshd + neovim if not present
// 3. Clones neovim config to ~/.config/nvim if specified
// 4. Starts sshd on port 2222
func buildEntrypointScript(ws *butlerv1alpha1.Workspace) string {
	var sb strings.Builder

	sb.WriteString(`set -e
# Ensure a "dev" user exists with uid 1000
if ! id dev >/dev/null 2>&1; then
  EXISTING=$(getent passwd 1000 2>/dev/null | cut -d: -f1) || true
  if [ -n "$EXISTING" ] && [ "$EXISTING" != "dev" ]; then
    usermod -l dev "$EXISTING" 2>/dev/null || true
    usermod -d /home/dev -m dev 2>/dev/null || true
    groupmod -n dev "$EXISTING" 2>/dev/null || true
  else
    if command -v useradd >/dev/null 2>&1; then
      useradd -m -s /bin/bash -u 1000 dev 2>/dev/null || true
    elif command -v adduser >/dev/null 2>&1; then
      adduser -D -s /bin/bash -u 1000 dev 2>/dev/null || true
    fi
  fi
fi
DEV_HOME=$(getent passwd dev 2>/dev/null | cut -d: -f6) || true
[ -z "$DEV_HOME" ] && DEV_HOME="/home/dev"
mkdir -p "$DEV_HOME"
# Install packages
if command -v apt-get >/dev/null 2>&1; then
  apt-get update -qq
  command -v sshd >/dev/null 2>&1 || apt-get install -y -qq openssh-server >/dev/null 2>&1 || true
  command -v git  >/dev/null 2>&1 || apt-get install -y -qq git >/dev/null 2>&1 || true
  command -v curl >/dev/null 2>&1 || apt-get install -y -qq curl >/dev/null 2>&1 || true
  command -v unzip >/dev/null 2>&1 || apt-get install -y -qq unzip >/dev/null 2>&1 || true
  command -v cc >/dev/null 2>&1 || apt-get install -y -qq build-essential >/dev/null 2>&1 || true
elif command -v apk >/dev/null 2>&1; then
  command -v sshd >/dev/null 2>&1 || apk add --no-cache openssh-server >/dev/null 2>&1 || true
  command -v git  >/dev/null 2>&1 || apk add --no-cache git >/dev/null 2>&1 || true
  command -v curl >/dev/null 2>&1 || apk add --no-cache curl >/dev/null 2>&1 || true
  command -v unzip >/dev/null 2>&1 || apk add --no-cache unzip >/dev/null 2>&1 || true
  command -v cc >/dev/null 2>&1 || apk add --no-cache build-base >/dev/null 2>&1 || true
fi
# Install modern neovim from GitHub releases (package managers ship ancient versions)
if ! command -v nvim >/dev/null 2>&1; then
  NVIM_VER="v0.10.3"
  ARCH=$(uname -m)
  case "$ARCH" in
    x86_64)       NVIM_PKG="nvim-linux64";;
    aarch64|arm64) NVIM_PKG="nvim-linux64";;
    *) NVIM_PKG="";;
  esac
  if [ -n "$NVIM_PKG" ]; then
    curl -fsSL "https://github.com/neovim/neovim/releases/download/${NVIM_VER}/${NVIM_PKG}.tar.gz" | tar xzf - -C /opt 2>/dev/null || true
    if [ -x "/opt/${NVIM_PKG}/bin/nvim" ]; then
      ln -sf "/opt/${NVIM_PKG}/bin/nvim" /usr/local/bin/nvim
    fi
  fi
fi
`)

	// Setup neovim config if specified
	if ws.Spec.EditorConfig != nil {
		if ws.Spec.EditorConfig.NeovimConfigRepo != "" {
			// Clone full config repo to ~/.config/nvim
			sb.WriteString(fmt.Sprintf(`# Clone neovim config
NVIM_DIR="$DEV_HOME/.config/nvim"
if [ ! -d "$NVIM_DIR/.git" ]; then
  mkdir -p "$DEV_HOME/.config"
  git clone %s "$NVIM_DIR" || echo "WARNING: failed to clone neovim config"
  chown -R 1000:1000 "$DEV_HOME/.config" 2>/dev/null || true
fi
`, ws.Spec.EditorConfig.NeovimConfigRepo))
		} else if ws.Spec.EditorConfig.NeovimConfigArchive != "" {
			// Extract base64-encoded tar.gz archive of full nvim config directory
			sb.WriteString(fmt.Sprintf(`# Extract neovim config archive
NVIM_DIR="$DEV_HOME/.config/nvim"
if [ ! -d "$NVIM_DIR" ] || [ -z "$(ls -A "$NVIM_DIR" 2>/dev/null)" ]; then
  mkdir -p "$NVIM_DIR"
  echo '%s' | base64 -d | tar xzf - -C "$NVIM_DIR" 2>/dev/null || echo "WARNING: failed to extract neovim config archive"
  chown -R 1000:1000 "$DEV_HOME/.config" 2>/dev/null || true
fi
`, ws.Spec.EditorConfig.NeovimConfigArchive))
		} else if ws.Spec.EditorConfig.NeovimInitLua != "" {
			// Write inline init.lua
			sb.WriteString(fmt.Sprintf(`# Write inline neovim config
NVIM_DIR="$DEV_HOME/.config/nvim"
if [ ! -f "$NVIM_DIR/init.lua" ]; then
  mkdir -p "$NVIM_DIR"
  cat > "$NVIM_DIR/init.lua" << 'NVIMLUA'
%s
NVIMLUA
  chown -R 1000:1000 "$DEV_HOME/.config" 2>/dev/null || true
fi
`, ws.Spec.EditorConfig.NeovimInitLua))
		}
	}

	sb.WriteString(`# Setup SSH
mkdir -p /run/sshd "$DEV_HOME/.ssh"
if [ -f /tmp/ssh-keys/authorized_keys ]; then
  cp /tmp/ssh-keys/authorized_keys "$DEV_HOME/.ssh/authorized_keys"
  chmod 600 "$DEV_HOME/.ssh/authorized_keys"
  chown -R 1000:1000 "$DEV_HOME/.ssh" 2>/dev/null || true
fi
if command -v sshd >/dev/null 2>&1; then
  /usr/sbin/sshd -D -p 2222 -o AuthorizedKeysFile="$DEV_HOME/.ssh/authorized_keys" -o PasswordAuthentication=no &
fi
exec sleep infinity`)

	return sb.String()
}
