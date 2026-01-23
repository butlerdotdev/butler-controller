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

package team

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

var _ = Describe("Team Controller", func() {
	const (
		timeout  = time.Second * 10
		interval = time.Millisecond * 250
	)

	// Use a fresh context per test
	var testCtx context.Context

	BeforeEach(func() {
		testCtx = context.Background()
	})

	Context("When creating a Team", func() {
		It("Should create a namespace with the same name", func() {
			team := &butlerv1alpha1.Team{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-team-ns",
				},
				Spec: butlerv1alpha1.TeamSpec{
					DisplayName: "Test Team",
					Description: "A team for testing namespace creation",
				},
			}

			Expect(k8sClient.Create(testCtx, team)).Should(Succeed())

			// Verify Team is created
			teamLookupKey := types.NamespacedName{Name: team.Name}
			createdTeam := &butlerv1alpha1.Team{}
			Eventually(func() bool {
				err := k8sClient.Get(testCtx, teamLookupKey, createdTeam)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			for i := 0; i < 3; i++ {
				_, err := testReconciler.Reconcile(testCtx, reconcile.Request{
					NamespacedName: teamLookupKey,
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify namespace was created
			namespace := &corev1.Namespace{}
			Eventually(func() bool {
				err := k8sClient.Get(testCtx, types.NamespacedName{Name: team.Name}, namespace)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Verify labels
			Expect(namespace.Labels[butlerv1alpha1.LabelTeam]).To(Equal(team.Name))
			Expect(namespace.Labels[butlerv1alpha1.LabelManagedBy]).To(Equal("butler"))

			// Verify annotations
			Expect(namespace.Annotations[butlerv1alpha1.AnnotationDescription]).To(Equal(team.Spec.Description))

			// Cleanup
			Expect(k8sClient.Delete(testCtx, team)).Should(Succeed())
		})

		It("Should create RoleBindings for users", func() {
			team := &butlerv1alpha1.Team{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-team-users",
				},
				Spec: butlerv1alpha1.TeamSpec{
					DisplayName: "Test Team Users",
					Access: butlerv1alpha1.TeamAccess{
						Users: []butlerv1alpha1.TeamUser{
							{
								Name: "admin@example.com",
								Role: butlerv1alpha1.TeamRoleAdmin,
							},
							{
								Name: "member@example.com",
								Role: butlerv1alpha1.TeamRoleOperator,
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(testCtx, team)).Should(Succeed())

			teamLookupKey := types.NamespacedName{Name: team.Name}

			// Run reconciliation multiple times to get through all phases
			for i := 0; i < 3; i++ {
				_, err := testReconciler.Reconcile(testCtx, reconcile.Request{
					NamespacedName: teamLookupKey,
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify admin RoleBinding
			adminRB := &rbacv1.RoleBinding{}
			Eventually(func() bool {
				err := k8sClient.Get(testCtx, types.NamespacedName{
					Name:      "butler-team-user-admin-example-com",
					Namespace: team.Name,
				}, adminRB)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(adminRB.RoleRef.Name).To(Equal("cluster-admin"))
			Expect(adminRB.Subjects).To(HaveLen(1))
			Expect(adminRB.Subjects[0].Name).To(Equal("admin@example.com"))
			Expect(adminRB.Subjects[0].Kind).To(Equal(rbacv1.UserKind))

			// Verify member RoleBinding
			memberRB := &rbacv1.RoleBinding{}
			Eventually(func() bool {
				err := k8sClient.Get(testCtx, types.NamespacedName{
					Name:      "butler-team-user-member-example-com",
					Namespace: team.Name,
				}, memberRB)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(memberRB.RoleRef.Name).To(Equal("edit"))
			Expect(memberRB.Subjects).To(HaveLen(1))
			Expect(memberRB.Subjects[0].Name).To(Equal("member@example.com"))

			// Cleanup
			Expect(k8sClient.Delete(testCtx, team)).Should(Succeed())
		})

		It("Should create RoleBindings for groups", func() {
			team := &butlerv1alpha1.Team{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-team-groups",
				},
				Spec: butlerv1alpha1.TeamSpec{
					DisplayName: "Test Team Groups",
					Access: butlerv1alpha1.TeamAccess{
						Groups: []butlerv1alpha1.TeamGroup{
							{
								Name: "platform-engineers",
								Role: butlerv1alpha1.TeamRoleAdmin,
							},
							{
								Name: "CN=APP-K8S-Users,OU=Groups,DC=corp",
								Role: butlerv1alpha1.TeamRoleOperator,
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(testCtx, team)).Should(Succeed())

			teamLookupKey := types.NamespacedName{Name: team.Name}

			// Run reconciliation
			for i := 0; i < 3; i++ {
				_, err := testReconciler.Reconcile(testCtx, reconcile.Request{
					NamespacedName: teamLookupKey,
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify admin group RoleBinding
			adminRB := &rbacv1.RoleBinding{}
			Eventually(func() bool {
				err := k8sClient.Get(testCtx, types.NamespacedName{
					Name:      "butler-team-group-platform-engineers",
					Namespace: team.Name,
				}, adminRB)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(adminRB.RoleRef.Name).To(Equal("cluster-admin"))
			Expect(adminRB.Subjects[0].Kind).To(Equal(rbacv1.GroupKind))

			// Verify AD group RoleBinding (sanitized name)
			adRB := &rbacv1.RoleBinding{}
			Eventually(func() bool {
				err := k8sClient.Get(testCtx, types.NamespacedName{
					Name:      "butler-team-group-cn-app-k8s-users-ou-groups-dc-corp",
					Namespace: team.Name,
				}, adRB)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(adRB.Subjects[0].Name).To(Equal("CN=APP-K8S-Users,OU=Groups,DC=corp"))

			// Cleanup
			Expect(k8sClient.Delete(testCtx, team)).Should(Succeed())
		})

		It("Should set status to Ready when reconciliation completes", func() {
			team := &butlerv1alpha1.Team{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-team-status",
				},
				Spec: butlerv1alpha1.TeamSpec{
					DisplayName: "Test Team Status",
				},
			}

			Expect(k8sClient.Create(testCtx, team)).Should(Succeed())

			teamLookupKey := types.NamespacedName{Name: team.Name}

			// Run reconciliation until ready
			for i := 0; i < 5; i++ {
				_, _ = testReconciler.Reconcile(testCtx, reconcile.Request{
					NamespacedName: teamLookupKey,
				})
			}

			// Verify status
			updatedTeam := &butlerv1alpha1.Team{}
			Eventually(func() butlerv1alpha1.TeamPhase {
				_ = k8sClient.Get(testCtx, teamLookupKey, updatedTeam)
				return updatedTeam.Status.Phase
			}, timeout, interval).Should(Equal(butlerv1alpha1.TeamPhaseReady))

			// Verify conditions
			Expect(meta.IsStatusConditionTrue(
				updatedTeam.Status.Conditions,
				butlerv1alpha1.TeamConditionNamespaceReady,
			)).To(BeTrue())

			Expect(meta.IsStatusConditionTrue(
				updatedTeam.Status.Conditions,
				butlerv1alpha1.TeamConditionRBACReady,
			)).To(BeTrue())

			Expect(meta.IsStatusConditionTrue(
				updatedTeam.Status.Conditions,
				butlerv1alpha1.TeamConditionReady,
			)).To(BeTrue())

			// Verify namespace is set in status
			Expect(updatedTeam.Status.Namespace).To(Equal(team.Name))

			// Cleanup
			Expect(k8sClient.Delete(testCtx, team)).Should(Succeed())
		})

		It("Should add finalizer to Team", func() {
			team := &butlerv1alpha1.Team{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-team-finalizer",
				},
				Spec: butlerv1alpha1.TeamSpec{
					DisplayName: "Test Team Finalizer",
				},
			}

			Expect(k8sClient.Create(testCtx, team)).Should(Succeed())

			teamLookupKey := types.NamespacedName{Name: team.Name}

			// First reconcile adds finalizer
			_, err := testReconciler.Reconcile(testCtx, reconcile.Request{
				NamespacedName: teamLookupKey,
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify finalizer
			updatedTeam := &butlerv1alpha1.Team{}
			Eventually(func() bool {
				_ = k8sClient.Get(testCtx, teamLookupKey, updatedTeam)
				for _, f := range updatedTeam.Finalizers {
					if f == butlerv1alpha1.FinalizerTeam {
						return true
					}
				}
				return false
			}, timeout, interval).Should(BeTrue())

			// Cleanup
			Expect(k8sClient.Delete(testCtx, team)).Should(Succeed())
		})
	})

	Context("When deleting a Team", func() {
		It("Should delete the namespace when no TenantClusters exist", func() {
			team := &butlerv1alpha1.Team{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-team-delete",
				},
				Spec: butlerv1alpha1.TeamSpec{
					DisplayName: "Test Team Delete",
				},
			}

			Expect(k8sClient.Create(testCtx, team)).Should(Succeed())

			teamLookupKey := types.NamespacedName{Name: team.Name}

			// Run reconciliation to create namespace
			for i := 0; i < 3; i++ {
				_, _ = testReconciler.Reconcile(testCtx, reconcile.Request{
					NamespacedName: teamLookupKey,
				})
			}

			// Verify namespace exists and team is Ready
			namespace := &corev1.Namespace{}
			Eventually(func() bool {
				err := k8sClient.Get(testCtx, types.NamespacedName{Name: team.Name}, namespace)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			readyTeam := &butlerv1alpha1.Team{}
			Expect(k8sClient.Get(testCtx, teamLookupKey, readyTeam)).Should(Succeed())
			Expect(readyTeam.Status.Phase).To(Equal(butlerv1alpha1.TeamPhaseReady))

			// Delete team
			Expect(k8sClient.Delete(testCtx, team)).Should(Succeed())

			// Run deletion reconciliation
			_, _ = testReconciler.Reconcile(testCtx, reconcile.Request{
				NamespacedName: teamLookupKey,
			})

			// Verify team is in Terminating phase
			updatedTeam := &butlerv1alpha1.Team{}
			Expect(k8sClient.Get(testCtx, teamLookupKey, updatedTeam)).Should(Succeed())
			Expect(updatedTeam.Status.Phase).To(Equal(butlerv1alpha1.TeamPhaseTerminating))

			// Verify namespace deletion was initiated (has DeletionTimestamp)
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(testCtx, types.NamespacedName{Name: team.Name}, ns)).Should(Succeed())
			Expect(ns.DeletionTimestamp.IsZero()).To(BeFalse(), "Namespace should have DeletionTimestamp set")

			// Verify team still has finalizer (waiting for namespace deletion)
			Expect(updatedTeam.Finalizers).To(ContainElement(butlerv1alpha1.FinalizerTeam))

			// NOTE: In envtest, namespaces never fully delete (no namespace controller).
			// In a real cluster, once the namespace is gone, the next reconcile would
			// remove the Team finalizer and the Team would be deleted.
			// We've verified the controller correctly:
			// 1. Sets Team to Terminating
			// 2. Initiates namespace deletion
			// 3. Keeps finalizer until namespace is gone
		})

		It("Should block deletion when TenantClusters exist", func() {
			team := &butlerv1alpha1.Team{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-team-block-delete",
				},
				Spec: butlerv1alpha1.TeamSpec{
					DisplayName: "Test Team Block Delete",
				},
			}

			Expect(k8sClient.Create(testCtx, team)).Should(Succeed())

			teamLookupKey := types.NamespacedName{Name: team.Name}

			// Run reconciliation to create namespace
			for i := 0; i < 3; i++ {
				_, _ = testReconciler.Reconcile(testCtx, reconcile.Request{
					NamespacedName: teamLookupKey,
				})
			}

			// Create a TenantCluster in the namespace
			tc := &butlerv1alpha1.TenantCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "blocking-cluster",
					Namespace: team.Name,
				},
				Spec: butlerv1alpha1.TenantClusterSpec{
					KubernetesVersion: "v1.30.0",
					Workers: butlerv1alpha1.WorkersSpec{
						Replicas: 1,
					},
				},
			}
			Expect(k8sClient.Create(testCtx, tc)).Should(Succeed())

			// Delete team
			Expect(k8sClient.Delete(testCtx, team)).Should(Succeed())

			// Run deletion reconciliation
			_, _ = testReconciler.Reconcile(testCtx, reconcile.Request{
				NamespacedName: teamLookupKey,
			})

			// Verify team still exists (blocked by finalizer)
			updatedTeam := &butlerv1alpha1.Team{}
			Expect(k8sClient.Get(testCtx, teamLookupKey, updatedTeam)).Should(Succeed())
			Expect(updatedTeam.Status.Phase).To(Equal(butlerv1alpha1.TeamPhaseTerminating))

			// Verify DeletionBlocked condition
			cond := meta.FindStatusCondition(updatedTeam.Status.Conditions, butlerv1alpha1.TeamConditionReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Reason).To(Equal("DeletionBlocked"))

			// Cleanup: delete the TenantCluster first
			Expect(k8sClient.Delete(testCtx, tc)).Should(Succeed())

			// Now deletion should proceed
			for i := 0; i < 5; i++ {
				_, _ = testReconciler.Reconcile(testCtx, reconcile.Request{
					NamespacedName: teamLookupKey,
				})
				time.Sleep(100 * time.Millisecond)
			}
		})
	})

	Context("When updating a Team", func() {
		It("Should update RoleBindings when users change", func() {
			team := &butlerv1alpha1.Team{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-team-update-users",
				},
				Spec: butlerv1alpha1.TeamSpec{
					DisplayName: "Test Team Update Users",
					Access: butlerv1alpha1.TeamAccess{
						Users: []butlerv1alpha1.TeamUser{
							{
								Name: "original@example.com",
								Role: butlerv1alpha1.TeamRoleOperator,
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(testCtx, team)).Should(Succeed())

			teamLookupKey := types.NamespacedName{Name: team.Name}

			// Initial reconciliation
			for i := 0; i < 3; i++ {
				_, _ = testReconciler.Reconcile(testCtx, reconcile.Request{
					NamespacedName: teamLookupKey,
				})
			}

			// Verify original RoleBinding exists
			originalRB := &rbacv1.RoleBinding{}
			Eventually(func() bool {
				err := k8sClient.Get(testCtx, types.NamespacedName{
					Name:      "butler-team-user-original-example-com",
					Namespace: team.Name,
				}, originalRB)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			// Update team with new user
			updatedTeam := &butlerv1alpha1.Team{}
			Expect(k8sClient.Get(testCtx, teamLookupKey, updatedTeam)).Should(Succeed())

			updatedTeam.Spec.Access.Users = []butlerv1alpha1.TeamUser{
				{
					Name: "new@example.com",
					Role: butlerv1alpha1.TeamRoleAdmin,
				},
			}
			Expect(k8sClient.Update(testCtx, updatedTeam)).Should(Succeed())

			// Reconcile after update
			for i := 0; i < 3; i++ {
				_, _ = testReconciler.Reconcile(testCtx, reconcile.Request{
					NamespacedName: teamLookupKey,
				})
			}

			// Verify new RoleBinding exists
			newRB := &rbacv1.RoleBinding{}
			Eventually(func() bool {
				err := k8sClient.Get(testCtx, types.NamespacedName{
					Name:      "butler-team-user-new-example-com",
					Namespace: team.Name,
				}, newRB)
				return err == nil
			}, timeout, interval).Should(BeTrue())

			Expect(newRB.RoleRef.Name).To(Equal("cluster-admin"))

			// Verify old RoleBinding is deleted
			Eventually(func() bool {
				err := k8sClient.Get(testCtx, types.NamespacedName{
					Name:      "butler-team-user-original-example-com",
					Namespace: team.Name,
				}, &rbacv1.RoleBinding{})
				return apierrors.IsNotFound(err)
			}, timeout, interval).Should(BeTrue())

			// Cleanup
			Expect(k8sClient.Delete(testCtx, team)).Should(Succeed())
		})
	})
})

// Unit tests for helper functions (standard Go tests, not Ginkgo)
func TestSanitizeName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple email",
			input:    "user@example.com",
			expected: "user-example-com",
		},
		{
			name:     "AD group DN",
			input:    "CN=APP-K8S-Admins,OU=Groups,DC=corp",
			expected: "cn-app-k8s-admins-ou-groups-dc-corp",
		},
		{
			name:     "uppercase",
			input:    "ADMIN",
			expected: "admin",
		},
		{
			name:     "leading hyphen",
			input:    "-test",
			expected: "test",
		},
		{
			name:     "trailing hyphen",
			input:    "test-",
			expected: "test",
		},
		{
			name:     "long name truncation",
			input:    "this-is-a-very-long-name-that-exceeds-the-kubernetes-limit-of-sixty-three-characters",
			expected: "this-is-a-very-long-name-that-exceeds-the-kubernetes-limit-of-s",
		},
		{
			name:     "special characters",
			input:    "user+test/foo\\bar",
			expected: "user-test-foo-bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeName(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
