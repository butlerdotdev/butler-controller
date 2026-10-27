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

package tenantcluster

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

var _ = Describe("TenantCluster Controller", func() {
	const (
		timeout  = 10 * time.Second
		interval = 100 * time.Millisecond
	)

	var (
		butlerConfig *butlerv1alpha1.ButlerConfig
		defaultNS    *corev1.Namespace
	)

	BeforeEach(func() {
		// Create butler-system namespace for credentials
		butlerSystemNS := newNamespace("butler-system")
		err := k8sClient.Create(ctx, butlerSystemNS)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}

		// Create default namespace for non-team clusters
		defaultNS = newNamespace("butler-tenants")
		err = k8sClient.Create(ctx, defaultNS)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}

		// Create credentials Secret for ProviderConfig
		credSecret := newProviderCredentialsSecret("default-credentials", "butler-system")
		err = k8sClient.Create(ctx, credSecret)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}

		// Create default ProviderConfig so infrastructure reconciliation can proceed
		pc := newProviderConfig("default", "butler-system")
		err = k8sClient.Create(ctx, pc)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		// Clean up TenantClusters
		tcList := &butlerv1alpha1.TenantClusterList{}
		Expect(k8sClient.List(ctx, tcList)).To(Succeed())
		for _, tc := range tcList.Items {
			// Remove finalizer first to allow deletion
			tc.Finalizers = nil
			_ = k8sClient.Update(ctx, &tc)
			_ = k8sClient.Delete(ctx, &tc)
		}

		// Clean up Teams
		teamList := &butlerv1alpha1.TeamList{}
		Expect(k8sClient.List(ctx, teamList)).To(Succeed())
		for _, team := range teamList.Items {
			team.Finalizers = nil
			_ = k8sClient.Update(ctx, &team)
			_ = k8sClient.Delete(ctx, &team)
		}

		// Clean up ButlerConfig
		if butlerConfig != nil {
			_ = k8sClient.Delete(ctx, butlerConfig)
			butlerConfig = nil
		}

		// Clean up ProviderConfigs
		pcList := &butlerv1alpha1.ProviderConfigList{}
		Expect(k8sClient.List(ctx, pcList)).To(Succeed())
		for _, pc := range pcList.Items {
			_ = k8sClient.Delete(ctx, &pc)
		}

		// Clean up namespaces created by controller (tenant namespaces)
		nsList := &corev1.NamespaceList{}
		Expect(k8sClient.List(ctx, nsList)).To(Succeed())
		for _, ns := range nsList.Items {
			if ns.Labels[butlerv1alpha1.LabelManagedBy] == "butler" &&
				ns.Labels[butlerv1alpha1.LabelTenant] != "" {
				_ = k8sClient.Delete(ctx, &ns)
			}
		}

		// Wait for cleanup
		time.Sleep(200 * time.Millisecond)
	})

	Describe("Enforced Mode Validation", func() {
		BeforeEach(func() {
			butlerConfig = newButlerConfig(butlerv1alpha1.MultiTenancyModeEnforced, "butler-tenants")
			Expect(k8sClient.Create(ctx, butlerConfig)).To(Succeed())
		})

		It("should fail when teamRef is missing", func() {
			tc := newTenantCluster("no-team-cluster", "butler-tenants", nil)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			// Wait for reconciliation and failure
			Eventually(func() butlerv1alpha1.TenantClusterPhase {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return ""
				}
				return tc.Status.Phase
			}, timeout, interval).Should(Equal(butlerv1alpha1.TenantClusterPhaseFailed))

			// Verify condition message
			cond := meta.FindStatusCondition(tc.Status.Conditions, butlerv1alpha1.TenantClusterConditionReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Message).To(ContainSubstring("teamRef is required"))
		})

		It("should fail when team does not exist", func() {
			teamName := "nonexistent-team"
			tc := newTenantCluster("orphan-cluster", "butler-tenants", &teamName)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			Eventually(func() butlerv1alpha1.TenantClusterPhase {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return ""
				}
				return tc.Status.Phase
			}, timeout, interval).Should(Equal(butlerv1alpha1.TenantClusterPhaseFailed))

			cond := meta.FindStatusCondition(tc.Status.Conditions, butlerv1alpha1.TenantClusterConditionReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Message).To(ContainSubstring("not found"))
		})

		It("should fail when namespace doesn't match team", func() {
			// Create team
			team := newTeam("team-alpha")
			Expect(k8sClient.Create(ctx, team)).To(Succeed())

			// Mark team ready with its namespace
			Eventually(func() error {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: team.Name}, team)
				if err != nil {
					return err
				}
				markTeamReady(team)
				return k8sClient.Status().Update(ctx, team)
			}, timeout, interval).Should(Succeed())

			// Create TenantCluster in wrong namespace
			teamName := team.Name
			tc := newTenantCluster("wrong-ns-cluster", "butler-tenants", &teamName)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			Eventually(func() butlerv1alpha1.TenantClusterPhase {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return ""
				}
				return tc.Status.Phase
			}, timeout, interval).Should(Equal(butlerv1alpha1.TenantClusterPhaseFailed))

			cond := meta.FindStatusCondition(tc.Status.Conditions, butlerv1alpha1.TenantClusterConditionReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Message).To(ContainSubstring("must be in team namespace"))
		})

		It("should succeed when team exists and namespace matches", func() {
			// Create team namespace first
			teamNS := newNamespace("team-beta")
			Expect(k8sClient.Create(ctx, teamNS)).To(Succeed())

			// Create team
			team := newTeam("team-beta")
			Expect(k8sClient.Create(ctx, team)).To(Succeed())

			// Mark team ready
			Eventually(func() error {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: team.Name}, team)
				if err != nil {
					return err
				}
				markTeamReady(team)
				return k8sClient.Status().Update(ctx, team)
			}, timeout, interval).Should(Succeed())

			// Create TenantCluster in correct namespace
			teamName := team.Name
			tc := newTenantCluster("correct-cluster", "team-beta", &teamName)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			// Validation should pass and tenant namespace should be created
			// Note: Phase may reach Failed because CAPI CRDs aren't installed in envtest,
			// but we verify validation passed by checking namespace creation
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return false
				}
				// Check that tenant namespace was set (proves validation passed)
				return tc.Status.TenantNamespace != ""
			}, timeout, interval).Should(BeTrue())

			// Verify the tenant namespace exists
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: tc.Status.TenantNamespace}, ns)).To(Succeed())

			// Clean up team namespace
			_ = k8sClient.Delete(ctx, teamNS)
		})
	})

	Describe("Optional Mode Validation", func() {
		BeforeEach(func() {
			butlerConfig = newButlerConfig(butlerv1alpha1.MultiTenancyModeOptional, "butler-tenants")
			Expect(k8sClient.Create(ctx, butlerConfig)).To(Succeed())
		})

		It("should succeed without teamRef in default namespace", func() {
			tc := newTenantCluster("standalone-cluster", "butler-tenants", nil)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			// Validation should pass and tenant namespace should be created
			// Note: Phase will be Failed because CAPI CRDs aren't installed in envtest,
			// but we verify validation passed by checking namespace creation
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return false
				}
				// Check that tenant namespace was set (proves validation passed)
				return tc.Status.TenantNamespace != ""
			}, timeout, interval).Should(BeTrue())

			// Verify the tenant namespace exists
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: tc.Status.TenantNamespace}, ns)).To(Succeed())
		})

		It("should fail without teamRef in non-default namespace", func() {
			// Create non-default namespace
			wrongNS := newNamespace("wrong-namespace")
			Expect(k8sClient.Create(ctx, wrongNS)).To(Succeed())

			tc := newTenantCluster("wrong-location", "wrong-namespace", nil)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			Eventually(func() butlerv1alpha1.TenantClusterPhase {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return ""
				}
				return tc.Status.Phase
			}, timeout, interval).Should(Equal(butlerv1alpha1.TenantClusterPhaseFailed))

			cond := meta.FindStatusCondition(tc.Status.Conditions, butlerv1alpha1.TenantClusterConditionReady)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Message).To(ContainSubstring("must be in default namespace"))

			// Clean up
			_ = k8sClient.Delete(ctx, wrongNS)
		})
	})

	Describe("Disabled Mode Validation", func() {
		BeforeEach(func() {
			butlerConfig = newButlerConfig(butlerv1alpha1.MultiTenancyModeDisabled, "butler-tenants")
			Expect(k8sClient.Create(ctx, butlerConfig)).To(Succeed())
		})

		It("should succeed in default namespace", func() {
			tc := newTenantCluster("simple-cluster", "butler-tenants", nil)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			// Validation should pass and tenant namespace should be created
			// Note: Phase will be Failed because CAPI CRDs aren't installed in envtest,
			// but we verify validation passed by checking namespace creation
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return false
				}
				// Check that tenant namespace was set (proves validation passed)
				return tc.Status.TenantNamespace != ""
			}, timeout, interval).Should(BeTrue())

			// Verify the tenant namespace exists
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: tc.Status.TenantNamespace}, ns)).To(Succeed())
		})

		It("should fail in non-default namespace", func() {
			otherNS := newNamespace("other-namespace")
			Expect(k8sClient.Create(ctx, otherNS)).To(Succeed())

			tc := newTenantCluster("misplaced-cluster", "other-namespace", nil)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			Eventually(func() butlerv1alpha1.TenantClusterPhase {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return ""
				}
				return tc.Status.Phase
			}, timeout, interval).Should(Equal(butlerv1alpha1.TenantClusterPhaseFailed))

			// Clean up
			_ = k8sClient.Delete(ctx, otherNS)
		})
	})

	Describe("Tenant Namespace Creation", func() {
		BeforeEach(func() {
			butlerConfig = newButlerConfig(butlerv1alpha1.MultiTenancyModeDisabled, "butler-tenants")
			Expect(k8sClient.Create(ctx, butlerConfig)).To(Succeed())
		})

		It("should create tenant namespace with correct labels", func() {
			tc := newTenantCluster("labeled-cluster", "butler-tenants", nil)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			// Wait for status to have tenant namespace
			var tenantNS string
			Eventually(func() string {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return ""
				}
				tenantNS = tc.Status.TenantNamespace
				return tenantNS
			}, timeout, interval).ShouldNot(BeEmpty())

			// Verify namespace exists with correct labels
			ns := &corev1.Namespace{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Name: tenantNS}, ns)
			}, timeout, interval).Should(Succeed())

			Expect(ns.Labels).To(HaveKeyWithValue(butlerv1alpha1.LabelManagedBy, "butler"))
			Expect(ns.Labels).To(HaveKeyWithValue(butlerv1alpha1.LabelTenant, tc.Name))
			Expect(ns.Labels).To(HaveKeyWithValue(butlerv1alpha1.LabelSourceNamespace, tc.Namespace))
			Expect(ns.Labels).To(HaveKeyWithValue(butlerv1alpha1.LabelSourceName, tc.Name))
		})

		It("should generate unique namespace name using UID", func() {
			tc := newTenantCluster("unique-ns-cluster", "butler-tenants", nil)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			Eventually(func() string {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return ""
				}
				return tc.Status.TenantNamespace
			}, timeout, interval).ShouldNot(BeEmpty())

			// Namespace name should be {name}-{uid[:8]}
			Expect(tc.Status.TenantNamespace).To(HavePrefix("unique-ns-cluster-"))
			Expect(len(tc.Status.TenantNamespace)).To(BeNumerically(">", len("unique-ns-cluster-")))
		})
	})

	Describe("Team RoleBinding", func() {
		BeforeEach(func() {
			butlerConfig = newButlerConfig(butlerv1alpha1.MultiTenancyModeEnforced, "butler-tenants")
			Expect(k8sClient.Create(ctx, butlerConfig)).To(Succeed())
		})

		It("should create per-role RoleBindings in tenant namespace for team access", func() {
			// Create team namespace
			teamNS := newNamespace("team-gamma")
			Expect(k8sClient.Create(ctx, teamNS)).To(Succeed())

			// Create team with users
			team := &butlerv1alpha1.Team{
				ObjectMeta: metav1.ObjectMeta{
					Name: "team-gamma",
				},
				Spec: butlerv1alpha1.TeamSpec{
					DisplayName: "Team Gamma",
					Access: butlerv1alpha1.TeamAccess{
						Users: []butlerv1alpha1.TeamUser{
							{Name: "alice@example.com", Role: butlerv1alpha1.TeamRoleAdmin},
							{Name: "bob@example.com", Role: butlerv1alpha1.TeamRoleOperator},
							{Name: "carol@example.com", Role: butlerv1alpha1.TeamRoleViewer},
						},
						Groups: []butlerv1alpha1.TeamGroup{
							{Name: "developers", Role: butlerv1alpha1.TeamRoleOperator},
							{Name: "auditors", Role: butlerv1alpha1.TeamRoleViewer},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, team)).To(Succeed())

			// Mark team ready
			Eventually(func() error {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: team.Name}, team)
				if err != nil {
					return err
				}
				markTeamReady(team)
				return k8sClient.Status().Update(ctx, team)
			}, timeout, interval).Should(Succeed())

			// Create TenantCluster
			teamName := team.Name
			tc := newTenantCluster("gamma-cluster", "team-gamma", &teamName)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			// Wait for tenant namespace to be created
			var tenantNS string
			Eventually(func() string {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return ""
				}
				tenantNS = tc.Status.TenantNamespace
				return tenantNS
			}, timeout, interval).ShouldNot(BeEmpty())

			user := func(n string) rbacv1.Subject {
				return rbacv1.Subject{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: n}
			}
			group := func(n string) rbacv1.Subject {
				return rbacv1.Subject{Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: n}
			}
			expectBinding := func(rbName, clusterRole string, subjects ...rbacv1.Subject) {
				rb := &rbacv1.RoleBinding{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: rbName, Namespace: tenantNS}, rb)
				}, timeout, interval).Should(Succeed())
				Expect(rb.RoleRef.Kind).To(Equal("ClusterRole"))
				Expect(rb.RoleRef.Name).To(Equal(clusterRole))
				Expect(rb.Subjects).To(ConsistOf(subjects))
				Expect(rb.Labels).To(HaveKeyWithValue(butlerv1alpha1.LabelManagedBy, "butler"))
				Expect(rb.Labels).To(HaveKeyWithValue(butlerv1alpha1.LabelTeam, "team-gamma"))
				Expect(rb.Labels).To(HaveKeyWithValue(butlerv1alpha1.LabelTenant, tc.Name))
			}

			expectBinding("butler-team-team-gamma-admin-access", "admin", user("alice@example.com"))
			expectBinding("butler-team-team-gamma-operator-access", "edit",
				user("bob@example.com"), group("developers"))
			expectBinding("butler-team-team-gamma-viewer-access", "view",
				user("carol@example.com"), group("auditors"))

			// The legacy all-members admin binding must not exist.
			err := k8sClient.Get(ctx, types.NamespacedName{
				Name: "butler-team-team-gamma-access", Namespace: tenantNS,
			}, &rbacv1.RoleBinding{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			// Only admins hold ClusterRole admin (which can read the admin kubeconfig Secret).
			rbList := &rbacv1.RoleBindingList{}
			Expect(k8sClient.List(ctx, rbList, client.InNamespace(tenantNS))).To(Succeed())
			for _, rb := range rbList.Items {
				if rb.RoleRef.Name == "admin" || rb.RoleRef.Name == "cluster-admin" {
					Expect(rb.Subjects).To(ConsistOf(user("alice@example.com")), "binding %s", rb.Name)
				}
			}

			// Clean up
			_ = k8sClient.Delete(ctx, teamNS)
		})

		It("should delete the legacy binding and empty per-role bindings", func() {
			teamNS := newNamespace("team-delta")
			Expect(k8sClient.Create(ctx, teamNS)).To(Succeed())

			team := &butlerv1alpha1.Team{
				ObjectMeta: metav1.ObjectMeta{Name: "team-delta"},
				Spec: butlerv1alpha1.TeamSpec{
					DisplayName: "Team Delta",
					Access: butlerv1alpha1.TeamAccess{
						Users: []butlerv1alpha1.TeamUser{
							{Name: "dana@example.com", Role: butlerv1alpha1.TeamRoleAdmin},
							{Name: "vic@example.com", Role: butlerv1alpha1.TeamRoleViewer},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, team)).To(Succeed())

			// Exercise the namespace reconcile directly with a non-cached client so
			// the test is deterministic regardless of the running manager.
			tenantNS := newNamespace("delta-tenant-ns")
			Expect(k8sClient.Create(ctx, tenantNS)).To(Succeed())
			teamName := team.Name
			tc := newTenantCluster("delta-cluster", "team-delta", &teamName)
			r := &Reconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			// Pre-existing legacy binding that granted admin to every member.
			legacy := &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "butler-team-team-delta-access",
					Namespace: tenantNS.Name,
					Labels: map[string]string{
						butlerv1alpha1.LabelManagedBy: "butler",
						butlerv1alpha1.LabelTeam:      "team-delta",
						butlerv1alpha1.LabelTenant:    tc.Name,
					},
				},
				RoleRef: rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "admin"},
				Subjects: []rbacv1.Subject{
					{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: "dana@example.com"},
					{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: "vic@example.com"},
				},
			}
			Expect(k8sClient.Create(ctx, legacy)).To(Succeed())

			Expect(r.reconcileTeamRoleBinding(ctx, tc, tenantNS.Name)).To(Succeed())

			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{
				Name: "butler-team-team-delta-access", Namespace: tenantNS.Name,
			}, &rbacv1.RoleBinding{}))).To(BeTrue())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{
				Name: "butler-team-team-delta-operator-access", Namespace: tenantNS.Name,
			}, &rbacv1.RoleBinding{}))).To(BeTrue(), "no operator subjects -> no operator binding")

			viewerRB := &rbacv1.RoleBinding{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: "butler-team-team-delta-viewer-access", Namespace: tenantNS.Name,
			}, viewerRB)).To(Succeed())
			Expect(viewerRB.RoleRef.Name).To(Equal("view"))

			// Idempotent.
			Expect(r.reconcileTeamRoleBinding(ctx, tc, tenantNS.Name)).To(Succeed())

			// Promote the viewer to operator: viewer binding is removed, operator created.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: team.Name}, team)).To(Succeed())
			team.Spec.Access.Users[1].Role = butlerv1alpha1.TeamRoleOperator
			Expect(k8sClient.Update(ctx, team)).To(Succeed())

			Expect(r.reconcileTeamRoleBinding(ctx, tc, tenantNS.Name)).To(Succeed())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{
				Name: "butler-team-team-delta-viewer-access", Namespace: tenantNS.Name,
			}, &rbacv1.RoleBinding{}))).To(BeTrue())
			opRB := &rbacv1.RoleBinding{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: "butler-team-team-delta-operator-access", Namespace: tenantNS.Name,
			}, opRB)).To(Succeed())
			Expect(opRB.RoleRef.Name).To(Equal("edit"))
			Expect(opRB.Subjects).To(ConsistOf(rbacv1.Subject{
				Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: "vic@example.com",
			}))

			// A per-role binding with a drifted roleRef is recreated.
			adminRB := &rbacv1.RoleBinding{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: "butler-team-team-delta-admin-access", Namespace: tenantNS.Name,
			}, adminRB)).To(Succeed())
			Expect(k8sClient.Delete(ctx, adminRB)).To(Succeed())
			drifted := &rbacv1.RoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "butler-team-team-delta-admin-access", Namespace: tenantNS.Name},
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cluster-admin"},
				Subjects:   adminRB.Subjects,
			}
			Expect(k8sClient.Create(ctx, drifted)).To(Succeed())
			Expect(r.reconcileTeamRoleBinding(ctx, tc, tenantNS.Name)).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name: "butler-team-team-delta-admin-access", Namespace: tenantNS.Name,
			}, adminRB)).To(Succeed())
			Expect(adminRB.RoleRef.Name).To(Equal("admin"))

			_ = k8sClient.Delete(ctx, tenantNS)
			_ = k8sClient.Delete(ctx, teamNS)
		})
	})

	Describe("Finalizer and Deletion", func() {
		BeforeEach(func() {
			butlerConfig = newButlerConfig(butlerv1alpha1.MultiTenancyModeDisabled, "butler-tenants")
			Expect(k8sClient.Create(ctx, butlerConfig)).To(Succeed())
		})

		It("should add finalizer on creation", func() {
			tc := newTenantCluster("finalizer-test", "butler-tenants", nil)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			Eventually(func() []string {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return nil
				}
				return tc.Finalizers
			}, timeout, interval).Should(ContainElement(butlerv1alpha1.FinalizerTenantCluster))
		})

		It("should delete tenant namespace on TenantCluster deletion", func() {
			tc := newTenantCluster("delete-test", "butler-tenants", nil)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			// Wait for tenant namespace
			var tenantNS string
			Eventually(func() string {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return ""
				}
				tenantNS = tc.Status.TenantNamespace
				return tenantNS
			}, timeout, interval).ShouldNot(BeEmpty())

			// Verify namespace exists
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: tenantNS}, ns)).To(Succeed())

			// Delete TenantCluster
			Expect(k8sClient.Delete(ctx, tc)).To(Succeed())

			// Wait for phase to become Deleting
			Eventually(func() butlerv1alpha1.TenantClusterPhase {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return ""
				}
				return tc.Status.Phase
			}, timeout, interval).Should(Equal(butlerv1alpha1.TenantClusterPhaseDeleting))

			// Tenant namespace should be terminating (envtest doesn't fully delete namespaces)
			Eventually(func() corev1.NamespacePhase {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: tenantNS}, ns)
				if apierrors.IsNotFound(err) {
					return corev1.NamespaceTerminating // Treat NotFound as success
				}
				if err != nil {
					return ""
				}
				return ns.Status.Phase
			}, timeout, interval).Should(Equal(corev1.NamespaceTerminating))

			// Note: In envtest, namespace deletion doesn't complete, so the TenantCluster
			// won't be fully deleted. In a real cluster, it would be removed once
			// the namespace is gone. Force cleanup for test.
			tc.Finalizers = nil
			_ = k8sClient.Update(ctx, tc)
		})
	})

	Describe("Default ButlerConfig Behavior", func() {
		// No ButlerConfig created - should use defaults

		It("should use default disabled mode when no ButlerConfig exists", func() {
			tc := newTenantCluster("no-config-cluster", "butler-tenants", nil)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			// Should succeed with default Disabled mode
			// Validation should pass and tenant namespace should be created
			// Note: Phase will be Failed because CAPI CRDs aren't installed in envtest,
			// but we verify validation passed by checking namespace creation
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      tc.Name,
					Namespace: tc.Namespace,
				}, tc)
				if err != nil {
					return false
				}
				// Check that tenant namespace was set (proves validation passed)
				return tc.Status.TenantNamespace != ""
			}, timeout, interval).Should(BeTrue())

			// Verify the tenant namespace exists
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: tc.Status.TenantNamespace}, ns)).To(Succeed())
		})
	})
})
