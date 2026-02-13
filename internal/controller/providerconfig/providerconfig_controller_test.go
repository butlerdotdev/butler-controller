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

package providerconfig

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

var _ = Describe("ProviderConfig Controller", func() {
	const (
		timeout  = 10 * time.Second
		interval = 100 * time.Millisecond
	)

	Describe("Valid Credentials (Harvester)", func() {
		const (
			namespaceName = "test-provider-harvester"
			secretName    = "harvester-creds"
			pcName        = "harvester-valid"
		)

		BeforeEach(func() {
			ns := newNamespace(namespaceName)
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		})

		AfterEach(func() {
			// Clean up ProviderConfig (remove finalizer first to avoid blocking)
			pc := &butlerv1alpha1.ProviderConfig{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc); err == nil {
				controllerutil.RemoveFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig)
				_ = k8sClient.Update(ctx, pc)
				_ = k8sClient.Delete(ctx, pc)
			}

			ns := newNamespace(namespaceName)
			_ = k8sClient.Delete(ctx, ns)
			time.Sleep(200 * time.Millisecond)
		})

		It("should set CredentialsValid=True and status.validated=true when secret has correct keys", func() {
			// Create Secret with the required "kubeconfig" key for Harvester
			secret := newSecret(secretName, namespaceName, map[string][]byte{
				"kubeconfig": []byte("apiVersion: v1\nkind: Config"),
			})
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())

			// Create ProviderConfig referencing the secret
			pc := newProviderConfig(pcName, namespaceName, butlerv1alpha1.ProviderTypeHarvester, secretName)
			Expect(k8sClient.Create(ctx, pc)).To(Succeed())

			// Eventually: condition CredentialsValid=True
			Eventually(func(g Gomega) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc)
				g.Expect(err).NotTo(HaveOccurred())

				cond := meta.FindStatusCondition(pc.Status.Conditions, ConditionCredentialsValid)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
				g.Expect(cond.Reason).To(Equal(butlerv1alpha1.ReasonReady))

				g.Expect(pc.Status.Validated).To(BeTrue())
				g.Expect(pc.Status.Ready).To(BeTrue())
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Missing Secret", func() {
		const (
			namespaceName = "test-provider-missing-secret"
			pcName        = "pc-missing-secret"
		)

		BeforeEach(func() {
			ns := newNamespace(namespaceName)
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		})

		AfterEach(func() {
			pc := &butlerv1alpha1.ProviderConfig{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc); err == nil {
				controllerutil.RemoveFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig)
				_ = k8sClient.Update(ctx, pc)
				_ = k8sClient.Delete(ctx, pc)
			}

			ns := newNamespace(namespaceName)
			_ = k8sClient.Delete(ctx, ns)
			time.Sleep(200 * time.Millisecond)
		})

		It("should set CredentialsValid=False when the referenced secret does not exist", func() {
			// Create ProviderConfig referencing a non-existent secret
			pc := newProviderConfig(pcName, namespaceName, butlerv1alpha1.ProviderTypeHarvester, "nonexistent-secret")
			Expect(k8sClient.Create(ctx, pc)).To(Succeed())

			// Eventually: condition CredentialsValid=False
			Eventually(func(g Gomega) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc)
				g.Expect(err).NotTo(HaveOccurred())

				cond := meta.FindStatusCondition(pc.Status.Conditions, ConditionCredentialsValid)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(cond.Reason).To(Equal(butlerv1alpha1.ReasonCredentialsInvalid))
				g.Expect(cond.Message).To(ContainSubstring("not found"))

				g.Expect(pc.Status.Validated).To(BeFalse())
				g.Expect(pc.Status.Ready).To(BeFalse())
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Missing Secret Keys (Nutanix)", func() {
		const (
			namespaceName = "test-provider-nutanix-keys"
			secretName    = "nutanix-creds-partial"
			pcName        = "pc-nutanix-missing-keys"
		)

		BeforeEach(func() {
			ns := newNamespace(namespaceName)
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		})

		AfterEach(func() {
			pc := &butlerv1alpha1.ProviderConfig{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc); err == nil {
				controllerutil.RemoveFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig)
				_ = k8sClient.Update(ctx, pc)
				_ = k8sClient.Delete(ctx, pc)
			}

			ns := newNamespace(namespaceName)
			_ = k8sClient.Delete(ctx, ns)
			time.Sleep(200 * time.Millisecond)
		})

		It("should set CredentialsValid=False with a message about missing keys", func() {
			// Create Secret with only "username" (missing "password")
			secret := newSecret(secretName, namespaceName, map[string][]byte{
				"username": []byte("admin"),
			})
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())

			// Create ProviderConfig with provider=nutanix
			pc := newProviderConfig(pcName, namespaceName, butlerv1alpha1.ProviderTypeNutanix, secretName)
			Expect(k8sClient.Create(ctx, pc)).To(Succeed())

			// Eventually: condition CredentialsValid=False with message about missing keys
			Eventually(func(g Gomega) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc)
				g.Expect(err).NotTo(HaveOccurred())

				cond := meta.FindStatusCondition(pc.Status.Conditions, ConditionCredentialsValid)
				g.Expect(cond).NotTo(BeNil())
				g.Expect(cond.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(cond.Reason).To(Equal(butlerv1alpha1.ReasonCredentialsInvalid))
				g.Expect(cond.Message).To(ContainSubstring("missing required keys"))
				g.Expect(cond.Message).To(ContainSubstring("password"))

				g.Expect(pc.Status.Validated).To(BeFalse())
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Team Scope Validation", func() {
		const (
			namespaceName = "test-provider-team-scope"
			secretName    = "team-scope-creds"
			pcName        = "pc-team-scope"
			teamName      = "test-team"
		)

		BeforeEach(func() {
			ns := newNamespace(namespaceName)
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())

			// Create the referenced Team
			team := newTeam(teamName)
			Expect(k8sClient.Create(ctx, team)).To(Succeed())
		})

		AfterEach(func() {
			pc := &butlerv1alpha1.ProviderConfig{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc); err == nil {
				controllerutil.RemoveFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig)
				_ = k8sClient.Update(ctx, pc)
				_ = k8sClient.Delete(ctx, pc)
			}

			team := &butlerv1alpha1.Team{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: teamName}, team); err == nil {
				_ = k8sClient.Delete(ctx, team)
			}

			ns := newNamespace(namespaceName)
			_ = k8sClient.Delete(ctx, ns)
			time.Sleep(200 * time.Millisecond)
		})

		It("should validate successfully when scope.type=team and teamRef points to an existing team", func() {
			// Create Secret with valid credentials
			secret := newSecret(secretName, namespaceName, map[string][]byte{
				"kubeconfig": []byte("apiVersion: v1\nkind: Config"),
			})
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())

			// Create ProviderConfig with team scope
			pc := newProviderConfig(pcName, namespaceName, butlerv1alpha1.ProviderTypeHarvester, secretName)
			pc.Spec.Scope = &butlerv1alpha1.ProviderConfigScope{
				Type: butlerv1alpha1.ProviderConfigScopeTeam,
				TeamRef: &butlerv1alpha1.LocalObjectReference{
					Name: teamName,
				},
			}
			Expect(k8sClient.Create(ctx, pc)).To(Succeed())

			// Eventually: credentials are valid and no scope error condition is set
			Eventually(func(g Gomega) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc)
				g.Expect(err).NotTo(HaveOccurred())

				credsCond := meta.FindStatusCondition(pc.Status.Conditions, ConditionCredentialsValid)
				g.Expect(credsCond).NotTo(BeNil())
				g.Expect(credsCond.Status).To(Equal(metav1.ConditionTrue))

				// The Ready condition should not be set to False with ValidationFailed
				readyCond := meta.FindStatusCondition(pc.Status.Conditions, butlerv1alpha1.ConditionTypeReady)
				if readyCond != nil {
					g.Expect(readyCond.Reason).NotTo(Equal(butlerv1alpha1.ReasonValidationFailed))
				}

				g.Expect(pc.Status.Validated).To(BeTrue())
				g.Expect(pc.Status.Ready).To(BeTrue())
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Team Scope Missing TeamRef", func() {
		const (
			namespaceName = "test-provider-no-teamref"
			secretName    = "no-teamref-creds"
			pcName        = "pc-no-teamref"
		)

		BeforeEach(func() {
			ns := newNamespace(namespaceName)
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		})

		AfterEach(func() {
			pc := &butlerv1alpha1.ProviderConfig{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc); err == nil {
				controllerutil.RemoveFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig)
				_ = k8sClient.Update(ctx, pc)
				_ = k8sClient.Delete(ctx, pc)
			}

			ns := newNamespace(namespaceName)
			_ = k8sClient.Delete(ctx, ns)
			time.Sleep(200 * time.Millisecond)
		})

		It("should set Ready=False when scope.type=team but teamRef is not set", func() {
			// Create Secret with valid credentials so credential validation passes
			secret := newSecret(secretName, namespaceName, map[string][]byte{
				"kubeconfig": []byte("apiVersion: v1\nkind: Config"),
			})
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())

			// Create ProviderConfig with scope.type=team but no teamRef
			pc := newProviderConfig(pcName, namespaceName, butlerv1alpha1.ProviderTypeHarvester, secretName)
			pc.Spec.Scope = &butlerv1alpha1.ProviderConfigScope{
				Type: butlerv1alpha1.ProviderConfigScopeTeam,
				// TeamRef intentionally nil
			}
			Expect(k8sClient.Create(ctx, pc)).To(Succeed())

			// Eventually: condition Ready=False with message about teamRef
			Eventually(func(g Gomega) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc)
				g.Expect(err).NotTo(HaveOccurred())

				readyCond := meta.FindStatusCondition(pc.Status.Conditions, butlerv1alpha1.ConditionTypeReady)
				g.Expect(readyCond).NotTo(BeNil())
				g.Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(readyCond.Reason).To(Equal(butlerv1alpha1.ReasonValidationFailed))
				g.Expect(readyCond.Message).To(ContainSubstring("teamRef"))
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Deletion Protection", func() {
		const (
			namespaceName = "test-provider-deletion"
			secretName    = "deletion-creds"
			pcName        = "pc-deletion-test"
			tcName        = "tc-referencing-pc"
		)

		BeforeEach(func() {
			ns := newNamespace(namespaceName)
			Expect(k8sClient.Create(ctx, ns)).To(Succeed())
		})

		AfterEach(func() {
			// Clean up TenantCluster first
			tc := &butlerv1alpha1.TenantCluster{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: tcName, Namespace: namespaceName}, tc); err == nil {
				_ = k8sClient.Delete(ctx, tc)
			}

			// Clean up ProviderConfig (remove finalizer to allow deletion)
			pc := &butlerv1alpha1.ProviderConfig{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc); err == nil {
				controllerutil.RemoveFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig)
				_ = k8sClient.Update(ctx, pc)
				_ = k8sClient.Delete(ctx, pc)
			}

			ns := newNamespace(namespaceName)
			_ = k8sClient.Delete(ctx, ns)
			time.Sleep(200 * time.Millisecond)
		})

		It("should add the finalizer to a new ProviderConfig", func() {
			// Create Secret
			secret := newSecret(secretName, namespaceName, map[string][]byte{
				"kubeconfig": []byte("apiVersion: v1\nkind: Config"),
			})
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())

			// Create ProviderConfig
			pc := newProviderConfig(pcName, namespaceName, butlerv1alpha1.ProviderTypeHarvester, secretName)
			Expect(k8sClient.Create(ctx, pc)).To(Succeed())

			// Eventually: finalizer is added
			Eventually(func(g Gomega) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(controllerutil.ContainsFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig)).To(BeTrue())
			}, timeout, interval).Should(Succeed())
		})

		It("should block deletion while TenantClusters reference the ProviderConfig", func() {
			// Create Secret
			secret := newSecret(secretName, namespaceName, map[string][]byte{
				"kubeconfig": []byte("apiVersion: v1\nkind: Config"),
			})
			Expect(k8sClient.Create(ctx, secret)).To(Succeed())

			// Create ProviderConfig
			pc := newProviderConfig(pcName, namespaceName, butlerv1alpha1.ProviderTypeHarvester, secretName)
			Expect(k8sClient.Create(ctx, pc)).To(Succeed())

			// Wait for finalizer to be added
			Eventually(func(g Gomega) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(controllerutil.ContainsFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig)).To(BeTrue())
			}, timeout, interval).Should(Succeed())

			// Create a TenantCluster referencing this ProviderConfig
			tc := newTenantCluster(tcName, namespaceName, pcName)
			Expect(k8sClient.Create(ctx, tc)).To(Succeed())

			// Delete the ProviderConfig (should be blocked by finalizer)
			Expect(k8sClient.Delete(ctx, pc)).To(Succeed())

			// Consistently: the ProviderConfig should still exist because the finalizer
			// blocks deletion while TenantClusters reference it
			Consistently(func(g Gomega) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: pcName, Namespace: namespaceName}, pc)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pc.DeletionTimestamp).NotTo(BeNil())
				g.Expect(controllerutil.ContainsFinalizer(pc, butlerv1alpha1.FinalizerProviderConfig)).To(BeTrue())
			}, 3*time.Second, interval).Should(Succeed())
		})
	})
})
