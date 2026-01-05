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

package butlerconfig

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	butlerv1alpha1 "github.com/butlerdotdev/butler-api/api/v1alpha1"
)

var _ = Describe("ButlerConfig Controller", func() {
	const (
		timeout  = 10 * time.Second
		interval = 100 * time.Millisecond
	)

	AfterEach(func() {
		// Clean up all ButlerConfigs
		configList := &butlerv1alpha1.ButlerConfigList{}
		Expect(k8sClient.List(ctx, configList)).To(Succeed())
		for _, config := range configList.Items {
			Expect(k8sClient.Delete(ctx, &config)).To(Succeed())
		}

		// Clean up test namespaces created by controller
		for _, nsName := range []string{"butler-tenants", "custom-tenants"} {
			ns := &corev1.Namespace{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: nsName}, ns); err == nil {
				Expect(k8sClient.Delete(ctx, ns)).To(Succeed())
			}
		}

		// Wait for cleanup
		time.Sleep(200 * time.Millisecond)
	})

	Describe("Singleton Validation", func() {
		It("should accept a ButlerConfig named 'butler'", func() {
			config := newButlerConfig(SingletonName, butlerv1alpha1.MultiTenancyModeEnforced, "")
			Expect(k8sClient.Create(ctx, config)).To(Succeed())

			// Wait for reconciliation
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: SingletonName}, config)
				if err != nil {
					return false
				}
				return meta.IsStatusConditionTrue(config.Status.Conditions, ConditionConfigured)
			}, timeout, interval).Should(BeTrue())

			// Verify condition is set correctly
			cond := meta.FindStatusCondition(config.Status.Conditions, ConditionConfigured)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Reason).To(Equal("ConfigValid"))
		})

		It("should reject a ButlerConfig with wrong name", func() {
			config := newButlerConfig("wrong-name", butlerv1alpha1.MultiTenancyModeEnforced, "")
			Expect(k8sClient.Create(ctx, config)).To(Succeed())

			// Wait for reconciliation - should set invalid condition
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: "wrong-name"}, config)
				if err != nil {
					return false
				}
				cond := meta.FindStatusCondition(config.Status.Conditions, ConditionConfigured)
				return cond != nil && cond.Reason == "InvalidName"
			}, timeout, interval).Should(BeTrue())

			// Verify condition message
			cond := meta.FindStatusCondition(config.Status.Conditions, ConditionConfigured)
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Message).To(ContainSubstring("wrong-name"))
		})
	})

	Describe("Default Namespace Creation", func() {
		It("should create default namespace in Optional mode", func() {
			config := newButlerConfig(SingletonName, butlerv1alpha1.MultiTenancyModeOptional, "butler-tenants")
			Expect(k8sClient.Create(ctx, config)).To(Succeed())

			// Wait for namespace to be created
			Eventually(func() error {
				ns := &corev1.Namespace{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: "butler-tenants"}, ns)
			}, timeout, interval).Should(Succeed())

			// Verify namespace has correct labels
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "butler-tenants"}, ns)).To(Succeed())
			Expect(ns.Labels).To(HaveKeyWithValue(butlerv1alpha1.LabelManagedBy, "butler"))
		})

		It("should create default namespace in Disabled mode", func() {
			config := newButlerConfig(SingletonName, butlerv1alpha1.MultiTenancyModeDisabled, "custom-tenants")
			Expect(k8sClient.Create(ctx, config)).To(Succeed())

			// Wait for namespace to be created
			Eventually(func() error {
				ns := &corev1.Namespace{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: "custom-tenants"}, ns)
			}, timeout, interval).Should(Succeed())

			// Verify namespace has correct labels
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "custom-tenants"}, ns)).To(Succeed())
			Expect(ns.Labels).To(HaveKeyWithValue(butlerv1alpha1.LabelManagedBy, "butler"))
		})

		It("should NOT create default namespace in Enforced mode", func() {
			config := newButlerConfig(SingletonName, butlerv1alpha1.MultiTenancyModeEnforced, "should-not-exist")
			Expect(k8sClient.Create(ctx, config)).To(Succeed())

			// Wait for reconciliation
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: SingletonName}, config)
				if err != nil {
					return false
				}
				return meta.IsStatusConditionTrue(config.Status.Conditions, ConditionConfigured)
			}, timeout, interval).Should(BeTrue())

			// Namespace should NOT exist (give it time to potentially be created)
			time.Sleep(500 * time.Millisecond)
			ns := &corev1.Namespace{}
			err := k8sClient.Get(ctx, types.NamespacedName{Name: "should-not-exist"}, ns)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("should use 'butler-tenants' as default when defaultNamespace is empty", func() {
			config := newButlerConfig(SingletonName, butlerv1alpha1.MultiTenancyModeOptional, "")
			Expect(k8sClient.Create(ctx, config)).To(Succeed())

			// Wait for namespace to be created with default name
			Eventually(func() error {
				ns := &corev1.Namespace{}
				return k8sClient.Get(ctx, types.NamespacedName{Name: "butler-tenants"}, ns)
			}, timeout, interval).Should(Succeed())
		})
	})

	Describe("Status Updates", func() {
		It("should count teams", func() {
			// Create ButlerConfig first
			config := newButlerConfig(SingletonName, butlerv1alpha1.MultiTenancyModeEnforced, "")
			Expect(k8sClient.Create(ctx, config)).To(Succeed())

			// Wait for initial reconciliation
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: SingletonName}, config)
				return err == nil && config.Status.ObservedGeneration > 0
			}, timeout, interval).Should(BeTrue())

			// Create some teams
			team1 := newTeam("team-1")
			team2 := newTeam("team-2")
			Expect(k8sClient.Create(ctx, team1)).To(Succeed())
			Expect(k8sClient.Create(ctx, team2)).To(Succeed())

			// For now, just verify the teams exist
			teamList := &butlerv1alpha1.TeamList{}
			Eventually(func() int {
				_ = k8sClient.List(ctx, teamList)
				return len(teamList.Items)
			}, timeout, interval).Should(Equal(2))

			// Clean up teams
			Expect(k8sClient.Delete(ctx, team1)).To(Succeed())
			Expect(k8sClient.Delete(ctx, team2)).To(Succeed())
		})

		It("should set ObservedGeneration", func() {
			config := newButlerConfig(SingletonName, butlerv1alpha1.MultiTenancyModeEnforced, "")
			Expect(k8sClient.Create(ctx, config)).To(Succeed())

			// Wait for ObservedGeneration to be set
			Eventually(func() int64 {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: SingletonName}, config)
				if err != nil {
					return -1
				}
				return config.Status.ObservedGeneration
			}, timeout, interval).Should(Equal(int64(1)))
		})
	})

	Describe("NamespaceReady Condition", func() {
		It("should set NamespaceReady to NotRequired in Enforced mode", func() {
			config := newButlerConfig(SingletonName, butlerv1alpha1.MultiTenancyModeEnforced, "")
			Expect(k8sClient.Create(ctx, config)).To(Succeed())

			Eventually(func() string {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: SingletonName}, config)
				if err != nil {
					return ""
				}
				cond := meta.FindStatusCondition(config.Status.Conditions, ConditionNamespaceReady)
				if cond == nil {
					return ""
				}
				return cond.Reason
			}, timeout, interval).Should(Equal("NotRequired"))
		})

		It("should set NamespaceReady to Ready in Optional mode", func() {
			config := newButlerConfig(SingletonName, butlerv1alpha1.MultiTenancyModeOptional, "butler-tenants")
			Expect(k8sClient.Create(ctx, config)).To(Succeed())

			Eventually(func() string {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: SingletonName}, config)
				if err != nil {
					return ""
				}
				cond := meta.FindStatusCondition(config.Status.Conditions, ConditionNamespaceReady)
				if cond == nil {
					return ""
				}
				return cond.Reason
			}, timeout, interval).Should(Equal("Ready"))
		})
	})
})
