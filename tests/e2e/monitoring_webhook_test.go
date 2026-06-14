package e2e_test

import (
	"maps"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	common "github.com/opendatahub-io/odh-platform-utilities/api/common"
	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"

	. "github.com/onsi/gomega"
)

const (
	TestNamespaceName      = "tests-monitoring-injection"
	TestPodMonitorName     = "test-podmonitor"
	TestServiceMonitorName = "test-servicemonitor"

	ODHLabelMonitoring = "monitoring.opendatahub.io/scrape"
)

func (tc *MonitoringTestCtx) runWebhookTests(t *testing.T) {
	t.Helper()

	t.Run("Group 10: Webhooks", func(t *testing.T) {
		t.Run("Setup", tc.ValidateMonitoringWebhookTestsSetup)
		t.Run("Label value enforcement on namespace", tc.ValidateMonitoringLabelValueEnforcementOnNamespace)
		t.Run("Label value enforcement on monitors", tc.ValidateMonitoringLabelValueEnforcementOnMonitors)
		t.Run("Monitors creation with custom labels", tc.ValidateMonitorsCreationWithCustomLabels)
		t.Run("Monitor label injection", tc.ValidateMonitorLabelInjection)
		t.Run("Monitor label injection on update", tc.ValidateMonitorLabelInjectionOnUpdate)
		t.Run("Monitor label injection on update with custom labels", tc.ValidateMonitorLabelInjectionOnUpdateWithCustomLabels)
		t.Run("Webhook skips non-monitored namespace", tc.ValidateWebhookSkipsNonMonitoredNamespace)
		t.Run("Webhook skips explicitly opted-out namespace", tc.ValidateWebhookSkipsExplicitlyOptedOutNamespace)
		t.Run("Webhook respects user opt-out", tc.ValidateWebhookRespectsUserOptOut)
		t.Run("Webhook idempotency", tc.ValidateWebhookIdempotency)
		t.Run("Webhook skips when monitoring disabled", tc.ValidateWebhookSkipsWhenMonitoringDisabled)
	})
}

func (tc *MonitoringTestCtx) createMonitorsEnvironment(t *testing.T, namespaceLabels map[string]string, monitorLabels map[string]string) {
	t.Helper()

	t.Logf("Pre-test cleanup: removing %s namespace and monitors if they exist", TestNamespaceName)
	tc.DeleteResource(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{Name: TestPodMonitorName, Namespace: TestNamespaceName}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)
	tc.DeleteResource(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{Name: TestServiceMonitorName, Namespace: TestNamespaceName}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)
	tc.DeleteResource(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: TestNamespaceName}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)
	t.Logf("Pre-test cleanup completed")

	tc.cleanupMonitoringAdmissionResources(t, TestPodMonitorName, TestServiceMonitorName)

	applyLabels := func(lbls map[string]string) jq.TransformFn {
		return func(obj *unstructured.Unstructured) error {
			if len(lbls) == 0 {
				return nil
			}
			currentLabels := obj.GetLabels()
			if currentLabels == nil {
				currentLabels = make(map[string]string)
			}
			maps.Copy(currentLabels, lbls)
			obj.SetLabels(currentLabels)
			return nil
		}
	}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: TestNamespaceName}),
		WithMutateFunc(applyLabels(namespaceLabels)),
	)

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			if err := jq.TransformPipeline(
				jq.Transform(`.spec.selector.matchLabels = {"app": "test"}`),
				jq.Transform(`.spec.podMetricsEndpoints = [{"port": "metrics"}]`),
			)(u); err != nil {
				return err
			}
			return applyLabels(monitorLabels)(u)
		}),
	)

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			if err := jq.TransformPipeline(
				jq.Transform(`.spec.selector.matchLabels = {"app": "test"}`),
				jq.Transform(`.spec.endpoints = [{"port": "metrics"}]`),
			)(u); err != nil {
				return err
			}
			return applyLabels(monitorLabels)(u)
		}),
	)
}

func (tc *MonitoringTestCtx) cleanupMonitoringAdmissionResources(t *testing.T, podMonitorName, serviceMonitorName string) {
	t.Helper()

	t.Cleanup(func() {
		if podMonitorName != "" {
			tc.DeleteResource(
				WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{Name: podMonitorName, Namespace: TestNamespaceName}),
				WithIgnoreNotFound(true),
				WithWaitForDeletion(true),
			)
		}
		if serviceMonitorName != "" {
			tc.DeleteResource(
				WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{Name: serviceMonitorName, Namespace: TestNamespaceName}),
				WithIgnoreNotFound(true),
				WithWaitForDeletion(true),
			)
		}
		tc.DeleteResource(
			WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: TestNamespaceName}),
			WithIgnoreNotFound(true),
			WithWaitForDeletion(true),
		)
	})
}

func (tc *MonitoringTestCtx) ValidateMonitoringWebhookTestsSetup(t *testing.T) {
	t.Helper()

	t.Logf("Setting up webhook tests: enabling monitoring and waiting for ready state")

	tc.updateMonitoringConfig(
		withManagementState(common.Managed),
		tc.withMetricsConfig(),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
		WithCondition(And(
			jq.Match(`.spec.metrics != null`),
			jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue),
		)),
		WithCustomErrorMsg("Webhook tests setup: Monitoring CR should be enabled and ready"),
	)

	t.Logf("Webhook tests setup complete: monitoring is enabled and ready")
}

func (tc *MonitoringTestCtx) ValidateMonitoringLabelValueEnforcementOnNamespace(t *testing.T) {
	t.Helper()

	tc.DeleteResource(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: TestNamespaceName}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)

	tc.cleanupMonitoringAdmissionResources(t, "", "")

	invalidNamespace := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestNamespaceName,
			Labels: map[string]string{
				ODHLabelMonitoring: "invalid-value",
			},
		},
	}

	err := tc.Client().Create(tc.Context(), invalidNamespace)
	tc.g.Expect(err).To(HaveOccurred(), "Validation policy should block namespace with invalid monitoring label value")
	tc.g.Expect(err).To(MatchError(ContainSubstring("must be set to 'true' or 'false'")), "Error message should indicate valid values")
}

func (tc *MonitoringTestCtx) ValidateMonitoringLabelValueEnforcementOnMonitors(t *testing.T) {
	t.Helper()

	tc.DeleteResource(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{Name: "test-invalid-podmonitor", Namespace: TestNamespaceName}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)
	tc.DeleteResource(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{Name: "test-invalid-servicemonitor", Namespace: TestNamespaceName}),
		WithIgnoreNotFound(true),
		WithWaitForDeletion(true),
	)

	tc.cleanupMonitoringAdmissionResources(t, "test-invalid-podmonitor", "test-invalid-servicemonitor")

	tc.createMonitorsEnvironment(t, nil, nil)

	invalidLabels := map[string]string{
		ODHLabelMonitoring: "invalid-value",
	}

	invalidPodMonitor := &unstructured.Unstructured{}
	invalidPodMonitor.SetGroupVersionKind(gvk.CoreosPodMonitor)
	invalidPodMonitor.SetName("test-invalid-podmonitor")
	invalidPodMonitor.SetNamespace(TestNamespaceName)
	invalidPodMonitor.SetLabels(invalidLabels)
	invalidPodMonitor.Object["spec"] = map[string]any{
		"selector": map[string]any{
			"matchLabels": map[string]any{"app": "test"},
		},
		"podMetricsEndpoints": []any{
			map[string]any{"port": "metrics"},
		},
	}

	err := tc.Client().Create(tc.Context(), invalidPodMonitor)
	tc.g.Expect(err).To(HaveOccurred(), "Validation policy should block PodMonitor with invalid monitoring label value")
	tc.g.Expect(err).To(MatchError(ContainSubstring("must be set to 'true' or 'false'")), "Error message should indicate valid values for PodMonitor")

	invalidServiceMonitor := &unstructured.Unstructured{}
	invalidServiceMonitor.SetGroupVersionKind(gvk.CoreosServiceMonitor)
	invalidServiceMonitor.SetName("test-invalid-servicemonitor")
	invalidServiceMonitor.SetNamespace(TestNamespaceName)
	invalidServiceMonitor.SetLabels(invalidLabels)
	invalidServiceMonitor.Object["spec"] = map[string]any{
		"selector": map[string]any{
			"matchLabels": map[string]any{"app": "test"},
		},
		"endpoints": []any{
			map[string]any{"port": "metrics"},
		},
	}

	err = tc.Client().Create(tc.Context(), invalidServiceMonitor)
	tc.g.Expect(err).To(HaveOccurred(), "Validation policy should block ServiceMonitor with invalid monitoring label value")
	tc.g.Expect(err).To(MatchError(ContainSubstring("must be set to 'true' or 'false'")), "Error message should indicate valid values for ServiceMonitor")
}

func (tc *MonitoringTestCtx) ValidateMonitorLabelInjection(t *testing.T) {
	t.Helper()

	nsLabels := map[string]string{
		ODHLabelMonitoring: "true",
	}

	tc.createMonitorsEnvironment(t, nsLabels, nil)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == "true"`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Mutating webhook should inject monitoring.opendatahub.io/scrape=true label into PodMonitor"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == "true"`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Mutating webhook should inject monitoring.opendatahub.io/scrape=true label into ServiceMonitor"),
	)
}

func (tc *MonitoringTestCtx) ValidateMonitorsCreationWithCustomLabels(t *testing.T) {
	t.Helper()

	nsLabels := map[string]string{
		ODHLabelMonitoring: "true",
	}

	customLabels := map[string]string{
		"app":  "my-app",
		"team": "platform",
	}

	tc.createMonitorsEnvironment(t, nsLabels, customLabels)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(And(
			jq.Match(`.metadata.labels."app" == "my-app"`),
			jq.Match(`.metadata.labels."team" == "platform"`),
			jq.Match(`.metadata.labels."%s" == "true"`, ODHLabelMonitoring),
		)),
		WithCustomErrorMsg("Webhook should inject monitoring=true AND preserve custom labels (PodMonitor)"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(And(
			jq.Match(`.metadata.labels."app" == "my-app"`),
			jq.Match(`.metadata.labels."team" == "platform"`),
			jq.Match(`.metadata.labels."%s" == "true"`, ODHLabelMonitoring),
		)),
		WithCustomErrorMsg("Webhook should inject monitoring=true AND preserve custom labels (ServiceMonitor)"),
	)
}

func (tc *MonitoringTestCtx) ValidateMonitorLabelInjectionOnUpdate(t *testing.T) {
	t.Helper()

	nsLabels := map[string]string{
		"temp-label": "temp-value",
	}

	tc.createMonitorsEnvironment(t, nsLabels, nil)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == null`, ODHLabelMonitoring)),
		WithCustomErrorMsg("PodMonitor should not have monitoring label when created in non-monitored namespace"),
	)

	tc.EventuallyResourcePatched(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: TestNamespaceName}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return jq.Transform(`.metadata.labels."%s" = "true"`, ODHLabelMonitoring)(u)
		}),
	)

	tc.EventuallyResourcePatched(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return jq.Transform(`.metadata.annotations."test-update" = "trigger-webhook"`)(u)
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == "true"`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Webhook should inject monitoring label on UPDATE operation (PodMonitor)"),
	)

	tc.EventuallyResourcePatched(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return jq.Transform(`.metadata.annotations."test-update" = "trigger-webhook"`)(u)
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == "true"`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Webhook should inject monitoring label on UPDATE operation (ServiceMonitor)"),
	)
}

func (tc *MonitoringTestCtx) ValidateMonitorLabelInjectionOnUpdateWithCustomLabels(t *testing.T) {
	t.Helper()

	nsLabels := map[string]string{
		"temp-label": "temp-value",
	}

	customLabels := map[string]string{
		"app":  "my-app",
		"team": "platform",
	}

	tc.createMonitorsEnvironment(t, nsLabels, customLabels)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(And(
			jq.Match(`.metadata.labels."app" == "my-app"`),
			jq.Match(`.metadata.labels."team" == "platform"`),
			jq.Match(`.metadata.labels."%s" == null`, ODHLabelMonitoring),
		)),
		WithCustomErrorMsg("PodMonitor should have custom labels but no monitoring label initially"),
	)

	tc.EventuallyResourcePatched(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: TestNamespaceName}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return jq.Transform(`.metadata.labels."%s" = "true"`, ODHLabelMonitoring)(u)
		}),
	)

	tc.EventuallyResourcePatched(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return jq.Transform(`.metadata.annotations."test-update" = "trigger-webhook"`)(u)
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(And(
			jq.Match(`.metadata.labels."app" == "my-app"`),
			jq.Match(`.metadata.labels."team" == "platform"`),
			jq.Match(`.metadata.labels."%s" == "true"`, ODHLabelMonitoring),
		)),
		WithCustomErrorMsg("Webhook should inject monitoring label AND preserve custom labels on UPDATE (PodMonitor)"),
	)

	tc.EventuallyResourcePatched(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			return jq.Transform(`.metadata.annotations."test-update" = "trigger-webhook"`)(u)
		}),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(And(
			jq.Match(`.metadata.labels."app" == "my-app"`),
			jq.Match(`.metadata.labels."team" == "platform"`),
			jq.Match(`.metadata.labels."%s" == "true"`, ODHLabelMonitoring),
		)),
		WithCustomErrorMsg("Webhook should inject monitoring label AND preserve custom labels on UPDATE (ServiceMonitor)"),
	)
}

func (tc *MonitoringTestCtx) ValidateWebhookSkipsNonMonitoredNamespace(t *testing.T) {
	t.Helper()

	nsLabels := map[string]string{
		"some-other-label": "value",
	}

	tc.createMonitorsEnvironment(t, nsLabels, nil)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == null`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Webhook should NOT inject monitoring label when namespace is not opted-in (PodMonitor)"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == null`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Webhook should NOT inject monitoring label when namespace is not opted-in (ServiceMonitor)"),
	)
}

func (tc *MonitoringTestCtx) ValidateWebhookSkipsExplicitlyOptedOutNamespace(t *testing.T) {
	t.Helper()

	nsLabels := map[string]string{
		ODHLabelMonitoring: "false",
	}

	tc.createMonitorsEnvironment(t, nsLabels, nil)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == null`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Webhook should NOT inject monitoring label when namespace explicitly has monitoring=false (PodMonitor)"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == null`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Webhook should NOT inject monitoring label when namespace explicitly has monitoring=false (ServiceMonitor)"),
	)
}

func (tc *MonitoringTestCtx) ValidateWebhookRespectsUserOptOut(t *testing.T) {
	t.Helper()

	nsLabels := map[string]string{
		ODHLabelMonitoring: "true",
	}

	userOptOutLabels := map[string]string{
		ODHLabelMonitoring: "false",
	}

	tc.createMonitorsEnvironment(t, nsLabels, userOptOutLabels)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == "false"`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Webhook should respect user's explicit monitoring=false on PodMonitor"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == "false"`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Webhook should respect user's explicit monitoring=false on ServiceMonitor"),
	)
}

func (tc *MonitoringTestCtx) ValidateWebhookIdempotency(t *testing.T) {
	t.Helper()

	nsLabels := map[string]string{
		ODHLabelMonitoring: "true",
	}

	existingLabels := map[string]string{
		ODHLabelMonitoring: "true",
	}

	tc.createMonitorsEnvironment(t, nsLabels, existingLabels)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == "true"`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Webhook should be idempotent - PodMonitor already has monitoring=true"),
	)

	tc.EnsureResourceExists(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
		WithCondition(jq.Match(`.metadata.labels."%s" == "true"`, ODHLabelMonitoring)),
		WithCustomErrorMsg("Webhook should be idempotent - ServiceMonitor already has monitoring=true"),
	)
}

func (tc *MonitoringTestCtx) ValidateWebhookSkipsWhenMonitoringDisabled(t *testing.T) {
	t.Helper()

	t.Cleanup(func() {
		tc.updateMonitoringConfig(
			withManagementState(common.Managed),
			tc.withMetricsConfig(),
		)

		tc.EnsureResourceExists(
			WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
			WithCondition(jq.Match(`.status.conditions[] | select(.type == "%s") | .status == "%s"`, common.ConditionTypeReady, metav1.ConditionTrue)),
			WithCustomErrorMsg("Monitoring should be re-enabled after webhook disabled test"),
		)
	})

	t.Logf("Disabling monitoring to test webhook behavior when monitoring is disabled")

	tc.updateMonitoringConfig(withManagementState(common.Removed))

	tc.EnsureResourceGone(
		WithMinimalObject(gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName}),
	)

	t.Logf("Monitoring disabled and Monitoring CR deleted, proceeding to test webhook behavior")

	nsLabels := map[string]string{
		ODHLabelMonitoring: "true",
	}

	noLabels := map[string]string{}

	tc.createMonitorsEnvironment(t, nsLabels, noLabels)

	podMonitor := tc.FetchResource(
		WithMinimalObject(gvk.CoreosPodMonitor, types.NamespacedName{
			Name:      TestPodMonitorName,
			Namespace: TestNamespaceName,
		}),
	)
	tc.g.Expect(podMonitor.GetLabels()).NotTo(HaveKey(ODHLabelMonitoring),
		"Webhook should NOT inject monitoring=true when monitoring is disabled (PodMonitor)")

	serviceMonitor := tc.FetchResource(
		WithMinimalObject(gvk.CoreosServiceMonitor, types.NamespacedName{
			Name:      TestServiceMonitorName,
			Namespace: TestNamespaceName,
		}),
	)
	tc.g.Expect(serviceMonitor.GetLabels()).NotTo(HaveKey(ODHLabelMonitoring),
		"Webhook should NOT inject monitoring=true when monitoring is disabled (ServiceMonitor)")

	t.Logf("Webhook correctly skipped injection when monitoring was disabled")
}
