package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	. "github.com/onsi/gomega"

	"github.com/opendatahub-io/odh-observability/internal/controller/gvk"
	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"
)

type TestContext struct {
	t                   *testing.T
	client              client.Client
	ctx                 context.Context
	g                   *gomega.WithT
	Timeouts            TestTimeouts
	MonitoringNamespace string
	MonitoringCRName    string

	DefaultResourceOpts []ResourceOpts
}

func (tc *TestContext) Client() client.Client {
	return tc.client
}

func (tc *TestContext) Context() context.Context {
	return tc.ctx
}

func NewTestContext(t *testing.T) (*TestContext, error) {
	t.Helper()

	cfg, err := ctrlcfg.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("error creating the config object: %w", err)
	}

	ctrlCli, err := client.New(cfg, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize client: %w", err)
	}

	g := gomega.NewWithT(t)
	g.SetDefaultEventuallyTimeout(testOpts.Timeouts.defaultEventuallyTimeout)
	g.SetDefaultEventuallyPollingInterval(testOpts.Timeouts.defaultEventuallyPollInterval)
	g.SetDefaultConsistentlyDuration(testOpts.Timeouts.defaultConsistentlyTimeout)
	g.SetDefaultConsistentlyPollingInterval(testOpts.Timeouts.defaultConsistentlyPollInterval)

	return &TestContext{
		t:                   t,
		client:              ctrlCli,
		ctx:                 context.Background(),
		g:                   g,
		Timeouts:            testOpts.Timeouts,
		MonitoringNamespace: testOpts.monitoringNamespace,
		MonitoringCRName:    testOpts.monitoringCRName,
	}, nil
}

func (tc *TestContext) NewResourceOptions(opts ...ResourceOpts) *ResourceOptions {
	ro := &ResourceOptions{tc: tc, t: tc.t}

	for _, opt := range tc.DefaultResourceOpts {
		opt(ro)
	}
	for _, opt := range opts {
		opt(ro)
	}

	return ro
}

func (tc *TestContext) fetchResource(
	t *testing.T,
	gvk schema.GroupVersionKind,
	nn types.NamespacedName,
) (*unstructured.Unstructured, error) {
	t.Helper()

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)

	err := tc.client.Get(tc.ctx, nn, u)
	if err != nil {
		return nil, err
	}

	return u, nil
}

func (tc *TestContext) FetchResource(opts ...ResourceOpts) *unstructured.Unstructured {
	ro := tc.NewResourceOptions(opts...)
	var result *unstructured.Unstructured

	tc.g.Eventually(func() error {
		u, err := tc.fetchResource(ro.t, ro.GVK, ro.NN)
		if err != nil {
			return err
		}
		result = u
		return nil
	}).Should(Succeed())

	return result
}

func (tc *TestContext) EnsureResourceExists(opts ...ResourceOpts) *unstructured.Unstructured {
	ro := tc.NewResourceOptions(opts...)
	var result *unstructured.Unstructured

	eventually := ro.tc.g.Eventually(func() (*unstructured.Unstructured, error) {
		u, err := tc.fetchResource(ro.t, ro.GVK, ro.NN)
		if err != nil {
			if k8serr.IsNotFound(err) {
				return nil, err
			}
			return nil, StopErr(err, "failed to get resource: %s %s", ro.GVK, ro.NN)
		}
		return u, nil
	})

	ro.applyEventuallyTimeouts(eventually)

	if ro.Condition != nil {
		eventually.Should(And(Not(BeNil()), ro.Condition), ro.errorMsg("resource should exist and match condition"))
	} else {
		eventually.Should(Not(BeNil()), ro.errorMsg("resource should exist"))
	}

	ro.tc.g.Eventually(func() (*unstructured.Unstructured, error) {
		u, err := tc.fetchResource(ro.t, ro.GVK, ro.NN)
		if err != nil {
			return nil, err
		}
		result = u
		return u, nil
	}).Should(Not(BeNil()))

	return result
}

func (tc *TestContext) EnsureResourceExistsConsistently(opts ...ResourceOpts) *unstructured.Unstructured {
	ro := tc.NewResourceOptions(opts...)

	tc.EnsureResourceExists(opts...)

	var result *unstructured.Unstructured

	consistently := ro.tc.g.Consistently(func() (*unstructured.Unstructured, error) {
		u, err := tc.fetchResource(ro.t, ro.GVK, ro.NN)
		if err != nil {
			return nil, err
		}
		result = u
		return u, nil
	})

	ro.applyConsistentlyTimeouts(consistently)

	if ro.Condition != nil {
		consistently.Should(And(Not(BeNil()), ro.Condition), ro.errorMsg("resource should consistently match condition"))
	} else {
		consistently.Should(Not(BeNil()), ro.errorMsg("resource should consistently exist"))
	}

	return result
}

func (tc *TestContext) EnsureResourcesExist(opts ...ResourceOpts) []unstructured.Unstructured {
	ro := tc.NewResourceOptions(opts...)
	var result []unstructured.Unstructured

	eventually := ro.tc.g.Eventually(func() ([]unstructured.Unstructured, error) {
		items := unstructured.UnstructuredList{}
		items.SetGroupVersionKind(ro.GVK)

		listOpts := []client.ListOption{}
		if ro.ListOptions != nil {
			listOpts = append(listOpts, ro.ListOptions)
		}

		err := tc.client.List(tc.ctx, &items, listOpts...)
		if err != nil {
			return nil, StopErr(err, "failed to list resources: %s", ro.GVK)
		}

		result = items.Items
		return result, nil
	})

	ro.applyEventuallyTimeouts(eventually)

	if ro.Condition != nil {
		eventually.Should(ro.Condition, ro.errorMsg("resources should match condition"))
	} else {
		eventually.Should(Not(BeEmpty()), ro.errorMsg("resources should exist"))
	}

	return result
}

func (tc *TestContext) EnsureResourceDoesNotExist(opts ...ResourceOpts) {
	ro := tc.NewResourceOptions(opts...)

	_, err := tc.fetchResource(ro.t, ro.GVK, ro.NN)
	ro.tc.g.Expect(k8serr.IsNotFound(err)).To(BeTrue(), ro.errorMsg("resource should not exist: %s %s", ro.GVK, ro.NN))
}

func (tc *TestContext) EnsureResourceGone(opts ...ResourceOpts) {
	ro := tc.NewResourceOptions(opts...)

	eventually := ro.tc.g.Eventually(func() bool {
		_, err := tc.fetchResource(ro.t, ro.GVK, ro.NN)
		return k8serr.IsNotFound(err)
	})

	ro.applyEventuallyTimeouts(eventually)
	eventually.Should(BeTrue(), ro.errorMsg("resource should be gone: %s %s", ro.GVK, ro.NN))
}

func (tc *TestContext) EnsureResourcesGone(opts ...ResourceOpts) {
	ro := tc.NewResourceOptions(opts...)

	eventually := ro.tc.g.Eventually(func() bool {
		_, err := tc.fetchResource(ro.t, ro.GVK, ro.NN)
		return k8serr.IsNotFound(err)
	})

	ro.applyEventuallyTimeouts(eventually)
	eventually.Should(BeTrue(), ro.errorMsg("resource should be gone: %s %s", ro.GVK, ro.NN))
}

func (tc *TestContext) EventuallyResourceCreated(opts ...ResourceOpts) *unstructured.Unstructured {
	ro := tc.NewResourceOptions(opts...)

	obj := ro.buildObject()

	var result *unstructured.Unstructured

	eventually := ro.tc.g.Eventually(func() error {
		err := tc.client.Create(tc.ctx, obj)
		if err != nil {
			return err
		}
		result = obj
		return nil
	})

	ro.applyEventuallyTimeouts(eventually)
	eventually.Should(Succeed(), ro.errorMsg("resource should be created"))

	if ro.Condition != nil {
		tc.EnsureResourceExists(opts...)
	}

	if ro.CleanupT != nil {
		ro.CleanupT.Cleanup(func() {
			tc.DeleteResource(
				WithMinimalObject(ro.GVK, ro.NN),
				WithIgnoreNotFound(true),
				WithWaitForDeletion(true),
			)
		})
	}

	return result
}

func (tc *TestContext) EventuallyResourcePatched(opts ...ResourceOpts) *unstructured.Unstructured {
	ro := tc.NewResourceOptions(opts...)
	var result *unstructured.Unstructured

	eventually := ro.tc.g.Eventually(func() (*unstructured.Unstructured, error) {
		u, err := tc.fetchResource(ro.t, ro.GVK, ro.NN)
		if err != nil {
			return nil, err
		}

		if ro.MutateFunc != nil {
			original := u.DeepCopy()

			if err := ro.MutateFunc(u); err != nil {
				return nil, StopErr(err, "failed to apply mutation")
			}

			patch := client.MergeFrom(original)
			if err := tc.client.Patch(tc.ctx, u, patch); err != nil {
				return nil, err
			}
		}

		result = u
		return u, nil
	})

	ro.applyEventuallyTimeouts(eventually)

	if ro.Condition != nil {
		eventually.Should(And(Not(BeNil()), ro.Condition), ro.errorMsg("resource should be patched and match condition"))
	} else {
		eventually.Should(Not(BeNil()), ro.errorMsg("resource should be patched"))
	}

	if ro.Condition != nil {
		ensureOpts := []ResourceOpts{
			WithMinimalObject(ro.GVK, ro.NN),
			WithCondition(ro.Condition),
		}
		ensureOpts = append(ensureOpts, ro.withTimeoutOpts()...)
		tc.EnsureResourceExists(ensureOpts...)
	}

	return result
}

func (tc *TestContext) EventuallyResourceCreatedOrPatched(opts ...ResourceOpts) *unstructured.Unstructured {
	ro := tc.NewResourceOptions(opts...)
	var result *unstructured.Unstructured

	eventually := ro.tc.g.Eventually(func() (*unstructured.Unstructured, error) {
		u := ro.buildObject()

		existing, err := tc.fetchResource(ro.t, ro.GVK, ro.NN)
		if k8serr.IsNotFound(err) {
			if ro.MutateFunc != nil {
				if err := ro.MutateFunc(u); err != nil {
					return nil, StopErr(err, "failed to apply mutation for create")
				}
			}
			if err := tc.client.Create(tc.ctx, u); err != nil {
				return nil, err
			}
			result = u
			return u, nil
		}
		if err != nil {
			return nil, err
		}

		if ro.MutateFunc != nil {
			original := existing.DeepCopy()
			if err := ro.MutateFunc(existing); err != nil {
				return nil, StopErr(err, "failed to apply mutation for patch")
			}
			patch := client.MergeFrom(original)
			if err := tc.client.Patch(tc.ctx, existing, patch); err != nil {
				return nil, err
			}
		}

		result = existing
		return existing, nil
	})

	ro.applyEventuallyTimeouts(eventually)

	if ro.Condition != nil {
		eventually.Should(And(Not(BeNil()), ro.Condition), ro.errorMsg("resource should be created/patched and match condition"))
	} else {
		eventually.Should(Not(BeNil()), ro.errorMsg("resource should be created/patched"))
	}

	return result
}

func (tc *TestContext) ConsistentlyResourceCreatedOrPatched(opts ...ResourceOpts) *unstructured.Unstructured {
	result := tc.EventuallyResourceCreatedOrPatched(opts...)

	ro := tc.NewResourceOptions(opts...)

	consistently := ro.tc.g.Consistently(func() (*unstructured.Unstructured, error) {
		return tc.fetchResource(ro.t, ro.GVK, ro.NN)
	})

	ro.applyConsistentlyTimeouts(consistently)

	if ro.Condition != nil {
		consistently.Should(And(Not(BeNil()), ro.Condition), ro.errorMsg("resource should consistently match condition"))
	} else {
		consistently.Should(Not(BeNil()), ro.errorMsg("resource should consistently exist"))
	}

	return result
}

func (tc *TestContext) DeleteResource(opts ...ResourceOpts) {
	ro := tc.NewResourceOptions(opts...)

	if ro.RemoveFinalizersOnDelete {
		tc.tryRemoveFinalizers(ro)
	}

	u := ro.buildObject()
	deleteOpts := []client.DeleteOption{}
	if ro.ClientDeleteOptions != nil {
		deleteOpts = append(deleteOpts, ro.ClientDeleteOptions)
	}

	err := tc.client.Delete(tc.ctx, u, deleteOpts...)
	if err != nil {
		if k8serr.IsNotFound(err) && ro.IgnoreNotFound {
			return
		}
		ro.tc.g.Expect(err).ToNot(HaveOccurred(), ro.errorMsg("failed to delete resource"))
		return
	}

	if ro.WaitForDeletion {
		goneOpts := []ResourceOpts{WithMinimalObject(ro.GVK, ro.NN)}
		goneOpts = append(goneOpts, ro.withTimeoutOpts()...)
		tc.EnsureResourceGone(goneOpts...)
	}
}

func (tc *TestContext) DeleteResources(opts ...ResourceOpts) {
	ro := tc.NewResourceOptions(opts...)

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(ro.GVK)

	deleteOpts := []client.DeleteAllOfOption{}
	for _, o := range ro.DeleteAllOfOptions {
		deleteOpts = append(deleteOpts, o)
	}

	err := tc.client.DeleteAllOf(tc.ctx, u, deleteOpts...)
	if err != nil && !k8serr.IsNotFound(err) {
		if !ro.IgnoreNotFound {
			ro.tc.g.Expect(err).ToNot(HaveOccurred(), ro.errorMsg("failed to delete resources"))
		}
	}
}

func (tc *TestContext) tryRemoveFinalizers(ro *ResourceOptions) {
	u, err := tc.fetchResource(ro.t, ro.GVK, ro.NN)
	if err != nil {
		return
	}

	if len(u.GetFinalizers()) == 0 {
		return
	}

	original := u.DeepCopy()
	u.SetFinalizers(nil)
	patch := client.MergeFrom(original)
	ro.tc.g.Expect(tc.client.Patch(tc.ctx, u, patch)).To(
		Succeed(),
		ro.errorMsg("failed to remove finalizers before delete"),
	)
}

func (tc *TestContext) EnsureDeploymentReady(opts ...ResourceOpts) {
	ro := tc.NewResourceOptions(opts...)

	eventually := ro.tc.g.Eventually(func() (bool, error) {
		u, err := tc.fetchResource(ro.t, ro.GVK, ro.NN)
		if err != nil {
			return false, err
		}

		readyReplicas, _, _ := unstructured.NestedInt64(u.Object, "status", "readyReplicas")
		replicas, _, _ := unstructured.NestedInt64(u.Object, "spec", "replicas")

		if replicas == 0 {
			replicas = 1
		}

		return readyReplicas >= replicas, nil
	})

	ro.applyEventuallyTimeouts(eventually)
	eventually.Should(BeTrue(), ro.errorMsg("deployment should be ready"))
}

func (tc *TestContext) EnsureResourceConditionMet(
	gvk schema.GroupVersionKind,
	nn types.NamespacedName,
	conditionType string,
	expectedStatus metav1.ConditionStatus,
	opts ...ResourceOpts,
) {
	mergedOpts := append([]ResourceOpts{
		WithMinimalObject(gvk, nn),
		WithCondition(
			And(
				Not(BeNil()),
				WithTransform(func(u *unstructured.Unstructured) bool {
					conditions, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
					for _, c := range conditions {
						cm, ok := c.(map[string]any)
						if !ok {
							continue
						}
						if cm["type"] == conditionType && cm["status"] == string(expectedStatus) {
							return true
						}
					}
					return false
				}, BeTrue()),
			),
		),
	}, opts...)

	tc.EnsureResourceExists(mergedOpts...)
}

type MonitoringTestCtx struct {
	*TestContext

	expectedDefaultReplicas int
}

func StopErr(err error, format string, args ...any) error {
	msg := format
	if len(args) != 0 {
		msg = fmt.Sprintf(format, args...)
	}

	return gomega.StopTrying(msg).Wrap(err)
}

// detectMonitoringNamespace reads spec.namespace from the Monitoring CR.
func (tc *TestContext) detectMonitoringNamespace(t *testing.T) string {
	t.Helper()

	u, err := tc.fetchResource(t, gvk.Monitoring, types.NamespacedName{Name: tc.MonitoringCRName})
	if err != nil {
		t.Fatalf("failed to fetch Monitoring CR to detect namespace: %v", err)
	}

	ns, _, _ := unstructured.NestedString(u.Object, "spec", "namespace")
	if ns == "" {
		t.Fatalf("Monitoring CR %s has no spec.namespace set", tc.MonitoringCRName)
	}

	return ns
}

// ensureOperatorPodRunning verifies that at least one odh-observability
// operator pod is Running on the cluster. Fails immediately if not found.
func (tc *TestContext) ensureOperatorPodRunning(t *testing.T) {
	t.Helper()

	pods := &unstructured.UnstructuredList{}
	pods.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("PodList"))

	err := tc.client.List(tc.ctx, pods,
		client.MatchingLabels{"app.kubernetes.io/name": "odh-observability"},
	)
	if err != nil {
		t.Fatalf("failed to list operator pods: %v", err)
	}

	for i := range pods.Items {
		phase, _, _ := unstructured.NestedString(pods.Items[i].Object, "status", "phase")
		if phase == "Running" {
			return
		}
	}

	t.Fatalf("no running odh-observability operator pod found on cluster — run 'make deploy' first")
}

// ensureCRDExists verifies that a CRD is registered on the cluster and fails
// the test immediately if it is not.
func (tc *TestContext) ensureCRDExists(t *testing.T, g schema.GroupVersionKind) {
	t.Helper()

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(g)

	err := tc.client.List(tc.ctx, list, client.Limit(1))
	if err != nil {
		t.Fatalf("CRD %s/%s not found on cluster — is the operator deployed? (%v)", g.Group, g.Kind, err)
	}
}

// OLM operator installation helpers.

func (tc *TestContext) ensureNamespaceExists(name string) {
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Namespace, types.NamespacedName{Name: name}),
		WithCustomErrorMsg("namespace %s should exist", name),
	)
}

func (tc *TestContext) ensureOperatorGroupExists(namespace, name string) {
	items := &unstructured.UnstructuredList{}
	items.SetGroupVersionKind(gvk.OperatorGroup)

	err := tc.client.List(tc.ctx, items, client.InNamespace(namespace))
	if err == nil && len(items.Items) > 0 {
		return
	}

	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.OperatorGroup, types.NamespacedName{Name: name, Namespace: namespace}),
		WithCustomErrorMsg("OperatorGroup %s/%s should exist", namespace, name),
	)
}

func (tc *TestContext) ensureSubscriptionExists(namespace, name, channel string) {
	tc.EventuallyResourceCreatedOrPatched(
		WithMinimalObject(gvk.Subscription, types.NamespacedName{Name: name, Namespace: namespace}),
		WithMutateFunc(func(u *unstructured.Unstructured) error {
			if err := unstructured.SetNestedField(u.Object, name, "spec", "name"); err != nil {
				return err
			}
			if err := unstructured.SetNestedField(u.Object, channel, "spec", "channel"); err != nil {
				return err
			}
			if err := unstructured.SetNestedField(u.Object, name, "spec", "package"); err != nil {
				return err
			}
			if err := unstructured.SetNestedField(u.Object, "redhat-operators", "spec", "source"); err != nil {
				return err
			}
			if err := unstructured.SetNestedField(u.Object, "openshift-marketplace", "spec", "sourceNamespace"); err != nil {
				return err
			}
			return unstructured.SetNestedField(u.Object, "Automatic", "spec", "installPlanApproval")
		}),
		WithCondition(jq.Match(`.status | has("installPlanRef")`)),
		WithEventuallyTimeout(tc.Timeouts.olmOperationTimeout),
		WithCustomErrorMsg("Subscription %s/%s should have an installPlanRef", namespace, name),
	)
}

func (tc *TestContext) ensureCSVSucceeded(namespace string, subscriptionNN types.NamespacedName) {
	tc.g.Eventually(func(g Gomega) {
		sub, err := tc.fetchResource(tc.t, gvk.Subscription, subscriptionNN)
		g.Expect(err).NotTo(HaveOccurred(), "failed to fetch Subscription %s", subscriptionNN)

		csvName, _, _ := unstructured.NestedString(sub.Object, "status", "currentCSV")
		if csvName == "" {
			csvName, _, _ = unstructured.NestedString(sub.Object, "status", "installedCSV")
		}
		g.Expect(csvName).NotTo(BeEmpty(), "Subscription %s has no currentCSV or installedCSV", subscriptionNN)

		csv, err := tc.fetchResource(tc.t, gvk.ClusterServiceVersion, types.NamespacedName{
			Name:      csvName,
			Namespace: namespace,
		})
		g.Expect(err).NotTo(HaveOccurred(), "failed to fetch CSV %s/%s", namespace, csvName)

		phase, _, _ := unstructured.NestedString(csv.Object, "status", "phase")
		g.Expect(phase).To(Equal("Succeeded"), "CSV %s phase is %s, expected Succeeded", csvName, phase)
	}).WithTimeout(tc.Timeouts.olmOperationTimeout).
		WithPolling(10 * time.Second).
		Should(Succeed())
}

func (tc *TestContext) EnsureOperatorInstalled(namespace, name, channel string) {
	tc.ensureNamespaceExists(namespace)
	tc.ensureOperatorGroupExists(namespace, name)

	nn := types.NamespacedName{Name: name, Namespace: namespace}
	tc.ensureSubscriptionExists(namespace, name, channel)
	tc.ensureCSVSucceeded(namespace, nn)
}


