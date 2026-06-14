package e2e_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/onsi/gomega"
	gTypes "github.com/onsi/gomega/types"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	jq "github.com/opendatahub-io/odh-observability/tests/e2e/matchers/jq"
)

type ResourceOpts func(*ResourceOptions)

type ResourceOptions struct {
	tc *TestContext
	t  *testing.T

	GVK schema.GroupVersionKind
	NN  types.NamespacedName

	Condition  gTypes.GomegaMatcher
	MutateFunc func(*unstructured.Unstructured) error

	EventuallyTimeout      *time.Duration
	EventuallyPollInterval *time.Duration

	ConsistentlyTimeout      *time.Duration
	ConsistentlyPollInterval *time.Duration

	IgnoreNotFound           bool
	WaitForDeletion          bool
	RemoveFinalizersOnDelete bool

	CleanupT *testing.T

	ListOptions         client.ListOption
	ClientDeleteOptions client.DeleteOption
	DeleteAllOfOptions  []client.DeleteAllOfOption

	ObjectContent  map[string]any
	CustomErrorArgs []any
}

func (ro *ResourceOptions) applyEventuallyTimeouts(eventually gomega.AsyncAssertion) {
	if ro.EventuallyTimeout != nil {
		eventually.WithTimeout(*ro.EventuallyTimeout)
	}
	if ro.EventuallyPollInterval != nil {
		eventually.WithPolling(*ro.EventuallyPollInterval)
	}
}

func (ro *ResourceOptions) applyConsistentlyTimeouts(consistently gomega.AsyncAssertion) {
	if ro.ConsistentlyTimeout != nil {
		consistently.WithTimeout(*ro.ConsistentlyTimeout)
	}
	if ro.ConsistentlyPollInterval != nil {
		consistently.WithPolling(*ro.ConsistentlyPollInterval)
	}
}

func (ro *ResourceOptions) errorMsg(format string, args ...any) string {
	if len(ro.CustomErrorArgs) > 0 {
		if f, ok := ro.CustomErrorArgs[0].(string); ok {
			return fmt.Sprintf(f, ro.CustomErrorArgs[1:]...)
		}
	}
	msg := fmt.Sprintf(format, args...)
	if ro.t != nil {
		return fmt.Sprintf("[%s] %s", ro.t.Name(), msg)
	}
	return msg
}

func (ro *ResourceOptions) buildObject() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(ro.GVK)
	u.SetName(ro.NN.Name)
	u.SetNamespace(ro.NN.Namespace)

	if ro.ObjectContent != nil {
		for k, v := range ro.ObjectContent {
			u.Object[k] = v
		}
	}

	return u
}

func (ro *ResourceOptions) withTimeoutOpts() []ResourceOpts {
	var opts []ResourceOpts
	if ro.EventuallyTimeout != nil {
		opts = append(opts, WithEventuallyTimeout(*ro.EventuallyTimeout))
	}
	if ro.EventuallyPollInterval != nil {
		opts = append(opts, WithEventuallyPollInterval(*ro.EventuallyPollInterval))
	}
	return opts
}

func WithMinimalObject(gvk schema.GroupVersionKind, nn types.NamespacedName) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.GVK = gvk
		ro.NN = nn
	}
}

func WithCondition(condition gTypes.GomegaMatcher) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.Condition = condition
	}
}

func WithMutateFunc(fn func(*unstructured.Unstructured) error) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.MutateFunc = fn
	}
}

func WithTransforms(transforms ...jq.TransformFn) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.MutateFunc = func(u *unstructured.Unstructured) error {
			pipeline := jq.TransformPipeline(transforms...)
			return pipeline(u)
		}
	}
}

func WithEventuallyTimeout(timeout time.Duration) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.EventuallyTimeout = &timeout
	}
}

func WithEventuallyPollInterval(interval time.Duration) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.EventuallyPollInterval = &interval
	}
}

func WithConsistentlyTimeout(timeout time.Duration) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.ConsistentlyTimeout = &timeout
	}
}

func WithConsistentlyPollInterval(interval time.Duration) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.ConsistentlyPollInterval = &interval
	}
}

func WithIgnoreNotFound(ignore bool) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.IgnoreNotFound = ignore
	}
}

func WithWaitForDeletion(wait bool) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.WaitForDeletion = wait
	}
}

func WithRemoveFinalizersOnDelete(remove bool) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.RemoveFinalizersOnDelete = remove
	}
}

func WithCleanup(t *testing.T) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.CleanupT = t
	}
}

func WithTest(t *testing.T) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.t = t
	}
}

func WithListOptions(opts client.ListOption) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.ListOptions = opts
	}
}

func WithDeleteAllOfOptions(opts ...client.DeleteAllOfOption) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.DeleteAllOfOptions = append(ro.DeleteAllOfOptions, opts...)
	}
}

func WithObjectContent(content map[string]any) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.ObjectContent = content
	}
}

func WithCustomErrorMsg(args ...any) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.CustomErrorArgs = args
	}
}

func WithEventuallyPollingInterval(interval time.Duration) ResourceOpts {
	return func(ro *ResourceOptions) {
		ro.EventuallyPollInterval = &interval
	}
}
