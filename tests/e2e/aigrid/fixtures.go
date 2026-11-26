package aigrid

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const fixtureLabel = "e2e.opendatahub.io/run"
const keyPath = "/v1/api-keys"
const subscriptionKind = "MaaSSubscription"
const conditionTrue = "True"
const typeField = "type"

type MaaSClient struct {
	Client *http.Client
	URL    string
}

// NewMaaSClient uses the explicit spoke kubecontext's authentication internally.
// The API endpoint has its own CA trust, independent of the Kubernetes API CA.
func NewMaaSClient(s Spoke) (MaaSClient, error) {
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: s.Context})
	cfg, err := loader.ClientConfig()
	if err != nil {
		return MaaSClient{}, errors.New("load MaaS management authentication from spoke kubecontext")
	}
	cfg.Host = s.MaaS.APIURL
	cfg.CAData, cfg.CAFile, cfg.ServerName, cfg.Insecure = nil, s.CAFile, "", false
	cfg.Timeout = 30 * time.Second
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return MaaSClient{}, errors.New("create authenticated MaaS management transport")
	}
	httpClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	return MaaSClient{Client: httpClient, URL: s.MaaS.APIURL}, nil
}

func (m MaaSClient) request(ctx context.Context, method, path string, body, into any) error {
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(m.URL, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return errors.New("construct MaaS management request")
	}
	req.Header.Set("Content-Type", "application/json")
	data, err := doRequest(m.Client, req)
	if err != nil {
		return err
	}
	if into != nil && json.Unmarshal(data, into) != nil {
		return errors.New("invalid JSON response from MaaS management API")
	}
	return nil
}

// Fixture creates a fresh subscription and matching key. Register Cleanup before
// Prepare so that API failures, readiness timeouts and assertions all clean up.
// Its key remains in memory; only the random subscription name is retained.
type Fixture struct {
	Client client.Client
	MaaS   MaaSClient
	Spoke  Spoke
	Name   string

	objects       []*unstructured.Unstructured
	keyID         string
	mintAttempted bool
}

func NewFixture(c client.Client, api MaaSClient, spoke Spoke) *Fixture {
	return &Fixture{Client: c, MaaS: api, Spoke: spoke, Name: "aigrid-" + uuid.NewString()}
}

func (f *Fixture) Prepare(ctx context.Context, interval, timeout time.Duration) (string, error) {
	m := f.Spoke.MaaS
	model := map[string]any{nameField: m.ModelRef, namespaceField: m.ModelNamespace}
	policy := f.object("MaaSAuthPolicy", map[string]any{
		"modelRefs": []any{model}, "subjects": map[string]any{"users": []any{m.Username}},
	})
	subscription := f.object(subscriptionKind, map[string]any{
		"organization": map[string]any{"id": f.Spoke.OrganizationID},
		"owner":        map[string]any{"users": []any{m.Username}}, "priority": int64(-100),
		"modelRefs": []any{map[string]any{nameField: m.ModelRef, namespaceField: m.ModelNamespace,
			"tokenRateLimits": []any{map[string]any{"limit": int64(10000), "window": "1m"}}}},
	})
	for _, obj := range []*unstructured.Unstructured{policy, subscription} {
		err := f.Client.Create(ctx, obj)
		// An ambiguous create may have reached the server. Cleanup may recover it
		// by the unique label, but must never adopt an AlreadyExists collision.
		if !k8serr.IsAlreadyExists(err) {
			f.objects = append(f.objects, obj.DeepCopy())
		}
		if err != nil {
			return "", fmt.Errorf("create fresh %s: %w", obj.GetKind(), err)
		}
	}
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		for _, obj := range f.objects {
			current := obj.DeepCopy()
			if err := f.Client.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
				return false, err
			}
			if !fixtureReady(current) {
				return false, nil
			}
			if current.GetKind() == subscriptionKind {
				ready, err := f.meteringReady(ctx, current)
				if err != nil || !ready {
					return false, err
				}
			}
		}
		return true, nil
	})
	if err != nil {
		return "", fmt.Errorf("fresh MaaS policy/subscription and downstream token-policy enforcement: %w", err)
	}
	var minted struct {
		ID           string `json:"id"`
		Key          string `json:"key"`
		Subscription string `json:"subscription"`
		Ephemeral    bool   `json:"ephemeral"`
	}
	f.mintAttempted = true
	err = f.MaaS.request(ctx, http.MethodPost, keyPath, map[string]any{
		nameField: f.Name, subscriptionField: f.Name, "expiresIn": "1h", ephemeralField: true,
	}, &minted)
	// Retain the ID even if validation fails, so the returned key is revoked.
	f.keyID = minted.ID
	if err != nil {
		return "", fmt.Errorf("mint fresh subscription key: %w", err)
	}
	if minted.ID == "" || strings.TrimSpace(minted.Key) == "" || minted.Subscription != f.Name || !minted.Ephemeral {
		return "", errors.New("MaaS must return an ephemeral key bound to the fresh subscription")
	}
	f.Spoke.Subscription = f.Name
	return minted.Key, nil
}

func (f *Fixture) object(kind string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "maas.opendatahub.io/v1alpha1", "kind": kind,
		"metadata": map[string]any{nameField: f.Name, namespaceField: f.Spoke.MaaS.Namespace,
			"labels": map[string]any{fixtureLabel: f.Name}},
		"spec": spec,
	}}
}

func fixtureReady(obj *unstructured.Unstructured) bool {
	return fixtureCondition(obj, "Ready")
}

func fixtureCondition(obj *unstructured.Unstructured, conditionType string) bool {
	conditions, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, raw := range conditions {
		c, ok := raw.(map[string]any)
		if ok && c[typeField] == conditionType && c["status"] == conditionTrue {
			return true
		}
	}
	return false
}

// Subscription Ready means reconciliation succeeded; metering can still be
// propagating to Kuadrant. A one-shot inference must wait for enforcement of
// the policy generation that actually includes this fresh subscription.
func (f *Fixture) meteringReady(ctx context.Context, subscription *unstructured.Unstructured) (bool, error) {
	statuses, _, _ := unstructured.NestedSlice(subscription.Object, "status", "tokenRateLimitStatuses")
	for _, raw := range statuses {
		status, ok := raw.(map[string]any)
		if !ok || status[modelField] != f.Spoke.MaaS.ModelRef || status["ready"] != true {
			continue
		}
		name, _ := status[nameField].(string)
		namespace, _ := status[namespaceField].(string)
		if name == "" || namespace == "" {
			continue
		}
		policy := &unstructured.Unstructured{}
		policy.SetAPIVersion("kuadrant.io/v1alpha1")
		policy.SetKind("TokenRateLimitPolicy")
		err := f.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, policy)
		if k8serr.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		observed, _, err := unstructured.NestedInt64(policy.Object, "status", "observedGeneration")
		if err != nil {
			return false, err
		}
		expected := f.Spoke.MaaS.Namespace + "/" + f.Name
		includes := false
		for sub := range strings.SplitSeq(policy.GetAnnotations()["maas.opendatahub.io/subscriptions"], ",") {
			includes = includes || strings.TrimSpace(sub) == expected
		}
		return includes && observed == policy.GetGeneration() && fixtureCondition(policy, "Accepted") && fixtureCondition(policy, "Enforced"), nil
	}
	return false, nil
}

// Cleanup revokes keys before deleting their subscription. UID and label checks
// prevent deletion of existing resources, including replacement objects.
func (f *Fixture) Cleanup(ctx context.Context) error {
	var errs []error
	if f.mintAttempted {
		if err := f.revoke(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	for _, obj := range slices.Backward(f.objects) {
		if err := f.deleteOwned(ctx, obj); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (f *Fixture) deleteOwned(ctx context.Context, original *unstructured.Unstructured) error {
	current := original.DeepCopy()
	err := f.Client.Get(ctx, client.ObjectKeyFromObject(original), current)
	if k8serr.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if current.GetLabels()[fixtureLabel] != f.Name || (original.GetUID() != "" && current.GetUID() != original.GetUID()) {
		return fmt.Errorf("refusing cleanup of %s %s: ownership changed", original.GetKind(), f.Name)
	}
	uid := current.GetUID()
	if err := f.Client.Delete(ctx, current, client.Preconditions{UID: &uid}); err != nil && !k8serr.IsNotFound(err) {
		return err
	}
	return wait.PollUntilContextCancel(ctx, time.Second, true, func(ctx context.Context) (bool, error) {
		err := f.Client.Get(ctx, client.ObjectKeyFromObject(original), current)
		if k8serr.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}

func (f *Fixture) revoke(ctx context.Context) error {
	ids := []string{f.keyID}
	if f.keyID == "" {
		// A timed-out POST can have minted a key. Search metadata (never key
		// values), including ephemeral entries, and check exact ownership.
		var result struct {
			Data []struct {
				ID           string `json:"id"`
				Name         string `json:"name"`
				Subscription string `json:"subscription"`
				Ephemeral    bool   `json:"ephemeral"`
			} `json:"data"`
			HasMore bool `json:"has_more"`
		}
		err := f.MaaS.request(ctx, http.MethodPost, keyPath+"/search", map[string]any{
			"filters":    map[string]any{subscriptionField: f.Name, "includeEphemeral": true},
			"pagination": map[string]any{"limit": 100, "offset": 0},
		}, &result)
		if err != nil {
			return fmt.Errorf("recover key metadata after ambiguous mint: %w", err)
		}
		if result.HasMore {
			return errors.New("unexpected paginated keys for a fresh subscription; cleanup cannot confirm revocation")
		}
		ids = nil
		for _, key := range result.Data {
			if key.ID != "" && key.Name == f.Name && key.Subscription == f.Name && key.Ephemeral {
				ids = append(ids, key.ID)
			}
		}
	}
	var errs []error
	for _, id := range ids {
		err := f.MaaS.request(ctx, http.MethodDelete, keyPath+"/"+url.PathEscape(id), nil, nil)
		var status httpStatusError
		if err != nil && (!errors.As(err, &status) || status != http.StatusNotFound) {
			errs = append(errs, fmt.Errorf("revoke fixture key: %w", err))
		}
	}
	return errors.Join(errs...)
}
