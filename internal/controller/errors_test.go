/*
Copyright 2025.

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

package controller

import (
	"errors"
	"fmt"
	"testing"

	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestStorageError_ConditionStatus pins the distinction the two storage errors
// exist to carry. Both withhold the claimants, so the error alone says nothing
// about how to report them; the status is what validateStorageIsolation reads
// to choose between MarkFalse and MarkUnknown.
//
// Getting this backwards is silent and costly in both directions. Reporting a
// check failure as False clears the published endpoint and has garbage
// collection remove the usage logs collector over one unreadable Secret.
// Reporting a real conflict as Unknown hides a configuration the user has to
// fix behind a status that reads like a transient blip.
func TestStorageError_ConditionStatus(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		err        storageError
		wantStatus metav1.ConditionStatus
		wantReason string
	}{
		{
			// The buckets were compared and found to overlap. A determinate
			// answer, and the user's to fix.
			name: "conflict is a determinate answer",
			err: &storageConflictError{
				claimants: []string{`usageLogs.storage (secret "usage-logs-secret")`, `logs.storage (secret "logs-secret")`},
				shared:    []string{"//s3.example.com/shared-bucket"},
			},
			wantStatus: metav1.ConditionFalse,
			wantReason: storageConflictReason,
		},
		{
			// The comparison never ran, so nothing is known either way. Built
			// bare rather than through the constructor, which pins the
			// zero-reason fallback.
			name:       "failed check establishes nothing",
			err:        &storageCheckError{err: errors.New("secrets is forbidden")},
			wantStatus: metav1.ConditionUnknown,
			wantReason: storageCheckFailedReason,
		},
		{
			// A Secret that has not been applied yet is an ordering race, not
			// a fault, but it leaves the comparison just as unanswered.
			name:       "missing secret establishes nothing",
			err:        newStorageCheckError(k8serr.NewNotFound(schema.GroupResource{Resource: "secrets"}, "logs-secret")),
			wantStatus: metav1.ConditionUnknown,
			wantReason: storageSecretMissingReason,
		},
		{
			name:       "unusable secret establishes nothing",
			err:        newStorageCheckError(fmt.Errorf("no bucket: %w", errStorageSecretInvalid)),
			wantStatus: metav1.ConditionUnknown,
			wantReason: storageSecretInvalidReason,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := test.err.conditionStatus(); got != test.wantStatus {
				t.Errorf("conditionStatus: want %s, got %s", test.wantStatus, got)
			}
			if got := test.err.conditionReason(); got != test.wantReason {
				t.Errorf("conditionReason: want %s, got %s", test.wantReason, got)
			}
			if test.err.Error() == "" {
				t.Error("a storage error must explain itself: the message reaches the user as the condition message")
			}
		})
	}
}

// TestNewStorageCheckError_ClassifiesCause covers the classification
// itself rather than the reasons it produces.
//
// The three cases ask different things of the user — create the Secret, fix
// its contents, fix access and wait — and only the last is a malfunction.
// Collapsing them would put an ordinary install-ordering race under a reason
// that reads as an operator bug, which is what this guards against.
func TestNewStorageCheckError_ClassifiesCause(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		cause      error
		wantReason string
	}{
		{
			name:       "not found is a missing secret",
			cause:      k8serr.NewNotFound(schema.GroupResource{Resource: "secrets"}, "logs-secret"),
			wantReason: storageSecretMissingReason,
		},
		{
			// Matched on the sentinel, not on the message, so rewording the
			// wrapper text cannot silently reclassify it.
			name:       "the sentinel is an unusable secret",
			cause:      fmt.Errorf("secret test-ns/logs-secret has no usable bucketnames entry: %w", errStorageSecretInvalid),
			wantReason: storageSecretInvalidReason,
		},
		{
			// Anything else is the cluster, not the configuration. This is
			// the case that must keep the generic reason: a Forbidden is not
			// something the user fixes by editing the Secret.
			name:       "forbidden stays a failed check",
			cause:      k8serr.NewForbidden(schema.GroupResource{Resource: "secrets"}, "logs-secret", errors.New("no permission")),
			wantReason: storageCheckFailedReason,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := newStorageCheckError(test.cause)
			if got := err.conditionReason(); got != test.wantReason {
				t.Errorf("conditionReason: want %s, got %s", test.wantReason, got)
			}
			// Classification must not consume the cause: the gate and its
			// tests still reach through for k8serr.IsForbidden.
			if !errors.Is(err, test.cause) {
				t.Error("the cause must stay reachable after classification")
			}
		})
	}
}

// TestStorageCheckError_UnwrapsCause keeps the API error underneath reachable.
// checkStorageIsolation wraps the cause rather than replacing it so that
// helpers like k8serr.IsForbidden still work on the result, which is how a
// permission problem is told apart from any other reason the check did not run.
func TestStorageCheckError_UnwrapsCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("connection refused")
	wrapped := &storageCheckError{err: cause}

	if !errors.Is(wrapped, cause) {
		t.Error("the cause must stay reachable through errors.Is")
	}

	var asStorageErr storageError
	if !errors.As(error(wrapped), &asStorageErr) {
		t.Fatal("a *storageCheckError must satisfy storageError, which is how the action recovers the status")
	}
	if got := asStorageErr.conditionStatus(); got != metav1.ConditionUnknown {
		t.Errorf("status recovered through the interface: want Unknown, got %s", got)
	}
}
