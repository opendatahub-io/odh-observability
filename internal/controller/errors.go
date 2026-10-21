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
	"strings"

	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Condition reasons carried by the LokiStack storage isolation errors below. A
// conflict is a rejected configuration; the other three are isolation
// questions that could not be answered. All of them withhold the stacks, but
// they differ in what the user has to do: separate the buckets, create the
// Secret, fix its contents, or fix cluster access and wait.
// The two Secret reasons name a Secret, they do not hold one; G101 matches on
// the identifier rather than the value.
const (
	lokiStorageConflictReason      = "LokiStorageConflict"
	lokiStorageCheckFailedReason   = "LokiStorageCheckFailed"
	lokiStorageSecretMissingReason = "LokiStorageSecretNotFound" //nolint:gosec // G101: condition reason, not a credential
	lokiStorageSecretInvalidReason = "LokiStorageSecretInvalid"  //nolint:gosec // G101: condition reason, not a credential
)

// errLokiStorageSecretInvalid marks a storage Secret that exists but does not
// name a bucket, so the classification below matches on the sentinel rather
// than on message text.
var errLokiStorageSecretInvalid = errors.New("storage Secret does not name a bucket")

// Condition reasons for the LokiStack API calls that can fail without telling
// us anything about the stacks themselves. Both are reported Unknown.
const (
	lokiCRDCheckFailedReason       = "LokiCRDCheckFailed"
	lokiReadinessCheckFailedReason = "ReadinessCheckFailed"
)

// lokiStorageError is a LokiStack storage isolation failure that names the
// status condition reason and status it should be reported under, so the caller
// can recover both with errors.As instead of matching on message text.
//
// The status distinguishes a rejected configuration, which is a determinate
// answer and reports False, from a check that could not run, which reports
// Unknown so that consumers of the condition hold rather than tear down.
type lokiStorageError interface {
	error
	conditionReason() string
	conditionStatus() metav1.ConditionStatus
}

var (
	_ lokiStorageError = (*lokiStorageConflictError)(nil)
	_ lokiStorageError = (*lokiStorageCheckError)(nil)
)

// lokiStorageConflictError reports two LokiStacks resolving to the same object
// store location. It wraps nothing: a rejected configuration has no underlying
// cause, it is the cause.
type lokiStorageConflictError struct {
	usageLogsSecret string
	logsSecret      string
	shared          []string
}

func (e *lokiStorageConflictError) Error() string {
	return fmt.Sprintf(
		"usageLogs.storage (secret %q) and logs.storage (secret %q) resolve to the same object storage [%s]: "+
			"both LokiStacks serve the \"application\" tenant and Loki keys objects by tenant rather than by stack, "+
			"so they would overwrite each other's index and chunks. Give each stack its own bucket",
		e.usageLogsSecret, e.logsSecret, strings.Join(e.shared, ", "))
}

func (e *lokiStorageConflictError) conditionReason() string {
	return lokiStorageConflictReason
}

// conditionStatus reports False: the stacks were compared and found to overlap.
func (e *lokiStorageConflictError) conditionStatus() metav1.ConditionStatus {
	return metav1.ConditionFalse
}

// lokiStorageCheckError reports that the isolation question could not be
// answered, which is not the same as answering that the stacks are isolated.
//
// The reason says why it could not be answered, because the three ways that
// happens call for different things from the user and only one of them is a
// malfunction. An empty reason falls back to the generic one, so a
// zero-valued literal still reports something sensible.
type lokiStorageCheckError struct {
	err    error
	reason string
}

// newLokiStorageCheckError classifies a storage read failure once, at the
// point where the cause is still intact. A Secret that does not exist yet is
// an ordinary ordering race during install rather than a fault, and saying
// "check failed" there reads like an operator bug.
func newLokiStorageCheckError(err error) *lokiStorageCheckError {
	reason := lokiStorageCheckFailedReason
	switch {
	case k8serr.IsNotFound(err):
		reason = lokiStorageSecretMissingReason
	case errors.Is(err, errLokiStorageSecretInvalid):
		reason = lokiStorageSecretInvalidReason
	}
	return &lokiStorageCheckError{err: err, reason: reason}
}

func (e *lokiStorageCheckError) Error() string {
	return "cannot verify LokiStack storage isolation: " + e.err.Error()
}

// Unwrap keeps the cause reachable, so helpers such as k8serr.IsForbidden still
// recognize the API error underneath.
func (e *lokiStorageCheckError) Unwrap() error {
	return e.err
}

func (e *lokiStorageCheckError) conditionReason() string {
	if e.reason == "" {
		return lokiStorageCheckFailedReason
	}
	return e.reason
}

// conditionStatus reports Unknown: the stacks could not be compared at all, so
// nothing has been established about them. The stacks are still withheld.
func (e *lokiStorageCheckError) conditionStatus() metav1.ConditionStatus {
	return metav1.ConditionUnknown
}
