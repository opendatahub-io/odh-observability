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

// Condition reasons carried by the object storage isolation errors below. A
// conflict is a rejected configuration; the other three are isolation
// questions that could not be answered. All of them withhold the claimants,
// but they differ in what the user has to do: separate the buckets, create the
// Secret, fix its contents, or fix cluster access and wait.
//
// They are named for storage rather than for Loki because the check is not a
// LokiStack concern: both claimants are LokiStacks today, and a future
// component that writes to object storage reports the same reasons on its own
// condition.
const (
	storageConflictReason      = "StorageConflict"
	storageCheckFailedReason   = "StorageCheckFailed"
	storageSecretMissingReason = "StorageSecretNotFound"
	storageSecretInvalidReason = "StorageSecretInvalid"
)

// errStorageSecretInvalid marks a storage Secret that exists but does not name
// a usable object store location — a bucketnames list with nothing usable in
// it, or an endpoint that is empty, hostless, or unparseable — so the
// classification below matches on the sentinel rather than on message text.
var errStorageSecretInvalid = errors.New("storage Secret does not name a usable object store location")

// Condition reasons for the LokiStack API calls that can fail without telling
// us anything about the stacks themselves. Both are reported Unknown.
const (
	lokiCRDCheckFailedReason       = "LokiCRDCheckFailed"
	lokiReadinessCheckFailedReason = "ReadinessCheckFailed"
)

// storageError is an object storage isolation failure that names the status
// condition reason and status it should be reported under, so the caller can
// recover both with errors.As instead of matching on message text.
//
// The status distinguishes a rejected configuration, which is a determinate
// answer and reports False, from a check that could not run, which reports
// Unknown so that consumers of the condition hold rather than tear down.
type storageError interface {
	error
	conditionReason() string
	conditionStatus() metav1.ConditionStatus
}

var (
	_ storageError = (*storageConflictError)(nil)
	_ storageError = (*storageCheckError)(nil)
)

// storageConflictError reports two or more claimants resolving to the same
// object store location. It wraps nothing: a rejected configuration has no
// underlying cause, it is the cause.
type storageConflictError struct {
	claimants []string
	shared    []string
}

// Error names the claimants without exposing the Secret-derived locations.
func (e *storageConflictError) Error() string {
	return fmt.Sprintf(
		"%s resolve to the same object storage: "+
			"both LokiStacks serve the \"application\" tenant and Loki keys objects by tenant rather than by stack, "+
			"so they would overwrite each other's index and chunks. Give each stack its own bucket",
		strings.Join(e.claimants, " and "))
}

func (e *storageConflictError) conditionReason() string {
	return storageConflictReason
}

// conditionStatus reports False: the claimants were compared and found to
// overlap.
func (e *storageConflictError) conditionStatus() metav1.ConditionStatus {
	return metav1.ConditionFalse
}

// storageCheckError reports that the isolation question could not be answered,
// which is not the same as answering that the claimants are isolated.
//
// The reason says why it could not be answered, because the three ways that
// happens call for different things from the user and only one of them is a
// malfunction. An empty reason falls back to the generic one, so a
// zero-valued literal still reports something sensible.
type storageCheckError struct {
	err    error
	reason string
}

// newStorageCheckError classifies a storage read failure once, at the point
// where the cause is still intact. A Secret that does not exist yet is an
// ordinary ordering race during install rather than a fault, and saying "check
// failed" there reads like an operator bug.
func newStorageCheckError(err error) *storageCheckError {
	reason := storageCheckFailedReason
	switch {
	case k8serr.IsNotFound(err):
		reason = storageSecretMissingReason
	case errors.Is(err, errStorageSecretInvalid):
		reason = storageSecretInvalidReason
	}
	return &storageCheckError{err: err, reason: reason}
}

func (e *storageCheckError) Error() string {
	return "cannot verify object storage isolation: " + e.err.Error()
}

// Unwrap keeps the cause reachable, so helpers such as k8serr.IsForbidden still
// recognize the API error underneath.
func (e *storageCheckError) Unwrap() error {
	return e.err
}

func (e *storageCheckError) conditionReason() string {
	if e.reason == "" {
		return storageCheckFailedReason
	}
	return e.reason
}

// conditionStatus reports Unknown: the claimants could not be compared at all,
// so nothing has been established about them. They are still withheld.
func (e *storageCheckError) conditionStatus() metav1.ConditionStatus {
	return metav1.ConditionUnknown
}
