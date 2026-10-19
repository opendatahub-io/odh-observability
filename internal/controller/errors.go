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
	"fmt"
	"strings"
)

// Condition reasons carried by the LokiStack storage isolation errors below. A
// conflict is a rejected configuration; a failed check is an isolation question
// that could not be answered. Both withhold the stacks, but only the first is
// the user's to fix.
const (
	lokiStorageConflictReason    = "LokiStorageConflict"
	lokiStorageCheckFailedReason = "LokiStorageCheckFailed"
)

// lokiStorageError is a LokiStack storage isolation failure that names the
// status condition reason it should be reported under, so the caller can
// recover the reason with errors.As instead of matching on message text.
type lokiStorageError interface {
	error
	conditionReason() string
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

// lokiStorageCheckError reports that the isolation question could not be
// answered, which is not the same as answering that the stacks are isolated.
type lokiStorageCheckError struct {
	err error
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
	return lokiStorageCheckFailedReason
}
