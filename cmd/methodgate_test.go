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

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestMethodGateHandler(t *testing.T) {
	// Upstream records whether it was ever reached, so we can assert that rejected
	// methods never make it past the gate.
	var upstreamHits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("upstream"))
	}))
	defer upstream.Close()

	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parsing upstream URL: %v", err)
	}

	gate := httptest.NewServer(methodGateHandler(upstreamURL))
	defer gate.Close()

	testCases := []struct {
		method        string
		wantStatus    int
		wantForwarded bool
	}{
		{http.MethodGet, http.StatusOK, true},
		{http.MethodHead, http.StatusOK, true},
		{http.MethodPost, http.StatusForbidden, false},
		{http.MethodPut, http.StatusForbidden, false},
		{http.MethodDelete, http.StatusForbidden, false},
		{http.MethodPatch, http.StatusForbidden, false},
		{http.MethodOptions, http.StatusForbidden, false},
	}

	for _, tc := range testCases {
		t.Run(tc.method, func(t *testing.T) {
			upstreamHits = 0

			req, err := http.NewRequestWithContext(context.Background(), tc.method, gate.URL+"/api/v1/query?namespace=allowed", nil)
			if err != nil {
				t.Fatalf("building request: %v", err)
			}
			resp, err := gate.Client().Do(req)
			if err != nil {
				t.Fatalf("issuing %s: %v", tc.method, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				t.Errorf("%s: got status %d, want %d", tc.method, resp.StatusCode, tc.wantStatus)
			}
			forwarded := upstreamHits > 0
			if forwarded != tc.wantForwarded {
				t.Errorf("%s: forwarded to upstream = %t, want %t", tc.method, forwarded, tc.wantForwarded)
			}
		})
	}
}
