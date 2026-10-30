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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func TestMethodGateHandler(t *testing.T) {
	// Upstream records whether it was ever reached, so we can assert that rejected
	// methods never make it past the gate. atomic: httptest serves on another
	// goroutine than the assertions below.
	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("upstream"))
	}))
	defer upstream.Close()

	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parsing upstream URL: %v", err)
	}

	gate := httptest.NewServer(methodGateHandler(upstreamURL, false))
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
			upstreamHits.Store(0)

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
			forwarded := upstreamHits.Load() > 0
			if forwarded != tc.wantForwarded {
				t.Errorf("%s: forwarded to upstream = %t, want %t", tc.method, forwarded, tc.wantForwarded)
			}
		})
	}
}

func TestMethodGateFormPost(t *testing.T) {
	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "body read failed", http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Upstream-Query", r.URL.RawQuery)
		_, _ = w.Write(body)
	}))
	defer upstream.Close()
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	gate := httptest.NewServer(methodGateHandler(upstreamURL, true))
	defer gate.Close()

	tests := []struct {
		name, path, contentType, encoding, body string
		wantStatus                              int
	}{
		{"query", prometheusInstantQueryPath, formContentType, "", "query=up", http.StatusOK},
		{"range", "/api/v1/query_range", "application/x-www-form-urlencoded; charset=UTF-8", "", "query=up&start=1&end=2", http.StatusOK},
		{"exemplars", "/api/v1/query_exemplars", formContentType, "", "query=up", http.StatusOK},
		{"series", "/api/v1/series", formContentType, "", "match%5B%5D=up", http.StatusOK},
		{"labels", "/api/v1/labels", formContentType, "", "match%5B%5D=up", http.StatusOK},
		{"body namespace", prometheusInstantQueryPath, formContentType, "", "query=up&namespace=other", http.StatusForbidden},
		{"encoded namespace", prometheusInstantQueryPath, formContentType, "", "query=up&%6eamespace=other", http.StatusForbidden},
		{"empty namespace", prometheusInstantQueryPath, formContentType, "", "query=up&namespace=", http.StatusForbidden},
		{"malformed form", prometheusInstantQueryPath, formContentType, "", "query=up&namespace=%zz", http.StatusBadRequest},
		{"semicolon", prometheusInstantQueryPath, formContentType, "", "query=up;namespace=other", http.StatusBadRequest},
		{"multipart", prometheusInstantQueryPath, "multipart/form-data; boundary=x", "", "namespace=other", http.StatusUnsupportedMediaType},
		{"encoded body", prometheusInstantQueryPath, formContentType, "gzip", "query=up", http.StatusUnsupportedMediaType},
		{"mutating endpoint", "/api/v2/silences", formContentType, "", "query=up", http.StatusForbidden},
		{"oversized", prometheusInstantQueryPath, formContentType, "", strings.Repeat("a", (10<<20)+1), http.StatusRequestEntityTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			upstreamHits.Store(0)
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
				gate.URL+tc.path+"?namespace=a&namespace=b", strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", tc.contentType)
			if tc.encoding != "" {
				req.Header.Set("Content-Encoding", tc.encoding)
			}
			resp, err := gate.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if got := upstreamHits.Load(); got != 0 && tc.wantStatus != http.StatusOK {
				t.Errorf("rejected request reached upstream %d times", got)
			} else if tc.wantStatus == http.StatusOK && got != 1 {
				t.Errorf("allowed request reached upstream %d times", got)
			}
			if tc.wantStatus == http.StatusOK {
				body, err := io.ReadAll(resp.Body)
				if err != nil || string(body) != tc.body {
					t.Errorf("forwarded body = %q, err = %v, want %q", body, err, tc.body)
				}
				if got := resp.Header.Get("X-Upstream-Query"); got != "namespace=a&namespace=b" {
					t.Errorf("forwarded URL query = %q", got)
				}
			}
		})
	}
}
