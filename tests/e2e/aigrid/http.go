package aigrid

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func HTTPClient(caFile string) (*http.Client, error) {
	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("default HTTP transport must be an *http.Transport")
	}
	transport := baseTransport.Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, err
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA file contains no certificates")
		}
		transport.TLSClientConfig.RootCAs = pool
	}
	return &http.Client{
		Transport: transport, Timeout: 30 * time.Second,
		// Never forward credentials to an unexpected redirect target.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
}

type Grafana struct {
	Client   *http.Client
	URL      string
	Password string
}

// Request omits response bodies from HTTP errors: a backend/proxy can echo
// authentication headers. Callers identify the datasource and assertion instead.
func (g Grafana) Request(ctx context.Context, method, path string, body, into any) error {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(g.URL, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.SetBasicAuth("admin", g.Password)
	req.Header.Set("Content-Type", "application/json")
	data, err := doRequest(g.Client, req)
	if err != nil {
		return err
	}
	if into != nil {
		if err := json.Unmarshal(data, into); err != nil {
			return errors.New("invalid JSON response from Grafana/datasource")
		}
	}
	return nil
}

func (g Grafana) AddDatasource(ctx context.Context, uid, name, kind string, b Backend) error {
	token, err := ReadCredential(b.TokenFile)
	if err != nil {
		return err
	}
	jsonData := map[string]any{"httpHeaderName1": "Authorization", "tlsSkipVerify": false}
	secure := map[string]string{"httpHeaderValue1": "Bearer " + token}
	if b.CAFile != "" {
		pem, err := os.ReadFile(b.CAFile)
		if err != nil {
			return err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return errors.New("datasource CA file contains no certificates")
		}
		jsonData["tlsAuthWithCACert"] = true
		secure["tlsCACert"] = string(pem)
	}
	return g.Request(ctx, http.MethodPost, "/api/datasources", map[string]any{
		"uid": uid, nameField: name, typeField: kind, "access": "proxy", "url": strings.TrimRight(b.URL, "/"),
		"jsonData": jsonData, "secureJsonData": secure, "editable": false,
	}, nil)
}

func (g Grafana) Number(ctx context.Context, query string, emptyIsZero bool) (float64, error) {
	path := "/api/datasources/proxy/uid/" + MetricsUID + "/api/v1/query?" + url.Values{"query": {query}}.Encode()
	var response struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Value []json.RawMessage `json:"value"`
			} `json:"result"`
		} `json:"data"`
		Warnings []string `json:"warnings"`
	}
	if err := g.Request(ctx, http.MethodGet, path, nil, &response); err != nil {
		return 0, err
	}
	if response.Status != "success" || response.Data.ResultType != "vector" || len(response.Warnings) > 0 {
		return 0, errors.New("ACM Thanos query failed, returned a non-vector result, or reported partial results/warnings")
	}
	if len(response.Data.Result) == 0 && emptyIsZero {
		return 0, nil
	}
	if len(response.Data.Result) != 1 || len(response.Data.Result[0].Value) != 2 {
		return 0, errors.New("expected one numeric ACM Thanos sample")
	}
	var raw string
	if err := json.Unmarshal(response.Data.Result[0].Value[1], &raw); err != nil {
		return 0, errors.New("ACM Thanos sample value must be a string")
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0, errors.New("ACM Thanos sample must be finite and nonnegative")
	}
	return value, nil
}

// Infer sends exactly one non-streaming request to the configured URL unchanged.
// The fresh subscription bound to apiKey identifies this run's telemetry.
// No retry is performed: retrying a POST would add unaccounted inference traffic.
func Infer(ctx context.Context, client *http.Client, s Spoke, apiKey, runID string) error {
	body, err := json.Marshal(map[string]any{
		modelField: s.Model, "stream": false, "max_tokens": 16,
		"messages": []map[string]string{{"role": "user", "content": "Reply with a short greeting. Test request " + runID}},
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.InferenceURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", runID)
	data, err := doRequest(client, req)
	if err != nil {
		return err
	}
	var response struct {
		Choices []json.RawMessage `json:"choices"`
		Usage   struct {
			Prompt     int64 `json:"prompt_tokens"`
			Completion int64 `json:"completion_tokens"`
			Total      int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return errors.New("inference response is not JSON")
	}
	if len(response.Choices) == 0 || response.Usage.Prompt <= 0 || response.Usage.Completion <= 0 ||
		response.Usage.Total != response.Usage.Prompt+response.Usage.Completion {
		return errors.New("inference must return choices and populated, consistent positive token usage")
	}
	return nil
}

func doRequest(client *http.Client, req *http.Request) ([]byte, error) {
	response, err := client.Do(req)
	if err != nil {
		// Keep URL/query credentials and transport diagnostics out of artifacts.
		return nil, errors.New("HTTP request failed (check endpoint reachability, TLS and timeout)")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, httpStatusError(response.StatusCode)
	}
	const limit = 2 << 20
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, errors.New("read HTTP response")
	}
	if len(data) > limit {
		return nil, errors.New("HTTP response exceeds 2 MiB")
	}
	return data, nil
}

type httpStatusError int

func (s httpStatusError) Error() string { return fmt.Sprintf("HTTP status %d", s) }
