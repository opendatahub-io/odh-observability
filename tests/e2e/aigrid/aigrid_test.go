package aigrid

import (
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func validConfig() Config {
	s := Spoke{Context: "spoke1", ClusterName: "source-one", ClusterID: "cluster-id-one", OrganizationID: "org-1",
		Subscription: "dedicated-sub", Model: "model-alias", ModelLabel: "served-model", InferenceURL: "https://gateway-one.example/v1/chat/completions",
		MaaS:                MaaSConfig{APIURL: "https://gateway-one.example/maas-api", Namespace: "maas", ModelRef: "model-ref", ModelNamespace: "models", Username: "test-user"},
		ControllerNamespace: "applications", ExpectedControllerImage: "registry.example/observability@sha256:" + strings.Repeat("a", 64)}
	two := s
	two.Context, two.ClusterName, two.ClusterID = "spoke2", "source-two", "cluster-id-two"
	two.InferenceURL, two.MaaS.APIURL = "https://gateway-two.example/v1/chat/completions", "https://gateway-two.example/maas-api"
	return Config{HubContext: "hub", Grafana: GrafanaConfig{Image: "registry.example/grafana:test"},
		Metrics: Backend{URL: "https://metrics.example", TokenFile: "metrics-token"},
		Logs:    Backend{URL: "https://logs.example/api/logs/v1/infrastructure", TokenFile: "logs-token"},
		Spokes:  []Spoke{s, two}, MetricNameRE: "authorized_hits(_total)?", Timeout: 5 * time.Minute, PollInterval: 5 * time.Second}
}

func TestConfigRejectsAmbiguousAndUnsafeFixtures(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
	}{
		{"missing organization", func(c *Config) { c.Spokes[0].OrganizationID = "" }},
		{"missing owner", func(c *Config) { c.Spokes[0].MaaS.Username = "" }},
		{"missing model reference", func(c *Config) { c.Spokes[0].MaaS.ModelRef = "" }},
		{"HTTP MaaS API", func(c *Config) { c.Spokes[0].MaaS.APIURL = "http://management.example" }},
		{"missing model", func(c *Config) { c.Spokes[0].Model = "" }},
		{"context aliases", func(c *Config) { c.Spokes[1].Context = c.Spokes[0].Context }},
		{"hub used as spoke", func(c *Config) { c.Spokes[0].Context = c.HubContext }},
		{"same cluster ID", func(c *Config) { c.Spokes[1].ClusterID = c.Spokes[0].ClusterID }},
		{"same source label", func(c *Config) { c.Spokes[1].ClusterName = c.Spokes[0].ClusterName }},
		{"one spoke", func(c *Config) { c.Spokes = c.Spokes[:1] }},
		{"unverified image tag", func(c *Config) { c.Spokes[0].ExpectedControllerImage = "registry.example/observability:latest" }},
		{"HTTP datasource", func(c *Config) { c.Metrics.URL = "http://metrics.example" }},
		{"credential URL", func(c *Config) { c.Logs.URL = "https://user:secret@logs.example" }},
		{"sensitive query", func(c *Config) { c.Spokes[0].InferenceURL += "?api_key=secret" }},
		{"ingest base", func(c *Config) { c.Logs.URL += "/otlp" }},
		{"unbounded wait", func(c *Config) { c.Timeout = 0 }},
		{"invalid regex", func(c *Config) { c.MetricNameRE = "[" }},
	}
	require.NoError(t, validConfig().Validate())
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.edit(&c)
			err := c.Validate()
			require.Error(t, err)
			require.NotContains(t, err.Error(), "user:secret")
			require.NotContains(t, err.Error(), "api_key=secret")
		})
	}
}

func TestLoadDefaultsAndRejectsUnknownFields(t *testing.T) {
	c := validConfig()
	c.Timeout, c.PollInterval, c.MetricNameRE, c.Spokes[0].ModelLabel = 0, 0, "", ""
	data, err := yaml.Marshal(c)
	require.NoError(t, err)
	file := filepath.Join(t.TempDir(), "grid.yaml")
	require.NoError(t, os.WriteFile(file, data, 0o600))
	loaded, err := Load(file)
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, loaded.Timeout)
	require.Equal(t, 5*time.Second, loaded.PollInterval)
	require.Equal(t, loaded.Spokes[0].Model, loaded.Spokes[0].ModelLabel)
	require.Equal(t, filepath.Join(filepath.Dir(file), "metrics-token"), loaded.Metrics.TokenFile)
	for _, suffix := range []string{"misspelledField: secret-value\n", "---\nhubContext: second\n"} {
		require.NoError(t, os.WriteFile(file, append(data, []byte(suffix)...), 0o600))
		_, err := Load(file)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret-value")
	}
}

func TestRunSpecificOATSContracts(t *testing.T) {
	c := validConfig()
	for _, s := range c.Spokes {
		t.Run(s.Context, func(t *testing.T) {
			data, err := RenderCase(s, c.MetricNameRE, "unique-run", 42, time.Unix(1800000000, 0))
			require.NoError(t, err)
			var doc struct {
				Expected struct {
					Metrics []struct {
						PromQL string `yaml:"promql"`
						Count  string `yaml:"count"`
						Value  string `yaml:"value"`
					} `yaml:"metrics"`
					Logs []struct {
						LogQL string `yaml:"logql"`
						Count string `yaml:"count"`
						Match []struct {
							MatchType  string `yaml:"match_type"`
							Name       string `yaml:"name"`
							Attributes []struct {
								Key   string `yaml:"key"`
								Value string `yaml:"value"`
							} `yaml:"attributes"`
						} `yaml:"match"`
					} `yaml:"logs"`
				} `yaml:"expected"`
			}
			require.NoError(t, yaml.Unmarshal(data, &doc))
			require.Len(t, doc.Expected.Metrics, 1)
			require.Len(t, doc.Expected.Logs, 1)
			metric, log := doc.Expected.Metrics[0], doc.Expected.Logs[0]
			require.Len(t, log.Match, 1, "force structured GCX JSON, including empty-result detection")
			require.Equal(t, "regexp", log.Match[0].MatchType)
			require.Equal(t, "^200 /v1/chat/completions$", log.Match[0].Name)
			require.Len(t, log.Match[0].Attributes, 8)
			// Each required equality prevents missing/wrong attribution from
			// satisfying the positive row-count assertions.
			for _, matcher := range []string{`organization_id="org-1"`, `subscription="dedicated-sub"`, `model="served-model"`} {
				require.Contains(t, metric.PromQL, matcher)
				require.Contains(t, log.LogQL, matcher)
			}
			require.Contains(t, metric.PromQL, fmt.Sprintf(`cluster=%q,clusterID=%q`, s.ClusterName, s.ClusterID))
			require.Contains(t, log.LogQL, fmt.Sprintf(`cluster_name=%q,cluster_id=%q`, s.ClusterName, s.ClusterID))
			require.Contains(t, metric.PromQL, `ai_grid_collection_path="rhai-collector"`)
			require.Contains(t, metric.PromQL, "> 42")
			require.Contains(t, metric.PromQL, "vector(time()) >= 1800000000")
			require.Equal(t, "> 42", metric.Value)
			require.Equal(t, ">= 1", metric.Count)
			require.Equal(t, ">= 1", log.Count)
			require.NotContains(t, log.LogQL, "unique-run", "run correlation is the fresh subscription, not the request URL")
			for _, fragment := range []string{"tokens_total > 0", "tokens_prompt > 0", "tokens_completion > 0", `response_type="hit"`} {
				require.Contains(t, log.LogQL, fragment)
			}
			require.NotContains(t, log.LogQL, "| json", "usage body is not JSON; token fields are structured metadata")
			require.NotContains(t, string(data), "grid_tenant_id")
			valid := map[string]string{
				"organization_id": s.OrganizationID, "cluster_name": s.ClusterName, "cluster_id": s.ClusterID,
				"subscription": s.Subscription, "model": s.ModelLabel,
				"tokens_total": "5", "tokens_prompt": "3", "tokens_completion": "2",
			}
			for _, scenario := range []struct {
				name, key, value, line string
				want                   bool
			}{
				{"valid plain request", "", "", "200 /v1/chat/completions", true},
				{"missing organization", "organization_id", "", "200 /v1/chat/completions", false},
				{"wrong organization", "organization_id", "wrong-org", "200 /v1/chat/completions", false},
				{"organization prefix", "organization_id", s.OrganizationID + "-other", "200 /v1/chat/completions", false},
				{"wrong source name", "cluster_name", "other-cluster", "200 /v1/chat/completions", false},
				{"wrong source ID", "cluster_id", "other-id", "200 /v1/chat/completions", false},
				{"wrong model", "model", "other-model", "200 /v1/chat/completions", false},
				{"missing usage", "tokens_total", "", "200 /v1/chat/completions", false},
				{"zero usage", "tokens_completion", "0", "200 /v1/chat/completions", false},
				{"historical subscription", "subscription", "previous-run", "200 /v1/chat/completions", false},
				{"wrong request path", "", "", "200 /v1/models", false},
				{"failed inference", "", "", "500 /v1/chat/completions", false},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					attrs := maps.Clone(valid)
					if scenario.key != "" {
						attrs[scenario.key] = scenario.value
					}
					match, regexErr := regexp.MatchString(log.Match[0].Name, scenario.line)
					require.NoError(t, regexErr)
					for _, attr := range log.Match[0].Attributes {
						attributeMatch, regexErr := regexp.MatchString(attr.Value, attrs[attr.Key])
						require.NoError(t, regexErr)
						match = match && attributeMatch
					}
					require.Equal(t, scenario.want, match, "generated OATS regexp row contract")
				})
			}
		})
	}
}

func TestQueryEscaping(t *testing.T) {
	s := validConfig().Spokes[0]
	s.OrganizationID = "org\"} or vector(1) #\n"
	q := CounterQuery(s, "authorized_hits")
	require.Contains(t, q, `organization_id="org\"} or vector(1) #\n"`)
	require.NotContains(t, q, "#\n", "a literal newline must not escape the matcher")
}

func TestGCXUsesOnlyTemporaryGrafanaAndNamedDatasources(t *testing.T) {
	data, err := GCXConfig("https://temporary.example", "random-password", "/tmp/route-ca.pem")
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, yaml.Unmarshal(data, &doc))
	require.Equal(t, 1, doc["version"])
	require.Contains(t, string(data), "prometheus: acm-thanos")
	require.Contains(t, string(data), "loki: hub-loki")
	require.Contains(t, string(data), "auth-method: basic")
	require.Contains(t, string(data), "ca-file: /tmp/route-ca.pem")
	env := OATSEnv([]string{"PATH=/bin", "GRAFANA_TOKEN=wrong-token", "GRAFANA_SERVER=wrong-host", "GCX_CONFIG=wrong-file", "OATS_TIMEOUT=1ms"}, "/private/gcx.yaml")
	require.Equal(t, []string{"PATH=/bin", "GCX_CONFIG=/private/gcx.yaml", "GCX_TELEMETRY=disabled"}, env)
	require.Equal(t, "error <redacted> <redacted>", Redact("error token password", "token", "password"))
	require.Equal(t, "Basic <redacted>", Redact("Basic "+base64.StdEncoding.EncodeToString([]byte("admin:random-password")), "random-password"))
}

func TestGrafanaCounterResponseValidation(t *testing.T) {
	tests := []struct {
		name, response string
		emptyIsZero    bool
		want           float64
		wantError      bool
	}{
		{"valid", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"42"]}]}}`, false, 42, false},
		{"missing baseline is zero", `{"status":"success","data":{"resultType":"vector","result":[]}}`, true, 0, false},
		{"missing readiness fails", `{"status":"success","data":{"resultType":"vector","result":[]}}`, false, 0, true},
		{"query error", `{"status":"error","error":"private-token"}`, true, 0, true},
		{"partial response", `{"status":"success","warnings":["partial"],"data":{"resultType":"vector","result":[]}}`, true, 0, true},
		{"NaN", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"NaN"]}]}}`, false, 0, true},
		{"negative", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"-1"]}]}}`, false, 0, true},
		{"ambiguous samples", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1"]},{"value":[1,"2"]}]}}`, false, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "/api/datasources/proxy/uid/acm-thanos/api/v1/query", r.URL.Path)
				assert.Equal(t, "sum(metric)", r.URL.Query().Get("query"))
				_, _ = w.Write([]byte(tc.response))
			}))
			defer server.Close()
			g := Grafana{Client: server.Client(), URL: server.URL, Password: "password"}
			actual, err := g.Number(t.Context(), "sum(metric)", tc.emptyIsZero)
			if tc.wantError {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "private-token")
			} else {
				require.NoError(t, err)
				require.InDelta(t, tc.want, actual, 0.0001)
			}
		})
	}
}

func TestDatasourceProvisioningAndErrorsDoNotExposeCredentials(t *testing.T) {
	file := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(file, []byte("private-backend-token\n"), 0o600))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "admin", user)
		assert.Equal(t, "temporary-password", password)
		assert.Equal(t, "/api/datasources", r.URL.Path)
		var doc map[string]any
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&doc))
		assert.Equal(t, "acm-thanos", doc["uid"])
		assert.Equal(t, "ACM Thanos", doc["name"])
		assert.Equal(t, map[string]any{"httpHeaderName1": "Authorization", "tlsSkipVerify": false}, doc["jsonData"])
		assert.Equal(t, map[string]any{"httpHeaderValue1": "Bearer private-backend-token"}, doc["secureJsonData"])
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("private-backend-token temporary-password"))
	}))
	defer server.Close()
	g := Grafana{Client: server.Client(), URL: server.URL, Password: "temporary-password"}
	err := g.AddDatasource(t.Context(), MetricsUID, "ACM Thanos", "prometheus", Backend{URL: "https://metrics.example", TokenFile: file})
	require.EqualError(t, err, "HTTP status 401")
}

func TestInferenceIsIdentifiableAndRequiresRealTokenUsage(t *testing.T) {
	for _, body := range []string{
		`{"choices":[{}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`,
		`{"choices":[{}]}`, `{"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`,
		`{"choices":[{}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":99}}`,
	} {
		t.Run(body, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Empty(t, r.URL.RawQuery, "ordinary MaaS inference must not gain query parameters")
				assert.Equal(t, "/v1/chat/completions", r.URL.Path)
				assert.Equal(t, "Bearer inference-key", r.Header.Get("Authorization"))
				var payload map[string]any
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
				assert.Equal(t, false, payload["stream"])
				assert.Equal(t, "model-alias", payload["model"])
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			s := validConfig().Spokes[0]
			s.InferenceURL = server.URL + "/v1/chat/completions"
			err := Infer(t.Context(), server.Client(), s, "inference-key", "run-unique")
			if strings.Contains(body, `"total_tokens":5`) && !strings.Contains(body, `"choices":[]`) {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.EqualValues(t, 1, calls.Load(), "inference POST must not be retried")
		})
	}
}

func TestTLSVerificationAndRedirectCredentialIsolation(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"database":"ok"}`))
	}))
	defer server.Close()
	c, err := HTTPClient("")
	require.NoError(t, err)
	g := Grafana{Client: c, URL: server.URL, Password: "password"}
	require.Error(t, g.Request(t.Context(), http.MethodGet, "/api/health", nil, nil), "untrusted certificate must fail")
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	require.NoError(t, os.WriteFile(caFile, caPEM, 0o600))
	g.Client, err = HTTPClient(caFile)
	require.NoError(t, err)
	require.NoError(t, g.Request(t.Context(), http.MethodGet, "/api/health", nil, nil), "explicit CA must enable verified TLS")

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://other.example/credentials", http.StatusFound)
	}))
	defer redirect.Close()
	g.URL = redirect.URL
	require.EqualError(t, g.Request(t.Context(), http.MethodGet, "/api/health", nil, nil), "HTTP status 302")
}

func TestCredentialFilesRejectEmptyAndMultilineTokens(t *testing.T) {
	file := filepath.Join(t.TempDir(), "credential")
	for _, value := range []string{"", " \n", "first\nsecond", "first\rsecond"} {
		require.NoError(t, os.WriteFile(file, []byte(value), 0o600))
		_, err := ReadCredential(file)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "first")
	}
}
