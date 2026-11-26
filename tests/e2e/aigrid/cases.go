package aigrid

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	MetricsUID            = "acm-thanos"
	LogsUID               = "hub-loki"
	GCXContext            = "ai-grid"
	grafanaPasswordKey    = "password"
	modelField            = "model"
	subscriptionField     = "subscription"
	organizationIDField   = "organization_id"
	clusterNameLogField   = "cluster_name"
	clusterIDLogField     = "cluster_id"
	tokensTotalField      = "tokens_total"
	tokensPromptField     = "tokens_prompt"
	tokensCompletionField = "tokens_completion"
	nameField             = "name"
	valueField            = "value"
	keyField              = "key"
	namespaceField        = "namespace"
	ephemeralField        = "ephemeral"
)

func CollectionQuery(s Spoke) string {
	return fmt.Sprintf(`min(up{cluster=%s,clusterID=%s,ai_grid_collection_path="rhai-collector"})`,
		strconv.Quote(s.ClusterName), strconv.Quote(s.ClusterID))
}

func CounterQuery(s Spoke, metricNameRE string) string {
	return fmt.Sprintf(`sum({__name__=~%s,cluster=%s,clusterID=%s,organization_id=%s,subscription=%s,model=%s,ai_grid_collection_path="rhai-collector"})`,
		strconv.Quote(metricNameRE), strconv.Quote(s.ClusterName), strconv.Quote(s.ClusterID),
		strconv.Quote(s.OrganizationID), strconv.Quote(s.Subscription), strconv.Quote(s.ModelLabel))
}

// LogQuery selects the run's fresh subscription. Usage fields are Loki structured
// metadata, not JSON in the log body (which is the HTTP status and request path).
func LogQuery(s Spoke) string {
	selector := fmt.Sprintf(`{service_name="models-as-a-service",cluster_name=%s,cluster_id=%s,organization_id=%s,subscription=%s,model=%s,response_type="hit"}`,
		strconv.Quote(s.ClusterName), strconv.Quote(s.ClusterID), strconv.Quote(s.OrganizationID),
		strconv.Quote(s.Subscription), strconv.Quote(s.ModelLabel))
	return selector + ` | tokens_total > 0 | tokens_prompt > 0 | tokens_completion > 0 | __error__=""`
}

// RenderCase requires a post-request increase and excludes historical evaluations
// even when GCX performs a range query. A missing or wrongly attributed series
// yields no rows; it cannot satisfy the positive OATS count assertion.
func RenderCase(s Spoke, metricNameRE, runID string, baseline float64, started time.Time) ([]byte, error) {
	if s.Subscription == "" {
		return nil, errors.New("a fresh subscription must be created before rendering assertions")
	}
	u, err := url.Parse(s.InferenceURL)
	if err != nil {
		return nil, errors.New("invalid inference URL")
	}
	metric := fmt.Sprintf("(%s > %s) and on() (vector(time()) >= %d)",
		CounterQuery(s, metricNameRE), strconv.FormatFloat(baseline, 'g', -1, 64), started.Unix())
	attributes := make([]map[string]string, 0, 8)
	for _, label := range []struct{ key, value string }{
		{organizationIDField, s.OrganizationID}, {clusterNameLogField, s.ClusterName}, {clusterIDLogField, s.ClusterID},
		{subscriptionField, s.Subscription}, {modelField, s.ModelLabel},
	} {
		attributes = append(attributes, map[string]string{keyField: label.key, valueField: "^" + regexp.QuoteMeta(label.value) + "$"})
	}
	for _, key := range []string{tokensTotalField, tokensPromptField, tokensCompletionField} {
		attributes = append(attributes, map[string]string{keyField: key, valueField: "^[1-9][0-9]*$"})
	}
	return yaml.Marshal(map[string]any{
		nameField: fmt.Sprintf("%s: ACM Thanos organization metrics and hub Loki usage (%s)", s.Context, runID),
		"tags":    []string{"e2e", "ai-grid"},
		"fixture": map[string]any{"remote": map[string]string{"endpoint": "default"}},
		"expected": map[string]any{
			"metrics": []map[string]string{{"promql": metric, "count": ">= 1", valueField: "> " + strconv.FormatFloat(baseline, 'g', -1, 64)}},
			// A structural match forces GCX JSON and OATS' parsed row counter;
			// text banners or "no results" messages cannot satisfy this assertion.
			"logs": []map[string]any{{"logql": LogQuery(s), "count": ">= 1", "match": []map[string]any{{
				"match_type": "regexp", nameField: "^200 " + regexp.QuoteMeta(u.EscapedPath()) + "$", "attributes": attributes,
			}}}},
		},
	})
}

// GCXConfig pins both named datasources and basic authentication to the temporary
// Grafana instance. The returned bytes contain its password: write only to a 0600
// temporary file, outside retained artifacts.
func GCXConfig(server, password, caFile string) ([]byte, error) {
	grafana := map[string]any{"server": server, "user": "admin", grafanaPasswordKey: password, "org-id": 1, "auth-method": "basic"}
	if caFile != "" {
		grafana["tls"] = map[string]string{"ca-file": caFile}
	}
	return yaml.Marshal(map[string]any{
		"version": 1, "current-context": GCXContext,
		"stacks": map[string]any{GCXContext: map[string]any{"grafana": grafana}},
		"contexts": map[string]any{GCXContext: map[string]any{
			"stack": GCXContext, "datasources": map[string]string{"prometheus": MetricsUID, "loki": LogsUID},
		}},
	})
}

// OATSEnv isolates the run from ambient Grafana credentials, datasource choices,
// OATS cache/retry overrides, and the caller's GCX context.
func OATSEnv(inherited []string, configFile string) []string {
	env := make([]string, 0, len(inherited)+2)
	for _, entry := range inherited {
		if strings.HasPrefix(entry, "GRAFANA_") || strings.HasPrefix(entry, "GCX_") || strings.HasPrefix(entry, "OATS_") {
			continue
		}
		env = append(env, entry)
	}
	return append(env, "GCX_CONFIG="+configFile, "GCX_TELEMETRY=disabled")
}

func Redact(text string, credentials ...string) string {
	for _, value := range credentials {
		if value != "" {
			text = strings.ReplaceAll(text, value, "<redacted>")
			text = strings.ReplaceAll(text, base64.StdEncoding.EncodeToString([]byte("admin:"+value)), "<redacted>")
		}
	}
	return text
}
