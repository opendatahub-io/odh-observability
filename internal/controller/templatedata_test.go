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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func rawExt(yaml string) runtime.RawExtension {
	return runtime.RawExtension{Raw: []byte(yaml)}
}

func TestValidateExporters_ValidOTLP(t *testing.T) {
	exporters := map[string]runtime.RawExtension{
		"otlp/custom": rawExt(`endpoint: https://collector.example.com:4317`),
	}
	result, err := validateExporters(exporters)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["otlp/custom"]; !ok {
		t.Error("expected otlp/custom in result")
	}
}

func TestValidateExporters_ReservedNamePrometheus(t *testing.T) {
	exporters := map[string]runtime.RawExtension{
		"prometheus": rawExt(`endpoint: https://example.com`),
	}
	_, err := validateExporters(exporters)
	if err == nil {
		t.Fatal("expected error for reserved name 'prometheus'")
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Errorf("expected 'reserved' in error, got: %v", err)
	}
}

func TestValidateExporters_ReservedNameOTLPTempo(t *testing.T) {
	exporters := map[string]runtime.RawExtension{
		"otlp/tempo": rawExt(`endpoint: https://example.com`),
	}
	_, err := validateExporters(exporters)
	if err == nil {
		t.Fatal("expected error for reserved name 'otlp/tempo'")
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Errorf("expected 'reserved' in error, got: %v", err)
	}
}

func TestValidateExporters_InvalidNameFormat(t *testing.T) {
	exporters := map[string]runtime.RawExtension{
		"has spaces": rawExt(`endpoint: https://example.com`),
	}
	_, err := validateExporters(exporters)
	if err == nil {
		t.Fatal("expected error for invalid name format")
	}
	if !strings.Contains(err.Error(), "component ID format") {
		t.Errorf("expected 'component ID format' in error, got: %v", err)
	}
}

func TestValidateExporters_OversizedConfig(t *testing.T) {
	big := strings.Repeat("x", maxExporterSize+1)
	exporters := map[string]runtime.RawExtension{
		"otlp/big": rawExt("endpoint: " + big),
	}
	_, err := validateExporters(exporters)
	if err == nil {
		t.Fatal("expected error for oversized config")
	}
	if !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Errorf("expected 'exceeds maximum size' in error, got: %v", err)
	}
}

func TestValidateExporters_ExcessiveNesting(t *testing.T) {
	// Build a YAML string with nesting deeper than maxNestingDepth.
	var b strings.Builder
	for i := range maxNestingDepth + 2 {
		b.WriteString(strings.Repeat("  ", i))
		b.WriteString("level")
		b.WriteString(strings.Repeat("x", i))
		b.WriteString(":\n")
	}
	b.WriteString(strings.Repeat("  ", maxNestingDepth+2))
	b.WriteString("val: true\n")

	exporters := map[string]runtime.RawExtension{
		"debug": rawExt(b.String()),
	}
	_, err := validateExporters(exporters)
	if err == nil {
		t.Fatal("expected error for excessive nesting")
	}
	if !strings.Contains(err.Error(), "nesting too deep") && !strings.Contains(err.Error(), "too many fields") {
		t.Errorf("expected nesting/fields error, got: %v", err)
	}
}

func TestValidateExporters_StringTooLong(t *testing.T) {
	long := strings.Repeat("a", maxStringLength+1)
	exporters := map[string]runtime.RawExtension{
		"debug": rawExt("verbosity: " + long),
	}
	_, err := validateExporters(exporters)
	if err == nil {
		t.Fatal("expected error for string too long")
	}
	if !strings.Contains(err.Error(), "string value too long") {
		t.Errorf("expected 'string value too long' in error, got: %v", err)
	}
}

func TestValidateExporters_InsecureExternalEndpoint(t *testing.T) {
	exporters := map[string]runtime.RawExtension{
		"otlp/ext": rawExt(`endpoint: http://external.example.com:4317`),
	}
	_, err := validateExporters(exporters)
	if err == nil {
		t.Fatal("expected error for insecure external endpoint")
	}
	if !strings.Contains(err.Error(), "insecure HTTP") {
		t.Errorf("expected 'insecure HTTP' in error, got: %v", err)
	}
}

func TestValidateExporters_InsecureLocalEndpointAllowed(t *testing.T) {
	exporters := map[string]runtime.RawExtension{
		"otlp/local": rawExt(`endpoint: http://prometheus.monitoring.svc.cluster.local:9090`),
	}
	_, err := validateExporters(exporters)
	if err != nil {
		t.Fatalf("local http endpoint should be allowed, got: %v", err)
	}
}

func TestValidateExporters_MissingRequiredField(t *testing.T) {
	exporters := map[string]runtime.RawExtension{
		"otlp/missing": rawExt(`compression: gzip`),
	}
	_, err := validateExporters(exporters)
	if err == nil {
		t.Fatal("expected error for missing required field 'endpoint'")
	}
	if !strings.Contains(err.Error(), "endpoint") {
		t.Errorf("expected 'endpoint' in error, got: %v", err)
	}
}

func TestValidateExporters_DisallowedField(t *testing.T) {
	exporters := map[string]runtime.RawExtension{
		"otlp/bad": rawExt("endpoint: https://example.com:4317\nunknown_field: true"),
	}
	_, err := validateExporters(exporters)
	if err == nil {
		t.Fatal("expected error for disallowed field")
	}
	if !strings.Contains(err.Error(), "unknown_field") {
		t.Errorf("expected 'unknown_field' in error, got: %v", err)
	}
}

func TestValidateExporters_EmptyConfig(t *testing.T) {
	exporters := map[string]runtime.RawExtension{
		"debug": rawExt(``),
	}
	result, err := validateExporters(exporters)
	if err != nil {
		t.Fatalf("empty config should be allowed for debug exporter, got: %v", err)
	}
	if _, ok := result["debug"]; ok {
		t.Error("empty config should not produce an entry in result")
	}
}

func TestValidateExporters_UnknownExporterTypePassesSecurityOnly(t *testing.T) {
	exporters := map[string]runtime.RawExtension{
		"custom_exporter": rawExt("some_field: some_value"),
	}
	result, err := validateExporters(exporters)
	if err != nil {
		t.Fatalf("unknown exporter type should pass (no schema to validate against), got: %v", err)
	}
	if _, ok := result["custom_exporter"]; !ok {
		t.Error("expected custom_exporter in result")
	}
}
