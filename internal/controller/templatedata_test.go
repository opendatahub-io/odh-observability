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
	"context"
	"errors"
	"strings"
	"testing"

	configv1 "github.com/openshift/api/config/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func rawExt(yaml string) runtime.RawExtension {
	return runtime.RawExtension{Raw: []byte(yaml)}
}

func TestAddTLSDataResolvesEffectiveProxyProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		apiServer  *configv1.APIServer
		wantMin    string
		wantCipher string
		wantCurves string
		wantErr    string
	}{
		{
			name:       "missing APIServer uses Intermediate defaults",
			apiServer:  nil,
			wantMin:    proxyTLSVersion12,
			wantCipher: "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
			wantCurves: "4588,29,23,24",
		},
		{
			name: "NoOpinion uses Intermediate defaults",
			apiServer: &configv1.APIServer{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
				Spec: configv1.APIServerSpec{
					TLSAdherence:       configv1.TLSAdherencePolicyNoOpinion,
					TLSSecurityProfile: &configv1.TLSSecurityProfile{Type: configv1.TLSProfileModernType},
				},
			},
			wantMin:    proxyTLSVersion12,
			wantCipher: "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
			wantCurves: "4588,29,23,24",
		},
		{
			name: "LegacyAdheringComponentsOnly uses Intermediate defaults",
			apiServer: &configv1.APIServer{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
				Spec: configv1.APIServerSpec{
					TLSAdherence:       configv1.TLSAdherencePolicyLegacyAdheringComponentsOnly,
					TLSSecurityProfile: &configv1.TLSSecurityProfile{Type: configv1.TLSProfileModernType},
				},
			},
			wantMin:    proxyTLSVersion12,
			wantCipher: "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
			wantCurves: "4588,29,23,24",
		},
		{
			name: "strict Modern uses TLS 1.3 without a cipher restriction",
			apiServer: &configv1.APIServer{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
				Spec: configv1.APIServerSpec{
					TLSAdherence:       configv1.TLSAdherencePolicyStrictAllComponents,
					TLSSecurityProfile: &configv1.TLSSecurityProfile{Type: configv1.TLSProfileModernType},
				},
			},
			wantMin:    proxyTLSVersion13,
			wantCipher: "",
			wantCurves: "4588,29,23,24",
		},
		{
			name: "unknown adherence is treated as strict",
			apiServer: &configv1.APIServer{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
				Spec: configv1.APIServerSpec{
					TLSAdherence: "Unknown",
					TLSSecurityProfile: &configv1.TLSSecurityProfile{
						Type: configv1.TLSProfileCustomType,
						Custom: &configv1.CustomTLSProfile{
							TLSProfileSpec: configv1.TLSProfileSpec{
								MinTLSVersion: configv1.VersionTLS12,
								Ciphers:       []string{"ECDHE-RSA-AES128-GCM-SHA256"},
								Groups: []configv1.TLSGroup{
									configv1.TLSGroupX25519,
									configv1.TLSGroupSecP521r1,
								},
							},
						},
					},
				},
			},
			wantMin:    proxyTLSVersion12,
			wantCipher: "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
			wantCurves: "29,25",
		},
		{
			name: "strict TLS 1.0 is not representable by the proxy",
			apiServer: &configv1.APIServer{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
				Spec: configv1.APIServerSpec{
					TLSAdherence: configv1.TLSAdherencePolicyStrictAllComponents,
					TLSSecurityProfile: &configv1.TLSSecurityProfile{
						Type: configv1.TLSProfileCustomType,
						Custom: &configv1.CustomTLSProfile{
							TLSProfileSpec: configv1.TLSProfileSpec{
								MinTLSVersion: configv1.VersionTLS10,
								Ciphers:       []string{"ECDHE-RSA-AES128-GCM-SHA256"},
								Groups:        []configv1.TLSGroup{configv1.TLSGroupX25519},
							},
						},
					},
				},
			},
			wantErr: "unsupported minimum TLS version",
		},
		{
			name: "strict groups with no proxy-supported curves fail",
			apiServer: &configv1.APIServer{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
				Spec: configv1.APIServerSpec{
					TLSAdherence: configv1.TLSAdherencePolicyStrictAllComponents,
					TLSSecurityProfile: &configv1.TLSSecurityProfile{
						Type: configv1.TLSProfileCustomType,
						Custom: &configv1.CustomTLSProfile{
							TLSProfileSpec: configv1.TLSProfileSpec{
								MinTLSVersion: configv1.VersionTLS12,
								Ciphers:       []string{"ECDHE-RSA-AES128-GCM-SHA256"},
								Groups:        []configv1.TLSGroup{configv1.TLSGroupSecP256r1MLKEM768},
							},
						},
					},
				},
			},
			wantErr: "no usable proxy curve preferences",
		},
		{
			name: "strict TLS 1.2 without cipher suites fails",
			apiServer: &configv1.APIServer{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
				Spec: configv1.APIServerSpec{
					TLSAdherence: configv1.TLSAdherencePolicyStrictAllComponents,
					TLSSecurityProfile: &configv1.TLSSecurityProfile{
						Type: configv1.TLSProfileCustomType,
						Custom: &configv1.CustomTLSProfile{
							TLSProfileSpec: configv1.TLSProfileSpec{
								MinTLSVersion: configv1.VersionTLS12,
							},
						},
					},
				},
			},
			wantErr: "TLS profile contains no cipher suites",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			require.NoError(t, configv1.Install(scheme))
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if test.apiServer != nil {
				builder = builder.WithObjects(test.apiServer)
			}

			templateData := map[string]any{}
			err := addTLSData(context.Background(), builder.Build(), templateData)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.wantMin, templateData["TLSMinVersion"])
			if test.wantCipher == "" {
				assert.Empty(t, templateData["TLSCipherSuites"])
			} else {
				assert.Contains(t, templateData["TLSCipherSuites"], test.wantCipher)
			}
			assert.Equal(t, test.wantCurves, templateData["TLSCurvePreferences"])
		})
	}
}

func TestFilterProxyCurvePreferencesFIPSMode(t *testing.T) {
	t.Parallel()

	got, err := filterProxyCurvePreferences("4588,29,23,24,4589", true)
	require.NoError(t, err)
	assert.Equal(t, "23,24", got)
}

func TestAddTLSDataDropsNonFIPSApprovedCurvesOnFIPSCluster(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, configv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	installConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-config-v1",
			Namespace: "kube-system",
		},
		Data: map[string]string{
			"install-config": "fips: true",
		},
	}
	apiServer := &configv1.APIServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
		Spec: configv1.APIServerSpec{
			TLSAdherence: configv1.TLSAdherencePolicyStrictAllComponents,
			TLSSecurityProfile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: configv1.VersionTLS12,
						Ciphers:       []string{"ECDHE-RSA-AES128-GCM-SHA256"},
						Groups: []configv1.TLSGroup{
							configv1.TLSGroupX25519,
							configv1.TLSGroupSecP256r1,
						},
					},
				},
			},
		},
	}
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(installConfig, apiServer).
		Build()

	templateData := map[string]any{}
	require.NoError(t, addTLSData(context.Background(), cli, templateData))
	assert.Equal(t, "23", templateData["TLSCurvePreferences"])
}

func TestAddTLSDataFailsClosedWhenClusterConfigIsMalformed(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, configv1.Install(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	installConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-config-v1",
			Namespace: "kube-system",
		},
		Data: map[string]string{"install-config": "{ invalid"},
	}
	apiServer := &configv1.APIServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
		Spec: configv1.APIServerSpec{
			TLSAdherence: configv1.TLSAdherencePolicyStrictAllComponents,
			TLSSecurityProfile: &configv1.TLSSecurityProfile{
				Type: configv1.TLSProfileCustomType,
				Custom: &configv1.CustomTLSProfile{
					TLSProfileSpec: configv1.TLSProfileSpec{
						MinTLSVersion: configv1.VersionTLS12,
						Ciphers:       []string{"ECDHE-RSA-AES128-GCM-SHA256"},
						Groups: []configv1.TLSGroup{
							configv1.TLSGroupX25519,
							configv1.TLSGroupSecP256r1,
						},
					},
				},
			},
		},
	}
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(installConfig, apiServer).
		Build()

	templateData := map[string]any{}
	err := addTLSData(context.Background(), cli, templateData)
	require.ErrorIs(t, err, errProxyTLSResolutionFailed)
	require.NotErrorIs(t, err, errInvalidProxyTLSProfile)
	assert.Empty(t, templateData)
}

func TestAddTLSDataDistinguishesOperationalResolutionFailure(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, configv1.Install(scheme))
	apiServer := &configv1.APIServer{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
	}
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(apiServer).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if key.Name == "cluster" {
					return k8serr.NewForbidden(
						schema.GroupResource{Group: "config.openshift.io", Resource: "apiservers"},
						key.Name,
						errors.New("not allowed"),
					)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	templateData := map[string]any{}
	err := addTLSData(context.Background(), cli, templateData)
	require.ErrorIs(t, err, errProxyTLSResolutionFailed)
	require.NotErrorIs(t, err, errInvalidProxyTLSProfile)
	assert.Empty(t, templateData)
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

// --- DNS-1123 namespace validation ---

func TestDNS1123LabelRe(t *testing.T) {
	valid := []string{
		"default",
		"my-namespace",
		"ns123",
		"a",
		"a-b-c-d",
		strings.Repeat("a", 63),
	}
	for _, v := range valid {
		if !dns1123LabelRe.MatchString(v) {
			t.Errorf("expected %q to be valid DNS-1123 label", v)
		}
	}

	invalid := []string{
		"",
		"-starts-with-dash",
		"ends-with-dash-",
		"UPPERCASE",
		"has space",
		"has.dot",
		"new\nline",
		"has\ttab",
		"injection\n---\napiVersion: v1",
		strings.Repeat("a", 64),
	}
	for _, v := range invalid {
		if dns1123LabelRe.MatchString(v) {
			t.Errorf("expected %q to be invalid DNS-1123 label", v)
		}
	}
}
