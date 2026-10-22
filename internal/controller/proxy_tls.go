/*
Copyright 2026.

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
	"fmt"
	"strconv"
	"strings"

	pkgtls "github.com/opendatahub-io/odh-platform-utilities/framework/tls"
	platformcluster "github.com/opendatahub-io/odh-platform-utilities/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	proxyTLSVersion12     = "VersionTLS12"
	proxyTLSVersion13     = "VersionTLS13"
	curveIDSecP256r1      = 23
	curveIDSecP384r1      = 24
	curveIDSecP521r1      = 25
	curveIDX25519         = 29
	curveIDX25519MLKEM768 = 4588
)

var (
	errProxyTLSResolutionFailed = errors.New("failed to resolve APIServer TLS profile for monitoring proxies")
	errInvalidProxyTLSProfile   = errors.New("invalid APIServer TLS profile for monitoring proxies")
)

var proxyCurveIDs = map[int]struct{}{
	curveIDSecP256r1:      {},
	curveIDSecP384r1:      {},
	curveIDSecP521r1:      {},
	curveIDX25519:         {},
	curveIDX25519MLKEM768: {},
}

var fipsApprovedProxyCurveIDs = map[int]struct{}{
	curveIDSecP256r1: {},
	curveIDSecP384r1: {},
	curveIDSecP521r1: {},
}

func addTLSData(ctx context.Context, c client.Client, templateData map[string]any) error {
	minVersion, cipherSuites, curvePreferences, err := pkgtls.FromAPIServerWithCurvePreferences(
		ctx,
		c,
		pkgtls.FormatGo,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", errProxyTLSResolutionFailed, err)
	}

	if minVersion != proxyTLSVersion12 && minVersion != proxyTLSVersion13 {
		return fmt.Errorf("%w: unsupported minimum TLS version %q", errInvalidProxyTLSProfile, minVersion)
	}
	if minVersion == proxyTLSVersion13 {
		cipherSuites = ""
	}

	fipsEnabled, err := platformcluster.IsFipsEnabled(ctx, c)
	if err != nil {
		return fmt.Errorf("%w: determining cluster FIPS status: %w", errProxyTLSResolutionFailed, err)
	}

	curvePreferences, err = filterProxyCurvePreferences(curvePreferences, fipsEnabled)
	if err != nil {
		return fmt.Errorf("%w: %w", errInvalidProxyTLSProfile, err)
	}

	templateData["TLSMinVersion"] = minVersion
	templateData["TLSCipherSuites"] = cipherSuites
	templateData["TLSCurvePreferences"] = curvePreferences
	return nil
}

func filterProxyCurvePreferences(curvePreferences string, fipsEnabled bool) (string, error) {
	if curvePreferences == "" {
		return "", nil
	}

	var usable []string
	seen := make(map[int]struct{})
	for rawCurveID := range strings.SplitSeq(curvePreferences, ",") {
		curveID, err := strconv.Atoi(rawCurveID)
		if err != nil {
			return "", fmt.Errorf("invalid numeric TLS curve preference %q: %w", rawCurveID, err)
		}
		if _, ok := proxyCurveIDs[curveID]; !ok {
			continue
		}
		if fipsEnabled {
			if _, ok := fipsApprovedProxyCurveIDs[curveID]; !ok {
				continue
			}
		}
		if _, ok := seen[curveID]; ok {
			continue
		}
		seen[curveID] = struct{}{}
		usable = append(usable, rawCurveID)
	}

	if len(usable) == 0 {
		return "", fmt.Errorf("TLS profile groups contain no usable proxy curve preferences: %q", curvePreferences)
	}
	return strings.Join(usable, ","), nil
}
