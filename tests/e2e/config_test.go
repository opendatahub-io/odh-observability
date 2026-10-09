package e2e_test

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

// API mode constants for test configuration.
const (
	APIModeModule = "module"
	APIModeDSC    = "dsc"
)

type TestTimeouts struct {
	defaultEventuallyTimeout        time.Duration
	defaultEventuallyPollInterval   time.Duration
	defaultConsistentlyTimeout      time.Duration
	defaultConsistentlyPollInterval time.Duration
	olmOperationTimeout             time.Duration
}

type TestContextConfig struct {
	monitoringNamespace string
	monitoringCRName    string
	installOperators    bool
	apiMode             string
	dsciCRName          string
	cooChannel          string
	compatibilityReport string
	Timeouts            TestTimeouts
}

// registerFlags registers test binary flags.
// These flags are mapped from env vars by runner.envToFlags (tests/e2e/runner/runner.go).
func (c *TestContextConfig) registerFlags() {
	flag.StringVar(&c.monitoringNamespace, "monitoring-namespace", "", "namespace where monitoring operands are deployed (auto-detected from operator or CR if omitted)")
	flag.StringVar(&c.monitoringCRName, "monitoring-cr-name", "", "name of the Monitoring CR")
	flag.BoolVar(&c.installOperators, "install-operators", true, "install dependent OLM operators before running tests")
	flag.StringVar(&c.apiMode, "api-mode", "module", "API mode: 'module' for standalone module CR, 'dsc' for DSC/DSCI integration")
	flag.StringVar(&c.dsciCRName, "dsci-cr-name", "default-dsci", "name of the DSCInitialization CR (DSC mode only)")
	flag.StringVar(&c.cooChannel, "coo-channel", "", "OLM channel for Cluster Observability Operator (default: stable)")
	flag.StringVar(&c.compatibilityReport, "compatibility-report", "", "optional path to write JSON compatibility diagnostics (always logged on failure)")

	flag.DurationVar(&c.Timeouts.defaultEventuallyTimeout, "eventually-timeout", 0, "default eventually timeout")
	flag.DurationVar(&c.Timeouts.defaultEventuallyPollInterval, "eventually-poll-interval", 0, "default eventually poll interval")
	flag.DurationVar(&c.Timeouts.defaultConsistentlyTimeout, "consistently-timeout", 0, "default consistently timeout")
	flag.DurationVar(&c.Timeouts.defaultConsistentlyPollInterval, "consistently-poll-interval", 0, "default consistently poll interval")
	flag.DurationVar(&c.Timeouts.olmOperationTimeout, "olm-timeout", 0, "timeout for OLM operator installation")
}

func (c *TestContextConfig) validate() error {
	switch c.apiMode {
	case APIModeModule, APIModeDSC:
	default:
		return fmt.Errorf("invalid api-mode %q: must be %q or %q", c.apiMode, APIModeModule, APIModeDSC)
	}
	if c.compatibilityReport != "" {
		if _, err := resolveCompatibilityReportPath(c.compatibilityReport); err != nil {
			return fmt.Errorf("invalid compatibility-report: %w", err)
		}
	}
	if err := validateForbiddenCompatibilityGateOverrides(); err != nil {
		return err
	}
	return nil
}

func (c *TestContextConfig) applyDefaults() {
	if c.monitoringCRName == "" {
		c.monitoringCRName = "default-monitoring"
	}
	if c.Timeouts.defaultEventuallyTimeout <= 0 {
		c.Timeouts.defaultEventuallyTimeout = 5 * time.Minute
	}
	if c.Timeouts.defaultEventuallyPollInterval <= 0 {
		c.Timeouts.defaultEventuallyPollInterval = 2 * time.Second
	}
	if c.Timeouts.defaultConsistentlyTimeout <= 0 {
		c.Timeouts.defaultConsistentlyTimeout = 30 * time.Second
	}
	if c.Timeouts.defaultConsistentlyPollInterval <= 0 {
		c.Timeouts.defaultConsistentlyPollInterval = 2 * time.Second
	}
	if c.Timeouts.olmOperationTimeout <= 0 {
		c.Timeouts.olmOperationTimeout = 5 * time.Minute
	}
}

func compatibilityReportBaseDir() (string, error) {
	for _, candidate := range []string{os.Getenv("ARTIFACTS"), os.Getenv("E2E_ARTIFACTS"), "e2e-artifacts"} {
		if candidate == "" {
			continue
		}
		abs, err := filepath.Abs(candidate)
		if err != nil {
			return "", err
		}
		return abs, nil
	}
	return "", errors.New("compatibility report base directory is not configured")
}

func resolveCompatibilityReportPath(requested string) (string, error) {
	if requested == "" {
		return "", nil
	}
	if filepath.IsAbs(requested) {
		return "", errors.New("absolute paths are not allowed")
	}
	clean := filepath.Clean(requested)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", errors.New("path traversal is not allowed")
	}
	base, err := compatibilityReportBaseDir()
	if err != nil {
		return "", err
	}
	resolvedBase, err := filepath.EvalSymlinks(base)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", err
		}
		resolvedBase = base
	}
	joined := filepath.Join(resolvedBase, clean)
	absJoined, err := filepath.Abs(joined)
	if err != nil {
		return "", err
	}
	baseWithSep := resolvedBase + string(os.PathSeparator)
	if absJoined != resolvedBase && !strings.HasPrefix(absJoined, baseWithSep) {
		return "", errors.New("path must stay under the artifacts directory")
	}
	return absJoined, nil
}

func writeCompatibilityReportFile(dest string, payload []byte) error {
	if err := rejectSymlinkInPath(dest); err != nil {
		return err
	}
	dir := filepath.Dir(dest)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	if err := rejectSymlinkInPath(dir); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".coo-compatibility-report-*.json")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, dest); err != nil {
		return err
	}
	return nil
}

func rejectSymlinkInPath(path string) error {
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				parent := filepath.Dir(current)
				if parent == current {
					return nil
				}
				current = parent
				continue
			}
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing to write through symlink at %s", current)
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
		current = parent
	}
}

// releaseGateContract is the only place to edit when the compatibility release gate targets a new RHOAI/COO line.
// There are no CLI flags or env overrides. Update this block, adjust release-gate unit tests if prefixes change,
// sync PersesImage with internal/controller/templatedata.go getPersesImage() when that default moves, and run
// TestCOOVersionCompatibility on a clean cluster before merge.
var releaseGateContract = struct {
	RHOAICSVPrefix string
	COOCSVPrefix   string
	PersesImage    string
}{
	RHOAICSVPrefix: "rhods-operator.3.6",
	COOCSVPrefix:   "cluster-observability-operator.v1.5",
	PersesImage:    "registry.redhat.io/cluster-observability-operator/perses-rhel9@sha256:a811b9345d884ba1c575584bec9be1d2a237902164a99887458a82d07e7c2376",
}

var cooCompatibilityGateSubtests = []string{
	"COO version matches release gate",
	"Monitoring and Perses reconcile",
	"Perses operand health",
	"Dashboard proxy APIs",
}

var forbiddenCompatibilityGateEnvVars = map[string]string{
	"E2E_TEST_EXPECTED_COO_VERSION_PREFIX": "COO CSV prefix is fixed to " + releaseGateContract.COOCSVPrefix,
	"E2E_TEST_EXPECTED_RHOAI_CSV_PREFIX":   "RHOAI CSV prefix is fixed to " + releaseGateContract.RHOAICSVPrefix,
	"E2E_TEST_SKIP_DASHBOARD_PROBE":        "dashboard proxy probes are mandatory for TestCOOVersionCompatibility",
}

func validateForbiddenCompatibilityGateOverrides() error {
	for env, reason := range forbiddenCompatibilityGateEnvVars {
		if os.Getenv(env) != "" {
			return fmt.Errorf("%s: %s", env, reason)
		}
	}
	return nil
}

func releaseGateCOOCSVMatches(csvName string) bool {
	return cooCSVMatchesPrefix(csvName, releaseGateContract.COOCSVPrefix)
}

func releaseGateRHOAICSVMatches(csvName string) bool {
	return cooCSVMatchesPrefix(csvName, releaseGateContract.RHOAICSVPrefix)
}

func cooCompatibilityGateIncludesDashboardProbe() bool {
	return slices.Contains(cooCompatibilityGateSubtests, "Dashboard proxy APIs")
}

func persesImageMatchesReleaseGate(actual string) error {
	actual = strings.TrimSpace(actual)
	if actual == "" {
		return errors.New("Perses image is empty")
	}
	expectedRepo, expectedDigest, err := parseOCIImageDigestReference(releaseGateContract.PersesImage)
	if err != nil {
		return fmt.Errorf("invalid release gate Perses image reference: %w", err)
	}
	actualRepo, actualDigest, err := parseOCIImageDigestReference(actual)
	if err != nil {
		return fmt.Errorf("Perses image %q is not a digest-pinned reference: %w", actual, err)
	}
	if actualRepo != expectedRepo || actualDigest != expectedDigest {
		return fmt.Errorf(
			"Perses image %q does not match RHOAI release gate %q",
			actual,
			releaseGateContract.PersesImage,
		)
	}
	return nil
}

var ociImageDigestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func parseOCIImageDigestReference(image string) (string, string, error) {
	image = strings.TrimSpace(image)
	const digestMarker = "@sha256:"
	idx := strings.LastIndex(image, digestMarker)
	if idx <= 0 {
		return "", "", errors.New("expected repository@sha256:<digest>")
	}
	repository := image[:idx]
	digest := image[idx+1:]
	if repository == "" {
		return "", "", errors.New("repository is empty")
	}
	if !ociImageDigestPattern.MatchString(digest) {
		return "", "", fmt.Errorf("digest %q is invalid", digest)
	}
	return repository, digest, nil
}
