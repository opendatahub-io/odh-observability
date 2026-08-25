package e2e_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/onsi/gomega"
	"gopkg.in/yaml.v3"
)

type oatsConfig struct {
	Cases []string `yaml:"cases"`
}

type oatsCaseFile struct {
	Name string `yaml:"name"`
}

func findProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("go.mod file not found in parent directories")
}

func ensureOatsBinaries(t *testing.T, projectRoot string) (string, string) {
	t.Helper()

	localBin := filepath.Join(projectRoot, "bin")
	oatsBin := filepath.Join(localBin, "oats")
	gcxBin := filepath.Join(localBin, "gcx")

	needOats := false
	needGcx := false

	if _, err := os.Stat(oatsBin); os.IsNotExist(err) {
		needOats = true
	}
	if _, err := os.Stat(gcxBin); os.IsNotExist(err) {
		needGcx = true
	}

	if needOats || needGcx {
		t.Logf("Installing tool dependencies into %s...", localBin)
		if err := os.MkdirAll(localBin, 0755); err != nil {
			t.Fatalf("Failed to create bin directory %s: %v", localBin, err)
		}

		args := []string{"install"}
		if needOats {
			args = append(args, "github.com/grafana/oats")
		}
		if needGcx {
			args = append(args, "github.com/grafana/gcx/cmd/gcx")
		}

		cmd := exec.Command("go", args...)
		cmd.Dir = projectRoot
		cmd.Env = append(os.Environ(), "GOBIN="+localBin)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("Failed to install oats/gcx tools: %v\nOutput: %s", err, string(out))
		}
	}

	return oatsBin, gcxBin
}

func buildOatsEnv(t *testing.T) []string {
	t.Helper()

	envMap := make(map[string]string)
	for _, envStr := range os.Environ() {
		parts := strings.SplitN(envStr, "=", 2)
		if len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}

	if envMap["GCX_TELEMETRY"] == "" {
		envMap["GCX_TELEMETRY"] = "disabled"
	}

	if envMap["GRAFANA_ORG_ID"] == "" {
		envMap["GRAFANA_ORG_ID"] = "1"
	}

	if envMap["GRAFANA_SERVER"] == "" {
		cmd := exec.Command("oc", "get", "route", "lgtm", "-n", "redhat-ods-monitoring", "-o", "jsonpath={.spec.host}")
		out, err := cmd.Output()
		if err == nil {
			host := strings.TrimSpace(string(out))
			if host != "" {
				envMap["GRAFANA_SERVER"] = "https://" + host
			}
		}
		if envMap["GRAFANA_SERVER"] == "" {
			t.Log("Warning: GRAFANA_SERVER is not set and could not be resolved from OpenShift route 'lgtm'")
		}
	}

	if envMap["GRAFANA_TOKEN"] == "" {
		cmd := exec.Command("oc", "whoami", "-t")
		out, err := cmd.Output()
		if err == nil {
			token := strings.TrimSpace(string(out))
			if token != "" {
				envMap["GRAFANA_TOKEN"] = token
			}
		}
		if envMap["GRAFANA_TOKEN"] == "" {
			t.Log("Warning: GRAFANA_TOKEN is not set and could not be resolved via 'oc whoami -t'")
		}
	}

	var envList []string
	for k, v := range envMap {
		envList = append(envList, fmt.Sprintf("%s=%s", k, v))
	}
	return envList
}

func TestOats(t *testing.T) {
	projectRoot, err := findProjectRoot()
	if err != nil {
		t.Fatalf("Failed to locate project root: %v", err)
	}

	oatsBin, gcxBin := ensureOatsBinaries(t, projectRoot)
	oatsEnv := buildOatsEnv(t)

	configPath := filepath.Join(projectRoot, "oats-config.yaml")
	configData, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("Failed to read %s: %v", configPath, err)
	}

	var cfg oatsConfig
	if err := yaml.Unmarshal(configData, &cfg); err != nil {
		t.Fatalf("Failed to unmarshal %s: %v", configPath, err)
	}

	if len(cfg.Cases) == 0 {
		t.Skip("No OATS test cases configured in oats-config.yaml")
	}

	for _, caseRelPath := range cfg.Cases {
		caseAbsPath := filepath.Join(projectRoot, caseRelPath)

		caseName := caseRelPath
		if caseData, err := os.ReadFile(caseAbsPath); err == nil {
			var cf oatsCaseFile
			if err := yaml.Unmarshal(caseData, &cf); err == nil && cf.Name != "" {
				caseName = fmt.Sprintf("%s (%s)", cf.Name, caseRelPath)
			}
		}

		t.Run(caseName, func(t *testing.T) {
			g := gomega.NewWithT(t)

			cmd := exec.Command(oatsBin, caseAbsPath, "--gcx", gcxBin, "--gcx-context", "default")
			cmd.Dir = projectRoot
			cmd.Env = oatsEnv

			var outBuf bytes.Buffer
			cmd.Stdout = &outBuf
			cmd.Stderr = &outBuf

			runErr := cmd.Run()
			outStr := outBuf.String()

			g.Expect(runErr).NotTo(gomega.HaveOccurred(), "OATS test case failed:\n%s", outStr)
		})
	}
}
