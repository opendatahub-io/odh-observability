package main

import (
	"os"

	"github.com/opendatahub-io/odh-observability/tests/e2e/runner"
)

func main() {
	os.Exit(runner.New().Run(os.Args[1:]).ExitCode)
}
