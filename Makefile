# Image URL to use all building/pushing image targets
IMG ?= quay.io/opendatahub/odh-observability:odh-stable
PLATFORM ?= linux/amd64
CGO_ENABLED ?= 1
IMAGE_BUILDER ?= podman

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", $$2 }' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopyObject, DeepCopyInto, and DeepCopyList implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet ## Run tests with coverage (full pipeline).
	go test $(shell go list ./... | grep -v /tests/e2e) -coverprofile cover.out

.PHONY: unit-test
unit-test: ## Run unit tests (no codegen prerequisites).
	go test $(shell go list ./... | grep -v /tests/e2e)

.PHONY: test-verbose
test-verbose: ## Run unit tests with verbose output.
	go test -v $(shell go list ./... | grep -v /tests/e2e)

.PHONY: lint
lint: golangci-lint ## Run golangci-lint against code.
	$(GOLANGCI_LINT) run

.PHONY: e2e-test
e2e-test: ## Run e2e tests against a cluster (requires KUBECONFIG).
	go test ./tests/e2e/ -v -timeout 120m -count=1 $(E2E_TEST_FLAGS)

##@ E2E Test Image

# E2E Test Image
E2E_IMG ?= quay.io/opendatahub/odh-observability-e2e:latest

.PHONY: e2e-image-build
e2e-image-build: ## Build e2e test container image.
	$(IMAGE_BUILDER) build --platform "$(PLATFORM)" \
		-f Dockerfiles/e2e-tests/e2e-tests.Dockerfile \
		-t "$(E2E_IMG)" .

.PHONY: e2e-image-push
e2e-image-push: ## Push e2e test container image.
	$(IMAGE_BUILDER) push "$(E2E_IMG)"

.PHONY: e2e-image
e2e-image: e2e-image-build e2e-image-push ## Build and push e2e test image.

KUBECONFIG ?= $(HOME)/.kube/config

E2E_ARTIFACTS ?= $(shell pwd)/e2e-artifacts

.PHONY: e2e-test-container
e2e-test-container: ## Run containerized e2e tests in module mode (standalone operator).
	mkdir -p "$(E2E_ARTIFACTS)"
	$(IMAGE_BUILDER) run --rm \
		--userns=keep-id \
		--user "$(shell id -u):$(shell id -g)" \
		-v "$(KUBECONFIG):/tmp/kubeconfig:ro,z" \
		-v "$(E2E_ARTIFACTS):/artifacts:Z" \
		-e KUBECONFIG=/tmp/kubeconfig \
		-e E2E_TEST_API_MODE=module \
		-e E2E_TEST_INSTALL_OPERATORS=true \
		-e E2E_TEST_MONITORING_CR_NAME=default-monitoring \
		"$(E2E_IMG)"

.PHONY: e2e-test-container-dsc
e2e-test-container-dsc: ## Run containerized e2e tests in DSC mode (via ODH platform operator).
	mkdir -p "$(E2E_ARTIFACTS)"
	$(IMAGE_BUILDER) run --rm \
		--userns=keep-id \
		--user "$(shell id -u):$(shell id -g)" \
		-v "$(KUBECONFIG):/tmp/kubeconfig:ro,z" \
		-v "$(E2E_ARTIFACTS):/artifacts:Z" \
		-e KUBECONFIG=/tmp/kubeconfig \
		-e E2E_TEST_API_MODE=dsc \
		-e E2E_TEST_INSTALL_OPERATORS=false \
		-e E2E_TEST_MONITORING_CR_NAME=default-monitoring \
		"$(E2E_IMG)"

##@ Prometheus Rules

PROMETHEUS_RULES_DIR = ./internal/controller
PROMETHEUS_RULE_TEMPLATES = $(shell find $(PROMETHEUS_RULES_DIR) -name "*-prometheusrules.tmpl.yaml" 2>/dev/null)
PROMETHEUS_ALERT_TESTS = $(shell find $(PROMETHEUS_RULES_DIR) -name "*-alerting.unit-tests.yaml" 2>/dev/null)
PROMETHEUS_ALERT_RULES := $(PROMETHEUS_ALERT_TESTS:.unit-tests.yaml=.rules.yaml)

%.rules.yaml: %.unit-tests.yaml $(YQ)
	@RULE_FILE=$$(dirname $<)/$$(basename $< -alerting.unit-tests.yaml)-prometheusrules.tmpl.yaml; \
	if [ ! -f "$$RULE_FILE" ]; then \
		echo "Error: PrometheusRule template file not found: $$RULE_FILE"; \
		exit 1; \
	fi; \
	echo "Generating $@ from $$RULE_FILE (alerts only, excluding recording rules)"; \
	sed 's/{{\.Namespace}}/redhat-ods-monitoring/g; s/{{ \.OperatorNamespace }}/redhat-ods-operator/g; s/{{ \.OperatorPodPrefix }}/odh-observability/g; s/{{`{{`}}/{{/g; s/{{`}}`}}/}}/g' "$$RULE_FILE" | \
		$(YQ) eval '.spec.groups' - | \
		$(YQ) eval 'del(.[] | .rules[] | select(.alert == null))' - | \
		$(YQ) eval '{"groups": .}' - > $@

.PHONY: validate-prometheus-rules
validate-prometheus-rules: $(YQ) ## Validate PrometheusRule template syntax.
	@echo "Validating PrometheusRule templates syntax..."
	@for tmpl_file in $(PROMETHEUS_RULE_TEMPLATES); do \
		echo "  Checking $$tmpl_file..."; \
		sed 's/{{\.Namespace}}/redhat-ods-monitoring/g; s/{{ \.OperatorNamespace }}/redhat-ods-operator/g; s/{{ \.OperatorPodPrefix }}/odh-observability/g; s/{{`{{`}}/{{/g; s/{{`}}`}}/}}/g' "$$tmpl_file" | \
			$(YQ) eval '.spec.groups' - | \
			$(YQ) eval '{"groups": .}' - | \
			promtool check rules --lint=none /dev/stdin > /dev/null || exit 1; \
	done
	@echo "All PrometheusRule templates are syntactically valid"

.PHONY: test-alerts
test-alerts: validate-prometheus-rules $(PROMETHEUS_ALERT_RULES) ## Run Prometheus alert unit tests.
	@echo "Running Prometheus alert unit tests..."
	@for test_file in $(PROMETHEUS_ALERT_TESTS); do \
		echo "  Testing $$test_file..."; \
		promtool test rules $$test_file || exit 1; \
	done
	@echo "All Prometheus alert tests passed!"

CLEANFILES += $(PROMETHEUS_ALERT_RULES)

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run controller locally (requires POD_NAMESPACE, e.g. POD_NAMESPACE=opendatahub make run).
ifndef POD_NAMESPACE
	$(error POD_NAMESPACE is not set. Usage: POD_NAMESPACE=opendatahub make run)
endif
	go run ./cmd/main.go

.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	podman build --platform $(PLATFORM) --build-arg CGO_ENABLED=$(CGO_ENABLED) -f Dockerfile -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	podman push ${IMG}

.PHONY: image
image: docker-build docker-push ## Build and push image with the manager.

##@ Deployment

HELM_RELEASE ?= odh-observability
HELM_CHART   ?= charts/odh-observability
NAMESPACE    ?= opendatahub

.PHONY: deploy
deploy: manifests helm-update-crds ## Deploy operator to cluster via Helm chart.
	helm upgrade --install $(HELM_RELEASE) $(HELM_CHART) \
		-n $(NAMESPACE) --create-namespace \
		--set operatorNamespace=$(NAMESPACE) \
		--set image.repository=$(firstword $(subst :, ,$(IMG))) \
		--set image.tag=$(lastword $(subst :, ,$(IMG)))

.PHONY: undeploy
undeploy: ## Remove operator from cluster.
	helm uninstall $(HELM_RELEASE) -n $(NAMESPACE) --ignore-not-found

.PHONY: helm-update-crds
helm-update-crds: manifests ## Copy generated CRDs into the Helm chart crds/ directory.
	mkdir -p charts/odh-observability/crds
	cp config/crd/bases/*.yaml charts/odh-observability/crds/

.PHONY: helm-lint
helm-lint: ## Lint Helm chart.
	helm lint charts/odh-observability

.PHONY: helm-template
helm-template: ## Render Helm chart templates.
	helm template odh-observability charts/odh-observability

##@ Build Dependencies

LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint
YQ ?= $(LOCALBIN)/yq

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	test -s $(LOCALBIN)/controller-gen || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	test -s "$(GOLANGCI_LINT)" || GOBIN="$(LOCALBIN)" go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2

.PHONY: yq
yq: $(YQ) ## Download yq locally if necessary.
$(YQ): $(LOCALBIN)
	test -s $(LOCALBIN)/yq || GOBIN=$(LOCALBIN) go install github.com/mikefarah/yq/v4@v4.53.2
