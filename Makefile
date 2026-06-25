# Image URL to use all building/pushing image targets
IMG ?= quay.io/opendatahub/odh-observability:odh-stable
PLATFORM ?= linux/amd64
CGO_ENABLED ?= 1

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

.PHONY: e2e-test
e2e-test: ## Run e2e tests against a cluster (requires KUBECONFIG).
	go test ./tests/e2e/ -v -timeout 120m -count=1 $(E2E_TEST_FLAGS)

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

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	test -s $(LOCALBIN)/controller-gen || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@latest
