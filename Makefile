# Image URL to use all building/pushing image targets
IMG ?= controller:latest
# YEAR defines the year value used for substituting the YEAR placeholder in the boilerplate header.
YEAR ?= $(shell date +%Y)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt",year=$(YEAR) paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# Integration tests run against a real, dockerized ClickHouse (build tag
# `integration`). The target boots a throwaway container, waits for it to
# accept queries, runs the tagged tests, and always tears the container down.
# Non-standard host ports so this never collides with a developer's local
# ClickHouse bound to the usual 9000/8123. The image's default user is
# localhost-only; CLICKHOUSE_USER/PASSWORD create an any-network user the host
# test can actually authenticate as through the port mapping.
CH_IT_IMAGE ?= clickhouse/clickhouse-server:24.8
CH_IT_CONTAINER ?= kubestream-it-clickhouse
CH_IT_ADDR ?= 127.0.0.1:19000
CH_IT_USER ?= kubestream
CH_IT_PASSWORD ?= kubestream

.PHONY: test-integration
test-integration: ## Run integration tests against a dockerized ClickHouse.
	@echo "Starting ClickHouse container '$(CH_IT_CONTAINER)'..."
	@$(CONTAINER_TOOL) rm -f $(CH_IT_CONTAINER) >/dev/null 2>&1 || true
	@$(CONTAINER_TOOL) run -d --name $(CH_IT_CONTAINER) \
		--ulimit nofile=262144:262144 \
		-e CLICKHOUSE_USER=$(CH_IT_USER) -e CLICKHOUSE_PASSWORD=$(CH_IT_PASSWORD) \
		-p 19000:9000 -p 18123:8123 \
		$(CH_IT_IMAGE) >/dev/null
	@trap '$(CONTAINER_TOOL) rm -f $(CH_IT_CONTAINER) >/dev/null 2>&1 || true' EXIT; \
	echo "Waiting for ClickHouse to accept connections..."; \
	for i in $$(seq 1 30); do \
		if $(CONTAINER_TOOL) exec $(CH_IT_CONTAINER) clickhouse-client --user $(CH_IT_USER) --password $(CH_IT_PASSWORD) --query "SELECT 1" >/dev/null 2>&1; then \
			echo "ClickHouse is ready."; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then echo "ClickHouse did not become ready in time"; exit 1; fi; \
		sleep 1; \
	done; \
	CH_TEST_ADDR=$(CH_IT_ADDR) CH_TEST_USER=$(CH_IT_USER) CH_TEST_PASSWORD=$(CH_IT_PASSWORD) \
		go test -tags=integration ./internal/sink/... -run Integration -v

# bench-load runs the synthetic-churn load harness (test/loadgen, Task 0.8)
# against a throwaway dockerized ClickHouse plus an in-process envtest apiserver.
# It reuses the same container bring-up as test-integration and additionally
# provides KUBEBUILDER_ASSETS (as the `test` target does) so envtest can start.
#
# PROFILE names one of the shipped scale profiles in test/loadgen/profiles/
# (Task 2.3): small, medium or massive. The profile file is the whole load
# definition — objects, rate, payload, duration, delete ratio, kinds, and the
# pass criteria the run judges itself against — so a published envelope in
# docs/PERFORMANCE.md is reproducible from its name alone:
#   make bench-load PROFILE=massive
#
# PPROF_DIR, if set, is a repo-relative directory the run writes its heap/alloc
# profiles and summary into; that is how the before/after pairs in docs/perf/
# were produced:
#   make bench-load PROFILE=massive PPROF_DIR=docs/perf/after
#
# LOADGEN_* still override individual knobs on top of the chosen profile, for
# bisecting one dimension without editing a shipped file:
#   make bench-load PROFILE=massive LOADGEN_DURATION=30s
#
# The timeout is generous because the massive profile spends minutes before its
# measured window even opens: 20,000 objects have to be created and their Added
# rows drained so the churn window measures churn rather than a backlog.
BENCH_TIMEOUT ?= 45m
PROFILE ?= small
PPROF_DIR ?=

.PHONY: bench-load
bench-load: setup-envtest ## Run the load benchmark harness (PROFILE=small|medium|massive) against a dockerized ClickHouse.
	@echo "Starting ClickHouse container '$(CH_IT_CONTAINER)'..."
	@$(CONTAINER_TOOL) rm -f $(CH_IT_CONTAINER) >/dev/null 2>&1 || true
	@$(CONTAINER_TOOL) run -d --name $(CH_IT_CONTAINER) \
		--ulimit nofile=262144:262144 \
		-e CLICKHOUSE_USER=$(CH_IT_USER) -e CLICKHOUSE_PASSWORD=$(CH_IT_PASSWORD) \
		-p 19000:9000 -p 18123:8123 \
		$(CH_IT_IMAGE) >/dev/null
	@trap '$(CONTAINER_TOOL) rm -f $(CH_IT_CONTAINER) >/dev/null 2>&1 || true' EXIT; \
	echo "Waiting for ClickHouse to accept connections..."; \
	for i in $$(seq 1 30); do \
		if $(CONTAINER_TOOL) exec $(CH_IT_CONTAINER) clickhouse-client --user $(CH_IT_USER) --password $(CH_IT_PASSWORD) --query "SELECT 1" >/dev/null 2>&1; then \
			echo "ClickHouse is ready."; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then echo "ClickHouse did not become ready in time"; exit 1; fi; \
		sleep 1; \
	done; \
	KUBEBUILDER_ASSETS="$$("$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" \
	CH_TEST_ADDR=$(CH_IT_ADDR) CH_TEST_USER=$(CH_IT_USER) CH_TEST_PASSWORD=$(CH_IT_PASSWORD) \
		go test -tags=integration ./test/loadgen/ -run TestLoadGenChurn -v -timeout $(BENCH_TIMEOUT) \
			-profile=$(PROFILE) $(if $(PPROF_DIR),-pprof-dir=$(CURDIR)/$(PPROF_DIR),)

# The e2e suite (Task 1.11) is the Phase 1 gate: a real Kind cluster, a real
# in-cluster ClickHouse (test/e2e/manifests/), real CRs, and every assertion made
# by querying ClickHouse directly. It assumes Kind and Docker are installed;
# everything else — the manager image, the ClickHouse image, the operator install
# and the backend — the suite brings up itself.
#
# Runtime target: under 15 minutes end to end on a developer machine with the
# ClickHouse image already pulled. The bulk of that is the manager image build,
# the two `kind load`s and the RBAC-recovery scenario, which waits out the rule
# reconciler's two-minute resync on purpose (an RBAC grant applied after the fact
# must self-heal without a restart). Go's default 10-minute test timeout would
# kill the suite mid-run, hence the explicit -timeout below.
#
# kubectl kuberc is disabled by default for test isolation; enable with:
# - KUBECTL_KUBERC=true
KIND_CLUSTER ?= kubestream-test-e2e

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) ;; \
	esac

E2E_TIMEOUT ?= 30m
# Teardown is unconditional: the suite reuses an existing cluster, so a failed run
# that left one behind would poison the next run with its own leftovers rather
# than fail on its own merits. Failures are still debuggable — the suite dumps the
# operator's log into the test output — and E2E_KEEP_CLUSTER=true keeps the
# cluster up when that is not enough.
E2E_KEEP_CLUSTER ?= false

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	@set +e; \
	KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) go test -tags=e2e ./test/e2e/ -v -ginkgo.v -timeout $(E2E_TIMEOUT); \
	status=$$?; \
	if [ "$(E2E_KEEP_CLUSTER)" = "true" ]; then \
		echo "E2E_KEEP_CLUSTER=true: leaving Kind cluster '$(KIND_CLUSTER)' up for inspection."; \
	else \
		$(MAKE) cleanup-test-e2e; \
	fi; \
	exit $$status

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

# The chaos suite (Task 2.1) is the Phase 2 failure-mode gate: the same real
# operator on a real Kind cluster as the e2e suite, but against a ClickHouse the
# suite stops and starts, with the operator killed mid-flight and its hand-off
# queue driven into saturation. It gets its own Kind cluster because its very
# first scenario needs the operator to boot against a backend that is not there —
# the opposite of the state every e2e scenario starts from — and because a
# half-finished chaos run must never be what an e2e run inherits.
#
# Runtime is dominated by waiting, not by work: three of the five scenarios have
# to outlast the ClickHouse writer's 60-second per-batch retry budget before the
# failure they are testing is even observable, and two of those wait out two full
# cycles. Budget an hour; hence the timeout below.
CHAOS_KIND_CLUSTER ?= kubestream-test-chaos
CHAOS_TIMEOUT ?= 60m
# As with the e2e suite, teardown is unconditional so a failed run cannot poison
# the next one; CHAOS_KEEP_CLUSTER=true keeps the cluster up for inspection when
# the operator log the suite dumps is not enough.
CHAOS_KEEP_CLUSTER ?= false

.PHONY: setup-test-chaos
setup-test-chaos: ## Set up a Kind cluster for the chaos suite if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(CHAOS_KIND_CLUSTER)"*) \
			echo "Kind cluster '$(CHAOS_KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(CHAOS_KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(CHAOS_KIND_CLUSTER) ;; \
	esac

.PHONY: test-chaos
test-chaos: setup-test-chaos manifests generate fmt vet ## Run the chaos suite. Expects an isolated environment using Kind.
	@set +e; \
	KIND=$(KIND) KIND_CLUSTER=$(CHAOS_KIND_CLUSTER) \
		go test -tags=chaos ./test/chaos/ -v -ginkgo.v -timeout $(CHAOS_TIMEOUT); \
	status=$$?; \
	if [ "$(CHAOS_KEEP_CLUSTER)" = "true" ]; then \
		echo "CHAOS_KEEP_CLUSTER=true: leaving Kind cluster '$(CHAOS_KIND_CLUSTER)' up for inspection."; \
	else \
		$(MAKE) cleanup-test-chaos; \
	fi; \
	exit $$status

.PHONY: cleanup-test-chaos
cleanup-test-chaos: ## Tear down the Kind cluster used for the chaos suite
	@$(KIND) delete cluster --name $(CHAOS_KIND_CLUSTER)

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

# OPERATOR_NAMESPACE is where `make run` looks up sink credentials Secrets. In
# cluster the Deployment supplies it from the downward API; running from a host
# there is no pod to read it from, and the operator refuses to guess.
OPERATOR_NAMESPACE ?= kubestream-system

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go --operator-namespace=$(OPERATOR_NAMESPACE)

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name kubestream-builder
	$(CONTAINER_TOOL) buildx use kubestream-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm kubestream-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

# The e2e overlay is config/default plus a pinned image and
# --ch-auto-create-schema (see test/e2e/manifests/operator/kustomization.yaml).
# These two targets exist so the suite installs through the same tool bootstrap
# every other deployment target uses, rather than reaching for a kustomize binary
# that may not have been downloaded yet.
.PHONY: deploy-e2e
deploy-e2e: manifests kustomize ## Deploy the controller with the e2e overlay. Used by test-e2e.
	"$(KUSTOMIZE)" build test/e2e/manifests/operator | "$(KUBECTL)" apply -f -

.PHONY: undeploy-e2e
undeploy-e2e: kustomize ## Remove the controller installed by deploy-e2e. Used by test-e2e.
	"$(KUSTOMIZE)" build test/e2e/manifests/operator | "$(KUBECTL)" delete --ignore-not-found=true -f -

# The chaos overlay is the same shape as the e2e one (see
# test/chaos/manifests/operator/kustomization.yaml). The two exist separately so
# each suite can evolve its own fixtures without silently changing the other's.
.PHONY: deploy-chaos
deploy-chaos: manifests kustomize ## Deploy the controller with the chaos overlay. Used by test-chaos.
	"$(KUSTOMIZE)" build test/chaos/manifests/operator | "$(KUBECTL)" apply -f -

.PHONY: undeploy-chaos
undeploy-chaos: kustomize ## Remove the controller installed by deploy-chaos. Used by test-chaos.
	"$(KUSTOMIZE)" build test/chaos/manifests/operator | "$(KUBECTL)" delete --ignore-not-found=true -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.20.1

#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?([0-9]+)\.([0-9]+).*/release-\1.\2/')

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.11.4
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		$(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
