# Image URL to use all building/pushing image targets
IMG ?= controller:latest
# VERSION is the version of the *shipped artifacts*: the Helm chart's appVersion
# and the image the committed dist/install.yaml names. It is deliberately not
# IMG's default — IMG is a developer's local build tag, VERSION is what a user
# installs — and it is the one place a release bump has to happen (see
# deploy/charts/kubestream/Chart.yaml, which must carry the same value).
VERSION ?= 0.1.0
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

# helm and kustomize are prerequisites because test/chart renders the chart and
# builds config/default to compare the two install paths against each other
# (Task 2.4). Those tests skip when the binaries are missing, so without this the
# chart's only real guard against drift would silently not run.
.PHONY: test
test: manifests generate fmt vet setup-envtest helm kustomize ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# Integration tests run against a real, dockerized ClickHouse (build tag
# `integration`). The target boots a throwaway container, waits for it to
# accept queries, runs the tagged tests, and always tears the container down.
# Two suites run here: internal/sink exercises the writer and reader paths, and
# test/queries executes every query kubestream publishes — docs/QUERIES.md and
# the Grafana dashboards — against tables built from the shipped DDL alone, which
# is how Task 3.2's "only frozen-schema columns" criterion is asserted. The two
# use separate databases, because `go test` runs package binaries concurrently.
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
		go test -tags=integration ./internal/sink/... ./test/queries/... -run Integration -v

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

# E2E_INSTALL selects *how* the suite installs the operator, and nothing else:
# kustomize (config/default, the default), helm (the chart) or installer
# (dist/install.yaml). The scenarios are identical in all three — the chart and
# the installer produce the same object names as config/default, which is what
# makes "the 1.11 happy path passes unmodified" a testable claim rather than a
# hopeful one (Task 2.4). E2E_FOCUS, when set, is passed to Ginkgo as a focus
# regex.
E2E_INSTALL ?= kustomize
E2E_FOCUS ?=

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	@set +e; \
	KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) E2E_INSTALL=$(E2E_INSTALL) \
		go test -tags=e2e ./test/e2e/ -v -ginkgo.v -timeout $(E2E_TIMEOUT) \
			$(if $(E2E_FOCUS),-ginkgo.focus="$(E2E_FOCUS)",); \
	status=$$?; \
	if [ "$(E2E_KEEP_CLUSTER)" = "true" ]; then \
		echo "E2E_KEEP_CLUSTER=true: leaving Kind cluster '$(KIND_CLUSTER)' up for inspection."; \
	else \
		$(MAKE) cleanup-test-e2e; \
	fi; \
	exit $$status

# The two install-path smokes. Each gets its own Kind cluster: `helm install`
# refuses to adopt objects it does not own, so a cluster left behind by a
# kustomize run would fail the helm smoke for a reason that has nothing to do
# with the chart.
#
# They focus the Phase 1 happy path — the acceptance criterion is that *it*
# passes unmodified against either install — which keeps each smoke to roughly one
# scenario rather than three times the full suite. Run the whole suite against an
# install path with E2E_FOCUS= (empty).
SMOKE_FOCUS ?= streams a Deployment
HELM_KIND_CLUSTER ?= kubestream-test-e2e-helm
INSTALLER_KIND_CLUSTER ?= kubestream-test-e2e-installer

.PHONY: test-e2e-helm
test-e2e-helm: ## Run the e2e smoke against a `helm install` of deploy/charts/kubestream.
	$(MAKE) test-e2e E2E_INSTALL=helm E2E_FOCUS="$(SMOKE_FOCUS)" KIND_CLUSTER=$(HELM_KIND_CLUSTER)

.PHONY: test-e2e-installer
test-e2e-installer: ## Run the e2e smoke against `kubectl apply -f dist/install.yaml`.
	$(MAKE) test-e2e E2E_INSTALL=installer E2E_FOCUS="$(SMOKE_FOCUS)" KIND_CLUSTER=$(INSTALLER_KIND_CLUSTER)

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

# dist/install.yaml is the single-file install path: `kubectl apply -f` it and you
# have the CRDs, the RBAC and the manager. It is committed, and `make
# build-installer` with no arguments reproduces it byte for byte — that is what
# makes it a *versioned* manifest rather than merely a generated one, since the
# image it names is $(INSTALLER_IMG), pinned to VERSION, not a floating tag.
# Override INSTALLER_IMG to point the manifest at your own build (the e2e
# installer smoke does exactly that).
#
# The image override is applied through a throwaway overlay in dist/ rather than
# with `kustomize edit set image` inside config/manager. That is deliberate:
# editing the committed base rewrites the image *name* there, and both the e2e and
# the chaos overlays select the manager image by the name `controller` — a
# rewritten base would silently stop matching and each suite would then run
# whatever image the base now pins, with no error anywhere.
INSTALLER_IMG ?= ghcr.io/yelzhy/kubestream:v$(VERSION)

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	printf '%s\n' \
		'# Generated by `make build-installer`; deleted again once dist/install.yaml is written.' \
		'apiVersion: kustomize.config.k8s.io/v1beta1' \
		'kind: Kustomization' \
		'resources:' \
		'- ../config/default' \
		> dist/kustomization.yaml
	cd dist && "$(KUSTOMIZE)" edit set image controller=$(INSTALLER_IMG)
	"$(KUSTOMIZE)" build dist > dist/install.yaml
	rm -f dist/kustomization.yaml

##@ Packaging

# The Helm chart is the second supported install path, and it is held to being
# the *same install*: it renders the same object names as `kustomize build
# config/default`, and test/chart asserts that RBAC rule for RBAC rule, so the
# acceptance suite can run against either one unmodified.
CHART_DIR ?= deploy/charts/kubestream
CHART_RELEASE ?= kubestream
CHART_NAMESPACE ?= kubestream-system

# The Kubernetes version rendered manifests are validated against. Derived from
# the same k8s.io/api version envtest is pinned to, so "the pinned Kubernetes
# version" means one thing across the whole repo. kubeconform wants a full
# x.y.z; the schema repository publishes .0 for every minor.
KUBECONFORM_K8S_VERSION ?= $(ENVTEST_K8S_VERSION).0

# Kinds kubeconform cannot judge, named one by one rather than waved through with
# -ignore-missing-schemas — with an explicit list, a *typo'd* kind is still a hard
# failure:
#
#   - our own three CRs have no published upstream schema. Their shape is
#     enforced by the CRDs themselves, in envtest (api/v1alpha1) and in the kind
#     smokes, which is stricter than anything kubeconform could do here.
#   - CustomResourceDefinition: the upstream schema repository publishes no
#     apiextensions group at all, at any version. The CRDs are controller-gen
#     output, asserted by api/v1alpha1/crdmanifests_test.go and applied to a real
#     apiserver by every envtest and kind run.
#
# --include-crds is still passed when rendering, so a CRD that failed to render
# at all remains a parse error here.
KUBECONFORM_SKIP ?= ClickHouseSink,StreamRule,ClusterStreamRule,CustomResourceDefinition
KUBECONFORM_FLAGS ?= -strict -summary \
	-kubernetes-version $(KUBECONFORM_K8S_VERSION) \
	-skip $(KUBECONFORM_SKIP)

# --kube-version is not optional: without a cluster to ask, `helm template`
# assumes a very old Kubernetes and would reject the chart's own kubeVersion
# constraint. --include-crds makes the rendering match what `helm install`
# actually applies, so the CRDs are validated too rather than quietly skipped.
HELM_TEMPLATE_FLAGS ?= --namespace $(CHART_NAMESPACE) \
	--kube-version $(KUBECONFORM_K8S_VERSION) --include-crds

.PHONY: helm-sync
helm-sync: manifests ## Refresh the chart's copies of the CRDs and the RBAC presets from config/.
	@# Helm requires CRDs to live in the chart's crds/ directory and preset rules
	@# are read out of files/presets/ at render time, so both are copies of
	@# config/. Copies drift, so they are generated by this target and asserted
	@# byte-identical to their sources by test/chart — a stale copy fails `make
	@# test`, it does not ship.
	rm -rf "$(CHART_DIR)/crds" "$(CHART_DIR)/files/presets"
	mkdir -p "$(CHART_DIR)/crds" "$(CHART_DIR)/files/presets"
	cp config/crd/bases/*.yaml "$(CHART_DIR)/crds/"
	cp config/rbac/presets/*.yaml "$(CHART_DIR)/files/presets/"
	@echo "Synced $$(ls -1 "$(CHART_DIR)/crds" | wc -l | tr -d ' ') CRDs and $$(ls -1 "$(CHART_DIR)/files/presets" | wc -l | tr -d ' ') presets into $(CHART_DIR)."

.PHONY: helm-lint
helm-lint: helm ## Lint the chart against its default values and every ci/ values file.
	"$(HELM)" lint "$(CHART_DIR)" --strict --kube-version $(KUBECONFORM_K8S_VERSION)
	@for values in "$(CHART_DIR)"/ci/*-values.yaml; do \
		echo "==> helm lint --strict --values $$values"; \
		"$(HELM)" lint "$(CHART_DIR)" --strict --kube-version $(KUBECONFORM_K8S_VERSION) \
			--values "$$values" || exit 1; \
	done

.PHONY: helm-template
helm-template: helm ## Render the chart to stdout (VALUES=<file> to render a values file).
	@"$(HELM)" template "$(CHART_RELEASE)" "$(CHART_DIR)" $(HELM_TEMPLATE_FLAGS) \
		$(if $(VALUES),--values "$(VALUES)",)

.PHONY: helm-kubeconform
helm-kubeconform: helm kubeconform ## Validate the chart's rendered output against the pinned Kubernetes version.
	@echo "==> kubeconform: $(CHART_DIR) (default values), Kubernetes $(KUBECONFORM_K8S_VERSION)"
	"$(HELM)" template "$(CHART_RELEASE)" "$(CHART_DIR)" $(HELM_TEMPLATE_FLAGS) \
		| "$(KUBECONFORM)" $(KUBECONFORM_FLAGS) -
	@for values in "$(CHART_DIR)"/ci/*-values.yaml; do \
		echo "==> kubeconform: $(CHART_DIR) ($$values)"; \
		"$(HELM)" template "$(CHART_RELEASE)" "$(CHART_DIR)" $(HELM_TEMPLATE_FLAGS) \
			--values "$$values" | "$(KUBECONFORM)" $(KUBECONFORM_FLAGS) - || exit 1; \
	done

.PHONY: installer-kubeconform
installer-kubeconform: build-installer kubeconform ## Validate dist/install.yaml against the pinned Kubernetes version.
	@echo "==> kubeconform: dist/install.yaml, Kubernetes $(KUBECONFORM_K8S_VERSION)"
	"$(KUBECONFORM)" $(KUBECONFORM_FLAGS) dist/install.yaml

.PHONY: verify-packaging
verify-packaging: helm-lint helm-kubeconform installer-kubeconform ## Lint and validate both install paths (chart + dist/install.yaml).

# The shipped dashboard and alert rules (Task 2.5). test/observability already runs
# under `make test` — it validates both artifacts against their JSON Schemas and
# checks that every metric they query is one the operator's collectors declare.
# What this target adds is promtool: whether the PromQL actually parses is a
# question only Prometheus's own checker can answer, and exporting PROMTOOL turns
# that sub-check from a skip into a requirement.
.PHONY: verify-observability
verify-observability: promtool ## Validate deploy/grafana and deploy/prometheus, including `promtool check rules`.
	@echo "==> validating the dashboard and alert rules (promtool: $(PROMTOOL))"
	PROMTOOL="$(PROMTOOL)" go test ./test/observability/... -count=1

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

# The Helm install path the e2e suite smokes (E2E_INSTALL=helm).
#
# The three steps ahead of `helm upgrade --install` are the real user path, not
# test scaffolding: Helm's --create-namespace cannot label a namespace, and the
# chart deliberately never templates a password, so both are the installer's job.
# Doing them here rather than after the install matters — the namespace is
# `restricted` *before* the manager pod is ever admitted, so a chart that violated
# Pod Security would fail this deploy instead of slipping in ahead of the label.
E2E_HELM_VALUES ?= test/e2e/manifests/helm-values.yaml
# The password is the same placeholder config/manager ships, so both install paths
# hand the suite's ClickHouse fixture an identical credential.
E2E_CH_PASSWORD ?= changeme

.PHONY: deploy-e2e-helm
deploy-e2e-helm: helm helm-sync ## Install the operator with the Helm chart, e2e values. Used by test-e2e.
	"$(KUBECTL)" create namespace $(CHART_NAMESPACE) --dry-run=client -o yaml | "$(KUBECTL)" apply -f -
	"$(KUBECTL)" label namespace $(CHART_NAMESPACE) pod-security.kubernetes.io/enforce=restricted --overwrite
	"$(KUBECTL)" create secret generic kubestream-clickhouse-credentials \
		--namespace $(CHART_NAMESPACE) --from-literal=password=$(E2E_CH_PASSWORD) \
		--dry-run=client -o yaml | "$(KUBECTL)" apply -f -
	"$(HELM)" upgrade --install $(CHART_RELEASE) "$(CHART_DIR)" \
		--namespace $(CHART_NAMESPACE) --values $(E2E_HELM_VALUES) --wait --timeout 5m

.PHONY: undeploy-e2e-helm
undeploy-e2e-helm: helm ## Remove the Helm-installed controller. Used by test-e2e.
	@# The CRDs are left behind: Helm never deletes what it installed from crds/,
	@# and pretending otherwise here would hide that from anyone reading this as
	@# documentation of the uninstall path.
	-"$(HELM)" uninstall $(CHART_RELEASE) --namespace $(CHART_NAMESPACE) --wait
	-"$(KUBECTL)" delete namespace $(CHART_NAMESPACE) --ignore-not-found=true

# The single-file install path the e2e suite smokes (E2E_INSTALL=installer):
# exactly what a user gets from `kubectl apply -f dist/install.yaml`, built here
# against the image the suite side-loads.
#
# The one delta is --ch-auto-create-schema, appended after the apply. The shipped
# manifest does not carry it (the operator runs no DDL uninvited), and the suite
# needs the schema to reach a freshly-started ClickHouse; a post-apply patch keeps
# that delta visible instead of burying it in a second overlay that would then
# drift from the artifact under test.
.PHONY: deploy-e2e-installer
deploy-e2e-installer: kustomize ## Install the operator from dist/install.yaml. Used by test-e2e.
	$(MAKE) build-installer INSTALLER_IMG=$(E2E_INSTALLER_IMG)
	"$(KUBECTL)" apply -f dist/install.yaml
	"$(KUBECTL)" patch deployment kubestream-controller-manager -n $(CHART_NAMESPACE) --type=json \
		-p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--ch-auto-create-schema"}]'

.PHONY: undeploy-e2e-installer
undeploy-e2e-installer: ## Remove the controller installed from dist/install.yaml. Used by test-e2e.
	"$(KUBECTL)" delete --ignore-not-found=true -f dist/install.yaml

# The image the installer smoke pins. It must match the tag test/e2e builds and
# side-loads into Kind (managerImage in test/e2e/e2e_suite_test.go), for the same
# reason the e2e kustomize overlay pins one: a manifest naming an image the node
# does not have would sit in ImagePullBackOff, not fail.
E2E_INSTALLER_IMG ?= example.com/kubestream:v0.0.1

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
# Helm and kubeconform are bootstrapped into bin/ like every other tool here,
# through `go install` — both are Go programs, so the packaging targets and the
# chart tests need no separately-provisioned toolchain on a developer machine or
# in CI.
HELM ?= $(LOCALBIN)/helm
KUBECONFORM ?= $(LOCALBIN)/kubeconform
# promtool is the one tool here that is not `go install`-able: the Prometheus
# module carries replace directives, which Go refuses to honour for a package
# installed from outside its own module. It is fetched from the release tarball
# instead — see the promtool target.
PROMTOOL ?= $(LOCALBIN)/promtool

## Tool Versions
#
# Every version pinned here is subject to one hard ceiling: the tool's module must
# declare a `go` directive no newer than this repository's own (see go.mod). CI runs
# actions/setup-go with `go-version-file: go.mod`, and that action additionally
# exports GOTOOLCHAIN=local — which switches off the automatic toolchain download
# that would otherwise paper over the gap. A tool needing a newer Go therefore does
# not quietly fetch one; `go install` fails outright and takes the whole bootstrap
# with it:
#
#   go: helm.sh/helm/v3@v3.21.3 requires go >= 1.26.0 (running go 1.25.7; GOTOOLCHAIN=local)
#
# The failure never reproduces on a developer machine that already has bin/ populated
# or a newer Go on $PATH, so before raising any pin below, check the tool's go.mod —
# `go mod download -x` on the module, or the .mod file on proxy.golang.org.
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.20.1
# helm v3.21.1 and kubeconform v0.8.0 are the first releases of each to require
# Go 1.26; these are the newest that still build under the go.mod `go` directive.
HELM_VERSION ?= v3.21.0
KUBECONFORM_VERSION ?= v0.7.0
# The Prometheus *release* version, which is what the download URL is built from
# (the Go module version of the same release reads v0.313.2 — the repository never
# adopted a /v3 module path — but no Go path is involved here).
PROMTOOL_VERSION ?= 3.13.2

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

.PHONY: helm
helm: $(HELM) ## Download helm locally if necessary.
$(HELM): $(LOCALBIN)
	$(call go-install-tool,$(HELM),helm.sh/helm/v3/cmd/helm,$(HELM_VERSION))

.PHONY: kubeconform
kubeconform: $(KUBECONFORM) ## Download kubeconform locally if necessary.
$(KUBECONFORM): $(LOCALBIN)
	$(call go-install-tool,$(KUBECONFORM),github.com/yannh/kubeconform/cmd/kubeconform,$(KUBECONFORM_VERSION))

# promtool is extracted from the official Prometheus release archive rather than
# `go install`ed, because the Prometheus module's replace directives make it
# uninstallable as a package. The version-suffixed binary and the symlink follow
# the same convention as go-install-tool, so switching PROMTOOL_VERSION re-fetches
# rather than silently keeping the old binary.
.PHONY: promtool
promtool: $(PROMTOOL) ## Download promtool locally if necessary.
$(PROMTOOL): $(LOCALBIN)
	@[ -f "$(PROMTOOL)-$(PROMTOOL_VERSION)" ] && \
		[ "$$(readlink -- "$(PROMTOOL)" 2>/dev/null)" = "$(PROMTOOL)-$(PROMTOOL_VERSION)" ] || { \
	set -e; \
	os=$$(go env GOOS); arch=$$(go env GOARCH); \
	archive="prometheus-$(PROMTOOL_VERSION).$$os-$$arch"; \
	url="https://github.com/prometheus/prometheus/releases/download/v$(PROMTOOL_VERSION)/$$archive.tar.gz"; \
	echo "Downloading $$url"; \
	tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tmp"' EXIT; \
	curl -fsSL "$$url" | tar -xzf - -C "$$tmp" "$$archive/promtool"; \
	rm -f "$(PROMTOOL)"; \
	mv "$$tmp/$$archive/promtool" "$(PROMTOOL)-$(PROMTOOL_VERSION)"; \
	chmod +x "$(PROMTOOL)-$(PROMTOOL_VERSION)"; \
	} ;\
	ln -sf "$$(realpath "$(PROMTOOL)-$(PROMTOOL_VERSION)")" "$(PROMTOOL)"

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
