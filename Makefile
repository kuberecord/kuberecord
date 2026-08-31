# Image URL to use all building/pushing image targets
IMG ?= controller:latest
# VERSION is the version of the *shipped artifacts*: the Helm chart's appVersion
# and the image the committed dist/install.yaml names. It is deliberately not
# IMG's default — IMG is a developer's local build tag, VERSION is what a user
# installs — and it is the one place a release bump has to happen (see
# deploy/charts/kuberecord/Chart.yaml, which must carry the same value).
VERSION ?= 0.2.1
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

# Integration tests run against real, dockerized backends (build tag
# `integration`). The target boots throwaway containers, waits for each to answer,
# runs the tagged tests, and always tears them down.
#
# Seven suites run here. internal/sink/clickhouse exercises the ClickHouse writer
# and reader paths, and internal/query/clickhouse runs the whole read-plane
# conformance suite against the real engine (Task 9.3) — the half a stand-in
# connection cannot prove, since no fake executes SQL: that FINAL really collapses
# an unmerged duplicate, that a DateTime64(9) bound compares at nanosecond
# precision, that JSONExtractString reaches into both Event spellings. Like
# test/queries it works in a database of its own, for the same reason.
# test/queries executes every query kuberecord publishes —
# docs/QUERIES.md and the Grafana dashboards — against tables built from the
# shipped DDL alone, which is how Task 3.2's "only frozen-schema columns"
# criterion is asserted; it uses a database of its own, because `go test` runs
# package binaries concurrently and two suites recreating resource_states in one
# database would delete each other's fixtures. The same package also runs every
# published *DuckDB* recipe against a real archive in MinIO through the duckdb CLI
# (Task 7.2) — a read path an S3 archive has, since the sink itself has none
# (D12) — and requires rows back, so a recipe that parses but selects nothing
# fails. internal/sink/s3/awsstore writes through the real S3 Writer to MinIO
# and reads the objects back (Task 6.6): key layout, decode fidelity, retry
# idempotency, both rotation triggers and the Object Lock headers, none of which a
# fake store can vouch for. And internal/query/objectsource/awssource runs the
# shipped read source against the same MinIO (Task 10.1), for the one thing no
# fake can establish: that a directory and a bucket answer a listing identically.
# Everything above that seam is tested against a directory, so the day the two
# diverge is the day those tests stop saying anything about a bucket. The three S3
# suites create a bucket per test, so they need no database-style isolation.
# test/agreement is the seventh and the only one that needs both containers at
# once (Task 11.11): it seeds one declarative corpus into both query backends —
# rows through the shipped DDL, encoded objects through the archive format — and
# requires them to answer identically. Each backend already passes the read-plane
# conformance suite, but each is seeded by its own harness into its own storage
# shape, so nothing else compares the two; a divergence in incarnation
# resolution, ordering or reconstruction base would be invisible without this. It
# lives under test/ because seeding one backend needs the ClickHouse driver and
# the other needs the S3 SDK, and the depguard rules confine each to two packages
# under internal/ with no package on both lists. It works in a database of its
# own, for the same reason the two suites above do.
#
# Non-standard host ports throughout, so this never collides with a developer's
# own ClickHouse on 9000/8123 or MinIO on 9000. ClickHouse's image default user is
# localhost-only; CLICKHOUSE_USER/PASSWORD create an any-network user the host
# test can actually authenticate as through the port mapping. MinIO requires a
# root password of at least eight characters, which is why the shared credential
# spells the project name rather than something shorter.
CH_IT_IMAGE ?= clickhouse/clickhouse-server:24.8
CH_IT_CONTAINER ?= kuberecord-it-clickhouse
CH_IT_ADDR ?= 127.0.0.1:19000
CH_IT_USER ?= kuberecord
CH_IT_PASSWORD ?= kuberecord

MINIO_IT_IMAGE ?= minio/minio:RELEASE.2025-04-22T22-12-26Z
MINIO_IT_CONTAINER ?= kuberecord-it-minio
MINIO_IT_ENDPOINT ?= http://127.0.0.1:19100
MINIO_IT_USER ?= kuberecord
MINIO_IT_PASSWORD ?= kuberecord

.PHONY: test-integration
test-integration: duckdb ## Run integration tests against a dockerized ClickHouse and MinIO.
	@echo "Starting ClickHouse container '$(CH_IT_CONTAINER)'..."
	@$(CONTAINER_TOOL) rm -f $(CH_IT_CONTAINER) >/dev/null 2>&1 || true
	@$(CONTAINER_TOOL) run -d --name $(CH_IT_CONTAINER) \
		--ulimit nofile=262144:262144 \
		-e CLICKHOUSE_USER=$(CH_IT_USER) -e CLICKHOUSE_PASSWORD=$(CH_IT_PASSWORD) \
		-p 19000:9000 -p 18123:8123 \
		$(CH_IT_IMAGE) >/dev/null
	@echo "Starting MinIO container '$(MINIO_IT_CONTAINER)'..."
	@$(CONTAINER_TOOL) rm -f $(MINIO_IT_CONTAINER) >/dev/null 2>&1 || true
	@$(CONTAINER_TOOL) run -d --name $(MINIO_IT_CONTAINER) \
		-e MINIO_ROOT_USER=$(MINIO_IT_USER) -e MINIO_ROOT_PASSWORD=$(MINIO_IT_PASSWORD) \
		-p 19100:9000 \
		$(MINIO_IT_IMAGE) server /data >/dev/null
	@trap '$(CONTAINER_TOOL) rm -f $(CH_IT_CONTAINER) $(MINIO_IT_CONTAINER) >/dev/null 2>&1 || true' EXIT; \
	echo "Waiting for ClickHouse to accept connections..."; \
	for i in $$(seq 1 30); do \
		if $(CONTAINER_TOOL) exec $(CH_IT_CONTAINER) clickhouse-client --user $(CH_IT_USER) --password $(CH_IT_PASSWORD) --query "SELECT 1" >/dev/null 2>&1; then \
			echo "ClickHouse is ready."; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then echo "ClickHouse did not become ready in time"; exit 1; fi; \
		sleep 1; \
	done; \
	echo "Waiting for MinIO to accept requests..."; \
	for i in $$(seq 1 30); do \
		if $(CONTAINER_TOOL) exec $(MINIO_IT_CONTAINER) curl -sf http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1; then \
			echo "MinIO is ready."; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then echo "MinIO did not become ready in time"; exit 1; fi; \
		sleep 1; \
	done; \
	CH_TEST_ADDR=$(CH_IT_ADDR) CH_TEST_USER=$(CH_IT_USER) CH_TEST_PASSWORD=$(CH_IT_PASSWORD) \
	S3_TEST_ENDPOINT=$(MINIO_IT_ENDPOINT) \
	S3_TEST_ACCESS_KEY_ID=$(MINIO_IT_USER) S3_TEST_SECRET_ACCESS_KEY=$(MINIO_IT_PASSWORD) \
	DUCKDB="$(DUCKDB)" \
		go test -tags=integration ./internal/sink/... ./internal/query/... ./test/queries/... ./test/agreement/... -run Integration -v

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
# Task 6.6 added the archive scenario — a real in-cluster MinIO, an S3Sink, and
# assertions read straight out of the bucket. Measured cost: **about 2 minutes**
# with the MinIO image already pulled (a cold first run adds its `docker pull`),
# against the ~4 minutes that task budgets. Where it goes: side-loading the image
# and rolling MinIO out (~10s), the sink's first write probe and the rule's
# reconcile (~25s), then ~110s for the lifecycle itself — three rotation windows at
# the CRD's 10-second floor, the operator going down and coming back, and one
# 35-second quiet window holding the "no Deleted record" claim open, which has to
# outlast a rotation period or it would be asserting latency rather than absence.
# The fixture is brought up by that scenario rather than in BeforeSuite, so a
# focused run (the two install-path smokes below) pays none of it.
#
# kubectl kuberc is disabled by default for test isolation; enable with:
# - KUBECTL_KUBERC=true
KIND_CLUSTER ?= kuberecord-test-e2e

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
HELM_KIND_CLUSTER ?= kuberecord-test-e2e-helm
HELM_OCI_KIND_CLUSTER ?= kuberecord-test-e2e-helm-oci
INSTALLER_KIND_CLUSTER ?= kuberecord-test-e2e-installer

.PHONY: test-e2e-helm
test-e2e-helm: ## Run the e2e smoke against a `helm install` of deploy/charts/kuberecord.
	$(MAKE) test-e2e E2E_INSTALL=helm E2E_FOCUS="$(SMOKE_FOCUS)" KIND_CLUSTER=$(HELM_KIND_CLUSTER)

# The same smoke, installed from an OCI reference instead of from the checkout
# (Task 8.1). Its own cluster for the reason the other two have theirs: `helm
# install` will not adopt objects it does not own, and the two Helm smokes install
# the same release name.
.PHONY: test-e2e-helm-oci
test-e2e-helm-oci: ## Run the e2e smoke against a `helm install` of the chart pushed to an OCI registry.
	$(MAKE) test-e2e E2E_INSTALL=helm-oci E2E_FOCUS="$(SMOKE_FOCUS)" KIND_CLUSTER=$(HELM_OCI_KIND_CLUSTER)

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
CHAOS_KIND_CLUSTER ?= kuberecord-test-chaos
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

##@ Quickstart

# The evaluation path (Task 3.4): a kind cluster, a single-node ClickHouse, the
# operator built from this clone, and rows to query — in under ten minutes.
#
# The whole recipe lives in examples/quickstart/, script included, so it can be
# read as documentation and run by hand step by step. This target only supplies
# the tool paths the script would otherwise have to guess at, and bootstraps
# kustomize into bin/ the same way every other target does.
#
# The script asserts that rows exist and exits non-zero if none arrive, which is
# what makes .github/workflows/quickstart.yml a test of the ten-minute claim
# rather than a restatement of it. QUICKSTART_BUDGET_SECONDS is what CI sets to
# enforce the time half of it; it is unset here on purpose, because a slow laptop
# is not a failed install.
QUICKSTART_CLUSTER ?= kuberecord-quickstart
QUICKSTART_BUDGET_SECONDS ?=
QUICKSTART_SCRIPT := examples/quickstart/quickstart.sh

.PHONY: quickstart
quickstart: kustomize ## Stand up kind + ClickHouse + the operator and query the rows it records.
	@KIND=$(KIND) KUBECTL=$(KUBECTL) KUSTOMIZE="$(KUSTOMIZE)" \
		CONTAINER_TOOL=$(CONTAINER_TOOL) MAKE="$(MAKE)" \
		QUICKSTART_CLUSTER=$(QUICKSTART_CLUSTER) \
		QUICKSTART_BUDGET_SECONDS=$(QUICKSTART_BUDGET_SECONDS) \
		./$(QUICKSTART_SCRIPT)

.PHONY: quickstart-down
quickstart-down: ## Delete the quickstart kind cluster and everything in it.
	@KIND=$(KIND) QUICKSTART_CLUSTER=$(QUICKSTART_CLUSTER) ./$(QUICKSTART_SCRIPT) down

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

# OPERATOR_NAMESPACE is where `make run` looks up sink credentials Secrets. In
# cluster the Deployment supplies it from the downward API; running from a host
# there is no pod to read it from, and the operator refuses to guess.
OPERATOR_NAMESPACE ?= kuberecord-system

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

# BUILDX_OUTPUT is what buildx does with what it builds. `--push` is the only
# useful answer for a real release, and *empty* is the answer for a rehearsal:
# buildx then builds every platform and discards the result, which is the only way
# to prove the multi-arch build compiles without a registry to write to (see
# release-dry-run).
BUILDX_OUTPUT ?= --push

.PHONY: docker-buildx
docker-buildx: ## Build (and, unless BUILDX_OUTPUT is empty, push) the manager image for every platform in PLATFORMS.
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	@# The build itself is *not* prefixed with `-`: a cross-compilation error or a
	@# registry that refuses the push must fail this target, or a release publishes
	@# a manifest naming an image that was never built. Creating and removing the
	@# builder may legitimately be a no-op, so those stay tolerant, and the cleanup
	@# runs from a trap — two trailing recipe lines would be skipped by make on
	@# exactly the failure that most needs the tree left clean.
	@set -e; \
	trap '$(CONTAINER_TOOL) buildx rm kuberecord-builder >/dev/null 2>&1 || true; rm -f Dockerfile.cross' EXIT; \
	$(CONTAINER_TOOL) buildx create --name kuberecord-builder --use >/dev/null 2>&1 \
		|| $(CONTAINER_TOOL) buildx use kuberecord-builder; \
	set -x; \
	$(CONTAINER_TOOL) buildx build $(BUILDX_OUTPUT) --platform=$(PLATFORMS) --tag $(IMG) -f Dockerfile.cross .

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
# IMAGE_REPO is the one place the published registry is named. Both install
# artifacts derive the image they pin from it, so a release cannot push to one
# registry and hand out manifests naming another.
IMAGE_REPO ?= ghcr.io/kuberecord/kuberecord
INSTALLER_IMG ?= $(IMAGE_REPO):v$(VERSION)

# INSTALLER_OUT is where the manifest is written. The default is the committed
# path — that is the file the staleness check in CI compares — and a release
# renders into dist/release/ instead, so packaging a tag never dirties the
# reviewed artifact.
INSTALLER_OUT ?= dist/install.yaml

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist "$$(dirname "$(INSTALLER_OUT)")"
	printf '%s\n' \
		'# Generated by `make build-installer`; deleted again once the manifest is written.' \
		'apiVersion: kustomize.config.k8s.io/v1beta1' \
		'kind: Kustomization' \
		'resources:' \
		'- ../config/default' \
		> dist/kustomization.yaml
	cd dist && "$(KUSTOMIZE)" edit set image controller=$(INSTALLER_IMG)
	"$(KUSTOMIZE)" build dist > "$(INSTALLER_OUT)"
	rm -f dist/kustomization.yaml

##@ Packaging

# The Helm chart is the second supported install path, and it is held to being
# the *same install*: it renders the same object names as `kustomize build
# config/default`, and test/chart asserts that RBAC rule for RBAC rule, so the
# acceptance suite can run against either one unmodified.
CHART_DIR ?= deploy/charts/kuberecord
CHART_RELEASE ?= kuberecord
CHART_NAMESPACE ?= kuberecord-system

# Where the chart is published as an OCI artifact (Task 8.1), derived from
# IMAGE_REPO so the registry keeps being named in exactly one place: a fork that
# moves IMAGE_REPO moves its chart with it rather than pushing somebody else's
# name. `ghcr.io/kuberecord/kuberecord` therefore yields the namespace
# `ghcr.io/kuberecord/charts`, into which `helm push` writes the chart under its
# own name — the published reference is CHART_OCI_REF below.
#
# This is additive to the `.tgz` a release attaches. It exists because the
# release-asset URL depends on GitHub's redirect from this repository's
# pre-migration path, and that redirect is destroyed for good if a repository
# named `kuberecord` is ever created under the old account. A registry reference
# depends on nothing but the registry.
CHART_OCI_NAMESPACE ?= $(patsubst %/,%,$(dir $(IMAGE_REPO)))/charts
CHART_OCI_REPO ?= oci://$(CHART_OCI_NAMESPACE)
# The reference a signature, a verification or a `helm install` names. `helm
# push` takes the namespace and appends the chart's own name; everything else
# takes the full path, and cosign takes it without the oci:// scheme.
CHART_OCI_REF ?= $(CHART_OCI_NAMESPACE)/$(CHART_RELEASE)
# Passed to every helm command that talks to a registry. Empty against ghcr.io;
# `--plain-http` against the throwaway registry the rehearsal and the OCI install
# smoke run, which speaks HTTP and has no certificate to present.
HELM_REGISTRY_FLAGS ?=

# A throwaway OCI registry on the host, used by two callers that both need a real
# registry and neither of which may publish to a real one: the release
# rehearsal's chart push, and the install smoke that installs from an OCI
# reference on Kind. It is a container rather than a service so `make` owns its
# lifecycle in both cases, including on a maintainer's laptop.
#
# Pinned by digest like every action in .github/workflows: a registry that
# decides what a chart push is talking to is not a place for a floating tag.
# 5001 rather than 5000, which macOS hands to AirPlay Receiver by default.
LOCAL_REGISTRY_IMAGE ?= registry:2@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373
LOCAL_REGISTRY_NAME ?= kuberecord-oci-registry
LOCAL_REGISTRY_PORT ?= 5001
LOCAL_REGISTRY_HOST ?= localhost:$(LOCAL_REGISTRY_PORT)

.PHONY: local-registry-up
local-registry-up: ## Start the throwaway OCI registry the chart push rehearsal and the OCI smoke use.
	@# Recreated rather than reused: a registry left behind by a failed run may
	@# already hold this version of the chart, and a push that silently resolved to
	@# an artifact from a previous run is exactly the failure these callers exist to
	@# catch. Removal is unconditional, so a container in any state is replaced.
	@$(CONTAINER_TOOL) rm -f "$(LOCAL_REGISTRY_NAME)" >/dev/null 2>&1 || true
	$(CONTAINER_TOOL) run -d --name "$(LOCAL_REGISTRY_NAME)" \
		-p $(LOCAL_REGISTRY_PORT):5000 $(LOCAL_REGISTRY_IMAGE) >/dev/null
	@# The container is up before the registry inside it is listening, and a push
	@# into that gap fails with a connection error that reads like a broken command.
	@set -e; \
	for _ in $$(seq 1 30); do \
		if curl -fsS "http://$(LOCAL_REGISTRY_HOST)/v2/" >/dev/null 2>&1; then \
			echo "registry: $(LOCAL_REGISTRY_HOST) is serving."; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "registry: $(LOCAL_REGISTRY_HOST) did not start within 30s."; \
	$(CONTAINER_TOOL) logs "$(LOCAL_REGISTRY_NAME)" 2>&1 | tail -20; \
	exit 1

.PHONY: local-registry-down
local-registry-down: ## Remove the throwaway OCI registry.
	@$(CONTAINER_TOOL) rm -f "$(LOCAL_REGISTRY_NAME)" >/dev/null 2>&1 || true

# The Kubernetes version rendered manifests are validated against. Derived from
# the same k8s.io/api version envtest is pinned to, so "the pinned Kubernetes
# version" means one thing across the whole repo. kubeconform wants a full
# x.y.z; the schema repository publishes .0 for every minor.
KUBECONFORM_K8S_VERSION ?= $(ENVTEST_K8S_VERSION).0

# Kinds kubeconform cannot judge, named one by one rather than waved through with
# -ignore-missing-schemas — with an explicit list, a *typo'd* kind is still a hard
# failure:
#
#   - our own four CRs have no published upstream schema. Their shape is
#     enforced by the CRDs themselves, in envtest (api/v1alpha1) and in the kind
#     smokes, which is stricter than anything kubeconform could do here.
#   - CustomResourceDefinition: the upstream schema repository publishes no
#     apiextensions group at all, at any version. The CRDs are controller-gen
#     output, asserted by api/v1alpha1/crdmanifests_test.go and applied to a real
#     apiserver by every envtest and kind run.
#
# --include-crds is still passed when rendering, so a CRD that failed to render
# at all remains a parse error here.
KUBECONFORM_SKIP ?= ClickHouseSink,S3Sink,StreamRule,ClusterStreamRule,CustomResourceDefinition
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

##@ Release

# A release is a tag, not a branch (Task 3.5): strangers install tags. Everything
# in this section exists so .github/workflows/release.yml is a thin caller of the
# same targets a maintainer runs by hand — which is what makes `make
# release-dry-run` a rehearsal of the real thing rather than an approximation of
# it. The policy the targets enforce is written down in docs/RELEASING.md.
#
# RELEASE_VERSION is the tag being released, `v`-prefixed. It defaults to the
# committed VERSION so a local rehearsal needs no arguments; CI passes the tag it
# was triggered by, and release-verify-version is what refuses a tag that
# disagrees with the tree it points at.
RELEASE_VERSION ?= v$(VERSION)
RELEASE_DIR ?= dist/release
RELEASE_NOTES ?= $(RELEASE_DIR)/RELEASE_NOTES.md
RELEASE_IMAGE ?= $(IMAGE_REPO):$(RELEASE_VERSION)
# Helm chart versions are semver without the `v`, and a prerelease suffix carries
# through unchanged.
RELEASE_CHART_VERSION = $(patsubst v%,%,$(RELEASE_VERSION))
CHANGELOG_SECTION := hack/changelog-section.sh

# Supply chain (Task 7.4). A release whose subject is tamper-evident audit has to
# be verifiable itself, so a tag publishes three things beyond the artifacts: a
# keyless cosign signature over the image, SLSA build provenance for the image and
# for every attached asset, and an SBOM of the image. What each of those proves —
# and, just as important, what it does not — is docs/VERIFYING.md.
#
# cosign and syft are expected on PATH rather than bootstrapped into bin/, which
# is the KUBECTL/KIND convention rather than the promtool/duckdb one. The reason
# is specific to these two: their own installers verify the checksum of the binary
# they fetch, and hand-rolling a `curl | tar` for them in the task about supply
# chain integrity would be trading a verified download for an unverified one. CI
# installs them with the pinned installer actions; a maintainer rehearsing locally
# installs them once (see docs/DEVELOPMENT.md). Every target below says which one
# it needs and how to get it.
COSIGN ?= cosign
SYFT ?= syft

# GITHUB_REPO is <owner>/<repo>, read out of the module path rather than written
# down again. It is what a keyless signature's certificate identity is built from
# and what `gh attestation verify` is pointed at, so a fork or a rename cannot
# leave the published verification commands naming somebody else's repository.
GITHUB_REPO ?= $(shell sed -n 's|^module github.com/||p' go.mod)

# The workflow that is allowed to have signed a release. A keyless signature does
# not say "somebody at this project signed this" — it says "this exact workflow,
# on this exact ref, in this repository signed this", and that whole string is the
# thing a verifier has to pin. Anything less specific accepts a signature from any
# workflow in any repository the issuer serves.
RELEASE_WORKFLOW ?= .github/workflows/release.yml
COSIGN_ISSUER ?= https://token.actions.githubusercontent.com
COSIGN_IDENTITY ?= https://github.com/$(GITHUB_REPO)/$(RELEASE_WORKFLOW)@refs/tags/$(RELEASE_VERSION)

# The image's digest and repository, written down by release-image-digest so the
# steps that follow name what was actually pushed rather than re-deriving it. A
# signature or an attestation is about a digest, never a tag: a tag can be moved
# to another image after the fact, which would leave a valid signature describing
# something nobody released.
RELEASE_DIGEST_FILE ?= $(RELEASE_DIR)/image-digest.txt
RELEASE_IMAGE_NAME_FILE ?= $(RELEASE_DIR)/image-name.txt

# The pushed chart's manifest digest, recorded by release-chart-push for the same
# reason the image's is: what gets signed is a digest, and re-resolving the tag
# between the push and the signature is a window in which the tag could have
# moved. The file also stands in for "the push happened" — release-chart-sign
# refuses to run without it.
RELEASE_CHART_DIGEST_FILE ?= $(RELEASE_DIR)/chart-digest.txt
# The packaged chart release-artifacts wrote. It is pushed rather than rebuilt,
# and that is not an optimisation: `helm package` stamps the current time into
# every tar header, so packaging the same chart twice produces two different
# archives. Pushing the file that was packaged is what makes the OCI artifact's
# layer byte-identical to the `.tgz` on the Release page — one sha256, listed
# once in checksums.txt, covering both ways of getting the chart.
RELEASE_CHART_TGZ = $(RELEASE_DIR)/$(CHART_RELEASE)-$(RELEASE_CHART_VERSION).tgz

# The SBOM is one document for one platform. Every platform in PLATFORMS is the
# same statically linked binary built from the same go.mod into the same
# distroless/static base, so the package set does not differ between them — only
# the architecture of the one binary does. Shipping four near-identical documents
# would imply a difference that is not there; the platform is stated in
# docs/VERIFYING.md instead.
#
# The SPDX spec version is pinned along with the format, so a syft upgrade cannot
# quietly change the shape of a published artifact.
RELEASE_SBOM ?= $(RELEASE_DIR)/kuberecord-$(RELEASE_CHART_VERSION)-sbom.spdx.json
SBOM_FORMAT ?= spdx-json@2.3
SBOM_PLATFORM ?= linux/amd64
# SBOM_SOURCE is what syft scans. Empty means "the image that was just pushed, by
# digest" — the only correct answer for a release. A rehearsal has no pushed image
# and passes a locally built one instead (see release-dry-run).
SBOM_SOURCE ?=
# The floor below which an SBOM is not an SBOM. A real scan of the manager finds
# well over a hundred packages — 123 at the time of writing, almost all of them Go
# modules read out of the binary — while a scan that found the distroless base and
# missed the binary finds about a dozen. That is the failure this catches. The
# floor sits far below the real count on purpose, so a dependency bump never has
# to move it.
SBOM_MIN_PACKAGES ?= 50

# require-tool fails with the sentence a reader needs rather than
# "make: cosign: No such file or directory".
# $1 - the tool (a name on PATH or a path)
# $2 - how to get it
define require-tool
@command -v "$(1)" >/dev/null 2>&1 || { \
	echo 'release: $(1) is not installed, and this target cannot run without it.'; \
	echo '  $(2)'; \
	echo '  CI installs it from a SHA-pinned action; see $(RELEASE_WORKFLOW).'; \
	exit 1; \
}
endef

.PHONY: release-verify-version
release-verify-version: ## Check that RELEASE_VERSION agrees with the committed VERSION and the chart.
	@base="$(RELEASE_VERSION)"; base="$${base#v}"; base="$${base%%-*}"; \
	if [ "$$base" != "$(VERSION)" ]; then \
		echo "release: tag $(RELEASE_VERSION) does not match VERSION $(VERSION) in the Makefile."; \
		echo "  The tag decides what is published and VERSION decides what the artifacts pin;"; \
		echo "  if they disagree, the release hands out manifests for a version nobody built."; \
		echo "  Bump VERSION and the chart's version/appVersion in the commit the tag points at."; \
		exit 1; \
	fi; \
	chart_version="$$(sed -n 's/^version: *//p' $(CHART_DIR)/Chart.yaml | head -1 | tr -d '"')"; \
	chart_app="$$(sed -n 's/^appVersion: *//p' $(CHART_DIR)/Chart.yaml | head -1 | tr -d '"')"; \
	if [ "$$chart_version" != "$(VERSION)" ] || [ "$$chart_app" != "v$(VERSION)" ]; then \
		echo "release: $(CHART_DIR)/Chart.yaml carries version=$$chart_version appVersion=$$chart_app;"; \
		echo "  VERSION is $(VERSION), so they must be $(VERSION) and v$(VERSION)."; \
		exit 1; \
	fi; \
	echo "release: $(RELEASE_VERSION) agrees with VERSION $(VERSION) and the chart."

.PHONY: release-notes
release-notes: ## Extract this release's CHANGELOG.md section into RELEASE_NOTES.
	@# The gate. What this writes becomes the GitHub Release body, so a tag with no
	@# section fails here rather than publishing an empty release. The section is
	@# written through a temporary file: a shell redirect would leave a truncated
	@# notes file behind on failure, and a later step would happily publish it.
	@mkdir -p "$(RELEASE_DIR)"
	@if ./$(CHANGELOG_SECTION) $(RELEASE_VERSION) > "$(RELEASE_NOTES).tmp"; then \
		mv "$(RELEASE_NOTES).tmp" "$(RELEASE_NOTES)"; \
		echo "release: wrote $$(wc -l < "$(RELEASE_NOTES)" | tr -d ' ') lines of notes to $(RELEASE_NOTES)."; \
	else \
		rm -f "$(RELEASE_NOTES).tmp"; \
		exit 1; \
	fi

.PHONY: release-artifacts
release-artifacts: release-verify-version release-notes helm helm-sync kubeconform ## Build install.yaml, the packaged chart and checksums into RELEASE_DIR.
	@# Stale artifacts from an earlier version in the same directory would end up in
	@# checksums.txt and then on the release, so the outputs are removed by name
	@# first (not the whole directory — the notes were just written into it). The
	@# SBOM is named here for the same reason, and removing it is safe in both
	@# orders that exist: a rehearsal and the release workflow alike build the
	@# artifacts first and scan an image afterwards. The recorded digest goes too:
	@# a digest left behind by an earlier rehearsal is what release-sbom and
	@# release-sign would otherwise read, and they would then describe and sign an
	@# image from a previous run without anything looking wrong.
	rm -f "$(RELEASE_DIR)/install.yaml" "$(RELEASE_DIR)"/*.tgz "$(RELEASE_DIR)/checksums.txt" \
		"$(RELEASE_SBOM)" "$(RELEASE_DIGEST_FILE)" "$(RELEASE_IMAGE_NAME_FILE)" \
		"$(RELEASE_CHART_DIGEST_FILE)"
	$(MAKE) build-installer INSTALLER_IMG=$(RELEASE_IMAGE) INSTALLER_OUT=$(RELEASE_DIR)/install.yaml
	@# The manifest people apply is the manifest that was validated, rather than a
	@# sibling of it built from the same sources.
	"$(KUBECONFORM)" $(KUBECONFORM_FLAGS) "$(RELEASE_DIR)/install.yaml"
	"$(HELM)" package "$(CHART_DIR)" \
		--version $(RELEASE_CHART_VERSION) --app-version $(RELEASE_VERSION) \
		--destination "$(RELEASE_DIR)"
	$(MAKE) release-checksums

# checksums.txt is one file listing every asset a release attaches, and it is
# recomputed rather than appended to: the SBOM is produced after the install
# artifacts (it describes an image that has to exist first), so this target runs
# twice on a real release and has to give the same answer as one run over a
# finished directory.
#
# It is also the attestation's subject list. `actions/attest-build-provenance` is
# handed this file rather than a glob, so "what is checksummed" and "what is
# attested" are the same set by construction — a glob that stopped matching would
# have quietly narrowed the attestation with nothing failing.
.PHONY: release-checksums
release-checksums: ## Hash every asset in RELEASE_DIR into checksums.txt.
	@# The install artifacts are mandatory: a checksums file that silently omits
	@# install.yaml is worse than no checksums file, because it looks complete.
	@# The SBOM is optional only in the sense that it is written later.
	@# sha256sum is GNU, shasum ships with macOS; the release runs on the first and
	@# a maintainer rehearses on either. Names in the file are relative to the
	@# directory, so `sha256sum -c checksums.txt` works wherever the assets land.
	@set -e; cd "$(RELEASE_DIR)"; \
	assets="install.yaml kuberecord-$(RELEASE_CHART_VERSION).tgz"; \
	for f in $$assets; do \
		[ -f "$$f" ] || { \
			echo "release: $(RELEASE_DIR)/$$f is missing; run release-artifacts first."; \
			exit 1; \
		}; \
	done; \
	if [ -f "$(notdir $(RELEASE_SBOM))" ]; then \
		assets="$$assets $(notdir $(RELEASE_SBOM))"; \
	fi; \
	if command -v sha256sum >/dev/null 2>&1; then \
		sha256sum $$assets > checksums.txt; \
	else \
		shasum -a 256 $$assets > checksums.txt; \
	fi
	@echo; echo "release: assets for $(RELEASE_VERSION) in $(RELEASE_DIR):"
	@cat "$(RELEASE_DIR)/checksums.txt"

.PHONY: release-image
release-image: release-verify-version ## Build the multi-arch release image (BUILDX_OUTPUT= to build without pushing).
	$(MAKE) docker-buildx IMG=$(RELEASE_IMAGE)

# release-image-digest records what was pushed, the same way a user pins by digest
# (see docs/VERIFYING.md) — so the release resolves its own subject exactly the way
# somebody verifying it does.
#
# The digest is computed from the raw manifest bytes rather than read out of a
# `--format '{{.Manifest.Digest}}'` template, and that is not fussiness: on at
# least one shipped buildx (Docker Desktop's v0.22) a template referencing
# `.Manifest` is ignored and the *default human-readable listing* is printed
# instead, exit code 0. A release that trusted it would sign whatever that text
# happened to parse as. Hashing the bytes is what a digest *is*, needs no jq, and
# behaves identically for a single manifest and an index.
#
# The repository name is written out beside the digest so the steps that follow
# (and the attestation step in the workflow, which cannot read a make variable)
# name the registry from the one place that decides it, IMAGE_REPO.
.PHONY: release-image-digest
release-image-digest: ## Record the pushed release image's digest and name in RELEASE_DIR.
	@mkdir -p "$(RELEASE_DIR)"
	@set -e; \
	if command -v sha256sum >/dev/null 2>&1; then sha=sha256sum; else sha="shasum -a 256"; fi; \
	raw="$$($(CONTAINER_TOOL) buildx imagetools inspect --raw $(RELEASE_IMAGE))"; \
	if [ -z "$$raw" ]; then \
		echo "release: $(RELEASE_IMAGE) has no manifest to read, so nothing was pushed."; \
		echo "  The empty string hashes to a perfectly well-formed digest, which is why"; \
		echo "  this is checked rather than hashed straight out of a pipeline."; \
		exit 1; \
	fi; \
	digest="sha256:$$(printf '%s' "$$raw" | $$sha | cut -d' ' -f1)"; \
	case "$$digest" in sha256:*) ;; *) \
		echo "release: $(RELEASE_IMAGE) resolved to \"$$digest\", which is not a sha256 digest."; \
		echo "  Nothing may be signed or attested until the push has produced one."; \
		exit 1 ;; \
	esac; \
	if [ "$${#digest}" -ne 71 ]; then \
		echo "release: \"$$digest\" is not 64 hex characters long; refusing to sign a truncated digest."; \
		exit 1; \
	fi; \
	printf '%s\n' "$$digest" > "$(RELEASE_DIGEST_FILE)"; \
	printf '%s\n' "$(IMAGE_REPO)" > "$(RELEASE_IMAGE_NAME_FILE)"; \
	echo "release: $(RELEASE_IMAGE) is $(IMAGE_REPO)@$$digest"

# release-image-subject is the digest-qualified reference every supply-chain
# target below operates on, read back from the file above so a signature and an
# SBOM cannot end up describing two different images.
release-image-subject = $$(cat "$(RELEASE_DIGEST_FILE)")

.PHONY: release-sbom
release-sbom: ## Generate the image SBOM (SPDX JSON) into RELEASE_DIR.
	$(call require-tool,$(SYFT),Install syft: https://github.com/anchore/syft#installation (`brew install syft`).)
	@mkdir -p "$(RELEASE_DIR)"
	@# Written through a temporary file for the same reason the notes are: a syft
	@# failure part-way must not leave a truncated document that the next step
	@# happily checksums and attaches to a release.
	@set -e; \
	source="$(SBOM_SOURCE)"; \
	if [ -z "$$source" ]; then \
		if [ ! -f "$(RELEASE_DIGEST_FILE)" ]; then \
			echo "release: $(RELEASE_DIGEST_FILE) does not exist, so there is no pushed image to describe."; \
			echo "  Run release-image-digest after a push, or scan a local image with"; \
			echo "  SBOM_SOURCE=docker:$(RELEASE_IMAGE) SBOM_PLATFORM= (what release-dry-run does)."; \
			exit 1; \
		fi; \
		source="registry:$(IMAGE_REPO)@$(release-image-subject)"; \
	fi; \
	platform=""; \
	if [ -n "$(SBOM_PLATFORM)" ]; then platform="--platform $(SBOM_PLATFORM)"; fi; \
	set -x; \
	"$(SYFT)" scan "$$source" $$platform \
		--source-name "$(IMAGE_REPO)" --source-version "$(RELEASE_VERSION)" \
		-o '$(SBOM_FORMAT)=$(RELEASE_SBOM).tmp'
	@# An SBOM that parses but describes nothing is the failure mode worth
	@# catching: syft succeeds on an image whose binary it could not read, and the
	@# result is a valid SPDX document listing the base image and little else.
	@set -e; \
	if ! grep -q '"spdxVersion"' "$(RELEASE_SBOM).tmp"; then \
		rm -f "$(RELEASE_SBOM).tmp"; \
		echo "release: syft did not produce an SPDX document."; \
		exit 1; \
	fi; \
	if ! grep -q 'github.com/$(GITHUB_REPO)' "$(RELEASE_SBOM).tmp"; then \
		rm -f "$(RELEASE_SBOM).tmp"; \
		echo "release: the SBOM does not mention github.com/$(GITHUB_REPO), so syft did not read"; \
		echo "  the manager binary — it described the base image only. Check the platform"; \
		echo "  (SBOM_PLATFORM=$(SBOM_PLATFORM)) actually exists in the image being scanned."; \
		exit 1; \
	fi; \
	packages="$$(grep -o -E '"SPDXID": ?"SPDXRef-Package' "$(RELEASE_SBOM).tmp" | wc -l | tr -d ' ')"; \
	if [ "$$packages" -lt $(SBOM_MIN_PACKAGES) ]; then \
		rm -f "$(RELEASE_SBOM).tmp"; \
		echo "release: the SBOM lists $$packages packages, fewer than the $(SBOM_MIN_PACKAGES) a real"; \
		echo "  scan of the manager finds. It is describing something other than this image."; \
		exit 1; \
	fi; \
	mv "$(RELEASE_SBOM).tmp" "$(RELEASE_SBOM)"; \
	echo "release: $$packages packages in $(RELEASE_SBOM)"

# The SBOM a rehearsal can produce. Nothing has been pushed, so there is no
# registry image to scan; a single-platform local build gives syft something real
# to read, which is the half of the step worth exercising — a scan that finds
# nothing fails here rather than attaching an empty document to a release.
#
# SBOM_PLATFORM is cleared because a local build produces the host's architecture,
# which on a maintainer's machine is not necessarily SBOM_PLATFORM. A rehearsal
# therefore describes the runner; a release describes $(SBOM_PLATFORM).
.PHONY: release-sbom-local
release-sbom-local: ## Build a single-platform image locally and describe that (the SBOM a rehearsal can produce).
	$(MAKE) docker-build IMG=$(RELEASE_IMAGE)
	$(MAKE) release-sbom SBOM_SOURCE=docker:$(RELEASE_IMAGE) SBOM_PLATFORM=

# Keyless, so there is no key to store, rotate or leak: the signature is bound to
# an ephemeral Fulcio certificate naming the workflow that asked for it, and the
# proof it existed is a public Rekor entry. The consequence is that a signature is
# a public statement — see docs/VERIFYING.md — which is exactly what it is for.
#
# --recursive because the release is a manifest list. Signing only the index leaves
# every per-platform digest unsigned, and a user who pins the arm64 image (the
# thing an admission controller resolves to) would then find nothing to verify.
.PHONY: release-sign
release-sign: ## Sign the pushed release image with cosign (keyless/OIDC).
	$(call require-tool,$(COSIGN),Install cosign: https://docs.sigstore.dev/cosign/system_config/installation/ (`brew install cosign`).)
	@set -e; \
	if [ ! -f "$(RELEASE_DIGEST_FILE)" ]; then \
		echo "release: $(RELEASE_DIGEST_FILE) does not exist; run release-image-digest after the push."; \
		echo "  Signing a tag rather than a digest is refused: a tag can be moved afterwards."; \
		exit 1; \
	fi; \
	set -x; \
	"$(COSIGN)" sign --yes --recursive "$(IMAGE_REPO)@$(release-image-subject)"

# The published verification commands, run against what was just published. This
# target exists so docs/VERIFYING.md is not a promise nobody checks: the release
# fails if the signature it just made does not verify under the identity the
# documentation tells a reader to pin.
.PHONY: release-verify
release-verify: ## Verify the published signature and provenance the way a user would.
	$(call require-tool,$(COSIGN),Install cosign: https://docs.sigstore.dev/cosign/system_config/installation/ (`brew install cosign`).)
	@set -e; \
	if [ ! -f "$(RELEASE_DIGEST_FILE)" ]; then \
		echo "release: $(RELEASE_DIGEST_FILE) does not exist; there is nothing published to verify."; \
		exit 1; \
	fi; \
	set -x; \
	"$(COSIGN)" verify \
		--certificate-oidc-issuer "$(COSIGN_ISSUER)" \
		--certificate-identity "$(COSIGN_IDENTITY)" \
		"$(IMAGE_REPO)@$(release-image-subject)" > /dev/null
	@echo "release: the signature on $(IMAGE_REPO)@$(release-image-subject) is valid,"
	@echo "  and it was made by $(COSIGN_IDENTITY)"
	@# gh is the documented path for provenance because it is what publishes it:
	@# `actions/attest-build-provenance` writes a Sigstore bundle to GitHub's
	@# attestations API, and gh knows how to fetch and verify it. It is optional
	@# here rather than required so this target still verifies the signature on a
	@# machine with no gh login.
	@set -e; \
	if command -v gh >/dev/null 2>&1; then \
		set -x; \
		gh attestation verify "oci://$(IMAGE_REPO)@$(release-image-subject)" --repo "$(GITHUB_REPO)"; \
	else \
		echo "release: gh is not installed, so the provenance attestation was not checked."; \
		echo "  See docs/VERIFYING.md for the command."; \
	fi

# Authenticating to the chart registry. It is a target rather than a line of YAML
# so the workflow keeps being a thin caller of commands a maintainer can run, and
# so the location of the bootstrapped helm binary stays a Makefile concern.
#
# The token arrives in the environment and is fed through stdin, never as an
# argument: arguments are visible to every process on the machine, and a release
# token that leaks is a token that can push over a published chart.
CHART_REGISTRY_HOST ?= $(firstword $(subst /, ,$(CHART_OCI_NAMESPACE)))

.PHONY: release-chart-login
release-chart-login: helm ## Authenticate to CHART_REGISTRY_HOST (CHART_REGISTRY_USER/CHART_REGISTRY_TOKEN from the environment).
	@set -e; \
	if [ -z "$$CHART_REGISTRY_TOKEN" ]; then \
		echo "release: CHART_REGISTRY_TOKEN is not set, so there is nothing to authenticate with."; \
		echo "  CI passes the workflow's GITHUB_TOKEN; a maintainer pushing by hand needs a"; \
		echo "  personal access token with write:packages."; \
		exit 1; \
	fi; \
	if [ -z "$$CHART_REGISTRY_USER" ]; then \
		echo "release: CHART_REGISTRY_USER is not set; $(CHART_REGISTRY_HOST) needs a username"; \
		echo "  alongside the token."; \
		exit 1; \
	fi; \
	printf '%s' "$$CHART_REGISTRY_TOKEN" | "$(HELM)" registry login "$(CHART_REGISTRY_HOST)" \
		--username "$$CHART_REGISTRY_USER" --password-stdin

# The chart, as an OCI artifact (Task 8.1). The `.tgz` on the Release page keeps
# being attached; this is a second way to get the same bytes, and it is the one
# that does not route through a GitHub redirect that a repository rename could
# take away.
#
# The digest is read out of `helm push`'s own report rather than resolved
# afterwards from the tag, and helm writes that report to *stderr* with nothing on
# stdout — hence the redirect. It is validated the same way release-image-digest
# validates its own: the empty string hashes and formats perfectly well, and a
# signature over a digest nobody checked is the failure worth refusing early.
#
# The output is captured rather than streamed, because the digest is in it, and
# `set -e` is lifted around that capture on purpose: an aborting assignment would
# take the shell down before anything is printed, and a failed push whose error
# nobody sees is the least useful failure there is — the registry's message is the
# whole diagnosis.
.PHONY: release-chart-push
release-chart-push: helm ## Push the packaged chart to CHART_OCI_REPO and record its digest.
	@set -e; \
	if [ ! -f "$(RELEASE_CHART_TGZ)" ]; then \
		echo "release: $(RELEASE_CHART_TGZ) does not exist; run release-artifacts first."; \
		echo "  The chart is pushed, never repackaged here: helm package stamps the"; \
		echo "  current time into every tar header, so a second packaging of the same"; \
		echo "  chart would put different bytes in the registry than on the Release page."; \
		exit 1; \
	fi; \
	mkdir -p "$(RELEASE_DIR)"; \
	echo "+ $(HELM) push $(RELEASE_CHART_TGZ) $(CHART_OCI_REPO) $(HELM_REGISTRY_FLAGS)"; \
	set +e; \
	out="$$("$(HELM)" push "$(RELEASE_CHART_TGZ)" "$(CHART_OCI_REPO)" $(HELM_REGISTRY_FLAGS) 2>&1)"; \
	status=$$?; \
	set -e; \
	printf '%s\n' "$$out" | sed 's/^/    /'; \
	if [ "$$status" -ne 0 ]; then \
		echo "release: helm push failed (exit $$status); no digest was recorded, so nothing"; \
		echo "  downstream can sign or verify a chart that was not published."; \
		exit "$$status"; \
	fi; \
	digest="$$(printf '%s\n' "$$out" | sed -n 's/^Digest: *//p' | head -1)"; \
	case "$$digest" in sha256:*) ;; *) \
		echo "release: helm push reported no sha256 digest for the chart."; \
		echo "  Its output is parsed for one; if the wording moved, this is where to look."; \
		exit 1 ;; \
	esac; \
	if [ "$${#digest}" -ne 71 ]; then \
		echo "release: \"$$digest\" is not 64 hex characters long; refusing to sign a truncated digest."; \
		exit 1; \
	fi; \
	printf '%s\n' "$$digest" > "$(RELEASE_CHART_DIGEST_FILE)"; \
	echo "release: $(CHART_OCI_REF):$(RELEASE_CHART_VERSION) is $(CHART_OCI_REF)@$$digest"

# release-chart-subject is the digest-qualified reference the signature and the
# verification below both name, read back from the file rather than re-derived.
release-chart-subject = $$(cat "$(RELEASE_CHART_DIGEST_FILE)")

# No --recursive, unlike the image: a chart is one manifest, not an index, so
# there is nothing underneath it to leave unsigned.
.PHONY: release-chart-sign
release-chart-sign: ## Sign the pushed chart with cosign (keyless/OIDC).
	$(call require-tool,$(COSIGN),Install cosign: https://docs.sigstore.dev/cosign/system_config/installation/ (`brew install cosign`).)
	@set -e; \
	if [ ! -f "$(RELEASE_CHART_DIGEST_FILE)" ]; then \
		echo "release: $(RELEASE_CHART_DIGEST_FILE) does not exist; run release-chart-push first."; \
		echo "  Signing a tag rather than a digest is refused: a tag can be moved afterwards."; \
		exit 1; \
	fi; \
	set -x; \
	"$(COSIGN)" sign --yes "$(CHART_OCI_REF)@$(release-chart-subject)"

# The chart's half of what release-verify does for the image, and it exists for
# the same reason: docs/VERIFYING.md publishes a command, and a command nobody
# runs against what was just published is a promise, not a check.
.PHONY: release-chart-verify
release-chart-verify: ## Verify the published chart signature the way a user would.
	$(call require-tool,$(COSIGN),Install cosign: https://docs.sigstore.dev/cosign/system_config/installation/ (`brew install cosign`).)
	@set -e; \
	if [ ! -f "$(RELEASE_CHART_DIGEST_FILE)" ]; then \
		echo "release: $(RELEASE_CHART_DIGEST_FILE) does not exist; there is no pushed chart to verify."; \
		exit 1; \
	fi; \
	set -x; \
	"$(COSIGN)" verify \
		--certificate-oidc-issuer "$(COSIGN_ISSUER)" \
		--certificate-identity "$(COSIGN_IDENTITY)" \
		"$(CHART_OCI_REF)@$(release-chart-subject)" > /dev/null
	@echo "release: the signature on $(CHART_OCI_REF)@$(release-chart-subject) is valid,"
	@echo "  and it was made by $(COSIGN_IDENTITY)"

# The chart push, rehearsed. Signing and attesting cannot be rehearsed — doing
# them *is* publishing — but a push can be, against a registry that is not a
# publication: a throwaway one on this machine, thrown away again afterwards.
#
# That makes this the one supply-chain step a rehearsal exercises for real rather
# than prints. What it proves is the half that actually breaks: that the chart
# exists to be pushed, that the reference and the flags are well-formed, and that
# `helm push`'s report still parses into a digest. What it cannot prove is that
# ghcr.io accepts the push — no rehearsal can, without pushing there.
#
# The digest recorded belongs to the throwaway registry, so it is removed again:
# leaving it behind is how a later release-chart-sign would sign a digest from a
# registry that no longer exists.
.PHONY: release-chart-rehearse
release-chart-rehearse: helm ## Push the packaged chart to a throwaway local registry; publish nothing.
	@echo "release: rehearsing the chart push against $(LOCAL_REGISTRY_HOST) — nothing is published."
	$(MAKE) local-registry-up
	@set -e; \
	status=0; \
	$(MAKE) --no-print-directory release-chart-push \
		CHART_OCI_NAMESPACE=$(LOCAL_REGISTRY_HOST)/charts \
		HELM_REGISTRY_FLAGS=--plain-http || status=$$?; \
	rm -f "$(RELEASE_CHART_DIGEST_FILE)"; \
	$(MAKE) --no-print-directory local-registry-down; \
	if [ "$$status" -ne 0 ]; then exit "$$status"; fi; \
	echo "release: the chart pushed and its digest parsed. Nothing reached $(CHART_OCI_REF)."

# What a rehearsal must not do, printed instead of done. Signing writes to a
# registry and to a public transparency log, and attesting writes a record to
# GitHub; all three are publications, so a rehearsal that performed them would not
# be a rehearsal. Printing the expanded recipes still catches the failure a
# rehearsal is for — a variable that does not expand, a target that no longer
# exists, a flag that was renamed — without publishing anything.
.PHONY: release-rehearse-publishing
release-rehearse-publishing: ## Print, without running, the publishing steps a rehearsal must not perform.
	@echo "release: a rehearsal stops short of publishing. A real run would execute:"
	@echo
	@$(MAKE) --dry-run --no-print-directory release-sign release-verify \
		RELEASE_VERSION="$(RELEASE_VERSION)" | sed 's/^/    /'
	@echo
	@# The chart push itself *is* rehearsed (release-chart-rehearse), against a
	@# throwaway registry. Its signature is not, for the same reason the image's is
	@# not, and against the real reference rather than the rehearsal's.
	@echo "  ...and, over the chart pushed to $(CHART_OCI_REF):"
	@$(MAKE) --dry-run --no-print-directory release-chart-sign release-chart-verify \
		RELEASE_VERSION="$(RELEASE_VERSION)" | sed 's/^/    /'
	@echo
	@echo "  ...and, from the workflow rather than from make, the two"
	@echo "  actions/attest-build-provenance calls over:"
	@echo "    $(IMAGE_REPO)@<the pushed digest>"
	@echo "    every asset listed in $(RELEASE_DIR)/checksums.txt"

.PHONY: release-dry-run
release-dry-run: release-artifacts verify-packaging ## Rehearse a release end to end: notes, artifacts, SBOM, checksums, and the multi-arch build with nothing pushed.
	$(MAKE) release-image BUILDX_OUTPUT=
	@# The multi-arch build above deliberately keeps nothing, so the SBOM comes
	@# from a locally built image — the same target the workflow's rehearsal path
	@# calls, so the two rehearsals exercise one code path.
	$(MAKE) release-sbom-local
	$(MAKE) release-checksums
	@echo
	$(MAKE) release-chart-rehearse
	@echo
	$(MAKE) release-rehearse-publishing
	@echo
	@echo "release: dry run for $(RELEASE_VERSION) complete. Nothing was pushed or published."
	@echo "  image      $(RELEASE_IMAGE) (built for $(PLATFORMS), discarded)"
	@echo "  notes      $(RELEASE_NOTES)"
	@echo "  artifacts  $(RELEASE_DIR)/install.yaml, $(RELEASE_CHART_TGZ), $(RELEASE_DIR)/checksums.txt"
	@echo "  chart      $(CHART_OCI_REF):$(RELEASE_CHART_VERSION) (pushed to a throwaway registry, discarded)"
	@echo "  sbom       $(RELEASE_SBOM)"
	@echo "  unsigned   a rehearsal signs nothing and attests nothing; see docs/VERIFYING.md"

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

.PHONY: e2e-helm-prereqs
e2e-helm-prereqs: ## Create the namespace, its Pod Security label and the credentials Secret a Helm install expects.
	"$(KUBECTL)" create namespace $(CHART_NAMESPACE) --dry-run=client -o yaml | "$(KUBECTL)" apply -f -
	"$(KUBECTL)" label namespace $(CHART_NAMESPACE) pod-security.kubernetes.io/enforce=restricted --overwrite
	"$(KUBECTL)" create secret generic kuberecord-clickhouse-credentials \
		--namespace $(CHART_NAMESPACE) --from-literal=password=$(E2E_CH_PASSWORD) \
		--dry-run=client -o yaml | "$(KUBECTL)" apply -f -

.PHONY: deploy-e2e-helm
deploy-e2e-helm: helm helm-sync e2e-helm-prereqs ## Install the operator with the Helm chart, e2e values. Used by test-e2e.
	"$(HELM)" upgrade --install $(CHART_RELEASE) "$(CHART_DIR)" \
		--namespace $(CHART_NAMESPACE) --values $(E2E_HELM_VALUES) --wait --timeout 5m

.PHONY: undeploy-e2e-helm
undeploy-e2e-helm: helm ## Remove the Helm-installed controller. Used by test-e2e.
	@# The CRDs are left behind: Helm never deletes what it installed from crds/,
	@# and pretending otherwise here would hide that from anyone reading this as
	@# documentation of the uninstall path.
	-"$(HELM)" uninstall $(CHART_RELEASE) --namespace $(CHART_NAMESPACE) --wait
	-"$(KUBECTL)" delete namespace $(CHART_NAMESPACE) --ignore-not-found=true

# The OCI install path the e2e suite smokes (E2E_INSTALL=helm-oci), and the only
# one whose subject is the *distribution channel* rather than the rendering: the
# chart is packaged, pushed to a registry and installed back out of it by
# reference, which is the sequence a release performs and a user consumes.
#
# It runs against a throwaway registry on this machine rather than against
# ghcr.io, and that is the point: this must exercise the chart in the working tree
# on every pull request, and no chart has been published for a commit that has not
# been released. Only helm talks to the registry — the manager image is still
# side-loaded into Kind — so nothing in the cluster needs to reach it.
#
# --plain-http throughout: the throwaway registry speaks HTTP and has no
# certificate. The published path needs no such flag, which is why it is a
# variable rather than baked into the recipes.
E2E_OCI_DIR ?= dist/e2e-oci

.PHONY: deploy-e2e-helm-oci
deploy-e2e-helm-oci: helm helm-sync local-registry-up e2e-helm-prereqs ## Package, push and install the chart from a local OCI registry. Used by test-e2e.
	@# Packaged into its own directory so a release rehearsal's dist/release/ is
	@# never what gets pushed here, and emptied first so a stale archive from an
	@# earlier run cannot be the one installed.
	rm -rf "$(E2E_OCI_DIR)"
	mkdir -p "$(E2E_OCI_DIR)"
	"$(HELM)" package "$(CHART_DIR)" --destination "$(E2E_OCI_DIR)"
	"$(HELM)" push "$(E2E_OCI_DIR)"/$(CHART_RELEASE)-*.tgz \
		oci://$(LOCAL_REGISTRY_HOST)/charts --plain-http
	@# By reference and by version, not from the directory that was just packaged:
	@# an install that read the chart off local disk would prove nothing about the
	@# registry it was pushed to.
	"$(HELM)" upgrade --install $(CHART_RELEASE) \
		oci://$(LOCAL_REGISTRY_HOST)/charts/$(CHART_RELEASE) --version $(VERSION) --plain-http \
		--namespace $(CHART_NAMESPACE) --values $(E2E_HELM_VALUES) --wait --timeout 5m

.PHONY: undeploy-e2e-helm-oci
undeploy-e2e-helm-oci: helm ## Remove the OCI-installed controller and the local registry. Used by test-e2e.
	-"$(HELM)" uninstall $(CHART_RELEASE) --namespace $(CHART_NAMESPACE) --wait
	-"$(KUBECTL)" delete namespace $(CHART_NAMESPACE) --ignore-not-found=true
	rm -rf "$(E2E_OCI_DIR)"
	$(MAKE) local-registry-down

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
	"$(KUBECTL)" patch deployment kuberecord-controller-manager -n $(CHART_NAMESPACE) --type=json \
		-p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--ch-auto-create-schema"}]'

.PHONY: undeploy-e2e-installer
undeploy-e2e-installer: ## Remove the controller installed from dist/install.yaml. Used by test-e2e.
	"$(KUBECTL)" delete --ignore-not-found=true -f dist/install.yaml

# The image the installer smoke pins. It must match the tag test/e2e builds and
# side-loads into Kind (managerImage in test/e2e/e2e_suite_test.go), for the same
# reason the e2e kustomize overlay pins one: a manifest naming an image the node
# does not have would sit in ImagePullBackOff, not fail.
E2E_INSTALLER_IMG ?= example.com/kuberecord:v0.0.1

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
# duckdb is not a Go program at all. It is the read path an S3 archive has: the
# recipes in docs/QUERIES.md are executed through this binary against a real
# MinIO (Task 7.2), so the CLI is a test dependency rather than a build one and is
# fetched from the upstream release like promtool. It is deliberately *not* a Go
# module dependency: nothing kuberecord ships links DuckDB, and adding a CGO
# database driver to go.mod so a documentation test could run would be paying for
# the archive's read path in every operator image.
DUCKDB ?= $(LOCALBIN)/duckdb

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
# The DuckDB CLI release the published recipes are tested against. It is a floor,
# not a ceiling: the recipes use `SET VARIABLE`/`getvariable()` (1.1.0 and later)
# and `read_json_auto`'s hive_partitioning, filename and map_inference_threshold
# parameters, so anything from this release forward runs them. Raising the pin is
# a normal dependency bump; lowering it below 1.1 would silently stop the
# parameter block from working.
DUCKDB_VERSION ?= 1.5.5

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

# duckdb is extracted from the official DuckDB release, choosing the single-file
# `.gz` asset over the `.zip` one so the download needs nothing but gzip — `unzip`
# is not universally present, and this target runs on developer machines as well
# as in CI. The version-suffixed binary and the symlink follow go-install-tool's
# convention, so switching DUCKDB_VERSION re-fetches rather than silently keeping
# the old binary.
#
# `INSTALL httpfs` runs here rather than being left to the first test: reading an
# s3:// URL needs that extension, DuckDB downloads it from extensions.duckdb.org
# on demand, and a network failure at bootstrap is a clear message about a missing
# prerequisite while the same failure mid-suite reads like a broken recipe. It is
# idempotent and a no-op once the extension is cached under the user's home.
#
# GOOS is mapped rather than used directly: DuckDB spells Darwin "osx", and the
# arch names (amd64/arm64) already agree.
.PHONY: duckdb
duckdb: $(DUCKDB) ## Download the DuckDB CLI locally if necessary (used by the published-recipe tests).
$(DUCKDB): $(LOCALBIN)
	@[ -f "$(DUCKDB)-$(DUCKDB_VERSION)" ] && \
		[ "$$(readlink -- "$(DUCKDB)" 2>/dev/null)" = "$(DUCKDB)-$(DUCKDB_VERSION)" ] || { \
	set -e; \
	case "$$(go env GOOS)" in darwin) os=osx ;; *) os=$$(go env GOOS) ;; esac; \
	arch=$$(go env GOARCH); \
	url="https://github.com/duckdb/duckdb/releases/download/v$(DUCKDB_VERSION)/duckdb_cli-$$os-$$arch.gz"; \
	echo "Downloading $$url"; \
	curl -fsSL "$$url" | gzip -dc > "$(DUCKDB)-$(DUCKDB_VERSION)"; \
	chmod +x "$(DUCKDB)-$(DUCKDB_VERSION)"; \
	rm -f "$(DUCKDB)"; \
	} ;\
	ln -sf "$$(realpath "$(DUCKDB)-$(DUCKDB_VERSION)")" "$(DUCKDB)"
	@echo "Installing the DuckDB httpfs extension (needed to read s3:// archives)..."
	@"$(DUCKDB)" -c "INSTALL httpfs;" >/dev/null

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
