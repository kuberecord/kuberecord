#!/usr/bin/env bash
#
# kuberecord zero-infrastructure quickstart: from a fresh clone to an answered
# `kuberecord timeline`, on a laptop, in under ten minutes — with no ClickHouse
# and no database of any kind.
#
# Run it through the Makefile, which supplies the tool paths:
#
#   make quickstart-zero-infra          # stand everything up and query it
#   make quickstart-zero-infra-down     # delete the kind cluster
#
# What makes this path different from `make quickstart` is what it does *not*
# stand up. There is one object store, the archive is a handful of compressed
# JSON Lines objects in it, and the whole read side is one static binary reading
# those objects directly. Nothing here is a reduced demonstration of the
# capture path: an S3Sink records exactly what a ClickHouseSink records, and the
# CLI answers the same questions from either. What it costs is stated where it is
# paid — see step 9 and docs/CLI.md#cold-scans.
#
# Every step below is something you would do by hand, in the same order, against
# a real cluster — the script is a transcript, not a special path. The one
# concession to being a script is that it *waits*: for a rollout, for a condition,
# for the first objects to be closed and PUT. That is also what makes the
# ten-minute claim testable rather than asserted: CI runs this file with
# ZERO_INFRA_BUDGET_SECONDS=600 and the run fails if it overruns.
#
# Environment knobs, all optional:
#
#   ZERO_INFRA_CLUSTER=<name>         kind cluster name (default kuberecord-zero-infra)
#   ZERO_INFRA_BUDGET_SECONDS=<n>     fail the run if it takes longer than n seconds
#                                     (unset by default: a slow laptop is not a bug)
#   ZERO_INFRA_OBJECT_TIMEOUT=<n>     seconds to wait for archived objects (default 180)
#   ZERO_INFRA_PORT=<n>               local port for the MinIO port-forward (default 19000)
#   ZERO_INFRA_CHART=<path|ref>       the chart to install (default: this clone's)
#   KIND / KUBECTL / HELM / CONTAINER_TOOL / GO / MAKE   tool overrides
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

KIND="${KIND:-kind}"
KUBECTL="${KUBECTL:-kubectl}"
HELM="${HELM:-${REPO_ROOT}/bin/helm}"
CONTAINER_TOOL="${CONTAINER_TOOL:-docker}"
GO="${GO:-go}"
MAKE="${MAKE:-make}"

CLUSTER="${ZERO_INFRA_CLUSTER:-kuberecord-zero-infra}"
BUDGET="${ZERO_INFRA_BUDGET_SECONDS:-}"
OBJECT_TIMEOUT="${ZERO_INFRA_OBJECT_TIMEOUT:-180}"
LOCAL_PORT="${ZERO_INFRA_PORT:-19000}"
CHART="${ZERO_INFRA_CHART:-${REPO_ROOT}/deploy/charts/kuberecord}"

# The image tag is fixed rather than configurable, for the same reason the other
# quickstart's is: two places naming one value, and a run that never rewrites a
# committed file.
IMG="kuberecord/zero-infra:local"
MINIO_IMAGE="minio/minio:RELEASE.2025-04-22T22-12-26Z"
PAUSE_IMAGE="registry.k8s.io/pause:3.10"

# Must match examples/zero-infra/minio.yaml, secret.yaml and sink.yaml. test/docs
# asserts that they agree.
MINIO_NAMESPACE="kuberecord-zero-infra"
BUCKET="kuberecord-zero-infra"
PREFIX="audit"
DEMO_NAMESPACE="zero-infra-demo"
OPERATOR_NAMESPACE="kuberecord-system"
RELEASE="kuberecord"
# Must match the clusterID the helm install sets below. It is stamped on every
# record, and it is what the CLI resolves — from the operator's Deployment here,
# and from the archive itself when there is no cluster to ask.
CLUSTER_ID="kuberecord-zero-infra"

# Where the CLI is built to. Both shipped names come from one compilation
# this builds the one name a script needs.
CLI="${REPO_ROOT}/bin/kuberecord"

log() { printf '\n\033[1m==> [%02d:%02d] %s\033[0m\n' $((SECONDS / 60)) $((SECONDS % 60)) "$*"; }
note() { printf '    %s\n' "$*"; }
fail() { printf '\n\033[1;31mzero-infra quickstart failed: %s\033[0m\n' "$*" >&2; exit 1; }

need() {
	command -v "$1" >/dev/null 2>&1 || fail "$1 is not installed or not on PATH. $2"
}

# sideload puts an image into the kind node, pulling it to the host first only if
# it is not already there. It goes through a `docker save` archive rather than
# `kind load docker-image` because published images are usually multi-platform
# indexes, which kind cannot handle. examples/quickstart/quickstart.sh explains
# the failure mode at length; this is the same function for the same reason.
sideload() {
	local image="$1" dir
	"${CONTAINER_TOOL}" image inspect "${image}" >/dev/null 2>&1 || "${CONTAINER_TOOL}" pull "${image}"
	dir="$(mktemp -d)"
	"${CONTAINER_TOOL}" save --platform "linux/${HOST_ARCH}" "${image}" -o "${dir}/image.tar"
	"${KIND}" load image-archive "${dir}/image.tar" --name "${CLUSTER}"
	rm -rf "${dir}"
}

# down tears the cluster back down. Deleting the kind cluster deletes everything
# this script created, the archive included — the object store is an emptyDir, and
# that is the point of an evaluation environment.
down() {
	log "Deleting kind cluster '${CLUSTER}'"
	"${KIND}" delete cluster --name "${CLUSTER}"
	printf '\nGone. Nothing outside the cluster was touched.\n'
}

if [ "${1:-}" = "down" ]; then
	need "${KIND}" "See https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
	down
	exit 0
fi

# mc runs one `mc` invocation inside the MinIO pod. Going through the pod keeps
# the bucket work free of any S3 client on your machine; the port-forward below
# exists only for the CLI, which is the half being demonstrated.
mc() {
	"${KUBECTL}" exec -n "${MINIO_NAMESPACE}" deploy/minio -- mc "$@"
}

# objects counts the *record* objects in the archive. `mc ls --recursive` over the
# prefix lists the closed ones; an object still accumulating in a writer worker's
# memory is deliberately not there, which is what the wait below is waiting for.
#
# The cluster_id= prefix is what makes this count records rather than everything:
# scope epochs are filed under format=jsonl-v1/scopes/ and are written the moment
# a rule opens a watch, so a count over the whole format= prefix would report the
# baseline as archived before a single record had been.
objects() {
	local n
	n="$(mc ls --recursive "local/${BUCKET}/${PREFIX}/format=jsonl-v1/cluster_id=${CLUSTER_ID}/" 2>/dev/null |
		grep -c 'jsonl.zst' || true)"
	case "${n}" in
	'' | *[!0-9]*) n=0 ;;
	esac
	printf '%s' "${n}"
}

# wait_for_objects polls objects() until it reaches `want` or the timeout runs
# out, and prints whatever it last saw. It does not fail on its own: the caller
# knows whether falling short is fatal.
wait_for_objects() { # want, timeout
	local deadline=$((SECONDS + $2)) n=0
	while [ "${SECONDS}" -lt "${deadline}" ]; do
		n="$(objects)"
		[ "${n}" -ge "$1" ] && break
		sleep 3
	done
	printf '%s' "${n}"
}

PORT_FORWARD_PID=""
stop_port_forward() {
	[ -n "${PORT_FORWARD_PID}" ] || return 0
	kill "${PORT_FORWARD_PID}" 2>/dev/null || true
	wait "${PORT_FORWARD_PID}" 2>/dev/null || true
	PORT_FORWARD_PID=""
}
trap stop_port_forward EXIT

diagnose() {
	printf '\n\033[1mLast 40 lines of the operator log:\033[0m\n' >&2
	"${KUBECTL}" logs -n "${OPERATOR_NAMESPACE}" "deploy/${RELEASE}-controller-manager" \
		--tail=40 >&2 2>/dev/null || printf '    (no operator pod to read)\n' >&2
	printf '\n\033[1mkuberecord custom resources:\033[0m\n' >&2
	"${KUBECTL}" get s3sinks,clusterstreamrules -o wide >&2 2>/dev/null || true
	printf '\nThe cluster is still up. Inspect it, then run: make quickstart-zero-infra-down\n' >&2
}
trap 'diagnose' ERR

##
## 0. Preflight
##
log "Checking prerequisites"
need "${CONTAINER_TOOL}" "Docker Desktop, Colima or Podman will do."
need "${KIND}" "See https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
need "${KUBECTL}" "See https://kubernetes.io/docs/tasks/tools/"
need "${GO}" "The CLI is built from this clone. See https://go.dev/dl/"
[ -x "${HELM}" ] || fail "helm not found at ${HELM}. Run 'make helm' first, or run this through 'make quickstart-zero-infra'."
"${CONTAINER_TOOL}" info >/dev/null 2>&1 || fail "${CONTAINER_TOOL} is installed but not running."
HOST_ARCH="$("${CONTAINER_TOOL}" version --format '{{.Server.Arch}}')"
[ -n "${HOST_ARCH}" ] || fail "could not determine the host architecture from ${CONTAINER_TOOL}."
note "$(${KIND} version)"
note "$("${HELM}" version --short)"

##
## 1. A cluster to watch
##
if "${KIND}" get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
	log "Reusing existing kind cluster '${CLUSTER}'"
else
	log "Creating kind cluster '${CLUSTER}'"
	"${KIND}" create cluster --name "${CLUSTER}" --config "${SCRIPT_DIR}/kind.yaml"
fi
"${KUBECTL}" config use-context "kind-${CLUSTER}" >/dev/null
note "kubectl context: kind-${CLUSTER}"

##
## 2. Images, side-loaded so no registry is on the critical path
##
log "Building the operator image from this clone (${IMG})"
"${MAKE}" -C "${REPO_ROOT}" docker-build IMG="${IMG}"
sideload "${IMG}"

log "Side-loading ${MINIO_IMAGE}"
sideload "${MINIO_IMAGE}"

# Best-effort: every kind node already ships a pause image, so this failing is not
# a reason to stop.
log "Side-loading ${PAUSE_IMAGE}"
sideload "${PAUSE_IMAGE}" >/dev/null 2>&1 ||
	note "could not side-load ${PAUSE_IMAGE}; the node's own copy will be used."

##
## 3. The operator, installed with helm
##
# The chart carries the CRDs, so this single install puts both the API and the
# controller that serves it in place. From a release you would install
# `oci://ghcr.io/kuberecord/charts/kuberecord --version X.Y.Z` and set no image
# at all; here the point is to run the operator this clone builds.
log "Installing the operator with helm (${CHART})"
"${HELM}" upgrade --install "${RELEASE}" "${CHART}" \
	--namespace "${OPERATOR_NAMESPACE}" --create-namespace \
	--set image.repository="${IMG%%:*}" \
	--set image.tag="${IMG##*:}" \
	--set clusterID="${CLUSTER_ID}" \
	--wait --timeout 5m
note "The operator is running and completely idle: no sink, no rules, nothing streamed."

##
## 4. An object store, and the credentials the operator writes with
##
# The Secret goes in first: a sink that cannot authenticate never reaches the sink
# runtime at all.
log "Creating the credentials Secret and a single-node MinIO"
"${KUBECTL}" apply --server-side -f "${SCRIPT_DIR}/secret.yaml"
"${KUBECTL}" apply --server-side -f "${SCRIPT_DIR}/minio.yaml"
"${KUBECTL}" -n "${MINIO_NAMESPACE}" rollout status deploy/minio --timeout=5m

# kuberecord never creates a bucket, on any backend and in any configuration:
# retention, encryption, lifecycle and Object Lock all belong to whoever owns the
# account, and Object Lock in particular is irreversible once enabled. See
# docs/RETENTION.md.
log "Creating the bucket (kuberecord never creates one)"
mc alias set local http://localhost:9000 \
	"$("${KUBECTL}" get secret minio-credentials -n "${MINIO_NAMESPACE}" -o 'jsonpath={.data.accessKeyId}' | base64 -d)" \
	"$("${KUBECTL}" get secret minio-credentials -n "${MINIO_NAMESPACE}" -o 'jsonpath={.data.secretAccessKey}' | base64 -d)" >/dev/null
mc mb --ignore-existing "local/${BUCKET}"

##
## 5. Point the operator at it
##
log "Creating the S3Sink"
"${KUBECTL}" apply --server-side -f "${SCRIPT_DIR}/sink.yaml"
# Ready=True means: credentials resolved and the bucket answered a write probe.
"${KUBECTL}" wait --for=condition=Ready s3sink/archive --timeout=3m
# HistoryUnavailable=True is expected and is not a fault: an S3Sink is Writer-only
# so it cannot read its own history back. Printing it here rather than
# leaving it to be discovered in `kubectl describe` is the point.
"${KUBECTL}" get s3sink archive \
	-o 'jsonpath={range .status.conditions[*]}{.type}={.status}/{.reason}{"\n"}{end}' |
	sed 's/^/    /'

##
## 6. Something to record, then the rule that records it
##
# Demo objects before the rule, on purpose: a rule whose namespaceSelector matches
# nothing yet is perfectly healthy and picks the namespace up on its next resync —
# correct, but it would spend a reconcile interval of the time budget proving it.
#
# --force-conflicts is for the re-run, not the first run. Step 7 patches both of
# these objects, which makes `kubectl-patch` the field manager of the two fields
# it touched; a second `make quickstart-zero-infra` against a reused cluster then
# refuses to apply this file over them. Taking the fields back is exactly right —
# this manifest is what the demo starts from, and the run is about to change them
# again.
log "Creating the demo namespace, Deployment and ConfigMap"
"${KUBECTL}" apply --server-side --force-conflicts -f "${SCRIPT_DIR}/demo.yaml"
"${KUBECTL}" -n "${DEMO_NAMESPACE}" rollout status deploy/checkout-api --timeout=3m

log "Creating the ClusterStreamRule"
"${KUBECTL}" apply --server-side -f "${SCRIPT_DIR}/rule.yaml"
"${KUBECTL}" wait --for=condition=Ready clusterstreamrule/zero-infra --timeout=3m
"${KUBECTL}" get clusterstreamrule zero-infra

##
## 7. Make some history
##
# The archive's objects close on rotation — 20s in examples/zero-infra/sink.yaml —
# so the baseline and the change are both waited for rather than assumed. Changing
# the objects before the informer's initial List has landed would fold the change
# into their *first* recorded state, leaving a baseline and no diff.
log "Waiting for the demo objects' initial state to be archived (up to ${OBJECT_TIMEOUT}s)"
baseline="$(wait_for_objects 1 "${OBJECT_TIMEOUT}")"
[ "${baseline}" -ge 1 ] || fail "no records were archived within ${OBJECT_TIMEOUT}s."
note "Baseline archived in ${baseline} object(s)."

log "Changing both objects, so there is a diff to look at"
# A *strategic* merge patch for the Deployment, which is kubectl's default and is
# what makes `containers` merge on the container's name. A plain --type=merge here
# would be a JSON merge patch, which replaces the whole array — dropping `image`
# and leaving an invalid Deployment.
"${KUBECTL}" -n "${DEMO_NAMESPACE}" patch deploy/checkout-api \
	-p '{"spec":{"template":{"spec":{"containers":[{"name":"app","resources":{"limits":{"cpu":"10m","memory":"64Mi"}}}]}}}}'
# A ConfigMap has no merge keys, so --type=merge is right here: a JSON merge patch
# over a flat map replaces the one key it names and leaves the rest alone.
"${KUBECTL}" -n "${DEMO_NAMESPACE}" patch configmap checkout-config \
	--type=merge -p '{"data":{"feature_flags":"new-checkout=on"}}'
note "Raised checkout-api's memory limit 16Mi -> 64Mi and flipped a feature flag."

log "Waiting for the changes to be closed into an object (rotation is 20s)"
after="$(wait_for_objects $((baseline + 1)) 180)"
[ "${after}" -gt "${baseline}" ] ||
	fail "the changes were not archived within 180s (still ${after} object(s))."
note "${after} object(s) in ${BUCKET}/${PREFIX}."

##
## 8. The CLI, built from this clone
##
# Cross-compiled, cgo-free and static in a release; here it is one
# `go build` for this host. Nothing it needs is installed anywhere: no database
# client, no DuckDB, no engine.
log "Building the CLI"
"${GO}" build -C "${REPO_ROOT}" -o "${CLI}" ./cmd/kubectl-kuberecord
note "$("${CLI}" version | head -1)"

##
## 9. Read the archive back
##
# A port-forward is how a laptop reaches an in-cluster object store. Against a
# real bucket there is nothing to forward: the same command with
# `--source s3://your-bucket/your-prefix` and your ordinary AWS credentials is the
# whole difference.
log "Port-forwarding MinIO to 127.0.0.1:${LOCAL_PORT}"
"${KUBECTL}" port-forward -n "${MINIO_NAMESPACE}" svc/minio "${LOCAL_PORT}:9000" >/dev/null 2>&1 &
PORT_FORWARD_PID=$!
# The probe runs in a subshell so the descriptor it opens closes with it. Closing
# one in this shell instead would need `exec 3>&-`, and an `exec` carrying a
# redirection and no command applies that redirection to the shell *permanently* —
# so the obvious `exec 3>&- 2>/dev/null` silently sends the rest of the run's
# stderr to /dev/null, taking every notice the CLI prints below with it.
pf_deadline=$((SECONDS + 60))
until (exec 3<>"/dev/tcp/127.0.0.1/${LOCAL_PORT}") 2>/dev/null; do
	[ "${SECONDS}" -lt "${pf_deadline}" ] || fail "the port-forward to MinIO never came up."
	sleep 1
done

# The CLI reads an S3-compatible store through the ordinary AWS credential chain,
# because every tool on the machine already reads it and kuberecord has no
# business owning a second one. These four variables are that chain.
export AWS_ACCESS_KEY_ID="kuberecord"
export AWS_SECRET_ACCESS_KEY="kuberecord-zero-infra"
export AWS_REGION="us-east-1"
export AWS_ENDPOINT_URL_S3="http://127.0.0.1:${LOCAL_PORT}"
export AWS_S3_FORCE_PATH_STYLE="true"

SOURCE="s3://${BUCKET}/${PREFIX}"
# --yes answers the confirmation an unindexed backend asks before a scan whose
# size it could not estimate. The windows below are narrow enough that a measured
# scan would never ask, but a script that could block on a prompt is a script that
# will, one day, on somebody's laptop. It is assumed off a terminal anyway.
kr() { "${CLI}" --source "${SOURCE}" --yes "$@"; }

log "What was being recorded, and when"
kr scopes

log "The flagship question: what changed on checkout-api, and who changed it"
kr timeline deploy/checkout-api -n "${DEMO_NAMESPACE}" --since 1h

log "The same change, with the old value beside the new one"
kr diff deploy/checkout-api -n "${DEMO_NAMESPACE}" --since 1h

log "Redaction: the ConfigMap's password never reached the archive"
kr get configmap/checkout-config -n "${DEMO_NAMESPACE}"

# The assertion the whole script exists to make. Exit code 3 would mean nothing
# was ever watching, which is a different finding from an empty timeline and is
# reported as one — either way this is a failure here, because both objects were
# demonstrably changed above.
#
# Both streams are captured, because both are part of the claim. stdout carries
# the changes; stderr carries the notices that say which backend answered, what
# the scan cost, and what this backend cannot record. A run that produced the rows
# with none of that explanation would still be a working CLI and would no longer
# be a working demonstration — and it is an easy thing to lose to a stray
# redirection, which is how this check came to be here.
log "Asserting the timeline is not empty, and that it explained itself"
answer="$(mktemp)"
notices="$(mktemp)"
kr timeline deploy/checkout-api -n "${DEMO_NAMESPACE}" --since 1h -o jsonl \
	>"${answer}" 2>"${notices}" || true

changes="$(grep -c '"event_type"' "${answer}" || true)"
case "${changes}" in
'' | *[!0-9]*) changes=0 ;;
esac
[ "${changes}" -ge 1 ] ||
	fail "the CLI read the archive but found no changes for ${DEMO_NAMESPACE}/checkout-api."
note "${changes} recorded change(s) read out of the archive."

grep -q 'objectsource' "${notices}" ||
	fail "the CLI answered but printed no notice naming the backend it answered from; \
an unexplained answer over an archive is the failure this tool exists not to produce."
note "$(grep -c . "${notices}") notice(s) on stderr, none of them on the pipe."
rm -f "${answer}" "${notices}"

# And the standalone half: no kubeconfig, no cluster, no API server. The address
# is spelled as the archive records it — `Deployment.apps/checkout-api` rather
# than `deploy/checkout-api` — because resolving a short name is the one thing
# that needs a server to ask (docs/CLI.md#reading-an-archive-without-a-cluster).
#
# An empty HOME rather than a nonexistent one: the point is that there is no
# kubeconfig and no kuberecord configuration to find, not that the filesystem is
# hostile. --cluster-id is passed because with no cluster to read the operator's
# Deployment from, an archive holding one cluster's history would still resolve it
# — and being explicit is what makes this step prove the flag rather than the
# fallback.
log "The same answer with no cluster at all"
NO_CLUSTER_HOME="$(mktemp -d)"
env -u KUBECONFIG -u XDG_CONFIG_HOME "HOME=${NO_CLUSTER_HOME}" \
	"${CLI}" --source "${SOURCE}" --yes --cluster-id "${CLUSTER_ID}" \
	timeline Deployment.apps/checkout-api -n "${DEMO_NAMESPACE}" --since 1h
rm -rf "${NO_CLUSTER_HOME}"

ELAPSED="${SECONDS}"
total="$(objects)"
trap - ERR

##
## Done
##
cat <<EOF

$(printf '\033[1;32m%s\033[0m' "kuberecord is archiving. ${total} object(s), ${changes} change(s) read, in ${ELAPSED}s.")

No database was installed. The whole read side is one static binary and a
prefix in an object store.

Keep going — the port-forward this script used has been closed, so start your own:

  kubectl port-forward -n ${MINIO_NAMESPACE} svc/minio ${LOCAL_PORT}:9000

  export AWS_ACCESS_KEY_ID=kuberecord
  export AWS_SECRET_ACCESS_KEY=kuberecord-zero-infra
  export AWS_ENDPOINT_URL_S3=http://127.0.0.1:${LOCAL_PORT}
  export AWS_S3_FORCE_PATH_STYLE=true

  ${CLI} --source ${SOURCE} timeline deploy/checkout-api -n ${DEMO_NAMESPACE} --since 1h

Then change something and watch it appear — an object closes 20 seconds later:

  kubectl -n ${DEMO_NAMESPACE} scale deploy/checkout-api --replicas=3

Save the profile once and drop the flags:

  ${CLI} config set-profile evaluation --backend s3 --bucket ${BUCKET} \\
      --prefix ${PREFIX} --endpoint http://127.0.0.1:${LOCAL_PORT} --force-path-style

What the archive cannot answer, and why it says so rather than guessing:
docs/CLI.md#backend-capability-differences. What it costs to scan a wide window
without an index: docs/CLI.md#cold-scans. Wide analytics over the same objects —
aggregations, joins, fleet-level drift — stay in SQL: docs/QUERIES.md.

Tear it all down with:  make quickstart-zero-infra-down
EOF

if [ -n "${BUDGET}" ] && [ "${ELAPSED}" -gt "${BUDGET}" ]; then
	fail "took ${ELAPSED}s, over the ${BUDGET}s budget."
fi
