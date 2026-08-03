#!/usr/bin/env bash
#
# kubestream quickstart: from a fresh clone to rows in ClickHouse, on a laptop,
# in under ten minutes.
#
# Run it through the Makefile, which supplies the tool paths:
#
#   make quickstart          # stand everything up and query it
#   make quickstart-down     # delete the kind cluster
#
# Every step below is something you would do by hand, in the same order, against
# a real cluster — the script is a transcript, not a special path. The one
# concession to being a script is that it *waits*: for a rollout, for a condition,
# for the first rows to land. That is also what makes the ten-minute claim
# testable rather than asserted: CI runs this file with
# QUICKSTART_BUDGET_SECONDS=600 and the run fails if it overruns.
#
# Environment knobs, all optional:
#
#   QUICKSTART_CLUSTER=<name>       kind cluster name (default kubestream-quickstart)
#   QUICKSTART_BUDGET_SECONDS=<n>   fail the run if it takes longer than n seconds
#                                   (unset by default: a slow laptop is not a bug)
#   QUICKSTART_ROW_TIMEOUT=<n>      seconds to wait for the first rows (default 180)
#   KIND / KUBECTL / KUSTOMIZE / CONTAINER_TOOL / MAKE   tool overrides
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

KIND="${KIND:-kind}"
KUBECTL="${KUBECTL:-kubectl}"
KUSTOMIZE="${KUSTOMIZE:-${REPO_ROOT}/bin/kustomize}"
CONTAINER_TOOL="${CONTAINER_TOOL:-docker}"
MAKE="${MAKE:-make}"

CLUSTER="${QUICKSTART_CLUSTER:-kubestream-quickstart}"
BUDGET="${QUICKSTART_BUDGET_SECONDS:-}"
ROW_TIMEOUT="${QUICKSTART_ROW_TIMEOUT:-180}"

# The image tag is fixed, not configurable: examples/quickstart/operator names it
# too, and kustomize cannot read an environment variable. Two places, one value,
# and a run that never rewrites a committed file.
IMG="kubestream/quickstart:local"
CH_IMAGE="clickhouse/clickhouse-server:24.8"
PAUSE_IMAGE="registry.k8s.io/pause:3.10"

# Must match examples/quickstart/clickhouse.yaml and secret.yaml. test/docs
# asserts all three agree.
#
# The QS_ prefix is not decoration. The pre-Phase-1 operator was configured by a
# family of bare CH_* environment variables, removed with no compatibility shim,
# and test/docs fails the build if one of those names reappears anywhere. Naming a
# local shell variable after one would be confusing even where it is harmless.
QS_CH_NAMESPACE="kubestream-quickstart"
QS_CH_USER="kubestream"
QS_CH_PASSWORD="quickstart"
QS_CH_DATABASE="kubestream"
OPERATOR_NAMESPACE="kubestream-system"
# Must match the CLUSTER_ID the quickstart overlay patches in. It is stamped on
# every row, so every query below can name it.
CLUSTER_ID="kubestream-quickstart"

log() { printf '\n\033[1m==> [%02d:%02d] %s\033[0m\n' $((SECONDS / 60)) $((SECONDS % 60)) "$*"; }
note() { printf '    %s\n' "$*"; }
fail() { printf '\n\033[1;31mquickstart failed: %s\033[0m\n' "$*" >&2; exit 1; }

need() {
	command -v "$1" >/dev/null 2>&1 || fail "$1 is not installed or not on PATH. $2"
}

# sideload puts an image into the kind node, pulling it to the host first only if
# it is not already there. Letting the kubelet pull instead would put a
# several-hundred-megabyte download inside the ten-minute budget on every cold
# run, and would make the quickstart fail on a machine with no registry access.
#
# It goes through a `docker save` archive rather than `kind load docker-image`
# because published images are usually multi-platform indexes, which kind cannot
# handle: it imports with --all-platforms, and containerd then fails looking for
# the manifests of the platforms a single-platform pull never fetched —
#
#   ctr: content digest sha256:…: not found
#
# Exporting this host's platform alone produces a plain single-platform archive
# kind imports without complaint. (`docker save --platform` needs Docker 25 or
# newer.) test/harness/images.go does the same thing for the e2e and chaos
# suites, for the same reason.
sideload() {
	local image="$1" dir
	"${CONTAINER_TOOL}" image inspect "${image}" >/dev/null 2>&1 || "${CONTAINER_TOOL}" pull "${image}"
	dir="$(mktemp -d)"
	# The kind node runs on the host's Docker, so the host's architecture is the
	# node's architecture.
	"${CONTAINER_TOOL}" save --platform "linux/${HOST_ARCH}" "${image}" -o "${dir}/image.tar"
	"${KIND}" load image-archive "${dir}/image.tar" --name "${CLUSTER}"
	rm -rf "${dir}"
}

# down tears the cluster back down. Deleting the kind cluster deletes everything
# the quickstart created, including the ClickHouse and every row in it — the
# backend is an emptyDir, and that is the point of an evaluation environment.
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

# One clickhouse-client invocation inside the server pod. Going through the pod
# rather than a port-forward keeps the happy path free of a background process
# whose lifetime the script would have to manage; the port-forward is what the
# *user* is told to run at the end, for a session that outlives this script.
ch() {
	"${KUBECTL}" exec -n "${QS_CH_NAMESPACE}" deploy/clickhouse -- \
		clickhouse-client --user "${QS_CH_USER}" --password "${QS_CH_PASSWORD}" \
		--database "${QS_CH_DATABASE}" --query "$1"
}

# count runs a scalar COUNT over this cluster's rows under an optional extra
# predicate. `resource_states` does not exist for the first second or two — the
# sink creates it before its first write — so a query that fails here is a retry,
# not an error, and anything that is not a number counts as zero.
count() {
	local n
	n="$(ch "SELECT count() FROM resource_states WHERE cluster_id = '${CLUSTER_ID}' ${1:+AND $1}" 2>/dev/null || echo 0)"
	case "${n}" in
	'' | *[!0-9]*) n=0 ;;
	esac
	printf '%s' "${n}"
}

# wait_for polls count() until it reaches `want` or the timeout runs out, and
# prints whatever it last saw. It does not fail on its own: the caller knows
# whether falling short is fatal or merely worth mentioning.
wait_for() { # predicate, want, timeout
	local deadline=$((SECONDS + $3)) n=0
	while [ "${SECONDS}" -lt "${deadline}" ]; do
		n="$(count "$1")"
		[ "${n}" -ge "$2" ] && break
		sleep 2
	done
	printf '%s' "${n}"
}

diagnose() {
	printf '\n\033[1mLast 40 lines of the operator log:\033[0m\n' >&2
	"${KUBECTL}" logs -n "${OPERATOR_NAMESPACE}" deploy/kubestream-controller-manager \
		--tail=40 >&2 2>/dev/null || printf '    (no operator pod to read)\n' >&2
	printf '\n\033[1mkubestream custom resources:\033[0m\n' >&2
	"${KUBECTL}" get clickhousesinks,clusterstreamrules -o wide >&2 2>/dev/null || true
	printf '\nThe cluster is still up. Inspect it, then run: make quickstart-down\n' >&2
}
trap 'diagnose' ERR

##
## 0. Preflight
##
log "Checking prerequisites"
need "${CONTAINER_TOOL}" "Docker Desktop, Colima or Podman will do."
need "${KIND}" "See https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
need "${KUBECTL}" "See https://kubernetes.io/docs/tasks/tools/"
[ -x "${KUSTOMIZE}" ] || fail "kustomize not found at ${KUSTOMIZE}. Run 'make kustomize' first, or run this through 'make quickstart'."
"${CONTAINER_TOOL}" info >/dev/null 2>&1 || fail "${CONTAINER_TOOL} is installed but not running."
HOST_ARCH="$("${CONTAINER_TOOL}" version --format '{{.Server.Arch}}')"
[ -n "${HOST_ARCH}" ] || fail "could not determine the host architecture from ${CONTAINER_TOOL}."
note "$(${KIND} version)"
note "$(${KUBECTL} version --client 2>/dev/null | head -1)"

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

log "Side-loading ${CH_IMAGE}"
sideload "${CH_IMAGE}"

# Best-effort: every kind node already ships a pause image, so this failing is not
# a reason to stop. The `|| note` also keeps `set -e` and the ERR trap out of it.
log "Side-loading ${PAUSE_IMAGE}"
sideload "${PAUSE_IMAGE}" >/dev/null 2>&1 ||
	note "could not side-load ${PAUSE_IMAGE}; the node's own copy will be used."

##
## 3. The operator
##
# config/default plus the four quickstart deltas — see the overlay's own comment.
# The CRDs are part of it, so this single apply installs both the API and the
# controller that serves it.
log "Installing the CRDs and the operator"
"${KUSTOMIZE}" build "${SCRIPT_DIR}/operator" | "${KUBECTL}" apply --server-side -f -
"${KUBECTL}" -n "${OPERATOR_NAMESPACE}" rollout status deploy/kubestream-controller-manager --timeout=5m
note "The operator is running and completely idle: no sink, no rules, nothing streamed."

##
## 4. ClickHouse, and the credentials the operator reads
##
# The Secret goes in first: it replaces the `changeme` placeholder that ships with
# config/manager, and it must be right before a sink is created, because a sink
# that cannot authenticate never reaches the sink runtime at all.
log "Creating the credentials Secret and a single-node ClickHouse"
"${KUBECTL}" apply --server-side -f "${SCRIPT_DIR}/secret.yaml"
"${KUBECTL}" apply --server-side -f "${SCRIPT_DIR}/clickhouse.yaml"
"${KUBECTL}" -n "${QS_CH_NAMESPACE}" rollout status deploy/clickhouse --timeout=5m

##
## 5. Point the operator at it
##
log "Creating the ClickHouseSink"
"${KUBECTL}" apply --server-side -f "${SCRIPT_DIR}/sink.yaml"
# Ready=True means: credentials resolved, the server answered, and its tables
# match schema v1 — the operator created them itself, because the overlay runs it
# with --ch-auto-create-schema.
"${KUBECTL}" wait --for=condition=Ready clickhousesink/default --timeout=3m
note "Sink 'default' is Ready: schema v1 tables exist and are valid."

##
## 6. Something to record, then the rule that records it
##
# Demo objects before the rule, on purpose: a rule whose namespaceSelector
# matches nothing yet is perfectly healthy (Ready with activeWatches: 0) and
# picks the namespace up on its next resync — correct, but it would spend a
# reconcile interval of the time budget proving it.
log "Creating the demo namespace, Deployment and ConfigMap"
"${KUBECTL}" apply --server-side -f "${SCRIPT_DIR}/demo.yaml"
"${KUBECTL}" -n quickstart-demo rollout status deploy/checkout-api --timeout=3m

log "Creating the ClusterStreamRule"
"${KUBECTL}" apply --server-side -f "${SCRIPT_DIR}/rule.yaml"
"${KUBECTL}" wait --for=condition=Ready clusterstreamrule/quickstart --timeout=3m
"${KUBECTL}" get clusterstreamrule quickstart

##
## 7. The baseline reaches ClickHouse
##
# This is the assertion the whole quickstart exists to make: if no rows arrive
# inside ROW_TIMEOUT the script exits non-zero, which is what makes the CI job a
# test of the ten-minute claim rather than a restatement of it.
#
# It is also a barrier, and the ordering matters. `Ready=True` on the rule means
# the rule is valid, permitted and registered — not that its informer has finished
# its initial List. Changing an object before that List lands folds the change
# into the object's *first* recorded state, and the run then has a baseline and no
# diff: correct behaviour, and a demonstration of nothing.
log "Waiting for the demo objects' initial state to reach ClickHouse (up to ${ROW_TIMEOUT}s)"
baseline="$(wait_for "namespace = 'quickstart-demo' AND name IN ('checkout-api', 'checkout-config')" 2 "${ROW_TIMEOUT}")"
[ "${baseline}" -ge 2 ] || fail "the demo objects' initial state did not reach ClickHouse within ${ROW_TIMEOUT}s (saw ${baseline} of 2 rows)."
note "Baseline recorded. A first sighting is 'Added', or 'Snapshot' while a scope is still warming its dedup cache from sink history."

##
## 8. Make some history
##
log "Changing both objects, so there is a diff to look at"
"${KUBECTL}" -n quickstart-demo scale deploy/checkout-api --replicas=3
"${KUBECTL}" -n quickstart-demo patch configmap checkout-config \
	--type=merge -p '{"data":{"feature_flags":"new-checkout=on"}}'
note "Scaled checkout-api 1 -> 3 and flipped a feature flag."

# The ConfigMap is the one worth waiting on: its change is a genuine spec edit, so
# its Modified row is a one-operation RFC 6902 patch. (The Deployment produces
# Modified rows too, but from its rollout status settling, which is a longer and
# less legible diff.)
log "Waiting for the changes to be recorded as diffs"
modified="$(wait_for "event_type = 'Modified' AND kind = 'ConfigMap'" 1 120)"
[ "${modified}" -ge 1 ] || fail "the ConfigMap change was not recorded as a Modified row within 120s."

ELAPSED="${SECONDS}"
rows="$(count "")"
trap - ERR

##
## 9. Show what landed
##
log "Rows recorded (cluster_id = '${CLUSTER_ID}')"
ch "SELECT event_type, kind, namespace, name, ts
    FROM resource_states
    WHERE cluster_id = '${CLUSTER_ID}'
    ORDER BY ts ASC
    LIMIT 20
    FORMAT PrettyCompact"

# The flag flip, as one RFC 6902 operation rather than a second copy of the
# object. This is the row that pays for the whole design: a ConfigMap that changes
# a thousand times costs a thousand patches, not a thousand ConfigMaps.
log "The feature-flag change, as a diff"
ch "SELECT ts, name, diff
    FROM resource_states
    WHERE cluster_id = '${CLUSTER_ID}' AND kind = 'ConfigMap'
      AND name = 'checkout-config' AND event_type = 'Modified'
    ORDER BY ts DESC
    LIMIT 1
    FORMAT Vertical"

log "The Deployment's rollout, recorded the same way"
ch "SELECT ts, name, event_type, substring(diff, 1, 120) AS diff_head
    FROM resource_states
    WHERE cluster_id = '${CLUSTER_ID}' AND kind = 'Deployment' AND event_type = 'Modified'
    ORDER BY ts DESC
    LIMIT 3
    FORMAT PrettyCompact"

# The demo ConfigMap was created with data.password = "hunter2", and the sink's
# redaction floor names data.password. Reading the two keys side by side is the
# whole demonstration: one survived, one never arrived. Filtered to
# checkout-config by name, because Kubernetes injects a kube-root-ca.crt
# ConfigMap into every namespace and it has neither key.
log "Redaction: the ConfigMap's password never reached the database"
ch "SELECT name,
           JSONExtractString(data, 'data', 'feature_flags') AS feature_flags,
           JSONExtractString(data, 'data', 'password')      AS password
    FROM resource_states
    WHERE cluster_id = '${CLUSTER_ID}' AND kind = 'ConfigMap'
      AND name = 'checkout-config' AND data != ''
    ORDER BY ts ASC
    LIMIT 1
    FORMAT PrettyCompact"

log "Watch scopes: when this rule started watching"
ch "SELECT ts, action, api_group, kind, namespace, rule_ref
    FROM watch_scopes
    WHERE cluster_id = '${CLUSTER_ID}'
    ORDER BY ts ASC
    FORMAT PrettyCompact"

##
## Done
##
cat <<EOF

$(printf '\033[1;32m%s\033[0m' "kubestream is streaming. ${rows} rows in ${ELAPSED}s.")

Query it yourself — port-forward ClickHouse, then connect:

  kubectl port-forward -n ${QS_CH_NAMESPACE} svc/clickhouse 9000:9000

  clickhouse-client --host 127.0.0.1 --port 9000 \\
    --user ${QS_CH_USER} --password ${QS_CH_PASSWORD} --database ${QS_CH_DATABASE}

Or without a local client, straight through the pod:

  kubectl exec -n ${QS_CH_NAMESPACE} deploy/clickhouse -- \\
    clickhouse-client --user ${QS_CH_USER} --password ${QS_CH_PASSWORD} --database ${QS_CH_DATABASE} \\
    --query "<your SQL>"

Your first query — everything that has happened, newest first:

  SELECT ts, event_type, kind, namespace, name, actors
  FROM kubestream.resource_states
  WHERE cluster_id = '${CLUSTER_ID}'
  ORDER BY ts DESC
  LIMIT 20;

Then: change something in the quickstart-demo namespace and watch it appear.

  kubectl -n quickstart-demo set env deploy/checkout-api RELEASE=v2

More: docs/QUERIES.md (incident windows, drift by actor, flap reports, state
reconstruction), docs/SCHEMA.md (what every column means), docs/DASHBOARDS.md
(the same questions as Grafana dashboards).

Tear it all down with:  make quickstart-down
EOF

if [ -n "${BUDGET}" ] && [ "${ELAPSED}" -gt "${BUDGET}" ]; then
	fail "took ${ELAPSED}s, over the ${BUDGET}s budget."
fi
