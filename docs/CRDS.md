# Custom resources (`kuberecord.io/v1alpha1`)

Three CRDs express the two-tier model: a **sink** says *where* state goes, a
**rule** says *what* to stream there.

They are validated entirely by CRD structural schemas and CEL
(`x-kubernetes-validations`). kubestream registers no admission webhooks and
needs no cert-manager, so a validation failure is an ordinary `kubectl apply`
error rather than a dependency you have to install and keep certificates alive
for.

| Kind | Scope | Purpose |
|---|---|---|
| `ClickHouseSink` | Cluster | One ClickHouse instance the operator may write to: connection, per-sink write-path tuning, and an admission policy over what may be written to it. The password lives in a Secret, never in the CR. |
| `StreamRule` | Namespaced | Streams the resources it names **in its own namespace** to a sink. Delegable: a team can opt their own workloads into the audit trail without cluster-level privileges. Naming a cluster-scoped kind here is invalid. |
| `ClusterStreamRule` | Cluster | A `StreamRule` spec plus `spec.namespaceSelector` (nil = all namespaces, including ones created later). The only type permitted to name cluster-scoped kinds. |

Install them with `make install`; they are also part of `make deploy`,
`make build-installer`, and the Helm chart's `crds/` directory. Working examples
live in [`config/samples/`](../config/samples/) and
[`examples/quickstart/`](../examples/quickstart/).

## A rule, annotated

```yaml
apiVersion: kuberecord.io/v1alpha1
kind: StreamRule
metadata: {name: team-payments-workloads, namespace: payments}
spec:
  sinkRef: default          # immutable — see below
  resources:
  - {group: apps, version: v1, kind: Deployment}
  - group: ""
    version: v1
    kind: ConfigMap
    labelSelector: {matchLabels: {kuberecord.io/audit: "true"}}
  extraRedaction:              # optional — adds to the sink's own floor
  - {fieldPath: "data.password"}
  - {annotation: my.company.io/api-token}
```

## Validation worth knowing about

- `kind` must be the Kind (`Deployment`), not the plural resource
  (`deployments`); `version` must look like `v1` / `v2beta1`; `group` must be
  empty (core) or a DNS-1123 subdomain. `resources` must be non-empty.
- `spec.sinkRef` defaults to `default` and is **immutable**. Moving a rule to
  another sink is delete + recreate: re-pointing a live rule would strand the
  dedup/diff baseline the pipeline built for every object in scope, so records
  would either re-emit as duplicates or be written as diffs against a baseline
  the new sink never received. Recreating re-warms the cache from the new sink's
  own history instead.
- `spec.connection.credentialsSecretRef.namespace` defaults to the **operator's
  own namespace**, and that default is a security boundary rather than a
  convenience: the operator's Secret read grant is a namespaced `Role` in that
  namespace only, so a cluster-scoped sink cannot be used to make it read a
  Secret its RBAC never intended to expose. The Secret must carry the password
  under the key `password`; one that exists without a non-empty `password`
  reports `CredentialsResolved=False/SecretKeyMissing` rather than silently
  authenticating as nobody.
- `spec.policy.allowedGVKs` restricts what a sink accepts; `kinds: ["*"]` means
  every kind in the group and the list rejects duplicates server-side. Omitting
  `allowedGVKs` allows everything **except** the hard deny-list — `v1/Secret` is
  never watchable in `v1alpha1`, and no policy can re-enable it.
- `spec.policy.redaction` (sink) and `spec.extraRedaction` (rule) list values to
  scrub before hashing. Each entry sets exactly one of `fieldPath` (dot segments
  plus an optional `[*]` array wildcard, e.g.
  `spec.template.spec.containers[*].env[*].value`) or `annotation` (one
  annotation key, for keys whose dots and slashes a `fieldPath` cannot spell).
  The syntax is pattern-checked and the exactly-one rule is CEL, so a malformed
  path is rejected at admission rather than discovered at stream time. Redaction
  is additive in every direction and is **not** a way to stream `v1/Secret`. See
  [`docs/SCHEMA.md`](SCHEMA.md#redaction).
- Writer knobs are bounded: `workers` in `[1, 64]`, `batchMaxRows` in
  `[1, 100000]`, `checkpointEvery` in `[0, 10000]`. Omitted fields default to the
  same values as the `--writer-*` flags
  ([`docs/CONFIGURATION.md`](CONFIGURATION.md)), except `checkpointEvery`, which
  has no flag twin and defaults to `50`.
- `spec.writer.checkpointEvery` is how many consecutive diff-only `Modified` rows
  one object gets before the next is written as a `Checkpoint` — a row carrying
  the full state *as well as* the diff, so reconstructing "state at time T"
  replays at most that many diffs instead of walking back to the object's
  creation. `0` turns checkpointing off for the sink. A single diff that comes
  out larger than the object it describes is checkpointed regardless of the
  cadence (unless it is off). See
  [`docs/SCHEMA.md`](SCHEMA.md#checkpoint-rows) for the reconstruction recipe.

`kubectl get` renders `READY`, `SINK`, `WATCHES`, `AGE` for both rule kinds, and
`READY`, `ADDR`, `AGE` for sinks. `ADDR` shows the full `host:port`: CRD printer
columns are plain JSONPath with no string functions, so the host cannot be split
out declaratively.

## Status conditions

Every CR reports why it is or is not working through `metav1.Condition`s carrying
`observedGeneration`, so `kubectl describe` is the primary debugging surface — a
rule that cannot run degrades **only itself**, and the process never exits over
one bad rule.

`Ready` is a roll-up: it is non-`True` whenever any specific condition below is,
and it carries that condition's own reason forward. Every *transition* of `Ready`
also emits an event (`Warning` on a degrade, `Normal` on recovery) — transitions
only, so a permanently degraded rule does not flood the namespace's event log on
every resync.

### `ClickHouseSink`

| Condition | Why it is not `True` |
|---|---|
| `CredentialsResolved` | `SecretNotFound` (missing, or outside the namespace the operator may read) or `SecretKeyMissing` (present, but no non-empty `password`). A sink that cannot authenticate is never handed to the sink runtime at all. |
| `SchemaValid` | `SchemaInvalid` — the backend answered and its columns are not the ones this build writes, so it needs a migration. `Unreachable` and `ProbePending` are reported as **`Unknown`**: a host that did not answer has told us nothing about its schema. |
| `Ready` | Rolls the two up. `Unreachable` rolls up to `False` (the sink certainly cannot be written to) while `ProbePending` stays `Unknown`. |

All three come from probes the sink runtime runs on **its own** goroutines; no
reconciler ever dials ClickHouse, so an unreachable sink costs a condition and
nothing else.

### `StreamRule` / `ClusterStreamRule`

| Condition | Why it is not `True` |
|---|---|
| `PolicyAllowed` | `SecretsDenied` (the rule names `v1/Secret`, denied in code — no sink policy can admit it) or `NotInAllowList` (outside a non-empty `spec.policy.allowedGVKs`). A refused rule contributes **nothing** to the watch plan: the refusal is enforced, not merely reported. |
| `ResourceResolved` | `KindsUnresolved`, with a per-kind message: a kind whose CRD is not installed **yet** (self-heals, no restart), or a cluster-scoped kind named by a namespaced `StreamRule` (permanent until the rule is edited — use a `ClusterStreamRule`). |
| `RBACGranted` | `MissingPermissions`, naming the resource, which of `get`/`list`/`watch` are missing, and the scope. The operator can never self-escalate: an administrator adds the grant and the rule activates on its own within one resync (~2m), no restart. `AccessReviewFailed` is `Unknown` — the review itself did not complete, which is not a verdict about the rule. |
| `Ready` | Rolls the three up, and additionally reports `SinkMissing` (its `sinkRef` names no sink — targets are withdrawn) or `SinkNotReady` (the sink exists but is unhealthy — targets are **kept**, see below). |

Failures are per-target wherever they can be: a rule naming five kinds, one of
which is not installed, streams the other four and says so in `ResourceResolved`.
Only the sink-level and policy-level verdicts are all-or-nothing, because they
are statements about the rule as a whole.

`status.activeWatches` is read back out of the desired-state registry, so it
counts the `(sink, kind, namespace)` scopes the rule actually contributes — a
rule can be `Ready` with `activeWatches: 0` when its `namespaceSelector`
currently matches no namespace.

### Why `SinkNotReady` keeps streaming

A degraded rule normally withdraws its watch targets; `SinkNotReady` deliberately
does not. An unreachable database is the failure the pipeline's requeue path
exists to absorb, and tearing down every watch over a probe blip would evict
every dedup baseline the sink serves (forcing a full re-emission on recovery) and
write a pair of false scope epochs per scope — which the sink could not even
accept, being the thing that is down. The rule reports `Ready=False/SinkNotReady`
and keeps streaming into the retrying pipeline.

## No finalizers

Neither CRD uses finalizers. There is nothing outside the process to clean up
(the registry is in-memory state that dies with it), a rule deleted while the
operator was down is reconciled by the level-triggered boot pass, and a finalizer
would add a failure mode worth more than it buys: a rule stuck `Terminating`
because the operator that must release it is not running.

## See also

- [`docs/SCHEMA.md`](SCHEMA.md) — what the rows these resources produce look like.
- [`docs/RBAC.md`](RBAC.md) — what a rule is allowed to watch, and how to grant more.
- [`docs/CONFIGURATION.md`](CONFIGURATION.md) — the operator-level settings that
  back a sink's omitted writer fields.
