# Custom resources (`kuberecord.io/v1alpha1`)

Three CRDs express the two-tier model: a **sink** says *where* state goes, a
**rule** says *what* to stream there.

They are validated entirely by CRD structural schemas and CEL
(`x-kubernetes-validations`). kuberecord registers no admission webhooks and
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
  sink: {kind: ClickHouseSink, name: default}   # immutable as a pair — see below
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
- `spec.sink` is **required** and names the target backend as a `{kind, name}`
  pair. `kind` defaults to `ClickHouseSink` and is constrained to the sink kinds
  this build actually serves, so a rule naming a kind no reconciler implements is
  rejected where it was typed rather than admitted and then parked forever with
  nothing to bind to. `name` has no default: a rule names one specific backend out
  of however many a cluster runs, and guessing which one its author meant is the
  kind of convenience that quietly streams an audit trail somewhere nobody chose.
  Both halves are the identity, because a name is only unique *within* a kind — a
  `ClickHouseSink` named `default` and an `S3Sink` named `default` are two
  unrelated backends.
- `spec.sink` is **immutable**, and the CEL rule guards the whole pair rather than
  each field, since changing the name and changing the kind are the same mistake
  with the same consequence. Moving a rule to another sink is delete + recreate:
  re-pointing a live rule would strand the dedup/diff baseline the pipeline built
  for every object in scope, so records would either re-emit as duplicates or be
  written as diffs against a baseline the new sink never received. Recreating
  re-warms the cache from the new sink's own history instead.
- A rule targets exactly one sink, permanently. To stream one resource set to two
  backends, author two rules naming the same resources and different sinks — each
  then carries its own dedup state, its own conditions and its own watch
  accounting, so one unreachable backend degrades one rule.
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

`kubectl get` renders `READY`, `SINK`, `SINK-KIND`, `WATCHES`, `AGE` for both rule
kinds, and `READY`, `ADDR`, `AGE` for sinks. The sink's kind gets a column of its
own rather than being folded into `SINK`, for the same reason `ADDR` shows a full
`host:port`: CRD printer columns are plain JSONPath with no string functions, so
neither a pair can be joined nor a host split out declaratively.

## Status conditions

Every CR reports why it is or is not working through `metav1.Condition`s carrying
`observedGeneration`, so `kubectl describe` is the primary debugging surface — a
rule that cannot run degrades **only itself**, and the process never exits over
one bad rule.

`Ready` is a roll-up: it is non-`True` whenever any specific condition below is,
and it carries that condition's own reason forward. The one exception is
`HistoryUnavailable`, which `Ready` deliberately ignores — it reports a declared
capability limit of a backend rather than something going wrong, and reads
backwards from every other condition here (see below). Every *transition* of `Ready`
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

### `S3Sink`

| Condition | Why it is not `True` |
|---|---|
| `CredentialsResolved` | `SecretNotFound` or `SecretKeyMissing`, as for a `ClickHouseSink`. A sink that names **no** Secret is `True` with `AmbientCredentials` — it authenticates from IRSA, workload identity or an instance role, which is the recommended shape on a cloud provider. If that chain then produces nothing, the probe reports it here as `CredentialsUnavailable`, not as an unreachable bucket. |
| `BucketReachable` | `BucketUnreachable` (DNS, a refused connection, a 5xx, a rejected credential — clears on its own) or `BucketIncompatible` (the bucket answered and refused the *shape* of object this sink writes, today a `spec.objectLock` against a bucket with no Object Lock configuration — permanent until a human changes the bucket or the spec). The probe **writes**, not `HEAD`s: a read-only credential passes a `HEAD` and then fails every `PUT`. |
| `Ready` | Rolls the two up. |
| `HistoryUnavailable` | **Reads backwards, and is `True` on a healthy sink.** See below. |

#### `HistoryUnavailable` — a limit, not a fault

Every other condition in this document follows the Kubernetes convention that
`True` is healthy. This one inverts it deliberately, in the same shape as a Node's
`NetworkUnavailable`, and the inversion is the point.

An `S3Sink` is a `Writer`-only backend: it cannot read its own history back,
because "what was the last recorded state of every object in this scope?" would
mean listing and decompressing every object ever written under the prefix — on the
operator's boot path, growing forever. So three behaviours are off for it:

* dedup **cache warm-up** — a restarting operator cannot learn what the archive
  already holds, so every record is a permanent `Snapshot` rather than
  `Added`/`Modified`;
* **zombie garbage collection** — an object deleted while the operator was down is
  never recorded as deleted; the archive's last word on it is its last observed
  state;
* **boot reconciliation of scope epochs** — a scope left open by a process that
  died stays open in the archive, so a reader must treat an unmatched `Started` as
  an epoch whose end is unknown.

That degradation is invisible in the archive itself — an object store with no
deletions in it looks exactly like an archive of a cluster where nothing was
deleted — which is why it is stated in the API rather than left to be inferred:

| Where | What it says |
|---|---|
| The sink | `HistoryUnavailable=True`, reason `WriterOnlySink`, with the three behaviours and both consequences in the message. `Ready` stays `True`. |
| A `Warning` event | Emitted once, when the condition is first established, so it shows up in `kubectl describe` without anyone knowing to look for the condition. |
| Every rule bound to it | The same condition, mirrored, naming the sink — because a rule's author may never read the cluster-scoped sink they named. |
| `kuberecord_safe_mode` | Pinned at `1` for every scope on the sink. See [`docs/OPERATING.md`](OPERATING.md). |

`Unknown`/`CapabilitiesUnknown` means the sink has no running instance yet, so
nothing has been detected. It resolves within a second or so of the first health
probe — unless the sink's credentials never resolved, in which case it was never
handed to the sink runtime at all and `CredentialsResolved` is the condition to
read.

The supported way to have both a queryable timeline and a cheap immutable archive
is the tee pattern: two rules over the same resources, one naming a
`ClickHouseSink` and one naming an `S3Sink`. One informer serves both and each
carries its own dedup state, so the limits above apply to the archiving rule
alone. See [`docs/TEE.md`](TEE.md).

#### `spec.objectLock` and what `BucketIncompatible` is telling you

`spec.objectLock` sets per-object S3 Object Lock retention, which is what makes
the archive tamper-evident. kuberecord cannot enable Object Lock on the bucket —
only a human on the account can, it implies versioning, and it cannot be undone —
so a sink asking for retention against a bucket that has none reports
`BucketReachable=False` with reason `BucketIncompatible` and stays there until
someone changes the bucket or the spec. Both retention modes, the lifecycle
interaction, and the honest limits of the WORM claim are
[`docs/RETENTION.md`](RETENTION.md).

### `StreamRule` / `ClusterStreamRule`

| Condition | Why it is not `True` |
|---|---|
| `PolicyAllowed` | `SecretsDenied` (the rule names `v1/Secret`, denied in code — no sink policy can admit it) or `NotInAllowList` (outside a non-empty `spec.policy.allowedGVKs`). A refused rule contributes **nothing** to the watch plan: the refusal is enforced, not merely reported. |
| `ResourceResolved` | `KindsUnresolved`, with a per-kind message: a kind whose CRD is not installed **yet** (self-heals, no restart), or a cluster-scoped kind named by a namespaced `StreamRule` (permanent until the rule is edited — use a `ClusterStreamRule`). |
| `RBACGranted` | `MissingPermissions`, naming the resource, which of `get`/`list`/`watch` are missing, and the scope. The operator can never self-escalate: an administrator adds the grant and the rule activates on its own within one resync (~2m), no restart. `AccessReviewFailed` is `Unknown` — the review itself did not complete, which is not a verdict about the rule. |
| `Ready` | Rolls the three up, and additionally reports `SinkMissing` (its `spec.sink` names no sink that exists — targets are withdrawn), `SinkNotReady` (the sink exists but is unhealthy — targets are **kept**, see below) or `LegacySinkRef` (the rule names no sink at all, which is what a rule authored against v0.1.0 looks like after an upgrade — see [`docs/UPGRADING.md`](UPGRADING.md)). |
| `HistoryUnavailable` | Mirrored from the sink this rule names, and **not** rolled into `Ready`: `True`/`WriterOnlySink` when that sink cannot reconstruct history, so an author reading only their rule still learns that its records are permanent `Snapshot`s and that deletions during an operator outage go unrecorded. See the `S3Sink` section above. |

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
- [`docs/TEE.md`](TEE.md) — streaming one resource set to two sinks at once, and
  what each half does and does not guarantee.
- [`docs/RETENTION.md`](RETENTION.md) — `spec.objectLock` in full: the bucket
  prerequisite, `GOVERNANCE` versus `COMPLIANCE`, lifecycle rules, and the limits
  of the tamper-evidence claim.
- [`docs/UPGRADING.md`](UPGRADING.md) — what to do when a `v0.x` minor changes one
  of these fields, the v0.2.0 sink-reference rename included.
