# Hot and cold: streaming to two backends

kuberecord's two backends answer different questions. ClickHouse gives you a
queryable timeline with real deletions, diffs and state reconstruction. An
object store gives you a cheap, immutable, WORM-capable archive that cannot
answer a question at all. Most people who want one eventually want both.

The supported way to have both is **two rules, not one clever sink** (D14):

```yaml
spec:
  sink: {kind: ClickHouseSink, name: hot}
  resources: [ … ]
---
spec:
  sink: {kind: S3Sink, name: cold}
  resources: [ … ]          # identical
```

That is the entire pattern. There is no multi-sink field, no fan-out mode, and
no sink that writes to two places. A rule targets exactly one sink, permanently
(`spec.sink` is immutable), and teeing is what you get by writing the rule twice.

A runnable version of everything on this page — MinIO, both sinks, both rules, a
workload to change — is [`examples/tee/`](../examples/tee/), and an e2e scenario
applies that example on every push and asserts that one object's change lands in
both backends.

## Why this is nearly free

The reason the answer is "write the rule twice" rather than "configure a fan-out"
is that the data plane was already built this way. A rule is not a pipeline; it
is a statement of interest, and the operator resolves the union of everyone's
interests into watches.

```text
                          apps/v1 Deployments in payments
                                       │
                                 ONE informer                 ← watch identity is
                                       │                        (resource, namespace)
                     ┌─────────────────┴─────────────────┐
                     ▼                                   ▼
        key{ClickHouseSink/hot, …}            key{S3Sink/cold, …}   ← sink is part of
                     │                                   │            the *work key*
                     ▼                                   ▼
             hashCache[hot]                       hashCache[cold]    ← independent
                     │                                   │
                     ▼                                   ▼
              batched INSERT                      rotated JSONL PUT
                     │                                   │
                     ▼                                   ▼
                 ClickHouse                          S3 / MinIO
```

Three consequences follow, and each is a property rather than a promise.

**One informer, not two.** Informer identity is `(resource, namespace)`. The sink
is part of the *work key*, not of the watch. Two rules wanting `apps/v1`
Deployments in `payments` therefore share one informer, one API-server watch, one
informer cache and one initial list — and each event becomes two pipeline keys.
Adding a cold tier to an existing rule set adds **no** watch load, no API-server
traffic and no cache memory. This is asserted directly, by pool size, in
`TestWatchManagerSharesOneInformerPerTarget` (`internal/watch/manager_test.go`).

What it *does* add is one queue slot, one hash and one write per event per sink,
and one `hashCache` per sink — so budget the second backend's write path and its
dedup memory, not a second watch. [`docs/PERFORMANCE.md`](PERFORMANCE.md) has the
per-sink envelopes.

**Two dedup caches, decided independently.** The `hashCache` is per sink
(`kuberecord_hashcache_entries{sink}`), and version-gated commits are per sink.
Neither half can suppress a write to the other, and neither can mistake the
other's success for its own: a write that fails leaves that sink's reserved cache
version uncommitted, so the object is written there again at its next
observation, while the healthy half carries on with a baseline the failure never
touched. The same separation holds for scope epochs — each sink gets its own
`Started` and `Stopped` record for the same watch.

**Two sets of status conditions, two failure domains.** An unreachable ClickHouse
degrades the hot rule and leaves the cold one streaming; an object store rejecting
writes degrades the cold rule and leaves the timeline intact. Neither tears down
the shared informer, because a sink being down is the failure the pipeline's
requeue path exists to absorb (see
[`docs/CRDS.md`](CRDS.md#why-sinknotready-keeps-streaming)).

## The two halves are not equivalent

This is the part to read before you rely on the archive.

An `S3Sink` is a `Writer`-only backend (D12): it cannot read its own history
back, because "what was the last recorded state of every object in this scope?"
would mean listing and decompressing every object ever written under the prefix,
on the operator's boot path, forever. So three behaviours are off for it, and the
same three are on for the ClickHouse half sitting beside it.

| | `ClickHouseSink` (hot) | `S3Sink` (cold) |
|---|---|---|
| First sighting of an object | `Added` once the scope has warmed | **always `Snapshot`**, permanently |
| Change to a known object | `Modified` with an RFC 6902 diff | `Modified` with the same diff |
| Periodic full state | `Checkpoint` every `checkpointEvery` diffs | not applicable — no diffs to checkpoint |
| Object deleted while the operator runs | `Deleted` | `Deleted` |
| Object deleted while the operator is **down** | `Deleted`, recovered by zombie GC | **nothing, ever** |
| Cache warm-up after a restart | warms from history; no re-emission | cannot warm; **everything re-snapshotted** |
| Scope epoch left open by a crash | reconciled on boot | stays open; a reader sees an unmatched `Started` |
| `HistoryUnavailable` | `False` / `HistoryReadable` | **`True` / `WriterOnlySink`**, while `Ready` stays `True` |

The row that costs money is the restart row: a `Writer`-only sink re-snapshots
everything in scope after every operator restart, in full. The row that costs
trust is the offline-deletion row, because the archive cannot tell you it is
missing something — an object store with no deletions in it looks exactly like
the archive of a cluster where nothing was deleted.

That is why the limitation is reported rather than left to be inferred, and why
it is reported in two places: on the sink, and mirrored onto **every rule naming
it**, since a rule's author may never read the cluster-scoped sink they named.
With a tee, the two rules sit side by side in one namespace and disagree:

```console
$ kubectl get streamrules -n payments -o custom-columns=\
NAME:.metadata.name,SINK:.spec.sink.kind,\
HISTORY:'.status.conditions[?(@.type=="HistoryUnavailable")].reason'

NAME           SINK             HISTORY
hot-timeline   ClickHouseSink   HistoryReadable
cold-archive   S3Sink           WriterOnlySink
```

Both are `Ready=True`. `Ready` deliberately ignores `HistoryUnavailable`, because
it reports a declared capability limit and not something going wrong — the full
argument is in
[`docs/CRDS.md`](CRDS.md#historyunavailable--a-limit-not-a-fault).

**The practical rule: the timeline is authoritative for what happened; the
archive is authoritative for what a record said.** Do not answer "was this
deleted?" from the archive, and do not plan to keep the timeline for seven years.

## When to reach for it

**Compliance retention.** The common shape: 90 days of queryable history and
seven years of immutable archive. ClickHouse is where an auditor's question gets
answered in a second; the bucket, with Object Lock enabled at creation and a
lifecycle policy underneath, is where the seven-year obligation is actually met —
and it is a claim ClickHouse structurally cannot make. An append-only table is
append-only only by convention: anyone who can reach the database can `ALTER
TABLE … DELETE` a row out of it, and nothing in the row says it happened. Object
Lock is enforced by the store instead, against everyone — which is the whole
point, and also the trade you are making in the same breath. **Redaction is
forward-only** ([`docs/SCHEMA.md`](SCHEMA.md#what-redaction-is-not)), so a value
already archived under `COMPLIANCE`-mode Object Lock cannot be scrubbed *or*
deleted before its retention expires.

**Disaster recovery.** The archive is the only copy that survives losing the
ClickHouse cluster, and it survives it without a backup schedule: the objects
were written once, are immutable, and are keyed by content hash, so a retried PUT
overwrites itself and can never duplicate. Rebuilding a queryable timeline from
it is a bulk load, not a restore — the objects are line-delimited records of the
same logical shape every backend stores (D9), so nothing is lost, but there is no
supported tool that does the load for you. Treat this as "the data survived", not
"the service survives".

**Data-lake integration.** `format=jsonl-v1/cluster_id=…/date=…/hour=…` is a
Hive-style layout on purpose (D15), so the bucket is directly readable by DuckDB,
Athena, Spark, Trino and anything else that globs partitions — no export job, no
ETL, no second copy. Joining cluster state against billing, CI or incident data
is then a query in whatever engine already holds those, rather than a pipeline
out of ClickHouse.

**Cost.** Object storage is one to two orders of magnitude cheaper per byte than
hot columnar storage, and the archive compresses well (zstd over JSONL of
normalized objects). Narrowing the hot side's retention while widening the cold
side's is usually the cheapest thing you can do to a kuberecord install.

## Authoring one

In full, and copy-pasteable. Two sinks:

```yaml
apiVersion: kuberecord.io/v1alpha1
kind: ClickHouseSink
metadata: {name: hot}
spec:
  connection:
    addr: clickhouse.kuberecord-system.svc:9000
    database: kuberecord
    username: kuberecord
    credentialsSecretRef: {name: kuberecord-clickhouse-credentials}   # operator's namespace
  writer:
    checkpointEvery: 50
  policy:
    redaction:
    - fieldPath: data.password
---
apiVersion: kuberecord.io/v1alpha1
kind: S3Sink
metadata: {name: cold}
spec:
  bucket: acme-cluster-audit
  prefix: clusters/prod-eu-west-1
  region: eu-west-1
  # Omit `credentials` entirely to authenticate from IRSA, workload identity or
  # an instance role — the preferred shape on a cloud provider, with no
  # long-lived key to leak or rotate.
  credentials:
    secretRef: {name: kuberecord-s3-credentials}                      # operator's namespace
  rotation:
    maxObjectBytes: 67108864     # 64Mi encoded; the object is the batch
    maxObjectAge: 5m
  objectLock:                    # the bucket must already have Object Lock enabled
    mode: GOVERNANCE
    retainDays: 2555             # seven years
  policy:
    redaction:                   # identical to the hot sink's, deliberately
    - fieldPath: data.password
```

And the two rules, which are the pattern:

```yaml
apiVersion: kuberecord.io/v1alpha1
kind: StreamRule
metadata: {name: hot-timeline, namespace: payments}
spec:
  sink: {kind: ClickHouseSink, name: hot}
  resources:
  - {group: apps, version: v1, kind: Deployment}
  - {group: "", version: v1, kind: ConfigMap}
---
apiVersion: kuberecord.io/v1alpha1
kind: StreamRule
metadata: {name: cold-archive, namespace: payments}
spec:
  sink: {kind: S3Sink, name: cold}     # the only line that differs
  resources:
  - {group: apps, version: v1, kind: Deployment}
  - {group: "", version: v1, kind: ConfigMap}
```

The same manifests, commented line by line and runnable against a kind cluster,
are [`examples/tee/`](../examples/tee/). Four things to get right:

**1. The `resources` lists must be identical.** Nothing enforces this — two rules
are two rules — so a drifted list means the archive holds a different set of
objects than the timeline, and nobody finds out until someone looks for a record
that was never written. Keep the two blocks adjacent in one file, and diff them.

**2. The redaction floors must be identical.** Both `spec.policy.redaction` on
the sinks and `spec.extraRedaction` on the rules. Redaction happens before
hashing, so a floor that is set on the hot sink and missing from the cold one
puts the cleartext value in the bucket — where, being forward-only, it cannot be
scrubbed later, and under Object Lock cannot be deleted either. Choosing an
archive tier must never be a way around a sink's redaction floor.

**3. The `allowedGVKs` policies must agree.** A kind admitted by one sink and
refused by the other gives you `PolicyAllowed=False` on one rule and a silently
one-sided tee.

**4. Name the sinks for what they are.** A name is only unique *within* a kind, so
a `ClickHouseSink` named `default` and an `S3Sink` named `default` are two
unrelated backends and both rules would read as `sink: {name: default}`. `hot` and
`cold` — or `timeline` and `archive` — make a rule's target legible without a
second lookup.

Both rules above are namespaced `StreamRule`s, which is the delegable shape: a
team can tee its own workloads without cluster-level privileges. For a fleet-wide
compliance archive, write the same two rules as `ClusterStreamRule`s with a
`namespaceSelector`; the pattern is identical and only the type changes. A common
variant is asymmetric on purpose — a cluster-scoped hot rule over everything, and
a namespaced cold rule over the regulated namespaces alone — which is legitimate,
and the one case where the "identical `resources`" advice above does not apply.
Write down why, next to the rules.

## What a tee does not do

- **It is not a migration path.** `spec.sink` is immutable, because re-pointing a
  live rule would strand the dedup and diff baselines the pipeline built for
  every object in scope: records would either re-emit as duplicates or be written
  as diffs against a baseline the new sink never received. Moving a rule between
  backends is delete and recreate, which re-warms from the new sink's own
  history.
- **It is not replication.** The two backends receive the same events and store
  different things; neither is a copy of the other, and no consistency is
  guaranteed *between* them. They can and do diverge in the ways the table above
  describes.
- **It does not double-charge the API server**, and it is worth saying twice,
  because that is usually the fear that stops people: one informer, one watch.
- **It is not limited to two.** Three rules over the same resources naming three
  sinks work exactly the same way. The informer is still one.

## See also

- [`examples/tee/`](../examples/tee/) — this page, runnable.
- [`docs/CRDS.md`](CRDS.md#historyunavailable--a-limit-not-a-fault) — the
  `HistoryUnavailable` condition and the D12 argument in full.
- [`docs/SCHEMA.md`](SCHEMA.md) — the `event_type` state machine, so `Snapshot`
  and `Added` mean something precise when you compare the two backends.
- [`docs/QUERIES.md`](QUERIES.md) — the ClickHouse query library, which is what
  the hot side exists to serve.
- [`docs/OPERATING.md`](OPERATING.md) — the per-sink metrics, including
  `kuberecord_safe_mode`, which is pinned at `1` for every scope on a
  `Writer`-only sink.
