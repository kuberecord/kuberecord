# kubestream ClickHouse Schema (v1)

This document is the authoritative reference for the kubestream ClickHouse
schema. The DDL is shipped in-repo under
[`deploy/clickhouse/schema/`](../deploy/clickhouse/schema/):

- [`001_resource_states.sql`](../deploy/clickhouse/schema/001_resource_states.sql) — the per-object change stream.
- [`002_watch_scopes.sql`](../deploy/clickhouse/schema/002_watch_scopes.sql) — the watch-scope epoch log.

> **Schema v1 is frozen.** Because kubestream had no users to keep compatibility
> with, the schema got exactly one free redesign window, and that window is now
> **closed**: as of Task 2.6 these two tables are a **public API** with an
> additive-only change policy. Every column below is deliberate, and none of them
> will change meaning, name, or type under `v1`.
> See [Stability & Versioning](#stability--versioning) for what that guarantees
> you and what it costs us.

## Stability & Versioning

**Schema version: `v1` — frozen.** Consumers may build queries, dashboards, and
downstream pipelines against everything in this document and expect it to keep
working for the life of `v1`.

> One property to read before building an incremental pipeline: `ts` is **event
> time, not ingestion time**, so rows can be inserted out of `ts` order and a
> `WHERE ts > :watermark` cursor can skip them permanently. See [`ts`
> ordering](#ts-ordering-event-time-not-ingestion-time) for the case that produces
> such a row and the three ways to consume around it. Snapshot and reconstruction
> reads are unaffected.

### What the freeze covers

- The **table names** `resource_states` and `watch_scopes`.
- The **name, ClickHouse type, and documented semantics** of every column in the
  two tables above. A column is never renamed, retyped, repurposed, or dropped
  under `v1`.
- The **engine and key layout** — `ReplacingMergeTree` and the `ORDER BY` tuple
  on `resource_states`, `MergeTree` and its `ORDER BY` on `watch_scopes`.
  The sort key is not cosmetic: at-least-once re-inserts collapse *because* a
  byte-identical row collides on the full `ORDER BY` key (see [Delivery
  semantics](#delivery-semantics)). Changing it would silently turn de-duplicated
  rows into duplicates, so it is as frozen as the columns.
- The **deletion contract**: `event_type` alone carries deletion semantics, and
  no data column will ever regain a magic-string sentinel.

### What the freeze deliberately does *not* cover

- **The set of `event_type` values is open.** `Added | Modified | Deleted |
  Snapshot | Checkpoint` are the values `v1` emits today; a future minor release
  may add one. Treat `event_type` as an open enum: a consumer that must be
  exhaustive should branch on the values it knows and pass others through, not
  fail on them. Existing values never change meaning.
- **The shape of the JSON inside `data` and `diff`.** That is the Kubernetes
  object as the API server served it (normalized, and optionally
  [redacted](#redaction)). kubestream does not own it and cannot freeze it.
- **Anything a deployment adds on top** — TTL clauses, extra indices,
  projections, materialized views, the database name. All of it is yours; the
  operator neither sets nor inspects it.

### Additive-only change policy

Any `v1` schema change must be **purely additive**, and must satisfy all of:

1. It adds a **new column** — never a modification or removal of an existing one.
2. The new column is `Nullable(...)` or carries a `DEFAULT`, so rows written by
   an operator that predates it are still valid rows.
3. It is appended **after** the existing columns and appears in **neither**
   `ORDER BY` nor `PARTITION BY`. Touching either would rewrite the dedup
   contract above, which makes the change breaking no matter how it is spelled.
4. It ships as a new numbered DDL file in
   [`deploy/clickhouse/schema/`](../deploy/clickhouse/schema/) using
   `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, so the auto-create path stays
   idempotent and an existing table is migrated in place.
5. It is documented in the column tables above in the same change.

**One such change is designed but deliberately not taken: `ingested_at
DateTime64(9) DEFAULT now64(9)`.** Because `ts` is event time, it does not
increase monotonically with insertion (see [`ts`
ordering](#ts-ordering-event-time-not-ingestion-time)); a server-defaulted
`ingested_at` would, and would hand incremental consumers a cursor that cannot
skip a late-arriving row. It satisfies every rule above — appended, `DEFAULT`-ed,
outside `ORDER BY` and `PARTITION BY` — so it remains available at any time
without a freeze violation, and this paragraph exists so a consumer who needs it
knows the door is open.

It is not being added now for two reasons: no consumer has asked for it, and an
unused column costs storage on every row of an append-only audit table forever.
The lookback and re-reconciliation patterns above solve the same problem for free
on the read side. Spending the freeze's additive budget speculatively is the one
thing a frozen schema cannot undo.

This is what makes both directions of operator/table version skew safe:

| Skew | Behavior |
|---|---|
| **Table newer than operator** (column added, operator not yet upgraded) | Fully tolerated, no degradation. Validation checks that every *required* column is present and correctly typed; it never enumerates the observed set, so unknown columns are ignored. The write path names its columns explicitly in the `INSERT`, so the unknown column simply takes its `DEFAULT`; the read paths `SELECT` named columns and never `SELECT *`. |
| **Operator newer than table** (operator requires a column the table lacks) | Degrades that sink alone: `SchemaValid=False` on the `ClickHouseSink`, naming the missing table/column. The process does not crash-loop and other sinks are unaffected. Apply the new DDL file to resolve it. |

The first row is a **tested contract**, not an implementation accident:
`TestValidateSchema` covers it against a fake `system.columns`, and
`TestSchemaForwardCompatibilityIntegration`
(`internal/sink/clickhouse/schema_integration_test.go`) proves it end-to-end
against a live ClickHouse — it adds columns to both tables, then asserts
validation passes *and* a real insert and warm-up read still round-trip.

### Breaking changes

A change that cannot be expressed additively — a retyped column, a different
sort key, a split table — does **not** happen to `v1`. It ships as a **new major
table version** (`resource_states_v2`) created alongside the existing table, with
a documented `INSERT INTO ... SELECT` migration and a release note. Both tables
are readable during the transition, and the operator writing `v2` is a new major
of the operator. An in-place incompatible `ALTER` is never performed by
kubestream and is never asked of an operator, because it would strand every query
already written against `v1`.

### Phase 3 fit check (the freeze gate)

Freezing was gated on confirming that the already-designed Phase 3 features fit
`v1` as it stands. Both do, so `v1` is frozen with no columns added:

- **Kubernetes Events ingestion (Task 3.1)** — an Event is structurally just
  another GVK. It is identified by the ordinary `api_group`/`kind`/`namespace`/
  `name` tuple, its full normalized JSON (including `count`, `involvedObject`,
  `reason`, `type`, `message`) lands in `data`, and `labels`/`actors` populate as
  they do for any object. An API server bumping `count` changes the normalized
  content, so it is an ordinary content change and produces a `Modified` row —
  full-`data`, empty-`diff`, since Events skip diffing. Everything else that
  differs about Events (suppressing `Deleted` emission, skipping the GC pass,
  never `Snapshot`-tagging) is *pipeline* behavior keyed on the GVK, and pipeline
  behavior needs no column. Scope rows are unchanged.
- **Redaction (Task 3.3)** — redaction scrubs values *after* normalization and
  *before* hashing, so it changes the bytes stored in `data` and `diff` and the
  value of `sha256`. Those are three existing columns holding the same kinds of
  values they always held; the redaction sentinel is self-describing where it
  appears. The one addition worth considering — per-row provenance of *which*
  policy was applied — was rejected: policy identity is configuration state that
  belongs on the `ClickHouseSink` CR and its status, not duplicated onto every
  row, where it would inflate every insert to answer a question `kubectl` answers
  exactly. If that judgement is ever reversed, it is a nullable additive column,
  which the policy above permits without a freeze violation.

Both have since shipped, and both shipped with **no columns added** — see
[Kubernetes Events](#kubernetes-events) and [Redaction](#redaction).

## Applying the schema

Either apply the two `.sql` files yourself (e.g. `clickhouse-client --multiquery
< 001_resource_states.sql`), or let the operator create them idempotently by
starting it with `--ch-auto-create-schema=true` (env twin
`CH_AUTO_CREATE_SCHEMA`). Auto-create is **off by default** — the operator never
mutates ClickHouse DDL unless explicitly asked — and it is an operator-level
setting rather than a per-sink field: whether this operator may run DDL at all is
a deployment-time decision, not one the author of a `ClickHouseSink` grants
themselves. When it is on, each sink instance applies the DDL in the background
before its first write, retrying an unreachable ClickHouse rather than blocking
startup.

Regardless of how the tables are created, every sink's health probe introspects
`system.columns` for both tables and verifies every required column name and
type. The check is **required-column, not exhaustive**: a column the operator
does not know about is ignored rather than treated as drift, which is what makes
the additive-only policy in [Stability & Versioning](#stability--versioning)
safe. A mismatch is reported as `SchemaValid=False` on that `ClickHouseSink`,
with the offending table/column in the condition message. It degrades that sink
alone: the process does not crash-loop, its readiness probe stays healthy (a
readiness flip would take every *other* sink out of service too), and the rules
streaming to the sink report `Ready=False/SinkNotReady` while keeping their
watches — see [status conditions](CRDS.md#status-conditions).

---

## `resource_states`

One row per observed state transition of a watched object.

| Column | Type | Semantics |
|---|---|---|
| `ts` | `DateTime64(9, 'UTC')` | Event timestamp (nanosecond precision, UTC). `Delta, ZSTD(1)` codec — monotonic-ish timestamps compress extremely well under delta coding. (Event time, **not** insert time: rows can arrive out of `ts` order, which matters if you tail this table — see [`ts` ordering](#ts-ordering-event-time-not-ingestion-time).) |
| `cluster_id` | `LowCardinality(String)` | Identifies the cluster this operator instance serves. Explicit in the schema (a future multi-cluster reader distinguishes rows by it); implicit in-process (one operator serves one cluster). |
| `event_type` | `LowCardinality(String)` | The state-machine label — see [Event-type state machine](#event-type-state-machine). |
| `api_group` | `LowCardinality(String)` | API group (e.g. `apps`; empty `""` for the core group). Part of the canonical identity. |
| `api_version` | `LowCardinality(String)` | API version observed (e.g. `v1`). Recorded for provenance but **not** part of identity — see [Identity is version-agnostic](#identity-is-version-agnostic). |
| `kind` | `LowCardinality(String)` | Object kind (e.g. `Deployment`). Part of the canonical identity. |
| `namespace` | `String` | Object namespace; `""` for cluster-scoped objects. Part of the canonical identity. |
| `name` | `String` | Object name. Part of the canonical identity. |
| `uid` | `String` | Kubernetes UID of the object incarnation. Distinguishes a delete-and-recreate of the same name (a "reincarnation") from a plain update. |
| `resource_version` | `String` | The object's `metadata.resourceVersion` at observation time. |
| `labels` | `Map(LowCardinality(String), String)` | The object's labels at observation time. |
| `actors` | `Array(LowCardinality(String))` | Field-manager names harvested from `metadata.managedFields` — the cheapest "who probably changed this" signal. De-duplicated and sorted; empty manager names are recorded as `unknown`. **Always empty (`[]`) on `Deleted` rows** — there is no live object left to inspect, so a deletion's authorship is intentionally not attributed. |
| `data` | `String` (`ZSTD(3)`) | Full normalized JSON of the object. Populated on `Added`, `Snapshot`, `Checkpoint`, and any `Modified` that fell back to full state; **empty** otherwise. |
| `diff` | `String` (`ZSTD(3)`) | RFC 6902 JSON Patch describing the change. Populated on `Modified` and `Checkpoint` (a `Checkpoint` carries both, and its `data` is the state *after* the diff); **empty** otherwise. See [Diff format](#diff-format) and [Checkpoint rows](#checkpoint-rows). |
| `sha256` | `String` | Hex SHA-256 of the normalized JSON, used for dedup/version-gating. **Empty on `Deleted`.** |

**Engine & layout:**

```sql
-- ReplacingMergeTree (not plain MergeTree): the operator's write path is
-- at-least-once. A lost acknowledgement after a successful server-side insert
-- makes the poison-isolation path re-insert a byte-identical row (same ts —
-- frozen once when the event was processed — plus identical sha256, uid,
-- event_type, data,
-- and diff). Such a re-insert collides on the full ORDER BY key, so
-- ReplacingMergeTree collapses it to a single row on merge. A genuinely-distinct
-- event never collides: ts is DateTime64(9) (nanosecond) and frozen per event,
-- so the ORDER BY tuple alone distinguishes real re-inserts from real events.
-- Readers needing exact counts before a merge must use FINAL (or an equivalent
-- argMax / LIMIT 1 BY dedup) — see docs/SCHEMA.md "Delivery semantics".
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (cluster_id, api_group, kind, namespace, name, ts)
```

The sort key leads with the identity tuple and ends with `ts`, so all history
for a single object is physically contiguous and time-ordered — the exact
access pattern of both the warm-up query and typical audit lookups. Monthly
partitioning keeps merges and TTL drops cheap at scale.

### Delivery semantics

The operator's write path is **at-least-once**, not exactly-once, at the row
level. `driver.Batch.Send()` is a network operation with three outcomes: nothing
inserted; everything inserted but the acknowledgement lost (timeout or connection
reset mid-response); or a partial insert. In the lost-ack and partial cases the
call returns an error while the rows are already durable, so the writer's
poison-isolation path re-inserts them — as a one-row prepared batch, through the
same encoder the original batch used.

A re-inserted row is **byte-identical** to the original: `ts` is stamped once
when the event is processed and bound unchanged into every attempt, so the
re-insert carries the same `ts`, `sha256`, `uid`, `event_type`, `data`, and
`diff`. Every column of the `ORDER BY` tuple is therefore identical, and
`resource_states` (`ReplacingMergeTree`) **collapses the duplicate to a single row
on merge**. The writer's per-job commit callback remains **exactly-once**
regardless — a lost ack never causes a job to be counted or committed twice, only
a physical row to be re-sent.

> **`ts` is an instant, not a wall-clock string.** Both operator tables bind
> timestamps as instants (Go `time.Time` in UTC), never as rendered
> `YYYY-MM-DD hh:mm:ss.fraction` text. The ClickHouse driver parses such text
> client-side and reinterprets it in the *process's* local zone, which shifted
> every row by the operator pod's UTC offset and — because the isolation path's
> re-insert was rendered by a different encoder — broke the byte-identical
> property above for any operator not running in UTC. Timestamps written by
> kubestream are now independent of `TZ`.

Because `ReplacingMergeTree` de-duplicates only on background merge, a naive
`SELECT *` can transiently observe a duplicate before the merge runs. **Any read
that must not double-count must use `FINAL`** (or an explicit `argMax` / `LIMIT 1
BY` dedup). The operator's own warm-up read (`statereader.go`) is already
dedup-safe without `FINAL`: it `GROUP BY (namespace, name, uid)` with
`argMax(…, ts)`, so it emits exactly one row per *incarnation* regardless of
unmerged duplicates, and argMax over byte-identical duplicates returns the same
value either way.

The grouping is per incarnation rather than per identity on purpose. An identity
whose history holds an incarnation that is not the newest **and** has no `Deleted`
row of its own is a death nobody recorded — a delete-and-recreate that happened
while the operator was down — and grouping per identity would hide it behind the
successor's `argMax(uid, ts)`. The warm-up seeds the newest incarnation and closes
out the others (see "Deletion semantics" below).

#### `ts` ordering: event time, not ingestion time

**`ts` is the instant the event occurred, not the instant the row was inserted.**
For an ordinary write the two are the same to within the pipeline's own latency:
the timestamp is stamped when the event is processed and the row follows within
milliseconds. For *recovered* rows they differ, deliberately — and that difference
is a property of the frozen contract, not an implementation detail that might go
away.

**Rows may therefore be inserted carrying a `ts` earlier than rows already
present in the table.** The concrete case is the one [Deletion
semantics](#deletion-semantics--no-sentinels) describes: a close-out `Deleted` row
for an incarnation whose death went unrecorded, dated from **that incarnation's
own last recorded `ts`** rather than from the moment of recovery. Both reasons for
that dating are load-bearing — it keeps a reconstruction in the order events
actually happened, and it is what makes a re-emitted close-out byte-identical and
therefore collapsible by `ReplacingMergeTree`.

The lag between such a row's `ts` and its insertion is **unbounded in principle**.
It is however long the operator was down or otherwise failed to observe the death,
*plus* however long the object had been stable before that: the close-out is dated
from the incarnation's last recorded event, so an object that sat unchanged for six
months before being deleted during an outage produces a close-out dated six months
back, inserted today.

**What this breaks.** Incremental tailing with a high-water-mark cursor —

```sql
-- Do not tail the table this way.
SELECT … FROM resource_states WHERE ts > {watermark:DateTime64(9, 'UTC')};
```

— will **skip such a row permanently**. Once the successor's rows land and advance
the cursor past their (later) `ts`, the close-out at the earlier `ts` arrives
behind the watermark and is never selected again. That is precisely the row the
close-out mechanism exists to produce, so a naive tail loses exactly the evidence
it would most want.

**What to do instead**, as options rather than a mandate — pick whichever fits how
much staleness the consumer tolerates:

- **Apply a lookback (grace) window to the watermark.** Advance the cursor to
  `max(ts) - lookback` rather than to `max(ts)`, sized to the operator's expected
  downtime. Cheap, and sufficient when downtime is bounded by an SLO. Rows are
  re-read within the window, so the consumer must be idempotent on
  `(identity, uid, ts, event_type)` — which it should be anyway, since the write
  path is at-least-once.
- **Periodically re-reconcile a window rather than tailing it once.** Keep the
  tail for latency and run a slower sweep that re-reads, say, the last 24 hours (or
  the whole retention window) and reconciles what it finds. This catches
  arbitrarily late arrivals without paying for a large lookback on every poll.
- **Read snapshots with `FINAL` instead of tailing at all.** A consumer that wants
  "what is the current state of everything" is better served by the aggregate
  recipes in this document than by a change feed; they are order-insensitive by
  construction (below).

**What this does *not* affect.** The warning is narrow, and worth scoping
explicitly so it does not read as a general caveat on the table:

- **Point-in-time reconstruction** ([Reconstructing state at an
  instant](#reconstructing-state-at-an-instant)) reads an object's whole history up
  to the target instant and orders it by `ts`. A late arrival is simply present or
  absent from that read; it is never skipped by a cursor, because there is no
  cursor.
- **The `argMax` recipes in this document** aggregate over full history and are
  order-insensitive by construction.
- **The operator's own reads.** This was verified, not assumed:
  `lastKnownStatesQuery` and `activeScopesQuery` (`statereader.go`) are full-history
  aggregates — `GROUP BY` plus `argMax`/`max`, with no `ts` cursor anywhere — so an
  out-of-order insert cannot hide a row from either. `scopeWasActiveQuery` does
  carry a `ts < ?` cutoff, but it reads `watch_scopes`, whose rows are always
  stamped at the moment of the transition and are **never** history-dated (see
  `watch_scopes`' `ts` column below). Warm-up and boot reconciliation are therefore
  unaffected.

### Deletion semantics — no sentinels

Schema v1 removes the pre-v1 magic-string sentinels that the `sha256` and
`data` columns used to carry for deletions. **`event_type` alone carries
deletion semantics now.** A `Deleted` row has empty `data`, empty `diff`, empty
`sha256`, and empty `actors`. Consumers must key off `event_type = 'Deleted'`,
never off sentinel values in the data columns.

**A `Deleted` row may be dated from history.** Most are stamped when the operator
observes the disappearance, but one case cannot be: an incarnation that died while
the operator was down and was replaced under the same name before it came back.
Nothing in the live pipeline ever sees that death — the successor is observed
first — so it is recovered from the sink's own history at warm-up, and the row it
writes carries **that incarnation's own last recorded `ts` and `api_version`**,
not the instant of the recovery.

Two consequences matter to a consumer:

- **Ordering is truthful.** The close-out sorts *before* the successor's first
  row, so a reconstruction reads `Added` → … → `Deleted` (old uid) → `Added` /
  `Snapshot` (new uid) in the order the events actually happened. A now-stamp
  would sort after the successor and make the identity's most recent event a
  deletion, hiding an object that exists.
- **The row is deterministic and dedup-safe.** Every field is a function of
  history, so a re-emitted close-out (a failed write, a re-warmed scope) is
  byte-identical to the first attempt and collapses on merge like any other
  `ReplacingMergeTree` duplicate. Once it lands, that uid's own latest event is
  normally `Deleted`, so the warm-up query excludes it. It shares its `ts` with
  the event it closes, though, so `argMax(event_type, ts)` may answer with either
  of the two — a later warm can re-emit the row, and the byte-identical property
  above is what makes that a no-op rather than a duplicate.

  A consumer reconstructing one incarnation should therefore treat a `Deleted`
  row as terminal for that `uid` regardless of ties, rather than ordering on `ts`
  alone.

---

## `watch_scopes`

The watch-scope epoch log. With dynamic rules, "we stopped watching X" and "X was
deleted" are different truths and must be recorded differently, or the audit trail
lies. A `Stopped` row means the operator stopped observing a scope; it is *not* a
statement that the objects in that scope were deleted.

| Column | Type | Semantics |
|---|---|---|
| `ts` | `DateTime64(9, 'UTC')` | Transition timestamp (`Delta, ZSTD(1)` codec). Stamped when the transition was observed, never when the row was written — a retried write records when the watch really started or stopped. |
| `cluster_id` | `LowCardinality(String)` | Cluster this operator serves. |
| `api_group` | `LowCardinality(String)` | API group of the watched scope. |
| `api_version` | `LowCardinality(String)` | API version of the watch target that triggered the transition. **Provenance only** — scope identity, like object identity, is version-agnostic, and one scope can be served by informers on two versions of the same resource at once. |
| `kind` | `LowCardinality(String)` | Kind of the watched scope. |
| `namespace` | `String` | Watched namespace; `""` means cluster-scoped or all-namespaces. It is part of the scope's **identity**: a rule pinned to one namespace and a cluster-wide rule over the same kind are two scopes with two independent epochs, so `namespace = ''` must be matched exactly rather than treated as a wildcard. |
| `action` | `LowCardinality(String)` | `Started` when a `(sink, scope)` gains its first interested rule; `Stopped` when it loses its last. |
| `rule_ref` | `String` | The rule that triggered the transition, as `"<kind>/<namespace>/<name>"` — `"streamrule/demo/audit"`, or `"clusterstreamrule//platform-baseline"` for a cluster-scoped rule, which renders an empty namespace segment. The kind is part of the reference because a `StreamRule` and a `ClusterStreamRule` may share a name. Empty for a `Stopped` row written by boot reconciliation (below), where the rule that had held the scope no longer exists. |

```sql
ENGINE = MergeTree
ORDER BY (cluster_id, api_group, kind, namespace, ts)
```

### Transition semantics

Rows are **transitions of a `(sink, scope)` pair**, not of a rule and not of an
informer:

- Two rules asking for the same scope on the same sink produce **one** `Started`
  row. Removing one of them produces **no** row; removing the last produces one
  `Stopped` row.
- A rule edit that only changes a selector produces no row at all — the scope never
  stopped being watched.
- `Started` is written only once the informer serving the scope is actually
  running, so a recorded `Started` is never a promise the operator failed to keep.

Multi-rule attribution deliberately lives in the owning CR's status, not here: this
is an append-only log, and a row naming one of several contributing rules could
never be corrected.

### Restarts, and epochs left open

A process exiting writes **no** `Stopped` row. Shutdown is not a rule going away,
and recording one would put a spurious epoch boundary in the trail on every
restart. Two consequences follow:

1. A scope whose most recent action is `Started` is either being watched right now
   or was left open by a process that did not come back.
2. On startup — once the desired state has settled — the operator enumerates the
   scopes whose most recent action is `Started`, and writes one `Stopped` row for
   each that no rule wants any more (the rule was deleted while it was down). It
   writes **zero** `Deleted` rows for the objects those scopes covered.

### Reading the log

Two queries drive the operator's own decisions, and both are useful to a consumer:

```sql
-- Was this scope watched in a previous epoch, as of a given instant?
-- (Used to decide whether an object missing from the cluster but present in
-- resource_states is a genuine deletion or merely pre-history.)
SELECT argMax(action, ts) = 'Started'
FROM watch_scopes
WHERE cluster_id = ? AND api_group = ? AND kind = ? AND namespace = ? AND ts < ?;

-- Which scopes are currently open for this cluster?
SELECT api_group, kind, namespace
FROM watch_scopes
WHERE cluster_id = ?
GROUP BY api_group, kind, namespace
HAVING argMax(action, ts) = 'Started';
```

The `ts <` cutoff in the first query matters: scope rows are written
asynchronously, so a scope's *own* `Started` row may land at any moment while its
warm-up is running. Anchoring the question to the instant the current epoch began
is what keeps "was it watched **before** now?" from answering itself.

### What a `Stopped` row means for `resource_states`

Nothing was deleted. The objects in the scope keep whatever last-known state
`resource_states` holds for them, correctly dated to on or before the `Stopped`
row. A consumer reconstructing "what existed when" should treat a scope's
last-known states as *as-of its `Stopped` row*, not as current.

---

## Event-type state machine

`event_type` is one of `Added | Modified | Deleted | Snapshot | Checkpoint`.

- **`Added`** — the object was observed for the first time (a genuine cache
  miss while the cache is trusted), or a reincarnation (same name, new UID)
  supersedes a prior incarnation. Carries full `data`.
- **`Modified`** — a subsequent observation whose content hash differs from the
  last recorded state. Carries a `diff` against the prior state; falls back to
  full `data` when a diff cannot be produced (see below).
- **`Deleted`** — the object is gone (live delete, reincarnation close-out of
  the old UID, or startup GC of a "zombie"). Empty `data`/`diff`/`sha256`. A
  close-out recovered from history is dated from that incarnation's own last
  event rather than from now — see "Deletion semantics" above.
- **`Snapshot`** — a cache miss observed *while the cache has not yet been
  warmed from ClickHouse history* (startup "SafeMode"). Tagged `Snapshot`
  rather than `Added` so a slow/unavailable ClickHouse at startup can't
  masquerade as a mass duplicate-`Added` storm. Carries full `data`.
- **`Checkpoint`** — a `Modified` in every semantic sense except its
  `event_type` and its populated `data`: it carries the `diff` **and** the full
  state, so a reader replaying an object's history never has to walk back
  further than the last one. See [Checkpoint rows](#checkpoint-rows).

Typical lifecycle for one object:

```
(first seen) --> Added --> Modified --> Modified --> ... --> Deleted
                   ^                                            |
                   +------------ (reincarnation, new UID) ------+
```

At startup before warm-up completes, first sightings are `Snapshot` instead of
`Added`; once warm, normal `Added`/`Modified` resumes.

## Kubernetes Events

`v1/Event` and `events.k8s.io/v1/Event` are streamed in a **built-in Events
mode**. Name either one in a rule's `resources:` and it applies; there is no
switch, and no way to turn it off. Everything below is a property of the *kind*,
so a query does not have to know which rule produced a row — it only has to know
that `kind = 'Event'` in one of those two groups.

An Event is append-only ephemera, not durable cluster state: the API server
creates one, **updates it in place** to bump `count` when the same thing happens
again, and lets it expire after roughly an hour. Three schema-visible
consequences follow.

**Every Event row carries full `data`, and `diff` is always empty.** The
interesting row is the Event as it stood when its count changed, and a reader
should be able to take a `count`, a `message` or an `involvedObject` straight off
it. Hash dedup still runs, so a resync that re-delivers an unchanged Event writes
nothing. `Checkpoint` rows therefore never appear for Events — there is no
diff-only run for one to interrupt.

**An Event's expiry is recorded as nothing at all.** There is no `Deleted` row
for a `v1/Event` or an `events.k8s.io/v1/Event`, ever — not for TTL expiry, not
for a `kubectl delete event`, and not as a reincarnation close-out when a newer
Event takes over an older one's name. An Event vanishing is its retention window
closing, not a change to the cluster, and recording it as a deletion would put
one false deletion in the audit trail per Event the cluster ever emitted. The
practical consequence for a query: **an Event's history simply stops**. Do not
read "no `Deleted` row" as "still live" for `kind = 'Event'` the way you would
for a Deployment; read the Event's own `lastTimestamp` (or
`series.lastObservedTime`) out of `data` instead.

**`Snapshot` does not appear either.** Snapshot hedges "genuinely new, or merely
unseen by this process?", and for an Event the answer is always the former: a
first sighting is `Added` even during warm-up. Warm-up still runs for an Events
scope — it primes the dedup hashes so a restart does not re-record every Event
still inside its TTL — but it never reconciles anything *away* from history, so
it cannot manufacture the deletions the previous paragraph rules out.

`watch_scopes` is unaffected: an Events scope writes ordinary `Started` and
`Stopped` rows like any other, so "when was this cluster's Event stream being
captured?" is answerable exactly as it is for every other kind.

Two operational notes:

- **Volume.** Events are typically the highest-cardinality, highest-churn kind in
  a cluster. Enable them per namespace with a `StreamRule` before reaching for a
  cluster-wide `ClusterStreamRule`, and size retention accordingly — this is why
  the `events` watch preset does not ship enabled (see
  [`docs/RBAC.md`](RBAC.md)).
- **Warm-up cost.** Because Events never get a `Deleted` row, nothing tombstones
  them in `resource_states`, so the warm-up query for an Events scope returns
  every Event still in ClickHouse for that scope. A retention TTL on
  `resource_states` (see [Suggested TTL](#suggested-ttl-optional-non-mandatory))
  is what bounds both that query and the memory its result seeds.

Query recipes for Events — including "everything that happened to object X around
time T" — are in [`docs/QUERIES.md`](QUERIES.md).

## Redaction

Values can be scrubbed out of every object before it is stored. Redaction is
configured in two places, and the two are **additive**:

- **`ClickHouseSink.spec.policy.redaction`** — the sink owner's floor, applied to
  everything any rule streams to that sink.
- **`StreamRule.spec.extraRedaction`** / **`ClusterStreamRule.spec.extraRedaction`**
  — a rule's additions on top of that floor.

A rule can add paths; nothing can take one away. Where two rules stream the same
object to the same sink, the union of their paths applies — there is exactly one
stored payload and one hash per object per sink, so honouring less than the union
would let one rule's existence unredact another's stream.

```yaml
apiVersion: kubestream.io/v1alpha1
kind: ClickHouseSink
metadata:
  name: default
spec:
  policy:
    redaction:
    - fieldPath: data.password
---
apiVersion: kubestream.io/v1alpha1
kind: StreamRule
metadata:
  name: app-config
  namespace: demo
spec:
  resources:
  - group: ""
    version: v1
    kind: ConfigMap
  extraRedaction:
  - fieldPath: spec.template.spec.containers[*].env[*].value
  - annotation: my.company.io/api-token
```

### Where it happens, and why that is the whole point

Values are scrubbed **after normalization and before hashing**. Everything
downstream is therefore a function of the *redacted* content:

| Column | Effect |
|---|---|
| `data` | the redacted object |
| `diff` | computed between two already-redacted states, so a change to a scrubbed value produces no patch operation carrying it |
| `sha256` | the hash of the redacted object |

The consequence worth stating plainly: two states of an object that differ
**only** in a redacted value are indistinguishable to kubestream. They hash
identically, so the second one deduplicates and **no row is written at all**.
That is deliberate. A design that redacted on the way out would leak the value
through the diff, and one that hashed before redacting would leave the `sha256`
column as a stable oracle to grind guessed values against.

The flip side is equally deliberate: kubestream cannot tell you *that* a redacted
value changed. If you need change detection on a secretish field, redact its
neighbours instead of it, or stream the object to a second sink under a different
policy.

### Path syntax

The grammar is deliberately tiny, and it is not JSONPath:

| Form | Matches |
|---|---|
| `data.password` | dot-separated field names |
| `spec.containers[*].env[*].value` | `[*]` iterates every element of an array |
| `spec.args[*]` | a terminal wildcard replaces each element |
| `annotation: my.company.io/api-token` | one annotation key, whatever characters it contains |

Nothing whose match set depends on the object's contents — no filters, no
recursive descent, no indices — is accepted, so what a policy redacts is
readable off the policy. The syntax is validated at admission by the CRDs: a
malformed path is rejected by the API server, not discovered at stream time.

The `annotation:` shorthand exists because annotation keys contain dots and
slashes: `kubectl.kubernetes.io/last-applied-configuration` written as a
`fieldPath` would mean six nested maps that do not exist. Exactly one of
`fieldPath` and `annotation` is set per entry.

### What a redacted value looks like

The value is replaced by the literal string `[REDACTED]`; the structure around it
is preserved. A reader can see that the field existed and was scrubbed, rather
than being unable to tell a hidden field from an absent one. A path whose leaf is
a map or an array replaces that whole subtree with the same string, and a path
that matches nothing in a given object is a silent no-op — which is what lets one
policy apply across a mixed resource list.

### Always on: `kubectl.kubernetes.io/last-applied-configuration`

This annotation is scrubbed on **every** object, under **every** policy,
including an entirely empty one. `kubectl apply` copies the complete submitted
object into it, so it embeds a verbatim second copy of the very values a policy
removes; leaving it alone would make every other rule cosmetic. It cannot be
turned off.

### What redaction is not

- **Not a Secrets unlock.** `v1/Secret` is denied in code (D8) and no redaction
  policy admits one, however thoroughly it would scrub it.
- **Not a substitute for query-side access control.** Everything not on a path is
  stored verbatim, and the flattening caveat in [`docs/RBAC.md`](RBAC.md) still
  applies: anyone who can query ClickHouse sees every object any rule streams.
- **Not retroactive.** Rows written before a path was added keep whatever they
  recorded. Adding a path changes the object's content and therefore its hash, so
  the next event for each object writes a fresh, redacted row.

## Diff format

`Modified` rows store an **RFC 6902 JSON Patch** in the `diff` column, produced
by [`wI2L/jsondiff`](https://github.com/wI2L/jsondiff) comparing the previous
normalized JSON against the current one. Normalization strips volatile fields
(`metadata.managedFields`, `metadata.resourceVersion`, `metadata.generation`)
before hashing and diffing, so cosmetic churn does not generate rows.
[Redaction](#redaction) runs in the same pass, on both sides, so a diff can never
carry a value the policy scrubbed out of `data`.

**Graceful degradation:** if no prior JSON baseline exists, or the diff/marshal
fails, the operator writes the full current state to `data` and leaves `diff`
empty — a full-state row is always correct, merely larger than a diff.

**Kubernetes Events are never diffed** — every one of their rows is full state
by design, not by degradation. See [Kubernetes Events](#kubernetes-events).

## Checkpoint rows

Diffs make the stream cheap to write and cheap to store, but they make a *read*
unbounded: with `Modified` rows carrying diffs only, "what did this object look
like at time T?" means replaying every diff back to the object's last full row —
and for a Deployment that has been reconciled for a year, that is the whole
year. A `Checkpoint` row is a `Modified` that also carries the full state, which
caps that walk.

Two independent triggers promote a `Modified` write to a `Checkpoint`:

1. **Cadence** — every `spec.writer.checkpointEvery` consecutive diff-only
   `Modified` rows for one object (default **50**; `0` disables checkpointing
   for that sink entirely). This is what bounds replay: a reconstruction never
   applies more than `checkpointEvery` patches.
2. **Size** — a single write whose diff is *larger* than the object it
   describes. Such a diff is pure loss: more bytes than the state itself, plus a
   replay step on top. It fires regardless of how far the cadence has
   progressed — but not when checkpointing is disabled.

Both sides of the size comparison are **uncompressed** bytes of the diff and of
the freshly serialized full object the pipeline already holds at diff time, so
the check costs nothing. Neither the row's `data` *column* (empty on a
`Modified`, which would make the trigger fire on every update) nor the
operator's in-memory zstd-compressed baseline (which would over-trigger by the
compression ratio) is ever an operand.

Consequences for a consumer:

- **`Checkpoint` is not a special case for aggregate reads.** It has the same
  identity, `uid`, `sha256` and `labels` a `Modified` would have carried, so
  `argMax(…, ts)` queries — including the operator's own warm-up read — are
  unaffected. Only a *replay* treats it specially, as its base.
- **A `Checkpoint`'s own `diff` must not be re-applied on top of its `data`.**
  The two describe the same transition: `data` is the state *after* the diff.
- **The cadence is per operator process, not per object lifetime.** The counter
  behind trigger 1 lives in the operator's memory and resets on restart, which
  costs nothing: a restarting operator starts from an empty (or history-warmed)
  cache, so its first row for an object is a full-state `Added`/`Snapshot`
  anyway, and the replay window re-baselines there. Nothing durable depends on
  the counter (Invariant 6), and a reconstruction never needs to know what it
  was — it reads the rows.

### Reconstructing state at an instant

This is the official recipe. It is what
`TestCheckpointStateReconstructionIntegration`
(`internal/sink/clickhouse/checkpoint_integration_test.go`) executes and asserts
byte-for-byte against the live object, so it stays true rather than merely
documented.

**Step 1 — read the object's history up to the target instant.** `FINAL` is
required, not optional: the write path is at-least-once, and replaying one
object's patch twice is not idempotent (a `remove` op fails the second time, an
`add` duplicates).

```sql
SELECT ts, event_type, data, diff
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND api_group  = {group:String}      -- '' for the core group
  AND kind       = {kind:String}
  AND namespace  = {namespace:String}  -- '' for cluster-scoped objects
  AND name       = {name:String}
  AND ts        <= {at:DateTime64(9, 'UTC')}
ORDER BY ts;
```

**Step 2 — find the base.** Take the **last row with a non-empty `data`** (an
`Added`, a `Snapshot`, a `Checkpoint`, or a `Modified` that fell back to full
state). Its `data` is the starting document. If the result set holds no
`data`-bearing row, history for this object begins before your retention window
and the state at `T` is not reconstructible.

Everything before the base row is irrelevant — that is the bound checkpointing
buys you.

**Step 3 — replay forward.** Apply the `diff` of every row *after* the base row,
in `ts` order, as an RFC 6902 JSON Patch. Skip the base row's own `diff` (see
above). Rows with an empty `diff` and non-empty `data` are full-state rows: they
*replace* the document rather than patching it.

**Step 4 — stop at a deletion.** If a `Deleted` row appears in the range, the
object did not exist at `T` — for that `uid`. Treat a `Deleted` row as terminal
for its `uid` regardless of `ts` ties (see [Deletion
semantics](#deletion-semantics--no-sentinels)), and start again from the next
`uid`'s first row if a successor was recreated under the same name.

**Verifying a reconstruction.** The `sha256` of the row you finished on is the
hex SHA-256 of the operator's normalized JSON for that state. Canonicalize your
reconstructed document (re-serialize it with sorted object keys) and hash it:
the two must match. That check is what turns "the replay ran without errors"
into "the replay produced the right state".

If the stream is [redacted](#redaction), both the replayed document and the hash
describe the *redacted* object — consistently, since redaction happens before
hashing. Comparing a reconstruction against a **live** object therefore means
applying the same policy to the live copy first; `pipeline.ObjectHash` takes the
policy as an argument for exactly this reason.

The digest to check it against — no replay needed, which also answers "is this
object's recorded state current?" on its own:

```sql
-- Latest recorded hash and last-known event for one object.
SELECT argMax(event_type, ts) AS last_event, argMax(sha256, ts) AS sha256, max(ts) AS ts
FROM resource_states
WHERE cluster_id = {cluster:String} AND api_group = {group:String}
  AND kind = {kind:String} AND namespace = {namespace:String} AND name = {name:String};
```

## Identity is version-agnostic

> An object's identity is `(cluster_id, api_group, kind, namespace, name)` —
> **version-agnostic** (`apps/v1` and a hypothetical `apps/v2` Deployment are
> the same object).

`api_version` is recorded for provenance but is **not** part of identity. This
is why the warm-up/restore query filters on `api_group`, `kind`, and
`cluster_id` (not `api_version`), and why the sort key omits `api_version`.
Filtering on `api_group` — not `kind` alone — is what keeps two resources that
share a Kind (e.g. `batch/v1` `Job` vs. a CRD `example.com/v1` `Job`) from
cross-contaminating each other's history.

## Suggested TTL (optional, non-mandatory)

kubestream does **not** impose a retention policy — audit data is often kept
indefinitely, and retention is a deployment decision. If you do want automatic
expiry, a TTL clause can be added to `resource_states` (and/or `watch_scopes`),
for example a 1-year retention:

```sql
ALTER TABLE resource_states MODIFY TTL toDateTime(ts) + INTERVAL 1 YEAR;
```

Or bake it into the table at creation time by appending to the DDL:

```sql
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (cluster_id, api_group, kind, namespace, name, ts)
TTL toDateTime(ts) + INTERVAL 1 YEAR;
```

> **Size a TTL with history-dated rows in mind.** `PARTITION BY toYYYYMM(ts)`
> partitions on **event time**, and a close-out `Deleted` row is dated from the
> incarnation it closes — which may be months back for an object that had been
> stable a long time before it died (see [`ts`
> ordering](#ts-ordering-event-time-not-ingestion-time)). Such a row lands in an
> *older* partition than the one currently being written, so a tight TTL can expire
> it on or shortly after arrival, and the object's history then ends without its
> deletion ever being visible. A retention window comfortably longer than the
> longest plausible operator outage plus the longest plausible object lifetime is
> what keeps that from happening; the 1-year example above is sized that way.

This is a suggestion only; the operator neither sets nor requires a TTL, and
schema validation ignores TTL clauses (it checks column names/types
only).
