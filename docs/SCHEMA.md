# kubestream ClickHouse Schema (v1)

This document is the authoritative reference for the kubestream ClickHouse
schema. The DDL is shipped in-repo under
[`deploy/clickhouse/schema/`](../deploy/clickhouse/schema/):

- [`001_resource_states.sql`](../deploy/clickhouse/schema/001_resource_states.sql) — the per-object change stream.
- [`002_watch_scopes.sql`](../deploy/clickhouse/schema/002_watch_scopes.sql) — the watch-scope epoch log.

> **Schema stability.** Under [D5] the schema gets one free redesign now (this
> is it) and is then **frozen as a public API** in Task 2.6. Every column below
> is deliberate; treat additions as append-only and breaking changes as
> off-limits after the freeze.

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
type. A mismatch is reported as `SchemaValid=False` on that `ClickHouseSink`,
with the offending table/column in the condition message. It degrades that sink
alone: the process does not crash-loop, its readiness probe stays healthy (a
readiness flip would take every *other* sink out of service too), and the rules
streaming to the sink report `Ready=False/SinkNotReady` while keeping their
watches — see the README's [status conditions](../README.md#status-conditions).

---

## `resource_states`

One row per observed state transition of a watched object.

| Column | Type | Semantics |
|---|---|---|
| `ts` | `DateTime64(9, 'UTC')` | Event timestamp (nanosecond precision, UTC). `Delta, ZSTD(1)` codec — monotonic-ish timestamps compress extremely well under delta coding. |
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
| `data` | `String` (`ZSTD(3)`) | Full normalized JSON of the object. Populated on `Added`, `Snapshot`, and `Checkpoint`; **empty** otherwise. |
| `diff` | `String` (`ZSTD(3)`) | RFC 6902 JSON Patch describing the change. Populated on `Modified` (and `Checkpoint`); **empty** otherwise. See [Diff format](#diff-format). |
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
- **`Checkpoint`** — **reserved until Task 2.2**; no code emits it yet. It will
  carry either full `data` or a `diff`. The column set and validation accept it
  now so the schema need not change when it lands.

Typical lifecycle for one object:

```
(first seen) --> Added --> Modified --> Modified --> ... --> Deleted
                   ^                                            |
                   +------------ (reincarnation, new UID) ------+
```

At startup before warm-up completes, first sightings are `Snapshot` instead of
`Added`; once warm, normal `Added`/`Modified` resumes.

## Diff format

`Modified` rows store an **RFC 6902 JSON Patch** in the `diff` column, produced
by [`wI2L/jsondiff`](https://github.com/wI2L/jsondiff) comparing the previous
normalized JSON against the current one. Normalization strips volatile fields
(`metadata.managedFields`, `metadata.resourceVersion`, `metadata.generation`)
before hashing and diffing, so cosmetic churn does not generate rows.

**Graceful degradation:** if no prior JSON baseline exists, or the diff/marshal
fails, the operator writes the full current state to `data` and leaves `diff`
empty — a full-state row is always correct, merely larger than a diff.

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

This is a suggestion only; the operator neither sets nor requires a TTL, and
schema validation ignores TTL clauses (it checks column names/types
only).

[D5]: ../kubestream-backlog.md
