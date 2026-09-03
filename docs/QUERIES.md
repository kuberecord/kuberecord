# kuberecord Query Library

Runnable SQL against both of kuberecord's storage backends
([`docs/SCHEMA.md`](SCHEMA.md)). The recipes up to [Reading Event history
correctly](#reading-event-history-correctly) read a `ClickHouseSink`'s tables; the
ones under [The S3 archive](#the-s3-archive) read an `S3Sink`'s objects with
DuckDB — the same objects [the CLI reads](#what-the-cli-reads) without it.

**This page is the wide half.** Questions about *one* object — what changed on it,
what it looked like at an instant, who last wrote a field — are
[`kubectl kuberecord`](CLI.md)'s, and it answers them with no query, no client and
no database. [CLI or SQL?](#cli-or-sql) is the split, and it is a division of
labour rather than two ways of doing the same thing.

Every ClickHouse query here uses only frozen columns, so it keeps working across
operator upgrades — and that is a tested claim rather than an intention: `make
test-integration` runs on every push and executes every statement on this page
against a ClickHouse whose tables were built from the shipped DDL and nothing else,
so a query naming a column that is not frozen fails CI (`test/queries`). Every
DuckDB recipe is executed by the same job against a real object store holding a
real archive, and must return rows.

The four shipped Grafana dashboards ask these same questions with the clicking
already done; see [`docs/DASHBOARDS.md`](DASHBOARDS.md). Each section below names
the dashboard that covers it.

> **If a value reads `[REDACTED]`, that is what is stored.** Redaction happens
> before hashing, so a scrubbed value is absent from `data`, from every `diff`,
> and from the `sha256` — not hidden at query time. Two states of an object that
> differ only in a redacted value produce **one** row, not two, so a "what
> changed?" query cannot see a change confined to a redacted path. See
> [Redaction](SCHEMA.md#redaction).

## CLI or SQL?

This page and [`kubectl kuberecord`](CLI.md) answer overlapping questions, and the
overlap is deliberate rather than accidental. They are complements, and the split
between them is the same one their designs are built around:

| | The CLI | This page |
|---|---|---|
| **Shape of question** | One object. One window, or one instant. | Many objects, or many changes reduced to a number. |
| **What you get** | A rendered answer, or a versioned JSON envelope. | A result set. |
| **What it needs** | One static binary. No engine, no client, and against an archive no cluster either. | A ClickHouse client, or DuckDB, or Athena. |
| **Where it stops** | It is not a query engine. It cannot group, join or aggregate, and it does not pretend to. | It cannot resolve an incarnation, replay a bounded window, or tell you a scope was never watched — not without you writing all three, correctly, every time. |

**Reach for the CLI for the single-object questions**, because the parts that are
easy to get wrong are already in it: incarnation resolution before filtering
a replay that starts from a full state rather than from the window's edge, prior
values recovered rather than left blank, and an empty answer explained against
the watch scopes instead of presented as silence.

| The question | The command | The SQL here |
|---|---|---|
| What changed on this object, and who changed it? | `kuberecord timeline deploy/x -n ns` | [Incident window](#incident-window--everything-that-changed-in-a-namespace), narrowed by name |
| What exactly moved — old value beside new? | `kuberecord diff deploy/x -n ns --since 2h` | — the diffs are inlined, but reconstructing the *old* side is a replay |
| What did it look like at 14:05? | `kuberecord get deploy/x -n ns --at 2026-08-28T14:05:00Z` | [Reconstructing an object's state at an instant](#reconstructing-an-objects-state-at-an-instant) |
| Who last wrote each field? | `kuberecord blame deploy/x -n ns` | — no single-statement equivalent |
| What did this deleted object contain? | `kuberecord timeline deploy/x -n ns --since 30d` | [What did this deleted object look like?](#what-did-this-deleted-object-look-like) |
| What was Kubernetes saying about it? | `kuberecord timeline pod/x -n ns --with-events` | [Events for object X around time T](#events-for-object-x-around-time-t) |
| Was anybody watching this scope? | `kuberecord scopes -n ns` | [Was anybody watching?](#was-anybody-watching) |

**Reach for the SQL for everything wider than one object.** Drift across a fleet,
flap counts, activity by namespace, "which objects cover this window", anything
grouped or joined — none of it is a shape the CLI has, and putting it there would
be building a worse query engine next to the one you already have. The
sections below are that half, and every statement in them is executed against a
real backend in CI.

Both read the same frozen contract, so the two transfer: the CLI's `-o json`
envelope spells its item fields with **the schema's own column names**, which
is what lets a `jq` recipe written against a SQL result work unchanged against CLI
output. Where the CLI's answer and a query here disagree, that is a bug in one of
them rather than a difference of opinion.

## Parameters

Every query is written with ClickHouse-native query parameters, so it can be run
without editing:

```console
$ clickhouse-client \
    --param_cluster=prod-eu-1 --param_namespace=payments \
    --param_from='2026-08-01 13:45:00.000' --param_to='2026-08-01 14:30:00.000' \
    --queries-file incident.sql
```

One vocabulary is used throughout, so parameters can be set once and reused
across queries:

| Parameter | Meaning |
|---|---|
| `cluster` | `cluster_id`. Always required — see below. |
| `group` | API group, `''` for the core group (`apps`, `''`, `rbac.authorization.k8s.io`). |
| `kind` | Kind, unqualified (`Deployment`). |
| `namespace` | Namespace, `''` for cluster-scoped objects. |
| `name` | Object name. |
| `uid` | Object UID, when one incarnation must be told from another. |
| `manager` | A field-manager name, normally your GitOps controller's. |
| `threshold` | How many changes make an object a flapper. |
| `from`, `to`, `at`, `t` | Time bounds, as `DateTime64(3, 'UTC')`. |

Three conventions apply throughout:

- **Always filter on `cluster_id`.** It is the leading column of the sort key, so
  omitting it turns every query into a full scan *and* silently mixes clusters.
- **`FINAL` where exactness matters.** `resource_states` is a
  `ReplacingMergeTree` and the write path is at-least-once, so an unmerged
  duplicate is possible. `FINAL` costs a merge at read time and is worth it for
  counts; a query that only inspects the newest rows can skip it.
- **Times are bound as UTC.** The parameters are declared `DateTime64(3, 'UTC')`
  rather than plain `DateTime64(3)` so a bound literal means the same instant
  regardless of the server's timezone. `ts` itself is stored as
  `DateTime64(9, 'UTC')`.

One caveat worth reading before building anything on top of these:
`ts` is the instant an event *occurred*, not the instant its row was inserted, and
a recovered row can arrive with a `ts` older than rows already present. Nothing on
this page is affected — every query here aggregates or filters a bounded window
rather than tailing a cursor — but an incremental pipeline built with
`WHERE ts > :watermark` is. See [`ts` ordering: event time, not ingestion
time](SCHEMA.md#ts-ordering-event-time-not-ingestion-time).

## Incident window — everything that changed in a namespace

The first question of every post-mortem: *"the payments namespace broke between
13:45 and 14:30 — what moved?"* Diffs are inlined, so this is the whole answer in
one result set rather than a list of things to go and look up.

> **This one stays SQL.** It is a whole namespace over a window, which is more
> than one object, and the CLI has no shape for it. Once the result has named a
> suspect, `kuberecord timeline <kind>/<name> -n <ns> --since …` is where the
> old-value-beside-new detail lives.

Dashboard: **Namespace Activity** for the shape of the window, **Object Timeline**
once you know which object to read.

```sql
SELECT
    ts,
    event_type,
    kind,
    name,
    resource_version,
    arrayStringConcat(arraySort(actors), ', ') AS field_managers,
    -- The change itself. A Modified row carries an RFC 6902 patch in `diff`;
    -- Added, Snapshot and Checkpoint rows carry the whole object in `data`
    -- instead, which is summarised here rather than printed — select `data`
    -- directly for the one row you actually want to read. A Deleted row carries
    -- neither, by design.
    multiIf(
        diff != '', diff,
        data != '', concat('(full state, ', toString(length(data)), ' bytes)'),
        ''
    ) AS change
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND namespace = {namespace:String}
  AND ts BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}
ORDER BY ts ASC;
```

A window that comes back empty is itself information, but read it carefully: it
means nothing *changed*, not that nothing was *watched*. Whether a scope was live
at the time is recorded separately, in `watch_scopes` — see [Reading the
log](SCHEMA.md#reading-the-log).

## Drift your GitOps controller did not cause

> **This one stays SQL** — it is a fleet question, grouped and counted. For one
> object's version of it, `kuberecord timeline <kind>/<name> --exclude-actor
> argocd-application-controller` applies the same predicate to one timeline.

*"What changed in this cluster that Git does not know about?"* Every row carries
the field managers that owned parts of the object at write time, so excluding the
GitOps controller's manager name leaves the changes it did not make.

Dashboard: **Drift by Actor**.

```sql
SELECT
    namespace,
    kind,
    name,
    count()  AS modifications,
    max(ts)  AS last_change,
    -- Every manager seen on those rows. groupUniqArrayArray flattens the per-row
    -- `actors` arrays into one distinct set; two or more names here means the
    -- object has several owners, which is the normal case and not yet a finding.
    arrayStringConcat(arraySort(groupUniqArrayArray(actors)), ', ') AS field_managers
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND ts BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}
  -- Checkpoint is a Modified that also carries full state, so it counts as a
  -- change; omitting it would undercount exactly the objects that change most.
  AND event_type IN ('Modified', 'Checkpoint')
  -- The exclusion. `manager` is the name your GitOps controller applies under,
  -- exactly as it appears in metadata.managedFields — Argo CD uses
  -- argocd-controller, Flux the name of the applying kustomize-controller.
  AND NOT has(actors, {manager:String})
GROUP BY namespace, kind, name
ORDER BY modifications DESC
LIMIT 100;
```

Two honest limits. `actors` is harvested from `metadata.managedFields`, which
records *ownership*, not authorship of a particular edit — a row is credited to
every manager holding a field at the time, so an object your controller also
manages will list it alongside whoever else touched it. And a manager name is
self-declared: `kubectl` clients pick their own, so a human edit may arrive as
`kubectl-edit`, `kubectl-client-side-apply`, or anything a script set.

## Top flappers

> **This one stays SQL.** `flapping` is deliberately out of scope for the CLI in
> v0.3.0: it is an aggregate over many objects, which is exactly what this page is
> for.

*"What in this cluster will not hold still?"* Flapping is usually two controllers
fighting over one field, and it is expensive twice: apiserver load, and rows here.

Dashboard: **Flap Report**.

```sql
SELECT
    namespace,
    kind,
    name,
    count() AS modifications,
    -- The fight, if there is one: the distinct managers writing to this object.
    arrayStringConcat(arraySort(groupUniqArrayArray(actors)), ', ') AS field_managers,
    min(ts) AS first_change,
    max(ts) AS last_change
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND ts BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}
  AND event_type IN ('Modified', 'Checkpoint')
GROUP BY namespace, kind, name
HAVING modifications >= {threshold:UInt32}
ORDER BY modifications DESC
LIMIT 50;
```

Pick `threshold` against the width of the window, not in the abstract: ten changes
in an hour is a fight, ten in a week is a deployment cadence. Status-heavy kinds
flap by nature — add `AND kind NOT IN ('Event', 'Lease', 'EndpointSlice')` before
concluding anything about the top of the list.

## Reconstructing an object's state at an instant

> **The CLI answers this**, on either backend, in one command:
>
> ```sh
> kuberecord get deploy/checkout -n payments --at 2026-08-28T14:05:00Z
> ```
>
> It does the replay below, prints the base row and the patch count in the header
> so the answer can be judged rather than trusted, and `--verify` re-hashes the
> result against the recorded digest. It also refuses to blend two incarnations of
> a reused name, which the SQL here will happily do if you forget `uid`. Use the
> SQL when you need the replay inside something else — a pipeline, a notebook, a
> report over many objects. See [`docs/CLI.md`](CLI.md#get---at).

*"What did this object actually look like at 14:05?"* `Modified` rows carry diffs
rather than full state, so the answer is a replay: take the newest row that
carries full state at or before the instant, then apply the patches after it.
`Checkpoint` rows are what keep that replay bounded.

Dashboard: none — reconstruction is a procedure, not a panel. The full recipe,
including how to verify the result against the recorded `sha256`, is
[Reconstructing state at an instant](SCHEMA.md#reconstructing-state-at-an-instant)
in the schema document; the SQL is here.

**Step 1 — read the history up to the instant.** `FINAL` is required, not
optional: replaying one object's patch twice is not idempotent, so an unmerged
duplicate row corrupts the result rather than merely slowing it down.

```sql
SELECT
    ts,
    event_type,
    uid,
    -- Non-empty on Added, Snapshot and Checkpoint: the full object.
    data,
    -- Non-empty on Modified and Checkpoint: the RFC 6902 patch.
    diff,
    sha256
FROM resource_states FINAL
WHERE cluster_id  = {cluster:String}
  AND api_group   = {group:String}      -- '' for the core group
  AND kind        = {kind:String}
  AND namespace   = {namespace:String}  -- '' for cluster-scoped objects
  AND name        = {name:String}
  AND ts         <= {at:DateTime64(3, 'UTC')}
ORDER BY ts ASC;
```

Then, in the client: take the **last** row with non-empty `data` as the base,
apply the `diff` of every row *after* it in `ts` order (skipping the base row's
own `diff`), and stop at a `Deleted` row — beyond one, the object did not exist
under that `uid`.

**Step 2 — check the replay.** The `sha256` of the row you finished on is the hex
SHA-256 of the operator's normalized JSON for that state. Re-serialize your
reconstruction with sorted keys, hash it, and compare. On a redacted stream both
sides describe the redacted object, so the check holds unchanged — comparing
against a *live* object is what needs the policy applied first. This query returns the
digest to compare against — and, on its own, answers "is what I have current?"
without any replay at all:

```sql
SELECT
    argMax(event_type, ts)       AS last_event,
    argMax(sha256, ts)           AS sha256,
    argMax(resource_version, ts) AS resource_version,
    max(ts)                      AS last_seen,
    uniqExact(uid)               AS incarnations
FROM resource_states
WHERE cluster_id = {cluster:String}
  AND api_group  = {group:String}
  AND kind       = {kind:String}
  AND namespace  = {namespace:String}
  AND name       = {name:String};
```

`uniqExact(uid)` is in that result for a reason: anything above `1` means the name
has been deleted and recreated, and the digest describes only the newest
incarnation. Note also that this query answers *newest by event time* — which for
an object whose deletion was recovered after an operator restart is not
necessarily the last row physically inserted. That is the intended reading here,
and it is why the reconstruction above keys on `ts` throughout.

## What did this deleted object look like?

> **For one object the CLI answers this**: `kuberecord timeline <kind>/<name> -n
> <ns> --since 30d` ends at the `Deleted` row, and `kuberecord get <kind>/<name>
> -n <ns> --at <just before it>` reconstructs what it held. The query below is the
> wide version — *every* deletion in a window, which is the sweep you run when you
> do not yet know which object to ask about. On an object archive there are no
> `Deleted` rows to find at all, and the CLI says so rather than reporting
> the object as still present.

*"Somebody deleted it and nobody admits to it — what was in it?"* The object is
gone from the cluster, but its last recorded state is not gone from here. This
finds every deletion in a window and pairs each with the newest full state
captured before it.

Dashboard: none — a deletion is a point in time, not a trend. Copy a result into
**Object Timeline** to see how the object got there.

```sql
WITH deletions AS
(
    SELECT
        namespace,
        kind,
        name,
        uid,
        max(ts) AS deleted_at
    FROM resource_states FINAL
    WHERE cluster_id = {cluster:String}
      AND event_type = 'Deleted'
      AND ts BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}
    -- Grouped by uid as well as name: a name that was deleted and recreated has
    -- two histories, and merging them would attribute one incarnation's contents
    -- to the other's deletion.
    GROUP BY namespace, kind, name, uid
)
SELECT
    d.namespace,
    d.kind,
    d.name,
    d.uid,
    d.deleted_at,
    argMax(s.data, s.ts) AS last_full_state,
    max(s.ts)            AS state_captured_at
FROM deletions AS d
INNER JOIN
(
    -- Only rows that carry a whole object; a Modified row's diff is useless
    -- without the state it patches.
    SELECT namespace, kind, name, uid, ts, data
    FROM resource_states FINAL
    WHERE cluster_id = {cluster:String}
      AND data != ''
) AS s
    ON s.namespace = d.namespace
   AND s.kind      = d.kind
   AND s.name      = d.name
   AND s.uid       = d.uid
WHERE s.ts <= d.deleted_at
GROUP BY d.namespace, d.kind, d.name, d.uid, d.deleted_at
ORDER BY d.deleted_at DESC;
```

`last_full_state` is the newest **snapshot**, not necessarily the object's final
state: any `Modified` rows between that snapshot and the deletion are patches this
query does not apply. For most objects the gap is small — a `Checkpoint` is
written every 50 modifications by default — and for an exact answer, feed
`deleted_at` into the reconstruction recipe above as `at`.

A deletion with no `last_full_state` at all means history for that `uid` begins
before your retention window. And note that `Deleted` rows carry no `actors`:
kuberecord inspects the live object for field managers, and on a deletion there is
no longer one to inspect, so "who deleted it" is a question this schema
deliberately does not answer.

## Events for object X around time T

> **The CLI answers this**, and interleaves the Events with the object's own
> changes in one table — which is the reading you actually want, since the point
> is which change the Event followed:
>
> ```sh
> kuberecord timeline deploy/checkout -n payments --with-events
> ```
>
> Both Event API groups are correlated for you, which is the fiddly half of the
> SQL below. The queries here are still the right tool for the aggregate versions:
> [noisiest reasons in a window](#noisiest-reasons-in-a-window), or Events across
> many objects at once.

The other post-mortem question: *"this Deployment broke at 14:05 — what was
Kubernetes saying about it and its pods?"*

Events are streamed by naming `v1/Event` or `events.k8s.io/v1/Event` in a rule
(see [Kubernetes Events](SCHEMA.md#kubernetes-events)). Every Event row carries
the whole Event in `data` and never a diff, which is what makes these queries
plain `JSONExtract` reads with no history replay.

**The join key lives inside `data`.** An Event names its subject in
`involvedObject` (core `v1`) or `regarding` (`events.k8s.io/v1`), and that
subject is the same `(kind, namespace, name, uid)` tuple kuberecord records in
its own columns for the object itself. UID is the exact key; name is the
forgiving one that still finds Events for an object that has since been
recreated.

Dashboard: **Object Timeline**, bottom panel.

### By UID — exact

Use the object's UID from its own `resource_states` rows, so you get Events for
*that* incarnation and no other.

```sql
SELECT
    ts,
    JSONExtractString(data, 'type')                    AS severity,   -- Normal | Warning
    JSONExtractString(data, 'reason')                  AS reason,
    JSONExtractString(data, 'message')                 AS message,
    -- core v1 spells the occurrence count `count`; events.k8s.io/v1 renders the
    -- same legacy Event's count as `deprecatedCount` and only fills `series.count`
    -- for an Event authored with a series. Take whichever is populated.
    greatest(
        JSONExtractUInt(data, 'count'),
        JSONExtractUInt(data, 'deprecatedCount'),
        JSONExtractUInt(data, 'series', 'count')
    )                                                  AS occurrences,
    name                                               AS event_name
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND kind = 'Event'
  AND namespace = {namespace:String}
  AND ts BETWEEN {t:DateTime64(3, 'UTC')} - INTERVAL 15 MINUTE
             AND {t:DateTime64(3, 'UTC')} + INTERVAL 15 MINUTE
  AND coalesce(
        nullIf(JSONExtractString(data, 'involvedObject', 'uid'), ''),  -- v1/Event
        nullIf(JSONExtractString(data, 'regarding',      'uid'), '')   -- events.k8s.io/v1/Event
      ) = {uid:String}
ORDER BY ts ASC;
```

One Event yields one row per `count` bump, so `occurrences` climbing down the
result is the same warning firing again — that repetition *is* the signal, and
it is the case this schema exists to keep.

### By name — survives a recreate

Drop the UID when you want everything Kubernetes said about a name, across
delete-and-recreate cycles:

```sql
SELECT ts, JSONExtractString(data, 'reason') AS reason,
       JSONExtractString(data, 'message')    AS message
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND kind = 'Event'
  AND namespace = {namespace:String}
  AND ts BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}
  AND coalesce(
        nullIf(JSONExtractString(data, 'involvedObject', 'name'), ''),
        nullIf(JSONExtractString(data, 'regarding',      'name'), '')
      ) = {name:String}
  AND coalesce(
        nullIf(JSONExtractString(data, 'involvedObject', 'kind'), ''),
        nullIf(JSONExtractString(data, 'regarding',      'kind'), '')
      ) = {kind:String}
ORDER BY ts ASC;
```

### Object changes and its Events, interleaved

The post-mortem shape: what changed, and what the cluster said about it, on one
timeline.

```sql
SELECT ts, 'change' AS source, event_type AS what, diff AS detail
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND api_group = {group:String} AND kind = {kind:String}
  AND namespace = {namespace:String} AND name = {name:String}
  AND ts BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}

UNION ALL

SELECT ts, 'event' AS source,
       JSONExtractString(data, 'reason')  AS what,
       JSONExtractString(data, 'message') AS detail
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND kind = 'Event'
  AND namespace = {namespace:String}
  AND ts BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}
  AND coalesce(
        nullIf(JSONExtractString(data, 'involvedObject', 'uid'), ''),
        nullIf(JSONExtractString(data, 'regarding',      'uid'), '')
      ) = {uid:String}

ORDER BY ts ASC;
```

### Noisiest reasons in a window

Triage before you know which object to look at:

```sql
SELECT
    JSONExtractString(data, 'reason') AS reason,
    coalesce(
      nullIf(JSONExtractString(data, 'involvedObject', 'kind'), ''),
      nullIf(JSONExtractString(data, 'regarding',      'kind'), '')
    )                                 AS subject_kind,
    count()                           AS rows,
    uniqExact(uid)                    AS distinct_events
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND kind = 'Event'
  AND ts BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}
  AND JSONExtractString(data, 'type') = 'Warning'
GROUP BY reason, subject_kind
ORDER BY rows DESC
LIMIT 25;
```

`rows` counts recorded occurrences (every count bump is a row) while
`distinct_events` counts Event objects — the gap between them is how much of the
noise is one thing repeating versus many things going wrong once.

## Reading Event history correctly

Two traps, both consequences of Events being ephemera rather than durable state
(see [Kubernetes Events](SCHEMA.md#kubernetes-events)):

- **There is never a `Deleted` row for an Event.** An Event's history just stops
  when it expires. Do not infer "still live" from the absence of a deletion the
  way you would for a Deployment — read the Event's own `lastTimestamp`
  (`series.lastObservedTime` in `events.k8s.io/v1`) out of `data`.
- **Filter on `api_group` if a rule names both spellings.** `v1/Event` and
  `events.k8s.io/v1/Event` are one storage behind two APIs, so a rule naming both
  records the same Event twice, once per group. Every query above omits
  `api_group` deliberately — it wants Events however they were captured — but add
  `AND api_group = ''` when you need one row per occurrence rather than one per
  API.

---

## The S3 archive

Everything above reads ClickHouse. Everything below reads the *archive* — the
zstd-compressed JSON Lines objects an `S3Sink` writes — and the difference is not
one of dialect but of capability. **An object store has no query engine.** There
is no index to seek, no sort key to prune on, and no server to push a predicate
down to; a question is answered by fetching objects and reading them. That is the
trade the archive tier makes deliberately: it is cheap to write and cheap to keep
forever, which is exactly what makes it expensive to interrogate.

[DuckDB](https://duckdb.org) is what makes it interrogable anyway, with no
infrastructure at all: one static binary reads `s3://` directly, so the recipes
below are the whole read path. No cluster, no catalog, no ingest step.

Two things to know before running any of them:

- **Every record is a `Snapshot`, and deletions may be missing.** A `Writer`-only
  sink never warms from its own history, so a first sighting is `Snapshot` rather
  than `Added` — permanently, and again after every operator restart. An object
  deleted while the operator was down produces **no record at all**. Neither is a
  defect to query around; see [Hot and cold](TEE.md) for the asymmetry in full and
  [the S3 mapping](SCHEMA.md#physical-mapping-to-s3-objects) for what the archive
  therefore does and does not contain.
- **Records and scope epochs are different objects with different line shapes.**
  Both live under `format=jsonl-v1/`, so a glob of
  `format=jsonl-v1/**/*.jsonl.zst` reads both and infers a schema from the union
  of two unrelated JSON shapes. Records are under `cluster_id=`, the scope log
  under `scopes/`; the setup block below derives one glob for each, and every
  recipe names the one it means.

Everything on this page is CI-tested, but not to the same depth, and the
difference is worth stating: the **DuckDB** recipes are executed against a real
MinIO holding a real archive and must return rows, so a recipe that is valid but
selects nothing fails. The **Athena** DDL is checked for structure only — that it
names every field of the record contract exactly once and projects the partitions
the writer actually produces. There is no AWS account in CI, so nothing executes
it.

### What the CLI reads

`kubectl kuberecord` answers `timeline`, `diff`, `get --at` and `scopes` from these
same objects, with no DuckDB and no database anywhere: the query backend in
`internal/query/objectsource` is the read path, and it is **pure Go — no cgo, no
embedded engine**, because that is what keeps the plugin a static
cross-compile. DuckDB is not being replaced by it. The CLI answers *narrow*
questions — one object, one window, one instant — and the recipes on this page are
what wide analytics still want.

It reads the contract documented once in
[`docs/SCHEMA.md`](SCHEMA.md#physical-mapping-to-s3-objects): the same key layout,
the same line fields, the same single zstd frame per object. Nothing about the
format is restated here. What follows is what a *reader* of it has to do, because
every one of these is a way to be quietly wrong, and a recipe on this page can be
wrong the same way:

- **It prunes to the window, and widens downward.** Only the `date=`/`hour=`
  prefixes the window touches are listed — a fully covered day as one `date=`
  prefix, a partial day hour by hour. The lower bound is then widened by the
  sink's `maxObjectAge`, because an object's partition comes from its *first*
  record and a change stamped 08:05 can live in the `hour=07` object. The CLI
  defaults that widening to the CR's ceiling of **1h**, which is correct against
  any legally configured sink; the upper bound is not widened, since an object
  never holds a record from before its own first one.
- **It never reads the scope log by accident, and never skips it either.** Records
  under `cluster_id=`, epochs under `scopes/`, as above. The scope log is read
  *whole* rather than clipped to the window, because pairing an epoch needs the
  `Started` that may predate it — a scope opened last year and still open covers
  this morning, and a clipped read would report "nobody was watching" about it.
- **It resolves the incarnation before applying any filter.** The identity is
  version-agnostic, and a `(namespace, name)` pair may span several UIDs
  Which incarnation a default query is about is decided from every
  change in the window, *then* the actor and field-path predicates narrow it —
  the other order lets a filter promote a deleted object's history into the living
  object's timeline.
- **It reconstructs by walking back, bounded.** `get --at` names an instant rather
  than a window, so the reader walks partitions backwards until it holds a
  full-state row to replay from, continuing one object span past it. The walk has a
  limit (30 days by default); exhausting it is reported as exhausting it, never as
  the object being absent.

And it declares what it cannot do, rather than presenting the archive as
equivalent to a database:

| Capability | Value | What the CLI does about it |
|---|---|---|
| `deletions` | `false` | A timeline that simply stops carries an explicit notice: the object may have been deleted without the deletion ever being recorded. No `Deleted` row is ever synthesized to close the gap. |
| `server_side_filter` | `false` | Predicates are applied to lines already decoded. The *result* is identical to a pushdown backend's — the conformance suite pins that — but a limit does not bound the work. |
| `point_query` | `false` | A single-object question costs the partitions its window lands in, so the CLI renders a scan estimate (object count and stored bytes, from the listing alone) before it starts. |
| `time_bound_required` | `true` | An unbounded query is refused up front, naming the flag that fixes it, rather than started and never finished. |

The one exception to that last row is `scopes`, which needs no window for the
reason above.

### Parameters (DuckDB)

The DuckDB analogue of the `--param_*` table above. DuckDB has no query
parameters of its own for a CLI session, so the library uses **session
variables**: edit this one block, then paste any recipe below it unchanged.

```sql duckdb-parameters
-- Where the archive is. `archive` is the bucket plus the sink's spec.prefix, with
-- no trailing slash; leave the prefix off entirely if spec.prefix is empty.
SET VARIABLE archive = 's3://kuberecord-archive/audit';
SET VARIABLE cluster = 'prod-eu-1';

-- How to reach it. These are the AWS defaults; for an in-cluster MinIO use the
-- Service address, url_style 'path' and use_ssl false (see the setup block).
SET VARIABLE endpoint  = 's3.eu-central-1.amazonaws.com';
SET VARIABLE region    = 'eu-central-1';
SET VARIABLE url_style = 'vhost';
SET VARIABLE use_ssl   = true;
SET VARIABLE key_id    = 'AKIAEXAMPLE';
SET VARIABLE secret    = 'wJalrXUtnFEMI/EXAMPLEKEY';

-- What to look at. `day` is one UTC date partition; window_from/window_to bound a
-- time window; the four object variables name one object, with '' for the core
-- API group and '' for a cluster-scoped object's namespace.
SET VARIABLE day         = '2026-08-20';
SET VARIABLE window_from = '2026-08-20 09:30:00';
SET VARIABLE window_to   = '2026-08-20 10:15:00';
-- "group" is quoted because GROUP is a reserved word; getvariable('group')
-- reads it by name either way.
SET VARIABLE "group"     = 'apps';
SET VARIABLE kind        = 'Deployment';
SET VARIABLE namespace   = 'payments';
SET VARIABLE name        = 'api';
```

| Variable | Meaning |
|---|---|
| `archive` | `s3://<bucket>/<spec.prefix>`, no trailing slash. |
| `cluster` | `cluster_id` — the `cluster_id=` partition. Always part of the glob, never a `WHERE` clause: it selects which objects are fetched at all. |
| `endpoint`, `region`, `url_style`, `use_ssl`, `key_id`, `secret` | How to reach the store. `url_style` is `'path'` for MinIO and most S3-compatible stores, `'vhost'` for AWS. |
| `day` | One UTC date, as the `date=` partition spells it. |
| `window_from`, `window_to` | A time window, as UTC timestamps. |
| `group`, `kind`, `namespace`, `name` | One object's [canonical identity](SCHEMA.md#identity-is-version-agnostic), minus the cluster. |

One property of session variables to know, because it decides how a mistake
shows up: `getvariable('typo')` returns **NULL**, it does not fail. A recipe
naming a variable you never set therefore filters on NULL and returns nothing at
all, rather than telling you why — which is why CI asserts that the variables
these recipes read and the variables this block declares are the same set.

### Session setup

Run once per DuckDB session. It loads the S3 client, registers the credential, and
derives the two globs every recipe reads — so a recipe never spells the layout
itself and a layout change is one edit here rather than one per query.

```sql duckdb-setup
-- httpfs is what teaches DuckDB to speak S3. It is an extension rather than
-- built in, so the first run downloads it; after that it is cached.
INSTALL httpfs;
LOAD httpfs;

-- The credential. PROVIDER config takes the key explicitly, which is what an
-- in-cluster MinIO needs; on AWS you can instead write
--     CREATE OR REPLACE SECRET kuberecord (TYPE s3, PROVIDER credential_chain,
--                                          REGION getvariable('region'));
-- and DuckDB will use the same environment, profile and instance credentials the
-- AWS CLI would. Reading the archive needs GetObject and ListBucket and nothing
-- else: the operator's own credential is PutObject-only, so an auditor is a
-- separate identity with separate rights rather than a borrowed one.
CREATE OR REPLACE SECRET kuberecord (
    TYPE       s3,
    PROVIDER   config,
    KEY_ID     getvariable('key_id'),
    SECRET     getvariable('secret'),
    REGION     getvariable('region'),
    ENDPOINT   getvariable('endpoint'),
    URL_STYLE  getvariable('url_style'),
    USE_SSL    getvariable('use_ssl')
);

-- The two globs. Records are partitioned by cluster, date and hour; the scope log
-- by date alone and outside cluster_id=, which is why one glob cannot serve both
-- (see docs/SCHEMA.md, "Physical mapping to S3 objects").
SET VARIABLE records = getvariable('archive')
    || '/format=jsonl-v1/cluster_id=' || getvariable('cluster') || '/**/*.jsonl.zst';
SET VARIABLE scopes  = getvariable('archive') || '/format=jsonl-v1/scopes/**/*.jsonl.zst';
```

Three `read_json_auto` arguments recur below, and all three are load bearing:

- `hive_partitioning = true` turns the `key=value` path segments into columns, so
  `date` and `hour` are queryable and DuckDB can skip whole objects instead of
  fetching them. This is the entire practical point of the key layout.
- `map_inference_threshold = 0` forces `labels` to a `MAP(VARCHAR, VARCHAR)`.
  Without it DuckDB infers a `STRUCT` from whichever label keys the sampled
  objects happen to carry, so the same query against a different day can produce a
  different schema — and `labels.app` compiles against one glob and fails against
  the next.
- `filename = true` adds the object key each row came from. It is how a scan
  reports *which objects* answer a question, which is the only way to narrow the
  next scan.

`timestamp` is read as text, deliberately: it is an RFC 3339 instant with
nanosecond precision, which no auto-detected type holds exactly. Cast it —
`TIMESTAMP` truncates to microseconds and is what you want for reading, while
`TIMESTAMP_NS` keeps all nine digits and is what you want when a nanosecond
distinguishes two records.

### A day of the archive

*"What did this cluster do on the 20th?"* The first thing to run against a new
archive, and the shape every other recipe is a variation on: one glob, one day.

```sql duckdb
SELECT
    CAST(timestamp AS TIMESTAMP) AS ts,
    event_type,
    kind,
    -- '' is a real value here, not a null: it is a cluster-scoped object.
    coalesce(nullif(namespace, ''), '(cluster-scoped)') AS namespace,
    name,
    actors,
    hour
FROM read_json_auto(
        getvariable('records'),
        hive_partitioning       = true,
        map_inference_threshold = 0)
-- The partition predicate. DuckDB prunes objects on this before fetching any of
-- them, so a year-deep archive costs one day's objects to read.
WHERE date = CAST(getvariable('day') AS DATE)
ORDER BY ts
LIMIT 200;
```

### One object's timeline

> **This is the one recipe the CLI replaces outright.** `kuberecord timeline
> <kind>/<name> -n <ns> --source s3://bucket/prefix` reads these same objects with
> no DuckDB installed, resolves the incarnation before filtering, and explains an
> empty result against the scope log. Keep the recipe for a session that is already
> in DuckDB, or for joining this timeline to something else.

*"Show me everything this Deployment did."* The archive's answer to [Object
Timeline](DASHBOARDS.md), and the recipe that shows most plainly what a
`Writer`-only sink costs: there is no sort key on identity, so this is a scan of
every object in the glob no matter how narrow the filter.

```sql duckdb
SELECT
    CAST(timestamp AS TIMESTAMP_NS) AS ts,
    event_type,
    resource_version,
    sha256,
    -- Which of the two payloads this record carries. A Snapshot carries full
    -- state, a Modified an RFC 6902 patch; the archive stores both as text.
    length(data) AS data_bytes,
    length(diff) AS diff_bytes,
    actors
FROM read_json_auto(
        getvariable('records'),
        hive_partitioning       = true,
        map_inference_threshold = 0)
-- "group" is quoted because GROUP is a reserved word. The logical record contract
-- spells the field `group`, which ClickHouse's mapping spells `api_group` — the
-- two backends map the same record, they do not share a column vocabulary.
WHERE "group"  = getvariable('group')
  AND kind      = getvariable('kind')
  AND namespace = getvariable('namespace')
  AND name      = getvariable('name')
ORDER BY ts;
```

Narrow it with `AND date = CAST(getvariable('day') AS DATE)` whenever you can:
the identity predicate above filters *rows*, and only the partition predicate
filters *objects*.

### Changes by actor

*"Who has been touching this cluster?"* `actors` is an array per record, so the
question is an unnest and a group — the archive's form of [Drift by
Actor](DASHBOARDS.md).

```sql duckdb
SELECT
    actor,
    count(*)                                 AS records,
    count(DISTINCT namespace || '/' || name) AS objects,
    min(CAST(timestamp AS TIMESTAMP))        AS first_seen,
    max(CAST(timestamp AS TIMESTAMP))        AS last_seen
FROM read_json_auto(
        getvariable('records'),
        hive_partitioning       = true,
        map_inference_threshold = 0),
     -- One row per (record, actor) pair. A record with three field managers
     -- counts once for each, which is the same convention groupUniqArrayArray
     -- gives the ClickHouse recipes.
     unnest(actors) AS a(actor)
WHERE date BETWEEN CAST(getvariable('window_from') AS DATE)
               AND CAST(getvariable('window_to')   AS DATE)
GROUP BY actor
ORDER BY records DESC, actor;
```

The same two honest limits as the ClickHouse version apply: `actors` records
*ownership* harvested from `metadata.managedFields`, not authorship of a
particular edit, and a manager name is self-declared. And one more here — a
`Deleted` record carries no actors at all, so deletions never appear in this
result.

### Activity by namespace

*"Where is the churn?"* The archive's [Namespace
Activity](DASHBOARDS.md): a window, grouped by namespace and kind.

```sql duckdb
SELECT
    coalesce(nullif(namespace, ''), '(cluster-scoped)') AS namespace,
    kind,
    count(*)             AS records,
    count(DISTINCT name) AS objects,
    -- Snapshot is a first sighting, Modified a change. On this backend the two
    -- do not mean "created" and "updated": a restart re-snapshots everything in
    -- scope, so a Snapshot count is churn plus restarts, not creations.
    count(*) FILTER (WHERE event_type = 'Modified') AS changes,
    max(CAST(timestamp AS TIMESTAMP))              AS last_activity
FROM read_json_auto(
        getvariable('records'),
        hive_partitioning       = true,
        map_inference_threshold = 0)
WHERE date BETWEEN CAST(getvariable('window_from') AS DATE)
               AND CAST(getvariable('window_to')   AS DATE)
GROUP BY namespace, kind
ORDER BY records DESC, namespace, kind;
```

### Which objects cover a time window

*"Which objects do I actually have to read?"* This is what the Hive-style layout
is **for**. Answer it first, and every subsequent question costs those objects
instead of the archive.

`glob()` answers it without reading a single byte of any object — the partitions
are in the key, so the store's own listing is the index:

```sql duckdb
WITH objects AS (
    SELECT
        file AS object_key,
        -- The partitions, read back out of the key. regexp_extract rather than
        -- hive_partitioning because nothing here opens an object.
        regexp_extract(file, 'date=(\d{4}-\d{2}-\d{2})', 1) AS date,
        regexp_extract(file, 'hour=(\d{2})', 1)              AS hour
    FROM glob(getvariable('records'))
)
SELECT object_key, date, hour
FROM objects
-- A lexicographic comparison on 'YYYY-MM-DDHH', which is sound only because the
-- hour partition is zero-padded ('09', never '9') — that is why it is.
WHERE date || hour BETWEEN strftime(CAST(getvariable('window_from') AS TIMESTAMP) - INTERVAL 1 HOUR,
                                    '%Y-%m-%d%H')
                       AND strftime(CAST(getvariable('window_to') AS TIMESTAMP), '%Y-%m-%d%H')
ORDER BY object_key;
```

**The one-hour widening on the lower bound is not slack, it is correctness.** An
object is filed under its **first** record's timestamp and keeps accepting records
until it rotates, so a record stamped 10:00 can live in the `hour=09` object. Widen
the lower bound by the sink's `spec.writer.maxObjectAge` (default 5 minutes,
ceiling 1 hour) — an hour is safe for any legal configuration. Nothing widens the
upper bound: an object never holds records from before its own first one.

Once you know the objects, reading them is the same scan with the partition
predicate spelled out — and this form reports what each object actually holds:

```sql duckdb
SELECT
    filename                                               AS object_key,
    count(*)                                               AS records,
    min(CAST(timestamp AS TIMESTAMP))                      AS first_ts,
    max(CAST(timestamp AS TIMESTAMP))                      AS last_ts,
    count(DISTINCT namespace || '/' || kind || '/' || name) AS objects
FROM read_json_auto(
        getvariable('records'),
        hive_partitioning       = true,
        map_inference_threshold = 0,
        filename                = true)
-- Prunes objects, then filters rows: the date term picks the partitions, the
-- timestamp term trims the records inside them that fall outside the window.
WHERE date BETWEEN CAST(getvariable('window_from') AS DATE) - INTERVAL 1 DAY
               AND CAST(getvariable('window_to')   AS DATE)
  AND CAST(timestamp AS TIMESTAMP) BETWEEN CAST(getvariable('window_from') AS TIMESTAMP)
                                       AND CAST(getvariable('window_to')   AS TIMESTAMP)
GROUP BY object_key
ORDER BY object_key;
```

### Was anybody watching?

> **The CLI answers this on either backend**: `kuberecord scopes`, which reads the
> same log and renders one row per period. It also consults it *automatically*
> behind every other command, so an empty timeline is never presented as silence —
> "nothing changed" and "nothing was watching" are different findings and the
> second exits `3`. The query below is for reading the log directly,
> or for pulling it into something else.

*"There are no records for this object after Tuesday — did nothing change, or was
nobody looking?"* On this backend that question has an answer inside the bucket,
and it is the scope log rather than the records that holds it.

```sql duckdb
SELECT
    CAST(ts AS TIMESTAMP) AS ts,
    action,
    "group",
    kind,
    coalesce(nullif(namespace, ''), '(all namespaces)') AS namespace,
    rule_ref
FROM read_json_auto(getvariable('scopes'), hive_partitioning = true)
WHERE cluster_id = getvariable('cluster')
ORDER BY ts;
```

A scope whose last row is `Started` is **not** necessarily still being watched.
This backend cannot reconcile its own epochs, so a process that died with scopes
open leaves them open forever: read an unmatched `Started` as "watching began
here, and this epoch's end is unrecorded". Note the shape difference too — the
scope log's timestamp column is `ts`, not `timestamp`, and it has no `event_type`;
it is a different line, not a variant of the record line.

### Athena

For an archive too large to pull through one DuckDB process, or one that other
people need to query without credentials of their own. The table below reads the
objects in place; **partition projection** means it needs no `MSCK REPAIR` and no
crawler, because every partition is derivable from the key.

```sql athena
CREATE EXTERNAL TABLE kuberecord_records (
    -- One column per field of the logical record contract, spelled exactly as the
    -- JSON line spells it (docs/SCHEMA.md, "The record line"). `timestamp` and
    -- `group` are reserved words: backticks here, double quotes in a SELECT.
    -- `timestamp` stays a string because Athena's own timestamp type holds
    -- milliseconds, not the nanoseconds the contract carries — read it with
    -- from_iso8601_timestamp("timestamp").
    `timestamp`        string,
    `event_type`       string,
    `group`            string,
    `version`          string,
    `kind`             string,
    `namespace`        string,
    `name`             string,
    `uid`              string,
    `resource_version` string,
    `labels`           map<string, string>,
    `actors`           array<string>,
    `data`             string,
    `diff`             string,
    `sha256`           string
)
-- cluster_id is a partition and therefore not also a column: Hive forbids one
-- name being both, and the key is the authority anyway — an object is refused if
-- its records disagree about cluster_id, so every line under cluster_id=X carries
-- exactly X.
PARTITIONED BY (
    `cluster_id` string,
    `date`       string,
    `hour`       string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
-- The format partition is inside LOCATION rather than being a column, which is
-- what scopes this table to one version of the object contract: a future
-- format=jsonl-v2 is a second table, not a schema change to this one.
LOCATION 's3://kuberecord-archive/audit/format=jsonl-v1/'
TBLPROPERTIES (
    'projection.enabled'            = 'true',
    -- injected: a cluster_id cannot be enumerated or generated, so every query
    -- must supply one in its WHERE clause. That is the same discipline the
    -- ClickHouse recipes follow for the same reason, enforced by the engine.
    'projection.cluster_id.type'    = 'injected',
    'projection.date.type'          = 'date',
    'projection.date.format'        = 'yyyy-MM-dd',
    -- Set the lower bound to when this archive started; NOW keeps the upper end
    -- moving without a DDL change. A range wider than the data costs nothing but
    -- empty prefix probes.
    'projection.date.range'         = '2026-01-01,NOW',
    'projection.date.interval'      = '1',
    'projection.date.interval.unit' = 'DAYS',
    -- digits = 2 is what makes the projection generate 'hour=09' rather than
    -- 'hour=9', which is the only spelling the writer produces.
    'projection.hour.type'          = 'integer',
    'projection.hour.range'         = '0,23',
    'projection.hour.digits'        = '2',
    'storage.location.template'     = 's3://kuberecord-archive/audit/format=jsonl-v1/cluster_id=${cluster_id}/date=${date}/hour=${hour}'
);
```

Then, with the same partition-first discipline as everywhere else:

```sql athena
SELECT from_iso8601_timestamp("timestamp") AS ts, event_type, kind, namespace, name, actors
FROM kuberecord_records
WHERE cluster_id = 'prod-eu-1'   -- required: cluster_id is an injected partition
  AND "date"     = '2026-08-20'
ORDER BY ts
LIMIT 200;
```

Four things to check before trusting this table in your account:

- **Athena engine v3.** ZSTD-compressed text is readable there; earlier engines
  are not guaranteed to read it. Athena decides compression from the file
  extension, which is why every object ends `.jsonl.zst`.
- **The scope log needs its own table.** It sits outside `cluster_id=` and is
  partitioned by `date` alone, so point a second table at
  `.../format=jsonl-v1/scopes/` with `date` as its only projected partition and
  the columns of the scope line rather than the record line.
- **`data` and `diff` are strings holding JSON**, not structs. Read into them with
  `json_extract_scalar(data, '$.spec.replicas')` rather than declaring a schema
  for a Kubernetes object kuberecord does not own.
- **This DDL is not executed in CI.** Its structure is asserted against the record
  contract and the key layout on every push; that Athena accepts it rests on the
  AWS documentation for [partition
  projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html)
  and [compression
  support](https://docs.aws.amazon.com/athena/latest/ug/compression-support-hive.html),
  not on a passing test.

### What none of these recipes need

Three habits from the ClickHouse half of this page are unnecessary here, and
knowing why is most of understanding the backend:

- **No `FINAL`, and no dedup.** An object's key is the SHA-256 of its own
  contents, so a retried upload overwrites rather than duplicating. There is
  nothing to collapse and no unmerged-duplicate window to read around.
- **No `Checkpoint` handling.** `S3Sink` has no `checkpointEvery`: the archive
  holds `Snapshot`, `Modified` and (live-observed) `Deleted` records, and never a
  `Checkpoint`. A `Snapshot` is the base a replay starts from, and after every
  restart there is a fresh one.
- **No reconstruction recipe.** Replaying diffs onto a base is identical to the
  [ClickHouse procedure](SCHEMA.md#reconstructing-state-at-an-instant) — take the
  last record with non-empty `data` at or before the instant, apply each later
  `diff` in `timestamp` order — but the read that feeds it is a scan rather than a
  seek, and the tee pattern exists so you do not have to do it here. If the same
  objects also stream to a `ClickHouseSink`, reconstruct there and keep the
  archive for what it is good at: being cheap, immutable, and still around in
  seven years.
