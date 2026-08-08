# kuberecord Query Library

Runnable SQL against the frozen v1 schema ([`docs/SCHEMA.md`](SCHEMA.md)). Every
query here uses only frozen columns, so it keeps working across operator
upgrades — and that is a tested claim rather than an intention: `make
test-integration` executes every statement on this page against a ClickHouse
whose tables were built from the shipped DDL and nothing else, so a query naming
a column that is not frozen fails CI (`test/queries`).

The four shipped Grafana dashboards ask these same questions with the clicking
already done; see [`docs/DASHBOARDS.md`](DASHBOARDS.md). Each section below names
the dashboard that covers it.

> **If a value reads `[REDACTED]`, that is what is stored.** Redaction happens
> before hashing, so a scrubbed value is absent from `data`, from every `diff`,
> and from the `sha256` — not hidden at query time. Two states of an object that
> differ only in a redacted value produce **one** row, not two, so a "what
> changed?" query cannot see a change confined to a redacted path. See
> [Redaction](SCHEMA.md#redaction).

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
