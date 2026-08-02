# kubestream Query Library

Runnable SQL against the frozen v1 schema ([`docs/SCHEMA.md`](SCHEMA.md)). Every
query here uses only frozen columns, so it keeps working across operator
upgrades.

> **Seeded by Task 3.1.** This file currently holds the Events recipes. The
> incident-window, drift-by-actor, flap-report and state-reconstruction queries
> arrive with the dashboards in Task 3.2.

Two conventions apply throughout:

- **Always filter on `cluster_id`.** It is the leading column of the sort key, so
  omitting it turns every query into a full scan *and* silently mixes clusters.
- **`FINAL` where exactness matters.** `resource_states` is a
  `ReplacingMergeTree` and the write path is at-least-once, so an unmerged
  duplicate is possible. `FINAL` costs a merge at read time and is worth it for
  counts; a query that only inspects the newest rows can skip it.

## Events for object X around time T

The post-mortem question: *"this Deployment broke at 14:05 — what was Kubernetes
saying about it and its pods?"*

Events are streamed by naming `v1/Event` or `events.k8s.io/v1/Event` in a rule
(see [Kubernetes Events](SCHEMA.md#kubernetes-events)). Every Event row carries
the whole Event in `data` and never a diff, which is what makes these queries
plain `JSONExtract` reads with no history replay.

**The join key lives inside `data`.** An Event names its subject in
`involvedObject` (core `v1`) or `regarding` (`events.k8s.io/v1`), and that
subject is the same `(kind, namespace, name, uid)` tuple kubestream records in
its own columns for the object itself. UID is the exact key; name is the
forgiving one that still finds Events for an object that has since been
recreated.

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
  AND ts BETWEEN {t:DateTime64(3)} - INTERVAL 15 MINUTE
             AND {t:DateTime64(3)} + INTERVAL 15 MINUTE
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
  AND ts BETWEEN {from:DateTime64(3)} AND {to:DateTime64(3)}
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
  AND ts BETWEEN {from:DateTime64(3)} AND {to:DateTime64(3)}

UNION ALL

SELECT ts, 'event' AS source,
       JSONExtractString(data, 'reason')  AS what,
       JSONExtractString(data, 'message') AS detail
FROM resource_states FINAL
WHERE cluster_id = {cluster:String}
  AND kind = 'Event'
  AND namespace = {namespace:String}
  AND ts BETWEEN {from:DateTime64(3)} AND {to:DateTime64(3)}
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
  AND ts BETWEEN {from:DateTime64(3)} AND {to:DateTime64(3)}
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
