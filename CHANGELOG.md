# Changelog

All notable changes to kuberecord are recorded here. The format is
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

The project carries four version numbers that move independently — the operator's,
the CRDs' (`v1alpha1`), the ClickHouse schema's (`v1`, frozen) and the S3 object
format's (`jsonl-v1`, frozen) — and [`docs/RELEASING.md`](docs/RELEASING.md) is the
policy for all four. The short version: the operator is pre-1.0, so while it is
`v0.x` a **minor bump may break**, and every break is spelled out below.

Every tagged release must have a section here. `hack/changelog-section.sh` reads
it, the release workflow refuses to publish a tag that lacks one, and what it
reads becomes the GitHub Release body — so this file *is* the release notes rather
than a summary of them.

## [Unreleased]

Nothing yet.

## [0.2.0] - 2026-08-24

The release that makes kuberecord multi-backend. Phase 4 rebuilt the sink
reference into a typed `(kind, name)` identity so that a second backend could
exist without colliding with the first, Phase 5 turned the sink contract into an
executable conformance suite so that every backend is held to it rather than to a
reading of it, Phase 6 added `S3Sink` — the first `Writer`-only archive tier —
and Phase 7 made the result usable and its compliance claim checkable.

Two things break for anyone running from `main`, both spelled out under
**Changed**: a rule's sink reference, and the Helm chart's sink values. The
migration is at the bottom of this section.

### Added

- **A second backend: `S3Sink`, an immutable archive tier.** The same cluster
  state kuberecord streams into ClickHouse can now be streamed into S3, or into
  anything that speaks its API — MinIO, and the on-premises stores beside it.
  `S3Sink` is a cluster-scoped CRD like `ClickHouseSink`, and the reference to
  one is spelled `sink: {kind: S3Sink, name: …}`.

  Its objects are its batches. Records accumulate in memory and are closed and
  PUT when `spec.rotation.maxObjectBytes` (64Mi) or `spec.rotation.maxObjectAge`
  (5m) fires, whichever comes first — there is no `batchMaxRows`/`batchMaxWait`
  pair for this backend, because the object *is* the batch and two sets of
  controls over one decision would leave an author guessing which one won. Each
  object is JSON Lines, one `sink.Record` per line under the Record's own field
  names, zstd-compressed, at a key built from the record content:

  ```
  <prefix>/format=jsonl-v1/cluster_id=<id>/date=<YYYY-MM-DD>/hour=<HH>/<content-hash>.jsonl.zst
  ```

  The hash in the key is what makes a retry safe: a re-sent batch is the same
  bytes at the same key, so the archive a reader sees holds one copy of it
  regardless of how many times the network took the acknowledgement away. Scope
  events are filed under `scopes/` alongside. The layout is deliberately the one
  a query engine partitions on, and it is a **versioned contract of its own**
  (`format=jsonl-v1`, D15) — not covered by the ClickHouse schema v1 freeze, and
  free to gain siblings without disturbing it.

  Authentication has a preferred shape and a supported one. Omit
  `spec.credentials` entirely and the sink builds its client from the ambient
  chain — IRSA, workload identity, an instance role — so no long-lived key exists
  to leak or rotate; that is the recommendation on a cloud provider. Name a
  Secret (`accessKeyId`, `secretAccessKey`) instead where there is no ambient
  identity to use. `spec.endpoint` and `spec.forcePathStyle` are what point the
  sink at MinIO.

  Writes are asynchronous, exactly as ClickHouse's are: no `PUT` ever happens on
  a workqueue worker (Invariant 1). Because each writer worker accumulates an
  object of its own, `spec.writer.workers × spec.rotation.maxObjectBytes` is the
  sink's steady-state memory ceiling, and the pair is capped at 4Gi at admission
  — both fields' individual bounds are reasonable and multiply into something
  that is not. The sink reports `Ready`, `CredentialsResolved`,
  `BucketReachable` — probed once a minute by writing a probe object, because a
  bucket that lists is not necessarily a bucket that accepts writes — and
  `HistoryUnavailable`, below. Every field is documented in
  [`docs/CRDS.md`](docs/CRDS.md), the object format in
  [`docs/SCHEMA.md`](docs/SCHEMA.md).

  **kuberecord never creates the bucket**, and will not: a bucket carries
  retention, encryption, lifecycle and Object Lock settings that belong to
  whoever owns the account, and an operator that created its own would create one
  with none of them.

- **A `Writer`-only sink says so, permanently and out loud.** `S3Sink` cannot
  read back what it wrote (D12). That single limit has consequences an operator
  must not have to discover from a graph: dedup-cache warm-up, zombie garbage
  collection and boot reconciliation of scope epochs are all disabled for such a
  sink, so after a restart every object in scope is re-recorded as a full
  `Snapshot` rather than de-duplicated against history. (While the process is
  watching, changes are still written as `Modified` records carrying a patch,
  exactly as on the ClickHouse side — it is the *first* sighting after a restart
  that differs.)

  So it is reported rather than inferred. The sink carries
  `HistoryUnavailable=True` with reason `WriterOnlySink` — a condition whose
  `True` is the healthy state, which is why it is deliberately left out of the
  `Ready` roll-up: `Ready` stays `True`, because nothing is wrong. Every rule
  bound to such a sink mirrors the condition onto its own status, so the
  operator's-eye view of a rule says what its backend cannot do, and a `Warning`
  event is emitted the first time. Silent degradation is the failure mode this
  release most wanted not to ship (Invariant 5).

- **The tee pattern: both, without choosing.** A rule targets exactly one sink,
  permanently (D14). To have a queryable timeline *and* a cheap immutable
  archive, write two rules over the same resources — one naming a
  `ClickHouseSink`, one naming an `S3Sink`. This is nearly free: the informer
  cache, the watch, the normalization and the hashing are shared, and only the
  write half doubles.

  It is documented in [`docs/TEE.md`](docs/TEE.md), including the ways the two
  halves are *not* equivalent, and shipped as a runnable example in
  [`examples/tee/`](examples/tee/) — MinIO, both sinks, both rules and a workload
  to change — which CI stands up and asserts on rather than leaving as prose.

- **A Sink Conformance Suite, and it gates every backend.** The properties in the
  sink contract — most of all that `commit(ok)` fires exactly once per job on
  every path, including rotation flush, retry and drain — were provable only for
  ClickHouse. With one backend that is redundant; with two it is dangerous, since
  a double commit corrupts the pipeline's version-gated dedup cache *silently*:
  the audit trail simply stops recording an object's changes, with nothing in the
  logs to say so.

  `internal/sink/conformance` is those properties as an executable suite, written
  against the sink interfaces alone and naming no backend (D11). The mandatory
  `Writer` half applies to everything. The optional halves — `StateReader`,
  `ScopeEventWriter`, `Prober` — are declared by each backend's harness and
  checked against the same type assertion the runtime makes: the suite runs a
  half when the claim and the backend agree it is there, says so by name when
  they agree it is not, and **fails the build when they disagree in either
  direction**. A capability that is quietly dropped, or quietly claimed, cannot
  reach a release. Both shipped backends run it.

- **Reading the archive: DuckDB recipes and an Athena table.**
  [`docs/QUERIES.md`](docs/QUERIES.md) gains a section for the S3 side — session
  setup, a day of the archive, one object's timeline, changes by actor, activity
  by namespace, which objects cover a time window, and whether anybody was
  watching — plus the Athena DDL for the same layout. An archive nothing can read
  is a write-only black hole, and this is the read path an `S3Sink` has, since
  the sink itself has none.

  Every DuckDB recipe is **executed against a real object store in CI**, against
  an archive the real writer produced, and is required to return rows — a recipe
  that parses but selects nothing fails. The Athena DDL is held against the
  record contract column by column instead: nothing in CI speaks to Athena, and
  the check is honest about which of the two claims it is making.

- **Object Lock, and a compliance story that states its limits.**
  `spec.objectLock` applies S3 Object Lock retention to every object at PUT time
  — `GOVERNANCE`, which a holder of the bypass permission can lift, or
  `COMPLIANCE`, which nobody can, including the account root, until the date
  passes. Object Lock must already be enabled on the bucket, which is a human's
  operation on the account; a sink asking for retention against a bucket without
  it reports `BucketReachable=False`/`BucketIncompatible` rather than writing
  unprotected objects.

  [`docs/RETENTION.md`](docs/RETENTION.md) is the page for it, and it is as
  explicit about what this does not buy as about what it does: kuberecord signs
  nothing, the operator's own credential can always write *new* objects, Object
  Lock prevents destruction rather than concealment, redaction stays
  forward-only, and an absence in the archive is not evidence of absence. It also
  covers the one non-obvious consequence — an Object Lock bucket is versioned, so
  a retried upload is *accepted* and can leave a second, byte-identical version
  that under `COMPLIANCE` nobody can remove until its date passes.

- **Releases are signed, attested and described.** A tag now publishes evidence
  alongside the artifacts, which is the least a project about tamper-evident
  audit can do for its own supply chain:

  - The image carries a **keyless cosign signature** — over the manifest list and
    over each per-platform manifest, so the digest a cluster actually resolves is
    the one that verifies. There is no key: the signature is bound to a
    short-lived certificate naming the release workflow at the tag that made it.
  - The image and **every attached asset** carry **SLSA build provenance**. The
    artifacts' attestation is generated from `checksums.txt` itself, so the set of
    files that is checksummed and the set that is attested cannot drift apart. The
    image's is also pushed into the registry beside the image, so it survives a
    mirror.
  - An **SPDX 2.3 SBOM** of the published image is attached to the release and
    covered by `checksums.txt`.

  The verification commands — and, in as many words, what they do *not* prove —
  are [`docs/VERIFYING.md`](docs/VERIFYING.md). The short version of the limit: a
  verified image says the operator is the one this project built. It says nothing
  about the rows and objects that operator writes, which kuberecord does not sign
  (see [`docs/RETENTION.md`](docs/RETENTION.md)).

  `v0.1.0` predates this and has `checksums.txt` only; there is no signature to
  look for on it.

### Changed

- **The project is renamed `kubestream` → `kuberecord`.** Nothing had been
  tagged, so there is no released version under the old name and no
  compatibility shim for it: the old names are gone rather than deprecated
  (D5). Everything below **breaks anyone running from `main`**.

  - **CRD API group `kubestream.io` → `kuberecord.io`** (breaking). Every
    resource is now `apiVersion: kuberecord.io/v1alpha1`, and the CRDs
    themselves are `clickhousesinks.kuberecord.io`,
    `streamrules.kuberecord.io` and `clusterstreamrules.kuberecord.io`.
    **The old CRDs and every CR under them must be deleted before installing
    the renamed build** — a group rename is not an in-place upgrade, the two
    groups are unrelated types to the API server, and leaving the old CRDs
    installed keeps their controllers' objects around with nothing reconciling
    them. Re-create the CRs under the new group afterwards.
  - **Prometheus metric namespace `kubestream_` → `kuberecord_`** (breaking).
    Every series is renamed; dashboards, recording rules and alerts that name
    the old prefix stop matching. The shipped Grafana dashboards and
    `deploy/prometheus/alerts.yaml` are updated in step.
  - **Container image `ghcr.io/yelzhy/kubestream` →
    `ghcr.io/yelzhy/kuberecord`** (breaking). No tags were ever pushed under
    the old path.
  - Go module path `github.com/yelzhy/kubestream` →
    `github.com/yelzhy/kuberecord`, matching the renamed GitHub repository.
  - Helm chart `kubestream` → `kuberecord`, and the default release namespace
    `kubestream-system` → `kuberecord-system`. Installed object names follow:
    `kuberecord-controller-manager`, `kuberecord-watcher`, and the service
    accounts and bindings beside them. Uninstall the old release rather than
    upgrading it — the renamed objects will not adopt the old ones.
  - The RBAC aggregation label
    `kubestream.io/aggregate-to-watcher` → `kuberecord.io/aggregate-to-watcher`.
    Any ClusterRole you wrote yourself to grant the operator extra watch
    rights must be relabelled, or it silently stops aggregating.
  - **Default `spec.connection.database` `kubestream` → `kuberecord`**
    (breaking). To keep writing to an existing database, set
    `spec.connection.database: kubestream` explicitly on the `ClickHouseSink`.
  - The process-internal actors annotation is now
    `internal.kuberecord.io/actors`. It is stripped before hashing and never
    persisted, so this changes no stored row and no content hash.

  **The ClickHouse schema is unchanged.** Schema v1 stays frozen: same table
  names, columns, engines, `ORDER BY` tuples and partitioning. Object content
  hashing is unchanged too, so a renamed operator pointed at an existing
  database warms from it and de-duplicates as before rather than rewriting
  every row.

- **A rule's `spec.sinkRef` string is replaced by `spec.sink {kind, name}`**
  (breaking, D10). A sink is now referenced by *which kind of backend* and
  *which CR of that kind*:

  ```yaml
  spec:
    sink:
      kind: ClickHouseSink   # optional; this is the default
      name: default
  ```

  The kind belongs in the reference because a name is only unique **within** a
  kind. A `ClickHouseSink` named `default` and an `S3Sink` named `default` are
  both legal in etcd and are two entirely unrelated backends; keyed on the name
  alone, whichever reconciled second would have displaced the first, and rules
  would then have streamed to a backend carrying another one's dedup cache and
  warm state — re-emitting every object, or suppressing genuine changes, with
  nothing in the logs to say so. This release makes that unrepresentable rather
  than merely unlikely, which is why it lands *before* a second sink kind exists
  to collide with.

  The field is **renamed, not retyped**, and that is a deliberate choice about how
  the break is discovered. A renamed field is an *unknown* field, so the API
  server prunes it and a v0.1.0 rule decodes with `sink` absent — loudly wrong
  rather than quietly defaulted. kuberecord registers no admission webhooks and
  needs no cert-manager (D4), so there is no conversion hook available to migrate
  the stored object either; the alternative would have been to keep accepting a
  bare name and guess its kind, which is exactly the guess that streams a
  cluster's audit trail somewhere nobody chose.

  `spec.sink` is required, `name` has no default, `kind` defaults to
  `ClickHouseSink` and is constrained by an enum to the kinds this build actually
  serves. The pair is immutable as a pair — changing the name and changing the
  kind are the same mistake with the same consequence.

  **Migration is delete-and-recreate**, per rule; there is no in-place edit. The
  exact sequence is in [`docs/UPGRADING.md`](docs/UPGRADING.md) and summarised
  under **Migration** below. Every other field — `resources`, `labelSelector`,
  `extraRedaction`, `namespaceSelector` — is re-applied unchanged.

  **If you skip it, the rule parks rather than misbehaves.** A rule that names no
  sink reports `Ready=False` with reason **`LegacySinkRef`**, withdraws its watch
  targets, and logs at `Error` level; the message names both spellings and the
  fix. Nothing else degrades: other rules keep streaming, the process does not
  exit, and no data is lost — the rule simply streams nothing until it is
  recreated. `kubectl get streamrule -A` also gains a `SINK-KIND` column beside
  `SINK`, because printer columns are plain JSONPath and a pair cannot be joined
  declaratively.

- **The `sink=` metric label value is now `<Kind>/<Name>`** (breaking). Every
  write-path metric a single sink reports — `kuberecord_write_queue_depth`,
  `kuberecord_writes_total`, `kuberecord_write_latency_seconds` and the rest —
  carries `sink="ClickHouseSink/default"` where it used to carry
  `sink="default"`. The kind is in the value for the same reason it is in the
  reference: two same-named sinks of different kinds would otherwise merge into
  one series describing neither.

  Expressions that group or sum `by (sink)` keep working unchanged — only the
  rendered value differs. Expressions that **match a value exactly** must be
  updated, and that is the case to watch: a selector matching nothing looks
  exactly like a metric sitting at zero. The shipped Grafana dashboards and
  `deploy/prometheus/alerts.yaml` need no edits (they group by the label and
  template it rather than pinning a value); only expressions you wrote yourself
  are affected. See [`docs/OPERATING.md`](docs/OPERATING.md).

- **The Helm chart's starter sink is now a `sinks` list** (breaking).
  `createDefaultSink` and the `defaultSink` block are gone; the chart takes a
  list of sinks instead, one entry per CR:

  ```yaml
  sinks:
    - kind: ClickHouseSink
      name: hot
      spec:
        connection:
          addr: clickhouse.kuberecord-system.svc:9000
          credentialsSecretRef:
            name: clickhouse-credentials
    - kind: S3Sink
      name: cold
      spec:
        bucket: kuberecord-archive
  ```

  The old values could express exactly one sink, of exactly one kind, through a
  value per field — a shape that would need a new block per backend and a chart
  release to reach a field somebody needed. An entry is now `{kind, name, spec}`
  with the **spec passed through verbatim**, so every field of every sink CRD is
  reachable, a future backend needs no chart change, and what is *valid* is
  decided by the CRD's own schema and CEL rules (D4) rather than by a second copy
  of them in the chart that could drift. The tee pattern above is two entries.

  There is no shim: an install still setting `createDefaultSink` renders no sink
  at all, silently, because Helm ignores values a chart does not use. Convert it
  before upgrading — the mapping is one-to-one and is in **Migration** below. The
  chart still **never creates a Secret**, at any values, and no sink spec carries
  a credential inline; both kinds name one instead.

### Migration

Upgrading from v0.1.0 requires one manual step, because a rule's sink reference
cannot be converted in place (see **Changed** above). The full walkthrough,
including what a skipped migration looks like in `kubectl describe`, is
[`docs/UPGRADING.md`](docs/UPGRADING.md).

Steps 1 and 2 run **before** the upgrade, while the old schema still serves the
old field:

1. Record which sink each rule names —
   `kubectl get streamrules.kuberecord.io -A -o custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,SINK:.spec.sinkRef`
   (and the same for `clusterstreamrules.kuberecord.io`).
2. Keep the whole bodies: `kubectl get streamrules.kuberecord.io -A -o yaml > rules-v0.1.0.yaml`.
3. Delete the rules. Neither kind uses finalizers, so this is immediate: each
   watch scope closes with a `Stopped` row and **no** `Deleted` rows for the
   objects it covered.
4. Upgrade the CRDs **and** the operator. With Helm the CRDs are a separate step
   (`kubectl apply -f deploy/charts/kuberecord/crds/`) because Helm installs
   `crds/` and never upgrades it; `dist/install.yaml` and `make deploy` carry
   them.
5. Re-apply each rule with `sink: {kind: ClickHouseSink, name: <the old sinkRef>}`.
   A recreated rule warms its dedup cache from the sink's own history, so
   unchanged objects are not re-recorded.

Then update any Prometheus expression that matched a `sink` label value exactly.

If you install with the Helm chart and had `createDefaultSink: true`, convert the
values in the same pass — the fields map one-to-one onto the first entry of the
new list, and nothing about the rendered CR changes:

| v0.1.0 | v0.2.0 |
|---|---|
| `createDefaultSink: true` | one entry in `sinks` |
| `defaultSink.name` | `sinks[0].name` |
| `defaultSink.connection.*` | `sinks[0].spec.connection.*` |
| `defaultSink.writer` | `sinks[0].spec.writer` |
| `defaultSink.policy` | `sinks[0].spec.policy` |

plus `kind: ClickHouseSink` on the entry, which the old values had no way to say.
A stale `createDefaultSink` is not an error and will not be reported: Helm ignores
values a chart does not use, so the sink simply stops being rendered. Since the
chart's `crds/` is installed but never upgraded by Helm, apply the CRDs
explicitly first — `kubectl apply -f deploy/charts/kuberecord/crds/` — or the
`S3Sink` kind will not exist to create.

## [0.1.0] - 2026-08-03

The first tagged release, and the one that sets the shape of the project. Phase 1
replaced the environment-variable global controller with the two-tier CRD
architecture, Phase 2 proved it at both extremes and froze the ClickHouse schema
at `v1`, and Phase 3 closed the product gaps that stop a stranger adopting it.

There is no upgrade path *to* this release, because there is nothing released to
upgrade from (D5). The **Removed** and **Migration** notes below are for anyone
who ran the pre-CRD code out of `main`.

### Added

- **Kubernetes Events ingestion.** Naming `v1/Event` or `events.k8s.io/v1/Event`
  in a rule's `resources:` now persists the cluster's Event stream past its ~1h
  TTL. Either spelling works — the two APIs are one storage — and there is no new
  CRD field, because every difference is a property of the kind rather than a
  preference: a knob here could only ever configure the operator into recording
  something untrue about Events. Three behaviours differ from an ordinary watched
  kind, and each exists to keep one specific falsehood out of the audit trail:
  - **Full state on every row, never a diff.** The API server *updates* an Event
    in place to bump `count`, and that update is an ordinary `Modified` for
    kuberecord — the case naive exporters drop, because they treat an Event as
    write-once. Carrying the whole Event means a `count`, a `message` or an
    `involvedObject` is readable straight off a row with no diff chain to replay.
    Hash dedup still runs, so a resync that changes nothing writes nothing, and
    `Checkpoint` rows never appear for Events (there is no diff run to interrupt).
  - **An expiry is recorded as nothing at all.** There is no `Deleted` row for an
    Event — not for TTL expiry, not for a forced delete, and not as a
    reincarnation close-out when a newer Event takes over an older one's name. An
    Event vanishing is its retention window closing, not a change to the cluster,
    and recording it as a deletion would put one false deletion in the trail per
    Event the cluster ever emitted. The dedup entry is still dropped, so the
    highest-churn kind in a cluster does not leak one cache entry per expiry.
  - **Warm-up seeds, and stops there.** Cache warm-up runs for an Events scope —
    that is what stops a restarting operator re-recording every Event still
    inside its TTL — but the zombie-GC pass, the close-out recovery, the epoch
    probe and the informer-sync wait are all skipped, so the pass can never
    manufacture the deletions the point above rules out. With no deletions to be
    ambiguous about, an Events scope never `Snapshot`-tags either: a cache miss on
    an Event is a new Event.

  `watch_scopes` is unaffected — an Events scope writes ordinary `Started` and
  `Stopped` rows — and the ClickHouse schema is untouched (v1 stays frozen).
- **An `events` watch preset** ([`config/rbac/presets/events.yaml`](config/rbac/presets/events.yaml),
  Helm value `rbac.presets.events`) granting `get,list,watch` on `events` in
  **both** API groups, so a rule may name either spelling without a partial grant.
  It is **not** enabled by default: Events are usually the highest-volume kind in
  a cluster, so the storage bill belongs to somebody who asked for it.
- **`kuberecord_pipeline_dropped_total{reason="ephemeral_delete"}`** — the new
  drop reason, counting Events whose TTL expired. It is the one drop reason with
  a healthy nonzero rate, and its shape is the cheapest available proxy for the
  Event churn the operator is absorbing.
- **[`docs/QUERIES.md`](docs/QUERIES.md)** — the query library, now complete. It
  answers the six flagship questions with runnable, commented SQL: the incident
  window for a namespace with diffs inlined; drift your GitOps controller did not
  cause; top flappers; reconstructing an object's state at an instant; everything
  Kubernetes said about an object around time T (joined by
  `involvedObject`/`regarding` out of `data`, seeded by the Events work above);
  and what a deleted object last looked like. One parameter vocabulary runs
  through all of them, so a set of `--param_*` flags carries from one query to the
  next.
- **Four product dashboards** in [`deploy/grafana/`](deploy/grafana/), reading
  ClickHouse rather than Prometheus — the read path, without a UI to install:
  - `object-timeline.json` — one object's rows and diffs over time, with the
    Kubernetes Events for it on the same page.
  - `drift-by-actor.json` — Modified rows grouped by the field managers in
    `actors`, with a variable naming the GitOps controller's manager so its
    changes can be excluded.
  - `flap-report.json` — objects ranked by change count per window against a
    threshold you set. The threshold line is a series the query returns, because
    Grafana cannot interpolate a variable into a panel's threshold configuration
    and a static line would silently disagree with the box.
  - `namespace-activity.json` — change volume as a namespace-by-time heatmap.

  They need the `grafana-clickhouse-datasource` plugin and a read-only ClickHouse
  user; [`docs/DASHBOARDS.md`](docs/DASHBOARDS.md) covers installing them, what
  each panel is for, and how to render them against load-harness demo data.
- **Published SQL is now tested.** `make test-integration` gained
  [`test/queries`](test/queries), which executes every statement in
  `docs/QUERIES.md` and in every dashboard against a ClickHouse built from the
  shipped DDL and nothing else — so "these queries use only frozen-schema
  columns" is asserted by ClickHouse's own parser rather than by a promise, and a
  query that returns no rows against the demo fixture fails too. The
  unit-testable half (extraction, Grafana macro expansion, variable
  interpolation) runs in `make test`, where it also catches a panel referencing a
  variable its dashboard does not declare — a mistake Grafana renders as an empty
  filter rather than an error.

- **Redaction (`spec.policy.redaction` on a sink, `spec.extraRedaction` on a
  rule).** Configured values are scrubbed out of every object **after
  normalization and before hashing**, which is what makes the guarantee a real
  one rather than a display convention: the stored payload, the diff baseline and
  the `sha256` are all functions of the redacted content, so a scrubbed value
  cannot resurface as a patch operation in a `diff` and cannot be ground out of
  the hash column by an attacker testing candidates. The direct consequence is
  that two states of an object differing **only** in a redacted value are
  indistinguishable to kuberecord: they hash identically and the second one
  deduplicates, writing no row at all. The cost of that is stated in
  [`docs/SCHEMA.md`](docs/SCHEMA.md#redaction) — kuberecord cannot report *that*
  a redacted value changed.
  - The two fields are **additive in every direction**. A sink's policy is a
    floor its owner sets without reviewing every rule written against it, a rule
    adds to that floor, and two rules streaming one object to one sink contribute
    the union of their paths — there is one payload and one hash per object per
    sink, so anything less than a union would let one rule's existence unredact
    another's stream.
  - The path syntax is deliberately tiny: dot segments, a `[*]` array wildcard
    (`spec.template.spec.containers[*].env[*].value`), and an `annotation:`
    shorthand for keys whose dots and slashes a field path cannot spell. No
    JSONPath construct whose match set depends on the object's contents is
    accepted, so what a policy redacts is readable off the policy. Syntax is
    enforced by CRD patterns and the exactly-one-of rule by CEL, so a malformed
    path is rejected at admission rather than degrading a rule at stream time.
  - **`kubectl.kubernetes.io/last-applied-configuration` is scrubbed
    unconditionally**, under every policy including an empty one. `kubectl apply`
    copies the entire submitted object into it, so leaving it alone would ship a
    verbatim second copy of every value the rest of the policy removes.
  - Redaction is **not** a Secrets unlock: `v1/Secret` stays denied in code (D8)
    however thoroughly a policy would scrub it. It is also not an answer to the
    RBAC-flattening caveat, only a way to bound it — see
    [`docs/RBAC.md`](docs/RBAC.md).
  - The ClickHouse schema is untouched (v1 stays frozen), as the Task 2.6 freeze
    gate predicted.
  - **Upgrade impact:** the always-on annotation scrub changes the normalized
    content of every object that carries a last-applied annotation, so those
    objects hash differently than they did before and each writes one fresh row
    on its next event. That is a one-off re-baseline, not a loop — and the same
    is true whenever a redaction path is added later. Rows written earlier keep
    whatever they recorded; redaction is not retroactive.

- **A quickstart that is tested, not claimed
  ([`examples/quickstart/`](examples/quickstart/)).** `make quickstart` takes a
  fresh clone to rows in ClickHouse on a kind cluster: it builds the operator
  from the working tree, side-loads it, installs `config/default` plus four
  documented deltas, stands up a single-node ClickHouse, applies a Secret, a
  `ClickHouseSink` and a `ClusterStreamRule`, creates and then changes a demo
  workload, and finishes by **querying the rows it recorded** — the change stream,
  the diff behind a scale-up, a redacted value that never arrived, and the
  `watch_scopes` epoch. `make quickstart-down` removes all of it.
  - The script exits non-zero if no rows arrive, so "it works" is an assertion
    rather than a screenshot, and every file it applies is an ordinary manifest
    the directory's README walks through by hand.
  - A new `quickstart.yml` CI job runs the same script on every push with
    `QUICKSTART_BUDGET_SECONDS=600`. The ten-minute claim in the README is
    therefore tested on a runner slower than most laptops; the budget is enforced
    only in CI, because a busy laptop is not a failed install.
  - Which parts are evaluation shortcuts (`emptyDir` storage, a committed demo
    password, `--ch-auto-create-schema`) and which are identical to a production
    install (all of the RBAC, the namespaced Secret grant, leader election, the
    authenticated metrics endpoint, the `restricted` Pod Security Standard) is
    spelled out rather than left for the reader to discover.

- **Tag-triggered releases ([`.github/workflows/release.yml`](.github/workflows/release.yml),
  [`docs/RELEASING.md`](docs/RELEASING.md)).** Strangers install tags, not `main`.
  Pushing `vX.Y.Z` builds and pushes the multi-arch image, then attaches
  `install.yaml`, the packaged Helm chart and `checksums.txt` to a GitHub Release
  whose body is this file's section for that version. Three properties are worth
  stating rather than leaving to be discovered:
  - **The release gate runs before anything is published.** A tag that disagrees
    with the committed `VERSION` and the chart's `version`/`appVersion`, or that
    has no section in this file, fails the release instead of publishing an empty
    one. `hack/changelog-section.sh` is the same script the gate and a maintainer
    run, so "will this tag release?" is answerable before tagging.
  - **One immutable image tag, and no floating `latest`.** Both install artifacts
    pin `ghcr.io/yelzhy/kuberecord:vX.Y.Z` exactly, so what a cluster runs is
    decided by the tag somebody chose rather than by whatever moved last. For a
    non-prerelease tag the attached `install.yaml` is byte-identical to the
    committed `dist/install.yaml`, which makes the artifact a stranger downloads
    the file that was reviewed.
  - **`make release-dry-run` is the whole release with nothing published** —
    notes, both install artifacts, checksums, `verify-packaging`, and the full
    multi-arch image build with no registry to push to. The same rehearsal runs
    in CI from a `workflow_dispatch`, and `test/release` covers the extractor and
    the wiring under `make test`.

- **An operator-health dashboard and sample alert rules.**
  [`deploy/grafana/operator-health.json`](deploy/grafana/operator-health.json) is a
  Grafana dashboard covering the whole write path — queue depth against capacity,
  write outcomes, latency p99, batch-size distribution, retry rate, enqueue
  backpressure — plus the two control-plane signals an operator needs: how many
  rules are degraded and which scopes are still warming in SafeMode. It is pinned
  to no particular Grafana or Prometheus: both the data source and the sink are
  dashboard variables.
  [`deploy/prometheus/alerts.yaml`](deploy/prometheus/alerts.yaml) is a
  `PrometheusRule` with four alerts — queue over 80% capacity for 5m, any
  failed-write rate for 10m, any rule `Ready=False` for 15m, any enqueue-timeout
  rate for 5m — each carrying the reasoning for its threshold in a comment above
  it. [`docs/OPERATING.md`](docs/OPERATING.md) is the page that ties them together:
  every metric, every panel, and what to actually do when an alert fires.
- **`kuberecord_rules{condition,status}`** — a new gauge from the rule
  reconcilers, counting how many `StreamRule`s and `ClusterStreamRule`s hold each
  condition at each status. It is deliberately identity-free (it counts rules, it
  does not name them: naming would make series cardinality a function of how many
  rules a cluster defines, and `kubectl` answers "which one?" perfectly well), and
  both rule kinds count into one series. Every `(condition, status)` series is
  published at 0 before any rule is reconciled, so an alert written against
  `status="false"` is well-defined from process start rather than starting its
  clock the moment the first rule degrades.
- `make verify-observability` validates both artifacts against JSON Schemas
  shipped beside them, runs `promtool check rules` over the `PrometheusRule`'s
  `.spec` (promtool is bootstrapped into `bin/` from the pinned Prometheus
  release), and cross-checks every `kuberecord_*` metric either file queries
  against the set the operator's collectors actually declare — so renaming a
  metric fails the build instead of quietly emptying a panel. The schema and
  cross-check halves run under plain `make test`; a new `Observability` workflow
  runs the full target in CI. Grafana publishes no stable, self-contained
  dashboard JSON Schema, so `deploy/grafana/dashboard.schema.json` is a curated
  subset written for this repository — a lint-grade check, documented as such in
  `docs/OPERATING.md`.

- **A Helm chart** (`deploy/charts/kuberecord`) and a **committed, versioned
  `dist/install.yaml`**, alongside the existing kustomize path. All three install
  *the same operator*: the chart renders the same object names as
  `kustomize build config/default`, and `test/chart` asserts that object by
  object — every Role, ClusterRole and binding compared rule for rule, plus the
  manager's arguments, environment, probes and security contexts. That is what
  lets the Phase 1 acceptance suite run against any of them unmodified:
  `E2E_INSTALL=kustomize|helm|installer` changes how the operator is installed and
  nothing else, and `make test-e2e-helm` / `make test-e2e-installer` run the
  lifecycle scenario against the two new paths (both in CI, on their own Kind
  clusters).
  Chart values cover the image (tag or digest), resources, replicas and leader
  election, the metrics endpoint, **which watch presets to install** (a boolean
  per preset, `core-workloads` on by default), extra labels, arbitrary manager
  flags and environment, pod placement, and an optional starter `ClickHouseSink`
  behind `createDefaultSink` (default `false`). The chart **never** creates or
  templates a Secret — a password given as a value would live in the release's
  stored manifest and in every `helm get values` — so sink credentials remain the
  installer's to create. Every value is documented in
  `deploy/charts/kuberecord/README.md`.
  The chart's `crds/` and `files/presets/` are generated copies of
  `config/crd/bases/` and `config/rbac/presets/` (Helm requires the first inside
  the chart; the preset templates read the second at render time).
  `make helm-sync` refreshes them and a test fails if they are stale, so a preset
  cannot mean two different things depending on how you installed.
- Packaging targets: `make helm-lint` (`helm lint --strict`, default values and
  both `ci/` values files), `make helm-kubeconform` (`helm template` piped through
  kubeconform at the Kubernetes version the repo already pins for envtest),
  `make installer-kubeconform`, `make helm-template`, and `make verify-packaging`
  which runs the lot. `helm` and `kubeconform` are bootstrapped into `bin/` by the
  Makefile like every other tool, so `make test` — which now runs the chart tests
  — needs nothing provisioned.

- **`Checkpoint` rows** — the `event_type` the schema reserved now ships. A
  `Checkpoint` is a `Modified` that also carries the full state, so
  reconstructing "what did this object look like at time T?" replays a bounded
  number of diffs instead of walking back to the object's creation. It is
  written every `spec.writer.checkpointEvery` consecutive diff-only `Modified`
  rows for an object (new per-sink field, default `50`, `0` disables, bounded
  `[0, 10000]`), and additionally whenever a single diff comes out larger than
  the object it describes — a diff that big costs more than the state it
  replaces. Aggregate reads are unaffected (`Checkpoint` carries the same
  identity, `uid` and `sha256` a `Modified` would have); only a replay treats it
  specially, as its base. The cadence counter lives in the operator's memory and
  resets on restart, which costs nothing because a restart re-baselines with a
  full-state row anyway.
- `docs/SCHEMA.md` publishes the **official state-reconstruction recipe** (read
  with `FINAL` → newest `data`-bearing row → replay subsequent RFC 6902 patches →
  verify against `sha256`), executed and asserted byte-for-byte against a live
  object by a new integration test.
- `pipeline.NormalizedJSON`, the canonical normalized JSON of an object, exported
  for the same reason `pipeline.ObjectHash` was: so an acceptance suite compares
  against what the write path actually produced instead of reimplementing the
  normalization rules and drifting from them.

- A chaos / failure-mode suite (`make test-chaos`) that runs the operator on its
  own Kind cluster against a ClickHouse it stops and starts, and kills the
  operator itself. One scenario per failure mode — backend down at boot, a
  mid-stream outage longer than the writer's retry budget, hand-off-queue
  saturation, a poison row that fails its batch, and a `SIGKILL` mid-flight
  followed by offline deletions — each asserting through direct ClickHouse
  queries *and* the operator's `/metrics` endpoint, plus a standing invariant
  that no object's deletion is ever recorded twice. `make deploy-chaos` /
  `make undeploy-chaos` install the operator with the suite's overlay.
- `pipeline.ObjectHash`, the canonical content hash of an object, exported so an
  acceptance suite can recompute what the write path put in the `sha256` column
  instead of reimplementing the normalization rules and silently drifting from
  them.
- `test/harness`, the acceptance vocabulary (kubectl wrappers, ClickHouse
  querying, condition and pod decoding, manifest rendering, metrics scraping and
  parsing) now shared by `test/e2e` and `test/chaos`, so both suites read the
  sink through one definition of what a row means.

- `--operator-namespace` (env `POD_NAMESPACE`, supplied from the downward API in
  the shipped Deployment). **Required**: it is the namespace a sink's
  `credentialsSecretRef` defaults to and the only namespace the operator reads
  Secrets in, so the operator refuses to guess it.
- `--pipeline-workers` (env `PIPELINE_WORKERS`, default 8) — goroutines draining
  the shared data-plane workqueue.
- An end-to-end acceptance suite (`make test-e2e`) that runs the operator on a
  real Kind cluster against a real in-cluster ClickHouse and asserts by querying
  the sink directly: the full create/scale/delete lifecycle of a watched object,
  scope epochs across a rule being deleted and re-created, RBAC degrade and
  self-heal, offline deletions and reincarnations across an operator restart, and
  cluster-scoped kinds. `make deploy-e2e` / `make undeploy-e2e` install the
  operator with the suite's overlay.

### Changed

- **The README is now an adoption page, and the reference material moved into
  `docs/`.** It opens with the positioning and the honest paragraph on how this
  relates to — and does not replace — the Kubernetes audit log, then the
  quickstart, five copy-pasteable queries, and an architecture diagram redrawn to
  show the control plane and the data plane as the two tiers they are. Three new
  pages hold what came out of it, so nothing was lost:
  [`docs/CRDS.md`](docs/CRDS.md) (every field of the three custom resources and
  every status condition they report), [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md)
  (operator flags and environment variables) and
  [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md) (the make targets and what each
  test suite proves). The metrics reference now lives with the runbook that
  interprets it, in [`docs/OPERATING.md`](docs/OPERATING.md).
- **The README's five queries are executed in CI.** `test/queries` already ran
  every statement in `docs/QUERIES.md` and in the shipped dashboards against a
  ClickHouse built from the frozen DDL alone; the README is now in that list,
  because its queries are the first SQL anyone runs and the ones least likely to
  be noticed rotting.
- **New `test/docs` package**, running under `make test`: the quickstart's files
  exist, are executable, and agree with each other on the demo password and the
  image tag; the Makefile and the CI job still point at the script; the README
  still carries its required sections and links; every relative link and heading
  anchor in every published page resolves; and none of the environment-variable
  configuration Phase 1 removed has reappeared in an instruction anywhere in the
  repository. `CHANGELOG.md` is exempt from that last check — and has its own
  test requiring it to keep naming them, since its migration table is how an
  upgrader finds out what each became.
- **[`deploy/grafana/dashboard.schema.json`](deploy/grafana/dashboard.schema.json)
  now covers ClickHouse-backed dashboards.** A target's datasource type decides
  what it must carry: PromQL in `expr` for Prometheus, SQL in `rawSql` plus
  `editorType`/`format`/`queryType` for ClickHouse. Requiring both would reject
  every dashboard and requiring neither would let a target that queries nothing
  validate, so the requirement is conditional. `test/observability` validates all
  five shipped dashboards against it, and fails if a sixth is added without being
  registered for checking.
- **`pipeline.ObjectHash` and `pipeline.NormalizedJSON` now take a
  `*pipeline.RedactionPolicy`** (`nil` = the built-in scrubs only). They exist so
  an acceptance suite can recompute what the write path stored instead of
  reimplementing normalization; redaction happens before hashing, so a recompute
  that did not know the stream's policy would silently stop comparing the real
  thing — exactly the drift those functions were exported to prevent.

- **The ClickHouse schema is frozen at `v1`.** D5's one free redesign window is
  now closed: `resource_states` and `watch_scopes` are a public API. Within `v1`
  no column is renamed, retyped, repurposed, or removed, and neither the engines
  nor the sort keys change — the `ORDER BY` tuple is what makes an at-least-once
  re-insert collapse instead of duplicating, so it is as frozen as the columns.
  Future changes are **additive only** (a new `Nullable`/`DEFAULT`-ed column,
  appended, outside the sort key, shipped as a numbered `ALTER TABLE ... ADD
  COLUMN IF NOT EXISTS` file); anything else becomes a new major table version
  with a documented migration, never an in-place incompatible `ALTER`.
  [`docs/SCHEMA.md`](docs/SCHEMA.md#stability--versioning) states the policy and
  records the gate it passed: Events ingestion and redaction, the two Phase 3
  features designed against this schema, were both re-checked on paper and need
  **no new columns** — Events reuse `kind`/`data`/`labels`/`actors` with a
  `count` bump as an ordinary content change, and redaction rewrites values
  inside `data`/`diff` before hashing.
  The schema check's tolerance of *unknown* columns is now a stated guarantee
  rather than an implementation detail, and is tested as one: a table migrated
  ahead of the operator writing to it validates, accepts inserts, and reads back
  through the warm-up query unchanged, with the unknown columns taking their
  declared defaults. That is what makes "migrate the table first, roll the
  operator second" a safe order.
- `test/utils.GetProjectDir` finds the module root by walking up to `go.mod`
  rather than stripping the literal `/test/e2e` from the working directory, which
  resolved to the wrong place for any suite outside that one directory.
- `make build-installer` applies its image override through a throwaway overlay in
  `dist/` instead of `kustomize edit set image` inside `config/manager`. Editing
  the committed base rewrote the manager image's *name* there, and both the e2e and
  the chaos overlays select that image by the name `controller` — so building an
  installer for a real registry would have silently stopped those overlays from
  matching, leaving each suite to run whatever image the base then pinned. The
  target's output is unchanged, and it no longer dirties the working tree. It now
  also takes `INSTALLER_OUT`, so a release can render the manifest into
  `dist/release/` without touching the committed one.
- `make docker-buildx` takes `BUILDX_OUTPUT` (default `--push`). Setting it empty
  builds every platform and pushes nothing, which is how a release rehearsal
  proves the multi-arch build without a registry to write to.

- `--cluster-id` (env `CLUSTER_ID`) survives unchanged: it identifies *this
  operator instance's* cluster and is stamped on every row and scope event, so it
  is not a property of any one sink.
- The six `--writer-*` flags (env `WRITER_*`) are now **fallbacks**, applied per
  field: a `ClickHouseSink` that sets `spec.writer.<field>` uses its own value,
  one that omits it uses the flag.
- `--ch-auto-create-schema` (env `CH_AUTO_CREATE_SCHEMA`) survives as an
  operator-level setting and now applies to every sink instance, which executes
  the shipped DDL in the background before its first write.
- The e2e suite no longer installs cert-manager. Validation is CEL-only and there
  are no webhooks (D4), so nothing in the suite ever needed it.

### Removed — BREAKING: all environment-variable configuration of *what* and *where*

The operator no longer takes its configuration from the process environment.
There is **no compatibility shim** (D5: the project has no existing users, so a
shim would only preserve a design nobody depends on):

| Removed | Replaced by |
|---|---|
| `WATCHED_GVKS` (and its `parseGVKList` parser) | `StreamRule` / `ClusterStreamRule` custom resources — `spec.resources`, evaluated live |
| `CH_ADDR` / `--ch-addr` | `ClickHouseSink` `spec.connection.addr` |
| `CH_DATABASE` / `--ch-database` | `ClickHouseSink` `spec.connection.database` |
| `CH_USERNAME` / `--ch-username` | `ClickHouseSink` `spec.connection.username` |
| `CH_PASSWORD` | the Secret named by `spec.connection.credentialsSecretRef`, read from the operator's own namespace only |
| `CH_DIAL_TIMEOUT` / `--ch-dial-timeout` | `ClickHouseSink` `spec.connection.dialTimeout` |
| `CH_READ_TIMEOUT` / `--ch-read-timeout` | `ClickHouseSink` `spec.connection.readTimeout` |

A single global ClickHouse connection opened at boot is gone with them: the
operator now runs one backend instance per `ClickHouseSink`, created, recycled
(on credential rotation) and drained at runtime.

The `clickhouse-schema` readiness check is also removed. A cluster with no sinks
is a valid, healthy state, and a schema mismatch is now reported as
`SchemaValid=False` on the affected `ClickHouseSink` rather than as process
unreadiness that would have taken every other sink out of service with it.
`/healthz` and `/readyz` are plain pings.

### Fixed

- **`make docker-buildx` swallowed its own failures.** The build line was prefixed
  with `-`, so make ignored a non-zero exit: a cross-compilation error or a
  registry that refused the push reported as a successful target. That is
  survivable for a developer's scratch build and not survivable for a release, so
  the `-` is gone from the build itself (it stays on the builder create/remove
  lines, which are allowed to be no-ops), and the builder and `Dockerfile.cross`
  are now cleaned up through a trap rather than by two lines that never run when
  the build fails.
- **The `helm` and `kubeconform` pins no longer break every CI job that needs
  them.** `helm.sh/helm/v3@v3.21.3` and `github.com/yannh/kubeconform@v0.8.0` both
  declare `go 1.26`, one minor ahead of this module's own `go` directive. That is
  normally invisible — Go just downloads a newer toolchain — but CI runs
  `actions/setup-go` with `go-version-file: go.mod`, and that action also exports
  `GOTOOLCHAIN=local`, which switches the automatic download off. `go install` then
  refused outright, and the failure landed on the *bootstrap*, so it took down every
  target that depends on either binary before a single test ran: `make test` (whose
  prerequisites now include `helm`), `make verify-packaging`, and `make
  test-e2e-helm`. The pins move back to `helm v3.21.0` and `kubeconform v0.7.0`, the
  newest releases of each that still build under this module's Go version. The
  ceiling is now written down beside the pins, because the failure cannot reproduce
  on a machine that already has `bin/` populated or a newer Go on `$PATH` — which is
  every developer machine, and is why it reached `main`.

- The operator could not reconcile any `ClickHouseSink` under its own shipped
  RBAC. The `SinkReconciler` watches Secrets to close the credential-rotation
  loop, and a watch through the manager's cache lists **cluster-wide** by
  default — but the operator's Secret grant is a namespaced `Role`, deliberately
  (D7). The API server refused the list, the cache never synced, and every sink
  sat unreconciled with an empty status. The manager's cache now confines its
  Secret informer to `--operator-namespace`, so the list it issues is exactly the
  one the Role permits; the grant is unchanged. Found by the new e2e suite —
  envtest cannot reproduce it, because its client is effectively an
  administrator.
- **A delete-and-recreate that happened while the operator was down recorded no
  `Deleted` row for the old UID.** The successor was recorded correctly and a
  plain offline deletion still yielded exactly one `Deleted`, but the old
  incarnation's death never reached the audit trail. Neither half of the
  mechanism was wrong: the live reincarnation branch needs the dedup cache to
  already hold the old UID (after a restart it never does — warm-up waits on
  ClickHouse while the informer only has to reach the API server), and the zombie
  GC's UID-gated claim is *supposed* to be refused for a key the live successor
  already holds.

  The evidence is now taken from where it actually survives — the sink's own
  history. `LastKnownStates` groups per `(namespace, name, uid)` instead of per
  identity, so an identity whose history holds an incarnation that is not the
  newest and has no `Deleted` row of its own is, by definition, a death nobody
  recorded. The warm-up seeds the newest incarnation exactly as before and emits
  one `Deleted` row for each unclosed prior, behind the same scope-epoch gate the
  zombie GC sits behind (pre-history is never fabricated into a deletion).

  Which incarnation history holds when the warm reads it depends on a race the
  operator does not control — whether the successor's own first row reached the
  sink before that read — so both outcomes are handled. If the successor's row
  had not landed, the old incarnation is seeded and swept like any other object,
  the sweep finds a different UID live under its name, and its UID-gated claim is
  refused (correctly: the cache entry belongs to the successor). That refusal is
  now recorded rather than dropped: the pass waits, bounded, for history to catch
  up and then closes the old UID out from it. No GC decision changed — the
  refusal still stands and the live entry is never claimed.

  The recovered row is **dated from history**, never from the recovery. That
  keeps reconstruction in the order events actually happened (`Added` → `Deleted`
  for the old UID → the successor's first row, rather than a close-out that sorts
  after the successor and buries it), and it makes every field of the row a
  function of history — so a re-emitted close-out is byte-identical and is
  collapsed on merge by `resource_states`' `ReplacingMergeTree`. Once the row
  lands, that UID's own latest event is `Deleted` and it is excluded from every
  later warm-up, so the recovery is self-limiting.
- **A deleted-and-re-created `ClickHouseSink` was never boot-reconciled again.**
  `Pipeline.RemoveSink` discarded the sink's dedup caches, but the warm/GC
  coordinator kept it marked as boot-reconciled forever, so the pass that writes
  `Stopped` rows for scopes an earlier process left open never ran for that name
  again and scopes orphaned during the sink's absence stayed open in
  `watch_scopes` indefinitely. The sink runtime now clears that bookkeeping
  (`WarmCoordinator.ForgetSink`) immediately alongside the pipeline eviction,
  cancelling any warm still in flight for the sink at the same time.

### Migration

For anyone who ran the pre-CRD code out of `main`:

1. `make install` (or `make deploy`) to install the `kuberecord.io/v1alpha1` CRDs.
2. Create the credentials Secret in the operator's namespace
   (`kubectl create secret generic clickhouse-credentials --from-literal=password=…`).
3. Translate the old `CH_*` environment block into a `ClickHouseSink` — name it
   `default` to match the shipped samples' `spec.sinkRef`.
4. Translate the old `WATCHED_GVKS` list into one or more `StreamRule` /
   `ClusterStreamRule` resources, and apply the matching RBAC preset from
   `config/rbac/presets/` so the operator is allowed to watch those kinds.

The full walkthrough is the README's [Installing](README.md#installing)
section, and [`examples/quickstart/`](examples/quickstart/) is the same sequence
as a runnable ten-minute path on a throwaway cluster.

[Unreleased]: https://github.com/yelzhy/kuberecord/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/yelzhy/kuberecord/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/yelzhy/kuberecord/releases/tag/v0.1.0
