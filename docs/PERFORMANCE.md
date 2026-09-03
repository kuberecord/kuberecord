# kuberecord — Performance

## Informer memory: the cache transform

Every informer the watch pool builds installs a `SetTransform` that runs before
an object is ever cached:

1. the field-manager names are harvested out of `metadata.managedFields`;
2. they are written to the operator-internal annotation
   `internal.kuberecord.io/actors` as a sorted, de-duplicated, comma-joined
   list;
3. `metadata.managedFields` is deleted from the cached copy.

`managedFields` is routinely the largest single section of a Kubernetes object
and is pure write-provenance bookkeeping. The operator needs exactly one fact
out of it — who touched the object — so keeping that fact and dropping the rest
shrinks every cached object in every informer, permanently. Together with the
compressed diff baselines below, this is the memory half of running well on tiny
and massive clusters alike: one shrunken copy in the informer cache, one
compressed copy in `hashCache`.

The annotation is transport, not content: the pipeline reads it into the record
and strips it again *before* hashing (see `normalizeObject`), so it cannot
perturb an object's hash or appear in a stored diff, and it is never written
back to the API server.

Two properties are worth knowing:

- **The transform is idempotent.** An object with no `managedFields` is returned
  untouched, so a re-`Replace` of already-cached objects cannot erase the
  annotation a previous pass wrote.
- **It never fails.** A transform error would drop the object from the informer
  entirely, so a malformed object is cached as-is instead;
  `ExtractActors` logs whatever it had to skip.

### No periodic resync

Every informer runs with a resync period of **0**. A resync re-delivers the
whole cache to the handlers on a timer, and nothing here needs that: the
pipeline is level-triggered per key (it reads current state, not the event), the
workqueue owns retries and backoff, and a failed write re-adds its own key. The
WatchManager's 30-second pool diff is the level-triggering safety net instead,
and it costs a registry snapshot plus a few map comparisons rather than a full
cache sweep.

This is a **recorded decision**, re-confirmed against measurement (see
["Two recorded decisions"](#two-recorded-decisions) below and the comment
on `resyncPeriod` in `internal/watch/pool.go`), not an omission.

## `hashCache` memory: compressed diff baselines

`hashCache` keeps a full normalized-JSON copy of every watched object — the
diff baseline — *in addition to* the informer cache's own copy. At scale
that second copy is the dominant memory cost of the operator. Kubernetes JSON
compresses extremely well, so each `CacheEntry.JSON` is now stored
**zstd-compressed** (`klauspost/compress`, `SpeedDefault`) and decompressed
only when a diff is actually computed. The unchanged-object dedup path is
hash-comparison-only and never decompresses.

### Baseline measurement

`BenchmarkHashCacheMemory` (in `internal/pipeline`) compresses a corpus of
realistic normalized Pod / Deployment / Service objects
(`internal/pipeline/testdata/`) and reports the aggregate reduction in
`CacheEntry` payload bytes. `TestCacheEntryCompressionReducesMemory` asserts
the reduction stays at or above the 60% target.

Reproduce:

```sh
go test ./internal/pipeline/ -run '^$' -bench BenchmarkHashCacheMemory -benchmem
```

Recorded result (corpus of 3 objects, `SpeedDefault`):

| Metric                          | Value        |
|---------------------------------|--------------|
| Aggregate raw baseline bytes    | 14234        |
| Aggregate compressed bytes      | 5501         |
| **Reduction**                   | **61.4%**    |

This is measured with each entry compressed **independently**, exactly as the
operator stores it — so the figure is conservative. Real clusters hold
thousands of structurally near-identical objects; per-entry compression
already clears the 60% bar without exploiting any cross-object redundancy.

### Hot-path allocation guard

`BenchmarkHashCacheShortCircuit` exercises the dedup short-circuit (cache
`Load` + hash comparison) and reports **0 allocs/op**, confirming the
unchanged-hash path decompresses nothing and does not regress allocations.

```sh
go test ./internal/pipeline/ -run '^$' -bench BenchmarkHashCacheShortCircuit -benchmem
```

### Failure behavior

- **Compression failure** (encoder unavailable): the raw bytes are stored with
  the `encodingRaw` marker and the anomaly is logged at `Error` level
  Diffing still works — it just costs more memory.
- **Decompression failure on diff** (corrupt/truncated entry): the pipeline
  logs at `Error` level and falls back to a **full-state write**, identical to
  the missing-baseline path. The event is never dropped or mis-recorded.

## S3 writer memory: the `workers` multiplier

The two sections above describe the operator's two large caches, and both are
shared by every sink. This one is neither shared nor a cache: it is what a single
`S3Sink`'s write path holds, and it is the one place in kuberecord where a
throughput knob is also a memory knob.

**`workers` means something different on an `S3Sink` than on a `ClickHouseSink`,
and that asymmetry is the whole trap.** On a `ClickHouseSink` a worker holds a row
batch bounded by `batchMaxRows` (1,000 rows) and flushed on `batchMaxWait`, so
raising `workers` buys concurrent inserts at a cost that stays small: it is a
throughput knob. On an `S3Sink` **the object is the batch** — each worker opens an
object of its own and accumulates compressed bytes into it until
`rotation.maxObjectBytes` or `rotation.maxObjectAge` closes it. The writer's
steady-state ceiling is therefore

```text
workers × maxObjectBytes        per sink
```

so on this backend `workers` is a throughput knob **and** a memory multiplier.
Same field name, two sinks, two different purchases.

### The sizing table

Every figure here is arithmetic over two spec fields — a **model**, not a
measurement. Nothing under [`docs/perf/`](perf/) and no `make bench-load` profile
exercises the S3 write path (the harness drives ClickHouse), so there is no
measured S3 envelope to quote and none is invented below.

| `writer.workers` | `rotation.maxObjectBytes` | steady-state ceiling | the shape it buys |
|---|---|---|---|
| **4** (default) | **64Mi** (default) | **256Mi** | the shipped compromise |
| 8 | 64Mi | 512Mi | double the concurrent PUTs, double the memory |
| 64 (the field's max) | 64Mi | **4Gi** (the admission bound) | maximum request concurrency |
| 4 | 1Gi (the field's max) | **4Gi** (the admission bound) | fewest, largest objects |

The object *count* moves with the same knob in the other direction, which is the
consequence most easily missed: N workers hold N partial objects, and each closes
on its own, so where one worker would have closed a single full object, N workers
close N smaller ones. `maxObjectAge` sharpens it — at the deadline every open
object closes regardless of how little it holds, so on a quiet cluster 64 workers
write 64 near-empty objects where 4 write 4. The 1Mi floor on `maxObjectBytes`
exists to keep an archive out of the small-file explosion that makes it expensive
to query; a high worker count is the other way to arrive there.

One term sits on top of the ceiling rather than inside it. Records are rendered to
their JSONL line at *enqueue*, not on the worker (`marshalRecordLine`, see
[`internal/sink/s3/object.go`](../internal/sink/s3/object.go)), so a saturated
hand-off queue holds `writer.queueSize` rendered records as bytes — 5,000 by
default, which at a 2 KiB record is roughly 10 MiB. Small beside the object
ceiling, and worth knowing because it is the term that grows while a sink is
*unreachable* rather than while it is busy.

### The ceiling is soft

`maxObjectBytes` is a rotation **trigger**, not a cap. `objectBuilder.append`
never refuses a record for making an object too large, and `full()` is evaluated
only *after* the record has been written into the frame — see
[`internal/sink/s3/object.go`](../internal/sink/s3/object.go), where the mechanism
and its bound are documented on `objectFlushSlack`. An object therefore overshoots
`maxObjectBytes` by at most the bytes written since the builder last refreshed its
compressed-length reading (256Ki of input, and the refresh is forced as soon as
the un-refreshed input could carry the object over the limit, so an object is
never closed *late* — only, at worst, slightly over). The true per-worker figure
is:

```text
maxObjectBytes + overshoot + that worker's zstd encoder state
```

The encoder state is a per-worker allocation — window and match tables at
`SpeedDefault`, concurrency 1 — that lives as long as the open frame does. It is
small beside a 64Mi object, and it is not zero.

**How much the overshoot matters depends entirely on where `maxObjectBytes` is
set**, because it is an absolute quantity rather than a proportion. At the 64Mi
default it is noise: a fraction of a percent of the object, invisible in any
memory limit you would actually set. At the 1Mi floor it is the same quantity
against an object 64 times smaller, and a sink tuned for small objects should be
sized against a figure meaningfully above its nominal product.

### Under a tee, the ceilings add

[`docs/TEE.md`](TEE.md)'s pattern is two rules over the same resources naming two
sinks. On the read side that is genuinely nearly free — one informer, one watch,
one informer cache, shared. Everything downstream of the work key is per sink,
though, so a second backend adds a second `hashCache` and a second write path's
footprint on top:

| term | how it is arrived at | at 20,000 watched objects |
|---|---|---|
| informer caches + one `hashCache` + the ClickHouse write path | **measured** — the `massive` profile run recorded in [`docs/perf/after/summary.txt`](perf/after/summary.txt) (peak RSS 398.1 MiB, Go heap in use 333.6 MiB) | ~400 MiB |
| the second `hashCache` | **modelled** by scaling a measured share: the committed heap profile attributes ~22 % of live heap to the compressed diff baselines ([`docs/perf/after/top-heap.txt`](perf/after/top-heap.txt)) | ~75 MiB |
| the `S3Sink` writer at defaults | **modelled**: 4 × 64Mi | 256 MiB |
| that sink's hand-off queue, saturated | **modelled**: 5,000 × ~2 KiB | ~10 MiB |
| **total** | | **~740 MiB** |

**So: budget on the order of 1 GiB for a defaults-everywhere tee at this scale**,
and re-derive the third row from your own `workers × maxObjectBytes` if you have
tuned it. Two honesty notes on that total. The measured row is an upper bound
rather than an attribution, because the load harness runs the churn generator in
the same process as the operator
([what these numbers do and do not attribute](#what-these-numbers-do-and-do-not-attribute)),
so the sum errs in the direction a memory limit wants. And it describes memory
only: a tee also doubles per-event CPU, since two keys mean two normalizations and
two hashes, which no row here captures.

### Where the ceiling is enforced

At admission, by a CEL rule on `S3Sink.spec`: `spec.writer.workers ×
spec.rotation.maxObjectBytes` must not exceed **4Gi**. Both fields' own bounds are
individually reasonable and multiply into something that is not — 64 workers at
1Gi each is 64Gi — so the pairing is rejected when the sink is applied rather than
discovered when the pod is OOM-killed. An operator who meets

```text
spec.writer.workers × spec.rotation.maxObjectBytes must not exceed 4Gi …
```

is reading the enforcement of this section. The 4Gi figure is chosen as a bound
that sits legibly next to a container memory limit, not measured from a benchmark:
The write path was exercised against MinIO for correctness and recorded no
memory figure, so there is nothing to derive a different number from. It leaves
every shape that actually helps throughput available — 64 workers at 64Mi is
exactly 4Gi, and S3 rewards request concurrency rather than object size.

**One case the rule does not judge.** Structural defaulting never descends into an
absent parent, so an `S3Sink` that omits the whole `spec.writer` block has no
`workers` value for the rule to read, and the rule assumes the schema default of
4. The write path, in that same case, falls back to the operator's
`--writer-workers` instead (`newS3SinkConfigBuilder` in `cmd/main.go`) — which is
also 4, unless the deployment raised it. A cluster running `--writer-workers=32`
with an `S3Sink` that omits `spec.writer` entirely therefore runs 32 workers
against a rule that admitted 4. State `spec.writer.workers` on the sink and the
two agree exactly; that is the shape to prefer on any deployment that has moved
the flag.

## Load harness + write-path baseline

`test/loadgen` is a synthetic-churn harness that drives realistic object churn
through the **real** pipeline — an in-process envtest apiserver → an informer →
the `internal/pipeline` workqueue → `CHWriter` → a dockerized ClickHouse — and
reports the figures Phase 0's throughput claims rest on. It supplies its own
minimal `ListerRegistry`/`SinkRouter` (one informer per watched kind, one sink,
everything in scope) because the production implementations arrive with the watch
manager and sink manager later in Phase 1. Its informers do install production's
own cache transform, since the size of a cached object is exactly what the memory
half of these numbers describes. It reports, to stdout:

- **sustained records/sec** — settled writes over the churn window;
- **p50 / p99 enqueue-block** — how long `Enqueue` blocked for queue room (the
  hot-path backpressure a pipeline worker actually feels);
- **peak `write_queue_depth`** — how close the hand-off queue came to saturation;
- **peak pipeline backlog** — how far behind the workqueue itself fell (the
  hand-off queue only describes the last hop);
- **CPU over the churn window** — user + system CPU, as "cores busy";
- **process RSS** — peak resident set (`getrusage`, unit-normalized per OS), plus
  the Go heap and runtime totals, which are attributable where RSS is
  not;
- **`hashCache` entries, dedup skips and dropped items** — so a run's figures can
  be checked against the work it claims to have done.

### Running it

```sh
make bench-load                       # the small profile (the default)
make bench-load PROFILE=massive       # one of the three named scale profiles
```

`make bench-load` stands up a throwaway ClickHouse container (as
`make test-integration` does) and provides `KUBEBUILDER_ASSETS` for envtest, then
runs the harness under the `integration` build tag. The load itself comes from a
named profile file rather than a command line — see
[Scale profiles](#scale-profiles-and-published-envelopes) below.

### Recorded dev-hardware baseline

Measured on an Apple-silicon dev laptop (darwin/arm64), ClickHouse
`clickhouse-server:24.8` in Docker, default writer tuning (queue 5000, 4 workers,
batch max 1000 rows / 1s), small profile (`objects=50, rate=200/s,
payload=2KiB, duration=10s`):

| Metric                   | Value      |
|--------------------------|------------|
| sustained records/sec    | ~201       |
| enqueue-block p50        | ~0.003 ms  |
| enqueue-block p99        | ~0.007 ms  |
| peak `write_queue_depth` | 3          |
| process RSS              | ~71 MiB    |

That load predates the named profiles, but it is still expressible against them,
which is what keeps the table above reproducible:

```sh
make bench-load PROFILE=small LOADGEN_OBJECTS=50 LOADGEN_RATE=200 \
  LOADGEN_PAYLOAD_BYTES=2048 LOADGEN_DURATION=10s
```

Pushed harder (`objects=300–400, rate=4000–6000/s, concurrency=16–64`), achieved
throughput plateaued at **~550–565 records/sec** while the write path stayed
essentially idle — p99 enqueue-block <0.01 ms and peak `write_queue_depth` ≤11.

> **Superseded by later measurement — read that plateau as an estimate, not a
> measurement.** This section originally attributed it to "the envtest apiserver's
> own write throughput". It was in fact the harness's own client-side rate limiter
> (envtest's 1,000-QPS `rest.Config`, two requests per mutation). Two things follow.
> The attribution was wrong, and the corrected story is below. And the figure
> itself is no longer traceable: it came from command-line knobs the harness no
> longer has, no committed profile run under [`docs/perf/`](perf/) stands behind it,
> and re-running the same load today raises the client's QPS and produces a
> different number. It is kept because *why* it was wrong is worth keeping, not
> because ~550–565 is a usable figure. The low-rate table above is unaffected — at
> 200 mutations/sec the limiter never bound, and the command above reproduces it.

### Initial SLO, and its verdict

> **Sustain ≥2,000 records/sec single-replica with p99 enqueue-block <10 ms while
> ClickHouse is healthy.**

**1,972 records/sec sustained**, at 20,000 watched objects across mixed GVKs,
under an **offered load of 2,000 mutations/sec** — with a **p99 enqueue-block of
0.015 ms** and a peak hand-off queue depth of 30 out of 5,000, on the hardware
documented below.

**Latency half: met**, by three orders of magnitude, and the queue never came close
to saturating. **Throughput half: near-target, not met as stated** — 1,972 is 98.6 %
of the 2,000 floor, and 98.6 % of a floor is not clearing it. What the run does
demonstrate is that the pipeline kept pace with the load it was driven at: it
sustained ~1,972 records/sec while being offered 2,000 mutations/sec, at 20,000
objects, without the hand-off queue or the enqueue latency showing strain. It does
**not** demonstrate a ceiling — nothing here drove the write path hard enough to
make it fall behind, so 1,972 is the highest rate yet applied, not the highest it
can do. The SLO above is left exactly as written rather than lowered to fit: a
target narrowly missed is worth more on the record than a target quietly moved.
Reproduce with:

```sh
make bench-load PROFILE=massive LOADGEN_RATE=2000 LOADGEN_DURATION=30s
```

The first harness could not validate the throughput half and attributed the ~550
records/sec plateau it saw to "the envtest apiserver's own write throughput". That
attribution was **wrong**, and finding out why was the single most valuable result
of the later harness work: envtest hands back a `rest.Config` capped at 1,000
QPS, and an ordinary update costs two requests (a Get and an Update), so the
harness's *own client-side rate limiter* was the ceiling at almost exactly 500
mutations/sec. The harness now raises QPS/Burst on the client it churns with (see
the comment on `writeCfg` in `test/loadgen/loadgen_test.go`) and creates 20,000
objects in under 5 seconds where it previously plateaued at ~550/sec.

The lesson is worth keeping: a benchmark that reports a ceiling has to prove the
ceiling belongs to the thing under test. That is why the harness now also reports
**dropped generator ticks** — if the load generator cannot keep up, the run says
so instead of publishing its own limit as the operator's.

## Scale profiles and published envelopes

"Production-grade for tiny **and** massive clusters from day one" needs
numbers, not adjectives. The load harness therefore ships three **named scale
profiles**, as data files in `test/loadgen/profiles/`:

| profile | objects | mutations/sec | payload | kinds | churn deletes | churn window |
|---------|---------|---------------|---------|-------|---------------|--------------|
| `small` | 500 | 10 | 2 KiB | ConfigMap | — | 30 s |
| `medium` | 5,000 | 100 | 2 KiB | ConfigMap, Deployment | — | 60 s |
| `massive` | 20,000 | 500 | 1 KiB | ConfigMap, Deployment, ServiceAccount | 10 % | 60 s |

```sh
make bench-load PROFILE=small
make bench-load PROFILE=medium
make bench-load PROFILE=massive
```

A profile file is the whole load definition — object count, rate, payload size,
duration, delete ratio, churn worker count, kinds, **and the pass criteria the run
judges itself against**. That last part is deliberate: a throughput number is
meaningless without the load that produced it, so the harness fails its own run
rather than leaving a human to compare stdout against this table. `make test`
also checks (without needing Docker or envtest) that all three profiles parse and
still describe at least the load the Phase 2 acceptance criteria name.

Individual knobs can be overridden for bisecting, without editing a shipped
profile or inventing a fourth one — profile < `LOADGEN_*` env twin < explicit
flag:

```sh
make bench-load PROFILE=massive LOADGEN_DURATION=30s LOADGEN_PAYLOAD_BYTES=8192
```

`massive` mixes three kinds across two API groups so identity keys, scopes and
informers are genuinely mixed rather than three flavours of one group, and so the
per-object overhead of a metadata-only kind (ServiceAccount) shows up next to a
deeply nested one (Deployment). Two built-ins are deliberately absent: `v1/Secret`
is hard-denied as a watchable kind and must never appear in a benchmark that
claims to describe the real data plane, and `v1/Service` allocates a cluster IP per
object, which exhausts envtest's default service CIDR after a couple of hundred.

### Measured envelopes

Hardware: **Apple M1 Pro (darwin/arm64), 8 logical CPUs, 16 GB RAM**, Go 1.26.2,
ClickHouse `clickhouse-server:24.8` in Docker on the same machine, envtest
apiserver in-process, default writer tuning (hand-off queue 5,000, 4 writer
workers, batch max 1,000 rows / 1 s), default 8 pipeline workers, single operator
"replica".

| | `small` | `medium` | `massive` |
|---|---|---|---|
| watched objects | 500 | 5,000 | 20,000 |
| **sustained records/sec** | 10 | 100 | **537** |
| enqueue-block **p50** | 0.005 ms | 0.004 ms | 0.002 ms |
| enqueue-block **p99** | 0.012 ms | 0.009 ms | **0.007 ms** |
| peak `write_queue_depth` (of 5,000) | 0 | 33 | 21 |
| peak pipeline backlog | 0 | 0 | 1 |
| **CPU over churn** | 0.03 cores | 0.14 cores | **0.43 cores** |
| **peak RSS** | 77 MiB | 227 MiB | **395 MiB** |
| Go heap in use | 44 MiB | 195 MiB | 322 MiB |
| Go runtime `Sys` | 59 MiB | 224 MiB | 393 MiB |
| `hashCache` entries | 502 | 5,002 | 20,002 |
| failed writes | 0 | 0 | 0 |
| dropped work items | 0 | 0 | 0 |

Each column is one run, with no profiling enabled. Run-to-run spread on the
`massive` profile across six runs was **537–545 records/sec, 0.40–0.43 cores and
395–401 MiB RSS** — i.e. all of the differences visible in this table between
otherwise identical configurations are inside that band, which is worth knowing
before reading a 1 % change in it as a regression. The peak queue depths are the
noisiest figures of all (they are single-sample maxima: 21–152 across those same
six runs) and should be read as "nowhere near the 5,000 capacity", not as
measurements.

`records/sec` tracks the applied mutation rate rather than any ceiling: every
profile settled every record it was given, with 0 dropped generator ticks. The
`massive` figure exceeds its 500 mutations/sec because 10 % of those mutations are
delete-and-recreate pairs, which produce two records each. The highest rate the
write path has been *driven* at is the SLO run above — 1,972 records/sec settled
against 2,000 offered, at the same 20,000 objects. That is a demonstrated rate,
not a measured ceiling: nothing in these runs has yet driven the pipeline hard
enough to make it fall behind.

`hashCache` entries exceed the profile's object count by the handful of ambient
objects a real cluster always has (envtest's own `kube-root-ca.crt` ConfigMaps and
default ServiceAccounts) — the informers are cluster-wide, so those are watched
too, which is the honest thing for the figures to include.

### Massive-profile pass criteria, and the verdict

Recorded in `test/loadgen/profiles/massive.json` and asserted by the run itself:

| criterion | required | measured | verdict |
|---|---|---|---|
| sustained records/sec | ≥ 500 | 537 (537–545 over six runs) | **pass** |
| p99 enqueue-block (healthy ClickHouse) | < 10 ms | 0.007 ms | **pass** |
| RSS ceiling at 20,000 objects | documented (guidance: single-digit GB) | **395 MiB** | **pass** — an order of magnitude inside the guidance |

The RSS result is the one worth dwelling on: 20,000 watched objects cost under
0.4 GB, against a target that allowed for single-digit GB. The heap profile
(`docs/perf/after/top-heap.txt`) attributes essentially all of it to the two caches
the design predicts — the informers' shrunken cached objects (~40 % of live heap,
`managedFields` already dropped by the transform) and the zstd-compressed diff
baselines in `hashCache` (~22 %). The two memory measures taken in Phase 0 are
what make that number what it is.

### What these numbers do and do not attribute

Stated plainly, because an envelope with unstated conditions is not a claim:

- **One process.** The harness runs the pipeline, the informers *and* the churn
  generator in a single Go process, so RSS and CPU are **upper bounds** on the
  operator's own footprint, not attributions. The generator is the larger share of
  CPU at high rates (it issues two apiserver requests per mutation).
- **envtest, not a real cluster.** The apiserver is in-process and unloaded; there
  is no controller-manager, no scheduler, and no other client competing for it.
- **Local ClickHouse.** The backend is a container on the same machine, so
  network latency to the sink is not represented. The write path's own behaviour
  under an *unhealthy* backend is the chaos suite's subject, not this
  harness's — every figure here is a healthy-backend figure by construction.
- **Production logging.** The harness installs the same development-mode zap
  logger `cmd/main.go` does, and the pipeline logs one `Info` line per recorded
  change. At 500 records/sec that is 500 log lines/sec, and it is included in the
  CPU figures above — deliberately, because it is what the shipped binary does.
- **The `massive` profile is generator-bound, on purpose.** It applies 500
  mutations/sec because that is what the acceptance criteria name; it is not the
  write path's ceiling.

## Two recorded decisions

### Informer resync period: 0

Every informer runs watch-driven only, with no periodic resync (see
[above](#no-periodic-resync) for why nothing needs one). The cost side is now
measured rather than asserted: each re-delivered object costs a full work item
that settles on the dedup path — a lister read, a normalize, a marshal and a hash
— which is ~36 kB of garbage and ~66 µs of CPU for a realistic Pod
(`BenchmarkProcessDedup`, after the allocation diet below). At the massive
profile's 20,000 objects that is **hundreds of megabytes of allocation and seconds
of CPU per sweep**, spent entirely to re-derive "nothing changed", against a churn
window that costs 0.43 cores in total. A resync would be the single largest source
of avoidable work in the process. The decision is recorded in code on
`resyncPeriod` in `internal/watch/pool.go`.

### Workqueue retry pacing: client-go's default controller rate limiter, unchanged

`pipeline.New` uses `workqueue.DefaultTypedControllerRateLimiter`, and that is a
choice rather than an inherited default. It is the max of two limiters, and **only
`AddRateLimited` passes through them** — `Add`, the path every informer event
takes, is never delayed, so nothing here paces normal streaming:

- **per item, exponential 5 ms → 1000 s.** A key whose write keeps failing backs
  off to a ~17-minute ceiling instead of spinning on a dead backend, and a settled
  item's `Forget` clears the accumulated penalty, so a transient failure leaves no
  lasting tax on the key.
- **overall, a 10 qps / 100 burst token bucket.** This is what shapes a *mass*
  retry: a sink outage fails every in-flight write at once, and without it the
  entire working set would re-arrive together and hammer a backend that is already
  unhealthy.

**The consequence, at the massive profile's scale:** after a total sink outage,
re-delivery of a 20,000-object working set is paced at 10 keys/second, so the tail
of a full recovery is on the order of half an hour. That is accepted. It is a
latency window and never a loss window — the pipeline is level-triggered, so
whenever a key comes back around it writes the object's *current* state, and any
object that changes again in the meantime is re-enqueued immediately by its own
informer event through the undelayed `Add` path. Raising the bucket would buy a
faster tail in exchange for a thundering herd against a backend that has just
recovered, which is the failure mode the chaos suite exists to keep out.
The decision is recorded in code in `pipeline.New`.

## Allocation diet

Four avoidable per-event allocation sources in the hot path were measured, then
fixed. Before/after pprof profiles from a full `massive` run are committed under
[`docs/perf/`](perf/), together with the one-line note for each fixed allocator and
the reasoning for what was investigated and *deliberately left alone* (chiefly
`jsondiff.CompareJSON`'s double JSON decode, which cannot be removed without
emitting a spurious `replace` operation for every integer field of every object).

Headline per-call results, from the `-benchmem` micro-benchmarks that isolate one
call each (`internal/pipeline/allocation_bench_test.go`,
`internal/watch/allocation_bench_test.go`):

| benchmark | before | after |
|---|---|---|
| `ProcessDedup/pod` (unchanged object — the most frequent outcome) | 88.3 kB, 1096 allocs, 86 µs | **36.0 kB, 801 allocs, 66 µs** |
| `ProcessModified/pod` (steady-state update) | 231 kB, 3469 allocs, 371 µs | **180 kB, 3174 allocs, 338 µs** |
| `NormalizeObject/transformed/pod` | 79.4 kB, 1089 allocs | **35.9 kB, 795 allocs** |
| `FanOut/match-all/add` (informer notification path) | 336 B, 2 allocs, 350 ns | **0 B, 0 allocs, 83 ns** |
| `FanOut/match-all/update` | 672 B, 4 allocs, 654 ns | **0 B, 0 allocs, 82 ns** |
| `LookupIdentity/namespaced` (per work item) | 8 B, 1 alloc, 90 ns | **0 B, 0 allocs, 57 ns** |

Reproduce:

```sh
go test ./internal/pipeline/ -run '^$' -bench 'BenchmarkNormalizeObject|BenchmarkProcess' -benchmem
go test ./internal/watch/    -run '^$' -bench 'BenchmarkFanOut|BenchmarkLookupIdentity'      -benchmem
```

End-to-end throughput on the `massive` profile is unchanged by these fixes (544
records/sec before and after), and that is the expected result rather than a
disappointment: that run is generator-bound and the pipeline was already using
under half of one core, so the diet buys headroom and GC pressure, which is what
the committed profiles measure. The single most important property of the largest
fix — `normalizeObject` no longer deep-copying every object — is that it changed
nothing observable: `TestNormalizeObjectMatchesTheDeepCopyReference` asserts the
normalized bytes are **byte-identical** to what the deep-copy version produced, for
every fixture and every object in `internal/pipeline/testdata/`, because those
bytes are hashed into the `sha256` column and stored as the diff baseline. A single
byte of drift would make every object in every cluster look changed at once.
