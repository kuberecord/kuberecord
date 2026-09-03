# kuberecord — committed pprof profiles

`before/` and `after/` are the heap and allocation profiles of one full **massive**
profile run (20,000 objects across ConfigMap + Deployment + ServiceAccount, 500
mutations/sec for 60 s, 10 % delete-and-recreate churn) taken either side of Task
2.3's allocation diet. Everything else about the two runs is identical: same
harness, same hardware, same ClickHouse container, same profile file. The only
difference is the four hot-path changes listed below.

Both runs met the massive profile's pass criteria, before and after
(`summary.txt` in each directory is the run's own report):

| | before | after |
|---|---|---|
| sustained records/sec | 545 | 544 |
| p99 enqueue-block | 0.007 ms | 0.007 ms |
| CPU over churn | 0.42 cores | 0.41 cores |
| peak RSS | 399.0 MiB | 398.1 MiB |

End-to-end throughput is unchanged **by design of the experiment, not by
accident**: this run is generator-bound (the harness's own churn ticker paces it
at ~500 mutations/sec) and the pipeline was already using well under half of one
core, so no allocation saving can show up as more records/sec here. What the diet
buys is headroom and GC pressure, which is what these profiles measure.

## Files

| file | what it is |
|---|---|
| `allocs.pb.gz` | cumulative allocation profile at the end of the churn window |
| `allocs-prechurn.pb.gz` | the same profile taken *before* churn started |
| `heap.pb.gz` | live-heap profile after the run drained (post-GC) |
| `summary.txt` | the run's own reported envelope |
| `top-allocs-churn.txt` | top allocators in the churn window, whole process |
| `top-allocs-churn-pipeline.txt` | the same, restricted to stacks through `internal/pipeline` |
| `top-heap.txt` | top holders of live memory |

The `-prechurn` snapshot exists because an allocation profile is cumulative from
process start, and the create phase (20,000 objects) would otherwise dominate the
totals. Subtract it to see the churn window alone:

```sh
go tool pprof -sample_index=alloc_space -base before/allocs-prechurn.pb.gz before/allocs.pb.gz
go tool pprof -sample_index=alloc_space -base after/allocs-prechurn.pb.gz  after/allocs.pb.gz

# the before/after diff for one hot-path function
go tool pprof -sample_index=alloc_space -focus='internal/pipeline' \
  -base before/allocs.pb.gz after/allocs.pb.gz
```

Profiling runs sample one allocation per 32 KiB instead of Go's default 512 KiB
(the harness raises `runtime.MemProfileRate` when `-pprof-dir` is set). At the
default rate a 60-second window leaves single-digit sample counts on individual
call sites, and a real 25 % improvement is then indistinguishable from noise.

## What the profiles say about memory at 20,000 objects

From `after/top-heap.txt` — the live heap is almost entirely the two caches the
design predicts, and nothing else is close:

- the **informer caches** (`sigs.k8s.io/json` decode trees, ~40 % of live heap):
  one shrunken copy of every watched object, `managedFields` already stripped by
  the cache transform;
- the **hashCache diff baselines** (`zstd.(*Encoder).encodeAll` retaining the
  compressed byte slices, ~22 %): one zstd-compressed normalized copy per object.

That is the shape the design intends (see the `hashCache` and informer-memory
sections of `../PERFORMANCE.md`), which is the useful thing a heap profile can
confirm.

## The four allocators that were fixed

Each was measured before being touched, not assumed. Figures are churn-window
allocated bytes from the profiles here, plus `-benchmem` deltas from the
micro-benchmarks that isolate a single call (`internal/pipeline/allocation_bench_test.go`,
`internal/watch/allocation_bench_test.go`) — those are the sharper instrument,
because this profile also contains an apiserver, a churn generator and a logger.

1. **`normalizeObject` deep-copied every object it normalized.**
   `runtime.DeepCopyJSONValue` was 48.9 % of the *bytes* allocated on the dedup
   path — the most frequent outcome in a real cluster, where an event that changes
   nothing hashable still costs a full work item. Now only the maps the function
   edits are copied (root, `metadata`, and `annotations` when a key comes out of
   it); `spec`/`status` are shared by reference and only read, by `json.Marshal`.
   *Profile:* `DeepCopyJSONValue` 5.24 MB → `stripVolatileFields` 2.10 MB (−60 %).
   *Benchmark (`BenchmarkProcessDedup/pod`):* 88.3 → 36.0 kB/op, 1096 → 801
   allocs/op, 86 → 66 µs/op.

2. **The full normalized state was converted to a string and thrown away.**
   `processUpsert` built `string(norm.JSON)` up front for the data column, then
   discarded it on both of the two most common outcomes (a dedup skip, and a
   diff-only `Modified`, whose data column is empty by schema-v1 design). It is
   now materialized once, only for rows that carry it.
   *Profile:* `processUpsert` flat 5.28 MB → 1.48 MB (−72 %).

3. **The informer notification path copied labels it never looked at.**
   `fanOut` extracted the object's label map (and, on an update, the previous
   object's too) before consulting any interest — even though the common case is
   that every interested rule asked for everything and no selector is ever
   evaluated. Extraction is now lazy.
   *Benchmark (`BenchmarkFanOut/match-all`):* 336 B / 2 allocs → **0 / 0** on an
   Add and 672 B / 4 allocs → **0 / 0** on an Update; 350 → 83 ns/op. This is the
   one path that runs inside an informer's own goroutine.

4. **The per-work-item scope lookup always allocated a combined slice.**
   `interestTable.lookupIdentity` cloned the exact-namespace matches and appended
   the cluster-wide ones, even though one side is almost always empty. It now
   returns the non-empty side directly.
   *Benchmark (`BenchmarkLookupIdentity`):* 8 B / 1 alloc → **0 / 0**; 90 → 57 ns/op.

Net effect on the pipeline's own churn-window allocation at the massive profile:
`Process` 54.53 → 50.16 MB (−8 %), `processUpsert` 50.39 → 44.71 MB (−11 %),
`normalizeObject` 14.28 → 11.52 MB (−19 %). The micro-benchmarks show larger
relative wins (−22 % bytes on a realistic Pod's `Modified` path, −59 % on its
dedup path) because the massive profile deliberately churns small 1 KiB objects,
where the diff's JSON decode — see below — is a bigger share of the total.

## Investigated and deliberately not changed

- **`jsondiff.CompareJSON` is the single largest allocator on the `Modified`
  path** (63 % of allocated objects in `BenchmarkProcessModified/pod`): it
  unmarshals *both* the stored baseline and the current object into `any` trees on
  every diff. The library offers `CompareWithoutMarshal`, which would let the
  current side reuse the decoded tree the informer already holds — and that is a
  correctness trap, not a shortcut. An in-memory unstructured tree holds integers
  as `int64`, while a JSON-decoded one holds them as `float64`, so comparing one
  against the other would emit a spurious `replace` operation for every integer
  field of every object, in every diff, forever. The double decode is the price of
  a truthful diff and stays.
- **`json.Marshal` of the normalized object** (26 % of the `Modified` path's
  allocated objects) is not avoidable: those exact bytes are the hash input, the
  stored diff baseline and the data column.
- **`recordCacheEntries`' per-event `WithLabelValues` lookup** was a suspect (the
  same pattern `SinkMetrics` avoids by resolving child collectors once). It does
  not appear anywhere near the top of either profile, so it was left alone —
  fixing invisible things is how a hot path acquires complexity for nothing.
