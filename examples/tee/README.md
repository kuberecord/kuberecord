# The tee pattern: one stream, two backends

A queryable timeline in ClickHouse **and** a cheap immutable archive in an object
store, from one watch. This directory is that pattern, applied.

```sh
make quickstart                                     # a cluster, the operator, a ClickHouse
kubectl apply -k examples/tee                       # MinIO, both sinks, both rules
./examples/tee/bucket.sh                            # kuberecord never creates the bucket
kubectl apply -f examples/tee/workload.yaml         # something to record
```

The conceptual half — why two rules rather than one clever sink, what it costs,
and when to reach for it — is [`docs/TEE.md`](../../docs/TEE.md). This page is the
runnable half.

## What you get

The quickstart's cluster, plus:

- a single-node MinIO, standing in for S3;
- a `ClickHouseSink` named `hot` and an `S3Sink` named `cold`;
- two namespaced `StreamRule`s over **the same two resource types**, one naming
  each sink;
- a demo Deployment and ConfigMap to change.

One informer on the API server serves both rules. Two dedup caches, two sets of
status conditions, two independent failure domains.

## The files

| File | What it is |
|---|---|
| [`minio.yaml`](minio.yaml) | Namespace, root credentials, Deployment and Service for a single-node MinIO. **Evaluation only** — `emptyDir` storage, a committed key pair, no Object Lock. |
| [`secret.yaml`](secret.yaml) | The same key pair beside the operator, in the only namespace it can read Secrets in. |
| [`hot-sink.yaml`](hot-sink.yaml) | The `ClickHouseSink`. An ordinary one — nothing about it is tee-specific. |
| [`cold-sink.yaml`](cold-sink.yaml) | The `S3Sink`. `Writer`-only (D12), with the rotation window tuned for watching rather than for running. |
| [`namespace.yaml`](namespace.yaml) | The namespace the two rules watch. |
| [`rules.yaml`](rules.yaml) | **The pattern.** Two rules, identical `resources`, different `spec.sink`. |
| [`workload.yaml`](workload.yaml) | A Deployment and a ConfigMap, applied after the pattern so their creation is recorded as a creation. |
| [`kustomization.yaml`](kustomization.yaml) | Everything except the workload, as one apply. |
| [`bucket.sh`](bucket.sh) | Creates the bucket through the `mc` that ships in the MinIO image. Idempotent. |

## Doing it by hand

**1. A cluster with the operator and a ClickHouse.** `make quickstart` is the
short way; any install from the [README](../../README.md#installing) works, as
long as there is a ClickHouse to point `hot-sink.yaml` at. If yours is elsewhere,
edit `spec.connection.addr` — the database, the username and the Secret name are
already the ones a quickstart install creates.

**2. The pattern.**

```sh
kubectl apply -k examples/tee
kubectl rollout status -n kuberecord-tee deploy/minio
```

**3. The bucket.** kuberecord never creates one: retention, encryption, lifecycle
and Object Lock all belong to whoever owns the account, and Object Lock can only
be enabled at bucket creation. `mc` ships in the MinIO image, so this needs no
client on your machine — [`bucket.sh`](bucket.sh) registers an alias inside the
pod and creates the bucket, and is safe to re-run.

```sh
./examples/tee/bucket.sh
```

**4. Wait for both halves to report themselves healthy.** This is worth doing
before applying a workload rather than after — see the note in
[`workload.yaml`](workload.yaml).

```sh
kubectl get clickhousesink hot s3sink cold
kubectl get streamrules -n tee-demo
```

Both sinks reach `Ready=True`. Both rules reach `Ready=True/Streaming`. The cold
sink and the rule naming it additionally report `HistoryUnavailable=True`, which
is a declared limit rather than a fault:

```console
$ kubectl get s3sink cold -o jsonpath='{.status.conditions[?(@.type=="HistoryUnavailable")].message}'
This sink cannot read its own history back, so three behaviours are disabled for
it: dedup cache warm-up, zombie garbage collection, and boot reconciliation of
scope epochs. An object first seen by this operator process is therefore always a
permanent Snapshot and never an Added, every object is re-snapshotted in full
after each operator restart, and deletions that occur while the operator is down
are never recorded. …
```

The hot rule reports the same condition as `False/HistoryReadable`. That
asymmetry, on two rules watching one set of objects, is the tee pattern's whole
trade written out in the API.

**5. Something to record.**

```sh
kubectl apply -f examples/tee/workload.yaml
kubectl scale -n tee-demo deployment/checkout-api --replicas=2
```

**6. Read the hot side.**

```sh
kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000 &
clickhouse-client --user kuberecord --password quickstart --database kuberecord --query "
  SELECT ts, event_type, kind, name, diff
  FROM resource_states
  WHERE cluster_id = 'kuberecord-quickstart' AND namespace = 'tee-demo'
  ORDER BY ts"
```

```text
2026-08-24 11:02:14.881   Added      Deployment  checkout-api    
2026-08-24 11:02:14.902   Added      ConfigMap   checkout-config 
2026-08-24 11:02:31.447   Modified   Deployment  checkout-api     [{"op":"replace","path":"/spec/replicas","value":2}]
```

**7. Read the cold side.** There is no query engine, so this is a scan — which is
the honest shape of this backend, and the reason the timeline exists beside it.
Within half a minute (the sink's `maxObjectAge`) the objects close and appear:

```sh
kubectl exec -n kuberecord-tee deploy/minio -- mc ls --recursive local/kuberecord-tee
```

```text
audit/format=jsonl-v1/cluster_id=kuberecord-quickstart/date=2026-08-24/hour=11/3f9a….jsonl.zst
audit/format=jsonl-v1/scopes/date=2026-08-24/1c40….jsonl.zst
```

`mc cat` streams the object to your terminal, so the decompression happens on
your machine and needs `zstd` and `jq` there:

```sh
kubectl exec -n kuberecord-tee deploy/minio -- \
  mc cat local/kuberecord-tee/audit/format=jsonl-v1/cluster_id=kuberecord-quickstart/date=2026-08-24/hour=11/3f9a….jsonl.zst \
  | zstd -d | jq -c '{event_type, kind, name}'
```

```text
{"event_type":"Snapshot","kind":"Deployment","name":"checkout-api"}
{"event_type":"Snapshot","kind":"ConfigMap","name":"checkout-config"}
{"event_type":"Modified","kind":"Deployment","name":"checkout-api"}
```

**`Snapshot` where the timeline said `Added`, for the same object, from the same
event.** Nothing went wrong. The archive cannot read its own history, so "is this
object new, or merely new to me?" is a question it can never answer, and
`Snapshot` is the safe answer to it. The `Modified` matches on both sides,
because in-process dedup state is unaffected by the missing read half.

That is a peek, not a read path. For anything real, point a query engine at the
bucket: `format=jsonl-v1/cluster_id=…/date=…/hour=…` is a Hive-style layout on
purpose, so DuckDB, Athena, Spark and Trino can glob the partitions directly
without an export step. See [`docs/TEE.md`](../../docs/TEE.md#when-to-reach-for-it).

## Proving the redaction floor holds on both sides

```sh
clickhouse-client --user kuberecord --password quickstart --database kuberecord --query "
  SELECT data FROM resource_states
  WHERE cluster_id = 'kuberecord-quickstart' AND name = 'checkout-config' AND data != ''
  LIMIT 1" | jq '.data.password'
# "[REDACTED]"
```

The same value is `[REDACTED]` in the archive. Both sinks set the same floor, and
they must: a value scrubbed from the timeline but archived in the clear is
unrecoverable, because redaction is forward-only and an object under Object Lock
cannot be rewritten. Keep the two `spec.policy.redaction` blocks identical.

## What this is not

**Not a durable archive.** MinIO here is an `emptyDir` with a committed password
and no Object Lock. A real cold tier is S3, or a MinIO somebody operates
properly, with Object Lock enabled on the bucket at creation.

**Not a way to get deletions into the archive.** Nothing configures that away —
it is what `Writer`-only means. An object deleted while the operator is down is
never recorded as deleted on the cold side; the archive's last word on it is its
last observed state. The hot side records the deletion normally. That is the
division of labour the pattern exists for.

## Tearing it down

```sh
kubectl delete -f examples/tee/workload.yaml
kubectl delete -k examples/tee
```

Deleting the sinks deletes no history: ClickHouse keeps its rows and the bucket
keeps its objects. `make quickstart-down` removes the cluster and everything in
it, MinIO's `emptyDir` included.
