# Zero-infrastructure quickstart

**A working kuberecord, and an answered `kuberecord timeline`, with no database
anywhere.** One `helm install`, one object store, one static binary. In under ten
minutes, and that is [tested rather than
asserted](../../.github/workflows/quickstart.yml).

```sh
make quickstart-zero-infra        # stand it all up and read the archive back
make quickstart-zero-infra-down   # delete the kind cluster and everything in it
```

## Why this path exists

The [other quickstart](../quickstart/) stands up a single-node ClickHouse and
reads the rows back with SQL. That is the right shape for a queryable timeline,
and it is what most installs end up running.

It is the wrong shape for *deciding whether to run kuberecord at all*. Standing up
a database to find out whether a tool is worth having is a bad trade, and it puts
a piece of infrastructure between a stranger and the thing they came to see. So
this path removes it:

- **Storage is an object store.** An `S3Sink` archives the same records into
  `format=jsonl-v1/` objects — the same record contract, the same fields, the same
  redaction. A bucket you already have works just as well as the MinIO this
  example stands up.
- **The read side is one binary.** `kubectl kuberecord` reads those objects
  directly. It is **pure Go with no cgo** (D18), so there is no engine to install,
  nothing to run as a service, and nothing to keep up to date.
- **Nothing about the capture path is reduced.** An `S3Sink` records what a
  `ClickHouseSink` records. The difference is on the read side, it is declared,
  and the CLI says so where it matters — see [what the archive cannot
  answer](#what-an-archive-cannot-answer) below.

## What is in here

| File | What it is |
|---|---|
| [`kind.yaml`](kind.yaml) | The one-node kind cluster. |
| [`minio.yaml`](minio.yaml) | A single-node MinIO in its own namespace, with an `emptyDir` for storage. The evaluation stand-in for a bucket. |
| [`secret.yaml`](secret.yaml) | The key pair the sink authenticates with, in the operator's namespace — the only namespace it can read a Secret from. |
| [`sink.yaml`](sink.yaml) | The `S3Sink`: bucket, prefix, endpoint, rotation, redaction floor. |
| [`demo.yaml`](demo.yaml) | A namespace, a Deployment and a ConfigMap to record. |
| [`rule.yaml`](rule.yaml) | The `ClusterStreamRule` that streams them into the sink. |
| [`zero-infra.sh`](zero-infra.sh) | The transcript, in order, with the waits a script needs and a person does not. |

## By hand

Every step the script takes is an ordinary command. With a cluster and a bucket
you already have, it is these:

```sh
# 1. The operator. From a release, this needs no checkout and no download.
helm install kuberecord oci://ghcr.io/kuberecord/charts/kuberecord \
  --version 0.3.0 \
  --namespace kuberecord-system --create-namespace \
  --set clusterID=my-cluster

# 2. The credentials it writes with — in the operator's namespace.
#    On a cloud provider, delete spec.credentials from the sink instead and
#    authenticate from the ambient chain (IRSA, workload identity, an
#    instance role). There is then no long-lived key to leak or rotate.
kubectl apply -f secret.yaml

# 3. Where the records go, and what is streamed there.
kubectl apply -f sink.yaml
kubectl wait --for=condition=Ready s3sink/archive --timeout=3m
kubectl apply -f rule.yaml

# 4. Read it back. No cluster access is needed for this step at all.
kuberecord timeline deploy/checkout-api -n zero-infra-demo \
  --source s3://my-bucket/audit --since 1h
```

Step 4 is the one worth pausing on: `--source` bypasses discovery entirely, so it
needs no kubeconfig, no sink custom resource and no permission to read a Secret.
An archive synced to a laptop answers the same questions from a plane. Credentials
come from the ordinary [AWS credential
chain](../../docs/CLI.md#--source) — environment, `~/.aws/config`, SSO, an instance
role — because every tool on the machine already reads it.

For an in-cluster MinIO like this example's, the chain is four environment
variables and a port-forward:

```sh
kubectl port-forward -n kuberecord-zero-infra svc/minio 19000:9000

export AWS_ACCESS_KEY_ID=kuberecord
export AWS_SECRET_ACCESS_KEY=kuberecord-zero-infra
export AWS_ENDPOINT_URL_S3=http://127.0.0.1:19000
export AWS_S3_FORCE_PATH_STYLE=true

kuberecord --source s3://kuberecord-zero-infra/audit \
  timeline deploy/checkout-api -n zero-infra-demo --since 1h
```

Or save it once and drop the flags:

```sh
kuberecord config set-profile evaluation --backend s3 \
  --bucket kuberecord-zero-infra --prefix audit \
  --endpoint http://127.0.0.1:19000 --force-path-style
```

## What an archive cannot answer

The CLI keys its behaviour on what a backend **declares**, not on its name, and it
reports a gap as a gap rather than as an empty result. Four differences from
ClickHouse are visible here, and all four are stated in the output when they
apply:

- **No deletions.** An object archive holds no `Deleted` records at all (D12), so
  a timeline that simply stops carries an explicit notice saying the object may
  have been deleted without the deletion ever being recorded. Nothing is
  synthesized to close the gap.
- **No index.** A single-object question costs every object in the partitions its
  window lands in. The CLI prints the estimate before it starts, confirms a wide
  window, reports progress, and offers `--max-objects` as a circuit breaker.
- **A window is required.** With neither end given, the CLI supplies 24 hours and
  says so.
- **No server-side filtering.** `--actor` and `--field` give an identical *result*
  either way — that is pinned by the query conformance suite — but `--limit` does
  not bound the work.

The whole table, with what each one costs, is
[`docs/CLI.md`](../../docs/CLI.md#backend-capability-differences). For wide
analytics over the same objects — aggregations, joins, fleet-level drift — the
DuckDB and Athena recipes in [`docs/QUERIES.md`](../../docs/QUERIES.md#the-s3-archive)
are the right tool, and the CLI does not pretend otherwise.

## Evaluation shortcuts, and what a real install does instead

Three things here are shortcuts. Everything else — all of the RBAC, all of the
pipeline, the whole sink runtime — is exactly how a production install works.

| Shortcut | What production does |
|---|---|
| MinIO with an `emptyDir`, so deleting the pod deletes the archive | A real bucket, with a real lifecycle policy, and — if the archive is a compliance artifact — Object Lock. See [`docs/RETENTION.md`](../../docs/RETENTION.md). |
| A committed key pair in [`secret.yaml`](secret.yaml) | A secret manager, or no key at all: delete `spec.credentials` and authenticate from the ambient chain. |
| `maxObjectAge: 20s`, so an object closes while you are watching | The shipped default of `5m` or larger. Every engine reading this layout pays per object, and a cluster rotating every 20 seconds buys latency with a small-file explosion. |

## Adding a database later

You do not migrate. A rule targets exactly one sink, permanently (D14), so a
queryable timeline beside this archive is a `ClickHouseSink` and a **second rule**
naming it — one watch, two backends, no extra load on the API server. That is the
tee pattern: [`docs/TEE.md`](../../docs/TEE.md), runnable at
[`examples/tee/`](../tee/).
