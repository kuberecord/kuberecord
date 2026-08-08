# Configuring the operator

**Where ClickHouse lives is not configured here.** It is a `ClickHouseSink` CR
plus a Secret — see [`docs/CRDS.md`](CRDS.md). The connection environment
variables the pre-Phase-1 operator read no longer exist, and
[`CHANGELOG.md`](../CHANGELOG.md) maps each of them onto its replacement field.

What is left, and what this page documents, describes *this operator instance*:
which cluster it is, where it reads credentials, how hard it works, and
fleet-wide fallbacks for anything a sink leaves unset.

Every setting is available as both a CLI flag and an environment variable. The
flag wins if both are set.

## Flags and environment variables

| Flag | Env Var | Default | Description |
|---|---|---|---|
| `--cluster-id` | `CLUSTER_ID` | `local-kind-cluster` | Identifier for this cluster, stamped on every row and scope event this operator writes. |
| `--operator-namespace` | `POD_NAMESPACE` | *(none — required)* | The namespace a sink's `credentialsSecretRef` defaults to, and the only namespace the operator reads Secrets in. The shipped Deployment sets it from the downward API. Startup fails if it is unset: guessing it would move a security boundary. |
| `--pipeline-workers` | `PIPELINE_WORKERS` | `8` | Goroutines draining the shared data-plane workqueue. Safe to raise at any value — per-key serialization comes from the workqueue contract, not the worker count. |
| `--ch-auto-create-schema` | `CH_AUTO_CREATE_SCHEMA` | `false` | Make every sink instance execute the shipped DDL ([`deploy/clickhouse/schema`](../deploy/clickhouse/schema/)) idempotently before its first write. Off by default — the operator never mutates ClickHouse DDL unless asked. Operator-level rather than per-sink on purpose: "may this operator run DDL?" is a deployment-time decision, not one the author of a sink CR grants themselves. |
| `--writer-queue-size` | `WRITER_QUEUE_SIZE` | `5000` | Fallback for `spec.writer.queueSize`: capacity (jobs) of a sink's async write hand-off queue. |
| `--writer-workers` | `WRITER_WORKERS` | `4` | Fallback for `spec.writer.workers`: workers draining a sink's write queue into ClickHouse. |
| `--writer-batch-max-rows` | `WRITER_BATCH_MAX_ROWS` | `1000` | Fallback for `spec.writer.batchMaxRows`: row count at which a worker flushes its accumulated insert batch. |
| `--writer-batch-max-wait` | `WRITER_BATCH_MAX_WAIT` | `1s` | Fallback for `spec.writer.batchMaxWait`: max time a batch's first job waits for the batch to fill before flushing regardless. |
| `--writer-enqueue-timeout` | `WRITER_ENQUEUE_TIMEOUT` | `2s` | Fallback for `spec.writer.enqueueTimeout`: how long `Enqueue` waits for queue room before returning an error (the job is never dropped silently). |
| `--writer-drain-timeout` | `WRITER_DRAIN_TIMEOUT` | `15s` | Fallback for `spec.writer.drainTimeout`: budget for draining a sink's queued writes during graceful shutdown. |
| `--metrics-bind-address` | — | `0` (disabled) | Metrics endpoint bind address; `:8443` for HTTPS, `:8080` for HTTP. |
| `--metrics-secure` | — | `true` | Serve the metrics endpoint over HTTPS. |
| `--health-probe-bind-address` | — | `:8081` | Health/readiness probe bind address. |
| `--leader-elect` | — | `false` | Enable leader election (for multi-replica deployments). |
| `--webhook-cert-path` / `--webhook-cert-name` / `--webhook-cert-key` | — | *(empty)* / `tls.crt` / `tls.key` | Webhook server TLS certificate (unused today — no webhooks are registered — reserved for future use). |
| `--metrics-cert-path` / `--metrics-cert-name` / `--metrics-cert-key` | — | *(empty)* / `tls.crt` / `tls.key` | Metrics server TLS certificate. |
| `--enable-http2` | — | `false` | Enable HTTP/2 on the metrics/webhook servers (disabled by default due to known CVEs). |

Standard `controller-runtime`/Zap logging flags (`--zap-devel`, `--zap-encoder`,
`--zap-log-level`, `--zap-stacktrace-level`, `--zap-time-encoding`) are also
available; run the binary with `--help` for the full, exact list.

## The `--writer-*` flags are fallbacks, not defaults

The six `--writer-*` flags are applied **per field**: a `ClickHouseSink` that
states `spec.writer.workers` uses its own value, one that omits it uses the flag.
Operators must be able to size the write path per environment, and a fleet-wide
default should not have to be repeated on every sink.

The per-attempt retry backoff cap (60s) remains an internal default, with no flag
and no CRD field.

`spec.writer.checkpointEvery` is the one writer field with **no** flag twin: it
defaults to `50` from the CRD itself, because how often a full state is written
alongside a diff is a property of the history you want to be able to reconstruct,
not of the process writing it.

Use `make bench-load PROFILE=small|medium|massive` to measure the effect of a
given tuning against a dockerized ClickHouse; the measured envelope for each
profile — throughput, p99 enqueue-block, CPU and RSS at up to 20,000 watched
objects — is published in [`docs/PERFORMANCE.md`](PERFORMANCE.md).

## Setting them

**Helm** — the chart passes flags through rather than growing a value per flag:

```yaml
extraArgs:
  - --ch-auto-create-schema
  - --pipeline-workers=16
  - --writer-batch-max-rows=5000
```

`clusterID`, `leaderElection.enabled`, `metrics.*` and `healthProbe.port` have
dedicated values because they change the rendered objects, not just the command
line. See the [chart README](../deploy/charts/kuberecord/README.md).

**Kustomize / `dist/install.yaml`** — the arguments live on the manager container
in [`config/manager/manager.yaml`](../config/manager/manager.yaml), and
`CLUSTER_ID` is an environment variable there. `POD_NAMESPACE` comes from the
downward API and should be left alone.

## Health probes

Both probes are plain pings. A cluster with no sinks is a valid, healthy state,
and a sink that is unreachable is reported as a condition on its own CR — not as
process unreadiness, which would take the pod out of service and stop every
*other* sink with it.

## See also

- [`docs/CRDS.md`](CRDS.md) — the custom resources, which is where everything
  about *what* is streamed and *where* it goes lives.
- [`docs/OPERATING.md`](OPERATING.md) — the metrics these knobs move, and what to
  do when one goes the wrong way.
- [`docs/PERFORMANCE.md`](PERFORMANCE.md) — measured envelopes per scale profile.
