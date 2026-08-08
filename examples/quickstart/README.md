# kuberecord quickstart

From a fresh clone to queryable cluster history in under ten minutes, on a
laptop, with nothing but Docker, [kind] and `kubectl`.

```sh
make quickstart        # stand everything up, then query it
make quickstart-down   # delete the kind cluster and everything in it
```

That is the whole thing. The rest of this page is what the script does, so you
can run it a step at a time against a cluster of your own — and so you can tell
which parts are an evaluation shortcut and which parts are how kuberecord is
actually meant to be installed.

## What you get

A single-node [kind] cluster running:

- the operator, built from your clone and side-loaded into the node;
- a single-node ClickHouse with the schema v1 tables;
- a `ClickHouseSink` and a `ClusterStreamRule` that stream a demo namespace's
  Deployments and ConfigMaps into it;
- a demo Deployment and ConfigMap, changed once each, so there is real history to
  query rather than an empty table.

The run ends by printing the rows it recorded, the diff behind one of them, proof
that a redacted value never reached the database, and the port-forward you need
to keep querying.

## The files

| File | What it is |
|---|---|
| [`kind.yaml`](kind.yaml) | The kind cluster: one control-plane node, no pinned node image. |
| [`clickhouse.yaml`](clickhouse.yaml) | Namespace, credentials, Deployment and Service for a single-node ClickHouse. **Evaluation only** — `emptyDir` storage, one replica, a committed password. |
| [`secret.yaml`](secret.yaml) | The credentials Secret the operator reads, in the operator's own namespace — the only namespace it can read Secrets in. |
| [`sink.yaml`](sink.yaml) | The `ClickHouseSink` named `default`: where state goes, how the write path is sized, what may be written, and the redaction floor. |
| [`rule.yaml`](rule.yaml) | The `ClusterStreamRule`: what gets streamed, and from which namespaces. |
| [`demo.yaml`](demo.yaml) | A namespace, a Deployment and a ConfigMap, so there is something to record. |
| [`operator/`](operator/) | A kustomize overlay: `config/default` plus four documented deltas. |
| [`quickstart.sh`](quickstart.sh) | The driver. Everything below, in order, with waits. |

## Doing it by hand

Every command here is one the script runs. `$` prompts are elided.

**1. A cluster.**

```sh
kind create cluster --name kuberecord-quickstart --config examples/quickstart/kind.yaml
```

**2. The operator image, side-loaded** so no registry sits on the critical path.
The tag is fixed because `examples/quickstart/operator` names it too.

```sh
make docker-build IMG=kuberecord/quickstart:local
docker save --platform linux/arm64 kuberecord/quickstart:local -o /tmp/image.tar
kind load image-archive /tmp/image.tar --name kuberecord-quickstart
```

Through a `docker save` archive rather than `kind load docker-image` because
published images are usually multi-platform indexes, and kind imports with
`--all-platforms`: containerd then fails looking for manifests that a
single-platform pull never fetched (`ctr: content digest sha256:…: not found`).
Substitute your own architecture for `arm64`. The script does the same for
ClickHouse and for `registry.k8s.io/pause`.

**3. The CRDs and the operator.** This is `config/default` — the same thing
`make deploy` installs — plus a pinned image, `--ch-auto-create-schema`, a
`CLUSTER_ID`, and the `restricted` Pod Security label. The overlay's own comment
explains each.

```sh
bin/kustomize build examples/quickstart/operator | kubectl apply --server-side -f -
kubectl -n kuberecord-system rollout status deploy/kuberecord-controller-manager
```

The operator is now running and **completely idle**: no sink, no rules, nothing
streamed, and no restart needed when that changes.

**4. Credentials, then a backend.** The Secret goes first — a sink that cannot
authenticate never reaches the sink runtime at all, so creating it in the other
order just means watching a condition go red and then green.

```sh
kubectl apply --server-side -f examples/quickstart/secret.yaml
kubectl apply --server-side -f examples/quickstart/clickhouse.yaml
kubectl -n kuberecord-quickstart rollout status deploy/clickhouse
```

**5. Point the operator at it.**

```sh
kubectl apply --server-side -f examples/quickstart/sink.yaml
kubectl wait --for=condition=Ready clickhousesink/default --timeout=3m
```

`Ready=True` means three separate things went right: the credentials resolved,
the server answered, and its tables match schema v1 — which the operator created
itself, because the overlay runs it with `--ch-auto-create-schema`. If it does
not go ready, `kubectl describe clickhousesink default` names which of the three
failed and why.

**6. Something to record, then the rule that records it.**

```sh
kubectl apply --server-side -f examples/quickstart/demo.yaml
kubectl -n quickstart-demo rollout status deploy/checkout-api

kubectl apply --server-side -f examples/quickstart/rule.yaml
kubectl wait --for=condition=Ready clusterstreamrule/quickstart --timeout=3m
kubectl get clusterstreamrule quickstart
```

The demo objects go in before the rule only to save time: a rule whose
`namespaceSelector` matches nothing yet is perfectly healthy — `Ready` with
`activeWatches: 0` — and picks the namespace up on its next resync. Correct, but
it would spend a reconcile interval proving it.

**7. Make some history** — but let the baseline land first.

`Ready=True` on the rule means the rule is valid, permitted and registered. It
does **not** mean the informer has finished its initial List. Change an object
before that lands and the change is folded into the object's *first* recorded
state: correct behaviour, and a demonstration of nothing. The script waits for
the two demo objects to appear in ClickHouse before touching them; by hand, look
for them first.

```sh
kubectl -n quickstart-demo scale deploy/checkout-api --replicas=3
kubectl -n quickstart-demo patch configmap checkout-config \
  --type=merge -p '{"data":{"feature_flags":"new-checkout=on"}}'
```

**8. Query it.**

```sh
kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000

clickhouse-client --host 127.0.0.1 --port 9000 \
  --user kuberecord --password quickstart --database kuberecord
```

Or without a local client, straight through the pod:

```sh
kubectl exec -n kuberecord-quickstart deploy/clickhouse -- \
  clickhouse-client --user kuberecord --password quickstart --database kuberecord \
  --query "SELECT ts, event_type, kind, namespace, name, actors
           FROM resource_states
           WHERE cluster_id = 'kuberecord-quickstart'
           ORDER BY ts DESC LIMIT 20
           FORMAT PrettyCompact"
```

You should see a first-sighting row for the Deployment and one for the ConfigMap,
each carrying the whole object, followed by `Modified` rows carrying an RFC 6902
diff of the scale-up and the flag flip — not a second copy of the object.

That first sighting reads `Added` once a scope has warmed its dedup cache from
the sink's own history, and `Snapshot` while it is still warming. On a brand-new
database there is no history to warm from, so either is normal here; the
distinction exists so that a restarting operator cannot re-announce a cluster it
already recorded. A `kube-root-ca.crt` ConfigMap appears too — Kubernetes injects
one into every namespace, and the rule streams the namespace, not a hand-picked
list of objects.

Two more worth running:

```sql
-- The flag flip as one RFC 6902 operation, not a second copy of the ConfigMap.
SELECT ts, name, diff
FROM resource_states
WHERE cluster_id = 'kuberecord-quickstart' AND kind = 'ConfigMap'
  AND name = 'checkout-config' AND event_type = 'Modified'
ORDER BY ts DESC LIMIT 1;

-- The demo ConfigMap was created with data.password = 'hunter2', and the sink's
-- redaction floor names data.password. One key survived; the other never arrived.
SELECT name,
       JSONExtractString(data, 'data', 'feature_flags') AS feature_flags,
       JSONExtractString(data, 'data', 'password')      AS password
FROM resource_states
WHERE cluster_id = 'kuberecord-quickstart' AND kind = 'ConfigMap'
  AND name = 'checkout-config' AND data != ''
ORDER BY ts ASC LIMIT 1;
```

## What is an evaluation shortcut, and what is not

Worth being explicit about, because the difference matters if you take any of
these files to a real cluster:

| Shortcut | Why it is fine here | What to do instead |
|---|---|---|
| ClickHouse on `emptyDir`, one replica | Deleting the cluster is meant to delete the data | Give it a PersistentVolumeClaim, or point the sink at a ClickHouse you already run — `spec.connection.addr` is all that changes |
| Password committed in two YAML files | Reachable only from inside a throwaway kind cluster | Sealed Secrets, External Secrets, SOPS, or a Kustomize `SecretGenerator` |
| `--ch-auto-create-schema` | Saves a step, and exercises a real code path | Apply [`deploy/clickhouse/schema/*.sql`](../../deploy/clickhouse/schema/) yourself; the operator then never runs DDL |
| The operator image built from your clone | It is the code you are evaluating | `helm install`, or `kubectl apply -f dist/install.yaml` — both install the same objects |

**Not** shortcuts, and identical to a production install: the RBAC (the
aggregated-ClusterRole model with only the `core-workloads` preset enabled), the
namespaced Secret grant, leader election, the authenticated metrics endpoint, the
`restricted` Pod Security Standard, and every line of the pipeline.

## Where to go next

- [`docs/QUERIES.md`](../../docs/QUERIES.md) — incident windows, drift by actor,
  flap reports, reconstructing an object's state at an instant.
- [`docs/SCHEMA.md`](../../docs/SCHEMA.md) — what every column means, and what
  `event_type` can be.
- [`docs/DASHBOARDS.md`](../../docs/DASHBOARDS.md) — the same questions as
  shipped Grafana dashboards.
- [`docs/CRDS.md`](../../docs/CRDS.md) — every field of the three custom
  resources, and every condition they report.
- [`docs/RBAC.md`](../../docs/RBAC.md) — how to grant a new kind in 30 seconds,
  with no operator restart.

## Troubleshooting

The script leaves the cluster up when it fails, and dumps the operator's log and
your custom resources before it exits. Beyond that:

| Symptom | Look at |
|---|---|
| `ClickHouseSink` not `Ready` | `kubectl describe clickhousesink default` — `CredentialsResolved`, `SchemaValid` and `Ready` each name their own reason |
| Rule not `Ready` | `kubectl describe clusterstreamrule quickstart` — `PolicyAllowed`, `ResourceResolved` and `RBACGranted`, each per-kind |
| Sink ready, rule ready, no rows | `kubectl logs -n kuberecord-system deploy/kuberecord-controller-manager` |
| `kind: command not found` | [kind's install guide][kind] |

[kind]: https://kind.sigs.k8s.io/docs/user/quick-start/#installation
