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
  Deployments, ConfigMaps and Kubernetes Events into it;
- a demo Deployment and ConfigMap, changed once each, so there is real history to
  query rather than an empty table.

The run ends by printing the rows it recorded, the diff behind one of them, the
Events the cluster raised about the demo Deployment, proof that a redacted value
never reached the database, and the port-forward you need to keep querying.

## The files

| File | What it is |
|---|---|
| [`kind.yaml`](kind.yaml) | The kind cluster: one control-plane node, no pinned node image. |
| [`clickhouse.yaml`](clickhouse.yaml) | Namespace, credentials, Deployment and Service for a single-node ClickHouse. **Evaluation only** — `emptyDir` storage, one replica, a committed password. |
| [`secret.yaml`](secret.yaml) | The credentials Secret the operator reads, in the operator's own namespace — the only namespace it can read Secrets in. |
| [`sink.yaml`](sink.yaml) | The `ClickHouseSink` named `default`: where state goes, how the write path is sized, what may be written, and the redaction floor. |
| [`rule.yaml`](rule.yaml) | The `ClusterStreamRule`: what gets streamed, and from which namespaces. Read its comment on the `Event` entry before copying the file — that one is a sizing decision. |
| [`demo.yaml`](demo.yaml) | A namespace, a Deployment and a ConfigMap, so there is something to record. |
| [`operator/`](operator/) | A kustomize overlay: `config/default` plus four documented deltas. |
| [`quickstart.sh`](quickstart.sh) | The driver. Everything below, in order, with waits. |

## Doing it by hand

Every command here is one the script runs, except the two readers at the end:
the script prints those rather than holding a port-forward open on your behalf.
`$` prompts are elided.

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

# One extra grant: the rule below streams v1/Event, and the `core-workloads`
# preset the install ships does not cover `events`.
kubectl apply --server-side -f config/rbac/presets/events.yaml

kubectl -n kuberecord-system rollout status deploy/kuberecord-controller-manager
```

That second apply is the whole of "grant a new kind": the aggregated watch role
picks the preset up by label, so nothing is patched and nothing restarts — see
[`docs/RBAC.md`](../../docs/RBAC.md). It is a separate command rather than a
fifth line in the overlay because kustomize refuses a resource file above the
kustomization's own directory; the overlay's comment has the long version. Skip
it and the rule still applies, still validates, and reports
`RBACGranted=False` with the resource it could not read named in the message.

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

The rule streams three kinds: Deployments, ConfigMaps and `v1/Event`. The third
is there so step 8 can show `--with-events` doing something, and it is the entry
to think about before copying this file. Events are captured for the **whole**
watched scope and correlated to a subject at read time, and an Event bump is a
full row rather than a diff — the API server updates `count` in place, so hash
dedup cannot suppress it and a crash-looping pod writes a row per `BackOff`. In
one namespace holding three `pause` pods that is a few dozen rows; in a
cluster-wide rule during a bad rollout it is the dominant term in write volume.
The comment beside the entry in [`rule.yaml`](rule.yaml) says so at the point of
copying, and [`docs/SCHEMA.md`](../../docs/SCHEMA.md#kubernetes-events) has what
the rows look like.

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

**8. Read it back.**

The CLI is the read side: the same rows, addressed the way you address a
Kubernetes object rather than as SQL. One build from this clone gets you the
standalone name; the same bytes on your `PATH` as `kubectl-kuberecord` are what
make `kubectl kuberecord …` work, and a released install is the same build again
([`docs/CLI.md`](../../docs/CLI.md#installing)).

```sh
go build -o bin/kuberecord ./cmd/kubectl-kuberecord

# In another terminal, or with a trailing &: it holds the port until stopped.
kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000

bin/kuberecord timeline deploy/checkout-api -n quickstart-demo \
  --with-events --sink-addr 127.0.0.1:9000
```

```console
→ discovered ClickHouseSink/default (127.0.0.1:9000/kuberecord, address from --sink-addr)
→ cluster-id kuberecord-quickstart (from the operator Deployment kuberecord-system/kuberecord-controller-manager)
Kind:     apps/Deployment
Object:   quickstart-demo/checkout-api
Cluster:  kuberecord-quickstart
UID:      e1a6782e-b181-4c9c-9222-48480b61c21a
Coverage: 2026-09-06T20:54:37Z → open (clusterstreamrule//quickstart)

TIME (UTC)               EVENT     ACTOR                            CHANGE
2026-09-06 20:54:37.660  Snapshot  kube-controller-manager,kubectl  full state recorded (snapshot)
2026-09-06 20:54:37.661  Event     kube-controller-manager          ScalingReplicaSet: Scaled up replica set checkout-a…
2026-09-06 20:54:39.945  Modified  kube-controller-manager,kubectl  ~ spec.replicas: 1 → 3
2026-09-06 20:54:39.953  Event     kube-controller-manager          ScalingReplicaSet: Scaled up replica set checkout-a…
2026-09-06 20:54:39.954  Modified  kube-controller-manager,kubectl  11 ops
2026-09-06 20:54:39.980  Modified  kube-controller-manager,kubectl  + status.unavailableReplicas: 2
2026-09-06 20:54:39.998  Modified  kube-controller-manager,kubectl  2 ops
! 2 rows are shortened to fit the CHANGE column; pass --full to print every operation
```

That is the README's opening example, reproduced against a cluster you stood up
five minutes ago: `kubectl scale` at 20:54:39.945, and eight milliseconds later
the `ScalingReplicaSet` Event the controller raised in response. The `Event` rows
are not changes to the Deployment — they are what the cluster *said* about it,
matched to the object when the archive was read rather than when it was captured.
Drop `--with-events` and they go with it, leaving the object's own history.

A first sighting reads `Snapshot` here rather than `Added` because the scope was
still warming its dedup cache from an empty database; step 9 has the distinction.

The port-forward is the step that is easy to skip and impossible to skip twice.
The `ClickHouseSink` records `clickhouse.kuberecord-quickstart.svc:9000`, which
is the right address — it is what the operator, running inside this cluster,
dials — and it resolves nowhere else. `--sink-addr` replaces that one field and
nothing else: the database, the user and the credentials still come from the
sink the CLI just discovered, and the two `→` lines at the top of the output say
so.

They are worth reading rather than scrolling past, and the second is why nothing
here passes a `--cluster-id`: the overlay stamped `kuberecord-quickstart` on the
operator, and the CLI read it back off the Deployment.

Run it without the forward and you get the failure this step exists to pre-empt,
which names both ways out of itself: the forwarded port above, and, for a cluster
you come back to, `config set-profile --from-sink ClickHouseSink/default` — which
writes the address, database and user down once, so later runs need no flags.

The CLI never forwards a port itself. That needs `create` on `pods/portforward`,
a write verb, and an audit reader that cannot alter the cluster it audits is the
whole point of it. The long version, including why none of this applies to
reading an archive with `--source`, is [running the CLI outside the
cluster](../../docs/CLI.md#running-the-cli-outside-the-cluster).

**9. Or query it with SQL.** The forwarded port from step 8 serves both; the CLI
asks narrow questions about one object, and this is everything else.

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

Three more worth running:

```sql
-- The flag flip as one RFC 6902 operation, not a second copy of the ConfigMap.
SELECT ts, name, diff
FROM resource_states
WHERE cluster_id = 'kuberecord-quickstart' AND kind = 'ConfigMap'
  AND name = 'checkout-config' AND event_type = 'Modified'
ORDER BY ts DESC LIMIT 1;

-- What `--with-events` interleaves, in SQL: the Events naming checkout-api as
-- their subject. The correlation is this predicate — read time, not capture
-- time — and `diff` is empty on every row, because an Event is recorded as full
-- state each time the API server bumps its count.
SELECT ts, JSONExtractString(data, 'reason') AS reason,
       JSONExtractString(data, 'message')    AS message
FROM resource_states
WHERE cluster_id = 'kuberecord-quickstart' AND kind = 'Event'
  AND JSONExtractString(data, 'involvedObject', 'name') = 'checkout-api'
ORDER BY ts ASC LIMIT 10;

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
| Streaming `v1/Event` from the demo namespace | One namespace, three `pause` pods: a few dozen full-state rows, and the flag the README leads with has something to interleave | Keep Events namespaced with a `StreamRule` and size retention first. A cluster-wide Event rule is a sizing decision — see [`rule.yaml`](rule.yaml) and [`docs/SCHEMA.md`](../../docs/SCHEMA.md#kubernetes-events) |

**Not** shortcuts, and identical to a production install: the RBAC (the
aggregated-ClusterRole model, with the `core-workloads` and `events` presets
enabled and nothing else), the namespaced Secret grant, leader election, the
authenticated metrics endpoint, the `restricted` Pod Security Standard, and every
line of the pipeline.

## Where to go next

- [`docs/CLI.md`](../../docs/CLI.md) — every command `kubectl kuberecord` has,
  where it reads from, and [what to do when the address it discovers only
  resolves inside the
  cluster](../../docs/CLI.md#running-the-cli-outside-the-cluster).
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
| `--with-events` interleaves nothing | The CLI says which of the three states it is in, and prints the YAML if the answer is that no rule streams Events. If the rule does name `v1/Event`, check `RBACGranted` on it — the `events` preset from step 3 is the usual omission |
| `kuberecord timeline` reports `no such host` | The port-forward from step 8. The sink's address resolves inside the cluster only — the CLI prints both routes out of it, and [running the CLI outside the cluster](../../docs/CLI.md#running-the-cli-outside-the-cluster) is the long version |
| `kind: command not found` | [kind's install guide][kind] |

[kind]: https://kind.sigs.k8s.io/docs/user/quick-start/#installation
