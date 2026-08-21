# Upgrading kuberecord

The operator is pre-1.0, so a **minor bump may break** — that is the whole of the
`v0.x` promise, and [`docs/RELEASING.md`](RELEASING.md) is the policy behind it.
This page is the other half of that promise: for every release that breaks
something, the steps to get from the version before it to the version after.

[`CHANGELOG.md`](../CHANGELOG.md) is the complete record of *what* changed. This
page is only the *what to do about it*, and it exists separately because upgrade
steps read badly interleaved with a list of changes — the step you skip is the one
you did not see.

The CRDs (`v1alpha1`) and the ClickHouse schema (`v1`, frozen) carry their own
version numbers and move independently of the operator's. No upgrade so far has
needed a ClickHouse migration, and none can while `v1` is frozen: no column is
renamed, retyped or removed, so an upgraded operator warms from an existing
database and de-duplicates against it exactly as the old one did.

## From v0.1.0 to v0.2.0

### `spec.sinkRef` → `spec.sink {kind, name}`

A rule used to name its sink with a bare string, `sinkRef: default`. It now names
a kind and a name:

```yaml
spec:
  sink:
    kind: ClickHouseSink   # optional; this is the default
    name: default
```

The kind is part of the reference because a name is only unique *within* a kind: a
`ClickHouseSink` named `default` and an `S3Sink` named `default` are two entirely
unrelated backends, and both are legal in etcd. See
[`docs/CRDS.md`](CRDS.md#validation-worth-knowing-about) for the field itself.

**This is not an in-place upgrade.** The field was renamed rather than retyped, so
once the v0.2.0 CRDs are installed the old spelling is an *unknown* field and the
API server prunes it. A rule inherited from v0.1.0 therefore decodes with `sink`
absent, and the reconciler refuses to guess: it parks the rule on
`Ready=False` with reason `LegacySinkRef`, withdraws its watch targets, and logs
at `Error` level. kuberecord registers no admission webhooks and needs no
cert-manager (D4), so there is nothing available to convert the stored object on
the way through. Defaulting the absent name to something plausible would have been
the alternative, and it is the one outcome worth more than a manual step: it would
stream a cluster's audit trail to a backend nobody chose, and say nothing.

Skipping the migration is safe, in the sense that it loses no data and takes
nothing else down: a parked rule degrades only itself, every other rule keeps
streaming, and the rule recovers the moment it is recreated. What it does not do
is stream. `kubectl describe` names the problem and the fix:

```console
$ kubectl describe streamrule payments-workloads -n payments
...
Status:
  Conditions:
    Type:     Ready
    Status:   False
    Reason:   LegacySinkRef
    Message:  This rule names no sink: spec.sink is empty, which is what a rule
              written against v0.1.0's spec.sinkRef string field looks like once
              that field is pruned as unknown. There is no conversion webhook and
              spec.sink is immutable, so it cannot be migrated in place: delete
              this rule and recreate it with spec.sink: {kind: ClickHouseSink,
              name: <the old sinkRef>}.
```

The message names both spellings on purpose: whoever wrote the rule will search
for the word they typed, and `kubectl explain` now only knows the other one.

#### The sequence

Steps 1 and 2 run **before** the upgrade, while the old schema still serves the
old field. Afterwards `spec.sinkRef` is an unknown field that the CRD's structural
schema does not describe and prunes on the next write — so do not count on being
able to read the name back out, and do not find out which way your cluster behaves
with the only copy of the mapping at stake.

```sh
# 1. Record which sink each rule names.
kubectl get streamrules.kuberecord.io -A \
  -o custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,SINK:.spec.sinkRef
kubectl get clusterstreamrules.kuberecord.io \
  -o custom-columns=NAME:.metadata.name,SINK:.spec.sinkRef

# 2. Keep the whole bodies too — `resources`, `labelSelector`, `extraRedaction`
#    and `namespaceSelector` are all re-applied unchanged, and re-deriving them
#    from memory is how a rule comes back narrower than it was.
kubectl get streamrules.kuberecord.io -A -o yaml        > rules-v0.1.0.yaml
kubectl get clusterstreamrules.kuberecord.io -o yaml    > clusterrules-v0.1.0.yaml

# 3. Delete the rules. Neither kind uses finalizers, so this is immediate; each
#    watch scope closes with a `Stopped` row in `watch_scopes` and *no* `Deleted`
#    rows for the objects it covered — "we stopped watching X" and "X was deleted"
#    are different truths and the schema records them differently, so the history
#    you already have stays true.
kubectl delete streamrules.kuberecord.io --all -A
kubectl delete clusterstreamrules.kuberecord.io --all

# 4. Upgrade the CRDs *and* the operator. With Helm the CRDs are a separate step,
#    because Helm installs `crds/` and never upgrades it: a plain `helm upgrade`
#    would leave the v0.1.0 schema serving, and step 5 would have its `sink` block
#    pruned straight back off again. (Same caveat as the chart's own README.)
kubectl apply -f <chart>/crds/
helm upgrade kuberecord <chart> -n kuberecord-system --reuse-values
#   or: kubectl apply -f dist/install.yaml   (carries the CRDs itself)
#   or: make deploy                          (carries the CRDs itself)

# 5. Re-apply each rule with the new field: `sinkRef: <name>` becomes
#    `sink: {kind: ClickHouseSink, name: <name>}`. Then watch them come up.
kubectl apply -f rules-v0.2.0.yaml
kubectl get streamrule -A
```

`kubectl get` now shows the kind in its own column, so a fleet of rules can be
checked at a glance:

```console
NAME                  READY   SINK      SINK-KIND        WATCHES   AGE
payments-workloads    True    default   ClickHouseSink   3         12s
```

A recreated rule warms its dedup cache from the sink's own history before it
trusts a cache miss, so an object that has not changed in the meantime is
deduplicated rather than re-recorded. That is the same mechanism that makes an
operator restart cheap, and it is why delete-and-recreate is the supported way to
move a rule rather than a compromise. What the migration does leave behind is a
`Stopped` and then a fresh `Started` row per scope in `watch_scopes` — an honest
record of a watch having been closed and reopened — and, for anything observed
before the warm completes, a `Snapshot` rather than an `Added` row, which is the
schema's way of saying "this is the state I found, not a change I witnessed".

### The `sink=` metric label is now `<Kind>/<Name>`

Every write-path metric a single sink reports carries a `sink` label, and its
value changed from the bare CR name to `sink.ID`'s rendering:

| Before | After |
|---|---|
| `sink="default"` | `sink="ClickHouseSink/default"` |

The kind is in the value for the same reason it is in the reference: two
same-named sinks of different kinds would otherwise merge into one series
describing neither.

Expressions that **group or sum** `by (sink)` keep working unchanged — only the
rendered value differs. Expressions that **match a value exactly** must be
updated, and this is the one to be careful about: a selector that no longer
matches anything reads on a dashboard exactly like a metric sitting at zero.

```promql
# before
sum(rate(kuberecord_writes_total{sink="default", outcome="failed"}[5m]))
# after
sum(rate(kuberecord_writes_total{sink="ClickHouseSink/default", outcome="failed"}[5m]))
```

The shipped Grafana dashboards and `deploy/prometheus/alerts.yaml` need no edits:
they group `by (sink)` and template the label rather than pinning a value. Only
expressions you wrote yourself are affected.
[`docs/OPERATING.md`](OPERATING.md#getting-the-metrics) has the full metric table.

## See also

- [`CHANGELOG.md`](../CHANGELOG.md) — every change, release by release, including
  the ones that break.
- [`docs/RELEASING.md`](RELEASING.md) — what each of the three version numbers
  promises, and what a tagged release publishes.
- [`docs/CRDS.md`](CRDS.md) — every field of the custom resources as they stand
  now.
- [`docs/SCHEMA.md`](SCHEMA.md#watch_scopes) — what a `Stopped` row does and does
  not claim, which is what makes step 3 above safe.
