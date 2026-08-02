# kubestream Dashboards

Four Grafana dashboards for reading what kubestream recorded. They are the
clickable form of [`docs/QUERIES.md`](QUERIES.md) — same questions, same frozen
columns, no UI to install beyond Grafana itself.

These are for reading **cluster history**. The dashboard for watching the
*operator* — queue depth, write outcomes, degraded rules — is a different one:
`operator-health.json`, documented in [`docs/OPERATING.md`](OPERATING.md).

| Dashboard | File | UID | Answers |
|---|---|---|---|
| Object Timeline | `deploy/grafana/object-timeline.json` | `kubestream-object-timeline` | "What happened to this one object, and what was Kubernetes saying about it?" |
| Drift by Actor | `deploy/grafana/drift-by-actor.json` | `kubestream-drift-by-actor` | "What changed that my GitOps controller did not do?" |
| Flap Report | `deploy/grafana/flap-report.json` | `kubestream-flap-report` | "What will not hold still?" |
| Namespace Activity | `deploy/grafana/namespace-activity.json` | `kubestream-namespace-activity` | "Where is this cluster moving?" |

## Prerequisites

- **Grafana.** These are written against `schemaVersion: 41`, the model Grafana 11
  writes. An older Grafana will still import them — it runs forward migrations and
  leaves fields it does not recognise alone — but will quietly ignore panel
  options it does not understand, so a panel may render plainer than intended.
- **The [ClickHouse data source
  plugin](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/)**
  (`grafana-clickhouse-datasource`), configured against the ClickHouse your
  `ClickHouseSink` writes to:

  ```console
  $ grafana-cli plugins install grafana-clickhouse-datasource
  ```

  A read-only ClickHouse user is enough — every query here is a `SELECT`.

## Installing

Import each file (**Dashboards → New → Import → Upload JSON file**) and pick your
ClickHouse data source when prompted. Nothing is pinned to one Grafana install:
every panel resolves its data source through the dashboard's own `Data source`
variable. The UIDs above are stable, so re-importing an updated copy replaces the
dashboard in place instead of creating a second one.

Then set the `Cluster` variable. It is the first thing every panel filters on —
`cluster_id` is the leading column of the table's sort key, so a query without it
both scans everything and mixes clusters together.

## Object Timeline

Pick a cluster, kind, namespace and name; set the time range to the incident.

| Panel | Read it when |
|---|---|
| Changes over time | Always first. The shape of the object's life: quiet, then a cluster of changes. |
| Field managers | You want to know who was involved before reading the diffs. |
| Latest recorded state | Checking whether what you have is current; `sha256` is what a reconstruction is verified against. |
| Rows and diffs | The actual answer. Every row kubestream wrote, newest first, with each change inlined. |
| Kubernetes Events for this object | The change rows alone do not explain it. Needs a rule streaming `v1/Event` or `events.k8s.io/v1/Event`. |

`Kind` is matched unqualified, without its API group, because kubestream's
identity key is version-agnostic and a kind name is unique in practice within a
cluster. If you genuinely run two same-named kinds from different groups, add an
`api_group` filter to the panels.

## Drift by Actor

Set `GitOps manager name` to the field-manager name your GitOps controller applies
under, exactly as it appears in `metadata.managedFields` — Argo CD uses
`argocd-controller`, Flux uses the name of the applying kustomize-controller.
Everything the dashboard calls drift is "changes without that name on them".

| Panel | Read it when |
|---|---|
| Share of changes the GitOps controller touched | Always. A reconciled cluster sits near 100%; the gap is the rest of this dashboard. |
| Objects with non-GitOps drift | After a freeze, or during an incident. Counts objects, not rows. |
| Top actors, GitOps excluded | "So who is it, then." |
| Modified rows by actor over time | Locating drift in time — a burst outside a deploy window. |
| Objects changed without the GitOps controller | The finding itself. Take a row to Object Timeline for the diffs. |

A manager name that matches nothing excludes nothing, so a mistyped value shows up
as a suspiciously low GitOps share rather than as an error. Two caveats carry over
from the schema: `actors` records field *ownership*, not authorship of a specific
edit, and manager names are self-declared, so a human edit can arrive as
`kubectl-edit`, `kubectl-client-side-apply`, or whatever a script set.

## Flap Report

`Flap threshold` is the number of changes within the dashboard's time range that
makes an object a flapper. Pick it against the width of the window: ten changes in
an hour is a fight, ten in a week is a deployment cadence.

| Panel | Read it when |
|---|---|
| Objects over the flap threshold | Always. Zero is the healthy reading. |
| Modified rows in window | For scale — is it a few objects churning or the whole cluster moving? |
| Flapping objects over time, against the threshold | Distinguishing bursts (a fight) from a flat line on the threshold (steady churn). |
| Objects by change count | The ranking. Two or more manager names on a high-count row is the signature of a fight. |

The threshold line on the timeline is a series the query returns, not a panel
setting. Grafana cannot interpolate a variable into a panel's threshold
configuration, so a static line would silently disagree with the box the moment
anyone changed it.

## Namespace Activity

| Panel | Read it when |
|---|---|
| Change volume by namespace | Always first. Hot cells are where to look. |
| Change volume by event type | Diagnosing a hot cell: `Modified` is churn, `Added` a rollout, `Deleted` a teardown, `Snapshot` an operator restart. |
| Kinds by volume | Before widening a rule — the top of this list is what will dominate storage. |
| Busiest namespaces | The heatmap as numbers. Many rows over few objects means churn; check the Flap Report. |

Cluster-scoped objects have an empty `namespace` in the schema and appear as
`(cluster-scoped)` rather than as a nameless row.

## Rendering against demo data

The load harness produces a cluster's worth of realistic history without a real
cluster, which is how these dashboards are checked visually. `make bench-load`
tears its ClickHouse down on exit, so for screenshots run the harness against a
container you keep:

```console
$ docker run -d --name kubestream-demo \
    -e CLICKHOUSE_USER=kubestream -e CLICKHOUSE_PASSWORD=kubestream \
    -p 19000:9000 -p 18123:8123 clickhouse/clickhouse-server:24.8

$ KUBEBUILDER_ASSETS="$(bin/setup-envtest use -i --bin-dir bin -p path)" \
  CH_TEST_ADDR=127.0.0.1:19000 CH_TEST_USER=kubestream CH_TEST_PASSWORD=kubestream \
  go test -tags=integration ./test/loadgen/ -run TestLoadGenChurn -v -timeout 45m -profile=small
```

The harness needs an envtest apiserver as well as ClickHouse; `-i` picks the
assets a previous `make test` already downloaded, so run that once first. The
`small` profile is the right one here — `massive` produces a better-looking
heatmap and takes tens of minutes to do it.

Point a ClickHouse data source at `localhost:9000` (or `localhost:18123` over
HTTP) and import the dashboards. The harness writes under `cluster_id = loadgen`,
so that is what the `Cluster` variable will offer. Its field managers are
synthetic, so set `GitOps manager name` to one that appears in the **Top actors**
panel before reading Drift by Actor as anything but a layout check.

Screenshots are a maintainer step, taken from a run like the one above and
committed alongside this page. CI does not render dashboards — it validates their
JSON, which is the part that can break silently.

## Validating changes to these files

Three checks, in increasing cost:

```console
$ make test                 # JSON Schema + panel/variable criteria (test/observability)
$ make verify-observability # the above, plus promtool over the alert rules
$ make test-integration     # every panel's SQL executed against a real ClickHouse
```

The last one is the one that matters when editing SQL. `test/queries` builds a
ClickHouse from the shipped DDL alone, seeds a small demo fixture, and runs every
statement in every dashboard *and* in `docs/QUERIES.md` against it — so a panel
that names a column outside the frozen schema, references a variable the dashboard
does not declare, or simply returns nothing, fails there rather than in front of a
user.
