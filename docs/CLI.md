# The kuberecord CLI

`kubectl kuberecord` answers questions about recorded Kubernetes state changes —
who changed what, when, and what the object looked like before — without needing
the cluster the change happened in to still exist.

It ships as one binary under two names. `kubectl-kuberecord` on your `PATH` makes
`kubectl kuberecord …` work; the same build installed as `kuberecord` works
standalone, which is what an auditor with an object-store archive and no cluster
access wants. Both are built from the same package and behave identically, down to
naming themselves correctly in their own help text.

This page is the reference for **the commands**, **where the CLI reads from** and
**how it is configured**.

- [`timeline`](#timeline)
- [`diff`](#diff)
- [`get --at`](#get---at)
- [`scopes`](#scopes)
- [Structured output](#structured-output)
- [Where the data comes from](#where-the-data-comes-from)
- [The cluster identity](#the-cluster-identity)
- [The configuration file](#the-configuration-file)
- [The read-only ClickHouse user](#the-read-only-clickhouse-user)
- [What the CLI asks of Kubernetes](#what-the-cli-asks-of-kubernetes)
- [Evaluation mode](#evaluation-mode)
- [Exit codes](#exit-codes)

## `timeline`

```console
$ kubectl kuberecord timeline deploy/checkout -n payments
→ discovered ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)
→ cluster-id prod-eu-1 (from the operator Deployment kuberecord-system/kuberecord-controller-manager)
Kind:     apps/Deployment
Object:   payments/checkout
Cluster:  prod-eu-1
UID:      7c9e6679-7425-40de-944b-e07fc1f90ae7
Coverage: 2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)

TIME (UTC)               EVENT     ACTOR                      CHANGE
2026-08-28 14:09:40.900  Modified  unknown                    ~ metadata.…deployment.kubernetes.io/revision: 1 → 2
2026-08-28 14:05:02.117  Modified  kube-controller-manager    ~3 ops
2026-08-28 14:03:11.482  Modified  kubectl-client-side-apply  ~ spec.…containers[0].resources.limits.memory: 2Gi → 512Mi
2026-08-28 14:02:58.001  Added     kubectl-client-side-apply  full state recorded
```

The header and the table go to **stdout**; every banner, notice and explanation
goes to **stderr**. One sentence: stdout is the data, stderr explains it. That is
what makes `timeline … | wc -l` count changes.

### Flags

| Flag | What it does |
|------|--------------|
| `--since`, `--until` | Bound the window. Either a duration — `90m`, `6h`, `3d`, `2w`, `1d6h` — or an instant: `2026-08-20`, `2026-08-20 14:00:00`, `2026-08-20T14:00:00Z`. Both read as *ago*. |
| `--limit` | At most this many changes, newest first. Default `100`; `0` means no limit. |
| `--reverse` | Show the same changes oldest first. It reorders rows; it does not select different ones. |
| `--actor`, `--exclude-actor` | Field-manager predicates. Repeatable. `--exclude-actor` is applied second and wins on conflict. |
| `--field` | Field-path prefixes, repeatable. Either spelling works: `spec.containers[0].image` or `spec.containers.0.image`. |
| `--uid` | Pin the timeline to one incarnation. |
| `--all-incarnations` | Show every incarnation in the window, with a `UID` column. |
| `--full` | Print every operation of every patch, unshortened. |
| `--with-events` | Interleave the Kubernetes Events recorded about the object. |

`-o wide` adds the full UID and the resource version, and prints timestamps at the
nanosecond precision the schema records.

### How a change is summarized

A one-operation patch is one line, with the operation's glyph — `+` added, `-`
removed, `~` replaced — the field path, and the values. A larger patch is
summarized as `~N ops`, and `--full` expands it:

```console
$ kubectl kuberecord timeline deploy/checkout -n payments --full
2026-08-28 14:05:02.117  Modified  kube-controller-manager    ~3 ops
    ~ spec.replicas: 3 → 5
    + spec.paused: true
    - spec.minReadySeconds: 10
```

Paths are RFC 6901 JSON Pointers converted to a dotted form with bracketed array
indices, elided in the middle when they exceed the column. The head is kept
because a change under `spec` and one under `status` are different news; the tail
is kept because that is what names the field.

**The value on the left of the arrow is reconstructed, not recorded.** An RFC 6902
operation carries the value it wrote and not the one it replaced, so the CLI
anchors a single `StateAt` just before the oldest change it is about to show and
replays the patches forward in memory — one round trip, not one per row. Three
things follow, and each is announced on stderr rather than left to be inferred:

- With `--actor`, `--exclude-actor` or `--field` in force the rows shown are **not
  consecutive**, so the arrow is dropped. Replaying only the surviving patches
  over a real base state would produce a document the object was never in, and the
  numbers read out of it would be confident and wrong.
- If the base has aged out of the retention window, or the backend cannot
  reconstruct state, the new value is still exact and the old one is absent.
- A patch that will not apply to the reconstructed state stops the replay at that
  row, and the notice names the row.

### Incarnations

Kubernetes reuses names. A `(namespace, name)` pair may have belonged to several
objects with different UIDs, and a timeline that spliced their histories together
would be a coherent-looking account of something that never happened. So the
newest incarnation is shown, the header names its UID, and the others are named
too:

```
! payments/checkout has had 2 incarnations in this window; showing the newest
  (7c9e6679-…). Pass --all-incarnations to see them all, or --uid to pin one
```

`--all-incarnations` adds a `UID` column, so no two of them can blur together in
one table.

### An empty result is never presented on its own

Every empty timeline is explained against the watch scopes that were open at the
time, and there are exactly three answers:

| What was found | What you get |
|----------------|--------------|
| The scope was watched across the whole window | `no changes recorded … The scope was confirmed watched over <interval>` — the silence is real. |
| The scope opened *after* the window started | A warning naming that instant and the rule that opened it: a change before then would not have been recorded. |
| No scope ever covered it | Exit **3**, and a message saying so. This is a finding, not an empty result — [`scopes`](#scopes) is where you go next. |

A backend with no scope log to read says that instead, and exits `0`: it cannot
tell the three apart, and pretending otherwise would be the failure this section
exists to prevent.

### What a backend cannot record

An object archive holds no deletions at all (see [`docs/TEE.md`](TEE.md) and the
S3 tier's design), so a timeline over one always simply stops. Without saying so,
that silence reads as "the object is still there":

```
! the objectsource backend does not record deletions, so this timeline ending is
  not evidence that the object still exists; it may have been deleted while
  unobserved. Check what was being watched with `scopes`
```

The same backend cannot answer an unbounded question either, so a window is
supplied — the last seven days — and announced. `--since` widens it.

### Colour, width and paging

Colour follows `--color=auto|always|never`. Under `auto` it is on only when stdout
is a terminal and `NO_COLOR` is unset; `--color=always` overrides `NO_COLOR`,
which is what the flag is for. The table is laid out to the terminal's width, or
to 120 columns when output is not a terminal. **There is no pager**: output goes
to stdout and stays there, so `| less -R` is yours to choose.

### Reading an archive without a cluster

`--source` needs no cluster, but resolving `deploy` into `apps/Deployment` does:
short names and plural resource names come from the server's own discovery data.
When the cluster cannot be reached, give the kind as it is recorded and no
discovery is needed:

```console
$ kuberecord timeline Deployment.apps/checkout -n payments --source ~/archives/kuberecord
```

Without `-n`, an address resolved this way is read as cluster-scoped.

## `diff`

The detail view, once `timeline` has named a suspect. It asks the backend the
same question `timeline` asks — same window, same incarnation, same coverage
consultation — and spends the whole page on the answer instead of one column of
it.

```console
$ kuberecord diff deploy/checkout -n payments --since 2h
Kind:     apps/Deployment
Object:   payments/checkout
Cluster:  prod-eu-1
UID:      7c9e6679-7425-40de-944b-e07fc1f90ae7
Coverage: 2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)

2026-08-28 14:05:02.117 UTC  Modified  kube-controller-manager
  ~ spec.replicas
      - 3
      + 5
  + spec.paused
      + true
  - spec.minReadySeconds
      - 10

2026-08-28 14:03:11.482 UTC  Modified  kubectl-client-side-apply
  ~ spec.template.spec.containers[0].resources.limits.memory
      - 2Gi
      + 512Mi
```

`+` is green, `-` is red, `~` is yellow.

### Flags

| Flag | Meaning |
|------|---------|
| `--since` | Only changes at or after this point: a duration (`6h`, `90m`, `3d`, `2w`) or an instant (`2026-08-20`, `2026-08-20T14:00:00Z`). |
| `--until` | Only changes at or before this point, in the same forms. |
| `--limit` | Examine at most this many changes, newest first. Default 100; zero means no limit. |
| `--reverse` | Oldest first. It reorders the blocks; it does not select different ones. |
| `--uid` | Pin the diff to one incarnation. |
| `--field` | Only changes touching one of these paths, matched by prefix, with every hunk of those changes. |
| `--full` | Print every operation and every value in full. |
| `--exit-code` | `0` when there are no changes, `1` when there are, as `git diff` does. |

### The old value is reconstructed, not stored

A recorded patch is RFC 6902, which carries the *new* value and nothing else. The
value on the left of each hunk comes from replaying the object's state up to that
change — one reconstruction per incarnation, not one per row. Where that replay
could not run, the hunk says `- (prior value not established)` rather than
leaving the field looking as though it had no value before, and a notice on
stderr says why.

This is why `--field` narrows what is *shown* rather than what is fetched. A path
predicate pushed into the query would make the returned rows a non-consecutive
slice of history, and replaying only those patches would report values the object
never held. So the query goes out unfiltered, the replay runs over everything it
needs, and a notice reports how many changes were examined — `--limit` bounds the
changes examined, not the ones shown.

### Redacted values are marked

A value that reads `[REDACTED]` is what is **stored**: redaction happens on the
way in, before hashing, so nothing downstream can tell it from a ConfigMap whose
value genuinely is that string. `diff` renders it dim and marks it:

```
  ~ data.password
      - [REDACTED]  (redacted by policy)
      + [REDACTED]  (redacted by policy)
```

See [`docs/SCHEMA.md`](SCHEMA.md#redaction) for what follows from that — in
particular that two states differing *only* in a redacted value produce one row,
not two.

### Nothing fills the terminal

A value over 200 characters is cut with `…(N more bytes, --full)`, and a change
touching more than 20 fields shows the first 20 and counts the rest. A fat
PodTemplate or a CRD carrying a large OpenAPI schema is exactly the case this
exists for. `--full` prints everything.

### `--exit-code`

`git diff`'s contract: `0` for no changes, `1` for changes found. Nothing prints
`error:` for it — the exit code is a finding, not a failure, and the notice
beside the document says so.

Exit `3` still outranks it. A script told "no changes" when nothing was ever
watching has been given the one answer [Invariant 9](#an-empty-result-is-never-presented-on-its-own)
exists to prevent, so a scope nobody watched is reported as such whatever
`--exit-code` asked for.

## `get --at`

What did this look like before?

```console
$ kuberecord get deploy/checkout -n payments --at 2h
# Reconstructed state — NOT A DEPLOYABLE MANIFEST.
#
# object:          apps/Deployment payments/checkout
# cluster:         prod-eu-1
# uid:             7c9e6679-7425-40de-944b-e07fc1f90ae7
# at:              2026-08-28T13:00:00Z
# base row:        2026-08-28T14:05:02Z (Checkpoint)
# patches applied: 0
# coverage:        2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)
#
# This is what kuberecord recorded, not what the API server held. Do not
# `kubectl apply -f` it: metadata.managedFields, metadata.resourceVersion and
# metadata.generation were stripped at capture, and every field a redaction
# policy covers carries the sentinel [REDACTED] in place of its value.
apiVersion: cli.kuberecord.io/v1alpha1
kind: Object
metadata:
  backend: clickhouse
  cluster_id: prod-eu-1
  coverage: ...
items:
- at: "2026-08-28T13:00:00Z"
  base_event: Checkpoint
  base_ts: "2026-08-28T14:05:02.117Z"
  object:
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: checkout
      namespace: payments
    ...
  patches_applied: 0
  sha256: 283f5a59…
  uid: 7c9e6679-7425-40de-944b-e07fc1f90ae7
```

The state is at `.items[0].object`, inside the same [envelope](#structured-output)
every other command answers in — so `kubectl apply -f` on this file fails loudly
instead of applying a stripped object, which is the strongest form of the warning
above it. `jq -r '.items[0].object'`, or `yq '.items[0].object'`, is the whole
document back.

| Flag | Meaning |
|------|---------|
| `--at` | The instant to reconstruct for: a duration or an instant, as `--since` takes. Defaults to now, which is the newest recorded state. |
| `--uid` | Pin the reconstruction to one incarnation. Empty means the newest incarnation alive at `--at`, never a blend of two. |
| `--verify` | Re-hash the reconstruction and compare it against the digest recorded for it. |

`-o` is `yaml` (the default), `json` or `jsonl`. There is nothing for the tabular
formats to lay out — a reconstructed object is a document, not a row.

### The header is not a courtesy

What comes out looks exactly like a manifest, and the obvious next thing to do
with it is `kubectl apply -f`. That would be wrong in three ways at once, none of
them visible in the document: volatile metadata was stripped before the state was
recorded, redacted fields carry a sentinel instead of their values, and the
document describes a past somebody deliberately moved the object out of. So the
header is mandatory and says so in those words. JSON has no comment syntax, so
for `-o json` and `-o jsonl` the identical block goes to **stderr** and stdout
stays something `jq` can read.

The provenance in it is not diagnostics. A state assembled from a base an hour
old and two patches deserves more confidence than one assembled from a base three
months old and four hundred, and `base row` and `patches applied` are what let a
reader judge which they have.

### `--verify`

Every row carries the SHA-256 of the state it recorded. `--verify` canonicalizes
the reconstruction — re-serializing it with sorted object keys, the procedure
[`docs/SCHEMA.md`](SCHEMA.md) specifies — hashes it, and compares:

```console
$ kuberecord get deploy/checkout -n payments --at 2h --verify
! verified: the reconstructed state hashes to 9f2b…, which is the digest recorded for it
```

A mismatch exits `1` and names both digests. It means the archive and the replay
disagree about what this object looked like, which is a chain-of-custody finding
and not a rounding error. A row carrying no digest is reported as unverifiable
rather than passed: reporting success there would be inventing an assurance
nobody gave.

`--verify` is an assertion rather than an annotation — the document is written
only if the check holds. `kuberecord get … --verify > object.yaml` is how this
flag gets used, and a disputed reconstruction is the last thing that should land
in that file.

## `scopes`

What was being recorded, and when. This is the compliance view, and it is the
command every other command's empty result points at.

```console
$ kubectl kuberecord scopes -n payments
Cluster: prod-eu-1
Scope:   every kind in namespace payments
Window:  all recorded history

KIND             NAMESPACE  FROM                     TO                       RULE
apps/Deployment  payments   2026-06-01 08:00:00.000  2026-07-02 09:14:00.000  StreamRule/payments/workloads
apps/Deployment  (all)      2026-07-02 09:14:00.000  (open)                   ClusterStreamRule/all-workloads
ConfigMap        payments   2026-07-02 09:14:00.000  2026-08-11 17:31:22.000  (not recorded)
```

| Flag | Meaning |
|------|---------|
| `--kind` | Only scopes for this kind. Takes what the object commands take — `deploy`, `deployments.apps`, `Deployment.apps` — and needs a cluster for the first two. |
| `-n`, `--namespace` | Only scopes covering this namespace, cluster-wide rules included. Without it, **every** namespace: unlike the object commands, the kubeconfig's current namespace does not narrow a compliance question. |
| `--since`, `--until` | Only periods overlapping this window. A period that merely overlaps is shown **whole**. |

`-o` is `table` (the default), `wide`, `json`, `jsonl` or `yaml`. `wide` widens
the timestamps to the nanosecond precision the schema records.

### Reading a row

Three cells say something a blank would not:

- **`(open)`** in `TO` means the scope is being watched *now*. There is no
  recorded end because there has not been one.
- **`(all)`** in `NAMESPACE` is the all-namespaces scope, not a missing value. A
  cluster-wide rule was watching objects in every namespace, including the one
  you asked about — which is why it appears in a namespaced listing, with a
  notice on stderr saying so. Your `--namespace` was not ignored.
- **`(not recorded)`** in `RULE` is an interval closed by a recovery pass whose
  rule no longer exists. That is a real state, and a blank there would read as a
  rule named by the empty string.

### An interval's end is not a deletion

The end of a period says **the recorder stopped watching**. It says nothing
whatever about the objects in that scope: they may still exist, they may have
been deleted three weeks later, and this table cannot tell you which. That is the
whole reason it exists — everything after an interval's end is unobserved, and
unobserved is not the same as unchanged.

### An empty listing is a finding

No periods means nothing was ever watching what you asked about, so no silence
anywhere in this cluster's history means what it appears to mean for that scope.
That is exit **3**, the same code `timeline` reaches when it works the fact out
from the other end, so one script keys on one code whichever command it asked:

```console
$ kuberecord scopes --kind Secret -n payments
Cluster: prod-eu-1
Scope:   Secret in namespace payments
Window:  all recorded history
error: no watch coverage recorded for the requested scope: no watch scope covering
Secret in namespace payments was open in cluster "prod-eu-1" during all recorded
history, so a silence there is not evidence that nothing changed — nothing was
being recorded to change
```

(For `Secret` specifically the answer is permanent: it is hard-denied as a
watchable kind, D8.)

A backend with no scope log at all cannot answer this command — there is no other
half of the question to fall back to — so it exits `1` naming the backend, rather
than printing an empty table that would read as "nothing was watching".

## Structured output

`-o json`, `-o jsonl` and `-o yaml` produce a **versioned envelope**, and it is a
public contract (D19). People script against this; a field renamed a release later
breaks a runbook silently, because `jq` reports nothing for a path that no longer
exists and the pipeline keeps running while producing empty findings.

```json
{
  "apiVersion": "cli.kuberecord.io/v1alpha1",
  "kind": "Timeline",
  "metadata": {
    "cluster_id": "prod-eu-1",
    "backend": "clickhouse",
    "coverage": {
      "available": true,
      "summary": "2026-07-02T09:14:00Z → open (ClusterStreamRule/all-workloads)",
      "intervals": [
        {
          "api_group": "apps",
          "kind": "Deployment",
          "namespace": "",
          "rule_ref": "ClusterStreamRule/all-workloads",
          "from": "2026-07-02T09:14:00Z",
          "to": null
        }
      ]
    }
  },
  "items": [
    {
      "ts": "2026-08-28T14:03:11.482Z",
      "event_type": "Modified",
      "actors": ["kubectl-client-side-apply"],
      "uid": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
      "resource_version": "1002",
      "api_version": "apps/v1",
      "data": "",
      "diff": "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/resources/limits/memory\",\"value\":\"512Mi\"}]",
      "sha256": "",
      "labels": {}
    }
  ]
}
```

### The four kinds

| `kind` | Produced by | What one item is |
|--------|-------------|------------------|
| `Timeline` | `timeline` | One recorded change, as the schema stores it. |
| `Diff` | `diff` | The same, plus `hunks` and `patch_error`. |
| `Object` | `get` | One reconstruction: `object` plus the provenance for it. |
| `Coverage` | `scopes` | One watch-scope interval. |

`items` is always a list, including when it is empty. `metadata` carries exactly
those three fields.

### Item field names are the schema's column names

`ts`, `event_type`, `actors`, `uid`, `resource_version`, `api_version`, `data`,
`diff`, `sha256`, `labels` — spelled exactly as
[`docs/SCHEMA.md`](SCHEMA.md) spells the columns, because the two are the same
data reached two ways. A `jq` recipe written against a SQL result transfers here
unchanged, which is the point of the mirroring rather than a detail of it. An
empty `actors` or `labels` is `[]` and `{}`, never `null`.

A `Diff` item adds `hunks`, one per patch operation:

```json
{ "op": "replace",
  "path": "spec.template.spec.containers[0].resources.limits.memory",
  "pointer": "/spec/template/spec/containers/0/resources/limits/memory",
  "old": "2Gi", "old_known": true, "new": "512Mi" }
```

`path` is the dotted grammar `--field` accepts and the table prints; `pointer` is
RFC 6901 as recorded, for a JSON Patch library. **Read `old_known`, not `old`**:
`old` is `null` both when the value really was JSON null and when the state replay
could not establish it, and those are different facts. A redacted value arrives as
the literal sentinel `[REDACTED]`.

### `metadata.coverage` on every command that queries changes

This is Invariant 9 in a form a script can branch on. Two answers with zero items
mean opposite things, and one field tells them apart without a second query:

| `available` | `intervals` | What it means |
|-------------|-------------|---------------|
| `true` | non-empty | The scope was watched. An empty `items` means nothing changed. |
| `true` | `[]` | **Nothing was ever watching.** An empty `items` means nothing was recorded. Exit `3`. |
| `false` | `[]` | The backend has no scope log and cannot say which of the two you have. |

### The additive-only policy

Within one `apiVersion`, fields may be **added** and are never renamed, removed or
repurposed — the same policy the frozen schema carries. Consumers must ignore
fields they do not recognize. Anything else is a new `apiVersion`.

### `jsonl` streams

`-o jsonl` writes the envelope head on the first line and then one item per line,
as each one arrives from the backend. Memory does not scale with the result, so a
six-figure timeline can be piped into something that reads it a line at a time:

```
{"apiVersion":"cli.kuberecord.io/v1alpha1","kind":"Timeline","metadata":{…}}
{"ts":"2026-08-28T14:09:40.9Z","event_type":"Modified",…}
{"ts":"2026-08-28T14:05:02.117Z","event_type":"Modified",…}
```

The head line carries no `items` key — it cannot, since nothing has been read yet
— but it carries the full `metadata`, coverage included, so a consumer processing
the stream as it arrives already knows whether an empty stream means "nothing
changed" or "nothing was watching".

One case holds items back, and it is bounded by a number you typed rather than by
the result: `--reverse` with `--limit N` has to read the newest N changes before
it can write the oldest of them, so at most N are held. Without a limit the two
orderings select the same changes, so the query is simply asked oldest-first and
nothing is held at all.

`json` and `yaml` are single documents and are complete before they are written,
which is what a single document means.

### Two recipes

The flagship question — who changed what — as one line per change:

```console
$ kubectl kuberecord timeline deploy/checkout -n payments -o jsonl \
  | jq -r 'select(.ts) | "\(.ts)  \(.actors | join(","))  \(.diff)"'
2026-08-28T14:05:02.117Z  kube-controller-manager    [{"op":"replace","path":"/spec/replicas",…
2026-08-28T14:03:11.482Z  kubectl-client-side-apply  [{"op":"replace","path":"/spec/template/spec/…
```

`select(.ts)` is what skips the head line: it is the only line with no `ts`.

Or, with the field paths already decoded, from `diff`:

```console
$ kubectl kuberecord diff deploy/checkout -n payments --since 24h -o json \
  | jq -r '.items[] | . as $c | .hunks[] | "\($c.ts)  \($c.actors[0])  \(.path): \(.old) → \(.new)"'
2026-08-28T14:05:02.117Z   kube-controller-manager    spec.replicas: 3 → 5
2026-08-28T14:03:11.482Z   kubectl-client-side-apply  spec.template.spec.containers[0].resources.limits.memory: 2Gi → 512Mi
```

An operation whose prior value the replay could not establish renders `null`
there too, which is why a script that cares reads `old_known` rather than testing
`old`.

Telling an empty result from an unobserved one, in a script that must not confuse
them:

```console
$ result=$(kubectl kuberecord timeline deploy/checkout -n payments --since 24h -o json)
$ jq -r 'if (.items | length) > 0 then "changed"
         elif .metadata.coverage.available | not then "backend cannot say"
         elif (.metadata.coverage.intervals | length) == 0 then "NOT WATCHED"
         else "watched, unchanged" end' <<<"$result"
```

Exit code `3` carries the same finding for a script that would rather branch on
that; the envelope is still written to stdout, so both work.

## Where the data comes from

Four steps, first match wins, and **the chosen source is always printed on
stderr**:

| Step | How | When it is the right one |
|------|-----|--------------------------|
| 1 | `--source <dir\|s3://bucket/prefix>` | An archive you can reach directly. No cluster, no CRs, no kubeconfig. |
| 2 | `--sink <kind>/<name>` | A cluster with more than one sink, or one you want to name explicitly. |
| 3 | The active profile | You cannot read the sink's Secret — which is most people (see below). |
| 4 | A discovered sink custom resource | The common case: exactly one `ClickHouseSink` or `S3Sink` in the cluster. |

Every one of them announces itself:

```console
$ kubectl kuberecord timeline deploy/checkout -n payments
→ discovered ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)
→ cluster-id prod-eu-1 (from the operator Deployment kuberecord-system/kuberecord-controller-manager)
```

Those lines go to **stderr**, so `-o json | jq` never receives them. They are not
optional: a tool that silently picked between four sources would eventually read
the wrong one and be believed, and for an audit trail being believed while wrong is
the worst available failure.

**Nothing prints a credential, at any verbosity.** The notice names the host, the
database and the sink; never the password, never the access key.

### `--source`

```console
$ kubectl kuberecord scopes --source ~/archives/kuberecord
$ kubectl kuberecord scopes --source s3://acme-audit/kuberecord
```

A plain path or a `file://` URL is a directory holding `format=jsonl-v1/`. An
`s3://` URL is a bucket and an optional prefix.

For a bucket, credentials come from the **AWS credential chain** — environment,
`~/.aws/config`, SSO, an instance role — because every tool on the machine already
reads it and kuberecord has no business owning a second one. `AWS_REGION` (or
`AWS_DEFAULT_REGION`) selects the region, defaulting to `us-east-1`, which MinIO
ignores; `AWS_ENDPOINT_URL_S3` is honoured by the SDK itself. Set
`AWS_S3_FORCE_PATH_STYLE=true` for a MinIO deployment that needs
`<endpoint>/<bucket>/<key>` addressing — or, better, put the endpoint and the
addressing style in a profile once.

### Discovery, and why it degrades

Discovery reads the `ClickHouseSink` and `S3Sink` custom resources through your
kubeconfig, and resolves the sink's `credentialsSecretRef` from the operator's
namespace. A `ClickHouseSink` needs the Secret; an `S3Sink` authenticating from the
ambient chain — which is the preferred state on a cloud provider — needs nothing at
all.

That Secret is the catch. The operator's aggregated ClusterRole grants Secret reads
in its own namespace and nowhere else, and that narrowness is deliberate
([`docs/RBAC.md`](RBAC.md)); most engineers are not granted the same. When the read
is refused the CLI says exactly that:

```
error: cannot read Secret kuberecord-system/clickhouse-credentials (forbidden);
configure a profile with `kubectl kuberecord config set-profile`
```

Not "connection failed". The database is fine; your permissions are what stopped
the query, and the remedy — a profile naming a read-only user — needs no new grant
from anybody.

The namespace it looks in is `--operator-namespace`, then `operatorNamespace` in
the configuration file, then the namespace of the operator's Deployment if it can
be found by label, then `kuberecord-system`.

## The cluster identity

Every recorded row carries a `cluster_id`: a string chosen when the operator was
installed. It is **not** a kubeconfig cluster entry, which is why the flag is
`--cluster-id` and `--cluster` remains kubectl's own.

It is resolved by five steps, and the answer is printed:

1. `--cluster-id`.
2. The configuration file's mapping for the current kubeconfig context —
   `kuberecord config set-context-cluster-id`.
3. The operator's Deployment in the target cluster, which carries `CLUSTER_ID` in
   its environment (as the Helm chart sets it) or `--cluster-id` in its arguments.
   This is what makes the ordinary case need no configuration at all.
4. The sink itself, if it holds exactly one cluster's history. This is what makes
   an archive on a laptop need no configuration either.
5. An error **listing the values that are there**:

   ```
   error: no cluster identity: this sink holds 3 of them (prod-eu-1, prod-us-1,
   staging). Pass --cluster-id, or record it for this kubeconfig context with
   `kubectl kuberecord config set-context-cluster-id`
   ```

Step 3 is a convenience and never a requirement: an unreachable cluster, a
forbidden Deployment list, or an operator running on the built-in default all
produce a notice on stderr and continue to step 4.

## The configuration file

`${XDG_CONFIG_HOME:-~/.config}/kuberecord/config.yaml`, mode `0600`.

```yaml
apiVersion: cli.kuberecord.io/v1alpha1
kind: Config

# Used when neither --source nor --sink is given. Overridden by --profile.
currentProfile: prod

# Where a sink's credentials Secret and the operator's Deployment are looked for.
# Optional: the CLI searches for the Deployment, then falls back to kuberecord-system.
operatorNamespace: kuberecord-system

profiles:
  prod:
    backend: clickhouse            # clickhouse | s3 | local
    clickhouse:
      addr: clickhouse.example:9000
      database: kuberecord
      username: kuberecord_ro
      passwordEnv: KUBERECORD_CLICKHOUSE_PASSWORD   # or passwordFile: /run/secrets/ch
      tls: true

  archive:
    backend: s3
    s3:
      bucket: acme-audit
      prefix: kuberecord           # no leading or trailing slash
      region: eu-west-1
      endpoint: https://minio.internal:9000   # scheme is mandatory
      forcePathStyle: true

  laptop:
    backend: local
    local:
      path: /home/you/archives/kuberecord
      prefix: ""                   # if the archive was written under one

# kubeconfig context name → kuberecord cluster identity.
contexts:
  prod-eu: prod-eu-1
  kind-kuberecord: local-kind-cluster
```

A few rules the file enforces rather than documents:

- **A password is never stored inline.** `clickhouse.password` is refused with an
  explanation pointing at `passwordEnv` and `passwordFile`. This file gets
  committed to dotfile repositories, synced between machines and pasted into
  issues; a credential in it stops being a credential.
- **S3 credentials are not in this file at all.** They come from the AWS chain.
  `accessKeyId`, `secretAccessKey` and `sessionToken` are refused by name.
- **An unset environment variable is an error, not an empty password.** A profile
  naming `KUBERECORD_CLICKHOUSE_PASSWORD` in a shell that never exported it says so
  here, instead of authenticating as nobody and failing three steps later.
- **A profile carries exactly the one stanza its `backend` names**, and unknown
  fields are refused — a typo that silently did nothing would be worse.

### `kuberecord config`

```console
# Write a profile. The first one in an empty file becomes the active one.
$ kuberecord config set-profile prod --backend clickhouse \
    --addr clickhouse.example:9000 --database kuberecord \
    --username kuberecord_ro --password-env KUBERECORD_CLICKHOUSE_PASSWORD

$ kuberecord config set-profile archive --backend s3 --bucket acme-audit \
    --prefix kuberecord --endpoint https://minio.internal:9000 --force-path-style

$ kuberecord config set-profile laptop --backend local --path ~/archives/kuberecord

# Choose the active one.
$ kuberecord config use-profile archive

# Record which kuberecord cluster a kubeconfig context reads.
$ kuberecord config set-context-cluster-id prod-eu-1          # the current context
$ kuberecord config set-context-cluster-id prod-eu prod-eu-1  # a named one

# Print the file. The document goes to stdout, its path to stderr.
$ kuberecord config view
$ kuberecord config view -o json | jq .profiles
```

## The read-only ClickHouse user

**This is the recommended posture, and it is what a profile should name.** The CLI
never writes: give it a credential that cannot.

```sql
CREATE USER kuberecord_ro IDENTIFIED WITH sha256_password BY 'a-password-you-generated';

-- The two tables of the frozen v1 schema, and nothing else.
GRANT SELECT ON kuberecord.resource_states TO kuberecord_ro;
GRANT SELECT ON kuberecord.watch_scopes   TO kuberecord_ro;

-- Belt and braces: no writes, and a bound on how long one analyst's question runs.
CREATE SETTINGS PROFILE kuberecord_readonly
  SETTINGS readonly = 1, max_execution_time = 60
  TO kuberecord_ro;
```

Then, on each engineer's machine:

```console
$ export KUBERECORD_CLICKHOUSE_PASSWORD='a-password-you-generated'
$ kuberecord config set-profile prod --backend clickhouse \
    --addr clickhouse.example:9000 --database kuberecord \
    --username kuberecord_ro --password-env KUBERECORD_CLICKHOUSE_PASSWORD
```

Why this rather than widening Kubernetes RBAC so everyone can read the operator's
Secret: that Secret holds the credential the operator **writes** with. Handing it to
a person to run queries with gives them the ability to insert rows into an audit
trail, which is the one thing an audit trail must be able to rule out. A separate
read-only user is both easier to grant and strictly safer, and it is revocable
without restarting the operator.

The same reasoning applies to the archive tier: the sink's own S3 credential is
documented as needing `PutObject` and nothing else, so it cannot read the archive
back even if it leaked. Give a reader its own key with `s3:ListBucket` and
`s3:GetObject` on the prefix, and no `PutObject`.

## What the CLI asks of Kubernetes

Only for discovery. `--source` and profiles need nothing.

| Verb | Resource | Needed for |
|------|----------|-----------|
| `get`, `list` | `clickhousesinks`, `s3sinks` (cluster-scoped) | Finding a sink to read |
| `get` | `secrets` in the operator's namespace | A `ClickHouseSink`'s password. **Usually not granted — use a profile.** |
| `list` | `deployments` in the operator's namespace | Reading the cluster identity. Optional; a notice is printed if refused. |

The CLI never writes to the cluster, and it never reads recorded history through
the operator — it reads the sink directly, as a client of the frozen schema
([`docs/SCHEMA.md`](SCHEMA.md)).

## Evaluation mode

Pointing `--source` at a directory or a bucket removes ClickHouse from the picture
entirely: an archive synced to a laptop answers the same questions through the same
commands, with no infrastructure and no credentials beyond the ones you already
have for the bucket.

The trade is query performance, and it is a real one. The object archive has no
index, so a single-object question over a wide window lists and decompresses every
object in that window's partitions. It is honest about that rather than quiet: the
cost is bounded, reported and interruptible. For wide analytics over an archive —
aggregations, joins, anything that reads more than one object's history — use the
DuckDB and Athena recipes in [`docs/QUERIES.md`](QUERIES.md), which are built for
exactly that shape.

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success. For `diff --exit-code`, additionally "no changes". |
| `1` | Runtime error: a well-formed request that could not be carried out. For `diff --exit-code`, additionally "changes found", which is a finding rather than a failure and prints no `error:` line. |
| `2` | Usage error: an unknown flag, a malformed object address, a bad value. |
| `3` | No coverage: nothing was ever watching the requested scope — which is a different fact from "nothing changed", and is reported as one. |

`diff --exit-code` is the one place `0` and `1` carry a second meaning, which is
why it is opt-in: it overloads codes that otherwise only mean success and failure.
Code `3` outranks it either way.

Code `3` is the one worth scripting against. Every other tool in this space
collapses "your query matched nothing" and "nothing was ever recorded here" into a
single successful empty result; kuberecord will not, because those two answers send
an engineer in opposite directions.
