# Kubernetes Events

Naming `kind: Event` in a rule's `resources` records the Kubernetes Events in
that rule's scope. It is the one entry in a rule whose cost is not proportional
to how many objects it names, and the one whose stream is wider than it looks —
so this page is what to read before it goes into a rule rather than after the
first storage graph.

- [What a rule records](#what-a-rule-records)
- [Filtering what is captured](#filtering-what-is-captured)
- [Where a filter is evaluated](#where-a-filter-is-evaluated)
- [The volume amplifier](#the-volume-amplifier)
- [Sizing an Event rule](#sizing-an-event-rule)
- [Reading Events back](#reading-events-back)
- [Known limitations](#known-limitations)

## What a rule records

`v1/Event` and `events.k8s.io/v1/Event` are the same storage behind two APIs.
Name either in a `resources` list and Events are streamed in a built-in Events
mode: there is no switch to turn on and none to turn off, because every
difference from an ordinary kind exists to stop kuberecord recording something
untrue.

- **Every row carries the whole Event, never a diff**, so a `count` bump is
  readable on its own. `Checkpoint` rows never appear for Events.
- **An Event's expiry is recorded as nothing at all** — no `Deleted` row, ever,
  not for its ~1h TTL, not for a `kubectl delete event`. An Event's history
  simply stops, so "no deletion" must not be read as "still live" the way it can
  for a Deployment.
- **`Snapshot` never appears either.** A first sighting is `Added` even during
  warm-up, because for an Event the hedge Snapshot expresses has a known answer.

[`docs/SCHEMA.md`](SCHEMA.md#kubernetes-events) is the full account of what an
Event row means. What it costs is [below](#the-volume-amplifier).

### Capture is scope-wide

**A rule naming `Event` records every Event in the namespaces it selects** — not
only the ones about the other kinds that same rule names. The width of the Event
stream is the width of the **scope**, and nothing else in the rule changes that.
The predicate that ties an Event to a subject lives entirely in the reader: it
runs at **read time**, in `kuberecord timeline --with-events` and in the
`involvedObject` recipes in [`docs/QUERIES.md`](QUERIES.md#events-for-object-x-around-time-t).

This is deliberate, and it is the design decision the rest of the page follows
from. The obvious alternative — capture only the Events whose subject this rule
already watches — is **rejected**, on two grounds:

- **It is lossy exactly when it matters.** Correlating at capture time means
  matching an Event against a cache of the objects that *exist*. The Events worth
  most during an incident are about objects that failed to exist or are ceasing
  to: `FailedScheduling` (the Pod is unschedulable, and this is the Event that
  says why), `FailedCreate` (the object never entered any cache), `Killing` and
  `Preempting` (the subject is on its way out, and whether it is still cached when
  its own Event arrives is a race). A filter that is accurate for healthy objects
  and lossy for failing ones is backwards for an audit trail.
- **It would make the archive non-deterministic.** What got recorded would depend
  on informer cache warmth and on the arrival order of two watch streams, so the
  same rule against the same cluster would capture different Events on two runs.
  Today "which Events were being recorded, over which interval, under whose rule"
  is answerable from `watch_scopes` and `rule_ref` alone. That property is worth
  more than the rows it costs.

The same rule decides which filter axes exist at all: **a filter reads fields the
Event itself carries, never the subject's.** `type`, `reason`, `involvedObject`
and the emitting component are all on the Event row. The subject's *labels* are
not, so there is no filter on them — matching against them would be the rejected
design under another name.

**A `labelSelector` does not narrow an Event entry, and looks as though it
should.** A selector matches the *watched object's own* labels, so on an Event
entry it is matched against the Event's labels — not against those of whatever
the Event is about — and Events, written by kubelet, the scheduler and the
controllers, carry essentially none. The result is not a narrower stream but an
empty one, on a rule that stays `Ready=True` throughout. Narrow the namespaces,
or use an `eventFilter`.

## Filtering what is captured

`spec.resources[].eventFilter` narrows Event capture using fields the Event
itself carries. It is valid only on an entry whose `kind` is `Event`; anywhere
else it is rejected at admission.

```yaml
- group: ""
  version: v1
  kind: Event
  eventFilter:
    excludeReasons: [Pulling, Pulled, Created, Started, Scheduled]
    subjectKinds: [Pod, ReplicaSet]
```

Within a list, OR. Across fields, AND. An absent or empty list is no constraint
at all, and a rule with no `eventFilter` records every Event in its scope.

| Field | Matches | Notes |
|---|---|---|
| `types` | `Normal`, `Warning` | Naming both is the same as naming neither. |
| `reasons` | the Event's `reason` | Mutually exclusive with `excludeReasons`. |
| `excludeReasons` | everything *but* these reasons | Prefer this where either would do — see below. |
| `sourceComponents` | the emitting component | Matched against whichever spelling the stored Event carries. |
| `subjectKinds` | the subject's `kind` | The Kind as the emitter spelled it, not a plural resource name. |
| `subjectNames` | the subject's exact `name` | ⚠️ Exact match, and generated names defeat it — see below. |

**Prefer `excludeReasons` to `reasons`.** An exclusion keeps the reasons nobody
has thought of yet, which — for a stream written by every controller in the
cluster — is most of them. An include list silently stops recording the day a
new operator starts emitting something worth seeing. It is also the form that
pushes down completely; see the next section.

**`subjectNames` is exact-match, and the names you most want are generated.** It
works for names a human chose and that survive a rollout: a Deployment, a
Service, a StatefulSet's Pods (`postgres-0`, `postgres-1`, which are ordinal and
stable). It is **useless for the Pods of a Deployment** — those names are
generated (`checkout-api-69dfc5f67d-ldw5j`), they change on every rollout, and a
name that no longer exists matches nothing while the rule stays `Ready`, so the
stream goes quiet with nothing anywhere saying why. There is no prefix or glob
form either ([why](#known-limitations)). For a workload whose Pod names move,
filter by `subjectKinds: [Pod]` and a namespace, and select the subject when you
read the archive back.

## Where a filter is evaluated

Every filter is evaluated in the operator, in the informer's event handler, and
that evaluation is what decides which rows are recorded. Some filters are
**additionally** pushed to the API server as a field selector, so the Events that
could never match are not sent over the network, cached, or transformed in the
first place.

Push-down is **a performance decision and never a content decision.** The
operator records the same rows either way; what changes is how much traffic and
memory was spent reaching them.

### Which filters push down

Kubernetes field selectors AND their terms and have no OR, which decides every
case:

| Filter shape | Pushes down? | Why |
|---|---|---|
| `types: [Warning]` | ✅ | A single-valued include is one `=` term. |
| `reasons: [BackOff]` | ✅ | Likewise. |
| `excludeReasons: [Pulled, Created, Started]` | ✅ at any length | An AND of `!=` terms *is* "none of these". |
| `subjectKinds: [Pod]` | ✅ | |
| `subjectNames: [postgres-0]` | ✅ | |
| `reasons: [BackOff, Killing]` | ❌ | A multi-valued include needs an OR, which field selectors do not have. |
| `sourceComponents: [kubelet]` | ❌ | See below. |

Axes are independent: a filter naming both a single-valued `types` and a
multi-valued `reasons` pushes the first and evaluates the second in the operator.

`sourceComponents` never pushes down. The two Event APIs spell "who emitted
this" four different ways and a rule is matched against all of them, whereas the
API server's own `source` selector is a *fallback chain* — `source.component`,
otherwise `reportingController`. For an Event carrying both spellings the two
disagree, so pushing it would change which rows are recorded.

Both Event APIs are supported. `events.k8s.io/v1` renamed the subject fields, so
the operator sends `regarding.kind` there and `involvedObject.kind` to core
`v1` — the recorded rows are identical.

### Push-down depends on what other rules exist

One informer serves every rule that watches the same resource in the same
namespace. That is what keeps two rules on one resource from costing two Lists,
and it means a field selector belongs to the *informer*, not to a rule.

So a selector is pushed to the API server **only when every rule sharing that
informer derives the same one.** When two rules on `(Event, production)` want
different subsets, the informer watches the stream whole and both rules are
filtered in the operator instead.

**This affects performance only. It never affects what is recorded.** Adding a
second rule can make a first rule's watch more expensive; it cannot change that
rule's rows, its coverage, or anything an auditor reads back. If you care about
the bandwidth, give the two rules the same filter, or put them in different
namespaces.

## The volume amplifier

Everything above is about **which Event streams are kept**. This is about how
many rows each one produces, and the two are different axes: **no filter on this
page bounds this.**

**A `count` bump writes a whole row.** When the same thing happens again the API
server does not create a second Event — it **updates the existing one in place**,
bumping `count` and moving `lastTimestamp`. That changes the object's content, so
the hash dedup that silently absorbs a no-op resync does not absorb it; and
because Events are never diffed, what gets stored is another complete copy rather
than a one-line patch. The amplifier therefore scales with how often Events
*recur*, not with how many distinct Events a cluster has.

The case to picture is a **crash-looping pod**. `kubelet` re-emits `BackOff` for
it on the restart back-off — up to once every five minutes, and far more often at
the start. Every re-emission is a `count` bump, and every bump is one more full
Event JSON in `resource_states`. A handful of pods stuck overnight is hundreds of
rows describing a situation that has not changed since the first one. **The
amplifier peaks exactly when a cluster is unhealthy**, which is when the write
path has least headroom and when somebody is reading the audit trail.

**Filtering does not solve this**, and the distributions make that worse than it
sounds, because they run opposite to intuition:

- `Normal` events (`Scheduled`, `Pulled`, `Created`, `Started`) are **numerous**
  and fire roughly **once**.
- `Warning` events (`BackOff`, `FailedScheduling`, `Unhealthy`) are **fewer** and
  **recur** for as long as the fault persists.

So `types: [Warning]` drops most rows in a healthy cluster and almost none in an
unhealthy one. It narrows a stream to what an operator wants to read — which is
worth having — and it does not bound what that stream costs when the cluster is
on fire. **No mechanism in this release bounds the recurrence itself**; what
bounds it today is the scope you chose and the retention you set, which is what
the next section is for.

## Sizing an Event rule

In descending order of how much they buy:

1. **Prefer a namespaced `StreamRule`.** One namespace's Events are a bounded,
   measurable quantity, and the rule's owner is the team whose workloads generate
   them. This is the shape [`examples/quickstart/`](../examples/quickstart/) uses.
2. **Otherwise, a `namespaceSelector` on the `ClusterStreamRule`.** Scope is the
   only thing the Event stream is a function of, so this is the only knob that
   reduces it. A `labelSelector` does not
   ([why](#capture-is-scope-wide)).
3. **Add an `eventFilter` for relevance.** `excludeReasons: [Pulling, Pulled,
   Created, Started, Scheduled]` removes the startup chatter, keeps everything
   nobody has thought of yet, and pushes down completely — so it costs less than
   the unfiltered rule at the API server as well as in storage. Size the rule as
   though it were not there: it changes which streams are kept, not how deep each
   one goes.
4. **Treat a cluster-wide Event rule as a decision.** It is defensible — an Event
   stream nobody scoped is also one nobody has to remember to widen — but take it
   having looked at two numbers first: the retention TTL on `resource_states`
   ([Suggested TTL](SCHEMA.md#suggested-ttl-optional-non-mandatory) and
   [`docs/RETENTION.md`](RETENTION.md)), which bounds storage and the warm-up
   query both; and, for an `S3Sink`, `spec.rotation.maxObjectBytes`, since Events
   are the workload most likely to make rotation size- rather than age-driven and
   `workers × maxObjectBytes` is resident memory.

Warm-up is the cost people miss. Because Events never get a `Deleted` row,
nothing tombstones them, so the warm-up query for an Events scope returns every
Event still stored for that scope — a retention TTL is what bounds both that
query and the memory its result seeds.

The `events` watch preset does not ship enabled for this reason and no other (see
[`docs/RBAC.md`](RBAC.md)). Granting it is one `kubectl apply` and needs no
restart, so nothing here is hard to undo — but the volume arrives before the
invoice does.

## Reading Events back

Capture is scope-wide, so everything that narrows an Event to a subject happens
here. Correlation takes the Event's own `involvedObject` (or `regarding`) from
the **Event row** and matches it against the object you asked about; the object's
own rows are never consulted, and both API spellings are correlated together.
That is why an object nobody ever watched can still have Events.

| Flag | What it does |
|---|---|
| [`timeline --with-events`](CLI.md#--with-events-that-finds-no-events) | Interleaves the Events recorded about the object with the object's own changes, in one table, oldest first. The reading you usually want, since the point is which change an Event followed. |
| [`timeline --events-only`](CLI.md#--events-only) | The Events and none of the object's own changes. It **implies** `--with-events`. |

Two things about `--events-only` are worth knowing before you script against it:

- **The header reports the coverage of Events**, labelled `Coverage (Events)`,
  and the same substitution reaches `metadata.coverage` in `-o json`. The rows
  came from the Event scope, so the header describes that scope rather than the
  object's.
- **The object's own scope is not consulted at all.** An object nobody was
  watching is normally an exit `3` no-coverage finding; under this flag it is not,
  because failing the command over the absence of something the reader excluded
  would put an error under a page of perfectly good Events.

An empty answer is explained either way, never presented on its own: the watch
scopes are consulted about `Event` — both API spellings — and the answer
distinguishes *Events were recorded, none about this object* from *no rule
streams Events to this sink* from *this backend has no scope log to read*. Only
the middle one is a configuration gap, and it is printed with the YAML that
closes it.

There is **no `kuberecord events` command**. It would ask what `timeline` asks
and hide rows of the answer; the name is kept for the namespace-wide Event search
that would be a differently-shaped question. The aggregate questions the CLI does
not ask — [noisiest reasons in a window](QUERIES.md#noisiest-reasons-in-a-window),
Events across many objects at once — are SQL, in
[`docs/QUERIES.md`](QUERIES.md#events-for-object-x-around-time-t); read
[its two traps](QUERIES.md#reading-event-history-correctly) before writing one by
hand.

## Known limitations

**There is no prefix or glob form for `subjectNames`.** Neither can be expressed
as a Kubernetes field selector, so supporting one would mean pulling the whole
Event stream of the namespace over the network to discard most of it locally —
which is what the filter exists to avoid. The consequence is the one spelled out
[above](#filtering-what-is-captured): generated Pod names cannot be matched, and
a filter naming a name that has rolled away records nothing while reporting
`Ready`.

**A filter change emits no scope transition.** Editing a rule's `eventFilter`
takes effect on the next Event without opening or closing a scope epoch, exactly
as editing its `labelSelector` does. The consequence is that an auditor reading a
continuous scope in `watch_scopes` cannot tell that its filter narrowed part-way
through. Whether a filter change *should* be a scope change is a real question,
but it is a behaviour change to existing `labelSelector` semantics and needs its
own decision.

## See also

- [CRDS.md](CRDS.md) — the full `StreamRule` and `ClusterStreamRule` reference.
- [SCHEMA.md](SCHEMA.md#kubernetes-events) — the frozen row schema, and what an
  Event row holds.
- [CLI.md](CLI.md#timeline) — the `timeline` reference in full.
- [QUERIES.md](QUERIES.md#events-for-object-x-around-time-t) — the SQL, for both
  backends.
- [PERFORMANCE.md](PERFORMANCE.md) — measured envelopes for the watch path.
