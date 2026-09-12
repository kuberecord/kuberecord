# Kubernetes Events

Naming `kind: Event` in a rule records the Kubernetes Events in that rule's
scope. This page is the reference for what that captures, how to narrow it, and
what the narrowing does and does not cost.

> **This page is being written across Phase 19 and Phase 20.** It currently
> covers the filter fields and where they are evaluated. The capture model, the
> volume amplifier, sizing guidance and the read-time half (`--with-events`,
> `--events-only`) arrive with Task 19.5 and Task 20.2.

- [Filtering what is captured](#filtering-what-is-captured)
- [Where a filter is evaluated](#where-a-filter-is-evaluated)
- [Filtering narrows relevance, not volume](#filtering-narrows-relevance-not-volume)
- [Known limitations](#known-limitations)

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
| `subjectNames` | the subject's exact `name` | ⚠️ Exact match only. See [Known limitations](#known-limitations). |

**Prefer `excludeReasons` to `reasons`.** An exclusion keeps the reasons nobody
has thought of yet, which — for a stream written by every controller in the
cluster — is most of them. An include list silently stops recording the day a
new operator starts emitting something worth seeing. It is also the form that
pushes down completely; see the next section.

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

## Filtering narrows relevance, not volume

A filter chooses **which Event streams are kept**. It does not change how many
rows each one produces.

A recurring Event is *updated in place* by the API server to bump its `count`.
Its content genuinely changes, so hash dedup cannot suppress it, and every
recurrence writes another full row — under every filter on this page, including
one that keeps a single reason.

The distributions make this worse than it sounds, because they run opposite to
intuition:

- `Normal` events (`Scheduled`, `Pulled`, `Created`, `Started`) are **numerous**
  and fire roughly **once**.
- `Warning` events (`BackOff`, `FailedScheduling`, `Unhealthy`) are **fewer** and
  **recur** for as long as the fault persists.

So `types: [Warning]` drops most rows in a healthy cluster and almost none in an
unhealthy one — it narrows a stream to what an operator wants to read, and it
does not bound what that stream costs when the cluster is on fire. Sizing
guidance is in [SCHEMA.md](SCHEMA.md); a mechanism that bounds the recurrence
itself is Phase 20's subject and is not in this release.

## Known limitations

**`subjectNames` is exact-match only, and its usefulness is inverted from what
you would expect.** It works for objects whose names a human chose and which
survive a rollout — a Deployment, a Service, a StatefulSet's Pods (`postgres-0`,
`postgres-1`, which are ordinal and stable). It is **useless for the Pods of a
Deployment**: those names are generated (`checkout-api-69dfc5f67d-ldw5j`), they
change on every rollout, and a name that no longer exists matches nothing while
the rule stays `Ready` — so the stream goes quiet with nothing anywhere saying
why. There is no prefix or glob form, because neither can be expressed as a
field selector and both would pull the whole Event stream of the namespace over
the network to evaluate locally.

**A filter change emits no scope transition.** Editing a rule's `eventFilter`
takes effect on the next Event without opening or closing a scope epoch, exactly
as editing its `labelSelector` does. The consequence is that an auditor reading a
continuous scope in `watch_scopes` cannot tell that its filter narrowed part-way
through. Whether a filter change *should* be a scope change is a real question,
but it is a behaviour change to existing `labelSelector` semantics and needs its
own decision.

## See also

- [CRDS.md](CRDS.md) — the full `StreamRule` and `ClusterStreamRule` reference.
- [SCHEMA.md](SCHEMA.md) — the frozen row schema, and what an Event row holds.
- [PERFORMANCE.md](PERFORMANCE.md) — measured envelopes for the watch path.
