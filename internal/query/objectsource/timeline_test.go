/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package objectsource

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// The incarnations and actors this file's fixtures use.
const (
	uidOld = "aaaaaaaa-0000-0000-0000-000000000001"
	uidNew = "bbbbbbbb-0000-0000-0000-000000000002"

	actorHelm    = "helm"
	actorKubectl = "kubectl"
)

// TestTimelineRefusesAnUnboundedQuery: every side of the window is required, and the
// refusal carries the sentinel.
//
// The conformance suite poses the fully unbounded case. The one-sided cases are this
// package's own, and they matter because they are the ones a caller reaches by
// accident: a zero To looks like "up to now" and is not, since the partition range
// would run to whichever partition the clock is in and list every empty one on the
// way. Refusing is kinder than a scan nobody can tell from a hang.
func TestTimelineRefusesAnUnboundedQuery(t *testing.T) {
	t.Parallel()

	from := testEpoch()
	to := testEpoch().Add(time.Hour)

	tests := []struct {
		name     string
		from, to time.Time
		refuse   bool
	}{
		{name: "neither bound", refuse: true},
		{name: "no start", to: to, refuse: true},
		{name: "no end", from: from, refuse: true},
		{name: "backwards", from: to, to: from, refuse: true},
		{name: "both bounds", from: from, to: to},
		{name: "an instant", from: from, to: from},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine, _ := engineOverDir(t, t.TempDir(), Options{Prefix: "audit"})

			it, err := engine.Timeline(context.Background(), query.TimelineQuery{
				Ref: testRef(), From: tc.from, To: tc.to,
			})
			if it != nil {
				defer func() { _ = it.Close() }()
			}
			switch {
			case tc.refuse && !errors.Is(err, query.ErrTimeBoundRequired):
				t.Errorf("Timeline = %v, want query.ErrTimeBoundRequired; a caller matches the sentinel "+
					"to name the flag that fixes it", err)
			case tc.refuse && it != nil:
				t.Errorf("a refused query also returned an iterator; a caller checking the error first " +
					"would be right to ignore it, and one checking the iterator first would render an " +
					"empty timeline the engine has just refused to produce")
			case !tc.refuse && err != nil:
				t.Errorf("Timeline = %v, want a bounded query to be answered", err)
			}

			// Incarnations carries the same rule, and for the same reason: it is a scan
			// over the same partitions.
			_, incErr := engine.Incarnations(context.Background(), testRef(), tc.from, tc.to)
			if got := errors.Is(incErr, query.ErrTimeBoundRequired); got != tc.refuse {
				t.Errorf("Incarnations = %v, want ErrTimeBoundRequired to be %t", incErr, tc.refuse)
			}
		})
	}
}

// reusedNameHistory is one name worn by two objects, changed by different actors.
//
// The older incarnation was managed by helm and the newer by kubectl, which is what
// makes the resolution order visible: a filter for helm's changes must not be able to
// promote the older incarnation into being "the newest".
func reusedNameHistory() conformance.History {
	row := func(offset time.Duration, uid, actor string, replicas int) conformance.Row {
		return conformance.Row{
			Ref: testRef(),
			Change: query.Change{
				TS:              testEpoch().Add(offset),
				EventType:       query.EventAdded,
				UID:             uid,
				ResourceVersion: fmt.Sprintf("%d", 100+replicas),
				APIVersion:      "apps/v1",
				Actors:          []string{actor},
				Data:            fmt.Sprintf(`{"kind":"Deployment","spec":{"replicas":%d}}`, replicas),
				SHA256:          fmt.Sprintf("%064d", replicas),
			},
		}
	}
	return conformance.History{Rows: []conformance.Row{
		row(0, uidOld, actorHelm, 1),
		row(time.Minute, uidOld, actorHelm, 2),
		row(2*time.Minute, uidNew, actorKubectl, 9),
		row(3*time.Minute, uidNew, actorKubectl, 8),
	}}
}

// TestTimelineResolvesTheIncarnationBeforeApplyingPredicates is the Invariant 7 trap,
// posed the way a filter poses it.
//
// The newest incarnation was managed by kubectl; the older one by helm. A query
// filtered to helm's changes is therefore a query about an incarnation that has none:
// the honest answer is empty. An engine that resolved the incarnation from the
// *filtered* rows would answer with the older incarnation's history instead — a
// coherent-looking account of a Deployment that was scaled to 2 and is now gone,
// rendered under the living object's name, with nothing in the output admitting the
// substitution.
//
// The empty answer is uncomfortable, which is why this needs a test rather than a
// comment: the wrong behaviour looks more helpful.
func TestTimelineResolvesTheIncarnationBeforeApplyingPredicates(t *testing.T) {
	t.Parallel()

	engine, _ := engineOver(t, reusedNameHistory(), Options{Prefix: "audit"})
	q := wholeWindow(testRef())

	unfiltered := drain(t, engine, q)
	if len(unfiltered) != 2 {
		t.Fatalf("the default timeline returned %d changes, want the newest incarnation's 2",
			len(unfiltered))
	}
	for _, c := range unfiltered {
		if c.UID != uidNew {
			t.Fatalf("the default timeline returned incarnation %s, want the newest (%s)", c.UID, uidNew)
		}
	}

	q.Actors = []string{actorHelm}
	filtered := drain(t, engine, q)
	if len(filtered) != 0 {
		t.Errorf("a timeline filtered to %q returned %d changes from incarnation %s. The newest "+
			"incarnation has no changes by that actor, so the honest answer is empty; resolving the "+
			"incarnation from the filtered rows would answer with a different object's history under "+
			"this object's name (Invariant 7)",
			actorHelm, len(filtered), filtered[0].UID)
	}

	// The same filter over every incarnation is a different question, and it does have
	// an answer — which is what makes the empty one above a choice rather than a bug.
	q.AllIncarnations = true
	spanning := drain(t, engine, q)
	if len(spanning) != 2 {
		t.Errorf("an all-incarnations timeline filtered to %q returned %d changes, want the older "+
			"incarnation's 2", actorHelm, len(spanning))
	}
}

// eventRow builds one recorded Kubernetes Event about the fixture object, in whichever
// of the two API groups the caller names.
//
// The subject key is spelled the way that group spells it: involvedObject in the core
// group, regarding in events.k8s.io. That difference is the whole reason this helper
// takes the group rather than defaulting to one.
func eventRow(offset time.Duration, apiGroup, name, reason, subjectUID string) conformance.Row {
	subject, apiVersion := "involvedObject", "v1"
	if apiGroup != "" {
		subject, apiVersion = "regarding", "events.k8s.io/v1"
	}
	target := testRef()
	data := fmt.Sprintf(
		`{"reason":%q,%q:{"kind":%q,"namespace":%q,"name":%q,"uid":%q}}`,
		reason, subject, target.Kind, target.Namespace, target.Name, subjectUID)

	return conformance.Row{
		Ref: query.ObjectRef{
			ClusterID: target.ClusterID,
			APIGroup:  apiGroup,
			Kind:      "Event",
			Namespace: target.Namespace,
			Name:      name,
		},
		Change: query.Change{
			TS:              testEpoch().Add(offset),
			EventType:       query.EventAdded,
			UID:             "event-" + name,
			ResourceVersion: "1",
			APIVersion:      apiVersion,
			// An Event's actors are the field managers of the Event object: the
			// controller that wrote it, never whoever changed the object it is about.
			// They are an actor the object's own changes never carry, so a filter
			// leaking across is visible.
			Actors: []string{"kubelet"},
			Data:   data,
			SHA256: fmt.Sprintf("%064s", name),
		},
	}
}

// TestTimelineCorrelatesEventsInBothSpellings: Events naming the object are merged in
// ts order, from either API group, and an actor filter on the object's changes does not
// silence them.
//
// Both halves have cost a real investigation somewhere. Handling one group spelling
// drops whichever half of a cluster's events happens to be captured the other way, and
// it drops it silently. Applying the object's actor filter to its commentary empties
// the Event half of almost every filtered timeline, and shows the reader "Kubernetes
// said nothing" about an incident Kubernetes had plenty to say about.
func TestTimelineCorrelatesEventsInBothSpellings(t *testing.T) {
	t.Parallel()

	history := reusedNameHistory()
	history.Rows = append(history.Rows,
		eventRow(30*time.Second, "", "checkout.core", "ScalingReplicaSet", uidOld),
		eventRow(150*time.Second, "events.k8s.io", "checkout.next", "FailedCreate", uidNew),
		// An Event about a different object, to prove the subject is really matched.
		conformance.Row{
			Ref: query.ObjectRef{
				ClusterID: testRef().ClusterID, APIGroup: "", Kind: "Event",
				Namespace: testRef().Namespace, Name: "other.event",
			},
			Change: query.Change{
				TS: testEpoch().Add(40 * time.Second), EventType: query.EventAdded,
				UID: "event-other", ResourceVersion: "1", APIVersion: "v1",
				Data: `{"involvedObject":{"kind":"Deployment","namespace":"payments","name":"billing"}}`,
			},
		},
	)

	engine, _ := engineOver(t, history, Options{Prefix: "audit"})

	q := wholeWindow(testRef())
	q.AllIncarnations = true
	q.IncludeEvents = true
	merged := drain(t, engine, q)

	var events, changes int
	for _, c := range merged {
		if c.EventType == query.EventKubernetes {
			events++
			continue
		}
		changes++
	}
	if changes != 4 || events != 2 {
		t.Fatalf("the merged timeline holds %d changes and %d events, want 4 and 2 (one per group "+
			"spelling, and none for the Event about another object): %v", changes, events, merged)
	}
	if !isOrderedByTS(merged) {
		t.Errorf("the merged timeline is not in ts order: %v", instantsOf(merged))
	}

	// Narrowed to one incarnation, the commentary narrows with it: uid is the exact
	// key, and it is right to apply precisely when the caller has said which
	// incarnation they mean.
	q.AllIncarnations = false
	q.UID = uidNew
	pinned := drain(t, engine, q)
	for _, c := range pinned {
		if c.EventType == query.EventKubernetes && c.UID != "event-checkout.next" {
			t.Errorf("a timeline pinned to %s merged the Event %s, which names the other incarnation",
				uidNew, c.UID)
		}
	}

	// And an actor filter over the object's own changes leaves the commentary alone.
	q.UID = ""
	q.AllIncarnations = true
	q.Actors = []string{actorHelm}
	filtered := drain(t, engine, q)
	var stillMerged int
	for _, c := range filtered {
		if c.EventType == query.EventKubernetes {
			stillMerged++
		}
	}
	if stillMerged != 2 {
		t.Errorf("a timeline filtered to %q merged %d events, want both. An Event's actors are the "+
			"field managers of the Event object, so filtering commentary by them manufactures a "+
			"silence the predicate was never about (Invariant 4)", actorHelm, stillMerged)
	}
}

// TestTimelineLimitAndReverseTakeFromTheEmissionEnd covers the reading the contract
// spells out bluntly, over a history this package can name row by row.
//
// The conformance suite pins the property; this asserts the interaction a caller
// actually issues — "the last two things that happened" is Reverse with a Limit, and
// getting it wrong returns the two oldest changes, which reads as an object that has
// not been touched in weeks.
func TestTimelineLimitAndReverseTakeFromTheEmissionEnd(t *testing.T) {
	t.Parallel()

	engine, _ := engineOver(t, manyChanges(6), Options{Prefix: "audit"})

	q := wholeWindow(testRef())
	q.Limit = 2
	oldest := drain(t, engine, q)
	if len(oldest) != 2 || !oldest[0].TS.Equal(testEpoch()) {
		t.Errorf("a limited timeline returned %v, want the two oldest changes", instantsOf(oldest))
	}

	q.Reverse = true
	newest := drain(t, engine, q)
	if len(newest) != 2 || !newest[0].TS.Equal(testEpoch().Add(5*time.Second)) {
		t.Errorf("a limited, reversed timeline returned %v, want the two newest changes",
			instantsOf(newest))
	}
}

// TestEngineRefusesUseAfterClose: every read reports a closed engine rather than
// reaching a source the caller may since have closed too.
//
// A use-after-close is a bug in the caller, and this is what makes it say so instead
// of surfacing as whatever a closed directory handle produces — a failure with no name
// and no obvious author.
func TestEngineRefusesUseAfterClose(t *testing.T) {
	t.Parallel()

	engine, _ := engineOverDir(t, t.TempDir(), Options{Prefix: "audit"})
	if err := engine.Close(); err != nil {
		t.Fatalf("closing the engine: %v", err)
	}
	// Idempotent, because a caller that both defers a Close and calls one explicitly
	// is doing the documented thing.
	if err := engine.Close(); err != nil {
		t.Errorf("closing the engine twice: %v", err)
	}

	ctx := context.Background()
	from, to := testEpoch(), testEpoch().Add(time.Hour)
	if _, err := engine.Timeline(ctx, query.TimelineQuery{Ref: testRef(), From: from, To: to}); err == nil {
		t.Error("Timeline answered after Close")
	}
	if _, err := engine.Incarnations(ctx, testRef(), from, to); err == nil {
		t.Error("Incarnations answered after Close")
	}
	if _, err := engine.StateAt(ctx, testRef(), to, ""); err == nil {
		t.Error("StateAt answered after Close")
	}
	if _, err := engine.Coverage(ctx, query.ScopeQuery{ClusterID: testRef().ClusterID}); err == nil {
		t.Error("Coverage answered after Close")
	}
	if _, err := engine.EstimateScan(ctx, testRef().ClusterID, from, to); err == nil {
		t.Error("EstimateScan answered after Close")
	}
}

// TestNewEngineNormalizesOptions pins the defaults, because each of them is a
// statement rather than a convenience.
func TestNewEngineNormalizesOptions(t *testing.T) {
	t.Parallel()

	if _, err := NewEngine(nil, Options{}); err == nil {
		t.Error("NewEngine accepted a nil source; resolving where the archive lives belongs to the " +
			"caller, so there is nothing for the engine to open")
	}

	local, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("opening a source: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })

	tests := []struct {
		name          string
		opts          Options
		wantSpan      time.Duration
		wantConc      int
		wantStateBack time.Duration
	}{
		{
			name: "the zero value takes the safe defaults", opts: Options{},
			wantSpan: DefaultObjectSpan, wantConc: DefaultConcurrency,
			wantStateBack: DefaultStateLookback,
		},
		{
			name: "the widening can be switched off explicitly",
			opts: Options{ObjectSpan: NoObjectSpan},
			// Zero, not negative: "no widening" is a claim about how the archive was
			// written, and it has to be spelled out rather than fallen into.
			wantSpan: 0, wantConc: DefaultConcurrency, wantStateBack: DefaultStateLookback,
		},
		{
			name:     "a caller that knows its writer passes the real span",
			opts:     Options{ObjectSpan: 5 * time.Minute, Concurrency: 2, StateLookback: time.Hour},
			wantSpan: 5 * time.Minute, wantConc: 2, wantStateBack: time.Hour,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine, err := NewEngine(local, tc.opts)
			if err != nil {
				t.Fatalf("NewEngine: %v", err)
			}
			switch {
			case engine.objectSpan != tc.wantSpan:
				t.Errorf("object span = %s, want %s", engine.objectSpan, tc.wantSpan)
			case engine.concurrency != tc.wantConc:
				t.Errorf("concurrency = %d, want %d", engine.concurrency, tc.wantConc)
			case engine.stateLookback != tc.wantStateBack:
				t.Errorf("state lookback = %s, want %s", engine.stateLookback, tc.wantStateBack)
			}
		})
	}
}

// TestNewEngineRefusesANegativeObjectSpan: an option that cannot have been meant is
// named at the constructor rather than obeyed.
//
// The coercion this replaces sent a negative span to zero, and the direction is the
// whole finding. Zero is the *tightest* ceiling this engine has — it is what
// NoObjectSpan buys for an archive that has earned it — so rounding an invalid value
// there let the newest-first walk stop one partition early and return a timeline
// missing its newest rows, through a validation path rather than through the
// algorithm. That is the failure the walk was written to make impossible, and it is
// the one a reader is least likely to check for, because coercing towards zero reads
// as caution.
//
// NoObjectSpan is negative too and is accepted, which is not an exception to the rule
// so much as the reason the rule can be strict: the deliberate case has a spelling, so
// every other negative duration is a bug in the caller and can be said so.
func TestNewEngineRefusesANegativeObjectSpan(t *testing.T) {
	t.Parallel()

	local, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("opening a source: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := local.Close(); closeErr != nil {
			t.Errorf("closing the fixture source: %v", closeErr)
		}
	})

	refused := []struct {
		name string
		span time.Duration
	}{
		{name: "a whole negative hour", span: -time.Hour},
		{name: "a negative writer configuration", span: -5 * time.Minute},
		// Adjacent to the sentinel on purpose: "negative" is not a synonym for
		// NoObjectSpan, and a caller one nanosecond away from it has still not said
		// what NoObjectSpan says.
		{name: "one nanosecond past the sentinel", span: 2 * NoObjectSpan},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine, err := NewEngine(local, Options{ObjectSpan: tc.span})
			if err == nil {
				if closeErr := engine.Close(); closeErr != nil {
					t.Errorf("closing the engine that should not have been built: %v", closeErr)
				}
				t.Fatalf("NewEngine accepted an ObjectSpan of %s. A negative span is not a tighter "+
					"archive: it trims hours off the bottom of every window and pushes the "+
					"newest-first walk's ceiling below the partition it has just read, so the "+
					"timeline comes back plausible and short", tc.span)
			}
			// Named, not merely refused: the caller has to be able to find the field
			// and the value without reading this package.
			if !containsAll(err.Error(), "ObjectSpan", tc.span.String(), "NoObjectSpan") {
				t.Errorf("the refusal of %s reads %q; it must name the field, the value supplied and "+
					"the way to say \"no spill\" deliberately", tc.span, err)
			}
		})
	}

	accepted := []struct {
		name     string
		span     time.Duration
		wantSpan time.Duration
	}{
		{name: "the sentinel is the one negative that means something", span: NoObjectSpan, wantSpan: 0},
		{name: "unset is unset", span: 0, wantSpan: DefaultObjectSpan},
	}
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			engine, err := NewEngine(local, Options{ObjectSpan: tc.span})
			if err != nil {
				t.Fatalf("NewEngine(%s): %v", tc.span, err)
			}
			t.Cleanup(func() {
				if closeErr := engine.Close(); closeErr != nil {
					t.Errorf("closing the engine: %v", closeErr)
				}
			})
			if engine.objectSpan != tc.wantSpan {
				t.Errorf("an ObjectSpan of %s resolved to %s, want %s",
					tc.span, engine.objectSpan, tc.wantSpan)
			}
		})
	}
}

// TestCapabilitiesAreTheTruthfulReducedSet states the declaration in a test as well
// as in the engine, so that changing one without the other fails here rather than
// misleading a reader of the output.
//
// Deletions is the one that matters most: false is not a limitation to be fixed but a
// fact about the archive (D12), and a renderer reads it to decide whether a timeline
// that simply stops needs a warning printed over it.
func TestCapabilitiesAreTheTruthfulReducedSet(t *testing.T) {
	t.Parallel()

	engine, _ := engineOverDir(t, t.TempDir(), Options{})
	want := query.Capabilities{
		Backend:           "objectsource",
		Deletions:         false,
		ServerSideFilter:  false,
		PointQuery:        false,
		TimeBoundRequired: true,
	}
	if got := engine.Capabilities(); got != want {
		t.Errorf("Capabilities() = %+v, want %+v", got, want)
	}
}

// isOrderedByTS reports whether changes are in non-decreasing ts order.
func isOrderedByTS(changes []query.Change) bool {
	for i := 1; i < len(changes); i++ {
		if changes[i].TS.Before(changes[i-1].TS) {
			return false
		}
	}
	return true
}
