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

package plan

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kuberecord/kuberecord/internal/sink"
)

const (
	nsProd    = "prod"
	nsStaging = "staging"

	ruleA = "prod/rule-a"
	ruleB = "prod/rule-b"
	ruleC = "prod/rule-c"

	selEverything = ""
	selAppWeb     = "app=web"
	selAppAPI     = "app=api"
)

var (
	gvkDeployment = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	gvkPod        = schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}

	// The two sinks every case in this file streams to. They are ClickHouseSinks
	// because that is the only kind an authored rule can name today; nothing here
	// depends on the kind beyond its being part of the identity.
	sinkDefault = sink.ID{Kind: sink.DefaultSinkKind, Name: "default"}
	sinkAudit   = sink.ID{Kind: sink.DefaultSinkKind, Name: "audit"}
)

// The parameter is sinkID: a target names the sink by its whole typed identity,
// which is what the data plane and RulesForSink both key on.
func target(sinkID sink.ID, gvk schema.GroupVersionKind, namespace, selector string) WatchTarget {
	return WatchTarget{Sink: sinkID, GVK: gvk, Namespace: namespace, Selector: selector}
}

func tkey(sinkID sink.ID, gvk schema.GroupVersionKind, namespace string) TargetKey {
	return TargetKey{Sink: sinkID, GVK: gvk, Namespace: namespace}
}

func state(key TargetKey, ruleKeys, selectors []string) TargetState {
	return TargetState{Key: key, RuleKeys: ruleKeys, Selectors: selectors}
}

// pending drains the change channel without blocking and reports how many
// notifications were buffered.
func pending(ch <-chan struct{}) int {
	n := 0
	for {
		select {
		case <-ch:
			n++
		default:
			return n
		}
	}
}

func formatSnapshot(snap map[TargetKey]TargetState) string {
	lines := make([]string, 0, len(snap))
	for key, st := range snap {
		lines = append(lines, fmt.Sprintf("  %s|%s|%s rules=%v selectors=%q redactions=%q",
			key.Sink, key.GVK, key.Namespace, st.RuleKeys, st.Selectors, st.Redactions))
	}
	slices.Sort(lines)
	if len(lines) == 0 {
		return "  <empty>"
	}
	return strings.Join(lines, "\n")
}

func assertSnapshot(t *testing.T, reg *Registry, want map[TargetKey]TargetState) {
	t.Helper()
	got := reg.Snapshot()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected snapshot:\ngot:\n%s\nwant:\n%s", formatSnapshot(got), formatSnapshot(want))
	}
}

func mustUpsert(t *testing.T, reg *Registry, ruleKey string, targets []WatchTarget) {
	t.Helper()
	if err := reg.Upsert(ruleKey, targets); err != nil {
		t.Fatalf("Upsert(%q): unexpected error: %v", ruleKey, err)
	}
}

// TestUpsertMergesRulesOntoOneTarget covers AC (a): rules that want the same
// (Sink, GVK, Namespace) share a single target, and the target survives until
// the last of them lets go.
func TestUpsertMergesRulesOntoOneTarget(t *testing.T) {
	reg := New()
	key := tkey(sinkDefault, gvkDeployment, nsProd)

	mustUpsert(t, reg, ruleA, []WatchTarget{target(sinkDefault, gvkDeployment, nsProd, selEverything)})
	mustUpsert(t, reg, ruleB, []WatchTarget{target(sinkDefault, gvkDeployment, nsProd, selAppWeb)})

	assertSnapshot(t, reg, map[TargetKey]TargetState{
		key: state(key, []string{ruleA, ruleB}, []string{selEverything, selAppWeb}),
	})

	// Dropping one contributor must keep the target alive and take that
	// contributor's selector with it — the surviving rule's intent must not
	// silently widen.
	reg.Remove(ruleA)
	assertSnapshot(t, reg, map[TargetKey]TargetState{
		key: state(key, []string{ruleB}, []string{selAppWeb}),
	})

	reg.Remove(ruleB)
	assertSnapshot(t, reg, map[TargetKey]TargetState{})
}

// TestUpsertOneRuleManySelectorsRefCounts guards the reason targetEntry counts
// instead of using sets: a single rule may land two targets on one key, and
// removing one of them must not release the other's reference.
func TestUpsertOneRuleManySelectorsRefCounts(t *testing.T) {
	reg := New()
	key := tkey(sinkDefault, gvkDeployment, nsProd)

	mustUpsert(t, reg, ruleA, []WatchTarget{
		target(sinkDefault, gvkDeployment, nsProd, selAppWeb),
		target(sinkDefault, gvkDeployment, nsProd, selAppAPI),
	})
	assertSnapshot(t, reg, map[TargetKey]TargetState{
		key: state(key, []string{ruleA}, []string{selAppAPI, selAppWeb}),
	})

	mustUpsert(t, reg, ruleA, []WatchTarget{target(sinkDefault, gvkDeployment, nsProd, selAppWeb)})
	assertSnapshot(t, reg, map[TargetKey]TargetState{
		key: state(key, []string{ruleA}, []string{selAppWeb}),
	})

	mustUpsert(t, reg, ruleA, nil)
	assertSnapshot(t, reg, map[TargetKey]TargetState{})
}

// TestUpsertAppliesExactDiff covers AC (b): a rule update adds and removes
// exactly the difference, and an update that changes nothing observable fires
// no notification.
func TestUpsertAppliesExactDiff(t *testing.T) {
	keyDeployProd := tkey(sinkDefault, gvkDeployment, nsProd)
	keyPodProd := tkey(sinkDefault, gvkPod, nsProd)
	keyDeployStaging := tkey(sinkDefault, gvkDeployment, nsStaging)
	keyDeployProdAudit := tkey(sinkAudit, gvkDeployment, nsProd)

	initial := []WatchTarget{
		target(sinkDefault, gvkDeployment, nsProd, selEverything),
		target(sinkDefault, gvkPod, nsProd, selEverything),
	}

	tests := []struct {
		name         string
		updated      []WatchTarget
		want         map[TargetKey]TargetState
		wantNotified bool
	}{
		{
			name: "adds one target and keeps the rest",
			updated: append(slices.Clone(initial),
				target(sinkDefault, gvkDeployment, nsStaging, selEverything)),
			want: map[TargetKey]TargetState{
				keyDeployProd:    state(keyDeployProd, []string{ruleA}, []string{selEverything}),
				keyPodProd:       state(keyPodProd, []string{ruleA}, []string{selEverything}),
				keyDeployStaging: state(keyDeployStaging, []string{ruleA}, []string{selEverything}),
			},
			wantNotified: true,
		},
		{
			name:    "removes one target and keeps the rest",
			updated: []WatchTarget{target(sinkDefault, gvkDeployment, nsProd, selEverything)},
			want: map[TargetKey]TargetState{
				keyDeployProd: state(keyDeployProd, []string{ruleA}, []string{selEverything}),
			},
			wantNotified: true,
		},
		{
			name: "swaps one target for another",
			updated: []WatchTarget{
				target(sinkDefault, gvkDeployment, nsProd, selEverything),
				target(sinkAudit, gvkDeployment, nsProd, selEverything),
			},
			want: map[TargetKey]TargetState{
				keyDeployProd:      state(keyDeployProd, []string{ruleA}, []string{selEverything}),
				keyDeployProdAudit: state(keyDeployProdAudit, []string{ruleA}, []string{selEverything}),
			},
			wantNotified: true,
		},
		{
			name: "narrows a selector on an unchanged key",
			updated: []WatchTarget{
				target(sinkDefault, gvkDeployment, nsProd, selAppWeb),
				target(sinkDefault, gvkPod, nsProd, selEverything),
			},
			want: map[TargetKey]TargetState{
				keyDeployProd: state(keyDeployProd, []string{ruleA}, []string{selAppWeb}),
				keyPodProd:    state(keyPodProd, []string{ruleA}, []string{selEverything}),
			},
			wantNotified: true,
		},
		{
			name:    "reordering the same targets changes nothing",
			updated: []WatchTarget{initial[1], initial[0]},
			want: map[TargetKey]TargetState{
				keyDeployProd: state(keyDeployProd, []string{ruleA}, []string{selEverything}),
				keyPodProd:    state(keyPodProd, []string{ruleA}, []string{selEverything}),
			},
			wantNotified: false,
		},
		{
			name:    "duplicate entries collapse and change nothing",
			updated: []WatchTarget{initial[0], initial[1], initial[0]},
			want: map[TargetKey]TargetState{
				keyDeployProd: state(keyDeployProd, []string{ruleA}, []string{selEverything}),
				keyPodProd:    state(keyPodProd, []string{ruleA}, []string{selEverything}),
			},
			wantNotified: false,
		},
		{
			name:         "emptying a rule drops every target it held",
			updated:      nil,
			want:         map[TargetKey]TargetState{},
			wantNotified: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := New()
			mustUpsert(t, reg, ruleA, initial)
			if n := pending(reg.Changes()); n != 1 {
				t.Fatalf("after initial Upsert: pending notifications = %d, want 1", n)
			}

			mustUpsert(t, reg, ruleA, tc.updated)

			assertSnapshot(t, reg, tc.want)
			if got := pending(reg.Changes()) > 0; got != tc.wantNotified {
				t.Errorf("notified = %t, want %t", got, tc.wantNotified)
			}
		})
	}
}

// TestSnapshotIsDeepCopy covers AC (c): a caller may do anything it likes to a
// snapshot without the registry noticing.
func TestSnapshotIsDeepCopy(t *testing.T) {
	reg := New()
	key := tkey(sinkDefault, gvkDeployment, nsProd)
	mustUpsert(t, reg, ruleA, []WatchTarget{target(sinkDefault, gvkDeployment, nsProd, selAppWeb)})
	mustUpsert(t, reg, ruleB, []WatchTarget{target(sinkDefault, gvkDeployment, nsProd, selAppAPI)})

	want := map[TargetKey]TargetState{
		key: state(key, []string{ruleA, ruleB}, []string{selAppAPI, selAppWeb}),
	}
	assertSnapshot(t, reg, want)

	snap := reg.Snapshot()
	st := snap[key]
	st.RuleKeys[0] = "vandalised"
	st.Selectors[0] = "vandalised"
	st.RuleKeys = append(st.RuleKeys, "extra")
	st.Selectors = append(st.Selectors, "extra")
	st.Key = tkey(sinkAudit, gvkPod, nsStaging)
	snap[key] = st
	snap[tkey(sinkAudit, gvkPod, nsStaging)] = st
	delete(snap, key)
	clear(snap)

	assertSnapshot(t, reg, want)
}

// TestSelectorCanonicalization covers the canonicalization AC: permuted
// spellings of one selector are one target with one merged selector, and a
// re-Upsert that only permutes the spelling is not a change.
func TestSelectorCanonicalization(t *testing.T) {
	permuted := target(sinkDefault, gvkDeployment, nsProd, "b=2,a=1")
	canonical := target(sinkDefault, gvkDeployment, nsProd, "a=1,b=2")

	if permuted.Key() != canonical.Key() {
		t.Fatalf("permuted selectors produced different TargetKeys: %v vs %v", permuted.Key(), canonical.Key())
	}

	reg := New()
	mustUpsert(t, reg, ruleA, []WatchTarget{permuted})
	mustUpsert(t, reg, ruleB, []WatchTarget{canonical})

	key := canonical.Key()
	assertSnapshot(t, reg, map[TargetKey]TargetState{
		key: state(key, []string{ruleA, ruleB}, []string{"a=1,b=2"}),
	})

	if n := pending(reg.Changes()); n == 0 {
		t.Fatal("expected a pending notification after the initial upserts")
	}
	mustUpsert(t, reg, ruleA, []WatchTarget{canonical})
	if n := pending(reg.Changes()); n != 0 {
		t.Errorf("re-spelling a selector notified %d times, want 0", n)
	}
}

// TestCanonicalSelector pins the metav1.LabelSelector conversion, including the
// deliberate nil => "match everything" deviation from
// metav1.LabelSelectorAsSelector.
func TestCanonicalSelector(t *testing.T) {
	tests := []struct {
		name     string
		selector *metav1.LabelSelector
		want     string
		wantErr  bool
	}{
		{
			name:     "nil selects everything",
			selector: nil,
			want:     selEverything,
		},
		{
			name:     "empty selects everything",
			selector: &metav1.LabelSelector{},
			want:     selEverything,
		},
		{
			name:     "matchLabels are sorted by key",
			selector: &metav1.LabelSelector{MatchLabels: map[string]string{"b": "2", "a": "1", "c": "3"}},
			want:     "a=1,b=2,c=3",
		},
		{
			name: "matchExpressions are sorted by key and value",
			selector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "tier", Operator: metav1.LabelSelectorOpIn, Values: []string{"web", "api"}},
					{Key: "env", Operator: metav1.LabelSelectorOpExists},
				},
			},
			want: "env,tier in (api,web)",
		},
		{
			name: "invalid operator is rejected",
			selector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "env", Operator: "Almost", Values: []string{"prod"}},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CanonicalSelector(tc.selector)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("CanonicalSelector() = %q, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("CanonicalSelector(): unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("CanonicalSelector() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestUpsertRejectsBadSelectorAtomically proves the all-or-nothing contract:
// one malformed selector degrades only the offending rule, and none of its
// other targets leak into the data plane.
func TestUpsertRejectsBadSelectorAtomically(t *testing.T) {
	reg := New()
	mustUpsert(t, reg, ruleA, []WatchTarget{target(sinkDefault, gvkDeployment, nsProd, selAppWeb)})
	before := reg.Snapshot()
	pending(reg.Changes())

	err := reg.Upsert(ruleB, []WatchTarget{
		target(sinkDefault, gvkPod, nsProd, selEverything),
		target(sinkDefault, gvkDeployment, nsStaging, "app=!!"),
	})
	if err == nil {
		t.Fatal("Upsert with a malformed selector returned nil error")
	}
	if !strings.Contains(err.Error(), ruleB) {
		t.Errorf("error %q does not name the offending rule %q", err, ruleB)
	}

	assertSnapshot(t, reg, before)
	if n := pending(reg.Changes()); n != 0 {
		t.Errorf("rejected Upsert notified %d times, want 0", n)
	}
}

// TestRemoveIsIdempotent lets reconcilers call Remove unconditionally on a
// delete event without generating spurious data-plane churn.
func TestRemoveIsIdempotent(t *testing.T) {
	reg := New()
	mustUpsert(t, reg, ruleA, []WatchTarget{target(sinkDefault, gvkDeployment, nsProd, selEverything)})
	pending(reg.Changes())

	reg.Remove(ruleA)
	if n := pending(reg.Changes()); n != 1 {
		t.Fatalf("first Remove notified %d times, want 1", n)
	}

	reg.Remove(ruleA)
	reg.Remove(ruleC)
	if n := pending(reg.Changes()); n != 0 {
		t.Errorf("removing unknown rules notified %d times, want 0", n)
	}
	assertSnapshot(t, reg, map[TargetKey]TargetState{})
}

// TestChangesCoalesce covers AC (e): a burst of writes leaves at most a single
// pending wake-up, so a slow WatchManager can never be flooded.
func TestChangesCoalesce(t *testing.T) {
	const bursts = 50

	reg := New()
	for i := range bursts {
		// Every iteration genuinely changes the desired state, so each one is
		// entitled to a notification; coalescing is what collapses them.
		mustUpsert(t, reg, ruleA, []WatchTarget{
			target(sinkDefault, gvkDeployment, fmt.Sprintf("ns-%d", i), selEverything),
		})
	}

	n := pending(reg.Changes())
	if n == 0 {
		t.Fatalf("%d changing Upserts produced no notification", bursts)
	}
	if n > 2 {
		t.Errorf("%d changing Upserts left %d pending notifications, want <= 2", bursts, n)
	}
}

// TestRegistryConcurrentAccess covers AC (d). Run under -race: 100 goroutines
// hammer Upsert/Remove/Snapshot while contending on a deliberately small set of
// shared TargetKeys. The final empty snapshot is the real assertion — it proves
// the ref-counts stayed balanced under interleaving, which a race detector
// alone would not catch.
func TestRegistryConcurrentAccess(t *testing.T) {
	const (
		goroutines = 100
		iterations = 50
	)
	namespaces := []string{nsProd, nsStaging, "dev"}
	maxTargets := len(namespaces) * 2

	reg := New()
	var writers sync.WaitGroup
	for g := range goroutines {
		writers.Go(func() {
			ruleKey := fmt.Sprintf("rule-%d", g)
			for i := range iterations {
				ns := namespaces[(g+i)%len(namespaces)]
				targets := []WatchTarget{
					target(sinkDefault, gvkDeployment, ns, selEverything),
					target(sinkAudit, gvkPod, ns, fmt.Sprintf("shard=%d", i%3)),
				}
				if err := reg.Upsert(ruleKey, targets); err != nil {
					t.Errorf("Upsert(%q): unexpected error: %v", ruleKey, err)
					return
				}
				if snap := reg.Snapshot(); len(snap) > maxTargets {
					t.Errorf("snapshot holds %d targets, want at most %d", len(snap), maxTargets)
					return
				}
				if i%2 == 1 {
					reg.Remove(ruleKey)
				}
			}
			reg.Remove(ruleKey)
		})
	}
	// A concurrent reader on the notification channel, so the non-blocking
	// send races against a real receiver rather than only against a full
	// buffer. It also proves Changes never blocks a writer.
	stop := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case <-reg.Changes():
			case <-stop:
				return
			}
		}
	}()

	writers.Wait()
	close(stop)
	<-drained

	assertSnapshot(t, reg, map[TargetKey]TargetState{})
}

// TestPackageImportsRemainMinimal enforces the dependency budget stated in the
// task: internal/plan is the operator's shared state and must never be able to
// reach a Kubernetes client, a sink *instance*, or a clock. Parsing the package's
// own source (rather than shelling out to `go list -deps`) keeps the check
// hermetic and constrains what this package *writes*, which is the property under
// review; transitive dependencies of apimachinery are not ours to police.
//
// internal/sink joined the budget in Task 4.1, for sink.ID and nothing else: a
// target has to say *which* sink it streams to, and once identity is typed that
// statement can only be made in the type the runtime keys on. What the budget
// still forbids is the thing it was written for — this package resolving,
// holding or writing to a backend — and isAllowedImport keeps that narrow by
// naming the one package rather than the whole internal tree.
func TestPackageImportsRemainMinimal(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		file, err := parser.ParseFile(fset, entry.Name(), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatalf("parse import path %s in %s: %v", spec.Path.Value, entry.Name(), err)
			}
			if isAllowedImport(path) {
				continue
			}
			t.Errorf("%s imports %q: internal/plan may only import the standard library, "+
				"k8s.io/apimachinery, and internal/sink (for sink.ID)", entry.Name(), path)
		}
	}
}

// isAllowedImport reports whether an import path is in internal/plan's budget.
// A standard-library path never has a dot in its first element, which is the
// same heuristic the go command itself uses to tell a module path from a
// standard one.
func isAllowedImport(path string) bool {
	root, _, _ := strings.Cut(path, "/")
	if !strings.Contains(root, ".") {
		return true
	}
	if path == "k8s.io/apimachinery" || strings.HasPrefix(path, "k8s.io/apimachinery/") {
		return true
	}
	// internal/sink, exactly — for sink.ID (see TestPackageImportsRemainMinimal).
	// Not a prefix match: internal/sink/clickhouse is a driver, and the whole point
	// of this budget is that this package can never reach one.
	return path == "github.com/kuberecord/kuberecord/internal/sink"
}

// TestRulesForSink covers the accessor the sink runtime uses to name a vanished
// sink's dependents, including the two properties that make the answer usable: it
// unions across a rule's several targets, and it reports nothing for a rule that
// currently contributes nothing (that rule's own reconcile already wrote a more
// specific condition than "your sink is gone").
func TestRulesForSink(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*Registry)
		sink  sink.ID
		want  []string
	}{
		{
			name:  "an unknown sink has no dependents",
			setup: func(*Registry) {},
			sink:  sinkDefault,
			want:  nil,
		},
		{
			name: "one rule, several targets, reported once",
			setup: func(reg *Registry) {
				mustUpsert(t, reg, ruleA, []WatchTarget{
					target(sinkDefault, gvkPod, nsProd, selEverything),
					target(sinkDefault, gvkDeployment, nsProd, selEverything),
					target(sinkDefault, gvkPod, nsStaging, selEverything),
				})
			},
			sink: sinkDefault,
			want: []string{ruleA},
		},
		{
			name: "several rules on one sink are sorted",
			setup: func(reg *Registry) {
				mustUpsert(t, reg, ruleB, []WatchTarget{target(sinkDefault, gvkPod, nsProd, selEverything)})
				mustUpsert(t, reg, ruleA, []WatchTarget{target(sinkDefault, gvkPod, nsStaging, selEverything)})
			},
			sink: sinkDefault,
			want: []string{ruleA, ruleB},
		},
		{
			name: "only the named sink's dependents are reported",
			setup: func(reg *Registry) {
				mustUpsert(t, reg, ruleA, []WatchTarget{target(sinkDefault, gvkPod, nsProd, selEverything)})
				mustUpsert(t, reg, ruleB, []WatchTarget{target(sinkAudit, gvkPod, nsProd, selEverything)})
			},
			sink: sinkAudit,
			want: []string{ruleB},
		},
		{
			// The collision typed identity exists to prevent: both sinks are named
			// "default" and are legal at once in etcd, so a name-only match would
			// report the ClickHouseSink's rule as a dependent of the S3Sink and park
			// it when the S3Sink was deleted.
			name: "a same-named sink of another kind is not the same sink",
			setup: func(reg *Registry) {
				mustUpsert(t, reg, ruleA, []WatchTarget{target(sinkDefault, gvkPod, nsProd, selEverything)})
			},
			sink: sink.ID{Kind: "S3Sink", Name: sinkDefault.Name},
			want: nil,
		},
		{
			name: "a rule that contributes nothing is not a dependent",
			setup: func(reg *Registry) {
				mustUpsert(t, reg, ruleA, []WatchTarget{target(sinkDefault, gvkPod, nsProd, selEverything)})
				mustUpsert(t, reg, ruleA, nil)
			},
			sink: sinkDefault,
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := New()
			tc.setup(reg)
			if got := reg.RulesForSink(tc.sink); !slices.Equal(got, tc.want) {
				t.Errorf("RulesForSink(%s) = %v, want %v", tc.sink, got, tc.want)
			}
		})
	}
}

// TestTargetCountForRule covers the accessor status.activeWatches is read from. The
// load-bearing case is the last one: two selectors on one (sink, GVK, namespace)
// contribute one *scope* to the data plane, so counting WatchTargets instead of
// TargetKeys would make a rule claim twice the watches it has.
func TestTargetCountForRule(t *testing.T) {
	tests := []struct {
		name    string
		targets []WatchTarget
		want    int
	}{
		{
			name: "no targets",
			want: 0,
		},
		{
			name:    "one target",
			targets: []WatchTarget{target(sinkDefault, gvkPod, nsProd, selEverything)},
			want:    1,
		},
		{
			name: "distinct namespaces count separately",
			targets: []WatchTarget{
				target(sinkDefault, gvkPod, nsProd, selEverything),
				target(sinkDefault, gvkPod, nsStaging, selEverything),
			},
			want: 2,
		},
		{
			name: "distinct sinks count separately",
			targets: []WatchTarget{
				target(sinkDefault, gvkPod, nsProd, selEverything),
				target(sinkAudit, gvkPod, nsProd, selEverything),
			},
			want: 2,
		},
		{
			name: "two selectors on one scope are one watch",
			targets: []WatchTarget{
				target(sinkDefault, gvkPod, nsProd, selAppWeb),
				target(sinkDefault, gvkPod, nsProd, selAppAPI),
			},
			want: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := New()
			mustUpsert(t, reg, ruleA, tc.targets)
			if got := reg.TargetCountForRule(ruleA); got != tc.want {
				t.Errorf("TargetCountForRule(%q) = %d, want %d", ruleA, got, tc.want)
			}
			// A rule the registry never knew is zero, not a panic: a reconciler
			// reads the count on every pass, including the one where its Upsert
			// was refused.
			if got := reg.TargetCountForRule(ruleC); got != 0 {
				t.Errorf("TargetCountForRule(unknown) = %d, want 0", got)
			}
		})
	}
}

// redactingTarget is target() plus a redaction path set, for the specs below.
// The ordinary target() helper leaves Redaction empty, which is what every rule
// that configures no redaction contributes.
//
// It pins the sink, kind and namespace because every spec below is about one
// target: what varies between them is which rules contribute it, under which
// selector, asking for which paths.
func redactingTarget(selector, redaction string) WatchTarget {
	t := target(sinkDefault, gvkDeployment, nsProd, selector)
	t.Redaction = redaction
	return t
}

// TestRedactionMergesAsAUnion covers the Task 3.3 property the data plane
// depends on: rules landing on one target contribute their redaction path sets
// to a union, and a set survives exactly as long as some rule still asks for it.
//
// Union rather than replacement is a security property, not a merge convenience:
// one target is one hashCache entry and one stored payload, so if the two
// disagreed and the registry picked a winner, adding a rule could *unredact*
// another rule's stream.
func TestRedactionMergesAsAUnion(t *testing.T) {
	const (
		floorOnly = "data.password"
		withExtra = "data.password\nspec.containers[*].env[*].value"
	)

	reg := New()
	key := tkey(sinkDefault, gvkDeployment, nsProd)

	mustUpsert(t, reg, ruleA, []WatchTarget{
		redactingTarget(selEverything, floorOnly),
	})
	mustUpsert(t, reg, ruleB, []WatchTarget{
		redactingTarget(selEverything, withExtra),
	})
	// A third rule on the same target that configures nothing: it must add no
	// entry at all, since "" means "I contribute no paths" rather than a real
	// member of the set (unlike the empty *selector*, which means "everything").
	mustUpsert(t, reg, ruleC, []WatchTarget{
		target(sinkDefault, gvkDeployment, nsProd, selEverything),
	})

	want := TargetState{
		Key:        key,
		RuleKeys:   []string{ruleA, ruleB, ruleC},
		Selectors:  []string{selEverything},
		Redactions: []string{floorOnly, withExtra},
	}
	assertSnapshot(t, reg, map[TargetKey]TargetState{key: want})

	// Dropping the rule that asked for the extra path withdraws that set and
	// leaves the other one standing.
	reg.Remove(ruleB)
	want.RuleKeys = []string{ruleA, ruleC}
	want.Redactions = []string{floorOnly}
	assertSnapshot(t, reg, map[TargetKey]TargetState{key: want})

	// The rule that never contributed a set cannot remove one either.
	reg.Remove(ruleC)
	want.RuleKeys = []string{ruleA}
	assertSnapshot(t, reg, map[TargetKey]TargetState{key: want})
}

// TestRedactionRefCountsAcrossOneRulesTargets covers the ref-counting case the
// per-rule map exists for: one rule contributing the same target twice (two
// selectors, same kind and namespace) must not have the first contribution's
// removal drop a path set the second still wants.
func TestRedactionRefCountsAcrossOneRulesTargets(t *testing.T) {
	const redaction = "data.password"
	reg := New()
	key := tkey(sinkDefault, gvkDeployment, nsProd)

	mustUpsert(t, reg, ruleA, []WatchTarget{
		redactingTarget(selAppWeb, redaction),
		redactingTarget(selAppAPI, redaction),
	})
	assertSnapshot(t, reg, map[TargetKey]TargetState{
		key: {
			Key:        key,
			RuleKeys:   []string{ruleA},
			Selectors:  []string{selAppAPI, selAppWeb},
			Redactions: []string{redaction},
		},
	})

	// Drop one of the two selectors: the redaction set is still wanted by the
	// other contribution.
	mustUpsert(t, reg, ruleA, []WatchTarget{
		redactingTarget(selAppAPI, redaction),
	})
	assertSnapshot(t, reg, map[TargetKey]TargetState{
		key: {
			Key:        key,
			RuleKeys:   []string{ruleA},
			Selectors:  []string{selAppAPI},
			Redactions: []string{redaction},
		},
	})
}

// TestRedactionEditIsATargetChange covers the level-triggering consequence: a
// rule that edits only its redaction policy must notify the data plane, because
// the compiled policy has to be rebuilt — while a re-Upsert of the same policy
// must not, or every resync would churn.
func TestRedactionEditIsATargetChange(t *testing.T) {
	reg := New()
	initial := []WatchTarget{
		redactingTarget(selEverything, "data.password"),
	}
	mustUpsert(t, reg, ruleA, initial)
	if n := pending(reg.Changes()); n != 1 {
		t.Fatalf("after the initial Upsert: pending notifications = %d, want 1", n)
	}

	mustUpsert(t, reg, ruleA, initial)
	if n := pending(reg.Changes()); n != 0 {
		t.Errorf("re-Upserting an unchanged policy notified %d times, want 0", n)
	}

	mustUpsert(t, reg, ruleA, []WatchTarget{
		redactingTarget(selEverything, "data.password\ndata.token"),
	})
	if n := pending(reg.Changes()); n != 1 {
		t.Errorf("editing the policy notified %d times, want 1", n)
	}
	key := tkey(sinkDefault, gvkDeployment, nsProd)
	assertSnapshot(t, reg, map[TargetKey]TargetState{
		key: {
			Key:        key,
			RuleKeys:   []string{ruleA},
			Selectors:  []string{selEverything},
			Redactions: []string{"data.password\ndata.token"},
		},
	})
}
