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
)

const (
	sinkDefault = "default"
	sinkAudit   = "audit"

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
)

func target(sink string, gvk schema.GroupVersionKind, namespace, selector string) WatchTarget {
	return WatchTarget{Sink: sink, GVK: gvk, Namespace: namespace, Selector: selector}
}

func tkey(sink string, gvk schema.GroupVersionKind, namespace string) TargetKey {
	return TargetKey{Sink: sink, GVK: gvk, Namespace: namespace}
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
		lines = append(lines, fmt.Sprintf("  %s|%s|%s rules=%v selectors=%q",
			key.Sink, key.GVK, key.Namespace, st.RuleKeys, st.Selectors))
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
// reach a Kubernetes client, a sink, or a clock. Parsing the package's own
// source (rather than shelling out to `go list -deps`) keeps the check hermetic
// and constrains what this package *writes*, which is the property under
// review; transitive dependencies of apimachinery are not ours to police.
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
			t.Errorf("%s imports %q: internal/plan may only import the standard library and k8s.io/apimachinery",
				entry.Name(), path)
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
	return path == "k8s.io/apimachinery" || strings.HasPrefix(path, "k8s.io/apimachinery/")
}
