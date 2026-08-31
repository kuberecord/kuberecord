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

package replay

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The Attribution rules, driven directly.
//
// blame_test.go exercises the command end to end against one story. These are the
// cases underneath it — the ones where two defensible implementations disagree
// and only one of them is honest: an interior write, a checkpoint carrying both a
// diff and the state that diff produced, a full-state row that says nothing about
// which fields it moved, and every shape of removal.

// attributionCase is one replay and the table it must produce.
type attributionCase struct {
	name string
	// seed is the state as it stood before the first row, empty for none.
	seed string
	rows []render.TimelineRow
	// fields and depth are the display narrowing, applied after the replay.
	fields []string
	depth  int
	// want is describeBlame's rendering of the expected rows, in order.
	want []string
	// notice is a fragment the replay must report, empty when it must report
	// nothing.
	notice string
}

func TestAttributeRun(t *testing.T) {
	tests := []attributionCase{
		{
			name: "an interior write claims every leaf beneath it",
			seed: `{"spec":{"a":"1","b":"2"}}`,
			rows: []render.TimelineRow{
				patchRow("14:01:00", "alice", `[{"op":"replace","path":"/spec/a","value":"9"}]`),
				patchRow("14:02:00", "bob",
					`[{"op":"replace","path":"/spec","value":{"a":"9","b":"3"}}]`),
			},
			// Both leaves belong to bob: the block replace is the change that last
			// wrote them, even though it names neither. Crediting alice with spec.a
			// is the wrong answer that looks right.
			want: []string{"spec.a = bob@14:02:00", "spec.b = bob@14:02:00"},
		},
		{
			name: "a checkpoint's own diff is not applied over the state it produced",
			// The diff below would fail against this seed — there is no /spec/a to
			// replace — so a replay that applied it on top of the checkpoint's data
			// would stall here and lose everything after it.
			seed: `{"spec":{}}`,
			rows: []render.TimelineRow{
				fullStateRow("14:01:00", "alice", query.EventCheckpoint, `{"spec":{"a":"1"}}`,
					`[{"op":"replace","path":"/spec/a","value":"1"}]`),
				patchRow("14:02:00", "bob", `[{"op":"add","path":"/spec/b","value":"2"}]`),
			},
			want: []string{"spec.b = bob@14:02:00", "spec.a = alice@14:01:00"},
		},
		{
			name: "a full-state row with no patch is attributed by comparison",
			seed: `{"spec":{"a":"1","b":"2"}}`,
			rows: []render.TimelineRow{
				fullStateRow("14:01:00", "carol", query.EventModified,
					`{"spec":{"a":"9","b":"2","c":"3"}}`, ""),
			},
			// The row moved two fields and left one alone, and said none of that.
			// Attributing all three would name carol against a field she did not
			// touch; attributing none would credit the field she did touch to
			// whoever wrote it before her.
			want: []string{
				"spec.a = carol@14:01:00", "spec.c = carol@14:01:00", "spec.b = (before window)",
			},
		},
		{
			name: "a first sighting with nothing before it wrote every field",
			rows: []render.TimelineRow{
				fullStateRow("14:01:00", "dave", query.EventAdded, `{"spec":{"a":"1"}}`, ""),
			},
			want: []string{"spec.a = dave@14:01:00"},
		},
		{
			name: "a subtree removal is one row rather than one per field",
			seed: `{"spec":{"a":"1","b":{"c":"2","d":"3"}}}`,
			rows: []render.TimelineRow{
				patchRow("14:01:00", "alice", `[{"op":"remove","path":"/spec/a"}]`),
				patchRow("14:02:00", "bob", `[{"op":"remove","path":"/spec/b"}]`),
			},
			// The emptied container is a field in its own right now — spec holds {} —
			// and it is attributed to the removal that emptied it rather than left
			// reading "(before window)" beside the removals that produced it.
			want: []string{
				"spec = bob@14:02:00", "spec.b (removed) = bob@14:02:00",
				"spec.a (removed) = alice@14:01:00",
			},
		},
		{
			name: "a field that comes back is not reported as removed",
			seed: `{"spec":{"a":"1"}}`,
			rows: []render.TimelineRow{
				patchRow("14:01:00", "alice", `[{"op":"remove","path":"/spec/a"}]`),
				patchRow("14:02:00", "bob", `[{"op":"add","path":"/spec/a","value":"2"}]`),
			},
			want: []string{"spec.a = bob@14:02:00"},
		},
		{
			name: "an empty object is a field somebody set",
			seed: `{"spec":{"selector":{},"ports":[]}}`,
			rows: []render.TimelineRow{},
			// Recursing past them would produce no pointer at all, and the fields
			// would vanish from a table that claims to list the object's fields.
			want: []string{"spec.ports = (before window)", "spec.selector = (before window)"},
		},
		{
			name: "a key holding a slash keeps its escape",
			seed: `{"metadata":{"annotations":{"deployment.kubernetes.io/revision":"1"}}}`,
			rows: []render.TimelineRow{
				patchRow("14:01:00", "alice",
					`[{"op":"replace","path":"/metadata/annotations/deployment.kubernetes.io~1revision",`+
						`"value":"2"}]`),
			},
			// One field, not two: the pointer's ~1 is a slash inside a single member
			// name, and a reading that split it would attribute nothing and list a
			// field nobody has.
			want: []string{"metadata.annotations.deployment.kubernetes.io/revision = alice@14:01:00"},
		},
		{
			name: "a merged Kubernetes Event attributes nothing",
			seed: `{"spec":{"a":"1"}}`,
			rows: []render.TimelineRow{{
				Change: query.Change{
					TS: instant("14:01:00"), EventType: query.EventKubernetes,
					Actors: []string{"kubelet"},
					Diff:   `[{"op":"replace","path":"/spec/a","value":"9"}]`,
				},
			}},
			// Every field of such a row describes the Event object rather than the
			// object whose timeline it was merged into.
			want: []string{"spec.a = (before window)"},
		},
		{
			name: "a deletion ends the run",
			seed: `{"spec":{"a":"1"}}`,
			rows: []render.TimelineRow{
				patchRow("14:01:00", "alice", `[{"op":"replace","path":"/spec/a","value":"9"}]`),
				{Change: query.Change{TS: instant("14:02:00"), EventType: query.EventDeleted}},
				patchRow("14:03:00", "bob", `[{"op":"replace","path":"/spec/a","value":"8"}]`),
			},
			want:   []string{"spec.a = alice@14:01:00"},
			notice: "deleted at",
		},
		{
			name: "a patch that will not apply stops the field list and not the attribution",
			seed: `{"spec":{"a":"1"}}`,
			rows: []render.TimelineRow{
				patchRow("14:01:00", "alice", `[{"op":"replace","path":"/spec/nonesuch","value":"9"}]`),
			},
			// The path is still attributed — a patch names what it writes whether or
			// not it applies — and the object's own fields are still listed.
			want:   []string{"spec.nonesuch (removed) = alice@14:01:00", "spec.a = (before window)"},
			notice: "did not apply to the reconstructed state",
		},
		{
			name: "an undecodable patch is reported rather than credited to somebody",
			seed: `{"spec":{"a":"1"}}`,
			rows: []render.TimelineRow{{
				Change: query.Change{
					TS: instant("14:01:00"), EventType: query.EventModified,
					Actors: []string{"alice"}, Diff: `{"not":"a patch"}`,
				},
				PatchErr: "decoding the recorded patch: json: cannot unmarshal object",
			}},
			want:   []string{"spec.a = (before window)"},
			notice: "could not be decoded",
		},
		{
			name: "--field narrows to a subtree",
			seed: `{"spec":{"a":"1"},"status":{"b":"2"}}`,
			rows: []render.TimelineRow{
				patchRow("14:01:00", "alice", `[{"op":"replace","path":"/status/b","value":"9"}]`),
			},
			fields: []string{"spec"},
			want:   []string{"spec.a = (before window)"},
		},
		{
			name: "--depth collapses to the newest write beneath it",
			seed: `{"spec":{"template":{"spec":{"x":"1","y":"2"}}}}`,
			rows: []render.TimelineRow{
				patchRow("14:01:00", "alice",
					`[{"op":"replace","path":"/spec/template/spec/x","value":"9"}]`),
				patchRow("14:02:00", "bob",
					`[{"op":"replace","path":"/spec/template/spec/y","value":"8"}]`),
			},
			depth: 2,
			want:  []string{"spec.template = bob@14:02:00"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var seed []byte
			if test.seed != "" {
				seed = []byte(test.seed)
			}

			result := AttributeRun(seed, test.rows)
			got := describeBlame(result.BlameRows(test.fields, test.depth))
			if !slices.Equal(got, test.want) {
				t.Errorf("the attribution is wrong.\nwant %v\ngot  %v", test.want, got)
			}
			assertNotice(t, result.Notices, test.notice)
		})
	}
}

// TestBlameRowsCountCollapsedFields is the other half of --depth: a collapsed row
// has to say how many fields it stands for, or it reads as a single field.
func TestBlameRowsCountCollapsedFields(t *testing.T) {
	result := AttributeRun([]byte(`{"spec":{"template":{"spec":{"x":"1","y":"2","z":"3"}}}}`), nil)

	rows := result.BlameRows(nil, 2)
	if len(rows) != 1 {
		t.Fatalf("%d rows at depth 2, want 1: %v", len(rows), describeBlame(rows))
	}
	if rows[0].Fields != 3 {
		t.Errorf("the collapsed row stands for %d fields, want 3", rows[0].Fields)
	}
}

// TestComparePointersOrdersIndicesAsNumbers keeps a container array in the order a
// reader expects.
//
// Lexicographic ordering of the whole pointer would put containers[10] between
// containers[1] and containers[2], which reads as data in no order at all.
func TestComparePointersOrdersIndicesAsNumbers(t *testing.T) {
	pointers := []string{
		"/spec/containers/10/image", "/spec/containers/2/image", "/spec/containers/1/image",
		"/spec/containers", "/spec/affinity",
	}
	slices.SortFunc(pointers, comparePointers)

	want := []string{
		"/spec/affinity", "/spec/containers", "/spec/containers/1/image",
		"/spec/containers/2/image", "/spec/containers/10/image",
	}
	if !slices.Equal(pointers, want) {
		t.Errorf("pointers sorted to\n%v\nwant\n%v", pointers, want)
	}
}

// instant builds a fixture timestamp on the fixed day these cases share.
func instant(clock string) time.Time {
	parsed, err := time.Parse(time.RFC3339, "2026-08-28T"+clock+"Z")
	if err != nil {
		panic(err)
	}
	return parsed
}

// patchRow is one recorded modification carrying a diff.
func patchRow(clock, actor, diff string) render.TimelineRow {
	return DecodeRows([]query.Change{{
		TS: instant(clock), EventType: query.EventModified, UID: "uid-1",
		Actors: []string{actor}, Diff: diff,
	}})[0]
}

// fullStateRow is one recorded change carrying state, with or without a diff.
func fullStateRow(clock, actor, event, data, diff string) render.TimelineRow {
	return DecodeRows([]query.Change{{
		TS: instant(clock), EventType: event, UID: "uid-1",
		Actors: []string{actor}, Data: data, Diff: diff,
	}})[0]
}

// describeBlame renders the rows compactly enough to be read as an expectation.
func describeBlame(rows []render.BlameRow) []string {
	described := make([]string, 0, len(rows))
	for _, row := range rows {
		path := row.Path
		if row.Removed {
			path += " (removed)"
		}
		if !row.Attributed {
			described = append(described, path+" = "+render.BeforeWindow)
			continue
		}
		actor := render.UnknownActor
		if len(row.Actors) > 0 {
			actor = row.Actors[0]
		}
		described = append(described,
			fmt.Sprintf("%s = %s@%s", path, actor, row.TS.UTC().Format("15:04:05")))
	}
	return described
}

// assertNotice checks that the replay reported what it had to and nothing it did
// not.
//
// Both directions matter: a degradation with no notice is the silent error
// Invariant 4 forbids, and a notice on a replay that went perfectly is noise that
// teaches a reader to skip the stream the honest half of this output goes to.
func assertNotice(t *testing.T, notices []render.Notice, want string) {
	t.Helper()

	if want == "" {
		if len(notices) > 0 {
			t.Errorf("the replay reported %q, but nothing about it needed qualifying", notices[0].Text)
		}
		return
	}
	for _, notice := range notices {
		if strings.Contains(notice.Text, want) {
			return
		}
	}
	t.Errorf("no notice mentions %q; the replay reported %v", want, notices)
}
