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

package query_test

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"github.com/kuberecord/kuberecord/internal/query"
)

// resourceStatesColumns is the frozen schema's resource_states column set,
// transcribed here from the schema documentation rather than imported.
//
// Transcribing it is the point. Importing the backend's column list would make
// this test agree with whatever that backend currently does, which is precisely
// the coupling the read plane exists to avoid; a second, independent copy is what
// turns a rename on either side into a failing test instead of a silent drift in
// a contract people script against.
var resourceStatesColumns = []string{
	"actors",
	"api_group",
	"api_version",
	"cluster_id",
	"data",
	"diff",
	"event_type",
	"kind",
	"labels",
	"name",
	"namespace",
	"resource_version",
	"sha256",
	"ts",
	"uid",
}

// watchScopesColumns is the frozen schema's watch_scopes column set, transcribed
// for the same reason.
var watchScopesColumns = []string{
	"action",
	"api_group",
	"api_version",
	"cluster_id",
	"kind",
	"namespace",
	"rule_ref",
	"ts",
}

// TestSerializedFieldNames pins the JSON spelling of every type a caller renders.
//
// These names are a public contract: people write jq against them, and the whole
// reason they mirror the schema's columns is so a recipe written against a SQL
// result keeps working against command-line output. Nothing in Go objects to
// renaming a struct tag, so this test is the only thing standing between a tidy-up
// and somebody's pipeline breaking after an upgrade.
func TestSerializedFieldNames(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  []string
	}{
		{
			name:  "ObjectRef carries the identity columns",
			value: query.ObjectRef{},
			want:  []string{"api_group", "cluster_id", "kind", "name", "namespace"},
		},
		{
			name:  "Change carries the per-observation columns",
			value: query.Change{},
			want: []string{
				"actors", "api_version", "data", "diff", "event_type",
				"labels", "resource_version", "sha256", "ts", "uid",
			},
		},
		{
			name:  "ScopeInterval names its scope columns as the schema spells them",
			value: query.ScopeInterval{},
			want:  []string{"api_group", "from", "kind", "namespace", "rule_ref", "to"},
		},
		{
			name:  "Incarnation",
			value: query.Incarnation{},
			want:  []string{"deleted", "first_seen", "last_seen", "uid"},
		},
		{
			name:  "Reconstruction reports its own provenance",
			value: query.Reconstruction{},
			want:  []string{"base_event", "base_ts", "object", "patches_applied", "sha256"},
		},
		{
			name:  "Capabilities",
			value: query.Capabilities{},
			want: []string{
				"backend", "deletions", "point_query",
				"server_side_filter", "time_bound_required",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := marshalledKeys(t, tc.value)
			if !slices.Equal(got, tc.want) {
				t.Errorf("serialized field names:\n got %v\nwant %v", got, tc.want)
			}
		})
	}
}

// TestChangeAndObjectRefTogetherCoverResourceStates is the strong form of the
// mirroring requirement, and the reason the split between the two types is safe.
//
// A Change is deliberately not self-describing: it omits the identity columns
// because every change in a result already belongs to one queried ObjectRef, and
// repeating five identical strings on every row of a hundred-thousand-row stream
// would be waste. What has to remain true is that the two halves together spell
// the whole row, exactly once each — so that a caller reassembling them produces
// the schema's column set with nothing missing and nothing duplicated under two
// names.
func TestChangeAndObjectRefTogetherCoverResourceStates(t *testing.T) {
	ref := marshalledKeys(t, query.ObjectRef{})
	change := marshalledKeys(t, query.Change{})

	for _, key := range ref {
		if slices.Contains(change, key) {
			t.Errorf("%q appears on both ObjectRef and Change: "+
				"a reassembled row would carry it twice", key)
		}
	}

	union := slices.Concat(ref, change)
	slices.Sort(union)
	if !slices.Equal(union, resourceStatesColumns) {
		t.Errorf("ObjectRef + Change do not spell the resource_states columns:\n"+
			" got %v\nwant %v", union, resourceStatesColumns)
	}
}

// TestScopeIntervalUsesSchemaSpellings checks the half of ScopeInterval that
// corresponds to stored columns, and states which half does not.
//
// From and To are derived: the scope log records individual Started and Stopped
// rows, and an interval is what pairing them produces. They are therefore allowed
// to be absent from the column set, while every remaining field is required to be
// spelled the way the column is — the case that would otherwise slip through is a
// field renamed to something more natural in Go ("rule" for "rule_ref") that
// quietly breaks the jq recipes the schema documentation publishes.
func TestScopeIntervalUsesSchemaSpellings(t *testing.T) {
	derived := []string{"from", "to"}

	for _, key := range marshalledKeys(t, query.ScopeInterval{}) {
		if slices.Contains(derived, key) {
			continue
		}
		if !slices.Contains(watchScopesColumns, key) {
			t.Errorf("ScopeInterval field %q is neither a watch_scopes column nor "+
				"one of the derived interval bounds %v", key, derived)
		}
	}
}

// marshalledKeys returns the sorted top-level JSON keys a value serializes to.
// Going through encoding/json rather than reflecting over struct tags is
// deliberate: it measures what a caller actually receives, including any field
// that has been made to disappear with a "-" tag or an unexported name.
func marshalledKeys(t *testing.T, value any) []string {
	t.Helper()

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal %T: %v", value, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("unmarshal %T into a field map: %v", value, err)
	}
	return slices.Sorted(maps.Keys(fields))
}
