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

package sink

import "testing"

// TestIDsWithTheSameNameNeverCollideAcrossKinds is the regression guard for the
// collision typed sink identity exists to prevent.
//
// Both of these are legal at once in etcd — sink CRs are cluster-scoped and
// uniqueness is per *kind*, so a ClickHouseSink named "default" and an S3Sink
// named "default" coexist happily. While the runtime keyed on the bare name,
// whichever reconciled second silently displaced the first in the routing table:
// rules would then stream to the wrong backend while carrying the other one's
// hashCache and warm state, re-emitting objects or suppressing genuine changes,
// with nothing in the logs to say so.
//
// The property asserted is therefore not "the struct has two fields" but the two
// consequences of that: the pair is a *distinct map key*, which is what keeps the
// routing table and every per-sink cache separate, and it *renders distinctly*,
// which is what keeps a log line and a metric series attributable to one backend.
func TestIDsWithTheSameNameNeverCollideAcrossKinds(t *testing.T) {
	clickhouse := ID{Kind: DefaultSinkKind, Name: "default"}
	s3 := ID{Kind: "S3Sink", Name: "default"}

	if clickhouse == s3 {
		t.Fatalf("%s and %s compare equal; sink identity is not kind-aware", clickhouse, s3)
	}

	// The map is the real subject: this is the shape of the manager's routing
	// table, the pipeline's per-sink state registry and the reconciler's probe
	// store alike.
	routes := map[ID]string{}
	routes[clickhouse] = "clickhouse-writer"
	routes[s3] = "s3-writer"

	if len(routes) != 2 {
		t.Fatalf("two sinks of different kinds sharing a name occupy %d map keys, want 2", len(routes))
	}
	if got := routes[clickhouse]; got != "clickhouse-writer" {
		t.Errorf("routes[%s] = %q, want the ClickHouse writer; the S3 sink overwrote it", clickhouse, got)
	}
	if got := routes[s3]; got != "s3-writer" {
		t.Errorf("routes[%s] = %q, want the S3 writer; the ClickHouse sink overwrote it", s3, got)
	}

	// And the rendering used for logs, metric labels and condition messages tells
	// the two apart, so an operator reading either can name the backend meant.
	if got, want := clickhouse.String(), "ClickHouseSink/default"; got != want {
		t.Errorf("ID.String() = %q, want %q", got, want)
	}
	if got, want := s3.String(), "S3Sink/default"; got != want {
		t.Errorf("ID.String() = %q, want %q", got, want)
	}
	if clickhouse.String() == s3.String() {
		t.Errorf("both sinks render as %q; a log line or metric series could not be attributed",
			clickhouse.String())
	}
}

// TestDefaultSinkKindIsTheFirstBackend pins the constant's value: it is the kind
// every v0.1.0 rule named implicitly, so a legacy unqualified name lifts onto
// exactly this kind and nothing else. Changing it would silently re-point every
// such reference at a different backend.
func TestDefaultSinkKindIsTheFirstBackend(t *testing.T) {
	if DefaultSinkKind != "ClickHouseSink" {
		t.Errorf("DefaultSinkKind = %q, want ClickHouseSink", DefaultSinkKind)
	}
}

// TestIDCompareOrdersByKindThenName covers the ordering every human-facing
// iteration (the start-up pass over pending sinks, SinkIDs, and the watch layer's
// interest sort) relies on for stable output now that its keys are structs
// rather than sortable strings.
func TestIDCompareOrdersByKindThenName(t *testing.T) {
	tests := []struct {
		name string
		a, b ID
		want int
	}{
		{"same identity", ID{"ClickHouseSink", "a"}, ID{"ClickHouseSink", "a"}, 0},
		{"kind decides first", ID{"ClickHouseSink", "z"}, ID{"S3Sink", "a"}, -1},
		{"name breaks a kind tie", ID{"S3Sink", "a"}, ID{"S3Sink", "b"}, -1},
		{"reversed name tie", ID{"S3Sink", "b"}, ID{"S3Sink", "a"}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Compare(tc.b); got != tc.want {
				t.Errorf("%s.Compare(%s) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
