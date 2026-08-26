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

package harness

import (
	"testing"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// The S3 read layer's two pure decisions, tested without a cluster: which records
// a filter matches, and which objects in a bucket are the archive.
//
// They are worth testing rather than reviewing because both are *silent* when
// wrong in the direction that matters. A filter that ignored a field would make
// every count in the e2e suite larger than it should be and every "exactly one"
// assertion pass for the wrong reason; a key classifier that admitted the scope
// log would feed lines of a different shape to a decoder typed for records. Both
// would read as a green suite.

// filterFixture is the archive under test: one namespace, one kind, several
// objects and event types.
func filterFixture() []sink.Record {
	return []sink.Record{
		{ClusterID: "c1", APIGroup: "apps", Kind: "Deployment", Namespace: "demo", Name: "web",
			UID: "uid-web", EventType: EventSnapshot},
		{ClusterID: "c1", APIGroup: "apps", Kind: "Deployment", Namespace: "demo", Name: "web",
			UID: "uid-web", EventType: EventModified},
		{ClusterID: "c1", APIGroup: "apps", Kind: "Deployment", Namespace: "demo", Name: "web",
			UID: "uid-web-2", EventType: EventSnapshot},
		{ClusterID: "c1", APIGroup: "apps", Kind: "Deployment", Namespace: "other", Name: "web",
			UID: "uid-elsewhere", EventType: EventSnapshot},
		{ClusterID: "c1", APIGroup: "", Kind: "Node", Namespace: "", Name: "node-1",
			UID: "uid-node", EventType: EventSnapshot},
		{ClusterID: "c2", APIGroup: "apps", Kind: "Deployment", Namespace: "demo", Name: "web",
			UID: "uid-web", EventType: EventSnapshot},
	}
}

func TestObjectFilterMatchesRecord(t *testing.T) {
	records := filterFixture()

	cases := []struct {
		name   string
		filter ObjectFilter
		want   int
	}{{
		name:   "every record for one object identity",
		filter: ObjectFilter{Group: GroupApps, Kind: KindDeployment, Namespace: "demo", Name: "web"},
		// Three: two incarnations under one name, one of them twice. Name is not
		// identity — UID is.
		want: 3,
	}, {
		name: "one incarnation of that name",
		filter: ObjectFilter{Group: GroupApps, Kind: KindDeployment, Namespace: "demo",
			Name: "web", UID: "uid-web"},
		want: 2,
	}, {
		name: "one event type of one incarnation",
		filter: ObjectFilter{Group: GroupApps, Kind: KindDeployment, Namespace: "demo",
			Name: "web", UID: "uid-web", EventTypes: []string{EventSnapshot}},
		want: 1,
	}, {
		name: "a class of event types",
		filter: ObjectFilter{Group: GroupApps, Kind: KindDeployment, Namespace: "demo",
			Name: "web", UID: "uid-web", EventTypes: CreationEvents},
		want: 1,
	}, {
		name: "an event type nothing carries",
		filter: ObjectFilter{Group: GroupApps, Kind: KindDeployment, Namespace: "demo",
			Name: "web", EventTypes: []string{EventDeleted}},
		want: 0,
	}, {
		// The empty namespace is a cluster-scoped object, not a wildcard: this
		// filter must not collect the namespaced Deployments.
		name:   "a cluster-scoped object's empty namespace is a value",
		filter: ObjectFilter{Group: GroupCore, Kind: KindNode, Namespace: "", Name: "node-1"},
		want:   1,
	}, {
		// Likewise the empty group is the core group. Asking for it must not match
		// apps/v1.
		name:   "the empty group is the core group",
		filter: ObjectFilter{Group: GroupCore, Kind: KindDeployment, Namespace: "demo"},
		want:   0,
	}, {
		name:   "a namespace narrows",
		filter: ObjectFilter{Group: GroupApps, Kind: KindDeployment, Namespace: "other"},
		want:   1,
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got int
			for _, record := range records {
				if tc.filter.MatchesRecord("c1", record) {
					got++
				}
			}
			if got != tc.want {
				t.Errorf("filter matched %d records, want %d", got, tc.want)
			}
		})
	}
}

// TestMatchesRecordIsScopedToOneCluster guards the reason cluster_id lives in the
// addressing rather than in each filter: an archive shared by two clusters must
// never answer one cluster's question with the other's records.
func TestMatchesRecordIsScopedToOneCluster(t *testing.T) {
	filter := ObjectFilter{Group: GroupApps, Kind: KindDeployment, Namespace: "demo", Name: "web"}
	for _, record := range filterFixture() {
		if record.ClusterID != "c2" {
			continue
		}
		if filter.MatchesRecord("c1", record) {
			t.Errorf("a record from cluster %q matched a query about cluster %q", record.ClusterID, "c1")
		}
	}
}

// TestKeyClassification pins which objects in a bucket are the archive, which are
// the scope log, and which are neither.
//
// The three trees have different line shapes and different readers, so the
// classification is part of the published layout rather than a convenience: a
// records query globs cluster_id=*, a scope query globs scopes/, and nothing
// should ever glob the probe.
func TestKeyClassification(t *testing.T) {
	m := &MinIO{Bucket: "archive", Prefix: "audit", ClusterID: "kind"}

	cases := []struct {
		key    string
		record bool
		scope  bool
		probe  bool
	}{{
		key:    "audit/format=jsonl-v1/cluster_id=kind/date=2026-08-23/hour=09/" + hex64 + ".jsonl.zst",
		record: true,
	}, {
		key:   "audit/format=jsonl-v1/scopes/date=2026-08-23/" + hex64 + ".jsonl.zst",
		scope: true,
	}, {
		key:   "audit/.kuberecord-probe",
		probe: true,
	}, {
		// Another cluster's records under the same prefix: the layout allows it, and
		// this suite must not read them as its own.
		key: "audit/format=jsonl-v1/cluster_id=other/date=2026-08-23/hour=09/" + hex64 + ".jsonl.zst",
	}, {
		// A prefix that merely *starts* with this cluster's id is a different
		// cluster. Segment matching, not substring matching.
		key: "audit/format=jsonl-v1/cluster_id=kind-2/date=2026-08-23/hour=09/" + hex64 + ".jsonl.zst",
	}, {
		// A future format version is not this contract, and must not be decoded as
		// though it were.
		key: "audit/format=jsonl-v2/cluster_id=kind/date=2026-08-23/hour=09/" + hex64 + ".jsonl.zst",
	}, {
		// Another sink's archive, sharing the bucket under its own prefix. Everything
		// past the prefix is identical, which is the point: only the prefix separates
		// them, so it has to be part of the question.
		key: "other/format=jsonl-v1/cluster_id=kind/date=2026-08-23/hour=09/" + hex64 + ".jsonl.zst",
	}, {
		key: "other/format=jsonl-v1/scopes/date=2026-08-23/" + hex64 + ".jsonl.zst",
	}, {
		key: "other/.kuberecord-probe",
	}, {
		// A prefix that merely starts with this sink's is a different prefix.
		key: "audit-2/format=jsonl-v1/cluster_id=kind/date=2026-08-23/hour=09/" + hex64 + ".jsonl.zst",
	}}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			if got := m.IsRecordKey(tc.key); got != tc.record {
				t.Errorf("IsRecordKey = %t, want %t", got, tc.record)
			}
			if got := m.IsScopeKey(tc.key); got != tc.scope {
				t.Errorf("IsScopeKey = %t, want %t", got, tc.scope)
			}
			if got := m.IsProbeKey(tc.key); got != tc.probe {
				t.Errorf("IsProbeKey = %t, want %t", got, tc.probe)
			}
		})
	}
}

// TestKeyClassificationWithNoPrefix covers the other supported configuration: a
// bucket dedicated to one archive, where spec.prefix is empty and every key opens
// with the format segment. It is a real configuration rather than an edge case, and
// the classifier must not require a leading segment that a correctly configured
// sink never writes.
func TestKeyClassificationWithNoPrefix(t *testing.T) {
	m := &MinIO{Bucket: "archive", Prefix: "", ClusterID: "kind"}

	record := "format=jsonl-v1/cluster_id=kind/date=2026-08-23/hour=09/" + hex64 + ".jsonl.zst"
	if !m.IsRecordKey(record) {
		t.Errorf("IsRecordKey(%q) = false, want true", record)
	}
	scope := "format=jsonl-v1/scopes/date=2026-08-23/" + hex64 + ".jsonl.zst"
	if !m.IsScopeKey(scope) {
		t.Errorf("IsScopeKey(%q) = false, want true", scope)
	}
	if !m.IsProbeKey(".kuberecord-probe") {
		t.Error(`IsProbeKey(".kuberecord-probe") = false, want true`)
	}
	// An empty prefix admits every key, so a prefixed archive in the same bucket is
	// indistinguishable from this sink's own. That is a property of the
	// configuration rather than of this code — a bucket shared this way cannot be
	// told apart by key alone — and it is asserted so the behaviour is deliberate.
	if !m.IsRecordKey("audit/" + record) {
		t.Error("an empty prefix must admit keys under any prefix; it cannot distinguish them")
	}
}

// hex64 stands in for a content hash. Its value is irrelevant to these cases —
// only its position in the key is — so it is a constant rather than a real digest.
const hex64 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
