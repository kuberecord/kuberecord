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

// The boundary, before anything below is trusted.
//
// This backend has two halves and they are proven in two places, because no
// single run can prove both.
//
//   - **The fake proves the Go logic.** Everything above the SQL — which
//     predicate is pushed down and which is applied to rows already read, which
//     incarnation a default query resolves to and in what order, how a change is
//     decoded, how a limit is composed with a filter, how a mid-stream failure
//     reaches Err rather than ending a loop quietly, whether an abandoned
//     iterator releases what it holds. All of it runs against a stand-in
//     connection, so all of it is the shipped code.
//
//   - **The integration run proves the SQL semantics.** That FINAL really does
//     collapse an unmerged duplicate, that a DateTime64(9) bound bound as a
//     string really does compare at nanosecond precision, that hasAny means over
//     a LowCardinality array what this package assumes it means, that
//     JSONExtractString reaches into the two Event spellings. None of that is
//     testable without ClickHouse, and none of it is left untested: the same
//     suite runs against the dockerized server under `make test-integration`
//     (see integration_test.go).
//
// Neither layer is sufficient alone, and "this backend passes conformance" must
// never be read as "verified end-to-end" — a distinction that only gets easier to
// lose once a second backend carries the same badge.

package clickhouse

import (
	"testing"

	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// fakeCapabilities is what this backend declares, named by hand rather than
// copied off Capabilities().
//
// The suite checks the declaration against the engine in both directions, and
// that check is only worth anything if the two were written independently. A
// harness that handed over the engine's own report would be comparing a value
// with itself.
//
// TimeBoundRequired is absent, and its absence is the declaration: this backend
// answers an unbounded query, because one object's history is a range read
// against the sort key rather than a scan of the table.
func fakeCapabilities() conformance.CapabilitySet {
	return conformance.DeclareCapabilities(
		conformance.CapDeletions,
		conformance.CapServerSideFilter,
		conformance.CapPointQuery,
	)
}

// newFakeHarness builds one engine over one stand-in connection, per property.
func newFakeHarness(t *testing.T) conformance.Harness {
	t.Helper()
	store := newFakeStore()
	engine, err := New(&fakeConn{store: store})
	if err != nil {
		t.Fatalf("building an engine over the stand-in connection: %v", err)
	}
	return conformance.Harness{
		Engine:         engine,
		Seed:           store.seed,
		SeedCorpus:     store.seedCorpus,
		SetStreamFault: store.setFault,
		Capabilities:   fakeCapabilities(),
	}
}

// TestQueryConformance runs the read-plane contract against this backend.
func TestQueryConformance(t *testing.T) {
	conformance.RunQuerySuite(t, newFakeHarness)
}
