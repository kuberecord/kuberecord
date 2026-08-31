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

// The boundary, before anything below is trusted — and here it matters more than
// usual, because this file makes a claim it is only half entitled to.
//
// # This is NOT the authoritative agreement run
//
// The authoritative one is test/agreement, against a dockerized server and a real
// object store, under `make test-integration`. This one puts the same corpus
// through a stand-in connection on one side and a directory on the other, so what
// it proves is that the two engines' *Go logic* agrees: which incarnation each
// resolves, what order each imposes, which row each bases a replay on, how each
// composes a limit with a filter. All of that is the shipped code and all of it is
// worth catching on every commit rather than only when somebody starts containers.
//
// What it cannot prove is the half that has to be true for the claim to mean
// anything to a deployment. The ClickHouse side here never executes SQL, so nothing
// establishes that a DateTime64(9) bound really compares at nanosecond precision,
// that FINAL collapses an unmerged duplicate, or that hasAny over a LowCardinality
// array means what this package reads it as. A stand-in store is precisely the
// hand-built internal representation the agreement task forbids the real run from
// seeding through — this file is the deliberate exception beside it, labelled, and
// not a substitute for it.
//
// So: green here means the two implementations agree about a history. Green in
// test/agreement means a table and a bucket do.

package clickhouse

import (
	"testing"

	"github.com/kuberecord/kuberecord/internal/query/conformance"
	"github.com/kuberecord/kuberecord/internal/query/objectsource"
	"github.com/kuberecord/kuberecord/internal/query/objectsource/archivetest"
)

// localArchivePrefix is the prefix the fixture archive is written under.
//
// Non-empty deliberately, for the reason the objectsource suite's own is: an empty
// prefix is the simpler configuration and the one a bug in prefix handling would
// survive.
const localArchivePrefix = "audit"

// TestQueryBackendsAgreeLocally seeds one corpus into both backends and requires
// them to answer it identically.
//
// The pairing is the real one — an indexed store beside an archive tier that records
// no deletions and demands a time bound — because that is what makes the
// capability-derived half of the comparison run at all. Two backends with identical
// declarations would leave every clause about a declared difference unexecuted.
func TestQueryBackendsAgreeLocally(t *testing.T) {
	conformance.RunAgreementSuite(t, newFakeAgreementHarness(t), newLocalArchiveHarness(t))
}

// newFakeAgreementHarness is this backend over a stand-in connection.
//
// It supplies no Seed and no stream fault: the agreement suite plants the corpus and
// asks questions, and a harness carrying levers it never uses would be a harness
// whose unused levers nobody notices going wrong.
func newFakeAgreementHarness(t *testing.T) conformance.Harness {
	t.Helper()

	store := newFakeStore()
	engine, err := New(&fakeConn{store: store})
	if err != nil {
		t.Fatalf("building an engine over the stand-in connection: %v", err)
	}
	return conformance.Harness{
		Engine:       engine,
		SeedCorpus:   store.seedCorpus,
		Capabilities: fakeCapabilities(),
	}
}

// newLocalArchiveHarness is the other backend over a directory.
//
// Its capability declaration is written out here rather than borrowed from the
// objectsource suite's own, which is the same discipline every harness in this
// repository follows: two declarations agreeing because they read one literal would
// not notice the day that engine's report changed. TimeBoundRequired is the only
// one, and the three absences are each a statement — this archive never receives a
// deletion (D12), there is nothing behind the seam to push a predicate into, and
// there is no index to seek with.
func newLocalArchiveHarness(t *testing.T) conformance.Harness {
	t.Helper()

	dir := t.TempDir()
	source, err := objectsource.NewLocal(dir)
	if err != nil {
		t.Fatalf("opening the fixture archive at %q: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("closing the fixture source: %v", err)
		}
	})

	engine, err := objectsource.NewEngine(source, objectsource.Options{Prefix: localArchivePrefix})
	if err != nil {
		t.Fatalf("building an engine over the fixture archive: %v", err)
	}
	return conformance.Harness{
		Engine: engine,
		SeedCorpus: func(c conformance.Corpus) error {
			_, writeErr := archivetest.WriteCorpusDir(dir, localArchivePrefix, c)
			return writeErr
		},
		Capabilities: conformance.DeclareCapabilities(conformance.CapTimeBoundRequired),
	}
}
