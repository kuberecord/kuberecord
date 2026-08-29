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

package clickhouse

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// storeWith builds an engine over a stand-in seeded with the rows given, rather
// than with a conformance History.
//
// The suite's fixtures record one cluster by construction — that is the point of
// them — and every question here is about several. Seeding the store directly is
// what lets a test say "two clusters in the scope log, a third only in the records"
// without inventing a fixture the contract has no use for.
func storeWith(t *testing.T, records, scopes []string) (*Engine, *fakeConn) {
	t.Helper()

	store := newFakeStore()
	for i, id := range records {
		row := stateRow{
			ts:        time.Date(2026, 3, 1, 12, 0, i, 0, time.UTC),
			clusterID: id,
			eventType: "Added",
			kind:      "Deployment",
			namespace: "payments",
			name:      "checkout",
		}
		// Twice, as the stand-in's own seeding does: an at-least-once write path
		// leaves an unmerged duplicate, and a probe that lost its dedup form would
		// otherwise still look right here.
		store.states = append(store.states, row, row)
	}
	for i, id := range scopes {
		store.scopes = append(store.scopes, scopeRow{
			ts:        time.Date(2026, 3, 1, 11, 0, i, 0, time.UTC),
			clusterID: id,
			kind:      "Deployment",
			action:    "Started",
		})
	}

	conn := &fakeConn{store: store}
	engine, err := New(conn)
	if err != nil {
		t.Fatalf("building an engine over the stand-in connection: %v", err)
	}
	t.Cleanup(func() {
		if err := engine.Close(); err != nil {
			t.Errorf("closing the engine: %v", err)
		}
	})
	return engine, conn
}

// TestClusterIDsAreAnsweredFromTheScopeLog: the cheap probe answers, and the
// expensive one is not run.
//
// Both halves matter. The list is what a caller renders into "pass --cluster-id:
// this sink holds …", so a missing or duplicated value is a wrong instruction to a
// user. And the statement count is the cost property: the fallback is FINAL over
// the whole record table, and running it when the scope log already answered would
// make the ordinary case pay the exceptional case's price.
func TestClusterIDsAreAnsweredFromTheScopeLog(t *testing.T) {
	engine, conn := storeWith(t,
		[]string{"prod-eu-1", "only-in-the-records"},
		[]string{"prod-us-1", "prod-eu-1", "prod-eu-1"})

	ids, err := engine.ClusterIDs(t.Context())
	if err != nil {
		t.Fatalf("ClusterIDs: %v", err)
	}
	if want := []string{"prod-eu-1", "prod-us-1"}; !slices.Equal(ids, want) {
		t.Errorf("ClusterIDs = %v, want %v (sorted and distinct)", ids, want)
	}

	statements := conn.statements()
	if len(statements) != 1 {
		t.Fatalf("the scope log answered, so exactly one probe should have run; got %d:\n%s",
			len(statements), strings.Join(statements, "\n---\n"))
	}
	if strings.Contains(statements[0], tableResourceStates) {
		t.Errorf("the probe that ran reads %s, which is FINAL over the whole of history and is meant "+
			"to run only when the scope log is empty:\n%s", tableResourceStates, statements[0])
	}
}

// TestClusterIDsFallBackToTheRecordTable: an archive whose scope log has been
// trimmed out from under its records still names its clusters.
//
// This is the case the fallback exists for. Retention on watch_scopes can outlive
// nothing — it is a separate table with its own TTL — and history with no scope
// rows left is history no cheap probe can attribute. Answering it expensively is
// better than answering "this sink holds no clusters" about a sink full of them.
func TestClusterIDsFallBackToTheRecordTable(t *testing.T) {
	engine, conn := storeWith(t, []string{"prod-us-1", "prod-eu-1", "prod-eu-1"}, nil)

	ids, err := engine.ClusterIDs(t.Context())
	if err != nil {
		t.Fatalf("ClusterIDs: %v", err)
	}
	if want := []string{"prod-eu-1", "prod-us-1"}; !slices.Equal(ids, want) {
		t.Errorf("ClusterIDs = %v, want %v (sorted and distinct across duplicated rows)", ids, want)
	}

	statements := conn.statements()
	if len(statements) != 2 {
		t.Fatalf("an empty scope log should have been followed by the record probe; got %d statements:\n%s",
			len(statements), strings.Join(statements, "\n---\n"))
	}
	if !strings.Contains(statements[1], tableResourceStates) {
		t.Errorf("the second probe should read %s:\n%s", tableResourceStates, statements[1])
	}
}

// TestClusterIDsOfAnEmptySinkIsNotAFailure.
//
// A sink with nothing in it holds no clusters, and that is a result. Reporting it
// as an error would put the caller in the position of explaining a failure when
// what it has is an answer — and the answer, "this sink is empty", is the one
// piece of information that tells a user their operator never wrote here.
func TestClusterIDsOfAnEmptySinkIsNotAFailure(t *testing.T) {
	engine, conn := storeWith(t, nil, nil)

	ids, err := engine.ClusterIDs(t.Context())
	if err != nil {
		t.Fatalf("ClusterIDs over an empty sink: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("ClusterIDs = %v, want none", ids)
	}
	if got := len(conn.statements()); got != 2 {
		t.Errorf("both probes should have been tried before concluding the sink is empty; got %d", got)
	}
}

// TestClusterIDsRefusesAfterClose: a use-after-close is a bug in the caller and
// says so, rather than reaching the driver and coming back as whatever that
// connection's state happens to produce.
func TestClusterIDsRefusesAfterClose(t *testing.T) {
	engine, _ := storeWith(t, []string{"prod"}, nil)
	if err := engine.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := engine.ClusterIDs(t.Context()); err == nil {
		t.Error("ClusterIDs after Close returned no error")
	}
}

// closeRecorder is a connection that counts the Closes it receives.
type closeRecorder struct {
	driver.Conn
	closes int
}

func (c *closeRecorder) Close() error {
	c.closes++
	return nil
}

// TestADialledEngineOwnsItsConnection: the two constructors differ in exactly one
// promise, and this is it.
//
// An engine built by New closes nothing, because closing a connection it was lent
// would break whatever else holds it, at a distance, for a reason nothing in the
// call names. An engine built by Dial closes the connection it opened, because
// nobody else has a reference with which to close it — a CLI that leaked one per
// invocation would be a socket leak nothing in the process would ever report.
// Idempotence holds either way: the ordinary shape is a caller that both defers a
// Close and calls one explicitly.
func TestADialledEngineOwnsItsConnection(t *testing.T) {
	t.Run("a lent connection is left alone", func(t *testing.T) {
		conn := &closeRecorder{}
		engine, err := New(conn)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := engine.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if conn.closes != 0 {
			t.Errorf("the engine closed a connection it was lent %d times", conn.closes)
		}
	})

	t.Run("a dialled connection is closed exactly once", func(t *testing.T) {
		conn := &closeRecorder{}
		engine := &Engine{conn: conn, ownsConn: true}
		if err := engine.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := engine.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if conn.closes != 1 {
			t.Errorf("the connection was closed %d times, want exactly 1", conn.closes)
		}
	})
}

// TestDialNeedsAnAddressAndDoesNotConnect.
//
// The second half is the load-bearing one. The driver's Open assembles a pool and
// makes no connection, and this backend relies on that: a CLI that dialled eagerly
// would pay a round trip before it had printed which sink it chose and why, and
// would report an unreachable host as though the choice itself had failed.
func TestDialNeedsAnAddressAndDoesNotConnect(t *testing.T) {
	if _, err := Dial(DialConfig{Database: "kuberecord"}); err == nil {
		t.Error("Dial with no address returned no error")
	}

	// A port nothing is listening on: if this ever starts connecting, it fails
	// here rather than in a user's terminal.
	engine, err := Dial(DialConfig{
		Addr:     "127.0.0.1:1",
		Database: "kuberecord",
		Username: "kuberecord_ro",
		Password: "unused",
		TLS:      true,
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Errorf("closing a dialled engine that never connected: %v", err)
	}
}
