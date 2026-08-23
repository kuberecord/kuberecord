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
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/yelzhy/kuberecord/internal/sink"
)

// The row-shaped claims both suites make. They carry no timeouts of their own:
// each suite sets its own Gomega defaults (SetDefaultEventuallyTimeout and
// friends), which is exactly the thing the two legitimately disagree about — a
// chaos scenario waits out a multi-minute outage where an e2e scenario would call
// the same wait a hang — while the claim itself stays one definition.

// EventuallyExactlyOneRow waits until the filter matches exactly one row and
// returns it.
//
// It settles on an exact count rather than a lower bound because that is the
// claim these suites actually make — "exactly one Deleted row" is the difference
// between a truthful audit trail and a duplicated one, and an assertion that
// stopped at "at least one" could not tell them apart. The optional timeout
// overrides the suite default for the waits that follow an operator restart or an
// outage.
func (ch *ClickHouse) EventuallyExactlyOneRow(filter ObjectFilter, timeout ...time.Duration) ResourceRow {
	GinkgoHelper()
	var rows []ResourceRow
	assertion := Eventually(func(g Gomega) {
		var err error
		rows, err = ch.ResourceRows(filter)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rows).To(HaveLen(1))
	})
	if len(timeout) > 0 {
		assertion = assertion.WithTimeout(timeout[0])
	}
	assertion.Should(Succeed())
	return rows[0]
}

// EventuallyAnyRows waits for at least one matching row, without committing to
// how many or of which event type.
func (ch *ClickHouse) EventuallyAnyRows(filter ObjectFilter, timeout ...time.Duration) []ResourceRow {
	GinkgoHelper()
	var rows []ResourceRow
	assertion := Eventually(func(g Gomega) {
		var err error
		rows, err = ch.ResourceRows(filter)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rows).NotTo(BeEmpty())
	})
	if len(timeout) > 0 {
		assertion = assertion.WithTimeout(timeout[0])
	}
	assertion.Should(Succeed())
	return rows
}

// ConsistentlyRowCount asserts the match count stays at want for the quiet
// window. It is how every "and no further rows appear" claim is made: an
// Eventually that passed the moment the right row landed cannot tell whether a
// wrong one arrives a second later.
func (ch *ClickHouse) ConsistentlyRowCount(filter ObjectFilter, want int) {
	GinkgoHelper()
	Consistently(func(g Gomega) {
		rows, err := ch.ResourceRows(filter)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rows).To(HaveLen(want))
	}).Should(Succeed())
}

// EventuallyScopeRows waits for at least one matching watch_scopes row and
// returns every match, oldest first.
func (ch *ClickHouse) EventuallyScopeRows(query ScopeQuery, timeout ...time.Duration) []ScopeRow {
	GinkgoHelper()
	var rows []ScopeRow
	assertion := Eventually(func(g Gomega) {
		var err error
		rows, err = ch.ScopeRows(query)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rows).NotTo(BeEmpty())
	})
	if len(timeout) > 0 {
		assertion = assertion.WithTimeout(timeout[0])
	}
	assertion.Should(Succeed())
	return rows
}

// ExpectNoDuplicateDeletes asserts the audit trail records every disappearance
// exactly once.
//
// Task 2.1 makes this a standing invariant rather than one scenario's assertion,
// and the reason is that every failure mode in that task has its own way of
// producing a second Deleted row: a reverted claim re-claimed, a GC pass retried
// against a stale snapshot, a close-out recovered twice from history, a restart
// that re-derives a deletion the previous process already wrote. Any of them
// would pass its own scenario's specific assertions while corrupting the trail,
// so the claim is made after every scenario, over the whole cluster's rows.
func (ch *ClickHouse) ExpectNoDuplicateDeletes() {
	GinkgoHelper()
	duplicates, err := ch.DuplicateDeletes()
	Expect(err).NotTo(HaveOccurred(), "failed to check for duplicate Deleted rows")
	Expect(duplicates).To(BeEmpty(), "an object's deletion was recorded more than once: %v", duplicates)
}

// The record-shaped claims, for a suite reading an S3 archive (see minio.go).
//
// They are the same claims as the row-shaped ones above, said of the same
// ObjectFilter, and they are deliberately separate functions rather than one
// generic pair: what a scenario reads is a property of the sink it is asserting
// against, and a helper that took "either backend" would have to be told which,
// at which point naming it is clearer.
//
// Two differences from the ClickHouse assertions are worth stating, because both
// are the backend's rather than the harness's:
//
//   - There is no FINAL and no merge to wait out. An object store's answer is
//     exact the moment the object is visible, because a retried PUT overwrites its
//     own key (D15) instead of leaving a duplicate to collapse later.
//   - A record only becomes visible when its object is *closed*, which rotation
//     decides. So every "and no further records appear" claim has to outlast one
//     rotation period, or it is asserting latency rather than absence — hence the
//     optional window on ConsistentlyRecordCount, which the ClickHouse twin does
//     not need.

// EventuallyRecordCount waits until the filter matches exactly want records and
// returns them.
func (m *MinIO) EventuallyRecordCount(filter ObjectFilter, want int, timeout ...time.Duration) []sink.Record {
	GinkgoHelper()
	var records []sink.Record
	assertion := Eventually(func(g Gomega) {
		var err error
		records, err = m.Records(filter)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(records).To(HaveLen(want))
	})
	if len(timeout) > 0 {
		assertion = assertion.WithTimeout(timeout[0])
	}
	assertion.Should(Succeed())
	return records
}

// EventuallyExactlyOneRecord waits until the filter matches exactly one record
// and returns it. As with the row assertion, the count is exact rather than a
// lower bound: "exactly one Snapshot" is the difference between an archive that
// re-snapshotted an object once on restart and one that is re-snapshotting it in
// a loop.
func (m *MinIO) EventuallyExactlyOneRecord(filter ObjectFilter, timeout ...time.Duration) sink.Record {
	GinkgoHelper()
	return m.EventuallyRecordCount(filter, 1, timeout...)[0]
}

// ConsistentlyRecordCount asserts the match count stays at want for the quiet
// window.
//
// The window is overridable, and for an absence claim it must be: a record the
// operator wrote a moment ago is not visible until rotation closes the object
// holding it, so a window shorter than the sink's maxObjectAge would report "no
// such record" about a record already on its way.
func (m *MinIO) ConsistentlyRecordCount(filter ObjectFilter, want int, window ...time.Duration) {
	GinkgoHelper()
	assertion := Consistently(func(g Gomega) {
		records, err := m.Records(filter)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(records).To(HaveLen(want))
	})
	if len(window) > 0 {
		assertion = assertion.WithTimeout(window[0])
	}
	assertion.Should(Succeed())
}

// EventuallyUID waits for an object to exist and returns its UID — the identity
// every row assertion is keyed by, and the only thing that distinguishes an
// object from its replacement under the same name.
func EventuallyUID(kind, name, namespace string, timeout ...time.Duration) string {
	GinkgoHelper()
	var uid string
	assertion := Eventually(func(g Gomega) {
		var err error
		uid, err = ObjectUID(kind, name, namespace)
		g.Expect(err).NotTo(HaveOccurred())
	})
	if len(timeout) > 0 {
		assertion = assertion.WithTimeout(timeout[0])
	}
	assertion.Should(Succeed())
	return uid
}
