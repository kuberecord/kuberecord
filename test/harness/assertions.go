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
