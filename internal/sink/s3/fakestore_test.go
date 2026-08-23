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

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/yelzhy/kuberecord/internal/sink"
	"github.com/yelzhy/kuberecord/internal/sink/conformance"
)

// fakeStore is the observable object store every test in this package drives. It
// is the one stand-in, shared by the conformance harness and the backend-specific
// tests, so both measure the same shipped writer against the same model of what
// an object store does.
//
// It models the three properties of a real bucket that the writer's correctness
// depends on:
//
//   - A PUT is atomic. An object is decoded and stored whole or not at all, which
//     is what makes "no partial-failure path" a property under test rather than an
//     assumption.
//   - A key holds one object. A second PUT of the same key *replaces* the object
//     already there, in place and without moving it, exactly as S3 does — so a
//     retried write leaves one object, and the store's contents (not a tally of
//     attempts) are what the conformance suite's durable set is derived from.
//   - Overwriting is only harmless if the bytes match. A repeat PUT carrying
//     different bytes under the same key would be an idempotency bug the object
//     key is supposed to make impossible, so this store treats it as a harness
//     failure rather than silently accepting the newer bytes (see checkOverwriteLocked).
type fakeStore struct {
	mu sync.Mutex

	// log is the ordered observation log. A durable object occupies one entry for
	// the life of the store, updated in place when its key is written again; a
	// failed attempt and the close each append an entry of their own.
	log []*storeEntry
	// index locates the entry holding a given key, so an overwrite updates that
	// entry rather than appending a second one.
	index map[string]*storeEntry

	// scopeWrites is every watch-scope transition the store has been handed, in
	// the order the objects carrying them arrived.
	scopeWrites []sink.ScopeEvent
	// scopeFault, when set, is the error every scope-object PUT meets, and
	// scopeAttempts counts those PUTs. It is a plain error rather than a
	// conformance.FaultFunc, and separate from fault, because the scope path has
	// nothing to settle and no lost-acknowledgement case to model: the only
	// question it raises is whether a transition survives the store being down,
	// which is what the retry queue answers.
	scopeFault    error
	scopeAttempts int

	// fault decides the outcome of each *record* object PUT; nil means success.
	fault conformance.FaultFunc

	// probeKey is the key the writer's health probe writes to, and probeOutcome is
	// what that write meets. The probe is routed away from the log and the fault
	// on purpose: it is not an audit write, and a suite that arranged a failing
	// probe would otherwise be arranging a failing archive too.
	probeKey     string
	probeOutcome conformance.ProbeOutcome
	probes       int
	// probeBlock, when non-nil, holds the probe open until it is closed or the
	// probe's own context expires. It is how a store that accepts the request and
	// then never answers is modelled — the failure mode that would pin the probe
	// goroutine, and the shutdown waiting behind it.
	probeBlock chan struct{}
	// probeRetention is the retention the most recent probe carried, so a test can
	// assert that the Object Lock configuration really is exercised once and then
	// left alone.
	probeRetention *Retention

	// harnessErr is the first modelling violation this store observed — an object
	// it could not decode, or a repeat PUT that changed the bytes under a key.
	// Remembered rather than returned, because a PUT runs on a worker goroutine
	// with no test to fail; the test asserts it in a t.Cleanup.
	harnessErr error

	closes int
}

// storeEntry is one line of the observation log: the event it contributes, and —
// for a durable object — the key and bytes it holds so an overwrite can be
// recognised.
type storeEntry struct {
	event conformance.Event
	key   string
	body  []byte
	// retention is what the PUT that filled this entry carried, kept so a test can
	// assert the Object Lock headers travelled with the object.
	retention *Retention
	// writes counts the PUTs that landed on this key. More than one is the normal,
	// expected shape of a retried write; it is what "one object, written twice,
	// byte-identical" is asserted on.
	writes int
}

func newFakeStore() *fakeStore {
	return &fakeStore{index: map[string]*storeEntry{}, probeOutcome: conformance.ProbeHealthy}
}

// PutObject implements ObjectStore. Which of the three kinds of object this is —
// a records object, a scope object, or the health probe's — is decided by its
// key, through the same helpers the writer builds those keys with, rather than by
// sniffing the body: guessing from the payload shape would work today and break
// the first time a line gains a field.
func (s *fakeStore) PutObject(ctx context.Context, in PutObjectInput) error {
	switch {
	case in.Key == s.probeKey:
		return s.putProbe(ctx, in)
	case isScopeObjectKey(in.Key):
		return s.putScopeObject(in)
	default:
		return s.putRecordObject(ctx, in)
	}
}

// putRecordObject is one durable-write attempt on the record path: the body is
// decoded back to the records it carries, the installed fault decides the
// outcome, and the attempt is logged with whatever it returned.
//
// The fault is called with the lock released. It is allowed — and used — to block
// until the suite lets it go, which is how a write is made to be genuinely in
// flight when shutdown begins; holding the store's lock across it would stall
// every other worker and the suite's own reads with it.
func (s *fakeStore) putRecordObject(ctx context.Context, in PutObjectInput) error {
	records, err := Decode(in.Body)
	if err != nil {
		s.noteHarnessErr(fmt.Errorf("decoding an object back to records failed (key %s): %w", in.Key, err))
	}

	s.mu.Lock()
	fault := s.fault
	s.mu.Unlock()

	var putErr error
	if fault != nil {
		putErr = fault(ctx, records)
	}

	s.record(conformance.Event{Kind: conformance.EventWrite, Records: records, Err: putErr}, in)
	return putErr
}

// putScopeObject stores a scope object and appends the transitions it carries.
//
// Scope objects are deliberately outside the fault's reach, for the same reason
// the ClickHouse harness keeps watch_scopes inserts outside it: the fault exists
// to break the *record* path, and a suite arranging a failing record write is not
// asking for the scope log to break too. They are also kept out of the event log,
// which is the record path's durable set — a scope object carries no sink.Records
// and would only appear there as an empty write.
func (s *fakeStore) putScopeObject(in PutObjectInput) error {
	events, err := decodeScopeObject(in.Body)
	if err != nil {
		s.noteHarnessErr(fmt.Errorf("decoding a scope object back to transitions failed (key %s): %w", in.Key, err))
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.scopeAttempts++
	if s.scopeFault != nil {
		return s.scopeFault
	}
	if prev, ok := s.index[in.Key]; ok {
		// The same transitions, re-written after a retry: one object, and it must
		// still be the same bytes.
		s.checkOverwriteLocked(prev, in)
		return nil
	}
	entry := &storeEntry{key: in.Key, body: bytes.Clone(in.Body), retention: in.Retention, writes: 1}
	s.index[in.Key] = entry
	s.scopeWrites = append(s.scopeWrites, events...)
	return nil
}

// putProbe answers the health probe with whatever state the suite (or a test)
// asked for.
func (s *fakeStore) putProbe(ctx context.Context, in PutObjectInput) error {
	s.mu.Lock()
	s.probes++
	s.probeRetention = in.Retention
	outcome, block := s.probeOutcome, s.probeBlock
	s.mu.Unlock()

	// Held open with the lock released, for the same reason the record path's
	// fault is: a store that stops answering must not stop the rest of the test.
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	switch outcome {
	case conformance.ProbeSchemaMismatch:
		// The shape of what this sink writes is not what the bucket accepts: on a
		// real store, retention headers against a bucket with no Object Lock
		// configuration. It must reach the prober as ErrBucketIncompatible, which
		// is the only thing that distinguishes it from an outage.
		return fmt.Errorf("%w: no ObjectLockConfiguration on this bucket", ErrBucketIncompatible)
	case conformance.ProbeUnreachable:
		return errors.New("dial tcp: connection refused")
	default:
		return nil
	}
}

// record appends an attempt to the log, or — when the attempt was durable and its
// key is already held — updates the entry that key already occupies. That in-place
// update is what models an overwrite: the store holds one object per key, so the
// durable set the conformance suite derives from this log must not grow when the
// same object is written again.
func (s *fakeStore) record(event conformance.Event, in PutObjectInput) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !event.Durable() {
		// Nothing reached storage: the attempt is visible in the log (the ordering
		// properties need it) but contributes no object.
		s.log = append(s.log, &storeEntry{event: event})
		return
	}
	if prev, ok := s.index[in.Key]; ok {
		s.checkOverwriteLocked(prev, in)
		prev.event = event
		prev.retention = in.Retention
		return
	}
	entry := &storeEntry{event: event, key: in.Key, body: bytes.Clone(in.Body), retention: in.Retention, writes: 1}
	s.log = append(s.log, entry)
	s.index[in.Key] = entry
}

// checkOverwriteLocked counts a repeat PUT on a key and fails the harness when it
// carried different bytes. Called with s.mu held.
func (s *fakeStore) checkOverwriteLocked(prev *storeEntry, in PutObjectInput) {
	prev.writes++
	if !bytes.Equal(prev.body, in.Body) && s.harnessErr == nil {
		s.harnessErr = fmt.Errorf("key %s was written twice with different bytes (%d then %d): "+
			"the content-addressed key promises that a repeat write of the same records is the same object",
			in.Key, len(prev.body), len(in.Body))
	}
}

// Close implements ObjectStore, landing in the same ordered log as the writes —
// which is what lets the drain-ordering property compare the two.
func (s *fakeStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes++
	s.log = append(s.log, &storeEntry{event: conformance.Event{Kind: conformance.EventClose}})
	return nil
}

// snapshot implements conformance.Harness.Events. The copy is mandatory: the
// suite reads the log while workers are still appending to it.
func (s *fakeStore) snapshot() []conformance.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := make([]conformance.Event, 0, len(s.log))
	for _, entry := range s.log {
		events = append(events, entry.event)
	}
	return events
}

// scopeSnapshot implements conformance.Harness.ScopeWrites.
func (s *fakeStore) scopeSnapshot() []sink.ScopeEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.scopeWrites)
}

// setFault implements conformance.Harness.SetFault.
func (s *fakeStore) setFault(f conformance.FaultFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault = f
}

// setScopeFault makes every subsequent scope-object PUT fail with err, or clears
// the failure when err is nil.
func (s *fakeStore) setScopeFault(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scopeFault = err
}

// scopeAttemptCount is how many scope-object PUTs the store has seen, failures
// included — the only way to tell "the flush has not happened yet" from "the
// flush happened and failed".
func (s *fakeStore) scopeAttemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scopeAttempts
}

// setProbeBlock makes every subsequent probe wait on block before answering.
func (s *fakeStore) setProbeBlock(block chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probeBlock = block
}

// setProbeOutcome implements conformance.Harness.SetProbeOutcome.
func (s *fakeStore) setProbeOutcome(outcome conformance.ProbeOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probeOutcome = outcome
}

func (s *fakeStore) noteHarnessErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.harnessErr == nil {
		s.harnessErr = err
	}
}

func (s *fakeStore) firstHarnessErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.harnessErr
}

// objects returns the store's record objects, in the order their keys were first
// written. It is what the backend-specific tests assert rotation on: how many
// objects a run produced, what each one holds, and how many times each was
// written.
func (s *fakeStore) objects() []storeEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]storeEntry, 0, len(s.log))
	for _, entry := range s.log {
		if entry.key == "" || isScopeObjectKey(entry.key) {
			continue
		}
		out = append(out, *entry)
	}
	return out
}

// scopeObjects returns the store's scope objects, in first-write order.
func (s *fakeStore) scopeObjects() []storeEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]storeEntry, 0, len(s.index))
	for _, entry := range s.index {
		if isScopeObjectKey(entry.key) {
			out = append(out, *entry)
		}
	}
	slices.SortFunc(out, func(a, b storeEntry) int { return bytes.Compare([]byte(a.key), []byte(b.key)) })
	return out
}

// closeCount is how many times the store was closed. Exactly one is the contract;
// a test asserts it directly where the conformance suite does not reach.
func (s *fakeStore) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

// probeCount and lastProbeRetention expose what the health probe did.
func (s *fakeStore) probeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probes
}

func (s *fakeStore) lastProbeRetention() *Retention {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probeRetention
}

// records returns every record in this entry's object, decoded.
func (e storeEntry) records() []sink.Record { return e.event.Records }

var _ ObjectStore = (*fakeStore)(nil)
