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
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// This file is the S3 backend's side of the multi-sink runtime: which halves of
// the sink contract this backend implements, which it deliberately does not, and
// the active health probe the SinkManager runs for it. The recycle discriminator
// (sink.InstanceConfig.Fingerprint) lives with the credential and endpoint settings
// it has to cover, in instanceconfig.go, and is not here.
//
// The assertions below are the whole capability declaration, and they are
// compile-time on purpose: SinkManager.newLiveSink discovers a backend's optional
// halves by type assertion, so a method that drifted out of one of these
// interfaces — a renamed method, a changed signature, a value receiver where a
// pointer is handed over — would silently produce a sink running with that half
// switched off and nothing in the logs to say so. Here, it is a build failure.
var (
	_ sink.Writer           = (*Writer)(nil)
	_ sink.ScopeEventWriter = (*Writer)(nil)
	_ sink.Prober           = (*Writer)(nil)
)

// ---------------------------------------------------------------------------
// sink.StateReader is deliberately NOT implemented, and that is the decision
// (D12) rather than an unfinished edge.
//
// An object store cannot answer the questions a StateReader answers. "What was
// the last recorded state of every object in this scope?" is a query over the
// whole history of a prefix, which for this backend means listing and
// decompressing every object ever written under it — unbounded work, growing
// forever, on the operator's boot path. The archive is built to be cheap to write
// and cheap to keep, and that is exactly what makes it expensive to interrogate.
// A StateReader that did it anyway would turn every restart into a full scan of
// the bucket, and a large enough bucket would mean an operator that never
// finishes starting.
//
// Three behaviours are therefore off for every sink of this kind, and they are
// off *visibly* (Invariant 5) — the SinkManager reports the missing half and the
// S3Sink CR carries HistoryUnavailable=True while Ready stays True:
//
//   - Cache warm-up. A restarting operator cannot learn what this archive already
//     holds, so it cannot tell a genuinely new object from one it has simply
//     forgotten. Every record it writes is therefore a permanent Snapshot: full
//     state, no diff, no Added/Modified distinction. That is not a downgrade of
//     the audit trail's *content* — every state is there — but a consumer must
//     read event_type accordingly.
//   - Zombie garbage collection. An object deleted while the operator was down is
//     never recorded as deleted here. The archive's last word on it is its last
//     observed state, and the absence of anything after that is the only evidence
//     it is gone.
//   - Boot reconciliation of scope epochs. The scope log this backend writes
//     (scopewriter.go) is recorded but never read back, so a scope left open by a
//     process that died stays open in the archive forever. A reader must treat an
//     unmatched Started as an epoch whose end is unknown.
//
// The supported way to have both a queryable timeline and a cheap immutable
// archive is the tee pattern (D14): two rules over the same resources, one naming
// a ClickHouseSink and one naming an S3Sink. The ClickHouse side keeps the
// history; this side keeps the bytes.
// ---------------------------------------------------------------------------

// probeObjectKey is the key the health probe writes to, relative to the sink's
// prefix.
//
// It is a fixed key so repeated probes leave one object instead of littering the
// bucket — one *current* object: on a versioned bucket each probe adds a version
// of this key, which is why retention is carried on the first probe only (see
// Probe) and why a bucket-wide default retention interacts badly with probing at
// all (docs/RETENTION.md). It deliberately sits *outside* the format=jsonl-v1
// partition so that no reader's glob over the archive ever meets it: it is operational
// exhaust, not audit data, and an object with a different line shape inside the
// records tree would break schema inference for every query engine pointed at it.
const probeObjectKey = ".kuberecord-probe"

// probeBody is what the probe writes. It is a fixed, self-describing line rather
// than an empty body so that whoever finds this object in a bucket can tell what
// wrote it and why, and so repeated probes write byte-identical objects.
var probeBody = []byte(`{"probe":"kuberecord","purpose":"verifies this sink can write to this bucket"}` + "\n")

// Probe implements sink.Prober: it reports whether this sink's bucket is
// reachable *and writable*, and whether the bucket will accept the shape of
// object this sink is configured to write.
//
// It writes. A HEAD or a GET would be cheaper and would be a lie: a read-only
// credential passes both, so the sink would report itself Ready and then fail
// every single write, with the CR saying nothing about why. The one thing an
// archive sink has to be able to do is put an object, so that is what is checked
// — the smallest possible one, at a fixed key outside the archive's own partition
// (see probeObjectKey).
//
// A refusal that is about the object's *shape* rather than about reachability is
// wrapped in sink.ErrSchemaInvalid, which is the same classification the
// ClickHouse backend gives a drifted table and means the same thing to whoever is
// on call: this will not clear on its own, a human has to change something. The
// case that exists is an S3Sink configured with spec.objectLock against a bucket
// that has no Object Lock configuration — which only a human on the account can
// give it, so no amount of waiting fixes it and every write this sink attempts
// will fail identically. Everything else — refused connections,
// DNS, expired credentials, a 5xx — is reachability, reported unwrapped, and the
// manager keeps retrying it.
//
// The retention headers are carried on the *first* successful probe only. They
// have to be carried at least once, since a bucket that will not accept them is
// precisely what the paragraph above is about; they must not be carried every
// time, because a bucket with Object Lock enabled is versioned, and a retained
// version per probe cycle is an ever-growing set of tiny objects that COMPLIANCE
// mode makes undeletable. Once is enough because Object Lock cannot be disabled
// on a bucket once enabled, so the answer cannot change under a running instance
// — and a change to spec.objectLock itself arrives as a new fingerprint and
// therefore as a new instance, which probes afresh.
//
// Like every other user of the shared client it registers in otherUsers under the
// closing check, so shutdown can never close the client while a probe is in
// flight (see Writer.otherUsers, step 5 of Start).
func (w *Writer) Probe(ctx context.Context) error {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return fmt.Errorf("s3writer: shutting down, refusing health probe")
	}
	w.otherUsers.Add(1)
	verifyLock := !w.lockVerified
	w.mu.Unlock()
	defer w.otherUsers.Done()

	in := PutObjectInput{Bucket: w.bucket, Key: w.probeKey(), Body: probeBody}
	if verifyLock {
		in.Retention = w.retention()
	}

	if err := w.store.PutObject(ctx, in); err != nil {
		if errors.Is(err, ErrBucketIncompatible) {
			return fmt.Errorf("%w: bucket %q will not accept the objects this sink writes: %w",
				sink.ErrSchemaInvalid, w.bucket, err)
		}
		return fmt.Errorf("write probe object to bucket %q: %w", w.bucket, err)
	}

	if verifyLock {
		w.mu.Lock()
		w.lockVerified = true
		w.mu.Unlock()
	}
	return nil
}

// probeKey is where this sink's probe object lives: under the sink's own prefix,
// so two sinks sharing a bucket do not probe through each other's key, and with
// no leading slash when the prefix is empty (the same treatment objectKey gives
// it).
func (w *Writer) probeKey() string {
	if w.prefix == "" {
		return probeObjectKey
	}
	return strings.Join([]string{w.prefix, probeObjectKey}, "/")
}
