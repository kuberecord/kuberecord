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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/yelzhy/kubestream/internal/sink"
)

// This file is the ClickHouse backend's side of the multi-sink runtime (Task
// 1.8): the two things the SinkManager needs from a backend that the write path
// itself has no use for — a recycle discriminator for a resolved configuration,
// and an active health probe. Both live here, in the backend, because
// internal/sink cannot import this package (the dependency runs the other way),
// so the manager can only reach them through the sink.InstanceConfig and
// sink.Prober contracts.
var (
	_ sink.InstanceConfig = Config{}
	_ sink.Prober         = (*CHWriter)(nil)
)

// Fingerprint implements sink.InstanceConfig: a digest of every setting a running
// CHWriter is built from, so the SinkManager can tell "the same sink,
// re-reconciled" from "the same sink, reconfigured".
//
// Every field participates, including the password — a rotated credential is
// precisely the change that must force a recycle, and it is the case the
// acceptance criteria single out. The result is a SHA-256 digest rather than the
// concatenated settings themselves because fingerprints are compared *and
// logged*: rendering the credential in clear here would leak it into the
// operator's log the first time a sink was recycled.
//
// Values are rendered with %q (and durations as strings), so no pair of
// neighbouring fields can be re-split to produce another configuration's digest —
// an addr of "a" with database "bc" must not fingerprint like "ab" with "c".
func (c Config) Fingerprint() string {
	h := sha256.New()
	// Errors from a hash writer are impossible by contract (hash.Hash never
	// returns one), and Fprintf's return is therefore not fallible in any way a
	// caller could act on — but it is still read and checked rather than
	// discarded, so this stays clean under Invariant 4's no-silent-errors rule.
	if _, err := fmt.Fprintf(h,
		"addr=%q db=%q user=%q pass=%q dial=%q read=%q autocreate=%t "+
			"batchrows=%d batchwait=%q queue=%d workers=%d enqueue=%q drain=%q",
		c.Addr, c.Database, c.Username, c.Password,
		c.DialTimeout.String(), c.ReadTimeout.String(), c.AutoCreateSchema,
		c.BatchMaxRows, c.BatchMaxWait.String(), c.WriteQueueSize, c.WriteWorkers,
		c.EnqueueTimeout.String(), c.ShutdownDrainTimeout.String(),
	); err != nil {
		// Unreachable; a digest of nothing would silently make every
		// configuration look identical, so it is reported rather than returned.
		return "unfingerprintable"
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Probe implements sink.Prober: it reports whether this sink's ClickHouse is
// reachable and carries the schema the operator writes against.
//
// It is the async health check the SinkManager runs on its own goroutine and
// reports over its result channel, which is what keeps Invariant 1 intact: the
// SinkReconciler learns that a sink is unreachable without ever dialling it
// itself. The two phases are ordered so the reported reason is the useful one — a
// Ping failure means "cannot reach it", and only a backend that answered is judged
// on its schema.
//
// A schema mismatch is wrapped in sink.ErrSchemaInvalid so the manager can label
// it SchemaInvalid without knowing anything about ClickHouse's system.columns: it
// will not resolve on its own and needs a migration, unlike an unreachable
// backend. A transient introspection failure is returned unwrapped and is
// therefore reported as unreachable, which is the truthful reading — the query
// itself did not complete.
//
// The probe never writes DDL, even when this writer was opened with
// AutoCreateSchema: a health check that mutated the backend's schema as a side
// effect of being asked "are you healthy?" would be a surprise no operator asked
// for. Auto-creation stays where it belongs, in the instance's own startup path
// (see CHWriter.Start).
//
// Like every other reader on this shared connection, the call registers in
// otherUsers under the closing check, so shutdown can never close conn while a
// probe is mid-flight (see CHWriter.otherUsers).
func (w *CHWriter) Probe(ctx context.Context) error {
	w.mu.Lock()
	if w.closing {
		w.mu.Unlock()
		return fmt.Errorf("chwriter: shutting down, refusing health probe")
	}
	w.otherUsers.Add(1)
	w.mu.Unlock()
	defer w.otherUsers.Done()

	if err := w.conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping clickhouse: %w", err)
	}

	err := validateSchema(ctx, w.conn, w.database)
	var mismatch *schemaMismatchError
	if errors.As(err, &mismatch) {
		return fmt.Errorf("%w: %w", sink.ErrSchemaInvalid, err)
	}
	return err
}
