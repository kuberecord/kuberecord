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

// This file is the line half of the object contract: what one JSON object per
// line holds, and how a frame of them is decoded without holding the frame.
//
// The two line shapes are declared here for the same reason the key layout is
// (see layout.go): the format is the contract between the write plane and this
// one, and an independent reader is what makes a format change detectable rather
// than invisible. docs/SCHEMA.md is the shared account both sides are written
// against; the field names below are its table, not this package's invention —
// note in particular `group` and `version`, which are the logical contract's
// names and deliberately not the column vocabulary another backend uses.

package objectsource

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/kuberecord/kuberecord/internal/query"
)

// eventKind is the kind a Kubernetes Event is recorded under, and eventGroups are
// the two API groups it may arrive in.
//
// v1/Event and events.k8s.io/v1/Event are one storage behind two APIs, and a
// cluster's rules may name either. Correlating only one of them would silently
// drop whichever half of the cluster's events happens to be captured the other
// way — a hole rather than a gap (Invariant 4).
const eventKind = "Event"

var eventGroups = [2]string{"", "events.k8s.io"}

// zstdMagic is the four bytes that open every zstd frame (RFC 8878).
//
// It is checked before the decoder is handed the stream so that a truncated,
// plaintext or mislabelled object fails with a message naming the actual problem
// instead of whatever the decompressor makes of the bytes.
var zstdMagic = [4]byte{0x28, 0xB5, 0x2F, 0xFD}

// recordLine is the physical form of one recorded change.
//
// Every field is always present in an object this format writes, so a reader need
// not distinguish absent from empty. Unknown fields are ignored: the format's
// change policy is additive, and an object written by a newer operator must still
// be readable here, minus the fields that did not exist when this code was built.
type recordLine struct {
	Timestamp       time.Time         `json:"timestamp"`
	ClusterID       string            `json:"cluster_id"`
	EventType       string            `json:"event_type"`
	APIGroup        string            `json:"group"`
	APIVersion      string            `json:"version"`
	Kind            string            `json:"kind"`
	Namespace       string            `json:"namespace"`
	Name            string            `json:"name"`
	UID             string            `json:"uid"`
	ResourceVersion string            `json:"resource_version"`
	Labels          map[string]string `json:"labels"`
	Actors          []string          `json:"actors"`
	Data            string            `json:"data"`
	Diff            string            `json:"diff"`
	SHA256          string            `json:"sha256"`
}

// change renders a line as the contract's change.
//
// The maps and slices are handed over rather than copied, which is safe precisely
// because decodeFrame zeroes its line between records: what is handed out belongs
// to one line and is never written to again. That is the ChangeIterator's own rule
// — an implementation must not hand out a buffer it intends to overwrite — and it
// is met here rather than by a defensive clone per retained row.
func (l *recordLine) change() query.Change {
	return query.Change{
		TS:              l.Timestamp,
		EventType:       l.EventType,
		Actors:          l.Actors,
		UID:             l.UID,
		ResourceVersion: l.ResourceVersion,
		APIVersion:      l.APIVersion,
		Data:            l.Data,
		Diff:            l.Diff,
		SHA256:          l.SHA256,
		Labels:          l.Labels,
	}
}

// namesObject reports whether a line records a change of ref.
//
// The identity is the canonical one and it is version-agnostic (Invariant 7): the
// API version a given observation was made at is provenance and takes no part.
// The cluster is compared even though the key partition already restricts the
// scan to it, because a listing is a prefix match and an archive whose partitions
// somebody rearranged by hand must not be read as if the path were authoritative.
func (l *recordLine) namesObject(ref query.ObjectRef) bool {
	return l.ClusterID == ref.ClusterID &&
		l.APIGroup == ref.APIGroup &&
		l.Kind == ref.Kind &&
		l.Namespace == ref.Namespace &&
		l.Name == ref.Name
}

// isEvent reports whether a line records a Kubernetes Event, in either of the two
// API groups it may have been captured through.
func (l *recordLine) isEvent() bool {
	if l.Kind != eventKind {
		return false
	}
	return l.APIGroup == eventGroups[0] || l.APIGroup == eventGroups[1]
}

// eventSubject is the object a Kubernetes Event is about.
type eventSubject struct {
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}

// eventEnvelope is the part of an Event's recorded data that names its subject.
//
// Both spellings are decoded because both occur: the core group names the subject
// in involvedObject, events.k8s.io names it in regarding.
type eventEnvelope struct {
	InvolvedObject eventSubject `json:"involvedObject"`
	Regarding      eventSubject `json:"regarding"`
}

// subject reads the subject out of an Event line's data.
//
// The two spellings are coalesced *per field*, not per object, which is the same
// reading the published recipes use: an Event whose payload carries one key with
// some fields empty is still matched on the fields it does fill. An undecodable
// payload yields the zero subject, which matches nothing — an Event nobody can
// attribute is not correlated, and it is not a reason to fail the timeline it was
// going to be commentary on.
func (l *recordLine) subject() eventSubject {
	var env eventEnvelope
	if err := json.Unmarshal([]byte(l.Data), &env); err != nil {
		return eventSubject{}
	}
	return eventSubject{
		Kind:      coalesce(env.InvolvedObject.Kind, env.Regarding.Kind),
		Namespace: coalesce(env.InvolvedObject.Namespace, env.Regarding.Namespace),
		Name:      coalesce(env.InvolvedObject.Name, env.Regarding.Name),
		UID:       coalesce(env.InvolvedObject.UID, env.Regarding.UID),
	}
}

// coalesce returns the first non-empty value.
func coalesce(first, second string) string {
	if first != "" {
		return first
	}
	return second
}

// namesTarget reports whether a subject is the object a timeline is about.
//
// The subject's *own* namespace is read out of the payload rather than taken from
// the Event object's namespace column, and the Event's namespace is deliberately
// not constrained anywhere: an Event lives in a namespace of its own choosing, and
// for a cluster-scoped object it is not the object's — which has none. Pinning it
// would correlate nothing for exactly the objects whose events are hardest to find
// another way.
func (s eventSubject) namesTarget(ref query.ObjectRef) bool {
	return s.Kind == ref.Kind && s.Namespace == ref.Namespace && s.Name == ref.Name
}

// scopeLine is the physical form of one watch-scope transition.
//
// Note `ts` rather than `timestamp`: this is a different line, not a variant of
// the record line, and reading it with the record line's field names would produce
// a log of transitions that all happened at the zero instant.
type scopeLine struct {
	TS         time.Time `json:"ts"`
	ClusterID  string    `json:"cluster_id"`
	APIGroup   string    `json:"group"`
	APIVersion string    `json:"version"`
	Kind       string    `json:"kind"`
	Namespace  string    `json:"namespace"`
	Action     string    `json:"action"`
	RuleRef    string    `json:"rule_ref"`
}

// scopeChange renders a transition as the contract's own.
func (l *scopeLine) scopeChange() query.ScopeChange {
	return query.ScopeChange{
		APIGroup:  l.APIGroup,
		Kind:      l.Kind,
		Namespace: l.Namespace,
		Action:    l.Action,
		RuleRef:   l.RuleRef,
		TS:        l.TS,
	}
}

// decodeFrame streams one object: zstd frame, line scanner, JSON decode, and one
// call to visit per line.
//
// # Why it is a stream and not a payload
//
// Objects rotate at up to 64Mi *compressed*, which is a multiple of that decoded.
// Reading one into memory to decode it would make peak memory a function of the
// object size times the fetch concurrency — the exact cost the bounded concurrency
// was chosen to control. So the frame is decoded incrementally and the line is
// reused, and what survives a line is only what visit chose to keep. A line that
// matches nothing is overwritten by the next one and never retained.
//
// The line is zeroed before every decode, and that is load bearing rather than
// tidy: decoding into a dirty struct leaves the previous line's value in any field
// the current line omits, so an object written by an older operator would inherit
// its neighbour's actors. Zeroing also releases the previous line's maps and
// slices from the reusable value, which is what makes handing them to a retained
// change safe.
//
// A visit that fails stops the frame and is returned unchanged. A malformed line
// is a failure and not a skip: a partially decoded object is indistinguishable
// from a short one, and reporting it as success would turn corruption into silent
// loss.
func decodeFrame(body io.Reader, visit func(*recordLine) error) error {
	return streamFrame(body, func(dec *json.Decoder) error {
		var line recordLine
		for {
			line = recordLine{}
			if err := dec.Decode(&line); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return fmt.Errorf("decoding a record line: %w", err)
			}
			if err := visit(&line); err != nil {
				return err
			}
		}
	})
}

// decodeScopeFrame is decodeFrame for the scope log's own line shape.
//
// It is a second function rather than one generic over the line type because the
// two shapes are read by different callers for different reasons, and because the
// framing they share is already shared — see streamFrame.
func decodeScopeFrame(body io.Reader, visit func(*scopeLine) error) error {
	return streamFrame(body, func(dec *json.Decoder) error {
		var line scopeLine
		for {
			line = scopeLine{}
			if err := dec.Decode(&line); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return fmt.Errorf("decoding a scope line: %w", err)
			}
			if err := visit(&line); err != nil {
				return err
			}
		}
	})
}

// streamFrame unwraps an object's zstd frame and hands the JSONL behind it to
// decode as a streaming decoder.
//
// The decoder is built per object, with concurrency 1 and the low-memory window,
// because the caller opens several objects at once under a cap: a decoder tuned
// for throughput on one large stream would multiply its window by that cap, and
// the point of the cap is that peak memory is a constant a person can reason
// about rather than a function of the archive's size.
//
// # Why json.Decoder and not bufio.Scanner
//
// This is JSON Lines, so a line scanner reads like the obvious tool and it is the
// wrong one. bufio.Scanner refuses any token longer than bufio.MaxScanTokenSize —
// 64 KiB by default — and a record here is routinely larger than that. A ConfigMap
// with a real data map, or a custom resource carrying
// x-kubernetes-preserve-unknown-fields, is past that limit well before it approaches
// the megabyte an etcd value may hold — and the state is then escaped into the line's
// data field, which only makes it longer.
//
// The two failure modes are unequal and both are bad. Scan returns false and Err
// reports bufio.ErrTooLong, which at least fails loudly — or, where the error is not
// checked, the line is truncated at the buffer boundary and what comes back is a
// partial object that JSON may well accept, decoding into a change nothing in the
// output admits is incomplete. That is the shape Invariant 4 exists to forbid, and
// no fixture would catch it unless one is written for it (see
// TestAnOversizedRecordSurvivesTheDecoder).
//
// json.Decoder has no per-value ceiling: it grows its buffer to whatever the value
// needs and reads values back to back out of one stream, which is exactly the JSONL
// shape. The bufio.NewReader below is a read-ahead buffer for the magic-byte peek and
// for the decompressor beneath it — it is not a tokenizer, and raising a limit on it
// is not the fix for anything.
func streamFrame(body io.Reader, decode func(*json.Decoder) error) error {
	buffered := bufio.NewReader(body)
	head, err := buffered.Peek(len(zstdMagic))
	switch {
	case errors.Is(err, io.EOF):
		return fmt.Errorf("the object holds %d bytes, which is not a zstd frame", len(head))
	case err != nil:
		return fmt.Errorf("reading the head of the object: %w", err)
	case !bytes.Equal(head, zstdMagic[:]):
		return fmt.Errorf("the object does not open with the zstd magic bytes (%#x), so it is not a "+
			"frame of this format: truncated, plaintext, or written by something else", head)
	}

	reader, err := zstd.NewReader(buffered,
		zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true))
	if err != nil {
		return fmt.Errorf("opening the object's zstd frame: %w", err)
	}
	// Close rather than a deferred error: the decoder releases buffers and,
	// depending on its options, a goroutine, and it returns nothing to report.
	defer reader.Close()

	return decode(json.NewDecoder(reader))
}
