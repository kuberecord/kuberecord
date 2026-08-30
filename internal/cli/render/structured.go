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

package render

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/kuberecord/kuberecord/internal/query"
)

// The structured output contract: a versioned envelope people script against.
//
// # Why it is versioned from the first release
//
// D19 says structured output is a public contract, and the reason is empirical
// rather than aspirational: the first thing anybody does with a tool like this is
// pipe it into `jq` and put the result in a runbook. A field renamed a release
// later breaks that runbook silently — `jq` reports nothing for a path that no
// longer exists, so the pipeline keeps running and starts producing empty
// findings. Carrying an apiVersion from the first release is what makes a future
// incompatible change *sayable* instead of merely regrettable.
//
// # Why item field names mirror the schema's columns
//
// Every item field below is spelled exactly as the frozen ClickHouse schema
// spells its column (docs/SCHEMA.md), because the two are the same data reached
// two ways. A `jq` recipe written against a SQL result therefore transfers to CLI
// output without being rewritten, and an engineer who has read either one has
// read both. That agreement is the whole point of the mirroring, which is why
// query.Change's own JSON tags are used unchanged rather than being restated
// here: a second spelling of "resource_version" is a spelling that eventually
// disagrees.
//
// # The additive-only policy
//
// Within one apiVersion, fields may be added and must never be renamed, removed
// or repurposed. A consumer must ignore fields it does not recognize. Anything
// else — dropping a field, changing what one means, changing a type — is a new
// apiVersion, exactly as it is for the schema itself.
//
// # What is deliberately not in it
//
// Notices are not. They go to standard error with every other qualification this
// CLI writes, which is what keeps `-o json | jq` safe in their presence. The one
// qualification a *script* cannot do without is coverage — the difference between
// "nothing changed" and "nothing was watching" (Invariant 9) — and that is why it
// is a structured field of the envelope rather than a sentence on the other
// stream.

// EnvelopeAPIVersion is the version of this contract.
//
// It shares a group with the configuration file's apiVersion deliberately — both
// are this CLI's own documents making the same promise (see cli.ConfigAPIVersion)
// — and is a separate constant just as deliberately: they are two contracts, and
// the day one of them has to break is not the day the other does.
const EnvelopeAPIVersion = "cli.kuberecord.io/v1alpha1"

// The envelope kinds, one per question a command answers (D19).
//
// A kind is what tells a consumer what the items are without inspecting them,
// which matters most for the empty case: an envelope with no items still says
// what it is an empty answer to.
const (
	// KindTimeline holds recorded changes, one item per change.
	KindTimeline = "Timeline"
	// KindDiff holds the same changes with their operations decoded and the
	// prior value each one destroyed recovered where a replay established it.
	KindDiff = "Diff"
	// KindObject holds one reconstructed state and the evidence for it.
	KindObject = "Object"
	// KindCoverage holds watch-scope intervals, one item per interval.
	KindCoverage = "Coverage"
)

// StructuredFormat is a serialization of the envelope.
//
// It is this package's own vocabulary rather than the command's OutputFormat,
// because the renderer must not depend on the flag surface: these are the
// serializations a document has, and the set of spellings a flag accepts is a
// different question that changes for different reasons.
type StructuredFormat string

// The serializations the envelope is written in.
const (
	// StructuredJSON is one indented document.
	StructuredJSON StructuredFormat = "json"
	// StructuredJSONL is the streaming form: the envelope head on the first
	// line, then one item per line. See NewStream.
	StructuredJSONL StructuredFormat = "jsonl"
	// StructuredYAML is one document, produced by transforming the JSON one, so
	// that the two are the same document in two syntaxes.
	StructuredYAML StructuredFormat = "yaml"
)

// EnvelopeHead is everything in the envelope that is not an item.
//
// It is a type of its own because `jsonl` writes it alone, on the first line,
// before any item exists. Sharing it with the whole-document forms is what keeps
// the streaming output a rearrangement of the same contract rather than a second
// contract with similar field names.
type EnvelopeHead struct {
	// APIVersion is EnvelopeAPIVersion. It is a field rather than an implied
	// constant because it is what a consumer branches on.
	APIVersion string `json:"apiVersion"`
	// Kind is one of the kinds above.
	Kind string `json:"kind"`
	// Metadata describes where the answer came from and what was watching.
	Metadata EnvelopeMetadata `json:"metadata"`
}

// EnvelopeMetadata is the provenance of an answer.
//
// It carries exactly three fields, which is D19's own list. Each is here because
// a script cannot do its job without it: which cluster's history this is, which
// engine produced it — two backends can disagree, and then knowing which one
// answered decides which answer to trust — and whether anything was watching.
type EnvelopeMetadata struct {
	// ClusterID is the kuberecord cluster identity (D21), the cluster_id column
	// of the frozen schema rather than a kubeconfig entry.
	ClusterID string `json:"cluster_id"`
	// Backend is the engine's own stable identifier, from
	// query.Capabilities.Backend.
	Backend string `json:"backend"`
	// Coverage is what the watch scopes say about the window that was asked
	// about.
	Coverage CoverageReport `json:"coverage"`
}

// CoverageReport is Invariant 9 in a form a script can branch on.
//
// The three fields answer three different questions, and collapsing any two of
// them would reintroduce the ambiguity the invariant exists to remove:
//
//   - Available false means the backend has no scope log at all, so it cannot say
//     whether anything was watching. That is a statement about the backend and
//     must not be read as "nothing was watching".
//   - Available true with no intervals is the finding: nothing was ever watching
//     this scope, so an empty item list is not evidence that nothing changed.
//   - Available true with intervals means the scope was watched over those
//     periods, and an empty item list means the object genuinely did not change
//     within them.
//
// This is what lets a consumer distinguish the two emptinesses from one
// invocation, rather than having to ask a second question it may not know it
// needs to ask.
type CoverageReport struct {
	// Available reports whether the backend could answer the coverage question
	// at all.
	Available bool `json:"available"`
	// Summary is the same sentence the human-readable header carries, so that a
	// script logging one line logs the line a person would have read.
	Summary string `json:"summary"`
	// Intervals are the periods the scope was watched, oldest first, with a null
	// `to` for one that is still open. It is never null: an empty list is the
	// "nothing was watching" answer and must be spelled as a list to be read as
	// one.
	Intervals []query.ScopeInterval `json:"intervals"`
}

// Envelope is the whole document, for the formats that have one.
//
// `jsonl` has no Envelope value at any point, by design: assembling one would
// mean holding every item, which is the thing that format exists not to do.
type Envelope struct {
	EnvelopeHead `json:",inline"`

	// Items are the answer. Never null — an empty answer is an empty list, and a
	// consumer that had to handle both would eventually handle only one.
	Items []any `json:"items"`
}

// DiffItem is one change with its operations decoded.
//
// The embedded Change contributes the schema's own columns, inline and unrenamed,
// so a Diff item is a Timeline item plus the two things `diff` computes that
// nothing else does: the decoded hunks, and the reason a patch could not be
// decoded when that happened.
type DiffItem struct {
	query.Change `json:",inline"`

	// PatchError says why the recorded diff could not be decoded. Present only
	// when it could not.
	//
	// Without it, a row whose patch is unreadable would arrive as a non-empty
	// `diff` column with an empty `hunks` list, which reads as a change that
	// touched nothing — a silent error of exactly the kind Invariant 4 forbids.
	PatchError string `json:"patch_error,omitempty"`

	// Hunks are the operations the patch recorded. Empty for a row that carries
	// no patch at all: a first sighting, a snapshot, a deletion.
	Hunks []Hunk `json:"hunks"`
}

// Hunk is one operation of a recorded patch.
type Hunk struct {
	// Op is the RFC 6902 operation name as recorded: add, remove or replace.
	Op string `json:"op"`
	// Path is the dotted display path — "spec.template.spec.containers[0].image"
	// — which is the grammar --field accepts and the table prints, so a path read
	// out of structured output can be pasted back into a query.
	Path string `json:"path"`
	// Pointer is the RFC 6901 pointer exactly as the patch recorded it, escapes
	// and all. Both spellings are carried because Path is the one a human uses
	// and Pointer is the one a JSON Patch library takes.
	Pointer string `json:"pointer"`
	// From is the source pointer of a move or copy, absent otherwise.
	From string `json:"from,omitempty"`
	// Old is the value this operation destroyed, recovered by replaying the
	// object's state up to the change.
	//
	// It is null both when the value really was JSON null and when the replay
	// could not establish it, which is why OldKnown exists and why a consumer
	// must read that rather than test this for null. Collapsing the two would be
	// the fabrication Invariant 4 forbids, told quietly.
	Old any `json:"old"`
	// OldKnown reports whether Old is an answer rather than an absence.
	OldKnown bool `json:"old_known"`
	// New is the operation's new value, absent on a remove. A redacted value
	// arrives here as the literal sentinel string a redaction policy wrote, which
	// is RedactionSentinel.
	New json.RawMessage `json:"new,omitempty"`
}

// ObjectItem is one reconstructed state and the evidence for how it was
// reconstructed.
//
// The provenance travels with the state rather than in the envelope's metadata
// because it describes this reconstruction, not this invocation: base_ts and
// patches_applied are how a reader judges the answer — a state assembled from a
// base an hour old and two patches invites more confidence than one assembled
// from a base three months old and four hundred.
type ObjectItem struct {
	// At is the instant the state was reconstructed for.
	At time.Time `json:"at"`
	// UID is the incarnation the state belongs to, empty when the recorded
	// document carried none.
	UID string `json:"uid"`
	// BaseTS is the timestamp of the full-state row the replay started from.
	BaseTS time.Time `json:"base_ts"`
	// BaseEvent is that row's event type.
	BaseEvent string `json:"base_event"`
	// PatchesApplied is how many patches were replayed over the base.
	PatchesApplied int `json:"patches_applied"`
	// SHA256 is the digest recorded for the last row consumed, spelled as the
	// schema's column is. It is what `--verify` compares a rehash of the state
	// against.
	SHA256 string `json:"sha256"`
	// Object is the reconstructed state. It is the state that was *recorded*,
	// which is not the object the API server held — see ObjectDocument for the
	// three ways it differs and why nothing should apply it.
	Object map[string]any `json:"object"`
}

// Stream writes an envelope, item by item.
//
// # Why a stream and not a value
//
// One of the three formats must not hold the answer in memory. A timeline can
// return six figures of changes — an object caught in a reconcile loop manages
// that in a day — and `jsonl` exists precisely so that such a result can be piped
// into something that processes it a line at a time. A function taking a
// completed []any could not offer that, so the writer is a cursor and the
// buffering, where there is any, is the format's rather than the caller's.
//
// The formats differ in what they can promise, and the difference is honest
// rather than hidden:
//
//   - jsonl writes the head immediately and each item as it arrives. Memory does
//     not scale with the number of items.
//   - json and yaml hold the items until Close, because a single document cannot
//     be finished before it is complete. YAML additionally cannot stream even in
//     principle here: sigs.k8s.io/yaml produces YAML by transforming the complete
//     JSON document, which is exactly what makes the two formats the same
//     document in two syntaxes.
//
// A caller that needs the streaming property asks for jsonl. A caller that asks
// for json gets a document whose size is the answer's size, which is what a
// single JSON document is.
type Stream struct {
	out    io.Writer
	format StructuredFormat
	head   EnvelopeHead

	// items is the buffer the whole-document formats need and jsonl never
	// touches. Nil for jsonl, so that a mistake in the switch below shows up as a
	// nil map write in a test rather than as memory quietly growing in
	// production.
	items []any

	// closed guards against a second Close writing a second document.
	closed bool
}

// NewStream begins an envelope of kind head.Kind in format, writing to out.
//
// For jsonl the head line is written here, before any item is known, which is
// what makes the metadata — and with it the coverage report — available to a
// consumer that is processing the stream as it arrives rather than after it ends.
func NewStream(out io.Writer, format StructuredFormat, head EnvelopeHead) (*Stream, error) {
	stream := &Stream{out: out, format: format, head: head}
	switch format {
	case StructuredJSONL:
		if err := stream.writeLine(head); err != nil {
			return nil, err
		}
	case StructuredJSON, StructuredYAML:
		// Empty rather than nil: an answer with no items must serialize as `[]`,
		// because a consumer iterating `.items` over a null gets an error where
		// the honest answer is zero iterations.
		stream.items = []any{}
	default:
		return nil, fmt.Errorf("%q is not a structured serialization", format)
	}
	return stream, nil
}

// Write adds one item.
//
// Under jsonl the item reaches out before this returns, which is the property the
// format exists for and the one its test asserts by interleaving.
func (s *Stream) Write(item any) error {
	if s.closed {
		return fmt.Errorf("writing an item to a closed %s envelope", s.format)
	}
	if s.format == StructuredJSONL {
		return s.writeLine(item)
	}
	s.items = append(s.items, item)
	return nil
}

// Close finishes the envelope.
//
// It must be called on every path, including a failure, because for json and yaml
// nothing has been written until it runs. Calling it twice is a caller error and
// is reported rather than ignored: the second document would be appended to the
// first, producing a stream neither format can parse.
func (s *Stream) Close() error {
	if s.closed {
		return fmt.Errorf("closing an already-closed %s envelope", s.format)
	}
	s.closed = true
	if s.format == StructuredJSONL {
		return nil
	}

	envelope := Envelope{EnvelopeHead: s.head, Items: s.items}
	encoded, err := encodeEnvelope(envelope, s.format)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(s.out, encoded); err != nil {
		return fmt.Errorf("writing the %s envelope: %w", s.format, err)
	}
	return nil
}

// writeLine writes one compact JSON document and a newline.
//
// json.Encoder is deliberately not used: it would be a second encoder with its
// own escaping settings, and the head line and the item lines have to be encoded
// identically for the output to be one format rather than two that look alike.
func (s *Stream) writeLine(value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encoding a %s item: %w", s.head.Kind, err)
	}
	if _, err := s.out.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("writing a %s item: %w", s.head.Kind, err)
	}
	return nil
}

// encodeEnvelope serializes a whole envelope.
//
// YAML goes through sigs.k8s.io/yaml, which marshals via JSON and therefore emits
// the same field names, the same ordering rules and the same scalar spellings the
// JSON form does. A reader comparing the two must see one document in two
// syntaxes, not two documents — the same agreement encodeObject keeps for a
// reconstruction.
func encodeEnvelope(envelope Envelope, format StructuredFormat) (string, error) {
	switch format {
	case StructuredJSON:
		encoded, err := json.MarshalIndent(envelope, "", "  ")
		if err != nil {
			return "", fmt.Errorf("encoding the %s envelope as JSON: %w", envelope.Kind, err)
		}
		return string(encoded) + "\n", nil
	case StructuredYAML:
		encoded, err := yaml.Marshal(envelope)
		if err != nil {
			return "", fmt.Errorf("encoding the %s envelope as YAML: %w", envelope.Kind, err)
		}
		return string(encoded), nil
	}
	return "", fmt.Errorf("%q is not a structured serialization", format)
}

// Hunks decodes one row's operations into the structured form.
//
// It reads the same render.Op values the tables and the hunk view render, rather
// than decoding the patch a second time, so that a path shown on a terminal and a
// path emitted to a script are the same string — including the RFC 6901
// unescaping, which is the step a second reading would get subtly wrong.
func Hunks(ops []Op) []Hunk {
	hunks := make([]Hunk, 0, len(ops))
	for _, op := range ops {
		hunks = append(hunks, Hunk{
			Op:       op.Type,
			Path:     DisplayPath(op.Path),
			Pointer:  op.Path,
			From:     op.From,
			Old:      op.Old,
			OldKnown: op.OldKnown,
			New:      op.Value,
		})
	}
	return hunks
}
