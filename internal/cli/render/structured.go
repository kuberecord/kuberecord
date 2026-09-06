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
	"strings"
	"time"

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
// The mirroring is of names, and only of names. Two columns are carried at a type
// the schema does not have — see ChangeItem — because the column type is a
// statement about what ClickHouse stores rather than about what a consumer needs,
// and a `jq` recipe depends on the spelling of a field rather than on its being a
// string. Changing one is still a break, and it was recorded as one.
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
// CLI writes, which is what keeps `-o json | jq` safe in their presence. Two
// qualifications a *script* cannot do without are the exceptions, and they are
// exceptions for the same reason: a consumer has to be able to act on them, and
// standard error is the stream `2>/dev/null` discards. Coverage is the difference
// between "nothing changed" and "nothing was watching" (Invariant 9);
// reconstruction is the difference between a document that was recorded and one
// that was assembled and must not be applied. Both are fields of the envelope
// rather than sentences on the other stream.

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
	// KindBlame holds per-field attribution, one item per field: which recorded
	// change last wrote it, and who was seen on that change.
	KindBlame = "Blame"
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
// Three of its fields are on every answer, which is D19's own list. Each is there
// because a script cannot do its job without it: which cluster's history this is,
// which engine produced it — two backends can disagree, and then knowing which
// one answered decides which answer to trust — and whether anything was watching.
//
// The fourth is present only on a KindObject envelope, because it is the only
// kind whose items are assembled rather than read. See ReconstructionReport for
// why a document that was assembled has to say so on the stream a script reads.
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
	// Reconstruction marks a document that was assembled from recorded history
	// rather than read back whole, and is absent from every other kind. It is a
	// pointer for exactly that reason: a Timeline carrying `"reconstruction":
	// null` would invite a consumer to test the field for null, and the honest
	// spelling of "this question has no reconstruction in it" is no key at all.
	Reconstruction *ReconstructionReport `json:"reconstruction,omitempty"`
}

// ReconstructionReport is the "not a deployable manifest" warning in a form a
// script can branch on.
//
// # Why the warning needs a second form
//
// WriteObject sends the human-facing header to standard error for JSON and JSONL,
// because neither format has a comment syntax and putting it on standard output
// would break the `jq` that is the whole reason somebody asked for JSON. That is
// the right call, and it has one consequence: `2>/dev/null` discards the warning,
// and a piped consumer never reads that stream in the first place. So
// `kuberecord get … -o json | jq '.items[0].object'` hands a script a document
// that looks exactly like a manifest, with nothing anywhere in its input saying
// it is a reconstruction.
//
// This is the same problem CoverageReport solves and it gets the same answer. A
// fact a consumer must be able to act on cannot live only in prose on the other
// stream — it has to be a field, on stdout, in every serialization.
//
// # Why these fields
//
// The first two are what a script branches on: what the document is, and what
// must not be done with it. NotDeployable is deliberately not left as an
// inference from Reconstructed, because the inference is precisely what an
// automated consumer does not make — a guard rail is `jq -e
// '.metadata.reconstruction.not_deployable'` and refuse to apply, and a field it
// can name is what makes that one line rather than a comment in a runbook.
//
// The rest are the evidence, and they are the same three facts a reader judges
// the reconstruction by on the header: a state assembled from a base an hour old
// and two patches deserves more confidence than one assembled from a base three
// months old and four hundred. They are spelled as ObjectItem spells them, and
// ObjectProvenance renders the header from this very value (see ReconstructionOf),
// so the sentence a person reads and the fields a script reads cannot come to
// describe different reconstructions.
type ReconstructionReport struct {
	// Reconstructed is always true where this report appears, and its value is
	// not the point: a consumer reads `.metadata.reconstruction.reconstructed`
	// on every document this CLI produces and gets true here and null — falsey —
	// everywhere else, without having to know which kinds are assembled.
	Reconstructed bool `json:"reconstructed"`

	// NotDeployable is always true for the same reason, and says the thing the
	// header says in words: this document must not be applied. Volatile metadata
	// was stripped before the state was recorded, redacted fields carry
	// RedactionSentinel in place of their values, and it describes a past
	// somebody deliberately moved the object out of.
	NotDeployable bool `json:"not_deployable"`

	// At is the instant the state was reconstructed for — the `--at` that was
	// asked about, not the wall clock the command ran on.
	At time.Time `json:"at"`

	// BaseTS is the timestamp of the full-state row the replay started from.
	BaseTS time.Time `json:"base_ts"`

	// BaseEvent is that row's event type, as the schema records it.
	BaseEvent string `json:"base_event"`

	// PatchesApplied is how many patches were replayed over the base.
	PatchesApplied int `json:"patches_applied"`
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

// ChangeItem is one change as an envelope carries it.
//
// # Why two columns are re-typed and the other eight are not
//
// The item is query.Change embedded, so every field name is the frozen schema's
// column name by construction rather than by a mapping somebody has to keep in
// step. Two of those columns are then shadowed here with a different Go type, and
// that is the whole of what this type does.
//
// The schema stores `data` and `diff` as String because ClickHouse stores
// strings. Emitting them as strings meant emitting a JSON document with JSON
// inside a string: every consumer had to parse a second time before it could
// reach a path, and in YAML the escaped payload wrapped mid-token across lines
// and could not be read at all. A format whose purpose is to be machine-readable
// failing to be machine-readable without a second parse is a defect rather than a
// preference, and the contract never required otherwise — it fixes the *names* of
// the fields, and it says nothing about their types. Names are what a `jq` recipe
// written against a SQL result depends on, and those are unchanged.
//
// # Why the bytes are carried rather than decoded
//
// Both fields are json.RawMessage, so the recorded bytes reach the output
// untouched. Decoding through map[string]any would route every number through
// float64, which rewrites an integer past 2^53 into a different integer and a
// value's written form into another one — in the output of a tool whose subject
// is what was recorded. It is also the cheaper path for a `data` column holding a
// full serialized object.
//
// # The two absences
//
// A row with no patch and a row with no full state are ordinary — a first
// sighting carries no diff, a deletion carries neither — and they serialize as
// `[]` and `{}`. Not null, and not omitted: an absent patch and an empty patch
// are different facts, and either spelling would make a consumer branch on
// presence to learn something the value already says.
//
// The same reasoning fixes the two collection columns, which is why NewChangeItem
// is the only way to build one of these. See there.
type ChangeItem struct {
	query.Change `json:",inline"`

	// Data is the full recorded state of the object, present on full-state rows
	// and `{}` on every other. It shadows query.Change.Data, whose string is the
	// column as the backend returns it.
	Data json.RawMessage `json:"data"`

	// Diff is the RFC 6902 patch against the previous state, present on
	// modifications and checkpoints and `[]` on every other row. It shadows
	// query.Change.Diff for the reason Data shadows its own column.
	//
	// On a checkpoint it describes the transition Data already reflects and must
	// not be re-applied over it — the same warning query.Change.Diff carries,
	// which parsing the value does nothing to change.
	Diff []json.RawMessage `json:"diff"`
}

// NewChangeItem prepares one change for the envelope, or reports the row as
// corrupt.
//
// It is the single place a Timeline item and a Diff item are built, so the two
// cannot come to disagree about what a change looks like on stdout.
//
// # The two shapes it fixes
//
// A nil slice and a nil map encode as JSON null, and the columns they mirror are
// an array and a map that a backend returns empty. The contract already says
// which reading is the honest one — of an actorless deletion, query.Change.Actors
// says "an empty list is the honest answer rather than a missing one" — and null
// is the other reading. It also breaks the obvious consumer: `.actors[]` fails on
// a null and yields nothing on an empty list, and failing is not what "this
// deletion had no actors" should do to somebody's pipeline.
//
// # Why a parse failure is a finding rather than a fallback
//
// A stored `diff` that will not parse is corrupt evidence. Emitting the raw
// string in its place would hide exactly the corruption an audit tool exists to
// surface, and emitting an empty array would say the change touched nothing,
// which is a stronger and more dangerous lie. So the row is named — by the two
// fields that identify it, ts and uid — and the invocation fails.
//
// The human renderings are deliberately not held to this. They already parse
// these columns to lay a row out, they already have somewhere to say "unreadable
// patch" inside the row, and a table that dropped four hundred readable changes
// over one damaged one would be a worse audit tool rather than a stricter one.
// Structured output has no such cell, and its consumer is a script.
func NewChangeItem(change query.Change) (ChangeItem, error) {
	if change.Actors == nil {
		change.Actors = []string{}
	}
	if change.Labels == nil {
		change.Labels = map[string]string{}
	}
	item := ChangeItem{Change: change}

	data, err := recordedValue(change.Data, '{', emptyObject)
	if err != nil {
		return ChangeItem{}, corruptColumn(change, "data", "a JSON object", err)
	}
	item.Data = data

	diff, err := recordedValue(change.Diff, '[', emptyArray)
	if err != nil {
		return ChangeItem{}, corruptColumn(change, "diff", "a JSON array", err)
	}
	// An array always decodes into a slice of raw elements, so this cannot fail
	// once recordedValue has established the shape. It is checked rather than
	// discarded because "cannot fail" is a claim about today's standard library,
	// and Invariant 4 does not have an exception for claims.
	if err := json.Unmarshal(diff, &item.Diff); err != nil {
		return ChangeItem{}, corruptColumn(change, "diff", "a JSON array", err)
	}
	if item.Diff == nil {
		item.Diff = []json.RawMessage{}
	}
	return item, nil
}

// The empty renderings of the two columns. Spelled once so that the value a
// patchless row carries and the value the documentation promises cannot drift,
// and as strings rather than as package-level byte slices so that every item gets
// its own copy: an item's fields belong to whoever holds the item.
const (
	emptyObject = "{}"
	emptyArray  = "[]"
)

// recordedValue validates one recorded column and returns its bytes unchanged.
//
// Validation is two steps rather than one decode into the destination type, and
// the reason is the message a user reads. Unmarshalling `{"op":…}` straight into
// a []json.RawMessage reports "cannot unmarshal object into Go value of type
// []json.RawMessage", which names this program's implementation rather than the
// reader's data. Decoding into a json.RawMessage first separates the two findings
// a reader actually has — the value is not JSON at all, or it is JSON of the
// wrong shape — and keeps the recorded bytes for the output either way.
//
// A blank column is the absent case, not a failure: a deletion records no state
// and no patch, and a first sighting records no patch.
func recordedValue(recorded string, open byte, empty string) (json.RawMessage, error) {
	if strings.TrimSpace(recorded) == "" {
		return json.RawMessage(empty), nil
	}
	var value json.RawMessage
	if err := json.Unmarshal([]byte(recorded), &value); err != nil {
		return nil, fmt.Errorf("it is not valid JSON: %w", err)
	}
	if len(value) == 0 || value[0] != open {
		return nil, fmt.Errorf("the recorded value is %s", jsonKind(value))
	}
	return value, nil
}

// jsonKind names the shape of a validated JSON value, for a message that has to
// say what was found rather than what could not be done with it.
func jsonKind(value json.RawMessage) string {
	if len(value) == 0 {
		return "empty"
	}
	switch value[0] {
	case '{':
		return "a JSON object"
	case '[':
		return "a JSON array"
	case '"':
		return "a JSON string"
	case 't', 'f':
		return "a JSON boolean"
	case 'n':
		return "JSON null"
	}
	return "a JSON number"
}

// corruptColumn phrases the finding: which row, which column, what is wrong, and
// what to do next.
//
// The row is named by ts and uid because those are the two fields that identify
// it — ts at the precision the schema records, since two changes a microsecond
// apart are two changes — and the reader's next step is to look at the stored
// value themselves, which the message says how to do.
//
// The recorded value is deliberately not quoted into the message. Printing it
// would be the fallback this refusal exists to avoid, one stream over, and the
// column can hold a full object whose contents nobody asked to have on their
// terminal.
func corruptColumn(change query.Change, column, want string, err error) error {
	return fmt.Errorf(
		"the change recorded at %s (%s) has an unreadable %s column, which must be %s: %w. "+
			"That is corrupt evidence rather than a formatting problem, so structured output will "+
			"not print the raw column in its place; `-o table` still renders the row, and reading "+
			"the stored value out of the backend is how to judge it",
		change.TS.UTC().Format(time.RFC3339Nano), describeUID(change.UID), column, want, err)
}

// describeUID names the incarnation a corrupt row belongs to, including when the
// row does not name one.
//
// A blank rendered as `uid ""` would read as a quoting accident. A row genuinely
// carrying no UID is a real state — an archive line written before the identity
// was known — and saying so is what keeps the reader looking at the timestamp
// rather than at the message.
func describeUID(uid string) string {
	if uid == "" {
		return "no uid recorded"
	}
	return "uid " + uid
}

// DiffItem is one change with its operations decoded.
//
// The embedded ChangeItem contributes the schema's own columns, inline and
// unrenamed, so a Diff item is a Timeline item plus the two things `diff`
// computes that nothing else does: the decoded hunks, and the reason a patch
// could not be decoded when that happened.
type DiffItem struct {
	ChangeItem `json:",inline"`

	// PatchError says why the recorded diff could not be decoded as a *patch*.
	// Present only when it could not.
	//
	// Its scope is narrower than it looks, and narrower than it once was. A diff
	// that is not a JSON array at all never reaches an item: NewChangeItem reports
	// the row as corrupt and the invocation fails. What is left for this field is
	// the value that parses as an array and is not a patch — an operation whose
	// `op` is a number, say — which is a defect in one entry rather than in the
	// column.
	//
	// Without it, such a row would arrive as a populated `diff` with an empty
	// `hunks` list, which reads as a change that touched nothing — a silent error
	// of exactly the kind Invariant 4 forbids.
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

// BlameItem is one field's attribution.
//
// The fields that exist in the schema are spelled as the schema spells them — ts,
// actors, uid, resource_version, event_type — because they are the schema's, read
// back through a different question. The three that do not exist there describe
// the attribution rather than the change: which field this is, whether the answer
// is inside the window at all, and how many of the object's fields the row stands
// for.
type BlameItem struct {
	// Path is the dotted display path — "spec.template.spec.containers[0].image" —
	// which is the grammar --field accepts and the table prints, so a path read out
	// of structured output can be pasted back into a query.
	Path string `json:"path"`

	// Pointer is the RFC 6901 pointer the path was rendered from. Both spellings
	// are carried for the reason a Hunk carries both: Path is the one a human
	// uses and Pointer is the one a JSON Patch library takes.
	Pointer string `json:"pointer"`

	// Attributed reports whether the rest of this item describes a change that was
	// read.
	//
	// **Read this, not ts.** False means the field's last write is older than the
	// window — the table's "(before window)" — and the fields below are then their
	// zero values rather than an answer. Collapsing the two would be the
	// fabrication Invariant 4 forbids, told quietly, and it is the same discipline
	// old_known keeps for a hunk's prior value.
	Attributed bool `json:"attributed"`

	// TS is when the last write happened, null when Attributed is false.
	TS *time.Time `json:"ts"`

	// Actors are the field managers seen on that change. Never null: an empty list
	// is the honest answer for a change that recorded none, and `.actors[]` fails
	// on a null where it should yield nothing.
	Actors []string `json:"actors"`

	// UID is the incarnation the attributing change belongs to.
	UID string `json:"uid"`

	// ResourceVersion is that change's resourceVersion.
	ResourceVersion string `json:"resource_version"`

	// EventType is that change's event type, as the schema records it.
	EventType string `json:"event_type"`

	// Removed reports that the last write deleted this path, so the field is no
	// longer part of the object. The item is emitted anyway, because who removed a
	// field is one of the two questions this command answers.
	Removed bool `json:"removed"`

	// Fields is how many of the object's fields this item stands for: one, unless
	// --depth collapsed a subtree into it.
	Fields int `json:"fields"`
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
//     principle here: YAMLDocument produces YAML by transforming the complete
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
// Both formats are reached through the JSON tags, so they emit the same field
// names in the same order with the same scalar spellings. A reader comparing the
// two must see one document in two syntaxes, not two documents.
//
// The YAML half goes through YAMLDocument rather than through the familiar
// sigs.k8s.io/yaml, and that file is where the reason lives: the familiar import
// transforms the JSON through a Go map, and a map is what sorted `kind` and
// `metadata` below a several-hundred-line `items` array.
func encodeEnvelope(envelope Envelope, format StructuredFormat) (string, error) {
	switch format {
	case StructuredJSON:
		encoded, err := json.MarshalIndent(envelope, "", "  ")
		if err != nil {
			return "", fmt.Errorf("encoding the %s envelope as JSON: %w", envelope.Kind, err)
		}
		return string(encoded) + "\n", nil
	case StructuredYAML:
		encoded, err := YAMLDocument(envelope)
		if err != nil {
			return "", fmt.Errorf("encoding the %s envelope as YAML: %w", envelope.Kind, err)
		}
		return encoded, nil
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
