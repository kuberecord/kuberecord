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
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// A reconstructed object, and the header that stops somebody deploying it.
//
// # Why the header is not optional
//
// What comes out of a reconstruction looks exactly like a manifest. It has an
// apiVersion, a kind, a metadata block and a spec, and the obvious next thing to
// do with it is `kubectl apply -f`. That would be wrong in three separate ways at
// once, none of them visible in the document:
//
//   - metadata.managedFields, metadata.resourceVersion and metadata.generation
//     were stripped before the state was ever recorded, so the object is not the
//     one the API server held.
//   - Every field a redaction policy covered carries RedactionSentinel rather than
//     its value, so applying it would write the literal string "[REDACTED]" into
//     a password field.
//   - It is a statement about the past. Applying it reverts an object to a state
//     somebody deliberately moved it out of.
//
// So the header is mandatory rather than a courtesy, and it says NOT A DEPLOYABLE
// MANIFEST in those words. YAML carries it as comments; JSON has no comments, so
// the identical block goes to standard error, which keeps stdout a document `jq`
// can read while still putting the warning in front of whoever ran the command.
//
// # Why the header is not sufficient on its own
//
// Standard error is the stream `2>/dev/null` discards and the one a pipe never
// reads, so `get … -o json | jq '.items[0].object'` receives a reconstruction
// with nothing in its input saying so. The header is for the person; the
// envelope's metadata.reconstruction marker is the same facts for the script, on
// stdout, in every format (ReconstructionReport). ReconstructionOf derives both,
// so there is one warning rendered twice rather than two warnings.
//
// # Why the provenance is in it
//
// A reconstruction is an assertion about the past that somebody may act on, and
// the base row and patch count are what let a reader judge it rather than trust
// it: a state assembled from a base an hour old and two patches deserves more
// confidence than one assembled from a base three months old and four hundred.

// notDeployable is the sentence the header exists for, in the words the
// acceptance criteria fix. It is a constant so that a test can assert the exact
// phrase rather than a paraphrase of it, and so that a rewording is a deliberate
// change to a warning rather than a drive-by edit.
const notDeployable = "NOT A DEPLOYABLE MANIFEST"

// ObjectDocument is a reconstructed state, ready to be written.
type ObjectDocument struct {
	// Kind is the object's group and kind, "apps/Deployment", or the bare kind
	// for the core group.
	Kind string
	// Ref is "namespace/name", or the bare name for a cluster-scoped kind.
	Ref string
	// Cluster is the kuberecord cluster identity (D21).
	Cluster string
	// UID is the incarnation the state belongs to. Empty when the recorded
	// document carried none, which is reported as such rather than left blank.
	UID string
	// At is the instant the state was reconstructed for.
	At time.Time
	// BaseTS is the timestamp of the full-state row the replay started from.
	BaseTS time.Time
	// BaseEvent is that row's event type.
	BaseEvent string
	// PatchesApplied is how many patches were replayed over the base.
	PatchesApplied int
	// SHA256 is the digest recorded for the row the replay finished on, which is
	// what --verify compares a rehash of the state against. Empty when no digest
	// was recorded, which is an absence rather than a failure and is reported as
	// one.
	SHA256 string
	// Coverage is the pre-rendered coverage summary, carried in the header for
	// the reason every other document carries one: an object reconstructed from a
	// period nobody was watching is a different answer from one reconstructed
	// from a period that was watched, and the reader has to be able to see which
	// they have (Invariant 9).
	Coverage string
	// State is the reconstructed object.
	State map[string]any
	// Notices are written to standard error, in order.
	Notices []Notice
}

// WriteObject writes the reconstruction to out as an envelope, and its notices to
// errOut.
//
// The state travels inside the versioned envelope rather than as a bare document,
// and the choice is deliberate twice over. It is what makes `kind: Object` — one
// of D19's four — something a command actually produces, so a consumer branches
// on the same field for every question this CLI answers. And it is the stronger
// form of the warning below: a document nobody should apply is now a document
// `kubectl apply -f` cannot apply, because the thing at the top of it is not a
// Kubernetes object.
//
// The provenance header stays mandatory either way. YAML carries it as comments
// above the envelope; JSON and JSONL have no comment syntax, so the identical
// block goes to standard error — which keeps stdout a document `jq` can read
// while still putting the warning in front of whoever ran the command.
//
// # Why the marker is stamped here rather than by the caller
//
// The head arrives without metadata.reconstruction and leaves with it, derived
// from the same document the header is derived from. Two things follow, and both
// are the point: there is no way to write an Object envelope through this
// function without the marker on it, and no way for the marker and the header to
// describe two different reconstructions. head is taken by value, so the
// caller's own is left as it was.
func WriteObject(
	out, errOut io.Writer, doc ObjectDocument, head EnvelopeHead,
	format StructuredFormat, opts Options,
) error {
	// The colour question is answered once, here, and every line rendered below
	// is spelled the same way afterwards — including the block that travels on
	// standard error, so the two routings stay one block rather than becoming two
	// that resemble each other.
	severity := NewSeverity(opts.Color)

	provenance := ObjectProvenance(doc, severity)
	reconstruction := ReconstructionOf(doc)
	head.Metadata.Reconstruction = &reconstruction

	if out != nil {
		if format == StructuredYAML {
			if _, err := io.WriteString(out, provenance); err != nil {
				return fmt.Errorf("writing the reconstructed object's header: %w", err)
			}
		}
		if err := writeObjectEnvelope(out, doc, head, format, severity); err != nil {
			return err
		}
	}

	if errOut == nil {
		return nil
	}
	warning := ""
	if format != StructuredYAML {
		warning = provenance
	}
	if warning == "" && len(doc.Notices) == 0 {
		return nil
	}
	if _, writeErr := io.WriteString(errOut, warning+renderNotices(doc.Notices, opts)); writeErr != nil {
		return fmt.Errorf("writing the reconstructed object's notices: %w", writeErr)
	}
	return nil
}

// writeObjectEnvelope writes the one-item envelope a reconstruction is, and —
// for YAML on a terminal — lets the recorded object stand out of it.
//
// The colour pass is a post-pass over the very bytes the plain path writes, which
// is the shape rather than an implementation detail: there is one document, and
// the coloured rendering of it can differ only by the escapes this function then
// wraps some of its lines in. Every other format, and YAML with colour off, does
// not go near it — a JSON document with escape sequences in it is not a JSON
// document.
func writeObjectEnvelope(
	out io.Writer, doc ObjectDocument, head EnvelopeHead, format StructuredFormat, severity Severity,
) error {
	if format != StructuredYAML || !severity.enabled {
		return streamObjectEnvelope(out, doc, head, format)
	}

	var document bytes.Buffer
	if err := streamObjectEnvelope(&document, doc, head, format); err != nil {
		return err
	}
	if _, err := io.WriteString(out, provenanceAroundObject(document.String(), severity)); err != nil {
		return fmt.Errorf("writing the reconstructed object's envelope: %w", err)
	}
	return nil
}

// streamObjectEnvelope writes the envelope itself.
//
// It goes through the same Stream every other structured answer does, so that a
// reconstruction and a timeline are the same document shape in the same
// serializations — including `jsonl`, where an answer of exactly one item is a
// head line and one item line rather than a special case.
func streamObjectEnvelope(
	out io.Writer, doc ObjectDocument, head EnvelopeHead, format StructuredFormat,
) error {
	stream, err := NewStream(out, format, head)
	if err != nil {
		return err
	}
	if writeErr := stream.Write(objectItem(doc)); writeErr != nil {
		return errors.Join(writeErr, stream.Close())
	}
	return stream.Close()
}

// Figure and ground in a document that serves two readers.
//
// # The decision this records
//
// The envelope is read by a machine and by a person, and it was designed for the
// machine. `-o yaml` is at once the format a script parses and the format
// somebody reaches for to look at a reconstructed object, and the two want
// opposite things: a script wants every fact at the same depth and in one
// spelling, a person wants the six bookkeeping fields out of the way of the
// object they ran the command for. Nothing decided which reader won; the machine
// simply got there first, and the person had been reading around the result ever
// since.
//
// Phase 15 resolved it in favour of owing humans readable output, and three tasks
// are what that cost. Task 15.3 gave `data` and `diff` real types, so the
// document reads without a second parse. Task 15.4 fixed key order, so it opens
// the way every other Kubernetes document a reader has met opens. This task, Task
// 15.5, makes the recorded object the only thing in it at full intensity. None of
// the three took anything away from the machine — the field names still mirror
// the frozen schema, and the plain rendering is byte for byte what it was — which
// is why the resolution was affordable at all.
//
// # The alternative that was not taken
//
// The other answer was a second document: leave the envelope machine-shaped and
// give `get` a human-oriented default format that prints the object with its
// provenance somewhere beside it. It is a real option, and it was rejected for
// one reason — it makes two renderings of one answer, and the second one then has
// to be kept true. Every field added to the envelope afterwards would need a
// decision about whether the human format shows it, and the rendering nobody
// scripts against is the one that quietly stops matching. A future
// reconsideration should start from these two options, not from this file.
//
// # Why colour rather than structure
//
// The proposal was to syntax-highlight the object. That means parsing the YAML
// and colouring by token: a second YAML renderer to keep correct, and a standing
// risk to the property that colour changes nothing but colour. Inverting it buys
// the same figure-ground separation for none of that — dim what surrounds the
// object, and the object is what is left.

// provenanceAroundObject paints every line of an Object envelope except the
// recorded object's own content in the provenance tier.
//
// # What it knows, and what it deliberately does not
//
// Two facts, and both are properties of the envelope this package emits rather
// than of YAML: an item's keys are written at indent 2, and `object` is the last
// of them (Task 15.4 fixed that order, and the goldens in testdata/envelope pin
// it). So a line at the envelope's own indentation decides which block follows
// it, and a line indented past every envelope key — or a blank one, which a
// literal block scalar inside a recorded value can produce — belongs to the block
// it is already inside.
//
// That is the whole model: two states and one key name. It does not parse, it
// does not tokenise, and it cannot tell a `spec` from an `annotations`. A change
// that needed it to understand the document any further than this would be the
// lift this approach exists to avoid, and the right response to that change is to
// stop and say so rather than to grow a parser here.
//
// The `object:` key line is painted with the wrapper rather than with the object.
// It is the envelope's field name, not the recorded state, and dimming it leaves
// the reader a signpost to stop at immediately before the payload lights up —
// which also means an empty `object: {}` needs no case of its own.
//
// # Why colour off returns early
//
// The uncoloured tier is the identity function, so the pass would be a no-op
// either way. Returning makes it a no-op by construction rather than by every
// line agreeing to be one: under --color=never, NO_COLOR and a redirected stdout
// these are the exact bytes the plain path wrote, which is what leaves golden
// files, `yq` and every redirect untouched.
func provenanceAroundObject(document string, severity Severity) string {
	if !severity.enabled {
		return document
	}

	lines := strings.Split(document, "\n")
	inObject := false
	for i, line := range lines {
		wrapper := true
		if line == "" || strings.HasPrefix(line, objectContentIndent) {
			// Deeper than any envelope key, so it belongs to whichever block is
			// open — the recorded object, or the coverage report above it.
			wrapper = !inObject
		} else {
			// At the envelope's own indentation, which is where the document says
			// what comes next. The key line itself is wrapper either way.
			inObject = isRecordedObjectKey(line)
		}
		if wrapper {
			lines[i] = severity.Provenance(line)
		}
	}
	return strings.Join(lines, "\n")
}

// The two spellings provenanceAroundObject reads the envelope's shape from.
//
// They are constants rather than literals in the pass above because they are a
// claim about what this package emits, and a claim is worth a name: the recorded
// state hangs off an item key at indent 2, so nothing inside it can appear at a
// shallower indent than four.
const (
	objectKeyLine       = "  object:"
	objectContentIndent = "    "
)

// isRecordedObjectKey reports whether a line is the item key the recorded state
// hangs off.
//
// Both spellings count. A state with fields is `  object:` with the document
// beneath it; a state with none is `  object: {}`, which the emitter writes
// inline, and reading only the first would leave the empty case in whichever
// block preceded it.
func isRecordedObjectKey(line string) bool {
	return line == objectKeyLine || strings.HasPrefix(line, objectKeyLine+" ")
}

// objectItem is the reconstruction as the envelope carries it.
//
// A nil state becomes an empty object rather than a JSON null, which is what
// marshalling a nil map produces and which would read as a recorded state that
// genuinely was null.
func objectItem(doc ObjectDocument) ObjectItem {
	state := doc.State
	if state == nil {
		state = map[string]any{}
	}
	return ObjectItem{
		At:             doc.At,
		UID:            doc.UID,
		BaseTS:         doc.BaseTS,
		BaseEvent:      doc.BaseEvent,
		PatchesApplied: doc.PatchesApplied,
		SHA256:         doc.SHA256,
		Object:         state,
	}
}

// ReconstructionOf is the provenance of a reconstruction as structured fields.
//
// It is the one place those values are derived from a document, and both
// renderings go through it: the marker the envelope's metadata carries, and — via
// ObjectProvenance below — the header a person reads. That is deliberate. Two
// independent readings of the same ObjectDocument would agree on the day they
// were written and drift the first time one of them was edited, and a header
// saying four hundred patches over a machine-readable field saying two is worse
// than either number alone.
func ReconstructionOf(doc ObjectDocument) ReconstructionReport {
	return ReconstructionReport{
		Reconstructed:  true,
		NotDeployable:  true,
		At:             doc.At,
		BaseTS:         doc.BaseTS,
		BaseEvent:      doc.BaseEvent,
		PatchesApplied: doc.PatchesApplied,
	}
}

// ObjectProvenance renders the mandatory header: what this document is, where it
// came from, and what it must not be used for.
//
// It is exported because the command writes it to standard error for JSON, and a
// second spelling of a warning is a warning that eventually only appears in one
// of the two formats. It is also the *only* definition of that wording: the
// machine-readable marker beside it carries fields rather than prose precisely so
// that this stays the single place the sentence lives.
//
// # Why the block recedes and one phrase in it does not
//
// A reader passes this block on every invocation, and by the third one they are
// reading past it to the document below — so it is provenance in the sense the
// tier means: facts that have to be available and do not have to be re-read.
// Every line of it is painted that way except one phrase, because one line in a
// block can be emphasised and two cannot: the second spends the first, and a
// block with two emphasised lines has none.
//
// The phrase that keeps it is NOT A DEPLOYABLE MANIFEST. It is the line that
// stops somebody piping this into `kubectl apply` — the misuse Phase 11 went as
// far as making the document structurally incapable of, rather than trusting
// words with it. The emphasis covers the phrase rather than the sentence around
// it, so what survives a skim is the warning and not its punctuation.
//
// severity is a parameter rather than a colour flag because the tiers are the
// vocabulary and the choice between them is what this function is deciding. With
// colour off every tier is the identity function, so the block is exactly the
// characters it always was.
func ObjectProvenance(doc ObjectDocument, severity Severity) string {
	// Read from the report rather than from doc, so the three facts the header
	// shares with the marker are literally the same values (see ReconstructionOf).
	reconstruction := ReconstructionOf(doc)

	fields := [][2]string{
		{"object", strings.TrimSpace(doc.Kind + " " + doc.Ref)},
		{"cluster", valueOrUnrecorded(doc.Cluster)},
		{"uid", valueOrUnrecorded(doc.UID)},
		{"at", FormatInstant(reconstruction.At)},
		{"base row", fmt.Sprintf("%s (%s)",
			FormatInstant(reconstruction.BaseTS), valueOrUnrecorded(reconstruction.BaseEvent))},
		{"patches applied", fmt.Sprintf("%d", reconstruction.PatchesApplied)},
		{"coverage", valueOrUnrecorded(doc.Coverage)},
	}

	width := 0
	for _, field := range fields {
		width = max(width, displayWidth(field[0]))
	}

	// Painted a line at a time, with the newline outside the escapes: a sequence
	// left open across a line break is one a pager, a partial copy or a terminal
	// with its own idea of line ends renders differently from this file.
	var built strings.Builder
	built.WriteString(severity.Provenance("# Reconstructed state — ") +
		severity.Emphasis(notDeployable) + severity.Provenance(".") + "\n")
	built.WriteString(severity.Provenance("#") + "\n")
	for _, field := range fields {
		built.WriteString(severity.Provenance("# "+pad(field[0]+":", width+1)+" "+field[1]) + "\n")
	}
	built.WriteString(severity.Provenance("#") + "\n")
	for _, line := range []string{
		"# This is what kuberecord recorded, not what the API server held. Do not",
		"# `kubectl apply -f` it: metadata.managedFields, metadata.resourceVersion and",
		"# metadata.generation were stripped at capture, and every field a redaction",
		"# policy covers carries the sentinel " + RedactionSentinel + " in place of its value.",
	} {
		built.WriteString(severity.Provenance(line) + "\n")
	}
	return built.String()
}

// valueOrUnrecorded renders an absent fact as the absence it is.
//
// A blank after a label reads as a value that is the empty string, and every one
// of these fields is either known or genuinely not in the history.
func valueOrUnrecorded(value string) string {
	if value == "" {
		return "not recorded"
	}
	return value
}
