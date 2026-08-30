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
func WriteObject(
	out, errOut io.Writer, doc ObjectDocument, head EnvelopeHead,
	format StructuredFormat, opts Options,
) error {
	provenance := ObjectProvenance(doc)

	if out != nil {
		if format == StructuredYAML {
			if _, err := io.WriteString(out, provenance); err != nil {
				return fmt.Errorf("writing the reconstructed object's header: %w", err)
			}
		}
		if err := writeObjectEnvelope(out, doc, head, format); err != nil {
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

// writeObjectEnvelope writes the one-item envelope a reconstruction is.
//
// It goes through the same Stream every other structured answer does, so that a
// reconstruction and a timeline are the same document shape in the same
// serializations — including `jsonl`, where an answer of exactly one item is a
// head line and one item line rather than a special case.
func writeObjectEnvelope(
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

// ObjectProvenance renders the mandatory header: what this document is, where it
// came from, and what it must not be used for.
//
// It is exported because the command writes it to standard error for JSON, and a
// second spelling of a warning is a warning that eventually only appears in one
// of the two formats.
func ObjectProvenance(doc ObjectDocument) string {
	fields := [][2]string{
		{"object", strings.TrimSpace(doc.Kind + " " + doc.Ref)},
		{"cluster", valueOrUnrecorded(doc.Cluster)},
		{"uid", valueOrUnrecorded(doc.UID)},
		{"at", FormatInstant(doc.At)},
		{"base row", fmt.Sprintf("%s (%s)", FormatInstant(doc.BaseTS), valueOrUnrecorded(doc.BaseEvent))},
		{"patches applied", fmt.Sprintf("%d", doc.PatchesApplied)},
		{"coverage", valueOrUnrecorded(doc.Coverage)},
	}

	width := 0
	for _, field := range fields {
		width = max(width, displayWidth(field[0]))
	}

	var built strings.Builder
	built.WriteString("# Reconstructed state — " + notDeployable + ".\n#\n")
	for _, field := range fields {
		built.WriteString("# " + pad(field[0]+":", width+1) + " " + field[1] + "\n")
	}
	built.WriteString("#\n")
	built.WriteString("# This is what kuberecord recorded, not what the API server held. Do not\n")
	built.WriteString("# `kubectl apply -f` it: metadata.managedFields, metadata.resourceVersion and\n")
	built.WriteString("# metadata.generation were stripped at capture, and every field a redaction\n")
	built.WriteString("# policy covers carries the sentinel " + RedactionSentinel + " in place of its value.\n")
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
