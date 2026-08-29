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

	"sigs.k8s.io/yaml"
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

// ObjectFormat is the serialization `get` was asked for.
//
// It is this package's own vocabulary rather than the command's OutputFormat,
// because the renderer must not depend on the flag surface: these are the two
// serializations a reconstructed object has, and the set of formats a flag
// accepts is a different question that changes for different reasons.
type ObjectFormat string

// The serializations a reconstructed object is written in.
const (
	// ObjectYAML carries the provenance header as comments, above the document.
	ObjectYAML ObjectFormat = "yaml"
	// ObjectJSON carries it on standard error, because JSON has no comments and
	// an invented field would corrupt the document a verification hashes.
	ObjectJSON ObjectFormat = "json"
)

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
	// State is the reconstructed object.
	State map[string]any
	// Notices are written to standard error, in order.
	Notices []Notice
}

// WriteObject writes the document to out in format, and its notices to errOut.
func WriteObject(out, errOut io.Writer, doc ObjectDocument, format ObjectFormat, opts Options) error {
	body, err := encodeObject(doc.State, format)
	if err != nil {
		return err
	}

	provenance := ObjectProvenance(doc)
	if out != nil {
		document := body
		if format == ObjectYAML {
			document = provenance + body
		}
		if _, writeErr := io.WriteString(out, document); writeErr != nil {
			return fmt.Errorf("writing the reconstructed object: %w", writeErr)
		}
	}

	if errOut == nil {
		return nil
	}
	warning := ""
	if format == ObjectJSON {
		// JSON has no comment syntax, and inventing a field to hold this would
		// change the document a --verify hashes. The block goes to the stream
		// every other qualification of every other command already goes to.
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

// encodeObject serializes the state.
//
// YAML goes through sigs.k8s.io/yaml, which marshals via JSON and therefore emits
// the same key ordering and the same scalar spellings the JSON form does. That
// agreement is worth having: a reader comparing the two formats of one
// reconstruction should see one document in two syntaxes, not two documents.
func encodeObject(state map[string]any, format ObjectFormat) (string, error) {
	if state == nil {
		// An empty document rather than "null", which is what marshalling a nil
		// map produces and which reads as a recorded state that was the JSON
		// null.
		state = map[string]any{}
	}
	switch format {
	case ObjectJSON:
		encoded, err := json.MarshalIndent(state, "", "  ")
		if err != nil {
			return "", fmt.Errorf("encoding the reconstructed object as JSON: %w", err)
		}
		return string(encoded) + "\n", nil
	case ObjectYAML:
		encoded, err := yaml.Marshal(state)
		if err != nil {
			return "", fmt.Errorf("encoding the reconstructed object as YAML: %w", err)
		}
		return string(encoded), nil
	}
	return "", fmt.Errorf("%q is not a serialization for a reconstructed object", format)
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
