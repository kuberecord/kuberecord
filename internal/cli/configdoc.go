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

package cli

import (
	"encoding/json"
	"io"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// What the `config` subcommands report to a program.
//
// The two kinds below are the four writing subcommands' own. What the file also
// holds is the format vocabulary and the encoder every `config` document is
// rendered through — including `get-profiles`, which writes nothing and whose
// document lives in getprofilescmd.go beside the command that produces it.
//
// # Why they report anything
//
// Because writing configuration is a step in a script, and until this file
// existed the only account of what a write had done was an arrow-prefixed
// sentence on stderr. A CI job that creates a profile from a sink and then wants
// to know whether it displaced one had to diff the file around the command. The
// `config` surface already renders documents where it reads — `view` and
// `resolve` — and there is no reason a write should be the half of it that a
// program cannot read.
//
// # Why two kinds and not one
//
// Because a single kind covering both subjects would carry `context` and
// `clusterID` on a document about a profile, and a profile stanza on a document
// about a kubeconfig context. That is the mistake VersionKind's doc comment
// refuses in as many words: a field that could never mean anything is a field a
// consumer is invited to read. Three of the four subcommands change a profile and
// say so under one kind with an `action`; the fourth changes a context mapping,
// which is a different subject, and gets its own.
//
// # The contract
//
// Both carry render.EnvelopeAPIVersion and neither is an envelope: no `metadata`,
// no `items`, because no question about recorded history was asked. What they do
// share with the five envelope kinds is the policy that version is governed by —
// fields may be added and must never be renamed, removed or repurposed within
// cli.kuberecord.io/v1alpha1 (D19). The field names are this CLI's own camelCase,
// as `version` and `config resolve` are and for the same reason: the envelope's
// item fields are spelled the way the frozen schema spells its columns because
// they are the same data reached two ways, and nothing here is schema data.
const (
	// ProfileChangeKind is what `set-profile`, `delete-profile` and `use-profile`
	// render.
	ProfileChangeKind = "ProfileChange"

	// ContextMappingKind is what `set-context-cluster-id` renders.
	ContextMappingKind = "ContextMapping"
)

// profileAction is what happened to the profile a ProfileChange names.
//
// Four values rather than three, because `use-profile` writes the same file about
// the same subject and a script that has to branch on which command it ran has
// been told less than the document could have told it. They are spelled as past
// participles because the document describes a write that has already landed:
// nothing here is a request.
type profileAction string

const (
	// profileCreated is a name that was not in the file.
	profileCreated profileAction = "created"

	// profileUpdated is a name that was, and whose stanza this write replaced
	// whole. See profileWrite.
	profileUpdated profileAction = "updated"

	// profileDeleted is a stanza removed from the file.
	profileDeleted profileAction = "deleted"

	// profileActivated is the active pointer moved to an existing profile,
	// changing no stanza.
	profileActivated profileAction = "activated"
)

// profileChangeDocument is what the three profile subcommands render in JSON and
// YAML.
type profileChangeDocument struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`

	// Action is what happened. It is the field a script branches on, because
	// which of the two stanzas below is present depends on it.
	Action profileAction `json:"action"`

	// Name is the profile the command acted on, and Path the file it acted in.
	// The path is here as well as on stderr so that a program collecting these
	// documents from several machines can tell them apart.
	Name string `json:"name"`
	Path string `json:"path"`

	// Profile is the stanza this name now carries. Absent for a deletion, where
	// there is none — rather than present and empty, which would describe a
	// profile with no backend that the file would itself refuse.
	Profile *resolve.Profile `json:"profile,omitempty"`

	// Previous is the stanza this write displaced: the one that was replaced by
	// an update, or removed by a deletion. Absent when nothing was displaced.
	//
	// It is the field this document was worth adding for. An update and a
	// deletion are the two operations that destroy something, and the thing they
	// destroyed is unrecoverable from the file afterwards — so a script that wants
	// to be able to put it back, or a reviewer reading a job's log, needs it
	// reported at the moment it stops existing.
	Previous *resolve.Profile `json:"previous,omitempty"`

	// CurrentProfile is the active pointer after the write, and "" when no
	// profile is active.
	//
	// Always present, including when it is empty, for the reason `config
	// resolve` always renders its probe report: an empty active pointer is an
	// ordinary state that a consumer must be able to read rather than infer from
	// a missing key. `delete-profile --force` produces exactly that.
	CurrentProfile string `json:"currentProfile"`
}

// contextMappingDocument is what `set-context-cluster-id` renders.
type contextMappingDocument struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`

	// Context is the kubeconfig context, and ClusterID the kuberecord cluster
	// identity it now reads. D21 is why these are two fields and not one.
	Context   string `json:"context"`
	ClusterID string `json:"clusterID"`

	// PreviousClusterID is the identity this context mapped to before, absent
	// when it mapped to nothing.
	//
	// Absent is unambiguous: the file refuses a context mapped to an empty
	// identity (Config.Validate), so there is no state this field could report as
	// "" that is not simply "there was no mapping".
	PreviousClusterID string `json:"previousClusterID,omitempty"`

	// Path is the file that was written.
	Path string `json:"path"`
}

// configFormat decides which rendering an invocation of a `config` subcommand
// asked for. An empty StructuredFormat means that subcommand's own human form —
// the confirmation on stderr for the four that write, the table for
// `get-profiles`.
//
// It is named for the subtree rather than for a write because the vocabulary is a
// property of the subtree: every one of these subcommands renders a single
// document about a configuration file, so the set of formats that can carry one
// is the same question for the reading members as for the writing ones. A second
// decider for the reader would be a second answer to it.
//
// `jsonl` and `diff` are refused by name, exactly as `config resolve` and
// `version` refuse them: the first is a streaming format for a result larger than
// memory and each of these is one document, the second renders change operations
// and neither a configuration write nor a profile listing has any. Rendering
// something else regardless would leave a user wondering why their flag did
// nothing (D31).
//
// On the writing subcommands it is called before anything is read from disk, so
// that a format nobody can render is refused rather than reported after the file
// has already been rewritten. That ordering is the same one writeProfile applies
// to validation and for the same reason.
func configFormat(subcommand string, format options.OutputFormat) (render.StructuredFormat, error) {
	switch format {
	case options.OutputTable, options.OutputWide:
		// `wide` means the same table with nothing elided, and there is no table
		// here at all: the whole report is the confirmation on stderr. Accepting it
		// rather than refusing it is that guarantee honoured — nothing is elided at
		// any width — and it is what a user who typed no --output at all arrives
		// with.
		return "", nil
	case options.OutputJSON:
		return render.StructuredJSON, nil
	case options.OutputYAML:
		return render.StructuredYAML, nil
	}
	return "", exit.UsageErrorf("config %s renders %s, %s or %s, not %s",
		subcommand, options.OutputTable, options.OutputJSON, options.OutputYAML, format)
}

// writeConfigDocument renders one `config` document in a structured format, or
// nothing for the human form.
//
// subject names the document in a failure, because a bare "encoding" error leaves
// the reader without the one fact that makes it actionable.
//
// The human form writes nothing here at all, and for a write that is not a silent
// no-op: the command has already said what it did on stderr, in the sentence a
// person reads, and stdout carries a command's data — the data of a write being
// the file it wrote. A subcommand whose human form *is* a document on stdout
// renders it before reaching this function and never asks it for the empty
// format; see writeProfiles.
func writeConfigDocument(out io.Writer, document any, format render.StructuredFormat, subject string) error {
	switch format {
	case "":
		return nil

	case render.StructuredJSON:
		encoded, err := json.MarshalIndent(document, "", "  ")
		if err != nil {
			return exit.RuntimeErrorf("encoding the %s: %w", subject, err)
		}
		return options.WriteAll(out, string(encoded)+"\n")

	case render.StructuredYAML:
		// Through the JSON tags and through render.YAMLDocument, so the two
		// serializations are one document in two syntaxes and `kind` stays where a
		// reader of any Kubernetes document expects it. See that file.
		encoded, err := render.YAMLDocument(document)
		if err != nil {
			return exit.RuntimeErrorf("encoding the %s: %w", subject, err)
		}
		return options.WriteAll(out, encoded)
	}
	// Unreachable through configFormat, which accepts three formats and
	// refuses the rest by name. Stated rather than ignored, because the
	// alternative to a stated error here is a command that writes a file, prints
	// nothing, and exits zero.
	return exit.RuntimeErrorf("config cannot render the %s as %q", subject, format)
}

// writeProfileChange renders a ProfileChange for one write.
//
// The stanzas are taken by pointer from the caller's own locals rather than
// copied out of the configuration, because the caller is the only thing that
// knows which of them survived the write it just performed.
func writeProfileChange(
	out io.Writer, format render.StructuredFormat, change profileChangeDocument,
) error {
	change.APIVersion = render.EnvelopeAPIVersion
	change.Kind = ProfileChangeKind
	return writeConfigDocument(out, change, format, "profile change")
}

// writeContextMapping renders a ContextMapping for one write.
func writeContextMapping(
	out io.Writer, format render.StructuredFormat, mapping contextMappingDocument,
) error {
	mapping.APIVersion = render.EnvelopeAPIVersion
	mapping.Kind = ContextMappingKind
	return writeConfigDocument(out, mapping, format, "context mapping")
}
