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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"sigs.k8s.io/yaml"

	"github.com/kuberecord/kuberecord/internal/cli/buildinfo"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// VersionKind is the kind of the document `version` renders in a structured
// format.
//
// It carries render.EnvelopeAPIVersion, and it is deliberately not one of that
// package's envelope kinds: an envelope's metadata is the provenance of an
// *answer* — which cluster, which engine, what was watching — and this command
// answers no question about recorded history. A Version document carrying
// `cluster_id: ""` and an empty coverage report would be inviting a consumer to
// read three fields that could never mean anything.
//
// --check does not change that. It adds a `setup` block describing the
// configuration this build is pointed at, which is a subject of its own rather
// than the provenance of rows nobody asked for, and it is absent from the
// document entirely when nobody asked for it.
//
// What it does share is the contract those kinds are governed by: the same
// apiVersion, and therefore the same additive-only policy. Fields may be added
// here and must never be renamed, removed or repurposed within
// cli.kuberecord.io/v1alpha1 (D19).
const VersionKind = "Version"

// versionDocument is what `version` renders in JSON and YAML.
//
// The field names are camelCase rather than the envelope's snake_case, and the
// difference is not an inconsistency: the envelope's item fields are spelled the
// way the frozen schema spells its columns, because they are the same data
// reached two ways (docs/SCHEMA.md). Nothing here is schema data. It is this
// CLI's own document, like the configuration file, and it follows that document's
// spelling.
type versionDocument struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`

	// Version is the release this binary was built from — the string to compare
	// against a release page — or `(devel)` or `unknown`. See buildinfo.
	Version string `json:"version"`

	// Commit is the abbreviated revision, suffixed `-dirty` when the tree it was
	// built from had uncommitted changes.
	Commit string `json:"commit"`

	// BuildDate is when the binary was linked, RFC 3339 in UTC.
	BuildDate string `json:"buildDate"`

	// GoVersion and Platform describe the toolchain and the target. Platform is
	// read at runtime, so it describes the binary that is running rather than the
	// archive it was claimed to come from.
	GoVersion string `json:"goVersion"`
	Platform  string `json:"platform"`

	// Backends are the query backends compiled into this build.
	Backends []versionBackend `json:"backends"`

	// Setup is what --check found out about the configuration this build is
	// pointed at, and is absent when --check was not given.
	//
	// Absent rather than present-and-empty, which is the opposite of the call
	// `config resolve` makes about its own probe report, and the difference is
	// the subject of the two documents. That command is *about* resolution, so a
	// probe it did not perform is a withheld part of its own answer and is
	// reported as one. This document is about the build; the setup is a second
	// subject, and a bare `version` that carried an empty one would be claiming
	// to describe something it never looked at.
	Setup *versionSetup `json:"setup,omitempty"`
}

// versionBackend is one compiled-in backend, as the document reports it.
//
// It is this package's spelling of resolve.CompiledBackend rather than that type
// with tags on it, because the structured output is a public contract and the
// resolver's type is not: the day one of them needs a field the other does not
// want must not be the day a released JSON document changes shape.
type versionBackend struct {
	// Name is what a profile's `backend:` field and the --source/--sink chain
	// spell.
	Name string `json:"name"`

	// Engine is the read-plane engine that name opens, spelled exactly as
	// metadata.backend spells it in every other structured answer this CLI
	// produces — which is what makes the two comparable.
	Engine string `json:"engine"`

	// Description is what that backend reads, named by its frozen format.
	Description string `json:"description"`
}

// versionSetup is the summary `version --check` adds: what the resolution chains
// chose, and whether the thing they chose answered.
//
// It is a summary and not a second copy of `config resolve`'s report. The two
// commands ask the same question of the same machinery and differ only in how
// much of the walk they show — this one reports the four facts somebody opening a
// bug report needs, and names the command that reports the nine steps behind them.
//
// Every field but Check is omitempty, because each one is a fact a chain either
// produced or did not, and an empty string for "the backend chain failed" would
// be a value a consumer had to interpret rather than one it could read. Check is
// always present for the reason it is always present in a Resolution: a script
// branching on the outcome should read a word rather than infer one from a
// missing key.
type versionSetup struct {
	// Backend names the resolved backend the way the resolution notice does, with
	// no credential in it at any verbosity. Absent when the chain resolved nothing.
	Backend string `json:"backend,omitempty"`

	// Engine is the read-plane engine behind it, spelled exactly as
	// metadata.backend spells it in every other structured answer — which is what
	// makes this comparable with the `engine` column above and with an answer
	// already in hand.
	Engine string `json:"engine,omitempty"`

	// BackendError is why the backend chain produced nothing. Present only when
	// it did not.
	BackendError string `json:"backendError,omitempty"`

	// ClusterID is the resolved kuberecord cluster identity (D21) — the
	// cluster_id column of the frozen schema, not a kubeconfig entry.
	ClusterID string `json:"clusterID,omitempty"`

	// ClusterIDSource says how it was arrived at, in the words the notice uses.
	ClusterIDSource string `json:"clusterIDSource,omitempty"`

	// ClusterIDError is why the identity chain produced nothing. Present only
	// when it failed.
	//
	// Under --check the chain's last step is always taken, so an identity that is
	// neither resolved nor failed cannot happen here — which is exactly why
	// --check is the flag that answers "is my setup working".
	ClusterIDError string `json:"clusterIDError,omitempty"`

	// Check is what asking the backend produced, in the vocabulary and under the
	// field names `config resolve` reports it with. It is the same type because
	// it is the same probe: a runbook that reads `.check.outcome` must not have
	// to know which of the two commands produced the document.
	Check resolutionCheck `json:"check"`
}

// newVersionCommand reports which build is running, what it can read, and — when
// asked — whether the setup it is pointed at works.
//
// By itself it opens nothing. That is the point of it: the reason somebody types
// `version` is usually that something else did not work, so a version command
// that needed a kubeconfig, a sink or a network would be unavailable in most of
// the situations it exists for.
//
// # Why the probe is a flag rather than an automatic extension
//
// Because the property above is worth more than the convenience. A `version` that
// reached for a backend whenever one happened to resolve would spend a dial
// timeout for anybody whose backend is unreachable — which is most of the people
// typing it — and the first command a user is told to run when nothing works
// would be the second-slowest thing they do. --check keeps the bare command
// instant and offline, and makes asking the question an explicit act.
//
// There is deliberately no `--version` flag beside it. kubectl, which this binary
// plugs into, has none either, and cobra's built-in one is handled before any
// command runs — so it could not honour -o, and `kuberecord --version -o json`
// would print a table while appearing to have been asked for JSON. One spelling
// that respects every flag beats two that disagree about one.
func newVersionCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	var check bool

	command := &cobra.Command{
		Use:   "version",
		Short: "Show the build and the backends compiled into it",
		Long: `Show the build and the backends compiled into it.

The version, the commit and the build date are stamped into the binary when it
is released, so they identify the artifact rather than the source tree it
resembles. A build made any other way reports what the Go toolchain recorded
about it — a module version for ` + "`go install`" + `, the revision for a build from
a checkout — and says ` + "`" + buildinfo.Unknown + "`" + ` where nothing could say.

The backend list is what this build can read, not what the project supports. It
is the first thing to check when a --source or a profile is refused.

By itself it contacts nothing: no cluster, no sink, no network.

--check is the exception, and it is the first thing to run when something is not
working. It resolves the backend exactly as a query command would, asks it
whether it answers, and reports the sink, the cluster identity and what came
back. That is a summary: ` + "`config resolve --check`" + ` puts the same question
through the same machinery and reports which step of the chains decided each of
those facts.`,
		Example: `  # Which build is this, and what can it read?
  kuberecord version

  # Is this setup working? The first thing to run when something is not.
  kuberecord version --check

  # For a script, or for a bug report.
  kuberecord version -o json`,

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return nil
			}
			return exit.UsageErrorf("%s takes no arguments, and %q is not one: it reports the build "+
				"that is running, which is one fact with nothing to narrow", cmd.Name(), args[0])
		},

		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			// Before anything is resolved, so that a format this command cannot
			// render is refused instantly rather than after a dial timeout.
			if formatErr := versionFormat(flags.Output); formatErr != nil {
				return formatErr
			}
			request := VersionRequest{Check: check, Output: flags.Output, InvokedAs: invokedAs}

			if !check {
				return RunVersion(cmd.Context(), buildinfo.Get(), nil, request, streams)
			}

			resolver, err := resolve.NewBackendResolver(flags, streams, invokedAs)
			if err != nil {
				return err
			}
			inspection := resolver.Inspect(cmd.Context(), true)
			if inspection.Backend != nil {
				defer func() {
					// Joined rather than replacing: the reason the command is
					// ending is whatever the chains found, and the tidying up must
					// not hide it.
					err = errors.Join(err, inspection.Backend.Close())
				}()
			}
			return RunVersion(cmd.Context(), buildinfo.Get(), inspection, request, streams)
		},
	}

	command.Flags().BoolVar(&check, options.FlagCheck, check,
		"Also resolve the backend, ask it whether it answers, and report the sink, the cluster "+
			"identity and what came back. Without it nothing is contacted, which is what keeps "+
			"the bare command usable when everything else has failed.")

	return command
}

// VersionRequest is one `version` invocation, resolved.
//
// It is separate from the resolve.Inspection RunVersion takes beside it because
// the two answer different questions: this is what the user asked for, and the
// inspection is what the chains found. The command builds an inspection only
// under --check, so the two always travel together — but a nil inspection is
// handled rather than assumed away, since the alternative is a nil dereference in
// the command that exists to work when nothing else does.
type VersionRequest struct {
	// Check asks the backend whether it can be reached.
	Check bool

	// Output is the rendering, already validated by versionFormat.
	Output options.OutputFormat

	// InvokedAs is how this process was invoked, so that the line pointing at
	// `config resolve` names a command the reader can actually type.
	InvokedAs string
}

// RunVersion writes the version document and returns what --check found wrong.
//
// It is exported, and takes an already-walked resolve.Inspection, for the reason
// RunResolve does: the whole of the command's behaviour — what the setup block
// says, which failure it exits with, how an unreachable backend reads — is then
// reachable from a test holding fixtures and a fixed build identity, rather than
// only from one that can reach a kubeconfig and a live sink.
//
// The probe and the failure come from `config resolve`'s own functions rather
// than from copies of them. That is the whole of Task 14.2's "one code path"
// requirement: the two commands differ in how much of the walk they render and in
// nothing else, so a change to what counts as reachable, or to which chain's
// failure a user is told about first, cannot reach one command without reaching
// the other.
//
// The document goes to stdout because it is this command's data. The failure is
// returned rather than printed, so that an unreachable backend produces the same
// message, with the same exit code and the same Task 13.1 explanation beneath it,
// that a query command would have produced from the same configuration.
func RunVersion(
	ctx context.Context, build buildinfo.Info, inspection *resolve.Inspection,
	request VersionRequest, streams genericiooptions.IOStreams,
) error {
	document := versionOf(build)
	if inspection == nil {
		return writeVersion(streams.Out, document, request.Output, "")
	}

	check, checkErr := checkReachability(ctx, inspection, request.Check)
	document.Setup = versionSetupOf(inspection, check)
	failure := resolutionFailure(inspection, checkErr)

	// The pointer to `config resolve` is printed only when this invocation is
	// about to fail, and only in the human form. On a working setup it would be
	// advice nobody needs; in a serialization it would be prose in a document a
	// parser reads. Where it does appear it is the answer to the question the
	// summary provokes — which of the nine steps decided this? — asked at the one
	// moment somebody wants it.
	advice := ""
	if failure != nil {
		advice = versionAdvice(request.InvokedAs)
	}
	if err := writeVersion(streams.Out, document, request.Output, advice); err != nil {
		return err
	}
	return failure
}

// versionSetupOf summarizes an inspection for the document.
//
// Every value is read off the Inspection rather than re-derived, so the summary
// cannot describe a chain the resolver did not walk.
func versionSetupOf(inspection *resolve.Inspection, check resolutionCheck) *versionSetup {
	setup := &versionSetup{
		BackendError:    errorText(inspection.BackendErr),
		ClusterID:       inspection.ClusterID,
		ClusterIDSource: inspection.ClusterIDSource,
		ClusterIDError:  errorText(inspection.ClusterIDErr),
		Check:           check,
	}
	if inspection.Backend != nil {
		setup.Backend = inspection.Backend.Description
		setup.Engine = inspection.Backend.Engine.Capabilities().Backend
	}
	return setup
}

// versionAdvice is the one line that sends a reader from this summary to the
// steps behind it.
func versionAdvice(invokedAs string) string {
	if invokedAs == "" {
		invokedAs = options.StandaloneName
	}
	return fmt.Sprintf("for which step decided what, run `%s config resolve --%s`",
		invokedAs, options.FlagCheck)
}

// versionFormat refuses, by name, the two formats this document has no shape for.
//
// It is a check of its own rather than the default arm of writeVersion's switch
// because --check dials: a user who asked for a rendering this command cannot
// produce must be told so before a round trip, not after one. writeVersion
// switches on the three that survive, and states its unreachable default rather
// than ignoring it, exactly as writeResolution does.
//
// `jsonl` and `diff` are the two refused. The first is a streaming format for a
// result larger than memory and this document is one item; the second is a
// rendering of change operations and there are none here. Rendering something
// else regardless would leave a user wondering why their flag did nothing.
func versionFormat(format options.OutputFormat) error {
	switch format {
	case options.OutputTable, options.OutputWide, options.OutputJSON, options.OutputYAML:
		return nil
	}
	return exit.UsageErrorf("version renders %s, %s or %s, not %s",
		options.OutputTable, options.OutputJSON, options.OutputYAML, format)
}

// versionOf assembles the document from the build's own identity and the list of
// backends the resolver can open.
//
// The backends come from resolve.CompiledBackends rather than from a list here,
// because the resolver is the only thing that opens anything: a second list in
// the command package would agree with it until the day a backend was added, and
// then it would be a version command reporting a capability the binary does not
// have.
func versionOf(build buildinfo.Info) versionDocument {
	compiled := resolve.CompiledBackends()
	backends := make([]versionBackend, 0, len(compiled))
	for _, backend := range compiled {
		backends = append(backends, versionBackend{
			Name:        string(backend.Kind),
			Engine:      backend.Engine,
			Description: backend.Description,
		})
	}

	return versionDocument{
		APIVersion: render.EnvelopeAPIVersion,
		Kind:       VersionKind,
		Version:    build.Version,
		Commit:     build.Commit,
		BuildDate:  build.BuildDate,
		GoVersion:  build.GoVersion,
		Platform:   build.Platform,
		Backends:   backends,
	}
}

// writeVersion renders the document in the requested format.
//
// `table` and `wide` render the human form, because `table` is the global default
// rather than a choice this command was given. advice is the human form's closing
// line and is dropped by the two serializations, which carry the same finding as
// the fields a parser reads.
func writeVersion(
	out io.Writer, doc versionDocument, format options.OutputFormat, advice string,
) error {
	switch format {
	case options.OutputTable, options.OutputWide:
		return options.WriteAll(out, renderVersion(doc, advice))

	case options.OutputJSON:
		encoded, err := json.MarshalIndent(doc, "", "  ")
		if err != nil {
			return exit.RuntimeErrorf("encoding the version: %w", err)
		}
		return options.WriteAll(out, string(encoded)+"\n")

	case options.OutputYAML:
		// Through the JSON tags, so the two serializations are one document in
		// two syntaxes rather than two documents that resemble each other.
		encoded, err := yaml.Marshal(doc)
		if err != nil {
			return exit.RuntimeErrorf("encoding the version: %w", err)
		}
		return options.WriteAll(out, string(encoded))
	}
	// Unreachable through versionFormat, which accepts four spellings of three
	// renderings and refuses the rest by name. Stated rather than ignored,
	// because the alternative to a stated error here is a command that prints
	// nothing and exits zero.
	return exit.RuntimeErrorf("version cannot render %q", format)
}

// renderVersion is the human form.
//
// The backend rows are padded to the widest entry rather than to a constant, so
// that adding a backend with a longer name cannot silently produce a ragged
// column nobody notices in review.
//
// Nothing here colourises and nothing wraps, which is the same decision
// renderResolution makes and for the same reason: the long lines in the setup
// block are addresses and error messages, and a renderer that folded them at a
// terminal width would break the one thing a reader does with them, which is copy
// them into a bug report.
func renderVersion(doc versionDocument, advice string) string {
	var page strings.Builder

	fmt.Fprintf(&page, "%s %s\n", options.StandaloneName, doc.Version)
	fmt.Fprintf(&page, "  commit  %s\n", doc.Commit)
	fmt.Fprintf(&page, "  built   %s\n", doc.BuildDate)
	fmt.Fprintf(&page, "  go      %s %s\n", doc.GoVersion, doc.Platform)

	if len(doc.Backends) == 0 {
		// Not reachable from a build of this repository, and printed rather than
		// omitted so that if it ever is, it reads as the finding it would be
		// rather than as a section somebody forgot to write.
		page.WriteString("\nno query backends are compiled into this build\n")
	} else {
		nameWidth, engineWidth := 0, 0
		for _, backend := range doc.Backends {
			nameWidth = max(nameWidth, len(backend.Name))
			engineWidth = max(engineWidth, len(backend.Engine))
		}

		page.WriteString("\nquery backends compiled in:\n")
		for _, backend := range doc.Backends {
			fmt.Fprintf(&page, "  %-*s  engine %-*s — %s\n",
				nameWidth, backend.Name, engineWidth, backend.Engine, backend.Description)
		}
	}

	if doc.Setup != nil {
		page.WriteString(renderSetup(*doc.Setup))
	}
	if advice != "" {
		page.WriteString("\n" + advice + "\n")
	}
	return page.String()
}

// renderSetup is the --check block.
//
// The gutter is measured across the block's own rows rather than shared with the
// backend table above it, because the two are read for different things: that one
// is a list being scanned down a column, and this one is four facts about a single
// configuration. Padding them together would widen the table to fit an address.
func renderSetup(setup versionSetup) string {
	rows := setupRows(setup)

	width := 0
	for _, row := range rows {
		width = max(width, len(row.label))
	}

	var block strings.Builder
	block.WriteString("\nsetup:\n")
	for _, row := range rows {
		line := "  " + padRight(row.label, width)
		if row.text != "" {
			line += "  " + row.text
		}
		block.WriteString(strings.TrimRight(line, " ") + "\n")
	}
	return block.String()
}

// setupRows lays the summary out as lines.
//
// It reuses resolutionRow's label-and-text form — the shape that type documents
// for a summary row, as opposed to the outcome-and-detail columns a step is
// rendered in — so that the two reports of one probe are laid out by one piece of
// code rather than by two that agree today.
//
// The last row's label is the outcome word itself: `reachable`, `unreachable`,
// `cannot be checked`. That is the vocabulary `config resolve`'s reachability
// block uses, and a reader who has seen one report can read the other without
// learning a second set of words.
func setupRows(setup versionSetup) []resolutionRow {
	var rows []resolutionRow

	if setup.Backend != "" {
		rows = append(rows,
			resolutionRow{label: "backend", text: setup.Backend},
			resolutionRow{label: "engine", text: setup.Engine})
	} else {
		rows = append(rows, resolutionRow{label: "backend", text: "unresolved"})
		if setup.BackendError != "" {
			rows = append(rows, resolutionRow{text: setup.BackendError})
		}
	}

	if setup.ClusterID != "" {
		rows = append(rows, resolutionRow{
			label: "cluster-id",
			text:  fmt.Sprintf("%s (%s)", setup.ClusterID, setup.ClusterIDSource),
		})
	} else {
		rows = append(rows, resolutionRow{label: "cluster-id", text: "undetermined"})
		if setup.ClusterIDError != "" {
			rows = append(rows, resolutionRow{text: setup.ClusterIDError})
		}
	}

	rows = append(rows, resolutionRow{label: setup.Check.Outcome, text: setup.Check.Detail})
	if setup.Check.Error != "" {
		rows = append(rows, resolutionRow{text: setup.Check.Error})
	}
	return rows
}
