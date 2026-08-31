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
// asks no backend anything. A Version document carrying `cluster_id: ""` and an
// empty coverage report would be inviting a consumer to read three fields that
// could never mean anything.
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

// newVersionCommand reports which build is running and what it can read.
//
// It opens nothing. That is the point of it: the reason somebody types `version`
// is usually that something else did not work, so a version command that needed a
// kubeconfig, a sink or a network would be unavailable in most of the situations
// it exists for.
//
// There is deliberately no `--version` flag beside it. kubectl, which this binary
// plugs into, has none either, and cobra's built-in one is handled before any
// command runs — so it could not honour -o, and `kuberecord --version -o json`
// would print a table while appearing to have been asked for JSON. One spelling
// that respects every flag beats two that disagree about one.
func newVersionCommand(flags *options.GlobalFlags, streams genericiooptions.IOStreams) *cobra.Command {
	return &cobra.Command{
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

It contacts nothing: no cluster, no sink, no network.`,
		Example: `  # Which build is this, and what can it read?
  kuberecord version

  # For a script, or for a bug report.
  kuberecord version -o json`,

		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return nil
			}
			return exit.UsageErrorf("%s takes no arguments, and %q is not one: it reports the build "+
				"that is running, which is one fact with nothing to narrow", cmd.Name(), args[0])
		},

		RunE: func(_ *cobra.Command, _ []string) error {
			return writeVersion(streams.Out, versionOf(buildinfo.Get()), flags.Output)
		},
	}
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
// rather than a choice this command was given. `jsonl` and `diff` are refused by
// name: the first is a streaming format for a result larger than memory and this
// document is one item, the second is a rendering of change operations and there
// are none here. Refusing them is the same call `config view` makes — rendering
// something else regardless would leave a user wondering why their flag did
// nothing.
func writeVersion(out io.Writer, doc versionDocument, format options.OutputFormat) error {
	switch format {
	case options.OutputTable, options.OutputWide:
		return options.WriteAll(out, renderVersion(doc))

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
	return exit.UsageErrorf("version renders %s, %s or %s, not %s",
		options.OutputTable, options.OutputJSON, options.OutputYAML, format)
}

// renderVersion is the human form.
//
// The backend rows are padded to the widest entry rather than to a constant, so
// that adding a backend with a longer name cannot silently produce a ragged
// column nobody notices in review.
func renderVersion(doc versionDocument) string {
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
		return page.String()
	}

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
	return page.String()
}
