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
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// `config get-profiles` is the file's *state*, where `config view` is its
// contents.
//
// The two answer different questions and kubectl splits them the same way —
// `config view` against `config get-contexts`. A dump of the file says what was
// written down. It cannot say which profile is active without being read
// carefully, it cannot say what each one points at without the reader assembling
// an address out of three fields, and it structurally cannot say whether any of
// them is usable right now.
//
// # The column that earns the command
//
// CREDENTIAL, because it is the only one that is not in the file. A profile
// stores the *name* of an environment variable or the *path* of a file, and
// whether that reference resolves is a fact about the shell and the disk this
// command is running on. That is the failure Task 17.4 wrote a page of prose
// about, and this is the same diagnosis in a form a reader takes in at a glance:
// three profiles, one of them with its variable exported.
//
// It is why this is a diagnostic rather than an inventory, and it is why the
// resolution is decided by resolve.Profile.Credential — which calls the same
// ResolvePassword every query resolves a password through, so a column saying
// `set` cannot disagree with what the next query finds.
//
// What it cannot report is *whose* password, and that is why TARGET names the
// ClickHouse principal (Task 18.5). A file whose profiles read as four different
// users through one variable name rendered as four rows saying
// `env KUBERECORD_CLICKHOUSE_PASSWORD (not set)` and nothing to tell them apart —
// so the user joined the locator, in the spelling a connection string uses, rather
// than becoming a sixth column that only one backend could fill.
//
// # What it will not do
//
// Contact anything. It reads the configuration file and the environment, and
// dials no backend even to say whether one answers: reachability is
// `config resolve --check`, and a second command that dialled would be two
// answers to one question that could differ. TestGetProfilesContactsNothing
// pins it.
//
// It also does not consult --profile, and that is not an oversight. CURRENT is
// the file's active pointer, as the `*` in `kubectl config get-contexts` is the
// file's current context; what *this invocation* would resolve to is a different
// question with nine steps behind it, and D26 gives that question to
// `config resolve`. An empty CURRENT column needs no sentence for the same
// reason: no marker means no active profile, which is the convention the layout
// is borrowed from, and what the chain then falls through to is again
// `config resolve`'s answer to give.
//
// # Why the table prints an address a completion menu withholds
//
// completeProfileNames deliberately describes a profile by its backend alone,
// because a menu appears unbidden while somebody is typing, possibly over their
// shoulder. This table was asked for by name, and it is the same information
// `config view` prints unredacted. Neither prints a credential, and neither can:
// the file cannot hold one (resolve.ClickHouseProfile.Password).

// ProfilesKind is the kind of the document `config get-profiles` renders in a
// structured format.
//
// It carries render.EnvelopeAPIVersion and is deliberately not one of that
// package's envelope kinds, for the reason VersionKind and ResolutionKind are
// not: an envelope's metadata is the provenance of an *answer* — which cluster,
// which engine, what was watching — and this command asks no backend anything. A
// Profiles document carrying `cluster_id: ""` and an empty coverage report would
// invite a consumer to read three fields that could never mean anything.
//
// What it does share is the contract those kinds are governed by: the same
// apiVersion, and therefore the same additive-only policy. Fields may be added
// here and must never be renamed, removed or repurposed within
// cli.kuberecord.io/v1alpha1 (D19).
const ProfilesKind = "Profiles"

// The column headings, and the marker under the first of them.
//
// Constants for the reason the timeline's and the scope table's are: the layout
// measures them, the golden files assert them, and a heading that drifted would
// silently change something people `awk` against.
const (
	columnProfileCurrent    = "CURRENT"
	columnProfileName       = "NAME"
	columnProfileBackend    = "BACKEND"
	columnProfileTarget     = "TARGET"
	columnProfileCredential = "CREDENTIAL"

	// currentProfileMarker marks the active profile. It is `kubectl config
	// get-contexts`'s own marker, so the table is legible without instruction.
	currentProfileMarker = "*"

	// profileGutter separates two columns, and is the gutter every other table in
	// this CLI uses.
	profileGutter = "  "
)

// profilesDocument is what `config get-profiles` renders in JSON and YAML.
//
// The field names are this CLI's own camelCase rather than the envelope's
// snake_case, exactly as versionDocument's and resolutionDocument's are: the
// envelope's item fields are spelled the way the frozen schema spells its
// columns because they are the same data reached two ways, and nothing here is
// schema data.
//
// It carries no stanza, and that is a decision. `config view -o json` renders the
// file, stanzas and all; repeating them here would be a second spelling of one
// piece of data reached from the command whose subject is something else. What is
// here instead is the part the file does not hold — which profile is active, the
// locator each one resolves to with its defaults applied, and whether its
// credential reference resolves on this machine.
type profilesDocument struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`

	// Path is the configuration file these profiles were read from, so that a
	// program collecting documents from several machines can tell them apart.
	Path string `json:"path"`

	// CurrentProfile is the active pointer, and "" when no profile is active.
	//
	// Always present, including when it is empty, for the reason
	// profileChangeDocument's is: an empty active pointer is an ordinary state
	// that a consumer must be able to read rather than infer from a missing key.
	CurrentProfile string `json:"currentProfile"`

	// Profiles are the file's profiles, sorted by name.
	//
	// Never nil, following the envelope's rule for `items`: an absent list would
	// be a file that was not read, and a consumer iterating over a null gets an
	// error where the honest answer is zero iterations. An empty configuration is
	// exactly that answer.
	Profiles []profileEntry `json:"profiles"`
}

// profileEntry is one profile, as this document reports it.
//
// It is this package's spelling of a profile's summary rather than
// resolve.Profile with tags on it, for the reason versionBackend is not
// resolve.CompiledBackend: the structured output is a public contract and the
// resolver's types are not, and the day one of them needs a field the other does
// not want must not be the day a released document changes shape.
type profileEntry struct {
	// Name is the key this profile has in the file.
	Name string `json:"name"`

	// Current reports whether this is the active profile — the row the `*`
	// marks. It is a field rather than something a consumer derives by comparing
	// with currentProfile above, because deriving it is the step a `jq` recipe
	// gets wrong.
	Current bool `json:"current"`

	// Backend is what this profile reads: `clickhouse`, `s3` or `local`.
	Backend resolve.BackendKind `json:"backend"`

	// Target is the locator it points at, with defaults applied — the address and
	// database a query would open, not the fields as typed, and for ClickHouse the
	// user it reads as in front of them: `kuberecord_ro@127.0.0.1:9000/kuberecord`.
	//
	// The principal is part of the locator rather than a field of its own, so that
	// this document and the TARGET column of the table say the same thing in the
	// same spelling. See resolve.Profile.Target for why it is here and not in
	// Describe.
	Target string `json:"target"`

	// Credential is where its credential comes from and whether that reference
	// resolves. Never a credential.
	Credential profileCredential `json:"credential"`
}

// profileCredential is a credential reference and its state, as the document
// reports them.
//
// The three fields are the whole of what may be said about a credential here:
// where it comes from, which reference names it, and whether that reference
// resolves. No field of it is derived from a credential's value — not its length,
// not whether it is empty — and a planted-password test asserts that no value
// reaches any rendering of this document at any verbosity.
type profileCredential struct {
	// Source is `env`, `file`, `ambient` or `none`.
	Source resolve.CredentialSource `json:"source"`

	// Reference is the variable name or the file path, absent for a source that
	// names neither.
	Reference string `json:"reference,omitempty"`

	// State is whether the reference resolves, in the vocabulary of its own
	// source: `set`/`not set`, `present`/`missing`/`unreadable`, or
	// `not checked`.
	//
	// A word rather than a boolean, for the reason resolutionCheck.Outcome is
	// one: `resolves: false` on an ambient credential nobody checked would be a
	// claim this command did not make.
	State resolve.CredentialState `json:"state"`
}

// newConfigGetProfilesCommand builds `config get-profiles`.
func newConfigGetProfilesCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	return &cobra.Command{
		Use:   "get-profiles",
		Short: "List the profiles, and whether each one's credential resolves",
		Long: `List the profiles in the kuberecord configuration file.

One row per profile: which is active, what each one points at, and where its
credential comes from — with whether that reference resolves on this machine,
checked as the table is drawn. An environment variable this shell never exported
reads "not set" here, which is otherwise the failure that surfaces as a
resolution error at the moment you run a query.

` + "`config view`" + ` prints the file; this prints its state. A file cannot say
what your shell holds or what is on your disk, and that column is the reason to
run this one.

An empty configuration is not an error: it is where every new user starts, so the
header prints with no rows under it and the line on stderr names the command that
writes a profile.

Nothing is contacted — no cluster, no backend, not even to ask whether one
answers, which is ` + "`config resolve --check`" + `. No credential value is
printed in any format, at any verbosity.`,
		Example: `  # Which profiles are there, and which of them can authenticate right now?
  kuberecord config get-profiles

  # For a script, or a bug report.
  kuberecord config get-profiles -o json | jq '.profiles[] | select(.current)'`,

		ValidArgsFunction: cobra.NoFileCompletions,

		RunE: func(_ *cobra.Command, args []string) error {
			// Its own sentence rather than rejectPositionalArgs, whose message is
			// about narrowing a scope with --kind and --namespace and would send
			// this reader looking for two flags this command does not have.
			if len(args) > 0 {
				return exit.UsageErrorf("config get-profiles takes no arguments, and %q is not one: "+
					"it lists every profile the file defines. One profile's full stanza is in "+
					"`config view`, which renders the file itself", args[0])
			}

			// Decided first, so that an invocation asking for a rendering nobody
			// can produce is refused before the file is read.
			format, err := configFormat("get-profiles", flags.Output)
			if err != nil {
				return err
			}

			path, err := resolve.DefaultConfigPath()
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			cfg, err := resolve.LoadConfig(path)
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			document := profilesOf(cfg, path)

			// The path goes to stderr, and is spelled as `config view` spells it:
			// `config get-profiles -o json | jq` receives the document alone, and a
			// reader with two machines' dotfiles in play can see which file this is.
			if err := options.WriteLine(streams.ErrOut, "# "+path); err != nil {
				return err
			}
			if err := writeProfiles(streams.Out, document, format,
				render.NewSeverity(options.ShouldColorize(flags.Color, streams.Out))); err != nil {
				return err
			}
			return explainNoProfiles(streams.ErrOut, document, commandNameOr(invokedAs),
				options.ShouldColorize(flags.Color, streams.ErrOut))
		},
	}
}

// profilesOf summarizes a configuration for this command.
//
// Sorted by name, which is the order every other listing of profiles in this CLI
// uses — the completion menu, DescribeProfileNames, the alternatives a refused
// deletion offers. It is also the only deterministic order available: Go's map
// iteration is randomized, and a table whose rows moved between invocations
// would be one nobody could diff.
func profilesOf(cfg *resolve.Config, path string) profilesDocument {
	document := profilesDocument{
		APIVersion:     render.EnvelopeAPIVersion,
		Kind:           ProfilesKind,
		Path:           path,
		CurrentProfile: cfg.CurrentProfile,
		Profiles:       make([]profileEntry, 0, len(cfg.Profiles)),
	}
	for _, name := range slices.Sorted(maps.Keys(cfg.Profiles)) {
		profile := cfg.Profiles[name]
		credential := profile.Credential()
		document.Profiles = append(document.Profiles, profileEntry{
			Name:    name,
			Current: name == cfg.CurrentProfile,
			Backend: profile.Backend,
			Target:  profile.Target(),
			Credential: profileCredential{
				Source:    credential.Source,
				Reference: credential.Reference,
				State:     credential.State,
			},
		})
	}
	return document
}

// writeProfiles renders the listing in the requested format.
//
// The human form is a table on stdout rather than nothing, which is where this
// command differs from the four that write: their data is the file they wrote and
// their report is the confirmation on stderr, and this one's data is the listing.
// So the empty format is handled here and writeConfigDocument is asked only for
// the two serializations it exists to produce.
func writeProfiles(
	out io.Writer, document profilesDocument, format render.StructuredFormat, severity render.Severity,
) error {
	if format == "" {
		return options.WriteAll(out, renderProfiles(document, severity))
	}
	return writeConfigDocument(out, document, format, "profile listing")
}

// renderProfiles lays the listing out as a table.
//
// The heading row is printed even with no rows under it, which is the opposite of
// the call renderScopes makes and is deliberate: an empty scope listing is a
// finding about a cluster's recording and must never read as an answered
// question, while an empty profile listing is the ordinary state of a first
// invocation. The header is what tells that reader the file was read and holds
// nothing, and the notice on stderr names the command that changes it.
//
// Nothing but the heading is painted. A cell here is a value somebody copies — an
// address, a variable name, a path — and the register that would suit
// `(not set)` is the one D30 reserves for lines a reader must not skip, which is
// not what a cell is (see render/severity.go: the tier is for a line that is not
// data). `config resolve`, the sibling report about a configuration, renders
// plain for the same reason. Nothing wraps either: a folded address is an address
// that cannot be pasted.
func renderProfiles(document profilesDocument, severity render.Severity) string {
	headings := []string{
		columnProfileCurrent, columnProfileName, columnProfileBackend,
		columnProfileTarget, columnProfileCredential,
	}

	// Every column but the last gets the width its content needs; the last is
	// never padded, because nothing follows it.
	rows := make([][]string, 0, len(document.Profiles))
	widths := make([]int, len(headings)-1)
	for i := range widths {
		widths[i] = utf8.RuneCountInString(headings[i])
	}
	for _, entry := range document.Profiles {
		cells := entry.cells()
		for i := range widths {
			widths[i] = max(widths[i], utf8.RuneCountInString(cells[i]))
		}
		rows = append(rows, cells)
	}

	var built strings.Builder
	// Painted after the layout is measured, so the escape sequences stay out of
	// the width arithmetic that lines the values up.
	built.WriteString(severity.Provenance(profileTableRow(headings, widths)) + "\n")
	for _, cells := range rows {
		built.WriteString(profileTableRow(cells, widths) + "\n")
	}
	return built.String()
}

// profileTableRow lays one row out over the measured widths.
//
// Trailing whitespace is trimmed because it is invisible on a terminal and
// permanent in a golden file — and because the CURRENT cell of every
// non-active row is empty, so an untrimmed table would have a line of padding
// down its right-hand side for a file whose active profile is the last one.
func profileTableRow(cells []string, widths []int) string {
	var built strings.Builder
	for i, cell := range cells {
		if i > 0 {
			built.WriteString(profileGutter)
		}
		if i < len(widths) {
			built.WriteString(padRight(cell, widths[i]))
			continue
		}
		built.WriteString(cell)
	}
	return strings.TrimRight(built.String(), " ")
}

// cells renders one profile's five columns, unpainted.
func (e profileEntry) cells() []string {
	current := ""
	if e.Current {
		current = currentProfileMarker
	}
	return []string{current, e.Name, string(e.Backend), e.Target, e.Credential.cell()}
}

// cell renders a credential for the table.
//
// One spelling of the document's own fields rather than a second description of
// them: the cell is the source, the reference it names and the state of that
// reference, so a reader comparing `-o json` with the table is reading the same
// three values. A source that names no reference is the bare word, which is all
// there is to say — `ambient` has nothing this command checked, and `none` has
// nothing to check.
func (c profileCredential) cell() string {
	if c.Reference == "" {
		return string(c.Source)
	}
	return fmt.Sprintf("%s %s (%s)", c.Source, c.Reference, c.State)
}

// explainNoProfiles says why the table has no rows, and what writes one.
//
// An empty configuration is not an error and does not fail: it is the state every
// new user is in, and the state of every user whose cluster has a sink custom
// resource to discover, who needs no profile at all. What it must not be is
// unexplained (Invariant 9) — a header with nothing under it is indistinguishable
// from a file that could not be read, and this is the sentence that tells them
// apart.
//
// Through render.WriteNotices rather than a line written here, so the marker, the
// hanging indent and the tier are this CLI's one implementation of a notice.
func explainNoProfiles(errOut io.Writer, document profilesDocument, command string, colorize bool) error {
	if len(document.Profiles) > 0 {
		return nil
	}
	return render.WriteNotices(errOut, []render.Notice{{
		Text: "this file defines no profiles, which is the ordinary state of a first invocation: " +
			"with none, resolution falls through to discovering a sink from the cluster.\n" +
			"To write one — with no flags it asks for what it needs, and its first question is " +
			"whether to read the settings out of a sink this cluster already holds:\n" +
			"    " + command + " config set-profile",
	}}, render.Options{Color: colorize})
}
