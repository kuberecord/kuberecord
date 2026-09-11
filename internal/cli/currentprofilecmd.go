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
	"maps"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// `config current-profile` is one token, and the whole command is that shape.
//
// `config get-profiles` already reports which profile is active — it is the
// column with the `*` in it — and that is the wrong form for the thing people
// actually do with the answer:
//
//	PROFILE=$(kuberecord config current-profile)
//
// against three lines of `awk` over a table whose columns are laid out to the
// width of their content, so a longer address in an unrelated row moves the
// field the script was counting. `kubectl config current-context` exists for
// exactly this reason and the muscle memory for it is real, which is why the verb
// is spelled the way kubectl spells it rather than `active-profile`.
//
// With it the profile surface matches `kubectl config` one for one: `set-profile`,
// `get-profiles`, `use-profile`, `current-profile`, `delete-profile`.
//
// # Why no active profile is a failure
//
// Because the shape above is what the command is for, and `$( )` cannot tell an
// empty answer from no answer. A script that captured "" and carried on would
// query whatever the rest of the resolution chain reached — a sink discovered
// from the cluster, most likely — while believing it had been told which profile
// to use. That is the same class of wrongness D24 refuses of an address: not a
// failure, but a success reported about somewhere the caller did not choose. Exit
// 1 is what makes `set -e` stop there.
//
// It is emphatically not a claim that a configuration with no active profile is
// broken. Every command that queries data resolves perfectly well without one,
// and the message says so rather than leaving a first-time reader to infer it
// (Invariant 9).
//
// # What it will not do
//
// Contact anything. It reads the configuration file and nothing else — no
// cluster, no backend, not even the environment, since unlike `get-profiles` it
// reports no credential state. TestCurrentProfileContactsNothing pins it in the
// shape TestCompletionContactsNothing uses.
//
// It also does not consult --profile, for the reason `get-profiles` does not: the
// subject is the file's active pointer, as `kubectl config current-context`'s is,
// and what *this invocation* would resolve to is a different question with nine
// steps behind it that D26 gives to `config resolve`.

// CurrentProfileKind is the kind of the document `config current-profile` renders
// in a structured format.
//
// It carries render.EnvelopeAPIVersion and is deliberately not one of that
// package's envelope kinds, for the reason VersionKind, ProfilesKind and
// ResolutionKind are not: an envelope's metadata is the provenance of an *answer*
// — which cluster, which engine, what was watching — and this command asks no
// backend anything.
//
// What it does share is the contract that version is governed by — fields may be
// added here and must never be renamed, removed or repurposed within
// cli.kuberecord.io/v1alpha1 (D19).
const CurrentProfileKind = "CurrentProfile"

// currentProfileDocument is what `config current-profile` renders in JSON and
// YAML.
//
// Two fields, and the restraint is the point. This command's subject is the
// *name*; what that profile points at and whether its credential resolves is
// `get-profiles`' subject, and `-o json | jq '.profiles[] | select(.current)'` is
// the recipe docs/CLI.md already gives for it. Repeating those fields here would
// be a second spelling of one piece of data reached from the command whose
// subject is something else — the call profilesDocument makes when it leaves the
// stanza to `config view`. The additive-only policy leaves room to change that
// mind later; nothing leaves room to unsay it.
//
// currentProfile rather than `name`, so that the same `jq` path reads the active
// profile out of this document, out of a Profiles listing and out of a
// ProfileChange (D19). Three documents about the same pointer should not spell it
// three ways.
type currentProfileDocument struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`

	// Path is the configuration file the pointer was read from, so that a program
	// collecting documents from several machines can tell them apart. It is the
	// field the human form deliberately does not print; see the command's RunE.
	Path string `json:"path"`

	// CurrentProfile is the active profile's name, and is never empty in a
	// rendered document: no active profile is a failure with no document at all,
	// which is what distinguishes this field from profilesDocument's, where "" is
	// an ordinary state of a listing that still has rows to show.
	CurrentProfile string `json:"currentProfile"`
}

// newConfigCurrentProfileCommand builds `config current-profile`.
func newConfigCurrentProfileCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	return &cobra.Command{
		Use:   "current-profile",
		Short: "Print the name of the active profile",
		Long: `Print the name of the active profile, and nothing else.

One token on stdout, with no header and no decoration, because the thing people
do with this answer is capture it:

    PROFILE=$(kuberecord config current-profile)

` + "`config get-profiles`" + ` marks the active profile with ` + "`*`" + ` and is the command to
reach for when the question is what the file holds. This one answers the narrower
question a script asks. The name versus the table.

No profile being active is an error rather than an empty success, and exit 1 is
the point of it: a script that captured "" and carried on would read from
wherever the rest of the resolution chain reached while believing it had been
told which profile to use. The message names what to do about it.

Nothing is contacted, and nothing is written. It reads the configuration file.`,
		Example: `  # The name, on stdout, and nothing else.
  kuberecord config current-profile

  # Which is the shape it exists for.
  PROFILE=$(kuberecord config current-profile) || exit 1`,

		ValidArgsFunction: cobra.NoFileCompletions,

		RunE: func(_ *cobra.Command, args []string) error {
			// Its own sentence rather than rejectPositionalArgs, whose message is
			// about narrowing a scope with --kind and --namespace and would send this
			// reader looking for two flags this command does not have. A name typed
			// here is somebody reaching for `use-profile`, so that is what it names.
			if len(args) > 0 {
				return exit.UsageErrorf("config current-profile takes no arguments, and %q is not one: "+
					"it prints the profile that is already active. To make that one active, "+
					"`%s config use-profile %s`", args[0], commandNameOr(invokedAs), args[0])
			}

			// Decided first, so that an invocation asking for a rendering nobody can
			// produce is refused before the file is read.
			format, err := configFormat("current-profile", flags.Output)
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
			// Config.Validate refuses a currentProfile naming nothing, so a
			// non-empty pointer here names a stanza that exists and there is no
			// third state to report.
			if cfg.CurrentProfile == "" {
				return errNoActiveProfile(cfg, path, commandNameOr(invokedAs))
			}

			if format == "" {
				// Nothing on stderr, which is where this command departs from `config
				// view` and `config get-profiles`: both print the file's path there so
				// a reader with two machines' dotfiles can see which file answered,
				// and both produce output somebody reads. This one produces a token
				// somebody captures, `kubectl config current-context` prints nothing
				// beside it, and the path is in this command's own -o json for a
				// program that needs it.
				//
				// Unpainted, for the same reason no cell of the profile table is: the
				// severity vocabulary is for lines that are not data (D27), and this
				// line is the whole of the data.
				return options.WriteLine(streams.Out, cfg.CurrentProfile)
			}
			return writeConfigDocument(streams.Out, currentProfileDocument{
				APIVersion:     render.EnvelopeAPIVersion,
				Kind:           CurrentProfileKind,
				Path:           path,
				CurrentProfile: cfg.CurrentProfile,
			}, format, "current profile")
		},
	}
}

// errNoActiveProfile refuses the one state in which this command has no answer,
// and names the routes out of it.
//
// Two messages, because the two states have different routes and only one of them
// has `use-profile` in it (D34). A file with profiles and no active pointer needs
// one chosen; a file with no profiles needs one written, and offering
// `use-profile` there would be a remedy naming nothing — the case
// errDeletingTheActiveProfile and switchRoute both give a sentence of their own,
// for the same reason: a command a reader cannot complete reads as though they
// should have known which name to substitute.
//
// Both name `config get-profiles`, which is the command that reports the state
// this one has just declined to summarise, and both are a single sentence in the
// register every other refusal in this subtree uses. The exit path paints the
// whole `error:` line at the Failure tier (D39), so a multi-line block of routes
// here would be a paragraph in red — which is the rendering failureDiagnostic
// explicitly declines to produce for cobra's usage block, for the same reason.
func errNoActiveProfile(cfg *resolve.Config, path, command string) error {
	if len(cfg.Profiles) == 0 {
		// Said here rather than left to `get-profiles`: a reader whose first
		// encounter with the profile surface is a failure must not conclude that
		// their installation is broken, and the fact that queries need no profile
		// at all is the sentence that stops them concluding it (Invariant 9).
		return exit.RuntimeErrorf("no profile is active, and %s defines none: write one with "+
			"`%s config set-profile`, which with no flags asks for what it needs, after which "+
			"`%s config get-profiles` reports the file's state. An empty file is not a broken "+
			"one — with no profile, resolution falls through to discovering a sink from the "+
			"cluster — but there is no name to print, and printing nothing would let a script "+
			"carry on as though it had been told which profile to use",
			path, command, command)
	}

	defined := slices.Sorted(maps.Keys(cfg.Profiles))
	// The rest of the list only when there is a rest of it, exactly as
	// errDeletingTheActiveProfile withholds it: with one profile the suggestion
	// has already named it, and a trailing "also defined: local" would be the same
	// word twice in one sentence.
	rest := ""
	if len(defined) > 1 {
		rest = fmt.Sprintf(" (also defined: %s)", strings.Join(defined[1:], ", "))
	}
	return exit.RuntimeErrorf("no profile is active in %s, so there is no name to print: choose one "+
		"with `%s config use-profile %s`, or see which of them has a credential that resolves "+
		"right now with `%s config get-profiles`%s",
		path, command, defined[0], command, rest)
}
