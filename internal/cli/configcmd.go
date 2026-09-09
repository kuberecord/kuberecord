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
	"maps"
	"slices"
	"strings"
	"unicode"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"sigs.k8s.io/yaml"
)

// `config` exists because discovery cannot serve everyone.
//
// A sink's password lives in a Secret in the operator's namespace, and the
// operator's own RBAC is the only grant that reaches it (D7). Most engineers
// cannot read it, and the right answer to that is not to widen anybody's
// permissions — it is a read-only ClickHouse user and a profile naming where the
// password comes from. Four of these subcommands are the smallest surface that
// makes writing such a profile a command rather than a documentation exercise.
//
// What they will not do is store a credential. See resolve.ClickHouseProfile.Password.
//
// The fifth, `delete-profile`, is what closes the lifecycle: create, inspect,
// switch, delete. A tool that writes configuration and cannot remove it leaves
// its users hand-editing the file it exists to spare them, and a profile left
// behind is not inert — it shadows discovery from step 3 of the resolution chain.
//
// The last three write nothing at all, and between them they are "inspect".
// `resolve` belongs here because a profile is one step of the chain that decides
// where an answer comes from, and the question it answers — "which step won, and
// why not the others" — is the one a reader of this file has when the file turns
// out not to be the step that won. `get-profiles` answers the question before
// that one: what is in the file, which of it is active, and which of it could
// authenticate right now. `current-profile` answers the narrowest of the three,
// and is the only one of them shaped for a program rather than a reader: one
// token, the active profile's name, because the `*` in a table is the wrong shape
// for `$( )`. See resolvecmd.go, getprofilescmd.go and currentprofilecmd.go.

// newConfigCommand builds the `config` subtree.
func newConfigCommand(flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string) *cobra.Command {
	config := &cobra.Command{
		Use:   "config",
		Short: "Read and write the kuberecord configuration file",
		Long: fmt.Sprintf(`Read and write the kuberecord configuration file.

The file lives at ${XDG_CONFIG_HOME:-~/.config}/%s/%s and is written 0600.

It holds profiles — where to read history from — and a mapping from kubeconfig
context to kuberecord cluster identity. It never holds a password: a profile
names an environment variable or a file to read one from, and a password written
inline is refused with an explanation.

`+"`config set-profile`"+` is an upsert: a name already in the file is replaced
whole, and the line it prints names the profile that is gone.
`+"`config delete-profile`"+` removes one, and refuses to remove the active one
without --force.

Four subcommands write nothing. `+"`config view`"+` prints the file.
`+"`config get-profiles`"+` prints its state: one row per profile, which is
active, what each points at, and whether its credential reference resolves on
this machine — which the file itself cannot say.
`+"`config current-profile`"+` prints the active profile's name alone, which is
the shape a script wants. `+"`config resolve`"+` reports which step of the
resolution chains this invocation would use, and why the earlier ones had nothing
to say.`,
			resolve.ConfigDirName, resolve.ConfigFileName),
		Args: rejectUnknownSubcommand,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}

	config.AddCommand(
		newConfigViewCommand(flags, streams),
		newConfigGetProfilesCommand(flags, streams, invokedAs),
		newConfigSetProfileCommand(flags, streams, invokedAs),
		newConfigUseProfileCommand(flags, streams),
		newConfigCurrentProfileCommand(flags, streams, invokedAs),
		newConfigDeleteProfileCommand(flags, streams, invokedAs),
		newConfigSetContextClusterIDCommand(flags, streams, invokedAs),
		newConfigResolveCommand(flags, streams, invokedAs),
	)
	return config
}

// newConfigViewCommand prints the configuration.
//
// It prints the file's *contents* rather than an effective configuration merged
// with the flags, because the question it answers is "what did I write down", and
// a view that folded in this invocation's flags would show a file that does not
// exist. Nothing is redacted, which is safe by construction: the file cannot hold
// a credential.
func newConfigViewCommand(flags *options.GlobalFlags, streams genericiooptions.IOStreams) *cobra.Command {
	return &cobra.Command{
		Use:   "view",
		Short: "Print the configuration file",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return exit.UsageErrorf("config view takes no arguments, and was given %q", args[0])
			}

			path, err := resolve.DefaultConfigPath()
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			cfg, err := resolve.LoadConfig(path)
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}

			// The path goes to stderr so that `config view -o json | jq` receives
			// the document alone, and so that a reader who has two machines'
			// dotfiles in play can see which file they are looking at.
			if err := options.WriteLine(streams.ErrOut, "# "+path); err != nil {
				return err
			}
			return writeConfig(streams.Out, cfg, flags.Output)
		},
	}
}

// writeConfig renders a configuration in the requested format.
//
// Only the two document formats are accepted. A configuration file is not a result
// set: there is no useful table of it, and `jsonl` is a streaming format for a
// result larger than memory. Refusing the other four by name is better than
// rendering YAML regardless and leaving a user to wonder why `-o table` did
// nothing.
func writeConfig(out io.Writer, cfg *resolve.Config, format options.OutputFormat) error {
	// Stamped on the way out even for a file that predates them, so that what is
	// printed is what a write of the same document would produce.
	cfg.APIVersion = resolve.ConfigAPIVersion
	cfg.Kind = resolve.ConfigKind

	switch format {
	case options.OutputYAML, options.OutputTable, options.OutputWide:
		// A configuration file is YAML, and `table` is the global default rather
		// than a choice this command was given; rendering the document is the only
		// sensible reading of it.
		encoded, err := yaml.Marshal(cfg)
		if err != nil {
			return exit.RuntimeErrorf("encoding the configuration: %w", err)
		}
		return options.WriteAll(out, string(encoded))

	case options.OutputJSON:
		encoded, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return exit.RuntimeErrorf("encoding the configuration: %w", err)
		}
		return options.WriteAll(out, string(encoded)+"\n")
	}
	return exit.UsageErrorf("config view renders %s or %s, not %s", options.OutputYAML, options.OutputJSON, format)
}

// newConfigSetProfileCommand writes one profile.
//
// Three routes reach one write. The per-field flags are the first, --from-sink is
// the second, and a bare invocation on a terminal is the third: the prompting
// layer in setprofilewizard.go asks for exactly what those flags would have
// carried, assembles the same struct, and calls the same writer. It is a layer
// over this path and never a second one beside it (D33).
func newConfigSetProfileCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	var (
		fromSink string
		use      bool
		fields   profileFields
	)

	command := &cobra.Command{
		Use:   "set-profile [NAME]",
		Short: "Create or replace a profile",
		Long: `Create or replace a profile in the kuberecord configuration file.

A profile says where to read recorded history from.

A name already in the file is replaced, and the whole stanza is replaced rather
than the fields this invocation mentions: a profile that named a password file
and is rewritten with --password-env keeps no reference to the file. The line on
stderr names what was there before, since nothing else holds it afterwards.

Writing a profile does not make it the active one. The active profile answers
every later command that names no source, so being switched to a store you wrote
in order to inspect it is a side effect worth asking for: --use writes and
activates in one command, and without it the two routes that print a next step —
--from-sink and the questions — name config use-profile instead. The exception is
a profile written into an otherwise empty file, which becomes the active one
because there is nothing to displace and no other reading of it — and the line on
stderr says so.

With no flags at all, on a terminal, it asks. The first question is whether to
read the settings from a sink this cluster already holds, which is --from-sink
reached without having to know it exists; the last thing printed is the flag
command that would have done the same thing without the questions.

--from-sink <kind>/<name> writes the whole stanza from a sink custom resource the
cluster already holds, so there is nothing to look up and nothing to mistype. A
cluster-internal address is recorded as a forwarded loopback port instead, and
the notice on stderr says so. It is the shortest route to a working profile, and
the one to reach for after a query has failed to reach the address a sink
records.

The per-field flags below are the escape hatch, for a profile with no custom
resource behind it: an archive synced to a laptop, a ClickHouse in a cluster this
kubeconfig does not reach, a machine with no kubeconfig at all.

A profile never holds a password either way: for ClickHouse, name an environment
variable with --password-env or a file with --password-file. For S3 and MinIO
there is nothing to name — credentials come from the AWS credential chain, which
every tool on the machine already reads.`,
		Example: `  # Answer questions instead of knowing the flags. Prints the flag form at the end.
  kuberecord config set-profile

  # From the sink the operator already streams to. No values to look up.
  kuberecord config set-profile local --from-sink ClickHouseSink/default

  # The same, and read through it from here on: one command rather than two.
  kuberecord config set-profile local --from-sink ClickHouseSink/default --use

  # By hand, for a backend no custom resource in this cluster describes:
  # a read-only ClickHouse user, with the password in the environment.
  kuberecord config set-profile prod --backend clickhouse \
      --addr clickhouse.example:9000 --database kuberecord \
      --username kuberecord_ro --password-env KUBERECORD_CLICKHOUSE_PASSWORD

  # An archive in MinIO.
  kuberecord config set-profile archive --backend s3 --bucket acme-audit \
      --prefix kuberecord --endpoint https://minio.internal:9000 --force-path-style

  # An archive synced to a laptop.
  kuberecord config set-profile laptop --backend local --path ~/archives/kuberecord`,
		ValidArgsFunction: cobra.NoFileCompletions,

		RunE: func(cmd *cobra.Command, args []string) error {
			// Refused ahead of everything else, and therefore on all three routes
			// rather than only under --from-sink. --sink-addr replaces the endpoint
			// of one invocation's *resolved* backend (D25); this command resolves
			// nothing and dials nothing, so a value given here would parse, change
			// no field, and leave its author believing they had set the address the
			// profile records. That is a silent no-op, which this release is
			// closing rather than adding to (D31).
			if cmd.Flags().Changed(options.FlagSinkAddr) {
				return errSinkAddrWritesNoFile()
			}

			// Decided before the questions are asked and before the file is
			// touched, so that an invocation asking for a rendering nobody can
			// produce is refused rather than answered after the write. It applies
			// to all three routes for the same reason the refusal above does.
			format, err := configFormat("set-profile", flags.Output)
			if err != nil {
				return err
			}

			if len(args) > 1 {
				return exit.UsageErrorf("config set-profile takes one argument, the profile name")
			}
			name, named := "", len(args) == 1
			if named {
				name = args[0]
			}

			// Mode selection, and the whole of it. No flag of this command's own
			// means the user has not said what they want, so ask; any of them means
			// they have, and the path below runs exactly as it did before this
			// command could ask anything. There is no --interactive, because a flag
			// to request the behaviour you get by typing nothing is a flag nobody
			// finds.
			if !setProfileFlagsGiven(cmd) {
				return runSetProfileWizard(cmd, flags, streams, invokedAs, name, format, use)
			}

			if !named {
				return exit.UsageErrorf("config set-profile takes one argument, the profile name")
			}
			if err := requireProfileName(name); err != nil {
				return err
			}

			if fromSink != "" {
				derived, err := deriveProfile(cmd, flags, streams, invokedAs, fromSink, fields.overrides())
				if err != nil {
					return err
				}
				// Colour is decided here rather than in resolve for the reason the
				// unreachable-backend block's is: --color, NO_COLOR and whether
				// stderr is a terminal are facts about this invocation, and a
				// renderer that consulted them itself would have golden files that
				// changed with the shell they were generated in.
				colorize := options.ShouldColorize(flags.Color, streams.ErrOut)
				_, err = writeProfile(profileWrite{
					name:        name,
					profile:     derived.Profile,
					explanation: derived.Explain(colorize),
					activate:    use,
					nextStep:    true,
					invokedAs:   invokedAs,
					severity:    render.NewSeverity(colorize),
					format:      format,
				}, streams)
				return err
			}

			profile, err := fields.stanza()
			if err != nil {
				return err
			}
			_, err = writeProfile(profileWrite{
				name:     name,
				profile:  profile,
				activate: use,
				severity: render.NewSeverity(options.ShouldColorize(flags.Color, streams.ErrOut)),
				format:   format,
			}, streams)
			return err
		},
	}

	command.Flags().StringVar(&fromSink, options.FlagFromSink, "",
		"Fill the profile in from a sink custom resource, as kind/name (for example "+
			"ClickHouseSink/default). A cluster-internal address is rewritten to a forwarded "+
			"loopback port, and the notice on stderr says so.")
	mustCompleteFlag(command, options.FlagFromSink, completeSinkRefs)

	// Registered beside --from-sink rather than in the table below, because it is
	// not a field of a profile: it says what to do with the file's active pointer
	// once the stanza is written. Two consequences follow from that and both are
	// wanted. It is not refused beside --from-sink, since the custom resource has
	// no opinion about which profile answers; and it does not count as having said
	// what to write, so `set-profile --use` on a terminal still asks the questions
	// — with the last of them already answered.
	//
	// No backquotes in the sentence: pflag reads backquoted text in a usage string
	// as the flag's value placeholder, so naming config use-profile that way would
	// print a boolean flag as taking one.
	command.Flags().BoolVar(&use, options.FlagUse, use,
		"Make this profile the active one as well as writing it, rather than running "+
			"config use-profile after. Without it the write decides nothing about which profile "+
			"answers, except for the first profile in an otherwise empty file.")

	// Registered from the table rather than one line each, because the table is
	// what the prompting layer walks and a flag registered beside it would be a
	// field with no question (see profileFieldFlags).
	for _, field := range profileFieldFlags {
		switch {
		case field.str != nil:
			command.Flags().StringVar(field.str(&fields), field.name, "", field.usage)
		case field.boolean != nil:
			command.Flags().BoolVar(field.boolean(&fields), field.name, false, field.usage)
		}
		if field.complete != nil {
			mustCompleteFlag(command, field.name, field.complete)
		}
	}

	return command
}

// errSinkAddrWritesNoFile refuses the per-invocation endpoint override.
//
// It is a function so that the sentence lives once. Every route through
// set-profile raises it, and a message spelled at each of them would be three
// spellings of one refusal.
func errSinkAddrWritesNoFile() error {
	return exit.UsageErrorf("--%s overrides the endpoint of one invocation's backend, and this "+
		"command writes a file rather than reading one: give --%s to set the address the "+
		"profile records", options.FlagSinkAddr, options.FlagAddr)
}

// setProfileFlagsGiven reports whether this invocation said anything about the
// profile it wants written.
//
// It asks about this command's own flags and no others, which is the difference
// between a rule and a rule that works. --kubeconfig, --context and
// --operator-namespace decide *which cluster's sinks* the first question can
// offer, so an invocation carrying one of them is precisely an invocation that
// wants to be asked; suppressing the questions for it would answer "which
// cluster?" by refusing to ask anything at all. --color, --output and -v say how
// this process renders and logs and have no opinion about a profile either.
//
// --use is this command's own and is still not one of them, which is the entry in
// this list that needs a sentence of its own. It says what to do with the active
// pointer after the write and nothing about what to write, so an invocation
// carrying it alone has named no field and has to be asked — and the questions
// then have their last one already answered. Counting it as "the user has said
// what they want" would send `set-profile --use` to the flag path, to be refused
// for a missing --backend it never claimed to carry.
//
// The set walked is the same table the flags were registered from, so a field
// added later cannot be one this question forgets about.
func setProfileFlagsGiven(cmd *cobra.Command) bool {
	if cmd.Flags().Changed(options.FlagFromSink) {
		return true
	}
	for _, field := range profileFieldFlags {
		if cmd.Flags().Changed(field.name) {
			return true
		}
	}
	return false
}

// requireProfileName is the one rule both routes apply to a profile name.
//
// It is a function for the same reason the validator is called rather than
// reimplemented: the prompting layer re-asks on exactly this message, and a
// second copy of the check is a second thing that can decide differently about an
// empty string.
func requireProfileName(name string) error {
	if name == "" {
		return exit.UsageErrorf("the profile name is empty")
	}
	return nil
}

// profileWrite is one profile on its way into the configuration file.
//
// It is a struct because the two routes that reach the write — a stanza typed
// flag by flag, and one derived from a sink custom resource — differ only in what
// they have to say about it afterwards, and a second copy of the load/merge/save
// sequence would be a second place for the activation rule to be decided.
//
// # A name already in the file is replaced, whole
//
// `set-profile` is an upsert, and what it writes over an existing name is the
// *entire* stanza rather than the fields this invocation happened to mention. A
// profile that named a password file and is rewritten with --password-env keeps no
// reference to the file.
//
// A field merge is the tempting alternative and it is refused: the profile it
// produced would depend on what was in the file beforehand, which makes it
// unreconstructible from the command that wrote it. Every message this command
// prints — the equivalent command the questions end with, the `--from-sink` line
// in a bug report — is a claim that running it again produces this profile, and a
// merge would make that claim false on any machine whose file started out
// different. It would also make the destructive case worse rather than better: a
// stanza half from a hand-tuned profile and half from a flag is a configuration
// nobody wrote.
//
// Replacement being destructive is why it is announced. See the `was:` line at the
// bottom of writeProfile, and D31 — the surprising-but-correct outcome is the one
// that has to be visible.
type profileWrite struct {
	// name is the key in the file's profiles map.
	name string

	// profile is the stanza to write.
	profile resolve.Profile

	// explanation is what --from-sink derived and why, already rendered. Empty for
	// a profile typed out by hand, which needs no explaining to the person who
	// just typed it.
	explanation string

	// activate asks for the active pointer to be moved to this profile: --use on
	// the flag path, and the last of the questions on the other.
	//
	// It is a request rather than the outcome. A profile that is already the active
	// one is not activated again, and one written into an otherwise empty file is
	// activated whether or not anybody asked — see writeProfile, where both are
	// decided in one place because the two routes must not disagree about what
	// this field means.
	activate bool

	// nextStep asks for the `config use-profile` line, which is printed only when
	// the write left the active pointer somewhere else.
	nextStep bool

	// invokedAs is how this process was invoked, so that line names a command the
	// reader can type.
	invokedAs string

	// severity paints the lines this write reports itself with. Its zero value is
	// colour-disabled, which is what a caller with nothing to say about colour
	// should get.
	severity render.Severity

	// format is the structured document this invocation asked for, empty for the
	// human form. It is decided by configFormat before the command reaches
	// this struct, so a format nobody can render never rewrites a file.
	format render.StructuredFormat
}

// activatesOnItsOwn reports whether writing name into cfg makes it the active
// profile with nobody having asked.
//
// It is the one carve-out from D38, and the comment is the reason it is allowed
// to exist. Activation is opt-in because the active profile is a side effect on
// every later command in the shell, and redirecting `timeline`, `diff` and `get`
// to a store somebody wrote in order to inspect it is D24's objection at the
// config layer. Neither half of that objection applies here: there is nothing to
// displace, and "the only profile in the file is the one that answers" is the only
// reading a first profile has. Requiring a second command to make it usable would
// be ceremony with no decision in it.
//
// Both clauses are load-bearing. No active pointer is not enough on its own —
// `delete-profile --force` leaves a file with several profiles and no pointer, and
// the next profile written into it would be activated by a rule reporting that it
// is the only one, which would be false. So the carve-out asks for what it claims:
// no pointer, and nothing in the file but this name.
func activatesOnItsOwn(cfg *resolve.Config, name string) bool {
	if cfg.CurrentProfile != "" {
		return false
	}
	for other := range cfg.Profiles {
		if other != name {
			return false
		}
	}
	return true
}

// activationIsADecision reports whether anybody has a choice to make about the
// active pointer when name is written into cfg.
//
// It is what the prompting layer asks before asking, and two states have no
// decision in them. The carve-out above decides for itself; and a name that is
// *already* the active profile cannot be made more so, since answering "no" would
// not deactivate it either. A question whose answer is disregarded whichever way
// it is given is worse than no question (D31) — the same judgement askPassword's
// offerNone is written with.
func activationIsADecision(cfg *resolve.Config, name string) bool {
	return !activatesOnItsOwn(cfg, name) && cfg.CurrentProfile != name
}

// writeProfile validates a profile, writes it, and says what it did.
//
// It reports whether the write made this profile the active one, which the
// prompting layer needs in order to print --use in the command it teaches: a line
// reproducing the stanza and not the activation would reproduce half of what its
// reader just watched happen.
func writeProfile(w profileWrite, streams genericiooptions.IOStreams) (bool, error) {
	// Validated before anything is read from disk, so a mistyped command cannot
	// rewrite a file only to be rejected on the way back in.
	if err := w.profile.Validate(); err != nil {
		return false, exit.UsageErrorf("profile %q: %w", w.name, err)
	}

	path, err := resolve.DefaultConfigPath()
	if err != nil {
		return false, exit.RuntimeErrorf("%w", err)
	}
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		return false, exit.RuntimeErrorf("%w", err)
	}
	if cfg.Profiles == nil {
		cfg.Profiles = map[string]resolve.Profile{}
	}
	// Read out before the assignment overwrites it, because it is the only
	// surviving account of what this command destroyed: the file holds the new
	// stanza a line later, and nothing anywhere holds the old one.
	previous, replaced := cfg.Profiles[w.name]
	// Read out ahead of the write for the same reason, and asked before the
	// insertion because both are questions about what the file *held*: whether
	// this name was already the one that answers, and whether there was anything
	// else in the file to displace.
	wasActive := cfg.CurrentProfile == w.name
	onlyOne := activatesOnItsOwn(cfg, w.name)
	cfg.Profiles[w.name] = w.profile

	// The whole of the activation rule, in one place, so that the flag path and
	// the questions cannot disagree about it. Asked for, or the only profile in
	// the file; an existing choice is never overridden, because that one is a
	// decision and `use-profile` is where it is made.
	activated := w.activate || onlyOne
	if activated {
		cfg.CurrentProfile = w.name
	}

	if err := resolve.SaveConfig(path, cfg); err != nil {
		return false, exit.RuntimeErrorf("%w", err)
	}

	// The two outcomes read differently on purpose. Creating a profile is what
	// the command was asked to do and is reported plainly; replacing one is
	// correct, asked for, and destructive, so the line names what is gone.
	//
	// Provenance is the tier for it rather than Warning (D27). Nothing went
	// wrong and no conclusion is misread without the line — the reader gets
	// exactly the profile they described. What they cannot get back is the
	// stanza that was there, and provenance is the register for a fact that has
	// to be available in scrollback without demanding to be read: it is where
	// the profile now in the file came from, in the same sense as which sink a
	// row was reconstructed from.
	confirmation := fmt.Sprintf("→ wrote profile %q in %s", w.name, path)
	if replaced {
		confirmation = w.severity.Provenance(fmt.Sprintf("→ updated profile %q in %s (was: %s)",
			w.name, path, previous.Describe()))
	}
	if err := options.WriteLine(streams.ErrOut, confirmation); err != nil {
		return activated, err
	}
	if line := activePointerReport(w, activated, wasActive); line != "" {
		if err := options.WriteLine(streams.ErrOut, w.severity.Provenance(line)); err != nil {
			return activated, err
		}
	}
	if w.explanation != "" {
		if err := options.WriteAll(streams.ErrOut, "\n"+w.explanation); err != nil {
			return activated, err
		}
	}
	// Withheld when this profile is the one that answers, however it came to be:
	// naming the command that makes it active is advice for something already
	// done, and the line above has just said so.
	if w.nextStep && !activated && !wasActive {
		if err := options.WriteLine(streams.ErrOut,
			fmt.Sprintf("\n→ to make it the active profile: `%s config use-profile %s`",
				commandNameOr(w.invokedAs), w.name)); err != nil {
			return activated, err
		}
	}

	action, displaced := profileCreated, (*resolve.Profile)(nil)
	if replaced {
		action, displaced = profileUpdated, &previous
	}
	// No `activated` field beside these: the document reports currentProfile after
	// the write, which is the fact a consumer needs and the one this write can be
	// held to. Whether the pointer *moved* is derivable from the action and the
	// name for every case a script can act on, and a second field saying the same
	// thing in a narrower way is a field to keep in step for nothing (D19).
	return activated, writeProfileChange(streams.Out, w.format, profileChangeDocument{
		Action:         action,
		Name:           w.name,
		Path:           path,
		Profile:        &w.profile,
		Previous:       displaced,
		CurrentProfile: cfg.CurrentProfile,
	})
}

// activePointerReport is what a write says about the active pointer, and "" for a
// write with nothing to say about it.
//
// Every line it returns is rendered in the Provenance tier by its caller, and that
// is the tier for the reason D27 defines it with: which profile answers is a fact
// the reader needs available in scrollback — a config change nobody typed most of
// all — while being one they have already read on every previous write. Nothing
// here is a Warning, because nothing here means a conclusion would be misread; and
// a creation's own confirmation stays out of the tier, so that the provenance lines
// in this block are the ones carrying something the command was not asked for.
//
// Four states, and the ordering between the first two is deliberate. A write that
// both asked for activation and would have got it anyway reports the asking: the
// carve-out's clause exists to explain a change nobody requested, and printing it
// to somebody who requested one would answer a question they did not ask.
//
// The asked-for line does not name --use, and the no-op line below does. That is
// not an inconsistency: a "yes" at the wizard's last question reaches the first
// line too, and a line naming a flag its reader never typed is the sort of
// statement contradicting its own transcript that this phase exists to remove. The
// no-op is reachable only from the flag, because the questions are not asked about
// a profile that already answers (activationIsADecision).
func activePointerReport(w profileWrite, activated, wasActive bool) string {
	switch {
	case activated && !wasActive && w.activate:
		return fmt.Sprintf("→ made %q the active profile, as asked", w.name)

	// Moved, and nobody asked: activatesOnItsOwn is the only other thing that sets
	// the pointer, so reaching here *is* the carve-out and the clause is a fact
	// rather than a guess. It is taken from the case above rather than passed in a
	// second time, which is what keeps the two from being able to disagree.
	case activated && !wasActive:
		return fmt.Sprintf("→ made %q the active profile (it is the only one)", w.name)

	// The no-op said out loud (D31). --use on the profile that already answers
	// changes nothing about the file, and a flag that produced no visible effect
	// has to say why rather than leave its author to wonder whether it was read.
	case w.activate:
		return fmt.Sprintf("→ --%s changed nothing: %q is already the active profile",
			options.FlagUse, w.name)

	// Nobody asked, and this name is the one that answers. The write is therefore
	// not inert in the way an upsert of any other profile is — the stanza just
	// replaced is the one the next command reads — and the reader is the one person
	// who cannot see that from the confirmation above.
	case wasActive:
		return fmt.Sprintf("→ %q is the active profile: this stanza is what the next command reads",
			w.name)
	}
	return ""
}

// deriveProfile reads a sink custom resource and turns it into a profile stanza.
//
// It is the whole of --from-sink at this layer: the flag conflicts, which are a
// question about flags and therefore belong to the command, and then a call into
// the same discovery path a query resolves through. Nothing about how a sink is
// read changes according to why it was read.
func deriveProfile(
	cmd *cobra.Command, flags *options.GlobalFlags, streams genericiooptions.IOStreams,
	invokedAs, value string, over resolve.ProfileOverrides,
) (*resolve.SinkProfile, error) {
	ref, err := resolve.ParseSinkRef(options.FlagFromSink, value)
	if err != nil {
		return nil, err
	}
	backend, err := backendForSinkKind(ref.Kind)
	if err != nil {
		return nil, err
	}
	// Refused before the cluster is contacted, for the same reason a profile is
	// validated before the file is opened: learning that two flags disagree should
	// not cost an API round trip and a Secret read.
	if err := refuseFromSinkConflicts(cmd, ref, backend); err != nil {
		return nil, err
	}

	resolver, err := resolve.NewBackendResolver(flags, streams, invokedAs)
	if err != nil {
		return nil, err
	}
	return resolver.ProfileFromSink(cmd.Context(), ref, over)
}

// backendForSinkKind says which profile backend a sink kind writes.
func backendForSinkKind(kind string) (resolve.BackendKind, error) {
	switch kind {
	case resolve.KindClickHouseSink:
		return resolve.BackendClickHouse, nil
	case resolve.KindS3Sink:
		return resolve.BackendS3, nil
	}
	return "", exit.UsageErrorf("--%s names the kind %q, which writes no profile this build knows",
		options.FlagFromSink, kind)
}

// profileFields is one profile's worth of answers, in the shape both routes to a
// write fill in.
//
// It exists so that the prompting layer can be a layer (D33). The flags bind
// directly into it, the questions write into it, and stanza() is the one function
// that turns it into something resolve.Profile.Validate has an opinion about — so
// a value typed at a prompt and the same value passed as a flag are assembled by
// one implementation and judged by one validator. Adding a field here without
// adding it to profileFieldFlags gives it neither.
type profileFields struct {
	Backend        string
	Addr           string
	Database       string
	Username       string
	PasswordEnv    string
	PasswordFile   string
	TLS            bool
	Bucket         string
	Region         string
	Endpoint       string
	ForcePathStyle bool
	Prefix         string
	Path           string
}

// overrides is the subset of these fields --from-sink may put over what a sink
// records.
//
// A method rather than a literal at the call site so that the mapping is stated
// once. It is the boundary resolve.ProfileOverrides documents — the endpoint, the
// TLS setting, the user and where the credential lives — and everything absent
// from it is a fact about where the sink writes, refused by
// refuseFromSinkConflicts before the cluster is contacted rather than dropped
// here.
func (f profileFields) overrides() resolve.ProfileOverrides {
	return resolve.ProfileOverrides{
		Addr: f.Addr, Username: f.Username, PasswordEnv: f.PasswordEnv,
		PasswordFile: f.PasswordFile, TLS: f.TLS,
	}
}

// profileField is one settable field of a profile: the flag that carries it, the
// backend it belongs to, and where its value lives.
type profileField struct {
	// name is the flag name, and the word the equivalent command prints.
	name string

	// backend is the one this flag belongs to, for the purpose of refusing it
	// beside --from-sink. --backend itself belongs to none, since it is the flag
	// that selects one.
	backend resolve.BackendKind

	// usage is the flag's help text and the prompt's question, in that order of
	// authorship and with no second copy. This is the whole of what the acceptance
	// criterion's "driven from the same field metadata the flags use" can mean
	// here: a sentence that describes what a field is for reads identically above
	// a `--help` entry and above a prompt, and a wizard carrying its own wording
	// would be a second description of one field, drifting from the first the day
	// somebody clarifies either.
	usage string

	// str and boolean locate the field within profileFields. Exactly one is
	// non-nil; the pair is how one table registers both StringVar and BoolVar
	// flags and asks both free-text and yes/no questions without a type switch at
	// every call site.
	str     func(*profileFields) *string
	boolean func(*profileFields) *bool

	// prompts are the backends whose questions include this field, defaulting to
	// the one in backend. It is separate because the two are not the same
	// question: backend says which backend *owns* the flag for a conflict message,
	// and --prefix is owned by s3 there while being a legitimate field of a local
	// profile too. Collapsing them would either stop asking a local profile for
	// its prefix or change what --from-sink says when it refuses one.
	prompts []resolve.BackendKind

	// complete is the flag's shell completion, for the fields whose values are a
	// closed set.
	complete cobra.CompletionFunc
}

// asks reports whether a profile of this backend is asked for this field.
func (f profileField) asks(backend resolve.BackendKind) bool {
	if f.prompts != nil {
		return slices.Contains(f.prompts, backend)
	}
	return f.backend == backend
}

// profileFieldFlags is every per-field flag `config set-profile` carries.
//
// It is the single description of that surface, and four things read it: the flag
// registration, the refusal of a flag --from-sink has no use for, the questions
// the prompting layer asks, and the equivalent command it prints at the end. A
// fifth field added to a profile is one row here and is picked up by all four; a
// field added anywhere else is a field with no flag, no question, or no way to be
// refused — and the first two of those are silent (Invariant 4).
//
// The order is the order `--help` lists the flags in, the order docs/CLI.md's
// table lists them in, and the order the questions are asked in. That last use is
// load-bearing, and it is why each backend's *required* field comes before its
// optional ones: the prompting layer validates the profile it has built so far
// after every answer, and attributing the complaint to the field just typed is
// only sound because every earlier field was already accepted in a profile that
// contained it. --path before --prefix is that rule and not a preference — with
// them the other way round, a local profile's prefix would be refused by a
// sentence about its missing path. See setProfileWizard.askFields.
var profileFieldFlags = []profileField{
	{
		name:  options.FlagBackend,
		usage: fmt.Sprintf("Which backend this profile reads. One of: %s.", options.JoinValues(resolve.BackendKinds)),
		str:   func(f *profileFields) *string { return &f.Backend },
		// Asked by no backend, because it is the question that chooses one. See
		// setProfileWizard.askBackend, which puts it through this same table entry.
		prompts:  []resolve.BackendKind{},
		complete: fixedEnum(resolve.BackendKinds, backendDescriptions),
	},
	{
		name:    options.FlagAddr,
		backend: resolve.BackendClickHouse,
		usage:   "ClickHouse native-protocol endpoint, as host:port.",
		str:     func(f *profileFields) *string { return &f.Addr },
	},
	{
		name:    options.FlagDatabase,
		backend: resolve.BackendClickHouse,
		usage: "ClickHouse database holding the frozen v1 tables. Empty leaves the server's own " +
			"default, which is rarely right: the operator writes to " + resolve.DefaultClickHouseDatabase + ".",
		str: func(f *profileFields) *string { return &f.Database },
	},
	{
		name:    options.FlagUsername,
		backend: resolve.BackendClickHouse,
		// One sentence in three places: `--help`, the typed path's prompt, and the
		// derived branch's prompt, which asks it under a line naming the sink's own
		// user (setProfileWizard.askDerivedCredential). It names the section rather
		// than the page because that section is now runnable — the CREATE USER and
		// GRANT SELECT a reader needs — and "see docs/CLI.md" for a several-hundred
		// line reference is a search rather than an answer.
		usage: "Which ClickHouse user this profile reads as. A read-only user is the " +
			"recommended posture; see " + resolve.DocsReadOnlyUser + ".",
		str: func(f *profileFields) *string { return &f.Username },
	},
	{
		name:    options.FlagPasswordEnv,
		backend: resolve.BackendClickHouse,
		usage:   "Name of an environment variable holding the ClickHouse password.",
		str:     func(f *profileFields) *string { return &f.PasswordEnv },
		// Neither password reference is asked for by field. The two are mutually
		// exclusive and the wizard asks where the password comes from instead, which
		// makes the refused state unreachable rather than reachable and corrected.
		prompts: []resolve.BackendKind{},
	},
	{
		name:    options.FlagPasswordFile,
		backend: resolve.BackendClickHouse,
		usage:   "Path to a file holding the ClickHouse password.",
		str:     func(f *profileFields) *string { return &f.PasswordFile },
		prompts: []resolve.BackendKind{},
	},
	{
		name:    options.FlagTLS,
		backend: resolve.BackendClickHouse,
		usage:   "Connect to ClickHouse over TLS, using the platform's trust store.",
		boolean: func(f *profileFields) *bool { return &f.TLS },
	},
	{
		name:    options.FlagBucket,
		backend: resolve.BackendS3,
		usage:   "S3 bucket holding the archive.",
		str:     func(f *profileFields) *string { return &f.Bucket },
	},
	{
		name:    options.FlagRegion,
		backend: resolve.BackendS3,
		usage:   fmt.Sprintf("Bucket region. Defaults to %s, which MinIO ignores.", resolve.DefaultS3Region),
		str:     func(f *profileFields) *string { return &f.Region },
	},
	{
		name:    options.FlagEndpoint,
		backend: resolve.BackendS3,
		usage:   "S3 API endpoint, with scheme, for MinIO and other S3-compatible stores.",
		str:     func(f *profileFields) *string { return &f.Endpoint },
	},
	{
		name:    options.FlagForcePathStyle,
		backend: resolve.BackendS3,
		usage:   "Address the bucket as <endpoint>/<bucket>/<key>, which most MinIO deployments need.",
		boolean: func(f *profileFields) *bool { return &f.ForcePathStyle },
	},
	{
		name:    options.FlagPath,
		backend: resolve.BackendLocal,
		usage:   "Directory holding a local archive — the one containing format=jsonl-v1/.",
		str:     func(f *profileFields) *string { return &f.Path },
	},
	{
		name:    options.FlagPrefix,
		backend: resolve.BackendS3,
		usage:   "The archive's key prefix within the bucket or directory, with no leading or trailing slash.",
		str:     func(f *profileFields) *string { return &f.Prefix },
		prompts: []resolve.BackendKind{resolve.BackendS3, resolve.BackendLocal},
	},
}

// stanza assembles the profile these answers describe.
//
// It is the only assembler. The flag path calls it with what pflag parsed, the
// prompting layer calls it after every answer, and both are therefore refused by
// one sentence for a backend that is not one of the three — which is what makes
// the shared table in setprofilewizard_internal_test.go able to assert that the
// two routes accept and reject the same inputs at all.
func (f profileFields) stanza() (resolve.Profile, error) {
	profile := resolve.Profile{Backend: resolve.BackendKind(f.Backend)}
	switch profile.Backend {
	case resolve.BackendClickHouse:
		profile.ClickHouse = &resolve.ClickHouseProfile{
			Addr: f.Addr, Database: f.Database, Username: f.Username,
			PasswordEnv: f.PasswordEnv, PasswordFile: f.PasswordFile, TLS: f.TLS,
		}
	case resolve.BackendS3:
		profile.S3 = &resolve.S3Profile{
			Bucket: f.Bucket, Prefix: f.Prefix, Region: f.Region,
			Endpoint: f.Endpoint, ForcePathStyle: f.ForcePathStyle,
		}
	case resolve.BackendLocal:
		profile.Local = &resolve.LocalProfile{Path: f.Path, Prefix: f.Prefix}
	default:
		// Named here because this is where a user who does not know --from-sink
		// exists actually lands: the flag that reads all of these out of the
		// cluster is worth one clause at the moment somebody is typing them by
		// hand.
		return resolve.Profile{}, exit.UsageErrorf("--%s %q is not one of %s; or give --%s <kind>/<name> "+
			"to read the whole stanza from a sink custom resource",
			options.FlagBackend, f.Backend, options.JoinValues(resolve.BackendKinds),
			options.FlagFromSink)
	}
	return profile, nil
}

// validate reports the first thing wrong with these answers, through the
// validator the configuration file itself is read with.
//
// There is deliberately nothing here but the two existing calls. A second
// validator — even one that agreed today — is one that drifts into accepting a
// value the file will later refuse, and the prompt would then be a friendlier way
// to write a profile that does not load.
func (f profileFields) validate() error {
	profile, err := f.stanza()
	if err != nil {
		return err
	}
	return profile.Validate()
}

// equivalent renders the flag command that produces this profile without the
// questions.
//
// It walks the same table the questions came from, so a field that can be asked
// for is a field that appears here — a wizard that could write something its
// printed command could not reproduce would be teaching a flag interface that
// does not exist.
//
// fromSink and addr are handled by the caller rather than read off the struct,
// because the derived route has no --backend and prints --addr on a rule of its
// own. See setProfileWizard.equivalentCommand.
func (f profileFields) equivalent(invokedAs, name string) []string {
	parts := []string{commandNameOr(invokedAs), "config", "set-profile", shellArg(name)}
	for _, field := range profileFieldFlags {
		switch {
		case field.boolean != nil:
			if *field.boolean(&f) {
				parts = append(parts, "--"+field.name)
			}
		case field.str != nil:
			if value := *field.str(&f); value != "" {
				parts = append(parts, "--"+field.name, shellArg(value))
			}
		}
	}
	return parts
}

// shellArg quotes a value that a shell would not read back as one word.
//
// The printed command is meant to be pasted — into a terminal, into a bug report,
// into a CI job — so a password file path with a space in it has to survive the
// round trip. Single quotes because they are literal in every POSIX shell; the
// embedded-quote case is spelled the way shells require rather than escaped,
// since there is no escape for a single quote inside single quotes.
func shellArg(value string) string {
	if value != "" && strings.IndexFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) &&
			!strings.ContainsRune("@%+=:,./-_", r)
	}) < 0 {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

// fromSinkOverrides are the flags --from-sink accepts beside itself, by backend.
//
// Exactly the settings a sink custom resource cannot state, or must not state for
// a reader: the endpoint, which is the field a forwarded port changes; the TLS
// setting, which spec.connection does not carry at all; and the user and
// credential, which should be a read-only ClickHouse user's rather than the
// operator's write credential. An S3Sink has none of these — its bucket, prefix,
// region and endpoint are all facts about where the archive is, and its
// credentials are not in this file at all.
var fromSinkOverrides = map[resolve.BackendKind][]string{
	resolve.BackendClickHouse: {
		options.FlagAddr, options.FlagUsername,
		options.FlagPasswordEnv, options.FlagPasswordFile, options.FlagTLS,
	},
}

// refuseFromSinkConflicts rejects a flag the named sink already answers.
//
// --sink-addr is not among them and is not checked here. It is refused for the
// whole command before any route is chosen, because it names nothing this command
// writes on any of them — see the top of set-profile's RunE.
func refuseFromSinkConflicts(cmd *cobra.Command, ref resolve.SinkRef, backend resolve.BackendKind) error {
	allowed := fromSinkOverrides[backend]
	for _, field := range profileFieldFlags {
		if !cmd.Flags().Changed(field.name) || slices.Contains(allowed, field.name) {
			continue
		}
		return exit.UsageErrorf("--%s %s and --%s cannot be given together: %s",
			options.FlagFromSink, ref, field.name, fromSinkConflict(ref, backend, field.name, field.backend))
	}
	return nil
}

// fromSinkConflict is the "because" half of a refusal, in the reader's terms.
//
// Three reasons, and they send a person to three different places: the kind
// already decided the backend, the flag belongs to a backend this sink is not, or
// the custom resource states the field and a profile disagreeing with it would
// read somewhere other than where the sink writes.
func fromSinkConflict(ref resolve.SinkRef, backend resolve.BackendKind, flag string, owner resolve.BackendKind) string {
	switch {
	case flag == options.FlagBackend:
		return fmt.Sprintf("the kind in --%s decides the backend, and %s writes a %s profile",
			options.FlagFromSink, ref, backend)
	case owner != backend:
		return fmt.Sprintf("--%s configures the %s backend, and %s writes a %s profile",
			flag, owner, ref, backend)
	}
	return fmt.Sprintf("%s states it, and a profile that disagreed with it would read somewhere "+
		"other than where that sink writes", ref)
}

// newConfigUseProfileCommand selects the active profile.
func newConfigUseProfileCommand(flags *options.GlobalFlags, streams genericiooptions.IOStreams) *cobra.Command {
	return &cobra.Command{
		Use:   "use-profile NAME",
		Short: "Make a profile the active one",
		Long: `Make a profile the active one.

The active profile is used when neither --source nor --sink is given, and it
takes precedence over discovering a sink from the cluster. Pass --profile to
override it for a single command.`,

		// One of the two commands whose whole argument is a profile name,
		// completed from the same file --profile is completed from.
		ValidArgsFunction: completeProfileNames,

		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := configFormat("use-profile", flags.Output)
			if err != nil {
				return err
			}
			if len(args) != 1 {
				return exit.UsageErrorf("config use-profile takes one argument, the profile name")
			}
			name := args[0]

			path, err := resolve.DefaultConfigPath()
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			cfg, err := resolve.LoadConfig(path)
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			profile, err := resolve.RequireProfile(cfg, path, name)
			if err != nil {
				return err
			}

			cfg.CurrentProfile = name
			if err := resolve.SaveConfig(path, cfg); err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			if err := options.WriteLine(streams.ErrOut,
				fmt.Sprintf("→ %q is now the active profile", name)); err != nil {
				return err
			}
			// No `previous` stanza: this write displaced nothing. It moved a
			// pointer, and the profile it moved away from is still in the file for
			// anybody who wants it.
			return writeProfileChange(streams.Out, format, profileChangeDocument{
				Action:         profileActivated,
				Name:           name,
				Path:           path,
				Profile:        &profile,
				CurrentProfile: name,
			})
		},
	}
}

// newConfigDeleteProfileCommand removes one profile.
//
// It exists because a tool that creates configuration and cannot remove it is
// incomplete, and because "edit the YAML" is not an answer here: the person who
// needed prompts to write a profile is not the person who should be hand-editing
// one. A stale profile is also actively harmful rather than merely untidy — it
// sits at step 3 of the resolution chain and shadows discovery, which is a
// confusion `config resolve` was partly built to diagnose, and this is the fix a
// user reaches for the moment they have diagnosed it.
//
// The name follows `kubectl config delete-context` rather than inventing
// `remove-profile`, for the reason every other spelling in this tree follows
// kubectl's: a verb somebody has already typed at one Kubernetes CLI should not
// have to be looked up at this one.
func newConfigDeleteProfileCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	var force bool

	command := &cobra.Command{
		Use:   "delete-profile NAME",
		Short: "Remove a profile",
		Long: `Remove a profile from the kuberecord configuration file.

Deleting the active profile is refused unless --force is given, because the
resolution chain would then name a profile that does not exist. With --force the
stanza is removed and the active pointer is cleared, so the next command resolves
through the rest of the chain instead of failing on a profile that is gone.

What was removed is printed, so the deletion is auditable in scrollback: nothing
else holds that stanza once the file is written.`,
		Example: `  kuberecord config delete-profile stale
  kuberecord config delete-profile local --force`,

		ValidArgsFunction: completeProfileNames,

		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := configFormat("delete-profile", flags.Output)
			if err != nil {
				return err
			}
			if len(args) != 1 {
				return exit.UsageErrorf("config delete-profile takes one argument, the profile name")
			}
			name := args[0]

			path, err := resolve.DefaultConfigPath()
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			cfg, err := resolve.LoadConfig(path)
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			removed, err := resolve.RequireProfile(cfg, path, name)
			if err != nil {
				return err
			}

			cleared := cfg.CurrentProfile == name
			if cleared && !force {
				return errDeletingTheActiveProfile(cfg, invokedAs, name)
			}
			delete(cfg.Profiles, name)
			if cleared {
				// Cleared rather than left dangling. SaveConfig validates on the
				// way out and would refuse a currentProfile naming nothing, so this
				// is what makes --force a deletion instead of an error — and it is
				// what stops the next command resolving to a profile that is gone.
				cfg.CurrentProfile = ""
			}

			if err := resolve.SaveConfig(path, cfg); err != nil {
				return exit.RuntimeErrorf("%w", err)
			}

			// Provenance, and the same judgement writeProfile's replacement line
			// makes: the deletion succeeded and was asked for, and what the line
			// carries is the stanza that no longer exists anywhere (D27).
			severity := render.NewSeverity(options.ShouldColorize(flags.Color, streams.ErrOut))
			if err := options.WriteLine(streams.ErrOut, severity.Provenance(
				fmt.Sprintf("→ deleted profile %q from %s (was: %s)",
					name, path, removed.Describe()))); err != nil {
				return err
			}
			if cleared {
				for _, line := range activePointerCleared(cfg, invokedAs) {
					if err := options.WriteLine(streams.ErrOut, line); err != nil {
						return err
					}
				}
			}

			return writeProfileChange(streams.Out, format, profileChangeDocument{
				Action: profileDeleted,
				Name:   name,
				Path:   path,
				// No `profile`: this name carries no stanza any more, and a present
				// but empty one would describe a profile with no backend that the
				// file would itself refuse.
				Previous:       &removed,
				CurrentProfile: cfg.CurrentProfile,
			})
		},
	}

	command.Flags().BoolVar(&force, options.FlagForce, force,
		"Delete the active profile as well, clearing the active pointer. Without it, deleting the "+
			"profile the resolution chain is pointing at is refused.")

	return command
}

// errDeletingTheActiveProfile refuses the one deletion that changes where the
// next command reads from.
//
// It names both routes past itself rather than only --force (D34). They are not
// the same decision: switching first keeps a profile active and is what somebody
// with a replacement wants, while --force leaves the chain to fall through to
// discovery and is what somebody clearing up wants. A message offering only the
// second would push the first person into a state they would then have to undo.
//
// A profile that is the only one in the file is told so instead of being offered a
// switch to nothing, because a remedy naming no command is worse than no remedy:
// it reads as though the reader should have known which name to substitute.
func errDeletingTheActiveProfile(cfg *resolve.Config, invokedAs, name string) error {
	command := commandNameOr(invokedAs)
	others := make([]string, 0, len(cfg.Profiles))
	for _, other := range slices.Sorted(maps.Keys(cfg.Profiles)) {
		if other != name {
			others = append(others, other)
		}
	}
	if len(others) == 0 {
		return exit.UsageErrorf("%q is the active profile and the only one in this file, so there is "+
			"nothing to switch to first: delete it and clear the active pointer with "+
			"`%s config delete-profile %s --%s`, after which the resolution chain falls through to "+
			"discovering a sink from the cluster", name, command, name, options.FlagForce)
	}
	// The rest of the list only when there is a rest of it. With one other profile
	// the suggestion above has already named it, and a trailing "also defined:
	// archive" would be the same word twice in one sentence.
	rest := ""
	if len(others) > 1 {
		rest = fmt.Sprintf(" (also defined: %s)", strings.Join(others[1:], ", "))
	}
	return exit.UsageErrorf("%q is the active profile, and deleting it would leave the resolution "+
		"chain naming a profile that does not exist: either switch first with "+
		"`%s config use-profile %s` and delete it after, or delete it and clear the active pointer "+
		"with `%s config delete-profile %s --%s`%s",
		name, command, others[0], command, name, options.FlagForce, rest)
}

// activePointerCleared says what --force did beyond the deletion.
//
// Two lines rather than one because they answer two questions, and the second is
// only askable when there is something to answer it with: no profile is active
// now, and — if any remain — which command chooses the next one. A deletion that
// left the file with no profiles at all says the first and stops, since the route
// out of that state is `set-profile`, which the resolution chain's own failure
// already names.
func activePointerCleared(cfg *resolve.Config, invokedAs string) []string {
	lines := []string{"→ no profile is active now: the resolution chain falls through to the " +
		"steps after it"}
	if len(cfg.Profiles) > 0 {
		lines = append(lines, fmt.Sprintf("→ to choose another: `%s config use-profile %s`",
			commandNameOr(invokedAs), slices.Sorted(maps.Keys(cfg.Profiles))[0]))
	}
	return lines
}

// newConfigSetContextClusterIDCommand records which kuberecord cluster a
// kubeconfig context reads.
//
// It is the second step of the cluster-id chain, and the one that pays off for an
// engineer working across several clusters: said once per context, and thereafter
// `--context prod-eu` carries the identity with it. D21 is the reason it has to be
// said at all — a kubeconfig context names an API server, and a kuberecord cluster
// identity is a string somebody chose when installing the operator.
func newConfigSetContextClusterIDCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	return &cobra.Command{
		Use:   "set-context-cluster-id [CONTEXT] CLUSTER_ID",
		Short: "Record the kuberecord cluster identity a kubeconfig context reads",
		Long: `Record the kuberecord cluster identity a kubeconfig context reads.

With one argument the current kubeconfig context is used, or the one named by
--context. With two, the context is named explicitly, which is what an engineer
writing several mappings in a row wants.`,
		Example: `  kuberecord config set-context-cluster-id prod-eu-1
  kuberecord config set-context-cluster-id prod-eu prod-eu-1
  kuberecord --context prod-eu config set-context-cluster-id prod-eu-1`,
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := configFormat("set-context-cluster-id", flags.Output)
			if err != nil {
				return err
			}

			var contextName, clusterID string
			switch len(args) {
			case 1:
				resolver := &resolve.BackendResolver{Flags: flags, Streams: streams, InvokedAs: invokedAs}
				contextName, clusterID = resolver.KubeContext(), args[0]
				if contextName == "" {
					return exit.UsageErrorf("no kubeconfig context is current, so there is nothing to map "+
						"%q to; name one: `%s config set-context-cluster-id <context> %s`",
						clusterID, commandNameOr(invokedAs), clusterID)
				}
			case 2:
				contextName, clusterID = args[0], args[1]
			default:
				return exit.UsageErrorf("config set-context-cluster-id takes [CONTEXT] CLUSTER_ID")
			}
			if clusterID == "" {
				return exit.UsageErrorf("the cluster identity is empty")
			}

			path, err := resolve.DefaultConfigPath()
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			cfg, err := resolve.LoadConfig(path)
			if err != nil {
				return exit.RuntimeErrorf("%w", err)
			}
			if cfg.Contexts == nil {
				cfg.Contexts = map[string]string{}
			}
			// Read out before the assignment, for the reason writeProfile reads the
			// displaced stanza out: this write is an upsert too, and the identity it
			// replaces is held nowhere else once the file is saved.
			previous := cfg.Contexts[contextName]
			cfg.Contexts[contextName] = clusterID

			if err := resolve.SaveConfig(path, cfg); err != nil {
				return exit.RuntimeErrorf("%w", err)
			}

			// Remapping a context is the same class of surprising-but-correct
			// outcome as replacing a profile, and it is reported the same way and in
			// the same tier. A mapping is what makes `--context prod-eu` carry an
			// identity, so a reader who has just silently pointed a context at a
			// different cluster's history is a reader who will trust the next answer
			// for the wrong reason.
			confirmation := fmt.Sprintf("→ context %q reads cluster %q", contextName, clusterID)
			if previous != "" && previous != clusterID {
				severity := render.NewSeverity(options.ShouldColorize(flags.Color, streams.ErrOut))
				confirmation = severity.Provenance(confirmation + fmt.Sprintf(" (was: %q)", previous))
			}
			if err := options.WriteLine(streams.ErrOut, confirmation); err != nil {
				return err
			}
			return writeContextMapping(streams.Out, format, contextMappingDocument{
				Context:           contextName,
				ClusterID:         clusterID,
				PreviousClusterID: previous,
				Path:              path,
			})
		},
	}
}

// commandNameOr is the invocation name for a message, defaulting to the
// standalone spelling.
func commandNameOr(invokedAs string) string {
	if invokedAs == "" {
		return options.StandaloneName
	}
	return invokedAs
}
