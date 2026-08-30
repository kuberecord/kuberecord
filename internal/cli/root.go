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
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"
)

// The two names this binary answers to, and the two ways it names itself back.
//
// Both are built from this package: krew installs the plugin binary, a direct
// download or `go install` gets the standalone one, and an engineer who has both
// on their PATH must not get two different help texts from one implementation.
const (
	// StandaloneName is the command as invoked directly.
	StandaloneName = "kuberecord"

	// PluginBinaryName is the file name kubectl requires of a plugin providing
	// the `kuberecord` subcommand. kubectl finds plugins by this convention
	// alone, so the name is fixed by kubectl rather than chosen here.
	PluginBinaryName = "kubectl-kuberecord"

	// PluginInvocation is how that plugin must describe itself: a user who ran
	// `kubectl kuberecord` and is shown `kuberecord timeline …` in an example
	// has been given a command that will not work unless they happen to also
	// have the standalone binary installed.
	PluginInvocation = "kubectl kuberecord"
)

// InvocationName reports how this process was invoked, for use in usage strings.
//
// It reads argv[0] and nothing else. A binary named kubectl-kuberecord is
// assumed to be running as a plugin even when it was executed directly, because
// kubectl passes no marker a plugin could distinguish on, and of the two
// possible mistakes — telling a plugin user to type `kuberecord`, or telling
// someone who invoked the plugin binary directly to type `kubectl kuberecord` —
// only the second names a command that actually works for them.
//
// Windows is handled explicitly rather than through filepath.Base, and both the
// backslash separator and the .exe suffix are dealt with on every platform. Task
// 12.1 ships a windows/amd64 archive, so the behaviour is real; but CI runs on
// Linux, and filepath.Base does not treat a backslash as a separator there, so
// deferring to it would leave the Windows path asserted by nothing. The cost is
// that a Unix file whose name genuinely contains a backslash is read as a path;
// the consequence of that is one cosmetically wrong word in a usage string, which
// is a better trade than an untested platform.
func InvocationName(args []string) string {
	if len(args) == 0 {
		return StandaloneName
	}

	base := args[0]
	if cut := strings.LastIndexAny(base, `/\`); cut >= 0 {
		base = base[cut+1:]
	}
	base = strings.TrimSuffix(base, ".exe")
	if base == PluginBinaryName {
		return PluginInvocation
	}
	return StandaloneName
}

// rootLong is the root command's description, and the place the exit codes are
// documented.
//
// They are in `--help` because that is where someone writing the wrapper script
// will look, and because a contract documented only in a repository file is a
// contract half the people bound by it never read.
const rootLong = `kuberecord answers questions about recorded Kubernetes state changes.

It reads history that a kuberecord operator streamed to a sink — who changed
what, when, and what the object looked like before — without needing the
cluster the change happened in to still exist.

Data is written to stdout and diagnostics to stderr, so structured output can
be piped without a warning banner reaching the pipe.

Exit codes:
  0   success
  1   runtime error: a well-formed request that could not be carried out
  2   usage error: an unknown flag, a malformed object address, a bad value
  3   no coverage: nothing was ever watching the requested scope, which is a
      different fact from "nothing changed" and is reported as one`

// NewRootCommand builds the command tree.
//
// invokedAs is what the command calls itself in usage strings, from
// InvocationName. streams is where output goes; taking it as an argument rather
// than reaching for os.Stdout is what lets a test assert the stdout/stderr split
// rather than trust it.
//
// The returned command silences cobra's own error and usage printing. That is
// not a style preference: cobra writes a usage block to OutOrStderr(), which
// resolves to the command's *out* writer once one is set, so leaving it enabled
// would put usage errors on stdout — the precise thing that must never reach a
// `| jq`. Run prints both, to stderr, instead.
//
// The parsed flag surface is returned alongside the command rather than stashed
// somewhere subcommands can find it. The commands added by later tasks are
// constructed with it as an argument, which keeps the dependency visible in
// their signatures and keeps this package free of a mutable global that two
// concurrently-built roots would share.
func NewRootCommand(invokedAs string, streams genericiooptions.IOStreams) (*cobra.Command, *GlobalFlags) {
	flags := NewGlobalFlags()

	root := &cobra.Command{
		Use:   StandaloneName,
		Short: "Query recorded Kubernetes state changes",
		Long:  rootLong,

		// See the doc comment: cobra's own printing would defeat the
		// stdout/stderr split this CLI is required to keep.
		SilenceUsage:  true,
		SilenceErrors: true,

		// Neither the shell-completion tree nor its help entry earns a place in
		// a plugin's command list; kubectl supplies completion for plugins
		// itself, and `kubectl kuberecord completion bash` would print a script
		// that installs a completion for a command name that does not exist.
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},

		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			return ApplyVerbosity(flags.Verbosity)
		},

		// A stray argument is a usage error, and it is classified here rather than
		// left to cobra. Cobra's own "unknown command" is a plain error, which
		// ExitCodeFor reads as a runtime failure — so without this, a typo would
		// exit 1 and a wrapper script told to retry on 1 and stop on 2 would retry
		// a misspelling forever.
		Args: rejectUnknownSubcommand,

		RunE: func(cmd *cobra.Command, _ []string) error {
			// Bare invocation is a request for help, not a failure: kubectl
			// behaves this way, and exiting non-zero for it would break the
			// habit of typing a command name to remember its flags.
			return cmd.Help()
		},
	}

	// Cobra's display-name annotation is what makes the plugin spelling correct
	// all the way down the tree. Setting Use to "kubectl kuberecord" instead
	// would look right on the root and then render every subcommand's path as
	// "kubectl timeline", because cobra takes a command's name from the first
	// word of Use.
	root.Annotations = map[string]string{cobra.CommandDisplayNameAnnotation: invokedAs}

	root.SetOut(streams.Out)
	root.SetErr(streams.ErrOut)
	root.SetIn(streams.In)

	// An unparseable flag is a usage error. Coding it here rather than at the
	// top of Execute means the classification survives cobra's own error
	// wrapping, which loses the pflag type.
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return &Error{Code: ExitUsageError, Err: err}
	})

	flags.AddFlags(root.PersistentFlags())

	// Commands are added here rather than by the caller so that both binaries —
	// and every test that builds a root — get the same tree, and so that each one
	// is constructed with the same parsed flag surface rather than reaching for a
	// package-level global two concurrently-built roots would share.
	root.AddCommand(
		newTimelineCommand(flags, streams, invokedAs),
		newDiffCommand(flags, streams, invokedAs),
		newGetCommand(flags, streams, invokedAs),
		newBlameCommand(flags, streams, invokedAs),
		newScopesCommand(flags, streams, invokedAs),
		newConfigCommand(flags, streams, invokedAs),
	)

	return root, flags
}

// rejectUnknownSubcommand is the Args validator every command with children uses.
//
// Cobra's built-in validator produces a plain error for an unknown subcommand, and
// a plain error is a runtime failure by ExitCodeFor's reckoning. That is the wrong
// code for a typo: exit 2 is the one that says "you typed something this program
// does not accept", and the distinction exists so that a wrapper script can retry a
// backend timeout without retrying a misspelling.
//
// The suggestions are cobra's own, kept because losing them would be a real
// regression in a tree whose subcommands are the point of the release.
func rejectUnknownSubcommand(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}
	message := fmt.Sprintf("unknown command %q for %q", args[0], cmd.CommandPath())
	if suggestions := cmd.SuggestionsFor(args[0]); len(suggestions) > 0 {
		message += "\n\nDid you mean this?\n\t" + strings.Join(suggestions, "\n\t")
	}
	return UsageErrorf("%s", message)
}
