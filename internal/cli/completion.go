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

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// Completion is static, and that is a decision rather than an omission.
//
// Every candidate produced here comes from a set compiled into the binary or from
// a file on the user's own disk. Nothing in this file contacts an API server or a
// backend. Completing resource kinds from cluster discovery is the obvious next
// idea and is deliberately refused: it would put a network round trip — and a
// kubeconfig, a context, an authentication prompt — behind a TAB press, which is
// a surprising side effect for a keystroke. It is the same instinct D23 applies
// to port-forwarding, one level down: a tool whose value is that it cannot alter
// anything should also be one that cannot contact anything you did not ask it to.
//
// The cost is real and docs/CLI.md states it. The menu offers kubectl's built-in
// short names and no CRD's, so a cluster serving `kubectl get vs` gets no help
// here. Typing the kind in full still works, and always did.
//
// # What the shells get, and what kubectl gets
//
// The four generators below produce a script for the *standalone* name. That is
// not a choice this file makes: cobra binds a generated script to c.Name(), a
// shell function binds to one word, and `kubectl kuberecord` is two. kubectl
// completes its plugins by a different mechanism entirely — an executable named
// kubectl_complete-kuberecord on PATH, which it calls with the words typed so far
// and which speaks the same protocol cobra's hidden __complete command already
// speaks. So the plugin needs no per-shell script at all, it needs one two-line
// shim, and a plugin user who ran this command is told so on stderr rather than
// left with a script that completes a command name they may not have installed.

// completionScript is one shell's generator and the instructions for using it.
//
// A table rather than four hand-written commands because the four differ in
// exactly two things — the generator to call and where the file goes — and the
// parts that must not differ (the argument rule, --no-descriptions, the notice,
// the stdout/stderr split) are then written once. It is also what the test walks,
// so a fifth shell cannot be added with no assertion over it.
type completionScript struct {
	// shell is the subcommand name and the word used in its own help text.
	shell string

	// long is the install instruction, with %[1]s standing in for the standalone
	// command name.
	long string

	// generate writes the script. includeDescriptions is the inverse of
	// --no-descriptions; PowerShell's generator is two functions rather than one
	// parameter, which the wrapper hides.
	generate func(root *cobra.Command, out io.Writer, includeDescriptions bool) error
}

// completionScripts is the shells this CLI generates for, in the order `--help`
// lists them: the three a Unix engineer might have, then Windows.
var completionScripts = []completionScript{
	{
		shell: "bash",
		long: `Generate the completion script for bash.

This depends on the bash-completion package. If it is not installed already,
your OS package manager has it.

To load completions in the current shell:

	source <(%[1]s completion bash)

To load them for every new shell, once:

	# Linux
	%[1]s completion bash > /etc/bash_completion.d/%[1]s

	# macOS, with Homebrew
	%[1]s completion bash > $(brew --prefix)/etc/bash_completion.d/%[1]s`,
		generate: func(root *cobra.Command, out io.Writer, includeDescriptions bool) error {
			return root.GenBashCompletionV2(out, includeDescriptions)
		},
	},
	{
		shell: "zsh",
		long: `Generate the completion script for zsh.

If completion is not enabled in your environment yet, once:

	echo "autoload -U compinit; compinit" >> ~/.zshrc

To load completions in the current shell:

	source <(%[1]s completion zsh)

To load them for every new shell, once:

	# Linux
	%[1]s completion zsh > "${fpath[1]}/_%[1]s"

	# macOS, with Homebrew
	%[1]s completion zsh > $(brew --prefix)/share/zsh/site-functions/_%[1]s`,
		generate: func(root *cobra.Command, out io.Writer, includeDescriptions bool) error {
			if !includeDescriptions {
				return root.GenZshCompletionNoDesc(out)
			}
			return root.GenZshCompletion(out)
		},
	},
	{
		shell: "fish",
		long: `Generate the completion script for fish.

To load completions in the current shell:

	%[1]s completion fish | source

To load them for every new shell, once:

	%[1]s completion fish > ~/.config/fish/completions/%[1]s.fish`,
		generate: func(root *cobra.Command, out io.Writer, includeDescriptions bool) error {
			return root.GenFishCompletion(out, includeDescriptions)
		},
	},
	{
		shell: "powershell",
		long: `Generate the completion script for PowerShell.

To load completions in the current session:

	%[1]s completion powershell | Out-String | Invoke-Expression

To load them for every new session, append that command's output to your
PowerShell profile.`,
		generate: func(root *cobra.Command, out io.Writer, includeDescriptions bool) error {
			if !includeDescriptions {
				return root.GenPowerShellCompletion(out)
			}
			return root.GenPowerShellCompletionWithDesc(out)
		},
	},
}

// kubectlShimCommand writes the executable kubectl completes a plugin from.
//
// kubectl looks for `kubectl_complete-<plugin>` on the PATH and calls it with the
// words typed so far, expecting back what cobra's own __complete command already
// produces — so the whole of a plugin's completion is this redirection. The
// request command is taken from cobra rather than typed, because it is cobra's
// name for cobra's protocol and a copy of it here would be the spelling that
// drifts.
//
// It is a function rather than two strings because both the `completion` help and
// the notice on stderr tell a reader to run it, and a remediation somebody can
// paste has to be one line in one place (Invariant 4).
//
// The `\n` are literal: this is a shell printf's format, not Go's.
func kubectlShimCommand() string {
	return fmt.Sprintf(`printf '#!/usr/bin/env sh\nexec %[1]s %[2]s "$@"\n' \
      > ~/.local/bin/kubectl_complete-%[3]s && chmod +x $_`,
		options.PluginBinaryName, cobra.ShellCompRequestCmd, options.StandaloneName)
}

// pluginCompletionNotice is what a `kubectl kuberecord` user is told, on stderr.
//
// It exists because the script this command writes is correct and is not theirs.
// krew installs the plugin binary alone, so a krew user who redirected this into
// a completion directory would have installed a completion for a command name
// that is not on their PATH — the failure root.go's CompletionOptions comment
// used to avoid by disabling the command outright, which fixed it for the user
// who read the source and for nobody else.
const pluginCompletionNotice = `notice: this script completes the standalone %[1]s command.
  %[2]s is completed by kubectl itself, from an executable named
  kubectl_complete-%[1]s on your PATH — one line, and no per-shell script:

    %[3]s

`

// newCompletionCommand builds the `completion` subtree.
func newCompletionCommand(streams genericiooptions.IOStreams, invokedAs string) *cobra.Command {
	completion := &cobra.Command{
		Use:   "completion",
		Short: "Generate a shell completion script",
		Long: fmt.Sprintf(`Generate a shell completion script for %[1]s.

The script completes subcommands, flags, the values of every flag whose values
are a closed set, the profiles in your configuration file, and kubectl's
built-in resource short names. It contacts no cluster and no backend: every
candidate comes from this binary or from your own configuration file, because a
TAB press is not a request to open a connection.

%[2]s is completed by kubectl rather than by a script from
here: a shell function binds to a single word, and that one is two. kubectl
completes a plugin from an executable on your PATH, which is one redirection
and no per-shell script:

    %[3]s`,
			options.StandaloneName, options.PluginInvocation, kubectlShimCommand()),

		Args:              rejectUnknownSubcommand,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}

	for _, script := range completionScripts {
		completion.AddCommand(newCompletionScriptCommand(script, streams, invokedAs))
	}
	return completion
}

// newCompletionScriptCommand builds one shell's subcommand.
func newCompletionScriptCommand(
	script completionScript, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	var noDescriptions bool

	command := &cobra.Command{
		Use:                   script.shell,
		Short:                 fmt.Sprintf("Generate the completion script for %s", script.shell),
		Long:                  fmt.Sprintf(script.long, options.StandaloneName),
		DisableFlagsInUseLine: true,
		ValidArgsFunction:     cobra.NoFileCompletions,

		// Cobra's own NoArgs would make a stray word a runtime failure; exit 2 is
		// the code that says "you typed something this program does not accept".
		// See rejectPositionalArgs for the same reasoning at a command whose
		// message differs.
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return nil
			}
			return exit.UsageErrorf("%s takes no arguments, and %q is not one: it writes one shell's "+
				"completion script to standard output", cmd.CommandPath(), args[0])
		},

		RunE: func(cmd *cobra.Command, _ []string) error {
			// Before the script, so that a reader who redirected stdout still sees
			// it, and so it cannot land in the middle of a half-written script.
			noteCompletionInvocation(streams.ErrOut, invokedAs)

			// cmd.Root() rather than cmd: a completion script is generated for the
			// whole tree, and cobra binds it to the root's Name() — which is the
			// standalone name under both invocations, by design. See the file's
			// opening comment.
			if err := script.generate(cmd.Root(), streams.Out, !noDescriptions); err != nil {
				return exit.RuntimeErrorf("generating the %s completion script: %w", script.shell, err)
			}
			return nil
		},
	}

	command.Flags().BoolVar(&noDescriptions, options.FlagNoDescriptions, noDescriptions,
		"Omit the description shown beside each candidate. bash appends a description to the "+
			"candidate itself rather than showing it in a second column, so a long menu is easier "+
			"to read without them.")

	return command
}

// noteCompletionInvocation tells a plugin user what the script on stdout is for.
//
// The write is checked and deliberately not propagated, exactly as
// BackendResolver.notef's is: a failed write to stderr means stderr itself has
// gone, there is nowhere left to report that, and the script the command was
// asked for is unaffected and still belongs on stdout.
func noteCompletionInvocation(errOut io.Writer, invokedAs string) {
	if errOut == nil || invokedAs != options.PluginInvocation {
		return
	}
	_, _ = fmt.Fprintf(errOut, pluginCompletionNotice,
		options.StandaloneName, options.PluginInvocation, kubectlShimCommand())
}

// mustCompleteFlag registers fn as the completion for a flag, or panics.
//
// It panics because the two ways cobra can refuse are both programming errors
// with no user-facing repair: the flag name does not exist on this command, or a
// completion for it was registered twice. Neither depends on input, an
// environment or a cluster, so a build that would fail once fails every time —
// and every test in this package builds a command tree, so the first `go test`
// after such a mistake is where it surfaces. The alternatives were worse:
// discarding the error is forbidden here, and failing a `timeline` because a
// completion menu could not be wired would trade an answer for a convenience.
func mustCompleteFlag(cmd *cobra.Command, flag string, fn cobra.CompletionFunc) {
	if err := cmd.RegisterFlagCompletionFunc(flag, fn); err != nil {
		panic(fmt.Sprintf("registering shell completion for --%s on %q: %v", flag, cmd.CommandPath(), err))
	}
}

// registerGlobalCompletions attaches completions to the persistent flag surface.
//
// It is called from NewRootCommand, beside the registration of the flags
// themselves, so that a flag and the menu it offers stay in one place. The
// per-command flags are registered the same way at their own sites: --backend and
// --from-sink in configcmd.go, --kind in scopes.go.
//
// Cobra keys these by *pflag.Flag, and the persistent flags are one set shared
// with every subcommand, so registering once on the root serves the whole tree.
func registerGlobalCompletions(root *cobra.Command) {
	mustCompleteFlag(root, options.FlagOutput, fixedEnum(options.OutputFormats(), outputFormatDescriptions))
	mustCompleteFlag(root, options.FlagColor, fixedEnum(options.ColorModes(), colorModeDescriptions))
	mustCompleteFlag(root, options.FlagProfile, completeProfileNames)
	mustCompleteFlag(root, options.FlagSink, completeSinkRefs)
}

// outputFormatDescriptions is the one-line gloss shown beside each --output value.
//
// The set itself lives in options, which is the only place that may grow it; this
// map only describes it, and a test fails if the two disagree. Splitting them that
// way keeps the accepted vocabulary in the package that validates it, rather than
// making a completion menu a second definition of what --output accepts.
var outputFormatDescriptions = map[options.OutputFormat]string{
	options.OutputTable: "aligned columns, for a terminal",
	options.OutputWide:  "the table, with the columns that did not fit",
	options.OutputJSON:  "the versioned envelope, one document",
	options.OutputJSONL: "one item per line, streamed",
	options.OutputYAML:  "the versioned envelope, as YAML",
	options.OutputDiff:  "the patch-oriented rendering",
}

// colorModeDescriptions is the same for --color. `auto` names NO_COLOR because
// that is the half of it people forget.
var colorModeDescriptions = map[options.ColorMode]string{
	options.ColorAuto:   "colour a terminal, unless NO_COLOR is set",
	options.ColorAlways: "colour even into a pipe, and over NO_COLOR",
	options.ColorNever:  "never colour",
}

// backendDescriptions is the same for `config set-profile --backend`, naming what
// each one reads rather than restating its own name.
var backendDescriptions = map[resolve.BackendKind]string{
	resolve.BackendClickHouse: "the frozen v1 schema in a ClickHouse instance",
	resolve.BackendS3:         "a jsonl-v1 archive in an S3-compatible bucket",
	resolve.BackendLocal:      "a jsonl-v1 archive in a local directory",
}

// fixedEnum turns a closed set and its glosses into a completion function.
//
// The candidates are built once, at registration, because the set cannot change
// after that; the shell filters them by prefix itself.
func fixedEnum[T ~string](values []T, descriptions map[T]string) cobra.CompletionFunc {
	candidates := make([]cobra.Completion, 0, len(values))
	for _, value := range values {
		candidates = append(candidates, cobra.CompletionWithDesc(string(value), descriptions[value]))
	}
	// NoFileComp rather than the default: without it a shell that finds no match
	// falls back to offering the files in the working directory, which for a flag
	// with six legal values is an answer that is never right.
	return cobra.FixedCompletions(candidates, cobra.ShellCompDirectiveNoFileComp)
}

// completeProfileNames offers the profiles the configuration file defines.
//
// It reads a file the user owns and contacts nothing, which is what keeps it
// inside the no-side-effects rule this whole file is written to.
//
// Both failures — no home directory to resolve the path against, an unreadable or
// invalid file — end the same way, with no candidates and no message. That is not
// a silent error in the sense Invariant 4 forbids: the same file is loaded by
// every command that resolves a backend, and each reports what is wrong with it
// loudly and at length. A TAB press is not a request for a diagnostic, the
// completion protocol gives it nowhere to put one (the shell scripts discard
// stderr by design), and a shell that printed a parse error over a half-typed
// command line would be worse than one that simply offered nothing.
func completeProfileNames(
	_ *cobra.Command, args []string, toComplete string,
) ([]cobra.Completion, cobra.ShellCompDirective) {
	// Guards `config use-profile NAME`, which takes exactly one: past the first
	// argument there is nothing left to name.
	if len(args) > 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	path, err := resolve.DefaultConfigPath()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	config, err := resolve.LoadConfig(path)
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	candidates := make([]cobra.Completion, 0, len(config.Profiles))
	for _, name := range slices.Sorted(maps.Keys(config.Profiles)) {
		if !strings.HasPrefix(name, toComplete) {
			continue
		}
		// The backend, and nothing else. A profile cannot hold a credential by
		// construction (resolve.ClickHouseProfile.Password), but it does hold the
		// address and user that describe how to obtain one, and none of that
		// belongs in a menu drawn over somebody's shoulder.
		description := string(config.Profiles[name].Backend)
		if name == config.CurrentProfile {
			description += ", the active profile"
		}
		candidates = append(candidates, cobra.CompletionWithDesc(name, description))
	}
	return candidates, cobra.ShellCompDirectiveNoFileComp
}

// completeSinkRefs offers the closed half of a --sink value.
//
// A sink is addressed as Kind/name. The kinds are a compiled-in pair; the names
// are objects in a cluster, and reading them is precisely the discovery this file
// refuses to do behind a keystroke. So the menu completes the kind and the slash
// and then stops, which is the honest amount of help available offline.
//
// NoSpace is what makes that work: without it the shell would put a space after
// the slash and the user would be typing the name as a second argument.
func completeSinkRefs(
	_ *cobra.Command, _ []string, toComplete string,
) ([]cobra.Completion, cobra.ShellCompDirective) {
	if strings.Contains(toComplete, "/") {
		// The kind is settled and the name is the cluster's to know.
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return []cobra.Completion{
			cobra.CompletionWithDesc(resolve.KindClickHouseSink+"/", "then the sink's own name"),
			cobra.CompletionWithDesc(resolve.KindS3Sink+"/", "then the sink's own name"),
		},
		cobra.ShellCompDirectiveNoSpace | cobra.ShellCompDirectiveNoFileComp
}

// shortName is one entry of the completion-only resource table.
type shortName struct {
	// Short is what a user types: kubectl's own abbreviation.
	Short string

	// Kind is how this CLI names that kind everywhere else — group/Kind, or a
	// bare Kind for the core group, as describeKind and render.ScopeKind spell it.
	// It is the description rather than a second candidate, so that the menu stays
	// one line per kind while still teaching the spelling that needs no cluster.
	Kind string
}

// resourceShortNames is the static table completion offers for a resource kind.
//
// # Why it exists at all
//
// The CLI does not otherwise carry one. Argument parsing resolves `deploy`
// through cli-runtime's RESTMapper, wrapped in restmapper.ShortcutExpander, which
// reads each resource's ShortNames out of the server's *own discovery data* — the
// same code path and the same data kubectl uses, which is what lets a CRD's short
// names work here without this repository knowing about them (see
// resource.go:NewResolver, and docs/CLI.md on reading an archive without a
// cluster). client-go carries no static table to borrow.
//
// So this is a new table, for completion and for nothing else. It is never
// consulted when resolving an address: a menu that suggested `deploy` and a
// resolver that accepted it must not be able to disagree, and the only way to
// guarantee that is for one of them to have no opinion.
//
// # What is in it
//
// kubectl's built-in short names, and only those. Deprecated and removed kinds
// are left out — a menu is a recommendation, and `psp` names something no
// supported cluster serves. Nothing here names a kind the operator refuses to
// watch: v1/Secret is hard-denied (D8) and has no short name to begin with, which
// a test asserts stays true.
//
// Sorted by the short name, which is the order the menu is read in.
var resourceShortNames = []shortName{
	{Short: "cj", Kind: "batch/CronJob"},
	{Short: "cm", Kind: "ConfigMap"},
	{Short: "crd", Kind: "apiextensions.k8s.io/CustomResourceDefinition"},
	{Short: "csr", Kind: "certificates.k8s.io/CertificateSigningRequest"},
	{Short: "deploy", Kind: "apps/Deployment"},
	{Short: "ds", Kind: "apps/DaemonSet"},
	{Short: "ep", Kind: "Endpoints"},
	{Short: "ev", Kind: "Event"},
	{Short: "hpa", Kind: "autoscaling/HorizontalPodAutoscaler"},
	{Short: "ing", Kind: "networking.k8s.io/Ingress"},
	{Short: "limits", Kind: "LimitRange"},
	{Short: "netpol", Kind: "networking.k8s.io/NetworkPolicy"},
	{Short: "no", Kind: "Node"},
	{Short: "ns", Kind: "Namespace"},
	{Short: "pc", Kind: "scheduling.k8s.io/PriorityClass"},
	{Short: "pdb", Kind: "policy/PodDisruptionBudget"},
	{Short: "po", Kind: "Pod"},
	{Short: "pv", Kind: "PersistentVolume"},
	{Short: "pvc", Kind: "PersistentVolumeClaim"},
	{Short: "quota", Kind: "ResourceQuota"},
	{Short: "rc", Kind: "ReplicationController"},
	{Short: "rs", Kind: "apps/ReplicaSet"},
	{Short: "sa", Kind: "ServiceAccount"},
	{Short: "sc", Kind: "storage.k8s.io/StorageClass"},
	{Short: "sts", Kind: "apps/StatefulSet"},
	{Short: "svc", Kind: "Service"},
}

// completeShortNames returns the table's entries matching what has been typed.
func completeShortNames(toComplete string) []cobra.Completion {
	candidates := make([]cobra.Completion, 0, len(resourceShortNames))
	for _, entry := range resourceShortNames {
		if !strings.HasPrefix(entry.Short, toComplete) {
			continue
		}
		candidates = append(candidates, cobra.CompletionWithDesc(entry.Short, entry.Kind))
	}
	return candidates
}

// completeObjectAddress is the ValidArgsFunction of every command that addresses
// one object.
//
// It completes the kind and stops. The two accepted spellings are `kind/name` and
// `kind name` (ParseResourceArg), and in both the name is an object in a cluster
// or an archive — neither of which may be read from here. So a token that already
// carries a slash, and every argument after the first, are answered with nothing
// rather than with the working directory's files.
func completeObjectAddress(
	_ *cobra.Command, args []string, toComplete string,
) ([]cobra.Completion, cobra.ShellCompDirective) {
	if len(args) > 0 || strings.Contains(toComplete, "/") {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return completeShortNames(toComplete), cobra.ShellCompDirectiveNoFileComp
}

// completeResourceKind is the flag-shaped half of the same thing, for `scopes
// --kind`, which takes the address's kind token and no name.
func completeResourceKind(
	_ *cobra.Command, _ []string, toComplete string,
) ([]cobra.Completion, cobra.ShellCompDirective) {
	return completeShortNames(toComplete), cobra.ShellCompDirectiveNoFileComp
}
