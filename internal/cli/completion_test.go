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

package cli_test

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// Shell completion (Task 14.3).
//
// The tests drive the two halves separately because a user meets them
// separately: `completion <shell>` writes a script once, and __complete answers
// every TAB press thereafter. Both go through cli.Run, which is the path a shell
// actually takes — a test that called cobra's generator directly would assert
// that cobra works, which is not this repository's question.
//
// There is deliberately no golden file over a generated script. Sixteen kilobytes
// of cobra's own shell is not this project's rendering, and pinning it would turn
// every cobra bump into a golden-file review of somebody else's code while saying
// nothing about the two things that can actually go wrong here: which command
// name the script binds, and whether it calls back into this binary. Those are
// asserted by name. What *is* this project's rendering — the menu behind every
// keystroke — is asserted value by value below.

// runAs is run() with control over argv[0], which is what decides whether the
// binary calls itself `kuberecord` or `kubectl kuberecord`.
func runAs(t *testing.T, argv0 string, args ...string) (stdout, stderr string, code int) {
	t.Helper()

	io, out, errOut := streams()
	code = cli.Run(append([]string{argv0}, args...), io)
	return out.String(), errOut.String(), code
}

// completionResult is one answer to a TAB press, decoded from the protocol
// __complete speaks: one candidate per line, each optionally carrying a
// tab-separated description, then a final line of ":" and the directive bitmap.
type completionResult struct {
	values       []string
	descriptions map[string]string
	directive    cobra.ShellCompDirective
}

// completeThrough drives the hidden __complete command the way a shell does.
//
// The empty final argument is what a shell sends for "the word under the cursor
// is empty", and it is passed by the caller rather than added here so that a test
// can also ask what a half-typed word completes to.
func completeThrough(t *testing.T, args ...string) completionResult {
	t.Helper()

	stdout, _, code := run(t, append([]string{"__complete"}, args...)...)
	if code != exit.Success {
		t.Fatalf("__complete %v exited %d:\n%s", args, code, stdout)
	}

	result := completionResult{descriptions: map[string]string{}, directive: -1}
	for line := range strings.SplitSeq(strings.TrimRight(stdout, "\n"), "\n") {
		if directive, found := strings.CutPrefix(line, ":"); found {
			parsed, err := strconv.Atoi(directive)
			if err != nil {
				t.Fatalf("__complete %v ended with an unparseable directive %q", args, line)
			}
			result.directive = cobra.ShellCompDirective(parsed)
			continue
		}
		value, description, _ := strings.Cut(line, "\t")
		result.values = append(result.values, value)
		result.descriptions[value] = description
	}
	if result.directive < 0 {
		t.Fatalf("__complete %v printed no directive line:\n%s", args, stdout)
	}
	return result
}

// TestCompletionGeneratesAScriptForEveryShell is the acceptance criterion's first
// half: each generator runs, and what it produces is bound to the name a user can
// actually type.
//
// The marker asserted per shell is that binding, because a script that generates
// cleanly and registers itself against the wrong command name is the failure this
// task's design question was about. Every one of them also has to reach back into
// the binary through __complete, which is what makes the script live rather than
// a static list frozen at generation time.
func TestCompletionGeneratesAScriptForEveryShell(t *testing.T) {
	tests := []struct {
		shell string
		bound string
	}{
		{"bash", "__start_kuberecord kuberecord"},
		{"zsh", "#compdef kuberecord"},
		{"fish", "complete -c kuberecord"},
		{"powershell", "Register-ArgumentCompleter -CommandName 'kuberecord'"},
	}

	for _, test := range tests {
		t.Run(test.shell, func(t *testing.T) {
			stdout, stderr, code := run(t, "completion", test.shell)
			if code != exit.Success {
				t.Fatalf("exit code = %d, want %d\nstderr: %s", code, exit.Success, stderr)
			}
			if !strings.Contains(stdout, test.bound) {
				t.Errorf("the %s script never binds %q; it completes some other command", test.shell, test.bound)
			}
			if !strings.Contains(stdout, cobra.ShellCompRequestCmd) {
				t.Errorf("the %s script never calls %s, so it cannot ask the binary anything",
					test.shell, cobra.ShellCompRequestCmd)
			}
			// The script is data. A notice on stderr is fine; a word of it on
			// stdout would end up inside the file a user just redirected.
			if stderr != "" {
				t.Errorf("standalone `completion %s` wrote to stderr: %s", test.shell, stderr)
			}
		})
	}
}

// TestCompletionNoDescriptionsAsksForTheOtherRequest checks the flag does the one
// thing it exists for.
//
// The two spellings are cobra's own hidden commands: with descriptions the script
// calls __complete, without them __completeNoDesc. Asserting the request the
// script makes is the only way to see the difference from outside, since the
// descriptions themselves are never in the script — they are fetched per
// keystroke.
func TestCompletionNoDescriptionsAsksForTheOtherRequest(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		t.Run(shell, func(t *testing.T) {
			with, _, code := run(t, "completion", shell)
			if code != exit.Success {
				t.Fatalf("exit code = %d", code)
			}
			without, _, code := run(t, "completion", shell, "--"+options.FlagNoDescriptions)
			if code != exit.Success {
				t.Fatalf("exit code = %d with --%s", code, options.FlagNoDescriptions)
			}

			if strings.Contains(with, cobra.ShellCompNoDescRequestCmd) {
				t.Errorf("the default %s script asks for %s, so descriptions are off by default",
					shell, cobra.ShellCompNoDescRequestCmd)
			}
			if !strings.Contains(without, cobra.ShellCompNoDescRequestCmd) {
				t.Errorf("--%s did not change what the %s script requests",
					options.FlagNoDescriptions, shell)
			}
		})
	}
}

// TestCompletionNotesThePluginRoute is why the command is no longer disabled.
//
// A krew install puts only kubectl-kuberecord on the PATH, so the script this
// command writes completes a name that user may not have. Refusing to write it
// would punish everyone who installed both names; writing it silently is what the
// old CompletionOptions comment objected to. The notice is the third option, and
// it has to name the executable kubectl actually looks for — a message that
// merely said "this will not work" would be the uninformative kind Invariant 4
// exists to forbid.
func TestCompletionNotesThePluginRoute(t *testing.T) {
	stdout, stderr, code := runAs(t, options.PluginBinaryName, "completion", "bash")
	if code != exit.Success {
		t.Fatalf("exit code = %d, want %d\nstderr: %s", code, exit.Success, stderr)
	}
	if !strings.Contains(stdout, "__start_kuberecord") {
		t.Error("the plugin invocation stopped writing a script; the notice is a note, not a refusal")
	}
	for _, want := range []string{
		"kubectl_complete-" + options.StandaloneName,
		options.PluginBinaryName + " " + cobra.ShellCompRequestCmd,
		options.PluginInvocation,
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the notice never mentions %q, so it names the problem and not the fix:\n%s",
				want, stderr)
		}
	}

	// The same line is in `completion --help`, which is where a reader who has
	// not yet run anything will look for it. Both are rendered from one function,
	// and this is what keeps that true.
	help, _, code := run(t, "completion", "--help")
	if code != exit.Success {
		t.Fatalf("`completion --help` exited %d", code)
	}
	shim := "kubectl_complete-" + options.StandaloneName
	if !strings.Contains(help, shim) {
		t.Errorf("`completion --help` never names %s, and its own text promises it:\n%s", shim, help)
	}

	// And the same command under the standalone name says none of it: the script
	// it wrote is that user's own, and a notice about somebody else's install is
	// noise on the stderr of a working command.
	_, standaloneStderr, code := run(t, "completion", "bash")
	if code != exit.Success {
		t.Fatalf("standalone exit code = %d", code)
	}
	if standaloneStderr != "" {
		t.Errorf("the standalone invocation carries a notice it has no use for:\n%s", standaloneStderr)
	}
}

// TestCompletionTakesNoArguments keeps a typo an exit 2 rather than an exit 1.
func TestCompletionTakesNoArguments(t *testing.T) {
	_, stderr, code := run(t, "completion", "bash", "extra")
	if code != exit.UsageError {
		t.Errorf("exit code = %d, want %d\nstderr: %s", code, exit.UsageError, stderr)
	}
}

// TestCompletionIsInTheCommandList keeps it discoverable.
func TestCompletionIsInTheCommandList(t *testing.T) {
	stdout, _, code := run(t, "--help")
	if code != exit.Success {
		t.Fatalf("exit code = %d", code)
	}
	if !strings.Contains(stdout, "completion") {
		t.Errorf("`--help` does not list `completion`:\n%s", stdout)
	}
}

// TestEnumeratedFlagsCompleteTheirValues is the acceptance criterion's second
// half, and the part a user feels on every keystroke.
//
// The expected sets are read from the packages that define them rather than
// retyped here, so that a seventh output format or a fourth backend fails this
// test by being absent from the menu rather than by being absent from a literal
// somebody forgot to update.
func TestEnumeratedFlagsCompleteTheirValues(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "--" + options.FlagOutput,
			args: []string{"--" + options.FlagOutput, ""},
			want: stringsOf(options.OutputFormats()),
		},
		{
			name: "--" + options.FlagColor,
			args: []string{"--" + options.FlagColor, ""},
			want: stringsOf(options.ColorModes()),
		},
		{
			name: "--" + options.FlagBackend,
			args: []string{"config", "set-profile", "example", "--" + options.FlagBackend, ""},
			want: stringsOf(resolve.BackendKinds),
		},
		{
			// The closed half of a Kind/name value. The name is an object in a
			// cluster and is deliberately not offered.
			name: "--" + options.FlagSink,
			args: []string{"--" + options.FlagSink, ""},
			want: []string{resolve.KindClickHouseSink + "/", resolve.KindS3Sink + "/"},
		},
		{
			name: "--" + options.FlagFromSink,
			args: []string{"config", "set-profile", "example", "--" + options.FlagFromSink, ""},
			want: []string{resolve.KindClickHouseSink + "/", resolve.KindS3Sink + "/"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := completeThrough(t, test.args...)
			if !slices.Equal(result.values, test.want) {
				t.Errorf("completed %v, want %v", result.values, test.want)
			}
			// Without this a shell offers the working directory's files when the
			// prefix matches nothing, which for a closed set is never right.
			if result.directive&cobra.ShellCompDirectiveNoFileComp == 0 {
				t.Errorf("directive %v does not include NoFileComp, so a miss falls back to files",
					result.directive)
			}
			// Every value is glossed. An unexplained `jsonl` beside an
			// unexplained `json` is a menu that has to be looked up elsewhere.
			for _, value := range result.values {
				if result.descriptions[value] == "" {
					t.Errorf("%q completes with no description", value)
				}
			}
		})
	}
}

// TestSinkCompletionStopsAtTheSlash is the boundary between what is compiled in
// and what would need a cluster.
func TestSinkCompletionStopsAtTheSlash(t *testing.T) {
	result := completeThrough(t, "--"+options.FlagSink, resolve.KindClickHouseSink+"/")
	if len(result.values) != 0 {
		t.Errorf("completed %v after the slash; the sink's name is the cluster's to know", result.values)
	}
	if result.directive&cobra.ShellCompDirectiveNoFileComp == 0 {
		t.Errorf("directive %v does not include NoFileComp, so the shell offers files instead",
			result.directive)
	}

	// And before the slash, NoSpace: without it the shell separates the kind from
	// the name it is waiting for.
	before := completeThrough(t, "--"+options.FlagSink, "")
	if before.directive&cobra.ShellCompDirectiveNoSpace == 0 {
		t.Errorf("directive %v does not include NoSpace, so `ClickHouseSink/` completes to "+
			"`ClickHouseSink/ `", before.directive)
	}
}

// TestObjectCommandsCompleteTheKind covers the four commands that address one
// object, and the two spellings each of them accepts.
func TestObjectCommandsCompleteTheKind(t *testing.T) {
	for _, command := range []string{"timeline", "diff", "get", "blame"} {
		t.Run(command, func(t *testing.T) {
			// A short prefix narrows to the one kind it names, with the recorded
			// spelling as its description — which is the spelling that needs no
			// cluster, and the reason the description is worth carrying.
			result := completeThrough(t, command, "deplo")
			if !slices.Equal(result.values, []string{"deploy"}) {
				t.Errorf("`%s deplo` completed %v, want [deploy]", command, result.values)
			}
			if got := result.descriptions["deploy"]; got != "apps/Deployment" {
				t.Errorf("deploy is described as %q, want %q", got, "apps/Deployment")
			}

			// `kind/name`: the kind is settled and the name is not this CLI's to
			// guess, offline.
			slashed := completeThrough(t, command, "deploy/check")
			if len(slashed.values) != 0 {
				t.Errorf("`%s deploy/check` completed %v; an object name needs a cluster",
					command, slashed.values)
			}

			// `kind name`: the second positional is the same name, spelled apart.
			second := completeThrough(t, command, "deploy", "")
			if len(second.values) != 0 {
				t.Errorf("`%s deploy ''` completed %v; an object name needs a cluster",
					command, second.values)
			}
			if second.directive&cobra.ShellCompDirectiveNoFileComp == 0 {
				t.Errorf("directive %v does not include NoFileComp, so a name completes to "+
					"the files in the working directory", second.directive)
			}
		})
	}
}

// TestScopesCompletesItsKindFlag: --kind takes the address's kind token and no
// name, so it gets the same table.
func TestScopesCompletesItsKindFlag(t *testing.T) {
	result := completeThrough(t, "scopes", "--kind", "st")
	if !slices.Equal(result.values, []string{"sts"}) {
		t.Errorf("`scopes --kind st` completed %v, want [sts]", result.values)
	}
}

// TestShortNameTableIsAMenuAndNotAContract states the two properties the table
// has to keep.
//
// Sorted, because the order it is written in is the order it is read in. And free
// of anything the operator refuses to record: v1/Secret is hard-denied (D8), and a
// menu offering a route to it would be advertising an empty answer.
func TestShortNameTableIsAMenuAndNotAContract(t *testing.T) {
	result := completeThrough(t, "timeline", "")
	if len(result.values) < 20 {
		t.Fatalf("the short-name table offered %d entries; that is not kubectl's set",
			len(result.values))
	}
	if !slices.IsSorted(result.values) {
		t.Errorf("the menu is not sorted: %v", result.values)
	}
	for _, value := range result.values {
		if strings.Contains(result.descriptions[value], "Secret") {
			t.Errorf("%q completes to %q, and v1/Secret is a kind this operator refuses to watch (D8)",
				value, result.descriptions[value])
		}
	}
	// The four the acceptance criterion names by hand, so that a table rewritten
	// from some other source still has to contain them.
	for _, want := range []string{"deploy", "sts", "cm", "ing"} {
		if !slices.Contains(result.values, want) {
			t.Errorf("the menu no longer offers %q", want)
		}
	}
}

// TestProfileCompletionReadsTheConfigFile is the one dynamic source, and the
// reason it is allowed: a file the user owns, not a cluster.
func TestProfileCompletionReadsTheConfigFile(t *testing.T) {
	path := configHome(t)
	writeConfigFile(t, path, `apiVersion: cli.kuberecord.io/v1alpha1
kind: Config
currentProfile: prod
profiles:
  prod:
    backend: clickhouse
    clickhouse:
      addr: clickhouse.example:9000
      username: kuberecord_ro
      passwordEnv: KUBERECORD_CLICKHOUSE_PASSWORD
  archive:
    backend: s3
    s3:
      bucket: acme-audit
`)

	for _, args := range [][]string{
		{"--" + options.FlagProfile, ""},
		{"config", "use-profile", ""},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			result := completeThrough(t, args...)
			if !slices.Equal(result.values, []string{"archive", "prod"}) {
				t.Errorf("completed %v, want [archive prod]", result.values)
			}
			// The backend, so a menu of four profiles says which is which — and
			// the active one is marked, because that is the one an omitted
			// --profile would have used anyway.
			if got := result.descriptions["archive"]; got != string(resolve.BackendS3) {
				t.Errorf("archive is described as %q, want %q", got, resolve.BackendS3)
			}
			if !strings.Contains(result.descriptions["prod"], "active") {
				t.Errorf("prod is described as %q and does not say it is the active profile",
					result.descriptions["prod"])
			}

			// Nothing that describes how to obtain a credential. The file cannot
			// hold one, but it does hold the address, the user and the name of the
			// variable the password comes from, and none of that belongs in a menu.
			for _, secret := range []string{"clickhouse.example", "kuberecord_ro", "PASSWORD"} {
				for value, description := range result.descriptions {
					if strings.Contains(description, secret) {
						t.Errorf("%q completes with %q, which names %q", value, description, secret)
					}
				}
			}
		})
	}
}

// TestProfileCompletionIsSilentWithoutAUsableFile: the ordinary state of a first
// invocation is no file at all, and a shell that printed a diagnostic over a
// half-typed command line would be worse than one that offered nothing. A broken
// file is reported, at length, by every command that resolves a backend.
func TestProfileCompletionIsSilentWithoutAUsableFile(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{name: "no file at all"},
		{name: "a file this build will not read", content: "apiVersion: cli.kuberecord.io/v9\nkind: Config\n"},
		{name: "a file that is not YAML", content: "{{{\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := configHome(t)
			if test.content != "" {
				writeConfigFile(t, path, test.content)
			}

			result := completeThrough(t, "--"+options.FlagProfile, "")
			if len(result.values) != 0 {
				t.Errorf("completed %v from an unusable configuration", result.values)
			}
			if result.directive&cobra.ShellCompDirectiveNoFileComp == 0 {
				t.Errorf("directive %v does not include NoFileComp", result.directive)
			}
		})
	}
}

// TestCompletionContactsNothing is the property that keeps a TAB press free of
// side effects — the same instinct D23 applies to port-forwarding, one level down.
//
// A kubeconfig that does not exist would fail any command that resolves a
// backend, so completions that still answer here have resolved nothing. The
// generators are included because a script is written by the same binary.
func TestCompletionContactsNothing(t *testing.T) {
	t.Setenv("KUBECONFIG", "/nonexistent/kubeconfig-for-a-completion-test")

	if result := completeThrough(t, "timeline", ""); len(result.values) == 0 {
		t.Error("resource kinds completed to nothing without a cluster; the table is not static")
	}
	if result := completeThrough(t, "--"+options.FlagOutput, ""); len(result.values) == 0 {
		t.Error("--output completed to nothing without a cluster")
	}
	if _, stderr, code := run(t, "completion", "bash"); code != exit.Success {
		t.Errorf("`completion bash` exited %d without a cluster: %s", code, stderr)
	}
}

// stringsOf renders a closed set of string-kinded values for comparison against
// what a shell was offered.
func stringsOf[T ~string](values []T) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, string(value))
	}
	return out
}
