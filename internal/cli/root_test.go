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
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
)

// streams returns an IOStreams whose three halves can be inspected separately.
//
// Separately is the whole point: every assertion in this file about where output
// went would pass against a single combined buffer, and the property under test
// is precisely that the two do not mix.
func streams() (genericiooptions.IOStreams, *bytes.Buffer, *bytes.Buffer) {
	var out, errOut bytes.Buffer
	return genericiooptions.IOStreams{
		In:     strings.NewReader(""),
		Out:    &out,
		ErrOut: &errOut,
	}, &out, &errOut
}

// TestInvocationName covers how the binary learns which of its two names it is
// wearing.
func TestInvocationName(t *testing.T) {
	tests := []struct {
		name string
		argv []string
		want string
	}{
		{"no argv at all", nil, options.StandaloneName},
		{"empty argv", []string{}, options.StandaloneName},
		{"standalone, bare", []string{"kuberecord"}, options.StandaloneName},
		{"standalone, absolute path", []string{"/usr/local/bin/kuberecord"}, options.StandaloneName},
		{"plugin, bare", []string{"kubectl-kuberecord"}, options.PluginInvocation},
		{"plugin, as kubectl execs it", []string{"/home/x/.krew/bin/kubectl-kuberecord"}, options.PluginInvocation},
		{"plugin on windows", []string{`C:\bin\kubectl-kuberecord.exe`}, options.PluginInvocation},
		{"standalone on windows", []string{`C:\bin\kuberecord.exe`}, options.StandaloneName},
		{"renamed by the user", []string{"/usr/local/bin/kr"}, options.StandaloneName},
		{"a plugin for something else", []string{"kubectl-other"}, options.StandaloneName},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := cli.InvocationName(test.argv); got != test.want {
				t.Errorf("InvocationName(%q) = %q, want %q", test.argv, got, test.want)
			}
		})
	}
}

// TestUsageStringsFollowTheInvocationName is the assertion behind the
// display-name annotation.
//
// The subcommand case is the one that matters. A root that merely spelled its Use
// as "kubectl kuberecord" would satisfy the first two checks and then render
// every subcommand's path as "kubectl <sub>", because cobra takes a command's
// name from the first word of Use — a help text telling an engineer to run
// `kubectl timeline`.
func TestUsageStringsFollowTheInvocationName(t *testing.T) {
	tests := []struct {
		name          string
		argv          []string
		wantPath      string
		wantChildPath string
	}{
		{
			name:          "standalone",
			argv:          []string{"/usr/local/bin/kuberecord"},
			wantPath:      "kuberecord",
			wantChildPath: "kuberecord timeline",
		},
		{
			name:          "plugin",
			argv:          []string{"/home/x/.krew/bin/kubectl-kuberecord"},
			wantPath:      "kubectl kuberecord",
			wantChildPath: "kubectl kuberecord timeline",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			io, _, _ := streams()
			root, _ := cli.NewRootCommand(cli.InvocationName(test.argv), io)
			// A stand-in for the commands Tasks 11.3 onwards add. It exists only
			// to prove the path renders correctly one level down.
			root.AddCommand(&cobra.Command{Use: "timeline", Short: "stand-in"})

			if got := root.CommandPath(); got != test.wantPath {
				t.Errorf("root CommandPath() = %q, want %q", got, test.wantPath)
			}
			if got := root.UseLine(); !strings.HasPrefix(got, test.wantPath) {
				t.Errorf("root UseLine() = %q, want it to start with %q", got, test.wantPath)
			}

			child, _, err := root.Find([]string{"timeline"})
			if err != nil {
				t.Fatalf("Find(timeline): %v", err)
			}
			if got := child.CommandPath(); got != test.wantChildPath {
				t.Errorf("child CommandPath() = %q, want %q", got, test.wantChildPath)
			}
		})
	}
}

// TestHelpNamesTheInvocation asserts the rendered help text, not just the
// computed paths, since that is what a user actually reads.
func TestHelpNamesTheInvocation(t *testing.T) {
	io, out, _ := streams()
	root, _ := cli.NewRootCommand(options.PluginInvocation, io)
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatalf("Execute(--help): %v", err)
	}

	help := out.String()
	if !strings.Contains(help, "kubectl kuberecord [flags]") {
		t.Errorf("plugin help does not show the plugin usage line:\n%s", help)
	}
}

// TestClusterIDIsDistinctFromKubeconfigCluster is D21 in executable form.
//
// The two flags mean entirely different things — one selects a kubeconfig
// cluster entry, the other selects which recorded cluster's history to read —
// and a collision between them would be discovered by a user rather than by us,
// after release, when it is expensive to change. So this asserts they are two
// flags, that neither shadows the other by shorthand, and that writing one does
// not write the other.
func TestClusterIDIsDistinctFromKubeconfigCluster(t *testing.T) {
	io, _, _ := streams()
	root, flags := cli.NewRootCommand(options.StandaloneName, io)
	set := root.PersistentFlags()

	kubeconfigCluster := set.Lookup(options.FlagKubeconfigCluster)
	if kubeconfigCluster == nil {
		t.Fatalf("--%s is not registered; cli-runtime's kubeconfig surface is missing",
			options.FlagKubeconfigCluster)
	}
	clusterID := set.Lookup(options.FlagClusterID)
	if clusterID == nil {
		t.Fatalf("--%s is not registered", options.FlagClusterID)
	}

	if kubeconfigCluster == clusterID {
		t.Fatal("--cluster and --cluster-id resolve to the same flag")
	}
	if kubeconfigCluster.Name == clusterID.Name {
		t.Errorf("both flags are named %q", clusterID.Name)
	}

	// A shorthand on either would let one be typed as the other by accident,
	// and pflag would resolve `-c` to whichever was registered first.
	if kubeconfigCluster.Shorthand != "" || clusterID.Shorthand != "" {
		t.Errorf("neither --cluster nor --cluster-id may take a shorthand, got %q and %q",
			kubeconfigCluster.Shorthand, clusterID.Shorthand)
	}

	// Distinct storage: writing one must leave the other alone. This is the
	// check that would fail if both flags were ever bound to the same variable.
	if err := set.Set(options.FlagKubeconfigCluster, "kubeconfig-entry"); err != nil {
		t.Fatalf("set --%s: %v", options.FlagKubeconfigCluster, err)
	}
	if err := set.Set(options.FlagClusterID, "recorded-cluster"); err != nil {
		t.Fatalf("set --%s: %v", options.FlagClusterID, err)
	}
	if flags.ClusterID != "recorded-cluster" {
		t.Errorf("ClusterID = %q, want %q", flags.ClusterID, "recorded-cluster")
	}
	if got := *flags.ConfigFlags.ClusterName; got != "kubeconfig-entry" {
		t.Errorf("ConfigFlags.ClusterName = %q, want %q", got, "kubeconfig-entry")
	}
}

// TestGlobalFlagSurface asserts every flag Task 11.1 fixes is present under the
// name it was fixed at.
//
// Flag names are the part of a CLI that cannot be changed after release without
// breaking somebody's script, which is why they are asserted by name rather than
// left to whatever the registration code happens to spell.
func TestGlobalFlagSurface(t *testing.T) {
	io, _, _ := streams()
	root, _ := cli.NewRootCommand(options.StandaloneName, io)
	set := root.PersistentFlags()

	tests := []struct {
		flag      string
		shorthand string
	}{
		// kubectl's own surface, from cli-runtime, asserted here because the
		// promise is that these behave identically to kubectl's.
		{"kubeconfig", ""},
		{"context", ""},
		{"namespace", "n"},
		{"cluster", ""},
		{"user", ""},
		{"as", ""},
		// kuberecord's own.
		{options.FlagClusterID, ""},
		{options.FlagOutput, "o"},
		{options.FlagColor, ""},
		{options.FlagSink, ""},
		{options.FlagSource, ""},
		{options.FlagProfile, ""},
		{options.FlagOperatorNamespace, ""},
		{options.FlagVerbosity, "v"},
	}

	for _, test := range tests {
		t.Run("--"+test.flag, func(t *testing.T) {
			flag := set.Lookup(test.flag)
			if flag == nil {
				t.Fatalf("--%s is not registered", test.flag)
			}
			if flag.Shorthand != test.shorthand {
				t.Errorf("--%s shorthand = %q, want %q", test.flag, flag.Shorthand, test.shorthand)
			}
		})
	}
}

// TestExitCodesThroughRun drives the whole binary the way a shell does, because
// the exit code is a public contract and the only honest test of it is the path
// main actually takes.
func TestExitCodesThroughRun(t *testing.T) {
	tests := []struct {
		name string
		argv []string
		want int
	}{
		{"bare invocation prints help", []string{"kuberecord"}, exit.Success},
		{"explicit help", []string{"kuberecord", "--help"}, exit.Success},
		{"unknown flag", []string{"kuberecord", "--no-such-flag"}, exit.UsageError},
		{"unknown command", []string{"kuberecord", "no-such-command"}, exit.UsageError},
		{"invalid output format", []string{"kuberecord", "-o", "toml"}, exit.UsageError},
		{"invalid colour mode", []string{"kuberecord", "--color", "sometimes"}, exit.UsageError},
		{"non-integer verbosity", []string{"kuberecord", "-v", "loud"}, exit.UsageError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			io, _, _ := streams()
			if got := cli.Run(test.argv, io); got != test.want {
				t.Errorf("Run(%q) = %d, want %d", test.argv, got, test.want)
			}
		})
	}
}

// TestDataAndDiagnosticsAreSeparated is the `| jq` guarantee.
//
// A command's structured output is consumed by a program, and a warning banner
// that reaches that program is a parse error rather than a warning. Cobra's own
// default would have failed this: it writes the usage block to OutOrStderr(),
// which resolves to the command's out writer once one is set, so a usage error
// would land on stdout.
func TestDataAndDiagnosticsAreSeparated(t *testing.T) {
	tests := []struct {
		name          string
		argv          []string
		wantStdout    bool
		wantStderr    bool
		stderrMustSay string
	}{
		{
			name:       "help is data and goes to stdout",
			argv:       []string{"kuberecord", "--help"},
			wantStdout: true,
			wantStderr: false,
		},
		{
			name:       "bare invocation is help and goes to stdout",
			argv:       []string{"kuberecord"},
			wantStdout: true,
			wantStderr: false,
		},
		{
			name:          "an unknown flag says nothing on stdout",
			argv:          []string{"kuberecord", "--no-such-flag"},
			wantStdout:    false,
			wantStderr:    true,
			stderrMustSay: "unknown flag",
		},
		{
			name:          "a rejected flag value says nothing on stdout",
			argv:          []string{"kuberecord", "-o", "toml"},
			wantStdout:    false,
			wantStderr:    true,
			stderrMustSay: "must be one of",
		},
		{
			name:          "an unknown command says nothing on stdout",
			argv:          []string{"kuberecord", "no-such-command"},
			wantStdout:    false,
			wantStderr:    true,
			stderrMustSay: "unknown command",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			io, out, errOut := streams()
			cli.Run(test.argv, io)

			if gotStdout := out.Len() > 0; gotStdout != test.wantStdout {
				t.Errorf("stdout non-empty = %t, want %t; stdout was:\n%s",
					gotStdout, test.wantStdout, out.String())
			}
			if gotStderr := errOut.Len() > 0; gotStderr != test.wantStderr {
				t.Errorf("stderr non-empty = %t, want %t; stderr was:\n%s",
					gotStderr, test.wantStderr, errOut.String())
			}
			if test.stderrMustSay != "" && !strings.Contains(errOut.String(), test.stderrMustSay) {
				t.Errorf("stderr does not mention %q:\n%s", test.stderrMustSay, errOut.String())
			}
		})
	}
}

// TestUsageErrorsCarryTheUsageBlock asserts the usage block accompanies a usage
// error and lands on stderr with it — a bare "unknown flag" with no reminder of
// what the flags are is a worse message than the one cobra would have printed.
func TestUsageErrorsCarryTheUsageBlock(t *testing.T) {
	io, out, errOut := streams()
	if got := cli.Run([]string{"kuberecord", "--no-such-flag"}, io); got != exit.UsageError {
		t.Fatalf("Run = %d, want %d", got, exit.UsageError)
	}
	if !strings.Contains(errOut.String(), "Usage:") {
		t.Errorf("stderr carries no usage block:\n%s", errOut.String())
	}
	if out.Len() != 0 {
		t.Errorf("stdout should be empty, got:\n%s", out.String())
	}
}

// TestHelpDocumentsTheExitCodes keeps the exit-code contract discoverable where
// the person writing a wrapper script will look for it.
func TestHelpDocumentsTheExitCodes(t *testing.T) {
	io, out, _ := streams()
	if got := cli.Run([]string{"kuberecord", "--help"}, io); got != exit.Success {
		t.Fatalf("Run(--help) = %d, want %d", got, exit.Success)
	}

	help := out.String()
	for _, want := range []string{"Exit codes:", "0 ", "1 ", "2 ", "3 ", "no coverage"} {
		if !strings.Contains(help, want) {
			t.Errorf("help does not document %q:\n%s", want, help)
		}
	}
}

// TestRunIgnoresTheProcessArguments pins the argv handling against cobra's
// default.
//
// Cobra reads os.Args[1:] whenever its own args are nil, so a Run that passed
// nil through for an argv of just the program name would silently execute the
// real process's arguments. It is a quiet bug: under `go test` the spliced
// arguments are the test binary's flags, so it surfaces as an unknown-flag
// failure in a case that passed no flags — and not at all if the binary happens
// to have been invoked bare.
//
// Setting os.Args to something that would certainly fail is what makes the
// assertion sharp: if the arguments leak, this exits 2 instead of 0.
func TestRunIgnoresTheProcessArguments(t *testing.T) {
	original := os.Args
	t.Cleanup(func() { os.Args = original })
	os.Args = []string{"kuberecord", "--a-flag-that-does-not-exist"}

	tests := []struct {
		name string
		argv []string
	}{
		{name: "argv with only the program name", argv: []string{"kuberecord"}},
		{name: "empty argv", argv: []string{}},
		{name: "nil argv", argv: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			io, out, errOut := streams()
			if got := cli.Run(test.argv, io); got != exit.Success {
				t.Errorf("Run(%q) = %d, want %d; the process arguments leaked in.\nstderr:\n%s",
					test.argv, got, exit.Success, errOut.String())
			}
			if out.Len() == 0 {
				t.Error("a bare invocation printed no help to stdout")
			}
		})
	}
}
