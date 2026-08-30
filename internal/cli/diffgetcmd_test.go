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
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli"
)

// The surface of `diff` and `get`: what they refuse before they open anything,
// and the code they refuse it with.
//
// Every case below is rejected while parsing, ahead of the backend resolver, so
// none of them reaches a kubeconfig or a sink. That ordering matters as much as
// the codes do: a mistyped flag must not cost a connection, and it must not be
// reported as a backend that would not answer.

// TestDiffAndGetRefuseMalformedInvocations pins the usage errors.
func TestDiffAndGetRefuseMalformedInvocations(t *testing.T) {
	tests := []struct {
		name string
		argv []string
		want string
	}{
		{
			name: "diff with no object at all",
			argv: []string{"diff"},
			want: "no object given",
		},
		{
			name: "diff with a kind and no name",
			argv: []string{"diff", "deploy"},
			want: "no object name given",
		},

		{
			name: "diff with a negative limit",
			argv: []string{"diff", "deploy/x", "--limit", "-1"},
			want: "is negative",
		},
		{
			name: "diff with an unreadable --since",
			argv: []string{"diff", "deploy/x", "--since", "yesterday"},
			want: "neither a duration",
		},
		{
			name: "diff with a window that ends before it starts",
			argv: []string{"diff", "deploy/x", "--since", "2026-08-20", "--until", "2026-08-01"},
			want: "ends before it starts",
		},
		{
			name: "diff with a flag it does not have",
			argv: []string{"diff", "deploy/x", "--nonesuch"},
			want: "unknown flag",
		},
		{
			name: "get with no object at all",
			argv: []string{"get"},
			want: "no object given",
		},
		{
			name: "get in a tabular format",
			argv: []string{"get", "deploy/x", "-o", "wide"},
			want: "get renders yaml, json or jsonl",
		},
		{
			name: "get with an unreadable --at",
			argv: []string{"get", "deploy/x", "--at", "the other day"},
			want: "neither a duration",
		},
		{
			name: "get with a flag it does not have",
			argv: []string{"get", "deploy/x", "--nonesuch"},
			want: "unknown flag",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			io, out, errOut := streams()
			code := cli.Run(append([]string{"kuberecord"}, test.argv...), io)

			if code != cli.ExitUsageError {
				t.Errorf("exit code %d, want %d.\nstderr:\n%s", code, cli.ExitUsageError, errOut.String())
			}
			if !strings.Contains(errOut.String(), test.want) {
				t.Errorf("the message does not explain the mistake.\nwant it to contain %q\ngot:\n%s",
					test.want, errOut.String())
			}
			if out.Len() != 0 {
				t.Errorf("a usage error reached stdout, where a pipe would receive it:\n%s", out.String())
			}
		})
	}
}

// TestDiffAndGetAreInTheCommandTree is the wiring check.
//
// Without it the whole of this file could pass against commands nobody could
// invoke, because every assertion above is about a rejection.
func TestDiffAndGetAreInTheCommandTree(t *testing.T) {
	io, out, _ := streams()
	if code := cli.Run([]string{"kuberecord", "--help"}, io); code != cli.ExitSuccess {
		t.Fatalf("`--help` exited %d", code)
	}
	for _, command := range []string{"diff", "get"} {
		if !strings.Contains(out.String(), command) {
			t.Errorf("the root's help does not list %s:\n%s", command, out.String())
		}
	}
}

// TestDiffHelpNamesEveryFlagTheTaskRequires guards the surface against a flag
// being renamed or dropped without the change being noticed.
func TestDiffHelpNamesEveryFlagTheTaskRequires(t *testing.T) {
	io, out, _ := streams()
	if code := cli.Run([]string{"kuberecord", "diff", "--help"}, io); code != cli.ExitSuccess {
		t.Fatalf("`diff --help` exited %d", code)
	}

	for _, flag := range []string{
		"--since", "--until", "--limit", "--reverse", "--uid", "--field", "--full", "--exit-code",
	} {
		if !strings.Contains(out.String(), flag) {
			t.Errorf("%s is missing from the help", flag)
		}
	}
}

// TestGetHelpNamesEveryFlagTheTaskRequires does the same for `get`.
func TestGetHelpNamesEveryFlagTheTaskRequires(t *testing.T) {
	io, out, _ := streams()
	if code := cli.Run([]string{"kuberecord", "get", "--help"}, io); code != cli.ExitSuccess {
		t.Fatalf("`get --help` exited %d", code)
	}

	for _, flag := range []string{"--at", "--uid", "--verify"} {
		if !strings.Contains(out.String(), flag) {
			t.Errorf("%s is missing from the help", flag)
		}
	}
}

// TestGetDefaultsToYAML covers the one place this command diverges from the
// global default.
//
// `table` is what -o carries when nobody has touched it, and a reconstructed
// object is a document rather than a row. Rather than refusing a bare invocation,
// an untouched -o resolves to YAML — the format that carries the "not a deployable
// manifest" header in the document itself. This asserts the untouched case is not
// refused, by reaching the point where the *backend* is what fails.
func TestGetDefaultsToYAML(t *testing.T) {
	unreachableCluster(t)

	io, _, errOut := streams()
	code := cli.Run([]string{
		"kuberecord", "get", "Deployment.apps/x", "-n", "y",
		"--source", t.TempDir(), "--cluster-id", "c", "--color=never",
	}, io)

	if code == cli.ExitUsageError {
		t.Fatalf("a bare `get` was refused for its output format:\n%s", errOut.String())
	}
	if strings.Contains(errOut.String(), "get renders yaml or json") {
		t.Errorf("an untouched -o was treated as an explicit table request:\n%s", errOut.String())
	}
}
