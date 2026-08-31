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
	"github.com/kuberecord/kuberecord/internal/cli/exit"
)

// The surface of `blame`: what it refuses before it opens anything, and the code
// it refuses it with.
//
// Every case below is rejected while parsing, ahead of the backend resolver, so
// none of them reaches a kubeconfig or a sink. A mistyped flag must not cost a
// connection, and it must not be reported as a backend that would not answer.

// TestBlameRefusesMalformedInvocations pins the usage errors.
func TestBlameRefusesMalformedInvocations(t *testing.T) {
	tests := []struct {
		name string
		argv []string
		want string
	}{
		{
			name: "no object at all",
			argv: []string{"blame"},
			want: "no object given",
		},
		{
			name: "a kind and no name",
			argv: []string{"blame", "deploy"},
			want: "no object name given",
		},
		{
			name: "a negative depth",
			argv: []string{"blame", "deploy/x", "--depth", "-1"},
			want: "is negative",
		},
		{
			name: "an unreadable --since",
			argv: []string{"blame", "deploy/x", "--since", "yesterday"},
			want: "neither a duration",
		},
		{
			name: "a window that ends before it starts",
			argv: []string{"blame", "deploy/x", "--since", "2026-08-20", "--until", "2026-08-01"},
			want: "ends before it starts",
		},
		{
			name: "the hunk rendering, which this command has no rows for",
			argv: []string{"blame", "deploy/x", "-o", "diff"},
			want: "blame does not render diff",
		},
		{
			name: "a flag it does not have",
			argv: []string{"blame", "deploy/x", "--full"},
			want: "unknown flag",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			io, out, errOut := streams()
			code := cli.Run(append([]string{"kuberecord"}, test.argv...), io)

			if code != exit.UsageError {
				t.Errorf("exit code %d, want %d.\nstderr:\n%s", code, exit.UsageError, errOut.String())
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

// TestBlameIsInTheCommandTree is the wiring check.
//
// Without it every assertion above could pass against a command nobody could
// invoke, because all of them are about a rejection.
func TestBlameIsInTheCommandTree(t *testing.T) {
	io, out, _ := streams()
	if code := cli.Run([]string{"kuberecord", "--help"}, io); code != exit.Success {
		t.Fatalf("`--help` exited %d", code)
	}
	if !strings.Contains(out.String(), "blame") {
		t.Errorf("the root's help does not list blame:\n%s", out.String())
	}
}

// TestBlameHelpNamesEveryFlagTheTaskRequires guards the surface against a flag
// being renamed or dropped without the change being noticed.
//
// The two absences are asserted as well, because both are decisions rather than
// omissions: --limit would move the replay's anchor and manufacture "(before
// window)" rows, and --all-incarnations would attribute one object's fields to
// changes made to another that wore the same name.
func TestBlameHelpNamesEveryFlagTheTaskRequires(t *testing.T) {
	io, out, _ := streams()
	if code := cli.Run([]string{"kuberecord", "blame", "--help"}, io); code != exit.Success {
		t.Fatalf("`blame --help` exited %d", code)
	}

	for _, flag := range []string{"--since", "--until", "--from", "--to", "--uid", "--field", "--depth"} {
		if !strings.Contains(out.String(), flag) {
			t.Errorf("%s is missing from the help", flag)
		}
	}
	// Spelled with their types, which is how cobra renders a flag it *defines*: a
	// bare "--limit" also appears in the description of the global --max-objects,
	// which exists to say that it is not a substitute for one.
	for _, flag := range []string{"--limit int", "--all-incarnations"} {
		if strings.Contains(out.String(), flag) {
			t.Errorf("%s is offered by blame, where it would make the attribution report writes the "+
				"data does not support:\n%s", flag, out.String())
		}
	}
}
