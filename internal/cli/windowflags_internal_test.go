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

// One bound, two names: the resolution table, tested where the two spellings
// still exist.
//
// Past windowFlags.resolve there is only --since and --until, which is the point
// of it — so this is the only place the alias is observable as anything other
// than an equivalent command line.

package cli

import (
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/spf13/pflag"
)

// parseWindowFlags registers the four names on a fresh set, parses argv, and
// resolves.
//
// A real pflag.FlagSet rather than fields assigned by hand, because the rule
// under test is stated in terms of which names were *given* — and the difference
// between "given empty" and "not given" is exactly what a hand-assigned struct
// cannot express.
func parseWindowFlags(t *testing.T, argv ...string) (*windowFlags, error) {
	t.Helper()

	var window windowFlags
	set := pflag.NewFlagSet("test", pflag.ContinueOnError)
	set.SetOutput(nopWriter{})
	window.addFlags(set, "since help", "until help")

	if err := set.Parse(argv); err != nil {
		t.Fatalf("parsing %q: %v", argv, err)
	}
	return &window, window.resolve(set)
}

// nopWriter silences pflag's own error printing, which would otherwise reach the
// test's output for the cases that are meant to fail.
type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestWindowAliasesResolveOntoOnePair is the whole contract of the second
// spelling: --from is --since, and nothing downstream can tell which was typed.
func TestWindowAliasesResolveOntoOnePair(t *testing.T) {
	t.Parallel()

	for name, testCase := range map[string]struct {
		argv       []string
		since      string
		until      string
		wantErrors []string
	}{
		"neither given": {
			argv: nil,
		},
		"the primary spelling": {
			argv:  []string{"--since", "3d", "--until", "1h"},
			since: "3d", until: "1h",
		},
		"the alias spelling": {
			argv:  []string{"--from", "3d", "--to", "1h"},
			since: "3d", until: "1h",
		},
		"one of each": {
			argv:  []string{"--since", "3d", "--to", "1h"},
			since: "3d", until: "1h",
		},
		"both names, same value": {
			// Harmless, and what a template that fills in both spellings produces.
			argv:  []string{"--since", "3d", "--from", "3d"},
			since: "3d",
		},
		"an instant under the alias": {
			argv:  []string{"--from", "2026-08-20T14:00:00Z"},
			since: "2026-08-20T14:00:00Z",
		},
		"both names, different values": {
			argv:       []string{"--since", "3d", "--from", "6h"},
			wantErrors: []string{"--since", "--from", `"3d"`, `"6h"`, "same bound"},
		},
		"both upper names, different values": {
			argv:       []string{"--until", "1h", "--to", "2h"},
			wantErrors: []string{"--until", "--to", "same bound"},
		},
		"an empty primary is not replaced by the alias": {
			// `--since ""` is a malformed value with a good error of its own, and
			// silently substituting --from for it would hide the typo behind a
			// window the user did not ask for.
			argv:       []string{"--since", "", "--from", "6h"},
			wantErrors: []string{"--since", "--from", "same bound"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			window, err := parseWindowFlags(t, testCase.argv...)
			if len(testCase.wantErrors) > 0 {
				if err == nil {
					t.Fatalf("%q was accepted; one bound given twice with two values is a window the "+
						"tool would have to choose between", testCase.argv)
				}
				if code := exit.CodeFor(err); code != exit.UsageError {
					t.Errorf("the conflict exits %d, want %d: it is something the user typed, and a "+
						"script that retries a backend timeout must not retry it", code, exit.UsageError)
				}
				for _, want := range testCase.wantErrors {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("the message does not mention %s: %v", want, err)
					}
				}
				return
			}

			if err != nil {
				t.Fatalf("resolve(%q): %v", testCase.argv, err)
			}
			if window.since != testCase.since {
				t.Errorf("--since resolved to %q, want %q", window.since, testCase.since)
			}
			if window.until != testCase.until {
				t.Errorf("--until resolved to %q, want %q", window.until, testCase.until)
			}
		})
	}
}
