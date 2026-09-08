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
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// The affordance sweep, kept honest by a test rather than by a review.
//
// A flag that changes what a reader sees is named at the point where its absence
// is visible: --full under a row the CHANGE column shortened, --all-incarnations
// under a timeline showing one of two objects that wore the name. Two things can
// go wrong with that, and only the first is obvious. The flag can be missing from
// the moment it is needed, which is what Task 15.6 fixed for --full. Or it can be
// named at a moment where the command does not *have* it — which is worse,
// because the reader types it and is refused, and a notice that answers "how do I
// see the others?" with `unknown flag` has spent the reader's attention to leave
// them no better off.
//
// The second is what the shared notice paths make easy: `timeline`, `diff` and
// `blame` gather through one function and print one incarnation banner, and only
// one of them has --all-incarnations.

// bareFlag matches a --flag mention that is not inside backticks.
//
// Backticked spans are removed before matching, because a notice is allowed to
// name another command's flag when it names the command with it — "`timeline
// --all-incarnations`" is a place to go, not a flag to add to what you just
// typed. A bare mention is an instruction to this command and has to work.
var (
	bareFlag        = regexp.MustCompile(`--[a-z][a-z0-9-]*`)
	backtickedSpans = regexp.MustCompile("`[^`]*`")
)

// TestNoNoticeNamesAFlagItsCommandRejects is the sweep's regression guard.
func TestNoNoticeNamesAFlagItsCommandRejects(t *testing.T) {
	tests := []struct {
		name    string
		command string

		// guard is a fragment of the notice the case is about. It is asserted
		// before the sweep so that a fixture which stopped producing that notice
		// fails as a drifted fixture rather than passing over an empty stream.
		guard  string
		stderr func(t *testing.T) string
	}{
		{
			name:    "timeline over a reused name",
			command: "timeline",
			guard:   "incarnations in this window",
			stderr: func(t *testing.T) string {
				t.Helper()
				_, stderr, err := runTimeline(t, twoIncarnations(), defaultRequest(), render.Options{})
				if err != nil {
					t.Fatalf("RunTimeline: %v", err)
				}
				return stderr
			},
		},
		{
			name:    "diff over a reused name",
			command: "diff",
			guard:   "incarnations in this window",
			stderr: func(t *testing.T) string {
				t.Helper()
				_, stderr, err := runDiff(t, twoIncarnations(), defaultDiffRequest(), render.Options{})
				if err != nil {
					t.Fatalf("RunDiff: %v", err)
				}
				return stderr
			},
		},
		{
			name:    "blame over a reused name",
			command: "blame",
			guard:   "incarnations in this window",
			stderr: func(t *testing.T) string {
				t.Helper()
				_, stderr, err := runBlame(t, twoIncarnations(), defaultBlameRequest(), render.Options{})
				if err != nil {
					t.Fatalf("RunBlame: %v", err)
				}
				return stderr
			},
		},
		{
			// The notice explainNoMatches prints names --actor, --exclude-actor
			// and --field bare, and only `timeline` pushes those into the query —
			// TimelineRequest.filtered is false for `diff` and `blame`, which
			// narrow their rendering instead. That is the property this case pins:
			// the notice is reachable from one command, and it is the command that
			// has all three flags.
			name:    "timeline whose filter matched nothing",
			command: "timeline",
			guard:   "the window itself is not empty",
			stderr: func(t *testing.T) string {
				t.Helper()
				_, stderr, err := runTimeline(t, watchedCheckoutEngine(), filteredEmptyRequest(), render.Options{})
				if err != nil {
					t.Fatalf("RunTimeline: %v", err)
				}
				return stderr
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stderr := tc.stderr(t)
			if !strings.Contains(stderr, tc.guard) {
				t.Fatalf("the fixture stopped producing %q, so this case is asserting over notices "+
					"that do not include the one it is about:\n%s", tc.guard, stderr)
			}

			command := subcommand(t, tc.command)
			for _, named := range namedFlags(stderr) {
				if flagExists(command, named) {
					continue
				}
				t.Errorf("`%s` names %s on stderr, and does not have it — a reader who types what "+
					"the notice suggests gets `unknown flag`:\n%s", tc.command, named, stderr)
			}
		})
	}
}

// TestTheIncarnationBannerOffersWhatEachCommandHas states the positive half.
//
// The guard above would pass against a banner that named no way out at all, which
// would be a worse notice and a passing test. This pins what each command's
// reader is actually told.
func TestTheIncarnationBannerOffersWhatEachCommandHas(t *testing.T) {
	_, timelineErr, err := runTimeline(t, twoIncarnations(), defaultRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(timelineErr, "Pass --all-incarnations to see them all, or --uid to pin one") {
		t.Errorf("`timeline` no longer offers the flag it has:\n%s", timelineErr)
	}

	for _, tc := range []struct {
		name   string
		stderr string
	}{
		{"diff", func() string {
			_, stderr, diffErr := runDiff(t, twoIncarnations(), defaultDiffRequest(), render.Options{})
			if diffErr != nil {
				t.Fatalf("RunDiff: %v", diffErr)
			}
			return stderr
		}()},
		{"blame", func() string {
			_, stderr, blameErr := runBlame(t, twoIncarnations(), defaultBlameRequest(), render.Options{})
			if blameErr != nil {
				t.Fatalf("RunBlame: %v", blameErr)
			}
			return stderr
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.Contains(tc.stderr, "Pass --uid to pin one") {
				t.Errorf("`%s` offers no way to pin an incarnation:\n%s", tc.name, tc.stderr)
			}
			if !strings.Contains(tc.stderr, "`timeline --all-incarnations`") {
				t.Errorf("`%s` does not say where the other incarnations can be read:\n%s",
					tc.name, tc.stderr)
			}
		})
	}
}

// namedFlags returns every bare --flag mention in text, deduplicated.
func namedFlags(text string) []string {
	bare := backtickedSpans.ReplaceAllString(text, " ")
	var named []string
	for _, match := range bareFlag.FindAllString(bare, -1) {
		if !slices.Contains(named, match) {
			named = append(named, match)
		}
	}
	return named
}

// subcommand finds one command of the real tree, so that what counts as "has
// this flag" is cobra's own answer rather than a list kept beside it.
func subcommand(t *testing.T, name string) *cobra.Command {
	t.Helper()

	io, _, _ := streams()
	root, _ := cli.NewRootCommand(options.StandaloneName, io)
	for _, command := range root.Commands() {
		if command.Name() == name {
			return command
		}
	}
	t.Fatalf("the command tree has no %q command", name)
	return nil
}

// flagExists reports whether a command accepts a flag, local or inherited.
func flagExists(command *cobra.Command, named string) bool {
	name := strings.TrimPrefix(named, "--")
	return command.Flags().Lookup(name) != nil ||
		command.InheritedFlags().Lookup(name) != nil ||
		command.Root().PersistentFlags().Lookup(name) != nil
}
