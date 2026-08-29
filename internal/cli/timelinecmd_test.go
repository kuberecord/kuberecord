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
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// The command's own surface: the invocations it refuses before it opens
// anything, and the exit code it refuses them with.
//
// Every case below is rejected while parsing, ahead of the backend resolver, so
// none of them reaches a kubeconfig or a sink. That ordering is the point as much
// as the codes are: a mistyped flag must not cost a connection, and it must not
// be reported as a backend that would not answer.

// TestTimelineRefusesMalformedInvocations pins the usage errors.
//
// Exit code 2 and not 1 for all of them, because a wrapper script told to retry a
// backend timeout must not retry a typo.
func TestTimelineRefusesMalformedInvocations(t *testing.T) {
	tests := []struct {
		name string
		argv []string
		want string
	}{
		{
			name: "no object at all",
			argv: []string{"timeline"},
			want: "no object given",
		},
		{
			name: "a kind with no name",
			argv: []string{"timeline", "deploy"},
			want: "no object name given",
		},
		{
			name: "two objects",
			argv: []string{"timeline", "deploy/a", "sts/b", "cm/c"},
			want: "a single object",
		},
		{
			name: "a structured format this command does not render yet",
			argv: []string{"timeline", "deploy/x", "-o", "json"},
			want: "timeline renders table or wide",
		},
		{
			name: "contradictory incarnation flags",
			argv: []string{"timeline", "deploy/x", "--uid", "abc", "--all-incarnations"},
			want: "contradict each other",
		},
		{
			name: "a negative limit",
			argv: []string{"timeline", "deploy/x", "--limit", "-1"},
			want: "is negative",
		},
		{
			name: "an unreadable --since",
			argv: []string{"timeline", "deploy/x", "--since", "yesterday"},
			want: "neither a duration",
		},
		{
			name: "a window that ends before it starts",
			argv: []string{"timeline", "deploy/x", "--since", "2026-08-20", "--until", "2026-08-01"},
			want: "ends before it starts",
		},
		{
			name: "a flag this command does not have",
			argv: []string{"timeline", "deploy/x", "--nonesuch"},
			want: "unknown flag",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			io, out, errOut := streams()
			code := cli.Run(append([]string{"kuberecord"}, test.argv...), io)

			if code != cli.ExitUsageError {
				t.Errorf("exit code %d, want %d", code, cli.ExitUsageError)
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

// TestTimelineIsInTheCommandTree is the wiring check.
//
// Without it the whole of this file could pass against a command nobody could
// invoke, because every assertion above is about a rejection.
func TestTimelineIsInTheCommandTree(t *testing.T) {
	io, out, _ := streams()
	if code := cli.Run([]string{"kuberecord", "--help"}, io); code != cli.ExitSuccess {
		t.Fatalf("`--help` exited %d", code)
	}
	if !strings.Contains(out.String(), "timeline") {
		t.Errorf("the root's help does not list timeline:\n%s", out.String())
	}
}

// TestTimelineHelpNamesEveryFlagTheTaskRequires guards the surface against a
// flag being renamed or dropped without the change being noticed.
func TestTimelineHelpNamesEveryFlagTheTaskRequires(t *testing.T) {
	io, out, _ := streams()
	if code := cli.Run([]string{"kuberecord", "timeline", "--help"}, io); code != cli.ExitSuccess {
		t.Fatalf("`timeline --help` exited %d", code)
	}

	for _, flag := range []string{
		"--since", "--until", "--limit", "--reverse", "--actor", "--exclude-actor",
		"--field", "--uid", "--all-incarnations", "--full", "--with-events",
	} {
		if !strings.Contains(out.String(), flag) {
			t.Errorf("%s is missing from the help", flag)
		}
	}
}

// ansiSequence matches the escape codes colour adds.
var ansiSequence = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// TestColourChangesNothingButColour is the property the table's alignment rests
// on.
//
// Escape sequences carry no display width, so a layout computed over painted
// cells would pad every coloured cell by the length of its codes — a wobble that
// only ever appears on a terminal, which is the one place nobody runs the tests.
func TestColourChangesNothingButColour(t *testing.T) {
	plain, _, err := runTimeline(t, flagshipEngine(), defaultRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline without colour: %v", err)
	}
	painted, _, err := runTimeline(t, flagshipEngine(), defaultRequest(), render.Options{Color: true})
	if err != nil {
		t.Fatalf("RunTimeline with colour: %v", err)
	}

	if !strings.Contains(painted, "\x1b[") {
		t.Fatal("--color=always produced no colour at all")
	}
	if stripped := ansiSequence.ReplaceAllString(painted, ""); stripped != plain {
		t.Errorf("colour changed the layout.\n--- without ---\n%s\n--- with, stripped ---\n%s", plain, stripped)
	}
}

// TestUnknownActorIsDimmed covers the one piece of colour the acceptance
// criteria name.
func TestUnknownActorIsDimmed(t *testing.T) {
	painted, _, err := runTimeline(t, flagshipEngine(), defaultRequest(), render.Options{Color: true})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(painted, "\x1b[2m"+render.UnknownActor+"\x1b[0m") {
		t.Errorf("an actorless change did not render %q dimmed:\n%s", render.UnknownActor, painted)
	}
}

// TestNoColorIsHonouredUnderAutoAndOverriddenByAlways pins the precedence the
// --color help text already promises.
func TestNoColorIsHonouredUnderAutoAndOverriddenByAlways(t *testing.T) {
	_, out, _ := streams()

	t.Setenv(cli.EnvNoColor, "1")
	if cli.ShouldColorize(cli.ColorAuto, out) {
		t.Error("NO_COLOR was set and auto still colourised")
	}
	if !cli.ShouldColorize(cli.ColorAlways, out) {
		t.Error("--color=always must override NO_COLOR; that is what the flag is for")
	}

	t.Setenv(cli.EnvNoColor, "")
	if cli.ShouldColorize(cli.ColorAuto, out) {
		t.Error("a buffer is not a terminal, so auto must not colourise it")
	}
	if cli.ShouldColorize(cli.ColorNever, out) {
		t.Error("--color=never colourised anyway")
	}
}

// TestTerminalWidthReportsNothingForAPipe is why the renderer takes a width.
//
// Zero rather than a guess, so that the caller decides what a pipe means and a
// golden file is not at the mercy of the window it was generated in.
func TestTerminalWidthReportsNothingForAPipe(t *testing.T) {
	_, out, _ := streams()
	if width := cli.TerminalWidth(out); width != 0 {
		t.Errorf("TerminalWidth on a buffer = %d, want 0", width)
	}
}

// flagshipEngine is the fixture the colour tests render.
func flagshipEngine() *fakeEngine {
	return &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}
}

// The address resolution, end to end through the real entry point.
//
// These two go through cli.Run against a kubeconfig naming a server that is not
// there, because the property under test only exists at that boundary: the REST
// mapper cli-runtime builds is lazy, so an unreachable cluster is not a
// construction failure but a lookup failure, and a test that stubbed the mapper
// would be asserting the shape of the stub.

// unreachableCluster isolates an invocation from the machine's own kubeconfig and
// configuration file, and points it at a server that is not listening.
func unreachableCluster(t *testing.T) {
	t.Helper()

	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("KUBECONFIG", filepath.Join("testdata", "kubeconfig"))
}

// TestTimelineReadsARecordedKindWithoutACluster is the evaluation-mode path: an
// archive on a laptop, and the cluster the changes happened in long gone.
//
// It resolves because the address is the identity the schema stores. Exit 3
// follows because the empty archive has no scope log either, which is the finding
// Invariant 9 exists to report rather than dress up as an empty result.
func TestTimelineReadsARecordedKindWithoutACluster(t *testing.T) {
	unreachableCluster(t)

	io, out, errOut := streams()
	code := cli.Run([]string{
		"kuberecord", "timeline", "Deployment.apps/x", "-n", "y",
		"--source", t.TempDir(), "--cluster-id", "c", "--color=never",
	}, io)

	if code != cli.ExitNoCoverage {
		t.Fatalf("exit code %d, want %d.\nstdout:\n%s\nstderr:\n%s",
			code, cli.ExitNoCoverage, out.String(), errOut.String())
	}
	if !strings.Contains(errOut.String(), "read Deployment.apps/x as recorded, without the cluster") {
		t.Errorf("the offline resolution was not announced:\n%s", errOut.String())
	}
	if !strings.Contains(out.String(), "Kind:     apps/Deployment") {
		t.Errorf("the address did not resolve to the recorded identity:\n%s", out.String())
	}
}

// TestTimelineRefusesAShortNameWithoutACluster is the other half of that story.
//
// Expanding `deploy` needs the server's own discovery data, and guessing at it
// would silently read a different object's history. The message names the address
// form that needs no cluster at all.
func TestTimelineRefusesAShortNameWithoutACluster(t *testing.T) {
	unreachableCluster(t)

	io, _, errOut := streams()
	code := cli.Run([]string{
		"kuberecord", "timeline", "deploy/x", "-n", "y",
		"--source", t.TempDir(), "--cluster-id", "c",
	}, io)

	if code != cli.ExitRuntimeError {
		t.Fatalf("exit code %d, want %d.\nstderr:\n%s", code, cli.ExitRuntimeError, errOut.String())
	}
	for _, want := range []string{"could not be reached", "Deployment.apps/x"} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("the message does not contain %q:\n%s", want, errOut.String())
		}
	}
}
