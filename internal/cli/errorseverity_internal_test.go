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
	"errors"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
)

// What stderr receives when a command fails, pinned as whole documents.
//
// The exit path assembles three things that are painted three different ways —
// the `error:` line in the Failure tier, cobra's usage block untouched, and the
// remediation routes in the tiers their own renderer gave them — and the only
// assertion that can see all three at once is one over the assembled string. Field
// use produced the finding this file exists for: the line saying the command had
// failed was the only unpainted thing on a screen that already had a notice in
// amber and provenance in dim above it (D39).
//
// The documents are compared rather than the parts, because the parts are correct
// in isolation today. What went wrong was their relative weight.

// updateFlagName is the flag that rewrites every golden file in this directory.
const updateFlagName = "update"

// updatingGoldens reports whether this run was asked to rewrite them.
//
// It reads the flag rather than declaring one. timeline_test.go registers
// `-update` for the external test package, both packages are linked into a single
// test binary, and a second flag.Bool("update") panics before any test runs — so
// the lookup happens here, at call time, by which point every package's init has
// run and testing has parsed the command line. The alternative was a second flag
// with a second name, which would have left `go test ./internal/cli/ -update`
// quietly updating half the directory.
//
// A missing flag is fatal rather than false: it would mean the name changed, and a
// golden helper that has silently stopped being updatable is one whose files stop
// being regenerated and start being edited by hand.
func updatingGoldens(t *testing.T) bool {
	t.Helper()

	registered := flag.Lookup(updateFlagName)
	if registered == nil {
		t.Fatalf("no -%s flag is registered: see timeline_test.go, which owns it for this directory",
			updateFlagName)
	}
	getter, ok := registered.Value.(flag.Getter)
	if !ok {
		t.Fatalf("the -%s flag does not report its value", updateFlagName)
	}
	value, ok := getter.Get().(bool)
	if !ok {
		t.Fatalf("the -%s flag is not a boolean", updateFlagName)
	}
	return value
}

// assertFailureGolden compares one assembled diagnostic against its file.
//
// Its own helper rather than the external package's assertGoldenDocument, which
// these cases cannot reach: the thing under test is unexported, and a document
// this one produces has no stdout half to put a section marker around.
func assertFailureGolden(t *testing.T, name, got string) {
	t.Helper()
	assertInternalGolden(t, "error", name, got)
}

// assertInternalGolden is that comparison for any of this package's
// internal-test goldens.
//
// The directory is a parameter for the reason assertGoldenIn's is on the external
// side: two copies of a compare-or-rewrite pair are two places for the write half
// to drift from the compare half, and a golden test whose halves disagree is one
// that passes after rewriting the thing it was meant to pin. This is the internal
// package's single copy, because the external one's -update flag cannot be
// reached from here — see updatingGoldens.
func assertInternalGolden(t *testing.T, dir, name, got string) {
	t.Helper()

	path := filepath.Join("testdata", dir, name+".golden")
	if updatingGoldens(t) {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatalf("creating the golden directory: %v", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
		return
	}

	want, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("reading %s (run `go test ./internal/cli/ -update` to create it): %v", path, err)
	}
	if got != string(want) {
		t.Errorf("the rendering of %s changed.\n--- want ---\n%s\n--- got ---\n%s", name, want, got)
	}
}

// sgrSequence matches an ANSI colour sequence — a CSI ending in `m` — and nothing
// else.
//
// A regular expression rather than a list of the sequences this path can emit, for
// the reason internal/cli/resolve gives at the same expression: the property is
// "nothing but escapes differ", and a list would be a second copy of a colour table
// that drifts the day one of them changes.
var sgrSequence = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// ansiRed is the sequence the mechanical tier at render.go:171 emits.
//
// Spelled out here, unlike everywhere else in these tests, because this one
// assertion is about that specific sequence and no other: the exit path must paint
// in red and the routes beneath it must not.
const ansiRed = "\x1b[31m"

// failedCommandFixture is a command whose usage block is the same on every machine.
//
// The real tree's block is not: it carries this machine's kubeconfig cache
// directory in a flag default, which is why the external package's goldens elide it
// altogether. That the block belongs to the subcommand that actually failed is
// asserted by TestUsageErrorsCarryTheUsageBlock, through the real tree; what is
// pinned here is that it is appended, and appended unpainted.
func failedCommandFixture() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "timeline <kind>/<name>",
		Short: "Show recorded changes to one object",

		// Never called. It is here because cobra prints the "Usage:" heading with
		// nothing under it for a command that cannot be run, and a fixture whose
		// block is missing the line a reader would actually retype would be
		// pinning the wrong page.
		RunE: func(*cobra.Command, []string) error { return nil },
	}
	cmd.Flags().String("since", "24h", "how far back to read")
	return cmd
}

// advisedFailure is a real failure that carries routes past itself.
//
// The active profile names an environment variable that is not set (Task 17.4), and
// it is used here rather than a hand-built error because the point of the case is
// the composition: a red `error:` line above a block that must keep the amber,
// dim and bold its own renderer chose for it. Nothing is contacted — a profile
// answers from the file alone.
func advisedFailure(t *testing.T) (*cobra.Command, error) {
	t.Helper()

	resolver, streams := failingProfileResolver(t)
	backend, err := resolver.Resolve(t.Context())
	if err == nil {
		if closeErr := backend.Close(); closeErr != nil {
			t.Errorf("closing the backend: %v", closeErr)
		}
		t.Fatal("a profile whose password reference does not resolve produced a backend")
	}

	root, _ := NewRootCommand(options.StandaloneName, streams)
	return root, err
}

// failureCase is one failure and the document it must produce.
type failureCase struct {
	name   string
	err    error
	failed *cobra.Command
}

// failureCases are the shapes the exit path has to render.
func failureCases(t *testing.T) []failureCase {
	t.Helper()

	root, advised := advisedFailure(t)
	return []failureCase{
		{
			// The ordinary failure: a well-formed request the backend would not
			// answer. One line, no block, nothing to elide.
			name:   "runtime",
			err:    exit.RuntimeErrorf("reading the timeline: connection reset by peer"),
			failed: failedCommandFixture(),
		},
		{
			// The one that appends cobra's own page, which must arrive in the
			// weight cobra wrote it in.
			name:   "usage",
			err:    exit.UsageErrorf("unknown flag: --no-such-flag"),
			failed: failedCommandFixture(),
		},
		{
			// `diff --exit-code` finding changes: an exit code, and deliberately
			// not a word. The golden file is empty, which is the assertion — a
			// tier applied here would put "error:" over a successful query.
			name:   "quiet",
			err:    &exit.Error{Code: exit.RuntimeError, Quiet: true, Err: errors.New("changes found")},
			failed: failedCommandFixture(),
		},
		{
			// A failure and the ways around it, which is the composition the
			// severity vocabulary exists to keep legible.
			name:   "advised",
			err:    advised,
			failed: root,
		},
	}
}

// TestTheExitPathRendersItsSeverity is the golden-file assertion, in both colour
// modes, and TestColourIsNothingButColour's property over the same documents.
//
// The two are one test because they are one claim about the pair of files: the
// coloured document is the plain one with escapes in it, so a rendering that
// reworded a sentence when colour was on, or moved the newline inside a painted
// span, could not be built back out of the plain file and fails here rather than
// surviving as a golden nobody regenerated.
func TestTheExitPathRendersItsSeverity(t *testing.T) {
	for _, tc := range failureCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			code := exit.CodeFor(tc.err)
			plain := failureDiagnostic(tc.err, code, tc.failed, false)
			painted := failureDiagnostic(tc.err, code, tc.failed, true)

			assertFailureGolden(t, tc.name, plain)
			assertFailureGolden(t, tc.name+"-color", painted)

			if plain == "" {
				// A quiet failure has nothing to compare and nothing to paint,
				// and asserting that both modes produce nothing is the whole of
				// what this case has to say.
				if painted != "" {
					t.Errorf("a quiet failure wrote something under colour: %q", painted)
				}
				return
			}
			if !sgrSequence.MatchString(painted) {
				t.Fatal("the coloured diagnostic carries no escape sequences at all")
			}
			if stripped := sgrSequence.ReplaceAllString(painted, ""); stripped != plain {
				t.Errorf("colour changes more than colour.\n--- plain ---\n%s\n--- stripped ---\n%s",
					plain, stripped)
			}
		})
	}
}

// TestTheFailureLineIsRedAndNothingElseIs.
//
// Two halves of one decision. The line that says no result arrived is painted in
// the mechanical red, so that it is findable beneath the amber and the dim above
// it — and the block beneath it is not, because those are the routes past the
// failure rather than the failure, and Task 17.4 drew that distinction on purpose.
// A single red span in the whole document is the shortest way to say both.
func TestTheFailureLineIsRedAndNothingElseIs(t *testing.T) {
	root, err := advisedFailure(t)
	painted := failureDiagnostic(err, exit.CodeFor(err), root, true)

	if !strings.HasPrefix(painted, ansiRed+"error: ") {
		t.Errorf("the diagnostic does not open in the failure tier:\n%q", firstLine(painted))
	}
	if got := strings.Count(painted, ansiRed); got != 1 {
		t.Errorf("red appears %d times, want once — on the line that failed:\n%s", got, painted)
	}

	// The block is passed through exactly as its own renderer produced it, rather
	// than re-rendered at this layer. Anything else and the tiers it chose would
	// be this file's opinion of them.
	var advisable remediation
	if !errors.As(err, &advisable) {
		t.Fatalf("the fixture failure carries no routes: %v", err)
	}
	if advice := advisable.Render(root.CommandPath(), true); !strings.HasSuffix(painted, advice) {
		t.Errorf("the routes were re-rendered on the way out.\n--- want ---\n%s\n--- got ---\n%s",
			advice, painted)
	}
}

// TestTheUsageBlockIsNotPainted.
//
// A cobra usage dump is a page of flag descriptions. In red it is unreadable, and
// the one line the reader is looking for — the message above it — stops being the
// thing that stands out.
func TestTheUsageBlockIsNotPainted(t *testing.T) {
	failed := failedCommandFixture()
	painted := failureDiagnostic(exit.UsageErrorf("unknown flag: --no-such-flag"), exit.UsageError, failed, true)

	usage := failed.UsageString()
	if !strings.Contains(painted, usage) {
		t.Fatalf("the usage block is not appended verbatim:\n%s", painted)
	}
	if escapes := sgrSequence.FindAllString(usage, -1); len(escapes) != 0 {
		t.Fatalf("the fixture's own usage block carries escapes, so this asserts nothing: %q", escapes)
	}
	// Everything after the message is the block, and it must be plain.
	if block := painted[strings.Index(painted, usage):]; sgrSequence.MatchString(block) {
		t.Errorf("the usage block was painted:\n%s", block)
	}
}

// TestAFailureIsExactlyOneWriteToStderr.
//
// The property the composition exists for: stderr is shared with whatever else the
// shell has pointed at it, and two writes would let another writer's line land
// between the message and the block that explains it. The count is the assertion —
// a diagnostic assembled correctly and written in three parts would pass every
// other test in this file.
func TestAFailureIsExactlyOneWriteToStderr(t *testing.T) {
	var out strings.Builder
	errOut := &countingWriter{}
	streams := genericiooptions.IOStreams{In: strings.NewReader(""), Out: &out, ErrOut: errOut}

	// A usage error, because it is the failure that writes the most: the message
	// and cobra's whole page. Nothing else reaches stderr on this path — the tree
	// silences cobra's own printing — so every write counted here is this one.
	if code := Run([]string{options.StandaloneName, "--no-such-flag"}, streams); code != exit.UsageError {
		t.Fatalf("Run = %d, want %d", code, exit.UsageError)
	}
	if errOut.writes != 1 {
		t.Errorf("the diagnostic took %d writes, want 1:\n%s", errOut.writes, errOut.String())
	}
	for _, want := range []string{"error: ", "Usage:"} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("the single write does not carry %q:\n%s", want, errOut.String())
		}
	}
	if out.Len() != 0 {
		t.Errorf("a failing invocation wrote to stdout, which belongs to data: %q", out.String())
	}
}

// TestTheColourModeDecidesTheExitPath, over the modes a caller can set.
//
// Terminal detection itself is not simulated: options.IsTerminal asks the kernel
// about a real file descriptor, and a pty allocated for this would be a
// platform-specific fixture for one line of production code. What is asserted
// instead is the shape that makes the question answerable in one place —
// diagnosticColor is the only site that names a stream, it names ErrOut, and
// failureDiagnostic cannot reach a stream at all.
func TestTheColourModeDecidesTheExitPath(t *testing.T) {
	for _, tc := range []struct {
		mode    options.ColorMode
		noColor string
		painted bool
	}{
		{mode: options.ColorAlways, painted: true},
		{mode: options.ColorNever, painted: false},
		// A strings.Builder is not a terminal, so `auto` resolves to plain —
		// which is what keeps every golden file in this repository free of
		// escapes.
		{mode: options.ColorAuto, painted: false},
		// NO_COLOR loses to an explicit --color=always, which is what the flag is
		// for, and wins over anything it has not been told about.
		{mode: options.ColorAlways, noColor: "1", painted: true},
		{mode: options.ColorAuto, noColor: "1", painted: false},
	} {
		name := string(tc.mode)
		if tc.noColor != "" {
			name += "-with-" + options.EnvNoColor
		}
		t.Run(name, func(t *testing.T) {
			if tc.noColor != "" {
				t.Setenv(options.EnvNoColor, tc.noColor)
			}
			streams := genericiooptions.IOStreams{
				In: strings.NewReader(""), Out: &strings.Builder{}, ErrOut: &strings.Builder{},
			}
			root, flags := NewRootCommand(options.StandaloneName, streams)
			if err := root.ParseFlags([]string{"--" + options.FlagColor, string(tc.mode)}); err != nil {
				t.Fatalf("parsing flags: %v", err)
			}

			diagnostic := failureDiagnostic(exit.RuntimeErrorf("the backend would not answer"),
				exit.RuntimeError, root, diagnosticColor(flags, streams))
			if painted := sgrSequence.MatchString(diagnostic); painted != tc.painted {
				t.Errorf("--%s=%s produced painted=%v, want %v", options.FlagColor, tc.mode, painted, tc.painted)
			}
		})
	}
}

// countingWriter is a stderr that remembers how many times it was written to.
//
// It holds its buffer rather than embedding it, and so implements io.Writer and
// nothing else. A promoted WriteString would be found by io.WriteString's fast
// path and would carry the diagnostic straight past the counter — which is how
// the count first read zero over a document that was plainly there.
type countingWriter struct {
	buffer strings.Builder
	writes int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.buffer.Write(p)
}

func (w *countingWriter) String() string { return w.buffer.String() }

// firstLine is the head of a document, for an error message that should not print
// four paragraphs to report a wrong first character.
func firstLine(text string) string {
	head, _, _ := strings.Cut(text, "\n")
	return head
}
