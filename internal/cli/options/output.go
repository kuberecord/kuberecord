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

package options

import (
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"golang.org/x/term"
)

// OutputFormat is the value of --output/-o.
//
// The set is closed and validated at parse time rather than at render time. A
// released CLI that quietly accepted `-o JSON` today would have to keep
// accepting it forever, and a `-o tabel` that produced a table would be a typo
// the user never learns they made. Rendering arrives with the commands that
// produce output; the vocabulary is fixed here because renaming a format after
// release breaks every script that spells it.
type OutputFormat string

// The formats every command that produces output must understand. `table` is
// the default; `wide` adds columns; `json` and `yaml` carry the versioned
// envelope; `jsonl` streams one item per line so a result larger than memory
// can still be piped; `diff` is the patch-oriented rendering.
const (
	OutputTable OutputFormat = "table"
	OutputWide  OutputFormat = "wide"
	OutputJSON  OutputFormat = "json"
	OutputJSONL OutputFormat = "jsonl"
	OutputYAML  OutputFormat = "yaml"
	OutputDiff  OutputFormat = "diff"
)

// outputFormats is the accepted set, in the order it is shown to a user. It is
// deliberately a slice and not a map: the help text and the error message both
// list it, and a stable order keeps those two readable and diffable.
var outputFormats = []OutputFormat{
	OutputTable, OutputWide, OutputJSON, OutputJSONL, OutputYAML, OutputDiff,
}

// OutputFormats returns the accepted --output values, in the order a user is
// shown them.
//
// It exists so that the help string, the rejection message and the shell
// completion menu are three renderings of one list rather than three lists that
// agree today. A clone is returned because the caller is outside this package and
// the set is closed: a consumer that could append to the accepted set would be
// teaching --output a format nothing renders.
func OutputFormats() []OutputFormat { return slices.Clone(outputFormats) }

// String implements pflag.Value.
func (f *OutputFormat) String() string { return string(*f) }

// Type implements pflag.Value and names the value in `--help`.
func (f *OutputFormat) Type() string { return "format" }

// Set implements pflag.Value, rejecting anything outside the closed set.
//
// The rejection is what routes an unknown format to exit.UsageError: pflag wraps
// this error, the root's flag-error function codes it, and the process ends
// with 2 rather than with a rendering surprise.
func (f *OutputFormat) Set(value string) error { return setEnum(f, value, outputFormats) }

// ColorMode is the value of --color.
//
// `auto` is the default and means "colour when stdout is a terminal and NO_COLOR
// is unset". The environment variable is honoured by the renderers rather than
// here, because a mode is what the user asked for and TTY-ness is a property of
// where the output is going — collapsing the two at parse time would make
// `--color=always | tee` lose its colour, which is exactly what `always` is for.
type ColorMode string

// The accepted colour modes, matching the vocabulary kubectl-adjacent tools and
// most Unix utilities already use.
const (
	ColorAuto   ColorMode = "auto"
	ColorAlways ColorMode = "always"
	ColorNever  ColorMode = "never"
)

// colorModes is the accepted set, in the order it is shown to a user.
var colorModes = []ColorMode{ColorAuto, ColorAlways, ColorNever}

// ColorModes returns the accepted --color values, in the order a user is shown
// them. See OutputFormats for why it is a clone.
func ColorModes() []ColorMode { return slices.Clone(colorModes) }

// String implements pflag.Value.
func (m *ColorMode) String() string { return string(*m) }

// Type implements pflag.Value and names the value in `--help`.
func (m *ColorMode) Type() string { return "mode" }

// Set implements pflag.Value, rejecting anything outside the closed set.
func (m *ColorMode) Set(value string) error { return setEnum(m, value, colorModes) }

// setEnum assigns value to target if it is one of allowed, and otherwise
// reports the accepted set.
//
// Both flag values are closed sets of strings with identical behaviour, and the
// one thing that must not drift between them is the shape of the rejection: the
// message a user sees when they mistype a format and the one they see when they
// mistype a colour mode should teach the same lesson. One function is how that
// stays true, and it is why the error names no flag — pflag has already
// prefixed `invalid argument "x" for "-o, --output" flag:` by the time this is
// read.
func setEnum[T ~string](target *T, value string, allowed []T) error {
	candidate := T(value)
	if !slices.Contains(allowed, candidate) {
		return fmt.Errorf("must be one of %s", JoinValues(allowed))
	}
	*target = candidate
	return nil
}

// JoinValues renders an accepted set for a help string or an error message.
func JoinValues[T ~string](values []T) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, string(value))
	}
	return strings.Join(parts, ", ")
}

// Colour is decided here rather than in the renderer, and the split is
// deliberate: --color is what the user asked for, while being attached to a
// terminal is a property of where the output is going. Collapsing the two at
// parse time would make `--color=always | tee` lose its colour, which is exactly
// what `always` is for.

// EnvNoColor is the environment variable every modern terminal tool honours.
//
// The convention it follows (no-color.org) is that the variable disables colour
// when it is present *and non-empty*, so an exported-but-blank NO_COLOR does not
// silently strip colour from a terminal the user meant to keep colourful.
const EnvNoColor = "NO_COLOR"

// ShouldColorize reports whether output written to out should carry ANSI colour.
//
// `always` overrides NO_COLOR, which is what the --color help text already
// promises: the variable is honoured under `auto`. That ordering matters for the
// one case it exists for — a user piping deliberately coloured output into `less
// -R` on a machine whose profile exports NO_COLOR for everything else.
func ShouldColorize(mode ColorMode, out io.Writer) bool {
	switch mode {
	case ColorNever:
		return false
	case ColorAlways:
		return true
	}
	if value, present := os.LookupEnv(EnvNoColor); present && value != "" {
		return false
	}
	return IsTerminal(out)
}

// TerminalWidth reports the column budget for out, or zero when it is not a
// terminal.
//
// Zero rather than a guess, so that the caller decides what a pipe means; see
// render.DefaultWidth for what it decides.
func TerminalWidth(out io.Writer) int {
	file, ok := out.(*os.File)
	if !ok {
		return 0
	}
	width, _, err := term.GetSize(int(file.Fd()))
	if err != nil || width <= 0 {
		// A terminal that will not report its size is treated as no terminal.
		// Guessing a width for it would be the same guess with a worse name.
		return 0
	}
	return width
}

// IsTerminalIn reports whether in is an interactive terminal.
//
// It is the input half of IsTerminal and is separate only because io.Reader and
// io.Writer are. It exists for the confirmation prompt: a terminal on stdout with
// a redirected stdin is an invocation nobody can answer, and asking anyway would
// hang a pipeline on a question its author never saw.
func IsTerminalIn(in io.Reader) bool {
	file, ok := in.(*os.File)
	return ok && term.IsTerminal(int(file.Fd()))
}

// IsTerminal reports whether out is an interactive terminal.
//
// The type assertion is the whole of the test: an io.Writer that is not an
// *os.File has no file descriptor to ask about, and a buffer in a test must
// never be mistaken for a terminal — that is what keeps the golden files free of
// escape sequences.
func IsTerminal(out io.Writer) bool {
	file, ok := out.(*os.File)
	return ok && term.IsTerminal(int(file.Fd()))
}

// WriteLine writes one line, reporting a failure rather than discarding it.
//
// Unlike a resolution notice — which is diagnostic, and whose loss costs nothing
// the exit code does not already carry — these lines are the whole of what a
// `config` command produces. A `set-profile` that wrote the file and then could not
// say so has done something the user cannot see, and reporting the write failure is
// the only way they find out that their terminal, not their configuration, is what
// went wrong.
func WriteLine(out io.Writer, line string) error {
	if out == nil {
		return nil
	}
	if _, err := io.WriteString(out, line+"\n"); err != nil {
		return exit.RuntimeErrorf("writing output: %w", err)
	}
	return nil
}

// WriteAll writes a rendered document, reporting a short write as the failure it
// is.
func WriteAll(out io.Writer, content string) error {
	if _, err := io.WriteString(out, content); err != nil {
		return exit.RuntimeErrorf("writing output: %w", err)
	}
	return nil
}
