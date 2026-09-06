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

package render

// The semantic tier of this CLI's colour, above the mechanical one in render.go.
//
// # Why it exists
//
// Everything below it paints by mechanism. eventColor knows what an event type
// is, opColor knows what an operation is, dim and red are two registers with no
// meaning of their own. None of them can answer "what is this line to the person
// reading it", because none of them is about the reader — so a notice saying
// *no state survives from before this window* was rendered at the same weight as
// the rows beneath it, not by decision but because there was nothing else to give
// it. The same is true of the header a reader passes over on every invocation.
//
// This file names the missing axis: three registers for a line that is not data,
// chosen by what the line does to a reader rather than by what produced it. It
// replaces nothing underneath. A row's event type is still coloured by what the
// event is, because that is a fact about the row and not about its importance.
//
// # The vocabulary is closed
//
// Three tiers, and a fourth is a design decision that needs a reason — not a
// convenience at one call site. The value of a set this small is that "which tier
// is this line?" has one obvious answer for every line the CLI prints, and each
// name added makes that question harder for every line already rendered: a fourth
// register costs one decision per call site, not one decision. A line that seems
// to want a new tier is usually a line arguing about emphasis *within* one of
// these three, and the answer to it is wording rather than a register.
//
// The names carry that weight only because they answer different questions.
// Warning is about a conclusion the reader would otherwise draw; Provenance is
// about a fact they need available rather than read; Emphasis is about what
// survives skimming. A candidate fourth tier that cannot be told apart from those
// three in one sentence is the case this paragraph exists to refuse.
//
// # Colour is never the whole message
//
// Every tier has to survive --color=never, NO_COLOR and a redirected stdout,
// where all three render as exactly their text and differ from each other not at
// all. Severity that matters in that state has to be carried in characters, which
// is what WarningMarker is for. Nothing here may add, drop or reorder a character
// when colour is on: that property is asserted by TestColourIsNothingButColour,
// and it is what makes the coloured golden file and the plain one the same
// document, one of them with escapes in it.

// WarningMarker prefixes a line rendered in the Warning tier.
//
// It is the half of that tier which survives having no colour at all — under
// NO_COLOR, in a redirected stream, in a golden file — and it is why a notice is
// still recognisable as a notice when every escape sequence is gone.
//
// A constant rather than a literal at each call site, because the marker is a
// piece of the vocabulary and not a formatting habit: a second character
// appearing in one place would be a fourth tier introduced without anyone
// deciding to have one. It is deliberately not applied by Warning itself, because
// the tier is also spent on multi-line prose — the unreachable-sink diagnostic is
// a paragraph — where prefixing only the first line would mark the paragraph
// rather than each of its lines.
const WarningMarker = "!"

// Severity renders a line that is not data.
//
// It is a value rather than a set of package-level functions so that a caller
// resolves the colour question once, where it knows the answer, and every line it
// renders afterwards is spelled the same way. The question itself — --color,
// NO_COLOR, whether stdout is a terminal — belongs to the caller for the same
// reason Options.Color does: a renderer that answered it would have golden files
// that changed with the window they were generated in.
//
// Its method set is the whole vocabulary, which is what makes the set closed in a
// way a reader can check: a fourth register would be a fourth method here, in one
// file, rather than an escape sequence appearing at a call site.
type Severity struct{ palette }

// NewSeverity binds the vocabulary to one invocation's colour decision.
func NewSeverity(color bool) Severity { return Severity{palette{enabled: color}} }

// Warning renders a line the reader will draw a false conclusion without.
//
// Reach for it when the line exists because the data on its own misleads: a
// timeline that stops against a backend that records no deletions, a window with
// no state before it, an estimate that says a scan is the work. Those are
// Invariant 4 and Invariant 9 lines, and they are the most load-bearing thing on
// the stream they are written to — a reader who skips them reads the answer
// wrongly and has no way to know it.
//
// It is not the tier for "something went wrong". A failure is a failed command
// with an exit code, not a qualification of an answer that was still produced.
func (s Severity) Warning(text string) string { return s.paint(ansiYellow, text) }

// Provenance renders a fact that must be available and need not be re-read.
//
// Reach for it where the answer is only judgeable if the reader can see where it
// came from — which cluster, which incarnation, which row an object was
// reconstructed from, which step of the resolution chain answered — but where the
// reader has already read it on the previous invocation and the one before that.
// The tier's job is to keep the audit trail present while letting the eye go
// past it to the answer.
//
// Recession is not suppression. Nothing here is ever the right rendering for a
// line whose absence would change a conclusion; that line is a Warning, and the
// two tiers must stay distinguishable so that a later pass at making the output
// quieter cannot collapse one into the other.
func (s Severity) Provenance(text string) string { return s.dim(text) }

// Emphasis renders the one line in a block that must survive the block being
// skimmed.
//
// Reach for it where a reader skipping the block would act wrongly on what they
// missed — NOT A DEPLOYABLE MANIFEST is the line and the reason the tier exists.
// Sparingly is part of the definition rather than advice about it: a block with
// two emphasised lines has none, because emphasis is relative to what surrounds
// it and the second one spends the first.
func (s Severity) Emphasis(text string) string { return s.paint(ansiBold, text) }
