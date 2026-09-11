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
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// The standing check's third question: does any rendered value omit the unit or
// frame that makes it unambiguous when copied? (D45, Task 18.9)
//
// # Why a third question, and where the other two are
//
// The first is D31 — can a flag do nothing, and does the CLI say why — and it is
// noopAudit in noop_test.go. The second is D34 — when a command stops, does it
// name the routes around the failure — and its regression half is
// affordance_test.go. This is the third, and it arrived the same way both of
// those did: from somebody at a terminal, reading `2026-09-10 22:41:13.263` as a
// local time and reporting it as wrong. It was not wrong. The `Z` had moved to a
// column heading, and a heading is not attached to the data — it scrolls off a
// nine-row table and it does not travel when a row is pasted into a post-mortem.
//
// The three questions are one pattern seen from three sides: a flag that did
// nothing, an error that named no route, and a value without its unit. Each was
// found in production, and each is answered by a table somebody has to fill in
// rather than by a fourth fix.
//
// # What is swept
//
// Every *kind* of value this CLI renders, not every call site. A call-site list
// would be a list of line numbers that goes stale on the first refactor; a list of
// value kinds is a list of questions, and a new kind of quantity has to be added
// to it before a reviewer can say the sweep still covers the output.

// frameVerdict is what the sweep has concluded about one kind of value.
type frameVerdict string

const (
	// carriesItsFrame: every rendering of it ends in something that says what it
	// is — a zone marker, a unit, the noun it counts.
	carriesItsFrame frameVerdict = "carries its frame"

	// selfDescribing: the value cannot be read as a quantity in some other frame,
	// because it has no frame to be in — an identifier, a name, an enum.
	selfDescribing frameVerdict = "self-describing"
)

// auditedValue is one row of the sweep.
type auditedValue struct {
	// kind names the quantity, in the words the output uses.
	kind string

	// verdict is the sweep's conclusion.
	verdict frameVerdict

	// why names what renders it and what the frame is, so a reviewer can check
	// the row rather than take it.
	why string
}

// frameAudit is the sweep Task 18.9 asks for, one row per kind of rendered value.
//
// The finding is the first row. The rest are the neighbouring candidates the task
// names — durations, byte sizes and counts — and the result there is "checked,
// none further found": each already carried its unit before this task, and the
// rows record what makes that true rather than asserting it.
var frameAudit = []auditedValue{
	{"an instant", carriesItsFrame, "the finding. render.Zone renders all three layouts and every " +
		"one of them ends in a frame: `Z` under the default, an explicit numeric offset under " +
		"--tz. It used to be `Z` on the wide and header layouts and nothing at all on the narrow " +
		"one, which is the layout the default table uses (D45). The heading names the zone rather " +
		"than an offset, because an offset would contradict half the rows of a table spanning a " +
		"DST change"},
	{"a window", carriesItsFrame, "options.DescribeWindow spells both edges through the same " +
		"Zone, so `2026-08-20T14:00:00Z to now` carries the frame its instants do. An unbounded " +
		"edge is the word `now` or `all recorded history` rather than a blank, which is D31's " +
		"reading of the same question"},
	{"a duration", carriesItsFrame, "coldscan.DescribeSpan renders `3d`, `2w`, `90m` — the units " +
		"--since itself accepts, so the figure can be pasted back in as the flag. Go's own " +
		"`2160h0m0s` is deliberately not used: it is a correct rendering of ninety days that " +
		"nobody reads as ninety days"},
	{"a byte size", carriesItsFrame, "coldscan.formatBytes renders `3.1 GiB`, never a raw count, " +
		"and the units are binary because that is what an object store's own console shows — so " +
		"the figure is comparable with the thing a reader would check it against"},
	{"an object count", carriesItsFrame, "the estimate reads `~1,240 objects`: the noun is in the " +
		"sentence, the tilde says it is an estimate, and the separators are what make six digits " +
		"legible. --max-objects is denominated in the same unit, so the number a user types is " +
		"the number they were shown"},
	{"a patch count", carriesItsFrame, "`3 ops` in the CHANGE column and `patches applied: 0` in " +
		"the reconstruction header. Both name what is being counted in the cell itself, because " +
		"neither has a column heading beside it that would"},
	{"a field count", carriesItsFrame, "`blame --depth`'s FIELDS column, whose heading is the " +
		"noun and whose cells are bare integers. It is the one count that leans on its heading, " +
		"and it may: a collapsed row's count is meaningless outside the table it collapses, so " +
		"there is no reading of it that survives being copied on its own to be wrong"},
	{"a resource version", selfDescribing, "an opaque token from the API server, rendered as " +
		"recorded. It is not a quantity and carries no unit to omit; the RESOURCE VERSION " +
		"heading and `rv ` in a diff's header line say which token it is"},
	{"a UID", selfDescribing, "an identifier. The narrow table abbreviates it and marks the " +
		"abbreviation with an ellipsis, which is the same obligation in its other form: a value " +
		"shortened without a mark is a value a reader believes"},
	{"a field path", selfDescribing, "a path in the display grammar, elided with an ellipsis when " +
		"the column cannot hold it. NormalizeFieldPath accepts either spelling back, so a path " +
		"read out of the CHANGE column and typed into --field matches"},
	{"an exit code", selfDescribing, "an integer with a documented meaning per value, listed in " +
		"docs/CLI.md's exit-code table. There is no unit a reader could assume wrongly"},
	{"a build date", carriesItsFrame, "`version` prints the linker's stamp, RFC 3339 in UTC, " +
		"marker included. It is deliberately outside --tz: it is a fact about the binary rather " +
		"than about recorded history, and it is a string from the build rather than an instant " +
		"this CLI ever parses"},
}

// TestEveryRenderedValueKindIsAuditedForItsFrame is the standing check.
//
// It cannot walk the output the way noopAudit walks the flag tree — there is no
// enumeration of "kinds of value" for it to walk — so it asserts what it can: that
// every row carries a reason, that no kind is audited twice, and that the audit is
// large enough to be a sweep. What makes it worth having is the same thing that
// makes noopAudit worth having: a reviewer meets the question in a table rather
// than in production.
func TestEveryRenderedValueKindIsAuditedForItsFrame(t *testing.T) {
	seen := make(map[string]frameVerdict, len(frameAudit))
	for _, row := range frameAudit {
		if previous, duplicate := seen[row.kind]; duplicate {
			t.Errorf("%q is audited twice, as %q and as %q; one kind has one verdict",
				row.kind, previous, row.verdict)
			continue
		}
		if strings.TrimSpace(row.why) == "" {
			t.Errorf("%q is recorded as %q with no reason; a verdict nobody can check is not an "+
				"audit", row.kind, row.verdict)
		}
		seen[row.kind] = row.verdict
	}

	// Non-vacuity, as noopAudit's flag walk keeps: an audit of three rows has
	// swept something other than this CLI's output.
	if len(seen) < 10 {
		t.Fatalf("the frame audit holds only %d kinds of value; the CLI renders more, so this "+
			"check is measuring nothing", len(seen))
	}
}

// bareInstant matches a timestamp with nothing after it saying what frame it is
// in.
//
// The negative lookahead Go's regexp does not have is done by matching the
// character that follows instead: an instant is bare when what comes after its
// seconds-and-fraction is neither `Z` nor a sign. The tail excludes `.` as well
// as `Z`, `+`, `-` and a digit, so that the optional fraction cannot be skipped
// and its own leading dot then counted as the character that proves the frame is
// missing. What it makes true is that `14:02:58.001Z` and `14:02:58.001+02:00`
// both fail to match while `14:02:58.001  Modified` matches.
var bareInstant = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:$|[^Z+\-0-9.])`)

// assertNoBareInstant is the property every rendering in this file is checked
// for.
func assertNoBareInstant(t *testing.T, where, rendered string) {
	t.Helper()

	for line := range strings.SplitSeq(rendered, "\n") {
		if found := bareInstant.FindString(line); found != "" {
			t.Errorf("%s renders %q, an instant with no frame on it: pasted into a ticket it "+
				"reads as a local time in whatever zone the reader is in (D45)\n%s",
				where, strings.TrimSpace(found), line)
		}
	}
}

// warsawZone is the reader's frame from the field report.
func warsawZone(t *testing.T) render.Zone {
	t.Helper()

	location, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		t.Fatalf("Europe/Warsaw is not in the database this binary carries: %v", err)
	}
	return render.NewZone(location, "Europe/Warsaw")
}

// TestEveryTabularSurfaceCarriesItsFrame sweeps the five commands that render an
// instant, in both the default frame and a shifted one.
//
// The commands rather than the renderer, because the renderer was never the whole
// of it: `diff` appended the word "UTC" to a value the table left bare, `scopes`
// and `blame` left theirs bare under headings that did not carry a frame either,
// and each of those is a decision made in its own file.
func TestEveryTabularSurfaceCarriesItsFrame(t *testing.T) {
	zones := map[string]render.Zone{"utc": render.UTC, "Europe/Warsaw": warsawZone(t)}

	for name, zone := range zones {
		t.Run(name, func(t *testing.T) {
			opts := render.Options{Zone: zone}

			timelineOut, timelineErr, err := runTimeline(t, watchedCheckoutEngine(), defaultRequest(), opts)
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}
			assertNoBareInstant(t, "timeline stdout", timelineOut)
			assertNoBareInstant(t, "timeline stderr", timelineErr)

			wide := opts
			wide.Wide = true
			wideOut, _, err := runTimeline(t, watchedCheckoutEngine(), defaultRequest(), wide)
			if err != nil {
				t.Fatalf("RunTimeline -o wide: %v", err)
			}
			assertNoBareInstant(t, "timeline -o wide", wideOut)

			diffOut, diffErr, err := runDiff(t, watchedCheckoutEngine(), defaultDiffRequest(), opts)
			if err != nil {
				t.Fatalf("RunDiff: %v", err)
			}
			assertNoBareInstant(t, "diff stdout", diffOut)
			assertNoBareInstant(t, "diff stderr", diffErr)

			blameOut, blameErr, err := runBlame(t, blamedCheckoutEngine(), defaultBlameRequest(), opts)
			if err != nil {
				t.Fatalf("RunBlame: %v", err)
			}
			assertNoBareInstant(t, "blame stdout", blameOut)
			assertNoBareInstant(t, "blame stderr", blameErr)

			scopesEngine := &fakeEngine{caps: clickHouseCapabilities(), intervals: recordedScopes()}
			scopesOut, scopesErr, err := runScopes(t, scopesEngine, scopesRequest(), opts)
			if err != nil {
				t.Fatalf("RunScopes: %v", err)
			}
			assertNoBareInstant(t, "scopes stdout", scopesOut)
			assertNoBareInstant(t, "scopes stderr", scopesErr)
		})
	}
}

// TestTheDiffHeaderStoppedNamingItsFrameTwice is the suffix that had to go.
//
// The narrow diff header said "UTC" in words because, unlike the table, it has no
// column heading to carry the frame. With the marker back on the value that
// suffix would render `…482Z UTC` under the default — one instant named twice —
// and `…+02:00 UTC` under --tz, which names two frames for one moment.
func TestTheDiffHeaderStoppedNamingItsFrameTwice(t *testing.T) {
	stdout, _, err := runDiff(t, watchedCheckoutEngine(), defaultDiffRequest(),
		render.Options{Zone: warsawZone(t)})
	if err != nil {
		t.Fatalf("RunDiff: %v", err)
	}

	if strings.Contains(stdout, "UTC") {
		t.Errorf("a diff rendered in Europe/Warsaw still says UTC:\n%s", stdout)
	}
	if !strings.Contains(stdout, "+02:00") {
		t.Errorf("a diff rendered in Europe/Warsaw carries no offset:\n%s", stdout)
	}
}

// TestTheTimeHeadingAgreesWithItsColumn closes the defect one layer along.
//
// A heading and a column that disagree about the frame is the thing this task
// exists to fix, so a --tz that moved the values and left `TIME (UTC)` above them
// would have reintroduced it in the fix.
func TestTheTimeHeadingAgreesWithItsColumn(t *testing.T) {
	for name, want := range map[string]struct {
		zone    render.Zone
		heading string
		value   string
	}{
		"utc":           {render.UTC, "TIME (UTC)", "Z  "},
		"Europe/Warsaw": {warsawZone(t), "TIME (Europe/Warsaw)", "+02:00  "},
	} {
		t.Run(name, func(t *testing.T) {
			stdout, _, err := runTimeline(t, watchedCheckoutEngine(), defaultRequest(),
				render.Options{Zone: want.zone})
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}
			if !strings.Contains(stdout, want.heading) {
				t.Errorf("the TIME heading is not %q:\n%s", want.heading, stdout)
			}
			if !strings.Contains(stdout, want.value) {
				t.Errorf("no row carries %q, so the heading describes a column that is not "+
					"there:\n%s", want.value, stdout)
			}
		})
	}
}

// TestTZMovesTheDisplayAndNotTheMoment is the conversion half.
//
// The task's own example: 22:41 on the 10th in UTC and 00:41 on the 11th in
// Warsaw are one instant, and the tester who read the first as the second was
// reading a real moment through a missing marker. A rendering that changed the
// moment rather than the frame would be a far worse bug than the one being fixed.
func TestTZMovesTheDisplayAndNotTheMoment(t *testing.T) {
	stdout, _, err := runTimeline(t, watchedCheckoutEngine(), defaultRequest(),
		render.Options{Zone: warsawZone(t)})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}

	// The fixture's first change is 14:02:58.001Z, which is 16:02:58.001+02:00.
	const shifted = "2026-08-28 16:02:58.001+02:00"
	if !strings.Contains(stdout, shifted) {
		t.Errorf("the first change does not render as %q in Europe/Warsaw:\n%s", shifted, stdout)
	}
	if strings.Contains(stdout, "2026-08-28 14:02:58.001") {
		t.Errorf("the UTC spelling survives in a Europe/Warsaw rendering, so one table carries "+
			"two frames:\n%s", stdout)
	}
}

// TestAnUnknownZoneIsAUsageError puts the flag's rejection on the real command
// tree, where the exit code is decided.
//
// Exit 2 rather than a rendering surprise, and it matters more here than for most
// flags: an instant in the wrong zone is still a perfectly plausible instant, so a
// zone that fell back silently would produce output nobody would question.
func TestAnUnknownZoneIsAUsageError(t *testing.T) {
	io, _, errOut := streams()

	code := cli.Run([]string{options.StandaloneName, "--tz", "Europe/Warsav", "version"}, io)
	if code != exit.UsageError {
		t.Errorf("a mistyped zone exited %d, want %d", code, exit.UsageError)
	}
	for _, want := range []string{"--tz", options.ZoneUTC, options.ZoneLocal, "Europe/Warsaw"} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("the rejection never names %q, so it says what is wrong and not what is "+
				"right:\n%s", want, errOut.String())
		}
	}
}

// TestTheDefaultIsUTCAndStatingItChangesNothing keeps `--tz utc` honest.
//
// The default stays UTC (D46) and saying so out loud has to produce the default
// rather than something that resembles it. A `utc` that resolved to a location
// object rather than collapsing onto the zero value would render `+00:00` where a
// bare invocation renders `Z` — two spellings of one frame, in the task about
// exactly that.
func TestTheDefaultIsUTCAndStatingItChangesNothing(t *testing.T) {
	var stated options.TimeZone
	if err := stated.Set(options.ZoneUTC); err != nil {
		t.Fatalf("Set(%q): %v", options.ZoneUTC, err)
	}

	bare, _, err := runTimeline(t, watchedCheckoutEngine(), defaultRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	explicit, _, err := runTimeline(t, watchedCheckoutEngine(), defaultRequest(),
		render.Options{Zone: stated.Zone()})
	if err != nil {
		t.Fatalf("RunTimeline --tz utc: %v", err)
	}

	if bare != explicit {
		t.Errorf("--tz utc renders differently from the default it states:\n%s\nversus\n%s",
			explicit, bare)
	}
	if !strings.Contains(bare, "2026-08-28 14:02:58.001Z") {
		t.Errorf("the default table does not carry the frame on the value:\n%s", bare)
	}
}

// TestTheBareInstantDetectorCatchesABareInstant is the sweep's non-vacuity half.
//
// Every assertion above is a regexp finding nothing, and a regexp that matched
// nothing at all would make the whole file pass over output it never read. These
// are the exact spellings the defect and the fix produce.
func TestTheBareInstantDetectorCatchesABareInstant(t *testing.T) {
	bare := []string{
		// The field report, verbatim.
		"2026-09-10 22:41:13.263",
		// The same row as it appeared in a table.
		"2026-09-10 22:41:13.263  Modified  argocd-application-controller",
		// Seconds precision, as a header field would have spelled it.
		"2026-09-10T22:41:13",
	}
	framed := []string{
		"2026-09-10 22:41:13.263Z",
		"2026-09-10 22:41:13.263Z  Modified  argocd",
		"2026-09-11 00:41:13.263+02:00",
		"2026-09-10T22:41:13.263000000Z",
		"2026-09-11T00:41:13+02:00",
		// A negative offset, which is the half a `+`-only check would miss.
		"2026-09-10 18:41:13.263-04:00",
	}

	for _, line := range bare {
		if !bareInstant.MatchString(line) {
			t.Errorf("the detector does not catch %q, so every sweep above is passing over "+
				"output it cannot read", line)
		}
	}
	for _, line := range framed {
		if found := bareInstant.FindString(line); found != "" {
			t.Errorf("the detector reports %q as bare (matched %q), which would fail the sweep "+
				"for output that is correct", line, found)
		}
	}
}

// structuredFormats are the three renderings of the versioned envelope.
//
// Named here rather than typed at each call below so that a fourth format cannot
// be added without the invariance sweep covering it.
var structuredFormats = []render.StructuredFormat{
	render.StructuredJSON, render.StructuredJSONL, render.StructuredYAML,
}

// TestStructuredOutputIgnoresTZ is the guard that matters most in this task.
//
// `ts` is a machine contract under D19, and its meaning may not depend on the
// shell that produced it (D46). The natural implementation of --tz threads a
// formatter through the renderer, and the structured path is the one that must
// not receive it — so the assertion is byte-identity of stdout rather than a
// check on one field: a coverage summary, a reconstruction header or a window
// rendered in the reader's zone would each slip past a test that only read `ts`.
//
// metadata is the half that was actually at risk. metadata.coverage.summary is
// built by coverageSummary, the same function that renders the human header's
// Coverage line, and threading the invocation's frame into it would have been the
// obvious change. coverageAnswer.Report pins it to render.UTC instead, one line
// from where Summary passes the frame through.
//
// Standard error is deliberately not compared. The notices are the human half of
// the invocation and do follow --tz, because a notice naming an instant in a
// frame the reader did not ask for would be the mixed-frame defect this task
// closes — and nothing parses stderr as an envelope.
func TestStructuredOutputIgnoresTZ(t *testing.T) {
	warsaw := render.Options{Zone: warsawZone(t)}

	for _, format := range structuredFormats {
		t.Run(string(format), func(t *testing.T) {
			commands := map[string]func(opts render.Options) (string, error){
				"timeline": func(opts render.Options) (string, error) {
					request := defaultRequest()
					request.Structured = format
					stdout, _, err := runTimeline(t, watchedCheckoutEngine(), request, opts)
					return stdout, err
				},
				"diff": func(opts render.Options) (string, error) {
					request := defaultDiffRequest()
					request.Timeline.Structured = format
					stdout, _, err := runDiff(t, watchedCheckoutEngine(), request, opts)
					return stdout, err
				},
				"blame": func(opts render.Options) (string, error) {
					request := defaultBlameRequest()
					request.Timeline.Structured = format
					stdout, _, err := runBlame(t, blamedCheckoutEngine(), request, opts)
					return stdout, err
				},
				"get": func(opts render.Options) (string, error) {
					stdout, _, err := runGetWith(t, checkpointEngine(t), getRequest(format), opts)
					return stdout, err
				},
				"scopes": func(opts render.Options) (string, error) {
					request := scopesRequest()
					request.Structured = format
					engine := &fakeEngine{caps: clickHouseCapabilities(), intervals: recordedScopes()}
					stdout, _, err := runScopes(t, engine, request, opts)
					return stdout, err
				},
			}

			for name, run := range commands {
				t.Run(name, func(t *testing.T) {
					plain, err := run(render.Options{Width: goldenWidth})
					if err != nil {
						t.Fatalf("%s: %v", name, err)
					}
					shifted, err := run(render.Options{Width: goldenWidth, Zone: warsaw.Zone})
					if err != nil {
						t.Fatalf("%s --tz Europe/Warsaw: %v", name, err)
					}

					if plain != shifted {
						t.Errorf("--tz Europe/Warsaw changed the %s envelope. `ts` is a machine "+
							"contract and a consumer must not find its meaning depends on the "+
							"shell that produced it (D19, D46).\nwithout --tz:\n%s\nwith:\n%s",
							name, plain, shifted)
					}
					// Non-vacuity: an envelope holding no instant would compare
					// equal whatever the frame did.
					if !strings.Contains(plain, "Z") {
						t.Errorf("the %s envelope carries no UTC instant, so this comparison is "+
							"measuring nothing:\n%s", name, plain)
					}
					assertNoBareInstant(t, name+" envelope", plain)
				})
			}
		})
	}
}

// TestTimelineRendersAShiftedZone pins the whole document in a non-UTC frame, in
// both colour modes.
//
// Golden rather than assertions for the reason every other rendering here has
// one: what is being pinned is the layout as well as the values — the heading
// widened by the offset the rows carry, and the header block and the notice on
// stderr in the same frame as the table between them. An assertion on a substring
// would pass with the header still in UTC.
//
// Both modes, because the frame is the one thing on the line that is *not*
// colour: TestColourIsNothingButColour's property says a coloured rendering must
// be reconstructible from the plain one, and a frame that arrived only under one
// of them would be a character the two do not share.
func TestTimelineRendersAShiftedZone(t *testing.T) {
	for mode, color := range map[string]bool{"": false, "-color": true} {
		t.Run("tz-warsaw"+mode, func(t *testing.T) {
			stdout, stderr, err := runTimeline(t, watchedCheckoutEngine(), defaultRequest(),
				render.Options{Color: color, Zone: warsawZone(t)})
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}
			assertGolden(t, "tz-warsaw"+mode, stdout, stderr)
		})
	}
}
