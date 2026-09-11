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

package render_test

import (
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// The field report, as a test: `2026-09-10 22:41:13.263` was read as a local time
// and reported as wrong. It was UTC, and the marker had moved to a column heading
// that scrolls off the table and does not travel when a row is pasted somewhere
// else (D45).

// fieldReportInstant is the instant from that report, to the millisecond the
// narrow layout shows.
//
// It is that one rather than a round number because the whole finding turns on
// two readings of a single moment: 22:41 on the 10th in UTC and 00:41 on the 11th
// in Warsaw are the same instant, and a test written on a timestamp whose date
// does not change under the conversion would not exercise the confusion.
var fieldReportInstant = time.Date(2026, 9, 10, 22, 41, 13, 263_000_000, time.UTC)

// warsaw is the zone from the report. It is loaded rather than constructed with
// FixedZone because the DST case below is the reason the heading names a zone and
// not an offset, and a fixed zone has no DST to have.
func warsaw(t *testing.T) render.Zone {
	t.Helper()

	location, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		t.Fatalf("Europe/Warsaw is not in the database this binary carries: %v", err)
	}
	return render.NewZone(location, "Europe/Warsaw")
}

// TestEveryLayoutCarriesItsFrame is the task's central assertion.
//
// Every rendering of an instant, in every zone, ends in something that says what
// frame it is in. A bare timestamp is the defect: it looks local wherever it is
// read, which is worse than being local, because nothing in it says otherwise.
func TestEveryLayoutCarriesItsFrame(t *testing.T) {
	zones := map[string]render.Zone{"utc": render.UTC, "Europe/Warsaw": warsaw(t)}

	for name, zone := range zones {
		t.Run(name, func(t *testing.T) {
			for layout, got := range map[string]string{
				"narrow":  zone.Narrow(fieldReportInstant),
				"wide":    zone.Wide(fieldReportInstant),
				"instant": zone.Instant(fieldReportInstant),
			} {
				if !strings.HasSuffix(got, "Z") && !strings.Contains(got, "+") && !strings.Contains(got, "-07") {
					t.Errorf("the %s layout rendered %q, which carries no frame: pasted into a "+
						"ticket it reads as a local time in whatever zone the reader is in (D45)",
						layout, got)
				}
			}
		})
	}
}

// TestTheDefaultRendersUTCWithAZ pins the three UTC layouts by value.
//
// By value rather than by property, because the narrow one is the layout the task
// changed and the other two are the layouts it must not have: a change to either
// of those would be a change to `-o wide` and to every header field in the CLI,
// made silently while fixing something else.
func TestTheDefaultRendersUTCWithAZ(t *testing.T) {
	for layout, want := range map[string]string{
		"narrow":  "2026-09-10 22:41:13.263Z",
		"wide":    "2026-09-10T22:41:13.263000000Z",
		"instant": "2026-09-10T22:41:13Z",
	} {
		got := map[string]string{
			"narrow":  render.UTC.Narrow(fieldReportInstant),
			"wide":    render.UTC.Wide(fieldReportInstant),
			"instant": render.UTC.Instant(fieldReportInstant),
		}[layout]
		if got != want {
			t.Errorf("the %s UTC layout rendered %q, want %q", layout, got, want)
		}
	}
}

// TestANonUTCZoneRendersAnExplicitOffset is the prohibition the flag exists for.
//
// A bare local timestamp is what a shell alias produces, and it is exactly what
// the CLI's design forbids. The offset is what makes the value survive being
// pasted somewhere the reader's own zone is different.
func TestANonUTCZoneRendersAnExplicitOffset(t *testing.T) {
	zone := warsaw(t)

	for layout, want := range map[string]string{
		"narrow":  "2026-09-11 00:41:13.263+02:00",
		"wide":    "2026-09-11T00:41:13.263000000+02:00",
		"instant": "2026-09-11T00:41:13+02:00",
	} {
		got := map[string]string{
			"narrow":  zone.Narrow(fieldReportInstant),
			"wide":    zone.Wide(fieldReportInstant),
			"instant": zone.Instant(fieldReportInstant),
		}[layout]
		if got != want {
			t.Errorf("the %s Europe/Warsaw layout rendered %q, want %q", layout, got, want)
		}
	}
}

// TestEveryFrameNamesTheSameInstant is the property behind all of the above.
//
// Three renderings, one moment. It is asserted by parsing each back and comparing
// what came out, so a layout that dropped a field — the date, the offset — or
// converted where it should have formatted fails here rather than in a reader's
// post-mortem.
func TestEveryFrameNamesTheSameInstant(t *testing.T) {
	zones := map[string]render.Zone{
		"utc":           render.UTC,
		"local":         render.NewZone(time.Local, "local"),
		"Europe/Warsaw": warsaw(t),
	}

	for name, zone := range zones {
		t.Run(name, func(t *testing.T) {
			// The wide layout, because it is the only one carrying the full
			// precision the instant has; a millisecond layout would compare equal
			// for a bug that lost nanoseconds.
			parsed, err := time.Parse("2006-01-02T15:04:05.000000000Z07:00", zone.Wide(fieldReportInstant))
			if err != nil {
				t.Fatalf("the wide layout produced %q, which is not a parseable instant: %v",
					zone.Wide(fieldReportInstant), err)
			}
			if !parsed.Equal(fieldReportInstant) {
				t.Errorf("%s renders %s, which is a different moment from %s: a conversion was "+
					"applied where a format change was intended",
					name, parsed.UTC(), fieldReportInstant)
			}
		})
	}
}

// TestTheHeadingNamesTheZoneRatherThanAnOffset is why the heading is a name.
//
// Europe/Warsaw is +02:00 in September and +01:00 in January. A heading carrying a
// fixed offset would contradict half the rows of any table spanning the change —
// which is the defect this task exists to close, reintroduced one layer along. A
// zone name cannot disagree with a row, and every value carries its own offset.
func TestTheHeadingNamesTheZoneRatherThanAnOffset(t *testing.T) {
	zone := warsaw(t)

	summer := zone.Narrow(fieldReportInstant)
	winter := zone.Narrow(time.Date(2026, 1, 10, 22, 41, 13, 0, time.UTC))
	if !strings.Contains(summer, "+02:00") || !strings.Contains(winter, "+01:00") {
		t.Fatalf("the fixture no longer spans a DST change (%q and %q), so this asserts nothing "+
			"about a heading that would have to name one of two offsets", summer, winter)
	}

	if got, want := zone.TimeColumn(), "TIME (Europe/Warsaw)"; got != want {
		t.Errorf("the TIME heading is %q, want %q: an offset there would be wrong for one of the "+
			"two rows above", got, want)
	}
	if got, want := render.UTC.TimeColumn(), "TIME (UTC)"; got != want {
		t.Errorf("the default TIME heading is %q, want %q", got, want)
	}
}

// TestUTCSpellingsCollapseOntoTheZeroValue keeps `--tz utc` from being a
// different rendering from passing no flag.
//
// Stating the default must produce the default. A Zone built from time.UTC that
// did not compare identical to the zero value would render through the zoned
// layouts, and `--tz utc` would print `+00:00` where a bare invocation prints `Z`
// — two spellings of one frame, in the task about exactly that.
func TestUTCSpellingsCollapseOntoTheZeroValue(t *testing.T) {
	named, err := time.LoadLocation("UTC")
	if err != nil {
		t.Fatalf("LoadLocation(\"UTC\"): %v", err)
	}

	for name, zone := range map[string]render.Zone{
		"nil location":   render.NewZone(nil, ""),
		"time.UTC":       render.NewZone(time.UTC, ""),
		"loaded by name": render.NewZone(named, "UTC"),
	} {
		if !zone.IsUTC() {
			t.Errorf("%s did not collapse onto UTC", name)
		}
		if got := zone.Narrow(fieldReportInstant); got != render.UTC.Narrow(fieldReportInstant) {
			t.Errorf("%s renders %q where the default renders %q", name, got,
				render.UTC.Narrow(fieldReportInstant))
		}
	}
}

// TestAnUnnameableZoneIsStillLabelled covers `--tz local` on a host whose local
// zone Go cannot name.
//
// time.Local.String() answers "Local" wherever the zone came from /etc/localtime,
// and a heading reading `TIME (Local)` names nothing a reader can act on. The
// values still carry their exact offsets, so the heading only has to point at the
// frame rather than be it.
func TestAnUnnameableZoneIsStillLabelled(t *testing.T) {
	zone := render.NewZone(time.FixedZone("", 3*60*60), "")
	if got, want := zone.TimeColumn(), "TIME (local)"; got != want {
		t.Errorf("the heading for an unnameable zone is %q, want %q", got, want)
	}
	if got, want := zone.Narrow(fieldReportInstant), "2026-09-11 01:41:13.263+03:00"; got != want {
		t.Errorf("an unnameable zone rendered %q, want %q: the value must carry the offset the "+
			"heading cannot", got, want)
	}
}
