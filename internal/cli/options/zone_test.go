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

package options_test

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli/options"
)

// TestTimeZoneAcceptsTheThreeForms covers what --tz takes.
//
// `utc` is in the list because stating the default has to be legal: a reader who
// has been told the CLI renders UTC and wants to be sure of it should be able to
// say so, and an accepted set that refused the default would punish the person
// being careful.
func TestTimeZoneAcceptsTheThreeForms(t *testing.T) {
	tests := []struct {
		name  string
		value string
		utc   bool
		label string
	}{
		{name: "the default, stated", value: "utc", utc: true, label: "UTC"},
		{name: "an IANA location", value: "Europe/Warsaw", label: "Europe/Warsaw"},
		{
			name: "Go's own spelling of UTC collapses onto the default",
			// LoadLocation accepts it, and it must not become a second frame that
			// renders +00:00 where the default renders Z.
			value: "UTC", utc: true, label: "UTC",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var zone options.TimeZone
			if err := zone.Set(test.value); err != nil {
				t.Fatalf("Set(%q): %v", test.value, err)
			}
			if got := zone.Zone().IsUTC(); got != test.utc {
				t.Errorf("Set(%q) produced IsUTC() = %v, want %v", test.value, got, test.utc)
			}
			if got := zone.Zone().Label(); got != test.label {
				t.Errorf("Set(%q) is labelled %q, want %q", test.value, got, test.label)
			}
			if got := zone.String(); got != test.value {
				t.Errorf("Set(%q) echoes %q; --help and an error must show the word the user typed",
					test.value, got)
			}
		})
	}
}

// TestLocalResolvesToTheHostsZone is separated from the table because what it
// resolves to depends on the machine, so only the wiring can be asserted.
func TestLocalResolvesToTheHostsZone(t *testing.T) {
	var zone options.TimeZone
	if err := zone.Set(options.ZoneLocal); err != nil {
		t.Fatalf("Set(%q): %v", options.ZoneLocal, err)
	}

	stamp := time.Date(2026, 9, 10, 22, 41, 13, 263_000_000, time.UTC)
	if got, want := zone.Zone().Narrow(stamp), stamp.In(time.Local).Format("2006-01-02 15:04:05.000-07:00"); got != want {
		// The want side spells the layout out rather than calling the renderer,
		// so this compares the flag's plumbing against time.Local directly rather
		// than against the thing under test.
		if !zone.Zone().IsUTC() {
			t.Errorf("--tz local rendered %q, want %q", got, want)
		}
	}
}

// TestAnUnknownZoneIsRejectedAndNamesTheForms is Invariant 4 on the flag.
//
// The rejection routes to exit 2 through pflag, which matters more here than for
// most flags: an instant in the wrong zone is still a perfectly plausible
// instant, so a silently-wrong frame is a failure nobody would notice.
func TestAnUnknownZoneIsRejectedAndNamesTheForms(t *testing.T) {
	for _, value := range []string{"Nowhere/Nothing", "Europe/Warsav", "+02:00", ""} {
		t.Run(value, func(t *testing.T) {
			var zone options.TimeZone
			err := zone.Set(value)
			if err == nil {
				t.Fatalf("Set(%q) was accepted; a zone the database does not have must be refused",
					value)
			}
			for _, form := range []string{options.ZoneUTC, options.ZoneLocal, "Europe/Warsaw"} {
				if !strings.Contains(err.Error(), form) {
					t.Errorf("the rejection of %q never names %q, so it says what is wrong and not "+
						"what is right: %v", value, form, err)
				}
			}
			if !zone.Zone().IsUTC() {
				t.Errorf("Set(%q) failed and still moved the frame", value)
			}
		})
	}
}

// TestZoneKeywordsAreTheCompletionMenu keeps the accepted set and the menu one
// list, as OutputFormats and ColorModes are.
func TestZoneKeywordsAreTheCompletionMenu(t *testing.T) {
	keywords := options.ZoneKeywords()
	if len(keywords) != 2 || keywords[0] != options.ZoneUTC || keywords[1] != options.ZoneLocal {
		t.Fatalf("ZoneKeywords() is %v, want [%s %s]", keywords, options.ZoneUTC, options.ZoneLocal)
	}

	// A clone, for the reason OutputFormats returns one: a consumer that could
	// append to the accepted set would be teaching --tz a word nothing resolves.
	keywords[0] = "mutated"
	if options.ZoneKeywords()[0] != options.ZoneUTC {
		t.Error("ZoneKeywords() hands out the package's own slice")
	}
}

// TestTheZoneDatabaseIsImportedHere keeps --tz's platform guarantee this
// package's own.
//
// The direct import, not the closure. `time/tzdata` is already reachable from the
// CLI binary because clickhouse-go imports it, so a closure check would pass with
// zone.go's blank import deleted — a vacuous test of exactly the kind
// TestTheReadPlaneContractIsActuallyReached exists to prevent elsewhere in this
// repository.
//
// What must not change is that the guarantee is declared here. Inherited, it is
// one a dependency can withdraw in a patch release, and the failure that follows
// is invisible on the machine that cuts the release and total on the Windows and
// scratch targets: `--tz Europe/Warsaw` stops resolving. Asserting the import is
// the only way to notice the day it goes.
func TestTheZoneDatabaseIsImportedHere(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("the go command is not on PATH, so the import cannot be checked: %v", err)
	}

	cmd := exec.Command(goBin, "list", "-f", `{{join .Imports "\n"}}`, ".")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, stderr.String())
	}

	if !slices.Contains(strings.Fields(string(out)), "time/tzdata") {
		t.Error("internal/cli/options no longer imports time/tzdata directly, so --tz with an IANA " +
			"location name resolves only where a dependency still happens to embed the database " +
			"and where the host has a zoneinfo directory (Task 18.9)")
	}
}
