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
	"slices"
	"time"

	// The IANA database, compiled in.
	//
	// Without it time.LoadLocation reads the host's zoneinfo, which Windows does
	// not have and a scratch container does not ship — so `--tz Europe/Warsaw`
	// would resolve on three of the five platforms `make build-cli` produces and
	// fail on the others with `unknown time zone`. A flag whose validity depends on
	// the operating system is a flag nobody can document, and it is the same
	// property D18 buys with pure Go and no cgo: one build serves every platform
	// identically.
	//
	// It is already in this binary's dependency closure — clickhouse-go imports it
	// — and that is exactly why it is written here as well. An inherited guarantee
	// is one somebody else can withdraw in a patch release, and the failure it
	// would cause is invisible on the machine that cuts the release and total on
	// Windows. The import costs nothing while the dependency keeps it and is the
	// whole of the guarantee the day it does not.
	//
	// It does not override the host's database. time.LoadLocation consults
	// $ZONEINFO and the system directory first and falls back to this, so a host
	// with a newer tzdata than the toolchain still wins.
	_ "time/tzdata"

	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// The two spellings that are not location names, and the one a user gets for
// free.
//
// `utc` is legal to state rather than merely being the default. A reader who has
// been told the CLI is UTC and wants to be sure of it should be able to say so,
// and an accepted set that refused the default would be a set that punished the
// person being careful.
const (
	// ZoneUTC is the default and the frame the schema records in (D46).
	ZoneUTC = "utc"

	// ZoneLocal is the host's own zone, rendered with an explicit numeric offset
	// like every other non-UTC frame.
	ZoneLocal = "local"
)

// zoneKeywords is the accepted set that is not an IANA name, in the order a user
// is shown it.
var zoneKeywords = []string{ZoneUTC, ZoneLocal}

// ZoneKeywords returns the --tz values that are not location names, in the order
// a user is shown them.
//
// It exists so that the help string, the rejection message and the shell
// completion menu are three renderings of one list rather than three lists that
// agree today — the reason OutputFormats is a function. The IANA half is
// deliberately not enumerated: it is six hundred names, a completion menu of it
// would be unusable, and the database is the authority on which of them exist.
func ZoneKeywords() []string { return slices.Clone(zoneKeywords) }

// TimeZone is the value of --tz: the frame every human-facing instant in one
// invocation is rendered in.
//
// # What it does not reach
//
// Structured output. `-o json`, `-o jsonl` and `-o yaml` emit UTC whatever this
// says, in every envelope kind and including metadata, because `ts` is a machine
// contract under D19 and a consumer must not find its meaning depends on the
// shell that produced it (D46). The enforcement is structural rather than
// conditional: the renderer's envelope path is never handed a Zone, so there is
// nothing there for a `--tz` to change.
//
// # Why the flag exists at all
//
// A non-UTC frame is rendered with an explicit numeric offset and never bare,
// which is the whole reason this is a flag rather than something a reader does
// with `date -d` in a shell alias: an alias produces exactly the bare local
// timestamp the CLI's design forbids, and the offset is what makes a pasted
// instant survive the paste (D45).
type TimeZone struct {
	// spelling is what the user typed, kept so that --help and an error message
	// echo the word rather than Go's name for the location behind it.
	spelling string

	// zone is the resolved frame. The zero value is UTC, so a TimeZone nobody set
	// renders the default.
	zone render.Zone
}

// String implements pflag.Value, and is what `--help` prints as the default.
func (z *TimeZone) String() string {
	if z.spelling == "" {
		return ZoneUTC
	}
	return z.spelling
}

// Type implements pflag.Value and names the value in `--help`.
func (z *TimeZone) Type() string { return "zone" }

// Set implements pflag.Value, rejecting a zone the database does not have.
//
// The rejection is what routes a mistyped zone to exit.UsageError: pflag wraps
// this error, the root's flag-error function codes it, and the process ends with
// 2 rather than with a silently wrong frame — which is the one failure mode a
// timezone flag must not have, since an instant in the wrong zone is still a
// perfectly plausible instant.
//
// It names the accepted forms rather than only the failure, for the reason every
// error this release touches does (Invariant 4): "unknown time zone Europe/Warsav"
// tells a reader they mistyped and not what the correct shapes are.
//
// The empty string is refused explicitly. time.LoadLocation answers it with UTC,
// so `--tz ""` would otherwise be a silent way of asking for the default through
// a value that looks like a mistake.
func (z *TimeZone) Set(value string) error {
	switch value {
	case "":
		return fmt.Errorf("must be %s, %s, or an IANA location name such as Europe/Warsaw, and not empty",
			ZoneUTC, ZoneLocal)
	case ZoneUTC:
		z.spelling, z.zone = value, render.UTC
		return nil
	case ZoneLocal:
		z.spelling, z.zone = value, render.NewZone(time.Local, ZoneLocal)
		return nil
	}

	location, err := time.LoadLocation(value)
	if err != nil {
		return fmt.Errorf("must be %s, %s, or an IANA location name such as Europe/Warsaw: %w",
			ZoneUTC, ZoneLocal, err)
	}
	z.spelling, z.zone = value, render.NewZone(location, value)
	return nil
}

// Zone is the resolved frame, for a renderer.
func (z TimeZone) Zone() render.Zone { return z.zone }
