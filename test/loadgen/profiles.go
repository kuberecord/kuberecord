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

package loadgen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ProfilesDir is where the shipped scale profiles live, relative to this
// package. They are data files rather than Go literals so that "what load did
// you measure?" is answerable by reading one small JSON file — the same file the
// harness and the numbers in docs/PERFORMANCE.md both come from — instead of by
// reconstructing a command line out of a Makefile and a table.
const ProfilesDir = "profiles"

// The three named profiles Task 2.3 publishes envelopes for. They are constants
// rather than a discovered directory listing because the acceptance criteria name
// them: a run of `make bench-load PROFILE=massive` has to mean the same load on
// every machine that reproduces the published table.
const (
	// ProfileSmall is the "tiny cluster" end of D2: a single team's namespace.
	ProfileSmall = "small"
	// ProfileMedium is a mid-size production cluster.
	ProfileMedium = "medium"
	// ProfileMassive is the "massive cluster" end of D2 and the profile the
	// Phase 2 acceptance criteria are stated against: ≥20,000 watched objects
	// and ≥500 sustained updates/sec across mixed GVKs, with deletes in the mix.
	ProfileMassive = "massive"
)

// ShippedProfiles are the profiles that must exist and stay runnable. Anything
// else in ProfilesDir is a developer's local experiment.
var ShippedProfiles = []string{ProfileSmall, ProfileMedium, ProfileMassive}

// Profile is one named load shape plus the pass criteria its run is judged
// against.
//
// Pass criteria travel *with* the profile on purpose. A throughput number is
// meaningless without the load that produced it, so keeping "20,000 objects at
// 500 mutations/sec" and "must settle ≥500 records/sec with p99 enqueue-block
// under 10 ms" in one file makes the claim self-describing — and makes the
// harness able to fail the run itself rather than leaving a human to compare
// stdout against a doc.
type Profile struct {
	// Name is the profile's identity, matched against the file's base name so a
	// mis-copied file cannot quietly run under the wrong label.
	Name string `json:"name"`
	// Description is what this shape is meant to represent, echoed into the run
	// log so a captured console session says which envelope it belongs to.
	Description string `json:"description"`

	// Objects is how many objects exist before the sustain phase starts, spread
	// evenly across Kinds.
	Objects int `json:"objects"`
	// Rate is the sustained mutations per second the churn phase targets.
	Rate int `json:"rate"`
	// PayloadBytes is the approximate size of the free-form filler each object
	// carries, which is what makes a profile's memory footprint per object
	// realistic rather than minimal.
	PayloadBytes int `json:"payloadBytes"`
	// Duration is how long the sustain phase runs, as a Go duration string.
	Duration Duration `json:"duration"`
	// DeleteRatio is the fraction of mutations that delete-and-recreate their
	// object instead of updating it, exercising the delete and reincarnation
	// paths (each such mutation produces two records, not one).
	DeleteRatio float64 `json:"deleteRatio"`
	// Concurrency is how many churn workers drive mutations in parallel. It
	// exists so the *generator* is never the bottleneck: the figures this
	// harness publishes are about the write path.
	Concurrency int `json:"concurrency"`
	// Kinds are the kinds churned, by Kind name (see churnKinds). More than one
	// is the "mixed GVKs" half of the massive profile: several informers, several
	// scopes, one shared pipeline and one shared hashCache keyspace.
	Kinds []string `json:"kinds"`

	// Pass is the recorded pass/fail criteria for this profile. A profile may
	// leave it empty, which means "measure and report, do not judge".
	Pass PassCriteria `json:"pass"`
}

// PassCriteria is what a profile's run must achieve to count as a pass. Zero
// fields are not checked, so a profile opts in per criterion.
type PassCriteria struct {
	// MinRecordsPerSec is the sustained settled-write rate the run must reach
	// over its churn window.
	MinRecordsPerSec float64 `json:"minRecordsPerSec"`
	// MaxEnqueueBlockP99Ms bounds the p99 time a pipeline worker spent blocked
	// handing a record to the sink — the hot-path backpressure Invariant 1 cares
	// about, measured while the backend is healthy.
	MaxEnqueueBlockP99Ms float64 `json:"maxEnqueueBlockP99Ms"`
}

// Duration is a time.Duration that reads from JSON as a Go duration string
// ("60s"), so a profile file says `"duration": "60s"` rather than an unlabelled
// nanosecond count nobody can review at a glance.
type Duration time.Duration

// UnmarshalJSON accepts a duration string. A bare number is rejected rather than
// guessed at: "60" could mean seconds or nanoseconds, and silently picking one
// would misreport an entire envelope.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("duration must be a Go duration string like \"60s\": %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalJSON renders the duration back as a string, so a round-tripped profile
// stays reviewable.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration().String())
}

// Duration returns the value as a time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// LoadProfile reads and validates the named profile from dir.
//
// Validation is eager and total: a profile with a zero rate or an unknown kind
// would otherwise surface as a division by zero or an empty run minutes into a
// twenty-thousand-object setup, and a published envelope must never be the
// product of a silently defaulted field.
func LoadProfile(dir, name string) (Profile, error) {
	path := filepath.Join(dir, name+".json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return Profile{}, fmt.Errorf("read load profile: %w", err)
	}

	var p Profile
	decoder := json.NewDecoder(bytes.NewReader(raw))
	// Unknown fields are an error: a typo'd key ("payload_bytes") would
	// otherwise leave the intended value at zero and report an envelope for a
	// load nobody asked for.
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return Profile{}, fmt.Errorf("parse load profile %s: %w", path, err)
	}

	if p.Name != name {
		return Profile{}, fmt.Errorf("load profile %s declares name %q, want %q", path, p.Name, name)
	}
	if err := p.Validate(); err != nil {
		return Profile{}, fmt.Errorf("invalid load profile %s: %w", path, err)
	}
	return p, nil
}

// Validate reports whether this profile describes a runnable load.
func (p Profile) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("name is required")
	}
	if p.Objects <= 0 {
		return fmt.Errorf("objects must be positive, got %d", p.Objects)
	}
	if p.Rate <= 0 {
		return fmt.Errorf("rate must be positive, got %d", p.Rate)
	}
	if p.PayloadBytes < 0 {
		return fmt.Errorf("payloadBytes must not be negative, got %d", p.PayloadBytes)
	}
	if p.Duration <= 0 {
		return fmt.Errorf("duration must be positive, got %s", p.Duration.Duration())
	}
	if p.DeleteRatio < 0 || p.DeleteRatio > 1 {
		return fmt.Errorf("deleteRatio must be in [0,1], got %v", p.DeleteRatio)
	}
	if p.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be positive, got %d", p.Concurrency)
	}
	if len(p.Kinds) == 0 {
		return fmt.Errorf("kinds must name at least one kind")
	}
	for _, kind := range p.Kinds {
		if _, ok := churnKinds[kind]; !ok {
			return fmt.Errorf("kind %q is not churnable by this harness (have %s)",
				kind, strings.Join(churnableKinds(), ", "))
		}
	}
	if len(slices.Compact(slices.Sorted(slices.Values(p.Kinds)))) != len(p.Kinds) {
		return fmt.Errorf("kinds must not repeat: %v", p.Kinds)
	}
	if p.Objects < len(p.Kinds) {
		return fmt.Errorf("objects (%d) must be at least one per kind (%d)", p.Objects, len(p.Kinds))
	}
	if p.Pass.MinRecordsPerSec < 0 {
		return fmt.Errorf("pass.minRecordsPerSec must not be negative, got %v", p.Pass.MinRecordsPerSec)
	}
	if p.Pass.MaxEnqueueBlockP99Ms < 0 {
		return fmt.Errorf("pass.maxEnqueueBlockP99Ms must not be negative, got %v", p.Pass.MaxEnqueueBlockP99Ms)
	}
	return nil
}

// String renders the profile as the one-line summary the harness logs and the
// performance doc quotes, so a table row and a console line always agree.
func (p Profile) String() string {
	return fmt.Sprintf("%s: objects=%d rate=%d/s payload=%dB duration=%s delete-ratio=%.2f concurrency=%d kinds=%s",
		p.Name, p.Objects, p.Rate, p.PayloadBytes, p.Duration.Duration(), p.DeleteRatio, p.Concurrency,
		strings.Join(p.Kinds, "+"))
}

// Overrides are per-knob overrides applied on top of a loaded profile, so a
// developer can bisect one dimension ("same massive profile, half the payload")
// without editing a shipped file and without inventing a fourth profile.
//
// Every field's zero value means "not overridden", which is why the numeric
// fields use -1 for "unset" wherever 0 is a value a caller might legitimately
// want (a zero delete ratio, a zero payload).
type Overrides struct {
	Objects      int           // <0: unset
	Rate         int           // <0: unset
	PayloadBytes int           // <0: unset
	Duration     time.Duration // <=0: unset
	DeleteRatio  float64       // <0: unset
	Concurrency  int           // <0: unset
	Kinds        []string      // empty: unset
}

// NoOverrides is the all-unset value, and the correct starting point for
// building one field by field.
func NoOverrides() Overrides {
	return Overrides{Objects: -1, Rate: -1, PayloadBytes: -1, Duration: 0, DeleteRatio: -1, Concurrency: -1}
}

// Apply returns p with o's set fields substituted, validated as a whole.
//
// It re-validates rather than trusting the caller because an override is exactly
// as capable of describing an unrunnable load as a profile file is, and the
// failure mode (a zero rate) is identical.
func (o Overrides) Apply(p Profile) (Profile, error) {
	if o.Objects >= 0 {
		p.Objects = o.Objects
	}
	if o.Rate >= 0 {
		p.Rate = o.Rate
	}
	if o.PayloadBytes >= 0 {
		p.PayloadBytes = o.PayloadBytes
	}
	if o.Duration > 0 {
		p.Duration = Duration(o.Duration)
	}
	if o.DeleteRatio >= 0 {
		p.DeleteRatio = o.DeleteRatio
	}
	if o.Concurrency >= 0 {
		p.Concurrency = o.Concurrency
	}
	if len(o.Kinds) > 0 {
		p.Kinds = o.Kinds
	}
	if err := p.Validate(); err != nil {
		return Profile{}, fmt.Errorf("invalid load profile %s after applying overrides: %w", p.Name, err)
	}
	return p, nil
}

// OverridesFromEnv reads the LOADGEN_* environment twins the Makefile forwards.
//
// getenv is a parameter so the resolution order — which is the part that decides
// whether a published envelope describes the profile it claims to — is testable
// without mutating the process environment. A malformed value is reported rather
// than ignored: silently running the default profile because LOADGEN_RATE was
// "500/s" is how a table ends up describing load nobody applied (Invariant 4's
// spirit, applied to the harness).
func OverridesFromEnv(getenv func(string) string) (Overrides, error) {
	o := NoOverrides()
	var err error
	if o.Objects, err = envInt(getenv, "LOADGEN_OBJECTS", o.Objects); err != nil {
		return Overrides{}, err
	}
	if o.Rate, err = envInt(getenv, "LOADGEN_RATE", o.Rate); err != nil {
		return Overrides{}, err
	}
	if o.PayloadBytes, err = envInt(getenv, "LOADGEN_PAYLOAD_BYTES", o.PayloadBytes); err != nil {
		return Overrides{}, err
	}
	if o.Concurrency, err = envInt(getenv, "LOADGEN_CONCURRENCY", o.Concurrency); err != nil {
		return Overrides{}, err
	}
	if raw := getenv("LOADGEN_DURATION"); raw != "" {
		parsed, parseErr := time.ParseDuration(raw)
		if parseErr != nil {
			return Overrides{}, fmt.Errorf("parse LOADGEN_DURATION %q: %w", raw, parseErr)
		}
		o.Duration = parsed
	}
	if raw := getenv("LOADGEN_DELETE_RATIO"); raw != "" {
		parsed, parseErr := strconv.ParseFloat(raw, 64)
		if parseErr != nil {
			return Overrides{}, fmt.Errorf("parse LOADGEN_DELETE_RATIO %q: %w", raw, parseErr)
		}
		o.DeleteRatio = parsed
	}
	if raw := getenv("LOADGEN_KINDS"); raw != "" {
		for part := range strings.SplitSeq(raw, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				o.Kinds = append(o.Kinds, trimmed)
			}
		}
	}
	return o, nil
}

// envInt parses one integer environment twin, leaving unset if absent.
func envInt(getenv func(string) string, key string, unset int) (int, error) {
	raw := getenv(key)
	if raw == "" {
		return unset, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s %q: %w", key, raw, err)
	}
	return parsed, nil
}
