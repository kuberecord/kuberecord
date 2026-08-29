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
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli"
)

// TestParseInstantReadsBothGrammars covers the one flag that takes either a
// duration or an instant.
//
// kubectl splits these into --since and --since-time, which is two flags for one
// question. One flag can tell them apart without ambiguity, because no timestamp
// begins with a digit followed by a duration unit.
func TestParseInstantReadsBothGrammars(t *testing.T) {
	now := time.Date(2026, 8, 28, 15, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		given string
		want  time.Time
	}{
		{"hours", "6h", now.Add(-6 * time.Hour)},
		{"minutes", "90m", now.Add(-90 * time.Minute)},
		{"a compound Go duration keeps its exact meaning", "1h30m", now.Add(-90 * time.Minute)},
		{"days, which Go's own parser refuses", "3d", now.Add(-72 * time.Hour)},
		{"weeks", "2w", now.Add(-14 * 24 * time.Hour)},
		{"days and hours together", "1d6h", now.Add(-30 * time.Hour)},
		{"a fractional day", "1.5d", now.Add(-36 * time.Hour)},
		{"an RFC 3339 instant", "2026-08-20T14:00:00Z", time.Date(2026, 8, 20, 14, 0, 0, 0, time.UTC)},
		{
			name:  "an RFC 3339 instant with an offset is normalized to UTC",
			given: "2026-08-20T16:00:00+02:00",
			want:  time.Date(2026, 8, 20, 14, 0, 0, 0, time.UTC),
		},
		{"a bare date is UTC midnight", "2026-08-20", time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)},
		{"a date and a time", "2026-08-20 14:00:00", time.Date(2026, 8, 20, 14, 0, 0, 0, time.UTC)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := cli.ParseInstant(test.given, now)
			if err != nil {
				t.Fatalf("ParseInstant(%q): %v", test.given, err)
			}
			if !got.Equal(test.want) {
				t.Errorf("ParseInstant(%q) = %s, want %s", test.given, got, test.want)
			}
		})
	}
}

// TestParseInstantRejectsWhatItCannotRead pins the failures as usage errors.
//
// Exit code 2 rather than 1 is the point: a wrapper script told to retry on a
// backend timeout must not retry a mistyped flag value.
func TestParseInstantRejectsWhatItCannotRead(t *testing.T) {
	now := time.Date(2026, 8, 28, 15, 0, 0, 0, time.UTC)

	for _, given := range []string{"", "   ", "yesterday", "3 days", "6hours", "-2h", "2026-13-45"} {
		t.Run(given, func(t *testing.T) {
			if _, err := cli.ParseInstant(given, now); err == nil {
				t.Fatalf("ParseInstant(%q) was accepted", given)
			} else if code := cli.ExitCodeFor(err); code != cli.ExitUsageError {
				t.Errorf("ParseInstant(%q) failed with exit code %d, want %d", given, code, cli.ExitUsageError)
			}
		})
	}
}

// TestDescribeWindow covers the phrasing an empty result depends on, including
// both unbounded ends.
func TestDescribeWindow(t *testing.T) {
	from := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 28, 15, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		from time.Time
		to   time.Time
		want string
	}{
		{"both ends", from, to, "2026-08-01T00:00:00Z to 2026-08-28T15:00:00Z"},
		{"no end", from, time.Time{}, "2026-08-01T00:00:00Z to now"},
		{"no start", time.Time{}, to, "everything up to 2026-08-28T15:00:00Z"},
		{"neither", time.Time{}, time.Time{}, "all recorded history"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := cli.DescribeWindow(test.from, test.to); got != test.want {
				t.Errorf("DescribeWindow = %q, want %q", got, test.want)
			}
		})
	}
}
