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
	"strings"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli"
)

// TestOutputFormatSet fixes the accepted vocabulary of --output.
//
// The set is closed on purpose. A released CLI that quietly accepted `-o JSON`
// would have to keep accepting it, and a `-o tabel` that fell through to a table
// would be a typo the user never learns they made.
func TestOutputFormatSet(t *testing.T) {
	tests := []struct {
		value   string
		want    cli.OutputFormat
		wantErr bool
	}{
		{value: "table", want: cli.OutputTable},
		{value: "wide", want: cli.OutputWide},
		{value: "json", want: cli.OutputJSON},
		{value: "jsonl", want: cli.OutputJSONL},
		{value: "yaml", want: cli.OutputYAML},
		{value: "diff", want: cli.OutputDiff},
		{value: "JSON", wantErr: true},
		{value: "tabel", wantErr: true},
		{value: "", wantErr: true},
		{value: "yml", wantErr: true},
	}

	for _, test := range tests {
		t.Run("-o "+test.value, func(t *testing.T) {
			format := cli.OutputTable
			err := format.Set(test.value)

			switch {
			case test.wantErr && err == nil:
				t.Fatalf("Set(%q) succeeded, want a rejection", test.value)
			case test.wantErr:
				// The message has to teach: it is the only thing between a user
				// and guessing the vocabulary.
				if !strings.Contains(err.Error(), "table") {
					t.Errorf("rejection does not list the accepted set: %v", err)
				}
				if format != cli.OutputTable {
					t.Errorf("a rejected value still changed the format to %q", format)
				}
			case err != nil:
				t.Fatalf("Set(%q): %v", test.value, err)
			case format != test.want:
				t.Errorf("Set(%q) gave %q, want %q", test.value, format, test.want)
			}
		})
	}
}

// TestColorModeSet fixes the accepted vocabulary of --color.
func TestColorModeSet(t *testing.T) {
	tests := []struct {
		value   string
		want    cli.ColorMode
		wantErr bool
	}{
		{value: "auto", want: cli.ColorAuto},
		{value: "always", want: cli.ColorAlways},
		{value: "never", want: cli.ColorNever},
		{value: "Always", wantErr: true},
		{value: "yes", wantErr: true},
		{value: "true", wantErr: true},
		{value: "", wantErr: true},
	}

	for _, test := range tests {
		t.Run("--color "+test.value, func(t *testing.T) {
			mode := cli.ColorAuto
			err := mode.Set(test.value)

			switch {
			case test.wantErr && err == nil:
				t.Fatalf("Set(%q) succeeded, want a rejection", test.value)
			case test.wantErr:
				if !strings.Contains(err.Error(), "always") {
					t.Errorf("rejection does not list the accepted set: %v", err)
				}
			case err != nil:
				t.Fatalf("Set(%q): %v", test.value, err)
			case mode != test.want:
				t.Errorf("Set(%q) gave %q, want %q", test.value, mode, test.want)
			}
		})
	}
}

// TestFlagValuesNameTheirTypeInHelp covers the placeholder pflag shows after the
// flag name. "format" and "mode" read better than pflag's default, which would
// be the Go type name.
func TestFlagValuesNameTheirTypeInHelp(t *testing.T) {
	format := cli.OutputTable
	if got := format.Type(); got != "format" {
		t.Errorf("OutputFormat.Type() = %q, want %q", got, "format")
	}
	if got := format.String(); got != "table" {
		t.Errorf("OutputFormat.String() = %q, want %q", got, "table")
	}

	mode := cli.ColorAuto
	if got := mode.Type(); got != "mode" {
		t.Errorf("ColorMode.Type() = %q, want %q", got, "mode")
	}
	if got := mode.String(); got != "auto" {
		t.Errorf("ColorMode.String() = %q, want %q", got, "auto")
	}
}
