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

package exit_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/query"
)

// TestExitCodeFor covers the one place a failure becomes a number a shell can
// branch on.
func TestExitCodeFor(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{
			name: "no error is success",
			err:  nil,
			want: exit.Success,
		},
		{
			name: "an uncoded error is a runtime error",
			err:  errors.New("the backend closed the connection"),
			want: exit.RuntimeError,
		},
		{
			name: "a usage error keeps its code",
			err:  exit.UsageErrorf("bad address %q", "deploy"),
			want: exit.UsageError,
		},
		{
			name: "a runtime error keeps its code",
			err:  exit.RuntimeErrorf("could not reach the sink"),
			want: exit.RuntimeError,
		},
		{
			name: "a coded error survives wrapping",
			err:  fmt.Errorf("resolving the object: %w", exit.UsageErrorf("bad address")),
			want: exit.UsageError,
		},
		{
			// Invariant 9: "nothing was watching" is a finding with a code of
			// its own, not an empty success.
			name: "the read plane's no-coverage sentinel is exit 3",
			err:  query.ErrNoCoverage,
			want: exit.NoCoverage,
		},
		{
			name: "the sentinel survives wrapping",
			err:  fmt.Errorf("timeline for deploy/nginx: %w", query.ErrNoCoverage),
			want: exit.NoCoverage,
		},
		{
			// An explicit decision beats an inferred one: a command that has
			// judged a missing-coverage condition to be a runtime failure in its
			// context gets to say so.
			name: "an explicit code wins over the sentinel it wraps",
			err:  &exit.Error{Code: exit.RuntimeError, Err: query.ErrNoCoverage},
			want: exit.RuntimeError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := exit.CodeFor(test.err); got != test.want {
				t.Errorf("CodeFor(%v) = %d, want %d", test.err, got, test.want)
			}
		})
	}
}

// TestExitCodesAreTheDocumentedNumbers pins the values themselves.
//
// The constants are the contract; a rename would be caught by the compiler, but
// a renumbering would silently change what every wrapper script does.
func TestExitCodesAreTheDocumentedNumbers(t *testing.T) {
	tests := []struct {
		name string
		got  int
		want int
	}{
		{"success", exit.Success, 0},
		{"runtime error", exit.RuntimeError, 1},
		{"usage error", exit.UsageError, 2},
		{"no coverage", exit.NoCoverage, 3},
	}

	for _, test := range tests {
		if test.got != test.want {
			t.Errorf("%s = %d, want %d", test.name, test.got, test.want)
		}
	}
}

// TestErrorUnwrapsAndReportsItsCode covers the two things attaching a code must
// not cost a caller: the ability to classify the wrapped failure, and a legible
// message.
func TestErrorUnwrapsAndReportsItsCode(t *testing.T) {
	underlying := errors.New("the object address has no name")
	coded := &exit.Error{Code: exit.UsageError, Err: underlying}

	if !errors.Is(coded, underlying) {
		t.Error("wrapping an error in exit.Error hid it from errors.Is")
	}
	if got := coded.Error(); got != underlying.Error() {
		t.Errorf("Error() = %q, want %q", got, underlying.Error())
	}
	if got := coded.ExitCode(); got != exit.UsageError {
		t.Errorf("ExitCode() = %d, want %d", got, exit.UsageError)
	}
}
