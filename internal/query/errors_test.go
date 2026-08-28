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

package query_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/kuberecord/kuberecord/internal/query"
)

// sentinels is every read-plane sentinel, paired with the name a failure should
// report. Every test below iterates it rather than naming errors individually, so
// a sentinel added later is covered by all of them the moment it is listed here.
var sentinels = map[string]error{
	"ErrNoCoverage":            query.ErrNoCoverage,
	"ErrObjectNotFound":        query.ErrObjectNotFound,
	"ErrTimeBoundRequired":     query.ErrTimeBoundRequired,
	"ErrCapabilityUnsupported": query.ErrCapabilityUnsupported,
}

// TestSentinelsSurviveWrapping is the property the contract actually promises:
// backends are expected to wrap these with context ("reading scope log for
// apps/Deployment: %w"), and a caller's decision — which notice to print, which
// exit code to use — has to keep working through however many layers of wrapping
// the call stack added.
func TestSentinelsSurviveWrapping(t *testing.T) {
	for name, sentinel := range sentinels {
		t.Run(name, func(t *testing.T) {
			if sentinel == nil {
				t.Fatalf("%s is nil", name)
			}

			// Two layers, because one layer works by accident under several wrong
			// implementations (a bare comparison, for instance) and two does not.
			inner := fmt.Errorf("backend: %w", sentinel)
			outer := fmt.Errorf("reading history for apps/Deployment: %w", inner)

			if !errors.Is(outer, sentinel) {
				t.Errorf("errors.Is could not find %s through two layers of wrapping: %v",
					name, outer)
			}
			if got := errors.Unwrap(inner); !errors.Is(got, sentinel) {
				t.Errorf("unwrapping one layer did not yield %s, got %v", name, got)
			}
		})
	}
}

// TestSentinelsAreDistinct guards the failure that would make every other
// assertion here pass while the package was useless: two sentinels sharing an
// identity. A caller matching ErrNoCoverage would then also match a capability
// gap, and the command-line client would report "nothing was watching" about a
// backend that simply has no scope log — the confusion between an absent fact and
// an unanswerable question that these errors exist to prevent.
func TestSentinelsAreDistinct(t *testing.T) {
	for name, sentinel := range sentinels {
		for otherName, other := range sentinels {
			if name == otherName {
				continue
			}
			if errors.Is(sentinel, other) {
				t.Errorf("%s matches %s: the two are not distinguishable by errors.Is",
					name, otherName)
			}
		}
	}
}

// TestSentinelMessagesAreSelfContained checks that each message reads as a
// statement about the data or the backend rather than as a bare failure. These
// strings reach an engineer's terminal verbatim, usually as the last line before
// they decide what to do next, so an empty or duplicated one is a real defect
// rather than a cosmetic one.
func TestSentinelMessagesAreSelfContained(t *testing.T) {
	seen := make(map[string]string, len(sentinels))
	for name, sentinel := range sentinels {
		msg := sentinel.Error()
		if msg == "" {
			t.Errorf("%s has an empty message", name)
			continue
		}
		if prev, dup := seen[msg]; dup {
			t.Errorf("%s and %s share the message %q", name, prev, msg)
			continue
		}
		seen[msg] = name
	}
}
