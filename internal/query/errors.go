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

package query

import "errors"

// The read plane's sentinel errors.
//
// They are sentinels rather than messages because each one drives a different
// visible behaviour — a distinct exit code, a distinct notice, a distinct
// suggestion — and a caller deciding that by matching on error text would break
// the first time a backend reworded its own message. Every one of them is
// errors.Is-able through any amount of wrapping, and backends are expected to
// wrap them with context rather than replace them: "reading scope log for
// apps/Deployment: no coverage" is more useful than either half alone.
//
// What they have in common is that none of them is a failure of the query. Each
// is an answer the caller has to render honestly, which is why they are named
// after the fact rather than after the operation that hit them.
var (
	// ErrNoCoverage means nothing was ever watching the scope that was asked
	// about, so there is no history to have an opinion about.
	//
	// It is the sharp end of Invariant 9. Rendered as "no changes", this fact
	// would tell an engineer their object sat untouched, when in truth nobody was
	// recording it — the difference between closing an investigation and starting
	// one. A caller surfaces it distinctly, and the command-line client gives it
	// its own exit code so a script can tell it from an empty result too.
	ErrNoCoverage = errors.New("no watch coverage recorded for the requested scope")

	// ErrObjectNotFound means the object did not exist at the instant asked
	// about: never recorded, or recorded and then deleted.
	//
	// It is also returned wrapped, with a message saying which, when history holds
	// rows for the object but no full-state row to replay from — the base predates
	// the retention window. That is a different fact from absence and the message
	// must say so, but it shares this sentinel because what the caller does about
	// it is the same: report that no state can be produced, and never substitute a
	// neighbouring instant's state for the one that was asked for.
	ErrObjectNotFound = errors.New("no recorded state for the object at the requested instant")

	// ErrTimeBoundRequired means the engine will not run an unbounded query.
	//
	// A backend that has to scan storage to answer at all (Capabilities
	// TimeBoundRequired) refuses up front instead of starting work it cannot
	// bound. Refusing is the kinder outcome: an unbounded scan against a large
	// archive is indistinguishable from a hang, and the caller can turn this error
	// into a message naming the flag that fixes it.
	ErrTimeBoundRequired = errors.New("this backend requires a time bound on the query")

	// ErrCapabilityUnsupported means the engine cannot answer this kind of
	// question at all — the storage has no scope log to read, or no way to
	// reconstruct state.
	//
	// It is deliberately not an empty result. An empty result is a claim about the
	// data; this is a statement about the backend, and collapsing the two is
	// exactly the silent-gap failure the capability declaration exists to prevent
	// (Invariant 4). A caller degrades the affected command with an explicit
	// notice and still answers everything else it was asked (Invariant 5).
	ErrCapabilityUnsupported = errors.New("this backend cannot answer the requested query")
)
