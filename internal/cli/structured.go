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

package cli

import (
	"context"
	"errors"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The command side of the structured contract: which flag value means which
// serialization, and how an answer's provenance is assembled.
//
// The contract itself — the envelope, its field names, its additive-only policy —
// lives in internal/cli/render, because it is a pure function of data a command
// already holds and because the golden files that pin it are worth more when they
// drive the real writer. What lives here is the part that needs a backend: the
// capability identifier that names the engine, and the coverage consultation that
// makes an empty answer explicable to a script rather than only to a person
// (Invariant 9).

// structuredFormat maps an -o value onto a serialization of the envelope.
//
// The boolean rather than an error is deliberate: `table` and `wide` are not
// failures here, they are the other branch, and a command asks this question
// precisely to decide which of the two renderings it is producing.
func structuredFormat(format OutputFormat) (render.StructuredFormat, bool) {
	switch format {
	case OutputJSON:
		return render.StructuredJSON, true
	case OutputJSONL:
		return render.StructuredJSONL, true
	case OutputYAML:
		return render.StructuredYAML, true
	}
	return "", false
}

// coverageAnswer is what the watch scopes said, including their having said
// nothing.
//
// It is a type rather than a pair of return values because the two ways a
// coverage question comes back empty must travel together everywhere they go: an
// engine with no scope log (Gap set) and an engine reporting that nothing was
// ever watching (Gap nil, Intervals empty) are different findings, and a caller
// holding only a slice would have lost the difference before it rendered
// anything (Invariant 4).
type coverageAnswer struct {
	// Intervals are the periods the scope was watched, oldest first. Empty when
	// nothing was, and also empty when the backend could not say — read Gap
	// before reading this.
	Intervals []query.ScopeInterval

	// Gap is query.ErrCapabilityUnsupported when the backend has no scope log,
	// and nil when it answered. It is never any other error: a backend that
	// failed to read a scope log it has is a failure, and askCoverage returns
	// that separately.
	Gap error
}

// Summary renders the answer as the sentence every document's header carries.
func (a coverageAnswer) Summary() string { return coverageSummary(a.Intervals, a.Gap) }

// Report renders the answer as the machine-readable half of Invariant 9.
//
// The summary is the identical sentence the human-readable header carries, so
// that a script logging one field logs the line a person would have read, and the
// intervals are the data that sentence was built from, so that a consumer can
// decide for itself rather than parse prose.
func (a coverageAnswer) Report() render.CoverageReport {
	intervals := a.Intervals
	if intervals == nil {
		// Empty rather than nil, so that `.intervals | length` answers 0 for a
		// consumer that reached for it before checking `available`, instead of
		// failing on a null.
		intervals = []query.ScopeInterval{}
	}
	return render.CoverageReport{
		Available: a.Gap == nil,
		Summary:   a.Summary(),
		Intervals: intervals,
	}
}

// askCoverage consults the watch scopes, tolerating an engine that has none.
//
// The returned error is only ever a real failure. A backend with no scope log
// answers query.ErrCapabilityUnsupported, which is a capability gap rather than a
// failure: it is carried in the answer and rendered as such, degrading the
// command with an explicit statement instead of failing an invocation it can
// still mostly answer (Invariant 5). A backend that has a scope log and could not
// read it is the opposite case and fails, because a partial coverage answer would
// report an outage that did not happen.
//
// subject names what the coverage was being asked about, for the failure message.
func askCoverage(
	ctx context.Context, backend *Backend, q query.ScopeQuery, subject string,
) (coverageAnswer, error) {
	intervals, err := backend.Engine.Coverage(ctx, q)
	switch {
	case err == nil:
		return coverageAnswer{Intervals: intervals}, nil
	case errors.Is(err, query.ErrCapabilityUnsupported):
		return coverageAnswer{Gap: err}, nil
	}
	return coverageAnswer{}, RuntimeErrorf("reading the watch scopes that cover %s: %w", subject, err)
}

// envelopeHead assembles the head of an envelope of the given kind.
//
// Everything in it comes from the backend rather than from the invocation, which
// is the point: metadata.cluster_id is the identity the resolution chain arrived
// at rather than whatever was typed, and metadata.backend is the engine's own
// declaration. When two backends hold the same object's history — which the tee
// pattern makes ordinary (D14) — those two fields are how a reader knows which
// answer they are holding.
func envelopeHead(backend *Backend, kind string, coverage coverageAnswer) render.EnvelopeHead {
	return render.EnvelopeHead{
		APIVersion: render.EnvelopeAPIVersion,
		Kind:       kind,
		Metadata: render.EnvelopeMetadata{
			ClusterID: backend.ClusterID,
			Backend:   backend.Engine.Capabilities().Backend,
			Coverage:  coverage.Report(),
		},
	}
}

// changeItem is one change as the envelope carries it.
//
// The type is query.Change unchanged, so the field names are the schema's column
// names by construction rather than by a mapping somebody has to keep in step.
// What it fixes is the *shape* of two absences: a nil slice and a nil map encode
// as JSON null, and the columns they mirror are an array and a map that a backend
// returns empty. The contract already says which reading is the honest one — of
// an actorless deletion, query.Change.Actors says "an empty list is the honest
// answer rather than a missing one" — and null is the other reading. It also
// breaks the obvious consumer: `.actors[]` fails on a null and yields nothing on
// an empty list, and failing is not what "this deletion had no actors" should do
// to somebody's pipeline.
func changeItem(change query.Change) query.Change {
	if change.Actors == nil {
		change.Actors = []string{}
	}
	if change.Labels == nil {
		change.Labels = map[string]string{}
	}
	return change
}

// writeItems writes a whole answer through an envelope stream.
//
// Close is called on every path, including a failed write, because for the
// whole-document formats nothing has been written until it runs: returning early
// on a mid-stream failure without closing would leave a command that reported an
// error *and* produced no document at all, when it could have produced the half
// it had.
func writeItems(stream *render.Stream, items []any) error {
	for _, item := range items {
		if err := stream.Write(item); err != nil {
			return errors.Join(RuntimeErrorf("%w", err), stream.Close())
		}
	}
	if err := stream.Close(); err != nil {
		return RuntimeErrorf("%w", err)
	}
	return nil
}
