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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// `get --at` is the question that closes the loop: what did this look like
// before?
//
// The answer is assembled rather than stored. The backend finds the newest
// full-state row at or before the instant and replays the patches after it, which
// is the procedure docs/SCHEMA.md specifies and query.Replay implements once for
// every backend — including the trap that a Checkpoint's own diff describes the
// state its data already holds and must not be applied over it.
//
// Two things follow from the answer being assembled, and both are surfaced rather
// than assumed.
//
// The first is that it is not a manifest, and it looks exactly like one. See
// render.ObjectDocument for why the header saying so is mandatory rather than
// polite.
//
// The second is that a replay can be wrong in a way no error reports. Every row
// carries the digest of the state it recorded, so --verify canonicalizes what was
// reconstructed, hashes it, and compares. A mismatch means the archive and the
// reconstruction disagree about what this object looked like, which is a
// chain-of-custody finding and not a rounding error.

// getFlags is one invocation's own flag surface.
type getFlags struct {
	at     string
	uid    string
	verify bool
}

// newGetCommand builds `get`.
func newGetCommand(
	flags *GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	local := &getFlags{}

	command := &cobra.Command{
		Use:   "get (KIND/NAME | KIND NAME)",
		Short: "Reconstruct what one object looked like at an instant",
		Long: `Reconstruct what one object looked like at an instant.

The state is rebuilt from recorded history: the newest full-state row at or
before --at, with every patch recorded after it replayed over the top. The
header says which row it started from and how many patches were applied, so the
answer can be judged rather than trusted.

What comes out is not a manifest and must not be applied. Volatile metadata was
stripped before the state was ever recorded, redacted fields carry a sentinel in
place of their values, and the document describes a past somebody deliberately
moved the object out of. The header says so in those words.

--verify re-hashes the reconstruction and compares it against the digest
recorded for the row the replay finished on. A mismatch means history and replay
disagree, which is a chain-of-custody finding, and it exits ` +
			fmt.Sprint(ExitRuntimeError) + `.`,
		Example: `  # What this Deployment looked like two hours ago.
  kuberecord get deploy/checkout -n payments --at 2h

  # At a named instant, as JSON, with the provenance on standard error.
  kuberecord get deploy/checkout -n payments --at 2026-08-28T14:04:00Z -o json

  # Prove the reconstruction matches the digest the archive recorded.
  kuberecord get deploy/checkout -n payments --at 2h --verify

  # Pin the reconstruction to one incarnation of a reused name.
  kuberecord get deploy/checkout -n payments --at 2026-08-28 --uid 7c9e6679-7425-40de-944b-e07fc1f90ae7`,

		RunE: func(cmd *cobra.Command, args []string) error {
			return runGetCommand(cmd, flags, local, args, streams, invokedAs)
		},
	}

	command.Flags().StringVar(&local.at, "at", local.at,
		"Reconstruct the state as of this point: a duration (6h, 90m, 3d, 2w) or an instant "+
			"(2026-08-20, 2026-08-20T14:00:00Z). Defaults to now, which is the newest recorded state.")
	command.Flags().StringVar(&local.uid, "uid", local.uid,
		"Pin the reconstruction to one incarnation by UID. "+
			"Empty means the newest incarnation alive at --at, never a blend of two.")
	command.Flags().BoolVar(&local.verify, "verify", local.verify,
		"Re-hash the reconstructed state and compare it against the digest recorded for it.")

	return command
}

// runGetCommand turns one invocation into a request, opens the backend, and runs
// it.
func runGetCommand(
	cmd *cobra.Command, flags *GlobalFlags, local *getFlags,
	args []string, streams genericiooptions.IOStreams, invokedAs string,
) (err error) {
	format, err := objectFormat(cmd, flags.Output)
	if err != nil {
		return err
	}
	arg, err := ParseResourceArg(args)
	if err != nil {
		return err
	}

	at := time.Now()
	if local.at != "" {
		if at, err = ParseInstant(local.at, at); err != nil {
			return err
		}
	}

	backend, ref, err := resolveObject(cmd.Context(), flags, streams, invokedAs, arg)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, backend.Close())
	}()

	request := GetRequest{Ref: ref, At: at, UID: local.uid, Verify: local.verify, Format: format}
	return RunGet(cmd.Context(), backend, request, streams, getRenderOptions(flags, streams))
}

// objectFormat decides which serialization a reconstruction is written in.
//
// The global default is `table`, which this command has nothing to render as: a
// reconstructed object is a document, not a row. Rather than refusing a bare
// invocation, an untouched -o resolves to YAML — which is both the format most
// people want and the one that carries the "not a deployable manifest" header in
// the document itself rather than on another stream. A user who asked for
// something this command cannot produce is told so by name, because finding out
// at the `jq` is worse than finding out here.
func objectFormat(cmd *cobra.Command, requested OutputFormat) (render.StructuredFormat, error) {
	if !cmd.Flags().Changed(FlagOutput) {
		return render.StructuredYAML, nil
	}
	if structured, ok := structuredFormat(requested); ok {
		return structured, nil
	}
	return "", UsageErrorf("get renders %s, %s or %s, not %s: a reconstructed object is a document "+
		"rather than a row, so there is nothing for the tabular formats to lay out",
		OutputYAML, OutputJSON, OutputJSONL, requested)
}

// getRenderOptions decides how the document's notices will look. The object
// itself is serialized rather than laid out, so no width applies to it.
func getRenderOptions(flags *GlobalFlags, streams genericiooptions.IOStreams) render.Options {
	return render.Options{
		Width: TerminalWidth(streams.Out),
		Color: ShouldColorize(flags.Color, streams.Out),
	}
}

// GetRequest is one `get` invocation, resolved.
type GetRequest struct {
	// Ref is the object whose state is wanted.
	Ref query.ObjectRef

	// At is the instant to reconstruct for.
	At time.Time

	// UID pins the reconstruction to one incarnation. Empty means the newest
	// incarnation alive at At — never a blend of two, since splicing them would
	// reconstruct an object that never existed (Invariant 7).
	UID string

	// Verify re-hashes the reconstruction and compares it against the recorded
	// digest.
	Verify bool

	// Format is the serialization of the envelope to write.
	Format render.StructuredFormat
}

// scopeQuery asks which scopes covered the object at or before the instant.
//
// It is unbounded at the start deliberately: the question a reconstruction has to
// have an answer to is whether this kind was *ever* watched, not whether it was
// watched inside some window the user did not ask about. ScopeQuery's covering
// reading of a namespace applies, so a cluster-wide rule counts as having watched
// an object in one namespace — which it genuinely did.
func (r GetRequest) scopeQuery() query.ScopeQuery {
	return query.ScopeQuery{
		ClusterID: r.Ref.ClusterID,
		APIGroup:  r.Ref.APIGroup,
		Kind:      r.Ref.Kind,
		Namespace: r.Ref.Namespace,
		To:        r.At,
	}
}

// RunGet reconstructs one object's state and renders it.
func RunGet(
	ctx context.Context, backend *Backend, request GetRequest,
	streams genericiooptions.IOStreams, opts render.Options,
) error {
	reconstruction, stateErr := backend.Engine.StateAt(ctx, request.Ref, request.At, request.UID)

	// Coverage is consulted on every invocation rather than only on a failure,
	// which is a change of shape from asking about it once the object turned out
	// to be missing. Two things fall out of it. The envelope's metadata carries it
	// on the success path too, so a script holding a reconstruction can see
	// whether the period it describes was being watched (Invariant 9). And the
	// failure path below is handed the same answer instead of asking its own,
	// which is what stops the two ever disagreeing about what was watching.
	coverage, err := askCoverage(ctx, backend, request.scopeQuery(), describeObject(request.Ref))
	if stateErr != nil {
		return stateFailure(backend, request, coverage, err, stateErr)
	}
	if err != nil {
		return err
	}

	document := render.ObjectDocument{
		Kind:           describeKind(request.Ref),
		Ref:            describeObject(request.Ref),
		Cluster:        request.Ref.ClusterID,
		UID:            reconstructedUID(reconstruction, request.UID),
		At:             request.At,
		BaseTS:         reconstruction.BaseTS,
		BaseEvent:      reconstruction.BaseEvent,
		PatchesApplied: reconstruction.PatchesApplied,
		SHA256:         reconstruction.SHA256,
		Coverage:       coverage.Summary(),
		State:          reconstruction.Object,
	}

	// --verify is an assertion, not an annotation: the document is produced only
	// if it holds. A reconstruction that disagrees with its recorded digest is one
	// nobody should be handed, least of all into a redirect — and
	// `kuberecord get … --verify > object.yaml` is exactly how somebody would use
	// this flag. So a failure is reported and nothing is written.
	if request.Verify {
		notice, verifyErr := verifyReconstruction(reconstruction)
		if verifyErr != nil {
			return verifyErr
		}
		document.Notices = appendNotice(document.Notices, notice)
	}

	head := envelopeHead(backend, render.KindObject, coverage)
	if writeErr := render.WriteObject(
		streams.Out, streams.ErrOut, document, head, request.Format, opts); writeErr != nil {
		return RuntimeErrorf("%w", writeErr)
	}
	return nil
}

// reconstructedUID names the incarnation the document describes.
//
// The recorded object's own metadata.uid is preferred over the flag, because it
// is what the reconstruction actually produced: a --uid that the backend
// interpreted differently, or an empty one the backend resolved for itself, must
// not leave the header naming an incarnation the document is not of.
func reconstructedUID(reconstruction *query.Reconstruction, requested string) string {
	metadata, ok := reconstruction.Object["metadata"].(map[string]any)
	if ok {
		if uid, isString := metadata["uid"].(string); isString && uid != "" {
			return uid
		}
	}
	return requested
}

// stateFailure explains why no state came back, against the watch scopes that
// were open at the time.
//
// This is Invariant 9 applied to a reconstruction. "No recorded state at that
// instant" and "nothing was ever watching this kind" are different findings, and
// an engineer handed the second dressed as the first concludes an object did not
// exist when in fact nobody was looking at it.
//
// coverage is the answer RunGet already obtained, and coverageErr is the failure
// to obtain it. They are arguments rather than a query of this function's own so
// that the header of a successful reconstruction and the explanation of an
// unsuccessful one can never be built from two different readings of the scope
// log.
func stateFailure(
	backend *Backend, request GetRequest, coverage coverageAnswer, coverageErr, err error,
) error {
	object := describeObject(request.Ref)
	instant := render.FormatInstant(request.At)

	switch {
	case errors.Is(err, query.ErrCapabilityUnsupported):
		return RuntimeErrorf("the %s backend cannot reconstruct state, so it cannot say what %s looked "+
			"like at %s: %w", backend.Engine.Capabilities().Backend, object, instant, err)
	case !errors.Is(err, query.ErrObjectNotFound):
		return RuntimeErrorf("reconstructing %s as of %s: %w", object, instant, err)
	}

	switch {
	case coverageErr != nil:
		return RuntimeErrorf("no recorded state for %s at %s, and the watch scopes that would say "+
			"whether anything was looking could not be read: %w", object, instant, coverageErr)
	case coverage.Gap != nil:
		return RuntimeErrorf("no recorded state for %s at %s, and this backend has no scope log: it "+
			"cannot say whether the object was absent or merely unobserved", object, instant)
	case len(coverage.Intervals) == 0:
		return fmt.Errorf("%w: nothing was ever watching %s %s in cluster %q at or before %s, so this "+
			"absence is not evidence that the object did not exist; the `%s` command lists what is "+
			"being recorded", query.ErrNoCoverage, describeKind(request.Ref), object,
			request.Ref.ClusterID, instant, scopesCommand)
	}
	return RuntimeErrorf("no recorded state for %s at %s: it had not been observed by then, or it had "+
		"already been deleted. The scope was watched over %s",
		object, instant, describeInterval(coverage.Intervals[0]))
}

// verifyReconstruction re-hashes the reconstructed state and compares it against
// the digest recorded for the row the replay finished on.
//
// # What is being compared
//
// The stored digest is the hex SHA-256 of the operator's normalized JSON for that
// state, and docs/SCHEMA.md specifies the check: canonicalize the reconstructed
// document — re-serialize it with sorted object keys — and hash it. encoding/json
// performs that canonicalization for free, since marshalling a map emits its keys
// in sorted order, and it is the same path internal/query/conformance's fidelity
// property takes. Both sides therefore agree by construction rather than by
// coincidence.
//
// The agreement is exact for every value float64 represents exactly, which covers
// everything a Kubernetes object holds in practice: strings, booleans, replica
// counts, ports, generations. An integer past 2^53 would round on the way through
// a decoded document and could report a mismatch that is an artefact of encoding
// rather than a finding — noted here because the alternative reading of a
// mismatch is a chain-of-custody incident, and a reader deserves to know the one
// case where it is not.
func verifyReconstruction(reconstruction *query.Reconstruction) (render.Notice, error) {
	recomputed, err := canonicalDigest(reconstruction.Object)
	if err != nil {
		return render.Notice{}, RuntimeErrorf("the reconstructed state could not be re-encoded for "+
			"verification, so --verify has neither confirmed nor denied it: %w", err)
	}

	if reconstruction.SHA256 == "" {
		// Not a mismatch, and not a pass either. Reporting success here would be
		// the tool inventing an assurance nobody gave it (Invariant 4).
		return render.Notice{}, RuntimeErrorf("--verify cannot check this reconstruction: no digest is "+
			"recorded for the row the replay finished on, so there is nothing to compare the "+
			"recomputed %s against", recomputed)
	}

	if recomputed != reconstruction.SHA256 {
		return render.Notice{}, RuntimeErrorf("the reconstruction does not match the digest recorded "+
			"for it: hashing the reconstructed state gives %s, and the archive recorded %s. History "+
			"and replay disagree about what this object looked like, which is a chain-of-custody "+
			"finding and not a rounding error", recomputed, reconstruction.SHA256)
	}
	return render.Notice{Text: fmt.Sprintf(
		"verified: the reconstructed state hashes to %s, which is the digest recorded for it",
		recomputed)}, nil
}

// canonicalDigest is the hex SHA-256 of a document canonicalized the way the
// schema's sha256 column was: object keys in sorted order, which is what
// encoding/json emits for a map.
func canonicalDigest(object map[string]any) (string, error) {
	encoded, err := json.Marshal(object)
	if err != nil {
		return "", fmt.Errorf("canonicalizing the reconstructed state: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
