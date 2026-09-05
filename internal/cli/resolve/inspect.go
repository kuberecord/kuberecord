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

package resolve

import (
	"context"
	"fmt"
	"slices"

	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/query"
)

// Resolution, made inspectable (D26).
//
// Nine steps decide where an answer comes from: four for the backend and five for
// the cluster identity. On a working command they produce two lines of notice,
// which is the right amount of ceremony for an answer somebody actually wanted.
// It is the wrong amount when the chain chose something unexpected — a stanza
// written months ago shadowing discovery, a --context pointing at the wrong
// cluster, an identity read from an operator that is not the one being asked
// about. The result is then wrong in a way that looks right, and today the only
// way to see why is to read this package's source or bisect flags.
//
// This file is the other reading of the same walk. Nothing here re-derives a
// decision: the steps are recorded by the walk itself, as it takes them, so a
// report can never describe a chain the resolver did not run.
//
// # Why the failures are values rather than returns
//
// The two chains fail for unrelated reasons — a Secret nobody may read, an
// identity nobody wrote down — and a report is most needed when one of them has.
// An Inspection therefore carries each chain's failure beside the other chain's
// answer, and the caller decides what to print and what to exit with. Resolve
// keeps the ordinary contract: for a command that is about to run a query, a
// half-resolved backend is no backend at all.
//
// # Why it asks nothing
//
// Inspect performs no round trip against the backend unless told to. Opening an
// engine does not (the ClickHouse driver validates options and assembles a pool;
// the archive sources resolve configuration), but the cluster-id chain's last
// step genuinely queries, and that step is withheld by default. The configuration
// a user most wants to inspect is the one whose backend cannot be reached, and a
// `config resolve` that stalled for a dial timeout on it would be useless
// precisely where it was needed.

// StepOutcome is what one step of a resolution chain did.
//
// Five values rather than a boolean, because "this step had nothing to say" and
// "this step was never consulted" send a reader to different places: the first is
// a fact about their configuration, the second is a fact about the step above it.
type StepOutcome string

// What a step can have done, as the report spells it.
const (
	// StepAnswered means this step produced the chain's result.
	StepAnswered StepOutcome = "answered"

	// StepSilent means the step was consulted and had nothing — the flag was not
	// given, no profile is active, the file maps no context. It is an ordinary
	// state and the commonest one.
	StepSilent StepOutcome = "silent"

	// StepFailed means the step had something to say and could not say it: a
	// Secret that may not be read, a sink that does not exist, a malformed flag.
	// It is the step that stopped the chain.
	StepFailed StepOutcome = "failed"

	// StepNotReached means an earlier step answered, or the chain stopped before
	// this one. Recorded rather than omitted, because a step missing from a
	// report reads as a step that was silent.
	StepNotReached StepOutcome = "not reached"

	// StepWithheld means this step would have contacted the backend and was not
	// taken. It is only ever the cluster-id chain's last step, and only when the
	// caller declined to ask (see Inspect).
	StepWithheld StepOutcome = "withheld"
)

// The names the report gives the cluster-id chain's steps.
//
// The backend chain's names come from Origin.Step, because that chain's steps
// already have identities the rest of the package uses. This one's do not: its
// steps are places to look rather than choices to record, so they are named here,
// once, in the words clusterid.go's own documentation uses for them.
const (
	stepClusterIDFlag      = "--" + options.FlagClusterID
	stepContextMapping     = "context mapping"
	stepOperatorDeployment = "operator Deployment"
	stepSink               = "the sink"
)

// backendChainSteps is the backend chain's steps in the order resolveTarget
// tries them.
func backendChainSteps() []string {
	return []string{
		OriginSourceFlag.Step(), OriginSinkFlag.Step(),
		OriginProfile.Step(), OriginDiscovered.Step(),
	}
}

// clusterIDChainSteps is the identity chain's steps in the order
// resolveClusterID tries them.
func clusterIDChainSteps() []string {
	return []string{stepClusterIDFlag, stepContextMapping, stepOperatorDeployment, stepSink}
}

// ChainStep is one step of a resolution chain and what it had to say.
type ChainStep struct {
	// Step names the step: a flag, a file, a place to look.
	Step string

	// Outcome is what it did.
	Outcome StepOutcome

	// Detail is the answer it gave, or the reason it gave none, in a sentence
	// fragment fit to sit in a table cell. It never carries a credential: every
	// value that reaches it comes from a description or an error message, and
	// both are held to that rule everywhere else in this package.
	Detail string
}

// record appends one step's outcome to a chain's record.
func record(chain *[]ChainStep, name string, outcome StepOutcome, format string, args ...any) {
	*chain = append(*chain, ChainStep{
		Step:    name,
		Outcome: outcome,
		Detail:  fmt.Sprintf(format, args...),
	})
}

// recordResult records a step that produced something: the detail it answered
// with, or the failure it produced instead.
//
// The two are one call because every step that can answer can also fail, and a
// call site that spelled the pair out each time is a call site that eventually
// records a failure as an answer.
func recordResult(chain *[]ChainStep, name string, err error, format string, args ...any) {
	if err != nil {
		record(chain, name, StepFailed, "%v", err)
		return
	}
	record(chain, name, StepAnswered, format, args...)
}

// completeChain appends the steps a walk never got to.
//
// A chain stops at the step that answers it, so the record it leaves behind is a
// prefix of the chain. The rest are stated as not reached rather than left out,
// because a report showing three of four steps invites the reader to conclude the
// fourth was silent.
func completeChain(chain *[]ChainStep, names []string) {
	for _, name := range names {
		if slices.ContainsFunc(*chain, func(step ChainStep) bool { return step.Step == name }) {
			continue
		}
		record(chain, name, StepNotReached, "")
	}
}

// stepWasTaken reports whether a chain actually took a named step, as opposed to
// having recorded that it did not.
//
// Answering and failing are both taking it; withheld and not-reached are not.
// The distinction is what tells a reachability check whether the backend has
// already been questioned.
func stepWasTaken(chain []ChainStep, name string) bool {
	for _, step := range chain {
		if step.Step == name {
			return step.Outcome == StepAnswered || step.Outcome == StepFailed
		}
	}
	return false
}

// Inspection is what the two chains decided, step by step.
//
// It is the value `kuberecord config resolve` renders, and the reason it exists
// rather than a second walk in the command package: the ordering of the steps and
// the reason each one had nothing to say are properties of this package's chain,
// and a description assembled next to a cobra command would agree with the chain
// until the day somebody reordered it.
type Inspection struct {
	// Backend is the opened backend, or nil when the chain failed. The caller
	// owns it and must Close it.
	Backend *Backend

	// Origin is the step that answered, or the step that failed. It is the empty
	// Origin when the chain was refused before its first step, which --sink-addr
	// can do.
	Origin Origin

	// BackendErr is why the backend chain produced nothing, or nil. It carries
	// its own exit code, so a caller that returns it unchanged reports a
	// malformed flag as a usage error exactly as a query command would.
	BackendErr error

	// BackendSteps is every step of that chain, in the order they are tried.
	BackendSteps []ChainStep

	// ClusterID is the resolved kuberecord cluster identity (D21), empty when the
	// chain did not resolve one.
	ClusterID string

	// ClusterIDSource says how it was arrived at, in the words the notice uses.
	ClusterIDSource string

	// ClusterIDErr is why the identity chain produced nothing, or nil.
	//
	// An empty ClusterID with a nil error is not a contradiction and is the state
	// a withheld last step leaves behind: nothing has failed, and the one step
	// that could still answer was not taken. See Inspect.
	ClusterIDErr error

	// ClusterIDSteps is every step of that chain, in the order they are tried.
	ClusterIDSteps []ChainStep

	// Asked reports whether the identity chain was allowed to question the
	// backend. It is what tells a caller whether reachability has already been
	// established, so that `--check` does not pay for a second round trip to
	// learn what the chain just found out.
	Asked bool
}

// Inspect runs both chains and reports what every step decided.
//
// ask permits the identity chain's last step, which is the only part of
// resolution that questions the backend. With ask false that step is recorded as
// withheld and the chain resolves to nothing rather than to a failure: the
// configuration is not broken, it simply has one more step that nobody has taken.
//
// No notice is printed. The report is the notice, and it goes to stdout where a
// command's data belongs.
func (r *BackendResolver) Inspect(ctx context.Context, ask bool) *Inspection {
	opts := walkOptions{reportBoth: true}
	if !ask {
		opts.unaskable = &ChainStep{
			Step:    stepSink,
			Outcome: StepWithheld,
			Detail:  fmt.Sprintf("would be asked which clusters it holds; --%s asks it", options.FlagCheck),
		}
	}
	return r.walk(ctx, opts)
}

// Check asks the backend the cheapest question the read plane has, so that a
// caller can report whether it can be reached at all.
//
// It asks for the clusters the backend holds, which is the identity chain's own
// last step, for two reasons. It is the one question every shipped engine answers
// without being told what to look for. And it exercises the whole path rather
// than a socket: DNS, the connection, the credential and — for ClickHouse — the
// database being the one the sink named. A bare TCP dial would pass against a
// server holding somebody else's history.
//
// An engine that cannot answer it is reported as the capability gap it is, with
// query.ErrCapabilityUnsupported, and is not a failure: a backend that cannot be
// probed without running a real query has said something true about itself, and
// turning that into a red result would be inventing a fault (Invariant 5).
//
// The failure is passed through this backend's own diagnosis, so an address that
// only resolves inside a cluster produces the explanation Task 13.1 wrote for it
// wherever it is met — here as much as under a query.
func (b *Backend) Check(ctx context.Context) error {
	lister, ok := b.Engine.(query.ClusterIDLister)
	if !ok {
		return fmt.Errorf("the %s backend cannot say whether it is reachable without running a "+
			"real query: %w", b.Engine.Capabilities().Backend, query.ErrCapabilityUnsupported)
	}
	_, err := lister.ClusterIDs(ctx)
	return b.diagnosis.wrap(err)
}
