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

// Package cli is kuberecord's command-line client: the command tree, the
// per-command flag sets, and the result domain each command asks its backend
// about.
//
// # The order the CLI is built in
//
// This package is the top of it. Everything below is a concern that was pulled
// out of it because it had a boundary of its own (Task 11.8), and the arrows run
// one way only — command construction depends on resolution, gating, replay and
// presentation, and none of them depends on command construction:
//
//	exit      the 0/1/2/3 contract; everything may depend on it, it depends on nothing
//	render    presentation: tables, diffs, the structured envelope
//	options   the invocation surface: global flags, formats, terminal, window parsing
//	resolve   backend resolution: --source/--sink, profiles, discovery, cluster identity
//	coldscan  cold-scan gating: estimation, confirmation, progress, --max-objects
//	replay    the row domain: decode, prior-state replay, field attribution
//	cli       this package: the cobra tree, and the query shaping welded to it
//
// What stayed here is what could not leave without being dismantled. Window
// parsing, prior-state lookup and attribution separated cleanly and did; incarnation
// selection, coverage explanation and structured-envelope assembly are written
// against TimelineRequest and each other, and moving them would have meant
// exporting nineteen identifiers to buy one import edge — which is a worse outcome
// than the flat package it was trying to improve on.
//
// # A client of the schema, not of the operator
//
// This package may not reach into the operator's runtime — not internal/pipeline,
// internal/watch, internal/controller, nor internal/sink (D20). It is a consumer
// of the frozen ClickHouse schema and of the read-plane contract in
// internal/query, and of nothing else this repository builds.
//
// The boundary is worth more than the convenience it costs. Coupled, every
// refactor of the write path would be a release of the CLI, and the CLI would
// drag controller-runtime into the link graph of a binary that never talks to an
// API server for anything but discovery and kubeconfig. internal/sink is denied
// for a reason of its own: it has a read half, and a command that could see it
// would be tempted to answer an analyst's question through the operator's
// warm-up reader — a different contract, with opposite pressures, that sits on
// the hot path's dependency graph (D16).
//
// It is enforced twice, because one enforcement is a convention: a depguard rule
// in .golangci.yml denies the import, and deps_test.go walks the whole transitive
// closure, which is where a boundary is actually lost — through an innocent
// helper package added later rather than through a direct import somebody would
// have questioned in review.
//
// Every package listed above carries that closure test, not just this one. A
// split that left the new packages guarded only by the depguard glob would have
// traded the transitive half of the assertion for tidier directories, and the
// transitive half is the half that catches the reach nobody meant to add.
//
// # Two names, one implementation
//
// The tree is built once and answers to both `kubectl kuberecord` and
// `kuberecord`, adjusting only what it calls itself. See InvocationName.
//
// # The structured output is a public contract
//
// `-o json`, `-o jsonl` and `-o yaml` produce a versioned envelope
// (render.EnvelopeAPIVersion) whose item field names mirror the frozen schema's
// column names exactly (D19). People script against this, and a field renamed a
// release later breaks a runbook silently — `jq` reports nothing for a path that
// no longer exists, so the pipeline keeps running while producing empty findings.
// Within one apiVersion the contract is additive only, exactly as the schema's is.
//
// # Where output goes
//
// Data goes to stdout. Diagnostics, warnings, progress and errors go to stderr,
// including the usage block printed after a usage error. That split is what makes
// `kuberecord timeline … -o json | jq` safe in the presence of a degradation
// notice, and a degradation notice is not optional — Invariants 4 and 5 require
// the CLI to say what a backend could not answer rather than to present the gap
// as a result.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"
)

// Run builds the command tree, executes args against it, and returns the process
// exit code.
//
// args is the whole argv, argv[0] included, because argv[0] is what decides
// whether this process calls itself `kubectl kuberecord` or `kuberecord`.
//
// It returns a code rather than calling os.Exit so that the whole of the CLI's
// behaviour — including its exit codes and its stdout/stderr discipline — is
// reachable from a test. main is the only place that exits.
func Run(args []string, streams genericiooptions.IOStreams) int {
	ctx, stop := interruptContext(context.Background())
	defer stop()
	return RunContext(ctx, args, streams)
}

// interruptContext returns a context cancelled by the signals that mean "stop".
//
// It exists because a cold scan is genuinely long: an unindexed backend answering
// a wide question spends minutes listing and decompressing, and the only correct
// response to Ctrl-C during it is for the scan to stop fetching, close its
// iterator and report that the window was not read to the end. A process that
// died where it stood would leave whatever had reached stdout looking like an
// answer.
//
// SIGTERM is honoured alongside SIGINT, which the acceptance criteria name. The
// CLI is run from CI jobs and wrappers that terminate rather than interrupt, and
// there is no version of this where a supervisor's TERM deserves a dirtier stop
// than a person's Ctrl-C.
//
// A *second* signal is deliberately fatal: NotifyContext stops handling after the
// first, so the default disposition takes the process down. That is the escape
// hatch for a scan wedged in a call that is not honouring its context, and losing
// it — by handling every signal forever — would be trading a clean exit for an
// unkillable one.
func interruptContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

// RunContext is Run with the cancellation supplied by the caller.
//
// It is exported so that the interruption path is reachable from a test without
// delivering a real signal to the test binary, and so that a future embedder can
// supply its own cancellation. Run is the whole of what main needs.
func RunContext(ctx context.Context, args []string, streams genericiooptions.IOStreams) int {
	root, flags := NewRootCommand(InvocationName(args), streams)

	// Always an explicit, non-nil slice. Cobra reads os.Args[1:] whenever its
	// args are nil — that is its documented default, not an edge case — so
	// handing it nil for an argv of just the program name would silently splice
	// the real process's arguments into a call that passed none. Under `go test`
	// those are the test binary's own flags, which is how this would be
	// discovered: as an unknown-flag failure in a test that passed no flags.
	rest := []string{}
	if len(args) > 1 {
		rest = args[1:]
	}
	root.SetArgs(rest)

	// ExecuteC returns the command that actually failed, which is what makes the
	// usage block below the right one: a usage error under a subcommand must
	// print that subcommand's usage, not the root's.
	failed, err := root.ExecuteContextC(ctx)

	// Checked before the error is, because an interrupted invocation must not exit
	// zero on the strength of a command that swallowed its cancellation. An answer
	// abandoned half way is not a short answer, and the exit code is the half a
	// wrapper script reads.
	if ctx.Err() != nil {
		err = interrupted(err)
	}
	if err == nil {
		return exit.Success
	}

	code := exit.CodeFor(err)

	// A quiet failure has already said everything it has to say, beside the
	// document it qualifies. See Error.Quiet.
	var coded *exit.Error
	if errors.As(err, &coded) && coded.Quiet {
		return code
	}

	// Built as one string and written once. Two writes would let another
	// writer's line land between the message and the usage block that explains
	// it, and stderr is shared with whatever else the shell has pointed at it.
	diagnostic := fmt.Sprintf("error: %v\n", err)
	if code == exit.UsageError {
		// A bare "unknown flag" with no reminder of what the flags are is a
		// worse message than the one cobra would have printed unprompted.
		diagnostic += "\n" + failed.UsageString()
	}
	diagnostic += unreachableAdvice(err, failed, flags, streams)

	// The write is checked rather than discarded because every fallible call
	// here is, and then deliberately not acted on: a failure means stderr itself
	// has gone — a closed pipe, a full disk — so there is nowhere left to report
	// it. The exit code already carries the failure the message would have
	// described, and it is the half a caller can still read.
	if _, writeErr := io.WriteString(streams.ErrOut, diagnostic); writeErr != nil {
		return code
	}

	return code
}

// unreachableAdvice is the block that turns an unreachable cluster-internal
// backend from a dead end into two commands (Task 13.1).
//
// It is rendered here, at the top, for three reasons that all point the same way.
// The failure it explains can be raised anywhere — during resolution, or from the
// first query several layers below a command — and this is the one place every
// path ends up. Colour is decided from --color, NO_COLOR and whether stderr is a
// terminal, and only here are all three known. And rendering once, into
// RunContext's single write, is what keeps the message out of the middle of a
// half-drawn table.
//
// cobra's own name for the command that failed is passed through, so the
// invocation the message tells the reader to re-run is the one they actually
// typed rather than a placeholder. It is the only thing this layer adds; the
// words belong to resolve/diagnose.go.
func unreachableAdvice(
	err error, failed *cobra.Command, flags *options.GlobalFlags, streams genericiooptions.IOStreams,
) string {
	var unreachable *resolve.UnreachableSinkError
	if !errors.As(err, &unreachable) {
		return ""
	}

	commandPath := ""
	if failed != nil {
		commandPath = failed.CommandPath()
	}
	colorize := false
	if flags != nil {
		colorize = options.ShouldColorize(flags.Color, streams.ErrOut)
	}
	return "\n" + unreachable.Render(commandPath, colorize)
}

// interrupted phrases the end of an invocation that was told to stop.
//
// The failure the command reported is kept rather than replaced: an interrupted
// cold scan already says which window it did not finish reading (see the object
// archive's own abandonment message), and that sentence is more useful than
// anything this layer could write. What is added is the word that says the
// stopping was asked for, so that "context canceled" in a CI log is not read as a
// backend that dropped the connection.
func interrupted(err error) error {
	if err == nil {
		return &exit.Error{
			Code: exit.RuntimeError,
			Err: errors.New("interrupted before the answer was complete, so nothing above is a " +
				"finished result"),
		}
	}
	return &exit.Error{Code: exit.RuntimeError, Err: fmt.Errorf("interrupted: %w", err)}
}
