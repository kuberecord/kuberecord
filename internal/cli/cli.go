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
// kubectl-conventional flag surface, and the rules by which a failure becomes an
// exit code.
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
// # Two names, one implementation
//
// The tree is built once and answers to both `kubectl kuberecord` and
// `kuberecord`, adjusting only what it calls itself. See InvocationName.
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
	"fmt"
	"io"

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
	root, _ := NewRootCommand(InvocationName(args), streams)

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
	failed, err := root.ExecuteC()
	if err == nil {
		return ExitSuccess
	}

	code := ExitCodeFor(err)

	// Built as one string and written once. Two writes would let another
	// writer's line land between the message and the usage block that explains
	// it, and stderr is shared with whatever else the shell has pointed at it.
	diagnostic := fmt.Sprintf("error: %v\n", err)
	if code == ExitUsageError {
		// A bare "unknown flag" with no reminder of what the flags are is a
		// worse message than the one cobra would have printed unprompted.
		diagnostic += "\n" + failed.UsageString()
	}

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
