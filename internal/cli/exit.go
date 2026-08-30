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
	"errors"
	"fmt"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Exit codes. These are a public contract from the first release: a wrapper
// script that branches on them is the reason the command exists at all, and a
// code that changes meaning between versions silently changes what that script
// does. They are documented in `--help` for the same reason.
//
// The interesting one is ExitNoCoverage. Every other CLI in this space collapses
// "your query matched nothing" and "nothing was ever watching that object" into
// a single successful empty result, and that collapse is precisely what
// Invariant 9 forbids: an engineer who greps for a deployment that was never in
// a StreamRule's scope must not be told, in the same shape as a real answer,
// that nothing happened to it. Giving it a code of its own means a script can
// tell the two apart without parsing prose.
const (
	// ExitSuccess is a completed command. For `diff --exit-code` (Task 11.4) it
	// additionally means "no changes", which is why that flag is opt-in: it
	// overloads a code that otherwise only means success.
	ExitSuccess = 0

	// ExitRuntimeError is a well-formed invocation that could not be carried
	// out: a backend that would not answer, a kind this cluster does not serve,
	// a reconstruction that disagreed with its recorded hash.
	ExitRuntimeError = 1

	// ExitUsageError is a malformed invocation — an unknown flag, an
	// unparseable object address, a flag value outside its documented set. It is
	// distinct from ExitRuntimeError because a script that retries on failure
	// should retry a backend timeout and must not retry a typo.
	ExitUsageError = 2

	// ExitNoCoverage means the request was well formed and the backend answered,
	// and the answer was that no watch scope ever covered the requested object.
	// It is not an error in the backend and not an empty result: it is the
	// finding that kuberecord was not looking (Invariant 9).
	ExitNoCoverage = 3
)

// Error is an error carrying the exit code the process should end with.
//
// It exists because the mapping from failure to exit code is a decision made
// where the failure happens — only the code that parsed an address knows the
// difference between "you typed this wrong" and "the cluster does not serve
// that kind" — and reconstructing that judgement at the top of main from an
// error string would be guesswork that drifts.
type Error struct {
	// Code is the process exit code this failure warrants.
	Code int

	// Err is the underlying failure, preserved for errors.Is/As by callers and
	// for the message printed to stderr.
	Err error

	// Quiet suppresses the "error: …" line Run would otherwise print.
	//
	// It exists for `diff --exit-code`, whose exit 1 means "changes found" and
	// not "something went wrong": git prints nothing for it, a script branches on
	// it, and an `error:` line would tell a human reading the same output that a
	// successful query had failed. The explanation still reaches them — as a
	// notice beside the document, where every other qualification of an answer
	// goes — so what is suppressed is the misleading word, not the information.
	Quiet bool
}

func (e *Error) Error() string { return e.Err.Error() }

// Unwrap keeps the wrapped failure inspectable, so attaching an exit code to an
// error never costs a caller the ability to classify what actually went wrong.
func (e *Error) Unwrap() error { return e.Err }

// ExitCode reports the code this failure warrants, satisfying the convention
// several standard tools use so that a future caller can classify without
// naming this type.
func (e *Error) ExitCode() int { return e.Code }

// UsageErrorf reports a malformed invocation: exit code ExitUsageError, and the
// command's usage block printed after the message.
func UsageErrorf(format string, args ...any) error {
	return &Error{Code: ExitUsageError, Err: fmt.Errorf(format, args...)}
}

// RuntimeErrorf reports a well-formed invocation that failed: exit code
// ExitRuntimeError, message only.
//
// It exists mainly for symmetry and for call sites that want the code stated
// rather than inferred; an uncoded error already resolves to ExitRuntimeError.
func RuntimeErrorf(format string, args ...any) error {
	return &Error{Code: ExitRuntimeError, Err: fmt.Errorf(format, args...)}
}

// ExitCodeFor is the single place a returned error becomes a process exit code.
//
// The order of the two checks is deliberate. An explicit *Error wins over the
// inferred mapping below it, so a command that has decided a missing-coverage
// condition is really a runtime failure in its context can say so and be
// believed. Only when nothing has decided does the sentinel from the read-plane
// contract get to speak for itself.
func ExitCodeFor(err error) int {
	if err == nil {
		return ExitSuccess
	}

	var coded *Error
	if errors.As(err, &coded) {
		return coded.Code
	}

	// query.ErrNoCoverage is the read plane's way of saying "nothing was
	// watching", and it is the only backend-independent error that has an exit
	// code of its own. Mapping it here rather than at each command's call site
	// means a command added later inherits Invariant 9's exit contract by
	// returning the sentinel the contract already defines.
	if errors.Is(err, query.ErrNoCoverage) {
		return ExitNoCoverage
	}

	return ExitRuntimeError
}
