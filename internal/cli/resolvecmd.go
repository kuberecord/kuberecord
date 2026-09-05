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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"sigs.k8s.io/yaml"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// `config resolve` is the answer to "why is it reading *that*".
//
// Nine steps decide where an answer comes from — four for the backend, five for
// the cluster identity — and on a working command they produce two lines of
// notice. That is the right amount of ceremony for an answer somebody wanted, and
// the wrong amount when the chain chose something unexpected: a profile written
// months ago shadowing discovery, a --context pointing at the wrong cluster, an
// identity read from an operator that is not the one being asked about. The
// result is then wrong in a way that looks right, and the only way to see why has
// been to read source or bisect flags.
//
// This command runs both chains and prints what every step decided. It answers no
// question about recorded history and returns no rows.
//
// # Why the reachability check is opt-in
//
// Because the configuration a user most wants to inspect is the one whose backend
// cannot be reached (D26). A command that dialled in order to describe itself
// would stall for a dial timeout on exactly the case it exists for, so nothing
// here contacts the backend unless --check says to. The identity chain's last
// step — the only part of resolution that questions the backend — is withheld and
// reported as withheld, which is a different finding from a chain that failed and
// is rendered as one.
//
// # Why the failures are printed rather than substituted
//
// A chain that cannot resolve returns the error a query command would have
// returned, unchanged and with its own exit code, so a malformed --sink is still
// a usage error here and a forbidden Secret is still a runtime one. The report is
// printed first and the failure returned second: the half that worked is the half
// a reader needs in order to understand the half that did not.

// ResolutionKind is the kind of the document `config resolve` renders in a
// structured format.
//
// It carries render.EnvelopeAPIVersion and is deliberately not one of that
// package's envelope kinds, for the reason VersionKind is not: an envelope's
// metadata is the provenance of an *answer* — which cluster, which engine, what
// was watching — and this command asks no backend anything. A Resolution carrying
// an empty coverage report would invite a consumer to read a field that could
// never mean anything.
//
// What it does share is the contract those kinds are governed by: the same
// apiVersion, and therefore the same additive-only policy. Fields may be added
// here and must never be renamed, removed or repurposed within
// cli.kuberecord.io/v1alpha1 (D19).
const ResolutionKind = "Resolution"

// resolutionDocument is what `config resolve` renders in JSON and YAML.
//
// The field names are this CLI's own camelCase rather than the envelope's
// snake_case, exactly as versionDocument's are: the envelope's item fields are
// spelled the way the frozen schema spells its columns because they are the same
// data reached two ways, and nothing here is schema data.
//
// The one exception is capabilities, which is query.Capabilities unchanged. That
// is the read plane's own declaration, surfaced as metadata.backend in every
// other structured answer and tabulated under those spellings in docs/CLI.md, and
// restating it here would be a second spelling of one contract — the same reason
// the envelope carries query.Change rather than a copy of it.
type resolutionDocument struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`

	// Backend is what the first chain decided.
	Backend resolutionBackend `json:"backend"`

	// ClusterID is what the second chain decided.
	ClusterID resolutionClusterID `json:"clusterID"`

	// Check is what --check found, and is present whether or not it was asked
	// for: a consumer branching on `.check.outcome` gets `not checked` rather
	// than a missing key, so the absence of a probe is a value it can read
	// instead of one it has to infer.
	Check resolutionCheck `json:"check"`
}

// resolutionBackend is the backend chain's result.
type resolutionBackend struct {
	// Resolved reports whether the chain produced a backend at all. It is the
	// field a script branches on, because every field below it is conditional on
	// this one.
	Resolved bool `json:"resolved"`

	// Origin is the step that answered, spelled as resolve.Origin spells it —
	// `discovered`, `profile`, `source-flag`, `sink-flag`. It is the step that
	// *failed* when Resolved is false.
	Origin string `json:"origin,omitempty"`

	// Description names the backend the way the resolution notice does, with no
	// credential in it at any verbosity.
	Description string `json:"description,omitempty"`

	// Capabilities is the chosen engine's own declaration of what it can answer.
	// Absent when no engine was opened.
	Capabilities *query.Capabilities `json:"capabilities,omitempty"`

	// Steps is every step of the chain, in the order they are tried.
	Steps []resolutionStep `json:"steps"`

	// Error is why the chain produced nothing. Present only when it did not.
	Error string `json:"error,omitempty"`
}

// resolutionClusterID is the identity chain's result.
type resolutionClusterID struct {
	// Resolved reports whether the chain produced an identity.
	//
	// False with an empty Error is not a contradiction: it is the state a
	// withheld last step leaves behind, where nothing failed and the one step
	// that could still answer was not taken. Read the steps to tell the two
	// apart, or pass --check and ask.
	Resolved bool `json:"resolved"`

	// Value is the kuberecord cluster identity (D21) — the cluster_id column of
	// the frozen schema, not a kubeconfig entry.
	Value string `json:"value,omitempty"`

	// Source says how it was arrived at, in the words the notice uses.
	Source string `json:"source,omitempty"`

	// Steps is every step of the chain, in the order they are tried.
	Steps []resolutionStep `json:"steps"`

	// Error is why the chain produced nothing. Present only when it failed, which
	// is not the same as not having resolved.
	Error string `json:"error,omitempty"`
}

// resolutionStep is one step of a chain, as the document reports it.
//
// It is this package's spelling of resolve.ChainStep rather than that type with
// tags on it, for the reason versionBackend is not resolve.CompiledBackend: the
// structured output is a public contract and the resolver's type is not, and the
// day one of them needs a field the other does not want must not be the day a
// released document changes shape.
type resolutionStep struct {
	// Step names the step: a flag, a file, a place to look.
	Step string `json:"step"`

	// Outcome is one of `answered`, `silent`, `failed`, `not reached`,
	// `withheld`. See resolve.StepOutcome for what each one means.
	Outcome string `json:"outcome"`

	// Detail is what it answered with, or why it had nothing.
	Detail string `json:"detail"`
}

// resolutionCheck is what asking the backend produced.
type resolutionCheck struct {
	// Requested reports whether --check was given.
	Requested bool `json:"requested"`

	// Outcome is one of the four below. It is a word rather than a boolean
	// because `reachable: false` on an invocation that never asked would be a
	// claim this command did not make.
	Outcome string `json:"outcome"`

	// Detail says what that outcome means for this invocation.
	Detail string `json:"detail"`

	// Error is the failure the probe met, with no credential in it. Present only
	// when the backend could not be reached.
	Error string `json:"error,omitempty"`
}

// The four things a reachability check can conclude.
const (
	// checkNotRun means no probe was performed: --check was not given, or there
	// was no backend to probe.
	checkNotRun = "not checked"

	// checkReachable means the backend answered.
	checkReachable = "reachable"

	// checkUnreachable means it did not. This is the outcome that carries Task
	// 13.1's explanation when the address is one that only resolves inside a
	// cluster.
	checkUnreachable = "unreachable"

	// checkUnsupported means the backend cannot say without running a real query.
	// It is a statement about the engine and not a fault, so it does not fail the
	// command (Invariant 5).
	checkUnsupported = "cannot be checked"
)

// ResolveRequest is one `config resolve` invocation, resolved.
//
// It is small because the Inspection carries the answer: everything the report
// says about why a step had nothing to say is recorded by the chain as it walks,
// rather than re-derived here from the flags. A second description of the chain,
// assembled beside a cobra command, would agree with the chain until the day
// somebody reordered it.
type ResolveRequest struct {
	// Check asks the backend whether it can be reached.
	Check bool

	// Structured names the serialization of the document to write. Empty means
	// the human-readable form.
	Structured render.StructuredFormat
}

// newConfigResolveCommand builds `config resolve`.
func newConfigResolveCommand(
	flags *options.GlobalFlags, streams genericiooptions.IOStreams, invokedAs string,
) *cobra.Command {
	var check bool

	command := &cobra.Command{
		Use:   "resolve",
		Short: "Show what the resolution chains would choose, without running a query",
		Long: `Show what the resolution chains would choose, without running a query.

Two chains decide where an answer comes from. The backend chain tries --source,
then --sink, then the active profile, then the cluster's own sink custom
resources. The cluster-identity chain tries --cluster-id, then the configuration
file's context mapping, then the operator's Deployment, then the sink itself.

This prints what every step of both decided: which one answered, and why the
ones before it had nothing to say. Each step is reported as one of:

  answered      it produced the chain's result
  silent        it was consulted and had nothing — no flag, no profile, no entry
  failed        it had something to say and could not say it
  not reached   an earlier step answered, or the chain stopped first
  withheld      it would have contacted the backend, and --check was not given

It contacts no backend unless --check is given, because the configuration most
worth inspecting is the one whose backend cannot be reached.`,
		Example: `  # What would this invocation read, and why?
  kuberecord config resolve

  # The same question about another context, with the backend actually dialled.
  kuberecord --context prod-eu config resolve --check

  # For a support request, which is a better first question than "what does
  # your config look like".
  kuberecord config resolve -o json`,

		Args: rejectPositionalArgs,

		RunE: func(cmd *cobra.Command, _ []string) (err error) {
			structured, err := resolutionFormat(flags.Output)
			if err != nil {
				return err
			}

			resolver, err := resolve.NewBackendResolver(flags, streams, invokedAs)
			if err != nil {
				return err
			}
			inspection := resolver.Inspect(cmd.Context(), check)
			if inspection.Backend != nil {
				defer func() {
					// Joined rather than replacing: the reason the command is
					// ending is whatever the chains found, and the tidying up must
					// not hide it.
					err = errors.Join(err, inspection.Backend.Close())
				}()
			}

			return RunResolve(cmd.Context(), inspection,
				ResolveRequest{Check: check, Structured: structured}, streams)
		},
	}

	command.Flags().BoolVar(&check, options.FlagCheck, check,
		"Also ask the resolved backend whether it can be reached, and report what happened. "+
			"Without it nothing is dialled, which is what lets a configuration whose backend is "+
			"unreachable still be inspected.")

	return command
}

// resolutionFormat decides which of this command's renderings an invocation asked
// for. An empty StructuredFormat means the human-readable form.
//
// `jsonl` and `diff` are refused by name, exactly as `version` refuses them: the
// first is a streaming format for a result larger than memory and this document
// is one item, the second renders change operations and there are none here.
// Rendering something else regardless would leave a user wondering why their flag
// did nothing.
func resolutionFormat(format options.OutputFormat) (render.StructuredFormat, error) {
	switch format {
	case options.OutputTable, options.OutputWide:
		return "", nil
	case options.OutputJSON:
		return render.StructuredJSON, nil
	case options.OutputYAML:
		return render.StructuredYAML, nil
	}
	return "", exit.UsageErrorf("config resolve renders %s, %s or %s, not %s",
		options.OutputTable, options.OutputJSON, options.OutputYAML, format)
}

// RunResolve reports one inspection and returns what it found wrong.
//
// It is exported, and takes an already-walked resolve.Inspection, for the reason
// RunTimeline takes an already-opened backend: the whole of the command's
// behaviour — which step is reported as answering, how a withheld step reads, what
// a failed probe prints — is then reachable from a test holding fixtures rather
// than only from one that can reach a kubeconfig and a live sink.
//
// The document goes to stdout because it is this command's data. The failure is
// returned rather than printed, so that a chain that cannot resolve produces the
// same message, with the same exit code, that a query command would have produced
// from the same configuration.
func RunResolve(
	ctx context.Context, inspection *resolve.Inspection, request ResolveRequest,
	streams genericiooptions.IOStreams,
) error {
	document := resolutionOf(inspection)
	check, checkErr := checkReachability(ctx, inspection, request.Check)
	document.Check = check

	if err := writeResolution(streams.Out, document, request.Structured); err != nil {
		return err
	}
	return resolutionFailure(inspection, checkErr)
}

// resolutionOf turns an inspection into the document, minus the probe.
func resolutionOf(inspection *resolve.Inspection) resolutionDocument {
	document := resolutionDocument{
		APIVersion: render.EnvelopeAPIVersion,
		Kind:       ResolutionKind,
		Backend: resolutionBackend{
			Resolved: inspection.Backend != nil,
			Origin:   string(inspection.Origin),
			Steps:    resolutionSteps(inspection.BackendSteps),
			Error:    errorText(inspection.BackendErr),
		},
		ClusterID: resolutionClusterID{
			Resolved: inspection.ClusterID != "",
			Value:    inspection.ClusterID,
			Source:   inspection.ClusterIDSource,
			Steps:    resolutionSteps(inspection.ClusterIDSteps),
			Error:    errorText(inspection.ClusterIDErr),
		},
	}
	if inspection.Backend != nil {
		capabilities := inspection.Backend.Engine.Capabilities()
		document.Backend.Description = inspection.Backend.Description
		document.Backend.Capabilities = &capabilities
	}
	return document
}

// resolutionSteps restates a chain's record in the document's own vocabulary.
//
// Never nil: an empty list would be a chain that was not walked, and a consumer
// iterating `.steps` over a null gets an error where the honest answer is zero
// iterations.
func resolutionSteps(steps []resolve.ChainStep) []resolutionStep {
	out := make([]resolutionStep, 0, len(steps))
	for _, step := range steps {
		out = append(out, resolutionStep{
			Step:    step.Step,
			Outcome: string(step.Outcome),
			// A message with a newline in it would break the alignment of a table
			// whose whole job is to be scanned down one column.
			Detail: strings.Join(strings.Fields(step.Detail), " "),
		})
	}
	return out
}

// errorText renders a failure for the document, or the empty string for none.
func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// checkReachability performs the probe, or says why it did not.
//
// It returns the failure separately from the report because the two have
// different jobs: the report is data and goes to stdout, and the failure decides
// the exit code and carries Task 13.1's explanation to the top of the CLI, where
// every command's unreachable-backend message is rendered.
//
// Two cases skip the probe rather than repeating a question that has already been
// put. An identity chain that questioned the backend and got an answer has proved
// it reachable. One that failed with an unreachable-backend diagnosis has proved
// the opposite, and dialling again would spend a second dial timeout to learn
// what is already known — on the one path where that wait is most expensive.
func checkReachability(
	ctx context.Context, inspection *resolve.Inspection, requested bool,
) (resolutionCheck, error) {
	if !requested {
		return resolutionCheck{
			Outcome: checkNotRun,
			Detail: fmt.Sprintf("nothing was dialled; --%s asks the backend whether it answers",
				options.FlagCheck),
		}, nil
	}
	if inspection.Backend == nil {
		return resolutionCheck{
			Requested: true,
			Outcome:   checkNotRun,
			Detail:    "the backend chain resolved nothing, so there was nothing to reach",
		}, nil
	}

	var alreadyDiagnosed *resolve.UnreachableSinkError
	switch {
	case inspection.Asked && inspection.ClusterIDErr == nil:
		return resolutionCheck{
			Requested: true,
			Outcome:   checkReachable,
			Detail:    "the backend answered the cluster-identity question the chain put to it",
		}, nil
	case errors.As(inspection.ClusterIDErr, &alreadyDiagnosed):
		return unreachableCheck(alreadyDiagnosed), alreadyDiagnosed
	}

	err := inspection.Backend.Check(ctx)
	switch {
	case err == nil:
		return resolutionCheck{
			Requested: true,
			Outcome:   checkReachable,
			Detail:    "the backend answered",
		}, nil
	case errors.Is(err, query.ErrCapabilityUnsupported):
		// A backend that cannot be probed without running a real query has said
		// something true about itself. Reporting that as a red result would be
		// inventing a fault, so it degrades with a statement instead (Invariant 5).
		return resolutionCheck{
			Requested: true,
			Outcome:   checkUnsupported,
			Detail:    err.Error(),
		}, nil
	}
	return unreachableCheck(err), err
}

// unreachableCheck is the report for a probe that failed.
func unreachableCheck(err error) resolutionCheck {
	return resolutionCheck{
		Requested: true,
		Outcome:   checkUnreachable,
		Detail:    "the backend could not be reached",
		Error:     err.Error(),
	}
}

// resolutionFailure decides what this invocation exits with.
//
// The backend chain comes first because its failure is the reason the others had
// nothing to work with, and reporting a consequence ahead of its cause sends the
// reader to the wrong place. Each error is returned exactly as the chain produced
// it, so a malformed --sink still exits 2 and a forbidden Secret still exits 1.
//
// An undetermined cluster identity is not a failure. Nothing went wrong: the
// chain has a step that would answer and this invocation declined to take it,
// which the report says in as many words.
func resolutionFailure(inspection *resolve.Inspection, checkErr error) error {
	switch {
	case inspection.BackendErr != nil:
		return inspection.BackendErr
	case inspection.ClusterIDErr != nil:
		return inspection.ClusterIDErr
	}
	return checkErr
}

// writeResolution renders the document in the requested format.
func writeResolution(out io.Writer, document resolutionDocument, format render.StructuredFormat) error {
	switch format {
	case "":
		return options.WriteAll(out, renderResolution(document))

	case render.StructuredJSON:
		encoded, err := json.MarshalIndent(document, "", "  ")
		if err != nil {
			return exit.RuntimeErrorf("encoding the resolution: %w", err)
		}
		return options.WriteAll(out, string(encoded)+"\n")

	case render.StructuredYAML:
		// Through the JSON tags, so the two serializations are one document in two
		// syntaxes rather than two documents that resemble each other.
		encoded, err := yaml.Marshal(document)
		if err != nil {
			return exit.RuntimeErrorf("encoding the resolution: %w", err)
		}
		return options.WriteAll(out, string(encoded))
	}
	// Unreachable through resolutionFormat, which accepts three formats and
	// refuses the rest by name. Stated rather than ignored, because the
	// alternative to a stated error here is a command that prints nothing and
	// exits zero.
	return exit.RuntimeErrorf("config resolve cannot render %q", format)
}

// renderResolution is the human form.
//
// Nothing here colourises and nothing wraps, which is a decision rather than an
// omission. This is a report about a configuration rather than a table of
// records: its long lines are error messages and file paths, and a renderer that
// folded them at a terminal width would break the one thing a reader does with
// them, which is copy them. `version`, the other command that describes the tool
// rather than the history, renders the same way.
//
// The gutter is measured across both chains so the two blocks line up under each
// other. A reader compares them vertically — which step answered here, which step
// answered there — and two independently-padded tables would make that comparison
// harder than reading either one.
func renderResolution(document resolutionDocument) string {
	rows := resolutionRows(document)

	labelWidth, outcomeWidth := 0, 0
	for _, row := range rows {
		if row.heading {
			continue
		}
		labelWidth = max(labelWidth, len(row.label))
		outcomeWidth = max(outcomeWidth, len(row.outcome))
	}

	var page strings.Builder
	for _, row := range rows {
		switch {
		case row.heading:
			page.WriteString(row.label + "\n")
		case row.blank:
			page.WriteString("\n")
		default:
			line := "  " + padRight(row.label, labelWidth)
			if row.text != "" {
				line += "  " + row.text
			} else if row.outcome != "" || row.detail != "" {
				line += "  " + padRight(row.outcome, outcomeWidth)
				if row.detail != "" {
					line += "  " + row.detail
				}
			}
			page.WriteString(strings.TrimRight(line, " ") + "\n")
		}
	}
	return page.String()
}

// resolutionRow is one line of the human form, before it is padded.
//
// A summary row — what the chain resolved to, what the probe found — sets text
// rather than outcome and detail, because those two are a step's columns and its
// own content is a sentence rather than a cell. Keeping them apart is what stops
// a long description from widening the column every step row is aligned in.
type resolutionRow struct {
	heading bool
	blank   bool
	label   string
	outcome string
	detail  string
	text    string
}

// resolutionRows lays the document out as lines.
func resolutionRows(document resolutionDocument) []resolutionRow {
	rows := []resolutionRow{{heading: true, label: "backend"}}
	rows = append(rows, stepRows(document.Backend.Steps)...)
	rows = append(rows, resolutionRow{blank: true})

	if document.Backend.Resolved {
		rows = append(rows,
			resolutionRow{label: "resolved", text: document.Backend.Description},
			resolutionRow{label: "engine", text: document.Backend.Capabilities.Backend},
			resolutionRow{label: "capabilities", text: describeCapabilities(*document.Backend.Capabilities)})
	} else {
		rows = append(rows, resolutionRow{label: "unresolved"})
	}

	rows = append(rows,
		resolutionRow{blank: true},
		resolutionRow{heading: true, label: "cluster identity"})
	rows = append(rows, stepRows(document.ClusterID.Steps)...)
	rows = append(rows, resolutionRow{blank: true})

	switch {
	case document.ClusterID.Resolved:
		rows = append(rows, resolutionRow{
			label: "resolved",
			text:  fmt.Sprintf("%s (%s)", document.ClusterID.Value, document.ClusterID.Source),
		})
	case document.ClusterID.Error != "":
		rows = append(rows, resolutionRow{label: "unresolved"})
	default:
		// Neither resolved nor failed: the last step was withheld, and the row
		// above already says which flag takes it.
		rows = append(rows, resolutionRow{label: "undetermined"})
	}

	rows = append(rows,
		resolutionRow{blank: true},
		resolutionRow{heading: true, label: "reachability"},
		resolutionRow{label: document.Check.Outcome, text: document.Check.Detail})
	if document.Check.Error != "" {
		rows = append(rows, resolutionRow{label: "", text: document.Check.Error})
	}
	return rows
}

// stepRows renders one chain's steps.
func stepRows(steps []resolutionStep) []resolutionRow {
	rows := make([]resolutionRow, 0, len(steps))
	for _, step := range steps {
		rows = append(rows, resolutionRow{label: step.Step, outcome: step.Outcome, detail: step.Detail})
	}
	return rows
}

// describeCapabilities renders the engine's declaration as one line.
//
// One line rather than four, because the question it answers is comparative: two
// setups that answer the same query differently differ here, and a reader holding
// two of these reports wants to put the lines side by side. The names are the
// contract's own, so a value read here matches the column in docs/CLI.md's
// capability table and the field in `-o json`.
func describeCapabilities(capabilities query.Capabilities) string {
	return strings.Join([]string{
		"deletions=" + yesNo(capabilities.Deletions),
		"server_side_filter=" + yesNo(capabilities.ServerSideFilter),
		"point_query=" + yesNo(capabilities.PointQuery),
		"time_bound_required=" + yesNo(capabilities.TimeBoundRequired),
	}, ", ")
}

// yesNo renders a declared capability.
func yesNo(declared bool) string {
	if declared {
		return "yes"
	}
	return "no"
}

// padRight pads text to width with spaces.
//
// It counts bytes rather than runes, which is correct for everything that reaches
// it: step names are flags and fixed English nouns, and outcomes are the five
// words resolve.StepOutcome defines. A value that could carry a multi-byte rune
// goes in the detail column, which is never padded because nothing follows it.
func padRight(text string, width int) string {
	if len(text) >= width {
		return text
	}
	return text + strings.Repeat(" ", width-len(text))
}
