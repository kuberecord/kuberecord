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

package cli_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The standing check: is any no-op silent? (D31, Task 16.7)
//
// # Why a test and not a review note
//
// Three times this release a capability worked correctly and was invisible at the
// moment it was needed — an unreachable address that named no fix (Task 13.1), a
// shortened row that never mentioned --full (Task 15.6), a --with-events that
// interleaved nothing and said nothing (Task 16.1). Each was found by somebody at
// a terminal, in a place nobody had looked. Three is a pattern, and a pattern is
// answered by a check rather than by a fourth fix.
//
// So the audit lives here rather than in a commit message. Every flag the command
// tree carries has a row below, and the sweep fails on a flag that has none —
// which makes the question "can this do nothing, and if so does the CLI say why?"
// something a new flag has to answer in review rather than in production.
//
// # The three verdicts
//
// alwaysVisible is the strongest and the rarest: the flag cannot produce output
// identical to its absence.
//
// explained is the flag that can, and does not. Something in the CLI notices and
// says so, and the row names what.
//
// deliberatelySilent is a decision, not a gap, and it is the reason this file is a
// table of prose rather than a list of names. --limit reaching its bound is the
// flag working; a notice for it would be noise on every invocation, and a sweep
// that demanded one for every flag would have made the output worse. What the row
// has to carry is why the silence is safe — which is the part a reviewer reads.

// noopVerdict is what the sweep has concluded about one flag.
type noopVerdict string

const (
	// alwaysVisible: no invocation of it produces output identical to its absence.
	alwaysVisible noopVerdict = "always visible"

	// explained: it can produce no visible change, and the CLI says why.
	explained noopVerdict = "explained"

	// deliberatelySilent: it can, and saying so would cost more than it is worth.
	// The row's reason is the whole of the justification.
	deliberatelySilent noopVerdict = "deliberately silent"
)

// auditedFlag is one row of the sweep.
type auditedFlag struct {
	// flag is the name as pflag knows it, without the leading dashes.
	flag string

	// verdict is the sweep's conclusion.
	verdict noopVerdict

	// why is the reason, and it is required for every verdict rather than only
	// for the silent ones: a row asserting that a flag is always visible without
	// saying what makes it so is a row nobody can check.
	why string
}

// inheritedFromKubectl is the reason every cli-runtime flag carries.
//
// They are listed one by one rather than matched by a prefix, at the cost of a
// little churn on a client-go bump, for the reason docs/CLI.md enumerates the
// same set: a rule that skipped them would be a sweep with a hole in it exactly
// the width of somebody else's flag surface, and adding the name is a one-line
// fix when it happens.
const inheritedFromKubectl = "cli-runtime's kubeconfig surface, with kubectl's own semantics. It is " +
	"consumed by the commands that build a client and ignored by the ones that build none, which is " +
	"what `kubectl version` does with -n. --namespace is the exception below"

// chainInput is the reason the backend-resolution globals carry.
const chainInput = "an input to the resolution chains, consumed by the commands that walk one. " +
	"`config resolve` exists precisely so that what the chains did with it is inspectable without " +
	"running a query (D26), and `version --check` reports what they produced"

// aBoundNotReached is the reason the flags that bound something carry.
const aBoundNotReached = "a bound that was not reached is the flag working, not a flag being " +
	"ignored. A notice on every invocation under the limit would be noise, and where the bound " +
	"does bite it names itself"

// noopAudit is the sweep, one row per flag in the tree.
//
// It is the enumerated audit Task 16.7 asks for, kept as code so that the next
// flag added is measured against it by `make test` rather than by a user.
var noopAudit = []auditedFlag{
	// `timeline`, `diff`, `blame`, `get`, `scopes` — the flags that shape an answer.
	{"since", explained, "an empty window is explained by explainEmpty against coverage, or by " +
		"scopesFinding; a window a backend forced is named by timelineBounds"},
	{"until", explained, "as --since"},
	{"from", explained, "an alias of --since, collapsed onto it before anything reads a bound"},
	{"to", explained, "an alias of --until"},
	{"limit", deliberatelySilent, aBoundNotReached + ": displayFilterNotice says '--limit bounds " +
		"the changes examined, not the ones shown' on the one path where the two differ"},
	{"reverse", deliberatelySilent, "an ordering over fewer than two rows is the same ordering. " +
		"There is nothing to report and no false conclusion available"},
	{"actor", explained, "predicateNotice re-reads the window without it and distinguishes 'the " +
		"filter matched nothing' from 'there was nothing to match' (Task 16.7)"},
	{"exclude-actor", explained, "as --actor, through the same probe"},
	{"field", explained, "on `timeline` through predicateNotice, on `diff` through " +
		"displayFilterNotice, on `blame` through blameFilterNotice — three commands, one " +
		"distinction: the filter's emptiness is never presented as the window's"},
	{"uid", explained, "on `timeline`, `diff` and `blame`, pinnedNotices names the incarnations " +
		"that are in the window; on `get`, pinnedIncarnationClause names the flag among the " +
		"reasons no state was found"},
	{"all-incarnations", alwaysVisible, "the header switches from `UID:` to `Incarnations:` and " +
		"the table gains a UID column, which happens for one incarnation as well as for five"},
	{"full", explained, "fullHint names it under a table whose CHANGE column withheld something, " +
		"and stays away when nothing was withheld (Task 15.6). Under a structured format it is " +
		"silent by design: the envelope carries every operation already, so --full asks for " +
		"something that is already true"},
	{"with-events", explained, "explainNoEvents gives the three states, and prints the rule " +
		"fragment that would make Events appear (Task 16.1)"},
	{"depth", deliberatelySilent, "it collapses paths onto their prefixes and merges the rows " +
		"that coincide, so it can make a table shorter and can never make it empty. " +
		"See blameFilterNotice for why it is not part of that predicate"},
	{"exit-code", deliberatelySilent, "the exit code is the effect, and 0-for-no-changes is " +
		"`git diff`'s own contract rather than the flag being ignored. The emptiness itself is " +
		"still explained by explainEmpty, and a non-zero code is announced by changesFoundNotice"},
	{"verify", alwaysVisible, "a pass prints the digest it checked, a failure exits 1 and writes " +
		"no document at all"},
	{"at", deliberatelySilent, "the header prints `at:`, `base row:` and `patches applied:` on " +
		"every invocation, so what the instant selected is on the page whether or not it moved"},
	{"kind", explained, "scopesFinding names the scope that came back empty and exits 3; a kind " +
		"resolved without the cluster is announced on stderr"},

	// `version` and `config resolve`.
	{"check", alwaysVisible, "it adds the setup block, including the `cannot be checked` outcome " +
		"when the chains produced nothing to probe"},

	// `config set-profile`.
	{"from-sink", alwaysVisible, "it writes the stanza and prints the derivation that produced it"},
	{"backend", alwaysVisible, "every route through set-profile announces the write; a flag " +
		"--from-sink already answers is refused by name rather than silently overridden"},
	{"addr", alwaysVisible, "as --backend"},
	{"database", alwaysVisible, "as --backend"},
	{"username", alwaysVisible, "as --backend"},
	{"password-env", alwaysVisible, "as --backend"},
	{"password-file", alwaysVisible, "as --backend"},
	{"tls", alwaysVisible, "as --backend"},
	{"bucket", alwaysVisible, "as --backend"},
	{"region", alwaysVisible, "as --backend"},
	{"endpoint", alwaysVisible, "as --backend"},
	{"force-path-style", alwaysVisible, "as --backend"},
	{"prefix", alwaysVisible, "as --backend"},
	{"path", alwaysVisible, "as --backend"},

	// `completion`.
	{"no-descriptions", alwaysVisible, "all four generators honour it; the two that cobra offers " +
		"no flag for are given the description-free entry point instead"},

	// Cobra's own.
	{"help", alwaysVisible, "it replaces the command's output with the help text"},

	// kuberecord's global surface.
	{"output", deliberatelySilent, "every format a command cannot render is refused by name. " +
		"`wide` on `version`, `config view` and `config resolve` renders identically to `table`, " +
		"and that is the flag's guarantee honoured rather than dropped: `wide` means the same " +
		"table with nothing elided, and those three documents elide nothing at any width. " +
		"docs/CLI.md's format matrix says so"},
	{"color", deliberatelySilent, "a rendering mode, never a request for content: it changes how " +
		"a line is painted and never which lines there are. A notice about colour would be the " +
		"noise it was warning about"},
	{"cluster-id", deliberatelySilent, chainInput},
	{"sink", deliberatelySilent, chainInput},
	{"source", deliberatelySilent, chainInput},
	{"profile", deliberatelySilent, chainInput},
	{"operator-namespace", deliberatelySilent, chainInput},
	{"sink-addr", explained, "it is refused by name on `config set-profile`, where it would " +
		"otherwise parse, change no field, and leave its author believing they had set the " +
		"address the profile records (errSinkAddrWritesNoFile, D31). Elsewhere it is " + chainInput},
	{"yes", deliberatelySilent, "it answers a question. A question that was not asked — an " +
		"indexed backend, a non-interactive stream, a scan under the confirmation width — needed " +
		"no answer, which is what `rm -f` does with a file nothing would have prompted about"},
	{"max-objects", deliberatelySilent, "a circuit breaker for the work --limit cannot bound " +
		"without an index. On an indexed backend the work is already bounded, and a breaker that " +
		"does trip names itself through coldscan.Stopped"},
	{"v", deliberatelySilent, "klog's own contract: a level nothing logs at logs nothing"},

	// Inherited from cli-runtime.
	{"namespace", explained, "the one inherited flag that is announced when it is dropped: a " +
		"--namespace given for a cluster-scoped kind is not part of that object's identity, and " +
		"objectNamespace says which flag it did not obey"},
	{"as", deliberatelySilent, inheritedFromKubectl},
	{"as-group", deliberatelySilent, inheritedFromKubectl},
	{"as-uid", deliberatelySilent, inheritedFromKubectl},
	{"as-user-extra", deliberatelySilent, inheritedFromKubectl},
	{"cache-dir", deliberatelySilent, inheritedFromKubectl},
	{"certificate-authority", deliberatelySilent, inheritedFromKubectl},
	{"client-certificate", deliberatelySilent, inheritedFromKubectl},
	{"client-key", deliberatelySilent, inheritedFromKubectl},
	{"cluster", deliberatelySilent, inheritedFromKubectl},
	{"context", deliberatelySilent, inheritedFromKubectl},
	{"disable-compression", deliberatelySilent, inheritedFromKubectl},
	{"insecure-skip-tls-verify", deliberatelySilent, inheritedFromKubectl},
	{"kubeconfig", deliberatelySilent, inheritedFromKubectl},
	{"request-timeout", deliberatelySilent, inheritedFromKubectl},
	{"server", deliberatelySilent, inheritedFromKubectl},
	{"tls-server-name", deliberatelySilent, inheritedFromKubectl},
	{"token", deliberatelySilent, inheritedFromKubectl},
	{"user", deliberatelySilent, inheritedFromKubectl},
}

// TestEveryFlagIsAuditedForSilentNoOps is the standing check.
//
// It walks the real command tree rather than a list kept beside it, so a flag
// added anywhere — a new command, a new global, a client-go bump that widens
// ConfigFlags — arrives here as a failure naming the commands that carry it.
func TestEveryFlagIsAuditedForSilentNoOps(t *testing.T) {
	owners := flagOwners(t)

	audited := make(map[string]auditedFlag, len(noopAudit))
	for _, row := range noopAudit {
		if previous, duplicate := audited[row.flag]; duplicate {
			t.Errorf("--%s is audited twice, as %q and as %q; one flag has one verdict",
				row.flag, previous.verdict, row.verdict)
			continue
		}
		if strings.TrimSpace(row.why) == "" {
			t.Errorf("--%s is recorded as %q with no reason; a verdict nobody can check is not "+
				"an audit", row.flag, row.verdict)
		}
		audited[row.flag] = row
	}

	for _, name := range sortedKeys(owners) {
		if _, ok := audited[name]; ok {
			continue
		}
		t.Errorf("--%s (on %s) is not in noopAudit. Answer D31 for it: can it produce output "+
			"identical to its absence, and if so does the CLI say why? A row saying "+
			"%q with a reason is a valid answer",
			name, strings.Join(owners[name], ", "), deliberatelySilent)
	}

	for _, row := range noopAudit {
		if _, ok := owners[row.flag]; !ok {
			t.Errorf("noopAudit still carries --%s, which no command in the tree has. A stale row "+
				"makes the sweep look more complete than it is", row.flag)
		}
	}

	// Non-vacuity, as docs_test.go's flag walk keeps: a walk that found a handful
	// has walked something other than this CLI, and every subtest above would then
	// pass by never running.
	if len(owners) < 55 {
		t.Fatalf("the flag walk found only %d flags (%v); the tree carries far more, so this "+
			"check is measuring nothing", len(owners), sortedKeys(owners))
	}
}

// flagOwners maps every flag in the tree to the commands that accept it.
//
// Collected across the whole tree first, so one unaudited name is one failure
// rather than one per command that inherits it.
func flagOwners(t *testing.T) map[string][]string {
	t.Helper()

	io, _, _ := streams()
	root, _ := cli.NewRootCommand(options.StandaloneName, io)

	owners := map[string][]string{}
	var walk func(cmd *cobra.Command, path string)
	walk = func(cmd *cobra.Command, path string) {
		// Cobra registers --help lazily, on the first help or execution. It is
		// initialized here so that the sweep covers the whole surface a user can
		// type rather than only the part this repository registers eagerly.
		cmd.InitDefaultHelpFlag()

		record := func(set *pflag.FlagSet) {
			set.VisitAll(func(f *pflag.Flag) {
				if !slices.Contains(owners[f.Name], path) {
					owners[f.Name] = append(owners[f.Name], path)
				}
			})
		}
		record(cmd.LocalFlags())
		record(cmd.PersistentFlags())
		for _, child := range cmd.Commands() {
			walk(child, strings.TrimSpace(path+" "+child.Name()))
		}
	}
	walk(root, "(global)")
	return owners
}

// sortedKeys is the deterministic order a failure lists names in.
func sortedKeys(m map[string][]string) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// The behaviour the audit's one finding produced.
//
// `timeline`'s predicates are pushed into the query, so the rows a filter removed
// never arrive and an emptiness it produced is indistinguishable from an empty
// window. explainEmpty was then handed that emptiness and stated, of a hundred
// recorded changes, that nothing had changed. See explainNoMatches.

// filteredEmptyRequest is a bare `timeline` narrowed to an actor nothing matches.
func filteredEmptyRequest() cli.TimelineRequest {
	request := defaultRequest()
	request.Actors = []string{"nobody"}
	return request
}

// TestTimelineSaysWhenAFilterEmptiedAWindowThatIsNotEmpty is the finding, fixed.
func TestTimelineSaysWhenAFilterEmptiedAWindowThatIsNotEmpty(t *testing.T) {
	engine := watchedCheckoutEngine()

	stdout, stderr, err := runTimeline(t, engine, filteredEmptyRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGolden(t, "filter-matched-nothing", stdout, stderr)

	if !strings.Contains(stderr, "the window itself is not empty") {
		t.Errorf("an emptiness the filter produced is presented as an empty window:\n%s", stderr)
	}
	if !strings.Contains(stderr, "--actor nobody") {
		t.Errorf("the notice does not name the filter that produced the silence, so the reader "+
			"cannot see what to change:\n%s", stderr)
	}
	if strings.Contains(stderr, "nothing changed in that period") {
		t.Errorf("the filter's emptiness was explained against coverage, which states something "+
			"false about a window holding %d changes:\n%s", len(checkoutHistory()), stderr)
	}

	// The probe is the second query, and it must ask the same question minus the
	// predicates: a probe that widened the window or unpinned the incarnation
	// would answer about something other than what was just shown.
	if len(engine.queries) != 2 {
		t.Fatalf("expected the timeline query and one unfiltered probe, got %d queries", len(engine.queries))
	}
	probe := engine.queries[1]
	switch {
	case len(probe.Actors) != 0 || len(probe.ExcludeActors) != 0 || len(probe.FieldPaths) != 0:
		t.Errorf("the probe carried the predicates it exists to remove: %+v", probe)
	case probe.UID != engine.queries[0].UID:
		t.Errorf("the probe asked about a different incarnation: %q, want %q", probe.UID, engine.queries[0].UID)
	case probe.From != engine.queries[0].From || probe.To != engine.queries[0].To:
		t.Errorf("the probe asked about a different window: %+v", probe)
	case probe.Limit != 1:
		t.Errorf("the probe read %d rows where existence is the whole of the answer", probe.Limit)
	case probe.IncludeEvents:
		t.Error("the probe merged Events, which the predicates deliberately never narrowed")
	}
}

// TestTimelineNamesEveryPredicateInForce keeps the notice actionable when more
// than one flag is narrowing.
func TestTimelineNamesEveryPredicateInForce(t *testing.T) {
	request := defaultRequest()
	request.Actors = []string{"nobody"}
	request.ExcludeActors = []string{"kube-controller-manager"}
	request.FieldPaths = []string{"spec.nonesuch"}

	_, stderr, err := runTimeline(t, watchedCheckoutEngine(), request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	for _, want := range []string{
		"--actor nobody", "--exclude-actor kube-controller-manager", "--field spec.nonesuch",
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("the notice does not name %q, so one of the three flags in force is invisible "+
				"in the explanation of its own result:\n%s", want, stderr)
		}
	}
}

// TestTimelineStillConsultsCoverageWhenTheWindowIsGenuinelyEmpty is the half the
// fix must not break.
//
// A filter over a scope nobody ever watched has to keep reaching the no-coverage
// finding and its exit 3: the probe is unfiltered, so a window nobody was
// watching holds nothing for it to find, and the emptiness is the window's rather
// than the filter's.
func TestTimelineStillConsultsCoverageWhenTheWindowIsGenuinelyEmpty(t *testing.T) {
	engine := &fakeEngine{caps: clickHouseCapabilities()}

	_, stderr, err := runTimeline(t, engine, filteredEmptyRequest(), render.Options{})
	if err == nil {
		t.Fatal("RunTimeline succeeded; a filter must not turn a scope nobody watched into an " +
			"ordinary empty result")
	}
	if !errors.Is(err, query.ErrNoCoverage) {
		t.Errorf("the failure does not carry query.ErrNoCoverage, so nothing maps it to exit %d: %v",
			exit.NoCoverage, err)
	}
	if strings.Contains(stderr, "the window itself is not empty") {
		t.Errorf("an empty window was reported as a filter's doing:\n%s", stderr)
	}
}

// TestTimelineReportsAProbeItCouldNotRun is Invariant 4 on the degradation path.
//
// A probe that could not run leaves "the filter did it" and "the window is empty"
// equally possible. Saying so is the only honest answer, and it must not become
// either the coverage explanation or a failed command.
func TestTimelineReportsAProbeItCouldNotRun(t *testing.T) {
	engine := watchedCheckoutEngine()
	engine.probeErr = errors.New("connection reset by peer")

	_, stderr, err := runTimeline(t, engine, filteredEmptyRequest(), render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: a probe is a qualification of an answer, not the answer: %v", err)
	}
	if !strings.Contains(stderr, "connection reset by peer") {
		t.Errorf("the probe failed and the notice does not say so, which is the silent error "+
			"Invariant 4 forbids:\n%s", stderr)
	}
	if strings.Contains(stderr, "nothing changed in that period") {
		t.Errorf("a probe that could not run was resolved into a claim it did not support:\n%s", stderr)
	}
}

// TestTimelineJudgesAFilterByTheChangesItCouldHaveKept is the --with-events case.
//
// The predicates narrow the object's own changes and deliberately leave merged
// Events alone, so a document made entirely of Event rows is one where --actor
// removed everything it could have kept. Judging the filter by whether the
// document was empty would have missed exactly that.
func TestTimelineJudgesAFilterByTheChangesItCouldHaveKept(t *testing.T) {
	engine := watchedCheckoutEngine()
	engine.events = []query.Change{{
		TS: at("2026-08-28T14:05:03.100Z"), EventType: query.EventKubernetes, UID: "e1",
		Actors: []string{"kube-controller-manager"}, APIVersion: "v1",
		Data: `{"type":"Normal","reason":"ScalingReplicaSet","message":"Scaled down to 1"}`,
	}}

	request := filteredEmptyRequest()
	request.WithEvents = true

	stdout, stderr, err := runTimeline(t, engine, request, render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if !strings.Contains(stdout, "ScalingReplicaSet") {
		t.Fatalf("the fixture stopped interleaving its Event, so this case asserts nothing about "+
			"a document made only of Event rows:\n%s", stdout)
	}
	if !strings.Contains(stderr, "the window itself is not empty") {
		t.Errorf("a filter that removed every change it could have kept said nothing, because the "+
			"Event rows made the document look answered:\n%s", stderr)
	}
}

// TestTimelineStructuredSaysWhenAFilterEmptiedTheWindow pins the same notice on
// the streaming path.
//
// The two paths share explainNoMatches and duplicate only the order the calls are
// made in, which is the half a comment cannot keep true.
func TestTimelineStructuredSaysWhenAFilterEmptiedTheWindow(t *testing.T) {
	for _, format := range []render.StructuredFormat{render.StructuredJSON, render.StructuredJSONL} {
		t.Run(string(format), func(t *testing.T) {
			request := filteredEmptyRequest()
			request.Structured = format

			_, stderr, err := runTimeline(t, watchedCheckoutEngine(), request, render.Options{})
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}
			if !strings.Contains(stderr, "the window itself is not empty") {
				t.Errorf("the streaming path presents a filter's emptiness as an empty window:\n%s", stderr)
			}
			if strings.Contains(stderr, "nothing changed in that period") {
				t.Errorf("the streaming path explained the filter's emptiness against coverage:\n%s", stderr)
			}
		})
	}
}

// TestGetNamesTheUIDItWasPinnedTo is the audit's other finding.
//
// `get --uid` had no listing to fall back on and no notice of its own, so a
// mistyped UID was answered with an explanation that never mentioned the flag:
// the object had not been observed, or had been deleted — and neither was true.
func TestGetNamesTheUIDItWasPinnedTo(t *testing.T) {
	engine := &fakeEngine{
		caps:      clickHouseCapabilities(),
		intervals: watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	request := cli.GetRequest{
		Ref: fixtureRef(), At: at("2026-08-28T15:00:00Z"), UID: priorUID,
		Format: render.StructuredYAML,
	}
	_, _, err := runGet(t, engine, request)
	if err == nil {
		t.Fatal("RunGet succeeded with no recorded state")
	}
	if !strings.Contains(err.Error(), "--uid") || !strings.Contains(err.Error(), priorUID) {
		t.Errorf("the failure never names the pin that caused it, so the flag to change is "+
			"invisible: %v", err)
	}
}
