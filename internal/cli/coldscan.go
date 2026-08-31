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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Making the cold scan's cost visible, refusable and interruptible.
//
// The object archive has no index (query.Capabilities.PointQuery false), so a
// question about one object costs every object in the partitions its window lands
// in — listed, fetched and decompressed. Ninety days of a busy cluster is
// thousands of objects and gigabytes off the wire for a table with four rows in
// it.
//
// That cost is the deliberate trade of the zero-infrastructure mode (D18), and the
// difference between a trade and a defect is entirely in whether the person paying
// it can see it. So four things happen around every such scan, and none of them is
// a courtesy:
//
//   - The window defaults to DefaultWindow instead of to everything, so a question
//     asked without thinking about time costs a day rather than the archive.
//   - The estimate (query.ScanEstimator) is printed before the first object is
//     fetched, and beyond ConfirmWindow — or when it could not be made at all —
//     it is printed as a question.
//   - Progress goes to stderr while it runs, because a tool that goes quiet for
//     four minutes is indistinguishable from one that has hung.
//   - --max-objects stops a scan that turns out larger than the estimate suggested,
//     naming itself, rather than running to completion because nothing was watching.
//
// An indexed backend is not guarded at all: there is nothing to estimate, nothing
// to narrate and nothing to break the circuit of. The guard is therefore keyed on
// the declared capability rather than on the backend's name (D17), which is what
// makes a future indexed backend inherit the right behaviour by declaring it.

// ScanOptions is one invocation's cold-scan safety surface.
//
// Interactive and ShowProgress are decided by the command from its streams rather
// than read from a terminal here, which is the same split --color already keeps:
// what the user asked for is a flag, where the output is going is a property of
// the invocation, and collapsing the two makes the behaviour untestable without a
// pseudo-terminal.
type ScanOptions struct {
	// AssumeYes answers the confirmation without asking. --yes sets it; so does a
	// non-interactive invocation, which is what keeps a script from hanging on a
	// prompt nobody is there to answer.
	AssumeYes bool

	// MaxObjects aborts the scan once it has fetched more than this many objects.
	// Zero means no breaker.
	MaxObjects int64

	// Interactive reports whether a confirmation can actually be asked for: both
	// the output and the input have to be a terminal. Stdout alone is not enough —
	// `kuberecord timeline … < /dev/null` on a terminal would prompt into a stdin
	// that answers EOF, which reads to the user as the tool refusing its own
	// question.
	Interactive bool

	// ShowProgress reports whether the progress line may be painted, which is
	// whether stderr is a terminal. It is stderr rather than stdout because that
	// is where the line goes: `-o json > file` on a terminal should still narrate.
	ShowProgress bool
}

// scanOptions reads the safety surface out of the flags and the streams.
func scanOptions(flags *GlobalFlags, streams genericiooptions.IOStreams) ScanOptions {
	return ScanOptions{
		AssumeYes:    flags.AssumeYes,
		MaxObjects:   flags.MaxObjects,
		Interactive:  isTerminal(streams.Out) && isTerminalIn(streams.In),
		ShowProgress: isTerminal(streams.ErrOut),
	}
}

// progressInterval is how often the progress line is repainted.
//
// Ten times a second: fast enough to read as live, slow enough that a scan
// fetching thousands of small objects spends its time on the archive rather than
// on the terminal. The callback itself fires once per object and is throttled
// here rather than in the engine, because the engine has no idea what its figures
// are being rendered on.
const progressInterval = 100 * time.Millisecond

// coldScan is a guarded scan in progress: the context the queries must use, and
// the teardown that stops the narration.
//
// The zero-cost case is a nil-safe one. An indexed backend gets a coldScan holding
// the caller's own context and nothing else, so the call sites do not branch.
type coldScan struct {
	// ctx is what every query of this scan must be issued with. It is the caller's
	// context when there is no breaker, and a cancellable child when there is.
	ctx context.Context

	cancel   context.CancelCauseFunc
	reporter query.ScanProgressReporter
	monitor  *scanMonitor
}

// stop tears the guard down: the callback is removed before the command writes
// anything else, so a repainting line cannot land in the middle of a notice.
//
// It is safe on a zero-value scan and safe to call more than once, because it is
// deferred at a call site that also has failure paths.
func (s *coldScan) stop() {
	if s == nil {
		return
	}
	if s.reporter != nil {
		s.reporter.SetScanProgress(nil)
		s.reporter = nil
	}
	if s.monitor != nil {
		s.monitor.clear()
		s.monitor = nil
	}
	if s.cancel != nil {
		// nil rather than a cause: the scan is over, and a cause recorded here
		// would be read by scanStopped as the reason it ended.
		s.cancel(nil)
		s.cancel = nil
	}
}

// scanLimitError is the cause a --max-objects trip cancels the scan with.
//
// It is a cause rather than a plain cancellation so that the failure the query
// returns — which is a context error by the time it surfaces — can be turned back
// into the sentence that names the flag. Without it an aborted scan would report
// "context canceled", which tells the user nothing they can act on and is
// indistinguishable from the Ctrl-C they did not press.
type scanLimitError struct {
	limit   int64
	scanned int64
}

func (e *scanLimitError) Error() string {
	return fmt.Sprintf(
		"the scan reached %s objects, past the --%s=%d circuit breaker, and was stopped before it had "+
			"read the whole window; narrow it with --since, or raise --%s",
		formatCount(e.scanned), FlagMaxObjects, e.limit, FlagMaxObjects)
}

// scanStopped reports the cause when this CLI, rather than the archive or the
// user, ended a scan.
//
// It exists because a cancelled query fails with a context error wherever it
// happened to be, and only the context knows why. Invariant 4's rule is that an
// answer that came back short must say why it is short, and "the breaker you set
// tripped" is the one reason a caller can do something about.
func scanStopped(ctx context.Context) error {
	var limit *scanLimitError
	if errors.As(context.Cause(ctx), &limit) {
		return limit
	}
	return nil
}

// beginColdScan gates an unindexed scan behind its estimate, and narrates it.
//
// It is called once per invocation, immediately after the window is settled and
// before any query is issued — including the incarnation listing, which costs the
// same partitions as the timeline itself and would otherwise be an unannounced
// scan in front of the announced one.
//
// The returned scan's ctx replaces the caller's for every subsequent query. The
// caller must stop it, and must do so before writing its document, so that the
// progress line is gone from the terminal by the time anything else is written.
//
// Errors: only a refused confirmation, which is a decision rather than a failure
// and is reported as an ordinary runtime error so that a wrapper script sees a
// non-zero exit for a question that was never answered. An estimate that cannot be
// produced is never itself the failure (Invariant 5) — refusing to answer a
// question because the warning about it could not be assembled would be the
// degradation making itself into the failure — but it does turn the scan into a
// question at any width, for the reason needsConfirmation gives.
func beginColdScan(
	ctx context.Context, backend *Backend, request TimelineRequest, from, to time.Time,
	streams genericiooptions.IOStreams,
) (*coldScan, error) {
	capabilities := backend.Engine.Capabilities()
	if capabilities.PointQuery {
		// An indexed backend seeks to the object's rows; the window is a predicate
		// rather than the work, so there is no cost to show and nothing to break.
		return &coldScan{ctx: ctx}, nil
	}

	opts := request.Scan
	size := estimateColdScan(ctx, backend.Engine, request.Ref.ClusterID, from, to, streams)
	if err := confirmColdScan(size, capabilities, from, to, opts, streams); err != nil {
		return nil, err
	}

	scan := &coldScan{ctx: ctx}
	reporter, ok := backend.Engine.(query.ScanProgressReporter)
	if !ok || (!opts.ShowProgress && opts.MaxObjects <= 0) {
		// Nothing to paint and nothing to enforce. The callback is not installed at
		// all rather than installed and ignored, so a piped invocation pays nothing
		// for a feature it cannot use.
		return scan, nil
	}

	scan.ctx, scan.cancel = context.WithCancelCause(ctx)
	scan.monitor = &scanMonitor{
		out:       streams.ErrOut,
		paint:     opts.ShowProgress,
		total:     size.figures,
		known:     size.known,
		limit:     opts.MaxObjects,
		cancel:    scan.cancel,
		lastAt:    time.Now(),
		unpainted: true,
	}
	scan.reporter = reporter
	reporter.SetScanProgress(scan.monitor.report)
	return scan, nil
}

// scanEstimate is what is known about a scan's size before it starts — including
// the two different ways it can be unknown.
//
// Keeping those two apart is the point of the type. "This engine offers no
// estimator" is a permanent property of a backend and says nothing about this
// particular scan; "an estimator was asked and could not answer" is a fact about
// this scan, and it is the one that has to change what is asked before it runs.
// Collapsing both into one bool, which is what this used to be, left the guard
// unable to tell them apart.
type scanEstimate struct {
	// figures is the estimate itself. It is meaningful only when known, and is
	// never rendered otherwise: see describeEstimate for why a zero would be worse
	// than an absence.
	figures query.ScanEstimate

	// known reports whether figures holds a real estimate, and so whether a number
	// may be printed at all.
	known bool

	// unmeasured reports that an estimator existed and could not answer.
	//
	// It is deliberately not the negation of known. An engine with no estimating
	// half is neither known nor unmeasured, because it never claimed it could
	// measure this or anything else; only a listing that was attempted and failed
	// is evidence that this scan's size is unknowable right now.
	unmeasured bool
}

// estimateColdScan asks what the scan will cost, and reports a backend that
// cannot say.
//
// The listing it performs is the same listing the scan is about to perform, so it
// is not free — but it opens no object (query.ScanEstimator's own rule), which is
// where the cost of a scan actually is. Paying one listing to avoid an unwanted
// scan of thousands of objects is the trade this whole file is built on.
func estimateColdScan(
	ctx context.Context, engine query.QueryEngine, clusterID string, from, to time.Time,
	streams genericiooptions.IOStreams,
) scanEstimate {
	estimator, ok := engine.(query.ScanEstimator)
	if !ok {
		return scanEstimate{}
	}

	estimate, err := estimator.EstimateScan(ctx, clusterID, from, to)
	if err != nil {
		// Reported, not swallowed, and not fatal. The scan is still answerable; what
		// has been lost is the warning about it, and saying so is what stops the
		// silence from reading as "this is cheap" (Invariant 4).
		//
		// The sentence stops at the fact, and names the underlying error so that a
		// broken listing is diagnosable rather than merely reported. What follows from
		// it — a question on a terminal, an assumed confirmation anywhere else — is
		// said by the next line, because this function cannot know which applies.
		_ = writeLine(streams.ErrOut, fmt.Sprintf(
			"→ the size of this scan could not be estimated (%v), so it is unknown", err))
		return scanEstimate{unmeasured: true}
	}
	return scanEstimate{figures: estimate, known: true}
}

// confirmColdScan prints the estimate and, when the scan is a decision, asks.
//
// The two cases are one function because they are one decision: the figures shown
// are identical, and only the punctuation at the end of the line differs. Keeping
// them apart would be two places for the estimate's wording to drift.
//
// There is exactly one refusal in here, and there must stay exactly one. Both
// reasons a scan becomes a question — too wide, or unmeasured — decline through the
// same sentence and the same exit code, because a user who has just said no should
// not have to work out which of two no's they were given.
func confirmColdScan(
	size scanEstimate, capabilities query.Capabilities,
	from, to time.Time, opts ScanOptions, streams genericiooptions.IOStreams,
) error {
	figures := describeEstimate(size)

	if !needsConfirmation(capabilities, size, from, to) {
		if !size.known {
			// Nothing to announce. Saying "an unmeasured number of objects" about an
			// engine that never offered a figure would be a warning with no content,
			// printed before every question; a failed estimate has already been reported
			// on by estimateColdScan, and repeating it here would be a second sentence
			// about the same absence.
			return nil
		}
		return writeLine(streams.ErrOut, fmt.Sprintf(
			"→ %s to scan%s: the %s backend has no index, so this window is the work",
			figures, describeScanSpan(from, to), capabilities.Backend))
	}

	if opts.AssumeYes || !opts.Interactive {
		return writeLine(streams.ErrOut, fmt.Sprintf(
			"→ %s to scan: %s. %s",
			figures, describeConfirmReason(size, capabilities, from, to), assumedReason(opts)))
	}

	confirmed, err := askConfirmation(streams, confirmPrompt(size))
	if err != nil {
		return err
	}
	if !confirmed {
		return RuntimeErrorf(
			"stopped at the confirmation: nothing was read. Narrow the window with --since, "+
				"cap the work with --%s, or pass --%s to skip this question",
			FlagMaxObjects, FlagAssumeYes)
	}
	return nil
}

// describeConfirmReason says why this scan is a decision, in the words of
// whichever reason made it one.
//
// Both reasons occupy the same position in the sentence, so the line explaining an
// assumed confirmation reads the same shape whichever applied. The width is
// deliberately not named in the unmeasured case: a narrow window is exactly the
// invocation the old guard let through unasked, and mentioning it here would
// suggest it was the thing being judged.
func describeConfirmReason(
	size scanEstimate, capabilities query.Capabilities, from, to time.Time,
) string {
	if size.unmeasured {
		return fmt.Sprintf(
			"its size could not be determined against the %s backend, which has no index",
			capabilities.Backend)
	}
	return fmt.Sprintf("%s is wider than %s against the %s backend, which has no index",
		describeScanWidth(from, to), DescribeSpan(ConfirmWindow), capabilities.Backend)
}

// confirmPrompt is the question itself.
//
// The unmeasured case says so in the prompt rather than leaving it to the notice
// printed above it, because the person answering is deciding on the basis of not
// knowing, and that has to be legible in the sentence they are answering rather
// than in one they may have scrolled past. The measured prompt is untouched: the
// figures and a question mark, which is what a reader has learned to skim.
func confirmPrompt(size scanEstimate) string {
	if size.unmeasured {
		return describeEstimate(size) +
			", because its size could not be determined — continue? [y/N] "
	}
	return describeEstimate(size) + " — continue? [y/N] "
}

// describeScanSpan names how wide the measured window is, or says nothing when it
// has no width to name.
//
// The width rather than the two instants, and the width rather than nothing. The
// estimate is written before the notices — it has to be, since it exists to be read
// before the scan starts — so at the moment a reader sees "~1,240 objects" the line
// that says which window was used has not been printed yet. Without this they are
// told a number and not what it measures, and the flag that changes it is a
// duration.
func describeScanSpan(from, to time.Time) string {
	if !measurableWindow(from, to) {
		return ""
	}
	return " for " + DescribeSpan(to.Sub(from))
}

// describeScanWidth is describeScanSpan for a sentence that must name a width
// either way.
//
// An unbounded window is spelled out rather than subtracted. Two zero instants
// subtract to a duration that renders as something confident and meaningless, and
// this string appears in the line explaining why a confirmation was assumed — which
// is the last thing a reader has before a long scan they did not agree to.
func describeScanWidth(from, to time.Time) string {
	if !measurableWindow(from, to) {
		return "an unbounded window"
	}
	return DescribeSpan(to.Sub(from))
}

// measurableWindow reports whether these two bounds have a width worth naming.
func measurableWindow(from, to time.Time) bool {
	return !from.IsZero() && !to.IsZero() && to.After(from)
}

// assumedReason says why a confirmation was not asked for, in the words of
// whichever reason applied.
//
// Both readings matter to a different reader. `--yes` is the user's own decision
// played back to them; "not a terminal" tells the person reading a CI log why the
// scan they are watching never paused, which is otherwise the sort of thing that
// gets debugged twice.
func assumedReason(opts ScanOptions) string {
	if opts.AssumeYes {
		return "Confirmed by --" + FlagAssumeYes + "."
	}
	return "Not a terminal, so the confirmation was assumed."
}

// needsConfirmation reports whether this scan is a decision rather than a
// courtesy, which it is for two independent reasons.
//
// The first is width: past ConfirmWindow the cost stops being incidental.
//
// The second is that the width is only a proxy for cost for as long as the listing
// that measures it works. An estimate that failed is not evidence of a small scan;
// it is the absence of evidence. Routing that case through the width threshold — as
// this did — inverted the guard, so the invocation the CLI knew least about was the
// one it asked the least about, with only the opt-in --max-objects left between a
// narrow question and an unbounded scan. An unmeasured scan is therefore a question
// at any width.
//
// That is not extended to an engine which never offered an estimator. Its silence
// is a permanent property of the backend rather than a fact about this scan, and
// prompting before every narrow question against it would train people to stop
// reading the prompt, which is the failure ConfirmWindow itself exists to avoid.
//
// A window with an unbounded end is not measurable and is treated as wide: an
// engine declaring TimeBoundRequired will have had both ends supplied by
// timelineBounds before this is reached, so the case is a caller that skipped that
// step, and guessing "narrow" for a window nobody bounded is the wrong direction
// to guess in.
//
// Nothing further is imposed on an unmeasured scan the user then confirmed: there
// is deliberately no implicit --max-objects ceiling for it. The estimate that would
// have bounded expectations is exactly what is missing, but the user was shown that
// and chose anyway, and a silent ceiling would truncate a scan they consented to.
// It could in any case only apply on the interactive path — the non-interactive one
// never confirms — so the same command would be bounded for a person and unbounded
// in a pipeline, which is a worse surprise than the one it would prevent.
// --max-objects stays explicit, and unchanged.
func needsConfirmation(
	capabilities query.Capabilities, size scanEstimate, from, to time.Time,
) bool {
	if !capabilities.TimeBoundRequired {
		return false
	}
	if size.unmeasured {
		return true
	}
	if from.IsZero() || to.IsZero() {
		return true
	}
	return to.Sub(from) > ConfirmWindow
}

// describeEstimate renders the figures the AC spells out: `~1,240 objects, ~3.1 GiB`.
//
// An unavailable estimate is spelled as such rather than as zero. "0 objects" in
// front of a scan that is about to read four thousand is worse than no number at
// all, because it would be believed.
func describeEstimate(size scanEstimate) string {
	if !size.known {
		return "an unmeasured number of objects"
	}
	return fmt.Sprintf("~%s objects, ~%s",
		formatCount(size.figures.Objects), formatBytes(size.figures.Bytes))
}

// askConfirmation puts the question on stderr and reads the answer.
//
// The prompt goes to stderr with every other diagnostic, because stdout is the
// document: a `-o json` piped into jq must not receive a question, and a user who
// pipes stdout still sees the prompt on their terminal. The reply is read from the
// command's own In, so a test drives it with a buffer.
//
// Anything that is not an explicit yes is a no, including a bare newline and an
// unreadable input. That is what [y/N] promises, and defaulting the other way for
// a scan measured in gigabytes would make the safest keystroke the most expensive
// one.
func askConfirmation(streams genericiooptions.IOStreams, prompt string) (bool, error) {
	if _, err := io.WriteString(streams.ErrOut, prompt); err != nil {
		return false, RuntimeErrorf("asking for confirmation: %w", err)
	}

	line, err := bufio.NewReader(streams.In).ReadString('\n')
	if err != nil && line == "" {
		// EOF with nothing typed is a stdin that has closed, which is a "no" with an
		// explanation rather than an error: the scan simply has nobody to ask.
		if writeErr := writeLine(streams.ErrOut, ""); writeErr != nil {
			return false, writeErr
		}
		return false, nil
	}

	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true, nil
	}
	return false, nil
}

// scanMonitor paints the progress line and trips the circuit breaker.
//
// It is one type rather than two because both are driven by the same callback and
// both are per-scan state; splitting them would mean installing two callbacks on
// an engine whose contract allows one.
type scanMonitor struct {
	out   io.Writer
	paint bool
	total query.ScanEstimate
	known bool
	limit int64

	cancel context.CancelCauseFunc

	// mu guards everything below it. The callback is invoked from the scan's own
	// fetch goroutines (query.ScanProgressReporter), so the throttle, the width of
	// the last line and the one-shot trip all need it.
	mu      sync.Mutex
	lastAt  time.Time
	lastLen int
	tripped bool

	// unpainted is true until the first repaint, so that a scan's first object
	// produces a line immediately instead of after the throttle's first interval.
	// A tool that waits a tenth of a second before admitting it is doing anything
	// is a tool whose quickest questions look like they did nothing at all.
	unpainted bool
}

// report is the callback the engine calls once per fetched object.
//
// The breaker is checked before the throttle, deliberately: a scan must not run
// past its bound because the terminal was busy. The cancellation is fired once —
// a second cancel with a cause is a no-op, but the state also stops the line being
// repainted after the scan has been told to stop.
func (m *scanMonitor) report(progress query.ScanProgress) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.limit > 0 && !m.tripped && progress.Objects > m.limit {
		m.tripped = true
		m.cancel(&scanLimitError{limit: m.limit, scanned: progress.Objects})
		m.erase()
		return
	}
	if !m.paint || m.tripped {
		return
	}
	if now := time.Now(); m.unpainted || now.Sub(m.lastAt) >= progressInterval {
		m.unpainted = false
		m.lastAt = now
		m.write(m.line(progress))
	}
}

// clear removes the progress line, so that whatever the command writes next
// starts on a clean one.
func (m *scanMonitor) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.erase()
}

// line renders one repaint.
//
// The denominator is present only when the estimate is: "412/1,240" and "412" are
// both honest, and "412/0" is not.
func (m *scanMonitor) line(progress query.ScanProgress) string {
	scanned := formatCount(progress.Objects)
	if m.known {
		scanned += "/" + formatCount(m.total.Objects)
	}
	return fmt.Sprintf("scanning %s objects, %s read", scanned, formatBytes(progress.Bytes))
}

// write repaints the line in place, padding over whatever was longer before it.
//
// Carriage returns rather than ANSI erasure so that the narration of a scan is
// readable on anything, including the terminals where colour is off. The write
// error is deliberately dropped: a failure here means stderr has gone, and a scan
// must not be abandoned because its narration could not be printed.
func (m *scanMonitor) write(line string) {
	padding := ""
	if pad := m.lastLen - len(line); pad > 0 {
		padding = strings.Repeat(" ", pad)
	}
	_, _ = io.WriteString(m.out, "\r"+line+padding+"\r")
	m.lastLen = len(line)
}

// erase blanks the line the monitor last painted, and only that.
//
// It must be idempotent: stop() calls clear, and a tripped breaker has already
// erased. Tracking the width is what makes this possible without clearing a line
// the monitor never wrote.
func (m *scanMonitor) erase() {
	if m.lastLen == 0 {
		return
	}
	_, _ = io.WriteString(m.out, "\r"+strings.Repeat(" ", m.lastLen)+"\r")
	m.lastLen = 0
}

// formatCount renders a count with thousands separators.
//
// The separators are not decoration. The figure exists so that a person can tell
// 1,240 from 12,400 at a glance, in the second before they answer a yes/no
// question, and unseparated digits are exactly what defeats that.
func formatCount(n int64) string {
	digits := strconv.FormatInt(n, 10)
	sign := ""
	if strings.HasPrefix(digits, "-") {
		sign, digits = "-", digits[1:]
	}

	var out strings.Builder
	for i, digit := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			out.WriteByte(',')
		}
		out.WriteRune(digit)
	}
	return sign + out.String()
}

// byteUnits are binary units, because that is what an object store reports and
// what a person comparing this figure with their bucket's console will see.
var byteUnits = []struct {
	suffix string
	scale  float64
}{
	{"TiB", 1 << 40},
	{"GiB", 1 << 30},
	{"MiB", 1 << 20},
	{"KiB", 1 << 10},
}

// formatBytes renders a size the way the estimate is meant to be read: two
// significant figures and a binary unit, never the raw count.
//
// One decimal place, because the number is being read as a magnitude — "3.1 GiB"
// answers "can I wait for this" and "3,328,599,655 bytes" does not.
func formatBytes(n int64) string {
	if n < 0 {
		return "0 B"
	}
	for _, unit := range byteUnits {
		if float64(n) >= unit.scale {
			return fmt.Sprintf("%.1f %s", float64(n)/unit.scale, unit.suffix)
		}
	}
	return fmt.Sprintf("%d B", n)
}

// DescribeSpan renders a duration the way the window flags accept one, so that a
// message about a window can be pasted back in as --since.
//
// Days and weeks rather than Go's hours-and-minutes, for the same reason
// parseWindowDuration accepts them: "2160h0m0s" is a correct rendering of ninety
// days that nobody reads as ninety days.
func DescribeSpan(d time.Duration) string {
	switch {
	case d <= 0:
		return "0s"
	case d%(7*24*time.Hour) == 0:
		return strconv.FormatInt(int64(d/(7*24*time.Hour)), 10) + "w"
	case d%(24*time.Hour) == 0:
		return strconv.FormatInt(int64(d/(24*time.Hour)), 10) + "d"
	case d%time.Hour == 0:
		return strconv.FormatInt(int64(d/time.Hour), 10) + "h"
	}
	return d.String()
}
