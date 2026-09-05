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
	"errors"
	"fmt"
	"net"
	"strings"
	"syscall"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The first failure a new user meets, and the one thing in this package that is
// prose before it is logic.
//
// A ClickHouseSink installed in a cluster records a Kubernetes Service name —
// `clickhouse.kuberecord-quickstart.svc:9000` is what examples/quickstart writes,
// and what any in-cluster deployment records, because it is the address the
// operator itself dials. Run the CLI from a laptop and the net package answers
// `no such host`, which is true, unhelpful, and indistinguishable from a typo.
//
// Nothing here is broken. Discovery worked, the custom resource says exactly what
// it should, and the address is correct for every reader that runs inside the
// cluster. What is missing is a sentence, and this file is that sentence: the CLI
// already holds the address, the database, the user, the sink's own name and the
// namespace its credentials live in, and it can spell out both routes out of the
// problem without the reader opening a document.
//
// # Why it is its own file
//
// The message is documentation that happens to be compiled. It will be edited by
// people improving the wording, who are not editing dial logic, and a paragraph
// living in the middle of resolve.go would be edited by neither.
//
// # Why the classification is narrow
//
// A wrongly-fired diagnostic is worse than no diagnostic. Telling the on-call
// engineer of a production ClickHouse that has fallen over to run
// `kubectl port-forward` sends them somewhere the fault is not, at the moment
// they can least afford it. So both halves must hold: the address has to be one
// that only resolves inside a cluster, *and* the failure has to be one of the two
// a laptop-versus-cluster-DNS mismatch actually produces. A cluster-internal
// address that times out is a network path problem; a public address that
// reports `no such host` is a typo. Neither is this, and neither gets this.
//
// # It is a diagnostic, not a fallback
//
// Nothing here dials, retries, substitutes an address or tries 127.0.0.1 (D24,
// Invariant 5). It names the fix; the user performs it. A CLI that quietly
// connected somewhere other than where it was told could not be an audit tool,
// because every answer it gave would carry an unstated "…from somewhere".

// DefaultClickHousePort is the native-protocol port a ClickHouse address is
// assumed to carry when it names none.
//
// It is used only to fill in the remediation commands. Nothing dials it: an
// address with no port is already a configuration this package would rather
// describe than repair.
const DefaultClickHousePort = "9000"

// The values the remediation commands are written with.
//
// The profile name and the environment variable are suggestions rather than
// contracts, and they are named here so that the two commands the message prints
// — the one that writes the profile and the one that activates it — cannot drift
// apart from each other or from docs/CLI.md, which uses the same variable.
const (
	localProfileName = "local"
	passwordEnvName  = "KUBERECORD_CLICKHOUSE_PASSWORD"
	loopbackHost     = "127.0.0.1"
)

// The two sections of docs/CLI.md a rendered message can send a reader to.
//
// Each names a section rather than the page. The page is a command reference
// several hundred lines long, and a reader who has just been told "see
// docs/CLI.md" by a failure has been handed a search, not an answer.
//
// They are two rather than one because the messages ask two different things of
// the reader. docsOutsideCluster is for somebody stuck: why the address is a
// Service name and why that is right, both routes out of it, and why this tool
// will not forward the port itself. docsReadOnlyUser is for somebody who is not
// stuck at all — `--from-sink` prints the credential advice after writing a
// profile for a perfectly public endpoint too, and pointing that reader at a
// port-forward section would be a non-sequitur.
//
// Both are printed on a line of their own and unpunctuated, for the same reason
// the address gets one: they are tokens to be copied rather than prose to be
// read, and a trailing full stop is a character that travels with them.
const (
	docsOutsideCluster = "docs/CLI.md#running-the-cli-outside-the-cluster"
	docsReadOnlyUser   = "docs/CLI.md#the-read-only-clickhouse-user"
)

// clusterInternalSuffixes are the DNS suffixes that only resolve inside a
// cluster.
//
// `.svc.cluster.local` is the fully qualified form, `.svc` is what the resolver's
// search path makes idiomatic and what the CRD's examples use, and
// `.cluster.local` on its own covers the pod and headless-endpoint spellings. The
// third subsumes the first; both are listed because a reader of this list should
// see the forms they will actually meet rather than have to work out which
// suffixes the shortest one happens to cover.
var clusterInternalSuffixes = []string{".svc", ".svc.cluster.local", ".cluster.local"}

// ClusterInternalAddr reports whether addr names a host that resolves inside a
// Kubernetes cluster and nowhere else.
//
// It is exported because it is the same question `config set-profile --from-sink`
// has to answer before deciding whether the address it read from a custom
// resource can be written into a profile unchanged (Task 13.3). One classifier,
// so that the command that rewrites an address and the message that explains why
// it needed rewriting can never disagree.
//
// The port is ignored: a Service name is a Service name whichever port is behind
// it. An IP literal is never cluster-internal — a pod IP is unroutable from a
// laptop too, but it is not a *name*, so port-forwarding a Service is not the
// advice its failure calls for. `localhost` is excluded explicitly, because it is
// the one single-label host in the world that resolves everywhere.
func ClusterInternalAddr(addr string) bool {
	host, _ := splitAddr(addr)
	if host == "" || host == "localhost" {
		return false
	}
	if net.ParseIP(host) != nil {
		return false
	}
	for _, suffix := range clusterInternalSuffixes {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	// A bare single-label host is resolved through the pod's search path, which
	// exists only inside a namespace. It is the shortest spelling of the same
	// fact the suffixes above state.
	return !strings.Contains(host, ".")
}

// splitAddr separates a ClickHouse address into a comparable host and a port.
//
// It is deliberately total: an address with no port, a bracketed IPv6 literal and
// a fully qualified name with a trailing dot all have to produce something, because
// the caller is already on a failure path and a parse error here would replace one
// unhelpful message with another. The host is lower-cased because DNS names are
// case-insensitive and the suffix comparisons above are not.
func splitAddr(addr string) (host, port string) {
	trimmed := strings.TrimSpace(addr)
	host, port, err := net.SplitHostPort(trimmed)
	if err != nil {
		host, port = trimmed, ""
	}
	host = strings.TrimSuffix(strings.Trim(host, "[]"), ".")
	if port == "" {
		port = DefaultClickHousePort
	}
	return strings.ToLower(host), port
}

// unreachableFromOutside reports whether err is one of the two failures a
// laptop-versus-cluster-DNS mismatch produces.
//
// The first is the ordinary one: the name does not resolve, which arrives as a
// *net.DNSError with IsNotFound set. The second is the port-forward that died, or
// was never started, on a name that *did* resolve — ECONNREFUSED, reached with
// errors.As through the *net.OpError and *os.SyscallError the net package wraps
// it in.
//
// Everything else passes through untouched, and the list of what "everything
// else" contains is the point of this function rather than an afterthought: a
// timeout is a network path or a server too busy to accept; a TLS failure is a
// certificate; an authentication rejection and a ClickHouse protocol error are
// the server answering. All four mean the address was reachable enough to fail
// later, and none of them is fixed by forwarding a port.
//
// Matching is structural rather than textual. `strings.Contains(err.Error(),
// "no such host")` would work today, agree with a translated libc tomorrow, and
// misfire on a ClickHouse error message that happened to quote a hostname.
//
// One platform note, stated rather than hidden: on Windows a refused connection
// arrives as WSAECONNREFUSED, which is not syscall.ECONNREFUSED, so the second
// half of this test does not fire there. The first half — the DNS failure, which
// is the case the quickstart produces and the reason this file exists — is
// platform-independent and does.
func unreachableFromOutside(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return true
	}
	var errno syscall.Errno
	return errors.As(err, &errno) && errno == syscall.ECONNREFUSED
}

// diagnosis is everything the message needs, gathered where it is already known.
//
// It is built in clickHouseTarget, which has just read the custom resource and
// resolved the namespace its Secret lives in, and it is carried on the target
// rather than fetched again later. That ordering is what makes the message
// specific: by the time a query fails, the code that fails knows an address and
// nothing about which sink it came from.
//
// It holds no credential. The password is resolved a few lines away in
// clickHouseTarget and deliberately does not travel here — a struct that carried
// it would be one refactor away from rendering it.
type diagnosis struct {
	// ref is the sink the address came from, for the first sentence.
	ref SinkRef

	// namespace is where the sink's credentials Secret lives, which is the
	// operator's own namespace unless the reference named one. It is the
	// fallback for `port-forward -n` when the address is a bare single-label
	// host and therefore carries no namespace of its own.
	namespace string

	// The three connection values the remediation commands are pre-filled from.
	// A user who has to retype them is a user who mistypes one.
	addr     string
	database string
	username string

	// commandName is how this process was invoked — `kuberecord` or
	// `kubectl kuberecord` — so that the `config` commands name something the
	// reader can actually type.
	commandName string
}

// armed reports whether this diagnosis is about an address the message applies to.
//
// It is the first of the two required conditions, and it is checked before the
// engine is wrapped at all so that a public endpoint gets exactly the code path it
// got before this file existed.
func (d diagnosis) armed() bool {
	return d.addr != "" && ClusterInternalAddr(d.addr)
}

// wrap annotates err when both conditions hold, and returns it untouched otherwise.
//
// The original is wrapped rather than replaced. errors.Is and errors.As still
// reach the *net.DNSError underneath, which is what a `-v` user needs to see and
// what a maintainer reading a bug report needs in order to believe the rest of it.
func (d diagnosis) wrap(err error) error {
	if err == nil || !d.armed() || !unreachableFromOutside(err) {
		return err
	}
	return &UnreachableSinkError{diagnosis: d, cause: err}
}

// watch returns engine with its errors passed through this diagnosis.
//
// # Why a wrapper and not a check at the dial
//
// chquery.Dial performs no round trip — clickhouse.Open validates options and
// assembles a pool, and the first connection is made when the first query runs.
// That is a decision the read plane defends in its own doc comment, and it is the
// right one: a CLI that dialled eagerly would pay a connection on paths that never
// query and would report an unreachable host before it had said which host it had
// chosen and why. The consequence for this file is that the failure it exists to
// explain does not surface during resolution at all. It surfaces from Timeline,
// Coverage, StateAt or Incarnations, several layers above, where nothing knows
// which sink the address came from.
//
// So the diagnosis travels with the engine. The wrapper performs no I/O, holds no
// state, and never retries: it forwards each call once and hands the returned
// error to wrap. An engine over a public address is not wrapped at all.
//
// # Why two types
//
// A decorator over an interface with optional halves has to preserve exactly the
// optional halves its subject implements, or a caller's type assertion starts
// answering a different question. query.ClusterIDLister is the one the shipped
// ClickHouse engine satisfies and the one resolveClusterID's last step asks for;
// query.ScanEstimator and query.ScanProgressReporter belong to the archive
// backend and are absent here, so a wrapper must not advertise them either.
// Wrapping conditionally is how that stays true without a type per combination.
func (d diagnosis) watch(engine query.QueryEngine) query.QueryEngine {
	if !d.armed() {
		return engine
	}
	watched := &diagnosingEngine{QueryEngine: engine, diagnosis: d}
	if lister, ok := engine.(query.ClusterIDLister); ok {
		return &diagnosingClusterLister{diagnosingEngine: watched, lister: lister}
	}
	return watched
}

// diagnosingEngine forwards the read-plane contract and annotates what comes back.
//
// The contract is embedded rather than restated so that Capabilities and Close —
// which cannot fail in a way this file has anything to say about — reach the real
// engine unmodified, and so that a method added to query.QueryEngine later is
// forwarded rather than silently missing.
type diagnosingEngine struct {
	query.QueryEngine
	diagnosis diagnosis
}

// The four methods that reach the network, each forwarded once and each handed
// its error. They are spelled out rather than generated because there are four of
// them and the alternative — reflection, or a generic helper over a call — would
// cost more to read than it saves to write.

func (e *diagnosingEngine) Timeline(
	ctx context.Context, q query.TimelineQuery,
) (query.ChangeIterator, error) {
	iterator, err := e.QueryEngine.Timeline(ctx, q)
	return iterator, e.diagnosis.wrap(err)
}

func (e *diagnosingEngine) StateAt(
	ctx context.Context, ref query.ObjectRef, at time.Time, uid string,
) (*query.Reconstruction, error) {
	reconstruction, err := e.QueryEngine.StateAt(ctx, ref, at, uid)
	return reconstruction, e.diagnosis.wrap(err)
}

func (e *diagnosingEngine) Coverage(
	ctx context.Context, q query.ScopeQuery,
) ([]query.ScopeInterval, error) {
	intervals, err := e.QueryEngine.Coverage(ctx, q)
	return intervals, e.diagnosis.wrap(err)
}

func (e *diagnosingEngine) Incarnations(
	ctx context.Context, ref query.ObjectRef, from, to time.Time,
) ([]query.Incarnation, error) {
	incarnations, err := e.QueryEngine.Incarnations(ctx, ref, from, to)
	return incarnations, e.diagnosis.wrap(err)
}

// diagnosingClusterLister adds back the one optional half the ClickHouse engine
// implements.
//
// It matters more than it looks. Cluster identity's last resort is to ask the
// backend which clusters it holds, and on a machine that cannot reach the backend
// that is frequently the first call made — so it is the call most likely to be the
// one that fails, and a wrapper that hid it would hide the diagnostic on the most
// common path to it.
type diagnosingClusterLister struct {
	*diagnosingEngine
	lister query.ClusterIDLister
}

func (e *diagnosingClusterLister) ClusterIDs(ctx context.Context) ([]string, error) {
	ids, err := e.lister.ClusterIDs(ctx)
	return ids, e.diagnosis.wrap(err)
}

// UnreachableSinkError is a backend that could not be reached because the CLI is
// outside the cluster the address belongs to.
//
// It is an ordinary error on every path that handles errors: it wraps its cause,
// carries no exit code of its own — so the process still ends with exit 1, exactly
// as an undiagnosed dial failure did — and prints a single line through %v. What
// it adds is Render, which the top of the CLI calls to put the two routes out of
// the problem underneath that line.
//
// The split between Error and Render is deliberate. A multi-line message returned
// from Error would be spliced into every caller that wraps it with context —
// "reading the timeline of deploy/checkout: <four paragraphs>: no such host" — and
// would arrive without colour, since an error string cannot know where it is going
// to be printed.
type UnreachableSinkError struct {
	diagnosis diagnosis
	cause     error
}

// Error is the one-line summary, naming the sink, the address and the real cause.
func (e *UnreachableSinkError) Error() string {
	return fmt.Sprintf("cannot reach %s at %s: %v", e.diagnosis.ref, e.diagnosis.addr, e.cause)
}

// Unwrap keeps the original failure reachable, so that diagnosing a failure never
// costs a caller the ability to classify it.
func (e *UnreachableSinkError) Unwrap() error { return e.cause }

// Render writes the explanation and the two routes out of it.
//
// commandPath is the command the user actually ran, as cobra spells it — the
// caller at the top of the CLI knows which command failed and this package does
// not. An empty one falls back to the program's own name, which is still correct
// prose and merely less specific.
//
// colorize is decided by the caller from --color, NO_COLOR and whether stderr is a
// terminal, for the same reason render.Options takes it as an argument: a function
// that consulted the environment itself would have golden files that changed with
// the shell they were generated in.
func (e *UnreachableSinkError) Render(commandPath string, colorize bool) string {
	paint := diagnosticPalette{enabled: colorize}
	d := e.diagnosis

	service, namespace, port := d.serviceTarget()
	invocation := commandPath
	if invocation == "" {
		invocation = d.commandName
	}
	forwarded := net.JoinHostPort(loopbackHost, port)

	var out strings.Builder
	line := func(text string) { out.WriteString(text + "\n") }

	// The address gets a line to itself, unwrapped, because it is the one string
	// in this message a reader may need to compare character by character with
	// what they have in a manifest.
	line(fmt.Sprintf("%s records the address %s.", d.ref, d.addr))
	line("")
	line(paint.dim("That name resolves inside the cluster and nowhere else, so discovery was right and so is"))
	line(paint.dim("the sink: this machine is simply outside it. kuberecord reads a cluster and never acts on"))
	line(paint.dim("one, so it will not forward a port for you."))
	line("")
	line(paint.dim("Forward it yourself, then re-run against the forwarded address:"))
	line("")
	line(paint.bold(fmt.Sprintf("    kubectl port-forward -n %s svc/%s %s:%s",
		namespace, service, port, port)))
	line(paint.bold(fmt.Sprintf("    %s … --%s %s", invocation, options.FlagSinkAddr, forwarded)))
	line("")
	line(paint.dim("Or write it down once, and every later invocation reads it:"))
	line("")
	line(paint.bold(fmt.Sprintf("    %s config set-profile %s --backend %s \\",
		d.commandName, localProfileName, BackendClickHouse)))
	line(paint.bold(fmt.Sprintf("        --addr %s --database %s --username %s \\",
		forwarded, d.database, d.username)))
	line(paint.bold(fmt.Sprintf("        --password-env %s", passwordEnvName)))
	line(paint.bold(fmt.Sprintf("    %s config use-profile %s", d.commandName, localProfileName)))
	line("")
	line(paint.dim(fmt.Sprintf("Export %s first. A read-only ClickHouse user is the", passwordEnvName)))
	line(paint.dim("recommended credential for it, and the operator's own is not. Both routes, and"))
	line(paint.dim("why this tool will not forward the port for you:"))
	line(paint.dim(docsOutsideCluster))

	return out.String()
}

// serviceTarget works out what `kubectl port-forward` has to be told.
//
// The service name and its namespace are read out of the address itself, because
// the address is the only place they are both stated: a ClickHouseSink is
// cluster-scoped (D6) and names no namespace of its own, and the Service it points
// at may be anywhere. `clickhouse.kuberecord-quickstart.svc:9000` therefore yields
// `svc/clickhouse -n kuberecord-quickstart`, which is exactly the command the
// quickstart's user needs.
//
// A bare single-label host states no namespace, and there the fallback is the one
// the operator itself would have resolved the name in — the namespace its Secret
// was read from. That is a guess, and it is the same guess the cluster makes.
func (d diagnosis) serviceTarget() (service, namespace, port string) {
	host, port := splitAddr(d.addr)

	name := strings.TrimSuffix(host, ".cluster.local")
	name = strings.TrimSuffix(name, ".svc")
	if service, fromAddr, found := strings.Cut(name, "."); found {
		return service, fromAddr, port
	}
	return name, d.namespace, port
}

// diagnosticPalette paints a line of the message, or does not.
//
// It carries two sequences and no more. The message is prose with commands in it,
// so the whole of what colour has to convey is which lines are meant to be typed;
// anything richer would be decoration on a page somebody is reading because
// something went wrong. The disabled palette returns its argument unchanged, so
// every call site reads the same whether colour is on or off — and so a call site
// that forgot to check cannot exist.
//
// The sequences are written out rather than taken from a dependency for the reason
// render's own are: a colour library brings a package-level enabled flag and a
// global writer, and this package deliberately has neither.
type diagnosticPalette struct{ enabled bool }

const (
	ansiReset = "\x1b[0m"
	ansiBold  = "\x1b[1m"
	ansiDim   = "\x1b[2m"
)

func (p diagnosticPalette) paint(sequence, text string) string {
	if !p.enabled || text == "" {
		return text
	}
	return sequence + text + ansiReset
}

func (p diagnosticPalette) bold(text string) string { return p.paint(ansiBold, text) }
func (p diagnosticPalette) dim(text string) string  { return p.paint(ansiDim, text) }
