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
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The diagnostic is tested from inside the package because the two things worth
// asserting about it are unexported by design: the classification, which is a
// private decision this package makes about an error it did not create, and the
// engine wrapper, which exists precisely so that no caller has to know it is
// there.

// updateDiagnoseGolden rewrites the rendered message instead of comparing to it.
//
//	go test ./internal/cli/resolve/ -run Message -update
var updateDiagnoseGolden = flag.Bool("update", false, "rewrite the golden files")

// The fixture is the quickstart, because the quickstart is the case this whole
// file exists for: examples/quickstart/sink.yaml records exactly this address, and
// a message that reads well for it reads well for every in-cluster install.
const (
	fixtureAddr      = "clickhouse.kuberecord-quickstart.svc:9000"
	fixtureNamespace = "kuberecord-system"

	// fixturePassword is the value that must never appear in the rendered
	// message. It is distinctive so that a test asserting its absence is
	// asserting something a substring search can actually find.
	fixturePassword = "correct-horse-battery-staple"
)

// fixtureDiagnosis is the quickstart's sink as clickHouseTarget would have
// described it.
func fixtureDiagnosis() diagnosis {
	return diagnosis{
		ref:         SinkRef{Kind: KindClickHouseSink, Name: "default"},
		namespace:   fixtureNamespace,
		addr:        fixtureAddr,
		database:    DefaultClickHouseDatabase,
		username:    "kuberecord",
		commandName: "kuberecord",
	}
}

// dnsNotFound is the failure a laptop gets for a Service name, in the shape the
// net package actually produces it: the DNSError is inside an OpError, which is
// why the classification has to traverse rather than type-switch.
func dnsNotFound(host string) error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{
		Err: "no such host", Name: host, IsNotFound: true,
	}}
}

// connectionRefused is the failure a laptop gets when the port-forward died, or
// was never started, on a name that did resolve.
func connectionRefused() error {
	return &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9000},
		Err:  os.NewSyscallError("connect", syscall.ECONNREFUSED),
	}
}

// TestClusterInternalAddrRecognisesEachForm covers every spelling of a name that
// resolves inside a cluster, and every near miss.
//
// The negatives are the half that matters. A classifier that answered "yes" to a
// public FQDN would send the on-call engineer of a ClickHouse that has genuinely
// fallen over to go and port-forward something, at the moment they can least
// afford the detour.
func TestClusterInternalAddrRecognisesEachForm(t *testing.T) {
	for _, tc := range []struct {
		name string
		addr string
		want bool
	}{
		{"a .svc name with a port", "clickhouse.kuberecord-quickstart.svc:9000", true},
		{"a .svc name without one", "clickhouse.kuberecord-quickstart.svc", true},
		{"the fully qualified form", "clickhouse.kuberecord-quickstart.svc.cluster.local:9000", true},
		{"the cluster domain alone", "clickhouse.cluster.local:9000", true},
		{"a bare single-label host", "clickhouse:9000", true},
		{"a bare single-label host with no port", "clickhouse", true},
		{"a trailing dot, which DNS allows", "clickhouse.kuberecord-quickstart.svc.:9000", true},
		{"upper case, which DNS ignores", "ClickHouse.Kuberecord-Quickstart.SVC:9000", true},

		{"a public FQDN", "clickhouse.example.com:9000", false},
		{"localhost, the one single-label host that resolves everywhere", "localhost:9000", false},
		{"localhost with no port", "localhost", false},
		{"an IPv4 literal", "127.0.0.1:9000", false},
		{"a routable IPv4 literal", "10.4.2.9:9000", false},
		{"an IPv6 literal", "[::1]:9000", false},
		{"an IPv6 literal with no port", "::1", false},
		{"an unbracketed IPv6 literal", "[fd00::1]:9000", false},
		{"nothing at all", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClusterInternalAddr(tc.addr); got != tc.want {
				t.Errorf("ClusterInternalAddr(%q) = %v, want %v", tc.addr, got, tc.want)
			}
		})
	}
}

// TestOnlyTheTwoFailuresOfThisMismatchAreDiagnosed pins the error half.
//
// Each negative is named rather than covered by a single "anything else" case,
// because each one is a different wrong answer this message could give: a timeout
// is a network path, a TLS failure is a certificate, and an authentication
// rejection or a protocol error is the server answering — all three mean the
// address was reachable enough to fail later, and none of them is fixed by
// forwarding a port.
func TestOnlyTheTwoFailuresOfThisMismatchAreDiagnosed(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"a name that does not resolve", dnsNotFound("clickhouse.kuberecord-quickstart.svc"), true},
		{"a refused connection", connectionRefused(), true},
		{"either of them, wrapped for context", fmt.Errorf("asking the backend: %w",
			dnsNotFound("clickhouse.kuberecord-quickstart.svc")), true},

		{"a DNS timeout", &net.OpError{Op: "dial", Err: &net.DNSError{
			Err: "i/o timeout", Name: "clickhouse.kuberecord-quickstart.svc", IsTimeout: true,
		}}, false},
		{"a temporary DNS failure", &net.OpError{Op: "dial", Err: &net.DNSError{
			Err: "server misbehaving", Name: "clickhouse.example.com", IsTemporary: true,
		}}, false},
		{"a dial timeout", &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}, false},
		{"a TLS verification failure", errors.New(
			"tls: failed to verify certificate: x509: certificate signed by unknown authority"), false},
		{"an authentication rejection", errors.New(
			"code: 516, message: kuberecord: Authentication failed: password is incorrect"), false},
		{"a ClickHouse protocol error", errors.New(
			"clickhouse: unexpected packet [7] from server"), false},
		{"a reset connection, which is not a refused one", &net.OpError{
			Op: "read", Err: os.NewSyscallError("read", syscall.ECONNRESET)}, false},
		{"a host that is unreachable rather than absent", &net.OpError{
			Op: "dial", Err: os.NewSyscallError("connect", syscall.EHOSTUNREACH)}, false},
		{"nothing", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := unreachableFromOutside(tc.err); got != tc.want {
				t.Errorf("unreachableFromOutside(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestBothConditionsAreRequired is the cross product, and the reason the two
// classifiers are separate functions rather than one.
//
// Three of the four cells are somebody else's problem: a cluster-internal address
// that times out is a network path, a public address that does not resolve is a
// typo, and a public address that answers slowly is a busy server. Only the fourth
// is the mismatch this file explains.
func TestBothConditionsAreRequired(t *testing.T) {
	internal := fixtureDiagnosis()
	public := fixtureDiagnosis()
	public.addr = "clickhouse.example.com:9000"

	timeout := &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
	notFound := dnsNotFound("clickhouse.kuberecord-quickstart.svc")

	for _, tc := range []struct {
		name      string
		diagnosis diagnosis
		err       error
		diagnosed bool
	}{
		{"a cluster-internal address that does not resolve", internal, notFound, true},
		{"a cluster-internal address that times out", internal, timeout, false},
		{"a public address that does not resolve", public, notFound, false},
		{"a public address that times out", public, timeout, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var unreachable *UnreachableSinkError
			got := errors.As(tc.diagnosis.wrap(tc.err), &unreachable)
			if got != tc.diagnosed {
				t.Errorf("diagnosed = %v, want %v", got, tc.diagnosed)
			}
			if !tc.diagnosed && !errors.Is(tc.diagnosis.wrap(tc.err), tc.err) {
				t.Error("an undiagnosed failure was not returned unchanged")
			}
		})
	}
}

// TestTheUnderlyingErrorSurvivesTheDiagnosis.
//
// The message is an addition, never a replacement. A user running with -v and a
// maintainer reading a bug report both need the real cause, and a diagnostic that
// swallowed it would trade a specific failure for a friendly one.
func TestTheUnderlyingErrorSurvivesTheDiagnosis(t *testing.T) {
	cause := dnsNotFound("clickhouse.kuberecord-quickstart.svc")
	diagnosed := fixtureDiagnosis().wrap(cause)

	if !errors.Is(diagnosed, cause) {
		t.Error("errors.Is no longer reaches the original failure")
	}
	var dnsErr *net.DNSError
	if !errors.As(diagnosed, &dnsErr) || !dnsErr.IsNotFound {
		t.Error("errors.As no longer reaches the *net.DNSError underneath")
	}
	if summary := diagnosed.Error(); !strings.Contains(summary, "no such host") {
		t.Errorf("the one-line summary hides the real cause: %s", summary)
	}
	for _, want := range []string{KindClickHouseSink + "/default", fixtureAddr} {
		if !strings.Contains(diagnosed.Error(), want) {
			t.Errorf("the one-line summary does not name %q: %s", want, diagnosed.Error())
		}
	}
}

// TestTheExitCodeIsUnchanged.
//
// Diagnosing a dial failure must not reclassify it. An undiagnosed one exits 1 —
// a well-formed request that could not be carried out — and a wrapper script that
// retries on 1 and stops on 2 must keep behaving identically whether or not the
// address happened to be a Service name.
func TestTheExitCodeIsUnchanged(t *testing.T) {
	cause := dnsNotFound("clickhouse.kuberecord-quickstart.svc")
	plain := exit.CodeFor(exit.RuntimeErrorf("%w", cause))
	diagnosed := exit.CodeFor(exit.RuntimeErrorf("%w", fixtureDiagnosis().wrap(cause)))

	if diagnosed != plain {
		t.Errorf("a diagnosed dial failure exits %d, an undiagnosed one exits %d", diagnosed, plain)
	}
	if diagnosed != exit.RuntimeError {
		t.Errorf("a dial failure exits %d, want %d", diagnosed, exit.RuntimeError)
	}
}

// TestTheMessageCarriesNoCredential.
//
// This drives the real clickHouseTarget rather than a hand-built diagnosis, which
// is the whole point of it: the password is resolved four lines from where the
// diagnosis is assembled, so it is exactly the value a later edit would sweep into
// the struct without noticing. A test over a fixture the test itself wrote could
// not catch that.
//
// The username may appear — a profile needs it and it is not a secret. The
// password may not, on any stream, at any verbosity.
func TestTheMessageCarriesNoCredential(t *testing.T) {
	const secretName = "clickhouse-credentials"

	resolver := &BackendResolver{
		InvokedAs: options.StandaloneName,
		Config:    &Config{OperatorNamespace: fixtureNamespace},
		Clients: &Clients{Typed: k8sfake.NewClientset(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: fixtureNamespace},
			Data:       map[string][]byte{secretKeyPassword: []byte(fixturePassword)},
		})},
	}
	ref := SinkRef{Kind: KindClickHouseSink, Name: "default"}
	sink := &v1alpha1.ClickHouseSink{Spec: v1alpha1.ClickHouseSinkSpec{
		Connection: v1alpha1.ConnectionSpec{
			Addr:                 fixtureAddr,
			Database:             DefaultClickHouseDatabase,
			Username:             "kuberecord",
			CredentialsSecretRef: v1alpha1.SecretReference{Name: secretName},
		},
	}}

	chosen, err := resolver.clickHouseTarget(t.Context(), ref, sink)
	if err != nil {
		t.Fatalf("resolving the sink: %v", err)
	}
	// Non-vacuity: the password really was read, so its absence below is the
	// diagnosis declining to carry it rather than the fixture never having one.
	if chosen.clickhouse.Password != fixturePassword {
		t.Fatalf("the fixture Secret was not resolved, so this test would pass over nothing")
	}

	rendered := (&UnreachableSinkError{
		diagnosis: chosen.diagnosis,
		cause:     dnsNotFound("clickhouse.kuberecord-quickstart.svc"),
	}).Render("kuberecord timeline", false)

	for name, written := range map[string]string{
		"the rendered message":     rendered,
		"the notice's description": chosen.description,
	} {
		if strings.Contains(written, fixturePassword) {
			t.Errorf("%s quotes the credential read from the Secret:\n%s", name, written)
		}
	}
	if strings.Contains(rendered, "--password ") || strings.Contains(rendered, "--password=") {
		t.Errorf("the message offers to put a password on a command line:\n%s", rendered)
	}
	if !strings.Contains(rendered, passwordEnvName) {
		t.Error("the profile route does not say where the password comes from")
	}
	if !strings.Contains(rendered, "--username kuberecord") {
		t.Error("the profile route omits the username discovery found, which a user would have to guess")
	}
}

// TestTheMessageNamesBothRoutes, pre-filled from what discovery already found.
//
// Asserted by content as well as by golden file, because a golden file
// regenerated after a regression keeps passing: these are the values whose
// presence is the point, and a message that stopped parsing the namespace out of
// the address would still render a perfectly well-formed page of prose.
func TestTheMessageNamesBothRoutes(t *testing.T) {
	rendered := (&UnreachableSinkError{
		diagnosis: fixtureDiagnosis(),
		cause:     dnsNotFound("clickhouse.kuberecord-quickstart.svc"),
	}).Render("kuberecord timeline", false)

	for _, want := range []string{
		// The port-forward route: the service and namespace parsed out of the
		// address, and the port carried over.
		"kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000",
		"kuberecord timeline … --sink-addr 127.0.0.1:9000",
		// The profile route, complete enough to paste.
		"kuberecord config set-profile local --backend clickhouse",
		"--addr 127.0.0.1:9000 --database kuberecord --username kuberecord",
		"kuberecord config use-profile local",
		// And the sentence that says the tool will not do it for the user (D23).
		"will not forward a port",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the message does not carry %q:\n%s", want, rendered)
		}
	}
}

// TestTheMessageNamesTheInvocationTheUserTyped.
//
// The command path comes from cobra at the top of the CLI, so a plugin user is
// told to run `kubectl kuberecord …` rather than a binary they may not have. An
// empty path is still correct prose, just less specific.
func TestTheMessageNamesTheInvocationTheUserTyped(t *testing.T) {
	plugin := fixtureDiagnosis()
	plugin.commandName = "kubectl kuberecord"
	failure := &UnreachableSinkError{diagnosis: plugin, cause: connectionRefused()}

	rendered := failure.Render("kubectl kuberecord blame", false)
	for _, want := range []string{
		"kubectl kuberecord blame … --sink-addr",
		"kubectl kuberecord config set-profile local",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the message does not carry %q:\n%s", want, rendered)
		}
	}

	if fallback := failure.Render("", false); !strings.Contains(fallback, "kubectl kuberecord … --sink-addr") {
		t.Errorf("with no command path the message does not fall back to the program name:\n%s", fallback)
	}
}

// TestABareHostFallsBackToTheOperatorNamespace.
//
// A single-label address states no namespace, and the namespace the port-forward
// needs has to come from somewhere. The operator's own is the same one the
// cluster's search path would have resolved the name in, which makes it the
// truthful guess rather than a convenient one.
func TestABareHostFallsBackToTheOperatorNamespace(t *testing.T) {
	bare := fixtureDiagnosis()
	bare.addr = "clickhouse:9440"

	rendered := (&UnreachableSinkError{diagnosis: bare, cause: connectionRefused()}).Render("", false)
	for _, want := range []string{
		"kubectl port-forward -n " + fixtureNamespace + " svc/clickhouse 9440:9440",
		"--addr 127.0.0.1:9440",
	} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the message does not carry %q:\n%s", want, rendered)
		}
	}
}

// TestTheMessageRendersInBothColourModes, against golden files.
//
// A golden file rather than assembled expectations because the thing under test
// is a page of prose whose line breaks, indentation and emphasis all matter at
// once — and because the whole point of the file is that it is edited by people
// improving the wording, who should see the diff a reader would have seen.
func TestTheMessageRendersInBothColourModes(t *testing.T) {
	failure := &UnreachableSinkError{
		diagnosis: fixtureDiagnosis(),
		cause:     dnsNotFound("clickhouse.kuberecord-quickstart.svc"),
	}

	for name, colorize := range map[string]bool{"plain": false, "color": true} {
		t.Run(name, func(t *testing.T) {
			assertDiagnoseGolden(t, name, failure.Render("kuberecord timeline", colorize))
		})
	}
}

// TestColourIsNothingButColour.
//
// Stripping the escape sequences from the coloured rendering must give back the
// uncoloured one exactly. Anything else means the two modes are two messages, and
// the one nobody generates golden files for is the one that rots.
func TestColourIsNothingButColour(t *testing.T) {
	failure := &UnreachableSinkError{
		diagnosis: fixtureDiagnosis(),
		cause:     dnsNotFound("clickhouse.kuberecord-quickstart.svc"),
	}

	painted := failure.Render("kuberecord timeline", true)
	if !strings.Contains(painted, ansiReset) {
		t.Fatal("the coloured rendering carries no escape sequences at all")
	}
	stripped := strings.NewReplacer(ansiReset, "", ansiBold, "", ansiDim, "").Replace(painted)
	if plain := failure.Render("kuberecord timeline", false); stripped != plain {
		t.Errorf("colour changes more than colour.\n--- plain ---\n%s\n--- stripped ---\n%s",
			plain, stripped)
	}
}

// assertDiagnoseGolden compares a rendering against its checked-in file.
func assertDiagnoseGolden(t *testing.T, name, got string) {
	t.Helper()

	path := filepath.Join("testdata", "diagnose", name+".golden")
	if *updateDiagnoseGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("creating the golden directory: %v", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s (run `go test ./internal/cli/resolve/ -update` to create it): %v", path, err)
	}
	if got != string(want) {
		t.Errorf("the rendering of %s changed.\n--- want ---\n%s\n--- got ---\n%s", name, want, got)
	}
}

// countingEngine is a read plane that fails the way an unreachable one does, and
// counts how often it is asked.
//
// The count is the assertion, not a diagnostic: this is what proves the diagnosis
// is a diagnosis. A wrapper that retried, or that tried a second address before
// giving up, would show as a second call here — and a CLI that silently connected
// somewhere other than where it was told could not be an audit tool, because every
// answer it gave would carry an unstated "…from somewhere".
type countingEngine struct {
	calls int
	err   error
}

func (e *countingEngine) Timeline(context.Context, query.TimelineQuery) (query.ChangeIterator, error) {
	e.calls++
	return nil, e.err
}

func (e *countingEngine) StateAt(
	context.Context, query.ObjectRef, time.Time, string,
) (*query.Reconstruction, error) {
	e.calls++
	return nil, e.err
}

func (e *countingEngine) Coverage(context.Context, query.ScopeQuery) ([]query.ScopeInterval, error) {
	e.calls++
	return nil, e.err
}

func (e *countingEngine) Incarnations(
	context.Context, query.ObjectRef, time.Time, time.Time,
) ([]query.Incarnation, error) {
	e.calls++
	return nil, e.err
}

func (e *countingEngine) Capabilities() query.Capabilities {
	return query.Capabilities{Backend: "clickhouse", Deletions: true}
}

func (e *countingEngine) Close() error { return nil }

// countingClusterLister is countingEngine with the one optional half the shipped
// ClickHouse engine implements.
type countingClusterLister struct{ *countingEngine }

func (e *countingClusterLister) ClusterIDs(context.Context) ([]string, error) {
	e.calls++
	return nil, e.err
}

// TestExactlyOneAttemptIsMadePerQuestion.
//
// Every method is driven once and the inner engine must see exactly one call for
// each. The wrapper opens nothing, retries nothing and substitutes nothing; it
// reads an error and adds a sentence to it (D24).
func TestExactlyOneAttemptIsMadePerQuestion(t *testing.T) {
	inner := &countingEngine{err: dnsNotFound("clickhouse.kuberecord-quickstart.svc")}
	watched := fixtureDiagnosis().watch(&countingClusterLister{countingEngine: inner})

	ctx := t.Context()
	calls := []struct {
		name string
		call func() error
	}{
		{"Timeline", func() error { _, err := watched.Timeline(ctx, query.TimelineQuery{}); return err }},
		{"StateAt", func() error {
			_, err := watched.StateAt(ctx, query.ObjectRef{}, time.Time{}, "")
			return err
		}},
		{"Coverage", func() error { _, err := watched.Coverage(ctx, query.ScopeQuery{}); return err }},
		{"Incarnations", func() error {
			_, err := watched.Incarnations(ctx, query.ObjectRef{}, time.Time{}, time.Time{})
			return err
		}},
		{"ClusterIDs", func() error {
			lister, ok := watched.(query.ClusterIDLister)
			if !ok {
				t.Fatal("wrapping hid the engine's ability to list clusters")
			}
			_, err := lister.ClusterIDs(ctx)
			return err
		}},
	}

	for i, tc := range calls {
		var unreachable *UnreachableSinkError
		if err := tc.call(); !errors.As(err, &unreachable) {
			t.Errorf("%s returned an undiagnosed failure: %v", tc.name, err)
		}
		if want := i + 1; inner.calls != want {
			t.Fatalf("after %s the backend had been asked %d times, want %d",
				tc.name, inner.calls, want)
		}
	}
}

// TestWatchingPreservesWhatTheEngineCanDo.
//
// A decorator over a contract with optional halves has to advertise exactly the
// halves its subject implements. Claiming one it does not have would make a
// caller's type assertion answer a question about the wrapper; dropping one it
// does have would silently remove a step of the cluster-identity chain.
func TestWatchingPreservesWhatTheEngineCanDo(t *testing.T) {
	armed := fixtureDiagnosis()

	plain := armed.watch(&countingEngine{})
	if _, ok := plain.(query.ClusterIDLister); ok {
		t.Error("wrapping invented an ability the engine does not have")
	}

	lister := armed.watch(&countingClusterLister{countingEngine: &countingEngine{}})
	if _, ok := lister.(query.ClusterIDLister); !ok {
		t.Error("wrapping dropped the engine's ability to list clusters")
	}
	if _, ok := lister.(query.ScanEstimator); ok {
		t.Error("wrapping invented a cold-scan estimator, which the ClickHouse engine is not")
	}
	if _, ok := lister.(query.ScanProgressReporter); ok {
		t.Error("wrapping invented a scan progress reporter, which the ClickHouse engine is not")
	}
	if got := lister.Capabilities().Backend; got != "clickhouse" {
		t.Errorf("wrapping changed the backend reported as metadata.backend: %q", got)
	}
}

// TestAPublicAddressIsNotWatchedAtAll.
//
// The wrapper is inert code on every path but one, and the cheapest way to keep it
// inert is not to install it. An engine over an address that resolves from
// anywhere is handed back exactly as it arrived, so nothing about that path can
// have changed.
func TestAPublicAddressIsNotWatchedAtAll(t *testing.T) {
	public := fixtureDiagnosis()
	public.addr = "clickhouse.example.com:9000"

	inner := &countingEngine{err: dnsNotFound("clickhouse.example.com")}
	if watched := public.watch(inner); watched != query.QueryEngine(inner) {
		t.Error("an engine over a public address was wrapped")
	}

	empty := diagnosis{}
	if watched := empty.watch(inner); watched != query.QueryEngine(inner) {
		t.Error("an engine with no diagnosis at all was wrapped")
	}
}
