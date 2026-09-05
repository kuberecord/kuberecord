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
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The unreachable-backend message is rendered at the top of the CLI (Task 13.1),
// and this is the only place the whole of that path can be exercised at once: a
// sink discovered from a cluster, an engine opened over the address it recorded,
// a real failure from the real driver when that address is dialled from outside
// the cluster, and the block that failure produces on stderr.
//
// The classification and the wording are tested in internal/cli/resolve. What is
// tested here is the wiring — that a failure raised layers below a command still
// arrives carrying its explanation, that the explanation obeys --color, and that
// no ordinary failure acquires one.

// The fixture cluster: an operator, a Secret, and one sink whose address is the
// Service name the quickstart writes.
const (
	diagnoseNamespace = "kuberecord-system"
	diagnoseSecret    = "clickhouse-credentials"
	diagnoseCluster   = "prod-eu-1"

	// diagnoseAddr is a name that exists only inside a cluster. `.svc` is not a
	// delegated top-level domain, so a resolver anywhere answers NXDOMAIN for it
	// — which is precisely the failure this whole path exists to explain.
	diagnoseAddr = "clickhouse.kuberecord-quickstart.svc:9000"
)

// diagnoseFixture resolves a backend over the fixture cluster and returns it.
func diagnoseFixture(t *testing.T) (*resolve.Backend, genericiooptions.IOStreams) {
	t.Helper()

	sink := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       resolve.KindClickHouseSink,
		"metadata":   map[string]any{"name": "default"},
		"spec": map[string]any{"connection": map[string]any{
			"addr":                 diagnoseAddr,
			"database":             resolve.DefaultClickHouseDatabase,
			"username":             "kuberecord",
			"credentialsSecretRef": map[string]any{"name": diagnoseSecret},
		}},
	}}
	listKinds := map[schema.GroupVersionResource]string{
		{Group: v1alpha1.GroupVersion.Group, Version: v1alpha1.GroupVersion.Version,
			Resource: "clickhousesinks"}: resolve.KindClickHouseSink + "List",
		{Group: v1alpha1.GroupVersion.Group, Version: v1alpha1.GroupVersion.Version,
			Resource: "s3sinks"}: resolve.KindS3Sink + "List",
	}

	streams := genericiooptions.IOStreams{
		In: strings.NewReader(""), Out: &strings.Builder{}, ErrOut: &strings.Builder{},
	}
	root, flags := NewRootCommand(options.StandaloneName, streams)
	if err := root.ParseFlags([]string{
		"--kubeconfig", filepath.Join("testdata", "kubeconfig"),
		"--" + options.FlagClusterID, diagnoseCluster,
		"--" + options.FlagOperatorNamespace, diagnoseNamespace,
	}); err != nil {
		t.Fatalf("parsing flags: %v", err)
	}

	resolver := &resolve.BackendResolver{
		Flags: flags, Streams: streams, InvokedAs: options.StandaloneName,
		Config: &resolve.Config{}, ConfigPath: filepath.Join(t.TempDir(), "config.yaml"),
		Clients: &resolve.Clients{
			Dynamic: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
				runtime.NewScheme(), listKinds, sink),
			Typed: k8sfake.NewClientset(&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: diagnoseSecret, Namespace: diagnoseNamespace},
				Data:       map[string][]byte{"password": []byte("correct-horse-battery-staple")},
			}),
		},
	}

	backend, err := resolver.Resolve(t.Context())
	if err != nil {
		t.Fatalf("resolving the fixture sink: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := backend.Close(); closeErr != nil {
			t.Errorf("closing the backend: %v", closeErr)
		}
	})
	return backend, streams
}

// unreachableFixture asks the resolved backend a question it cannot answer, and
// returns the failure.
//
// The dial is real and so is the driver: nothing here fakes an error, because the
// property under test is that a *net.DNSError raised inside clickhouse-go still
// arrives at the top of the CLI recognisable. A machine whose resolver answers
// something other than NXDOMAIN for a `.svc` name — a wildcarding ISP resolver is
// the way that happens — cannot exercise this, and says so rather than failing.
func unreachableFixture(t *testing.T) (genericiooptions.IOStreams, error) {
	t.Helper()

	backend, streams := diagnoseFixture(t)
	_, err := backend.Engine.Coverage(t.Context(), query.ScopeQuery{ClusterID: diagnoseCluster})
	if err == nil {
		t.Fatalf("querying %s succeeded, which means something is answering on that name", diagnoseAddr)
	}

	var unreachable *resolve.UnreachableSinkError
	if !errors.As(err, &unreachable) {
		t.Skipf("this machine's resolver does not answer NXDOMAIN for %s, so the "+
			"laptop-versus-cluster-DNS mismatch cannot be reproduced here: %v", diagnoseAddr, err)
	}
	return streams, err
}

// TestAnUnreachableClusterInternalSinkExplainsItself, end to end.
//
// Resolution succeeds — the sink is right and discovery found it — and the failure
// arrives from the first query, several layers below the command, which is exactly
// why the diagnosis travels with the engine rather than living at the dial.
func TestAnUnreachableClusterInternalSinkExplainsItself(t *testing.T) {
	streams, err := unreachableFixture(t)

	root, flags := NewRootCommand(options.StandaloneName, streams)
	if parseErr := root.ParseFlags([]string{"--" + options.FlagColor, string(options.ColorNever)}); parseErr != nil {
		t.Fatalf("parsing flags: %v", parseErr)
	}

	advice := unreachableAdvice(err, root, flags, streams)
	for _, want := range []string{
		"kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000",
		"--" + options.FlagSinkAddr + " 127.0.0.1:9000",
		"config set-profile local --backend clickhouse",
		"config use-profile local",
	} {
		if !strings.Contains(advice, want) {
			t.Errorf("the advice does not carry %q:\n%s", want, advice)
		}
	}

	// The real cause survives all the way up, which is what a -v user and a
	// maintainer reading a bug report both need.
	if !strings.Contains(err.Error(), "no such host") {
		t.Errorf("the failure no longer names its real cause: %v", err)
	}
	// And it is still the exit code an undiagnosed dial failure had.
	if code := exit.CodeFor(err); code != exit.RuntimeError {
		t.Errorf("a diagnosed dial failure exits %d, want %d", code, exit.RuntimeError)
	}

	// Nothing about a failed resolution or a failed query reached stdout. The
	// advice is a diagnostic, and RunContext writes it into the same stderr line
	// as `error:` — a block on stdout would corrupt a `| jq` (Invariant 4).
	if produced := streams.Out.(*strings.Builder).String(); produced != "" {
		t.Errorf("a failing invocation wrote to stdout, which belongs to data: %q", produced)
	}
}

// TestTheAdviceObeysTheColourMode.
//
// The block is written to stderr through the same single write as the `error:`
// line above it, so the only decision left at this layer is whether to paint it —
// and that decision belongs to --color and to whether stderr is a terminal, never
// to this function.
func TestTheAdviceObeysTheColourMode(t *testing.T) {
	streams, err := unreachableFixture(t)

	for _, tc := range []struct {
		mode    options.ColorMode
		painted bool
	}{
		{options.ColorNever, false},
		{options.ColorAlways, true},
		// A strings.Builder is not a terminal, so `auto` resolves to plain — which
		// is what keeps the golden files in this repository free of escapes.
		{options.ColorAuto, false},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			root, flags := NewRootCommand(options.StandaloneName, streams)
			if parseErr := root.ParseFlags([]string{"--" + options.FlagColor, string(tc.mode)}); parseErr != nil {
				t.Fatalf("parsing flags: %v", parseErr)
			}
			advice := unreachableAdvice(err, root, flags, streams)
			if painted := strings.Contains(advice, "\x1b["); painted != tc.painted {
				t.Errorf("--%s=%s produced painted=%v, want %v", options.FlagColor, tc.mode, painted, tc.painted)
			}
		})
	}
}

// TestAnOrdinaryFailureGetsNoAdvice.
//
// The block is four paragraphs long. Attaching it to a failure it does not explain
// would bury the one line that did, and would send a reader to port-forward
// something that is not the problem.
func TestAnOrdinaryFailureGetsNoAdvice(t *testing.T) {
	streams := genericiooptions.IOStreams{
		In: strings.NewReader(""), Out: &strings.Builder{}, ErrOut: &strings.Builder{},
	}
	root, flags := NewRootCommand(options.StandaloneName, streams)

	for name, err := range map[string]error{
		"an ordinary runtime failure": exit.RuntimeErrorf("reading the timeline: connection reset by peer"),
		"a usage error":               exit.UsageErrorf("unknown flag --nope"),
		"a missing-coverage finding":  query.ErrNoCoverage,
		"nothing at all":              nil,
	} {
		if advice := unreachableAdvice(err, root, flags, streams); advice != "" {
			t.Errorf("%s acquired an unreachable-backend block:\n%s", name, advice)
		}
	}
}
