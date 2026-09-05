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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// --sink-addr through the whole chain: which step still owns the answer, what the
// notice says about it, and which routes refuse it.
//
// The dial configuration itself — the four fields the override must leave alone —
// is asserted in internal/cli/resolve/sinkaddr_internal_test.go, because
// resolve.Backend hands back an opened engine rather than the settings it was
// opened with.

// forwarded is the endpoint a `kubectl port-forward` produces, and the value the
// unreachable-backend diagnostic tells people to pass.
const forwarded = "127.0.0.1:9000"

// sinkAddrMarkerText is the marker the notice must carry, spelled here from the
// flag constant rather than as a literal so that renaming the flag fails the
// build instead of quietly weakening this assertion.
const sinkAddrMarkerText = ", address from --" + options.FlagSinkAddr

// TestSinkAddrIsVisibleInTheNotice.
//
// The chain's honesty property is that stderr always says where the data came
// from. An override that rewrote the address without changing that line would
// print `discovered ClickHouseSink/default (127.0.0.1:9000/kuberecord)` — a
// sentence that is false about the custom resource it names, since that resource
// records a Service DNS name. So the marker is asserted for every route that
// accepts the flag.
//
// The origin is asserted alongside it, and it is the point of the case rather than
// a detail: discovery is still the step that answered — four of the five fields
// and the credential came from the resource it found — so reporting `using
// --sink-addr` would claim the resource was never consulted (D25).
func TestSinkAddrIsVisibleInTheNotice(t *testing.T) {
	tests := []struct {
		name       string
		config     *resolve.Config
		args       []string
		wantOrigin resolve.Origin
		wantNotice string
	}{
		{
			name:       "discovery keeps the answer, and the notice carries the override",
			args:       []string{"--sink-addr", forwarded, "--cluster-id", theCluster},
			wantOrigin: resolve.OriginDiscovered,
			wantNotice: "→ discovered ClickHouseSink/default (" + forwarded + "/" +
				resolve.DefaultClickHouseDatabase + sinkAddrMarkerText + ")",
		},
		{
			name: "a named sink keeps the answer too",
			args: []string{
				"--sink", "ClickHouseSink/default", "--sink-addr", forwarded,
				"--cluster-id", theCluster,
			},
			wantOrigin: resolve.OriginSinkFlag,
			wantNotice: "→ using --sink ClickHouseSink/default (" + forwarded + "/" +
				resolve.DefaultClickHouseDatabase + sinkAddrMarkerText + ")",
		},
		{
			name: "an active ClickHouse profile takes it as well",
			config: &resolve.Config{
				CurrentProfile: "prod",
				Profiles: map[string]resolve.Profile{
					"prod": {Backend: resolve.BackendClickHouse, ClickHouse: &resolve.ClickHouseProfile{
						Addr:     "clickhouse.kuberecord-system.svc:9000",
						Database: resolve.DefaultClickHouseDatabase,
						Username: "reader",
					}},
				},
			},
			args:       []string{"--sink-addr", forwarded, "--cluster-id", theCluster},
			wantOrigin: resolve.OriginProfile,
			wantNotice: "→ using profile prod (ClickHouse at " + forwarded + "/" +
				resolve.DefaultClickHouseDatabase + sinkAddrMarkerText + ")",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clients := fakeClients(
				[]*unstructured.Unstructured{
					clickHouseSink("default", "clickhouse.kuberecord-system.svc:9000"),
				},
				credentialsSecret(operatorNamespace), operatorDeployment(operatorNamespace, theCluster))

			resolver, notices := resolverOver(t, tc.config, clients, tc.args...)

			backend, err := resolver.Resolve(t.Context())
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			t.Cleanup(func() { closeBackend(t, backend) })

			if backend.Origin != tc.wantOrigin {
				t.Errorf("resolve.Origin = %q, want %q — the override modifies a step's result "+
					"rather than being a step of its own", backend.Origin, tc.wantOrigin)
			}
			if !strings.Contains(notices.String(), tc.wantNotice) {
				t.Errorf("stderr does not say the endpoint was overridden.\n got: %s\nwant a line: %s",
					notices.String(), tc.wantNotice)
			}

			// metadata.backend is the engine's own stable identifier — the value
			// docs/CLI.md says the `version` command's engine column matches, and
			// what a script branches on to know which engine answered. It is not
			// the resolution's description and does not carry the marker; the
			// notice is where the override is reported. Asserted rather than
			// merely noted, so that a later edit pointing this field at
			// Backend.Description fails here instead of silently changing a
			// released contract.
			if got := backend.Engine.Capabilities().Backend; got != "clickhouse" {
				t.Errorf("metadata.backend is %q, want the engine identifier %q", got, "clickhouse")
			}
		})
	}
}

// TestSinkAddrRefusesTheRoutesWithNoEndpointToReplace.
//
// Each of these would otherwise be a flag that parsed and did nothing, and the
// symptom of that is a query answered from the address the user was trying not to
// use. Exit 2 throughout: the user typed something this program does not accept,
// which is the code a wrapper script must not retry.
func TestSinkAddrRefusesTheRoutesWithNoEndpointToReplace(t *testing.T) {
	archive := localArchive(t, theCluster)

	tests := []struct {
		name   string
		config *resolve.Config
		sinks  []*unstructured.Unstructured
		args   []string
		want   []string
	}{
		{
			name:  "--source reads a location directly, so nothing recorded an endpoint",
			sinks: []*unstructured.Unstructured{clickHouseSink("default", "clickhouse:9000")},
			args:  []string{"--source", archive, "--sink-addr", forwarded},
			want: []string{
				"--" + options.FlagSinkAddr, "--" + options.FlagSource, archive,
				"cannot be given together",
			},
		},
		{
			name: "a profile reading an archive has no endpoint of this shape",
			config: &resolve.Config{
				CurrentProfile: "laptop",
				Profiles: map[string]resolve.Profile{
					"laptop": {Backend: resolve.BackendLocal, Local: &resolve.LocalProfile{Path: archive}},
				},
			},
			sinks: []*unstructured.Unstructured{clickHouseSink("default", "clickhouse:9000")},
			args:  []string{"--sink-addr", forwarded, "--cluster-id", theCluster},
			want: []string{
				"--" + options.FlagSinkAddr, `profile "laptop"`, string(resolve.BackendLocal),
				string(resolve.BackendClickHouse),
			},
		},
		{
			name: "a named S3Sink is an object store",
			sinks: []*unstructured.Unstructured{
				clickHouseSink("default", "clickhouse:9000"),
				s3Sink("archive", "acme-audit", "kuberecord"),
			},
			args: []string{"--sink", "S3Sink/archive", "--sink-addr", forwarded},
			want: []string{
				"--" + options.FlagSinkAddr, "S3Sink/archive", "object store",
				resolve.KindClickHouseSink, "--" + options.FlagSource,
			},
		},
		{
			// Named differently from the case above so that the two routes to an
			// object store are visibly two routes: this sink was never typed, and
			// the message still has to name it.
			name:  "a discovered S3Sink is refused the same way",
			sinks: []*unstructured.Unstructured{s3Sink("cold", "acme-cold", "kuberecord")},
			args:  []string{"--sink-addr", forwarded, "--cluster-id", theCluster},
			want: []string{
				"--" + options.FlagSinkAddr, "S3Sink/cold", "object store",
				resolve.KindClickHouseSink,
			},
		},
		{
			name:  "a value that is not host:port names the expected form",
			sinks: []*unstructured.Unstructured{clickHouseSink("default", "clickhouse:9000")},
			args:  []string{"--sink-addr", "clickhouse", "--cluster-id", theCluster},
			want:  []string{"--" + options.FlagSinkAddr, "names no port", "host:port", "127.0.0.1:9000"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clients := fakeClients(tc.sinks,
				credentialsSecret(operatorNamespace), operatorDeployment(operatorNamespace, theCluster))

			resolver, _ := resolverOver(t, tc.config, clients, tc.args...)

			backend, err := resolver.Resolve(t.Context())
			if err == nil {
				closeBackend(t, backend)
				t.Fatalf("Resolve accepted --%s on a route with no endpoint to replace",
					options.FlagSinkAddr)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the failure does not mention %q:\n%v", want, err)
				}
			}
			if got := exit.CodeFor(err); got != exit.UsageError {
				t.Errorf("exit code %d, want %d — a conflicting flag is not something a wrapper "+
					"script should retry", got, exit.UsageError)
			}
		})
	}
}

// TestAMalformedSinkAddrContactsNothing.
//
// The shape is checked before the chain runs, so a missing port is reported
// without a kubeconfig round trip, a sink listing or a Secret read. That ordering
// is worth asserting rather than assuming: it is the difference between being told
// about a typo immediately and being told after the tool has failed to reach a
// cluster for an unrelated reason.
func TestAMalformedSinkAddrContactsNothing(t *testing.T) {
	clients := fakeClients(
		[]*unstructured.Unstructured{clickHouseSink("default", "clickhouse:9000")},
		credentialsSecret(operatorNamespace), operatorDeployment(operatorNamespace, theCluster))

	var contacted []string
	record := func(action clienttesting.Action) (bool, runtime.Object, error) {
		contacted = append(contacted, action.GetVerb()+" "+action.GetResource().Resource)
		return false, nil, nil
	}
	clients.Dynamic.(*dynamicfake.FakeDynamicClient).PrependReactor("*", "*", record)
	clients.Typed.(*k8sfake.Clientset).PrependReactor("*", "*", record)

	resolver, _ := resolverOver(t, nil, clients, "--sink-addr", "clickhouse")

	if _, err := resolver.Resolve(t.Context()); err == nil {
		t.Fatal("Resolve accepted a value that is not host:port")
	}
	if len(contacted) != 0 {
		t.Errorf("a malformed flag value reached the cluster first: %v", contacted)
	}
}

// TestSinkAddrIsRegisteredBesideSink.
//
// The flag has to exist on the persistent set — every command resolves a backend,
// so an override registered on one of them would be a flag that worked on
// `timeline` and not on `get`. Its help text is checked for the promise it makes,
// because that text is the only description most people will read.
func TestSinkAddrIsRegisteredBesideSink(t *testing.T) {
	io, _, _ := streams()
	root, _ := cli.NewRootCommand(options.StandaloneName, io)

	flag := root.PersistentFlags().Lookup(options.FlagSinkAddr)
	if flag == nil {
		t.Fatalf("--%s is not a persistent flag, so it would work on some commands and not others",
			options.FlagSinkAddr)
	}
	if flag.DefValue != "" {
		t.Errorf("--%s defaults to %q; not given must mean not overridden",
			options.FlagSinkAddr, flag.DefValue)
	}
	for _, want := range []string{"host:port", "instead"} {
		if !strings.Contains(flag.Usage, want) {
			t.Errorf("the help for --%s does not mention %q: %s", options.FlagSinkAddr, want, flag.Usage)
		}
	}
	if root.PersistentFlags().Lookup(options.FlagSink) == nil {
		t.Errorf("--%s is missing, and --%s is documented as modifying it",
			options.FlagSink, options.FlagSinkAddr)
	}
}
