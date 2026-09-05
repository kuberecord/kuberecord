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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
)

// --from-sink is tested from inside the package for the half a command cannot
// see. The written stanza is exported and could be asserted from outside, but the
// three things worth pinning here are not: that the address rule reaches the same
// classifier the unreachable-backend message uses, that the Secret's value is
// never extracted, and that the explanation reads as prose. The command-level
// behaviour — the flag conflicts, the file on disk — is in
// internal/cli/fromsink_internal_test.go.

// The fixture cluster: the quickstart's ClickHouseSink, an archive beside it, and
// the Secret the first one names. fixtureAddr, fixtureNamespace and
// fixturePassword are diagnose_internal_test.go's, deliberately: the address this
// command rewrites is the address that message explains, and a second spelling of
// it would let the two drift.
const (
	fromSinkSecret   = "clickhouse-credentials"
	fromSinkUsername = "kuberecord"
	fromSinkBucket   = "acme-audit"
	fromSinkPrefix   = "kuberecord"
	fromSinkRegion   = "eu-west-1"
)

// clickHouseSinkRef and s3SinkRef are the two sinks the fixture holds.
var (
	clickHouseSinkRef = SinkRef{Kind: KindClickHouseSink, Name: "default"}
	s3SinkRef         = SinkRef{Kind: KindS3Sink, Name: "archive"}
)

// fromSinkClickHouse builds the ClickHouseSink custom resource as the API server
// would hand one back.
func fromSinkClickHouse(addr string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       KindClickHouseSink,
		"metadata":   map[string]any{"name": clickHouseSinkRef.Name},
		"spec": map[string]any{"connection": map[string]any{
			"addr":                 addr,
			"database":             DefaultClickHouseDatabase,
			"username":             fromSinkUsername,
			"credentialsSecretRef": map[string]any{"name": fromSinkSecret},
		}},
	}}
}

// fromSinkS3 builds the S3Sink. Its credentials are ambient, which is the
// supported and, on a cloud provider, preferred state.
func fromSinkS3(endpoint string) *unstructured.Unstructured {
	spec := map[string]any{
		"bucket":         fromSinkBucket,
		"prefix":         fromSinkPrefix,
		"region":         fromSinkRegion,
		"forcePathStyle": true,
	}
	if endpoint != "" {
		spec["endpoint"] = endpoint
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       KindS3Sink,
		"metadata":   map[string]any{"name": s3SinkRef.Name},
		"spec":       spec,
	}}
}

// fromSinkResolver builds a resolver over the given custom resources and Secret
// data.
//
// The operator namespace is declared rather than searched, because what these
// cases are about is the profile and not the namespace chain — which
// resolve_test.go already walks end to end. Nil secretData seeds no Secret at all,
// which is how the "cannot be checked" cases are produced.
func fromSinkResolver(
	t *testing.T, secretData map[string][]byte, sinks ...*unstructured.Unstructured,
) *BackendResolver {
	t.Helper()

	listKinds := map[schema.GroupVersionResource]string{
		clickHouseSinkGVR: KindClickHouseSink + "List",
		s3SinkGVR:         KindS3Sink + "List",
	}
	seeded := make([]runtime.Object, 0, len(sinks))
	for _, sink := range sinks {
		seeded = append(seeded, sink)
	}

	var objects []runtime.Object
	if secretData != nil {
		objects = append(objects, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: fromSinkSecret, Namespace: fixtureNamespace},
			Data:       secretData,
		})
	}
	return &BackendResolver{
		InvokedAs: "kuberecord",
		Config:    &Config{OperatorNamespace: fixtureNamespace},
		Clients: &Clients{
			Dynamic: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
				runtime.NewScheme(), listKinds, seeded...),
			Typed: k8sfake.NewClientset(objects...),
		},
	}
}

// goodSecret is the Secret a correctly configured sink names.
func goodSecret() map[string][]byte {
	return map[string][]byte{secretKeyPassword: []byte(fixturePassword)}
}

// TestProfileFromSinkDecidesTheAddressInThreeCases is the acceptance criterion's
// central rule.
//
// The three are separate cases rather than a default with an exception, because
// the wrong answer in each direction is a different failure: writing a Service
// name produces a profile that fails exactly as discovery did, and rewriting a
// public endpoint produces one that reaches nothing at all.
func TestProfileFromSinkDecidesTheAddressInThreeCases(t *testing.T) {
	const publicAddr = "clickhouse.example.com:9440"

	for _, tc := range []struct {
		name        string
		recorded    string
		over        ProfileOverrides
		wantAddr    string
		wantRewrite bool
		wantForward string
	}{
		{
			name:     "--addr wins, which is what Task 13.1's message tells the reader to run",
			recorded: fixtureAddr,
			over:     ProfileOverrides{Addr: "127.0.0.1:19000"},
			wantAddr: "127.0.0.1:19000",
		},
		{
			name:        "a cluster-internal name is rewritten to the forwarded port",
			recorded:    fixtureAddr,
			wantAddr:    "127.0.0.1:9000",
			wantRewrite: true,
			wantForward: "kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000",
		},
		{
			name:        "a bare single-label host takes the operator's namespace",
			recorded:    "clickhouse:9000",
			wantAddr:    "127.0.0.1:9000",
			wantRewrite: true,
			wantForward: "kubectl port-forward -n " + fixtureNamespace + " svc/clickhouse 9000:9000",
		},
		{
			name:        "a non-standard port travels into the rewrite",
			recorded:    "clickhouse.kuberecord-quickstart.svc:19000",
			wantAddr:    "127.0.0.1:19000",
			wantRewrite: true,
			wantForward: "kubectl port-forward -n kuberecord-quickstart svc/clickhouse 19000:19000",
		},
		{
			name:     "a public endpoint is written unchanged",
			recorded: publicAddr,
			wantAddr: publicAddr,
		},
		{
			name:     "--addr on a public endpoint still wins",
			recorded: publicAddr,
			over:     ProfileOverrides{Addr: "ch.internal:9000"},
			wantAddr: "ch.internal:9000",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := fromSinkResolver(t, goodSecret(), fromSinkClickHouse(tc.recorded))

			derived, err := resolver.ProfileFromSink(t.Context(), clickHouseSinkRef, tc.over)
			if err != nil {
				t.Fatalf("ProfileFromSink: %v", err)
			}
			if derived.Profile.ClickHouse.Addr != tc.wantAddr {
				t.Errorf("clickhouse.addr = %q, want %q", derived.Profile.ClickHouse.Addr, tc.wantAddr)
			}
			if derived.AddrRewritten != tc.wantRewrite {
				t.Errorf("AddrRewritten = %v, want %v", derived.AddrRewritten, tc.wantRewrite)
			}
			if derived.PortForward != tc.wantForward {
				t.Errorf("PortForward = %q, want %q", derived.PortForward, tc.wantForward)
			}
			if derived.RecordedAddr != tc.recorded {
				t.Errorf("RecordedAddr = %q, want the address the custom resource states, %q",
					derived.RecordedAddr, tc.recorded)
			}
			// The other four fields are the custom resource's, whatever happened to
			// the address: a profile that quietly changed the database would read a
			// different archive than the sink writes.
			if err := derived.Profile.Validate(); err != nil {
				t.Errorf("the derived profile does not validate: %v", err)
			}
			if derived.Profile.ClickHouse.Database != DefaultClickHouseDatabase {
				t.Errorf("clickhouse.database = %q, want the sink's %q",
					derived.Profile.ClickHouse.Database, DefaultClickHouseDatabase)
			}
			if derived.Profile.ClickHouse.Username != fromSinkUsername {
				t.Errorf("clickhouse.username = %q, want the sink's %q",
					derived.Profile.ClickHouse.Username, fromSinkUsername)
			}
		})
	}
}

// TestProfileFromSinkWritesTheCRDDefaultsRatherThanBlanks.
//
// A profile is a complete description of where to read from. An empty database
// leaves the server's own default, which is not where the operator wrote — so a
// sink relying on the CRD's defaulting must still produce a stanza that names
// them.
func TestProfileFromSinkWritesTheCRDDefaultsRatherThanBlanks(t *testing.T) {
	bare := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       KindClickHouseSink,
		"metadata":   map[string]any{"name": clickHouseSinkRef.Name},
		"spec": map[string]any{"connection": map[string]any{
			"addr":                 "clickhouse.example.com:9000",
			"credentialsSecretRef": map[string]any{"name": fromSinkSecret},
		}},
	}}
	resolver := fromSinkResolver(t, goodSecret(), bare)

	derived, err := resolver.ProfileFromSink(t.Context(), clickHouseSinkRef, ProfileOverrides{})
	if err != nil {
		t.Fatalf("ProfileFromSink: %v", err)
	}
	if got := derived.Profile.ClickHouse.Database; got != DefaultClickHouseDatabase {
		t.Errorf("clickhouse.database = %q, want the CRD's default %q", got, DefaultClickHouseDatabase)
	}
	if got := derived.Profile.ClickHouse.Username; got != DefaultClickHouseUsername {
		t.Errorf("clickhouse.username = %q, want the CRD's default %q", got, DefaultClickHouseUsername)
	}
}

// TestProfileFromSinkTakesTheOverridesTheCustomResourceCannotState.
//
// The user, the credential and the TLS setting: a ClickHouseSink states the first
// as the operator's *writer*, cannot state the second usefully for a reader at
// all, and has no field for the third.
func TestProfileFromSinkTakesTheOverridesTheCustomResourceCannotState(t *testing.T) {
	resolver := fromSinkResolver(t, goodSecret(), fromSinkClickHouse(fixtureAddr))

	derived, err := resolver.ProfileFromSink(t.Context(), clickHouseSinkRef, ProfileOverrides{
		Username: "kuberecord_ro", PasswordFile: "/run/secrets/ch", TLS: true,
	})
	if err != nil {
		t.Fatalf("ProfileFromSink: %v", err)
	}
	stanza := derived.Profile.ClickHouse
	if stanza.Username != "kuberecord_ro" {
		t.Errorf("clickhouse.username = %q, want the overridden read-only user", stanza.Username)
	}
	if stanza.PasswordFile != "/run/secrets/ch" || stanza.PasswordEnv != "" {
		t.Errorf("the credential reference is %+v, want the file alone", stanza)
	}
	if !stanza.TLS {
		t.Error("clickhouse.tls is false, and --tls was given: spec.connection cannot state it, " +
			"so the flag is the only way to say it")
	}
}

// TestProfileFromSinkDefaultsToTheDocumentedVariable.
//
// A profile with no password reference validates and then authenticates as
// nobody. The variable is the one Task 13.1's message prints and docs/CLI.md
// names, so what the reader was told to export is what the profile reads.
func TestProfileFromSinkDefaultsToTheDocumentedVariable(t *testing.T) {
	resolver := fromSinkResolver(t, goodSecret(), fromSinkClickHouse(fixtureAddr))

	derived, err := resolver.ProfileFromSink(t.Context(), clickHouseSinkRef, ProfileOverrides{})
	if err != nil {
		t.Fatalf("ProfileFromSink: %v", err)
	}
	if got := derived.Profile.ClickHouse.PasswordEnv; got != passwordEnvName {
		t.Errorf("clickhouse.passwordEnv = %q, want %q", got, passwordEnvName)
	}
}

// TestProfileFromSinkNeverCarriesThePassword is the acceptance criterion's
// safeguard, asserted at the two places a credential could escape: the struct in
// memory, and the file on disk.
//
// This is the path most likely to tempt an inline write, because it is the one
// standing next to a Secret it can read.
func TestProfileFromSinkNeverCarriesThePassword(t *testing.T) {
	resolver := fromSinkResolver(t, goodSecret(), fromSinkClickHouse(fixtureAddr))

	derived, err := resolver.ProfileFromSink(t.Context(), clickHouseSinkRef, ProfileOverrides{})
	if err != nil {
		t.Fatalf("ProfileFromSink: %v", err)
	}
	if derived.Profile.ClickHouse.Password != "" {
		t.Error("the derived profile holds a password inline")
	}
	if strings.Contains(derived.Explain(false), fixturePassword) {
		t.Error("the explanation printed the password")
	}

	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := &Config{Profiles: map[string]Profile{"local": derived.Profile}, CurrentProfile: "local"}
	if err := SaveConfig(path, cfg); err != nil {
		t.Fatalf("SaveConfig: %v", err)
	}
	written, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("reading what was written: %v", err)
	}
	if strings.Contains(string(written), fixturePassword) {
		t.Errorf("the configuration file holds the password in plain text:\n%s", written)
	}
}

// TestProfileFromSinkReportsWhatItCouldNotCheck.
//
// The Secret is read to confirm the key the sink names is there, and every way
// that can fail is a sentence rather than an error: nothing in the written profile
// depends on the answer, and the engineer who most needs --from-sink is exactly
// the one D7 denies Secret reads to.
func TestProfileFromSinkReportsWhatItCouldNotCheck(t *testing.T) {
	for _, tc := range []struct {
		name     string
		data     map[string][]byte
		forbid   bool
		wantSaid []string
	}{
		{
			name: "a Secret holding the key is checked and said to be",
			data: goodSecret(),
		},
		{
			name:     "a Secret created with the wrong key names the keys it does hold",
			data:     map[string][]byte{"PASSWORD": []byte(fixturePassword), "user": []byte("x")},
			wantSaid: []string{`no "password" key`, "PASSWORD, user"},
		},
		{
			name:     "a Secret that is not there says so",
			wantSaid: []string{"could not be read", "not found"},
		},
		{
			name:     "a forbidden read is named as itself, not as a broken cluster",
			data:     goodSecret(),
			forbid:   true,
			wantSaid: []string{"could not be read", "forbidden"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := fromSinkResolver(t, tc.data, fromSinkClickHouse(fixtureAddr))
			if tc.forbid {
				resolver.Clients.Typed.(*k8sfake.Clientset).PrependReactor("get", "secrets",
					func(clienttesting.Action) (bool, runtime.Object, error) {
						return true, nil, apierrors.NewForbidden(
							corev1.Resource("secrets"), fromSinkSecret, errors.New("no"))
					})
			}

			derived, err := resolver.ProfileFromSink(t.Context(), clickHouseSinkRef, ProfileOverrides{})
			if err != nil {
				t.Fatalf("ProfileFromSink: %v", err)
			}
			// Written either way: the profile's password comes from the reader's
			// own environment, not from this Secret.
			if derived.Profile.ClickHouse.Addr == "" {
				t.Fatal("no profile was derived")
			}
			if derived.Credential == nil {
				t.Fatal("the credential the sink names was not reported")
			}
			if got := derived.Credential.String(); got != fixtureNamespace+"/"+fromSinkSecret {
				t.Errorf("Credential = %q, want the Secret the sink names", got)
			}

			if len(tc.wantSaid) == 0 {
				if derived.CredentialUnverified != "" {
					t.Errorf("a readable Secret was reported unverified: %q", derived.CredentialUnverified)
				}
				return
			}
			for _, want := range tc.wantSaid {
				if !strings.Contains(derived.CredentialUnverified, want) {
					t.Errorf("CredentialUnverified = %q, want it to mention %q",
						derived.CredentialUnverified, want)
				}
			}
			// And it is said in the message, not only in the struct.
			if !strings.Contains(derived.Explain(false), derived.CredentialUnverified) {
				t.Error("the explanation does not carry what could not be checked")
			}
		})
	}
}

// TestProfileFromSinkTransfersAnArchiveDirectly.
//
// An object store has no address that resolves only inside a cluster: a bucket is
// a bucket from anywhere. Everything transfers, and the credentials that are
// missing from the stanza are missing on purpose.
func TestProfileFromSinkTransfersAnArchiveDirectly(t *testing.T) {
	resolver := fromSinkResolver(t, nil, fromSinkS3("https://minio.example.com:9000"))

	derived, err := resolver.ProfileFromSink(t.Context(), s3SinkRef, ProfileOverrides{})
	if err != nil {
		t.Fatalf("ProfileFromSink: %v", err)
	}
	if derived.Profile.Backend != BackendS3 || derived.Profile.S3 == nil {
		t.Fatalf("an S3Sink produced %+v, want an s3 stanza", derived.Profile)
	}
	if err := derived.Profile.Validate(); err != nil {
		t.Errorf("the derived profile does not validate: %v", err)
	}
	stanza := derived.Profile.S3
	if stanza.Bucket != fromSinkBucket || stanza.Prefix != fromSinkPrefix ||
		stanza.Region != fromSinkRegion || stanza.Endpoint != "https://minio.example.com:9000" ||
		!stanza.ForcePathStyle {
		t.Errorf("the archive did not transfer: %+v", stanza)
	}
	if derived.EndpointInternal {
		t.Error("a public endpoint was called cluster-internal")
	}
	if derived.Credential != nil {
		t.Errorf("a sink with ambient credentials reported the Secret %s", derived.Credential)
	}
	// No Secret was read at all: an S3 profile carries no credentials, so demanding
	// the permission would be asking for something to throw away.
	for _, action := range resolver.Clients.Typed.(*k8sfake.Clientset).Actions() {
		if action.GetResource().Resource == "secrets" {
			t.Errorf("deriving an S3 profile read a Secret: %v", action)
		}
	}
}

// TestProfileFromSinkNotesAnArchiveOnlyTheClusterCanReach.
//
// It is written unchanged — an endpoint carries a scheme and a certificate name,
// and substituting a forwarded port for it is a guess with a TLS failure at the
// end. What must not happen is silence about it (Invariant 9).
func TestProfileFromSinkNotesAnArchiveOnlyTheClusterCanReach(t *testing.T) {
	const endpoint = "http://minio.kuberecord-system.svc:9000"
	resolver := fromSinkResolver(t, nil, fromSinkS3(endpoint))

	derived, err := resolver.ProfileFromSink(t.Context(), s3SinkRef, ProfileOverrides{})
	if err != nil {
		t.Fatalf("ProfileFromSink: %v", err)
	}
	if !derived.EndpointInternal {
		t.Fatalf("%s was not recognised as cluster-internal", endpoint)
	}
	if derived.Profile.S3.Endpoint != endpoint {
		t.Errorf("s3.endpoint = %q, want it written unchanged", derived.Profile.S3.Endpoint)
	}
	if !strings.Contains(derived.Explain(false), endpoint) {
		t.Error("the explanation does not name the endpoint it is warning about")
	}
}

// TestProfileFromSinkRefusesClickHouseOverridesForAnArchive.
//
// The command refuses each of these flags by name before the cluster is
// contacted; this is the guard that keeps the exported API honest for any other
// caller, and it must not write a profile that silently dropped them.
func TestProfileFromSinkRefusesClickHouseOverridesForAnArchive(t *testing.T) {
	resolver := fromSinkResolver(t, nil, fromSinkS3(""))

	_, err := resolver.ProfileFromSink(t.Context(), s3SinkRef, ProfileOverrides{Addr: "127.0.0.1:9000"})
	if err == nil {
		t.Fatal("an endpoint override against an object store was accepted")
	}
	if !strings.Contains(err.Error(), "object store") {
		t.Errorf("the refusal does not say why: %v", err)
	}
}

// TestProfileFromSinkClassifiesAMissingSinkTheWayAQueryDoes.
//
// Reading a sink does not change according to why it was read, so a name that is
// not there fails through getSink's own classification rather than through a
// second one written for this command.
func TestProfileFromSinkClassifiesAMissingSinkTheWayAQueryDoes(t *testing.T) {
	resolver := fromSinkResolver(t, goodSecret())

	_, err := resolver.ProfileFromSink(t.Context(),
		SinkRef{Kind: KindClickHouseSink, Name: "nope"}, ProfileOverrides{})
	if err == nil {
		t.Fatal("a sink that does not exist produced a profile")
	}
	if !strings.Contains(err.Error(), "cannot read ClickHouseSink/nope") {
		t.Errorf("the failure does not name what it could not read: %v", err)
	}
}

// TestExplainReadsAsProse pins every shape of the message.
//
// Golden files for the same reason the unreachable-backend block has them: this is
// documentation that happens to be compiled, it will be edited by people improving
// the wording, and the thing worth reviewing is the paragraph rather than the
// diff of a format string.
func TestExplainReadsAsProse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sink    *unstructured.Unstructured
		ref     SinkRef
		over    ProfileOverrides
		secret  map[string][]byte
		forbid  bool
		colored bool
	}{
		{name: "rewritten", sink: fromSinkClickHouse(fixtureAddr), ref: clickHouseSinkRef, secret: goodSecret()},
		{
			name: "rewritten-colored", sink: fromSinkClickHouse(fixtureAddr), ref: clickHouseSinkRef,
			secret: goodSecret(), colored: true,
		},
		{
			name: "public", sink: fromSinkClickHouse("clickhouse.example.com:9440"),
			ref: clickHouseSinkRef, secret: goodSecret(),
		},
		{
			name: "overridden", sink: fromSinkClickHouse(fixtureAddr), ref: clickHouseSinkRef,
			over:   ProfileOverrides{Addr: "127.0.0.1:19000", Username: "kuberecord_ro", PasswordFile: "/run/ch"},
			secret: goodSecret(),
		},
		{name: "unverified", sink: fromSinkClickHouse(fixtureAddr), ref: clickHouseSinkRef, forbid: true},
		{name: "archive", sink: fromSinkS3("https://minio.example.com:9000"), ref: s3SinkRef},
		{name: "archive-internal", sink: fromSinkS3("http://minio.kuberecord-system.svc:9000"), ref: s3SinkRef},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := fromSinkResolver(t, tc.secret, tc.sink)
			if tc.forbid {
				resolver.Clients.Typed.(*k8sfake.Clientset).PrependReactor("get", "secrets",
					func(clienttesting.Action) (bool, runtime.Object, error) {
						return true, nil, apierrors.NewForbidden(
							corev1.Resource("secrets"), fromSinkSecret, errors.New("no"))
					})
			}

			derived, err := resolver.ProfileFromSink(t.Context(), tc.ref, tc.over)
			if err != nil {
				t.Fatalf("ProfileFromSink: %v", err)
			}
			assertFromSinkGolden(t, tc.name, derived.Explain(tc.colored))
		})
	}
}

// TestExplainColoursNothingButColour.
//
// The painted rendering must differ from the plain one only by escape sequences:
// a palette that also changed the words would make the golden files a description
// of one terminal.
func TestExplainColoursNothingButColour(t *testing.T) {
	resolver := fromSinkResolver(t, goodSecret(), fromSinkClickHouse(fixtureAddr))

	derived, err := resolver.ProfileFromSink(t.Context(), clickHouseSinkRef, ProfileOverrides{})
	if err != nil {
		t.Fatalf("ProfileFromSink: %v", err)
	}

	painted := derived.Explain(true)
	if !strings.Contains(painted, ansiReset) {
		t.Fatal("the coloured rendering carries no escape sequences at all")
	}
	stripped := strings.NewReplacer(ansiReset, "", ansiBold, "", ansiDim, "").Replace(painted)
	if plain := derived.Explain(false); stripped != plain {
		t.Errorf("colour changes more than colour.\n--- plain ---\n%s\n--- stripped ---\n%s",
			plain, stripped)
	}
}

// assertFromSinkGolden compares a rendering against its checked-in file.
func assertFromSinkGolden(t *testing.T, name, got string) {
	t.Helper()

	path := filepath.Join("testdata", "fromsink", name+".golden")
	if *updateDiagnoseGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("creating the golden directory: %v", err)
		}
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
		return
	}

	want, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("reading %s (run `go test ./internal/cli/resolve/ -update` to create it): %v", path, err)
	}
	if got != string(want) {
		t.Errorf("the rendering of %s changed.\n--- want ---\n%s\n--- got ---\n%s", name, want, got)
	}
}
