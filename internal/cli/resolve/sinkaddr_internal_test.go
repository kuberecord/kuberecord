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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
)

// The override is tested from inside the package for the half a caller cannot
// see: the dial configuration. The whole promise of --sink-addr is about the four
// fields it does *not* touch, and resolve.Backend exposes an opened engine rather
// than the settings it was opened with — so an end-to-end test can assert the
// notice and the exit code, and only this one can assert that the database and the
// credential still came from the custom resource. The chain-level behaviour is in
// internal/cli/sinkaddr_test.go.

// The forwarded endpoint every case here overrides with, and the sink's own
// settings, which must survive it.
const (
	forwardedAddr = "127.0.0.1:9000"
	sinkUsername  = "kuberecord"
	sinkTimeout   = 7 * time.Second
)

// overrideResolver builds a resolver holding one Secret and the given override.
func overrideResolver(t *testing.T, sinkAddr string) *BackendResolver {
	t.Helper()
	const secretName = "clickhouse-credentials"
	return &BackendResolver{
		Flags:     &options.GlobalFlags{SinkAddr: sinkAddr},
		InvokedAs: options.StandaloneName,
		Config:    &Config{OperatorNamespace: fixtureNamespace},
		Clients: &Clients{Typed: k8sfake.NewClientset(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: fixtureNamespace},
			Data:       map[string][]byte{secretKeyPassword: []byte(fixturePassword)},
		})},
	}
}

// overrideSink is the quickstart's ClickHouseSink, with every field the override
// must leave alone set to something distinguishable.
func overrideSink() *v1alpha1.ClickHouseSink {
	return &v1alpha1.ClickHouseSink{Spec: v1alpha1.ClickHouseSinkSpec{
		Connection: v1alpha1.ConnectionSpec{
			Addr:                 fixtureAddr,
			Database:             DefaultClickHouseDatabase,
			Username:             sinkUsername,
			DialTimeout:          &metav1.Duration{Duration: sinkTimeout},
			CredentialsSecretRef: v1alpha1.SecretReference{Name: "clickhouse-credentials"},
		},
	}}
}

// TestSinkAddrReplacesTheEndpointAndNothingElse is the whole of D25 as an
// assertion.
//
// It is written as a comparison against the same resolution without the flag
// rather than as a list of expected values, because the property is "one field
// differs" and a list would keep passing if a later edit started sourcing the
// database from somewhere else that happened to produce the same string.
func TestSinkAddrReplacesTheEndpointAndNothingElse(t *testing.T) {
	ref := SinkRef{Kind: KindClickHouseSink, Name: "default"}

	plain, err := overrideResolver(t, "").clickHouseTarget(t.Context(), ref, overrideSink())
	if err != nil {
		t.Fatalf("resolving the sink with no override: %v", err)
	}
	overridden, err := overrideResolver(t, forwardedAddr).clickHouseTarget(t.Context(), ref, overrideSink())
	if err != nil {
		t.Fatalf("resolving the sink with --%s: %v", options.FlagSinkAddr, err)
	}

	if plain.clickhouse.Addr != fixtureAddr {
		t.Fatalf("the unoverridden dial reads %q, want the sink's own %q — this test compares "+
			"against nothing otherwise", plain.clickhouse.Addr, fixtureAddr)
	}
	if overridden.clickhouse.Addr != forwardedAddr {
		t.Errorf("dial address %q, want the override %q", overridden.clickhouse.Addr, forwardedAddr)
	}

	// Every other field of the dial, named individually so a failure says which
	// one moved rather than that two structs differ.
	for _, field := range []struct {
		name      string
		got, want any
	}{
		{"database", overridden.clickhouse.Database, plain.clickhouse.Database},
		{"username", overridden.clickhouse.Username, plain.clickhouse.Username},
		{"password", overridden.clickhouse.Password, plain.clickhouse.Password},
		{"TLS", overridden.clickhouse.TLS, plain.clickhouse.TLS},
		{"dial timeout", overridden.clickhouse.DialTimeout, plain.clickhouse.DialTimeout},
	} {
		if field.got != field.want {
			t.Errorf("--%s changed the %s to %v, want the sink's own %v",
				options.FlagSinkAddr, field.name, field.got, field.want)
		}
	}

	// Non-vacuity: the comparison above is worth nothing if the sink's own values
	// were never resolved in the first place.
	if plain.clickhouse.Database != DefaultClickHouseDatabase ||
		plain.clickhouse.Username != sinkUsername ||
		plain.clickhouse.Password != fixturePassword ||
		plain.clickhouse.DialTimeout != sinkTimeout {
		t.Fatalf("the sink's own settings were not resolved (%+v), so the comparison above "+
			"compared defaults with defaults", plain.clickhouse)
	}
}

// TestTheDescriptionCarriesTheOverride.
//
// The description is what the notice on stderr prints, and the notice is the
// property the resolution chain exists to keep: a line reading `discovered
// ClickHouseSink/default (127.0.0.1:9000/kuberecord)` would be false about the
// custom resource it names, because that resource records no such address.
func TestTheDescriptionCarriesTheOverride(t *testing.T) {
	ref := SinkRef{Kind: KindClickHouseSink, Name: "default"}

	overridden, err := overrideResolver(t, forwardedAddr).clickHouseTarget(t.Context(), ref, overrideSink())
	if err != nil {
		t.Fatalf("resolving the sink: %v", err)
	}
	want := "ClickHouseSink/default (" + forwardedAddr + "/" + DefaultClickHouseDatabase +
		", address from --" + options.FlagSinkAddr + ")"
	if overridden.description != want {
		t.Errorf("description %q, want %q", overridden.description, want)
	}

	plain, err := overrideResolver(t, "").clickHouseTarget(t.Context(), ref, overrideSink())
	if err != nil {
		t.Fatalf("resolving the sink with no override: %v", err)
	}
	if strings.Contains(plain.description, options.FlagSinkAddr) {
		t.Errorf("an unoverridden description names the flag anyway: %q", plain.description)
	}
}

// TestAnOverriddenAddressDisarmsTheDiagnostic.
//
// The unreachable-backend message says that a name resolves inside the cluster and
// nowhere else. Printed about 127.0.0.1 it would be false, and it would tell
// somebody whose port-forward had died to go and start the one they already
// started. The diagnosis reads the *effective* address, so an override that points
// at a forwarded port takes the message out of play entirely.
func TestAnOverriddenAddressDisarmsTheDiagnostic(t *testing.T) {
	ref := SinkRef{Kind: KindClickHouseSink, Name: "default"}

	overridden, err := overrideResolver(t, forwardedAddr).clickHouseTarget(t.Context(), ref, overrideSink())
	if err != nil {
		t.Fatalf("resolving the sink: %v", err)
	}
	if overridden.diagnosis.addr != forwardedAddr {
		t.Errorf("the diagnosis describes %q, want the address actually dialled %q",
			overridden.diagnosis.addr, forwardedAddr)
	}
	if overridden.diagnosis.armed() {
		t.Errorf("the cluster-internal diagnostic is armed for %q, which is a loopback address "+
			"this process can reach", overridden.diagnosis.addr)
	}
	if wrapped := overridden.diagnosis.wrap(connectionRefused()); wrapped == nil {
		t.Fatal("wrap dropped the error")
	} else if _, diagnosed := wrapped.(*UnreachableSinkError); diagnosed {
		t.Errorf("a refused connection to the forwarded port is reported as a cluster-internal "+
			"address problem: %v", wrapped)
	}

	// Non-vacuity: the same sink without the override is still diagnosed, so the
	// assertions above are the override's doing rather than the fixture's.
	plain, err := overrideResolver(t, "").clickHouseTarget(t.Context(), ref, overrideSink())
	if err != nil {
		t.Fatalf("resolving the sink with no override: %v", err)
	}
	if !plain.diagnosis.armed() {
		t.Fatal("the unoverridden sink is not diagnosed either, so this test proves nothing")
	}
}

// TestValidateSinkAddrTakesShapeAndNothingElse.
//
// The rule is deliberately narrow: the value has to look like host:port, and the
// name is never resolved. Resolution here would succeed or fail for the same
// reason the dial is about to, which is two error paths for one problem — and the
// second reports it worse, because by then nothing knows the address came from a
// flag.
func TestValidateSinkAddrTakesShapeAndNothingElse(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  []string
	}{
		{name: "not given at all", value: ""},
		{name: "loopback and a port", value: "127.0.0.1:9000"},
		{name: "a name and a port", value: "clickhouse.example.com:9440"},
		{name: "localhost", value: "localhost:9000"},
		{name: "a bracketed IPv6 literal", value: "[::1]:9000"},
		{name: "the highest port", value: "127.0.0.1:65535"},
		{
			name:  "a bare host names the expected form",
			value: "clickhouse",
			want:  []string{"names no port", "host:port", exampleSinkAddr},
		},
		{
			name:  "a cluster-internal name with no port is still a bare host",
			value: "clickhouse.kuberecord-system.svc",
			want:  []string{"names no port", "host:port"},
		},
		{
			name:  "no host",
			value: ":9000",
			want:  []string{"names no host", "host:port"},
		},
		{
			name:  "a trailing colon and no port",
			value: "127.0.0.1:",
			want:  []string{"names no port", "host:port"},
		},
		{
			name:  "a mistyped port",
			value: "127.0.0.1:900O",
			want:  []string{`the port "900O"`, "between 1 and 65535"},
		},
		{
			name:  "port zero",
			value: "127.0.0.1:0",
			want:  []string{"between 1 and 65535"},
		},
		{
			name:  "a port past the end of the range",
			value: "127.0.0.1:65536",
			want:  []string{"between 1 and 65535"},
		},
		{
			name:  "an unbracketed IPv6 literal says how to bracket it",
			value: "::1:9000",
			want:  []string{"not host:port", "[::1]:9000"},
		},
		{
			name:  "a URL names its scheme",
			value: "http://127.0.0.1:8123",
			want:  []string{`scheme "http"`, "no scheme", exampleSinkAddr},
		},
		{
			name:  "the native-protocol URL form is refused the same way",
			value: "clickhouse://user@host:9000/db",
			want:  []string{`scheme "clickhouse"`, "no scheme"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSinkAddr(tc.value)
			if len(tc.want) == 0 {
				if err != nil {
					t.Fatalf("validateSinkAddr(%q) = %v, want it accepted", tc.value, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateSinkAddr(%q) accepted a value that is not host:port", tc.value)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the message does not mention %q:\n%v", want, err)
				}
			}
			if !strings.Contains(err.Error(), "--"+options.FlagSinkAddr) {
				t.Errorf("the message does not name the flag it is about:\n%v", err)
			}
			if got := exit.CodeFor(err); got != exit.UsageError {
				t.Errorf("exit code %d, want %d: a malformed flag value is something the user "+
					"typed, and a wrapper script must not retry it", got, exit.UsageError)
			}
		})
	}
}

// TestAProfileTakesTheOverrideOnlyWhenItIsClickHouse.
//
// A profile is step 3, so a ClickHouse profile that is merely active — named by
// the file's currentProfile rather than by --profile — is what the override has to
// reach for the flag to work at all on a machine that has one. The archive
// backends have no endpoint of this shape, and refusing them is what keeps the
// flag from parsing and silently doing nothing.
func TestAProfileTakesTheOverrideOnlyWhenItIsClickHouse(t *testing.T) {
	clickHouse := Profile{
		Backend: BackendClickHouse,
		ClickHouse: &ClickHouseProfile{
			Addr: "clickhouse.kuberecord-system.svc:9000", Database: "audit",
			Username: sinkUsername, TLS: true,
		},
	}

	chosen, err := targetFromProfile("prod", clickHouse, forwardedAddr)
	if err != nil {
		t.Fatalf("overriding a ClickHouse profile: %v", err)
	}
	if chosen.clickhouse.Addr != forwardedAddr {
		t.Errorf("dial address %q, want the override %q", chosen.clickhouse.Addr, forwardedAddr)
	}
	if chosen.clickhouse.Database != "audit" || chosen.clickhouse.Username != sinkUsername ||
		!chosen.clickhouse.TLS {
		t.Errorf("--%s changed more than the endpoint: %+v", options.FlagSinkAddr, chosen.clickhouse)
	}
	want := "prod (ClickHouse at " + forwardedAddr + "/audit, address from --" +
		options.FlagSinkAddr + ")"
	if chosen.description != want {
		t.Errorf("description %q, want %q", chosen.description, want)
	}

	for _, tc := range []struct {
		name    string
		profile Profile
	}{
		{
			name:    "an s3 profile",
			profile: Profile{Backend: BackendS3, S3: &S3Profile{Bucket: "acme-audit"}},
		},
		{
			name:    "a local profile",
			profile: Profile{Backend: BackendLocal, Local: &LocalProfile{Path: "/archives"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := targetFromProfile("archive", tc.profile, forwardedAddr)
			if err == nil {
				t.Fatalf("--%s was accepted against the %s backend", options.FlagSinkAddr, tc.profile.Backend)
			}
			for _, want := range []string{
				"--" + options.FlagSinkAddr, "archive", string(tc.profile.Backend),
				string(BackendClickHouse), "--" + options.FlagSource,
			} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the message does not mention %q:\n%v", want, err)
				}
			}
			if got := exit.CodeFor(err); got != exit.UsageError {
				t.Errorf("exit code %d, want %d", got, exit.UsageError)
			}
		})
	}

	// Without the flag, every backend still resolves as it did.
	for _, profile := range []Profile{
		clickHouse,
		{Backend: BackendS3, S3: &S3Profile{Bucket: "acme-audit"}},
		{Backend: BackendLocal, Local: &LocalProfile{Path: "/archives"}},
	} {
		if _, err := targetFromProfile("any", profile, ""); err != nil {
			t.Errorf("the %s backend stopped resolving without the flag: %v", profile.Backend, err)
		}
	}
}
