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
	"os"
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
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// The file --from-sink writes is asserted from inside the package, because the
// write itself is unexported and the two halves have to be tested together: a
// derivation that carried no password is worth little if the write puts one there
// anyway, and only the pair of them touches disk.
//
// The flag conflicts are the other half and are driven through the whole binary in
// configcmd_test.go, since they are refused before the cluster is contacted and
// therefore need no cluster at all.

// The fixture cluster: the quickstart's sink, an archive beside it, and the Secret
// the first one names.
const (
	sinkNamespace = "kuberecord-system"
	sinkSecret    = "clickhouse-credentials"
	// sinkPassword is the value that must reach neither the file nor either
	// stream. It is distinctive so that asserting its absence is asserting
	// something a substring search can find.
	sinkPassword = "correct-horse-battery-staple"
	// internalAddr is what examples/quickstart/sink.yaml records, and what every
	// in-cluster install records: the address the operator itself dials.
	internalAddr = "clickhouse.kuberecord-quickstart.svc:9000"
)

// fromSinkFixture builds a resolver over the fixture cluster, with the
// configuration file pointed at a temporary directory.
func fromSinkFixture(t *testing.T) (*resolve.BackendResolver, genericiooptions.IOStreams, string) {
	t.Helper()

	home := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", home)
	configPath := filepath.Join(home, resolve.ConfigDirName, resolve.ConfigFileName)

	clickHouse := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       resolve.KindClickHouseSink,
		"metadata":   map[string]any{"name": "default"},
		"spec": map[string]any{"connection": map[string]any{
			"addr":                 internalAddr,
			"database":             resolve.DefaultClickHouseDatabase,
			"username":             "kuberecord",
			"credentialsSecretRef": map[string]any{"name": sinkSecret},
		}},
	}}
	archive := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       resolve.KindS3Sink,
		"metadata":   map[string]any{"name": "archive"},
		"spec": map[string]any{
			"bucket": "acme-audit", "prefix": "kuberecord",
			"region": "eu-west-1", "forcePathStyle": true,
		},
	}}
	gvr := func(resource string) schema.GroupVersionResource {
		return schema.GroupVersionResource{
			Group: v1alpha1.GroupVersion.Group, Version: v1alpha1.GroupVersion.Version, Resource: resource,
		}
	}
	listKinds := map[schema.GroupVersionResource]string{
		gvr("clickhousesinks"): resolve.KindClickHouseSink + "List",
		gvr("s3sinks"):         resolve.KindS3Sink + "List",
	}

	streams := genericiooptions.IOStreams{
		In: strings.NewReader(""), Out: &strings.Builder{}, ErrOut: &strings.Builder{},
	}
	resolver := &resolve.BackendResolver{
		Streams:    streams,
		InvokedAs:  options.StandaloneName,
		Config:     &resolve.Config{OperatorNamespace: sinkNamespace},
		ConfigPath: configPath,
		Clients: &resolve.Clients{
			Dynamic: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
				runtime.NewScheme(), listKinds, clickHouse, archive),
			Typed: k8sfake.NewClientset(&corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: sinkSecret, Namespace: sinkNamespace},
				Data:       map[string][]byte{"password": []byte(sinkPassword)},
			}),
		},
	}
	return resolver, streams, configPath
}

// derive runs the two halves the command runs, and returns what each stream saw.
func derive(t *testing.T, ref resolve.SinkRef, name string) (stdout, stderr string) {
	t.Helper()

	resolver, streams, _ := fromSinkFixture(t)
	derived, err := resolver.ProfileFromSink(t.Context(), ref, resolve.ProfileOverrides{})
	if err != nil {
		t.Fatalf("ProfileFromSink: %v", err)
	}
	if err := writeProfile(profileWrite{
		name:        name,
		profile:     derived.Profile,
		explanation: derived.Explain(false),
		nextStep:    true,
		invokedAs:   options.StandaloneName,
	}, streams); err != nil {
		t.Fatalf("writeProfile: %v", err)
	}
	return streams.Out.(*strings.Builder).String(), streams.ErrOut.(*strings.Builder).String()
}

// TestFromSinkWritesAUsableProfileAndNoCredential is the acceptance criterion's
// central pair: a complete stanza, and no plaintext password anywhere near it.
func TestFromSinkWritesAUsableProfileAndNoCredential(t *testing.T) {
	stdout, stderr := derive(t, resolve.SinkRef{Kind: resolve.KindClickHouseSink, Name: "default"}, "local")

	path := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), resolve.ConfigDirName, resolve.ConfigFileName)
	written, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("reading the configuration the command wrote: %v", err)
	}
	// The assertion the acceptance criterion names, made against the bytes on disk
	// rather than against the struct: this is the path standing next to a Secret
	// it can read, and the file is where an inline write would land.
	if strings.Contains(string(written), sinkPassword) {
		t.Errorf("the configuration file holds the password in plain text:\n%s", written)
	}
	if strings.Contains(stderr, sinkPassword) || strings.Contains(stdout, sinkPassword) {
		t.Error("the password reached one of the streams")
	}
	if stdout != "" {
		t.Errorf("config set-profile wrote to stdout, which belongs to data: %q", stdout)
	}

	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig of what was written: %v", err)
	}
	profile, ok := cfg.Profiles["local"]
	if !ok {
		t.Fatalf("the file holds no profile named local: %+v", cfg)
	}
	if profile.Backend != resolve.BackendClickHouse || profile.ClickHouse == nil {
		t.Fatalf("the stanza is not a ClickHouse one: %+v", profile)
	}
	// Complete: every field the backend needs, with the cluster-internal address
	// replaced by the forwarded port it expects.
	if profile.ClickHouse.Addr != "127.0.0.1:9000" {
		t.Errorf("clickhouse.addr = %q, want the forwarded port", profile.ClickHouse.Addr)
	}
	if profile.ClickHouse.Database != resolve.DefaultClickHouseDatabase ||
		profile.ClickHouse.Username != "kuberecord" ||
		profile.ClickHouse.PasswordEnv == "" {
		t.Errorf("the stanza is not complete: %+v", profile.ClickHouse)
	}

	// And the rewrite is announced, with the command that makes it work.
	for _, want := range []string{
		internalAddr,
		"resolves inside the cluster and nowhere else",
		"kubectl port-forward -n kuberecord-quickstart svc/clickhouse 9000:9000",
		`wrote profile "local"`,
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr does not mention %q:\n%s", want, stderr)
		}
	}
}

// TestFromSinkWritesAnArchiveStanza.
//
// An S3Sink transfers directly, and the resulting profile carries no credentials
// at all — which is not an omission, it is what S3Profile documents.
func TestFromSinkWritesAnArchiveStanza(t *testing.T) {
	_, stderr := derive(t, resolve.SinkRef{Kind: resolve.KindS3Sink, Name: "archive"}, "archive")

	path := filepath.Join(os.Getenv("XDG_CONFIG_HOME"), resolve.ConfigDirName, resolve.ConfigFileName)
	cfg, err := resolve.LoadConfig(path)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	profile, ok := cfg.Profiles["archive"]
	if !ok {
		t.Fatalf("the file holds no profile named archive: %+v", cfg)
	}
	if profile.Backend != resolve.BackendS3 || profile.S3 == nil {
		t.Fatalf("the stanza is not an S3 one: %+v", profile)
	}
	if profile.S3.Bucket != "acme-audit" || profile.S3.Prefix != "kuberecord" ||
		profile.S3.Region != "eu-west-1" || !profile.S3.ForcePathStyle {
		t.Errorf("the archive did not transfer: %+v", profile.S3)
	}
	if !strings.Contains(stderr, "AWS chain") {
		t.Errorf("stderr does not say where credentials come from:\n%s", stderr)
	}
}

// TestFromSinkLeavesTheActiveProfileAlone.
//
// Writing a profile is not choosing one. A machine that already reads `prod` must
// go on reading it until somebody says otherwise — and the line that says how is
// printed rather than the switch being made.
func TestFromSinkLeavesTheActiveProfileAlone(t *testing.T) {
	resolver, streams, configPath := fromSinkFixture(t)

	existing := &resolve.Config{
		CurrentProfile: "prod",
		Profiles: map[string]resolve.Profile{"prod": {
			Backend:    resolve.BackendClickHouse,
			ClickHouse: &resolve.ClickHouseProfile{Addr: "clickhouse.example:9000"},
		}},
	}
	if err := resolve.SaveConfig(configPath, existing); err != nil {
		t.Fatalf("seeding the configuration: %v", err)
	}

	derived, err := resolver.ProfileFromSink(t.Context(),
		resolve.SinkRef{Kind: resolve.KindClickHouseSink, Name: "default"}, resolve.ProfileOverrides{})
	if err != nil {
		t.Fatalf("ProfileFromSink: %v", err)
	}
	if err := writeProfile(profileWrite{
		name: "local", profile: derived.Profile, explanation: derived.Explain(false),
		nextStep: true, invokedAs: options.StandaloneName,
	}, streams); err != nil {
		t.Fatalf("writeProfile: %v", err)
	}

	cfg, err := resolve.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("resolve.LoadConfig: %v", err)
	}
	if cfg.CurrentProfile != "prod" {
		t.Errorf("currentProfile = %q, want it left at prod", cfg.CurrentProfile)
	}
	if _, ok := cfg.Profiles["local"]; !ok {
		t.Error("the derived profile was not written")
	}

	stderr := streams.ErrOut.(*strings.Builder).String()
	if strings.Contains(stderr, "is now the active profile") {
		t.Errorf("the active profile was switched silently:\n%s", stderr)
	}
	if !strings.Contains(stderr, "config use-profile local") {
		t.Errorf("stderr does not print the next step to run:\n%s", stderr)
	}
}

// TestFromSinkIntoAnEmptyFileActivatesAndSaysSo.
//
// The first profile in an empty file becomes the active one — the rule the
// individual-flag path has always followed, kept here because it is announced and
// because requiring a second command to make the only profile usable is ceremony
// with no decision in it. What must not appear is the next-step line, which would
// be telling the reader to do something already done.
func TestFromSinkIntoAnEmptyFileActivatesAndSaysSo(t *testing.T) {
	_, stderr := derive(t, resolve.SinkRef{Kind: resolve.KindClickHouseSink, Name: "default"}, "local")

	if !strings.Contains(stderr, `"local" is now the active profile`) {
		t.Errorf("the only profile in an empty file was not activated:\n%s", stderr)
	}
	if strings.Contains(stderr, "config use-profile local") {
		t.Errorf("the next-step line was printed for a profile that is already active:\n%s", stderr)
	}
}
