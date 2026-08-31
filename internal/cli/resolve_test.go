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
	"os"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
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
	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// The fixture cluster these tests resolve against: one operator, one Secret, and
// whichever sinks a case seeds.
const (
	operatorNamespace = "kuberecord-system"
	secretName        = "clickhouse-credentials"
	// theSecret is the value that must never appear in output, at any verbosity.
	// It is distinctive so that a test asserting its absence is asserting
	// something a substring search can actually find.
	theSecret = "correct-horse-battery-staple"

	// theCluster is the kuberecord cluster identity the fixture cluster records
	// under, named once so that a case resolving it from the wrong step of the
	// chain is visibly wrong rather than merely different.
	theCluster = "prod-eu-1"
)

// sinkGVR builds the resource a sink kind lives in, spelled independently of the
// CLI's own constants so a rename there is caught here rather than agreed with.
func sinkGVR(resource string) schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group: v1alpha1.GroupVersion.Group, Version: v1alpha1.GroupVersion.Version, Resource: resource,
	}
}

// clickHouseSink builds a ClickHouseSink custom resource as the API server would
// hand one back, in the database the operator's own default names.
func clickHouseSink(name, addr string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       resolve.KindClickHouseSink,
		"metadata":   map[string]any{"name": name},
		"spec": map[string]any{
			"connection": map[string]any{
				"addr":                 addr,
				"database":             resolve.DefaultClickHouseDatabase,
				"username":             "kuberecord",
				"dialTimeout":          "5s",
				"credentialsSecretRef": map[string]any{"name": secretName},
			},
		},
	}}
}

// s3Sink builds an S3Sink custom resource. Its credentials are ambient, which is
// the supported and, on a cloud provider, preferred state — and the shape that
// needs no Secret permission at all.
func s3Sink(name, bucket, prefix string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": v1alpha1.GroupVersion.String(),
		"kind":       resolve.KindS3Sink,
		"metadata":   map[string]any{"name": name},
		"spec": map[string]any{
			"bucket":   bucket,
			"prefix":   prefix,
			"region":   "eu-west-1",
			"rotation": map[string]any{"maxObjectAge": "5m"},
		},
	}}
}

// operatorDeployment builds the operator's own Deployment, labelled the way both
// install paths label it, carrying the cluster identity as the chart does — in the
// environment rather than as an argument.
func operatorDeployment(namespace, clusterID string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kuberecord-controller-manager",
			Namespace: namespace,
			Labels: map[string]string{
				"control-plane":          "controller-manager",
				"app.kubernetes.io/name": "kuberecord",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name: "manager",
					Env:  []corev1.EnvVar{{Name: "CLUSTER_ID", Value: clusterID}},
				}}},
			},
		},
	}
}

// credentialsSecret is the Secret a ClickHouseSink references.
func credentialsSecret(namespace string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
		Data:       map[string][]byte{"password": []byte(theSecret)},
	}
}

// fakeClients assembles the Kubernetes access from fixtures.
//
// client-go's own fakes rather than a hand-rolled seam: the messages under test
// are decided by how an API error is classified, and only a real apierrors value
// travelling through a real client exercises that.
func fakeClients(sinks []*unstructured.Unstructured, objects ...runtime.Object) *resolve.Clients {
	listKinds := map[schema.GroupVersionResource]string{
		sinkGVR("clickhousesinks"): resolve.KindClickHouseSink + "List",
		sinkGVR("s3sinks"):         resolve.KindS3Sink + "List",
	}
	seeded := make([]runtime.Object, 0, len(sinks))
	for _, sink := range sinks {
		seeded = append(seeded, sink)
	}
	return &resolve.Clients{
		Dynamic: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
			runtime.NewScheme(), listKinds, seeded...),
		Typed: k8sfake.NewClientset(objects...),
	}
}

// resolverOver builds a resolver whose cluster is the fixture and whose
// configuration is the one given.
func resolverOver(t *testing.T, cfg *resolve.Config, clients *resolve.Clients, args ...string) (
	*resolve.BackendResolver, *strings.Builder,
) {
	t.Helper()

	io, _, _ := streams()
	var notices strings.Builder
	io.ErrOut = &notices

	root, flags := cli.NewRootCommand(options.StandaloneName, io)
	root.SetArgs(append([]string{"--kubeconfig", kubeconfigPath}, args...))
	// Parse the flags without running anything: the resolver reads the parsed
	// surface, and driving it through a command would need a command this task
	// does not add.
	if err := root.ParseFlags(append([]string{"--kubeconfig", kubeconfigPath}, args...)); err != nil {
		t.Fatalf("parsing %v: %v", args, err)
	}

	if cfg == nil {
		cfg = &resolve.Config{}
	}
	return &resolve.BackendResolver{
		Flags:      flags,
		Streams:    io,
		InvokedAs:  options.StandaloneName,
		Config:     cfg,
		ConfigPath: filepath.Join(t.TempDir(), "config.yaml"),
		Clients:    clients,
	}, &notices
}

// closeBackend releases a resolved backend, reporting a failure rather than
// discarding it: a leaked connection or an unreleased source is a defect worth
// hearing about even in a test that was about to fail for another reason.
func closeBackend(t *testing.T, backend *resolve.Backend) {
	t.Helper()
	if backend == nil {
		return
	}
	if err := backend.Close(); err != nil {
		t.Errorf("closing the backend: %v", err)
	}
}

// localArchive writes an archive holding one cluster's keys and returns its
// directory.
func localArchive(t *testing.T, clusterIDs ...string) string {
	t.Helper()

	dir := t.TempDir()
	for _, id := range clusterIDs {
		key := filepath.Join(dir, "format=jsonl-v1", "cluster_id="+id, "date=2026-03-01", "hour=09")
		if err := os.MkdirAll(key, 0o750); err != nil {
			t.Fatalf("seeding the archive: %v", err)
		}
		if err := os.WriteFile(filepath.Join(key, "a.jsonl.zst"), nil, 0o600); err != nil {
			t.Fatalf("seeding the archive: %v", err)
		}
	}
	return dir
}

// TestTheChainPrefersTheMostExplicitSource walks the four steps against one
// cluster, changing only what the user said.
//
// The order is the acceptance criterion, and it is asserted as an order rather than
// as four independent cases: every step here would also answer on its own, so a
// chain that consulted them in the wrong sequence would pass four separate tests
// and still read the wrong archive.
func TestTheChainPrefersTheMostExplicitSource(t *testing.T) {
	archive := localArchive(t, theCluster)

	profileConfig := func() *resolve.Config {
		return &resolve.Config{
			CurrentProfile: "laptop",
			Profiles: map[string]resolve.Profile{
				"laptop": {Backend: resolve.BackendLocal, Local: &resolve.LocalProfile{Path: archive}},
			},
		}
	}

	tests := []struct {
		name        string
		config      *resolve.Config
		args        []string
		wantOrigin  resolve.Origin
		wantNotice  string
		wantBackend string
	}{
		{
			name:        "--source wins over everything",
			config:      profileConfig(),
			args:        []string{"--source", archive, "--cluster-id", theCluster},
			wantOrigin:  resolve.OriginSourceFlag,
			wantNotice:  "→ using --source " + archive + " (local archive)",
			wantBackend: "objectsource",
		},
		{
			name:        "--sink wins over a profile and over discovery",
			config:      profileConfig(),
			args:        []string{"--sink", "S3Sink/archive", "--cluster-id", theCluster},
			wantOrigin:  resolve.OriginSinkFlag,
			wantNotice:  "→ using --sink S3Sink/archive (s3://acme-audit/kuberecord, region eu-west-1)",
			wantBackend: "objectsource",
		},
		{
			name:        "the active profile wins over discovery",
			config:      profileConfig(),
			args:        []string{"--cluster-id", theCluster},
			wantOrigin:  resolve.OriginProfile,
			wantNotice:  "→ using profile laptop (local archive at " + archive + ")",
			wantBackend: "objectsource",
		},
		{
			name:        "with nothing said, the cluster's own sink is discovered",
			config:      nil,
			args:        []string{"--cluster-id", theCluster},
			wantOrigin:  resolve.OriginDiscovered,
			wantNotice:  "→ discovered ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)",
			wantBackend: "clickhouse",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clients := fakeClients(
				[]*unstructured.Unstructured{
					clickHouseSink("default", "clickhouse.kuberecord-system.svc:9000"),
				},
				credentialsSecret(operatorNamespace), operatorDeployment(operatorNamespace, theCluster))
			// The --sink case needs a second sink to name; the discovery case must
			// not see it, or discovery would have two to choose between.
			if tc.wantOrigin == resolve.OriginSinkFlag {
				clients = fakeClients(
					[]*unstructured.Unstructured{
						clickHouseSink("default", "clickhouse.kuberecord-system.svc:9000"),
						s3Sink("archive", "acme-audit", "kuberecord"),
					},
					credentialsSecret(operatorNamespace), operatorDeployment(operatorNamespace, theCluster))
			}

			resolver, notices := resolverOver(t, tc.config, clients, tc.args...)

			backend, err := resolver.Resolve(t.Context())
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			t.Cleanup(func() { closeBackend(t, backend) })

			if backend.Origin != tc.wantOrigin {
				t.Errorf("resolve.Origin = %q, want %q", backend.Origin, tc.wantOrigin)
			}
			if got := backend.Engine.Capabilities().Backend; got != tc.wantBackend {
				t.Errorf("opened the %q backend, want %q", got, tc.wantBackend)
			}
			if !strings.Contains(notices.String(), tc.wantNotice) {
				t.Errorf("stderr does not carry the chosen source.\n got: %s\nwant a line: %s",
					notices.String(), tc.wantNotice)
			}
		})
	}
}

// TestNothingResolvesToAnActionableFailure.
//
// Each of these is a dead end for a different reason, and the test is over the
// *message* rather than over the error, because a resolution failure is read by
// somebody who has told the tool nothing and needs to know what they can say. An
// error that merely reports the absence sends them to the documentation.
func TestNothingResolvesToAnActionableFailure(t *testing.T) {
	tests := []struct {
		name    string
		sinks   []*unstructured.Unstructured
		args    []string
		reactor func(*k8sfake.Clientset, *dynamicfake.FakeDynamicClient)
		want    []string
		code    int
	}{
		{
			name:  "no sinks at all",
			sinks: nil,
			want: []string{
				"no sink custom resources in this cluster",
				"--source", "--sink", "config set-profile",
			},
			code: exit.RuntimeError,
		},
		{
			name: "several sinks",
			sinks: []*unstructured.Unstructured{
				clickHouseSink("hot", "clickhouse:9000"),
				s3Sink("archive", "acme-audit", ""),
			},
			want: []string{"2 sinks", "ClickHouseSink/hot", "S3Sink/archive", "--sink"},
			code: exit.RuntimeError,
		},
		{
			name:  "a forbidden list says so, and names the routes that need no permission",
			sinks: nil,
			reactor: func(_ *k8sfake.Clientset, dyn *dynamicfake.FakeDynamicClient) {
				dyn.PrependReactor("list", "clickhousesinks",
					func(clienttesting.Action) (bool, runtime.Object, error) {
						return true, nil, apierrors.NewForbidden(
							schema.GroupResource{Group: "kuberecord.io", Resource: "clickhousesinks"},
							"", errors.New("no"))
					})
			},
			want: []string{"forbidden", "--source", "config set-profile"},
			code: exit.RuntimeError,
		},
		{
			name:  "a --profile nobody defined is a usage error, never a fall-through",
			sinks: []*unstructured.Unstructured{clickHouseSink("default", "clickhouse:9000")},
			args:  []string{"--profile", "stagign"},
			want:  []string{"stagign", "no profile"},
			code:  exit.UsageError,
		},
		{
			name:  "an unknown sink kind is a usage error",
			sinks: nil,
			args:  []string{"--sink", "PostgresSink/main"},
			want:  []string{"PostgresSink", resolve.KindClickHouseSink, resolve.KindS3Sink},
			code:  exit.UsageError,
		},
		{
			name:  "a malformed --sink is a usage error",
			sinks: nil,
			args:  []string{"--sink", "default"},
			want:  []string{"expected <kind>/<name>"},
			code:  exit.UsageError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clients := fakeClients(tc.sinks, credentialsSecret(operatorNamespace))
			if tc.reactor != nil {
				tc.reactor(clients.Typed.(*k8sfake.Clientset), clients.Dynamic.(*dynamicfake.FakeDynamicClient))
			}

			resolver, _ := resolverOver(t, nil, clients, tc.args...)

			backend, err := resolver.Resolve(t.Context())
			if err == nil {
				closeBackend(t, backend)
				t.Fatal("Resolve succeeded where it should have failed")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("the failure does not mention %q:\n%v", want, err)
				}
			}
			if got := exit.CodeFor(err); got != tc.code {
				t.Errorf("exit code %d, want %d — a script that retries on %d must not retry this",
					got, tc.code, exit.RuntimeError)
			}
		})
	}
}

// TestAForbiddenSecretSaysExactlyThat.
//
// This is the message the acceptance criteria spell out, and it is spelled out
// because the failure it describes is the most common one this tool has: the
// operator's RBAC grants Secret reads in its own namespace and nothing wider (D7),
// so most engineers cannot read the credential a sink points at. Reporting that as
// "connection failed" would send them to a database they can reach, to debug a
// problem that is in their own permissions — and the remedy, a profile, needs no
// new grant at all.
func TestAForbiddenSecretSaysExactlyThat(t *testing.T) {
	clients := fakeClients(
		[]*unstructured.Unstructured{clickHouseSink("default", "clickhouse:9000")},
		operatorDeployment(operatorNamespace, theCluster))
	clients.Typed.(*k8sfake.Clientset).PrependReactor("get", "secrets",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				corev1.Resource("secrets"), secretName, errors.New("no"))
		})

	resolver, _ := resolverOver(t, nil, clients)

	_, err := resolver.Resolve(t.Context())
	if err == nil {
		t.Fatal("Resolve succeeded with an unreadable Secret")
	}

	want := "cannot read Secret " + operatorNamespace + "/" + secretName +
		" (forbidden); configure a profile with `kuberecord config set-profile`"
	if !strings.Contains(err.Error(), want) {
		t.Errorf("the failure is not the one the user needs.\n got: %v\nwant: %s", err, want)
	}
	for _, mustNot := range []string{"connection", "dial", "timeout"} {
		if strings.Contains(strings.ToLower(err.Error()), mustNot) {
			t.Errorf("the failure blames the connection (%q), which is not what went wrong: %v",
				mustNot, err)
		}
	}
}

// TestTheRemediationNamesTheCommandTheUserCanType: the plugin spelling.
//
// A user who ran `kubectl kuberecord` and is told to run `kuberecord config
// set-profile` has been given a command that does not exist on their machine
// unless they also installed the standalone binary. The invocation name is already
// known; using it costs nothing.
func TestTheRemediationNamesTheCommandTheUserCanType(t *testing.T) {
	clients := fakeClients(
		[]*unstructured.Unstructured{clickHouseSink("default", "clickhouse:9000")})
	clients.Typed.(*k8sfake.Clientset).PrependReactor("get", "secrets",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(corev1.Resource("secrets"), secretName, errors.New("no"))
		})

	resolver, _ := resolverOver(t, nil, clients)
	resolver.InvokedAs = options.PluginInvocation

	_, err := resolver.Resolve(t.Context())
	if err == nil {
		t.Fatal("Resolve succeeded with an unreadable Secret")
	}
	if !strings.Contains(err.Error(), "`kubectl kuberecord config set-profile`") {
		t.Errorf("a plugin user is told to type a command they do not have: %v", err)
	}
}

// TestAMissingSecretKeyNamesTheKeysThatArePresent.
//
// `--from-literal=PASSWORD=…` is the mistake, and it is invisible until something
// says which key was looked for and which are there. Key names are not secrets;
// the values they hold appear nowhere.
func TestAMissingSecretKeyNamesTheKeysThatArePresent(t *testing.T) {
	secret := credentialsSecret(operatorNamespace)
	secret.Data = map[string][]byte{"PASSWORD": []byte(theSecret), "username": []byte("kuberecord")}

	clients := fakeClients(
		[]*unstructured.Unstructured{clickHouseSink("default", "clickhouse:9000")}, secret)
	resolver, _ := resolverOver(t, nil, clients)

	_, err := resolver.Resolve(t.Context())
	if err == nil {
		t.Fatal("Resolve succeeded with a Secret that has no password key")
	}
	for _, want := range []string{`no "password" key`, "PASSWORD, username"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the failure does not mention %q:\n%v", want, err)
		}
	}
	if strings.Contains(err.Error(), theSecret) {
		t.Error("the failure quotes a Secret's value")
	}
}

// TestNoCredentialIsEverPrinted, at any verbosity.
//
// The notice on stderr is read over shoulders, pasted into issues and captured by
// CI logs. It says what was discovered and never what was in the Secret, and this
// asserts that over the whole invocation — both streams, and the resolved
// Description a later command will render — rather than over the one line that was
// written with it in mind.
func TestNoCredentialIsEverPrinted(t *testing.T) {
	clients := fakeClients(
		[]*unstructured.Unstructured{clickHouseSink("default", "clickhouse:9000")},
		credentialsSecret(operatorNamespace), operatorDeployment(operatorNamespace, theCluster))

	io, out, errOut := streams()
	root, flags := cli.NewRootCommand(options.StandaloneName, io)
	if err := root.ParseFlags([]string{"--kubeconfig", kubeconfigPath, "-v", "10"}); err != nil {
		t.Fatalf("parsing flags: %v", err)
	}
	resolver := &resolve.BackendResolver{
		Flags: flags, Streams: io, InvokedAs: options.StandaloneName,
		Config: &resolve.Config{}, ConfigPath: filepath.Join(t.TempDir(), "config.yaml"), Clients: clients,
	}

	backend, err := resolver.Resolve(t.Context())
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	t.Cleanup(func() { closeBackend(t, backend) })

	for name, written := range map[string]string{
		"stdout":      out.String(),
		"stderr":      errOut.String(),
		"description": backend.Description,
	} {
		if strings.Contains(written, theSecret) {
			t.Errorf("%s carries the password read from the Secret: %s", name, written)
		}
	}
	if !strings.Contains(errOut.String(), "discovered ClickHouseSink/default") {
		t.Errorf("stderr does not say what was discovered: %s", errOut.String())
	}
}

// TestSourceIsReadWithoutACluster covers the shapes --source takes.
//
// No fixture cluster is wired in at all, and that is the assertion as much as the
// parsing: an evaluator with an archive on a laptop, or an auditor with a bucket
// and a read-only key, must get an answer with no kubeconfig anywhere in the path
// (D18). A resolver that reached for a cluster on this route would fail here.
func TestSourceIsReadWithoutACluster(t *testing.T) {
	archive := localArchive(t, theCluster)

	tests := []struct {
		name       string
		source     string
		env        map[string]string
		wantNotice string
		wantErr    []string
	}{
		{
			name:       "a plain directory",
			source:     archive,
			wantNotice: "using --source " + archive + " (local archive)",
		},
		{
			name:       "a file URL",
			source:     "file://" + archive,
			wantNotice: "using --source " + archive + " (local archive)",
		},
		{
			name:       "a bucket, with the region from the SDK's own variable",
			source:     "s3://acme-audit/kuberecord",
			env:        map[string]string{"AWS_REGION": "eu-west-1"},
			wantNotice: "using --source s3://acme-audit/kuberecord (region eu-west-1)",
		},
		{
			name:       "a bucket with no region configured",
			source:     "s3://acme-audit",
			wantNotice: "using --source s3://acme-audit (region us-east-1)",
		},
		{
			name:    "a bucket URL naming no bucket",
			source:  "s3:///kuberecord",
			wantErr: []string{"names no bucket", "s3://bucket[/prefix]"},
		},
		{
			name:    "a scheme this build cannot read",
			source:  "gs://acme-audit/kuberecord",
			wantErr: []string{"gs", "use s3://"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for name, value := range tc.env {
				t.Setenv(name, value)
			}
			if tc.env == nil {
				// The SDK's variables leak in from a developer's own shell
				// otherwise, and the default-region case would pass or fail
				// depending on whose machine ran it.
				t.Setenv("AWS_REGION", "")
				t.Setenv("AWS_DEFAULT_REGION", "")
			}

			resolver, notices := resolverOver(t, nil, nil,
				"--source", tc.source, "--cluster-id", theCluster)

			backend, err := resolver.Resolve(t.Context())
			if len(tc.wantErr) > 0 {
				if err == nil {
					closeBackend(t, backend)
					t.Fatal("Resolve accepted a source it should have refused")
				}
				for _, want := range tc.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("the failure does not mention %q:\n%v", want, err)
					}
				}
				if got := exit.CodeFor(err); got != exit.UsageError {
					t.Errorf("exit code %d, want %d: a bad flag value is a usage error",
						got, exit.UsageError)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			t.Cleanup(func() { closeBackend(t, backend) })

			if !strings.Contains(notices.String(), tc.wantNotice) {
				t.Errorf("stderr does not carry the chosen source.\n got: %s\nwant a line: %s",
					notices.String(), tc.wantNotice)
			}
		})
	}
}

// TestTheOperatorNamespaceIsResolvedBeforeASecretIsRead.
//
// A SecretReference with no namespace means the operator's own namespace, and that
// default is a security boundary rather than a convenience: it is the only
// namespace the operator's aggregated ClusterRole grants Secret reads in (D7). A
// reader that guessed differently looks for the credential in the wrong place and
// reports a Secret that does not exist — a message that sends somebody to check
// their sink when nothing is wrong with it.
//
// The order below is stated first, searched second, defaulted last. Stating it is
// the escape hatch for a locked-down cluster where the search is forbidden and the
// install is somewhere other than the default.
func TestTheOperatorNamespaceIsResolvedBeforeASecretIsRead(t *testing.T) {
	const elsewhere = "audit-system"

	tests := []struct {
		name            string
		config          *resolve.Config
		args            []string
		withDeployment  bool
		secretNamespace string
		wantErr         string
	}{
		{
			name:            "--operator-namespace is believed outright",
			args:            []string{"--operator-namespace", elsewhere},
			secretNamespace: elsewhere,
		},
		{
			name:            "the configuration file's operatorNamespace",
			config:          &resolve.Config{OperatorNamespace: elsewhere},
			secretNamespace: elsewhere,
		},
		{
			name:            "otherwise the operator's own Deployment is searched for",
			withDeployment:  true,
			secretNamespace: elsewhere,
		},
		{
			name:            "and with no operator to find, the default install namespace",
			secretNamespace: operatorNamespace,
		},
		{
			name:            "a Secret whose namespace is stated needs none of this",
			secretNamespace: "somewhere-else-entirely",
		},
		{
			name:            "the wrong namespace is reported as the missing Secret it is",
			secretNamespace: elsewhere,
			wantErr:         "Secret " + operatorNamespace + "/" + secretName + " does not exist",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sink := clickHouseSink("default", "clickhouse:9000")
			if tc.name == "a Secret whose namespace is stated needs none of this" {
				// The reference names its own namespace, which the CRD permits and
				// which must win over every step of the resolution above.
				connection := sink.Object["spec"].(map[string]any)["connection"].(map[string]any)
				connection["credentialsSecretRef"] = map[string]any{
					"name": secretName, "namespace": tc.secretNamespace,
				}
			}

			objects := []runtime.Object{credentialsSecret(tc.secretNamespace)}
			if tc.withDeployment {
				objects = append(objects, operatorDeployment(elsewhere, theCluster))
			}
			clients := fakeClients([]*unstructured.Unstructured{sink}, objects...)

			resolver, _ := resolverOver(t, tc.config, clients,
				append(tc.args, "--cluster-id", theCluster)...)

			backend, err := resolver.Resolve(t.Context())
			if tc.wantErr != "" {
				if err == nil {
					closeBackend(t, backend)
					t.Fatalf("Resolve found a Secret in %s that is only in %s",
						operatorNamespace, tc.secretNamespace)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("the failure does not name the namespace it looked in:\n%v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			closeBackend(t, backend)
		})
	}
}

// TestAnS3SinksCredentialsSecretIsResolved.
//
// The archive tier's Secret carries two keys rather than one, and the session
// token is optional because only temporary credentials have one — a static key
// without it is the ordinary case and must not be reported as incomplete. The
// discovery notice names the bucket and the region and, as everywhere, no part of
// the key.
func TestAnS3SinksCredentialsSecretIsResolved(t *testing.T) {
	sink := s3Sink("archive", "acme-audit", "kuberecord")
	sink.Object["spec"].(map[string]any)["credentials"] = map[string]any{
		"secretRef": map[string]any{"name": "archive-credentials"},
	}

	newSecret := func(data map[string][]byte) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "archive-credentials", Namespace: operatorNamespace},
			Data:       data,
		}
	}

	t.Run("a static key", func(t *testing.T) {
		clients := fakeClients([]*unstructured.Unstructured{sink}, newSecret(map[string][]byte{
			"accessKeyId":     []byte("AKIAEXAMPLE"),
			"secretAccessKey": []byte(theSecret),
		}))
		resolver, notices := resolverOver(t, nil, clients, "--cluster-id", theCluster)

		backend, err := resolver.Resolve(t.Context())
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		t.Cleanup(func() { closeBackend(t, backend) })

		want := "→ discovered S3Sink/archive (s3://acme-audit/kuberecord, region eu-west-1)"
		if !strings.Contains(notices.String(), want) {
			t.Errorf("stderr does not name what was discovered.\n got: %s\nwant a line: %s",
				notices.String(), want)
		}
		if strings.Contains(notices.String(), theSecret) ||
			strings.Contains(notices.String(), "AKIAEXAMPLE") {
			t.Errorf("stderr carries part of the access key: %s", notices.String())
		}
	})

	t.Run("a Secret missing half the key names the key it wanted", func(t *testing.T) {
		clients := fakeClients([]*unstructured.Unstructured{sink}, newSecret(map[string][]byte{
			"accessKeyId": []byte("AKIAEXAMPLE"),
		}))
		resolver, _ := resolverOver(t, nil, clients, "--cluster-id", theCluster)

		backend, err := resolver.Resolve(t.Context())
		if err == nil {
			closeBackend(t, backend)
			t.Fatal("Resolve accepted a Secret holding half a key")
		}
		if !strings.Contains(err.Error(), `no "secretAccessKey" key`) {
			t.Errorf("the failure does not name the missing key:\n%v", err)
		}
	})
}

// TestAProfileIsOpenedWithWhatItReferences.
//
// The ClickHouse case is the one profiles exist for, and the assertion is that the
// *reference* is followed: the password comes out of the environment at resolution
// time, and a variable the shell never exported fails here rather than as an
// authentication error later.
func TestAProfileIsOpenedWithWhatItReferences(t *testing.T) {
	profileConfig := &resolve.Config{
		CurrentProfile: profileProd,
		Profiles: map[string]resolve.Profile{
			profileProd: {
				Backend: resolve.BackendClickHouse,
				ClickHouse: &resolve.ClickHouseProfile{
					Addr: "clickhouse.example:9000", Database: "kuberecord",
					Username: "kuberecord_ro", PasswordEnv: "KUBERECORD_TEST_PROFILE_PASSWORD",
					TLS: true,
				},
			},
		},
	}

	t.Run("the password is read from the environment it names", func(t *testing.T) {
		t.Setenv("KUBERECORD_TEST_PROFILE_PASSWORD", theSecret)

		resolver, notices := resolverOver(t, profileConfig, fakeClients(nil), "--cluster-id", theCluster)
		backend, err := resolver.Resolve(t.Context())
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		t.Cleanup(func() { closeBackend(t, backend) })

		want := "→ using profile prod (ClickHouse at clickhouse.example:9000/kuberecord)"
		if !strings.Contains(notices.String(), want) {
			t.Errorf("stderr does not name the profile it used.\n got: %s\nwant a line: %s",
				notices.String(), want)
		}
		if strings.Contains(notices.String(), theSecret) {
			t.Errorf("stderr carries the password: %s", notices.String())
		}
	})

	t.Run("a variable the shell never exported fails here, not later", func(t *testing.T) {
		resolver, _ := resolverOver(t, profileConfig, fakeClients(nil), "--cluster-id", theCluster)

		backend, err := resolver.Resolve(t.Context())
		if err == nil {
			closeBackend(t, backend)
			t.Fatal("Resolve dialled with a password the environment does not hold")
		}
		for _, want := range []string{"prod", "KUBERECORD_TEST_PROFILE_PASSWORD"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the failure does not mention %q:\n%v", want, err)
			}
		}
	})
}
