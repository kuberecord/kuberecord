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
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/cli/resolve"
)

// The weight of a resolution notice, asserted as the only thing colour changes.
//
// Two properties are under test here and they are deliberately checked in one
// comparison. The first is the decision: an ordinary resolution recedes into the
// provenance tier, and one where something shadowed the ordinary answer does not.
// The second is that the decision is *nothing but* colour — every coloured line
// is reconstructed from the plain one, so a weight that added a marker, reworded
// a sentence or reordered a line could not be built back out of the plain
// rendering at any weight and fails here.
//
// The cases are whole resolutions rather than calls to the writer, because what
// is being asserted is which chain step answered, and only a walk of the chain
// can be wrong about that.

// noticeExpectation is one line of stderr: the weight it must carry, and a
// fragment identifying the step that produced it.
//
// The fragment is not there to pin the wording. It is there so that a case whose
// fixture drifted — a profile that stopped shadowing, an operator that stopped
// being found — fails as the wrong resolution rather than passing because the
// chain it now walks happens to print the same number of lines.
type noticeExpectation struct {
	routine  bool
	contains string
}

// noticeWeightCase is one resolution and the weights it must print at.
type noticeWeightCase struct {
	name    string
	config  *resolve.Config
	clients *resolve.Clients
	args    []string
	want    []noticeExpectation
}

// notices resolves once under one colour mode and returns the lines of stderr.
func (tc noticeWeightCase) notices(t *testing.T, mode string) []string {
	t.Helper()

	resolver, stderr := resolverOver(t, tc.config, tc.clients,
		append([]string{"--color=" + mode}, tc.args...)...)
	backend, err := resolver.Resolve(t.Context())
	if err != nil {
		t.Fatalf("Resolve under --color=%s: %v", mode, err)
	}
	t.Cleanup(func() { closeBackend(t, backend) })

	return strings.Split(strings.TrimRight(stderr.String(), "\n"), "\n")
}

// paintedNotice is what one plain notice line looks like at a given weight.
//
// Built from the plain line rather than from a literal, which is the whole
// point: the coloured rendering must be the plain one with escapes around the
// sentence and nothing else — same marker, same spacing, same words. The "→"
// stays outside the paint because the tier belongs to the sentence, exactly as
// render.renderNotices leaves "!" unpainted.
func paintedNotice(plain string, routine bool) string {
	const marker = "→ "
	text := strings.TrimPrefix(plain, marker)
	if !routine {
		return marker + text
	}
	return marker + render.NewSeverity(true).Provenance(text)
}

// TestResolutionNoticesRecedeUnlessTheyAreUnusual is Task 15.7's acceptance
// criterion.
func TestResolutionNoticesRecedeUnlessTheyAreUnusual(t *testing.T) {
	for _, tc := range noticeWeightScenarios(t) {
		t.Run(tc.name, func(t *testing.T) {
			plain := tc.notices(t, "never")
			painted := tc.notices(t, "always")

			if len(plain) != len(tc.want) {
				t.Fatalf("stderr carried %d notices, want %d:\n%s",
					len(plain), len(tc.want), strings.Join(plain, "\n"))
			}
			if len(painted) != len(plain) {
				t.Fatalf("colour changed the number of notices: %d with, %d without",
					len(painted), len(plain))
			}

			for i, want := range tc.want {
				if !strings.Contains(plain[i], want.contains) {
					t.Errorf("notice %d is not the step this case is about.\n got: %s\nwant a mention of: %s",
						i, plain[i], want.contains)
				}
				if strings.Contains(plain[i], "\x1b") {
					t.Errorf("notice %d carries an escape sequence under --color=never: %q", i, plain[i])
				}
				if got, wantLine := painted[i], paintedNotice(plain[i], want.routine); got != wantLine {
					t.Errorf("notice %d is at the wrong weight (routine=%t).\n got: %q\nwant: %q",
						i, want.routine, got, wantLine)
				}
			}
		})
	}
}

// noticeWeightScenarios builds the resolutions worth telling apart.
func noticeWeightScenarios(t *testing.T) []noticeWeightCase {
	t.Helper()

	archive := localArchive(t, theCluster)
	theSink := func() []*unstructured.Unstructured {
		return []*unstructured.Unstructured{
			clickHouseSink("default", "clickhouse.kuberecord-system.svc:9000"),
		}
	}
	withSink := func(extra ...runtime.Object) *resolve.Clients {
		return fakeClients(theSink(),
			append([]runtime.Object{credentialsSecret(operatorNamespace)}, extra...)...)
	}
	// A second operator has to differ by name: two Deployments sharing one
	// namespace and one name are one object, and the fixture would seed a cluster
	// with a single operator in it.
	renamed := func(name string, deployment *appsv1.Deployment) *appsv1.Deployment {
		deployment.Name = name
		return deployment
	}

	// The archive is read directly and the cluster holds no operator, so the
	// identity chain runs out of local answers and has to ask the backend.
	noOperator := fakeClients(nil)

	forbiddenDeployments := fakeClients(nil)
	forbiddenDeployments.Typed.(*k8sfake.Clientset).PrependReactor("list", "deployments",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				appsv1.Resource("deployments"), "", errors.New("no"))
		})

	return []noticeWeightCase{
		{
			// The invocation the documentation opens with. Both lines say the tool
			// did the ordinary thing, and both recede.
			name:    "the ordinary resolution recedes on both lines",
			clients: withSink(operatorDeployment(operatorNamespace, theCluster)),
			args:    []string{"--cluster-id", theCluster},
			want: []noticeExpectation{
				{routine: true, contains: "discovered ClickHouseSink/default"},
				{routine: true, contains: "cluster-id " + theCluster + " (from --cluster-id)"},
			},
		},
		{
			// The case the task exists for: a stanza written months ago answering
			// in front of a sink the cluster is still pointing at.
			name: "a profile shadowing a discoverable sink stands out",
			config: &resolve.Config{
				CurrentProfile: "laptop",
				Profiles: map[string]resolve.Profile{
					"laptop": {Backend: resolve.BackendLocal, Local: &resolve.LocalProfile{Path: archive}},
				},
			},
			clients: withSink(operatorDeployment(operatorNamespace, theCluster)),
			args:    []string{"--cluster-id", theCluster},
			want: []noticeExpectation{
				{routine: false, contains: "using profile laptop"},
				{routine: true, contains: "(from --cluster-id)"},
			},
		},
		{
			// Nobody stated the identity, so it was read off whichever Deployment
			// carries the operator's label in the cluster the kubeconfig points at.
			name:    "an identity inferred from a Deployment stands out",
			clients: withSink(operatorDeployment(operatorNamespace, theCluster)),
			want: []noticeExpectation{
				{routine: true, contains: "discovered ClickHouseSink/default"},
				{routine: false, contains: "from the operator Deployment"},
			},
		},
		{
			// The zero-infrastructure path (D18): the flag chose the backend and
			// the archive's own contents chose the identity. Neither is discovery
			// and neither was stated, so neither recedes.
			name:    "--source with the identity taken from the archive stands out on both lines",
			clients: noOperator,
			args:    []string{"--source", archive},
			want: []noticeExpectation{
				{routine: false, contains: "using --source"},
				{routine: false, contains: "the only cluster in this sink"},
			},
		},
		{
			// The one line where the resolver admits it chose between candidates.
			name: "several operators is announced at full weight",
			clients: withSink(
				renamed("kuberecord-a", operatorDeployment(operatorNamespace, theCluster)),
				renamed("kuberecord-b", operatorDeployment(operatorNamespace, theCluster))),
			// The operator lookup runs first here, because discovering a sink has
			// to know which namespace holds its credentials Secret (D7) — so the
			// admission that the tool chose is on the stream before the choice it
			// was made for.
			want: []noticeExpectation{
				{routine: false, contains: "this cluster has 2 kuberecord operators"},
				{routine: true, contains: "discovered ClickHouseSink/default"},
				{routine: false, contains: "from the operator Deployment"},
			},
		},
		{
			// A step with nothing to say is not an unusual resolution. On a laptop
			// with a stale kubeconfig this fires on every invocation, which is the
			// never-varying signal the provenance tier exists for.
			name:    "a step that could not be taken recedes",
			clients: forbiddenDeployments,
			args:    []string{"--source", archive},
			want: []noticeExpectation{
				{routine: false, contains: "using --source"},
				{routine: true, contains: "cannot list Deployments"},
				{routine: false, contains: "the only cluster in this sink"},
			},
		},
	}
}

// TestNeitherWeightChangesAByteOfPlainOutput is the byte-identity half.
//
// The test above compares each coloured line against the plain one beside it,
// which proves the two agree but not what they agree *on*. This states the
// stronger claim the changelog makes: under --color=never, a routine resolution
// and a shadowed one write exactly the lines they wrote before this change —
// same marker, same spacing, same words, no line added or dropped. A redirect, a
// pipe and a golden file taken last week are all unaffected, and the weights are
// a terminal affordance rather than a change to the output.
func TestNeitherWeightChangesAByteOfPlainOutput(t *testing.T) {
	archive := localArchive(t, theCluster)
	clients := func() *resolve.Clients {
		return fakeClients(
			[]*unstructured.Unstructured{
				clickHouseSink("default", "clickhouse.kuberecord-system.svc:9000"),
			},
			credentialsSecret(operatorNamespace), operatorDeployment(operatorNamespace, theCluster))
	}

	tests := []struct {
		name string
		this noticeWeightCase
		want []string
	}{
		{
			name: "routine",
			this: noticeWeightCase{clients: clients(), args: []string{"--cluster-id", theCluster}},
			want: []string{
				"→ discovered ClickHouseSink/default (clickhouse.kuberecord-system.svc:9000/kuberecord)",
				"→ cluster-id " + theCluster + " (from --cluster-id)",
			},
		},
		{
			name: "shadowed",
			this: noticeWeightCase{
				config: &resolve.Config{
					CurrentProfile: "laptop",
					Profiles: map[string]resolve.Profile{
						"laptop": {Backend: resolve.BackendLocal,
							Local: &resolve.LocalProfile{Path: archive}},
					},
				},
				clients: clients(),
				args:    []string{"--cluster-id", theCluster},
			},
			want: []string{
				"→ using profile laptop (local archive at " + archive + ")",
				"→ cluster-id " + theCluster + " (from --cluster-id)",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.this.notices(t, "never")
			if strings.Join(got, "\n") != strings.Join(tc.want, "\n") {
				t.Errorf("plain stderr changed.\n got:\n%s\nwant:\n%s",
					strings.Join(got, "\n"), strings.Join(tc.want, "\n"))
			}
		})
	}
}
