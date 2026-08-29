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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/kuberecord/kuberecord/internal/cli"
)

// The cluster identity is resolved by a five-step chain, and each step is asserted
// on its own below — that is the acceptance criterion, and it is the right shape
// for these particular steps because each one is available in a different
// deployment: a scripted invocation has the flag, a long-lived workstation has the
// context mapping, an ordinary cluster has the operator, and an archive on a laptop
// has none of them and only the sink to ask.
//
// The chain's *order* is asserted too, in the second test. Four steps that each
// work in isolation can still be consulted in the wrong sequence, and the symptom
// of that is reading a different cluster's history — an answer that looks exactly
// like a correct one.

// kubeconfigContext is the current context in the fixture kubeconfig, and
// therefore the key a context mapping has to be written under to be found.
const kubeconfigContext = "kuberecord-test"

// clusterIDResolvedFrom runs a resolution over a local archive and returns the
// identity and the notice explaining where it came from.
func clusterIDResolvedFrom(
	t *testing.T, cfg *cli.Config, clients *cli.Clients, archive string, args ...string,
) (*cli.Backend, string, error) {
	t.Helper()

	resolver, notices := resolverOver(t, cfg, clients, append([]string{"--source", archive}, args...)...)
	backend, err := resolver.Resolve(t.Context())
	t.Cleanup(func() { closeBackend(t, backend) })
	return backend, notices.String(), err
}

// TestEachStepOfTheClusterIDChainAnswersOnItsOwn.
func TestEachStepOfTheClusterIDChainAnswersOnItsOwn(t *testing.T) {
	t.Run("1: --cluster-id", func(t *testing.T) {
		archive := localArchive(t, "an-archive-holding-something-else")

		backend, notices, err := clusterIDResolvedFrom(t, nil, fakeClients(nil), archive,
			"--cluster-id", theCluster)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if backend.ClusterID != theCluster {
			t.Errorf("ClusterID = %q, want the flag's value", backend.ClusterID)
		}
		if !strings.Contains(notices, "→ cluster-id prod-eu-1 (from --cluster-id)") {
			t.Errorf("stderr does not say where the identity came from: %s", notices)
		}
	})

	t.Run("2: the configuration file's context mapping", func(t *testing.T) {
		archive := localArchive(t, "an-archive-holding-something-else")
		cfg := &cli.Config{Contexts: map[string]string{kubeconfigContext: theCluster}}

		backend, notices, err := clusterIDResolvedFrom(t, cfg, fakeClients(nil), archive)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if backend.ClusterID != theCluster {
			t.Errorf("ClusterID = %q, want the mapping's value", backend.ClusterID)
		}
		if !strings.Contains(notices, `maps the context "`+kubeconfigContext+`"`) {
			t.Errorf("stderr does not say the mapping answered: %s", notices)
		}
	})

	t.Run("3: the operator's own Deployment", func(t *testing.T) {
		archive := localArchive(t, "an-archive-holding-something-else")
		clients := fakeClients(nil, operatorDeployment(operatorNamespace, theCluster))

		backend, notices, err := clusterIDResolvedFrom(t, nil, clients, archive)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if backend.ClusterID != theCluster {
			t.Errorf("ClusterID = %q, want the operator's own identity", backend.ClusterID)
		}
		if !strings.Contains(notices,
			"from the operator Deployment "+operatorNamespace+"/kuberecord-controller-manager") {
			t.Errorf("stderr does not name the Deployment it read: %s", notices)
		}
	})

	t.Run("4: the only cluster in the sink", func(t *testing.T) {
		archive := localArchive(t, theCluster)

		backend, notices, err := clusterIDResolvedFrom(t, nil, fakeClients(nil), archive)
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if backend.ClusterID != theCluster {
			t.Errorf("ClusterID = %q, want the archive's only cluster", backend.ClusterID)
		}
		if !strings.Contains(notices, "the only cluster in this sink") {
			t.Errorf("stderr does not say the sink answered: %s", notices)
		}
	})

	t.Run("5: several clusters, and the failure lists them", func(t *testing.T) {
		archive := localArchive(t, "prod-us-1", theCluster, "staging")

		_, _, err := clusterIDResolvedFrom(t, nil, fakeClients(nil), archive)
		if err == nil {
			t.Fatal("a sink holding three clusters resolved to one of them")
		}
		for _, want := range []string{
			"prod-eu-1, prod-us-1, staging",
			"--cluster-id",
			"config set-context-cluster-id",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the failure does not mention %q, so it is a dead end rather than a next "+
					"step:\n%v", want, err)
			}
		}
	})
}

// TestTheClusterIDChainIsConsultedInOrder.
//
// Each pair below has two steps that can both answer and disagree, so only the
// order decides which is used. They are worth pinning because the failure they
// prevent is silent: reading prod-us-1's history under a header saying prod-eu-1
// is an answer that looks entirely correct.
func TestTheClusterIDChainIsConsultedInOrder(t *testing.T) {
	archive := localArchive(t, "from-the-sink")
	mapping := &cli.Config{Contexts: map[string]string{kubeconfigContext: "from-the-mapping"}}
	withOperator := func() *cli.Clients {
		return fakeClients(nil, operatorDeployment(operatorNamespace, "from-the-operator"))
	}

	tests := []struct {
		name    string
		config  *cli.Config
		clients *cli.Clients
		args    []string
		want    string
	}{
		{
			name:    "the flag beats the mapping",
			config:  mapping,
			clients: withOperator(),
			args:    []string{"--cluster-id", "from-the-flag"},
			want:    "from-the-flag",
		},
		{
			name:    "the mapping beats the operator",
			config:  mapping,
			clients: withOperator(),
			want:    "from-the-mapping",
		},
		{
			name:    "the operator beats the sink",
			clients: withOperator(),
			want:    "from-the-operator",
		},
		{
			name:    "the sink is asked only when nothing else answered",
			clients: fakeClients(nil),
			want:    "from-the-sink",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend, _, err := clusterIDResolvedFrom(t, tc.config, tc.clients, archive, tc.args...)
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if backend.ClusterID != tc.want {
				t.Errorf("ClusterID = %q, want %q", backend.ClusterID, tc.want)
			}
		})
	}
}

// TestTheOperatorsIdentityIsReadFromEitherSpelling.
//
// The acceptance criterion says the operator Deployment's `CLUSTER_ID` *argument*,
// and the shipped chart sets an environment variable of that name while
// cmd/main.go also accepts `--cluster-id` as a flag. Both are read, and the
// argument wins, because that is the precedence the operator itself applies — a
// reader that disagreed would report an identity the operator is not writing.
func TestTheOperatorsIdentityIsReadFromEitherSpelling(t *testing.T) {
	tests := []struct {
		name      string
		container corev1.Container
		want      string
	}{
		{
			name: "the environment variable the chart sets",
			container: corev1.Container{
				Name: "manager",
				Env:  []corev1.EnvVar{{Name: "CLUSTER_ID", Value: "from-the-environment"}},
			},
			want: "from-the-environment",
		},
		{
			name: "the flag, joined with an equals sign",
			container: corev1.Container{
				Name: "manager",
				Args: []string{"--leader-elect", "--cluster-id=from-the-argument"},
			},
			want: "from-the-argument",
		},
		{
			name: "the flag, as two arguments",
			container: corev1.Container{
				Name: "manager",
				Args: []string{"--cluster-id", "from-the-argument"},
			},
			want: "from-the-argument",
		},
		{
			name: "the flag wins over the variable, as it does in the operator",
			container: corev1.Container{
				Name: "manager",
				Args: []string{"--cluster-id=from-the-argument"},
				Env:  []corev1.EnvVar{{Name: "CLUSTER_ID", Value: "from-the-environment"}},
			},
			want: "from-the-argument",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deployment := operatorDeployment(operatorNamespace, "")
			deployment.Spec.Template.Spec.Containers = []corev1.Container{tc.container}

			backend, _, err := clusterIDResolvedFrom(t, nil, fakeClients(nil, deployment),
				localArchive(t, "from-the-sink"))
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if backend.ClusterID != tc.want {
				t.Errorf("ClusterID = %q, want %q", backend.ClusterID, tc.want)
			}
		})
	}
}

// TestAnOperatorThatNamesNoIdentityIsNotAnAnswer.
//
// An operator running on the flag's built-in default says nothing about its
// identity, and guessing that default would produce a value matching nothing in the
// sink — an empty timeline with no explanation, which is exactly the outcome
// Invariant 9 exists to prevent. Falling through to the sink is the honest move.
func TestAnOperatorThatNamesNoIdentityIsNotAnAnswer(t *testing.T) {
	deployment := operatorDeployment(operatorNamespace, "")
	deployment.Spec.Template.Spec.Containers = []corev1.Container{{Name: "manager"}}

	backend, _, err := clusterIDResolvedFrom(t, nil, fakeClients(nil, deployment),
		localArchive(t, "from-the-sink"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if backend.ClusterID != "from-the-sink" {
		t.Errorf("ClusterID = %q, want the chain to have continued to the sink", backend.ClusterID)
	}
}

// TestAnUnreadableClusterDegradesRatherThanFailing.
//
// The operator lookup is a convenience: it saves typing an identity the cluster
// already knows. A forbidden list, or an API server that is simply gone, must
// therefore continue the chain — a laptop with a stale kubeconfig has to be able to
// read an archive on its own disk (D18) — and must say so, because a step that was
// skipped rather than answered is something the user should be able to see
// (Invariant 4).
func TestAnUnreadableClusterDegradesRatherThanFailing(t *testing.T) {
	clients := fakeClients(nil)
	clients.Typed.(*k8sfake.Clientset).PrependReactor("list", "deployments",
		func(clienttesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(
				schema.GroupResource{Group: appsv1.GroupName, Resource: "deployments"},
				"", errNotAllowed)
		})

	backend, notices, err := clusterIDResolvedFrom(t, nil, clients, localArchive(t, "from-the-sink"))
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if backend.ClusterID != "from-the-sink" {
		t.Errorf("ClusterID = %q, want the chain to have continued to the sink", backend.ClusterID)
	}
	if !strings.Contains(notices, "forbidden") {
		t.Errorf("stderr does not report the step that was skipped: %s", notices)
	}
}

// TestAnEmptySinkSaysItIsEmpty.
//
// "This sink holds no history" and "you did not say which cluster" are different
// facts, and the first one is the more useful: it says the operator never wrote
// here, which is a configuration problem somewhere else entirely.
func TestAnEmptySinkSaysItIsEmpty(t *testing.T) {
	_, _, err := clusterIDResolvedFrom(t, nil, fakeClients(nil), localArchive(t))
	if err == nil {
		t.Fatal("an empty archive resolved to a cluster identity")
	}
	if !strings.Contains(err.Error(), "no recorded history") {
		t.Errorf("the failure does not distinguish an empty sink from an unnamed cluster:\n%v", err)
	}
}

// errNotAllowed is the cause a forbidden fixture carries.
var errNotAllowed = errors.New("not allowed")
