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
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/kuberecord/kuberecord/api/v1alpha1"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
)

// Discovery reads the sink custom resources through the kubeconfig, and it is the
// step that makes the common case need no configuration at all: a cluster whose
// operator is streaming to one ClickHouse has already written down where that is,
// in an object anyone with cluster-read access can see.
//
// What it must not do is assume it can see everything. The operator's own RBAC
// grants Secret reads in one namespace and no more (D7), and most engineers have
// less than that — so every failure here is classified rather than collapsed into
// "connection failed". "You cannot read that Secret" and "that host is down" send
// a person to two different places, and only one of them is where the problem is.

// The API this package reads. The group and version are the CRDs' own; the
// resources are their plural names, which is what a dynamic client addresses.
var (
	clickHouseSinkGVR = schema.GroupVersionResource{
		Group: v1alpha1.GroupVersion.Group, Version: v1alpha1.GroupVersion.Version,
		Resource: "clickhousesinks",
	}
	s3SinkGVR = schema.GroupVersionResource{
		Group: v1alpha1.GroupVersion.Group, Version: v1alpha1.GroupVersion.Version,
		Resource: "s3sinks",
	}
)

// The sink kinds a rule may target, spelled as the CRDs spell them. They are the
// vocabulary of --sink and of every message that lists what could be discovered.
const (
	KindClickHouseSink = "ClickHouseSink"
	KindS3Sink         = "S3Sink"
)

// sinkKinds is the accepted set, in the order it is shown to a user.
var sinkKinds = []string{KindClickHouseSink, KindS3Sink}

// operatorSelector finds the operator's Deployment.
//
// Both install paths label it identically — the chart's selectorLabels and
// config/manager/manager.yaml agree — and the selector uses only those two labels
// because everything else they carry (instance, version, managed-by) differs
// between the two, and a selector that matched only a Helm install would fail
// silently for exactly the users who installed the other way.
const operatorSelector = "control-plane=controller-manager,app.kubernetes.io/name=kuberecord"

// The keys a credentials Secret carries.
//
// These are copies. The originals are DefaultCredentialsSecretKey and its S3
// siblings in internal/controller/sink_controller.go and s3sink_controller.go,
// which this package may not import (D20) — the CLI is a client of the frozen
// contracts, and the operator's runtime is not one of them. They are copied rather
// than imported for the same reason internal/cli/resource.go carries its own copy
// of the kind resolver: the import *is* the coupling, and a constant with a comment
// naming its source is cheaper to keep in step than a dependency edge is to remove
// later.
const (
	secretKeyPassword        = "password"
	secretKeyAccessKeyID     = "accessKeyId"
	secretKeySecretAccessKey = "secretAccessKey"
	secretKeySessionToken    = "sessionToken"
)

// Clients is the Kubernetes access discovery needs.
//
// Two clients rather than one, and neither of them controller-runtime's. The
// dynamic client reads the sink CRs as unstructured objects, which are then
// converted into the API types this repository already publishes — so the CRD
// remains the single description of those fields without a generated clientset to
// maintain. The typed client reads the two core resources: the Secret a sink
// references, and the Deployment that names the cluster identity.
//
// It is a struct the caller may supply so that a test can drive the whole
// resolution chain against client-go's own fakes, which is the only way to assert
// that a forbidden Secret produces the message the acceptance criterion spells out
// rather than something that merely mentions permissions.
type Clients struct {
	// Dynamic reads the sink custom resources.
	Dynamic dynamic.Interface

	// Typed reads Secrets and Deployments.
	Typed kubernetes.Interface
}

// SinkRef names one sink custom resource.
type SinkRef struct {
	Kind string
	Name string
}

// String renders the ref the way --sink takes it, which is also how every message
// about a sink spells one.
func (s SinkRef) String() string { return s.Kind + "/" + s.Name }

// ParseSinkRef reads a <kind>/<name> value, naming the flag it came from when it
// cannot.
//
// Kinds are matched case-insensitively and in both singular and plural, because
// the flag is typed by hand and `--sink clickhousesink/default` is not a mistake
// anybody needs to be corrected about. What it will not do is guess at a kind it
// does not know: a typo that resolved to the wrong CRD would send the query at the
// wrong archive.
//
// The flag name is a parameter because two flags carry this shape — --sink
// selects a sink to read through, and `config set-profile --from-sink` selects one
// to copy a profile out of — and a correction that named the wrong one would send
// the reader to a flag they did not type.
func ParseSinkRef(flag, value string) (SinkRef, error) {
	kind, name, found := strings.Cut(value, "/")
	if !found || kind == "" || name == "" {
		return SinkRef{}, exit.UsageErrorf(
			"malformed --%s %q: expected <kind>/<name>, for example %s/default",
			flag, value, KindClickHouseSink)
	}

	normalized := strings.TrimSuffix(strings.ToLower(kind), "s")
	for _, known := range sinkKinds {
		if normalized == strings.TrimSuffix(strings.ToLower(known), "s") {
			return SinkRef{Kind: known, Name: name}, nil
		}
	}
	return SinkRef{}, exit.UsageErrorf("--%s names the kind %q, which is not one of %s",
		flag, kind, strings.Join(sinkKinds, ", "))
}

// gvrFor maps a sink kind to the resource that holds it.
func gvrFor(kind string) (schema.GroupVersionResource, error) {
	switch kind {
	case KindClickHouseSink:
		return clickHouseSinkGVR, nil
	case KindS3Sink:
		return s3SinkGVR, nil
	}
	return schema.GroupVersionResource{}, exit.UsageErrorf("no sink kind named %q; one of %s",
		kind, strings.Join(sinkKinds, ", "))
}

// getSink fetches one sink custom resource by kind and name.
func (r *BackendResolver) getSink(ctx context.Context, ref SinkRef) (*unstructured.Unstructured, error) {
	clients, err := r.clients()
	if err != nil {
		return nil, err
	}
	gvr, err := gvrFor(ref.Kind)
	if err != nil {
		return nil, err
	}

	sink, err := clients.Dynamic.Resource(gvr).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, r.classifySinkAccess(err, "read "+ref.String())
	}
	return sink, nil
}

// listSinks returns every sink custom resource in the cluster, of both kinds.
//
// A kind whose CRD is not installed is not an error: a cluster running only the
// archive tier has no ClickHouseSink CRD at all, and refusing to discover its
// S3Sink because of that would be reporting the absence of something nobody
// intended to have. A kind the user may not list *is* an error, because that is a
// gap in what this listing can see and a silent one would make "no sinks found"
// mean two different things.
func (r *BackendResolver) listSinks(ctx context.Context) ([]sinkCandidate, error) {
	clients, err := r.clients()
	if err != nil {
		return nil, err
	}

	var found []sinkCandidate
	for _, kind := range sinkKinds {
		gvr, gvrErr := gvrFor(kind)
		if gvrErr != nil {
			return nil, gvrErr
		}

		list, listErr := clients.Dynamic.Resource(gvr).List(ctx, metav1.ListOptions{})
		if listErr != nil {
			if apierrors.IsNotFound(listErr) || meta.IsNoMatchError(listErr) {
				// The CRD is not installed in this cluster.
				continue
			}
			return nil, r.classifySinkAccess(listErr, "list "+kind)
		}
		for i := range list.Items {
			found = append(found, sinkCandidate{
				ref:    SinkRef{Kind: kind, Name: list.Items[i].GetName()},
				object: &list.Items[i],
			})
		}
	}
	return found, nil
}

// sinkCandidate is one sink the cluster holds, before anything has been resolved
// from it.
type sinkCandidate struct {
	ref    SinkRef
	object *unstructured.Unstructured
}

// classifySinkAccess turns an API error about a sink into a message that names
// what to do next.
//
// A forbidden read here is not a broken cluster and not a broken tool: it is an
// engineer whose RBAC does not extend to cluster-scoped custom resources, which is
// an ordinary state in a locked-down cluster. Telling them so, and naming the two
// routes that do not need the permission, is the difference between a dead end and
// a next step.
func (r *BackendResolver) classifySinkAccess(err error, what string) error {
	if apierrors.IsForbidden(err) {
		return exit.RuntimeErrorf("cannot %s (forbidden); read an archive directly with --%s, "+
			"or configure a profile with `%s config set-profile`", what, options.FlagSource, r.commandName())
	}
	return exit.RuntimeErrorf("cannot %s: %w", what, err)
}

// decodeClickHouseSink converts a discovered object into the published API type.
func decodeClickHouseSink(object *unstructured.Unstructured) (*v1alpha1.ClickHouseSink, error) {
	var sink v1alpha1.ClickHouseSink
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &sink); err != nil {
		return nil, exit.RuntimeErrorf("decoding %s/%s: %w", KindClickHouseSink, object.GetName(), err)
	}
	return &sink, nil
}

// decodeS3Sink converts a discovered object into the published API type.
func decodeS3Sink(object *unstructured.Unstructured) (*v1alpha1.S3Sink, error) {
	var sink v1alpha1.S3Sink
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &sink); err != nil {
		return nil, exit.RuntimeErrorf("decoding %s/%s: %w", KindS3Sink, object.GetName(), err)
	}
	return &sink, nil
}

// secretData reads a Secret a sink references.
//
// The three failures are told apart deliberately, because they are three different
// jobs for whoever reads the message. Forbidden is the one the acceptance criterion
// spells out: it is by far the most common, it is not a fault in the cluster, and
// the remedy — a profile — needs no new permission. Not found means the sink
// references a Secret that is not there, which is a broken sink and the operator
// will be saying so on its own status. Anything else is the API server, reported as
// itself.
func (r *BackendResolver) secretData(ctx context.Context, ref v1alpha1.SecretReference, owner SinkRef,
) (map[string][]byte, error) {
	clients, err := r.clients()
	if err != nil {
		return nil, err
	}

	namespace := ref.Namespace
	if namespace == "" {
		resolved, nsErr := r.operatorNamespace(ctx)
		if nsErr != nil {
			return nil, nsErr
		}
		namespace = resolved
	}

	secret, err := clients.Typed.CoreV1().Secrets(namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsForbidden(err):
		return nil, exit.RuntimeErrorf("cannot read Secret %s/%s (forbidden); configure a profile with "+
			"`%s config set-profile`", namespace, ref.Name, r.commandName())
	case apierrors.IsNotFound(err):
		return nil, exit.RuntimeErrorf("Secret %s/%s does not exist, and %s names it as where its "+
			"credentials live", namespace, ref.Name, owner)
	case err != nil:
		return nil, exit.RuntimeErrorf("cannot read Secret %s/%s: %w", namespace, ref.Name, err)
	}
	return secret.Data, nil
}

// requireSecretKey reads one key, reporting which keys the Secret does hold when it
// is absent.
//
// Naming the keys present is safe and is the whole value of the message: a Secret
// created with `--from-literal=PASSWORD=…` is the mistake this catches, and it is
// invisible until something says the key it looked for was `password`. Key names
// are not secrets; the values they hold never appear here or anywhere else.
func requireSecretKey(data map[string][]byte, key string, owner SinkRef, namespace, name string) (string, error) {
	value, ok := data[key]
	if !ok {
		return "", exit.RuntimeErrorf("Secret %s/%s has no %q key, which is where %s expects its "+
			"credential (keys present: %s)", namespace, name, key, owner, describeKeys(data))
	}
	return string(value), nil
}

// describeKeys lists a Secret's key names for a message about a missing one.
func describeKeys(data map[string][]byte) string {
	if len(data) == 0 {
		return "none"
	}
	names := make([]string, 0, len(data))
	for name := range data {
		names = append(names, name)
	}
	slices.Sort(names)
	return strings.Join(names, ", ")
}

// findOperatorDeployment locates the operator's Deployment, or reports that it
// could not be seen.
//
// It is memoized, error included, because two separate steps want it — the
// namespace a sink's Secret defaults to, and the cluster identity the operator was
// started with — and a search that ran twice would double the cost of a resolution
// the user did not ask for.
//
// The search is scoped to the declared operator namespace when there is one, and
// runs across all namespaces when there is not. Listing cluster-wide is a real
// permission and not everyone has it, which is exactly why the flag and the
// configuration field come first.
//
// Not finding it is not an error. An engineer reading an archive from a cluster
// that was decommissioned last month has no operator to find, and that is precisely
// the case this tool exists to serve; the caller falls through to the next step of
// its chain. A *forbidden* search is likewise not an error, for the same reason: it
// is a permission asked for opportunistically, and demanding it would make a
// locked-down cluster unusable for a question that has other answers. It is
// reported as a notice, because a step that was skipped rather than answered is
// something the user should be able to see (Invariant 4).
func (r *BackendResolver) findOperatorDeployment(ctx context.Context) (*operatorInfo, error) {
	if r.operatorSearched {
		return r.operator, r.operatorErr
	}
	r.operatorSearched = true

	clients, err := r.clients()
	if err != nil {
		r.operatorErr = err
		return nil, err
	}

	namespace := r.declaredOperatorNamespace()
	list, err := clients.Typed.AppsV1().Deployments(namespace).List(ctx,
		metav1.ListOptions{LabelSelector: operatorSelector})
	if err != nil {
		// Every failure here is a notice and not an error, including an
		// unreachable API server. This lookup is a convenience — it saves the user
		// typing an identity the cluster already knows — and the chain that uses
		// it has another step after this one. Failing the whole invocation because
		// an optional shortcut did not work would mean a laptop with a stale
		// kubeconfig could not read an archive sitting on its own disk, which is
		// the case this tool most wants to serve (D18).
		//
		// Recorded as well as printed, in one string used twice, so that the
		// resolution report on stdout says the same thing this notice says on
		// stderr rather than a vaguer version of it.
		r.operatorUnseen = fmt.Sprintf("cannot list Deployments%s (%s)",
			describeNamespace(namespace), reasonFor(err))
		r.notef("%s, so the cluster identity cannot be read from the operator", r.operatorUnseen)
		return nil, nil
	}
	if len(list.Items) == 0 {
		return nil, nil
	}

	deployment := &list.Items[0]
	info := &operatorInfo{
		namespace: deployment.GetNamespace(),
		name:      deployment.GetName(),
		clusterID: clusterIDFromPodSpec(deployment.Spec.Template.Spec.Containers),
	}
	if len(list.Items) > 1 {
		// Several operators is a legitimate installation — one per tenant, one per
		// team — and picking the first silently would attribute the wrong cluster
		// identity to a query. Say which was used and how to override it.
		r.notef("this cluster has %d kuberecord operators; reading the identity from %s/%s "+
			"(override with --%s)", len(list.Items), info.namespace, info.name, options.FlagClusterID)
	}
	r.operator = info
	return info, nil
}

// operatorInfo is what the operator's Deployment tells us about itself.
type operatorInfo struct {
	namespace string
	name      string
	// clusterID is the identity the operator stamps on every row it writes, or
	// empty if the Deployment does not say — in which case the operator is running
	// on the flag's own default and this cannot report it as a fact.
	clusterID string
}

// reasonFor renders why an API call failed, short enough for a notice.
//
// Forbidden is named as itself rather than quoted from the API server, because it
// is the one a reader acts on differently: it is not a broken cluster, it is a
// permission this tool asked for opportunistically and did not get.
func reasonFor(err error) string {
	if apierrors.IsForbidden(err) {
		return "forbidden"
	}
	return err.Error()
}

// describeNamespace renders a namespace for a message, including the
// all-namespaces case.
func describeNamespace(namespace string) string {
	if namespace == "" {
		return " across all namespaces"
	}
	return " in " + namespace
}

// clusterIDFromPodSpec reads the cluster identity out of the manager container.
//
// Both spellings are read, and the acceptance criterion's assumption is worth
// stating plainly: it says the operator's `CLUSTER_ID` *argument*, and in the
// shipped chart it is an environment variable (`CLUSTER_ID`), while cmd/main.go
// also accepts `--cluster-id` as a flag and falls back to that variable. So both
// are read here, the argument first, because a flag on the command line overrides
// the environment in the operator itself and this must agree with it.
//
// An operator that says neither is running on the flag's built-in default, which
// this deliberately does not report: guessing a default would produce an identity
// that matches nothing in the sink, and an empty answer here sends the caller to
// the next step of the chain, which asks the sink itself.
func clusterIDFromPodSpec(containers []corev1.Container) string {
	for _, container := range containers {
		if id := clusterIDFromArgs(container.Args); id != "" {
			return id
		}
		if id := clusterIDFromArgs(container.Command); id != "" {
			return id
		}
		for _, env := range container.Env {
			// Only a literal value can be read. A valueFrom reference would need
			// another API read and, for a Secret, a permission this tool must not
			// require for a convenience.
			if env.Name == "CLUSTER_ID" && env.Value != "" {
				return env.Value
			}
		}
	}
	return ""
}

// clusterIDFromArgs reads --cluster-id from an argument list, in both spellings.
func clusterIDFromArgs(args []string) string {
	for i, arg := range args {
		if value, found := strings.CutPrefix(arg, "--"+options.FlagClusterID+"="); found {
			return value
		}
		if arg == "--"+options.FlagClusterID && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

// errNoSinksFound is returned when discovery ran and the cluster holds no sink.
//
// It is a sentinel because the caller has more context than this function does:
// the message a user needs names every route they have not tried, and only the
// resolver knows which of them are still open.
var errNoSinksFound = errors.New("no sink custom resources in this cluster")

// describeSinks renders a list of discovered sinks for a message asking the user to
// choose.
func describeSinks(candidates []sinkCandidate) string {
	names := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		names = append(names, candidate.ref.String())
	}
	return strings.Join(names, ", ")
}
