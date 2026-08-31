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
	"fmt"
	"strings"
	"unicode"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/query"
)

// ResourceArg is one object address as the user typed it, before any cluster
// was consulted.
//
// Parsing and resolution are separate because only resolution needs a cluster.
// A malformed address is a usage error that can be reported instantly and
// offline; an address naming a kind this cluster does not serve is a runtime
// finding that costs a discovery round-trip. Collapsing them would make every
// "you typed it wrong" message wait on the network, and would make the parser
// untestable without a fake API server.
type ResourceArg struct {
	// Resource is the kind, resource or short name as typed: "deploy",
	// "deployments", "deployments.apps", "Deployment".
	Resource string

	// Name is the object's name.
	Name string
}

// ParseResourceArg reads the positional arguments of a command addressing one
// object.
//
// It accepts the two spellings kubectl accepts — `kind/name` and `kind name` —
// and refuses everything else with a usage error. One object, not several: every
// command in this release answers a question about a single object's history,
// and accepting a list only to reject it later would be a worse message than
// refusing it here.
func ParseResourceArg(args []string) (ResourceArg, error) {
	switch len(args) {
	case 0:
		return ResourceArg{}, exit.UsageErrorf(
			"no object given: expected <kind>/<name> or <kind> <name>, for example deploy/nginx")

	case 1:
		resource, name, found := strings.Cut(args[0], "/")
		switch {
		case !found:
			return ResourceArg{}, exit.UsageErrorf(
				"no object name given for %q: expected %s/<name> or %s <name>",
				args[0], args[0], args[0])
		case resource == "" || name == "":
			return ResourceArg{}, exit.UsageErrorf(
				"malformed object address %q: expected <kind>/<name>, for example deploy/nginx", args[0])
		case strings.Contains(name, "/"):
			return ResourceArg{}, exit.UsageErrorf(
				"malformed object address %q: expected exactly one %q", args[0], "/")
		}
		return ResourceArg{Resource: resource, Name: name}, nil

	case 2:
		if strings.Contains(args[0], "/") {
			return ResourceArg{}, exit.UsageErrorf(
				"mixed object address forms: %q already names an object, so %q is unexpected",
				args[0], args[1])
		}
		if args[0] == "" || args[1] == "" {
			return ResourceArg{}, exit.UsageErrorf(
				"malformed object address: expected <kind> <name>, for example deploy nginx")
		}
		return ResourceArg{Resource: args[0], Name: args[1]}, nil

	default:
		return ResourceArg{}, exit.UsageErrorf(
			"expected one object, got %d arguments: these commands answer questions about a single object",
			len(args))
	}
}

// UnknownResourceError reports that an address named a kind this cluster does
// not serve.
//
// It is a distinct type rather than a bare error because the two ways an address
// can fail need different messages: this one means the cluster has no such kind,
// which a user fixes by installing a CRD or correcting a spelling, and it is
// worth telling them which of the two they are looking at without their having
// to read a REST mapper's own phrasing.
type UnknownResourceError struct {
	// Address is the resource token as the user typed it, so the message names
	// what they wrote rather than what it was normalized into.
	Address string

	// Err is the REST mapper's verdict, kept for Unwrap so the precise discovery
	// failure survives into `-v` output.
	Err error
}

func (e *UnknownResourceError) Error() string {
	return fmt.Sprintf("the cluster does not serve any resource named %q: "+
		"check the spelling, or `kubectl api-resources` for what it does serve", e.Address)
}

// Unwrap exposes the mapper's error so `-v` can show whether the group, the
// version or only the resource was missing.
func (e *UnknownResourceError) Unwrap() error { return e.Err }

// ResolvedResource is an address that a cluster's discovery data has agreed to.
type ResolvedResource struct {
	// GVK is the kind the address names. Its Group and Kind are what identity is
	// expressed in: the schema's canonical key is version-agnostic (Invariant 7),
	// so the version here is provenance rather than identity.
	GVK schema.GroupVersionKind

	// GVR is the resource the kind maps to, carried because the plural name is
	// what a user will check against `kubectl api-resources`.
	GVR schema.GroupVersionResource

	// Namespaced reports whether the kind is namespaced. A cluster-scoped kind
	// has no namespace to record, and ObjectRef drops one accordingly.
	Namespaced bool

	// Name is the object's name, carried through from the address.
	Name string
}

// ObjectRef turns a resolved address into the read plane's canonical identity.
//
// namespace is ignored for a cluster-scoped kind, because such an object has no
// namespace in the recorded history and a reference carrying one would match
// nothing. A caller that set --namespace explicitly should say so on stderr
// before discarding it; this function stays pure so that the decision to warn
// belongs to the command that knows whether the flag was typed.
func (r ResolvedResource) ObjectRef(clusterID, namespace string) query.ObjectRef {
	if !r.Namespaced {
		namespace = ""
	}
	return query.ObjectRef{
		ClusterID: clusterID,
		APIGroup:  r.GVK.Group,
		Kind:      r.GVK.Kind,
		Namespace: namespace,
		Name:      r.Name,
	}
}

// parseRecordedKind reads a token as the identity the schema itself stores: a
// capitalised Kind, optionally qualified with its group.
//
// It is the whole of what can be resolved without a cluster, and the capital is
// the test that keeps it honest. `deploy` and `deployments` are not kinds; turning
// either into one would need the server's own discovery data, and guessing at it
// offline would silently read a different object's history — or, for `scopes`,
// report that a kind nobody spells that way was never watched.
//
// The boolean rather than an error is deliberate: the callers phrase the failure
// differently — one is addressing an object and can name it, the other is
// narrowing a listing — and a shared message would fit neither.
func parseRecordedKind(token string) (schema.GroupVersionKind, bool) {
	fullySpecified, groupKind := schema.ParseKindArg(token)
	gvk := groupKind.WithVersion("")
	if fullySpecified != nil {
		gvk = *fullySpecified
	}
	if gvk.Kind == "" || !unicode.IsUpper(rune(gvk.Kind[0])) {
		return schema.GroupVersionKind{}, false
	}
	return gvk, true
}

// Resolver turns an address into the kind, resource and scope it names.
//
// It is constructed over a meta.RESTMapper rather than over a kubeconfig so that
// the whole of resolution is testable against a hand-built mapper, and so that
// the mapper the CLI actually uses — cli-runtime's, which already wraps a
// shortcut expander — is a wiring decision made once at the call site.
type Resolver struct {
	mapper meta.RESTMapper
}

// NewResolver returns a Resolver over mapper.
//
// In production mapper comes from ConfigFlags.ToRESTMapper(), which is a
// discovery-backed mapper wrapped in restmapper.ShortcutExpander. The expander
// is where `deploy`, `sts`, `cm` and `ing` come from: it reads each resource's
// ShortNames out of the server's own discovery data, which is the same code path
// and the same data kubectl uses, so a short name resolves here exactly as it
// does there — including for a CRD that declares short names of its own.
func NewResolver(mapper meta.RESTMapper) *Resolver {
	return &Resolver{mapper: mapper}
}

// Resolve maps arg onto the kind it names and reports whether that kind is
// namespaced.
//
// # What is copied, and from where
//
// The shape below — try a fully-specified resource.version.group first, fall
// back to a group-qualified resource, and only then treat the token as a kind —
// is kubectl's own address resolution (k8s.io/cli-runtime/pkg/resource,
// Builder.mappingFor), reduced to the single-object case. It is copied rather
// than imported because that package pulls kustomize's API and kyaml in behind
// it, for a resource builder this CLI does not use.
//
// The classification of a failure is copied from the operator's resolver,
// internal/watch/resolver.go (see its classify function), and must stay in step
// with it: a no-match is routed through meta.IsNoMatchError rather than through
// a type assertion on *meta.NoKindMatchError, because the pinned mapper reports
// the condition in more than one shape — an unknown kind inside a known group is
// a NoKindMatchError, while an entirely unknown group arrives as a multi-error
// wrapping NoResourceMatchError. A type assertion would misclassify the second,
// which is by far the common case, as a discovery outage.
//
// It is copied rather than imported because D20 forbids this package from
// depending on internal/watch at all: the CLI is a client of the frozen schema,
// not of the operator's runtime.
func (r *Resolver) Resolve(arg ResourceArg) (ResolvedResource, error) {
	// Resource names are lower-case by API convention, and kubectl accepts
	// `Deployment/x` as readily as `deployment/x`. Normalizing here rather than
	// asking the mapper twice keeps the two spellings genuinely identical.
	token := strings.ToLower(arg.Resource)

	gvk, err := r.kindFor(token)
	if err != nil {
		return ResolvedResource{}, err
	}

	mapping, err := r.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return ResolvedResource{}, r.classify(arg.Resource, err)
	}

	return ResolvedResource{
		GVK:        mapping.GroupVersionKind,
		GVR:        mapping.Resource,
		Namespaced: mapping.Scope.Name() == meta.RESTScopeNameNamespace,
		Name:       arg.Name,
	}, nil
}

// kindFor resolves a resource token to a kind, trying the address forms in the
// order kubectl tries them.
//
// The order is what makes `deployments.apps` unambiguous: read as a resource it
// is the deployments resource in the apps group, and read as a kind it would be
// a kind literally named "deployments" in group "apps", which no cluster serves.
// Resources are tried first because that is the form a user is far more likely
// to have typed, and because short names only exist on that side.
func (r *Resolver) kindFor(token string) (schema.GroupVersionKind, error) {
	// ParseResourceArg splits "resource.version.group" when there are at least
	// two dots, and always also yields the "resource.group" reading. Both are
	// tried, most specific first: a token may have had two dots for an unrelated
	// reason, so the fully-specified reading failing says nothing about the
	// group-qualified one.
	fullySpecifiedGVR, groupResource := schema.ParseResourceArg(token)

	candidates := make([]schema.GroupVersionResource, 0, 2)
	if fullySpecifiedGVR != nil {
		candidates = append(candidates, *fullySpecifiedGVR)
	}
	candidates = append(candidates, groupResource.WithVersion(""))

	for _, candidate := range candidates {
		// A reading that misses is not a failure and its error is not reported:
		// it would name a resource the user did not mean. The verdict that
		// matters is the last reading's, and that one is reported — by the
		// RESTMapping call below, which is the only attempt whose failure means
		// the address cannot be understood at all.
		if gvk, err := r.mapper.KindFor(candidate); err == nil && !gvk.Empty() {
			return gvk, nil
		}
	}

	// Nothing read it as a resource, so try it as a kind. This is what lets an
	// address name a kind that has no distinct resource spelling, and it is the
	// step whose failure is the one worth reporting.
	fullySpecifiedGVK, groupKind := schema.ParseKindArg(token)
	if fullySpecifiedGVK == nil {
		withoutVersion := groupKind.WithVersion("")
		fullySpecifiedGVK = &withoutVersion
	}
	mapping, err := r.mapper.RESTMapping(fullySpecifiedGVK.GroupKind(), fullySpecifiedGVK.Version)
	if err != nil {
		return schema.GroupVersionKind{}, r.classify(token, err)
	}
	return mapping.GroupVersionKind, nil
}

// classify turns a REST mapper failure into UnknownResourceError when — and only
// when — it means "this cluster does not serve that".
//
// Anything else (an unreachable or erroring API server) is passed through
// wrapped: it is not the address's fault and must not be reported as a typo.
func (r *Resolver) classify(address string, err error) error {
	if meta.IsNoMatchError(err) {
		return &UnknownResourceError{Address: address, Err: err}
	}
	return fmt.Errorf("resolve %q against the cluster: %w", address, err)
}
