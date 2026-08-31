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
	"fmt"
	"strings"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The kuberecord cluster identity is a string somebody chose when they installed
// the operator, and it is not derivable from a kubeconfig — that is the whole of
// why D21 keeps --cluster-id and --cluster apart. So it is resolved by a chain,
// and the chain is ordered by how much evidence each step has:
//
//  1. --cluster-id, which is the user saying it outright.
//  2. the configuration file's context mapping, which is the user having said it
//     once for this kubeconfig context.
//  3. the operator's own Deployment, which is the cluster stating what it stamps
//     on every row it writes. This is the step that makes the common case need no
//     configuration at all.
//  4. the sink, if it holds exactly one cluster's history. This is what makes the
//     zero-infrastructure path zero-configuration too: an archive on a laptop has
//     no kubeconfig and no operator, and steps 2 and 3 have nothing to say.
//  5. an error naming the values that were found, which is the difference between
//     a dead end and a next step.
//
// Every step announces which one answered, because reading the wrong cluster's
// history is a failure that looks exactly like a correct answer.

// resolveClusterID walks the chain and returns the identity and how it was found.
//
// engine is consulted only by step 4, and only if the earlier steps found nothing:
// it is the most expensive question in the read plane on an archive backend, and
// paying for it when the user already said --cluster-id would be charging for an
// answer nobody asked for.
func (r *BackendResolver) resolveClusterID(ctx context.Context, engine query.QueryEngine) (string, string, error) {
	if id := r.Flags.ClusterID; id != "" {
		return id, "from --" + options.FlagClusterID, nil
	}

	if id, contextName := r.clusterIDFromContext(); id != "" {
		return id, fmt.Sprintf("from %s, which maps the context %q", r.ConfigPath, contextName), nil
	}

	id, source, err := r.clusterIDFromOperator(ctx)
	if err != nil {
		return "", "", err
	}
	if id != "" {
		return id, source, nil
	}

	return r.clusterIDFromSink(ctx, engine)
}

// clusterIDFromContext reads the configuration file's context mapping.
//
// The context is the one this invocation is actually using — --context when given,
// the kubeconfig's current-context otherwise — because those are the two ways a
// user selects a cluster and a mapping keyed on the wrong one would answer for a
// cluster they are not looking at.
//
// No kubeconfig at all is an ordinary state here (an archive on a laptop), and it
// resolves to no context and therefore no mapping, silently: the chain continues,
// and the failure message at the end of it lists every step that had nothing to
// say.
func (r *BackendResolver) clusterIDFromContext() (id, contextName string) {
	if len(r.Config.Contexts) == 0 {
		return "", ""
	}
	contextName = r.KubeContext()
	if contextName == "" {
		return "", ""
	}
	return r.Config.Contexts[contextName], contextName
}

// KubeContext reports the kubeconfig context this invocation is using.
func (r *BackendResolver) KubeContext() string {
	if r.Flags == nil || r.Flags.ConfigFlags == nil {
		return ""
	}
	if r.Flags.ConfigFlags.Context != nil && *r.Flags.ConfigFlags.Context != "" {
		return *r.Flags.ConfigFlags.Context
	}
	raw, err := r.Flags.ConfigFlags.ToRawKubeConfigLoader().RawConfig()
	if err != nil {
		// No readable kubeconfig. That is not a failure of this step; it is this
		// step having nothing to work with, which the chain handles by continuing.
		return ""
	}
	return raw.CurrentContext
}

// clusterIDFromOperator reads the identity the operator was started with.
//
// This is the step that makes the ordinary case zero-config, and it is also the
// step most likely to be unavailable: the cluster may be gone, the Deployment may
// be in a namespace this user cannot list, or the operator may be running on the
// flag's built-in default and therefore saying nothing about its identity. All
// three resolve to "no answer" and continue the chain rather than failing it. Only
// an API error that is not a permission problem stops the walk, because that one
// means the cluster could not be asked rather than that it had nothing to say.
func (r *BackendResolver) clusterIDFromOperator(ctx context.Context) (id, source string, err error) {
	if _, clientErr := r.clients(); clientErr != nil {
		// No cluster to ask. Reading an archive without a kubeconfig is a supported
		// and expected shape, so this is not reported here; the chain's own failure
		// message says which steps had nothing.
		return "", "", nil
	}

	info, err := r.findOperatorDeployment(ctx)
	if err != nil {
		return "", "", err
	}
	if info == nil || info.clusterID == "" {
		return "", "", nil
	}
	return info.clusterID, fmt.Sprintf("from the operator Deployment %s/%s", info.namespace, info.name), nil
}

// clusterIDFromSink asks the backend which clusters it holds, and is the last step
// before an error.
//
// Exactly one is the answer. Zero and several are both failures, and so is a
// backend that cannot be asked — which is reported as the capability gap it is
// rather than as an empty result (Invariant 4): "this backend cannot enumerate
// clusters" and "this sink holds none" are different facts, and only one of them
// means the user is pointing at the wrong place.
func (r *BackendResolver) clusterIDFromSink(ctx context.Context, engine query.QueryEngine) (string, string, error) {
	lister, ok := engine.(query.ClusterIDLister)
	if !ok {
		return "", "", exit.RuntimeErrorf(
			"no cluster identity: nothing named one and this backend cannot list the clusters it "+
				"holds. %s", r.clusterIDRemedies())
	}

	ids, err := lister.ClusterIDs(ctx)
	if err != nil {
		return "", "", exit.RuntimeErrorf("no cluster identity: nothing named one, and asking the backend "+
			"which clusters it holds failed: %w", err)
	}

	switch len(ids) {
	case 0:
		return "", "", exit.RuntimeErrorf("no cluster identity: this sink holds no recorded history at "+
			"all, so there is nothing here to name. Check that the operator is streaming to it, or "+
			"read a different source with --%s", options.FlagSource)
	case 1:
		return ids[0], "the only cluster in this sink", nil
	}
	return "", "", exit.RuntimeErrorf("no cluster identity: this sink holds %d of them (%s). %s",
		len(ids), strings.Join(ids, ", "), r.clusterIDRemedies())
}

// clusterIDRemedies is the sentence that turns a failed resolution into an
// instruction.
//
// Both routes are named because they serve different users: the flag is for the
// one-off, and the context mapping is for the engineer who works across four
// clusters and should have to say it once per kubeconfig context rather than once
// per command.
func (r *BackendResolver) clusterIDRemedies() string {
	return fmt.Sprintf("Pass --%s, or record it for this kubeconfig context with "+
		"`%s config set-context-cluster-id`", options.FlagClusterID, r.commandName())
}
