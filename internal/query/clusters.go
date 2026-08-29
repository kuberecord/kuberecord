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

package query

import "context"

// ClusterIDLister is the optional half of the read plane for engines that can say
// which clusters they hold history for.
//
// Every other question in this package is asked *about* a cluster:
// [TimelineQuery] carries one in its [ObjectRef], [ScopeQuery] carries one
// outright. That leaves a gap at the very start of an invocation, because the
// caller has to name a cluster before it can ask anything, and the cluster
// identity is a string an operator chose — not something a kubeconfig knows.
//
// This interface closes that gap, and the shape of the closing is the point. A
// command-line client resolves the identity from its flag, its configuration and
// the operator's own Deployment first; this is the last resort before an error,
// and it is what turns that error from "pass --cluster-id" into "pass
// --cluster-id: this sink holds prod-eu-1, prod-us-1". A failure that lists the
// values a user could have typed is a different experience from one that tells
// them a flag exists.
//
// It is a separate interface rather than a method on [QueryEngine] for the same
// reason [ScanEstimator] is: not every engine can answer cheaply or at all, and a
// method that some implementations had to stub would make "no clusters" and "I
// cannot tell you" the same empty slice. So the capability is detected:
//
//	if lister, ok := engine.(query.ClusterIDLister); ok {
//	        ...
//	}
//
// and a caller that finds it absent says so, rather than reporting an emptiness it
// did not measure (Invariant 4).
type ClusterIDLister interface {
	// ClusterIDs reports every distinct cluster_id the engine can see, sorted, with
	// no duplicates.
	//
	// The sort is part of the contract because the result is rendered to a human in
	// an error message and compared in tests; an order that varied with storage
	// layout would make both worse for no gain.
	//
	// This is permitted to be the most expensive question in the package. An engine
	// with no index over the identity answers it by listing, and the caller is
	// expected to reach for it only when it has no cheaper way to learn the cluster —
	// which is why it takes no window: narrowing by time would make an old, quiet
	// cluster disappear from the very list that exists to tell a user it is there.
	//
	// Errors: whatever prevented the read. An engine holding no history at all
	// returns an empty slice and a nil error, which is a result — the sink is empty —
	// and not a failure.
	ClusterIDs(ctx context.Context) ([]string, error)
}
