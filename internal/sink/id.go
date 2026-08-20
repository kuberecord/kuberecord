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

package sink

import "cmp"

// DefaultSinkKind is the kind an *unqualified* sink reference resolves to.
//
// It is ClickHouseSink for exactly one reason: ClickHouse was the first backend,
// so every rule written against v0.1.0 named a ClickHouseSink implicitly and
// every bare sink name still in the codebase means one. It is not a privileged
// kind — nothing in the runtime treats it specially, no path falls back to it
// when another kind fails to resolve, and D6's future backends are chosen just
// as freely.
//
// Its only legitimate uses are the two places a kind is genuinely known rather
// than guessed: lifting a legacy unqualified name onto a typed identity, and a
// ClickHouseSink-specific component naming its own kind. In particular
// SinkManager.Ensure deliberately does *not* apply it — defaulting the kind
// there would resurrect the collision typed identity exists to prevent, so an
// incomplete ID is rejected instead.
const DefaultSinkKind = "ClickHouseSink"

// ID identifies one sink instance: which kind of backend, and which CR of that
// kind. It is the runtime's key for everything owned per sink — the routing
// table, the dedup caches, the per-sink metric series.
//
// The kind is part of the identity because a name is only unique *within* a
// kind: a ClickHouseSink named "default" and an S3Sink named "default" are both
// legal in etcd and are two entirely unrelated backends. Keyed on the name
// alone, whichever reconciled second silently displaced the first, and rules
// would then stream to a backend carrying another one's hashCache and warm
// state — re-emitting every object, or suppressing genuine changes, with
// nothing in the logs to say so.
//
// It is two plain strings so that it stays comparable: directly usable as a map
// key and as part of a workqueue item, with no allocation on the hot path and no
// hand-built composite string for a call site to get wrong.
type ID struct {
	// Kind is the sink CR's kind, spelled as the API server spells it
	// ("ClickHouseSink"), so a log line or a condition message names something an
	// operator can pass straight to kubectl.
	Kind string
	// Name is the sink CR's name. Sink CRs are cluster-scoped (D6), so a kind and
	// a name are a complete reference — there is no namespace to carry.
	Name string
}

// String renders the identity as "<Kind>/<Name>". It is the single rendering
// used for log values, metric label values and condition messages, so one sink
// reads the same way everywhere an operator might meet it.
//
// "/" is a safe separator precisely because a Kubernetes kind cannot contain
// one: the rendering is unambiguous, and an operator reading sink="S3Sink/audit"
// off a dashboard knows which of two same-named backends it describes.
func (id ID) String() string { return id.Kind + "/" + id.Name }

// compareIDs orders IDs by kind, then by name.
//
// It exists so the two places that iterate the sink registry for a human — the
// start-up pass over pending sinks and SinkIDs — stay deterministic now that
// their keys are structs rather than sortable strings. Ordering by the fields
// rather than by String() keeps the sort independent of the rendering.
func compareIDs(a, b ID) int {
	return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Name, b.Name))
}
