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

// Package sink defines the backend-agnostic contract every kuberecord storage
// backend implements. The pipeline (reconcilers, cache warm-up) depends only on
// these interfaces and value types, never on a concrete driver, so a further
// backend — Postgres, Elasticsearch, Kafka, all still genuinely future — is a new
// implementation of Writer / ScopeEventWriter / StateReader rather than a change
// to the hot path.
//
// Two implementations ship today, and between them they are the evidence that the
// contract's optional halves are real rather than theoretical. ClickHouse
// (internal/sink/clickhouse) implements all four: Writer, StateReader,
// ScopeEventWriter and Prober. S3 (internal/sink/s3) implements every half except
// StateReader, because an archive tier cannot read its own history back — what
// that costs, and why it is a declared limit rather than a gap, is documented once
// at internal/sink/s3/instance.go and in docs/TEE.md rather than restated here.
//
// A backend is therefore free to implement as little as Writer. That is the point
// of splitting the contract: the pipeline asks what a sink can do instead of
// assuming, and a sink that can do less degrades visibly rather than silently.
package sink

import "time"

// Record is the universal structure used to send a row to a sink. It is a pure
// data type: how a Record maps onto a backend's schema (column order, encoding,
// query) is entirely the backend implementation's concern, so this struct
// carries no query or driver detail.
type Record struct {
	Timestamp       time.Time         `json:"timestamp"`
	ClusterID       string            `json:"cluster_id"`
	EventType       string            `json:"event_type"` // Added, Modified, Deleted, Snapshot
	APIGroup        string            `json:"group"`
	APIVersion      string            `json:"version"`
	Kind            string            `json:"kind"`
	Namespace       string            `json:"namespace"`
	Name            string            `json:"name"`
	UID             string            `json:"uid"`
	ResourceVersion string            `json:"resource_version"`
	Labels          map[string]string `json:"labels"`
	// Actors are the distinct, sorted field-manager names harvested from the
	// object's managedFields (see extractActors and the resource_states.actors
	// column) — the cheapest "who probably changed this" signal. Deleted rows
	// carry no actors: there is no live object left to inspect, so a deletion's
	// authorship is intentionally not attributed here.
	Actors []string `json:"actors"`
	Data   string   `json:"data"` // Full JSON (for Added)
	// Diff is an RFC 6902 JSON Patch (wI2L/jsondiff) describing the change on a
	// Modified event; empty on every other event type. Named to match the
	// schema-v1 "diff" column (renamed from the old "diff_data").
	Diff   string `json:"diff"`
	SHA256 string `json:"sha256"`
}
