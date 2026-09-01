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
	chquery "github.com/kuberecord/kuberecord/internal/query/clickhouse"
	"github.com/kuberecord/kuberecord/internal/query/objectsource"
)

// What a binary can read, reported by the binary rather than by its documentation.
//
// `kuberecord version` prints this (Task 12.1), and the question it answers is
// narrow and practical: somebody has been told to point the CLI at an archive in a
// bucket, and the invocation failed. Whether their build can open a bucket at all
// is the first thing to establish, and a documentation page cannot answer it for
// the binary in front of them.
//
// It lives beside the resolution chain rather than in the command package because
// this package is the only one that opens anything. A list assembled next to the
// cobra command would be a second statement of what the CLI supports, and the two
// would agree until the day a backend was added.

// CompiledBackend is one place this build knows how to read history from.
type CompiledBackend struct {
	// Kind is the name a --source, a --sink or a profile spells, and the value of
	// a profile's `backend:` field.
	Kind BackendKind

	// Engine is the read-plane engine that kind opens, spelled exactly as
	// metadata.backend spells it in structured output. Two kinds share one engine
	// — a bucket and a directory are the same format read through different
	// sources — and printing both columns is what lets a reader match a `version`
	// line against the `backend` field of an answer they are holding.
	Engine string

	// Description is what that kind reads, in the vocabulary of the two frozen
	// formats: the ClickHouse schema (`v1`) and the archive object format
	// (`jsonl-v1`, D15).
	Description string
}

// CompiledBackends reports the query backends this build can open, in the order
// BackendKinds lists them.
//
// Every entry is compiled in unconditionally: D18 makes the CLI pure Go with no
// cgo, so there is no build tag that could leave one of these out and no
// configuration that could add one. The list is still reported rather than
// assumed, because "what this binary supports" is a fact about the artifact, and
// the day it stops being uniform is the day a user needs to be able to ask.
func CompiledBackends() []CompiledBackend {
	return []CompiledBackend{
		{
			Kind:        BackendClickHouse,
			Engine:      chquery.BackendName,
			Description: "schema v1 in ClickHouse",
		},
		{
			Kind:        BackendS3,
			Engine:      objectsource.BackendName,
			Description: "jsonl-v1 archive in an S3-compatible bucket",
		},
		{
			Kind:        BackendLocal,
			Engine:      objectsource.BackendName,
			Description: "jsonl-v1 archive in a directory",
		},
	}
}
