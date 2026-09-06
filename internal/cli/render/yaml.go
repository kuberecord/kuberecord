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

package render

import (
	"encoding/json"
	"fmt"

	yaml "go.yaml.in/yaml/v2"
)

// The YAML serialization of every document this CLI writes, in declaration
// order.
//
// # Why sigs.k8s.io/yaml is not used here
//
// It is the familiar import, it is what the rest of this repository reaches for,
// and it is the wrong tool for exactly one reason: it sorts. `yaml.Marshal(v)`
// there is `json.Marshal(v)` followed by `JSONToYAML`, and JSONToYAML parses the
// JSON into an `interface{}` — a Go map. A map has no order, so the emitter
// receives the keys sorted and writes them that way.
//
// Nothing chose that. `encoding/json` emits struct fields in declaration order,
// so an envelope is `apiVersion, kind, metadata, items` right up until the map,
// and the map is where it becomes `apiVersion, items, kind, metadata` — `kind`
// and `metadata` below a several-hundred-line `items` array. Every Kubernetes
// document a reader has ever opened begins `apiVersion, kind, metadata`, and one
// that begins `apiVersion, items` reads as malformed even when it is not.
//
// The defect survived two phases because nothing pinned the order. That is why
// the goldens in testdata/envelope pin it per kind rather than only end to end:
// a library that changed its mind about sorting must fail a test here, not
// produce a document somebody notices in a support thread.
//
// # What this does instead
//
// The same two steps, with one type changed. The document is marshalled to JSON,
// which fixes the order, and the JSON is then parsed into a yaml.MapSlice — an
// ordered mapping — rather than into a map. Everything downstream is the emitter
// sigs.k8s.io/yaml already uses, reached through the library it already uses it
// from, so key order is the only thing that differs from what this CLI emitted
// before: quoting, block scalars, sequence indentation, integers too large for
// int64 and the normalization of 1.50 to 1.5 are all unchanged, and a test
// asserts that by marshalling both ways and comparing the parsed structures.
//
// # Why not yaml.v3 against the struct
//
// Marshalling the envelope struct with a v3 library preserves declaration order
// directly, which sounds like the shorter road. It is not, and each of the three
// reasons is a regression rather than an inconvenience:
//
//   - v3 reads `yaml:` tags, not `json:` ones. Every field would need a second
//     tag spelling a frozen schema column a second time — the thing ChangeItem's
//     doc comment refuses in as many words, because a second spelling of
//     "resource_version" is a spelling that eventually disagrees. The embedded
//     query.Change could not be tagged from this package at all.
//   - json.RawMessage is a []byte, and a v3 encoder writes []byte as base64.
//     `data` and `diff` became real structures one task ago precisely so that a
//     consumer would not have to parse twice; they would become base64 blobs
//     nobody can parse at all.
//   - v3 resolves scalars by YAML 1.2, where `yes` is a string, so it emits a
//     recorded string "yes" unquoted. Every YAML 1.1 reader — sigs.k8s.io/yaml,
//     and therefore kubectl and this repository's own tests — reads that back as
//     boolean true. A recorded value silently changing type in the output of an
//     audit tool is the failure Invariant 4 exists to forbid, and it would not
//     be visible in review.

// YAMLDocument renders one whole document as YAML, keys in declaration order.
//
// It is the single YAML encoder for everything this CLI writes to stdout — the
// envelope of all five kinds, the `config resolve` report and the `version`
// document — so that the property a reader learns from one of them holds for all
// of them, and so that the reasoning above lives in one place rather than at
// four call sites that would drift.
//
// The document is named in both failures because a bare "error marshalling" is
// the uninformative error Invariant 4 rules out: the reader needs to know which
// document this CLI could not render.
func YAMLDocument(document any) (string, error) {
	encoded, err := json.Marshal(document)
	if err != nil {
		return "", fmt.Errorf("encoding %T for YAML: %w", document, err)
	}

	// Into a MapSlice rather than an interface{}: this single choice is the whole
	// of the fix, and reverting it reverts the key order silently.
	var ordered yaml.MapSlice
	if err := yaml.Unmarshal(encoded, &ordered); err != nil {
		return "", fmt.Errorf("reading %T back as an ordered document: %w", document, err)
	}

	rendered, err := yaml.Marshal(ordered)
	if err != nil {
		return "", fmt.Errorf("encoding %T as YAML: %w", document, err)
	}
	return string(rendered), nil
}
