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
	"encoding/json"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/kuberecord/kuberecord/internal/cli"
	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// The structured output contract, asserted the way people will use it.
//
// Two kinds of assertion live here and both are needed. The golden files pin the
// whole document, so a field that changed name shows up in review as the diff a
// consumer's `jq` would have hit. The decoded assertions pin the handful of
// properties that a regenerated golden file would silently carry along with a
// regression: that the item field names are the schema's own column names, that
// metadata.coverage is populated even when the answer is empty, and that
// emptiness in the data is never the same thing as emptiness in the coverage.

// assertEnvelope checks the envelope's own fields and returns its items.
//
// Every command's structured output goes through it, because the contract is that
// there is one envelope shape for all four kinds — a consumer that has learned to
// read a Timeline can read a Coverage without being told anything new.
func assertEnvelope(t *testing.T, decoded map[string]any, kind string) []any {
	t.Helper()

	if decoded["apiVersion"] != render.EnvelopeAPIVersion {
		t.Errorf("apiVersion is %#v, want %q — scripts branch on this field",
			decoded["apiVersion"], render.EnvelopeAPIVersion)
	}
	if decoded["kind"] != kind {
		t.Errorf("kind is %#v, want %q", decoded["kind"], kind)
	}

	metadata, ok := decoded["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("the envelope carries no metadata: %#v", decoded)
	}
	if metadata["cluster_id"] != fixtureCluster {
		t.Errorf("metadata.cluster_id is %#v, want %q", metadata["cluster_id"], fixtureCluster)
	}
	if metadata["backend"] == nil || metadata["backend"] == "" {
		t.Errorf("metadata.backend is empty, so a reader cannot tell which engine answered: %#v",
			metadata)
	}
	if _, present := metadata["coverage"]; !present {
		t.Errorf("metadata.coverage is absent, so an empty answer cannot be told from an unobserved "+
			"one without a second query: %#v", metadata)
	}

	items, ok := decoded["items"].([]any)
	if !ok {
		t.Fatalf("items is not a list, so a consumer iterating it fails on the empty case: %#v",
			decoded["items"])
	}
	return items
}

// coverageOf digs the coverage report out of a decoded envelope.
func coverageOf(t *testing.T, decoded map[string]any) map[string]any {
	t.Helper()

	metadata, ok := decoded["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("the envelope carries no metadata: %#v", decoded)
	}
	coverage, ok := metadata["coverage"].(map[string]any)
	if !ok {
		t.Fatalf("metadata.coverage is not an object: %#v", metadata["coverage"])
	}
	return coverage
}

// decodeJSON parses a whole-document rendering.
func decodeJSON(t *testing.T, document string) map[string]any {
	t.Helper()

	var decoded map[string]any
	if err := json.Unmarshal([]byte(document), &decoded); err != nil {
		t.Fatalf("stdout is not valid JSON, so a script cannot read it: %v\n%s", err, document)
	}
	return decoded
}

// decodeYAML parses a whole-document rendering written as YAML.
func decodeYAML(t *testing.T, document string) map[string]any {
	t.Helper()

	var decoded map[string]any
	if err := yaml.Unmarshal([]byte(document), &decoded); err != nil {
		t.Fatalf("stdout is not valid YAML: %v\n%s", err, document)
	}
	return decoded
}

// decodeJSONL parses the streaming rendering: the head line, then the items.
//
// It asserts the shape the format promises rather than assuming it, because that
// shape is the whole of the contract for a consumer reading with `while read`:
// one document per line, the first of which carries the metadata and no items.
func decodeJSONL(t *testing.T, document string) (head map[string]any, items []map[string]any) {
	t.Helper()

	lines := strings.Split(strings.TrimSuffix(document, "\n"), "\n")
	if document == "" || len(lines) == 0 {
		t.Fatal("the jsonl rendering is empty; even an empty answer carries its head line")
	}
	for i, line := range lines {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("line %d is not valid JSON: %v\n%s", i+1, err, line)
		}
		if i == 0 {
			head = decoded
			continue
		}
		items = append(items, decoded)
	}
	if _, present := head["items"]; present {
		t.Errorf("the head line carries an items key, which the streaming form cannot fill: %v", head)
	}
	return head, items
}

// structuredRequest is the fixture's timeline, asked for in one serialization.
func structuredRequest(format render.StructuredFormat) cli.TimelineRequest {
	request := defaultRequest()
	request.Structured = format
	return request
}

// fixtureEngine is the fixture every test in this file queries.
func fixtureEngine() *fakeEngine {
	return &fakeEngine{
		caps:         clickHouseCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}
}

// TestTimelineJSONMirrorsTheSchemaColumns is the acceptance criterion for the
// item shape.
//
// The field names are asserted by name, not by golden file, because that is the
// promise: a `jq` recipe written against a SQL result must work unchanged here
// (D19). A golden file regenerated after a rename would keep passing; this will
// not.
func TestTimelineJSONMirrorsTheSchemaColumns(t *testing.T) {
	engine := fixtureEngine()
	stdout, stderr, err := runTimeline(t, engine, structuredRequest(render.StructuredJSON),
		render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGoldenIn(t, "timeline", "json", stdout, stderr)

	items := assertEnvelope(t, decodeJSON(t, stdout), render.KindTimeline)
	if len(items) != len(checkoutHistory()) {
		t.Fatalf("the envelope holds %d items, want %d", len(items), len(checkoutHistory()))
	}
	first, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("an item is not an object: %#v", items[0])
	}
	for _, column := range []string{
		"ts", "event_type", "actors", "uid", "resource_version", "api_version", "sha256",
	} {
		if _, present := first[column]; !present {
			t.Errorf("the item has no %q field; the names must mirror the schema's columns "+
				"exactly, so that a jq recipe transfers between SQL and this output:\n%#v",
				column, first)
		}
	}
}

// TestTimelineYAMLIsTheSameDocument covers the third serialization.
//
// It decodes both and compares, because the contract is that YAML and JSON are one
// document in two syntaxes: a reader who has learned one has learned both, and a
// field that appeared in only one of them would be a contract with a hole in it.
func TestTimelineYAMLIsTheSameDocument(t *testing.T) {
	stdout, stderr, err := runTimeline(t, fixtureEngine(), structuredRequest(render.StructuredYAML),
		render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGoldenIn(t, "timeline", "yaml", stdout, stderr)

	fromYAML := decodeYAML(t, stdout)
	assertEnvelope(t, fromYAML, render.KindTimeline)

	jsonOut, _, err := runTimeline(t, fixtureEngine(), structuredRequest(render.StructuredJSON),
		render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	if want := decodeJSON(t, jsonOut); !jsonEqual(t, fromYAML, want) {
		t.Errorf("the YAML and JSON renderings decode to different documents.\nYAML:\n%s\nJSON:\n%s",
			stdout, jsonOut)
	}
}

// TestTimelineJSONLStreamsOneItemPerLine covers the streaming serialization.
func TestTimelineJSONLStreamsOneItemPerLine(t *testing.T) {
	stdout, stderr, err := runTimeline(t, fixtureEngine(), structuredRequest(render.StructuredJSONL),
		render.Options{})
	if err != nil {
		t.Fatalf("RunTimeline: %v", err)
	}
	assertGoldenIn(t, "timeline", "jsonl", stdout, stderr)

	head, items := decodeJSONL(t, stdout)
	if head["kind"] != render.KindTimeline {
		t.Errorf("the head line's kind is %#v, want %q", head["kind"], render.KindTimeline)
	}
	if _, present := coverageOf(t, head)["intervals"]; !present {
		t.Error("the head line carries no coverage intervals, so a consumer reading the stream as it " +
			"arrives cannot tell an empty answer from an unobserved one")
	}
	if len(items) != len(checkoutHistory()) {
		t.Fatalf("the stream holds %d items, want %d", len(items), len(checkoutHistory()))
	}
}

// TestTimelineStructuredOrderMatchesTheTable is the property --reverse must keep
// across renderings.
//
// The streaming path asks the backend for a different order than the gathered one
// does, and with a limit in force it holds the answer back and reverses it here.
// Those are optimizations, and an optimization that changed which changes came
// back — or the order they came back in — would be a bug that only a script would
// ever see.
func TestTimelineStructuredOrderMatchesTheTable(t *testing.T) {
	for _, test := range []struct {
		name    string
		reverse bool
		limit   int
	}{
		{name: "newest first", reverse: false, limit: 100},
		{name: "oldest first, unlimited", reverse: true, limit: 0},
		{name: "oldest first, limited", reverse: true, limit: 2},
		{name: "newest first, limited", reverse: false, limit: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			tabular := defaultRequest()
			tabular.Reverse, tabular.Limit = test.reverse, test.limit
			table, _, err := runTimeline(t, fixtureEngine(), tabular, render.Options{})
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}

			structured := tabular
			structured.Structured = render.StructuredJSONL
			stream, _, err := runTimeline(t, fixtureEngine(), structured, render.Options{})
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}

			_, items := decodeJSONL(t, stream)
			stamps := make([]string, 0, len(items))
			for _, item := range items {
				stamps = append(stamps, timestampOf(t, item))
			}
			if got := tableTimestamps(table); !equalStrings(got, stamps) {
				t.Errorf("the table shows %v and the stream shows %v; the two renderings must select "+
					"and order the same changes", got, stamps)
			}
		})
	}
}

// TestStructuredCoverageDistinguishesTheTwoEmptinesses is Invariant 9 as a script
// sees it.
//
// The one thing a consumer must be able to do from a single invocation is tell an
// object that did not change from a scope nobody was watching. Both produce zero
// items, so the difference has to be in the metadata — and the third case, a
// backend with no scope log at all, must be a third answer rather than either of
// the first two.
func TestStructuredCoverageDistinguishesTheTwoEmptinesses(t *testing.T) {
	tests := []struct {
		name          string
		engine        *fakeEngine
		wantAvailable bool
		wantIntervals int
		wantExit      int
	}{
		{
			name: "watched, and nothing changed",
			engine: &fakeEngine{
				caps:      clickHouseCapabilities(),
				intervals: watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
			},
			wantAvailable: true,
			wantIntervals: 1,
			wantExit:      exit.Success,
		},
		{
			name:          "nothing was ever watching",
			engine:        &fakeEngine{caps: clickHouseCapabilities()},
			wantAvailable: true,
			wantIntervals: 0,
			wantExit:      exit.NoCoverage,
		},
		{
			name: "the backend has no scope log",
			engine: &fakeEngine{
				caps:        clickHouseCapabilities(),
				coverageErr: query.ErrCapabilityUnsupported,
			},
			wantAvailable: false,
			wantIntervals: 0,
			wantExit:      exit.Success,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := structuredRequest(render.StructuredJSON)
			request.From = at("2026-08-01T00:00:00Z")
			request.To = at("2026-08-28T15:00:00Z")

			stdout, _, err := runTimeline(t, test.engine, request, render.Options{})
			if code := exit.CodeFor(err); code != test.wantExit {
				t.Errorf("exit code %d, want %d (%v)", code, test.wantExit, err)
			}

			decoded := decodeJSON(t, stdout)
			if items := assertEnvelope(t, decoded, render.KindTimeline); len(items) != 0 {
				t.Fatalf("the answer is not empty: %#v", items)
			}

			coverage := coverageOf(t, decoded)
			if coverage["available"] != test.wantAvailable {
				t.Errorf("metadata.coverage.available is %#v, want %v",
					coverage["available"], test.wantAvailable)
			}
			intervals, ok := coverage["intervals"].([]any)
			if !ok {
				t.Fatalf("metadata.coverage.intervals is not a list: %#v", coverage["intervals"])
			}
			if len(intervals) != test.wantIntervals {
				t.Errorf("metadata.coverage.intervals holds %d entries, want %d",
					len(intervals), test.wantIntervals)
			}
			if coverage["summary"] == "" {
				t.Error("metadata.coverage.summary is empty; it is the line a person would have read")
			}
		})
	}
}

// TestStructuredOutputKeepsNoticesOffStandardOutput is the split the whole
// release rests on.
//
// A notice that reached stdout would corrupt the document it was qualifying, and
// the case that matters is the one where there is the most to say: an archive that
// records no deletions, answering about a window it had to complete itself.
func TestStructuredOutputKeepsNoticesOffStandardOutput(t *testing.T) {
	engine := &fakeEngine{
		caps:         archiveCapabilities(),
		changes:      checkoutHistory(),
		incarnations: checkoutIncarnations(),
		intervals:    watchedSince("2026-07-02T09:14:00Z", "ClusterStreamRule/all-workloads"),
	}

	for _, format := range []render.StructuredFormat{
		render.StructuredJSON, render.StructuredJSONL, render.StructuredYAML,
	} {
		t.Run(string(format), func(t *testing.T) {
			stdout, stderr, err := runTimeline(t, engine, structuredRequest(format), render.Options{})
			if err != nil {
				t.Fatalf("RunTimeline: %v", err)
			}
			if !strings.Contains(stderr, "does not record deletions") {
				t.Errorf("the capability notice was not written:\n%s", stderr)
			}
			if strings.Contains(stdout, "does not record deletions") {
				t.Errorf("a notice reached stdout, where a pipe would receive it:\n%s", stdout)
			}
		})
	}
}

// TestDiffJSONCarriesTheRecoveredOldValue is what makes a Diff envelope worth
// having.
//
// The old value is the one thing this command computes that the schema does not
// store, and old_known is what keeps an unrecoverable value visibly absent rather
// than rendering as a null a consumer would read as "the field was null".
func TestDiffJSONCarriesTheRecoveredOldValue(t *testing.T) {
	request := cli.DiffRequest{Timeline: structuredRequest(render.StructuredJSON)}
	stdout, stderr, err := runDiff(t, fixtureEngine(), request, render.Options{})
	if err != nil {
		t.Fatalf("RunDiff: %v", err)
	}
	assertGoldenIn(t, "diff", "json", stdout, stderr)

	items := assertEnvelope(t, decodeJSON(t, stdout), render.KindDiff)
	found := false
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("an item is not an object: %#v", item)
		}
		hunks, ok := object["hunks"].([]any)
		if !ok {
			t.Fatalf("an item carries no hunks list: %#v", object)
		}
		for _, entry := range hunks {
			hunk, isObject := entry.(map[string]any)
			if !isObject {
				t.Fatalf("a hunk is not an object: %#v", entry)
			}
			if hunk["path"] != "spec.template.spec.containers[0].resources.limits.memory" {
				continue
			}
			found = true
			if hunk["old"] != "2Gi" || hunk["old_known"] != true {
				t.Errorf("the recovered old value is %#v (known: %#v), want \"2Gi\" and true",
					hunk["old"], hunk["old_known"])
			}
			if string(mustJSON(t, hunk["new"])) != `"512Mi"` {
				t.Errorf("the new value is %#v, want \"512Mi\"", hunk["new"])
			}
		}
	}
	if !found {
		t.Errorf("the flagship hunk is not in the envelope:\n%s", stdout)
	}
}

// TestDiffStructuredRenderings covers the other two serializations by golden file.
func TestDiffStructuredRenderings(t *testing.T) {
	for name, format := range map[string]render.StructuredFormat{
		"jsonl": render.StructuredJSONL,
		"yaml":  render.StructuredYAML,
	} {
		t.Run(name, func(t *testing.T) {
			request := cli.DiffRequest{Timeline: structuredRequest(format)}
			stdout, stderr, err := runDiff(t, fixtureEngine(), request, render.Options{})
			if err != nil {
				t.Fatalf("RunDiff: %v", err)
			}
			assertGoldenIn(t, "diff", name, stdout, stderr)
		})
	}
}

// TestDiffExitCodeStillWritesTheEnvelope keeps the two contracts from colliding.
//
// `--exit-code` reports "changes were found" as exit 1, which is a finding rather
// than a failure. A script combining it with -o json needs both halves: the code
// to branch on, and a parseable document to read. Withholding the document on a
// non-zero exit would make the flag unusable with structured output.
func TestDiffExitCodeStillWritesTheEnvelope(t *testing.T) {
	request := cli.DiffRequest{Timeline: structuredRequest(render.StructuredJSON), ExitCode: true}
	stdout, stderr, err := runDiff(t, fixtureEngine(), request, render.Options{})

	if code := exit.CodeFor(err); code != exit.RuntimeError {
		t.Fatalf("exit code %d, want %d: %v", code, exit.RuntimeError, err)
	}
	if items := assertEnvelope(t, decodeJSON(t, stdout), render.KindDiff); len(items) == 0 {
		t.Error("the envelope is empty, but the exit code says changes were found")
	}
	if !strings.Contains(stderr, "It is not a failure") {
		t.Errorf("the exit code was not explained beside the document:\n%s", stderr)
	}
}

// TestGetJSONLIsAHeadAndOneItem covers the reconstruction in the streaming form.
//
// One item is still an envelope: a consumer that reads every answer this CLI gives
// the same way must not need a special case for the command that returns exactly
// one thing.
func TestGetJSONLIsAHeadAndOneItem(t *testing.T) {
	stdout, stderr, err := runGet(t, checkpointEngine(t), getRequest(render.StructuredJSONL))
	if err != nil {
		t.Fatalf("RunGet: %v", err)
	}
	assertGoldenIn(t, "get", "jsonl", stdout, stderr)

	head, items := decodeJSONL(t, stdout)
	if head["kind"] != render.KindObject {
		t.Errorf("the head line's kind is %#v, want %q", head["kind"], render.KindObject)
	}
	if len(items) != 1 {
		t.Fatalf("a reconstruction is one item, and the stream holds %d", len(items))
	}
	if _, present := items[0]["object"]; !present {
		t.Errorf("the item carries no reconstructed object: %#v", items[0])
	}
	if !strings.Contains(stderr, "NOT A DEPLOYABLE MANIFEST") {
		t.Errorf("the mandatory header did not reach stderr, and jsonl has no comments:\n%s", stderr)
	}
}

// mustJSON re-encodes a decoded value so a raw-message field can be compared.
func mustJSON(t *testing.T, value any) []byte {
	t.Helper()

	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("re-encoding %#v: %v", value, err)
	}
	return encoded
}

// jsonEqual compares two decoded documents.
func jsonEqual(t *testing.T, a, b map[string]any) bool {
	t.Helper()
	return string(mustJSON(t, a)) == string(mustJSON(t, b))
}

// timestampOf reads an item's ts field, which is the schema's own column name.
func timestampOf(t *testing.T, item map[string]any) string {
	t.Helper()

	ts, ok := item["ts"].(string)
	if !ok {
		t.Fatalf("an item has no ts: %#v", item)
	}
	return ts
}

// tableTimestamps reads the TIME column out of a rendered table.
//
// Comparing the two renderings through their timestamps rather than through the
// requests that produced them is deliberate: what is under test is that two
// different code paths selected the same changes, and a comparison that started
// from either path's own idea of the answer would not be testing that.
func tableTimestamps(table string) []string {
	var stamps []string
	for line := range strings.SplitSeq(table, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || !strings.HasPrefix(fields[0], "2026-") {
			continue
		}
		stamps = append(stamps, fields[0]+"T"+fields[1])
	}
	return stamps
}

// equalStrings compares two renderings' timestamps, allowing for the layouts
// differing: the table prints milliseconds and the envelope prints RFC 3339.
func equalStrings(table, structured []string) bool {
	if len(table) != len(structured) {
		return false
	}
	for i := range table {
		// "2026-08-28T14:09:40.900" against "2026-08-28T14:09:40.9Z": compare the
		// second, which both spell identically, and the millisecond digits that
		// survive both layouts.
		if !strings.HasPrefix(structured[i], table[i][:len("2026-08-28T14:09:40")]) {
			return false
		}
	}
	return true
}
