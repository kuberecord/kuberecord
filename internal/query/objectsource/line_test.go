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

package objectsource

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// largeConfigMap renders a ConfigMap whose data map is comfortably larger than
// bufio.MaxScanTokenSize once it is encoded, escaped into a record line and read back.
//
// It is a map of many keys rather than one padded string on purpose. A single enormous
// value would exercise byte counting; a real object of this size is a few hundred
// entries of ordinary length — a certificate bundle, a rendered configuration tree, a
// custom resource with x-kubernetes-preserve-unknown-fields — and that is what has to
// come back through the decoder unchanged.
func largeConfigMap(t *testing.T, entries int) string {
	t.Helper()

	data := make(map[string]string, entries)
	for i := range entries {
		data[fmt.Sprintf("service-%03d.conf", i)] = fmt.Sprintf(
			"upstream backend-%03d {\n  server 10.%d.%d.%d:8443 max_fails=3;\n  keepalive 32;\n}\n"+
				"# rendered by the platform team, revision %d\n%s",
			i, i%256, (i*7)%256, (i*13)%256, 4000+i,
			strings.Repeat(fmt.Sprintf("  proxy_set_header X-Route-%03d $host;\n", i), 4))
	}

	object := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": "checkout", "namespace": "payments"},
		"data":       data,
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		t.Fatalf("encoding the oversized fixture object: %v", err)
	}
	return string(encoded)
}

// TestAnOversizedRecordSurvivesTheDecoder: one record larger than 64 KiB comes back
// whole.
//
// **This test is the guard against a future switch from json.Decoder to bufio.Scanner
// in streamFrame.** If it has just started failing, that is what has been reintroduced,
// and the fix is not to raise a buffer limit until it passes.
//
// The format is JSON Lines, so a line scanner reads like the obvious tool. It is not:
// bufio.Scanner refuses any token longer than bufio.MaxScanTokenSize — 64 KiB by
// default — and a record here is routinely larger. A ConfigMap with a real data map, or
// a custom resource carrying preserved unknown fields, is past it well before it
// approaches the megabyte an etcd value may hold, and escaping the state into the
// line's data field only makes it longer.
//
// Of the two ways it then fails, the quiet one is the reason this fixture exists. Scan
// returns false and Err reports bufio.ErrTooLong, which is survivable because it is
// loud. But where the error goes unchecked, the line is truncated at the buffer
// boundary and what comes back is a *partial object* — one that JSON may well accept,
// that decodes into a change, and that nothing in the output admits is incomplete
// (Invariant 4). Every other fixture in this package would pass either way, because
// none of them holds a record anywhere near this size.
func TestAnOversizedRecordSurvivesTheDecoder(t *testing.T) {
	t.Parallel()

	data := largeConfigMap(t, 400)
	if len(data) <= bufio.MaxScanTokenSize {
		t.Fatalf("the fixture record's state is %d bytes, which is not past bufio.MaxScanTokenSize "+
			"(%d); a fixture that fits inside the limit proves nothing about the decoder that has no "+
			"limit", len(data), bufio.MaxScanTokenSize)
	}

	ref := query.ObjectRef{
		ClusterID: testRef().ClusterID, APIGroup: "", Kind: "ConfigMap",
		Namespace: "payments", Name: "checkout",
	}
	history := conformance.History{Rows: []conformance.Row{{
		Ref: ref,
		Change: query.Change{
			TS:              testEpoch(),
			EventType:       query.EventAdded,
			UID:             uidNew,
			ResourceVersion: "9001",
			APIVersion:      "v1",
			Actors:          []string{actorKubectl},
			Data:            data,
			SHA256:          fmt.Sprintf("%064d", 1),
		},
	}}}

	engine, _ := engineOver(t, history, Options{Prefix: "audit"})
	changes := drain(t, engine, query.TimelineQuery{
		Ref:  ref,
		From: testEpoch().Add(-time.Minute),
		To:   testEpoch().Add(time.Minute),
	})

	if len(changes) != 1 {
		t.Fatalf("a scan over an archive holding one oversized record returned %d changes, want 1",
			len(changes))
	}
	if got := changes[0].Data; got != data {
		t.Fatalf("the record came back as %d bytes, want the %d that were written. A decoder with a "+
			"per-value ceiling either refuses this line or truncates it into a partial object that "+
			"decodes to something plausible and wrong — and this fixture is the only thing in the "+
			"package that would notice.\nfirst divergence at byte %d",
			len(got), len(data), firstDivergence(got, data))
	}
}

// firstDivergence reports where two strings stop agreeing, so a truncation says where it
// happened rather than dumping 64 KiB into a failure message.
func firstDivergence(got, want string) int {
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			return i
		}
	}
	return min(len(got), len(want))
}
