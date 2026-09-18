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

// The SUBJECT column: which object each row of a `--owned` page belongs to.
//
// A flat merge of three objects' Events with no attribution is less useful than
// three separate invocations, so the column is the half of the feature a reader
// actually acts on. Everything it shows comes out of the Event's own payload,
// because that is the only place a merged row records what it is about.

package render_test

import (
	"strings"
	"testing"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// subjectInstant is when every row below is dated, fixed so a rendered column has
// the same width in every case.
var subjectInstant = time.Date(2026, 8, 28, 14, 7, 3, 771000000, time.UTC)

// TestEventSubjectReadsBothAPISpellings pins the coalesce the correlation itself
// applies. Reading one spelling would leave every Event captured through the other
// API rendering as `unknown` — a silence produced by the renderer, which is no
// easier to notice than one produced by a query.
func TestEventSubjectReadsBothAPISpellings(t *testing.T) {
	for _, tc := range []struct{ name, data, want string }{
		{
			name: "v1 involvedObject",
			data: `{"reason":"FailedScheduling","involvedObject":{"kind":"Pod","name":"checkout-ldw5j"}}`,
			want: "Pod/checkout-ldw5j",
		},
		{
			name: "events.k8s.io regarding",
			data: `{"reason":"FailedCreate","regarding":{"kind":"ReplicaSet","name":"checkout-7d4f"}}`,
			want: "ReplicaSet/checkout-7d4f",
		},
		{
			name: "both, coalesced per field",
			data: `{"involvedObject":{"kind":"Pod"},"regarding":{"name":"checkout-ldw5j"}}`,
			want: "Pod/checkout-ldw5j",
		},
		{
			name: "a name and no kind",
			data: `{"involvedObject":{"name":"checkout-ldw5j"}}`,
			want: "checkout-ldw5j",
		},
		{
			name: "no subject at all",
			data: `{"reason":"BackOff"}`,
			want: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			detail, ok := render.ParseEvent(tc.data)
			if !ok {
				t.Fatalf("ParseEvent refused %q", tc.data)
			}
			if got := detail.Subject(); got != tc.want {
				t.Fatalf("the subject rendered as %q, want %q", got, tc.want)
			}
		})
	}
}

// TestSubjectColumnAppearsOnlyUnderOwned keeps every other command's table byte for
// byte what it was.
//
// The column is a widening of the flagship layout, and the CHANGE column is what
// pays for it — so it exists exactly when a row could be about something other than
// the object in the header, and never otherwise.
func TestSubjectColumnAppearsOnlyUnderOwned(t *testing.T) {
	doc := render.TimelineDocument{
		Kind: "apps/Deployment", Object: "payments/checkout", Cluster: "prod-eu-1",
		Subject: "Deployment/checkout",
		Rows: []render.TimelineRow{{Change: query.Change{
			TS: subjectInstant, EventType: query.EventKubernetes, Actors: []string{"kubelet"},
			Data: `{"type":"Warning","reason":"BackOff",` +
				`"involvedObject":{"kind":"Pod","name":"checkout-7d4f-ldw5j"}}`,
		}}},
	}

	plain := renderRows(t, doc, render.Options{Width: 120})
	if strings.Contains(plain, "SUBJECT") {
		t.Fatalf("an ordinary timeline grew a SUBJECT column:\n%s", plain)
	}

	owned := renderRows(t, doc, render.Options{Width: 120, Owned: true})
	if !strings.Contains(owned, "SUBJECT") {
		t.Fatalf("a --owned timeline has no SUBJECT column:\n%s", owned)
	}
	if !strings.Contains(owned, "Pod/checkout-7d4f-ldw5j") {
		t.Fatalf("the Event row is not attributed to its subject:\n%s", owned)
	}
}

// TestSubjectColumnAttributesAChangeToTheObjectInTheHeader is the other half of the
// column, and it is not a detail: `--owned` widens the Event correlation and never
// the state half, so a row that is not an Event is always the object the reader
// named.
func TestSubjectColumnAttributesAChangeToTheObjectInTheHeader(t *testing.T) {
	doc := render.TimelineDocument{
		Kind: "apps/Deployment", Object: "payments/checkout", Cluster: "prod-eu-1",
		Subject: "Deployment/checkout",
		Rows: []render.TimelineRow{{Change: query.Change{
			TS: subjectInstant, EventType: query.EventModified, Actors: []string{"kubectl"},
			Diff: `[{"op":"replace","path":"/spec/replicas","value":5}]`,
		}}},
	}

	rendered := renderRows(t, doc, render.Options{Width: 120, Owned: true})
	if !strings.Contains(rendered, "Deployment/checkout") {
		t.Fatalf("a change was not attributed to the object in the header:\n%s", rendered)
	}
}

// TestSubjectColumnAdmitsAnUnreadableSubject covers the branch that should be
// unreachable.
//
// An Event reaches this table by matching a subject, so a payload naming none is one
// this renderer could not read — and attributing it to the object in the header
// would be inventing exactly the attribution the column exists to report.
func TestSubjectColumnAdmitsAnUnreadableSubject(t *testing.T) {
	doc := render.TimelineDocument{
		Kind: "apps/Deployment", Object: "payments/checkout", Cluster: "prod-eu-1",
		Subject: "Deployment/checkout",
		Rows: []render.TimelineRow{{Change: query.Change{
			TS: subjectInstant, EventType: query.EventKubernetes,
			Data: `{"type":"Warning","reason":"BackOff"}`,
		}}},
	}

	rendered := renderRows(t, doc, render.Options{Width: 120, Owned: true})
	if !strings.Contains(rendered, render.UnknownSubject) {
		t.Fatalf("an Event naming no subject was attributed to something:\n%s", rendered)
	}
}
