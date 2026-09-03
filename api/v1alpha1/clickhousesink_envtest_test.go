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

package v1alpha1

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

// validSink returns a ClickHouseSink the apiserver must accept. Every rejection
// case below starts from this and breaks exactly one thing, so a failure names
// the rule that fired rather than "something in this object is wrong".
func validSink() *ClickHouseSink {
	return &ClickHouseSink{
		ObjectMeta: objectMeta(""),
		Spec: ClickHouseSinkSpec{
			Connection: ConnectionSpec{
				Addr:                 "clickhouse.kuberecord-system.svc:9000",
				CredentialsSecretRef: SecretReference{Name: "kuberecord-clickhouse"},
			},
		},
	}
}

// sinkWithWriter returns a valid sink whose writer block is replaced.
func sinkWithWriter(w WriterSpec) *ClickHouseSink {
	s := validSink()
	s.Spec.Writer = w
	return s
}

// sinkWithPolicy returns a valid sink whose policy admits exactly sel.
func sinkWithPolicy(sel GVKSelector) *ClickHouseSink {
	s := validSink()
	s.Spec.Policy = SinkPolicy{AllowedGVKs: []GVKSelector{sel}}
	return s
}

func TestClickHouseSinkValidation(t *testing.T) {
	runAPICases(t, []apiCase{
		{
			name: "minimal-valid-sink-is-accepted",
			obj:  validSink(),
		},
		{
			name: "empty-addr-is-rejected",
			obj: func() *ClickHouseSink {
				s := validSink()
				s.Spec.Connection.Addr = ""
				return s
			}(),
			wantErr: "should be at least 1 chars long",
		},
		{
			name: "missing-credentials-secret-name-is-rejected",
			obj: func() *ClickHouseSink {
				s := validSink()
				s.Spec.Connection.CredentialsSecretRef.Name = ""
				return s
			}(),
			wantErr: "should be at least 1 chars long",
		},
		{
			name:    "batchmaxrows-above-range-is-rejected",
			obj:     sinkWithWriter(WriterSpec{BatchMaxRows: ptrTo(int32(100001))}),
			wantErr: "should be less than or equal to 100000",
		},
		{
			name:    "batchmaxrows-below-range-is-rejected",
			obj:     sinkWithWriter(WriterSpec{BatchMaxRows: ptrTo(int32(0))}),
			wantErr: "should be greater than or equal to 1",
		},
		{
			name: "batchmaxrows-at-upper-bound-is-accepted",
			obj:  sinkWithWriter(WriterSpec{BatchMaxRows: ptrTo(int32(100000))}),
		},
		{
			// The ceiling is an honesty bound (see CheckpointEvery): a cadence
			// beyond it disables bounded replay while looking enabled.
			name:    "checkpointevery-above-range-is-rejected",
			obj:     sinkWithWriter(WriterSpec{CheckpointEvery: ptrTo(int32(10001))}),
			wantErr: "should be less than or equal to 10000",
		},
		{
			name:    "checkpointevery-below-range-is-rejected",
			obj:     sinkWithWriter(WriterSpec{CheckpointEvery: ptrTo(int32(-1))}),
			wantErr: "should be greater than or equal to 0",
		},
		{
			// Zero is *not* out of range: it is the documented way to turn
			// checkpointing off for a sink, unlike every other writer knob whose
			// floor is 1.
			name: "checkpointevery-zero-is-accepted-as-the-off-switch",
			obj:  sinkWithWriter(WriterSpec{CheckpointEvery: ptrTo(int32(0))}),
		},
		{
			name: "checkpointevery-at-upper-bound-is-accepted",
			obj:  sinkWithWriter(WriterSpec{CheckpointEvery: ptrTo(int32(10000))}),
		},
		{
			name:    "workers-above-range-is-rejected",
			obj:     sinkWithWriter(WriterSpec{Workers: ptrTo(int32(65))}),
			wantErr: "should be less than or equal to 64",
		},
		{
			name:    "workers-below-range-is-rejected",
			obj:     sinkWithWriter(WriterSpec{Workers: ptrTo(int32(0))}),
			wantErr: "should be greater than or equal to 1",
		},
		{
			name: "wildcard-kinds-are-accepted",
			obj:  sinkWithPolicy(GVKSelector{Group: "apps", Version: "v1", Kinds: []string{"*"}}),
		},
		{
			name:    "duplicate-kinds-are-rejected",
			obj:     sinkWithPolicy(GVKSelector{Group: "apps", Version: "v1", Kinds: []string{"Deployment", "Deployment"}}),
			wantErr: "Duplicate value",
		},
		{
			name:    "lowercase-kind-in-policy-is-rejected",
			obj:     sinkWithPolicy(GVKSelector{Group: "apps", Version: "v1", Kinds: []string{"deployments"}}),
			wantErr: "should match",
		},
		{
			name:    "bad-version-in-policy-is-rejected",
			obj:     sinkWithPolicy(GVKSelector{Group: "apps", Version: "1.0", Kinds: []string{"Deployment"}}),
			wantErr: "should match",
		},
		{
			name:    "empty-kinds-list-is-rejected",
			obj:     sinkWithPolicy(GVKSelector{Group: "apps", Version: "v1", Kinds: []string{}}),
			wantErr: "should have at least 1 items",
		},
		// Redaction. The syntax cases are the same ones the rule CRD
		// runs (see ruleValidationCases) — the two fields are the same type, and
		// a sink's floor is exactly as unforgiving as a rule's addition.
		{
			name:    "sink-redaction-field-path-is-accepted",
			obj:     sinkWithRedaction(RedactionRule{FieldPath: "data.password"}),
			wantErr: "",
		},
		{
			name: "sink-redaction-annotation-is-accepted",
			obj:  sinkWithRedaction(RedactionRule{Annotation: "my.company.io/api-token"}),
		},
		{
			name:    "sink-redaction-with-both-fields-is-rejected",
			obj:     sinkWithRedaction(RedactionRule{FieldPath: "data.password", Annotation: "token"}),
			wantErr: "exactly one of fieldPath or annotation must be set",
		},
		{
			name:    "sink-redaction-with-neither-field-is-rejected",
			obj:     sinkWithRedaction(RedactionRule{}),
			wantErr: "exactly one of fieldPath or annotation must be set",
		},
		{
			name:    "sink-redaction-indexed-path-is-rejected",
			obj:     sinkWithRedaction(RedactionRule{FieldPath: "spec.containers[0].name"}),
			wantErr: "should match",
		},
	})
}

// sinkWithRedaction returns a valid sink whose policy scrubs exactly rule.
func sinkWithRedaction(rule RedactionRule) *ClickHouseSink {
	s := validSink()
	s.Spec.Policy = SinkPolicy{Redaction: []RedactionRule{rule}}
	return s
}

// TestClickHouseSinkDefaults pins the writer/connection defaults to the shipped
// clickhouse.Default* values. They are asserted through a real apiserver
// round-trip rather than by reading the CRD YAML because CRD defaulting is what
// an operator actually experiences: omit the block, get these numbers.
func TestClickHouseSinkDefaults(t *testing.T) {
	ctx := context.Background()
	sink := validSink()
	sink.SetName("defaulting-sink")
	if err := k8sClient.Create(ctx, sink); err != nil {
		t.Fatalf("creating sink: %v", err)
	}
	defer deleteObject(ctx, t, sink)

	got := &ClickHouseSink{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: sink.Name}, got); err != nil {
		t.Fatalf("reading sink back: %v", err)
	}

	tests := []struct {
		field string
		got   string
		want  string
	}{
		{field: "spec.connection.database", got: got.Spec.Connection.Database, want: "kuberecord"},
		{field: "spec.connection.username", got: got.Spec.Connection.Username, want: "default"},
		{field: "spec.connection.dialTimeout", got: durationString(got.Spec.Connection.DialTimeout), want: "5s"},
		{field: "spec.connection.readTimeout", got: durationString(got.Spec.Connection.ReadTimeout), want: "10s"},
		{field: "spec.writer.queueSize", got: int32String(got.Spec.Writer.QueueSize), want: "5000"},
		{field: "spec.writer.workers", got: int32String(got.Spec.Writer.Workers), want: "4"},
		{field: "spec.writer.batchMaxRows", got: int32String(got.Spec.Writer.BatchMaxRows), want: "1000"},
		{field: "spec.writer.batchMaxWait", got: durationString(got.Spec.Writer.BatchMaxWait), want: "1s"},
		{field: "spec.writer.enqueueTimeout", got: durationString(got.Spec.Writer.EnqueueTimeout), want: "2s"},
		{field: "spec.writer.drainTimeout", got: durationString(got.Spec.Writer.DrainTimeout), want: "15s"},
		{field: "spec.writer.checkpointEvery", got: int32String(got.Spec.Writer.CheckpointEvery), want: "50"},
	}
	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s defaulted to %q, want %q", tt.field, tt.got, tt.want)
			}
		})
	}
}
