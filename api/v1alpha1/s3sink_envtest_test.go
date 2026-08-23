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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// validS3Sink returns an S3Sink the apiserver must accept: a bucket and nothing
// else. Every rejection case below starts from this and breaks exactly one
// thing, so a failure names the rule that fired rather than "something in this
// object is wrong".
//
// That the minimal object is *this* minimal is itself the assertion the Phase 6
// criteria ask for: a bucket name alone is a working spec, credentials included
// (omitted means the ambient chain).
func validS3Sink() *S3Sink {
	return &S3Sink{
		ObjectMeta: objectMeta(""),
		Spec:       S3SinkSpec{Bucket: "kuberecord-audit"},
	}
}

// s3SinkWith applies edit to a valid sink, so a case reads as the one thing it
// changes.
func s3SinkWith(edit func(*S3Sink)) *S3Sink {
	s := validS3Sink()
	edit(s)
	return s
}

// unstructuredS3Sink builds an S3Sink from a raw spec map.
//
// It exists for the three cases the typed client cannot express, all of which a
// YAML author can write by hand: a string field whose empty value is
// indistinguishable from unset once `omitempty` has run (`prefix: ""`), a
// metav1.Duration that is not a duration at all, and a required field genuinely
// absent rather than submitted empty.
func unstructuredS3Sink(spec map[string]any) clientObject {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(GroupVersion.WithKind("S3Sink"))
	if err := unstructured.SetNestedMap(u.Object, spec, "spec"); err != nil {
		panic("building unstructured S3Sink: " + err.Error())
	}
	return u
}

func TestS3SinkValidation(t *testing.T) {
	runAPICases(t, append(s3SinkShapeCases(), s3SinkPolicyCases()...))
}

// s3SinkShapeCases covers everything specific to this CRD: the bucket, the key
// prefix, the endpoint, the credential union, the format enum, rotation, object
// lock and the writer.
func s3SinkShapeCases() []apiCase {
	return []apiCase{
		{
			// The Phase 6 criterion "accepts a credentials-omitted spec" and
			// "accepts a minimal spec" are the same object: ambient credentials
			// are spelled by leaving the block out.
			name: "minimal-sink-with-no-credentials-is-accepted",
			obj:  validS3Sink(),
		},
		{
			name:    "empty-bucket-is-rejected",
			obj:     s3SinkWith(func(s *S3Sink) { s.Spec.Bucket = "" }),
			wantErr: "should be at least 1 chars long",
		},
		{
			name: "overlong-bucket-is-rejected",
			obj: s3SinkWith(func(s *S3Sink) {
				s.Spec.Bucket = "b012345678901234567890123456789012345678901234567890123456789012"
			}),
			wantErr: "Too long: may not be more than 63 bytes",
		},

		// The key prefix. Each rejection below produces a `//` or an unescaped
		// character in *every* object the sink would ever write, and the object
		// key layout is a public contract (D15) — so they are caught here rather
		// than normalised away in the key builder, where the CR would keep saying
		// something the bucket does not.
		{
			name:    "prefix-with-a-leading-slash-is-rejected",
			obj:     s3SinkWith(func(s *S3Sink) { s.Spec.Prefix = "/clusters/prod" }),
			wantErr: "should match",
		},
		{
			name:    "prefix-with-a-trailing-slash-is-rejected",
			obj:     s3SinkWith(func(s *S3Sink) { s.Spec.Prefix = "clusters/prod/" }),
			wantErr: "should match",
		},
		{
			name:    "prefix-with-an-empty-segment-is-rejected",
			obj:     s3SinkWith(func(s *S3Sink) { s.Spec.Prefix = "clusters//prod" }),
			wantErr: "should match",
		},
		{
			name:    "prefix-with-a-space-is-rejected",
			obj:     s3SinkWith(func(s *S3Sink) { s.Spec.Prefix = "audit trail" }),
			wantErr: "should match",
		},
		{
			name: "nested-prefix-is-accepted",
			obj:  s3SinkWith(func(s *S3Sink) { s.Spec.Prefix = "clusters/prod/kube_audit-1.0" }),
		},
		{
			// An explicitly empty prefix is what a templated manifest renders when
			// its prefix value is unset, and it means the same as omitting the
			// field. Only an unstructured write can reach it: `omitempty` drops it
			// from a typed object, so without this case the pattern's optional
			// outer group would be untested.
			name: "explicitly-empty-prefix-is-accepted",
			obj: unstructuredS3Sink(map[string]any{
				"bucket": "kuberecord-audit", "prefix": "",
			}),
		},

		// The endpoint. A bare host:port is the mistake worth catching at
		// admission: the CR would take it and the client constructor would not.
		{
			name:    "endpoint-without-a-scheme-is-rejected",
			obj:     s3SinkWith(func(s *S3Sink) { s.Spec.Endpoint = "minio.kuberecord-system.svc:9000" }),
			wantErr: "should match",
		},
		{
			name: "http-endpoint-is-accepted",
			obj: s3SinkWith(func(s *S3Sink) {
				s.Spec.Endpoint = "http://minio.kuberecord-system.svc:9000"
			}),
		},
		{
			name: "https-endpoint-with-a-path-is-accepted",
			obj:  s3SinkWith(func(s *S3Sink) { s.Spec.Endpoint = "https://s3.example.com/gateway" }),
		},
		{
			name:    "uppercase-region-is-rejected",
			obj:     s3SinkWith(func(s *S3Sink) { s.Spec.Region = "US-East-1" }),
			wantErr: "should match",
		},

		// The credential union: omitted (ambient) or naming a Secret, never the
		// half-written third state.
		{
			name: "credentials-naming-a-secret-are-accepted",
			obj: s3SinkWith(func(s *S3Sink) {
				s.Spec.Credentials = &S3CredentialsSpec{
					SecretRef: &SecretReference{Name: "kuberecord-s3-credentials"},
				}
			}),
		},
		{
			// The message, not merely the rejection, is the point: an author who
			// meant "no credentials" has to be told to remove the block.
			name:    "empty-credentials-block-is-rejected",
			obj:     s3SinkWith(func(s *S3Sink) { s.Spec.Credentials = &S3CredentialsSpec{} }),
			wantErr: "omit spec.credentials entirely",
		},
		{
			name: "credentials-secret-without-a-name-is-rejected",
			obj: s3SinkWith(func(s *S3Sink) {
				s.Spec.Credentials = &S3CredentialsSpec{SecretRef: &SecretReference{}}
			}),
			wantErr: "should be at least 1 chars long",
		},

		{
			name:    "unknown-format-is-rejected",
			obj:     s3SinkWith(func(s *S3Sink) { s.Spec.Format = "parquet" }),
			wantErr: "Unsupported value",
		},
		{
			name: "the-one-format-spelled-out-is-accepted",
			obj:  s3SinkWith(func(s *S3Sink) { s.Spec.Format = S3ObjectFormatJSONLV1Zstd }),
		},

		// Rotation. maxObjectBytes carries plain Minimum/Maximum; maxObjectAge is
		// a string in the schema, so its range is the CEL rule and its own message
		// is what proves the rule (rather than a parse error) rejected the value.
		{
			name:    "maxobjectbytes-below-range-is-rejected",
			obj:     s3SinkWithRotation(S3RotationSpec{MaxObjectBytes: ptrTo(int64(1048575))}),
			wantErr: "spec.rotation.maxObjectBytes",
		},
		{
			name:    "maxobjectbytes-above-range-is-rejected",
			obj:     s3SinkWithRotation(S3RotationSpec{MaxObjectBytes: ptrTo(int64(1073741825))}),
			wantErr: "spec.rotation.maxObjectBytes",
		},
		{
			name: "maxobjectbytes-at-the-bounds-is-accepted",
			obj:  s3SinkWithRotation(S3RotationSpec{MaxObjectBytes: ptrTo(int64(1048576))}),
		},
		{
			name: "maxobjectbytes-at-the-ceiling-is-accepted",
			obj:  s3SinkWithRotation(S3RotationSpec{MaxObjectBytes: ptrTo(int64(1073741824))}),
		},
		{
			name:    "maxobjectage-below-range-is-rejected",
			obj:     s3SinkWithRotation(S3RotationSpec{MaxObjectAge: durationPtr("9s")}),
			wantErr: "maxObjectAge must be a duration between 10s and 1h",
		},
		{
			name:    "maxobjectage-above-range-is-rejected",
			obj:     s3SinkWithRotation(S3RotationSpec{MaxObjectAge: durationPtr("2h")}),
			wantErr: "maxObjectAge must be a duration between 10s and 1h",
		},
		{
			name: "maxobjectage-at-the-floor-is-accepted",
			obj:  s3SinkWithRotation(S3RotationSpec{MaxObjectAge: durationPtr("10s")}),
		},
		{
			name: "maxobjectage-at-the-ceiling-is-accepted",
			obj:  s3SinkWithRotation(S3RotationSpec{MaxObjectAge: durationPtr("1h")}),
		},
		{
			// The reason the CEL rule leads with a `matches` and not with
			// `duration(self)`: an unparseable value must be rejected by the rule,
			// with the rule's own message, rather than blow up inside it. A typed
			// client cannot submit this — only a person editing YAML can.
			name: "maxobjectage-that-is-not-a-duration-is-rejected-by-the-rule",
			obj: unstructuredS3Sink(map[string]any{
				"bucket":   "kuberecord-audit",
				"rotation": map[string]any{"maxObjectAge": "banana"},
			}),
			wantErr: "maxObjectAge must be a duration between 10s and 1h",
		},

		// Object Lock. COMPLIANCE retention cannot be lifted by anyone once
		// applied, so admission is the last place a wrong value is correctable.
		{
			name: "object-lock-is-accepted",
			obj: s3SinkWith(func(s *S3Sink) {
				s.Spec.ObjectLock = &S3ObjectLockSpec{Mode: ObjectLockModeCompliance, RetainDays: 365}
			}),
		},
		{
			name: "unknown-object-lock-mode-is-rejected",
			obj: s3SinkWith(func(s *S3Sink) {
				s.Spec.ObjectLock = &S3ObjectLockSpec{Mode: "ADVISORY", RetainDays: 1}
			}),
			wantErr: "Unsupported value",
		},
		{
			name: "object-lock-retaining-nothing-is-rejected",
			obj: s3SinkWith(func(s *S3Sink) {
				s.Spec.ObjectLock = &S3ObjectLockSpec{Mode: ObjectLockModeGovernance, RetainDays: 0}
			}),
			wantErr: "should be greater than or equal to 1",
		},
		{
			name: "object-lock-with-a-retention-but-no-mode-is-rejected",
			obj: unstructuredS3Sink(map[string]any{
				"bucket":     "kuberecord-audit",
				"objectLock": map[string]any{"retainDays": int64(30)},
			}),
			wantErr: "spec.objectLock.mode: Required value",
		},

		// The writer. The four knobs are the shared ones, and they are bounded
		// exactly as ClickHouse's are — see TestSharedWriterKnobsAgreeAcrossSinks
		// for the assertion that the two schemas actually agree.
		{
			name:    "writer-workers-above-range-is-rejected",
			obj:     s3SinkWithWriter(S3WriterSpec{Workers: ptrTo(int32(65))}),
			wantErr: "should be less than or equal to 64",
		},
		{
			name:    "writer-queuesize-below-range-is-rejected",
			obj:     s3SinkWithWriter(S3WriterSpec{QueueSize: ptrTo(int32(0))}),
			wantErr: "should be greater than or equal to 1",
		},
		{
			name: "writer-at-the-bounds-is-accepted",
			obj: s3SinkWithWriter(S3WriterSpec{
				Workers: ptrTo(int32(64)), QueueSize: ptrTo(int32(1000000)),
			}),
		},
	}
}

// s3SinkPolicyCases re-runs the sink-policy expectations against this CRD.
//
// They are not redundant with the ClickHouseSink table: SinkPolicy is one type
// used by two CRDs, and the property under test is that a rule written into that
// shared type reaches both schemas. Redaction is a per-sink floor, so a backend
// whose policy validated more loosely would make choosing that backend a way
// around the floor — which is precisely the temptation an archive tier creates.
func s3SinkPolicyCases() []apiCase {
	withPolicy := func(p SinkPolicy) *S3Sink {
		return s3SinkWith(func(s *S3Sink) { s.Spec.Policy = p })
	}
	redacting := func(rule RedactionRule) *S3Sink {
		return withPolicy(SinkPolicy{Redaction: []RedactionRule{rule}})
	}
	return []apiCase{
		{
			name: "s3-policy-wildcard-kinds-are-accepted",
			obj: withPolicy(SinkPolicy{AllowedGVKs: []GVKSelector{
				{Group: "apps", Version: "v1", Kinds: []string{"*"}},
			}}),
		},
		{
			name: "s3-policy-duplicate-kinds-are-rejected",
			obj: withPolicy(SinkPolicy{AllowedGVKs: []GVKSelector{
				{Group: "apps", Version: "v1", Kinds: []string{"Deployment", "Deployment"}},
			}}),
			wantErr: "Duplicate value",
		},
		{
			name: "s3-policy-lowercase-kind-is-rejected",
			obj: withPolicy(SinkPolicy{AllowedGVKs: []GVKSelector{
				{Group: "apps", Version: "v1", Kinds: []string{"deployments"}},
			}}),
			wantErr: "should match",
		},
		{
			name: "s3-redaction-field-path-is-accepted",
			obj:  redacting(RedactionRule{FieldPath: "data.password"}),
		},
		{
			name: "s3-redaction-annotation-is-accepted",
			obj:  redacting(RedactionRule{Annotation: "my.company.io/api-token"}),
		},
		{
			name:    "s3-redaction-with-both-fields-is-rejected",
			obj:     redacting(RedactionRule{FieldPath: "data.password", Annotation: "token"}),
			wantErr: "exactly one of fieldPath or annotation must be set",
		},
		{
			name:    "s3-redaction-with-neither-field-is-rejected",
			obj:     redacting(RedactionRule{}),
			wantErr: "exactly one of fieldPath or annotation must be set",
		},
		{
			name:    "s3-redaction-indexed-path-is-rejected",
			obj:     redacting(RedactionRule{FieldPath: "spec.containers[0].name"}),
			wantErr: "should match",
		},
	}
}

// s3SinkWithRotation and s3SinkWithWriter return a valid sink with one block
// replaced.
func s3SinkWithRotation(r S3RotationSpec) *S3Sink {
	return s3SinkWith(func(s *S3Sink) { s.Spec.Rotation = r })
}

func s3SinkWithWriter(w S3WriterSpec) *S3Sink {
	return s3SinkWith(func(s *S3Sink) { s.Spec.Writer = w })
}

// durationPtr parses a duration literal for a table entry, panicking on a typo:
// a malformed literal here is a broken test, not a test of malformed input (that
// case goes through unstructuredS3Sink, which is the only way to submit one).
func durationPtr(s string) *metav1.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic("bad duration literal in the S3Sink table: " + s)
	}
	return &metav1.Duration{Duration: d}
}

// TestS3SinkDefaults pins what a bucket-name-only spec becomes.
//
// It is asserted through a real apiserver round-trip rather than by reading the
// CRD YAML because CRD defaulting is what an operator actually experiences: write
// one field, get these values. The four writer numbers are deliberately the same
// ones a ClickHouseSink defaults to — an author who has tuned one sink has tuned
// both — and TestSharedWriterKnobsAgreeAcrossSinks is what keeps that true.
func TestS3SinkDefaults(t *testing.T) {
	ctx := context.Background()
	sink := validS3Sink()
	sink.SetName("defaulting-s3-sink")
	if err := k8sClient.Create(ctx, sink); err != nil {
		t.Fatalf("creating sink: %v", err)
	}
	defer deleteObject(ctx, t, sink)

	got := &S3Sink{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: sink.Name}, got); err != nil {
		t.Fatalf("reading sink back: %v", err)
	}

	tests := []struct {
		field string
		got   string
		want  string
	}{
		{field: "spec.region", got: got.Spec.Region, want: "us-east-1"},
		{field: "spec.format", got: string(got.Spec.Format), want: "jsonl-v1-zstd"},
		{field: "spec.forcePathStyle", got: boolString(got.Spec.ForcePathStyle), want: "false"},
		{field: "spec.rotation.maxObjectBytes", got: int64String(got.Spec.Rotation.MaxObjectBytes), want: "67108864"},
		{field: "spec.rotation.maxObjectAge", got: durationString(got.Spec.Rotation.MaxObjectAge), want: "5m0s"},
		{field: "spec.writer.queueSize", got: int32String(got.Spec.Writer.QueueSize), want: "5000"},
		{field: "spec.writer.workers", got: int32String(got.Spec.Writer.Workers), want: "4"},
		{field: "spec.writer.enqueueTimeout", got: durationString(got.Spec.Writer.EnqueueTimeout), want: "2s"},
		{field: "spec.writer.drainTimeout", got: durationString(got.Spec.Writer.DrainTimeout), want: "15s"},
	}
	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s defaulted to %q, want %q", tt.field, tt.got, tt.want)
			}
		})
	}

	// Credentials stay absent. A default here would silently turn every ambient
	// sink into one demanding a Secret that does not exist.
	if got.Spec.Credentials != nil {
		t.Errorf("spec.credentials materialised as %+v; an omitted block must stay omitted", got.Spec.Credentials)
	}
	// So does object lock: retention nobody asked for is not a safe default in
	// either direction — GOVERNANCE would be theatre, COMPLIANCE irreversible.
	if got.Spec.ObjectLock != nil {
		t.Errorf("spec.objectLock materialised as %+v; an omitted block must stay omitted", got.Spec.ObjectLock)
	}
}
