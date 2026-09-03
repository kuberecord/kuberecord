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
	"slices"
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
	cases := slices.Concat(s3SinkShapeCases(), s3SinkWorkerMemoryCases(), s3SinkPolicyCases())
	runAPICases(t, cases)
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
		// key layout is a public contract — so they are caught here rather
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
			// 64 workers is legal *at the default rotation size*: 64 × 64Mi is
			// exactly S3WriterMemoryBudgetBytes, so this case sits on the cross-field
			// rule's boundary rather than comfortably inside it. Lower the budget and
			// this stops being a writer-bounds case and starts being a memory case —
			// which is the whole reason s3SinkWorkerMemoryCases spells the boundary
			// out separately.
			name: "writer-at-the-bounds-is-accepted",
			obj: s3SinkWithWriter(S3WriterSpec{
				Workers: ptrTo(int32(64)), QueueSize: ptrTo(int32(1000000)),
			}),
		},
	}
}

// The figures the worker-memory cases are built from. They are spelled as
// expressions rather than as decimal literals so a reader can check the
// arithmetic against the budget without a calculator.
const (
	oneMi       = int64(1) << 20
	oneGi       = int64(1) << 30
	halfGi      = oneGi / 2
	defaultMi   = 64 * oneMi
	budgetOver  = halfGi + oneMi // 8 × this is just over the budget
	budgetUnder = halfGi - oneMi // 8 × this is just under it
)

// s3SinkWorkerMemoryCases covers the cross-field rule on spec: workers ×
// maxObjectBytes may not exceed S3WriterMemoryBudgetBytes.
//
// It is the one bound on this CRD that neither field can carry alone. Both are
// individually reasonable — 1Gi is a sane largest object, 64 a sane worker count —
// and the product of the two ceilings is 64Gi, because maxObjectBytes is measured
// on the encoded payload and each worker accumulates an object of its own. An
// author who reads `workers` as the throughput knob it is on ClickHouseSink gets a
// 64× memory multiplier instead, and finds out by watching the operator get
// OOM-killed.
//
// The table asserts both directions deliberately. Rejecting the ceiling pairing
// proves nothing on its own — a rule that rejected everything would pass that case
// — so every shape that genuinely helps throughput is asserted to still be
// admitted, including the two that sit exactly on the boundary.
func s3SinkWorkerMemoryCases() []apiCase {
	// The rejection message is one static string for every violation: a
	// messageExpression naming the offending numbers is refused at CRD
	// installation time on cost grounds (see S3SinkSpec's rule comment), so the
	// text names the operands by field path instead. Matching a fragment of it is
	// what proves *this* rule fired rather than a per-field Maximum.
	const wantRejection = "must not exceed 4Gi"

	return []apiCase{
		{
			// The shipped defaults: 4 × 64Mi = 256Mi. Not trivially small, which is
			// why the bound is on the product rather than on either field.
			name: "worker-memory-at-the-defaults-is-accepted",
			obj:  validS3Sink(),
		},
		{
			// The shape that actually helps: S3 rewards request concurrency, so many
			// workers over small objects is the tuning that scales. 64 × 1Mi = 64Mi.
			name: "worker-memory-many-workers-with-a-small-rotation-is-accepted",
			obj:  s3SinkWithWorkerMemory(64, oneMi),
		},
		{
			// And its mirror: one worker may hold the largest object the CRD admits.
			name: "worker-memory-one-worker-with-the-largest-rotation-is-accepted",
			obj:  s3SinkWithWorkerMemory(1, oneGi),
		},
		{
			// The pairing the API comment used to only warn about. 64 × 1Gi = 64Gi.
			name:    "worker-memory-both-ceilings-together-are-rejected",
			obj:     s3SinkWithWorkerMemory(64, oneGi),
			wantErr: wantRejection,
		},
		{
			// One Mi per worker over the line, so the rejection is the product's and
			// not a rounded-off approximation of it: 8 × 513Mi = 4Gi + 8Mi.
			name:    "worker-memory-just-above-the-budget-is-rejected",
			obj:     s3SinkWithWorkerMemory(8, budgetOver),
			wantErr: wantRejection,
		},
		{
			// The reason clause, asserted once: the failure text has to explain *why*
			// two individually-legal values are illegal together, or the only way to
			// fix the spec is to read the source.
			name:    "worker-memory-rejection-explains-that-workers-multiplies-memory",
			obj:     s3SinkWithWorkerMemory(16, halfGi),
			wantErr: "each worker accumulates its own object",
		},
		{
			// Exactly on the budget is admitted: the rule is <=, and a bound that
			// rejected its own documented figure would make the number in the message
			// wrong.
			name: "worker-memory-exactly-at-the-budget-is-accepted",
			obj:  s3SinkWithWorkerMemory(8, halfGi),
		},
		{
			name: "worker-memory-just-below-the-budget-is-accepted",
			obj:  s3SinkWithWorkerMemory(8, budgetUnder),
		},

		// The omitted-field half. A typed client always sends `rotation: {}` and
		// `writer: {}` — `omitempty` does nothing for a non-pointer struct — so the
		// apiserver defaults their children and the rule reads real values. Only a
		// hand-written YAML spec can leave the blocks out entirely, and then nothing
		// defaults them: structural defaulting does not descend into an absent
		// parent, which is what makes the rule's fallback literals load-bearing
		// rather than decorative. TestS3SinkOmittedRotationAndWriterStayAbsent pins
		// that mechanism; these three cases pin what the rule does with it.
		{
			name: "worker-memory-spec-omitting-both-blocks-is-accepted",
			obj: unstructuredS3Sink(map[string]any{
				"bucket": "kuberecord-audit",
			}),
		},
		{
			// Only `writer` omitted, so workers falls back to 4 against an explicit
			// 1Gi: exactly the budget. This is as far as the fallback can be pushed —
			// 1Gi is maxObjectBytes' own ceiling — which is why the omitted-workers
			// branch is asserted by acceptance at the boundary rather than by a
			// rejection just past it.
			name: "worker-memory-spec-omitting-writer-is-accepted-at-the-budget",
			obj: unstructuredS3Sink(map[string]any{
				"bucket":   "kuberecord-audit",
				"rotation": map[string]any{"maxObjectBytes": oneGi},
			}),
		},
		{
			// Only `rotation` omitted, so maxObjectBytes falls back to 64Mi against 64
			// workers: exactly the budget again, and for the same reason (64 is
			// workers' own ceiling).
			name: "worker-memory-spec-omitting-rotation-is-accepted-at-the-budget",
			obj: unstructuredS3Sink(map[string]any{
				"bucket": "kuberecord-audit",
				"writer": map[string]any{"workers": int64(64)},
			}),
		},
		{
			// The same pairing written out, which is what pins where the fallback
			// actually sits: 64 workers at the *default* rotation size is exactly the
			// budget, admitted.
			name: "worker-memory-max-workers-at-the-default-rotation-is-exactly-the-budget",
			obj:  s3SinkWithWorkerMemory(64, defaultMi),
		},
		{
			// And one Mi more is refused. Together with the omitted-rotation case
			// above, this is what distinguishes "the fallback is 64Mi" from "the rule
			// skipped an omitted block": the boundary is at the default, not anywhere
			// else, and it is a boundary rather than an exemption.
			name:    "worker-memory-max-workers-one-mi-past-the-default-rotation-is-rejected",
			obj:     s3SinkWithWorkerMemory(64, defaultMi+oneMi),
			wantErr: wantRejection,
		},
		{
			// The present-value branch, proved against the same shape: add the one
			// worker the fallback would not have counted and the same spec is
			// rejected. Without this, a rule that ignored `writer` entirely would
			// pass every case above.
			name: "worker-memory-spec-naming-one-worker-past-the-budget-is-rejected",
			obj: unstructuredS3Sink(map[string]any{
				"bucket":   "kuberecord-audit",
				"rotation": map[string]any{"maxObjectBytes": oneGi},
				"writer":   map[string]any{"workers": int64(5)},
			}),
			wantErr: wantRejection,
		},
	}
}

// s3SinkWithWorkerMemory returns a valid sink whose two memory operands are set
// explicitly, leaving every other rotation and writer knob defaulted.
func s3SinkWithWorkerMemory(workers int32, maxObjectBytes int64) *S3Sink {
	return s3SinkWith(func(s *S3Sink) {
		s.Spec.Rotation.MaxObjectBytes = ptrTo(maxObjectBytes)
		s.Spec.Writer.Workers = ptrTo(workers)
	})
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

// TestS3SinkOmittedRotationAndWriterStayAbsent pins the defaulting mechanism the
// worker-memory rule's fallback literals rest on, because that rule is only as
// honest as this fact.
//
// The rule reads `has(self.writer) && has(self.writer.workers) ? … : 4`. That
// fallback is dead weight if the apiserver materializes `writer` for a spec that
// omitted it, and it is load-bearing if it does not — and which of the two is true
// is not a property of this project at all, it is CRD structural defaulting's:
// defaults are applied to unspecified fields of an object that *exists*, and
// `rotation` and `writer` have no defaults of their own, so an absent block stays
// absent and its children are never visited. That is also why the branch cannot be
// exercised by a rejection anywhere in the table above: each default multiplied by
// the opposite field's ceiling lands exactly on the budget (4 × 1Gi, 64 × 64Mi), so
// the fallback can be pushed to the boundary and no further.
//
// It is asserted through a real apiserver round-trip rather than read off the CRD
// because the claim is about behaviour, not about markers. Only an unstructured
// write can express it: `omitempty` does not drop a non-pointer struct, so a typed
// S3Sink always sends `rotation: {}` and `writer: {}` and always gets them
// defaulted — which is a fine thing to happen and is what TestS3SinkDefaults
// covers, but it is the other case.
//
// If this test ever fails because both blocks came back populated, the rule's
// fallbacks have become unreachable: the defaults would then be applied before CEL
// runs and the ternaries could be simplified away. Nothing would be *wrong* — the
// rule would keep judging the same numbers — but the comment on S3SinkSpec would
// have stopped being true, which is the failure this pins.
func TestS3SinkOmittedRotationAndWriterStayAbsent(t *testing.T) {
	ctx := context.Background()
	obj := unstructuredS3Sink(map[string]any{"bucket": "kuberecord-audit"})
	obj.SetName("omitted-rotation-and-writer")
	if err := k8sClient.Create(ctx, obj); err != nil {
		t.Fatalf("creating a sink that omits rotation and writer: %v", err)
	}
	defer deleteObject(ctx, t, obj)

	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(GroupVersion.WithKind("S3Sink"))
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: obj.GetName()}, got); err != nil {
		t.Fatalf("reading the sink back: %v", err)
	}

	for _, block := range []string{"rotation", "writer"} {
		t.Run(block, func(t *testing.T) {
			value, found, err := unstructured.NestedFieldNoCopy(got.Object, "spec", block)
			if err != nil {
				t.Fatalf("reading spec.%s: %v", block, err)
			}
			if found {
				t.Errorf("spec.%s came back as %v for a spec that omitted it.\n"+
					"The worker-memory rule on spec falls back to the documented defaults for an "+
					"absent block precisely because structural defaulting does not descend into an "+
					"absent parent. If it now does, those fallbacks are unreachable and S3SinkSpec's "+
					"comment about them is wrong.", block, value)
			}
		})
	}

	// The sibling scalars *are* defaulted, which is what makes the assertion above
	// a statement about absent parents rather than about defaulting being off for
	// this CRD.
	region, found, err := unstructured.NestedString(got.Object, "spec", "region")
	if err != nil || !found || region != "us-east-1" {
		t.Errorf("spec.region = %q (present %v, err %v), want us-east-1: if the top-level defaults "+
			"did not fire either, this test proves nothing about absent parents", region, found, err)
	}
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
