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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// S3ObjectFormat names the physical encoding of the objects an S3Sink writes.
//
// It is a named type rather than a bare string so the encoder can switch on it
// exhaustively once there is more than one member, and so the one legal spelling
// lives in Go rather than only in a marker comment.
type S3ObjectFormat string

const (
	// S3ObjectFormatJSONLV1Zstd is version 1 of the JSON Lines layout, zstd
	// compressed: one sink.Record per line, fields named after the Record's own
	// JSON tags, nothing reordered and nothing omitted.
	//
	// The version is *inside* the value rather than alongside it because the
	// object format is its own public, versioned contract (D15) — separate from
	// the ClickHouse physical schema and frozen on its own timeline. A reader
	// pointed at a bucket must be able to tell which contract produced the bytes
	// from the sink's spec alone, and the same "jsonl-v1" token is what appears in
	// the object key's `format=` partition, so the CR and the bucket layout name
	// the same thing.
	S3ObjectFormatJSONLV1Zstd S3ObjectFormat = "jsonl-v1-zstd"
)

// ObjectLockMode is an S3 Object Lock retention mode, spelled exactly as the S3
// API spells it.
//
// The two modes differ in who can shorten a retention that has already been
// applied, which is the entire compliance question: GOVERNANCE retention can be
// lifted by a principal holding s3:BypassGovernanceRetention, COMPLIANCE
// retention can be lifted by nobody at all, including the account root, until it
// expires. kuberecord neither weakens nor interprets either one — it passes the
// mode through on the PUT — so the constant names are the S3 names.
type ObjectLockMode string

const (
	// ObjectLockModeGovernance protects an object from deletion except by a
	// principal explicitly granted the bypass permission.
	ObjectLockModeGovernance ObjectLockMode = "GOVERNANCE"

	// ObjectLockModeCompliance protects an object from deletion by anyone,
	// including the account root user, until its retention date passes. Choosing
	// it is irreversible for the objects it is applied to: a misconfigured
	// retainDays cannot be undone by deleting the S3Sink, only by waiting.
	ObjectLockModeCompliance ObjectLockMode = "COMPLIANCE"
)

// S3CredentialsSpec is how an S3Sink authenticates, when it authenticates
// explicitly.
//
// Its *absence* is meaningful and is the recommended shape on a cloud provider:
// omit spec.credentials entirely and the sink builds its client from the ambient
// credential chain — IRSA, workload identity, or an instance role — so no
// long-lived key exists to leak or rotate. Setting it names a Secret instead,
// which is what MinIO and on-premises deployments need.
//
// The CEL rule below rejects the third state, an empty `credentials: {}`, which
// is neither of those and is always a half-finished edit. It is a validation rule
// rather than a `required` marker on secretRef precisely so the rejection message
// can name the ambient alternative: an author who meant "no credentials here"
// needs to be told to remove the block, not that a field they never wanted is
// missing.
//
// +kubebuilder:validation:XValidation:rule="has(self.secretRef)",message="spec.credentials must name a secretRef; to use ambient credentials (IRSA, workload identity or an instance role) omit spec.credentials entirely"
type S3CredentialsSpec struct {
	// SecretRef names the Secret holding this sink's access key ID and secret
	// access key. Its namespace defaults to the operator's own namespace — see
	// SecretReference.Namespace for why that default is a security boundary
	// rather than a convenience.
	// +optional
	SecretRef *SecretReference `json:"secretRef,omitempty"`
}

// S3RotationSpec decides when an accumulating object is closed and PUT.
//
// Rotation is what batching is for this backend: an S3 object *is* the batch, so
// these two knobs govern the same trade the ClickHouse writer spends
// batchMaxRows/batchMaxWait on — object count and per-object overhead against
// end-to-end latency and the amount of un-PUT data a crash can cost. They are the
// only two triggers; whichever fires first closes the object.
type S3RotationSpec struct {
	// MaxObjectBytes is the encoded size at which an accumulating object is
	// closed and written, in bytes.
	//
	// It is measured on the *encoded* (compressed) payload, because that is what
	// the object costs to store and to read back, and because it is the only
	// figure a reader can predict from the bucket alone.
	//
	// The floor of 1Mi and ceiling of 1Gi are both about the read side. Below
	// 1Mi an active cluster produces the small-file explosion that makes an
	// archive expensive to query — every engine reading this layout pays per
	// object. Above 1Gi a single object stops being a practical unit of retry or
	// of partial recovery, and a worker's in-flight memory (see
	// S3WriterSpec.Workers) grows with it.
	// +optional
	// +kubebuilder:default=67108864
	// +kubebuilder:validation:Minimum=1048576
	// +kubebuilder:validation:Maximum=1073741824
	MaxObjectBytes *int64 `json:"maxObjectBytes,omitempty"`

	// MaxObjectAge is the longest an object's *first* record waits before the
	// object is closed and written regardless of size.
	//
	// It is what bounds how much of the audit trail is only in memory at any
	// moment: on a quiet cluster nothing else would ever close an object, and a
	// crash would take the whole open batch with it. The floor of 10s keeps a
	// quiet cluster from turning every record into its own object; the ceiling of
	// 1h keeps the exposure window inside the archive's coarsest path partition,
	// so an object never spans two hour= prefixes' worth of waiting.
	//
	// The bound is one CEL rule rather than Minimum/Maximum because a duration is
	// a string in the schema, and it carries its own shape guard rather than a
	// companion Pattern marker for two reasons: controller-gen refuses a Pattern
	// on a metav1.Duration at all, and `duration()` *errors* on an unparseable
	// string, which the API server reports as machinery failing rather than as the
	// author's value being wrong. CEL's logical operators absorb errors from the
	// side that did not decide the result, so putting the match first makes the
	// whole rule total: garbage fails the match and is rejected with the message
	// below, and only a well-formed duration is ever parsed.
	//
	// The shape deliberately admits no fractional component: `1.5h` is spelled
	// `1h30m`, and keeping the backslash-free character class out of a CEL string
	// literal keeps the rule readable through two layers of quoting.
	// +optional
	// +kubebuilder:default="5m"
	// +kubebuilder:validation:XValidation:rule="self.matches('^([0-9]+(ns|us|ms|s|m|h))+$') && duration(self) >= duration('10s') && duration(self) <= duration('1h')",message="maxObjectAge must be a duration between 10s and 1h, spelled without a fractional component (30s, 5m, 1h30m)"
	MaxObjectAge *metav1.Duration `json:"maxObjectAge,omitempty"`
}

// S3ObjectLockSpec applies per-object S3 Object Lock retention at PUT time.
//
// kuberecord does not configure the bucket and cannot: Object Lock must already
// be enabled on it (which on S3 is only possible at bucket creation). This block
// sets the retention *of each object kuberecord writes*, which is the half an
// operator can express declaratively — and the half that makes the archive
// tamper-evident, since a retained object cannot be overwritten or deleted before
// its date even by the credential that wrote it.
//
// One consequence deserves stating here rather than in a runbook: retention makes
// the idempotent-overwrite property of the object key conditional. A retried PUT
// of an identical batch produces the same key and the same bytes (Task 6.2), and
// S3 rejects the overwrite of a locked object — harmlessly, because the object
// already holds exactly those bytes, but visibly in the sink's logs.
type S3ObjectLockSpec struct {
	// Mode is the retention mode applied to every object. COMPLIANCE is
	// irreversible for the objects it covers — see ObjectLockModeCompliance.
	// +kubebuilder:validation:Enum=GOVERNANCE;COMPLIANCE
	Mode ObjectLockMode `json:"mode"`

	// RetainDays is how long, in days from the PUT, each object is retained.
	//
	// The ceiling of 36500 (a century) is a typo guard rather than a technical
	// limit: with COMPLIANCE mode an extra digit is not correctable by anyone, so
	// the one place it can still be caught is admission.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=36500
	RetainDays int32 `json:"retainDays"`
}

// S3WriterSpec sizes an S3Sink's asynchronous write path.
//
// It is deliberately a strict subset of ClickHouse's WriterSpec, and the three
// absentees are not oversights:
//
//   - batchMaxRows and batchMaxWait have no meaning here, because for S3 the
//     object *is* the batch and spec.rotation governs it. Having both would be
//     two sets of controls over one decision, and an author would have to know
//     which one won.
//   - checkpointEvery bounds the cost of replaying diffs, and an S3Sink writes no
//     diffs: it cannot read its own history, so every record it receives is a
//     permanent Snapshot (D12). A cadence over a thing that never happens would
//     read as a knob that does nothing.
//
// What the four shared knobs mean is identical to their ClickHouse twins, down to
// the defaults, so an author who has tuned one sink has tuned both. That identity
// is asserted against the generated schemas rather than left to review — see
// TestSharedWriterKnobsAgreeAcrossSinks.
type S3WriterSpec struct {
	// QueueSize is the capacity, in jobs, of the async hand-off queue between
	// pipeline workers and this sink's writers.
	// +optional
	// +kubebuilder:default=5000
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000000
	QueueSize *int32 `json:"queueSize,omitempty"`

	// Workers is how many goroutines drain the queue into S3.
	//
	// Each worker accumulates its own object, so this multiplies two costs rather
	// than one: the writer's steady-state memory ceiling is workers ×
	// maxObjectBytes (at the ceilings of both, 64Gi — do not pair them), and the
	// object count grows with it, since N workers close N partial objects where
	// one worker would have closed a single full one. The ceiling of 64 matches
	// ClickHouse's for a different reason: not because the backend punishes
	// concurrency — S3 rewards it — but because memory does.
	// +optional
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	Workers *int32 `json:"workers,omitempty"`

	// EnqueueTimeout is how long Enqueue waits for queue room before returning
	// an error. It is a backpressure signal, never a silent drop: the caller
	// propagates the error so the pipeline's own requeue/backoff takes over.
	// +optional
	// +kubebuilder:default="2s"
	EnqueueTimeout *metav1.Duration `json:"enqueueTimeout,omitempty"`

	// DrainTimeout is the budget for flushing queued writes during graceful
	// shutdown, before the client is closed and any still-queued job is settled
	// as failed. For this backend the drain includes closing and PUTting each
	// worker's partial object, so it must accommodate a round-trip per worker,
	// not merely the queue's length.
	// +optional
	// +kubebuilder:default="15s"
	DrainTimeout *metav1.Duration `json:"drainTimeout,omitempty"`
}

// S3SinkSpec is the desired state of an S3Sink.
type S3SinkSpec struct {
	// Bucket is the S3 (or MinIO) bucket every object is written to. kuberecord
	// never creates it: a bucket carries retention, encryption, lifecycle and
	// Object Lock settings that belong to whoever owns the account, and an
	// operator that created its own would create one with none of them.
	//
	// There is deliberately no character Pattern beyond the length bound. S3's
	// current naming rules are stricter than what exists in the wild — buckets
	// created in us-east-1 before 2018 may carry uppercase letters and
	// underscores, and MinIO deployments set their own rules — so a pattern
	// written to today's documentation would reject buckets that work. An
	// unusable name fails visibly on the BucketReachable condition instead.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Bucket string `json:"bucket"`

	// Prefix is an optional key prefix every object is written under, letting one
	// bucket hold several clusters' archives (or share space with something else
	// entirely).
	//
	// It is a path *fragment*, not a path: no leading slash, no trailing slash,
	// no empty segments. The object key is built as
	// `<prefix>/format=jsonl-v1/…`, so a trailing slash would silently produce a
	// `//` in every key this sink ever writes and quietly break the documented
	// layout (D15) for every reader of the bucket. The permitted characters are
	// the conservative subset that needs no escaping in a key, a URL or a query
	// engine's glob; it can be widened later without invalidating anything
	// already written, which is not true in the other direction.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^([A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*)?$`
	Prefix string `json:"prefix,omitempty"`

	// Region is the AWS region the bucket lives in.
	//
	// It defaults to us-east-1 because the SDK requires *some* region even when
	// talking to MinIO, which ignores it — that default is what makes a bucket
	// name alone a working spec. Unlike a guessed sink name, a guessed region
	// cannot send the audit trail somewhere nobody chose: S3 bucket names are
	// globally unique, so a wrong region does not resolve to a different bucket,
	// it fails loudly and lands on the BucketReachable condition.
	// +optional
	// +kubebuilder:default="us-east-1"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9-]+$`
	Region string `json:"region,omitempty"`

	// Endpoint overrides the S3 API endpoint, which is how MinIO and other
	// S3-compatible stores are addressed. Empty means AWS S3 itself, resolved
	// from Region.
	//
	// The scheme is mandatory. `minio.kuberecord-system.svc:9000` is the mistake
	// this Pattern exists to catch: the SDK needs an absolute URL, and a bare
	// host:port is accepted by the CR, rejected by the client constructor, and
	// diagnosed nowhere near where it was typed.
	//
	// `http://` is permitted, because in-cluster MinIO over plain HTTP is a normal
	// deployment — but it does mean this cluster's entire audit stream, redactions
	// applied and all, crosses the network unencrypted.
	// +optional
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^https?://[^\s/]+(/[^\s]*)?$`
	Endpoint string `json:"endpoint,omitempty"`

	// ForcePathStyle addresses buckets as `<endpoint>/<bucket>/<key>` rather than
	// as `<bucket>.<endpoint>/<key>`.
	//
	// It is false by default because virtual-hosted style is what AWS S3 wants,
	// and true is what most MinIO deployments need: a bucket-as-subdomain URL
	// only resolves if DNS (and any TLS certificate) covers `*.<endpoint>`, which
	// a Service name in a cluster does not.
	// +optional
	// +kubebuilder:default=false
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`

	// Credentials names the Secret holding this sink's access key, or is omitted
	// to authenticate from the ambient credential chain. See S3CredentialsSpec:
	// the absence is a supported, and on a cloud provider preferred, state.
	// +optional
	Credentials *S3CredentialsSpec `json:"credentials,omitempty"`

	// Format is the physical encoding of the objects this sink writes.
	//
	// The enum has exactly one member on purpose. Reserving the field now means
	// adding `parquet` later is an additive change to a schema authors already
	// spell explicitly, rather than a breaking one that has to invent a default
	// for every S3Sink already in etcd — and it makes every existing CR say, in
	// its own text, which versioned contract (D15) produced its objects.
	// +optional
	// +kubebuilder:default="jsonl-v1-zstd"
	// +kubebuilder:validation:Enum=jsonl-v1-zstd
	Format S3ObjectFormat `json:"format,omitempty"`

	// Rotation decides when an accumulating object is closed and written.
	// +optional
	Rotation S3RotationSpec `json:"rotation,omitempty"`

	// ObjectLock applies per-object retention at PUT time. Omitted means objects
	// are written with no retention of their own, and the bucket's own lifecycle
	// and default-retention settings apply unchanged.
	// +optional
	ObjectLock *S3ObjectLockSpec `json:"objectLock,omitempty"`

	// Writer sizes the asynchronous write path for this sink.
	//
	// It carries a strict subset of ClickHouse's knobs: batchMaxRows and batchMaxWait
	// are absent because for S3 the object *is* the batch and spec.rotation
	// governs it, and checkpointEvery is absent because a Writer-only sink emits
	// no diffs to checkpoint (D12). See S3WriterSpec for the full reasoning — the
	// omissions are deliberate, not pending.
	// +optional
	Writer S3WriterSpec `json:"writer,omitempty"`

	// Policy restricts what may be written to this sink.
	//
	// It is the same type, the same shape and the same semantics as
	// ClickHouseSink's, which is load-bearing rather than tidy: redaction is a
	// per-sink floor, and if a backend could carry a weaker policy shape then
	// choosing that backend would be a way to opt out of the floor. Streaming to
	// an archive nobody queries is exactly when the temptation to skip redaction
	// is highest, and exactly when the data lives longest.
	// +optional
	Policy SinkPolicy `json:"policy,omitempty"`
}

// S3SinkStatus is the observed state of an S3Sink.
type S3SinkStatus struct {
	// Conditions carries Ready, CredentialsResolved, BucketReachable and
	// HistoryUnavailable (see the condition-type constants).
	//
	// The first three follow the usual reading, and are set by asynchronous
	// probes: a control-plane reconciler never talks to S3 on its own goroutine
	// (Invariant 1), so an unreachable bucket slows nothing down — it just reports
	// here. HistoryUnavailable does not follow the usual reading: it is True on a
	// healthy sink, permanently, and says what this backend cannot do rather than
	// what has gone wrong with it. See ConditionHistoryUnavailable.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// ObservedGeneration is the metadata.generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// S3Sink declares one S3 or MinIO bucket kuberecord may archive to.
//
// It is cluster-scoped (D6) for the same reasons ClickHouseSink is: rules in any
// namespace reference it by name, and a bucket is infrastructure owned by the
// platform team rather than by whoever owns a namespace. Credentials are the only
// part not in this object, and may not be in the cluster at all — see
// S3CredentialsSpec.
//
// It is the first Writer-only backend (D12). It cannot read its own history, so
// cache warm-up, zombie garbage collection and boot reconciliation of scope
// epochs are all disabled for it, and every record it receives is a permanent
// Snapshot. That is a declared capability limit rather than a defect, and it is
// reported on the sink itself as HistoryUnavailable=True while Ready stays True.
// The supported way to have both a queryable timeline and a cheap immutable
// archive is the tee pattern (D14): two rules over the same resources, one naming
// a ClickHouseSink and one naming this.
//
// The BUCKET printer column shows spec.bucket alone. The prefix is not in it
// because CRD printer columns are plain JSONPath with no string functions, so
// "bucket/prefix" cannot be rendered declaratively, and a column showing only the
// prefix would be blank on most sinks.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="READY",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="BUCKET",type=string,JSONPath=`.spec.bucket`
// +kubebuilder:printcolumn:name="AGE",type=date,JSONPath=`.metadata.creationTimestamp`
type S3Sink struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of S3Sink.
	// +required
	Spec S3SinkSpec `json:"spec"`

	// status defines the observed state of S3Sink.
	// +optional
	Status S3SinkStatus `json:"status,omitempty"`
}

// S3SinkList contains a list of S3Sink.
// +kubebuilder:object:root=true
type S3SinkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []S3Sink `json:"items"`
}

func init() {
	SchemeBuilder.Register(&S3Sink{}, &S3SinkList{})
}
