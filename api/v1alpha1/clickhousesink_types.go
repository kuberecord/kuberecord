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

// ConnectionSpec describes how to reach one ClickHouse instance.
//
// Only the username lives in the CR; the password lives in a Secret
// (CredentialsSecretRef) so a ClickHouseSink can be committed to Git and read
// by anyone with cluster-read access without leaking a credential.
type ConnectionSpec struct {
	// Addr is the ClickHouse native-protocol endpoint as "host:port", e.g.
	// "clickhouse.kuberecord-system.svc:9000".
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Addr string `json:"addr"`

	// Database is the ClickHouse database holding kuberecord's tables.
	// +optional
	// +kubebuilder:default="kuberecord"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Database string `json:"database,omitempty"`

	// Username is the ClickHouse user the operator authenticates as. Its
	// password comes from CredentialsSecretRef.
	// +optional
	// +kubebuilder:default="default"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Username string `json:"username,omitempty"`

	// CredentialsSecretRef names the Secret holding this sink's password.
	// Its namespace defaults to the operator's own namespace — see
	// SecretReference.Namespace for why that default is a security boundary
	// rather than a convenience.
	CredentialsSecretRef SecretReference `json:"credentialsSecretRef"`

	// DialTimeout bounds establishing a connection to Addr.
	// +optional
	// +kubebuilder:default="5s"
	DialTimeout *metav1.Duration `json:"dialTimeout,omitempty"`

	// ReadTimeout bounds a single query's read phase.
	// +optional
	// +kubebuilder:default="10s"
	ReadTimeout *metav1.Duration `json:"readTimeout,omitempty"`
}

// WriterSpec sizes this sink's asynchronous write path.
//
// These are the same six knobs the operator previously took as global
// --writer-* flags, now scoped per sink: a cluster may stream a low-volume
// audit rule to one ClickHouse and a firehose to another, and the two have
// genuinely different queue and batching needs. Defaults match the shipped
// clickhouse.Default* constants, so an unset field keeps the behavior an
// operator already tuned against.
//
// None of these knobs can make a write block the hot path — Enqueue is a
// bounded hand-off in every configuration. They trade memory
// (QueueSize) and end-to-end latency (BatchMaxWait) against insert efficiency
// (BatchMaxRows) and backpressure sensitivity (EnqueueTimeout).
type WriterSpec struct {
	// QueueSize is the capacity, in jobs, of the async hand-off queue between
	// pipeline workers and this sink's writers.
	// +optional
	// +kubebuilder:default=5000
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000000
	QueueSize *int32 `json:"queueSize,omitempty"`

	// Workers is how many goroutines drain the queue into ClickHouse. The
	// upper bound is 64 because ClickHouse punishes high insert concurrency
	// far more than it rewards it — past a few dozen concurrent inserters the
	// server spends its time merging parts, not ingesting.
	// +optional
	// +kubebuilder:default=4
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=64
	Workers *int32 `json:"workers,omitempty"`

	// BatchMaxRows is the row count at which a worker flushes its accumulated
	// insert batch. Row-per-INSERT is ClickHouse's pathological write pattern,
	// so the floor is 1 only to keep the knob honest, never as a recommended
	// value; the ceiling of 100000 bounds the memory one worker can hold and
	// the size of the batch a single failure can poison.
	// +optional
	// +kubebuilder:default=1000
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100000
	BatchMaxRows *int32 `json:"batchMaxRows,omitempty"`

	// BatchMaxWait is the longest a batch's first job waits for the batch to
	// fill before it is flushed regardless. It is what keeps a quiet cluster's
	// records from sitting in memory indefinitely behind an unmet row target.
	// +optional
	// +kubebuilder:default="1s"
	BatchMaxWait *metav1.Duration `json:"batchMaxWait,omitempty"`

	// EnqueueTimeout is how long Enqueue waits for queue room before returning
	// an error. It is a backpressure signal, never a silent drop: the caller
	// propagates the error so the pipeline's own requeue/backoff takes over.
	// +optional
	// +kubebuilder:default="2s"
	EnqueueTimeout *metav1.Duration `json:"enqueueTimeout,omitempty"`

	// DrainTimeout is the budget for flushing queued writes during graceful
	// shutdown, before the connection is closed and any still-queued job is
	// settled as failed.
	// +optional
	// +kubebuilder:default="15s"
	DrainTimeout *metav1.Duration `json:"drainTimeout,omitempty"`

	// CheckpointEvery is how many consecutive diff-only `Modified` rows this sink
	// accepts for one object before the next one is written as a `Checkpoint` —
	// a row carrying the full state *and* the diff.
	//
	// It is the knob that bounds replay cost. With diffs-only history,
	// reconstructing "state at time T" means replaying every diff back to the
	// object's last full row, which is unbounded for a long-lived object; a
	// Checkpoint every N modifications caps that walk at N diffs (see
	// docs/SCHEMA.md). It is per-sink because the trade — storage for query cost
	// — belongs to whoever owns the ClickHouse instance and its analysts.
	//
	// `0` disables checkpointing entirely for this sink. The ceiling of 10000 is
	// not a technical limit but an honesty one: a cadence beyond it is
	// indistinguishable from disabling the feature while looking like it is on.
	// A single diff that comes out larger than the object it describes is
	// checkpointed regardless of the cadence — unless checkpointing is disabled.
	// +optional
	// +kubebuilder:default=50
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10000
	CheckpointEvery *int32 `json:"checkpointEvery,omitempty"`
}

// ClickHouseSinkSpec is the desired state of a ClickHouseSink.
type ClickHouseSinkSpec struct {
	// Connection is how to reach ClickHouse.
	Connection ConnectionSpec `json:"connection"`

	// Writer sizes the asynchronous write path for this sink.
	// +optional
	Writer WriterSpec `json:"writer,omitempty"`

	// Policy restricts what may be written to this sink.
	// +optional
	Policy SinkPolicy `json:"policy,omitempty"`
}

// ClickHouseSinkStatus is the observed state of a ClickHouseSink.
type ClickHouseSinkStatus struct {
	// Conditions carries Ready, SchemaValid and CredentialsResolved (see the
	// condition-type constants). All three are set by asynchronous probes:
	// a control-plane reconciler never dials ClickHouse on its own goroutine
	// so an unreachable sink slows nothing down — it just reports here.
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

// ClickHouseSink declares one ClickHouse instance kuberecord may write to.
//
// It is cluster-scoped because rules in any namespace reference it by
// name, and because a sink is infrastructure owned by the platform team rather
// than by whoever owns the namespace a rule lives in. The password is the only
// part not in this object; it lives in the Secret named by
// spec.connection.credentialsSecretRef.
//
// The ADDR printer column shows the full "host:port" from
// spec.connection.addr: CRD printer columns are plain JSONPath with no string
// functions, so the host cannot be split out declaratively.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="READY",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="ADDR",type=string,JSONPath=`.spec.connection.addr`
// +kubebuilder:printcolumn:name="AGE",type=date,JSONPath=`.metadata.creationTimestamp`
type ClickHouseSink struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec defines the desired state of ClickHouseSink.
	// +required
	Spec ClickHouseSinkSpec `json:"spec"`

	// status defines the observed state of ClickHouseSink.
	// +optional
	Status ClickHouseSinkStatus `json:"status,omitempty"`
}

// ClickHouseSinkList contains a list of ClickHouseSink.
// +kubebuilder:object:root=true
type ClickHouseSinkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClickHouseSink `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClickHouseSink{}, &ClickHouseSinkList{})
}
