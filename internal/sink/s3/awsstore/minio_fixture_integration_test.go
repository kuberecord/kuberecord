//go:build integration

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

// This file is the fixture the S3 integration suite runs against: a real object
// store, reached the way the shipped code reaches it, plus the read side the
// shipped code deliberately does not have.
//
// It lives in this package rather than beside the write path because this is the
// only package where the two can meet. internal/sink/s3 links no SDK — that is
// enforced by the `object-store-client-is-confined` depguard rule and is the
// whole reason s3.ObjectStore exists (see internal/sink/s3/client.go) — so a test
// inside that package can drive rotation, retry and commit logic against a
// stand-in but can never reach a bucket. Here, awsstore.New builds the same
// client cmd/main.go builds, and the assertions in
// writer_minio_integration_test.go drive the same s3.Writer the operator runs.
//
// The read side (LIST, GET, HEAD) is SDK usage that exists only for these tests.
// That asymmetry is the point of D12 restated as code: kuberecord's S3 credential
// needs PutObject and nothing else, so anything that reads the archive back —
// this fixture, the documented DuckDB recipes, the CLI's own object source in
// internal/query/objectsource/awssource, an auditor — is a separate consumer with
// separate rights, not a capability of the sink. The CLI's source is why that
// package is the second one excepted from object-store-client-is-confined: it is
// a different consumer, so it is a different package, and neither reaches the
// other.
package awsstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/kuberecord/kuberecord/internal/pipeline"
	"github.com/kuberecord/kuberecord/internal/sink"
	kbs3 "github.com/kuberecord/kuberecord/internal/sink/s3"
)

// The MinIO service `make test-integration` stands up, and the environment it is
// announced through.
//
// The endpoint defaults to the port that target maps rather than to MinIO's own
// 9000, which is the one place this suite's defaults differ in spirit from the
// ClickHouse ones. Those default to 127.0.0.1:9000 so a developer's local server
// just works — but that is ClickHouse's native port, so pointing an S3 client at
// it by default would produce a confusing failure against a running ClickHouse
// rather than a clear one against a missing MinIO.
const (
	envEndpoint  = "S3_TEST_ENDPOINT"
	envAccessKey = "S3_TEST_ACCESS_KEY_ID"
	envSecretKey = "S3_TEST_SECRET_ACCESS_KEY"

	defaultEndpoint  = "http://127.0.0.1:19100"
	defaultAccessKey = "kuberecord"
	defaultSecretKey = "kuberecord"

	// itRegion is the region every client here is built with. MinIO ignores it;
	// the SDK requires one, and the S3Sink CRD defaults spec.region to this same
	// value, so the fixture is configured exactly as a defaulted CR would be.
	itRegion = "us-east-1"
)

// itClusterID and itPrefix are the cluster and prefix every object in this suite
// is filed under. They are fixed values because they are part of what the key
// assertions state: an object key is derived from the sink's prefix and the
// record's cluster_id, and a suite that varied either could not spell out the
// expected key.
const (
	itClusterID = "minio-it-cluster"
	itPrefix    = "audit/kuberecord"
)

// itLockMode is the only Object Lock mode this suite ever writes.
//
// GOVERNANCE, never COMPLIANCE: a COMPLIANCE-retained version cannot be removed
// by anyone until its date passes — including the suite that wrote it — so a run
// would leave a bucket behind for as long as the retention it asked for. What
// COMPLIANCE means is asserted from the CRD's validation and stated in
// docs/RETENTION.md; it does not need an undeletable object written on every CI
// run to be true. It is a constant rather than a literal per call site so the
// choice is made once and visibly.
const itLockMode = "GOVERNANCE"

// itSinkID labels the metrics this suite's writers publish. Nothing asserts on
// them — the write path's metrics are covered by unit tests — but a Writer needs
// a non-nil Metrics, and building it the way cmd/main.go does keeps this fixture
// from depending on a shape the operator never uses.
var itSinkID = sink.ID{Kind: "S3Sink", Name: "integration"}

// itMetrics builds a throwaway per-sink metrics view. Each call gets its own
// registry, so two writers in one test binary cannot collide on a duplicate
// collector registration.
func itMetrics() kbs3.Metrics {
	return pipeline.NewPipelineMetrics(prometheus.NewRegistry()).ForSink(itSinkID)
}

// envOr returns the environment value for key, or def when it is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// itCredentials is the static key the fixture and the sink both authenticate
// with. The MinIO container runs with these as its root credentials, so the
// sink's spec.credentials path (a static key, not the ambient chain) is what is
// under test — which is the shape an in-cluster MinIO deployment uses.
func itCredentials() kbs3.Credentials {
	return kbs3.Credentials{
		AccessKeyID:     envOr(envAccessKey, defaultAccessKey),
		SecretAccessKey: envOr(envSecretKey, defaultSecretKey),
	}
}

// itClientConfig is the resolved client configuration for this fixture's MinIO:
// an endpoint override and path-style addressing, which is exactly what
// newS3SinkConfigBuilder produces for an S3Sink naming `endpoint` and
// `forcePathStyle: true`.
//
// forcePathStyle is not optional against this fixture and that is worth stating:
// a bucket-as-subdomain URL only resolves where DNS covers *.<endpoint>, which it
// does not for a container on 127.0.0.1 nor for a Service name in a cluster.
func itClientConfig() kbs3.ClientConfig {
	return kbs3.ClientConfig{
		Region:         itRegion,
		Endpoint:       envOr(envEndpoint, defaultEndpoint),
		ForcePathStyle: true,
		Credentials:    itCredentials(),
	}
}

// newITStore builds the shipped object store against the fixture's MinIO. It is
// the same constructor the sink factory calls, so a configuration this suite
// cannot build is a configuration the operator cannot build either.
func newITStore(t *testing.T) *Store {
	t.Helper()
	store, err := New(context.Background(), itClientConfig())
	if err != nil {
		t.Fatalf("build the object store for %s: %v", itClientConfig().Endpoint, err)
	}
	return store
}

// bucketFixture is one bucket this suite owns, plus the read-only SDK client it
// is inspected through.
type bucketFixture struct {
	name   string
	locked bool
	client *awss3.Client
}

// itBucketSeq numbers the buckets one run creates, so two fixtures in the same
// test binary cannot collide.
var itBucketSeq int

// newBucket creates a bucket for one test and returns the fixture that reads it.
//
// The name carries a per-run suffix rather than being a fixed constant, which is
// a deliberate departure from the ClickHouse suites' "drop and recreate the
// tables" opening. A bucket with Object Lock enabled is versioned and holds
// retained object versions that cannot be removed without a governance bypass on
// every single one, so "start from an empty well-known bucket" would mean shipping
// that bypass machinery purely to make a second run possible. A fresh bucket per
// test is exact by construction instead: every object in it was written by the
// test that is asserting on it.
//
// locked buckets are created with Object Lock enabled, which also enables
// versioning: the two cannot be separated, and both are prerequisites kuberecord
// documents that it cannot set for an operator (see v1alpha1.S3ObjectLockSpec
// and docs/RETENTION.md). Enabling the lock at creation rather than afterwards is
// this fixture's choice and not S3's requirement — AWS S3 and recent MinIO both
// allow it on an existing versioned bucket, but the MinIO release the Makefile
// pins predates that, and creation-time is the shape every implementation
// supports.
//
// Cleanup is best effort and reported rather than silent (Invariant 4 applied to
// the suite itself): the container `make test-integration` stands up is thrown
// away at the end of the target, so a leftover bucket costs a developer running
// against a persistent MinIO one small bucket, and knowing about it beats a
// cleanup that quietly failed.
func newBucket(ctx context.Context, t *testing.T, locked bool) *bucketFixture {
	t.Helper()

	itBucketSeq++
	name := fmt.Sprintf("kuberecord-it-%d-%d", time.Now().UnixNano(), itBucketSeq)
	client := newReaderClient(ctx, t)

	in := &awss3.CreateBucketInput{Bucket: aws.String(name)}
	if locked {
		in.ObjectLockEnabledForBucket = aws.Bool(true)
	}
	if _, err := client.CreateBucket(ctx, in); err != nil {
		t.Fatalf("create bucket %q (locked=%t) on %s: %v", name, locked, itClientConfig().Endpoint, err)
	}

	fixture := &bucketFixture{name: name, locked: locked, client: client}
	t.Cleanup(func() { fixture.removeQuietly(t) })
	return fixture
}

// newReaderClient builds the SDK client the fixture reads through.
//
// It is assembled here rather than borrowed from Store because Store exposes
// PutObject alone, on purpose: the interface the write path speaks is the
// permission set an operator has to grant, and widening it so a test could list a
// bucket would make that claim untrue. So the read side gets its own client,
// configured identically.
func newReaderClient(ctx context.Context, t *testing.T) *awss3.Client {
	t.Helper()
	cfg := itClientConfig()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(awshttp.NewBuildableClient().Freeze()),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.Credentials.AccessKeyID, cfg.Credentials.SecretAccessKey, "")),
	)
	if err != nil {
		t.Fatalf("resolve the AWS configuration for the fixture's reader: %v", err)
	}
	return awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
}

// storedObject is one object as the fixture reads it back: where it is, what it
// holds, and the retention the store reports for it.
type storedObject struct {
	key  string
	body []byte

	// lockMode and retainUntil are what the store reports for this object's S3
	// Object Lock retention; lockMode is empty when the object carries none.
	lockMode    string
	retainUntil time.Time
}

// keys lists every object in the bucket, in the order the store returns them
// (lexicographic by key, which for this layout is also chronological — see the
// partition layout in internal/sink/s3/encoder.go).
func (b *bucketFixture) keys(ctx context.Context, t *testing.T) []string {
	t.Helper()
	var keys []string
	var token *string
	for {
		page, err := b.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            aws.String(b.name),
			ContinuationToken: token,
		})
		if err != nil {
			t.Fatalf("list bucket %q: %v", b.name, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
		if !aws.ToBool(page.IsTruncated) {
			return keys
		}
		token = page.NextContinuationToken
	}
}

// versionIDsOf lists the version IDs the store holds for exactly one key, plus
// how many delete markers sit on it.
//
// It exists because on a versioned bucket "how many objects are at this key?" and
// "how many versions are at this key?" are different questions with different
// answers, and only the first one is what a reader of the archive sees. keys and
// objects above answer the first (ListObjectsV2 returns current versions); this
// answers the second. A retried PUT is visible only here — see
// TestARetriedObjectOnALockedBucketLeavesOneCurrentVersionIntegration.
//
// The listing is filtered by prefix, which S3 cannot do more precisely than that,
// so the exact-key check is done here: a content-hash key cannot be a prefix of
// another key in this layout, but relying on that would make the helper wrong the
// day the layout gains a suffix.
func (b *bucketFixture) versionIDsOf(ctx context.Context, t *testing.T, key string) (versions []string, deleteMarkers int) {
	t.Helper()
	var keyMarker, versionMarker *string
	for {
		page, err := b.client.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{
			Bucket:          aws.String(b.name),
			Prefix:          aws.String(key),
			KeyMarker:       keyMarker,
			VersionIdMarker: versionMarker,
		})
		if err != nil {
			t.Fatalf("list the versions of %q in bucket %q: %v", key, b.name, err)
		}
		for _, v := range page.Versions {
			if aws.ToString(v.Key) == key {
				versions = append(versions, aws.ToString(v.VersionId))
			}
		}
		for _, m := range page.DeleteMarkers {
			if aws.ToString(m.Key) == key {
				deleteMarkers++
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			return versions, deleteMarkers
		}
		keyMarker, versionMarker = page.NextKeyMarker, page.NextVersionIdMarker
	}
}

// objectVersion reads one named version of one key whole, with the retention the
// store reports for that version. Retention is a property of a version and not
// of a key, so this is the only way to ask about a version that is no longer the
// current one.
func (b *bucketFixture) objectVersion(ctx context.Context, t *testing.T, key, versionID string) storedObject {
	t.Helper()
	out, err := b.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket:    aws.String(b.name),
		Key:       aws.String(key),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		t.Fatalf("get version %q of object %q from bucket %q: %v", versionID, key, b.name, err)
	}
	defer func() {
		if closeErr := out.Body.Close(); closeErr != nil {
			t.Errorf("close the body of version %q of object %q: %v", versionID, key, closeErr)
		}
	}()

	var body bytes.Buffer
	if _, err := body.ReadFrom(out.Body); err != nil {
		t.Fatalf("read version %q of object %q from bucket %q: %v", versionID, key, b.name, err)
	}
	return storedObject{
		key:         key,
		body:        body.Bytes(),
		lockMode:    string(out.ObjectLockMode),
		retainUntil: aws.ToTime(out.ObjectLockRetainUntilDate),
	}
}

// object reads one object whole, with the retention the store reports for it.
func (b *bucketFixture) object(ctx context.Context, t *testing.T, key string) storedObject {
	t.Helper()
	out, err := b.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(b.name),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("get object %q from bucket %q: %v", key, b.name, err)
	}
	defer func() {
		if closeErr := out.Body.Close(); closeErr != nil {
			t.Errorf("close the body of object %q: %v", key, closeErr)
		}
	}()

	var body bytes.Buffer
	if _, err := body.ReadFrom(out.Body); err != nil {
		t.Fatalf("read object %q from bucket %q: %v", key, b.name, err)
	}
	return storedObject{
		key:         key,
		body:        body.Bytes(),
		lockMode:    string(out.ObjectLockMode),
		retainUntil: aws.ToTime(out.ObjectLockRetainUntilDate),
	}
}

// objects reads every object in the bucket.
func (b *bucketFixture) objects(ctx context.Context, t *testing.T) []storedObject {
	t.Helper()
	keys := b.keys(ctx, t)
	out := make([]storedObject, 0, len(keys))
	for _, key := range keys {
		out = append(out, b.object(ctx, t, key))
	}
	return out
}

// recordObjects reads every *record* object in the bucket, leaving out the scope
// log and the health probe's object.
//
// The distinction is the layout's, not this fixture's: records live under
// cluster_id=, the scope log under scopes/, and the probe outside format=jsonl-v1
// altogether (see internal/sink/s3/instance.go). A reader globbing the archive
// makes the same split, which is why it is worth making here rather than
// asserting over whatever happens to be in the bucket.
func (b *bucketFixture) recordObjects(ctx context.Context, t *testing.T) []storedObject {
	t.Helper()
	var out []storedObject
	for _, obj := range b.objects(ctx, t) {
		if isRecordKey(obj.key) {
			out = append(out, obj)
		}
	}
	return out
}

// records decodes every record in the bucket, in key order, through the shipped
// decoder. Anything the writer put there, a reader gets back — that is the claim
// s3.Decode exists to make, and reading the archive back through it is what makes
// this suite's fidelity assertion an end-to-end one.
func (b *bucketFixture) records(ctx context.Context, t *testing.T) []sink.Record {
	t.Helper()
	var out []sink.Record
	for _, obj := range b.recordObjects(ctx, t) {
		decoded, err := kbs3.Decode(obj.body)
		if err != nil {
			t.Fatalf("decode object %q (%d bytes): %v", obj.key, len(obj.body), err)
		}
		out = append(out, decoded...)
	}
	return out
}

// removeQuietly empties the bucket and deletes it.
//
// Governance retention is bypassed on the way, per version, which is what makes
// itLockMode's choice of GOVERNANCE the load-bearing one: this is the code that
// could not clean up after a COMPLIANCE run.
func (b *bucketFixture) removeQuietly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var versions *string
	var keyMarker *string
	for {
		page, err := b.client.ListObjectVersions(ctx, &awss3.ListObjectVersionsInput{
			Bucket:          aws.String(b.name),
			VersionIdMarker: versions,
			KeyMarker:       keyMarker,
		})
		if err != nil {
			t.Logf("cleanup: listing versions of bucket %q: %v", b.name, err)
			return
		}
		for _, v := range page.Versions {
			b.deleteVersionQuietly(ctx, t, aws.ToString(v.Key), aws.ToString(v.VersionId))
		}
		for _, m := range page.DeleteMarkers {
			b.deleteVersionQuietly(ctx, t, aws.ToString(m.Key), aws.ToString(m.VersionId))
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		versions, keyMarker = page.NextVersionIdMarker, page.NextKeyMarker
	}

	if _, err := b.client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(b.name)}); err != nil {
		t.Logf("cleanup: deleting bucket %q: %v", b.name, err)
	}
}

func (b *bucketFixture) deleteVersionQuietly(ctx context.Context, t *testing.T, key, version string) {
	in := &awss3.DeleteObjectInput{
		Bucket:    aws.String(b.name),
		Key:       aws.String(key),
		VersionId: aws.String(version),
	}
	if b.locked {
		in.BypassGovernanceRetention = aws.Bool(true)
	}
	if _, err := b.client.DeleteObject(ctx, in); err != nil {
		t.Logf("cleanup: deleting %q version %q from bucket %q: %v", key, version, b.name, err)
	}
}

// isRecordKey reports whether a key names a record object under the documented
// layout: inside format=jsonl-v1, under this suite's cluster_id= partition, with
// the format's own suffix.
//
// It is spelled out here rather than imported because the layout constants are
// unexported in internal/sink/s3 — and that is the right way round. This suite
// asserts the *published contract* (D15), so it has to state the contract
// independently; a fixture that derived the expected keys from the same constants
// the writer builds them from could not catch a change to them.
func isRecordKey(key string) bool {
	return keyHasSegment(key, "format=jsonl-v1") &&
		keyHasSegment(key, "cluster_id="+itClusterID) &&
		strings.HasSuffix(key, ".jsonl.zst")
}

// keyHasSegment reports whether segment appears as a whole path segment of key.
// Whole segments, not substrings: "cluster_id=demo" must not match a key filed
// under "cluster_id=demo-2".
func keyHasSegment(key, segment string) bool {
	return slices.Contains(strings.Split(key, "/"), segment)
}

// jsonlLines decompresses an object's payload and returns its JSONL lines.
//
// It exists for the scope log, which s3.Decode cannot read: the decoder is typed
// to sink.Record, and a scope line decoded into a Record would succeed while
// filling in almost nothing — a passing assertion about the wrong thing. A
// consumer of this archive reads the scope log as its own line shape (the
// documented DuckDB recipes glob it separately), and so does this suite.
func jsonlLines(t *testing.T, payload []byte) [][]byte {
	t.Helper()
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("open a zstd decoder: %v", err)
	}
	defer dec.Close()

	jsonl, err := dec.DecodeAll(payload, nil)
	if err != nil {
		t.Fatalf("decompress a %d-byte payload: %v", len(payload), err)
	}
	var lines [][]byte
	for line := range bytes.SplitSeq(jsonl, []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			lines = append(lines, line)
		}
	}
	return lines
}

// runWriter starts a Writer, runs body against it, then shuts it down and waits
// for the drain to finish.
//
// Shutdown is part of every test here rather than an afterthought: the drain is
// what writes a partial object, so "the objects this batch produced" is only a
// complete answer once Start has returned. The error Start returns is asserted
// too — it is how a store that could not be closed reaches a test.
func runWriter(t *testing.T, w *kbs3.Writer, body func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()

	body()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("writer Start returned an error: %v", err)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("the writer never finished shutting down")
	}
}

// itRecord builds record i of this suite's corpus: distinct in every field that
// identifies it, stable across calls so a replay is genuinely identical, and
// dated from a fixed epoch in UTC so an object's date/hour partition never
// depends on when the suite ran.
//
// UTC matters for the fidelity assertion as well as for the key: encoding/json
// renders a time.Time as RFC 3339 and parses "Z" back into UTC, so a UTC corpus
// round-trips to a reflect.DeepEqual-identical value where a zoned one would not.
func itRecord(i int) sink.Record {
	return sink.Record{
		Timestamp:       itEpoch.Add(time.Duration(i) * time.Second),
		ClusterID:       itClusterID,
		EventType:       "Snapshot",
		APIGroup:        "apps",
		APIVersion:      "v1",
		Kind:            "Deployment",
		Namespace:       "minio-it",
		Name:            fmt.Sprintf("obj-%04d", i),
		UID:             "uid-" + strconv.Itoa(i),
		ResourceVersion: strconv.Itoa(1000 + i),
		Labels:          map[string]string{"app": fmt.Sprintf("obj-%04d", i)},
		Actors:          []string{"kuberecord-integration"},
		Data:            fmt.Sprintf(`{"kind":"Deployment","metadata":{"name":"obj-%04d"},"filler":%q}`, i, itFiller),
		Diff:            "",
		SHA256:          fmt.Sprintf("%064x", i),
	}
}

// itEpoch is the instant the corpus is dated from. A fixed, non-current instant
// is what lets a key assertion spell out `date=` and `hour=`.
var itEpoch = time.Date(2026, 3, 14, 9, 26, 53, 0, time.UTC)

// itFiller pads each record's data so a record is a realistic few hundred bytes
// rather than a few dozen. It matters for the size-rotation test, which needs a
// single record to be able to fill an object.
const itFiller = "kuberecord-integration-filler-kuberecord-integration-filler-" +
	"kuberecord-integration-filler-kuberecord-integration-filler"

// itRecords builds n records of the corpus.
func itRecords(n int) []sink.Record {
	out := make([]sink.Record, n)
	for i := range n {
		out[i] = itRecord(i)
	}
	return out
}

// commitLog is how every job in this suite settles: one counter per job, so a
// stranded job and a double-settled one are both visible, plus the outcome each
// job saw.
//
// It is a local copy of the instrument the write path's unit tests use, because
// those live in package s3 and are unreachable from here. It is small and it is
// the thing under test at its most important — Invariant 3 is exactly-once
// commits — so restating it beside the assertions costs less than exporting a
// test helper from the package it is testing.
type commitLog struct {
	mu     sync.Mutex
	counts map[int]int
	oks    map[int]bool
}

func newCommitLog() *commitLog {
	return &commitLog{counts: map[int]int{}, oks: map[int]bool{}}
}

// commitFor returns job i's callback. Callbacks fire on writer goroutines, so
// every access to the maps below is guarded.
func (c *commitLog) commitFor(i int) func(bool) {
	return func(ok bool) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.counts[i]++
		c.oks[i] = ok
	}
}

// assertSettledOnce fails unless every one of the first n jobs settled exactly
// once and successfully.
//
// Success is not a parameter because every case in this suite writes to a store
// that is really there: the one failure it arranges is a *lost acknowledgement*,
// after which the retry succeeds and the jobs must still settle true — reporting
// a failed write to the pipeline for an object the bucket holds would be the bug.
// A permanent-failure path exists and is asserted where it belongs, against a
// stand-in store in the write path's own tests.
func (c *commitLog) assertSettledOnce(t *testing.T, n int) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range n {
		switch got := c.counts[i]; {
		case got == 0:
			t.Errorf("job %d never settled; every job must settle exactly once (Invariant 3)", i)
		case got > 1:
			t.Errorf("job %d settled %d times; every job must settle exactly once (Invariant 3)", i, got)
		}
		if !c.oks[i] {
			t.Errorf("job %d settled as a failed write, but its object is in the bucket", i)
		}
	}
	if len(c.counts) != n {
		t.Errorf("%d jobs settled, want %d", len(c.counts), n)
	}
}

// enqueueAll hands every record to the writer, settling each through the log.
func enqueueAll(ctx context.Context, t *testing.T, w *kbs3.Writer, records []sink.Record, log *commitLog) {
	t.Helper()
	for i, record := range records {
		if err := w.Enqueue(ctx, sink.Job{Record: record, Commit: log.commitFor(i)}); err != nil {
			t.Fatalf("enqueue record %d: %v", i, err)
		}
	}
}

// eventually polls until want reports true, or fails the test.
//
// It exists because rotation is time-driven: the age trigger fires on a timer and
// the PUT that follows it is a network round-trip, so "the object appeared" is
// only ever eventually true. The interval is short and the budget generous, so a
// failure here reads as "this never happened" rather than as "this machine was
// busy".
func eventually(t *testing.T, budget time.Duration, what string, want func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		if want() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", budget, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// faultOnce wraps an object store so the first PUT it sees is *forwarded* to the
// real store and then reported as a failure.
//
// That is the lost acknowledgement, modelled honestly: the object really does
// land, and the writer really is told it did not, which is the one case that
// makes this backend's write path at-least-once (see s3.Writer.flush). Anything
// weaker — failing without forwarding — would test the retry loop but not the
// property the content-addressed key exists for, which is that the retry
// *overwrites* rather than duplicating.
//
// It is the same trick the ClickHouse suite's lost-ack test plays with a direct
// re-insert, expressed at the store boundary because a PUT has no partial outcome
// to arrange.
type faultOnce struct {
	inner kbs3.ObjectStore

	mu    sync.Mutex
	puts  int
	faked bool
}

func newFaultOnce(inner kbs3.ObjectStore) *faultOnce {
	return &faultOnce{inner: inner}
}

// errLostAck is what the wrapped store reports for the attempt that actually
// succeeded. It is deliberately not an SDK error: awsstore's classification is
// about real refusals, and this models the case where the response never arrived
// at all.
var errLostAck = errors.New("integration: the acknowledgement of a successful PUT was lost")

func (f *faultOnce) PutObject(ctx context.Context, in kbs3.PutObjectInput) error {
	if err := f.inner.PutObject(ctx, in); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.puts++
	if !f.faked {
		f.faked = true
		return errLostAck
	}
	return nil
}

func (f *faultOnce) Close() error { return f.inner.Close() }

// attempts is how many PUTs reached the real store.
func (f *faultOnce) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts
}

// Compile-time proof that the wrapper is still the interface the writer takes.
var _ kbs3.ObjectStore = (*faultOnce)(nil)

// awsErrorCode and awsErrorMessage report what the store actually said, so a
// classification assertion that fails names the code and the wording it saw
// rather than only the sentinel it did not match. That matters here more than
// usual: refusesObjectLock matches on both, and the whole point of asserting it
// against a real server is to learn when a server says something new.
func awsErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

func awsErrorMessage(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorMessage()
	}
	return ""
}
