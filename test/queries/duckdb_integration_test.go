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

// This file is the S3 half of Task 3.2's discipline, added by Task 7.2: every
// DuckDB recipe kuberecord publishes is executed against a real object store
// holding a real archive, and required to return rows.
//
// It is the counterpart of clickhouse_integration_test.go and makes the same
// argument. A published query is the one part of this project that nothing
// compiles: a renamed field, a mistyped variable, a glob that quietly matches the
// scope log as well as the records — all of them ship silently and surface as an
// empty result in front of the person who trusted the page. Requiring rows rather
// than merely a successful run is what catches the valid-but-useless recipe.
//
// Two things differ from the ClickHouse suite, both because of what this backend
// is rather than how it is tested:
//
//   - The fixture is built with the shipped s3.Encode rather than by driving the
//     live Writer. Encode is the format's reference form (see its doc comment), and
//     it files an object under its *first* record's timestamp — which is what lets
//     this fixture place records in chosen date and hour partitions. A Writer
//     driven for real rotates on size and age against the wall clock, so it can
//     only ever produce the current hour, and the partition-pruning recipe would
//     degenerate into a single-partition query that proves nothing about the layout.
//   - The scope-log object is hand-written from the documented line shape, because
//     no exported encoder for it exists. That is the right way round, and the same
//     choice test/harness/minio.go makes for the key layout: a consumer of a
//     published contract (D15) states it independently, so a change to the writer's
//     own unexported type breaks this test instead of silently redefining what it
//     asserts.
package queries

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/klauspost/compress/zstd"

	"github.com/yelzhy/kuberecord/internal/sink"
	kbs3 "github.com/yelzhy/kuberecord/internal/sink/s3"
	"github.com/yelzhy/kuberecord/internal/sink/s3/awsstore"
)

// The MinIO service `make test-integration` stands up, announced the same way it
// is to the S3 writer's own suite so the two cannot drift onto different
// containers.
const (
	envEndpoint  = "S3_TEST_ENDPOINT"
	envAccessKey = "S3_TEST_ACCESS_KEY_ID"
	envSecretKey = "S3_TEST_SECRET_ACCESS_KEY"
	envDuckDB    = "DUCKDB"

	defaultEndpoint  = "http://127.0.0.1:19100"
	defaultAccessKey = "kuberecord"
	defaultSecretKey = "kuberecord"

	// itRegion is what MinIO ignores and the SDK requires, and is also the
	// S3Sink CRD's default for spec.region.
	itRegion = "us-east-1"
)

// The archive this suite writes and the parameter values that point the published
// recipes at it.
//
// The dates are fixed rather than relative to now, which is the opposite of the
// ClickHouse suite's choice and for a reason that only applies here: a Grafana
// macro expands to a window around `now`, so that fixture has to be dated from
// now to fall inside it, while these recipes take their window from variables this
// file binds. Fixed dates make the partition assertions exact — "the 09 partition
// and the 10 partition, not the 11" is a claim about the fixture, not about the
// hour the suite happens to run in.
const (
	itClusterID = "queries-it-cluster"
	itPrefix    = "audit/kuberecord"

	itDay        = "2026-08-20"
	itWindowFrom = "2026-08-20 09:30:00"
	itWindowTo   = "2026-08-20 10:15:00"

	// The object every timeline recipe reads: the one with history.
	itGroup     = "apps"
	itKind      = "Deployment"
	itNamespace = "demo"
	itName      = "api"
)

// TestPublishedDuckDBRecipesRunAgainstTheArchiveIntegration executes every
// `duckdb` block in docs/QUERIES.md against a real MinIO archive.
//
// The published `duckdb-setup` block runs verbatim as part of the preamble: the
// httpfs load, the credential and the two globs are the document's, not this
// file's, so a setup block that stopped working would fail here rather than in
// front of a reader. Only the values a reader edits — the bucket, the cluster, the
// endpoint, the window — are supplied by the fixture, and the test asserts that
// the set it supplies is exactly the set the published parameters block declares.
func TestPublishedDuckDBRecipesRunAgainstTheArchiveIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	library, err := FromMarkdown(repoPath("docs", "QUERIES.md"))
	if err != nil {
		t.Fatalf("FromMarkdown: %v", err)
	}
	recipes := ByDialect(library, DialectDuckDB)
	if len(recipes) == 0 {
		t.Fatal("docs/QUERIES.md publishes no DuckDB recipes; this suite would pass vacuously")
	}

	bucket := newArchiveBucket(ctx, t)
	seedArchive(ctx, t, bucket)

	duck := DuckDB{
		Binary:   duckDBBinary(t),
		Preamble: fixtureBindings(bucket) + "\n" + publishedSetup(t, library),
	}

	for _, recipe := range recipes {
		t.Run(shortSource(recipe.Source), func(t *testing.T) {
			rows, err := duck.Rows(ctx, recipe.SQL)
			if err != nil {
				t.Fatalf("%s: %v", recipe.Source, err)
			}
			if rows == 0 {
				// The same reasoning as the ClickHouse suite's: a recipe that runs
				// and selects nothing is indistinguishable from a lost archive, which
				// is the failure this page exists to prevent.
				t.Errorf("%s returned no rows against the demo archive:\n\n%s", recipe.Source, recipe.SQL)
			}
		})
	}
}

// publishedSetup returns the document's own `duckdb-setup` block, and fails if
// there is not exactly one. Exactly one, because a second would mean recipes
// below it run under a session the suite assembled rather than the one the page
// tells a reader to assemble.
func publishedSetup(t *testing.T, library []Query) string {
	t.Helper()
	setup := ByDialect(library, DialectDuckDBSetup)
	if len(setup) != 1 {
		t.Fatalf("docs/QUERIES.md publishes %d duckdb-setup blocks, want exactly 1", len(setup))
	}
	return setup[0].SQL
}

// fixtureBindings is the test's half of the preamble: a value for every variable
// the published parameters block declares, pointing at this suite's own archive.
//
// TestFixtureBindingsCoverThePublishedParameters keeps it honest in both
// directions — a variable added to the document with no binding here, and a
// binding here for a variable the document no longer declares.
func fixtureBindings(bucket *archiveBucket) string {
	endpoint, useSSL := endpointForDuckDB(clientConfig().Endpoint)
	return strings.Join([]string{
		fmt.Sprintf("SET VARIABLE archive = 's3://%s/%s';", bucket.name, itPrefix),
		fmt.Sprintf("SET VARIABLE cluster = '%s';", itClusterID),
		fmt.Sprintf("SET VARIABLE endpoint = '%s';", endpoint),
		fmt.Sprintf("SET VARIABLE region = '%s';", itRegion),
		// Path-style addressing is not optional against MinIO: a bucket-as-subdomain
		// URL only resolves where DNS covers *.<endpoint>, which it does not for a
		// container on 127.0.0.1.
		"SET VARIABLE url_style = 'path';",
		fmt.Sprintf("SET VARIABLE use_ssl = %t;", useSSL),
		fmt.Sprintf("SET VARIABLE key_id = '%s';", credentialsFromEnv().AccessKeyID),
		fmt.Sprintf("SET VARIABLE secret = '%s';", credentialsFromEnv().SecretAccessKey),
		fmt.Sprintf("SET VARIABLE day = '%s';", itDay),
		fmt.Sprintf("SET VARIABLE window_from = '%s';", itWindowFrom),
		fmt.Sprintf("SET VARIABLE window_to = '%s';", itWindowTo),
		fmt.Sprintf("SET VARIABLE \"group\" = '%s';", itGroup),
		fmt.Sprintf("SET VARIABLE kind = '%s';", itKind),
		fmt.Sprintf("SET VARIABLE namespace = '%s';", itNamespace),
		fmt.Sprintf("SET VARIABLE name = '%s';", itName),
	}, "\n")
}

// endpointForDuckDB splits an endpoint URL into the host:port DuckDB's S3 secret
// wants and the USE_SSL flag implied by its scheme. DuckDB takes the two
// separately, so an endpoint carrying its scheme would be read as a hostname.
func endpointForDuckDB(endpoint string) (string, bool) {
	if rest, ok := strings.CutPrefix(endpoint, "https://"); ok {
		return rest, true
	}
	return strings.TrimPrefix(endpoint, "http://"), false
}

// duckDBBinary resolves the duckdb CLI.
//
// A missing binary fails rather than skipping. A skip here would be the silent
// channel this project has already been bitten by: the suite would report success
// on a machine where not one recipe had been executed.
func duckDBBinary(t *testing.T) string {
	t.Helper()
	binary := envOr(envDuckDB, repoPath("bin", "duckdb"))
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("the duckdb CLI is not at %q (%v); run this suite through `make test-integration`, "+
			"which bootstraps it into bin/, or point $%s at one", binary, err, envDuckDB)
	}
	return binary
}

// ---------------------------------------------------------------------------
// The archive fixture
// ---------------------------------------------------------------------------

// archiveBucket is the bucket one run owns, plus the client that fills and
// removes it.
//
// The client is the fixture's own rather than the sink's: s3.ObjectStore exposes
// PutObject alone, deliberately, because that interface is the permission set an
// operator has to grant. Creating and emptying a bucket is a consumer's right, not
// the sink's, so it gets a client of its own — the same split awsstore's fixture
// makes for its reader.
type archiveBucket struct {
	name   string
	client *awss3.Client
	store  *awsstore.Store
}

func credentialsFromEnv() kbs3.Credentials {
	return kbs3.Credentials{
		AccessKeyID:     envOr(envAccessKey, defaultAccessKey),
		SecretAccessKey: envOr(envSecretKey, defaultSecretKey),
	}
}

func clientConfig() kbs3.ClientConfig {
	return kbs3.ClientConfig{
		Region:         itRegion,
		Endpoint:       envOr(envEndpoint, defaultEndpoint),
		ForcePathStyle: true,
		Credentials:    credentialsFromEnv(),
	}
}

// newArchiveBucket creates this run's bucket and the two clients that write it.
//
// A per-run name rather than a fixed one: `go test` may be running this package
// beside others against the same MinIO, and a shared bucket would make "the
// archive holds exactly these objects" a claim about whoever ran last.
func newArchiveBucket(ctx context.Context, t *testing.T) *archiveBucket {
	t.Helper()

	cfg := clientConfig()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(awshttp.NewBuildableClient().Freeze()),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.Credentials.AccessKeyID, cfg.Credentials.SecretAccessKey, "")),
	)
	if err != nil {
		t.Fatalf("resolve the AWS configuration for the fixture: %v", err)
	}
	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
		o.BaseEndpoint = aws.String(cfg.Endpoint)
	})

	name := fmt.Sprintf("kuberecord-queries-it-%d", time.Now().UnixNano())
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("create bucket %q on %s: %v", name, cfg.Endpoint, err)
	}

	// The objects themselves go through the shipped store, so what DuckDB reads
	// was uploaded by the code the operator runs.
	store, err := awsstore.New(ctx, cfg)
	if err != nil {
		t.Fatalf("build the shipped object store for %s: %v", cfg.Endpoint, err)
	}

	bucket := &archiveBucket{name: name, client: client, store: store}
	t.Cleanup(func() { bucket.removeQuietly(t) })
	return bucket
}

// put writes one object through the shipped store.
func (b *archiveBucket) put(ctx context.Context, t *testing.T, key string, body []byte) {
	t.Helper()
	if err := b.store.PutObject(ctx, kbs3.PutObjectInput{Bucket: b.name, Key: key, Body: body}); err != nil {
		t.Fatalf("put %q into bucket %q: %v", key, b.name, err)
	}
}

// removeQuietly empties and deletes the bucket. Best effort and reported rather
// than silent (Invariant 4 applied to the suite itself): the container is thrown
// away with the target, so a leftover object costs a developer running against a
// persistent MinIO one small bucket, and knowing beats a cleanup that failed
// quietly.
func (b *archiveBucket) removeQuietly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var token *string
	for {
		page, err := b.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            aws.String(b.name),
			ContinuationToken: token,
		})
		if err != nil {
			t.Logf("cleanup: listing bucket %q: %v", b.name, err)
			return
		}
		for _, obj := range page.Contents {
			if _, err := b.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
				Bucket: aws.String(b.name), Key: obj.Key,
			}); err != nil {
				t.Logf("cleanup: deleting %q: %v", aws.ToString(obj.Key), err)
			}
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		token = page.NextContinuationToken
	}
	if _, err := b.client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(b.name)}); err != nil {
		t.Logf("cleanup: deleting bucket %q: %v", b.name, err)
	}
	if err := b.store.Close(); err != nil {
		t.Logf("cleanup: closing the object store: %v", err)
	}
}

// seedArchive writes a small cluster's worth of history, shaped like an archive a
// Writer-only sink actually produces: first sightings are `Snapshot` and never
// `Added`, changes are `Modified` with a patch, one object was deleted while the
// operator was watching, and no `Checkpoint` appears anywhere (an S3Sink has no
// checkpointEvery). Getting that shape right is what makes the recipes' event-type
// filters meaningful rather than decorative.
//
// Records are grouped into objects by the partition they belong to, because that
// is what the writer does: one object holds records from one cluster, date and
// hour, keyed by its first record's timestamp.
func seedArchive(ctx context.Context, t *testing.T, bucket *archiveBucket) {
	t.Helper()

	const (
		apiUID    = "11111111-1111-1111-1111-111111111111"
		cmUID     = "22222222-2222-2222-2222-222222222222"
		dnsUID    = "33333333-3333-3333-3333-333333333333"
		roleUID   = "44444444-4444-4444-4444-444444444444"
		legacyUID = "55555555-5555-5555-5555-555555555555"

		gitopsMgr = "argocd-controller"
		humanMgr  = "kubectl-edit"
		statusMgr = "kube-controller-manager"

		deploymentState = `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api"},"spec":{"replicas":2}}`
		scaleUp         = `[{"op":"replace","path":"/spec/replicas","value":3}]`
	)

	appLabels := map[string]string{"app": "api", "tier": "backend"}

	// Every group is one object. The partition an object lands in is derived from
	// its first record, so the groups are also the assertions the partition
	// recipes make: the 23:00 object of the previous day is what the window's
	// one-hour widening must not pull in, and the 11:00 object is what a window
	// ending at 10:15 must exclude.
	groups := [][]sink.Record{
		{ // 2026-08-19, hour=23 — the day before, so a date filter has something to exclude.
			record(t, "2026-08-19T23:50:00.123456789Z", "Snapshot", "apps", "Deployment",
				"demo", "api", apiUID, "90", appLabels, []string{gitopsMgr}, deploymentState, "", "sha-api-prev"),
		},
		{ // 2026-08-20, hour=09
			record(t, "2026-08-20T09:15:00Z", "Snapshot", "apps", "Deployment",
				"demo", "api", apiUID, "100", appLabels, []string{gitopsMgr}, deploymentState, "", "sha-api-0"),
			record(t, "2026-08-20T09:20:00Z", "Snapshot", "", "ConfigMap",
				"demo", "settings", cmUID, "200", nil, []string{humanMgr},
				`{"apiVersion":"v1","kind":"ConfigMap","data":{"k":"v"}}`, "", "sha-cm-0"),
			record(t, "2026-08-20T09:45:00Z", "Modified", "apps", "Deployment",
				"demo", "api", apiUID, "110", appLabels, []string{humanMgr, statusMgr}, "", scaleUp, "sha-api-1"),
		},
		{ // 2026-08-20, hour=10
			record(t, "2026-08-20T10:05:00Z", "Modified", "apps", "Deployment",
				"demo", "api", apiUID, "120", appLabels, []string{gitopsMgr}, "", scaleUp, "sha-api-2"),
			record(t, "2026-08-20T10:07:00Z", "Snapshot", "apps", "Deployment",
				"kube-system", "coredns", dnsUID, "300", nil, []string{statusMgr},
				`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"coredns"}}`, "", "sha-dns-0"),
			// A cluster-scoped object, so the '' namespace has a representative and
			// the coalesce every namespace recipe applies is exercised.
			record(t, "2026-08-20T10:09:00Z", "Snapshot", "rbac.authorization.k8s.io", "ClusterRole",
				"", "viewer", roleUID, "400", nil, []string{gitopsMgr},
				`{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole","metadata":{"name":"viewer"}}`,
				"", "sha-role-0"),
		},
		{ // 2026-08-20, hour=11 — outside the window every window recipe binds.
			// A live-observed deletion: the only kind this backend ever records.
			// Empty data, diff, sha256 and actors, exactly as the contract specifies.
			record(t, "2026-08-20T11:30:00Z", "Deleted", "apps", "Deployment",
				"demo", "legacy", legacyUID, "500", nil, nil, "", "", ""),
		},
	}

	for _, group := range groups {
		object, err := kbs3.Encode(itPrefix, group)
		if err != nil {
			t.Fatalf("encode an archive object: %v", err)
		}
		bucket.put(ctx, t, object.Key, object.Payload)
	}

	seedScopeLog(ctx, t, bucket)
	seedProbeObject(ctx, t, bucket)
}

// record builds one sink.Record with a parsed timestamp, so the fixture reads as
// the instants it means rather than as time arithmetic.
//
// api_version is not a parameter: every object here is served at v1, and the
// contract makes that field provenance rather than identity (identity is
// version-agnostic, Invariant 7), so nothing a recipe asks depends on varying it.
func record(t *testing.T, ts, eventType, group, kind, namespace, name, uid, rv string,
	labels map[string]string, actors []string, data, diff, sha string) sink.Record {
	t.Helper()
	at, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Fatalf("parse fixture timestamp %q: %v", ts, err)
	}
	if labels == nil {
		labels = map[string]string{}
	}
	if actors == nil {
		actors = []string{}
	}
	return sink.Record{
		Timestamp: at, ClusterID: itClusterID, EventType: eventType,
		APIGroup: group, APIVersion: "v1", Kind: kind, Namespace: namespace, Name: name,
		UID: uid, ResourceVersion: rv, Labels: labels, Actors: actors,
		Data: data, Diff: diff, SHA256: sha,
	}
}

// seedScopeLog writes one scope-log object: the archive's answer to "was anybody
// watching?", and the object that makes the records glob's exclusion of `scopes/`
// a tested claim rather than a convention. A recipe globbing
// `format=jsonl-v1/**` instead of `format=jsonl-v1/cluster_id=*/**` reads these
// lines too, and infers its schema from the union of two unrelated shapes.
//
// The lines are written here from the documented shape rather than through the
// writer, which has no exported scope encoder — see this file's header for why
// that is the right way round.
func seedScopeLog(ctx context.Context, t *testing.T, bucket *archiveBucket) {
	t.Helper()

	type scopeLine struct {
		TS         time.Time `json:"ts"`
		ClusterID  string    `json:"cluster_id"`
		APIGroup   string    `json:"group"`
		APIVersion string    `json:"version"`
		Kind       string    `json:"kind"`
		Namespace  string    `json:"namespace"`
		Action     string    `json:"action"`
		RuleRef    string    `json:"rule_ref"`
	}

	at := func(ts string) time.Time {
		t.Helper()
		parsed, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			t.Fatalf("parse fixture timestamp %q: %v", ts, err)
		}
		return parsed
	}

	lines := []scopeLine{
		{at("2026-08-20T09:00:00Z"), itClusterID, "apps", "v1", "Deployment", "demo",
			"Started", "streamrule/demo/audit"},
		{at("2026-08-20T09:00:01Z"), itClusterID, "", "v1", "ConfigMap", "demo",
			"Started", "streamrule/demo/audit"},
		// An epoch that closed, so the log holds both actions and a reader can tell
		// a closed epoch from one left open.
		{at("2026-08-20T12:00:00Z"), itClusterID, "", "v1", "ConfigMap", "demo",
			"Stopped", "streamrule/demo/audit"},
	}

	var jsonl bytes.Buffer
	enc := json.NewEncoder(&jsonl)
	for _, line := range lines {
		if err := enc.Encode(line); err != nil {
			t.Fatalf("encode a scope line: %v", err)
		}
	}

	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("build a zstd encoder: %v", err)
	}
	defer func() {
		if closeErr := encoder.Close(); closeErr != nil {
			t.Errorf("close the zstd encoder: %v", closeErr)
		}
	}()

	key := fmt.Sprintf("%s/format=jsonl-v1/scopes/date=%s/%s.jsonl.zst", itPrefix, itDay, "fixture")
	bucket.put(ctx, t, key, encoder.EncodeAll(jsonl.Bytes(), nil))
}

// seedProbeObject writes the object the sink's health probe leaves behind. It is
// the one thing in a real bucket that is not audit data, so an archive without it
// would let a recipe that globbed too widely pass.
func seedProbeObject(ctx context.Context, t *testing.T, bucket *archiveBucket) {
	t.Helper()
	bucket.put(ctx, t, itPrefix+"/.kuberecord-probe",
		[]byte(`{"probe":"kuberecord","purpose":"verifies this sink can write to this bucket"}`+"\n"))
}

// TestFixtureBindingsCoverThePublishedParametersIntegration keeps the two halves
// of the preamble in step: every variable docs/QUERIES.md tells a reader to set is
// bound by this suite, and this suite binds nothing the document does not declare.
//
// Without it the coupling degrades in the direction that fails open — a new
// variable with no binding reads as NULL, so the recipe using it filters on
// nothing and the only symptom is a result set that happens to still be non-empty.
func TestFixtureBindingsCoverThePublishedParametersIntegration(t *testing.T) {
	library, err := FromMarkdown(repoPath("docs", "QUERIES.md"))
	if err != nil {
		t.Fatalf("FromMarkdown: %v", err)
	}
	blocks := ByDialect(library, DialectDuckDBParameters)
	if len(blocks) != 1 {
		t.Fatalf("docs/QUERIES.md publishes %d duckdb-parameters blocks, want exactly 1", len(blocks))
	}

	published := DeclaredVariables(blocks[0].SQL)
	bound := DeclaredVariables(fixtureBindings(&archiveBucket{name: "fixture"}))
	if !slices.Equal(published, bound) {
		t.Errorf("the published parameters and the fixture bindings disagree\npublished: %v\n   bound: %v",
			published, bound)
	}
}

// envOr returns the environment value for key, or def when it is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
