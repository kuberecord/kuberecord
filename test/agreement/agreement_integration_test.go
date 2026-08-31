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

// The authoritative cross-backend agreement run: one declarative corpus, both real
// backends, the same questions, the answers required to match.
//
// # What this adds to everything that already passes
//
// Both query backends pass the read-plane conformance suite and both pass it
// legitimately. But each is seeded by its own harness, from its own fixture code,
// into its own storage shape — rows in a table on one side, compressed JSONL objects
// on the other. What that certifies is that each engine is internally consistent
// with what its own harness wrote. Nothing in it compares the two.
//
// That gap sits directly under the properties Phases 9 and 10 were built on.
// Resolving the incarnation before the predicates are applied, and settling a
// short-circuited walk on the UID a full scan would have picked, are correctness
// arguments about *reading a given history*. A divergence — the two resolving a
// different incarnation, ordering nanosecond-adjacent changes differently,
// disagreeing about which row is the reconstruction base — would be invisible today,
// because no test gives both the same history and compares the answers.
//
// # Why it lives here rather than under internal/
//
// Seeding ClickHouse needs the driver; seeding an object store needs the S3 SDK. Two
// depguard rules confine each of those to two packages apiece, over the whole of
// internal/ and over test files as well as sources, and no package is on both lists —
// deliberately, since a query backend that could reach the other's client would stop
// being independently testable. Both rules scope themselves to internal/ precisely so
// that an integration harness can dial a real server, which is what test/queries
// already does with both clients. This is that, for two backends at once.
//
// # Why it is not enough on its own, and what is
//
// A faster variant runs on every commit: internal/query/clickhouse/agreement_test.go
// pairs a stand-in connection with a directory, and proves the two engines' Go logic
// agrees. It is explicitly not authoritative — its ClickHouse side never executes
// SQL — and this run is why that is acceptable rather than a gap.
//
// # Runtime
//
// The corpus is ten records and four scope transitions, sized for coverage of the
// cases where divergence is plausible and not for volume. Seeding costs one batch
// insert against the table and eleven small PUTs against the bucket; the whole run is
// a few seconds against the containers `make test-integration` stands up.

package agreement

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	schemaddl "github.com/kuberecord/kuberecord/deploy/clickhouse/schema"
	chquery "github.com/kuberecord/kuberecord/internal/query/clickhouse"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
	"github.com/kuberecord/kuberecord/internal/query/objectsource"
	"github.com/kuberecord/kuberecord/internal/query/objectsource/archivetest"
	"github.com/kuberecord/kuberecord/internal/query/objectsource/awssource"
)

// TestQueryBackendsAgreeIntegration is the run that counts.
func TestQueryBackendsAgreeIntegration(t *testing.T) {
	ctx := t.Context()
	conformance.RunAgreementSuite(t, newTableHarness(ctx, t), newArchiveHarness(ctx, t))
}

// ---------------------------------------------------------------------------
// The indexed backend
// ---------------------------------------------------------------------------

// agreementDatabase is this suite's own database.
//
// Its own, and not the one the other integration suites use: `go test` runs package
// binaries concurrently, and a suite that truncates resource_states while another
// drops and recreates it would delete the other's fixtures nondeterministically. The
// failure would look like a backend bug rather than like the scheduling accident it
// is, which for a suite whose whole output is "these two backends disagree" would be
// an expensive hour.
const agreementDatabase = "kuberecord_agreement_it"

// The INSERT statements the corpus is seeded through: the frozen schema's own column
// order, written out because this file is a writer against a schema it otherwise only
// reads, and a column list that drifted from the DDL would shift every value one
// place with nothing to catch it but a wrong answer.
const (
	insertStates = `INSERT INTO resource_states (
        ts, cluster_id, event_type, api_group, api_version, kind, namespace, name, uid,
        resource_version, labels, actors, data, diff, sha256
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	insertScopes = `INSERT INTO watch_scopes (
        ts, cluster_id, api_group, api_version, kind, namespace, action, rule_ref
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
)

// newTableHarness is the indexed backend over a real server.
//
// Its capability declaration is written by hand here rather than borrowed from that
// backend's own suite, which is the discipline every harness in this repository
// follows: two declarations agreeing because they read one literal would not notice
// the day the engine's report changed. TimeBoundRequired is absent, and the absence is
// the statement — one object's history is a range read against the sort key rather
// than a scan of the table.
//
// No Seed and no stream fault: the agreement suite plants the corpus and asks
// questions, and a harness carrying levers it never uses is a harness whose unused
// levers nobody notices going wrong.
func newTableHarness(ctx context.Context, t *testing.T) conformance.Harness {
	t.Helper()

	conn := openServer(ctx, t)
	engine, err := chquery.New(conn)
	if err != nil {
		t.Fatalf("building an engine over the integration connection: %v", err)
	}
	return conformance.Harness{
		Engine:     engine,
		SeedCorpus: func(c conformance.Corpus) error { return seedServer(ctx, conn, c) },
		Capabilities: conformance.DeclareCapabilities(
			conformance.CapDeletions,
			conformance.CapServerSideFilter,
			conformance.CapPointQuery,
		),
	}
}

// openServer dials the dockerized server, gives this suite a private database, and
// applies the shipped DDL into it.
func openServer(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()

	bootstrap, err := chdriver.Open(serverOptions(envOr("CH_TEST_DB", "default")))
	if err != nil {
		t.Fatalf("opening a bootstrap connection: %v", err)
	}
	for _, statement := range []string{
		"DROP DATABASE IF EXISTS " + agreementDatabase,
		"CREATE DATABASE " + agreementDatabase,
	} {
		if err := bootstrap.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := bootstrap.Close(); err != nil {
		t.Fatalf("closing the bootstrap connection: %v", err)
	}

	conn, err := chdriver.Open(serverOptions(agreementDatabase))
	if err != nil {
		t.Fatalf("opening a connection to %s: %v", agreementDatabase, err)
	}
	t.Cleanup(func() {
		// Best effort on the way out: the dockerized target is discarded anyway, and
		// a persistent one is left clean for the next run.
		if err := conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+agreementDatabase); err != nil {
			t.Logf("dropping %s: %v", agreementDatabase, err)
		}
		if err := conn.Close(); err != nil {
			t.Logf("closing the connection: %v", err)
		}
	})

	applyShippedDDL(ctx, t, conn)
	return conn
}

// serverOptions describes the connection this suite wants to one database.
func serverOptions(database string) *chdriver.Options {
	return &chdriver.Options{
		Addr: []string{envOr("CH_TEST_ADDR", "127.0.0.1:9000")},
		Auth: chdriver.Auth{
			Database: database,
			Username: envOr("CH_TEST_USER", "default"),
			Password: os.Getenv("CH_TEST_PASSWORD"),
		},
		Protocol:    chdriver.Native,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second,
	}
}

// applyShippedDDL executes the embedded DDL files in filename order.
//
// The files are the ones frozen as a public API in Task 2.6, read here rather than
// restated: an agreement run against a schema written out in its own test file would
// be an agreement between two engines and their author's memory of the schema.
func applyShippedDDL(ctx context.Context, t *testing.T, conn driver.Conn) {
	t.Helper()

	entries, err := schemaddl.FS.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the shipped DDL: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	slices.Sort(names)

	for _, name := range names {
		ddl, readErr := schemaddl.FS.ReadFile(name)
		if readErr != nil {
			t.Fatalf("reading %s: %v", name, readErr)
		}
		// The native protocol executes one statement per Exec; trimming a trailing
		// ';' avoids an empty second statement being parsed.
		if err := conn.Exec(ctx, strings.TrimRight(strings.TrimSpace(string(ddl)), ";")); err != nil {
			t.Fatalf("executing %s: %v", name, err)
		}
	}
}

// seedServer makes the corpus the server's recorded past, as rows in the shipped
// tables.
//
// The corpus's flush labels carry nothing this backend stores — one recorded change
// is one row whether or not it shared a flush with the next — so the row rendering is
// the whole of the translation, and inventing a distinction the table does not have
// would be seeding a shape no operator produces.
//
// ts is bound as a time.Time and never as a formatted datetime string: the driver
// parses such a string client-side and reinterprets its digits in time.Local, so a
// machine outside UTC would seed the corpus shifted by its own offset and the
// nanosecond-adjacent pair would disagree for a reason that has nothing to do with
// either backend.
func seedServer(ctx context.Context, conn driver.Conn, corpus conformance.Corpus) error {
	states, err := conn.PrepareBatch(ctx, insertStates)
	if err != nil {
		return fmt.Errorf("preparing a resource_states batch: %w", err)
	}
	for _, r := range corpus.Rows() {
		labels := r.Change.Labels
		if labels == nil {
			labels = map[string]string{}
		}
		actors := r.Change.Actors
		if actors == nil {
			actors = []string{}
		}
		appendErr := states.Append(
			r.Change.TS.UTC(), r.Ref.ClusterID, r.Change.EventType, r.Ref.APIGroup, r.Change.APIVersion,
			r.Ref.Kind, r.Ref.Namespace, r.Ref.Name, r.Change.UID, r.Change.ResourceVersion,
			labels, actors, r.Change.Data, r.Change.Diff, r.Change.SHA256,
		)
		if appendErr != nil {
			return fmt.Errorf("appending a resource_states row: %w", appendErr)
		}
	}
	if err := states.Send(); err != nil {
		return fmt.Errorf("sending the resource_states batch: %w", err)
	}

	scopes, err := conn.PrepareBatch(ctx, insertScopes)
	if err != nil {
		return fmt.Errorf("preparing a watch_scopes batch: %w", err)
	}
	for _, s := range corpus.Scopes {
		// The transitions carry no cluster of their own, so the harness stamps the
		// corpus's — see conformance.FixtureClusterID. api_version is provenance and
		// never identity, so any recorded value is truthful.
		appendErr := scopes.Append(
			s.TS.UTC(), conformance.FixtureClusterID, s.APIGroup, "v1",
			s.Kind, s.Namespace, string(s.Action), s.RuleRef,
		)
		if appendErr != nil {
			return fmt.Errorf("appending a watch_scopes row: %w", appendErr)
		}
	}
	if err := scopes.Send(); err != nil {
		return fmt.Errorf("sending the watch_scopes batch: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// The archive backend
// ---------------------------------------------------------------------------

// The MinIO service `make test-integration` stands up, announced through the same
// names and defaults every other suite here uses, so one container serves them all
// and a developer configures one thing.
const (
	envEndpoint  = "S3_TEST_ENDPOINT"
	envAccessKey = "S3_TEST_ACCESS_KEY_ID"
	envSecretKey = "S3_TEST_SECRET_ACCESS_KEY"

	defaultEndpoint  = "http://127.0.0.1:19100"
	defaultAccessKey = "kuberecord"
	defaultSecretKey = "kuberecord"

	// storeRegion is the region every client here is built with. MinIO ignores it;
	// the SDK requires one.
	storeRegion = "us-east-1"

	// archivePrefix is the prefix the corpus archive is written under. Non-empty
	// deliberately: an empty prefix is the simpler configuration and the one a bug in
	// prefix handling would survive.
	archivePrefix = "audit"
)

// newArchiveHarness is the archive backend over a real object store.
//
// TimeBoundRequired is the only capability declared, and the three absences are each a
// statement. Deletions: this archive never receives one (D12), which is the difference
// the agreement suite spends most of its capability reasoning on. ServerSideFilter:
// there is nothing behind the seam to push a predicate into. PointQuery: there is no
// index, so one object's history costs the partitions its window lands in.
func newArchiveHarness(ctx context.Context, t *testing.T) conformance.Harness {
	t.Helper()

	bucket := newBucket(ctx, t)
	client := storeClient(ctx, t)

	source, err := awssource.New(ctx, awssource.Config{
		Bucket:   bucket,
		Region:   storeRegion,
		Endpoint: envOr(envEndpoint, defaultEndpoint),
		// Path style is not optional against this fixture: a bucket-as-subdomain URL
		// only resolves where DNS covers *.<endpoint>, which it does not for a
		// container on 127.0.0.1.
		ForcePathStyle: true,
		Credentials: awssource.Credentials{
			AccessKeyID:     envOr(envAccessKey, defaultAccessKey),
			SecretAccessKey: envOr(envSecretKey, defaultSecretKey),
		},
	})
	if err != nil {
		t.Fatalf("building a source for bucket %q: %v", bucket, err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("closing the source: %v", err)
		}
	})

	engine, err := objectsource.NewEngine(source, objectsource.Options{Prefix: archivePrefix})
	if err != nil {
		t.Fatalf("building an engine over bucket %q: %v", bucket, err)
	}

	return conformance.Harness{
		Engine: engine,
		SeedCorpus: func(c conformance.Corpus) error {
			// One object per flush, real keys, real lines, real frames: the objects a
			// writer would have produced, which is what makes the comparison a
			// comparison about this format rather than about a shape built for a test.
			_, writeErr := archivetest.WriteCorpus(func(key string, body []byte) error {
				_, putErr := client.PutObject(ctx, &awss3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Body:   bytes.NewReader(body),
				})
				return putErr
			}, archivePrefix, c)
			return writeErr
		},
		Capabilities: conformance.DeclareCapabilities(conformance.CapTimeBoundRequired),
	}
}

// storeClient builds the raw SDK client the fixture writes its archive with.
//
// The source under test cannot seed its own fixture: it lists and fetches, which is
// the whole of what reading an archive needs, and widening it so a test could write
// one would make that claim untrue.
func storeClient(ctx context.Context, t *testing.T) *awss3.Client {
	t.Helper()

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(storeRegion),
		awsconfig.WithHTTPClient(awshttp.NewBuildableClient().Freeze()),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			envOr(envAccessKey, defaultAccessKey), envOr(envSecretKey, defaultSecretKey), "")),
	)
	if err != nil {
		t.Fatalf("resolving the AWS configuration for the fixture: %v", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(envOr(envEndpoint, defaultEndpoint))
	})
}

// newBucket creates a bucket for this run and empties it on the way out.
//
// A fresh bucket rather than a well-known one that is emptied: every object in it was
// written by the run asserting on it, which for a suite whose finding would be "these
// two backends disagree" is worth the CreateBucket call.
func newBucket(ctx context.Context, t *testing.T) string {
	t.Helper()

	name := fmt.Sprintf("kuberecord-agreement-it-%d", time.Now().UnixNano())
	client := storeClient(ctx, t)
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("creating bucket %q on %s: %v", name, envOr(envEndpoint, defaultEndpoint), err)
	}
	t.Cleanup(func() { emptyBucket(t, client, name) })
	return name
}

// emptyBucket deletes a fixture bucket and everything in it, reporting what it could
// not remove instead of swallowing it.
func emptyBucket(t *testing.T, client *awss3.Client, bucket string) {
	t.Helper()

	ctx := context.Background()
	pages := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			t.Logf("cleanup: listing bucket %q: %v", bucket, err)
			return
		}
		for _, entry := range page.Contents {
			_, err := client.DeleteObject(ctx, &awss3.DeleteObjectInput{
				Bucket: aws.String(bucket), Key: entry.Key,
			})
			if err != nil {
				t.Logf("cleanup: deleting %q: %v", aws.ToString(entry.Key), err)
			}
		}
	}
	if _, err := client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Logf("cleanup: deleting bucket %q: %v", bucket, err)
	}
}

// envOr returns the environment value for key, or def when it is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
