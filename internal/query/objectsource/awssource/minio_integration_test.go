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

// This suite runs the shipped read source against a real object store, and it
// exists for one assertion in particular: that the local source is a faithful
// stand-in for a bucket.
//
// Everything built on this seam is tested against a directory. That is only sound
// if a directory answers the same questions a bucket does — same order, same
// meaning for a prefix, same story for an object that is not there — and no fake
// can vouch for it, because a fake is written from the same reading of S3's
// behaviour as the local source is. So the same key set is written to both, both
// are listed through the same interface, and the two listings are compared.
//
// The rest is what only a real store can show: that no delimiter really does give a
// flat listing, that MinIO's ordering is the byte ordering the contract promises,
// and that a credential which cannot read is reported as a refusal rather than as
// an empty archive.
package awssource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/kuberecord/kuberecord/internal/query/objectsource"
)

// The MinIO service `make test-integration` stands up, and the environment it is
// announced through. They are the same names and defaults the write path's suite
// uses, so one container serves both and a developer configures one thing.
const (
	envEndpoint  = "S3_TEST_ENDPOINT"
	envAccessKey = "S3_TEST_ACCESS_KEY_ID"
	envSecretKey = "S3_TEST_SECRET_ACCESS_KEY"

	defaultEndpoint  = "http://127.0.0.1:19100"
	defaultAccessKey = "kuberecord"
	defaultSecretKey = "kuberecord"

	// itRegion is the region every client here is built with. MinIO ignores it; the
	// SDK requires one.
	itRegion = "us-east-1"
)

// itKeys is the archive both sources are given.
//
// The first six are the layout the writer produces, across two clusters, two dates
// and two hours — which is what makes "list one hour" a question with a wrong answer
// available. The last two are the ordering trap: 'audit-2' and 'audit0' bracket
// 'audit/' in byte order, because '-' is 0x2D, '/' is 0x2F and '0' is 0x30. A
// listing sorted by path component rather than by whole key puts them in the wrong
// place, and this is the case that says so.
var itKeys = []string{
	"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=07/aaaa.jsonl.zst",
	"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=07/bbbb.jsonl.zst",
	"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=08/cccc.jsonl.zst",
	"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-15/hour=00/dddd.jsonl.zst",
	"audit/format=jsonl-v1/cluster_id=staging/date=2026-03-14/hour=07/eeee.jsonl.zst",
	"audit/format=jsonl-v1/scopes/date=2026-03-14/ffff.jsonl.zst",
	"audit-2/sibling-before.jsonl.zst",
	"audit0/sibling-after.jsonl.zst",
}

// itPrefixes are the questions both sources are asked. Each one is a shape the
// engine's partition pruning actually produces.
var itPrefixes = []string{
	"",
	"audit/",
	"audit",
	"audit/format=jsonl-v1/cluster_id=prod-eu-1/",
	"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=07/",
	"audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-1",
	"audit/format=jsonl-v1/scopes/",
	"audit/format=jsonl-v1/cluster_id=nowhere/",
}

// TestIntegrationObjectSourceAgainstMinIO runs the shipped source against a real
// bucket and against a directory holding the same archive.
func TestIntegrationObjectSourceAgainstMinIO(t *testing.T) {
	ctx := t.Context()
	bucket := newITBucket(ctx, t)
	seedBucket(ctx, t, bucket)

	src, err := New(ctx, itConfig(bucket, itSecretKey()))
	if err != nil {
		t.Fatalf("build the source for bucket %q: %v", bucket, err)
	}
	t.Cleanup(func() { _ = src.Close() })

	local := seedDirectory(t)

	t.Run("a bucket and a directory answer identically", func(t *testing.T) {
		// The assertion the whole test strategy rests on. If these ever diverge, every
		// test written against the local source stops saying anything about a bucket.
		for _, prefix := range itPrefixes {
			remote := drain(ctx, t, src, prefix)
			onDisk := drain(ctx, t, local, prefix)
			if !slices.Equal(remote, onDisk) {
				t.Errorf("List(%q) differs between the two sources:\n bucket: %v\n   disk: %v",
					prefix, remote, onDisk)
			}
		}
	})

	t.Run("the listing is flat and in whole-key byte order", func(t *testing.T) {
		got := keysOf(drain(ctx, t, src, ""))

		want := slices.Clone(itKeys)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Errorf("listing:\n got: %v\nwant: %v", got, want)
		}
	})

	t.Run("sizes come from the listing alone", func(t *testing.T) {
		for _, object := range drain(ctx, t, src, "") {
			if want := int64(len(bodyFor(object.Key))); object.Size != want {
				t.Errorf("%s: size %d, want %d", object.Key, object.Size, want)
			}
		}
	})

	t.Run("an object reads back byte for byte", func(t *testing.T) {
		body, err := src.Open(ctx, itKeys[0])
		if err != nil {
			t.Fatalf("Open(%q): %v", itKeys[0], err)
		}
		defer func() { _ = body.Close() }()

		got, err := io.ReadAll(body)
		if err != nil {
			t.Fatalf("read %q: %v", itKeys[0], err)
		}
		if want := bodyFor(itKeys[0]); string(got) != want {
			t.Errorf("body = %q, want %q", got, want)
		}
	})

	t.Run("a key that is not there is reported as such", func(t *testing.T) {
		// The case a lifecycle rule produces every day: an object named by a listing
		// and gone by the time it is fetched. It has to be distinguishable, or a scan
		// cannot carry on past it with a recorded gap.
		body, err := src.Open(ctx, "audit/format=jsonl-v1/cluster_id=prod-eu-1/date=2026-03-14/hour=07/gone.jsonl.zst")
		if err == nil {
			_ = body.Close()
			t.Fatal("Open succeeded for a key that was never written")
		}
		if !errors.Is(err, objectsource.ErrKeyNotFound) {
			t.Errorf("Open = %v, want ErrKeyNotFound", err)
		}
	})

	t.Run("a credential that cannot read says so", func(t *testing.T) {
		// The likeliest first failure of the whole feature. An engineer reuses the
		// sink's credential, which is documented as needing PutObject and nothing else,
		// and gets refused on the very first listing. "This credential cannot read the
		// archive" and "the archive is empty" must not look the same (Invariant 4).
		refused, err := New(ctx, itConfig(bucket, "not-the-secret-key"))
		if err != nil {
			t.Fatalf("build a source with the wrong credential: %v", err)
		}
		defer func() { _ = refused.Close() }()

		it := refused.List(ctx, "audit/")
		defer func() { _ = it.Close() }()
		for it.Next() {
			t.Errorf("a refused listing yielded %v", it.Object())
		}
		if !errors.Is(it.Err(), objectsource.ErrAccessDenied) {
			t.Errorf("Err = %v, want ErrAccessDenied", it.Err())
		}
	})
}

// TestIntegrationListPagesThroughMoreThanOnePage proves the paging against the
// store's own page size rather than against a fake's.
//
// MaxKeys is not a knob this source exposes — a caller has no reason to care — so
// the page boundary is crossed the way a real archive crosses it: with more objects
// than fit in one response. A thousand and one small objects is the smallest number
// that does.
func TestIntegrationListPagesThroughMoreThanOnePage(t *testing.T) {
	ctx := t.Context()
	bucket := newITBucket(ctx, t)
	client := itClient(ctx, t, itSecretKey())

	const objects = 1001
	keys := make([]string, 0, objects)
	for i := range objects {
		key := fmt.Sprintf("paged/%05d.jsonl.zst", i)
		keys = append(keys, key)
		putObject(ctx, t, client, bucket, key, key)
	}
	slices.Sort(keys)

	src, err := New(ctx, itConfig(bucket, itSecretKey()))
	if err != nil {
		t.Fatalf("build the source for bucket %q: %v", bucket, err)
	}
	t.Cleanup(func() { _ = src.Close() })

	if got := keysOf(drain(ctx, t, src, "paged/")); !slices.Equal(got, keys) {
		t.Errorf("a listing across a page boundary returned %d keys, want %d", len(got), len(keys))
	}
}

// itEndpoint, itAccessKey and itSecretKey resolve the fixture's connection settings.
func itEndpoint() string  { return envOr(envEndpoint, defaultEndpoint) }
func itAccessKey() string { return envOr(envAccessKey, defaultAccessKey) }
func itSecretKey() string { return envOr(envSecretKey, defaultSecretKey) }

// envOr returns the environment value for key, or def when it is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// itConfig is the configuration an in-cluster MinIO deployment produces: an endpoint
// override, path-style addressing and a static key.
//
// forcePathStyle is not optional against this fixture and that is worth stating: a
// bucket-as-subdomain URL only resolves where DNS covers *.<endpoint>, which it does
// not for a container on 127.0.0.1 nor for a Service name in a cluster.
func itConfig(bucket, secretKey string) Config {
	return Config{
		Bucket:         bucket,
		Region:         itRegion,
		Endpoint:       itEndpoint(),
		ForcePathStyle: true,
		Credentials:    Credentials{AccessKeyID: itAccessKey(), SecretAccessKey: secretKey},
	}
}

// itClient builds the raw SDK client the fixture writes its archive with.
//
// The source under test cannot seed its own fixture: it lists and fetches, which is
// the whole of what reading an archive needs, and widening it so a test could write
// one would make that claim untrue. So the setup gets its own client — the same
// asymmetry, in the other direction, that the write path's suite already lives with.
func itClient(ctx context.Context, t *testing.T, secretKey string) *awss3.Client {
	t.Helper()

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(itRegion),
		awsconfig.WithHTTPClient(awshttp.NewBuildableClient().Freeze()),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(itAccessKey(), secretKey, "")),
	)
	if err != nil {
		t.Fatalf("resolve the AWS configuration for the fixture: %v", err)
	}
	return awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(itEndpoint())
	})
}

// itBucketSeq numbers the buckets one run creates, so two fixtures in the same test
// binary cannot collide.
var itBucketSeq int

// newITBucket creates a bucket for one test.
//
// A fresh bucket per test rather than a well-known one that is emptied: every object
// in it was written by the test asserting on it, which is exactly what a listing
// assertion needs. Cleanup is best effort and reported rather than silent — the
// container `make test-integration` stands up is thrown away at the end of the
// target, so a leftover bucket costs a developer running against a persistent MinIO
// one small bucket, and knowing about it beats a cleanup that quietly failed.
func newITBucket(ctx context.Context, t *testing.T) string {
	t.Helper()

	itBucketSeq++
	name := fmt.Sprintf("kuberecord-query-it-%d-%d", time.Now().UnixNano(), itBucketSeq)
	client := itClient(ctx, t, itSecretKey())

	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("create bucket %q on %s: %v", name, itEndpoint(), err)
	}
	t.Cleanup(func() { removeITBucket(t, client, name) })
	return name
}

// removeITBucket deletes a fixture bucket and everything in it, reporting what it
// could not remove instead of swallowing it.
func removeITBucket(t *testing.T, client *awss3.Client, bucket string) {
	t.Helper()

	ctx := context.Background()
	pages := awss3.NewListObjectsV2Paginator(client, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			t.Logf("cleanup: list bucket %q: %v", bucket, err)
			return
		}
		for _, entry := range page.Contents {
			if _, err := client.DeleteObject(ctx, &awss3.DeleteObjectInput{
				Bucket: aws.String(bucket), Key: entry.Key,
			}); err != nil {
				t.Logf("cleanup: delete %q from %q: %v", aws.ToString(entry.Key), bucket, err)
			}
		}
	}
	if _, err := client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Logf("cleanup: delete bucket %q: %v", bucket, err)
	}
}

// seedBucket writes the fixture archive into the bucket.
func seedBucket(ctx context.Context, t *testing.T, bucket string) {
	t.Helper()

	client := itClient(ctx, t, itSecretKey())
	for _, key := range itKeys {
		putObject(ctx, t, client, bucket, key, bodyFor(key))
	}
}

// putObject writes one object.
func putObject(ctx context.Context, t *testing.T, client *awss3.Client, bucket, key, body string) {
	t.Helper()

	if _, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	}); err != nil {
		t.Fatalf("write %q to %q: %v", key, bucket, err)
	}
}

// seedDirectory writes the same archive to a directory and returns a source over it.
func seedDirectory(t *testing.T) objectsource.ObjectSource {
	t.Helper()

	dir := t.TempDir()
	for _, key := range itKeys {
		path := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create the parents of %q: %v", key, err)
		}
		if err := os.WriteFile(path, []byte(bodyFor(key)), 0o600); err != nil {
			t.Fatalf("write %q: %v", key, err)
		}
	}

	src, err := objectsource.NewLocal(dir)
	if err != nil {
		t.Fatalf("open the local archive at %q: %v", dir, err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src
}

// bodyFor is an object's content: its own key, so a body that comes back from the
// wrong object says which one it came from. The keys differ in length, which is what
// makes the size assertions worth making.
func bodyFor(key string) string { return "payload for " + key }

// drain reads a whole listing, failing the test if it ended in an error.
func drain(ctx context.Context, t *testing.T, src objectsource.ObjectSource, prefix string) []objectsource.Object {
	t.Helper()

	it := src.List(ctx, prefix)
	defer func() { _ = it.Close() }()

	var objects []objectsource.Object
	for it.Next() {
		objects = append(objects, it.Object())
	}
	if err := it.Err(); err != nil {
		t.Fatalf("List(%q): %v", prefix, err)
	}
	return objects
}

// keysOf reduces a listing to what most assertions are about.
func keysOf(objects []objectsource.Object) []string {
	keys := make([]string, 0, len(objects))
	for _, object := range objects {
		keys = append(keys, object.Key)
	}
	return keys
}
