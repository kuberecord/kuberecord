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

package awssource

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/kuberecord/kuberecord/internal/query/objectsource"
)

const testBucket = "kuberecord-archive"

// TestListPagesThroughAWholePrefix is the property the streaming contract exists
// for: a listing longer than one response is one listing to its caller.
//
// The continuation tokens are asserted as well as the keys, because a paging bug
// that repeats a page or drops one produces a plausible listing rather than an
// error — and a scan built on it would be quietly missing an hour of history.
func TestListPagesThroughAWholePrefix(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{pages: []*awss3.ListObjectsV2Output{
		page(true, "second", object("a/1", 10), object("a/2", 20)),
		page(true, "third", object("a/3", 30)),
		page(false, "", object("a/4", 40)),
	}}
	src := &Source{client: api, bucket: testBucket, closeIdle: func() {}}

	got := listObjects(t, src, "a/")

	want := []objectsource.Object{
		{Key: "a/1", Size: 10}, {Key: "a/2", Size: 20},
		{Key: "a/3", Size: 30}, {Key: "a/4", Size: 40},
	}
	if !slices.Equal(got, want) {
		t.Errorf("listing:\n got: %v\nwant: %v", got, want)
	}

	if len(api.listCalls) != 3 {
		t.Fatalf("made %d list requests, want 3", len(api.listCalls))
	}
	wantTokens := []string{"", "second", "third"}
	for i, call := range api.listCalls {
		if got := aws.ToString(call.Bucket); got != testBucket {
			t.Errorf("request %d asked bucket %q, want %q", i, got, testBucket)
		}
		if got := aws.ToString(call.Prefix); got != "a/" {
			t.Errorf("request %d asked prefix %q, want %q", i, got, "a/")
		}
		if got := aws.ToString(call.ContinuationToken); got != wantTokens[i] {
			t.Errorf("request %d carried token %q, want %q", i, got, wantTokens[i])
		}
		if call.Delimiter != nil {
			// A delimiter would stop the listing at the next separator, which is what
			// draws "folders" in a bucket browser — and would hide every object below
			// the first level of a partitioned archive.
			t.Errorf("request %d set a delimiter (%q); the listing must be flat and recursive",
				i, aws.ToString(call.Delimiter))
		}
	}
}

// TestListStopsPagingWhenTheCallerStops covers what makes a scan interruptible.
// Abandoning a listing has to stop the requests, not merely stop the reads.
func TestListStopsPagingWhenTheCallerStops(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{pages: []*awss3.ListObjectsV2Output{
		page(true, "second", object("a/1", 1), object("a/2", 2)),
		page(false, "", object("a/3", 3)),
	}}
	src := &Source{client: api, bucket: testBucket, closeIdle: func() {}}

	it := src.List(t.Context(), "a/")
	if !it.Next() {
		t.Fatalf("the listing was empty: %v", it.Err())
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if it.Next() {
		t.Errorf("a closed iterator yielded %v", it.Object())
	}
	if err := it.Err(); err != nil {
		t.Errorf("Err after an early Close = %v, want nil: abandoning a scan is not a failure", err)
	}
	if len(api.listCalls) != 1 {
		t.Errorf("made %d list requests after one page was abandoned, want 1", len(api.listCalls))
	}
}

// TestListRefusesAPageThatPromisesMoreAndSaysNothing is Invariant 4 at the seam. A
// store that reports more results without a token leaves no way to ask for the rest,
// and stopping quietly there would hand back a listing that looks complete and is
// not.
func TestListRefusesAPageThatPromisesMoreAndSaysNothing(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{pages: []*awss3.ListObjectsV2Output{
		{Contents: []awss3types.Object{object("a/1", 1)}, IsTruncated: aws.Bool(true)},
	}}
	src := &Source{client: api, bucket: testBucket, closeIdle: func() {}}

	it := src.List(t.Context(), "a/")
	defer func() { _ = it.Close() }()
	for it.Next() { //nolint:revive // draining is the point; the error is the assertion
	}
	if it.Err() == nil {
		t.Error("a truncated listing with no continuation token was reported as a complete listing")
	}
}

// TestListSkipsAnEntryWithNoKey keeps a malformed response from taking down a scan
// over an entry nobody could have fetched anyway.
func TestListSkipsAnEntryWithNoKey(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{pages: []*awss3.ListObjectsV2Output{
		page(false, "", object("a/1", 1), awss3types.Object{Size: aws.Int64(9)}, object("a/2", 2)),
	}}
	src := &Source{client: api, bucket: testBucket, closeIdle: func() {}}

	got := listObjects(t, src, "a/")
	want := []objectsource.Object{{Key: "a/1", Size: 1}, {Key: "a/2", Size: 2}}
	if !slices.Equal(got, want) {
		t.Errorf("listing:\n got: %v\nwant: %v", got, want)
	}
}

// TestListReportsAFailureThroughErr covers the case an audit tool must never round
// off: a listing that died on its second page is not a short listing.
func TestListReportsAFailureThroughErr(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{
		pages:   []*awss3.ListObjectsV2Output{page(true, "second", object("a/1", 1))},
		listErr: &smithy.GenericAPIError{Code: "AccessDenied", Message: "not authorised"},
	}
	src := &Source{client: api, bucket: testBucket, closeIdle: func() {}}

	it := src.List(t.Context(), "a/")
	defer func() { _ = it.Close() }()
	if !it.Next() {
		t.Fatalf("the first page did not arrive: %v", it.Err())
	}
	for it.Next() { //nolint:revive // draining is the point; the error is the assertion
	}

	err := it.Err()
	if !errors.Is(err, objectsource.ErrAccessDenied) {
		t.Errorf("Err = %v, want ErrAccessDenied", err)
	}
	if !strings.Contains(err.Error(), testBucket) {
		t.Errorf("Err = %v, which does not name the bucket it failed against", err)
	}
}

// TestListStopsOnContextCancellation covers the interruptible half of a bounded
// scan. The cancellation must arrive through Err rather than as a silently short
// listing.
func TestListStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	api := &fakeAPI{pages: []*awss3.ListObjectsV2Output{page(false, "", object("a/1", 1))}}
	src := &Source{client: api, bucket: testBucket, closeIdle: func() {}}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	it := src.List(ctx, "a/")
	defer func() { _ = it.Close() }()
	if it.Next() {
		t.Errorf("a cancelled listing yielded %v", it.Object())
	}
	if !errors.Is(it.Err(), context.Canceled) {
		t.Errorf("Err = %v, want context.Canceled", it.Err())
	}
	if len(api.listCalls) != 0 {
		t.Errorf("a cancelled listing still made %d requests", len(api.listCalls))
	}
}

// TestOpenHandsBackTheLiveBody is what keeps peak memory independent of object size.
// A source that read the response to satisfy io.ReadCloser would make the concurrency
// cap a memory multiplier — the opposite of the reason a cap exists.
func TestOpenHandsBackTheLiveBody(t *testing.T) {
	t.Parallel()

	body := &recordingBody{Reader: strings.NewReader("payload")}
	api := &fakeAPI{getBody: body}
	src := &Source{client: api, bucket: testBucket, closeIdle: func() {}}

	got, err := src.Open(t.Context(), "a/1")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if body.reads != 0 {
		t.Errorf("Open read the object body %d times before returning it", body.reads)
	}

	content, err := io.ReadAll(got)
	if err != nil {
		t.Fatalf("read the object: %v", err)
	}
	if string(content) != "payload" {
		t.Errorf("object body = %q, want %q", content, "payload")
	}
	if err := got.Close(); err != nil {
		t.Fatalf("close the object: %v", err)
	}
	if !body.closed {
		t.Error("closing the returned reader did not close the response body")
	}

	if len(api.getCalls) != 1 {
		t.Fatalf("made %d get requests, want 1", len(api.getCalls))
	}
	if got := aws.ToString(api.getCalls[0].Bucket); got != testBucket {
		t.Errorf("get asked bucket %q, want %q", got, testBucket)
	}
	if got := aws.ToString(api.getCalls[0].Key); got != "a/1" {
		t.Errorf("get asked key %q, want %q", got, "a/1")
	}
}

// TestClassification pins the two failures the seam gives names to, and the ones it
// deliberately leaves anonymous.
//
// The distinctions are the ones a caller acts on. A key that is gone is a gap to
// record and carry on from; a credential that may not read is a sentence to print
// and stop. Everything else is neither, and dressing it up as one of the two would
// send the caller down the wrong path with confidence.
func TestClassification(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want error
	}{
		{
			name: "the SDK's typed missing key",
			err:  &awss3types.NoSuchKey{Message: aws.String("no such key")},
			want: objectsource.ErrKeyNotFound,
		},
		{
			name: "a store spelling it as a code",
			err:  &smithy.GenericAPIError{Code: "NoSuchKey"},
			want: objectsource.ErrKeyNotFound,
		},
		{
			name: "a store spelling it NotFound",
			err:  &smithy.GenericAPIError{Code: "NotFound"},
			want: objectsource.ErrKeyNotFound,
		},
		{
			name: "an explicit refusal",
			err:  &smithy.GenericAPIError{Code: "AccessDenied"},
			want: objectsource.ErrAccessDenied,
		},
		{
			name: "a bucket with access switched off",
			err:  &smithy.GenericAPIError{Code: "AllAccessDisabled"},
			want: objectsource.ErrAccessDenied,
		},
		{
			// The rest of the 403 family — an invalid key id, a signature mismatch, an
			// expired session — is one sentence to a CLI user, so it is one sentinel.
			name: "a bare 403 with an unfamiliar code",
			err:  forbidden("SignatureDoesNotMatch"),
			want: objectsource.ErrAccessDenied,
		},
		{
			// Also a 404, and deliberately not folded in: reporting a mistyped bucket as
			// an object that aged out turns a configuration error into an ordinary
			// archive event and sends the caller looking in the wrong place.
			name: "a missing bucket is not a missing key",
			err:  &awss3types.NoSuchBucket{Message: aws.String("no such bucket")},
			want: nil,
		},
		{
			name: "an unreachable endpoint",
			err:  errors.New("dial tcp: connection refused"),
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := &Source{client: &fakeAPI{getErr: tc.err}, bucket: testBucket, closeIdle: func() {}}
			_, err := src.Open(t.Context(), "a/1")
			if err == nil {
				t.Fatal("Open succeeded")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Errorf("Open = %v, want it to be %v", err, tc.want)
			}
			if tc.want == nil {
				for _, sentinel := range []error{objectsource.ErrKeyNotFound, objectsource.ErrAccessDenied} {
					if errors.Is(err, sentinel) {
						t.Errorf("Open = %v, which was classified as %v", err, sentinel)
					}
				}
			}
			if !errors.Is(err, tc.err) {
				t.Errorf("Open = %v, which no longer carries the underlying failure", err)
			}
		})
	}
}

// TestNewValidatesItsConfiguration keeps three mistakes from being reported later, by
// the SDK, as if the bucket were at fault.
func TestNewValidatesItsConfiguration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cfg  Config
	}{
		{name: "no bucket", cfg: Config{Region: "us-east-1"}},
		{name: "no region", cfg: Config{Bucket: testBucket}},
		{
			name: "an endpoint that is not a URL",
			cfg:  Config{Bucket: testBucket, Region: "us-east-1", Endpoint: "http://minio\x7f"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if src, err := New(t.Context(), tc.cfg); err == nil {
				_ = src.Close()
				t.Error("New accepted the configuration")
			}
		})
	}
}

// TestNewAcceptsAMinIOStyleConfiguration covers the configuration an in-cluster
// deployment actually uses: an endpoint override, path-style addressing and a static
// key. It is the combination that makes MinIO and S3 the same code below this point.
func TestNewAcceptsAMinIOStyleConfiguration(t *testing.T) {
	t.Parallel()

	src, err := New(t.Context(), Config{
		Bucket:         testBucket,
		Region:         "us-east-1",
		Endpoint:       "http://minio.kuberecord.svc:9000",
		ForcePathStyle: true,
		Credentials:    Credentials{AccessKeyID: "key", SecretAccessKey: "secret"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestIsAmbient pins the rule that decides whose archive is read.
//
// It is all three fields being empty rather than any of them, so a half-filled key is
// a configuration error the SDK reports rather than a silent fall-through to whatever
// profile the machine has configured — which on a laptop would read a different
// account's archive and say nothing about it.
func TestIsAmbient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		creds Credentials
		want  bool
	}{
		{name: "the zero value", creds: Credentials{}, want: true},
		{name: "a static key", creds: Credentials{AccessKeyID: "k", SecretAccessKey: "s"}, want: false},
		{name: "a temporary key", creds: Credentials{AccessKeyID: "k", SecretAccessKey: "s", SessionToken: "t"}, want: false},
		{name: "half a key", creds: Credentials{AccessKeyID: "k"}, want: false},
		{name: "a token alone", creds: Credentials{SessionToken: "t"}, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.creds.IsAmbient(); got != tc.want {
				t.Errorf("IsAmbient() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestTheSDKExceptionIsStillUsed keeps this package's depguard exception from
// outliving its reason.
//
// object-store-client-is-confined excepts exactly two directories, and an exception
// nobody uses is an exception nobody re-argues — it is simply an open door for the
// next import that wants one. If the SDK ever leaves this package, the exception must
// leave with it.
func TestTheSDKExceptionIsStillUsed(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("the go command is not on PATH: %v", err)
	}

	cmd := exec.Command(goBin, "list", "-f", `{{range .Imports}}{{println .}}{{end}}`, ".")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, stderr.String())
	}

	for line := range strings.SplitSeq(string(out), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "github.com/aws/aws-sdk-go-v2") {
			return
		}
	}
	t.Error("this package no longer imports the AWS SDK, so its exception in " +
		"object-store-client-is-confined is now an open door rather than a stated " +
		"reason: remove the exception with the import")
}

// fakeAPI stands in for the S3 client, so pagination and error classification are
// asserted directly rather than through an HTTP round-trip. It is the same discipline
// the write path's store follows with putObjectAPI.
type fakeAPI struct {
	// pages are handed out in order; listErr ends the sequence once they run out,
	// which is how a mid-listing failure is staged.
	pages   []*awss3.ListObjectsV2Output
	listErr error

	getBody io.ReadCloser
	getErr  error

	listCalls []awss3.ListObjectsV2Input
	getCalls  []awss3.GetObjectInput
}

// ListObjectsV2 records the request and answers with the next staged page.
func (f *fakeAPI) ListObjectsV2(_ context.Context, in *awss3.ListObjectsV2Input,
	_ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	f.listCalls = append(f.listCalls, *in)
	if len(f.pages) == 0 {
		if f.listErr != nil {
			return nil, f.listErr
		}
		return &awss3.ListObjectsV2Output{}, nil
	}
	next := f.pages[0]
	f.pages = f.pages[1:]
	return next, nil
}

// GetObject records the request and answers with the staged body or failure.
func (f *fakeAPI) GetObject(_ context.Context, in *awss3.GetObjectInput,
	_ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	f.getCalls = append(f.getCalls, *in)
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &awss3.GetObjectOutput{Body: f.getBody}, nil
}

// recordingBody is a response body that remembers whether anyone read it, which is
// how "handed back unread" is asserted rather than assumed.
type recordingBody struct {
	*strings.Reader
	reads  int
	closed bool
}

func (b *recordingBody) Read(p []byte) (int, error) {
	b.reads++
	return b.Reader.Read(p)
}

func (b *recordingBody) Close() error {
	b.closed = true
	return nil
}

// page builds one listing response.
func page(truncated bool, token string, contents ...awss3types.Object) *awss3.ListObjectsV2Output {
	out := &awss3.ListObjectsV2Output{Contents: contents, IsTruncated: aws.Bool(truncated)}
	if token != "" {
		out.NextContinuationToken = aws.String(token)
	}
	return out
}

// object builds one listing entry.
func object(key string, size int64) awss3types.Object {
	return awss3types.Object{Key: aws.String(key), Size: aws.Int64(size)}
}

// forbidden builds the shape a store's 403 arrives in: an API error carried inside an
// HTTP response error, which is where the status code the classifier falls back to
// lives.
func forbidden(code string) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusForbidden}},
			Err:      &smithy.GenericAPIError{Code: code},
		},
		RequestID: "test-request",
	}
}

// listObjects drains a listing, failing the test if it ended in an error. The error
// check is not optional for a caller and it is not optional here either.
func listObjects(t *testing.T, src objectsource.ObjectSource, prefix string) []objectsource.Object {
	t.Helper()

	it := src.List(t.Context(), prefix)
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
