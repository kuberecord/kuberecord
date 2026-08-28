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

// Package awssource is the AWS SDK v2 implementation of the read-side object
// source (objectsource.ObjectSource): LIST a prefix, GET an object.
//
// It is the second package permitted to link the SDK, and the boundary moved for a
// reason the write path already states in prose. internal/sink/s3/client.go
// documents the credential a kuberecord sink needs as "a single PutObject and
// nothing else, no LIST, no GET, no DELETE", and the S3 integration fixture says
// the same thing the other way round: anything that reads the archive back is a
// separate consumer with separate rights, not a capability of the sink. This is
// that consumer. Putting it in internal/sink/s3/awsstore instead would have made
// the operator's own binary link a LIST/GET client it must never use, and would
// have put the write path — internal/sink, internal/sink/s3, and the
// controller-runtime and backoff dependencies behind them — into the link graph of
// every CLI invocation. Two packages, one per plane, is the same shape the
// ClickHouse driver already has.
//
// Everything that differs between AWS S3 and a MinIO deployment is resolved here,
// once, when the client is constructed: the region, the endpoint override, the
// addressing style and the credential chain. Listing and fetching are then the same
// code against either.
//
// The package deliberately does not share a configuration type with the write
// path's client for the same reason it is not in that package: importing it to save
// four fields would reinstate exactly the dependency this split exists to prevent.
package awssource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/kuberecord/kuberecord/internal/query/objectsource"
)

// Credentials is a static access key, or the zero value for the ambient chain.
//
// Its zero value is meaningful and is the shape a well-run environment should
// prefer: an engineer's own SSO session, an assumed role, an instance profile —
// resolved by the SDK from the process environment and the shared config file, with
// no long-lived key written down anywhere.
type Credentials struct {
	// AccessKeyID and SecretAccessKey are the two halves of a static key. Both
	// empty means the ambient chain.
	AccessKeyID     string
	SecretAccessKey string

	// SessionToken is set only for temporary credentials.
	SessionToken string
}

// IsAmbient reports whether these credentials say "resolve me from the
// environment" rather than carrying a key.
//
// It is all three fields being empty rather than any of them, so a half-filled key
// is a configuration error the SDK reports rather than a silent fall-through to
// whatever identity the machine happens to have — which, on a laptop with a
// long-lived profile configured, would read the wrong account's archive and say
// nothing.
func (c Credentials) IsAmbient() bool {
	return c.AccessKeyID == "" && c.SecretAccessKey == "" && c.SessionToken == ""
}

// Config is everything that decides which archive is read and how it is reached.
//
// The bucket is here, and not on each call, because a source is bound to one
// archive for its whole life (see objectsource.ObjectSource). That is the opposite
// of the write path's PutObjectInput, which carries its bucket per request so a
// misrouted object is visible in the request itself; there is no equivalent hazard
// for a reader, and a bucket argument on every List would be four words of
// ceremony at every call site in the CLI.
type Config struct {
	// Bucket is the bucket holding the archive.
	Bucket string

	// Region is the bucket's region. The SDK requires one even against MinIO,
	// which ignores it.
	Region string

	// Endpoint is an absolute URL overriding AWS S3's own resolved endpoint, or
	// empty for AWS S3.
	Endpoint string

	// ForcePathStyle addresses the bucket as <endpoint>/<bucket>/<key> rather than
	// as <bucket>.<endpoint>/<key>. It is what most in-cluster MinIO deployments
	// need, since a bucket-as-subdomain URL only resolves where DNS (and any TLS
	// certificate) covers *.<endpoint>.
	ForcePathStyle bool

	// Credentials is a static key, or the zero value for the ambient chain.
	Credentials Credentials
}

// objectReadAPI is the whole of the SDK surface this package calls: two
// operations, which is also the whole of the permission set reading an archive
// needs (s3:ListBucket and s3:GetObject).
//
// It is an interface so this package's own tests can drive pagination and error
// classification directly, without an HTTP round-trip standing between the
// assertion and the thing asserted — the same discipline the write path's
// putObjectAPI follows. The generated *s3.Client satisfies it.
type objectReadAPI interface {
	ListObjectsV2(ctx context.Context, in *awss3.ListObjectsV2Input,
		optFns ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *awss3.GetObjectInput,
		optFns ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
}

// Source is one archive in one bucket, read through a configured S3 client.
type Source struct {
	client objectReadAPI
	bucket string

	// closeIdle releases the HTTP transport's idle connections on Close. It is a
	// function rather than the client itself because what the SDK hands back as its
	// HTTP client is an interface; see idleCloserFor.
	closeIdle func()
}

// New builds a source over one bucket.
//
// It performs no network I/O: resolving the AWS configuration reads the process
// environment and, at most, the shared config file, and the credential chain it
// assembles is lazy — a credential is retrieved when the first request is signed.
// A broken chain therefore surfaces on the first List or Open, wrapped, rather than
// here, which is the right place for it: a CLI that failed at construction could
// not tell the user which archive it had failed to reach.
//
// ctx bounds only that configuration resolution. It is not retained.
func New(ctx context.Context, cfg Config) (*Source, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("awssource: a bucket is required")
	}
	if cfg.Region == "" {
		// Without one the SDK's complaint arrives later, out of endpoint resolution,
		// phrased as if the bucket were at fault.
		return nil, errors.New("awssource: a region is required")
	}
	if cfg.Endpoint != "" {
		if _, err := url.Parse(cfg.Endpoint); err != nil {
			return nil, fmt.Errorf("awssource: endpoint %q is not a URL: %w", cfg.Endpoint, err)
		}
	}

	// The SDK's own default HTTP client, frozen: it carries the tuned transport
	// (connection pooling, TLS floor, the S3-appropriate redirect policy) and
	// freezing it keeps the SDK from re-deriving those from a default mode later.
	httpClient := awshttp.NewBuildableClient().Freeze()

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(httpClient),
	}
	if !cfg.Credentials.IsAmbient() {
		// An explicit key wins outright: passing it as the provider means no part of
		// the ambient chain is consulted, so a source naming a key can never silently
		// fall back to a profile that happens to be configured and read a different
		// account's archive.
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.Credentials.AccessKeyID,
				cfg.Credentials.SecretAccessKey,
				cfg.Credentials.SessionToken,
			),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("awssource: resolve the AWS configuration: %w", err)
	}

	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.UsePathStyle = cfg.ForcePathStyle
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})

	return &Source{client: client, bucket: cfg.Bucket, closeIdle: idleCloserFor(httpClient)}, nil
}

// List streams the objects under prefix. See objectsource.ObjectSource.List.
//
// No delimiter is set, so the listing is flat and recursive over the whole prefix
// rather than stopping at the next separator — the "folders" a bucket browser shows
// are that delimiter's doing and would hide every object below the first level.
// S3 returns keys in ascending byte order, which is the order the contract promises
// and the order the local source is made to reproduce.
func (s *Source) List(ctx context.Context, prefix string) objectsource.ObjectIterator {
	return &listIterator{ctx: ctx, client: s.client, bucket: s.bucket, prefix: prefix}
}

// Open returns the object's body. See objectsource.ObjectSource.Open.
//
// The body is handed back as the SDK produced it, unread: it is the live response
// stream, so a 64Mi object costs a buffer's worth of memory to consume rather than
// its own size. That is what lets a scan fetch several objects at once under a
// concurrency cap without the cap becoming a memory multiplier.
func (s *Source) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, classify(s.bucket, "get object", key, err)
	}
	return out.Body, nil
}

// Close releases the transport's idle connections. Bodies handed out by Open are
// the caller's; this closes the client, not them.
func (s *Source) Close() error {
	s.closeIdle()
	return nil
}

// classify turns one failure into the vocabulary the seam speaks, so that a caller
// handling a vanished object or a refused credential writes the same code whichever
// source it is talking to.
//
// It is a function rather than a method because the listing iterator needs it too,
// and it outlives the call that produced it: an iterator carries the bucket it is
// paging, not the source that opened it.
func classify(bucket, op, subject string, err error) error {
	wrapped := fmt.Errorf("awssource: %s %q in bucket %q: %w", op, subject, bucket, err)
	switch {
	case isNoSuchKey(err):
		return fmt.Errorf("%w: %w", objectsource.ErrKeyNotFound, wrapped)
	case isAccessDenied(err):
		return fmt.Errorf("%w: %w", objectsource.ErrAccessDenied, wrapped)
	default:
		return wrapped
	}
}

// isNoSuchKey reports whether err is the store saying the key does not exist.
//
// The typed error is checked first because that is what the SDK models the case as,
// and the codes are checked because S3-compatible stores answer a GET for a missing
// key with either spelling. A missing *bucket* is deliberately not matched: it is
// also a 404, and folding it in would report a mistyped bucket as an object that had
// aged out — a configuration error dressed up as an ordinary archive event.
func isNoSuchKey(err error) bool {
	var missing *awss3types.NoSuchKey
	if errors.As(err, &missing) {
		return true
	}
	switch apiErrorCode(err) {
	case "NoSuchKey", "NotFound":
		return true
	default:
		return false
	}
}

// isAccessDenied reports whether err is the store refusing this identity.
//
// Both halves matter. The codes cover a store that answers with one and no usable
// status, and the status covers the rest of the 403 family — an invalid key id, a
// signature mismatch, an expired session — which are all, from the CLI's point of
// view, the same sentence: this credential cannot read the archive. That sentence is
// worth getting right because it is the likeliest first failure of the whole
// feature: the sink's own credential is documented as write-only, and reusing it is
// the obvious thing to try.
func isAccessDenied(err error) bool {
	switch apiErrorCode(err) {
	case "AccessDenied", "AllAccessDisabled":
		return true
	}
	var status interface{ HTTPStatusCode() int }
	return errors.As(err, &status) && status.HTTPStatusCode() == 403
}

// apiErrorCode returns the store's error code, or "" if err did not come from one.
func apiErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

// idleCloserFor produces Close's body for the SDK's HTTP client.
//
// The indirection exists because the SDK's own BuildableClient cannot release the
// connections it pooled: its GetTransport hands back a clone, so a source holding
// one would have no reference to the live pool at all. Its frozen form is an
// *http.Client, which does expose the release, and the assertion is what recovers
// that without this package hard-coding the SDK's internal type.
//
// The write path's store needs the same thing and spells it the same way. They are
// not shared, because sharing would require one of these two packages to import the
// other and the whole point of there being two is that neither does.
func idleCloserFor(httpClient aws.HTTPClient) func() {
	if closer, ok := httpClient.(interface{ CloseIdleConnections() }); ok {
		return closer.CloseIdleConnections
	}
	return func() {}
}

// listIterator pages through a prefix, holding one page of results at a time.
//
// One page is 1000 keys at the S3 default, which is the point of paging here: a
// ninety-day prefix runs to hundreds of thousands of keys, and the next page is
// fetched only when the caller has consumed the last one — so a scan that stops
// early stops paying immediately.
type listIterator struct {
	ctx    context.Context
	client objectReadAPI
	bucket string
	prefix string

	page  []awss3types.Object
	next  int
	token *string
	// started distinguishes "no continuation token because this is the first page"
	// from "no continuation token because the listing is over".
	started bool

	cur    objectsource.Object
	err    error
	closed bool
}

// Next advances to the next object, fetching a page when the current one runs out.
// See objectsource.ObjectIterator.
func (it *listIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	for {
		if err := it.ctx.Err(); err != nil {
			it.err = err
			return false
		}
		if it.next < len(it.page) {
			entry := it.page[it.next]
			it.next++
			if entry.Key == nil {
				// A listing entry with no key is not an object anyone can fetch. It has
				// never been observed; skipping it rather than dereferencing is what keeps
				// a malformed response from taking down a scan.
				continue
			}
			it.cur = objectsource.Object{Key: *entry.Key, Size: aws.ToInt64(entry.Size)}
			return true
		}
		if it.started && it.token == nil {
			return false
		}
		if !it.fetch() {
			return false
		}
	}
}

// fetch loads the next page, reporting whether it succeeded.
func (it *listIterator) fetch() bool {
	out, err := it.client.ListObjectsV2(it.ctx, &awss3.ListObjectsV2Input{
		Bucket:            aws.String(it.bucket),
		Prefix:            aws.String(it.prefix),
		ContinuationToken: it.token,
	})
	if err != nil {
		it.err = classify(it.bucket, "list objects under prefix", it.prefix, err)
		return false
	}

	it.page = out.Contents
	it.next = 0
	it.started = true

	switch {
	case !aws.ToBool(out.IsTruncated):
		it.token = nil
	case out.NextContinuationToken == nil:
		// A store that says there is more and does not say where leaves no way to ask
		// for the rest. Stopping quietly here would hand back a listing that looks
		// complete and is not, and a scan built on it would report that nothing changed
		// in a window it never finished reading (Invariant 4).
		it.err = fmt.Errorf(
			"awssource: listing %q in bucket %q reported more results but returned no "+
				"continuation token, so the rest of the listing cannot be requested",
			it.prefix, it.bucket)
		return false
	default:
		it.token = out.NextContinuationToken
	}
	return true
}

// Object returns the object Next advanced to. See objectsource.ObjectIterator.
func (it *listIterator) Object() objectsource.Object { return it.cur }

// Err returns what ended the listing, or nil if it ran out. See
// objectsource.ObjectIterator.
func (it *listIterator) Err() error { return it.err }

// Close abandons the listing. It holds no connection between pages — a page is a
// completed request — so this releases the page it is holding and nothing else,
// which is what makes breaking out of a scan free.
func (it *listIterator) Close() error {
	it.closed = true
	it.page = nil
	return nil
}

// Compile-time proof that this package implements the contract the read plane
// declares, asserted here rather than discovered at the wiring site where a
// signature drift would surface in a file that has nothing to do with either.
var (
	_ objectsource.ObjectSource   = (*Source)(nil)
	_ objectsource.ObjectIterator = (*listIterator)(nil)
	_ objectReadAPI               = (*awss3.Client)(nil)
)
