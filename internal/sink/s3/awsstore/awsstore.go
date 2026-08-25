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

// Package awsstore is the AWS SDK v2 implementation of the tiny object-store
// interface the S3 backend writes through (s3.ObjectStore).
//
// It is its own package, beside internal/sink/s3 rather than inside it, so that
// the shipped rotation, retry and commit logic is exercised against a stand-in
// store rather than against a mocked HTTP client — the discipline s3/client.go
// states, and the reason the interface exists at all. The write path's package
// links no SDK; this one links nothing else.
//
// Everything that differs between AWS S3 and a MinIO deployment is resolved here,
// once, when the client is constructed: the region, the endpoint override, the
// addressing style and the credential chain. Writing an object is then the same
// code against either, which is what "MinIO works without special-casing" means
// in practice.
package awsstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/kuberecord/kuberecord/internal/sink"
	kbs3 "github.com/kuberecord/kuberecord/internal/sink/s3"
)

// putObjectAPI is the whole of the SDK surface this package calls.
//
// It is an interface so this package's own tests can drive PutObject's request
// mapping and error classification directly, without an HTTP round-trip standing
// between the assertion and the thing asserted. The generated *s3.Client
// satisfies it.
type putObjectAPI interface {
	PutObject(ctx context.Context, in *awss3.PutObjectInput,
		optFns ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
}

// Store is one S3Sink's object store: a configured S3 client, plus the transport
// whose pooled connections Close releases.
type Store struct {
	client putObjectAPI

	// closeIdle releases the HTTP transport's idle connections on Close. It is a
	// function rather than the client itself because what the SDK hands back as
	// its HTTP client is an interface; see New.
	closeIdle func()
}

// New builds the object store for one S3Sink from its resolved client
// configuration.
//
// It performs no network I/O and must not: it is called from the sink.Factory,
// which the SinkManager invokes inline on the reconciler's goroutine (Invariant
// 1). Resolving the AWS configuration reads process environment and, at most, the
// shared config file, and the credential chain it assembles is lazy — a
// credential is retrieved when the first request is signed, which happens on the
// writer's own goroutines. That is also why a broken ambient chain cannot be
// reported from here: it is reported by the health probe, as
// sink.ErrCredentialsUnavailable (see credentialClassifier).
//
// ctx bounds only that configuration resolution. It is not retained.
func New(ctx context.Context, cfg kbs3.ClientConfig) (*Store, error) {
	if cfg.Region == "" {
		// The CRD defaults spec.region, so this is a programming error in the
		// configuration builder rather than a user's omission — but the SDK's own
		// complaint about it arrives later, from endpoint resolution, phrased as if
		// the bucket were at fault.
		return nil, errors.New("awsstore: a region is required (the S3Sink CRD defaults it to us-east-1)")
	}
	if cfg.Endpoint != "" {
		// The CRD's Pattern has already rejected a bare host:port, so reaching this
		// means something no schema could catch. Failing here, by name, is the
		// whole point: an unparseable endpoint surfaces as a sink that cannot be
		// built rather than as one that reports the bucket unreachable.
		if _, err := url.Parse(cfg.Endpoint); err != nil {
			return nil, fmt.Errorf("awsstore: endpoint %q is not a URL: %w", cfg.Endpoint, err)
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
		// An explicit key wins outright: passing it as the provider means no part
		// of the ambient chain is consulted, so a sink naming a Secret can never
		// silently fall back to the pod's own identity if that Secret is wrong.
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
		return nil, fmt.Errorf("awsstore: resolve the AWS configuration: %w", err)
	}
	// Wrap whatever chain was resolved, so a failure to *produce* a credential is
	// distinguishable from a failure to reach the bucket with one.
	awsCfg.Credentials = &credentialClassifier{inner: awsCfg.Credentials}

	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		// The two knobs that make MinIO work without special-casing anywhere else.
		o.UsePathStyle = cfg.ForcePathStyle
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})

	return &Store{client: client, closeIdle: idleCloserFor(httpClient)}, nil
}

// idleCloserFor produces Close's body for the SDK's HTTP client.
//
// The indirection exists because the SDK's own BuildableClient cannot release the
// connections it pooled: its GetTransport hands back a *clone*, so a store holding
// one would have no reference to the live pool at all. Its frozen form is an
// *http.Client, which does expose the release, and the assertion is what recovers
// that without this package hard-coding the SDK's internal type.
//
// A future SDK returning something else leaves the pool to be reclaimed when the
// transport is garbage collected. That is a mild, bounded regression — idle
// connections on a recycled sink's client — rather than a failure, which is why it
// degrades quietly here instead of refusing to build the sink.
func idleCloserFor(httpClient aws.HTTPClient) func() {
	if closer, ok := httpClient.(interface{ CloseIdleConnections() }); ok {
		return closer.CloseIdleConnections
	}
	return func() {}
}

// PutObject implements s3.ObjectStore: one object, written whole.
//
// The body is handed over as a *bytes.Reader — seekable, with its length known —
// so the SDK signs and checksums it as a plain request rather than as a streamed
// one. That matters for compatibility rather than for speed: the chunked, trailing
// -checksum encoding the SDK falls back to for an unseekable body is the shape
// some S3-compatible stores refuse.
//
// SDK-level retries are left at their defaults and are *not* a second retry
// policy competing with the writer's. They cover the retryable-in-milliseconds
// cases an object store has (a 503 SlowDown, a dropped connection); the writer's
// own backoff decides when an object is abandoned and its records handed back to
// the pipeline. Both are bounded by ctx, which the writer sets to its per-PUT
// timeout, so neither can run long.
func (s *Store) PutObject(ctx context.Context, in kbs3.PutObjectInput) error {
	req := &awss3.PutObjectInput{
		Bucket:        aws.String(in.Bucket),
		Key:           aws.String(in.Key),
		Body:          bytes.NewReader(in.Body),
		ContentLength: aws.Int64(int64(len(in.Body))),
	}
	if in.Retention != nil {
		// Passed through, never interpreted: the mode is spelled as S3 spells it
		// and the instant was fixed when the object was built, so a retried PUT
		// repeats this request rather than re-dating it.
		req.ObjectLockMode = awss3types.ObjectLockMode(in.Retention.Mode)
		req.ObjectLockRetainUntilDate = aws.Time(in.Retention.RetainUntil)
	}

	if _, err := s.client.PutObject(ctx, req); err != nil {
		return classify(in, err)
	}
	return nil
}

// Close releases the transport's idle connections. It is called once, after every
// worker has stopped, so nothing can be in flight.
func (s *Store) Close() error {
	s.closeIdle()
	return nil
}

// classify turns one PUT failure into the vocabulary the sink contract speaks.
//
// Three outcomes, and the distinction is what an operator is told to do about it:
//
//   - A bucket that will not accept the *shape* of object this sink writes is
//     wrapped in s3.ErrBucketIncompatible, which the writer's probe turns into
//     sink.ErrSchemaInvalid: nothing about waiting fixes it, a human must change
//     the bucket or the spec.
//   - A credential that could never be produced already carries
//     sink.ErrCredentialsUnavailable from credentialClassifier, and is passed
//     through with context added rather than re-wrapped, so errors.Is still finds
//     it.
//   - Everything else is reachability, reported as-is, and the manager keeps
//     retrying it.
func classify(in kbs3.PutObjectInput, err error) error {
	wrapped := fmt.Errorf("put object %q in bucket %q: %w", in.Key, in.Bucket, err)
	switch {
	case errors.Is(err, sink.ErrCredentialsUnavailable):
		return wrapped
	case in.Retention != nil && refusesObjectLock(err):
		return fmt.Errorf("%w: %w", kbs3.ErrBucketIncompatible, wrapped)
	default:
		return wrapped
	}
}

// objectLockRefusalCodes are the S3 error codes a bucket without Object Lock
// answers a retained PUT with.
//
// Three of them, because the wire is not uniform: AWS S3 answers InvalidRequest,
// while S3-compatible implementations have been observed to use InvalidArgument
// and InvalidBucketState for the same refusal. The code alone is far too broad to
// classify on — every one of them also covers ordinary bad requests — which is why
// the message has to mention Object Lock as well (see refusesObjectLock).
var objectLockRefusalCodes = map[string]struct{}{
	"InvalidRequest":         {},
	"InvalidArgument":        {},
	"InvalidBucketState":     {},
	"InvalidRetentionPeriod": {},
}

// refusesObjectLock reports whether err is a bucket refusing per-object retention
// because it has no Object Lock configuration.
//
// It matches on the code *and* the message because neither is sufficient: the
// codes above are shared with ordinary malformed requests, and the message's
// wording differs between implementations ("Bucket is missing Object Lock
// Configuration", "Object Lock configuration does not exist for this bucket").
// Normalising the message to letters and digits is what lets one check cover
// "Object Lock" and "ObjectLockConfiguration" alike.
//
// Matching on a message is unlovely, and it is deliberately the *narrower* half of
// the test rather than the only one. The alternative — treating every InvalidRequest
// as a permanent incompatibility — would park a sink for good on a transient
// request-level rejection, which is the more expensive mistake by far.
func refusesObjectLock(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	if _, ok := objectLockRefusalCodes[apiErr.ErrorCode()]; !ok {
		return false
	}
	return strings.Contains(normalise(apiErr.ErrorMessage()), "objectlock")
}

// normalise lowercases a message and drops everything that is not a letter or a
// digit, so spacing and punctuation cannot decide whether a phrase matches.
func normalise(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// credentialClassifier wraps a credential provider so that a failure to produce a
// credential is recognisable through the SDK's error chain.
//
// It exists because the SDK has no typed sentinel for "the chain produced
// nothing": the failure arrives at the caller as an operation error wrapping "get
// identity: get credentials: failed to refresh cached credentials, …", which is
// distinguishable from a network failure only by reading English. Every one of
// those layers wraps with %w, so marking the error at the point it is *created*
// makes it findable with errors.Is at the point it is handled — and keeps that
// knowledge in this package, where the SDK is, rather than in the reconciler that
// writes the condition.
//
// The wrapping is why an S3Sink using IRSA reports CredentialsResolved=False
// rather than BucketReachable=False when its role binding is wrong. Without it,
// every identity problem would read as a network problem.
type credentialClassifier struct {
	inner aws.CredentialsProvider
}

// Retrieve implements aws.CredentialsProvider.
func (c *credentialClassifier) Retrieve(ctx context.Context) (aws.Credentials, error) {
	creds, err := c.inner.Retrieve(ctx)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("%w: %w", sink.ErrCredentialsUnavailable, err)
	}
	return creds, nil
}

// Compile-time proof that this package implements the interface the write path
// declares, and that the classifier is still a credential provider. Both are
// asserted here rather than discovered at the wiring site, where a signature
// drift would surface in a file that has nothing to do with either.
var (
	_ kbs3.ObjectStore        = (*Store)(nil)
	_ aws.CredentialsProvider = (*credentialClassifier)(nil)
)
