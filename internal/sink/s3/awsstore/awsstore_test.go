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

package awsstore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/kuberecord/kuberecord/internal/sink"
	kbs3 "github.com/kuberecord/kuberecord/internal/sink/s3"
)

// hermeticEnv detaches a test from whatever AWS configuration the machine running
// it happens to have.
//
// Without it these tests read the developer's own ~/.aws, and a laptop with a
// working profile would pass a test about a *broken* credential chain. Pointing
// both config paths at a file that does not exist is the SDK-supported way to say
// "no shared configuration"; disabling IMDS is what keeps the chain from
// attempting a 169.254.169.254 round-trip, which would make the test slow on a
// laptop and different on an EC2 instance.
func hermeticEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_PROFILE", "AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
		"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("AWS_CONFIG_FILE", "/nonexistent/kuberecord/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/nonexistent/kuberecord/credentials")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

// staticConfig is a client configuration with a key, so nothing about a test is
// decided by the ambient chain.
func staticConfig(endpoint string, pathStyle bool) kbs3.ClientConfig {
	return kbs3.ClientConfig{
		Region:         "us-east-1",
		Endpoint:       endpoint,
		ForcePathStyle: pathStyle,
		Credentials: kbs3.Credentials{
			AccessKeyID:     "AKIAKUBERECORD",
			SecretAccessKey: "secret-access-key",
		},
	}
}

// sdkClient recovers the generated client from a store, which is where the two
// MinIO-shaped settings end up.
func sdkClient(t *testing.T, store *Store) *awss3.Client {
	t.Helper()
	client, ok := store.client.(*awss3.Client)
	if !ok {
		t.Fatalf("store holds a %T, want the generated *s3.Client", store.client)
	}
	return client
}

// TestNewHonoursEndpointAndForcePathStyle is the acceptance criterion that makes
// MinIO work without special-casing: both settings must reach the client, because
// an in-cluster MinIO needs the endpoint override *and* path-style addressing (a
// bucket-as-subdomain URL only resolves where DNS covers *.<endpoint>).
//
// It asserts on the constructed client's own options rather than on a request,
// because that is where a dropped setting actually hides — a request-level
// assertion would pass for a client that ignored the endpoint and happened to be
// pointed at the test server another way.
func TestNewHonoursEndpointAndForcePathStyle(t *testing.T) {
	hermeticEnv(t)

	store, err := New(context.Background(), staticConfig("http://minio.kuberecord-system.svc:9000", true))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	opts := sdkClient(t, store).Options()

	if opts.BaseEndpoint == nil || *opts.BaseEndpoint != "http://minio.kuberecord-system.svc:9000" {
		t.Errorf("BaseEndpoint = %v, want the configured endpoint", opts.BaseEndpoint)
	}
	if !opts.UsePathStyle {
		t.Error("UsePathStyle is false; a MinIO Service name cannot be addressed virtual-host style")
	}
	if opts.Region != "us-east-1" {
		t.Errorf("Region = %q, want us-east-1", opts.Region)
	}
}

// TestNewLeavesTheEndpointUnsetForAWS pins the other half: an S3Sink with no
// endpoint must resolve AWS S3 itself from the region. Setting BaseEndpoint to the
// empty string instead of leaving it nil would send every request to a URL with no
// host.
func TestNewLeavesTheEndpointUnsetForAWS(t *testing.T) {
	hermeticEnv(t)

	store, err := New(context.Background(), staticConfig("", false))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	opts := sdkClient(t, store).Options()

	if opts.BaseEndpoint != nil {
		t.Errorf("BaseEndpoint = %q, want nil so the region resolves AWS S3", *opts.BaseEndpoint)
	}
	if opts.UsePathStyle {
		t.Error("UsePathStyle is true by default; AWS S3 wants virtual-hosted addressing")
	}
}

// TestNewRejectsAnUnusableConfiguration covers the two failures that must be
// reported by name here rather than as a mysteriously unreachable bucket later.
func TestNewRejectsAnUnusableConfiguration(t *testing.T) {
	hermeticEnv(t)

	if _, err := New(context.Background(), kbs3.ClientConfig{}); err == nil {
		t.Error("New accepted a configuration with no region")
	}
	if _, err := New(context.Background(), staticConfig("http://[::1]:bad-port/", false)); err == nil {
		t.Error("New accepted an endpoint that is not a URL")
	}
}

// TestStaticCredentialsWinOutright proves a sink naming a Secret authenticates as
// that Secret and nothing else. The ambient environment here carries a different
// key on purpose: if the chain were consulted at all, the retrieved credential
// would be the environment's.
func TestStaticCredentialsWinOutright(t *testing.T) {
	hermeticEnv(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAFROMTHEENVIRONMENT")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "environment-secret")

	store, err := New(context.Background(), staticConfig("", false))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	creds, err := sdkClient(t, store).Options().Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if creds.AccessKeyID != "AKIAKUBERECORD" {
		t.Errorf("AccessKeyID = %q, want the key from the Secret", creds.AccessKeyID)
	}
}

// TestAmbientChainIsUsedWhenNoKeyIsGiven is the IRSA/instance-role case: an
// S3Sink that omits spec.credentials must authenticate from the environment, so no
// long-lived key has to exist in the cluster at all.
func TestAmbientChainIsUsedWhenNoKeyIsGiven(t *testing.T) {
	hermeticEnv(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAAMBIENT")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "ambient-secret")

	store, err := New(context.Background(), kbs3.ClientConfig{Region: "eu-central-1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	creds, err := sdkClient(t, store).Options().Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if creds.AccessKeyID != "AKIAAMBIENT" {
		t.Errorf("AccessKeyID = %q, want the ambient chain's key", creds.AccessKeyID)
	}
}

// TestABrokenAmbientChainIsClassifiedAsCredentials is the reason
// credentialClassifier exists, and it pins the SDK's error wrapping as well as our
// own: with no key anywhere, the failure must arrive at the caller satisfying
// errors.Is(err, sink.ErrCredentialsUnavailable) *through* the SDK's operation,
// identity and cache layers.
//
// If a future SDK stopped wrapping with %w somewhere in that chain, an IRSA
// misconfiguration would silently start reporting itself as an unreachable bucket
// — sending an operator to read firewall rules about a broken role binding. This
// test is what makes that a build failure instead.
func TestABrokenAmbientChainIsClassifiedAsCredentials(t *testing.T) {
	hermeticEnv(t)

	store, err := New(context.Background(), kbs3.ClientConfig{Region: "us-east-1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	err = store.PutObject(context.Background(), kbs3.PutObjectInput{
		Bucket: "audit", Key: "probe", Body: []byte("{}\n"),
	})
	if err == nil {
		t.Fatal("PutObject succeeded with no credentials available")
	}
	if !errors.Is(err, sink.ErrCredentialsUnavailable) {
		t.Errorf("error %v is not classified as sink.ErrCredentialsUnavailable", err)
	}
	if errors.Is(err, kbs3.ErrBucketIncompatible) {
		t.Errorf("error %v is classified as a bucket incompatibility", err)
	}
}

// recordedRequest is one request the fake object store observed.
type recordedRequest struct {
	method string
	path   string
	host   string
	body   []byte
	header http.Header
}

// fakeS3 is an HTTP stand-in for S3 that records what it was sent and answers
// every PUT with a 200. It exists to assert the *wire* shape of a PUT — the path
// the addressing style produces, the body, the Object Lock headers — which is the
// one thing an in-process fake of the SDK client cannot show.
type fakeS3 struct {
	mu       sync.Mutex
	requests []recordedRequest
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	f.mu.Lock()
	f.requests = append(f.requests, recordedRequest{
		method: r.Method, path: r.URL.Path, host: r.Host, body: body, header: r.Header.Clone(),
	})
	f.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) recorded() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

// TestPutObjectWritesTheBodyAtThePathStyleKey drives the real SDK client against
// an HTTP stand-in, so what is asserted is the request that would reach MinIO.
func TestPutObjectWritesTheBodyAtThePathStyleKey(t *testing.T) {
	hermeticEnv(t)
	fake := &fakeS3{}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	store, err := New(context.Background(), staticConfig(srv.URL, true))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("Close: %v", closeErr)
		}
	})

	body := []byte("{\"cluster_id\":\"kind\"}\n")
	in := kbs3.PutObjectInput{
		Bucket: "audit",
		Key:    "archive/format=jsonl-v1/cluster_id=kind/date=2026-08-23/hour=05/abc.jsonl.zst",
		Body:   body,
	}
	if err := store.PutObject(context.Background(), in); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	got := fake.recorded()
	if len(got) != 1 {
		t.Fatalf("the store received %d requests, want 1", len(got))
	}
	req := got[0]
	if req.method != http.MethodPut {
		t.Errorf("method = %q, want PUT", req.method)
	}
	if want := "/" + in.Bucket + "/" + in.Key; req.path != want {
		t.Errorf("path = %q, want %q — path-style addressing puts the bucket in the path", req.path, want)
	}
	if string(req.body) != string(body) {
		t.Errorf("body = %q, want %q", req.body, body)
	}
	// No retention was configured, so the object must carry none of its own: the
	// bucket's default retention, if it has one, is then what applies.
	for _, header := range []string{"X-Amz-Object-Lock-Mode", "X-Amz-Object-Lock-Retain-Until-Date"} {
		if v := req.header.Get(header); v != "" {
			t.Errorf("%s = %q on an unretained object, want it absent", header, v)
		}
	}
}

// TestPutObjectCarriesTheRetentionHeaders proves spec.objectLock reaches the wire.
// It is the whole compliance claim of the backend: an object written without these
// headers is deletable by whoever wrote it.
func TestPutObjectCarriesTheRetentionHeaders(t *testing.T) {
	hermeticEnv(t)
	fake := &fakeS3{}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	store, err := New(context.Background(), staticConfig(srv.URL, true))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	retainUntil := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	err = store.PutObject(context.Background(), kbs3.PutObjectInput{
		Bucket:    "audit",
		Key:       "k",
		Body:      []byte("{}\n"),
		Retention: &kbs3.Retention{Mode: "COMPLIANCE", RetainUntil: retainUntil},
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	got := fake.recorded()
	if len(got) != 1 {
		t.Fatalf("the store received %d requests, want 1", len(got))
	}
	if mode := got[0].header.Get("X-Amz-Object-Lock-Mode"); mode != "COMPLIANCE" {
		t.Errorf("X-Amz-Object-Lock-Mode = %q, want COMPLIANCE", mode)
	}
	// The header is an ISO-8601 instant; what matters is that it is *this* instant,
	// since a retried PUT must repeat the request rather than re-date it.
	date := got[0].header.Get("X-Amz-Object-Lock-Retain-Until-Date")
	parsed, err := time.Parse(time.RFC3339, date)
	if err != nil {
		t.Fatalf("X-Amz-Object-Lock-Retain-Until-Date %q is not an RFC3339 instant: %v", date, err)
	}
	if !parsed.Equal(retainUntil) {
		t.Errorf("retain-until = %s, want %s", parsed, retainUntil)
	}
}

// stubAPI is an in-process stand-in for the generated client, so classification
// can be driven from a chosen error rather than from a bucket that has to be
// persuaded to produce one.
type stubAPI struct{ err error }

func (s stubAPI) PutObject(context.Context, *awss3.PutObjectInput,
	...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	return nil, s.err
}

// TestClassification is the table the sink contract's vocabulary is decided by:
// which failures are permanent and need a human, and which are reachability the
// manager must keep retrying.
func TestClassification(t *testing.T) {
	lockRefusal := &smithy.GenericAPIError{
		Code:    "InvalidRequest",
		Message: "Bucket is missing Object Lock Configuration",
	}
	retention := &kbs3.Retention{Mode: "GOVERNANCE", RetainUntil: time.Now().Add(time.Hour)}

	cases := []struct {
		name         string
		err          error
		retention    *kbs3.Retention
		incompatible bool
	}{
		{
			name:         "a retained PUT refused by a bucket without Object Lock is permanent",
			err:          lockRefusal,
			retention:    retention,
			incompatible: true,
		},
		{
			// The same wire error on an unretained PUT cannot be about Object Lock,
			// whatever it says: this sink did not ask for any. Reading it as a
			// permanent incompatibility would park a working sink for good.
			name:      "the same error without retention configured is not about Object Lock",
			err:       lockRefusal,
			retention: nil,
		},
		{
			name:      "an ordinary bad request is not an incompatibility",
			err:       &smithy.GenericAPIError{Code: "InvalidRequest", Message: "Missing required header"},
			retention: retention,
		},
		{
			name:      "an access denial is reachability, not incompatibility",
			err:       &smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"},
			retention: retention,
		},
		{
			name:      "a transport failure is reachability",
			err:       errors.New("dial tcp 10.0.0.1:9000: connect: connection refused"),
			retention: retention,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &Store{client: stubAPI{err: tc.err}, closeIdle: func() {}}
			err := store.PutObject(context.Background(), kbs3.PutObjectInput{
				Bucket: "audit", Key: "k", Body: []byte("{}\n"), Retention: tc.retention,
			})
			if err == nil {
				t.Fatal("PutObject reported success for a failing store")
			}
			if got := errors.Is(err, kbs3.ErrBucketIncompatible); got != tc.incompatible {
				t.Errorf("ErrBucketIncompatible = %t, want %t (error: %v)", got, tc.incompatible, err)
			}
			// The backend's own words survive in every case, so the condition
			// message a reconciler writes says what actually happened.
			if !errors.Is(err, tc.err) {
				t.Errorf("error %v no longer wraps the store's own error", err)
			}
		})
	}
}

// closableClient is an HTTP client that can release its pool, like the SDK's
// frozen one.
type closableClient struct{ closed int }

func (c *closableClient) Do(*http.Request) (*http.Response, error) { return nil, errors.New("unused") }
func (c *closableClient) CloseIdleConnections()                    { c.closed++ }

// plainClient cannot, standing in for a future SDK that hands back something else.
type plainClient struct{}

func (plainClient) Do(*http.Request) (*http.Response, error) { return nil, errors.New("unused") }

// TestIdleCloserReleasesThePoolWhenItCan covers both halves of Close: a client
// that can release its connections is asked to, and one that cannot degrades to a
// no-op rather than panicking on a nil call — a recycled sink must not take the
// process down over a connection pool.
func TestIdleCloserReleasesThePoolWhenItCan(t *testing.T) {
	client := &closableClient{}
	store := &Store{client: stubAPI{}, closeIdle: idleCloserFor(client)}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if client.closed != 1 {
		t.Errorf("CloseIdleConnections called %d times, want 1", client.closed)
	}

	plain := &Store{client: stubAPI{}, closeIdle: idleCloserFor(plainClient{})}
	if err := plain.Close(); err != nil {
		t.Errorf("Close on a client with no pool to release: %v", err)
	}
}

// TestTheSDKsFrozenClientCanReleaseItsPool is the assumption idleCloserFor is
// built on, asserted rather than assumed: the SDK's own default HTTP client, once
// frozen, exposes the release. If an upgrade changed that, Close would silently
// stop releasing anything — so the degradation documented in idleCloserFor stays a
// documented fallback rather than the everyday path.
func TestTheSDKsFrozenClientCanReleaseItsPool(t *testing.T) {
	hermeticEnv(t)
	store, err := New(context.Background(), staticConfig("", false))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var frozen aws.HTTPClient = sdkClient(t, store).Options().HTTPClient
	if _, ok := frozen.(interface{ CloseIdleConnections() }); !ok {
		t.Errorf("the SDK's HTTP client (%T) can no longer release its idle connections", frozen)
	}
}
