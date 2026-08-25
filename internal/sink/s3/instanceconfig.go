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

package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/kuberecord/kuberecord/internal/sink"
)

// Credentials is one S3Sink's static access key, already read out of the Secret
// its spec.credentials.secretRef names.
//
// Its *zero value is meaningful and supported*: it means "authenticate from the
// ambient credential chain" — IRSA, workload identity, or an instance role — which
// is the shape a cloud deployment should prefer, since no long-lived key then
// exists to leak or to rotate. That is why this is a plain struct rather than a
// pointer or an interface: the absence of a key is a state the configuration has to
// carry and fingerprint like any other, not a nil to guard against at every use.
//
// It holds the secret in clear because it must — the AWS SDK needs the bytes to
// sign with — so it travels exactly as far as it has to: the reconciler reads it,
// the configuration builder puts it here, the client constructor consumes it. It
// is never logged, and Fingerprint below is what keeps it out of the one place a
// configuration is otherwise rendered for humans.
type Credentials struct {
	// AccessKeyID and SecretAccessKey are the two halves of a static key. Both
	// empty means the ambient chain; see IsAmbient.
	AccessKeyID     string
	SecretAccessKey string

	// SessionToken is set only for temporary credentials (an assumed role's key
	// handed to the operator through a Secret). It is optional, and an empty value
	// on an otherwise complete key is the ordinary long-lived-key case rather than
	// a half-finished one.
	SessionToken string
}

// IsAmbient reports whether these credentials say "resolve me from the
// environment" rather than carrying a key.
//
// It is deliberately *both* halves being empty rather than either, so a Secret
// that carries one key and not the other cannot be mistaken for a request to use
// the ambient chain. Such a Secret is a mistake the reconciler rejects with
// CredentialsResolved=False, which is a far more useful report than silently
// authenticating as whatever identity the pod happens to have.
func (c Credentials) IsAmbient() bool {
	return c.AccessKeyID == "" && c.SecretAccessKey == "" && c.SessionToken == ""
}

// ClientConfig is everything that decides *how the bucket is reached*, as opposed
// to what is written into it (Config).
//
// The split is the one client.go describes: everything that differs between AWS S3
// and a MinIO deployment — region, endpoint, addressing style, credential chain —
// is resolved once, when the client is constructed, so that writing an object is
// the same code path against either. It carries no bucket, because the bucket
// travels on each PutObjectInput: a client bound to a bucket would make a misrouted
// object an invisible property of a client somebody constructed elsewhere.
type ClientConfig struct {
	// Region is spec.region. The SDK requires one even against MinIO, which
	// ignores it.
	Region string

	// Endpoint is spec.endpoint: an absolute URL overriding AWS S3's own resolved
	// endpoint, or empty for AWS S3. The CRD's Pattern has already established
	// that a non-empty value carries a scheme.
	Endpoint string

	// ForcePathStyle is spec.forcePathStyle: address the bucket as
	// <endpoint>/<bucket>/<key> rather than as <bucket>.<endpoint>/<key>. It is
	// what most in-cluster MinIO deployments need, since a bucket-as-subdomain URL
	// only resolves where DNS (and any TLS certificate) covers *.<endpoint>.
	ForcePathStyle bool

	// Credentials is the static key from spec.credentials.secretRef, or the zero
	// value for the ambient chain.
	Credentials Credentials
}

// SinkConfig is one S3Sink's fully-resolved backend configuration: how to reach
// the bucket, and how to write to it.
//
// It is the sink.InstanceConfig the SinkManager diffs and builds an instance from,
// which is why it is one type covering both halves rather than two passed
// separately: the recycle decision is about the sink as a whole, and a
// configuration split across two values is a configuration whose fingerprint can
// silently cover only one of them.
type SinkConfig struct {
	// Client decides how the bucket is reached.
	Client ClientConfig

	// Writer decides what is written and when it is rotated. See Config.
	Writer Config
}

// Fingerprint implements sink.InstanceConfig: a digest of every setting a running
// instance of this sink is built from, so the SinkManager can tell "the same sink,
// re-reconciled" from "the same sink, reconfigured".
//
// Every field participates, the secret access key and session token included — a
// rotated credential is precisely the change that must force a recycle, and it is
// the case Task 6.4's acceptance criteria single out. The result is a SHA-256
// digest rather than the settings themselves because fingerprints are compared
// *and logged*: rendering the key in clear here would put an operator's S3
// credential in the operator's log the first time a sink was recycled.
//
// Values are rendered with %q (durations and the object-lock policy as strings), so
// no pair of neighbouring fields can be re-split into another configuration's
// digest — a prefix of "a" with bucket "bc" must not fingerprint like "ab" with
// "c". The same discipline as clickhouse.Config.Fingerprint, deliberately: the two
// are read side by side whenever a recycle is being explained.
func (c SinkConfig) Fingerprint() string {
	h := sha256.New()
	// Errors from a hash writer are impossible by contract (hash.Hash never
	// returns one), so Fprintf's return is not fallible in any way a caller could
	// act on — but it is still read and checked rather than discarded, so this
	// stays clean under Invariant 4's no-silent-errors rule. See the branch below
	// for what the unreachable constant is and is not.
	if _, err := fmt.Fprintf(h,
		"region=%q endpoint=%q pathstyle=%t akid=%q secret=%q token=%q "+
			"bucket=%q prefix=%q maxbytes=%d maxage=%q lock=%q "+
			"queue=%d workers=%d enqueue=%q drain=%q",
		c.Client.Region, c.Client.Endpoint, c.Client.ForcePathStyle,
		c.Client.Credentials.AccessKeyID, c.Client.Credentials.SecretAccessKey,
		c.Client.Credentials.SessionToken,
		c.Writer.Bucket, c.Writer.Prefix, c.Writer.MaxObjectBytes, c.Writer.MaxObjectAge.String(),
		objectLockFingerprint(c.Writer.ObjectLock),
		c.Writer.QueueSize, c.Writer.Workers,
		c.Writer.EnqueueTimeout.String(), c.Writer.DrainTimeout.String(),
	); err != nil {
		// Unreachable by hash.Hash's contract: its Write never returns an error, so
		// Fprintf to one cannot fail. The constant is a tombstone, not a mitigation
		// — if this branch were ever reached, every configuration would digest
		// identically and every reconfiguration would look like a no-op. Nothing
		// reports it because nothing here can: this method has no logger, and
		// returning an error would put a failure mode into sink.InstanceConfig that
		// no implementation of it has. Deliberately identical to
		// clickhouse.Config.Fingerprint's branch; the two are read side by side.
		return "unfingerprintable"
	}
	return hex.EncodeToString(h.Sum(nil))
}

// objectLockFingerprint renders spec.objectLock for the digest.
//
// "none" is spelled out rather than rendered as an empty string so that removing
// the block and setting it to a zero-valued mode cannot produce the same digest:
// the difference between "write objects with no retention of their own" and "write
// them retained" is the whole point of the field.
func objectLockFingerprint(lock *ObjectLock) string {
	if lock == nil {
		return "none"
	}
	return fmt.Sprintf("%s/%d", lock.Mode, lock.RetainDays)
}

// Compile-time proof that this package's configuration is what the sink runtime
// diffs. It is asserted here, beside the digest, rather than at the wiring site
// where a signature drift would surface far from the type that caused it.
var _ sink.InstanceConfig = SinkConfig{}
