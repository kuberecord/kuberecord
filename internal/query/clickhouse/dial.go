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

package clickhouse

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Dialling lives here, in the package that already holds the driver, and not in
// the caller that wants a connection.
//
// The read-plane contract says construction and credential handling belong to the
// caller (see New), and that remains true of the *decision*: what host, which
// user, where the password came from, whether TLS is wanted. What cannot live in
// the caller is the driver import. The `clickhouse-driver-is-confined` depguard
// rule admits the driver to exactly two packages, so a command-line client cannot
// so much as name driver.Conn — and confining a dependency to a package while
// requiring its type in another package's call is not a boundary, it is a
// contradiction.
//
// So the caller passes a description of the connection it wants and gets back an
// engine. New(conn) is untouched and still the way an owner of a connection lends
// one; Dial is for the caller that has none and should not have to link a driver
// to get one.

// Default timeouts for a dialled connection.
//
// They are the CLI's defaults rather than a sink's. A ClickHouseSink's
// spec.connection.readTimeout is sized for the operator's inserts — a bound on a
// write that must not wedge the hot path — and a query asked by a human is not
// that: a cold single-object timeline over a large table legitimately takes longer
// than any insert should. Copying the sink's read bound into a reader is how a CLI
// acquires a mysterious timeout at exactly the moment the answer was interesting,
// so a caller reading a sink is expected to carry over the dial bound and leave
// this one alone.
const (
	// DefaultDialTimeout bounds establishing the connection. It matches the CRD's
	// own default: reaching a host is reaching a host, whoever is asking.
	DefaultDialTimeout = 5 * time.Second

	// DefaultReadTimeout bounds a single read from the socket — one block of a
	// result, not the whole query.
	DefaultReadTimeout = 30 * time.Second
)

// clientProductName is what a dialled connection calls itself to the server.
//
// It reaches system.query_log through the client_name column, which is what makes
// a query somebody ran from a laptop distinguishable from the operator's inserts
// when a DBA is working out where load came from. The write path does not set one,
// so today every kuberecord connection looks alike; this is the half that can be
// fixed without touching the hot path.
const clientProductName = "kuberecord-cli"

// DialConfig describes a connection to open.
//
// It is deliberately not the CRD's ConnectionSpec: this package may not depend on
// the API types, and a reader has needs a writer does not (TLS, a read bound of
// its own). The overlap is spelled out field by field so a caller mapping a sink
// into it can see exactly which of its values are being carried over.
type DialConfig struct {
	// Addr is the native-protocol endpoint as "host:port".
	Addr string

	// Database is the database holding the frozen v1 tables. Empty leaves the
	// server's own default, which is almost never what a caller wants — the
	// statements this package emits are unqualified, so this field is what decides
	// where they land.
	Database string

	// Username is the user to authenticate as. Empty leaves the driver's default,
	// which is `default`.
	Username string

	// Password is that user's password, already resolved from wherever it was
	// stored. It is held in memory and handed to the driver; nothing in this
	// package logs it, renders it or puts it in an error.
	Password string

	// TLS requests a TLS connection with the platform's roots and a TLS 1.2 floor.
	//
	// It is a bool rather than a *tls.Config because the one thing a richer type
	// would buy — InsecureSkipVerify — is the one thing a tool that reads an audit
	// trail should not make easy. A private CA belongs in the system trust store,
	// where every other client on the machine will also find it.
	TLS bool

	// DialTimeout bounds establishing the connection. Zero selects
	// DefaultDialTimeout.
	DialTimeout time.Duration

	// ReadTimeout bounds a single read from the socket. Zero selects
	// DefaultReadTimeout.
	ReadTimeout time.Duration
}

// Dial opens a connection and returns an engine that owns it.
//
// Owning it is the difference from New, and it is why Close can be trusted either
// way: an engine built by New closes nothing, because the connection belongs to
// whoever lent it; an engine built by Dial closes the connection it opened,
// because nobody else has a reference to close. Both promises are the same
// sentence in the contract — Close releases what the engine itself created.
//
// No round trip happens here. The driver's Open validates the options and
// assembles a pool; the first connection is made when the first query runs. That
// is deliberate and not merely inherited: a CLI that dialled eagerly would pay a
// connection on `--help`-adjacent paths and would report an unreachable host
// before it had said which host it had chosen and why.
func Dial(cfg DialConfig) (*Engine, error) {
	if cfg.Addr == "" {
		return nil, errors.New("clickhouse query engine: an address is required to dial")
	}

	opts := &clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Protocol:    clickhouse.Native,
		DialTimeout: cfg.DialTimeout,
		ReadTimeout: cfg.ReadTimeout,
		// The anonymous struct is the driver's own declaration, repeated because
		// Go has no other way to write a value of an unnamed type.
		ClientInfo: clickhouse.ClientInfo{Products: []struct {
			Name    string
			Version string
		}{{Name: clientProductName}}},
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = DefaultDialTimeout
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = DefaultReadTimeout
	}
	if cfg.TLS {
		opts.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		// The driver's error names the option it rejected and never the credential;
		// the address is added because a caller may have several sinks configured
		// and the message is read without the invocation in front of it.
		return nil, fmt.Errorf("clickhouse query engine: opening a connection to %s: %w", cfg.Addr, err)
	}

	return &Engine{conn: conn, ownsConn: true}, nil
}
