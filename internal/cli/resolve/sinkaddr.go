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

package resolve

import (
	"net"
	"strconv"
	"strings"

	"github.com/kuberecord/kuberecord/internal/cli/exit"
	"github.com/kuberecord/kuberecord/internal/cli/options"
)

// --sink-addr: the one field of a resolved ClickHouse backend that is wrong when
// this process runs outside the cluster.
//
// Resolution answers five questions about a ClickHouseSink — address, database,
// username, credentials, dial timeout — and a laptop behind a `kubectl
// port-forward` disagrees with exactly one of them. Writing a profile covers the
// case somebody returns to, and remains the better answer for it. This flag
// covers the other one: a colleague's cluster, a one-off query, a CI job that
// forwards and then queries, where writing a file to disk is friction rather than
// convenience.
//
// # Why the shape matters more than the feature
//
// Every step of the chain announces itself on stderr, and that notice is the
// property the whole package exists to keep: a tool that silently chose where to
// read from would eventually read the wrong place and be believed. An override
// that quietly rewrote the address without changing what the notice said would
// break that property from the inside — `discovered ClickHouseSink/default
// (127.0.0.1:9000/kuberecord)` is a sentence that is false about the custom
// resource it names. So the override is *visible*: sinkAddrNote puts a marker in
// the description, and the description is what the notice prints.
//
// # Why the origin does not change
//
// A new Origin would be the obvious-looking choice and it is wrong. Discovery is
// still the step that answered — four of the five fields came from the custom
// resource it found, and the credentials came from the Secret that resource
// named. Reporting `using --sink-addr` would say the CR was never consulted. The
// override is a modifier on a step's result, so it is the *description* that
// carries it, not the step's identity. See clickHouseTarget.
//
// # Why conflicts are refused rather than ignored
//
// Three routes have no endpoint of this shape to replace: --source reads a
// location directly, a non-ClickHouse profile reads an archive, and an S3Sink is
// an object store. Accepting the flag and doing nothing with it would be a silent
// error (Invariant 4) whose symptom is a query answered from the address the user
// was trying to avoid. Each conflict is a usage error naming what it conflicts
// with and what to use instead, exactly as windowFlags refuses one bound given
// under two names with two values.

// exampleSinkAddr is the form every message below holds up as the expected one.
//
// It is assembled from the two constants diagnose.go writes its remediation with,
// so the example a user is corrected with here is character-for-character the one
// the unreachable-backend message told them to type.
const exampleSinkAddr = loopbackHost + ":" + DefaultClickHousePort

// maxPort is the largest number a TCP port can be.
const maxPort = 65535

// sinkAddr is the endpoint override this invocation was given, or the empty
// string for none.
//
// Flags is checked for nil because a resolver assembled in a test may carry none
// — the same reason declaredOperatorNamespace checks it — and a nil dereference
// here would fail every one of those tests for a reason unrelated to what they
// assert.
func (r *BackendResolver) sinkAddr() string {
	if r.Flags == nil {
		return ""
	}
	return r.Flags.SinkAddr
}

// sinkAddrNote is what a description carries when the endpoint was overridden.
//
// It is one function rather than a literal at each of the two construction sites
// so that the marker a test asserts on and the marker the notice prints cannot
// become two strings. The leading comma places it inside the parentheses the
// descriptions already use: `ClickHouseSink/default (127.0.0.1:9000/kuberecord,
// address from --sink-addr)`.
func sinkAddrNote(overridden bool) string {
	if !overridden {
		return ""
	}
	return ", address from --" + options.FlagSinkAddr
}

// validateSinkAddr rejects a value that is not shaped like host:port.
//
// It resolves nothing. A validator that looked the name up would succeed or fail
// for the same reason the dial is about to, which is two error paths reporting one
// problem — and the second of them would report it worse, because by then nobody
// knows the address came from a flag. What it does check is the shape, which the
// dial cannot report usefully: `clickhouse` reaches the driver as a hostname with
// no port and comes back as a connection failure rather than as "you left the port
// off".
//
// The port must be a number in range rather than merely present. Go's net package
// would accept a service name from /etc/services, and the value this flag receives
// is a forwarded port somebody just typed — so the reading that catches
// `127.0.0.1:900O` is worth more than the one that preserves `host:clickhouse`.
//
// An empty value means the flag was not given and is not an error.
func validateSinkAddr(value string) error {
	if value == "" {
		return nil
	}

	// Checked before SplitHostPort, which would report a URL as "too many colons"
	// and send the reader looking for a colon they cannot see the problem with.
	if scheme, _, found := strings.Cut(value, "://"); found {
		return exit.UsageErrorf("--%s %q carries the scheme %q: this is the ClickHouse native "+
			"protocol, which is addressed as host:port with no scheme, for example %s",
			options.FlagSinkAddr, value, scheme, exampleSinkAddr)
	}

	host, port, err := net.SplitHostPort(value)
	if err != nil {
		if !strings.Contains(value, ":") {
			return exit.UsageErrorf("--%s %q names no port: expected host:port, for example %s",
				options.FlagSinkAddr, value, exampleSinkAddr)
		}
		return exit.UsageErrorf("--%s %q is not host:port: an IPv6 literal is bracketed, "+
			"as in [::1]:%s", options.FlagSinkAddr, value, DefaultClickHousePort)
	}
	if host == "" {
		return exit.UsageErrorf("--%s %q names no host: expected host:port, for example %s",
			options.FlagSinkAddr, value, exampleSinkAddr)
	}
	if port == "" {
		return exit.UsageErrorf("--%s %q names no port: expected host:port, for example %s",
			options.FlagSinkAddr, value, exampleSinkAddr)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > maxPort {
		return exit.UsageErrorf("--%s %q names the port %q, which is not a number between 1 and %d: "+
			"expected host:port, for example %s",
			options.FlagSinkAddr, value, port, maxPort, exampleSinkAddr)
	}
	return nil
}

// errSinkAddrWithSource refuses the override against a location read directly.
//
// --source is the zero-infrastructure path: no custom resource, no profile, no
// cluster. There is no recorded endpoint in that route for this flag to replace,
// and a bucket or a directory is not dialled at all.
func errSinkAddrWithSource(source string) error {
	return exit.UsageErrorf("--%s and --%s cannot be given together: --%s %q reads that location "+
		"directly, so nothing recorded an endpoint for --%s to replace. Give --%s on its own for an "+
		"archive, or --%s on its own to redirect the ClickHouse the chain resolves",
		options.FlagSinkAddr, options.FlagSource, options.FlagSource, source,
		options.FlagSinkAddr, options.FlagSource, options.FlagSinkAddr)
}

// errSinkAddrWithProfile refuses the override against a profile reading an archive.
//
// A ClickHouse profile accepts it — the flag replaces that profile's
// clickhouse.addr and leaves its database, user, credential reference and TLS
// setting alone, which is the same one-field promise it makes to a custom
// resource. The two archive backends have no endpoint of this shape at all.
func errSinkAddrWithProfile(name string, backend BackendKind) error {
	return exit.UsageErrorf("--%s replaces a ClickHouse endpoint, and the profile %q names the %s "+
		"backend, which is an archive read as objects rather than a server that is dialled. "+
		"Select a %s profile with --%s, or read the archive directly with --%s",
		options.FlagSinkAddr, name, backend, BackendClickHouse, options.FlagProfile, options.FlagSource)
}

// errSinkAddrWithS3Sink refuses the override against an object store.
//
// One message for both routes that reach an S3Sink — named with --sink, or found
// by discovery because it was the only sink in the cluster — because the sentence
// a reader needs is the same either way and two spellings of it would be two
// things to keep in step. The ref names which sink it was, so neither route loses
// specificity.
func errSinkAddrWithS3Sink(ref SinkRef) error {
	return exit.UsageErrorf("--%s replaces a ClickHouse endpoint, and %s is an object store with no "+
		"endpoint of that shape: its bucket, region and endpoint URL come from the custom resource. "+
		"Name a %s with --%s, or read the archive directly with --%s s3://bucket/prefix",
		options.FlagSinkAddr, ref, KindClickHouseSink, options.FlagSink, options.FlagSource)
}
