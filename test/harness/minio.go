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

package harness

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

	. "github.com/onsi/ginkgo/v2" //nolint:revive,staticcheck
	. "github.com/onsi/gomega"    //nolint:revive,staticcheck

	"github.com/kuberecord/kuberecord/internal/sink"
	"github.com/kuberecord/kuberecord/internal/sink/s3"
)

// This file is how a suite reads an S3 archive: it runs `mc` inside the MinIO pod
// with `kubectl exec` and decodes the objects through the shipped decoder.
//
// It is deliberately the same shape as clickhouse.go, for the same reason: both
// suites should describe "the records for this object" once, whichever backend
// holds them, so a change to what a backend stores breaks the assertions rather
// than quietly changing what they mean. The two read layers are as alike as the
// backends allow — and where they differ, the difference is the backend's, not the
// harness's:
//
//   - ClickHouse is asked a question and answers it. An object store has no query
//     engine, so this reads every record object under the sink's prefix and
//     filters in Go. That is exactly what the documented DuckDB recipes do; the
//     archive is cheap to write and to keep, which is what makes it expensive to
//     interrogate (D12).
//   - Decoding goes through s3.Decode, the shipped reader, rather than through a
//     JSON reader of the suite's own. Anything the writer put in the bucket, a
//     reader gets back — that is the claim the format makes, and asserting through
//     any other decoder would be asserting it of something kuberecord does not
//     ship.
//
// `mc` runs inside the server pod rather than an S3 client running on the test
// host, which is the trade clickhouse.go explains: every assertion polls, and a
// forwarded connection that drops mid-suite is the classic e2e flake. There is no
// forwarded connection to drop this way, and the MinIO image ships `mc` and
// `base64`, so it needs no second image either.

// mcAlias is the alias name every `mc` invocation here uses. It is passed through
// MC_HOST_<alias> in the environment rather than configured with `mc alias set`,
// so no state accumulates in the pod and one exec is one self-contained command.
const mcAlias = "kr"

// probeObjectName is the object kuberecord's S3 health probe writes, relative to
// the sink's prefix. The suites need to know it for one reason: it is the one
// object in the bucket that is *not* audit data, so a suite counting records must
// be able to say so rather than tripping over it. It sits outside format=jsonl-v1
// on purpose (see internal/sink/s3/instance.go).
const probeObjectName = ".kuberecord-probe"

// The record layout's own segments, spelled here because a suite reading this
// archive is a consumer of the published contract (D15) and must state it
// independently. The constants in internal/sink/s3 are unexported, and that is
// the right way round: a harness that derived the expected layout from the
// writer's own constants could not catch a change to them.
const (
	formatSegment = "format=jsonl-v1"
	scopesSegment = "scopes"
	objectSuffix  = ".jsonl.zst"
)

// MinIO addresses one suite's object store: which pod to exec into, which
// credentials to use, which bucket and prefix the sink under test writes to, and
// which cluster_id its records carry.
//
// As with ClickHouse, cluster_id is part of the addressing rather than of each
// query: it is constant for a suite (one operator instance serves one cluster,
// Invariant 7), and leaving it out of even one question would silently widen that
// question to another run's records.
type MinIO struct {
	// Namespace and Deployment locate the MinIO pod to exec into.
	Namespace  string
	Deployment string
	// User and Password are the root credentials `mc` authenticates with. They are
	// the same values the S3Sink's credentials Secret holds, so the suite and the
	// operator are reading and writing as one identity.
	User     string
	Password string
	// Bucket and Prefix are the sink's spec.bucket and spec.prefix.
	Bucket string
	Prefix string
	// ClusterID is the value the operator under test stamps every record with.
	ClusterID string

	// mu guards objects, the decoded-object memo.
	mu sync.Mutex
	// objects memoizes each object's decoded records by key.
	//
	// Safe because an object key is the SHA-256 of its own contents (D15): an
	// object can be rewritten under its key — that is what makes a retried PUT
	// harmless — but never with different records. And necessary because every
	// assertion polls: without it, a scenario holding two dozen objects would
	// spawn two dozen `kubectl exec`s per poll and spend its budget re-reading
	// bytes that cannot have changed.
	objects map[string][]sink.Record
}

// MakeBucket makes the sink's bucket exist. It is idempotent, so a suite that
// reuses a cluster does not have to know whether a previous run got this far.
//
// It has to happen before the S3Sink is created: the sink's health probe writes,
// so a bucket that does not exist yet is a sink that reports itself unhealthy and
// then recovers on a later probe — a slower start for no gain, and a confusing
// first status for anyone watching.
func (m *MinIO) MakeBucket() {
	GinkgoHelper()
	out, err := m.run("mc mb --ignore-existing '" + m.target("") + "'")
	Expect(err).NotTo(HaveOccurred(), "failed to create bucket %q: %s", m.Bucket, out)
}

// Keys returns every object key in the bucket, oldest first.
//
// The order is the store's own (lexicographic), which for the record layout is
// also chronological down to the hour: date= and hour= partitions sort the same
// way they run, which is the practical point of the Hive-style layout. Within one
// hour the tail is a content hash and therefore unordered — a suite that cares
// about order within an hour has to read the records' own timestamps.
func (m *MinIO) Keys() ([]string, error) {
	out, err := m.run("mc ls --recursive --json '" + m.target("") + "'")
	if err != nil {
		return nil, err
	}
	var keys []string
	for line := range strings.SplitSeq(out, "\n") {
		// kubectl may interleave its own notices with the command's output, and
		// utils.Run merges stderr in; only JSON object lines are listings.
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var entry struct {
			Status string `json:"status"`
			Type   string `json:"type"`
			Key    string `json:"key"`
			Error  struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, fmt.Errorf("decode listing %q: %w", line, err)
		}
		if entry.Status != "success" {
			return nil, fmt.Errorf("listing bucket %q: %s", m.Bucket, entry.Error.Message)
		}
		if entry.Type == "file" && entry.Key != "" {
			keys = append(keys, entry.Key)
		}
	}
	return keys, nil
}

// RecordKeys returns the keys of the record objects alone — the archive proper,
// without the scope log and without the health probe's object.
//
// The split is the layout's, not this harness's: records live under a cluster_id=
// partition, the scope log under scopes/, and the probe outside format=jsonl-v1
// altogether. A reader globbing this archive makes the same split, which is why
// the suites make it here rather than counting whatever happens to be in the
// bucket.
func (m *MinIO) RecordKeys() ([]string, error) {
	keys, err := m.Keys()
	if err != nil {
		return nil, err
	}
	var records []string
	for _, key := range keys {
		if m.IsRecordKey(key) {
			records = append(records, key)
		}
	}
	return records, nil
}

// IsRecordKey reports whether a key names a record object *this sink* wrote,
// under the documented layout:
// <prefix>/format=jsonl-v1/cluster_id=<id>/date=…/hour=…/<hash>.jsonl.zst.
//
// The prefix and the cluster are part of the question, not decoration. A bucket
// may hold several sinks' archives under different prefixes and several clusters'
// records under one prefix, and a suite that read either as its own would be
// counting somebody else's history.
func (m *MinIO) IsRecordKey(key string) bool {
	segments := strings.Split(key, "/")
	return m.underPrefix(key) &&
		keyHasSegment(segments, formatSegment) &&
		keyHasSegment(segments, "cluster_id="+m.ClusterID) &&
		strings.HasSuffix(key, objectSuffix)
}

// IsScopeKey reports whether a key names a scope-log object this sink wrote.
//
// Unlike the records tree, the scope log is not partitioned by cluster: it is
// small enough that a cluster_id= level would only produce near-empty prefixes
// (see internal/sink/s3/scopewriter.go), so the prefix is all that separates two
// sinks' scope logs in one bucket.
func (m *MinIO) IsScopeKey(key string) bool {
	segments := strings.Split(key, "/")
	return m.underPrefix(key) &&
		keyHasSegment(segments, formatSegment) &&
		keyHasSegment(segments, scopesSegment) &&
		strings.HasSuffix(key, objectSuffix)
}

// IsProbeKey reports whether a key names this sink's health-probe object.
func (m *MinIO) IsProbeKey(key string) bool {
	return m.underPrefix(key) && strings.HasSuffix(key, probeObjectName)
}

// underPrefix reports whether a key sits under the sink's spec.prefix. An empty
// prefix is an ordinary configuration — a bucket dedicated to one archive — and
// admits every key, exactly as the writer's own treatment of it does.
func (m *MinIO) underPrefix(key string) bool {
	if m.Prefix == "" {
		return true
	}
	return strings.HasPrefix(key, m.Prefix+"/")
}

// keyHasSegment reports whether want is one of an object key's whole path
// segments. Whole segments, not substrings: a record filed under
// cluster_id=demo-2 must not answer a question about cluster_id=demo.
func keyHasSegment(segments []string, want string) bool {
	return slices.Contains(segments, want)
}

// Records returns every record in the archive matching the filter, in object-key
// order and, within an object, in write order.
//
// It reads the whole archive under the sink's prefix on the first call and only
// the objects it has not seen on later ones — see MinIO.objects. That is the
// honest shape of a query against this backend: there is no index, so "the
// records for this object" is a scan, and a suite pretending otherwise would be
// modelling a backend kuberecord does not have.
func (m *MinIO) Records(filter ObjectFilter) ([]sink.Record, error) {
	keys, err := m.RecordKeys()
	if err != nil {
		return nil, err
	}
	var matched []sink.Record
	for _, key := range keys {
		records, err := m.objectRecords(key)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if filter.MatchesRecord(m.ClusterID, record) {
				matched = append(matched, record)
			}
		}
	}
	return matched, nil
}

// objectRecords decodes one object, reading it from the bucket only once.
func (m *MinIO) objectRecords(key string) ([]sink.Record, error) {
	m.mu.Lock()
	cached, ok := m.objects[key]
	m.mu.Unlock()
	if ok {
		return cached, nil
	}

	payload, err := m.Object(key)
	if err != nil {
		return nil, err
	}
	records, err := s3.Decode(payload)
	if err != nil {
		return nil, fmt.Errorf("decode object %q (%d bytes): %w", key, len(payload), err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.objects == nil {
		m.objects = map[string][]sink.Record{}
	}
	m.objects[key] = records
	return records, nil
}

// Object returns one object's bytes, exactly as the store holds them.
//
// The bytes travel base64-encoded and fenced between markers because they are a
// compressed frame: `utils.Run` merges stderr into the output it returns, and
// kubectl is entitled to prepend notices of its own, so raw binary on the same
// stream could not be told apart from either.
func (m *MinIO) Object(key string) ([]byte, error) {
	// The fences carry characters base64 has none of, so nothing in the payload
	// can look like one, and they are single-quoted so the shell cannot read them
	// as redirections.
	const openFence, closeFence = "<kr>", "</kr>"
	out, err := m.run(fmt.Sprintf("printf '%%s' '%s'; mc cat '%s' | base64 -w0; printf '%%s' '%s'",
		openFence, m.target(key), closeFence))
	if err != nil {
		return nil, err
	}
	start := strings.Index(out, openFence)
	end := strings.LastIndex(out, closeFence)
	if start < 0 || end < start {
		return nil, fmt.Errorf("no fenced payload in the output for object %q: %s", key, out)
	}
	encoded := strings.TrimSpace(out[start+len(openFence) : end])
	payload, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		// The fenced text is quoted rather than only named: anything that reaches
		// here instead of base64 is a message from `mc` or the shell, and reading it
		// is the difference between "the payload was malformed" and knowing why.
		return nil, fmt.Errorf("decode the base64 payload of object %q: %w (fenced output was %q)",
			key, err, encoded)
	}
	return payload, nil
}

// target renders an `mc` target: the alias, the bucket, and a key or key prefix
// under it. An empty key addresses the bucket itself.
func (m *MinIO) target(key string) string {
	target := mcAlias + "/" + m.Bucket
	if key != "" {
		target += "/" + key
	}
	return target
}

// run executes one shell script inside the MinIO pod with `mc` configured.
//
// The configuration is *exported* rather than prefixed onto the command. A
// `VAR=value cmd` prefix applies to that one command, so anything past a `;` or a
// `|` — which is every script here that reads an object — would run without the
// alias and fail; and because a pipeline reports its last command's status, the
// failure would arrive as an empty payload rather than as an error. pipefail is
// what turns that class of failure back into a non-zero exit the caller sees.
//
// The alias is supplied through MC_HOST_<alias> rather than `mc alias set`, so
// the invocation carries its whole configuration and leaves nothing behind in the
// pod for the next one to inherit. HOME points at the pod's writable emptyDir
// because mc insists on a home directory even when it needs no config from it.
func (m *MinIO) run(script string) (string, error) {
	preamble := fmt.Sprintf("set -o pipefail; export HOME=/tmp; export MC_HOST_%s='http://%s:%s@127.0.0.1:9000'; ",
		mcAlias, m.User, m.Password)
	out, err := Kubectl("exec", "-n", m.Namespace, "deploy/"+m.Deployment, "--",
		"sh", "-c", preamble+script)
	if err != nil {
		return "", fmt.Errorf("mc %q: %w", script, err)
	}
	return out, nil
}

// MatchesRecord reports whether a record satisfies the filter. It is the
// object-store counterpart of ObjectFilter.where, and it applies exactly the same
// fields with exactly the same optionality — Group, Kind and Namespace always,
// including when empty (the empty group is the core group and the empty namespace
// is a cluster-scoped object), Name, UID and EventTypes only when set.
//
// One filter type serving both backends is the point: a scenario that has asked
// ClickHouse "how many Deleted rows does this object have?" asks an S3 archive the
// same question with the same value, and the two cannot drift into meaning
// different things.
func (f ObjectFilter) MatchesRecord(clusterID string, record sink.Record) bool {
	if record.ClusterID != clusterID ||
		record.APIGroup != f.Group ||
		record.Kind != f.Kind ||
		record.Namespace != f.Namespace {
		return false
	}
	if f.Name != "" && record.Name != f.Name {
		return false
	}
	if f.UID != "" && record.UID != f.UID {
		return false
	}
	if len(f.EventTypes) == 0 {
		return true
	}
	return slices.Contains(f.EventTypes, record.EventType)
}
