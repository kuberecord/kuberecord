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

// Package archivetest writes a conformance history into the archive format, so
// that a query engine over the format can be asked the read plane's questions.
//
// # Why it is a package and not a test helper
//
// Two suites need it and they cannot share a test file: the conformance run over a
// directory lives in the engine's own package, and the run against a real object
// store lives in the package permitted to link the store's client. A shared
// fixture is the only way both can be seeded from the *same* history, which is
// what makes "this engine passes the suite over a directory" evidence about a
// bucket rather than about a directory.
//
// # Why it spells the format out again
//
// It writes the key layout and the line shape from its own literals rather than
// from the engine's, and that is the same discipline the write path's stand-in
// follows: a fixture that asked the implementation what the implementation expects
// would agree with it by construction, and a fixture that cannot disagree proves
// nothing. docs/SCHEMA.md is the account both are written against.
//
// # Why one record per object
//
// A real archive batches, and this fixture deliberately does not. Two properties
// come out of it. Keys are content hashes, so their listing order has nothing to
// do with time — an engine that emitted objects in the order it listed or fetched
// them produces a shuffled timeline and fails the ordering properties, where a
// single object holding pre-sorted lines would have let it pass. And it gives a
// suite a way to break one object out of many: the object holding the nth change
// is addressable, which is what an injected mid-stream failure needs.
//
// WriteObjectAt is the deliberate exception to both of those rules — one object, as
// many records as the caller names, in a partition the caller chooses — and it exists
// because the shape it writes is one no derivation can produce. See its own comment.
package archivetest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/kuberecord/kuberecord/internal/query"
	"github.com/kuberecord/kuberecord/internal/query/conformance"
)

// The key layout, written out here rather than imported. See the package comment.
const (
	formatPartition = "format=jsonl-v1"
	scopesPartition = "scopes"
	clusterSegment  = "cluster_id="
	dateSegment     = "date="
	hourSegment     = "hour="
	objectSuffix    = ".jsonl.zst"
	dateLayout      = "2006-01-02"
	hourLayout      = "15"
)

// recordLine is the physical form of one recorded change: the field names
// docs/SCHEMA.md publishes, in the order it publishes them.
//
// Note `group` and `version`, which are the logical contract's names for the API
// group and version and deliberately not the column vocabulary another backend
// spells them with. A fixture that used the other spelling would seed an archive
// no reader of this format could attribute, and every property would fail for a
// reason that had nothing to do with the engine.
type recordLine struct {
	Timestamp       time.Time         `json:"timestamp"`
	ClusterID       string            `json:"cluster_id"`
	EventType       string            `json:"event_type"`
	APIGroup        string            `json:"group"`
	APIVersion      string            `json:"version"`
	Kind            string            `json:"kind"`
	Namespace       string            `json:"namespace"`
	Name            string            `json:"name"`
	UID             string            `json:"uid"`
	ResourceVersion string            `json:"resource_version"`
	Labels          map[string]string `json:"labels"`
	Actors          []string          `json:"actors"`
	Data            string            `json:"data"`
	Diff            string            `json:"diff"`
	SHA256          string            `json:"sha256"`
}

// scopeLine is the physical form of one watch-scope transition. `ts`, not
// `timestamp`: it is a different line, not a variant of the record line.
type scopeLine struct {
	TS         time.Time `json:"ts"`
	ClusterID  string    `json:"cluster_id"`
	APIGroup   string    `json:"group"`
	APIVersion string    `json:"version"`
	Kind       string    `json:"kind"`
	Namespace  string    `json:"namespace"`
	Action     string    `json:"action"`
	RuleRef    string    `json:"rule_ref"`
}

// Put writes one object into an archive. It is the whole of what seeding needs,
// and it is deliberately not the read seam's own interface: a source that could
// write would stop being evidence that reading an archive needs only LIST and GET.
type Put func(key string, body []byte) error

// Layout is where a seeded history ended up.
//
// RecordKeys is the point of it. The keys are in the order of the changes they
// hold, oldest first, so a suite that wants to break the archive after n changes
// can name the objects holding the rest — which is how a mid-stream failure is
// injected into a backend whose stream is a set of objects rather than a cursor.
type Layout struct {
	// RecordKeys holds one key per retained change, oldest change first.
	RecordKeys []string
	// ScopeKeys holds one key per seeded scope transition, oldest first.
	ScopeKeys []string
	// Dropped is how many rows the format cannot represent and this fixture
	// therefore did not write — see Write.
	Dropped int
}

// Write encodes a history into archive objects and hands each one to put.
//
// Rows the format cannot hold are dropped rather than refused, and there is
// exactly one kind: a deletion. This archive tier is written by a Writer that
// never receives one (D12), so an archive containing a Deleted line would be a
// fixture no operator could have produced — and the engine's Deletions capability,
// which is declared false, would be certified against a history that contradicts
// it. Dropping them here is what makes the declaration and the fixture agree; the
// suite expects exactly the same drop.
//
// An object's partition comes from the record's own timestamp, in UTC, never from
// the wall clock: a fixture written today out of history dated last March belongs
// in last March's partitions, and a reader that pruned by the clock would find
// nothing.
func Write(put Put, prefix string, history conformance.History) (*Layout, error) {
	if put == nil {
		return nil, fmt.Errorf("archivetest: a Put is required to write an archive")
	}

	rows := slices.Clone(history.Rows)
	// Oldest first, so RecordKeys is addressable by change order. A stable sort
	// keeps rows recorded at the same nanosecond in the order the fixture listed
	// them, which is the order the suite's own expectation is built in.
	slices.SortStableFunc(rows, func(a, b conformance.Row) int {
		return a.Change.TS.Compare(b.Change.TS)
	})

	layout := &Layout{}
	for _, row := range rows {
		if row.Change.EventType == query.EventDeleted {
			layout.Dropped++
			continue
		}
		key, body, err := encodeRecord(prefix, row)
		if err != nil {
			return nil, err
		}
		if err := put(key, body); err != nil {
			return nil, fmt.Errorf("archivetest: write %q: %w", key, err)
		}
		layout.RecordKeys = append(layout.RecordKeys, key)
	}

	scopes := slices.Clone(history.Scopes)
	slices.SortStableFunc(scopes, func(a, b conformance.ScopeTransition) int {
		return a.TS.Compare(b.TS)
	})
	for _, transition := range scopes {
		key, body, err := encodeScope(prefix, transition)
		if err != nil {
			return nil, err
		}
		if err := put(key, body); err != nil {
			return nil, fmt.Errorf("archivetest: write %q: %w", key, err)
		}
		layout.ScopeKeys = append(layout.ScopeKeys, key)
	}
	return layout, nil
}

// WriteDir writes a history into a directory tree, which is the archive a local
// source reads.
//
// It is here rather than in each suite because the mapping from key to path is
// part of the claim a directory and a bucket answer identically: keys are
// slash-separated and become directories, with nothing else invented on the way.
func WriteDir(dir, prefix string, history conformance.History) (*Layout, error) {
	return Write(func(key string, body []byte) error {
		path := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("create the parents of %q: %w", key, err)
		}
		return os.WriteFile(path, body, 0o600)
	}, prefix, history)
}

// WriteObjectAt writes one object into the partition of at, holding every row it is
// given, whatever those rows are stamped.
//
// It exists for the one archive shape a derived key cannot express, and that shape is
// not a curiosity: an object's partition comes from its *first* record and it keeps
// accepting records until it rotates, so an object filed under hour=17 legitimately
// carries a record stamped 18:30. docs/SCHEMA.md warns every reader about it, the
// engine widens its partition range downward because of it, and the newest-first
// walk's ceiling is the same widening applied in the other direction. A fixture that
// could not produce one could not test any of that.
//
// Write is still the way to seed a history. This is the deliberate exception beside
// it rather than a second way to do the same thing: Write chooses the partition from
// the record, which is right for every fixture that is not about this, and here the
// caller chooses it and the rows are free to disagree.
//
// name is the key's last segment, before the suffix. A real archive puts a content
// hash there and nothing reading the format interprets it, so a fixture is free to
// spell out what the object is for — which is what a failure message wants to say.
func WriteObjectAt(put Put, prefix, name string, at time.Time, rows []conformance.Row) error {
	if put == nil {
		return fmt.Errorf("archivetest: a Put is required to write an archive")
	}
	if len(rows) == 0 {
		return fmt.Errorf("archivetest: WriteObjectAt needs at least one row: an empty object is a " +
			"frame a reader is entitled to reject, not a partition holding nothing")
	}

	var payload []byte
	for _, row := range rows {
		if row.Change.EventType == query.EventDeleted {
			// The same rule Write applies, refused here instead of dropped: a caller
			// naming its rows one by one has said which object it wants, and silently
			// writing a shorter one would be a fixture that is not what it says it is.
			return fmt.Errorf("archivetest: WriteObjectAt was given a %s row; this archive tier is "+
				"written by a Writer that never receives a deletion (D12), so no operator could "+
				"produce this object", query.EventDeleted)
		}
		line, err := marshalLine(recordLineOf(row))
		if err != nil {
			return err
		}
		payload = append(payload, line...)
	}

	// The partition comes from at and the cluster from the rows, which is the one
	// place those two can disagree — and the disagreement is the point.
	key := joinSegments(prefix, formatPartition, clusterSegment+rows[0].Ref.ClusterID,
		dateSegment+at.UTC().Format(dateLayout),
		hourSegment+at.UTC().Format(hourLayout),
		name+objectSuffix)
	if err := put(key, compress(payload)); err != nil {
		return fmt.Errorf("archivetest: write %q: %w", key, err)
	}
	return nil
}

// encodeRecord renders one change as its own object.
func encodeRecord(prefix string, row conformance.Row) (key string, body []byte, err error) {
	payload, err := marshalLine(recordLineOf(row))
	if err != nil {
		return "", nil, err
	}
	hash := contentHash(payload)
	key = joinSegments(prefix, formatPartition, clusterSegment+row.Ref.ClusterID,
		dateSegment+row.Change.TS.UTC().Format(dateLayout),
		hourSegment+row.Change.TS.UTC().Format(hourLayout),
		hash+objectSuffix)
	return key, compress(payload), nil
}

// recordLineOf projects a suite row onto the physical line.
//
// Shared by the two writers so that an object placed by hand and an object placed by
// the derivation hold the *same* line for the same row. A fixture whose two paths
// disagreed about a field would make the property under test depend on which writer
// happened to seed it.
func recordLineOf(row conformance.Row) recordLine {
	return recordLine{
		Timestamp:       row.Change.TS,
		ClusterID:       row.Ref.ClusterID,
		EventType:       row.Change.EventType,
		APIGroup:        row.Ref.APIGroup,
		APIVersion:      row.Change.APIVersion,
		Kind:            row.Ref.Kind,
		Namespace:       row.Ref.Namespace,
		Name:            row.Ref.Name,
		UID:             row.Change.UID,
		ResourceVersion: row.Change.ResourceVersion,
		Labels:          row.Change.Labels,
		Actors:          row.Change.Actors,
		Data:            row.Change.Data,
		Diff:            row.Change.Diff,
		SHA256:          row.Change.SHA256,
	}
}

// encodeScope renders one transition as its own object, under the date-only
// partition the scope log uses.
func encodeScope(prefix string, transition conformance.ScopeTransition) (key string, body []byte, err error) {
	line := scopeLine{
		TS: transition.TS.UTC(),
		// The transition carries no cluster of its own — the suite seeds scopes with
		// no rows beside them — so the fixture stamps the suite's, which is the value
		// a ScopeQuery will arrive asking about.
		ClusterID: conformance.FixtureClusterID,
		APIGroup:  transition.APIGroup,
		Kind:      transition.Kind,
		Namespace: transition.Namespace,
		Action:    string(transition.Action),
		RuleRef:   transition.RuleRef,
	}
	payload, err := marshalLine(line)
	if err != nil {
		return "", nil, err
	}
	hash := contentHash(payload)
	key = joinSegments(prefix, formatPartition, scopesPartition,
		dateSegment+transition.TS.UTC().Format(dateLayout), hash+objectSuffix)
	return key, compress(payload), nil
}

// marshalLine renders one line of JSONL: one JSON value, newline terminated.
//
// HTML escaping is off, matching the format: these bytes are read by people and by
// query engines rather than embedded in a page, and the two forms decode
// identically.
func marshalLine(value any) ([]byte, error) {
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return nil, fmt.Errorf("archivetest: marshal a line: %w", err)
	}
	return out.Bytes(), nil
}

// contentHash is the hex SHA-256 of the *uncompressed* payload, which is what the
// key's last segment holds.
//
// Over the uncompressed bytes deliberately, exactly as the format specifies: a
// compressor's output is not required to be bit-stable, so a key derived from the
// compressed bytes would move whenever the compression library changed and this
// fixture would stop producing the keys a reader expects to find.
func contentHash(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// compress wraps a payload in the single zstd frame every object of this format
// is. The level is not part of the contract and is not asserted anywhere.
func compress(payload []byte) []byte {
	return encoder.EncodeAll(payload, nil)
}

// encoder is shared because zstd's EncodeAll is safe for concurrent use, and a
// fixture writing a few hundred tiny objects has no reason to build one per object.
//
// Construction cannot fail with no options; the error is still read rather than
// discarded, and a nil encoder would panic on first use rather than silently
// writing plaintext objects a reader would reject.
var encoder = func() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		panic("archivetest: cannot build a zstd encoder: " + err.Error())
	}
	return enc
}()

// joinSegments joins key segments with "/", dropping empty ones — so an empty
// prefix contributes no segment rather than a leading slash.
func joinSegments(segments ...string) string {
	kept := make([]string, 0, len(segments))
	for _, s := range segments {
		if s != "" {
			kept = append(kept, s)
		}
	}
	return strings.Join(kept, "/")
}
