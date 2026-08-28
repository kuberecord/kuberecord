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
	"context"
	"fmt"
	"time"

	"github.com/kuberecord/kuberecord/internal/query"
)

// Incarnations lists the distinct UIDs recorded under one name in a window,
// oldest first.
//
// It exists so a caller can say "there are two other incarnations of this name"
// before rendering one of them. Kubernetes reuses names freely, and a timeline
// that showed one incarnation without admitting the others is the same splice
// Invariant 7 forbids, told quietly.
//
// Deleted reports what the *history* holds, and nothing more. False means no
// deletion was recorded — which is not a claim that the object still exists, and
// on a backend that cannot record deletions at all never could be. This backend
// can, so here the two readings coincide; a renderer must still qualify the field
// by Capabilities().Deletions rather than learn the habit of trusting it.
func (e *Engine) Incarnations(
	ctx context.Context, ref query.ObjectRef, from, to time.Time,
) (incarnations []query.Incarnation, err error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}

	stmt := incarnationsStatement(ref, from, to)
	rows, err := e.conn.Query(ctx, stmt.SQL, stmt.Args...)
	if err != nil {
		return nil, fmt.Errorf("reading the incarnations of %s: %w", describeRef(ref), err)
	}
	defer closeAfter(rows, &err)

	for rows.Next() {
		var (
			inc       query.Incarnation
			deletions uint64
		)
		scanErr := rows.Scan(&inc.UID, &inc.FirstSeen, &inc.LastSeen, &deletions)
		if scanErr != nil {
			return nil, fmt.Errorf("decoding an incarnation of %s: %w", describeRef(ref), scanErr)
		}
		inc.Deleted = deletions > 0
		incarnations = append(incarnations, inc)
	}
	// A short list here is worse than an error: it is the answer to "how many other
	// objects have worn this name", and one that silently omitted an incarnation
	// would let a caller render a single timeline as the whole story.
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("streaming the incarnations of %s: %w", describeRef(ref), rowsErr)
	}
	return incarnations, nil
}
