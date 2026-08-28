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

// The scan: how a set of pruned partition prefixes becomes decoded lines, in
// bounded parallel, deterministically, without holding the archive in memory.
//
// Three properties are load bearing here, and each of them is a decision rather
// than an implementation detail.
//
// **Bounded.** Objects are fetched through an errgroup with a concurrency cap, so
// peak memory is a function of that cap and not of how many objects the window
// holds. What each fetch keeps is what its accumulator chose to keep — for a
// single-object question, a handful of lines out of an object holding thousands.
//
// **Deterministic.** Every object gets its own accumulator, indexed by its position
// in the listing, and the results are read back in that order once every fetch has
// finished. A scan that appended to a shared slice would produce a result ordered
// by which fetch happened to finish first: correct only after a sort, and different
// from one run to the next in the ties. The same reasoning applies to failures —
// the error reported is the first *in key order*, not the first to be raised.
//
// **Non-cancelling between siblings, promptly cancelled by its caller.** One
// object's failure does not cancel its siblings. That is the opposite of the usual
// errgroup reflex and it is deliberate: this scan's caller delivers the lines it did
// read and reports the failure afterwards (Invariant 4), and cancelling on a failure
// would make how much it delivered a function of goroutine scheduling. An audit
// answer that is short by a different amount every time it is asked is worse than one
// that is short by a stated amount.
//
// The caller's *own* cancellation is a different thing entirely and is honoured at
// once: a scan whose context is done stops scheduling work rather than queueing one
// doomed fetch per remaining key, and reports the abandonment rather than returning a
// short result that reads like a complete one.

package objectsource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"golang.org/x/sync/errgroup"
)

// scanned holds what one object contributed, and what went wrong reading it.
type scanned[A any] struct {
	key string
	acc A
	err error
}

// scanPartitions lists and reads every object under prefixes, a group of partitions
// at a time, handing each object's accumulator to fold before the group is released.
//
// decode is called once per object, from the goroutine that fetched it, with a zero
// accumulator and the object's body. It owns the reading of that object and nothing
// else, so it needs no lock. fold is called from the scanning goroutine, in listing
// order, once per object.
//
// # Why a group at a time
//
// A group is one cap's worth of partitions, so the listings still fan out — the cap
// already serialized them into that many waves, and grouping costs no round trips. What
// it buys is the bookkeeping: a key and an accumulator slot per object, released with
// each group instead of held for the whole window. Ninety days of a busy cluster is
// hundreds of thousands of keys, and holding all of them would put an archive-sized
// figure back into a scan that has just been careful not to hold the archive.
//
// # What the returned error is
//
// The first failure in scan order — group by group, and within a group by key. In *that*
// order rather than in the order failures were raised, because an archive with two
// unreadable objects must be described the same way every time it is read; a message
// that changes between runs is one nobody can search for.
//
// An object's failure does not stop the scan: the caller delivers what was folded and
// reports this afterwards (Invariant 4). A listing failure and a cancellation both do
// stop it, for the reasons scanOneGroup states, and when they do, the reason is joined
// to whatever had already been recorded rather than replacing it — a scan that lost an
// object and was then abandoned is short for two independent reasons, and a caller
// choosing an exit code from errors.Is has to be able to find either.
func scanPartitions[A any](
	ctx context.Context, e *Engine, prefixes []string,
	decode func(*A, io.Reader) error, fold func(*A),
) error {
	var failure error
	for group := range slices.Chunk(prefixes, e.concurrency) {
		err, abort := scanOneGroup(ctx, e, group, decode, fold)
		if abort {
			// What was folded is still delivered; what stops here is the reading.
			return errors.Join(failure, err)
		}
		if err != nil && failure == nil {
			failure = err
		}
	}
	return failure
}

// scanOneGroup lists one group of partitions, reads every object under them and folds
// the accumulators in listing order.
//
// It reports the first failure in key order *and* whether the scan must stop there,
// because the two are not the same question. A failed object is a hole of known size
// and the scan carries on — it can say "this is short by that object". A failed
// listing is a hole of unknown size: nothing knows what the partition held, so an
// answer built from its neighbours would report "nothing changed" for a window that
// was never read. A cancelled context is a third thing again — a caller who has
// stopped waiting for the answer — and it stops the scan for a reason that has
// nothing to do with the archive.
//
// A cancelled group's own per-key failures are deliberately dropped. Which objects a
// group managed to fail on before it was interrupted is a function of scheduling, so
// reporting one of them would make an interrupted query describe the same archive
// differently from one run to the next. The abandonment is reported instead, and it
// reads the same every time.
func scanOneGroup[A any](
	ctx context.Context, e *Engine, prefixes []string,
	decode func(*A, io.Reader) error, fold func(*A),
) (failure error, abort bool) {
	parts, err := listObjectKeys(ctx, e, prefixes)
	if err != nil {
		return err, true
	}

	results, cancelled := fetchObjects(ctx, e, parts, decode)
	for i := range results {
		if results[i].err != nil && failure == nil {
			failure = results[i].err
		}
		fold(&results[i].acc)
	}
	if cancelled != nil {
		return abandoned(cancelled), true
	}
	return failure, false
}

// abandoned phrases a scan that stopped because its caller stopped waiting.
//
// Reporting it at all is the point. An abandoned scan that returned a short,
// successful-looking result would read as "nothing changed" for a window it never
// finished, which is exactly the failure Invariant 4 names — and it is worse here than
// a slow answer, because the reader has no way to tell the two apart.
func abandoned(cause error) error {
	return fmt.Errorf("the scan was abandoned before it had read the whole window, so this result is "+
		"short by however much was left unread: %w", cause)
}

// fetchObjects reads one group's objects with bounded parallelism, into one
// accumulator each. It returns the slots, and the cancellation that cut the group
// short if there was one.
//
// The results are indexed by the object's position in the listing and read back in that
// order, which is what keeps a scan deterministic. A scan that appended to a shared
// slice would produce a result ordered by which fetch happened to finish first: correct
// only after a sort, and different from one run to the next in the ties.
//
// The keys arrive one slice per prefix rather than merged, and are copied into the
// slots here — which is the *only* place a group's keys are materialized end to end.
// See listObjectKeys for why the merged slice this function used to be handed does not
// exist any more.
func fetchObjects[A any](
	ctx context.Context, e *Engine, parts [][]string, decode func(*A, io.Reader) error,
) ([]scanned[A], error) {
	total := 0
	for _, part := range parts {
		total += len(part)
	}

	out := make([]scanned[A], total)
	at := 0
	for i, part := range parts {
		for _, key := range part {
			out[at].key = key
			at++
		}
		// Released as it is merged, so the peak is the slots plus the parts still to
		// come rather than the slots plus every part.
		parts[i] = nil
	}

	cancelled := waitAll(ctx, e.concurrency, total, func(i int) {
		out[i].err = readObject(ctx, e, out[i].key, &out[i].acc, decode)
	})
	return out, cancelled
}

// waitAll runs n units of work with at most limit of them in flight, and returns once
// every one has finished. It reports the context's own error when the context was done
// at any point during the run, and nil otherwise.
//
// It is errgroup used as a bounded barrier rather than as an error channel, and the
// difference is the whole reason it is written out here. errgroup's own error handling
// reports whichever unit failed *first in time*, which for a set of parallel fetches is
// a function of scheduling — so a scan over an archive with two unreadable objects
// would describe it differently from one run to the next. Every unit therefore records
// its own failure in its own slot, and the caller picks the first one in a deterministic
// order. Wait is called for the ordering it establishes: every unit has finished writing
// before any slot is read.
//
// # What a cancellation does, and what it deliberately does not do
//
// A done context stops the scheduling, and any unit already queued returns without
// running. Both halves are needed: the loop can be interrupted between iterations, and
// a unit can be queued and then sit behind the limiter while the context is cancelled.
// Without them an interrupted scan of two hundred thousand keys would still queue one
// unit per remaining key, each performing an Open that fails immediately — a sub-second
// drain that nonetheless reads as an unresponsive tool to whoever pressed Ctrl-C.
//
// A unit that did not run writes *nothing* into its slot, and that is the part worth
// stating. Recording a cancellation per skipped unit would leave the caller's "first
// failure in key order" rule selecting whichever key the interruption happened to land
// on, which is a message that changes between runs of the same interrupted query.
// Skipped slots therefore stay exactly as the caller left them, and the cancellation is
// reported once — here, as the return.
func waitAll(ctx context.Context, limit, n int, run func(i int)) error {
	var group errgroup.Group
	group.SetLimit(limit)
	for i := range n {
		if ctx.Err() != nil {
			break
		}
		group.Go(func() error {
			// Checked again inside the unit: it may have been queued before the
			// cancellation and started after it.
			if ctx.Err() != nil {
				return nil
			}
			run(i)
			return nil
		})
	}
	// Cannot report anything — every function above returns nil, deliberately, for the
	// reason stated above. What an interruption looks like is the return below, which
	// is a property of the context rather than of any one unit.
	_ = group.Wait()
	return ctx.Err()
}

// readObject fetches one object and decodes it.
//
// An object named by a listing and gone by the time it is fetched is reported
// rather than swallowed, and that is a considered choice. It is an ordinary event
// in an archive under a lifecycle rule, and the seam documents the caller's correct
// response as carrying on with a *recorded* gap — so the scan carries on, and the
// gap is recorded here as this object's failure, which the caller surfaces after
// delivering everything it did read. Swallowing it would make an expired partition
// indistinguishable from a quiet one, which is the whole failure Invariant 4 names.
func readObject[A any](
	ctx context.Context, e *Engine, key string, acc *A, decode func(*A, io.Reader) error,
) (err error) {
	body, openErr := e.src.Open(ctx, key)
	if openErr != nil {
		if errors.Is(openErr, ErrKeyNotFound) {
			return fmt.Errorf("%q was listed but had gone by the time it was fetched, so this scan is "+
				"short by whatever it held: %w", key, openErr)
		}
		return fmt.Errorf("fetching %q: %w", key, openErr)
	}
	// The return is named so that this really does reach the caller. A body left
	// open is a pooled connection never returned, which shows up on the
	// hundred-thousandth object of a scan rather than the first. The close failure
	// is joined to the decode's rather than replacing it: the decode's is the one
	// that says what the scan is missing.
	defer func() {
		if closeErr := body.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("releasing %q after reading it: %w", key, closeErr))
		}
	}()

	if decodeErr := decode(acc, body); decodeErr != nil {
		return fmt.Errorf("reading %q: %w", key, decodeErr)
	}
	return nil
}

// listObjectKeys lists every prefix and returns the object keys under them, one slice
// per prefix — in prefix order, and within a prefix in the byte order the source lists
// them in.
//
// The prefixes are listed in parallel under the same cap the fetches use. A window
// of ninety days is ninety listings, and against a remote store performing them
// one after another would put ninety round trips in front of the first byte
// fetched. The results are still assembled in prefix order, because the order the
// listings *complete* in is not an order at all.
//
// Keys that do not name an object of this format are dropped rather than fetched.
// An archive's prefix may hold a health-probe object, a store's own marker or
// somebody's notes, and none of them is a frame of records.
//
// # Why the parts are not merged here
//
// Merging them would put a second full listing on the heap beside the first, live at
// the same moment: a pre-sized destination is allocated before the first element is
// copied into it, so "pre-size and append" lowers the *count* of allocations and not
// the peak. Ninety days of a busy cluster is on the order of a hundred thousand keys,
// and the merged slice's headers alone are then megabytes held for no purpose beyond
// concatenation. fetchObjects has to build a slot per object regardless, so it copies
// the keys straight into those slots and releases each part as it goes — and the
// merged slice simply stops existing.
//
// The ordering guarantee is unchanged by that, and it is unchanged *by construction*
// rather than by a sort that happens to agree: the parts are indexed by prefix, so
// prefix order is the slice's own order, and a part is exactly what one listing
// yielded.
func listObjectKeys(ctx context.Context, e *Engine, prefixes []string) ([][]string, error) {
	perPrefix := make([][]string, len(prefixes))
	errs := make([]error, len(prefixes))

	cancelled := waitAll(ctx, e.concurrency, len(prefixes), func(i int) {
		perPrefix[i], errs[i] = listOnePrefix(ctx, e, prefixes[i])
	})
	if cancelled != nil {
		// Reported ahead of the per-prefix failures rather than after them. Under a
		// cancellation those failures are whichever listings happened to be in flight,
		// so choosing between them would describe the same interrupted query
		// differently from one run to the next.
		return nil, abandoned(cancelled)
	}
	// The first failure in prefix order, for the same reason a per-object failure is
	// taken in key order: an archive must be described the same way every time it is read.
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return perPrefix, nil
}

// listOnePrefix drains one prefix's listing.
func listOnePrefix(ctx context.Context, e *Engine, prefix string) (keys []string, err error) {
	it := e.src.List(ctx, prefix)
	defer closeListing(it, &err)

	for it.Next() {
		if key := it.Object().Key; isObjectKey(key) {
			keys = append(keys, key)
		}
	}
	// The check the seam insists on: a listing that failed on its third page is a
	// listing that looks complete and merely short, and a scan built on one reports
	// "nothing changed" for a window it never read.
	if listErr := it.Err(); listErr != nil {
		return nil, fmt.Errorf("listing %q: %w", prefix, listErr)
	}
	return keys, nil
}

// closeListing releases a listing, promoting a close failure into the read's own error
// when the read otherwise succeeded.
//
// Promoting rather than discarding it matters for a reader whose result is a list: an
// iterator that could not be released may well have failed mid-listing too, and a short
// list returned with a nil error is a partial answer presented as a whole one. When the
// read has already failed the close failure is dropped, since it is usually a
// consequence of the first failure rather than news.
func closeListing(it ObjectIterator, err *error) {
	if closeErr := it.Close(); closeErr != nil && *err == nil {
		*err = fmt.Errorf("objectsource: releasing a listing: %w", closeErr)
	}
}
