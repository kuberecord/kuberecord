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

package cli

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// Which object a timeline is about, when a name has belonged to several.
//
// Kubernetes reuses names freely, and a (namespace, name) pair may span any
// number of UIDs. A timeline that spliced a deleted Deployment's history onto its
// replacement's would be a coherent-looking account of something that never
// happened (Invariant 7), and one that showed a single incarnation without
// admitting the others is the same mistake told quietly.
//
// # Why the CLI chooses rather than the engine
//
// query.TimelineQuery treats an empty UID as "the newest incarnation in the
// window", so the engine would happily choose. The choice is made here anyway,
// from the same listing the banner is built from, because the header names a UID
// and the banner names a count: taking those two from one listing and the rows
// from a separate implicit decision is how a header and a table come to disagree
// about which object they describe. Pinning what was listed makes that
// impossible rather than unlikely.

// incarnationChoice is the outcome of that decision.
type incarnationChoice struct {
	// uid is the incarnation the header names. Empty when nothing could be
	// listed and no row has yet supplied one.
	uid string

	// pinned is the UID handed to the query. Empty means the engine decides —
	// either because every incarnation was asked for, or because the listing
	// failed and there is nothing to pin.
	pinned string

	// listed is every UID in the window, set only when all of them are being
	// shown. Its presence is what gives the table its UID column.
	listed []string
}

// selectIncarnation decides which incarnation to show and what to say about the
// others.
//
// A failure to list them degrades rather than fails: the timeline is still
// answerable, and ending the release's flagship command because a header field
// could not be filled would trade the whole answer for one line of it
// (Invariant 5). What it must not do is degrade silently, so every path that
// gives up returns a notice.
func selectIncarnation(
	ctx context.Context, engine query.QueryEngine, request TimelineRequest, from, to time.Time,
) (incarnationChoice, []render.Notice) {
	incarnations, err := engine.Incarnations(ctx, request.Ref, from, to)
	if err != nil {
		return incarnationChoice{uid: request.UID, pinned: pinnedUID(request)},
			[]render.Notice{{Text: describeIncarnationFailure(err), Warning: true}}
	}

	uids := make([]string, 0, len(incarnations))
	for _, incarnation := range incarnations {
		uids = append(uids, incarnation.UID)
	}

	switch {
	case request.UID != "":
		return incarnationChoice{uid: request.UID, pinned: request.UID}, pinnedNotices(request, uids)
	case request.AllIncarnations:
		return incarnationChoice{uid: newestUID(incarnations), listed: uids}, nil
	}

	newest := newestUID(incarnations)
	return incarnationChoice{uid: newest, pinned: newest}, otherIncarnationNotices(request, incarnations, newest)
}

// pinnedUID is the UID the query carries when nothing could be listed.
//
// It is the user's own --uid or nothing at all; --all-incarnations deliberately
// yields nothing, because the query's AllIncarnations flag is what expresses it
// and a UID set alongside would override the flag outright.
func pinnedUID(request TimelineRequest) string {
	if request.AllIncarnations {
		return ""
	}
	return request.UID
}

// describeIncarnationFailure phrases a listing failure as what the reader loses
// by it.
func describeIncarnationFailure(err error) string {
	if errors.Is(err, query.ErrCapabilityUnsupported) {
		return "this backend cannot list incarnations, so if this name has belonged to more than " +
			"one object, the others are not named here"
	}
	return fmt.Sprintf("the incarnations of this name could not be listed (%v), so the timeline "+
		"below is whichever one the backend chose", err)
}

// newestUID picks the incarnation a bare invocation shows.
//
// query.Incarnations returns them oldest first by FirstSeen, and the newest by
// that measure is the object wearing the name now — which is what somebody
// investigating right now almost always means. LastSeen breaks a tie, because two
// incarnations whose first rows share a timestamp are separated by which one is
// still producing rows.
func newestUID(incarnations []query.Incarnation) string {
	newest := ""
	var first, last time.Time
	for _, incarnation := range incarnations {
		if newest == "" || incarnation.FirstSeen.After(first) ||
			(incarnation.FirstSeen.Equal(first) && incarnation.LastSeen.After(last)) {
			newest, first, last = incarnation.UID, incarnation.FirstSeen, incarnation.LastSeen
		}
	}
	return newest
}

// pinnedNotices reports a --uid that names nothing in the window.
//
// Not an error: the UID may be perfectly real and simply outside --since, and
// refusing the command would make the user guess which of the two it was. The
// notice names the alternative that is actually there.
func pinnedNotices(request TimelineRequest, uids []string) []render.Notice {
	if len(uids) == 0 || slices.Contains(uids, request.UID) {
		return nil
	}
	return []render.Notice{{
		Text: fmt.Sprintf("no changes are recorded for uid %s in this window; the incarnations that "+
			"are here are %s", request.UID, strings.Join(uids, ", ")),
		Warning: true,
	}}
}

// otherIncarnationNotices is the banner that keeps a single-incarnation timeline
// honest.
//
// It says how many others there are and which flag shows them, because a reader
// who is not told cannot know that the history they are looking at begins where
// another object's ended.
func otherIncarnationNotices(
	request TimelineRequest, incarnations []query.Incarnation, newest string,
) []render.Notice {
	others := len(incarnations) - 1
	if others < 1 {
		return nil
	}
	return []render.Notice{{
		Text: fmt.Sprintf("%s has had %s in this window; showing the newest (%s). "+
			"Pass --all-incarnations to see them all, or --uid to pin one",
			describeObject(request.Ref), pluralIncarnations(len(incarnations)), newest),
	}}
}

// pluralIncarnations spells the count so the banner reads as a sentence.
func pluralIncarnations(count int) string {
	if count == 2 {
		return "2 incarnations"
	}
	return fmt.Sprintf("%d incarnations", count)
}

// distinctUIDs reads the incarnations out of the rows themselves.
//
// It is the fallback for --all-incarnations when the listing failed: the table
// must carry a UID column whenever it may span more than one incarnation, and a
// column driven by a listing that did not happen would be absent exactly when it
// mattered.
func distinctUIDs(rows []render.TimelineRow) []string {
	var uids []string
	for _, row := range rows {
		if row.Change.EventType == query.EventKubernetes {
			continue
		}
		if !slices.Contains(uids, row.Change.UID) {
			uids = append(uids, row.Change.UID)
		}
	}
	return uids
}

// firstObjectUID is the incarnation the rows themselves name.
//
// It is the fallback for a header whose UID could not be listed, and it skips
// merged Kubernetes Event rows for the reason every other reader of these rows
// does: an Event row's UID is the Event object's, so with --with-events the
// newest row can easily be one, and the header would name the identity of a
// message about the object rather than of the object.
func firstObjectUID(rows []render.TimelineRow) string {
	for _, row := range rows {
		if row.Change.EventType != query.EventKubernetes {
			return row.Change.UID
		}
	}
	return ""
}

// resolveKindOffline reads an address that needs no discovery data.
//
// It exists because reading an archive is a supported way to work (D18): the
// cluster the changes happened in may be unreachable, or gone. What it accepts is
// the identity the schema itself stores — a capitalised Kind, optionally
// qualified with its group — and nothing else. It will not expand `deploy` or
// singularize `deployments`, because both need the server's own discovery data
// and guessing at them would silently read a different object's history.
//
// namespaced is the caller's, not this function's, because offline there is
// nothing to ask: the only honest reading is that the object is in the namespace
// the user named, and is cluster-scoped when they named none.
func resolveKindOffline(arg ResourceArg, namespaced bool) (ResolvedResource, error) {
	gvk, ok := parseRecordedKind(arg.Resource)
	if !ok {
		return ResolvedResource{}, fmt.Errorf(
			"%q cannot be resolved without one: short names and plural resource names come from the "+
				"cluster's own discovery data. Give the kind as it is recorded — Deployment/%s or "+
				"Deployment.apps/%s — which needs no cluster at all",
			arg.Resource, arg.Name, arg.Name)
	}
	return ResolvedResource{
		GVK:        gvk,
		GVR:        gvk.GroupVersion().WithResource(strings.ToLower(gvk.Kind) + "s"),
		Namespaced: namespaced,
		Name:       arg.Name,
	}, nil
}
