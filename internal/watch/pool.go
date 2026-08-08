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

package watch

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/yelzhy/kuberecord/internal/pipeline"
)

// resyncPeriod is 0 for every informer in the pool: no periodic resync. This is a
// recorded decision (Task 2.3), re-confirmed against measurement, not an omission.
//
// A resync re-delivers every cached object to the handlers at a fixed interval.
// The classic reason to want that — a reconciler that may have failed to act on
// an edge and needs a nudge — does not apply here: the pipeline is level-
// triggered per key (it reads current state, not the event), the workqueue owns
// retries with backoff, and a failed write re-adds its own key. So a resync would
// buy nothing and cost a full cache sweep per interval, which at cluster scale
// (D2) is exactly the kind of periodic work that turns a quiet operator into a
// noisy one. Watch-driven only, therefore, with the WatchManager's own 30s pool
// diff as the level-triggering safety net.
//
// The cost side of that decision is now measured rather than asserted. Every
// re-delivered object costs a full work item that settles on the dedup path — a
// lister read, a normalize, a marshal and a hash — which after Task 2.3's
// allocation work is ~36 KB of garbage and ~66 µs of CPU for a realistic Pod
// (BenchmarkProcessDedup in internal/pipeline). At the massive profile's 20,000
// objects that is hundreds of megabytes of allocation and seconds of CPU per
// sweep, spent entirely to re-derive "nothing changed" — against a churn window
// that costs 0.43 cores in total (docs/PERFORMANCE.md). A resync would be the
// single largest source of avoidable work in the process.
const resyncPeriod = 0

// stopWaitTimeout bounds how long stopping one informer waits for its goroutine
// to return before giving up and logging the leak.
//
// It is generous because a clean stop is the norm and the wait is off any hot
// path, but it is bounded because a wedged informer goroutine must never be able
// to hold up a rule deletion (or process shutdown) indefinitely — the operator
// degrades with a loud Error log instead (Invariant 5).
const stopWaitTimeout = 30 * time.Second

// Enqueuer is the pool's hand-off to the data plane: one call per interested sink
// per event.
//
// It is deliberately the narrowest possible interface — no error, no blocking, no
// return value — because it is called from an informer's notification goroutine,
// where anything that can block is a violation of Invariant 1. pipeline.Pipeline
// satisfies it (see the assertion in manager.go).
type Enqueuer interface {
	// Add enqueues one identity key for processing. It must not block.
	Add(key pipeline.Key)
}

// informerEntry is one running informer plus everything needed to stop it and to
// read from it.
type informerEntry struct {
	// key is the (GVR, namespace) identity this informer serves.
	key informerKey

	// gvk is the kind the GVR was resolved from, carried so the handler can key
	// work items by Kind without consulting discovery on the event path.
	gvk schema.GroupVersionKind

	// informer is the shared informer; its indexer is the watch cache the
	// pipeline reads through WatchManager.Get.
	informer cache.SharedIndexInformer

	// cancel stops this informer independently of its siblings. Per-target
	// cancellation is the whole point of hand-building the pool: controller-
	// runtime's cache has one lifetime, fixed at manager construction, and no way
	// to retire a single watch.
	cancel context.CancelFunc

	// stopped is closed when the informer's Run goroutine has returned, which is
	// what makes "the watch is gone" an observable fact rather than an assumption
	// (and what the goleak shutdown test asserts against).
	stopped chan struct{}
}

// pool is the set of running informers, one per (GVR, namespace) target, each
// with its own context and goroutine.
//
// It owns lifecycle only. What an event *means* — which sinks care, which
// selectors match — is the interest table's business, consulted at event time so
// that rule edits never have to touch a running informer.
type pool struct {
	// mu guards entries. Writes happen only from the WatchManager's single
	// reconcile loop; reads happen on every pipeline lookup.
	mu      sync.RWMutex
	entries map[informerKey]*informerEntry

	dyn   dynamic.Interface
	table *interestTable
	queue Enqueuer

	// log is a field rather than a per-call argument because the notification
	// path has no context to carry one.
	log logr.Logger

	// stopTimeout is how long stop waits for an informer goroutine. Injectable so
	// the leak-detection branch is testable without a 30-second test.
	stopTimeout time.Duration
}

// newPool returns an empty pool that will build informers from dyn and consult
// table at event time.
func newPool(dyn dynamic.Interface, table *interestTable, queue Enqueuer, log logr.Logger) *pool {
	return &pool{
		entries:     make(map[informerKey]*informerEntry),
		dyn:         dyn,
		table:       table,
		queue:       queue,
		log:         log,
		stopTimeout: stopWaitTimeout,
	}
}

// size reports how many informers are running. It is the pool-size counter the
// sharing and selector-stability tests assert against: two rules on one
// (GVR, namespace) must never cost two informers, and a selector edit must never
// change this number.
func (p *pool) size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.entries)
}

// entryFor returns the running informer for key, if any.
func (p *pool) entryFor(key informerKey) (*informerEntry, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	entry, ok := p.entries[key]
	return entry, ok
}

// retain level-triggers the pool towards wanted: informers nobody wants any more
// are stopped, informers that should exist but do not are started.
//
// Stops run first so a rule deletion frees its watch before any new List traffic
// starts, which matters on a cluster where a rule edit swaps one large scope for
// another. A start failure is logged and left alone rather than retried in a
// tight loop: the WatchManager's next diff pass (a change notification or the 30s
// tick) attempts it again, so a transient failure self-heals without this
// function needing a retry policy of its own (Invariant 5).
func (p *pool) retain(ctx context.Context, wanted map[informerKey]schema.GroupVersionKind) {
	for _, key := range p.keys() {
		if _, keep := wanted[key]; !keep {
			p.stop(key)
		}
	}

	// Sorted so a pass that starts several informers does so in a stable order,
	// which keeps startup logs readable and test expectations deterministic.
	for _, key := range slices.SortedFunc(maps.Keys(wanted), compareInformerKeys) {
		if _, running := p.entryFor(key); running {
			continue
		}
		if err := p.start(ctx, key, wanted[key]); err != nil {
			p.log.Error(err, "Failed to start informer, will retry on the next pool diff",
				"gvr", key.GVR.String(), "namespace", key.Namespace)
		}
	}
}

// keys returns the informerKeys currently running, sorted.
func (p *pool) keys() []informerKey {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return slices.SortedFunc(maps.Keys(p.entries), compareInformerKeys)
}

// start builds and runs one informer for key.
//
// The informer is fully configured — transform installed, handler registered —
// *before* its goroutine starts, because both calls are rejected once an informer
// is running, and because an event delivered before the handler existed would be
// an object silently missing from the stream.
func (p *pool) start(ctx context.Context, key informerKey, gvk schema.GroupVersionKind) error {
	log := p.log.WithValues("gvr", key.GVR.String(), "namespace", key.Namespace, "kind", gvk.Kind)

	informer := cache.NewSharedIndexInformerWithOptions(
		p.listWatchFor(key),
		&unstructured.Unstructured{},
		cache.SharedIndexInformerOptions{
			ResyncPeriod: resyncPeriod,
			// No custom indexers: every lookup this pool answers is by
			// namespace/name (see WatchManager.Get), which the default key
			// function already indexes. Extra indexes would cost memory per
			// object for nothing (D2).
			ObjectDescription: key.String(),
		},
	)

	if err := informer.SetTransform(TransformObject); err != nil {
		return fmt.Errorf("install informer transform for %s: %w", key, err)
	}
	// A watch that cannot be established (most often missing RBAC for a kind a
	// rule named) would otherwise only ever surface in client-go's own logs.
	// Routing it through our logger is what makes it an operator-visible anomaly
	// (Invariant 4); the reflector keeps retrying with backoff regardless, so a
	// grant added later self-heals.
	if err := informer.SetWatchErrorHandlerWithContext(p.watchErrorHandler(key)); err != nil {
		return fmt.Errorf("install watch error handler for %s: %w", key, err)
	}

	entry := &informerEntry{
		key:      key,
		gvk:      gvk,
		informer: informer,
		stopped:  make(chan struct{}),
	}
	if _, err := informer.AddEventHandler(p.handlerFor(entry)); err != nil {
		return fmt.Errorf("register event handler for %s: %w", key, err)
	}

	// Each informer gets its own cancellable child context: that is what lets one
	// target stop without disturbing its siblings. klog.NewContext threads our
	// logger into the informer's internals so the reflector's own messages arrive
	// with this target's identity attached.
	informerCtx, cancel := context.WithCancel(klog.NewContext(ctx, log))
	entry.cancel = cancel

	p.mu.Lock()
	if _, running := p.entries[key]; running {
		// Unreachable from the single reconcile loop, but a double-start would
		// leak the first informer's goroutine silently, so it is refused loudly.
		p.mu.Unlock()
		cancel()
		return fmt.Errorf("informer for %s is already running", key)
	}
	p.entries[key] = entry
	p.mu.Unlock()

	go func() {
		defer close(entry.stopped)
		entry.informer.RunWithContext(informerCtx)
	}()

	log.Info("Started informer")
	return nil
}

// stop cancels one informer and waits for its goroutine to return.
//
// Waiting (rather than firing and forgetting) is what makes a stopped scope
// verifiably stopped: once this returns, no further event for that target can
// reach the pipeline, so the caller's cache eviction cannot race a late event
// re-populating what it just dropped. A goroutine that outlives stopTimeout is
// reported at Error level with its target's identity and abandoned — a leaked
// informer is a bug worth a page, but it must not wedge rule deletion.
func (p *pool) stop(key informerKey) {
	p.mu.Lock()
	entry, ok := p.entries[key]
	delete(p.entries, key)
	p.mu.Unlock()
	if !ok {
		return
	}

	entry.cancel()
	timer := time.NewTimer(p.stopTimeout)
	defer timer.Stop()
	select {
	case <-entry.stopped:
		p.log.Info("Stopped informer", "gvr", key.GVR.String(), "namespace", key.Namespace)
	case <-timer.C:
		p.log.Error(errInformerStopTimeout, "Informer goroutine did not exit within the stop timeout",
			"gvr", key.GVR.String(), "namespace", key.Namespace, "kind", entry.gvk.Kind,
			"timeout", p.stopTimeout.String())
	}
}

// stopAll stops every informer, used on process shutdown.
//
// Cancellation is issued to all of them first and only then awaited, so a pool of
// N informers costs one stop timeout rather than N — shutdown budget is shared
// with the sink's own drain (see sink.Writer), and spending it serially would eat
// into the time the last records need to land.
func (p *pool) stopAll() {
	p.mu.Lock()
	entries := slices.Collect(maps.Values(p.entries))
	p.entries = make(map[informerKey]*informerEntry)
	p.mu.Unlock()

	for _, entry := range entries {
		entry.cancel()
	}

	timer := time.NewTimer(p.stopTimeout)
	defer timer.Stop()
	for _, entry := range entries {
		select {
		case <-entry.stopped:
		case <-timer.C:
			p.log.Error(errInformerStopTimeout, "Informer goroutine did not exit during shutdown",
				"gvr", entry.key.GVR.String(), "namespace", entry.key.Namespace,
				"kind", entry.gvk.Kind, "timeout", p.stopTimeout.String())
		}
	}
}

// errInformerStopTimeout gives the leaked-goroutine log line a non-nil error
// value. The condition is the log's whole message; no caller branches on it.
var errInformerStopTimeout = errors.New("informer goroutine outlived its stop timeout")

// listWatchFor builds the ListWatch one informer runs on, straight off the
// dynamic client.
//
// No field or label selector is set. Label filtering is applied handler-side (see
// scopeInterest.matches) so that two rules with different selectors share one
// informer and a selector edit needs no re-List: the documented trade-off is
// informer bandwidth (we watch a superset of what any single rule wants) for pool
// simplicity, and it is the right side of that trade because a re-List is the
// most expensive thing this operator can ask an API server to do.
func (p *pool) listWatchFor(key informerKey) *cache.ListWatch {
	resource := p.dyn.Resource(key.GVR).Namespace(key.Namespace)
	return &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return resource.List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			return resource.Watch(ctx, options)
		},
	}
}

// watchErrorHandler reports a broken watch through the operator's logger.
//
// The classification mirrors client-go's own default handler: an expired resource
// version and a normally-closed connection are routine reflector plumbing and log
// at debug level, while anything else (forbidden, unreachable, malformed) is a
// genuine anomaly and logs at Error with the target's identity (Invariant 4).
func (p *pool) watchErrorHandler(key informerKey) cache.WatchErrorHandlerWithContext {
	return func(_ context.Context, _ *cache.Reflector, err error) {
		log := p.log.WithValues("gvr", key.GVR.String(), "namespace", key.Namespace)
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, io.EOF):
			// The informer is stopping, or the API server closed the watch
			// normally. Both are expected and re-established by the reflector.
			log.V(1).Info("Watch closed")
		case apierrors.IsResourceExpired(err), apierrors.IsGone(err):
			log.V(1).Info("Watch expired, the reflector will re-list", "err", err.Error())
		default:
			log.Error(err, "Watch failed, the reflector will retry with backoff")
		}
	}
}

// handlerFor builds the event handler for one informer.
//
// All three verbs converge on the same fan-out: resolve the object's identity,
// ask the interest table who cares, enqueue one key per interested sink. Nothing
// here reads or writes the sink, and nothing here can block on I/O — an informer
// handler that stalls stalls the whole watch (Invariant 1).
func (p *pool) handlerFor(entry *informerEntry) cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			p.fanOut(entry, obj, nil)
		},
		UpdateFunc: func(oldObj, newObj any) {
			p.fanOut(entry, newObj, oldObj)
		},
		DeleteFunc: func(obj any) {
			p.fanOut(entry, obj, nil)
		},
	}
}

// fanOut enqueues one work key per sink interested in this event.
//
// previous is the pre-update object on an Update and nil otherwise; it is
// consulted only so an object leaving a selector's scope still produces a final
// work item (see scopeInterest.matchesEither).
//
// A key is enqueued for the *identity*, never for the event: the pipeline reads
// current state from the watch cache when it picks the item up, which is what
// lets the workqueue collapse an update storm into a single Process call and what
// makes a Delete and an Add for the same name indistinguishable at this layer.
func (p *pool) fanOut(entry *informerEntry, obj, previous any) {
	target, err := eventTargetOf(obj)
	if err != nil {
		p.log.Error(err, "Dropping an informer event with an unusable object",
			"gvr", entry.key.GVR.String(), "namespace", entry.key.Namespace, "kind", entry.gvk.Kind)
		return
	}

	// Labels are read lazily — at most once per event, and only if some interest
	// actually filters on them. Extracting them costs a fresh map copy per object
	// (unstructured.GetLabels deep-copies), twice on an update, and the
	// overwhelmingly common case is that every interested rule asked for
	// everything and no selector is ever consulted. Measured (Task 2.3) at two
	// avoidable allocations per Add and four per Update, on the informer
	// notification path — the one path Invariant 1 says must stay cheap.
	var (
		currentLabels  map[string]string
		previousLabels map[string]string
		labelsRead     bool
	)
	readLabels := func() {
		if labelsRead {
			return
		}
		labelsRead = true
		currentLabels = target.labelsOf()
		// A tombstone whose object was lost carries no labels; previous is nil on
		// Add and Delete. Both sides being absent is fine — matchesEither then
		// simply evaluates the selectors it does have.
		if previous != nil {
			if prev, prevErr := eventTargetOf(previous); prevErr == nil {
				previousLabels = prev.labelsOf()
			}
		}
	}

	for _, in := range p.table.interestsFor(entry.key) {
		// A tombstone whose object was lost has no labels to evaluate selectors
		// against. Fanning out to every interested sink is the truthful choice:
		// the object is gone, and a key for an identity no sink ever recorded
		// settles as a no-op in the pipeline (its delete claim finds no cache
		// entry), whereas skipping the event could strand a recorded object as
		// permanently live in the sink.
		if !in.matchAll && target.labelsKnown() {
			readLabels()
			if !in.matchesEither(currentLabels, previousLabels) {
				continue
			}
		}
		p.queue.Add(in.keyFor(target.namespace, target.name))
	}
}

// eventTarget is the minimum an event contributes to fan-out: which object it is,
// and — on demand — what it is labelled.
type eventTarget struct {
	namespace string
	name      string

	// obj is the object the event carried, kept so its labels can be extracted
	// only if a selector actually needs them (see fanOut). It is nil for a
	// tombstone whose last-known object was lost.
	obj *unstructured.Unstructured
}

// labelsKnown reports whether this event has an object to evaluate selectors
// against. It is false only for a tombstone whose last-known object was lost.
//
// It is asked as its own question, rather than callers checking for empty labels,
// because "this object has no labels" and "we cannot know this object's labels"
// must lead to opposite decisions: the first excludes the object from a selector's
// scope, the second fans out to everyone.
func (t eventTarget) labelsKnown() bool { return t.obj != nil }

// labelsOf returns the object's labels as a fresh map, or nil when the event
// carried no object to read them from.
func (t eventTarget) labelsOf() map[string]string {
	if t.obj == nil {
		return nil
	}
	return t.obj.GetLabels()
}

// eventTargetOf extracts an event's identity, unwrapping a
// cache.DeletedFinalStateUnknown tombstone.
//
// Tombstones arrive whenever the informer notices a deletion it did not see
// happen — a watch gap closed by a re-List. The inner object is the last state
// the cache held, which is exactly what fan-out needs; when even that is
// unavailable client-go still supplies the cache key, so identity is recovered
// from the key itself rather than the event being dropped (a dropped tombstone is
// an object that stays "alive" in the sink forever).
func eventTargetOf(obj any) (eventTarget, error) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		if tombstone.Obj == nil {
			namespace, name, err := cache.SplitMetaNamespaceKey(tombstone.Key)
			if err != nil {
				return eventTarget{}, fmt.Errorf("split tombstone cache key %q: %w", tombstone.Key, err)
			}
			return eventTarget{namespace: namespace, name: name}, nil
		}
		obj = tombstone.Obj
	}

	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return eventTarget{}, fmt.Errorf("informer delivered a %T, want *unstructured.Unstructured", obj)
	}
	return eventTarget{
		namespace: u.GetNamespace(),
		name:      u.GetName(),
		obj:       u,
	}, nil
}

// TransformObject is every informer's cache.TransformFunc: it harvests the actor
// names from metadata.managedFields onto an annotation and then deletes
// managedFields from the copy that gets cached.
//
// This is the informer-memory half of D2 (the hashCache half is Task 0.7):
// managedFields is routinely the largest single section of a Kubernetes object,
// it is pure write-provenance bookkeeping, and the operator needs exactly one
// fact out of it — who touched the object. Harvesting that fact into a single
// annotation and dropping the rest shrinks every cached object, for every
// informer, permanently. The annotation is operator-internal transport: the
// pipeline reads it into the record and strips it before hashing (see
// normalizeObject), so it can never perturb an object's hash, and it is never
// written back to the API server.
//
// The pinned client-go (v0.35.0) guarantees a transform sees each object before
// any other reader and may mutate it in place, and it explicitly does *not* pass
// tombstones or resync deltas through the transform. It also asks that a
// transform be idempotent, which the managedFields check below provides: an
// object that has already been transformed has no managedFields left and is
// returned untouched, so a re-Replace of cached objects cannot erase the actors
// annotation it set the first time.
//
// It never returns an error. A transform error drops the object from the informer
// entirely — the stream would lose it silently — so a malformed object degrades
// to "cached as-is" instead (Invariant 5); ExtractActors already logs the
// malformed parts it skipped.
//
// It is exported for one caller: the load harness (test/loadgen, Task 2.3), whose
// informers must cache objects in exactly the shape production's do. The RSS
// envelopes published in docs/PERFORMANCE.md are largely the size of those caches,
// so a harness with its own copy of this logic would eventually publish numbers
// for a shape the operator no longer uses.
func TransformObject(obj any) (any, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return obj, nil
	}

	_, found, err := unstructured.NestedFieldNoCopy(u.Object, "metadata", "managedFields")
	if err != nil || !found {
		// Already transformed, or an object whose metadata is not shaped like
		// metadata. Either way there is nothing to harvest and nothing to strip.
		return obj, nil
	}

	if actors := pipeline.ExtractActors(u); len(actors) > 0 {
		annotations := u.GetAnnotations()
		if annotations == nil {
			annotations = make(map[string]string, 1)
		}
		// ExtractActors returns a sorted, de-duplicated slice, so the encoded
		// value is deterministic for a given actor set — an object whose actors
		// did not change must not look changed to anything downstream.
		annotations[pipeline.ActorsAnnotation] = pipeline.EncodeActors(actors)
		u.SetAnnotations(annotations)
	}
	unstructured.RemoveNestedField(u.Object, "metadata", "managedFields")
	return u, nil
}

// compareInformerKeys orders informer keys for stable iteration.
func compareInformerKeys(a, b informerKey) int {
	return cmp.Or(
		cmp.Compare(a.GVR.String(), b.GVR.String()),
		cmp.Compare(a.Namespace, b.Namespace),
	)
}
