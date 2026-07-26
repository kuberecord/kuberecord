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

package pipeline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCacheKeyDistinguishesGVKCollision is the core Task 0.4 regression: two
// objects that share a Kind but live in different API groups — batch/v1 Job
// and a CRD example.com/v1 Job — must produce distinct identity keys, and thus
// wholly independent hashCache entries. Before the fix cacheKey keyed on Kind
// alone, so both collapsed onto one entry and cross-contaminated each other's
// dedup/warm-up history (a latent audit-corruption bug, Invariant 7).
func TestCacheKeyDistinguishesGVKCollision(t *testing.T) {
	batchJob := Key{Group: "batch", Kind: "Job", Namespace: "foo", Name: "bar"}
	crdJob := Key{Group: "example.com", Kind: "Job", Namespace: "foo", Name: "bar"}

	batchKey := batchJob.cacheKey()
	crdKey := crdJob.cacheKey()

	if batchKey == crdKey {
		t.Fatalf("cacheKey collided across groups: batch/v1 Job and example.com/v1 Job both produced %q", batchKey)
	}

	// Prove the distinct keys drive independent hashCache entries: a write
	// under one key must never be observable under the other.
	var cache hashCache
	cache.Reserve(batchKey, CacheEntry{Hash: "batch-hash", UID: "batch-uid"})
	cache.Reserve(crdKey, CacheEntry{Hash: "crd-hash", UID: "crd-uid"})

	got, ok := cache.Load(batchKey)
	if !ok || got.Hash != "batch-hash" || got.UID != "batch-uid" {
		t.Fatalf("batch/v1 Job entry corrupted by example.com/v1 Job write: got %+v (ok=%v)", got, ok)
	}
	got, ok = cache.Load(crdKey)
	if !ok || got.Hash != "crd-hash" || got.UID != "crd-uid" {
		t.Fatalf("example.com/v1 Job entry corrupted by batch/v1 Job write: got %+v (ok=%v)", got, ok)
	}
	if cache.Len() != 2 {
		t.Fatalf("expected 2 independent cache entries, got %d", cache.Len())
	}
}

// TestCacheKeyIsVersionAgnostic asserts the other half of Invariant 7: apps/v1
// and a hypothetical apps/v2 Deployment are the SAME object, so they must key
// identically. The fix adds the group discriminator without adding version.
func TestCacheKeyIsVersionAgnostic(t *testing.T) {
	// The queue Key has no version field at all, which is the structural half of
	// this guarantee: apps/v1 and apps/v2 events for one Deployment produce the
	// identical Key, so they cannot help but share a cache entry.
	fromV1 := Key{Group: "apps", Kind: "Deployment", Namespace: "ns", Name: "name"}
	fromV2 := Key{Group: "apps", Kind: "Deployment", Namespace: "ns", Name: "name"}

	if got, want := fromV2.cacheKey(), fromV1.cacheKey(); got != want {
		t.Fatalf("cacheKey must be version-agnostic: apps/v2 gave %q, apps/v1 gave %q", got, want)
	}
}

// TestCacheKeyIgnoresSink asserts the other structural choice: the sink is part
// of the *queue* key (so two sinks get two independent work items) but never part
// of the *cache* key, because each sink owns its own hashCache instance. Encoding
// it in both places would silently double every cache entry.
func TestCacheKeyIgnoresSink(t *testing.T) {
	a := Key{Sink: "default", Group: "apps", Kind: "Deployment", Namespace: "ns", Name: "name"}
	b := a
	b.Sink = "audit"

	if a == b {
		t.Fatal("keys for two sinks must be distinct queue items")
	}
	if a.cacheKey() != b.cacheKey() {
		t.Fatalf("cacheKey must not embed the sink: %q vs %q", a.cacheKey(), b.cacheKey())
	}
}

// TestCacheKeyCoreGroupAndClusterScoped documents the shape for core-group and
// cluster-scoped (empty-namespace) objects, both of which key unambiguously.
func TestCacheKeyCoreGroupAndClusterScoped(t *testing.T) {
	pod := Key{Group: "", Kind: "Pod", Namespace: "default", Name: "p"}
	if got, want := pod.cacheKey(), "|Pod|default/p"; got != want {
		t.Fatalf("core-group key = %q, want %q", got, want)
	}
	// An empty namespace still renders its "/", so a cluster-scoped object keys
	// as "|Node|/n1" — still unambiguous, and identical to the format the
	// pre-pipeline builder produced (cache keys are not persisted, but keeping the
	// shape stable keeps this regression suite meaningful).
	node := Key{Group: "", Kind: "Node", Name: "n1"}
	if got, want := node.cacheKey(), "|Node|/n1"; got != want {
		t.Fatalf("cluster-scoped key = %q, want %q", got, want)
	}
}

// TestScopeKeyPrefixMatchesItsOwnKeys is the correctness proof for prefix-based
// scope eviction: every key in a scope must start with that scope's prefix, and
// no key from a sibling namespace or another kind may.
func TestScopeKeyPrefixMatchesItsOwnKeys(t *testing.T) {
	inScope := Key{Group: "apps", Kind: "Deployment", Namespace: "foo", Name: "web"}
	scope := inScope.Scope()

	if !strings.HasPrefix(inScope.cacheKey(), scope.scopeKeyPrefix()) {
		t.Fatalf("key %q is not matched by its own scope prefix %q", inScope.cacheKey(), scope.scopeKeyPrefix())
	}

	// A namespace that merely shares a prefix must not match — this is what the
	// trailing "/" in the prefix is for.
	sibling := Key{Group: "apps", Kind: "Deployment", Namespace: "foobar", Name: "web"}
	if strings.HasPrefix(sibling.cacheKey(), scope.scopeKeyPrefix()) {
		t.Fatalf("scope prefix %q wrongly matches namespace foobar", scope.scopeKeyPrefix())
	}

	// Another kind in the same group must not match either.
	otherKind := Key{Group: "apps", Kind: "DaemonSet", Namespace: "foo", Name: "web"}
	if strings.HasPrefix(otherKind.cacheKey(), scope.scopeKeyPrefix()) {
		t.Fatalf("scope prefix %q wrongly matches kind DaemonSet", scope.scopeKeyPrefix())
	}

	// An all-namespaces scope covers every namespace of its kind, including the
	// cluster-scoped (empty-namespace) rendering.
	allNamespaces := ScopeKey{Group: "apps", Kind: "Deployment"}
	for _, key := range []Key{inScope, sibling, {Group: "apps", Kind: "Deployment", Name: "cluster-wide"}} {
		if !strings.HasPrefix(key.cacheKey(), allNamespaces.scopeKeyPrefix()) {
			t.Errorf("all-namespaces prefix %q does not match key %q", allNamespaces.scopeKeyPrefix(), key.cacheKey())
		}
	}
}

// TestNoRogueIdentityKeyConcatenation enforces the "exactly one function
// constructs identity keys" acceptance criterion: no non-test source file in
// this package may hand-build a key by concatenating `Kind + "/"`. cacheKey is
// the sole canonical builder and uses "|" delimiters, so a match here means a
// call site has drifted back to the pre-fix, collision-prone pattern.
func TestNoRogueIdentityKeyConcatenation(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading package dir: %v", err)
	}
	const forbidden = `Kind + "/"`
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("%s contains a rogue identity-key concatenation %q; all keys must go through cacheKey", name, forbidden)
		}
	}
}
