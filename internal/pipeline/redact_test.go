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
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// The specs in this file cover the redaction engine in isolation: the path
// grammar, what each path shape rewrites, and the two properties the rest of the
// pipeline leans on — that Apply never mutates the object it is given, and that
// an empty policy still scrubs the last-applied annotation.
//
// The security properties that involve the *pipeline* (equal hashes, dedup
// skips, diffs with no unredacted fragment) live in process_test.go, next to the
// machinery they constrain.

// mustCompile compiles paths for a test, failing rather than returning an error.
func mustCompile(t *testing.T, paths ...string) *RedactionPolicy {
	t.Helper()
	policy, err := CompileRedaction(paths)
	if err != nil {
		t.Fatalf("CompileRedaction(%v): %v", paths, err)
	}
	return policy
}

// TestParseRedactionPath pins the grammar. It is deliberately small, and the
// invalid cases are as load-bearing as the valid ones: every one of them is a
// string the CRD's pattern rejects, so a mismatch here means either the API
// server admits something the data plane cannot compile (a rule that degrades to
// streaming nothing) or the reverse.
func TestParseRedactionPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		wantSegs []redactionSegment
		wantErr  bool
	}{
		{
			name:     "single segment",
			path:     "data",
			wantSegs: []redactionSegment{{name: "data"}},
		},
		{
			name:     "nested segments",
			path:     "spec.template.metadata",
			wantSegs: []redactionSegment{{name: "spec"}, {name: "template"}, {name: "metadata"}},
		},
		{
			name: "array wildcards",
			path: "spec.containers[*].env[*].value",
			wantSegs: []redactionSegment{
				{name: "spec"},
				{name: "containers", wildcard: true},
				{name: "env", wildcard: true},
				{name: "value"},
			},
		},
		{
			name:     "terminal wildcard",
			path:     "spec.args[*]",
			wantSegs: []redactionSegment{{name: "spec"}, {name: "args", wildcard: true}},
		},
		{
			name: "quoted key with dots and a slash",
			path: AnnotationRedactionPath("kubectl.kubernetes.io/last-applied-configuration"),
			wantSegs: []redactionSegment{
				{name: "metadata"},
				{name: "annotations"},
				{name: "kubectl.kubernetes.io/last-applied-configuration"},
			},
		},
		{
			name:     "underscores and hyphens in names",
			path:     "data.my-key_2",
			wantSegs: []redactionSegment{{name: "data"}, {name: "my-key_2"}},
		},
		{name: "empty", path: "", wantErr: true},
		{name: "leading dot", path: ".spec", wantErr: true},
		{name: "trailing dot", path: "spec.", wantErr: true},
		{name: "double dot", path: "spec..containers", wantErr: true},
		{name: "leading digit", path: "9spec", wantErr: true},
		{name: "indexed subscript", path: "spec.containers[0].name", wantErr: true},
		{name: "unterminated subscript", path: "spec.containers[*", wantErr: true},
		{name: "unquoted subscript", path: "metadata.annotations[key]", wantErr: true},
		{name: "empty quoted key", path: `metadata.annotations[""]`, wantErr: true},
		{name: "junk after a subscript", path: "spec.containers[*]x", wantErr: true},
		{name: "recursive descent", path: "spec..*.value", wantErr: true},
		{name: "space in a name", path: "spec.my field", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseRedactionPath(tc.path)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseRedactionPath(%q) = %+v, want an error", tc.path, got.segments)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRedactionPath(%q): %v", tc.path, err)
			}
			if !reflect.DeepEqual(got.segments, tc.wantSegs) {
				t.Errorf("parseRedactionPath(%q) segments = %+v, want %+v", tc.path, got.segments, tc.wantSegs)
			}
			if got.raw != tc.path {
				t.Errorf("parseRedactionPath(%q) raw = %q, want the path as written", tc.path, got.raw)
			}
		})
	}
}

// TestRedactionApply covers every path shape against a real object, including
// the two cases whose *absence* of an effect is the specification: a path that
// matches nothing, and a path whose leaf is not a string.
func TestRedactionApply(t *testing.T) {
	object := func() map[string]any {
		return map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"name": "app",
				"annotations": map[string]any{
					"my.company.io/api-token": "s3cret",
					"keep":                    "me",
				},
			},
			"data": map[string]any{"password": "hunter2", "user": "admin"},
			"spec": map[string]any{
				"replicas": int64(3),
				"paused":   false,
				"args":     []any{"--token=abc", "--verbose"},
				"config":   map[string]any{"nested": map[string]any{"secret": "deep"}},
				"containers": []any{
					map[string]any{
						"name": "app",
						"env": []any{
							map[string]any{"name": "TOKEN", "value": "abc"},
							map[string]any{"name": "MODE", "value": "debug"},
						},
					},
					map[string]any{
						"name": "sidecar",
						// No env at all: the wildcard must skip it rather than fail.
						"image": "busybox",
					},
				},
			},
		}
	}

	tests := []struct {
		name   string
		paths  []string
		assert func(t *testing.T, out map[string]any)
	}{
		{
			name:  "scalar path",
			paths: []string{"data.password"},
			assert: func(t *testing.T, out map[string]any) {
				data := out["data"].(map[string]any)
				if data["password"] != RedactionSentinel {
					t.Errorf("data.password = %v, want the sentinel", data["password"])
				}
				if data["user"] != "admin" {
					t.Errorf("data.user = %v, want it untouched", data["user"])
				}
			},
		},
		{
			name:  "nested map path",
			paths: []string{"spec.config.nested.secret"},
			assert: func(t *testing.T, out map[string]any) {
				nested := out["spec"].(map[string]any)["config"].(map[string]any)["nested"].(map[string]any)
				if nested["secret"] != RedactionSentinel {
					t.Errorf("spec.config.nested.secret = %v, want the sentinel", nested["secret"])
				}
			},
		},
		{
			name:  "wildcard over nested arrays",
			paths: []string{"spec.containers[*].env[*].value"},
			assert: func(t *testing.T, out map[string]any) {
				containers := out["spec"].(map[string]any)["containers"].([]any)
				env := containers[0].(map[string]any)["env"].([]any)
				for i, entry := range env {
					value := entry.(map[string]any)["value"]
					if value != RedactionSentinel {
						t.Errorf("env[%d].value = %v, want the sentinel", i, value)
					}
				}
				if name := env[0].(map[string]any)["name"]; name != "TOKEN" {
					t.Errorf("env[0].name = %v, want it untouched", name)
				}
				// The second container has no env: the wildcard skipped it
				// instead of failing or inventing one.
				if _, present := containers[1].(map[string]any)["env"]; present {
					t.Error("the env-less container gained an env key")
				}
			},
		},
		{
			name:  "terminal wildcard replaces every element",
			paths: []string{"spec.args[*]"},
			assert: func(t *testing.T, out map[string]any) {
				args := out["spec"].(map[string]any)["args"].([]any)
				for i, arg := range args {
					if arg != RedactionSentinel {
						t.Errorf("spec.args[%d] = %v, want the sentinel", i, arg)
					}
				}
			},
		},
		{
			name:  "annotation shorthand",
			paths: []string{AnnotationRedactionPath("my.company.io/api-token")},
			assert: func(t *testing.T, out map[string]any) {
				annotations := out["metadata"].(map[string]any)["annotations"].(map[string]any)
				if annotations["my.company.io/api-token"] != RedactionSentinel {
					t.Errorf("annotation = %v, want the sentinel", annotations["my.company.io/api-token"])
				}
				if annotations["keep"] != "me" {
					t.Errorf("the other annotation = %v, want it untouched", annotations["keep"])
				}
			},
		},
		{
			name: "paths matching nothing are a silent no-op",
			paths: []string{
				"spec.nosuchfield",
				"nosuchtoplevel.deeper",
				"data.password.deeper",  // a scalar is not a map
				"spec.replicas[*].name", // a scalar is not an array
				"spec.containers[*].nosuchfield",
			},
			assert: func(t *testing.T, out map[string]any) {
				want := object()
				if !reflect.DeepEqual(out, want) {
					t.Errorf("object changed:\n got: %v\nwant: %v", out, want)
				}
			},
		},
		{
			name:  "non-string leaves are replaced by the sentinel string",
			paths: []string{"spec.replicas", "spec.paused", "spec.config", "spec.args"},
			assert: func(t *testing.T, out map[string]any) {
				spec := out["spec"].(map[string]any)
				for _, field := range []string{"replicas", "paused", "config", "args"} {
					if spec[field] != RedactionSentinel {
						t.Errorf("spec.%s = %#v, want the sentinel string", field, spec[field])
					}
				}
			},
		},
		{
			name:  "several paths apply together",
			paths: []string{"data.password", "spec.containers[*].env[*].value"},
			assert: func(t *testing.T, out map[string]any) {
				if out["data"].(map[string]any)["password"] != RedactionSentinel {
					t.Error("data.password survived")
				}
				env := out["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["env"].([]any)
				if env[0].(map[string]any)["value"] != RedactionSentinel {
					t.Error("the env value survived")
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := object()
			out := mustCompile(t, tc.paths...).Apply(in)
			tc.assert(t, out)

			// Whatever the path did, the caller's own object is untouched: it is
			// the informer's shared instance in production.
			if !reflect.DeepEqual(in, object()) {
				t.Errorf("Apply mutated the object it was given: %v", in)
			}
		})
	}
}

// TestRedactionAppliesTheBuiltinUnderAnEmptyPolicy is the AC's "empty user
// policy" case, and the reason a nil policy is not an opt-out: kubectl copies
// the whole submitted object into the last-applied annotation, so an operator
// who configured nothing would otherwise have every applied object's full prior
// state in ClickHouse.
func TestRedactionAppliesTheBuiltinUnderAnEmptyPolicy(t *testing.T) {
	const planted = `{"data":{"password":"hunter2"}}`
	build := func() map[string]any {
		return map[string]any{
			"metadata": map[string]any{
				"annotations": map[string]any{LastAppliedConfigAnnotation: planted},
			},
		}
	}

	for _, tc := range []struct {
		name   string
		policy *RedactionPolicy
	}{
		{name: "nil policy", policy: nil},
		{name: "compiled from no paths", policy: mustCompile(t)},
		{name: "compiled from unrelated paths", policy: mustCompile(t, "spec.nosuchfield")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := tc.policy.Apply(build())
			annotations := out["metadata"].(map[string]any)["annotations"].(map[string]any)
			if annotations[LastAppliedConfigAnnotation] != RedactionSentinel {
				t.Errorf("last-applied annotation = %v, want the sentinel",
					annotations[LastAppliedConfigAnnotation])
			}
			encoded, err := json.Marshal(out)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if strings.Contains(string(encoded), "hunter2") {
				t.Errorf("the planted value survived in %s", encoded)
			}
		})
	}
}

// TestRedactionSharesWhatItDoesNotEdit pins the copy-on-write optimization
// itself. Apply runs on every object the data plane observes, so a redactor that
// deep-copied would hand back the per-event allocation Task 2.3 removed — and a
// redactor that mutated in place would corrupt the informer's cache for every
// other reader. Only the maps on a changed path may be copies.
func TestRedactionSharesWhatItDoesNotEdit(t *testing.T) {
	status := map[string]any{"phase": "Running"}
	sidecar := map[string]any{"name": "sidecar", "image": "busybox"}
	object := map[string]any{
		"status": status,
		"spec": map[string]any{
			"containers": []any{
				map[string]any{"name": "app", "env": []any{map[string]any{"value": "abc"}}},
				sidecar,
			},
		},
	}

	out := mustCompile(t, "spec.containers[*].env[*].value").Apply(object)

	if !sameMap(out["status"], status) {
		t.Error("status was copied; only maps on a path that changed may be")
	}
	containers := out["spec"].(map[string]any)["containers"].([]any)
	if !sameMap(containers[1], sidecar) {
		t.Error("the untouched container was copied")
	}
	if sameMap(out["spec"], object["spec"]) {
		t.Error("spec was shared; rewriting it would mutate the caller's object")
	}
	if got := containers[0].(map[string]any)["env"].([]any)[0].(map[string]any)["value"]; got != RedactionSentinel {
		t.Errorf("the env value = %v, want the sentinel", got)
	}

	// An object no path matches is handed straight back, allocating nothing —
	// the common case on a cluster where redaction is configured for one kind.
	untouched := mustCompile(t, "spec.nosuchfield").Apply(object)
	if !sameMap(untouched, object) {
		t.Error("an object no path matched was copied")
	}
}

// TestRedactionIsIdempotent matters because a value is redacted on every event
// for its object, and because warm-up re-reads state the write path already
// scrubbed: re-applying a policy must be a no-op, not a second rewrite that
// allocates fresh copies of every parent map.
func TestRedactionIsIdempotent(t *testing.T) {
	policy := mustCompile(t, "data.password")
	once := policy.Apply(map[string]any{"data": map[string]any{"password": "hunter2"}})
	twice := policy.Apply(once)

	if !sameMap(once, twice) {
		t.Error("re-applying a policy copied the object again")
	}
	if got := twice["data"].(map[string]any)["password"]; got != RedactionSentinel {
		t.Errorf("password = %v, want the sentinel", got)
	}
}

// TestCompileRedactionIsCanonical covers the property every hash depends on:
// two spellings of one policy must produce the same policy, so a rule edit that
// reorders or repeats entries cannot change a single stored hash.
func TestCompileRedactionIsCanonical(t *testing.T) {
	builtin := AnnotationRedactionPath(LastAppliedConfigAnnotation)

	first := mustCompile(t, "spec.containers[*].env[*].value", "data.password")
	second := mustCompile(t, "data.password", "spec.containers[*].env[*].value", "data.password", builtin)

	if !slices.Equal(first.Paths(), second.Paths()) {
		t.Errorf("paths differ by spelling:\n%v\n%v", first.Paths(), second.Paths())
	}
	if !slices.Contains(first.Paths(), builtin) {
		t.Errorf("built-in scrub missing from %v", first.Paths())
	}
}

// TestCompileRedactionRejectsBadPaths asserts the fail-closed direction: one
// unparseable path fails the whole policy rather than being skipped. Skipping
// would leave the stream running and still writing the value someone asked to
// have scrubbed, which is the one failure mode redaction may not have.
func TestCompileRedactionRejectsBadPaths(t *testing.T) {
	if _, err := CompileRedaction([]string{"data.password", "spec.containers[0]"}); err == nil {
		t.Fatal("CompileRedaction accepted a malformed path")
	}
}

// TestMergeRedaction covers the union rule that makes `extraRedaction` additive:
// merging can only ever add paths, and no policy's presence weakens another's.
func TestMergeRedaction(t *testing.T) {
	sinkFloor := mustCompile(t, "data.password")
	ruleExtra := mustCompile(t, "spec.containers[*].env[*].value")

	merged := MergeRedaction(sinkFloor, ruleExtra, nil)
	want := []string{
		AnnotationRedactionPath(LastAppliedConfigAnnotation),
		"data.password",
		"spec.containers[*].env[*].value",
	}
	slices.Sort(want)
	if !slices.Equal(merged.Paths(), want) {
		t.Errorf("merged paths = %v, want %v", merged.Paths(), want)
	}

	if got := MergeRedaction(nil, nil); got != nil {
		t.Errorf("MergeRedaction(nil, nil) = %v, want nil (built-ins only)", got.Paths())
	}
	if got := MergeRedaction(); got != nil {
		t.Errorf("MergeRedaction() = %v, want nil (built-ins only)", got.Paths())
	}
	// A nil policy in the mix contributes the built-ins and nothing else — it
	// can never remove a path a real policy asked for.
	if got := MergeRedaction(nil, sinkFloor); !slices.Equal(got.Paths(), sinkFloor.Paths()) {
		t.Errorf("merging with nil changed the policy: %v", got.Paths())
	}
}

// TestNilRedactionPolicyPaths keeps the nil-means-built-ins contract legible
// from the outside: a caller inspecting an unconfigured stream's policy sees the
// scrub that is nonetheless in force, rather than an empty list implying none.
func TestNilRedactionPolicyPaths(t *testing.T) {
	var policy *RedactionPolicy
	if got := policy.Paths(); !slices.Equal(got, []string{AnnotationRedactionPath(LastAppliedConfigAnnotation)}) {
		t.Errorf("(*RedactionPolicy)(nil).Paths() = %v, want the built-in scrub", got)
	}
}
