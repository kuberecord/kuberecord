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

package render_test

import (
	"testing"
	"unicode/utf8"

	"github.com/kuberecord/kuberecord/internal/cli/render"
)

// TestDisplayPath covers the conversion from what a patch stores to what a
// reader is shown.
func TestDisplayPath(t *testing.T) {
	tests := []struct {
		name    string
		pointer string
		want    string
	}{
		{"a plain member", "/spec/replicas", "spec.replicas"},
		{
			name:    "an array index becomes a subscript",
			pointer: "/spec/template/spec/containers/0/image",
			want:    "spec.template.spec.containers[0].image",
		},
		{"a two-digit index", "/spec/containers/12/name", "spec.containers[12].name"},
		{
			name: "a leading zero is a member name, not an index",
			// RFC 6901 spells array indices without a leading zero, so this is
			// the one case the numeric ambiguity resolves from the token alone.
			pointer: "/data/01",
			want:    "data.01",
		},
		{
			name:    "a slash in a key is unescaped before the dot join",
			pointer: "/metadata/annotations/kubectl.kubernetes.io~1last-applied-configuration",
			want:    "metadata.annotations.kubectl.kubernetes.io/last-applied-configuration",
		},
		{
			name: "the two escapes are undone in the mandated order",
			// ~01 encodes a literal "~1". Undoing ~0 first would turn it into a
			// slash and split the segment in two.
			pointer: "/metadata/labels/a~01b",
			want:    "metadata.labels.a~1b",
		},
		{"the whole document", "", "/"},
		{"a single segment", "/status", "status"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := render.DisplayPath(test.pointer); got != test.want {
				t.Errorf("DisplayPath(%q) = %q, want %q", test.pointer, got, test.want)
			}
		})
	}
}

// TestNormalizeFieldPath is the assertion behind the promise that a path copied
// out of the output can be pasted into --field.
//
// The two grammars exist for good reasons — the filter's is the one redaction
// policies use, the display one is the one people read — and the gap between them
// would otherwise be an empty result the tool manufactured itself.
func TestNormalizeFieldPath(t *testing.T) {
	tests := []struct {
		name  string
		given string
		want  string
	}{
		{"the display spelling", "spec.containers[0].image", "spec.containers.0.image"},
		{"the filter spelling passes through", "spec.containers.0.image", "spec.containers.0.image"},
		{"no index at all", "spec.replicas", "spec.replicas"},
		{"several indices", "a[1].b[2].c", "a.1.b.2.c"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := render.NormalizeFieldPath(test.given); got != test.want {
				t.Errorf("NormalizeFieldPath(%q) = %q, want %q", test.given, got, test.want)
			}
		})
	}
}

// TestElide covers the middle-elision the acceptance criteria call for, and the
// property that matters more than any single example: the result never exceeds
// the width it was given.
func TestElide(t *testing.T) {
	const long = "spec.template.spec.containers[0].resources.limits.memory"

	tests := []struct {
		name  string
		path  string
		width int
		want  string
	}{
		{"already short enough", "spec.replicas", 40, "spec.replicas"},
		{"exactly the width", "spec.replicas", 13, "spec.replicas"},
		{
			name:  "the acceptance criteria's own example",
			path:  long,
			width: 43,
			want:  "spec.…containers[0].resources.limits.memory",
		},
		{
			name:  "a tighter budget gives up more of the middle",
			path:  long,
			width: 30,
			want:  "spec.…resources.limits.memory",
		},
		{
			name: "an index stays attached to what it indexes",
			// "spec.…[0].image" would name a subscript of nothing.
			path:  long,
			width: 20,
			want:  "spec.…limits.memory",
		},
		{
			name:  "not even the leaf fits beside the head",
			path:  long,
			width: 8,
			want:  "…memory",
		},
		{"a single segment can only be truncated", "averylongsinglesegment", 10, "averylong…"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := render.Elide(test.path, test.width)
			if got != test.want {
				t.Errorf("Elide(%q, %d) = %q, want %q", test.path, test.width, got, test.want)
			}
			if width := utf8.RuneCountInString(got); width > test.width {
				t.Errorf("Elide(%q, %d) produced %d columns, which overflows the column it was fitted to",
					test.path, test.width, width)
			}
		})
	}
}

// TestElideNeverExceedsItsWidth is the property the table's alignment depends on.
//
// Every example above is a case somebody thought of; this one sweeps the widths a
// real column takes, because the failure it guards against — one row a character
// too long — is invisible in a diff and obvious in a terminal.
func TestElideNeverExceedsItsWidth(t *testing.T) {
	paths := []string{
		"spec.template.spec.containers[0].resources.limits.memory",
		"metadata.annotations.kubectl.kubernetes.io/last-applied-configuration",
		"status",
		"a.b",
	}
	for _, path := range paths {
		for width := 1; width <= 80; width++ {
			if got := utf8.RuneCountInString(render.Elide(path, width)); got > width {
				t.Fatalf("Elide(%q, %d) produced %d columns", path, width, got)
			}
		}
	}
}
