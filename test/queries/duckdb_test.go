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

package queries

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yelzhy/kuberecord/internal/sink"
	kbs3 "github.com/yelzhy/kuberecord/internal/sink/s3"
)

// This file is the half of the S3 recipes' coverage that needs no object store,
// and therefore runs in `make test` on every push: the fence markers, the
// variable vocabulary, and the agreement between the globs the page publishes and
// the keys the shipped encoder actually writes.

func TestFromMarkdownDialects(t *testing.T) {
	doc := "```sql\nSELECT 1;\n```\n\n" +
		"```sql duckdb\nSELECT 2;\n```\n\n" +
		"```sql duckdb-parameters\nSET VARIABLE a = 'x';\n```\n\n" +
		"```sql duckdb-setup\nLOAD httpfs;\n```\n\n" +
		"```sql athena\nCREATE EXTERNAL TABLE t (a string);\n```\n"

	got, err := FromMarkdown(writeDoc(t, doc))
	if err != nil {
		t.Fatalf("FromMarkdown: %v", err)
	}
	want := []Dialect{
		DialectClickHouse, DialectDuckDB, DialectDuckDBParameters, DialectDuckDBSetup, DialectAthena,
	}
	if len(got) != len(want) {
		t.Fatalf("extracted %d blocks, want %d: %+v", len(got), len(want), got)
	}
	for i, q := range got {
		if q.Dialect != want[i] {
			t.Errorf("block %d is dialect %q, want %q", i, q.Dialect, want[i])
		}
	}
	if only := ByDialect(got, DialectDuckDB); len(only) != 1 || only[0].SQL != "SELECT 2;" {
		t.Errorf("ByDialect(duckdb) = %+v, want just the duckdb block", only)
	}
}

// TestFromMarkdownRejectsAnUnknownDialect is the anti-vacuity half of the marker
// scheme. A typo that silently produced an unexecuted block is exactly the
// failure the dialect vocabulary exists to prevent, so it has to be an error.
func TestFromMarkdownRejectsAnUnknownDialect(t *testing.T) {
	path := writeDoc(t, "```sql duckdbb\nSELECT 1;\n```\n")
	if _, err := FromMarkdown(path); err == nil {
		t.Fatal("a misspelled fence marker was accepted; it must fail rather than yield a block nothing runs")
	} else if !strings.Contains(err.Error(), "duckdbb") {
		t.Errorf("error is %q, want it to name the offending marker", err)
	}

	// A language that merely starts with "sql" is not a marked sql fence at all.
	path = writeDoc(t, "```sqlite\nSELECT 1;\n```\n")
	got, err := FromMarkdown(path)
	if err != nil {
		t.Fatalf("FromMarkdown: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("a ```sqlite fence was extracted as %d block(s); the marker needs a space", len(got))
	}
}

func TestVariableHelpers(t *testing.T) {
	block := "SET VARIABLE archive = 's3://b/p';\n" +
		"set variable cluster = 'c';\n" +
		"SET VARIABLE \"group\" = 'apps';\n"
	if got, want := DeclaredVariables(block), []string{"archive", "cluster", "group"}; !slices.Equal(got, want) {
		t.Errorf("DeclaredVariables() = %v, want %v", got, want)
	}

	recipe := "SELECT * FROM read_json_auto(getvariable('records'))\n" +
		"WHERE \"group\" = getvariable('group') AND kind = getvariable( 'kind' )\n" +
		"  AND name = getvariable('group');"
	if got, want := ReferencedVariables(recipe), []string{"group", "kind", "records"}; !slices.Equal(got, want) {
		t.Errorf("ReferencedVariables() = %v, want %v", got, want)
	}
}

// s3Library returns the S3 half of the published library, split by dialect, and
// fails if any of the three block kinds is missing — a page that stopped
// publishing them would otherwise make every check below pass vacuously.
func s3Library(t *testing.T) (params, setup Query, recipes []Query) {
	t.Helper()

	library, err := FromMarkdown(repoPath("docs", "QUERIES.md"))
	if err != nil {
		t.Fatalf("FromMarkdown: %v", err)
	}
	paramBlocks := ByDialect(library, DialectDuckDBParameters)
	setupBlocks := ByDialect(library, DialectDuckDBSetup)
	recipes = ByDialect(library, DialectDuckDB)

	if len(paramBlocks) != 1 {
		t.Fatalf("docs/QUERIES.md publishes %d duckdb-parameters blocks, want exactly 1", len(paramBlocks))
	}
	if len(setupBlocks) != 1 {
		t.Fatalf("docs/QUERIES.md publishes %d duckdb-setup blocks, want exactly 1", len(setupBlocks))
	}
	// The five recipe subjects Task 7.2 requires — a day's objects, one object's
	// timeline, changes by actor, activity by namespace, and locating the objects
	// covering a window — plus the second form of the last one and the scope log.
	if len(recipes) < 5 {
		t.Fatalf("docs/QUERIES.md publishes %d DuckDB recipes, want at least the 5 the "+
			"acceptance criteria name", len(recipes))
	}
	return paramBlocks[0], setupBlocks[0], recipes
}

// TestPublishedRecipesUseOnlyDeclaredVariables is the DuckDB counterpart of
// Interpolate's strictness about Grafana variables, and it matters more here:
// getvariable() on an unset variable returns NULL rather than failing, so a
// mistyped name is a predicate that quietly matches nothing rather than an error
// anyone sees.
func TestPublishedRecipesUseOnlyDeclaredVariables(t *testing.T) {
	params, setup, recipes := s3Library(t)

	declared := append(DeclaredVariables(params.SQL), DeclaredVariables(setup.SQL)...)
	slices.Sort(declared)

	for _, block := range append([]Query{setup}, recipes...) {
		t.Run(shortSource(block.Source), func(t *testing.T) {
			referenced := ReferencedVariables(block.SQL)
			if len(referenced) == 0 {
				t.Errorf("block reads no variables at all; it cannot be pointed at anybody's archive:\n%s",
					block.SQL)
			}
			for _, name := range referenced {
				if !slices.Contains(declared, name) {
					t.Errorf("reads getvariable(%q), which neither the parameters block nor the setup "+
						"block declares (declared: %v)", name, declared)
				}
			}
		})
	}

	// And the other direction: a declared variable nothing reads is a parameter a
	// reader is told to set for no reason.
	read := map[string]bool{}
	for _, block := range append([]Query{setup}, recipes...) {
		for _, name := range ReferencedVariables(block.SQL) {
			read[name] = true
		}
	}
	for _, name := range DeclaredVariables(params.SQL) {
		if !read[name] {
			t.Errorf("the parameters block declares %q, which no setup block or recipe reads", name)
		}
	}
}

// TestPublishedRecipesReadThePublishedGlobs keeps the layout in one place. A
// recipe that spelled a bucket path itself would work on the day it was written
// and rot the first time the layout changed, and it would do so silently, since
// nothing but this check knows the difference.
func TestPublishedRecipesReadThePublishedGlobs(t *testing.T) {
	_, setup, recipes := s3Library(t)

	globs := DeclaredVariables(setup.SQL)
	for _, recipe := range recipes {
		read := ReferencedVariables(recipe.SQL)
		if !slices.ContainsFunc(read, func(name string) bool { return slices.Contains(globs, name) }) {
			t.Errorf("%s reads none of the globs the setup block derives (%v):\n%s",
				recipe.Source, globs, recipe.SQL)
		}
	}
}

// TestPublishedRecordGlobMatchesTheShippedKeyLayout holds the published glob
// against a key the shipped encoder actually produced.
//
// This is the check that stops the page from documenting a layout kuberecord does
// not write. It restates none of internal/sink/s3's constants — those are
// deliberately unexported — and instead asks s3.Encode for a real object and
// compares the glob's literal parts against the key it chose. Rename
// format=jsonl-v1, drop the cluster_id partition or change the extension, and the
// glob stops matching here rather than in front of an auditor.
func TestPublishedRecordGlobMatchesTheShippedKeyLayout(t *testing.T) {
	_, setup, _ := s3Library(t)

	const (
		archive   = "s3://kuberecord-archive/audit"
		prefix    = "audit"
		clusterID = "prod-eu-1"
	)
	glob, err := evalConcat(globExpression(t, setup.SQL, "records"), map[string]string{
		"archive": archive,
		"cluster": clusterID,
	})
	if err != nil {
		t.Fatalf("evaluate the published records glob: %v", err)
	}

	object, err := kbs3.Encode(prefix, []sink.Record{{
		Timestamp: time.Date(2026, 8, 20, 9, 15, 0, 0, time.UTC),
		ClusterID: clusterID,
		EventType: "Snapshot",
		Kind:      "Deployment",
		Namespace: "demo",
		Name:      "api",
	}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	key := "s3://kuberecord-archive/" + object.Key

	// The literal head and tail of the glob — everything the pattern is not free
	// to vary. `**` and `*` cover the date and hour partitions and the content
	// hash, which are per-object by design and not part of what a glob asserts.
	head, _, found := strings.Cut(glob, "*")
	if !found {
		t.Fatalf("the published records glob has no wildcard at all: %q", glob)
	}
	if !strings.HasPrefix(key, head) {
		t.Errorf("the published glob and the shipped key layout disagree\nglob head: %q\n      key: %q", head, key)
	}
	tail := glob[strings.LastIndex(glob, "*")+1:]
	if !strings.HasSuffix(key, tail) {
		t.Errorf("the published glob and the shipped key layout disagree\nglob tail: %q\n      key: %q", tail, key)
	}
}

// globExpression returns the right-hand side of one `SET VARIABLE <name> = …`
// statement from a block.
//
// Comments are stripped before the statement is located, because the published
// setup block is heavily commented and a comment may contain both a semicolon and
// an equals sign — `cluster_id=` appears in one — either of which would fool a
// scan that split the block naively.
func globExpression(t *testing.T, block, name string) string {
	t.Helper()
	decl := regexp.MustCompile(`(?is)SET\s+VARIABLE\s+"?` + regexp.QuoteMeta(name) + `"?\s*=\s*(.*?);`)
	m := decl.FindStringSubmatch(stripSQLLineComments(block))
	if m == nil {
		t.Fatalf("the setup block declares no variable named %q", name)
	}
	return strings.TrimSpace(m[1])
}

// evalConcat evaluates a DuckDB `'literal' || getvariable('name')` concatenation
// with the given bindings.
//
// It implements exactly the one expression form the setup block uses, and rejects
// anything else rather than guessing: a glob assembled some other way would make
// the comparison above a comparison against this helper's imagination.
func evalConcat(expr string, bindings map[string]string) (string, error) {
	var out strings.Builder
	for term := range strings.SplitSeq(expr, "||") {
		term = strings.TrimSpace(term)
		switch {
		case strings.HasPrefix(term, "'") && strings.HasSuffix(term, "'") && len(term) >= 2:
			out.WriteString(term[1 : len(term)-1])
		case strings.HasPrefix(term, "getvariable("):
			names := ReferencedVariables(term)
			if len(names) != 1 {
				return "", &evalError{term: term, reason: "is not a single getvariable() reference"}
			}
			value, ok := bindings[names[0]]
			if !ok {
				return "", &evalError{term: term, reason: "names a variable with no binding"}
			}
			out.WriteString(value)
		default:
			return "", &evalError{term: term, reason: "is neither a string literal nor a getvariable() call"}
		}
	}
	return out.String(), nil
}

type evalError struct {
	term   string
	reason string
}

func (e *evalError) Error() string { return "term " + e.term + " " + e.reason }

func writeDoc(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "doc.md")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return path
}
