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
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"slices"
	"testing"
)

// The files this assertion is made over: the builders, and the table that is
// supposed to cover them.
const (
	buildersFile = "sql.go"
	dedupTable   = "TestEveryResourceStatesReadCarriesADedupForm"
	dedupFile    = "sql_test.go"
)

// dedupTableExemptions are the functions returning a statement that the dedup
// table deliberately does not cover, and why.
//
// An exemption is a decision somebody wrote down. The alternative — skipping any
// statement whose text does not contain "FROM resource_states" — would make the
// coverage assertion self-fulfilling: a builder that stopped reading the table by
// accident would exempt itself, which is the failure this whole test exists to
// notice.
var dedupTableExemptions = map[string]string{
	"renderSelect": "the shared renderer, not a read: it takes its table as a parameter and " +
		"every builder below is rendered through it, so asserting over it would be asserting " +
		"over an argument rather than over a statement this package emits",
	"coverageStatement": "reads watch_scopes, which is a plain MergeTree with nothing to " +
		"collapse; TestCoverageReadCarriesNoFinal pins the opposite property for it, that it " +
		"must carry no FINAL at all",
	"renderSelectAll": "the shared renderer for the one read with no WHERE clause, not a read " +
		"of its own: like renderSelect it takes its table as a parameter, so asserting over it " +
		"would be asserting over an argument",
	"clusterIDsFromScopesStatement": "reads watch_scopes for the same reason coverageStatement " +
		"does, and TestClusterIDProbesAreShapedForTheirTables pins the same opposite property: no " +
		"FINAL on a plain MergeTree. Its resource_states sibling is in the table",
}

// TestEveryStatementBuilderIsCoveredByTheDedupTable enumerates the builders from
// the source and requires each one to appear in the dedup table.
//
// The table it guards is the correctness assertion of this package, and the
// table is hand-maintained. Its own comment says the assertion is "over the
// builders rather than over a handful of statements written by hand" — which it
// is, but through a hand-written list of invocations. A builder added later for
// flapping, or for search, or for anything a command needs, would simply not be
// in that list, and every test in this package would still pass.
//
// That is the vacuity class this repository has caught four times before, and the
// remedy it already carries: assert over the source rather than over a list, the
// way the t.Parallel prohibition in internal/controller does. A list cannot notice
// what was never added to it; a parse can.
//
// This test guards the table's completeness. It does not replace the table, and
// it asserts nothing about SQL text — what a covered statement must contain is
// still the table's business.
func TestEveryStatementBuilderIsCoveredByTheDedupTable(t *testing.T) {
	builders := statementBuilders(t, buildersFile)
	if len(builders) == 0 {
		t.Fatalf("no function in %s was found to return a statement, so this test asserted nothing; "+
			"the builders were renamed, moved, or the result type changed", buildersFile)
	}

	covered := functionsCalledIn(t, dedupFile, dedupTable)
	if len(covered) == 0 {
		t.Fatalf("%s in %s calls no function this test could see, so the coverage check below would "+
			"pass or fail for the wrong reason; the table was rewritten into a shape this test cannot "+
			"read", dedupTable, dedupFile)
	}

	for _, name := range builders {
		reason, exempt := dedupTableExemptions[name]
		switch {
		case exempt && slices.Contains(covered, name):
			t.Errorf("%s is exempted from %s (%q) and yet appears in its table: one of the two is "+
				"stale, and an exemption nobody re-reads is how a real omission gets excused later",
				name, dedupTable, reason)
		case exempt:
		case !slices.Contains(covered, name):
			t.Errorf("%s in %s returns a statement and does not appear in %s's table.\n"+
				"resource_states is a ReplacingMergeTree over an at-least-once write path, so a read "+
				"of it without a dedup form can return one recorded change as two rows — an unmerged "+
				"duplicate rendered as a second change in an audit timeline, the same scale-down at "+
				"the same nanosecond twice, with nothing saying the cluster did not do it twice.\n"+
				"Add %s to the table in %s. If it genuinely does not read %s, add it to "+
				"dedupTableExemptions with the reason, so the decision is written down rather than "+
				"inferred from its SQL.",
				name, buildersFile, dedupTable, name, dedupFile, tableResourceStates)
		}
	}

	// An exemption for a function that no longer exists is the same rot in the
	// other direction: it reads like a considered decision and covers nothing.
	for _, name := range slices.Sorted(maps.Keys(dedupTableExemptions)) {
		if !slices.Contains(builders, name) {
			t.Errorf("dedupTableExemptions names %q, which no longer returns a statement in %s; "+
				"remove the entry with the function", name, buildersFile)
		}
	}
}

// statementBuilders returns, sorted, the name of every function in filename whose
// single result is a statement.
//
// The result type is the test's definition of "a builder", because it is what the
// dedup table can actually take an element of. A function that assembled SQL and
// returned a string would not be one, and would be invisible here — which is why
// TestEmittedStatementsCarryADedupForm exists alongside, driving the engine's
// entry points through a recording connection and inspecting whatever reaches it.
// The two together cover both halves: what the builders can produce, and what the
// engine really sends.
func statementBuilders(t *testing.T, filename string) []string {
	t.Helper()

	file := parseSource(t, filename)
	var names []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Type.Results == nil || len(fn.Type.Results.List) != 1 {
			continue
		}
		ident, ok := fn.Type.Results.List[0].Type.(*ast.Ident)
		if ok && ident.Name == "statement" {
			names = append(names, fn.Name.Name)
		}
	}
	slices.Sort(names)
	return names
}

// functionsCalledIn returns the plainly-named functions called anywhere inside
// one declaration.
//
// Plainly named means an identifier rather than a selector: a builder is called
// as timelineStatement(...), and a method call on a fake or on testing.T is not a
// builder. Scanning the whole declaration rather than reaching into the table
// literal is deliberate — it keeps this test working if the table is restructured,
// since what it needs to know is whether the builder is exercised there at all.
func functionsCalledIn(t *testing.T, filename, funcName string) []string {
	t.Helper()

	file := parseSource(t, filename)
	var target *ast.FuncDecl
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == funcName && fn.Body != nil {
			target = fn
			break
		}
	}
	if target == nil {
		t.Fatalf("%s declares no %s, so the table this test guards has been renamed or removed; "+
			"the coverage property it asserts is now unowned", filename, funcName)
	}

	var names []string
	ast.Inspect(target.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if ident, ok := call.Fun.(*ast.Ident); ok {
			names = append(names, ident.Name)
		}
		return true
	})
	slices.Sort(names)
	return slices.Compact(names)
}

// parseSource parses one file of this package.
func parseSource(t *testing.T, filename string) *ast.File {
	t.Helper()

	file, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parsing %s: %v", filename, err)
	}
	return file
}
