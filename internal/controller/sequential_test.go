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

package controller

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestNoTestInThisPackageRunsInParallel enforces the sequential-execution rule
// stated at the top of suite_test.go.
//
// It is a source-level assertion rather than a runtime one on purpose. The constraint
// is about what code this package may *contain* in future, and a runtime check can
// only see the tests that exist: the parallel test that breaks stageRule is by
// definition the one nobody has written yet. Parsing is also what makes this test
// able to describe the violation it forbids — `t.Parallel()` appears in the failure
// message below as a string literal, and an AST walk does not confuse that with a
// call.
//
// Any selector call named Parallel is flagged, not only one whose receiver is spelled
// `t`. A subtest closure that renames its *testing.T — `func(sub *testing.T) {
// sub.Parallel() }` — breaks stageRule in exactly the same way, and nothing in this
// package has a Parallel method of its own for the broader match to trip over.
func TestNoTestInThisPackageRunsInParallel(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package directory: %v", err)
	}

	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, entry.Name(), nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			// The enclosing declaration is reported rather than the closure the call
			// sits in, because that is the name `go test -run` takes and the name the
			// reader has to go and look at.
			reportParallelCalls(t, fset, entry.Name(), fn)
		}
	}
}

// reportParallelCalls fails once per Parallel() call inside fn, naming where it is
// and what it would break.
func reportParallelCalls(t *testing.T, fset *token.FileSet, filename string, fn *ast.FuncDecl) {
	t.Helper()

	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Parallel" {
			return true
		}
		t.Errorf("%s:%d: %s calls %s.Parallel(), and tests in internal/controller must run sequentially.\n"+
			"harness.stageRule (suite_test.go) relaxes a rule CRD's schema for the duration of one write "+
			"and restores it immediately afterwards, and every test in this package shares one envtest "+
			"apiserver. A test running concurrently with that window can have its own object admitted "+
			"against the relaxed schema, or see an admission-rejection assertion pass for the wrong "+
			"reason — producing a failure whose cause is in an unrelated file and whose timing decides "+
			"whether it appears at all.\n"+
			"If this test genuinely needs parallelism, move it to a package that does not use stageRule; "+
			"the constraint is a property of sharing that helper, not of the subject under test.",
			filename, fset.Position(call.Pos()).Line, fn.Name.Name, receiverName(sel.X))
		return true
	})
}

// receiverName renders the expression a Parallel() call was made on, so the failure
// message quotes the call as it is actually written. Anything that is not a plain
// identifier is reported generically rather than half-printed.
func receiverName(expr ast.Expr) string {
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return "a *testing.T"
}
