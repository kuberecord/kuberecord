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
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// This file is the DuckDB half of the published-query harness: the variable
// vocabulary the S3 recipes are written against, and the runner that executes one
// of them.
//
// DuckDB is driven through its CLI rather than through a Go driver, and that is a
// deliberate cost decision rather than convenience. The only Go bindings are CGO
// wrappers around a bundled DuckDB build; adding one to go.mod so that a
// documentation test could run would put a database engine into the dependency
// graph of an operator that links none, and `make lint`, `make build` and every
// image build would carry it. The CLI is fetched into bin/ by the Makefile like
// promtool, stays out of go.mod entirely, and is additionally the *same* program a
// reader of docs/QUERIES.md will paste these recipes into — so what CI proves and
// what a user does are the same act.

// setVariableDecl matches one `SET VARIABLE <name> =` declaration, with or
// without the double quotes a reserved word needs (`SET VARIABLE "group"`).
//
// DuckDB has no `--param_x` equivalent, so a session variable is the closest
// thing to the ClickHouse-native `{name:Type}` parameters the rest of the library
// uses: one editable block at the top, every recipe below it copy-pasteable
// unchanged. This regexp is what lets a test assert that the block and the
// recipes agree — which matters more here than it would elsewhere, because
// getvariable() on an unset variable returns NULL rather than failing, so a
// misspelled name is a filter that quietly matches nothing.
var setVariableDecl = regexp.MustCompile(`(?im)^\s*SET\s+VARIABLE\s+"?([a-zA-Z_][a-zA-Z0-9_]*)"?\s*=`)

// getVariableRef matches one getvariable('name') reference.
var getVariableRef = regexp.MustCompile(`getvariable\(\s*'([a-zA-Z_][a-zA-Z0-9_]*)'\s*\)`)

// DeclaredVariables returns the session variables a block defines, sorted.
func DeclaredVariables(sqlText string) []string {
	return uniqueSubmatches(setVariableDecl, sqlText)
}

// ReferencedVariables returns the session variables a block reads, sorted.
//
// It is the DuckDB counterpart of Parameters: a recipe naming a variable nothing
// declares is the same defect as a dashboard panel naming a template variable its
// dashboard does not have, and it fails the same way — silently, as a filter that
// matches everything or nothing.
func ReferencedVariables(sqlText string) []string {
	return uniqueSubmatches(getVariableRef, sqlText)
}

func uniqueSubmatches(re *regexp.Regexp, text string) []string {
	seen := map[string]bool{}
	for _, m := range re.FindAllStringSubmatch(text, -1) {
		seen[m[1]] = true
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// DuckDB executes published DuckDB recipes through the duckdb CLI.
type DuckDB struct {
	// Binary is the path to the duckdb CLI. The Makefile's `duckdb` target puts
	// one in bin/ and exports it as $DUCKDB.
	Binary string

	// Preamble is the script run ahead of every recipe, in the same session: the
	// caller's own variable bindings followed by the published `duckdb-setup`
	// block. Splitting it that way is what keeps the setup a tested artifact —
	// the bindings are the test's, the extension load, the credential and the
	// globs derived from them are the document's, executed verbatim.
	Preamble string
}

// rowCountMarker prefixes the one line of a run's output the runner reads back.
//
// A marker rather than "the last line": the preamble is not silent (CREATE SECRET
// reports success), DuckDB prints extension notices on first load, and a recipe's
// own output would otherwise have to be told apart from all of it by position.
const rowCountMarker = "kuberecord-matched-rows="

// Rows executes one recipe and returns how many rows it produced.
//
// The recipe is wrapped in a counting query rather than having its output parsed,
// because what the caller needs is exactly the assertion the acceptance criterion
// asks for — that the statement runs *and* selects something — and a row count is
// that in one value, whatever shape the recipe's own result set has. The wrapper
// puts the closing parenthesis on its own line so a recipe ending in a `--`
// comment, as a commented recipe well may, does not comment it out.
//
// Everything runs in one CLI invocation, and it has to: the secret and the
// variables the preamble creates live in the session, so a recipe executed by a
// second process would be executed against a DuckDB that has never heard of the
// archive.
func (d DuckDB) Rows(ctx context.Context, recipe string) (int, error) {
	body := strings.TrimSuffix(strings.TrimSpace(recipe), ";")
	script := fmt.Sprintf("%s\nSELECT '%s' || count(*) FROM (\n%s\n) AS recipe;\n",
		d.Preamble, rowCountMarker, body)

	// -noheader -list keeps the output to one value per row with no box drawing to
	// strip, and -init /dev/null stops a developer's own ~/.duckdbrc from taking
	// part in a test run.
	cmd := exec.CommandContext(ctx, d.Binary, "-init", "/dev/null", "-noheader", "-list")
	cmd.Stdin = strings.NewReader(script)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	runErr := cmd.Run()

	// The script is echoed on every failure path. A DuckDB parse error names a
	// line number, and the line it names is a line of the wrapped script rather
	// than of the document, so without it the message points nowhere.
	if runErr != nil {
		return 0, fmt.Errorf("duckdb: %w\n--- output ---\n%s\n--- script ---\n%s", runErr, out.String(), script)
	}
	for line := range strings.SplitSeq(out.String(), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, rowCountMarker) {
			continue
		}
		count, err := strconv.Atoi(strings.TrimPrefix(trimmed, rowCountMarker))
		if err != nil {
			return 0, fmt.Errorf("duckdb: row count %q is not a number: %w", trimmed, err)
		}
		return count, nil
	}
	return 0, fmt.Errorf("duckdb: the run reported no row count\n--- output ---\n%s\n--- script ---\n%s",
		out.String(), script)
}
