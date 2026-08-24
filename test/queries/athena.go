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
	"fmt"
	"regexp"
	"strings"
)

// This file parses the Athena DDL kuberecord publishes for the S3 archive, so a
// test can hold it against two things that *are* in the repository: the logical
// record contract (sink.Record's own JSON tags) and the object key layout
// (internal/sink/s3/encoder.go).
//
// That is the whole of the coverage available here, and the limit is worth
// stating rather than working around: CI has no AWS account, so nothing executes
// this DDL. What is asserted is that it names every field of the contract exactly
// once, partitions on exactly the segments the writer produces, and projects
// those partitions over the template the writer's keys actually match. What is
// *not* asserted is that Athena accepts it — that claim rests on the AWS
// documentation cited beside the DDL, and a reader deserves to know which of the
// two kinds of confidence they are being offered.

// AthenaColumn is one column of a Hive external table: its name as the DDL
// spells it (backticks removed) and its declared type.
type AthenaColumn struct {
	Name string
	Type string
}

// AthenaTable is the structure of one CREATE EXTERNAL TABLE statement.
type AthenaTable struct {
	Name string
	// Columns are the data columns, in declaration order; Partitions are the
	// PARTITIONED BY columns. Hive keeps the two disjoint — a name cannot be both
	// — which is why cluster_id appears in one and not the other.
	Columns    []AthenaColumn
	Partitions []AthenaColumn
	SerDe      string
	StoredAs   string
	Location   string
	// Properties is the TBLPROPERTIES map, which for a projected table is where
	// the whole partition contract lives.
	Properties map[string]string
}

// ColumnNames returns the data column names in declaration order.
func (t *AthenaTable) ColumnNames() []string { return namesOf(t.Columns) }

// PartitionNames returns the partition column names in declaration order.
func (t *AthenaTable) PartitionNames() []string { return namesOf(t.Partitions) }

func namesOf(columns []AthenaColumn) []string {
	out := make([]string, 0, len(columns))
	for _, c := range columns {
		out = append(out, c.Name)
	}
	return out
}

var (
	athenaCreate = regexp.MustCompile(`(?is)CREATE\s+EXTERNAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` +
		"`?" + `([a-zA-Z_][a-zA-Z0-9_.]*)` + "`?")
	athenaPartitions = regexp.MustCompile(`(?is)PARTITIONED\s+BY\s*`)
	athenaProperties = regexp.MustCompile(`(?is)TBLPROPERTIES\s*`)
	athenaSerDe      = regexp.MustCompile(`(?is)ROW\s+FORMAT\s+SERDE\s+'([^']*)'`)
	athenaStoredAs   = regexp.MustCompile(`(?is)STORED\s+AS\s+([a-zA-Z_]+)`)
	athenaLocation   = regexp.MustCompile(`(?is)LOCATION\s+'([^']*)'`)
	athenaColumnDecl = regexp.MustCompile("(?s)^`?([a-zA-Z_][a-zA-Z0-9_]*)`?\\s+(.+)$")
	athenaProperty   = regexp.MustCompile(`(?s)^'([^']*)'\s*=\s*'([^']*)'$`)
)

// ParseAthenaDDL reads one CREATE EXTERNAL TABLE statement into its structure.
//
// It is a structural parser, not a SQL implementation: it understands the clauses
// this project's own DDL uses and rejects anything it cannot account for, which is
// the right failure for a test fixture. Comments are stripped first, so the
// published DDL can be as heavily commented as the rest of the library without
// the parser having to know what a comment means inside a type.
func ParseAthenaDDL(ddl string) (*AthenaTable, error) {
	stmt := stripSQLLineComments(ddl)

	head := athenaCreate.FindStringSubmatchIndex(stmt)
	if head == nil {
		return nil, fmt.Errorf("athena: no CREATE EXTERNAL TABLE statement")
	}
	table := &AthenaTable{Name: stmt[head[2]:head[3]], Properties: map[string]string{}}

	columnList, afterColumns, err := balancedGroup(stmt, head[1])
	if err != nil {
		return nil, fmt.Errorf("athena: column list of %s: %w", table.Name, err)
	}
	if table.Columns, err = parseAthenaColumns(columnList); err != nil {
		return nil, fmt.Errorf("athena: column list of %s: %w", table.Name, err)
	}

	tail := stmt[afterColumns:]
	if loc := athenaPartitions.FindStringIndex(tail); loc != nil {
		partitionList, _, pErr := balancedGroup(tail, loc[1])
		if pErr != nil {
			return nil, fmt.Errorf("athena: PARTITIONED BY of %s: %w", table.Name, pErr)
		}
		if table.Partitions, pErr = parseAthenaColumns(partitionList); pErr != nil {
			return nil, fmt.Errorf("athena: PARTITIONED BY of %s: %w", table.Name, pErr)
		}
	}
	if loc := athenaProperties.FindStringIndex(tail); loc != nil {
		propertyList, _, tErr := balancedGroup(tail, loc[1])
		if tErr != nil {
			return nil, fmt.Errorf("athena: TBLPROPERTIES of %s: %w", table.Name, tErr)
		}
		if table.Properties, tErr = parseAthenaProperties(propertyList); tErr != nil {
			return nil, fmt.Errorf("athena: TBLPROPERTIES of %s: %w", table.Name, tErr)
		}
	}
	if m := athenaSerDe.FindStringSubmatch(tail); m != nil {
		table.SerDe = m[1]
	}
	if m := athenaStoredAs.FindStringSubmatch(tail); m != nil {
		table.StoredAs = m[1]
	}
	if m := athenaLocation.FindStringSubmatch(tail); m != nil {
		table.Location = m[1]
	}
	return table, nil
}

func parseAthenaColumns(list string) ([]AthenaColumn, error) {
	entries, err := splitTopLevel(list)
	if err != nil {
		return nil, err
	}
	out := make([]AthenaColumn, 0, len(entries))
	for _, entry := range entries {
		m := athenaColumnDecl.FindStringSubmatch(entry)
		if m == nil {
			return nil, fmt.Errorf("%q is not a `name type` declaration", entry)
		}
		out = append(out, AthenaColumn{Name: m[1], Type: strings.Join(strings.Fields(m[2]), " ")})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("the list is empty")
	}
	return out, nil
}

func parseAthenaProperties(list string) (map[string]string, error) {
	entries, err := splitTopLevel(list)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(entries))
	for _, entry := range entries {
		m := athenaProperty.FindStringSubmatch(entry)
		if m == nil {
			return nil, fmt.Errorf("%q is not a 'key' = 'value' pair", entry)
		}
		if _, dup := out[m[1]]; dup {
			return nil, fmt.Errorf("property %q is set twice", m[1])
		}
		out[m[1]] = m[2]
	}
	return out, nil
}

// balancedGroup returns the contents of the parenthesised group beginning at or
// after from, and the offset just past its closing parenthesis.
//
// Quoted strings are skipped over, so a parenthesis inside a location template
// (or an apostrophe inside a comment that survived stripping) cannot close the
// group early.
func balancedGroup(s string, from int) (string, int, error) {
	open := strings.Index(s[from:], "(")
	if open < 0 {
		return "", 0, fmt.Errorf("no parenthesised group follows")
	}
	open += from

	depth, quoted := 0, false
	for i := open; i < len(s); i++ {
		switch {
		case s[i] == '\'':
			quoted = !quoted
		case quoted:
		case s[i] == '(':
			depth++
		case s[i] == ')':
			depth--
			if depth == 0 {
				return s[open+1 : i], i + 1, nil
			}
		}
	}
	return "", 0, fmt.Errorf("the group opened at offset %d is never closed", open)
}

// splitTopLevel splits a comma-separated list on the commas that are not inside
// parentheses, angle brackets or quotes — so `map<string, string>` is one entry
// rather than two.
func splitTopLevel(list string) ([]string, error) {
	var out []string
	depth, quoted, start := 0, false, 0
	for i := 0; i < len(list); i++ {
		switch {
		case list[i] == '\'':
			quoted = !quoted
		case quoted:
		case list[i] == '(' || list[i] == '<':
			depth++
		case list[i] == ')' || list[i] == '>':
			depth--
		case list[i] == ',' && depth == 0:
			out = append(out, strings.TrimSpace(list[start:i]))
			start = i + 1
		}
	}
	if quoted {
		return nil, fmt.Errorf("the list ends inside a quoted string")
	}
	if trailing := strings.TrimSpace(list[start:]); trailing != "" {
		out = append(out, trailing)
	}
	return out, nil
}

// stripSQLLineComments removes `--` comments, leaving anything that looks like
// one inside a quoted string alone.
func stripSQLLineComments(sqlText string) string {
	var b strings.Builder
	for line := range strings.SplitSeq(sqlText, "\n") {
		quoted := false
		cut := len(line)
		for i := 0; i < len(line); i++ {
			if line[i] == '\'' {
				quoted = !quoted
				continue
			}
			if !quoted && line[i] == '-' && i+1 < len(line) && line[i+1] == '-' {
				cut = i
				break
			}
		}
		b.WriteString(line[:cut])
		b.WriteString("\n")
	}
	return b.String()
}
