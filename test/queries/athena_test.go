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
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/yelzhy/kuberecord/internal/sink"
)

func TestParseAthenaDDL(t *testing.T) {
	ddl := `
CREATE EXTERNAL TABLE t (
    -- a comment mentioning a ) and a ' quote is not structure
    ` + "`a`" + ` string,
    ` + "`m`" + ` map<string, string>
)
PARTITIONED BY (
    ` + "`p`" + ` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://b/p/format=jsonl-v1/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'storage.location.template' = 's3://b/p/format=jsonl-v1/p=${p}'
);`

	table, err := ParseAthenaDDL(ddl)
	if err != nil {
		t.Fatalf("ParseAthenaDDL: %v", err)
	}
	if table.Name != "t" {
		t.Errorf("table name is %q, want t", table.Name)
	}
	if got, want := table.ColumnNames(), []string{"a", "m"}; !slices.Equal(got, want) {
		t.Errorf("columns are %v, want %v", got, want)
	}
	// The type must survive intact: `map<string, string>` holds a comma, and a
	// splitter that took it for a column boundary would silently declare two.
	if table.Columns[1].Type != "map<string, string>" {
		t.Errorf("map column type is %q, want %q", table.Columns[1].Type, "map<string, string>")
	}
	if got, want := table.PartitionNames(), []string{"p"}; !slices.Equal(got, want) {
		t.Errorf("partitions are %v, want %v", got, want)
	}
	if table.SerDe == "" || table.StoredAs != "TEXTFILE" || table.Location != "s3://b/p/format=jsonl-v1/" {
		t.Errorf("serde/stored-as/location are %q/%q/%q", table.SerDe, table.StoredAs, table.Location)
	}
	if table.Properties["projection.enabled"] != "true" ||
		table.Properties["storage.location.template"] != "s3://b/p/format=jsonl-v1/p=${p}" {
		t.Errorf("properties are %v", table.Properties)
	}

	if _, err := ParseAthenaDDL("SELECT 1"); err == nil {
		t.Error("a statement that is not a CREATE EXTERNAL TABLE was accepted")
	}
}

// recordJSONTags returns sink.Record's JSON field names in declaration order,
// which is also the order the S3 backend writes them in one line.
func recordJSONTags(t *testing.T) []string {
	t.Helper()
	typ := reflect.TypeFor[sink.Record]()
	out := make([]string, 0, typ.NumField())
	for i := range typ.NumField() {
		tag, _, _ := strings.Cut(typ.Field(i).Tag.Get("json"), ",")
		if tag == "" {
			t.Fatalf("sink.Record field %q carries no json tag", typ.Field(i).Name)
		}
		out = append(out, tag)
	}
	return out
}

// TestAthenaDDLMatchesTheRecordContract holds the published Athena DDL against
// the two things in this repository it has to agree with: sink.Record's own JSON
// tags, which are what a line of the archive contains (D9), and the partition
// segments the writer puts in a key.
//
// **Structure only.** There is no AWS account in CI, so nothing here executes the
// DDL and nothing here proves Athena accepts it — that rests on the AWS
// documentation cited beside it in docs/QUERIES.md. What this test does prove is
// that the table names every field of the contract exactly once, in the order the
// line writes them, partitions on exactly what the key carries, and projects those
// partitions over a template whose literal segments match the layout. Those are
// the ways the DDL can go stale without anybody noticing; "Athena rejected it" is
// not, because whoever runs it finds out immediately.
func TestAthenaDDLMatchesTheRecordContract(t *testing.T) {
	library, err := FromMarkdown(repoPath("docs", "QUERIES.md"))
	if err != nil {
		t.Fatalf("FromMarkdown: %v", err)
	}
	blocks := ByDialect(library, DialectAthena)
	if len(blocks) == 0 {
		t.Fatal("docs/QUERIES.md publishes no Athena DDL; this check would pass vacuously")
	}

	var table *AthenaTable
	for _, block := range blocks {
		parsed, parseErr := ParseAthenaDDL(block.SQL)
		if parseErr != nil {
			// The section also publishes an example SELECT against the table, which
			// is not DDL; only a CREATE statement is a table.
			continue
		}
		if table != nil {
			t.Fatalf("docs/QUERIES.md publishes more than one Athena table; this test asserts about one")
		}
		table = parsed
	}
	if table == nil {
		t.Fatal("docs/QUERIES.md publishes no CREATE EXTERNAL TABLE statement")
	}

	// The partitions are the key's own, in the key's own order.
	wantPartitions := []string{"cluster_id", "date", "hour"}
	if got := table.PartitionNames(); !slices.Equal(got, wantPartitions) {
		t.Errorf("partitions are %v, want %v — the order is the key's, outermost first", got, wantPartitions)
	}

	// The columns are every logical field except the one the partition carries.
	// Exact equality including order: the order is the JSON line's, so a reader
	// comparing the two documents reads them in step, and a field added to Record
	// is a deliberate edit here rather than a silent omission.
	wantColumns := slices.DeleteFunc(recordJSONTags(t), func(tag string) bool { return tag == "cluster_id" })
	if got := table.ColumnNames(); !slices.Equal(got, wantColumns) {
		t.Errorf("columns are\n got: %v\nwant: %v\n(every sink.Record json tag except cluster_id, which is a "+
			"partition — Hive forbids one name being both)", got, wantColumns)
	}

	// A structured field declared as a string would parse and then hand every
	// query a JSON blob to pick apart, so the two that have structure are checked.
	structured := map[string]string{"labels": "map<", "actors": "array<"}
	for _, column := range table.Columns {
		if prefix, ok := structured[column.Name]; ok && !strings.HasPrefix(column.Type, prefix) {
			t.Errorf("column %q is declared %q, want a %s…> type", column.Name, column.Type, prefix)
		}
	}

	if table.SerDe == "" {
		t.Error("the table declares no ROW FORMAT SERDE, so Athena would read the lines as raw text")
	}
	if !strings.HasSuffix(table.Location, "/format=jsonl-v1/") {
		t.Errorf("LOCATION is %q, want it to end at the format partition: the table is scoped to one "+
			"version of the object contract (D15), and a later format is a second table", table.Location)
	}

	// Partition projection: enabled, every partition typed, and the two whose
	// spelling the writer fixes constrained to that spelling.
	if table.Properties["projection.enabled"] != "true" {
		t.Error("projection.enabled is not 'true', so the table needs a crawler or MSCK REPAIR after all")
	}
	for _, name := range wantPartitions {
		if table.Properties["projection."+name+".type"] == "" {
			t.Errorf("partition %q has no projection.%s.type, so Athena projects nothing for it", name, name)
		}
	}
	if got := table.Properties["projection.date.format"]; got != "yyyy-MM-dd" {
		t.Errorf("projection.date.format is %q, want yyyy-MM-dd — the spelling of the date= partition", got)
	}
	if got := table.Properties["projection.hour.digits"]; got != "2" {
		t.Errorf("projection.hour.digits is %q, want 2: the writer zero-pads the hour ('09', never '9'), "+
			"and a projection without it generates keys that do not exist", got)
	}

	// The template is the layout, spelled once more in the one place Athena reads
	// it. It has to continue LOCATION and name each partition in key order.
	template := table.Properties["storage.location.template"]
	wantTemplate := table.Location + "cluster_id=${cluster_id}/date=${date}/hour=${hour}"
	if template != wantTemplate {
		t.Errorf("storage.location.template is\n got: %q\nwant: %q", template, wantTemplate)
	}
}
