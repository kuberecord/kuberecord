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

package replay

import (
	"github.com/kuberecord/kuberecord/internal/cli/render"
	"github.com/kuberecord/kuberecord/internal/query"
)

// DecodeRows turns changes into rows, decoding each patch once.
//
// A patch that will not decode is carried as an error on the row rather than
// dropped: the change still happened, and an audit timeline missing an entry is
// worse than one carrying a cell that says the patch was unreadable.
func DecodeRows(changes []query.Change) []render.TimelineRow {
	rows := make([]render.TimelineRow, 0, len(changes))
	for _, change := range changes {
		row := render.TimelineRow{Change: change}
		ops, err := render.PatchOps(change.Diff)
		if err != nil {
			row.PatchErr = err.Error()
		}
		row.Ops = ops
		rows = append(rows, row)
	}
	return rows
}
