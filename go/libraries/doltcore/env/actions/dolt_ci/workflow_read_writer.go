// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dolt_ci

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
)

type WorkflowReadWriter interface {
	WorkflowReader
	WorkflowWriter
}

type doltWorkflowReadWriter struct {
	w WorkflowWriter
	r WorkflowReader
}

var _ WorkflowReadWriter = &doltWorkflowReadWriter{}

func NewDoltWorkflowReadWriter(commiterName, commiterEmail string, queryFunc QueryFunc) *doltWorkflowReadWriter {
	return &doltWorkflowReadWriter{
		w: NewWorkflowWriter(commiterName, commiterEmail, queryFunc),
		r: NewWorkflowReader(queryFunc),
	}
}

func (d *doltWorkflowReadWriter) GetWorkflow(ctx *sql.Context, db sqle.Database, workflowName string) (*Workflow, error) {
	return d.r.GetWorkflow(ctx, db, workflowName)
}

func (d *doltWorkflowReadWriter) StoreAndCommit(ctx *sql.Context, db sqle.Database, workflow *Workflow) error {
	return d.w.StoreAndCommit(ctx, db, workflow)
}
