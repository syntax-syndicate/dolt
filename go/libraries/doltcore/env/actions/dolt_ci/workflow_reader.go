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
	"context"
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"strconv"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

var expectedDoltCITablesOrdered = []string{
	doltdb.WorkflowsTableName,
	doltdb.WorkflowEventsTableName,
	doltdb.WorkflowEventTriggersTableName,
	doltdb.WorkflowEventTriggerBranchesTableName,
	doltdb.WorkflowEventTriggerActivitiesTableName,
	doltdb.WorkflowJobsTableName,
	doltdb.WorkflowStepsTableName,
	doltdb.WorkflowSavedQueryStepsTableName,
	doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName,
}

const (
	doltCITimeFormat = "2006-01-02 15:04:05"
)

type QueryCB func(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error)

type WorkflowReader interface {
	GetWorkflowAtRef(ctx context.Context, refName, cSpecStr string) (Workflow, error)
	ListWorkflowsAtRef(ctx context.Context, refName, cSpecStr string) ([]Workflow, error)
}

type doltWorkflowReader struct {
	qcb QueryCB
}

var _ WorkflowReader = &doltWorkflowReader{}

func NewWorkflowReader(qcb QueryCB) *doltWorkflowReader {
	return &doltWorkflowReader{qcb: qcb}
}

func (d *doltWorkflowReader) selectAllFromWorkflowsTableQuery() string {
	return fmt.Sprintf("select * from %s;", doltdb.WorkflowsTableName)
}

func (d *doltWorkflowReader) selectOneFromWorkflowsTableQuery(workflowName string) string {
	return fmt.Sprintf("select * from %s where name = '%s' limit 1;", doltdb.WorkflowsTableName, workflowName)
}

func (d *doltWorkflowReader) selectAllFromWorkflowEventsTableByWorkflowNameQuery(workflowName string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsWorkflowNameFkColName, workflowName)
}

func (d *doltWorkflowReader) selectAllFromWorkflowJobsTableByWorkflowNameQuery(workflowName string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsWorkflowNameFkColName, workflowName)
}

func (d *doltWorkflowReader) selectAllFromSavedQueryStepExpectedRowColumnResultsTableBySavedQueryStepIdQuery(savedQueryStepID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s' limit 1;", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName, savedQueryStepID)
}

func (d *doltWorkflowReader) selectAllFromSavedQueryStepsTableByWorkflowStepIdQuery(stepID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s' limit 1;", doltdb.WorkflowSavedQueryStepsTableName, doltdb.WorkflowSavedQueryStepsWorkflowStepIdFkColName, stepID)
}

func (d *doltWorkflowReader) selectAllFromWorkflowStepsTableByWorkflowJobIdQuery(jobID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s'", doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsWorkflowJobIdFkColName, jobID)
}

func (d *doltWorkflowReader) selectAllFromWorkflowEventTriggersTableByWorkflowEventIdQuery(eventID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName, eventID)
}

func (d *doltWorkflowReader) selectAllFromWorkflowEventTriggerBranchesTableByEventTriggerIdQuery(triggerID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerBranchesTableName, doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName, triggerID)
}

func (d *doltWorkflowReader) selectAllFromWorkflowEventTriggerActivitiesTableByEventTriggerIdQuery(triggerID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerActivitiesTableName, doltdb.WorkflowEventTriggerActivitiesWorkflowEventTriggersIdFkColName, triggerID)
}

func (d *doltWorkflowReader) newWorkflow(cvs ColumnValues) (*Workflow, error) {
	wf := &Workflow{
		Events: make([]*WorkflowEvent, 0),
		Jobs:   make([]*WorkflowJob, 0),
	}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowsCreatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			wf.CreatedAt = t
		case doltdb.WorkflowsUpdatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			wf.UpdatedAt = t
		case doltdb.WorkflowsNameColName:
			name := WorkflowName(cv.Value)
			wf.Name = &name
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflows column: %s", cv.ColumnName))
		}
	}

	return wf, nil
}

func (d *doltWorkflowReader) newWorkflowEvent(cvs ColumnValues) (*WorkflowEvent, error) {
	we := &WorkflowEvent{
		Triggers: make([]*WorkflowEventTrigger, 0),
	}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowEventsIdPkColName:
			id := WorkflowEventId(cv.Value)
			we.Id = &id
		case doltdb.WorkflowEventsEventTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}
			t, err := toWorkflowEventType(i)
			if err != nil {
				return nil, err
			}
			we.EventType = t
		case doltdb.WorkflowEventsWorkflowNameFkColName:
			name := WorkflowName(cv.Value)
			we.WorkflowNameFK = &name
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflow events column: %s", cv.ColumnName))
		}
	}

	return we, nil
}

func (d *doltWorkflowReader) newWorkflowJob(cvs ColumnValues) (*WorkflowJob, error) {
	wj := &WorkflowJob{
		Steps: make([]*WorkflowStep, 0),
	}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowJobsIdPkColName:
			id := WorkflowJobId(cv.Value)
			wj.Id = &id
		case doltdb.WorkflowJobsNameColName:
			wj.Name = cv.Value
		case doltdb.WorkflowJobsCreatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			wj.CreatedAt = t
		case doltdb.WorkflowJobsUpdatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			wj.UpdateAt = t
		case doltdb.WorkflowJobsWorkflowNameFkColName:
			name := WorkflowName(cv.Value)
			wj.WorkflowNameFK = &name
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflow jobs column: %s", cv.ColumnName))
		}
	}

	return wj, nil
}

func (d *doltWorkflowReader) newWorkflowSavedQueryStepExpectedRowColumnResult(cvs ColumnValues) (*WorkflowSavedQueryExpectedRowColumnResult, error) {
	r := &WorkflowSavedQueryExpectedRowColumnResult{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName:
			id := WorkflowSavedQueryStepId(cv.Value)
			r.WorkflowSavedQueryStepIdFK = &id
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountComparisonTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}
			t, err := toWorkflowSavedQueryExpectedRowColumnComparisonResultType(i)
			if err != nil {
				return nil, err
			}
			r.ExpectedRowCountComparisonType = t
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountComparisonTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}
			t, err := toWorkflowSavedQueryExpectedRowColumnComparisonResultType(i)
			if err != nil {
				return nil, err
			}
			r.ExpectedColumnCountComparisonType = t
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}
			r.ExpectedRowCount = int64(i)
		case doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}

			r.ExpectedColumnCount = int64(i)
		default:
			return nil, errors.New(fmt.Sprintf("unknown saved query expected row column results column: %s", cv.ColumnName))
		}
	}

	return r, nil
}

func (d *doltWorkflowReader) newWorkflowSavedQueryStep(cvs ColumnValues) (*WorkflowSavedQueryStep, error) {
	sq := &WorkflowSavedQueryStep{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowSavedQueryStepsIdPkColName:
			id := WorkflowSavedQueryStepId(cv.Value)
			sq.Id = &id
		case doltdb.WorkflowSavedQueryStepsSavedQueryNameColName:
			sq.SavedQueryName = cv.Value
		case doltdb.WorkflowSavedQueryStepsWorkflowStepIdFkColName:
			id := WorkflowStepId(cv.Value)
			sq.WorkflowStepIdFK = &id
		case doltdb.WorkflowSavedQueryStepsExpectedResultsTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}

			t, err := toWorkflowSavedQueryExpectedResultsType(i)
			if err != nil {
				return nil, err
			}

			sq.SavedQueryExpectedResultsType = t
		default:
			return nil, errors.New(fmt.Sprintf("unknown saved query step column: %s", cv.ColumnName))
		}
	}

	return sq, nil
}

func (d *doltWorkflowReader) newWorkflowStep(cvs ColumnValues) (*WorkflowStep, error) {
	ws := &WorkflowStep{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowStepsIdPkColName:
			id := WorkflowStepId(cv.Value)
			ws.Id = &id
		case doltdb.WorkflowStepsNameColName:
			ws.Name = cv.Value
		case doltdb.WorkflowStepsWorkflowJobIdFkColName:
			id := WorkflowJobId(cv.Value)
			ws.WorkflowJobIdFK = &id
		case doltdb.WorkflowStepsStepOrderColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}

			ws.StepOrder = i
		case doltdb.WorkflowStepsStepTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}

			t, err := toWorkflowStepType(i)
			if err != nil {
				return nil, err
			}

			ws.StepType = t
		case doltdb.WorkflowStepsCreatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			ws.CreatedAt = t
		case doltdb.WorkflowStepsUpdatedAtColName:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			ws.UpdatedAt = t
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflow step column: %s", cv.ColumnName))
		}
	}

	return ws, nil
}

func (d *doltWorkflowReader) newWorkflowEventTrigger(cvs ColumnValues) (*WorkflowEventTrigger, error) {
	et := &WorkflowEventTrigger{
		Activities: make([]*WorkflowEventTriggerActivity, 0),
		Branches:   make([]*WorkflowEventTriggerBranch, 0),
	}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowEventTriggersIdPkColName:
			id := WorkflowEventTriggerId(cv.Value)
			et.Id = &id
		case doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName:
			id := WorkflowEventId(cv.Value)
			et.WorkflowEventIdFK = &id
		case doltdb.WorkflowEventTriggersEventTriggerTypeColName:
			i, err := strconv.Atoi(cv.Value)
			if err != nil {
				return nil, err
			}
			t, err := toWorkflowEventTriggerType(i)
			if err != nil {
				return nil, err
			}
			et.EventTriggerType = t
		default:
			return nil, errors.New(fmt.Sprintf("unknown dworkflow event triggers column: %s", cv.ColumnName))
		}
	}

	return et, nil
}

func (d *doltWorkflowReader) newWorkflowEventTriggerBranch(cvs ColumnValues) (*WorkflowEventTriggerBranch, error) {
	tb := &WorkflowEventTriggerBranch{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowEventTriggerBranchesIdPkColName:
			id := WorkflowEventTriggerBranchId(cv.Value)
			tb.Id = &id
		case doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName:
			id := WorkflowEventTriggerId(cv.Value)
			tb.WorkflowEventTriggerIdFk = &id
		case doltdb.WorkflowEventTriggerBranchesBranchColName:
			tb.Branch = cv.Value
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflow event trigger branches column: %s", cv.ColumnName))
		}
	}

	return tb, nil
}

func (d *doltWorkflowReader) newWorkflowEventTriggerActivity(cvs ColumnValues) (*WorkflowEventTriggerActivity, error) {
	ta := &WorkflowEventTriggerActivity{}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case doltdb.WorkflowEventTriggerActivitiesIdPkColName:
			id := WorkflowEventTriggerActivityId(cv.Value)
			ta.Id = &id
		case doltdb.WorkflowEventTriggerActivitiesWorkflowEventTriggersIdFkColName:
			id := WorkflowEventTriggerId(cv.Value)
			ta.WorkflowEventTriggerIdFk = &id
		case doltdb.WorkflowEventTriggerActivitiesActivityColName:
			ta.Activity = cv.Value
		default:
			return nil, errors.New(fmt.Sprintf("unknown workflow event trigger activities column: %s", cv.ColumnName))
		}
	}

	return ta, nil
}

func (d *doltWorkflowReader) validateTables(shaTables map[string]struct{}) error {
	for _, t := range expectedDoltCITablesOrdered {
		_, ok := shaTables[t]
		if !ok {
			return errors.New(fmt.Sprintf("expected workflow table not found: %s", t))
		}
	}
	return nil
}

func (d *doltWorkflowReader) sqlReadQuery(ctx *sql.Context, query string, cb func(ctx context.Context, cvs ColumnValues) error) error {
	sch, rowIter, _, err := d.qcb(ctx, query)
	if err != nil {
		return err
	}

	rows, err := sql.RowIterToRows(ctx, rowIter)
	if err != nil {
		return err
	}

	size := len(sch)
	for _, row := range rows {

		cvs := make(ColumnValues, size)

		for i := range size {
			col := sch[i]
			val := row[i]
			cv, err := NewColumnValue(col, val)
			if err != nil {
				return err
			}
			cvs[i] = cv
		}

		err = cb(ctx, cvs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *doltWorkflowReader) readRowValues(ctx *sql.Context, db sqle.Database, cb func(ctx context.Context, cvs ColumnValues) error, query string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Read); err != nil {
		return err
	}

	if query == "" {
		return nil
	}

	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		return fmt.Errorf("Error parsing empty SQL statement")
	} else if err != nil {
		return fmt.Errorf("Error parsing SQL: %v.", err.Error())
	}

	switch sqlStatement.(type) {
	case *sqlparser.Select, *sqlparser.OtherRead, *sqlparser.Show, *sqlparser.SetOp, *sqlparser.Explain:
		return d.sqlReadQuery(ctx, query, cb)
	case *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete, *sqlparser.DDL:
		return fmt.Errorf("Error only read queries supported")
	default:
		return fmt.Errorf("Unsupported SQL statement: '%v'.", query)
	}
	return nil
}
