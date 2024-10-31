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
	"strconv"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
)

var ExpectedDoltCITablesOrdered = []doltdb.TableName{
	doltdb.TableName{Name: doltdb.WorkflowsTableName},
	doltdb.TableName{Name: doltdb.WorkflowEventsTableName},
	doltdb.TableName{Name: doltdb.WorkflowEventTriggersTableName},
	doltdb.TableName{Name: doltdb.WorkflowEventTriggerBranchesTableName},
	doltdb.TableName{Name: doltdb.WorkflowEventTriggerActivitiesTableName},
	doltdb.TableName{Name: doltdb.WorkflowJobsTableName},
	doltdb.TableName{Name: doltdb.WorkflowStepsTableName},
	doltdb.TableName{Name: doltdb.WorkflowSavedQueryStepsTableName},
	doltdb.TableName{Name: doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName},
}

const (
	doltCITimeFormat = "2006-01-02 15:04:05"
)

var ErrWorkflowNotFound = errors.New("workflow not found")
var ErrMultipleWorkflowsFound = errors.New("multiple workflows found")

type QueryFunc func(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error)

type WorkflowReader interface {
	GetWorkflow(ctx *sql.Context, db sqle.Database, workflowName string) (*Workflow, error)
}

type doltWorkflowReader struct {
	queryFunc QueryFunc
}

var _ WorkflowReader = &doltWorkflowReader{}

func NewWorkflowReader(queryFunc QueryFunc) *doltWorkflowReader {
	return &doltWorkflowReader{queryFunc: queryFunc}
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

func (d *doltWorkflowReader) validateTables(tableMap map[string]struct{}) error {
	for _, t := range ExpectedDoltCITablesOrdered {
		_, ok := tableMap[t.Name]
		if !ok {
			return errors.New(fmt.Sprintf("expected workflow table not found: %s", t))
		}
	}
	return nil
}

func (d *doltWorkflowReader) sqlReadQuery(ctx *sql.Context, query string, cb func(ctx context.Context, cvs ColumnValues) error) error {
	sch, rowIter, _, err := d.queryFunc(ctx, query)
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

func (d *doltWorkflowReader) getInitialWorkflowSavedQueryExpectedRowColumnResultBySavedQueryStepId(ctx *sql.Context, sqsID WorkflowSavedQueryStepId) (*WorkflowSavedQueryExpectedRowColumnResult, error) {
	query := d.selectAllFromSavedQueryStepExpectedRowColumnResultsTableBySavedQueryStepIdQuery(string(sqsID))
	workflowSavedQueryExpectedResults, err := d.retrieveWorkflowSavedQueryExpectedRowColumnResults(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(workflowSavedQueryExpectedResults) < 1 {
		return nil, nil
	}
	if len(workflowSavedQueryExpectedResults) > 1 {
		return nil, errors.New(fmt.Sprintf("expected no more than one row column result for saved query step: %s", sqsID))
	}
	return workflowSavedQueryExpectedResults[0], nil
}

func (d *doltWorkflowReader) getInitialWorkflowSavedQueryStepsByStepId(ctx *sql.Context, stepID WorkflowStepId) (*WorkflowSavedQueryStep, error) {
	query := d.selectAllFromSavedQueryStepsTableByWorkflowStepIdQuery(string(stepID))
	savedQuerySteps, err := d.retrieveWorkflowSavedQuerySteps(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(savedQuerySteps) < 1 {
		return nil, nil
	}
	if len(savedQuerySteps) > 1 {
		return nil, errors.New(fmt.Sprintf("expected no more than one saved query step for step: %s", stepID))
	}
	return savedQuerySteps[0], nil
}

func (d *doltWorkflowReader) listInitialWorkflowStepsByJobId(ctx *sql.Context, jobID WorkflowJobId) ([]*WorkflowStep, error) {
	query := d.selectAllFromWorkflowStepsTableByWorkflowJobIdQuery(string(jobID))
	return d.retrieveWorkflowSteps(ctx, query)
}

func (d *doltWorkflowReader) listInitialWorkflowJobsByWorkflowName(ctx *sql.Context, workflowName string) ([]*WorkflowJob, error) {
	query := d.selectAllFromWorkflowJobsTableByWorkflowNameQuery(string(workflowName))
	return d.retrieveWorkflowJobs(ctx, query)
}

func (d *doltWorkflowReader) listInitialWorkflowEventTriggerActivitiesByEventTriggerId(ctx *sql.Context, triggerID WorkflowEventTriggerId) ([]*WorkflowEventTriggerActivity, error) {
	query := d.selectAllFromWorkflowEventTriggerActivitiesTableByEventTriggerIdQuery(string(triggerID))
	return d.retrieveWorkflowEventTriggerActivities(ctx, query)
}

func (d *doltWorkflowReader) listInitialWorkflowEventTriggersByEventId(ctx *sql.Context, eventID WorkflowEventId) ([]*WorkflowEventTrigger, error) {
	query := d.selectAllFromWorkflowEventTriggersTableByWorkflowEventIdQuery(string(eventID))
	return d.retrieveWorkflowEventTriggers(ctx, query)

}

func (d *doltWorkflowReader) listInitialWorkflowEventsByWorkflowName(ctx *sql.Context, workflowName string) ([]*WorkflowEvent, error) {
	query := d.selectAllFromWorkflowEventsTableByWorkflowNameQuery(string(workflowName))
	return d.retrieveWorkflowEvent(ctx, query)
}

func (d *doltWorkflowReader) listInitialWorkflowEventTriggerBranchesByEventTriggerId(ctx *sql.Context, triggerID WorkflowEventTriggerId) ([]*WorkflowEventTriggerBranch, error) {
	query := d.selectAllFromWorkflowEventTriggerBranchesTableByEventTriggerIdQuery(string(triggerID))
	return d.retrieveWorkflowEventTriggerBranches(ctx, query)
}

func (d *doltWorkflowReader) retrieveWorkflowSavedQueryExpectedRowColumnResults(ctx *sql.Context, query string) ([]*WorkflowSavedQueryExpectedRowColumnResult, error) {
	workflowSavedQueryExpectedResults := make([]*WorkflowSavedQueryExpectedRowColumnResult, 0)

	cb := func(cbCtx context.Context, cvs ColumnValues) error {
		er, rerr := d.newWorkflowSavedQueryStepExpectedRowColumnResult(cvs)
		if rerr != nil {
			return rerr
		}

		workflowSavedQueryExpectedResults = append(workflowSavedQueryExpectedResults, er)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowSavedQueryExpectedResults, nil
}

func (d *doltWorkflowReader) retrieveWorkflowSavedQuerySteps(ctx *sql.Context, query string) ([]*WorkflowSavedQueryStep, error) {
	workflowSavedQuerySteps := make([]*WorkflowSavedQueryStep, 0)

	cb := func(cbCtx context.Context, cvs ColumnValues) error {
		sq, rerr := d.newWorkflowSavedQueryStep(cvs)
		if rerr != nil {
			return rerr
		}

		workflowSavedQuerySteps = append(workflowSavedQuerySteps, sq)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowSavedQuerySteps, nil
}

func (d *doltWorkflowReader) retrieveWorkflowSteps(ctx *sql.Context, query string) ([]*WorkflowStep, error) {
	workflowSteps := make([]*WorkflowStep, 0)

	cb := func(cbCtx context.Context, cvs ColumnValues) error {
		s, rerr := d.newWorkflowStep(cvs)
		if rerr != nil {
			return rerr
		}

		workflowSteps = append(workflowSteps, s)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowSteps, nil
}

func (d *doltWorkflowReader) retrieveWorkflowJobs(ctx *sql.Context, query string) ([]*WorkflowJob, error) {
	workflowJobs := make([]*WorkflowJob, 0)

	cb := func(cbCtx context.Context, cvs ColumnValues) error {
		j, rerr := d.newWorkflowJob(cvs)
		if rerr != nil {
			return rerr
		}
		workflowJobs = append(workflowJobs, j)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowJobs, nil
}

func (d *doltWorkflowReader) retrieveWorkflowEventTriggerActivities(ctx *sql.Context, query string) ([]*WorkflowEventTriggerActivity, error) {
	workflowEventTriggerActivities := make([]*WorkflowEventTriggerActivity, 0)

	cb := func(cbCtx context.Context, cvs ColumnValues) error {
		a, rerr := d.newWorkflowEventTriggerActivity(cvs)
		if rerr != nil {
			return rerr
		}
		workflowEventTriggerActivities = append(workflowEventTriggerActivities, a)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowEventTriggerActivities, nil
}

func (d *doltWorkflowReader) retrieveWorkflowEventTriggerBranches(ctx *sql.Context, query string) ([]*WorkflowEventTriggerBranch, error) {
	workflowEventTriggerBranches := make([]*WorkflowEventTriggerBranch, 0)

	cb := func(cbCtx context.Context, cvs ColumnValues) error {
		b, rerr := d.newWorkflowEventTriggerBranch(cvs)
		if rerr != nil {
			return rerr
		}

		workflowEventTriggerBranches = append(workflowEventTriggerBranches, b)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowEventTriggerBranches, nil
}

func (d *doltWorkflowReader) retrieveWorkflowEventTriggers(ctx *sql.Context, query string) ([]*WorkflowEventTrigger, error) {
	workflowEventTriggers := make([]*WorkflowEventTrigger, 0)

	cb := func(cbCtx context.Context, cvs ColumnValues) error {
		wet, rerr := d.newWorkflowEventTrigger(cvs)
		if rerr != nil {
			return rerr
		}
		workflowEventTriggers = append(workflowEventTriggers, wet)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowEventTriggers, nil
}

func (d *doltWorkflowReader) retrieveWorkflowEvent(ctx *sql.Context, query string) ([]*WorkflowEvent, error) {
	workflowEvents := make([]*WorkflowEvent, 0)

	cb := func(cbCtx context.Context, cvs ColumnValues) error {
		we, rerr := d.newWorkflowEvent(cvs)
		if rerr != nil {
			return rerr
		}

		workflowEvents = append(workflowEvents, we)
		return nil
	}

	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}

	return workflowEvents, nil
}

func (d *doltWorkflowReader) retrieveWorkflows(ctx *sql.Context, query string) ([]*Workflow, error) {
	workflows := make([]*Workflow, 0)
	cb := func(cbCtx context.Context, cvs ColumnValues) error {
		wf, rerr := d.newWorkflow(cvs)
		if rerr != nil {
			return rerr
		}
		workflows = append(workflows, wf)
		return nil
	}
	err := d.sqlReadQuery(ctx, query, cb)
	if err != nil {
		return nil, err
	}
	return workflows, nil
}

func (d *doltWorkflowReader) getInitialWorkflow(ctx *sql.Context, workflowName string) (*Workflow, error) {
	query := d.selectOneFromWorkflowsTableQuery(string(workflowName))

	workflows, err := d.retrieveWorkflows(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(workflows) == 0 {
		return nil, ErrWorkflowNotFound
	}
	if len(workflows) > 1 {
		return nil, ErrMultipleWorkflowsFound
	}
	return workflows[0], nil
}

func (d *doltWorkflowReader) listInitialWorkflowsAtRefPage(ctx *sql.Context) ([]*Workflow, error) {
	query := d.selectAllFromWorkflowsTableQuery()
	return d.retrieveWorkflows(ctx, query)
}

func (d *doltWorkflowReader) listWorkflowsAtRef(ctx *sql.Context) ([]*Workflow, error) {
	workflows, err := d.listInitialWorkflowsAtRefPage(ctx)
	if err != nil {
		return nil, err
	}

	for _, workflow := range workflows {
		if workflow.Name == nil {
			return nil, ErrWorkflowNameIsNil
		}
		err = d.updateWorkflowEvents(ctx, workflow, string(*workflow.Name))
		if err != nil {
			return nil, err
		}

		err = d.updateWorkflowJobs(ctx, workflow, string(*workflow.Name))
		if err != nil {
			return nil, err
		}
	}

	return workflows, nil
}

func (d *doltWorkflowReader) updateWorkflowEvents(ctx *sql.Context, workflow *Workflow, workflowName string) error {
	events, err := d.listInitialWorkflowEventsByWorkflowName(ctx, workflowName)
	if err != nil {
		return err
	}

	for _, event := range events {
		if event.Id == nil {
			return errors.New("workflow event id was nil")
		}

		triggers, err := d.listInitialWorkflowEventTriggersByEventId(ctx, *event.Id)
		if err != nil {
			return err
		}

		for _, trigger := range triggers {
			if trigger.Id == nil {
				return errors.New("workflow trigger id was nil")
			}

			triggerBranches, err := d.listInitialWorkflowEventTriggerBranchesByEventTriggerId(ctx, *trigger.Id)
			if err != nil {
				return err
			}

			triggerActivities, err := d.listInitialWorkflowEventTriggerActivitiesByEventTriggerId(ctx, *trigger.Id)
			if err != nil {
				return err
			}

			trigger.Branches = triggerBranches
			trigger.Activities = triggerActivities
		}

		event.Triggers = triggers
	}

	workflow.Events = events
	return nil
}

func (d *doltWorkflowReader) updateWorkflowJobs(ctx *sql.Context, workflow *Workflow, workflowName string) error {
	jobs, err := d.listInitialWorkflowJobsByWorkflowName(ctx, workflowName)
	if err != nil {
		return err
	}

	for _, job := range jobs {
		if job.Id == nil {
			return errors.New("workflow job id was nil")
		}

		steps, err := d.listInitialWorkflowStepsByJobId(ctx, *job.Id)
		if err != nil {
			return err
		}

		for _, step := range steps {
			if step.Id == nil {
				return errors.New("workflow step id was nil")
			}

			savedQueryStep, err := d.getInitialWorkflowSavedQueryStepsByStepId(ctx, *step.Id)
			if err != nil {
				return err
			}

			if savedQueryStep != nil && savedQueryStep.Id != nil {
				savedQueryExpectedResult, err := d.getInitialWorkflowSavedQueryExpectedRowColumnResultBySavedQueryStepId(ctx, *savedQueryStep.Id)
				if err != nil {
					return err
				}

				savedQueryStep.ExpectedRowColumnResult = savedQueryExpectedResult
				step.SavedQueryStep = savedQueryStep
			}
		}

		job.Steps = steps
	}

	workflow.Jobs = jobs
	return nil
}

func (d *doltWorkflowReader) getWorkflow(ctx *sql.Context, workflowName string) (*Workflow, error) {
	workflow, err := d.getInitialWorkflow(ctx, workflowName)
	if err != nil {
		return nil, err
	}

	err = d.updateWorkflowEvents(ctx, workflow, workflowName)
	if err != nil {
		return nil, err
	}

	err = d.updateWorkflowJobs(ctx, workflow, workflowName)
	if err != nil {
		return nil, err
	}

	return workflow, nil
}

func (d *doltWorkflowReader) GetWorkflow(ctx *sql.Context, db sqle.Database, workflowName string) (*Workflow, error) {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Read); err != nil {
		return nil, err
	}

	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	_, exists := dSess.GetDoltDB(ctx, dbName)
	if !exists {
		return nil, fmt.Errorf("database not found in database %s", dbName)
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return nil, fmt.Errorf("roots not found in database %s", dbName)
	}

	root := roots.Working

	tables, err := root.GetTableNames(ctx, doltdb.DefaultSchemaName)
	if err != nil {
		return nil, err
	}

	tableMap := make(map[string]struct{})
	for _, table := range tables {
		if doltdb.IsDoltCITable(table) {
			tableMap[table] = struct{}{}
		}
	}

	err = d.validateTables(tableMap)
	if err != nil {
		return nil, err
	}

	return d.getWorkflow(ctx, workflowName)
}
