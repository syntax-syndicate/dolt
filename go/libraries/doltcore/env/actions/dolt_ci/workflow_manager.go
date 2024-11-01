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
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/google/uuid"
	"strconv"
	"time"
)

const (
	doltCITimeFormat = "2006-01-02 15:04:05"
)

var ErrWorkflowNameIsNil = errors.New("workflow name is nil")
var ErrWorkflowNotFound = errors.New("workflow not found")
var ErrMultipleWorkflowsFound = errors.New("multiple workflows found")

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

type QueryFunc func(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error)

type WorkflowManager interface {
	StoreAndCommit(ctx *sql.Context, db sqle.Database, config *WorkflowConfig) error
}

type doltWorkflowManager struct {
	commiterName  string
	commiterEmail string
	queryFunc     QueryFunc
}

var _ WorkflowManager = &doltWorkflowManager{}

func NewWorkflowManager(commiterName, commiterEmail string, queryFunc QueryFunc) *doltWorkflowManager {
	return &doltWorkflowManager{
		commiterName:  commiterName,
		commiterEmail: commiterEmail,
		queryFunc:     queryFunc,
	}
}

func (d *doltWorkflowManager) selectAllFromWorkflowsTableQuery() string {
	return fmt.Sprintf("select * from %s;", doltdb.WorkflowsTableName)
}

func (d *doltWorkflowManager) selectOneFromWorkflowsTableQuery(workflowName string) string {
	return fmt.Sprintf("select * from %s where name = '%s' limit 1;", doltdb.WorkflowsTableName, workflowName)
}

func (d *doltWorkflowManager) selectAllFromWorkflowEventsTableByWorkflowNameQuery(workflowName string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsWorkflowNameFkColName, workflowName)
}

func (d *doltWorkflowManager) selectAllFromWorkflowJobsTableByWorkflowNameQuery(workflowName string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsWorkflowNameFkColName, workflowName)
}

func (d *doltWorkflowManager) selectAllFromSavedQueryStepExpectedRowColumnResultsTableBySavedQueryStepIdQuery(savedQueryStepID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s' limit 1;", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName, savedQueryStepID)
}

func (d *doltWorkflowManager) selectAllFromSavedQueryStepsTableByWorkflowStepIdQuery(stepID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s' limit 1;", doltdb.WorkflowSavedQueryStepsTableName, doltdb.WorkflowSavedQueryStepsWorkflowStepIdFkColName, stepID)
}

func (d *doltWorkflowManager) selectAllFromWorkflowStepsTableByWorkflowJobIdQuery(jobID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s'", doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsWorkflowJobIdFkColName, jobID)
}

func (d *doltWorkflowManager) selectAllFromWorkflowEventTriggersTableByWorkflowEventIdQuery(eventID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName, eventID)
}

func (d *doltWorkflowManager) selectAllFromWorkflowEventTriggerBranchesTableByEventTriggerIdQuery(triggerID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerBranchesTableName, doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName, triggerID)
}

func (d *doltWorkflowManager) selectAllFromWorkflowEventTriggerActivitiesTableByEventTriggerIdQuery(triggerID string) string {
	return fmt.Sprintf("select * from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerActivitiesTableName, doltdb.WorkflowEventTriggerActivitiesWorkflowEventTriggersIdFkColName, triggerID)
}

// todo: add select by ids for each thing

// todo: add inserts for each thing
func (d *doltWorkflowManager) insertIntoWorkflowsTableQuery(workflowName string) (string, string) {
	return workflowName, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`) values ('%s', now(), now());", doltdb.WorkflowsTableName, doltdb.WorkflowsNameColName, doltdb.WorkflowsCreatedAtColName, doltdb.WorkflowsUpdatedAtColName, workflowName)
}

func (d *doltWorkflowManager) insertIntoWorkflowEventsTableQuery(workflowName string, eventType int) (string, string) {
	eventID := uuid.NewString()
	return eventID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`) values ('%s', '%s', %d);", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsIdPkColName, doltdb.WorkflowEventsWorkflowNameFkColName, doltdb.WorkflowEventsEventTypeColName, eventID, workflowName, eventType)
}

func (d *doltWorkflowManager) insertIntoWorkflowEventTriggersTableQuery(eventID string, triggerType int) (string, string) {
	triggerID := uuid.NewString()
	return triggerID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`) values ('%s', '%s', %d);", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersIdPkColName, doltdb.WorkflowEventTriggersWorkflowEventsIdFkColName, doltdb.WorkflowEventTriggersEventTriggerTypeColName, triggerID, eventID, triggerType)
}

func (d *doltWorkflowManager) insertIntoWorkflowEventTriggerBranchesTableQuery(triggerID, branch string) (string, string) {
	branchID := uuid.NewString()
	return branchID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`) values ('%s', '%s', '%s');", doltdb.WorkflowEventTriggerBranchesTableName, doltdb.WorkflowEventTriggerBranchesIdPkColName, doltdb.WorkflowEventTriggerBranchesWorkflowEventTriggersIdFkColName, doltdb.WorkflowEventTriggerBranchesBranchColName, branchID, triggerID, branch)
}

func (d *doltWorkflowManager) insertIntoWorkflowEventTriggerActivitiesTableQuery(triggerID, activity string) (string, string) {
	activityID := uuid.NewString()
	return activityID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`) values ('%s', '%s', '%s');", doltdb.WorkflowEventTriggerActivitiesTableName, doltdb.WorkflowEventTriggerActivitiesIdPkColName, doltdb.WorkflowEventTriggerActivitiesWorkflowEventTriggersIdFkColName, doltdb.WorkflowEventTriggerActivitiesActivityColName, activityID, triggerID, activity)
}

func (d *doltWorkflowManager) insertIntoWorkflowJobsTableQuery(jobName, workflowName string) (string, string) {
	jobID := uuid.NewString()
	return jobID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`, `%s`, `%s`) values ('%s', '%s', '%s', now(), now());", doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsIdPkColName, doltdb.WorkflowJobsNameColName, doltdb.WorkflowJobsWorkflowNameFkColName, doltdb.WorkflowJobsCreatedAtColName, doltdb.WorkflowJobsUpdatedAtColName, jobID, jobName, workflowName)
}

func (d *doltWorkflowManager) insertIntoWorkflowStepsTableQuery(stepName, jobID string, stepOrder, stepType int) (string, string) {
	stepID := uuid.NewString()
	return stepID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`, `%s`, `%s`, `%s`, `%s`) values ('%s', '%s', '%s', %d, %d, now(), now());", doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsIdPkColName, doltdb.WorkflowStepsNameColName, doltdb.WorkflowStepsWorkflowJobIdFkColName, doltdb.WorkflowStepsStepOrderColName, doltdb.WorkflowStepsStepTypeColName, doltdb.WorkflowStepsCreatedAtColName, doltdb.WorkflowStepsUpdatedAtColName, stepID, stepName, jobID, stepOrder, stepType)
}

func (d *doltWorkflowManager) insertIntoWorkflowSavedQueryStepsTableQuery(savedQueryName, stepID string, expectedResultsType int) (string, string) {
	savedQueryStepID := uuid.NewString()
	return savedQueryStepID, fmt.Sprintf("insert into %s (`%s`, `%s`, `%s`, `%s`) values ('%s', '%s', '%s', %d);", doltdb.WorkflowSavedQueryStepsTableName, doltdb.WorkflowSavedQueryStepsIdPkColName, doltdb.WorkflowSavedQueryStepsWorkflowStepIdFkColName, doltdb.WorkflowSavedQueryStepsSavedQueryNameColName, doltdb.WorkflowSavedQueryStepsExpectedResultsTypeColName, savedQueryStepID, stepID, savedQueryName, expectedResultsType, stepID)
}

func (d *doltWorkflowManager) insertIntoWorkflowSavedQueryStepExpectedRowColumnResultsTableQuery(savedQueryStepID string, expectedColumnComparisonType, expectedRowComparisonType int, expectedColumnCount, expectedRowCount int64) (string, string) {
	return savedQueryStepID, fmt.Sprintf("insert into %s (`saved_query_step_id_fk`, `expected_column_count_comparison_type`,`expected_row_count_comparison_type`, `expected_column_count`, `expected_row_count`) values ('%s', %d, %d, %d, %d);", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountComparisonTypeColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountComparisonTypeColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedColumnCountColName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsExpectedRowCountColName, savedQueryStepID, expectedColumnComparisonType, expectedRowComparisonType, expectedColumnCount, expectedRowCount)
}

// todo: add sql for update to each thing??

// todo: add sql for delete from each thing
func (d *doltWorkflowManager) deleteFromWorkflowsTableByWorkflowNameQuery(workflowName string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowsTableName, doltdb.WorkflowsNameColName, workflowName)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventsTableByWorkflowEventIdQuery(eventId string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowEventsTableName, doltdb.WorkflowEventsIdPkColName, eventId)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventTriggersTableByWorkflowEventTriggerIdQuery(triggerID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowEventTriggersTableName, doltdb.WorkflowEventTriggersIdPkColName, triggerID)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventTriggerBranchesTableByEventTriggerBranchIdQuery(branchID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerBranchesTableName, doltdb.WorkflowEventTriggerBranchesIdPkColName, branchID)
}

func (d *doltWorkflowManager) deleteFromWorkflowEventTriggerActivitiesTableByEventTriggerActivityIdQuery(activityID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowEventTriggerActivitiesTableName, doltdb.WorkflowEventTriggerActivitiesIdPkColName, activityID)
}

func (d *doltWorkflowManager) deleteFromWorkflowJobsTableByWorkflowJobIdQuery(jobID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowJobsTableName, doltdb.WorkflowJobsIdPkColName, jobID)
}

func (d *doltWorkflowManager) deleteFromWorkflowStepsTableByWorkflowStepIdQuery(stepID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowStepsTableName, doltdb.WorkflowStepsIdPkColName, stepID)
}

func (d *doltWorkflowManager) deleteFromSavedQueryStepsTableByWorkflowStepIdQuery(savedQueryStepID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowSavedQueryStepsTableName, doltdb.WorkflowSavedQueryStepsIdPkColName, savedQueryStepID)
}

func (d *doltWorkflowManager) deleteFromSavedQueryStepExpectedRowColumnResultsTableBySavedQueryStepIdQuery(savedQueryStepID string) string {
	return fmt.Sprintf("delete from %s where `%s` = '%s';", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsSavedQueryStepIdFkColName, savedQueryStepID)
}

func (d *doltWorkflowManager) newWorkflow(cvs ColumnValues) (*Workflow, error) {
	wf := &Workflow{}

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

func (d *doltWorkflowManager) newWorkflowEvent(cvs ColumnValues) (*WorkflowEvent, error) {
	we := &WorkflowEvent{}

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

func (d *doltWorkflowManager) newWorkflowJob(cvs ColumnValues) (*WorkflowJob, error) {
	wj := &WorkflowJob{}

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

func (d *doltWorkflowManager) newWorkflowSavedQueryStepExpectedRowColumnResult(cvs ColumnValues) (*WorkflowSavedQueryExpectedRowColumnResult, error) {
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

func (d *doltWorkflowManager) newWorkflowSavedQueryStep(cvs ColumnValues) (*WorkflowSavedQueryStep, error) {
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

func (d *doltWorkflowManager) newWorkflowStep(cvs ColumnValues) (*WorkflowStep, error) {
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

func (d *doltWorkflowManager) newWorkflowEventTrigger(cvs ColumnValues) (*WorkflowEventTrigger, error) {
	et := &WorkflowEventTrigger{}

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

func (d *doltWorkflowManager) newWorkflowEventTriggerBranch(cvs ColumnValues) (*WorkflowEventTriggerBranch, error) {
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

func (d *doltWorkflowManager) newWorkflowEventTriggerActivity(cvs ColumnValues) (*WorkflowEventTriggerActivity, error) {
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

func (d *doltWorkflowManager) validateWorkflowTables(ctx *sql.Context) error {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	_, exists := dSess.GetDoltDB(ctx, dbName)
	if !exists {
		return fmt.Errorf("database not found in database %s", dbName)
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return fmt.Errorf("roots not found in database %s", dbName)
	}

	root := roots.Working

	tables, err := root.GetTableNames(ctx, doltdb.DefaultSchemaName)
	if err != nil {
		return err
	}

	tableMap := make(map[string]struct{})
	for _, table := range tables {
		if doltdb.IsDoltCITable(table) {
			tableMap[table] = struct{}{}
		}
	}

	for _, t := range ExpectedDoltCITablesOrdered {
		_, ok := tableMap[t.Name]
		if !ok {
			return errors.New(fmt.Sprintf("expected workflow table not found: %s", t))
		}
	}

	return nil
}

func (d *doltWorkflowManager) sqlReadQuery(ctx *sql.Context, query string, cb func(ctx *sql.Context, cvs ColumnValues) error) error {
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

func (d *doltWorkflowManager) getWorkflowSavedQueryExpectedRowColumnResultBySavedQueryStepId(ctx *sql.Context, sqsID WorkflowSavedQueryStepId) (*WorkflowSavedQueryExpectedRowColumnResult, error) {
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

func (d *doltWorkflowManager) getWorkflowSavedQueryStepsByStepId(ctx *sql.Context, stepID WorkflowStepId) (*WorkflowSavedQueryStep, error) {
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

func (d *doltWorkflowManager) listWorkflowStepsByJobId(ctx *sql.Context, jobID WorkflowJobId) ([]*WorkflowStep, error) {
	query := d.selectAllFromWorkflowStepsTableByWorkflowJobIdQuery(string(jobID))
	return d.retrieveWorkflowSteps(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowJobsByWorkflowName(ctx *sql.Context, workflowName string) ([]*WorkflowJob, error) {
	query := d.selectAllFromWorkflowJobsTableByWorkflowNameQuery(string(workflowName))
	return d.retrieveWorkflowJobs(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventTriggerActivitiesByEventTriggerId(ctx *sql.Context, triggerID WorkflowEventTriggerId) ([]*WorkflowEventTriggerActivity, error) {
	query := d.selectAllFromWorkflowEventTriggerActivitiesTableByEventTriggerIdQuery(string(triggerID))
	return d.retrieveWorkflowEventTriggerActivities(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventTriggersByEventId(ctx *sql.Context, eventID WorkflowEventId) ([]*WorkflowEventTrigger, error) {
	query := d.selectAllFromWorkflowEventTriggersTableByWorkflowEventIdQuery(string(eventID))
	return d.retrieveWorkflowEventTriggers(ctx, query)

}

func (d *doltWorkflowManager) listWorkflowEventsByWorkflowName(ctx *sql.Context, workflowName string) ([]*WorkflowEvent, error) {
	query := d.selectAllFromWorkflowEventsTableByWorkflowNameQuery(string(workflowName))
	return d.retrieveWorkflowEvent(ctx, query)
}

func (d *doltWorkflowManager) listWorkflowEventTriggerBranchesByEventTriggerId(ctx *sql.Context, triggerID WorkflowEventTriggerId) ([]*WorkflowEventTriggerBranch, error) {
	query := d.selectAllFromWorkflowEventTriggerBranchesTableByEventTriggerIdQuery(string(triggerID))
	return d.retrieveWorkflowEventTriggerBranches(ctx, query)
}

func (d *doltWorkflowManager) retrieveWorkflowSavedQueryExpectedRowColumnResults(ctx *sql.Context, query string) ([]*WorkflowSavedQueryExpectedRowColumnResult, error) {
	workflowSavedQueryExpectedResults := make([]*WorkflowSavedQueryExpectedRowColumnResult, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
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

func (d *doltWorkflowManager) retrieveWorkflowSavedQuerySteps(ctx *sql.Context, query string) ([]*WorkflowSavedQueryStep, error) {
	workflowSavedQuerySteps := make([]*WorkflowSavedQueryStep, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
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

func (d *doltWorkflowManager) retrieveWorkflowSteps(ctx *sql.Context, query string) ([]*WorkflowStep, error) {
	workflowSteps := make([]*WorkflowStep, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
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

func (d *doltWorkflowManager) retrieveWorkflowJobs(ctx *sql.Context, query string) ([]*WorkflowJob, error) {
	workflowJobs := make([]*WorkflowJob, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
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

func (d *doltWorkflowManager) retrieveWorkflowEventTriggerActivities(ctx *sql.Context, query string) ([]*WorkflowEventTriggerActivity, error) {
	workflowEventTriggerActivities := make([]*WorkflowEventTriggerActivity, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
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

func (d *doltWorkflowManager) retrieveWorkflowEventTriggerBranches(ctx *sql.Context, query string) ([]*WorkflowEventTriggerBranch, error) {
	workflowEventTriggerBranches := make([]*WorkflowEventTriggerBranch, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
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

func (d *doltWorkflowManager) retrieveWorkflowEventTriggers(ctx *sql.Context, query string) ([]*WorkflowEventTrigger, error) {
	workflowEventTriggers := make([]*WorkflowEventTrigger, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
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

func (d *doltWorkflowManager) retrieveWorkflowEvent(ctx *sql.Context, query string) ([]*WorkflowEvent, error) {
	workflowEvents := make([]*WorkflowEvent, 0)

	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
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

func (d *doltWorkflowManager) retrieveWorkflows(ctx *sql.Context, query string) ([]*Workflow, error) {
	workflows := make([]*Workflow, 0)
	cb := func(cbCtx *sql.Context, cvs ColumnValues) error {
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

func (d *doltWorkflowManager) getWorkflow(ctx *sql.Context, workflowName string) (*Workflow, error) {
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

func (d *doltWorkflowManager) storeFromConfig(ctx *sql.Context, config *WorkflowConfig) (*Workflow, error) {
	// todo: run query to see if workflow in table exists
	// if not, create it

	// todo: fetch all events associated with workflow
	/// make state of db match the config

	//statements, err := d.getInsertUpdateStatements(workflow)
	//if err != nil {
	//	return err
	//}
	//
	//for _, statement := range statements {
	//	err = d.sqlWriteQuery(ctx, statement)
	//	if err != nil {
	//		return err
	//	}
	//}

	return nil, nil
}

func (d *doltWorkflowManager) commitWorkflow(ctx *sql.Context, workflow *Workflow) error {
	return d.sqlWriteQuery(ctx, fmt.Sprintf("CALL DOLT_COMMIT('-Am' 'Successfully stored workflow: %s', '--author', '%s <%s>');", string(*workflow.Name), d.commiterName, d.commiterEmail))
}

// TODO: fix all the insert templates!!!

func (d *doltWorkflowManager) sqlWriteQuery(ctx *sql.Context, query string) error {
	_, rowIter, _, err := d.queryFunc(ctx, query)
	if err != nil {
		return err
	}
	_, err = sql.RowIterToRows(ctx, rowIter)
	return err
}

func (d *doltWorkflowManager) StoreAndCommit(ctx *sql.Context, db sqle.Database, config *WorkflowConfig) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	workflow, err := d.storeFromConfig(ctx, config)
	if err != nil {
		return err
	}

	return d.commitWorkflow(ctx, workflow)
}
