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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/datas"
)

var ErrWorkflowNameIsNil = errors.New("workflow name is nil")
var ErrReadQueriesNotSupported = errors.New("read queries not supported in this context")
var ErrAlterDDLQueriesNotSupported = errors.New("alter and ddl queries not supported in this context")

type WorkflowWriter interface {
	StoreAndCommit(ctx *sql.Context, db sqle.Database, workflow *Workflow) error
}

type doltWorkflowWriter struct {
	commiterName  string
	commiterEmail string
	queryFunc     QueryFunc
}

var _ WorkflowWriter = &doltWorkflowWriter{}

func NewWorkflowWriter(commiterName, commiterEmail string, queryFunc QueryFunc) *doltWorkflowWriter {
	return &doltWorkflowWriter{
		commiterName:  commiterName,
		commiterEmail: commiterEmail,
		queryFunc:     queryFunc,
	}
}

func (d *doltWorkflowWriter) writeWorkflow(ctx *sql.Context, workflow *Workflow) error {
	statements, err := d.getInsertUpdateStatements(workflow)
	if err != nil {
		return err
	}

	for _, statement := range statements {
		err = d.execWriteQuery(ctx, statement)
		if err != nil {
			return err
		}
	}

	return nil
}

// TODO: fix all the insert templates!!!

func (d *doltWorkflowWriter) sqlWriteQuery(ctx *sql.Context, query string) error {
	_, rowIter, _, err := d.queryFunc(ctx, query)
	if err != nil {
		return err
	}
	_, err = sql.RowIterToRows(ctx, rowIter)
	return err
}

func (d *doltWorkflowWriter) execWriteQuery(ctx *sql.Context, query string) error {
	sqlStatement, err := sqlparser.Parse(query)
	if err == sqlparser.ErrEmpty {
		return fmt.Errorf("Error parsing empty SQL statement")
	} else if err != nil {
		return fmt.Errorf("Error parsing SQL: %v.", err.Error())
	}

	switch sqlStatement.(type) {
	case *sqlparser.Select, *sqlparser.OtherRead, *sqlparser.Show, *sqlparser.SetOp, *sqlparser.Explain:
		return ErrReadQueriesNotSupported
	case *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete:
		return d.sqlWriteQuery(ctx, query)
	case *sqlparser.AlterTable, *sqlparser.DDL:
		return ErrAlterDDLQueriesNotSupported
	default:
		return fmt.Errorf("Unsupported SQL statement: '%v'.", query)
	}
}

func (d *doltWorkflowWriter) getWorkflowInsertUpdates(workflow *Workflow) ([]string, error) {
	if workflow.Name == nil {
		return []string{}, ErrWorkflowNameIsNil
	}
	statements := make([]string, 0)
	insertUpdateStatement := fmt.Sprintf("insert ignore into %s (`%s`, `%s`, `%s`) values ('%s', now(), now());", doltdb.WorkflowsTableName, doltdb.WorkflowsNameColName, doltdb.WorkflowsCreatedAtColName, doltdb.WorkflowsUpdatedAtColName, string(*workflow.Name))
	statements = append(statements, insertUpdateStatement)
	return statements, nil
}

func (d *doltWorkflowWriter) getWorkflowEventTriggerActivitiesInsertUpdates(triggerID WorkflowEventTriggerId, activities []*WorkflowEventTriggerActivity) ([]string, error) {
	statements := make([]string, 0)
	for _, activity := range activities {
		id := string(*activity.Id)
		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `workflow_event_triggers_id_fk`, `activity`) values ('%s', '%s', '%s') on duplicate key update `workflow_event_triggers_id_fk` = '%s', `activity` = '%s';", doltdb.WorkflowEventTriggerActivitiesTableName, id, triggerID, activity.Activity, triggerID, activity.Activity)
		statements = append(statements, insertUpdateStatement)
	}
	return statements, nil
}

func (d *doltWorkflowWriter) getWorkflowEventTriggerBranchesInsertUpdates(triggerID WorkflowEventTriggerId, branches []*WorkflowEventTriggerBranch) ([]string, error) {
	statements := make([]string, 0)
	for _, branch := range branches {
		id := string(*branch.Id)
		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `workflow_event_triggers_id_fk`, `branch`) values ('%s', '%s', '%s') on duplicate key update `workflow_event_triggers_id_fk` = '%s', `branch` = '%s';", doltdb.WorkflowEventTriggerBranchesTableName, id, triggerID, branch.Branch, triggerID, branch.Branch)
		statements = append(statements, insertUpdateStatement)

	}
	return statements, nil
}

func (d *doltWorkflowWriter) getWorkflowEventTriggersInsertUpdates(eventID WorkflowEventId, triggers []*WorkflowEventTrigger) ([]string, error) {
	statements := make([]string, 0)
	for _, trigger := range triggers {
		id := string(*trigger.Id)
		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `workflow_event_id_fk`, `event_trigger_type`) values ('%s', '%s', %d) on duplicate key update `workflow_event_id_fk` = '%s', `event_trigger_type` = %d;", doltdb.WorkflowEventTriggersTableName, id, eventID, trigger.EventTriggerType, eventID, trigger.EventTriggerType)
		statements = append(statements, insertUpdateStatement)

		activityInsertUpdateStatements, err := d.getWorkflowEventTriggerActivitiesInsertUpdates(*trigger.Id, trigger.Activities)
		if err != nil {
			return nil, err
		}
		statements = append(statements, activityInsertUpdateStatements...)

		branchesInsertUpdateStatements, err := d.getWorkflowEventTriggerBranchesInsertUpdates(*trigger.Id, trigger.Branches)
		if err != nil {
			return nil, err
		}
		statements = append(statements, branchesInsertUpdateStatements...)
	}
	return statements, nil
}

func (d *doltWorkflowWriter) getWorkflowEventInsertUpdates(workflowName string, events []*WorkflowEvent) ([]string, error) {
	statements := make([]string, 0)

	for _, event := range events {
		id := string(*event.Id)
		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `workflow_name_fk`, `event_type`) values ('%s', '%s', %d) on duplicate key update `workflow_name_fk` = '%s', `event_type` = %d;", doltdb.WorkflowEventsTableName, id, workflowName, event.EventType, workflowName, event.EventType)
		statements = append(statements, insertUpdateStatement)

		eventTriggerStatements, err := d.getWorkflowEventTriggersInsertUpdates(*event.Id, event.Triggers)
		if err != nil {
			return nil, err
		}

		statements = append(statements, eventTriggerStatements...)
	}
	return statements, nil
}

func (d *doltWorkflowWriter) getWorkflowSavedQueryStepExpectedRowColumnResultInsertUpdates(savedQueryStepID WorkflowSavedQueryStepId, result *WorkflowSavedQueryExpectedRowColumnResult) ([]string, error) {
	statements := make([]string, 0)
	insertUpdateStatement := fmt.Sprintf("insert into %s (`saved_query_step_id_fk`, `expected_row_count_comparison_type`, `expected_column_count_comparison_type`, `expected_row_count`, `expected_column_count`) values ('%s', %d, %d, %d, %d)", doltdb.WorkflowSavedQueryStepExpectedRowColumnResultsTableName, savedQueryStepID, result.ExpectedRowCountComparisonType, result.ExpectedColumnCountComparisonType, result.ExpectedRowCount, result.ExpectedColumnCount)
	statements = append(statements, insertUpdateStatement)
	return statements, nil
}

func (d *doltWorkflowWriter) getWorkflowStepSavedQueryStepInsertUpdates(stepID WorkflowStepId, savedQueryStep *WorkflowSavedQueryStep) ([]string, error) {
	statements := make([]string, 0)
	id := string(*savedQueryStep.Id)
	insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `step_id_fk`, `saved_query_name`, `saved_query_expected_results_type`) values ('%s', '%s', '%s', %d) on duplicate key update `step_id_fk` = '%s', `saved_query_name` = '%s', `saved_query_expected_results_type` = %d;", doltdb.WorkflowSavedQueryStepsTableName, id, stepID, savedQueryStep.SavedQueryName, savedQueryStep.SavedQueryExpectedResultsType, stepID, savedQueryStep.SavedQueryName, savedQueryStep.SavedQueryExpectedResultsType)
	statements = append(statements, insertUpdateStatement)
	if savedQueryStep.ExpectedRowColumnResult != nil {
		resultStatements, err := d.getWorkflowSavedQueryStepExpectedRowColumnResultInsertUpdates(*savedQueryStep.Id, savedQueryStep.ExpectedRowColumnResult)
		if err != nil {
			return nil, err
		}
		statements = append(statements, resultStatements...)
	}
	return statements, nil
}

func (d *doltWorkflowWriter) getWorkflowStepInsertUpdates(jobID WorkflowJobId, steps []*WorkflowStep) ([]string, error) {
	statements := make([]string, 0)
	for _, step := range steps {
		id := string(*step.Id)
		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `name`, `job_id_fk`, `step_order`, `step_type`, `created_at`, `updated_at`) values ('%s', '%s', '%s', %d, %d, now(), now()) on duplicate key update `name` = '%s', `job_id_fk` = '%s', `step_order` = %d, `step_type` = %d;", doltdb.WorkflowStepsTableName, id, step.Name, jobID, step.StepOrder, step.StepType, step.Name, jobID, step.StepOrder, step.StepType)
		statements = append(statements, insertUpdateStatement)
		if step.SavedQueryStep != nil {
			savedQueryStepInsertUpdateStatement, err := d.getWorkflowStepSavedQueryStepInsertUpdates(*step.Id, step.SavedQueryStep)
			if err != nil {
				return nil, err
			}
			statements = append(statements, savedQueryStepInsertUpdateStatement...)
		}
	}
	return statements, nil
}

func (d *doltWorkflowWriter) getWorkflowJobInsertUpdates(workflowName string, jobs []*WorkflowJob) ([]string, error) {
	statements := make([]string, 0)
	for _, job := range jobs {
		id := string(*job.Id)
		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `name`, `workflow_name_fk`, `created_at`, `updated_at`) values ('%s', '%s', '%s', now(), now()) on duplicate key update `name` = '%s', `workflow_name_fk` = '%s', `updated_at` = now();", doltdb.WorkflowJobsTableName, id, job.Name, workflowName, job.Name, workflowName)
		statements = append(statements, insertUpdateStatement)

		stepInsertUpdateStatements, err := d.getWorkflowStepInsertUpdates(*job.Id, job.Steps)
		if err != nil {
			return nil, err
		}
		statements = append(statements, stepInsertUpdateStatements...)
	}
	return statements, nil
}

func (d *doltWorkflowWriter) getInsertUpdateStatements(workflow *Workflow) ([]string, error) {
	if workflow.Name == nil {
		return nil, ErrWorkflowNameIsNil
	}
	statements := make([]string, 0)
	workflowInsertUpdates, err := d.getWorkflowInsertUpdates(workflow)
	if err != nil {
		return nil, err
	}
	statements = append(statements, workflowInsertUpdates...)

	eventInsertUpdates, err := d.getWorkflowEventInsertUpdates(string(*workflow.Name), workflow.Events)
	if err != nil {
		return nil, err
	}
	statements = append(statements, eventInsertUpdates...)

	jobInsertUpdates, err := d.getWorkflowJobInsertUpdates(string(*workflow.Name), workflow.Jobs)
	if err != nil {
		return nil, err
	}
	statements = append(statements, jobInsertUpdates...)
	return statements, nil
}

func (d *doltWorkflowWriter) StoreAndCommit(ctx *sql.Context, db sqle.Database, workflow *Workflow) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	err := d.writeWorkflow(ctx, workflow)
	if err != nil {
		return err
	}

	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	ddb, exists := dSess.GetDoltDB(ctx, dbName)
	if !exists {
		return fmt.Errorf("database not found in database %s", dbName)
	}

	roots, ok := dSess.GetRoots(ctx, dbName)
	if !ok {
		return fmt.Errorf("roots not found in database %s", dbName)
	}

	roots, err = actions.StageTables(ctx, roots, ExpectedDoltCITablesOrdered, true)
	if err != nil {
		return err
	}

	ws, err := dSess.WorkingSet(ctx, dbName)
	if err != nil {
		return err
	}

	ws = ws.WithWorkingRoot(roots.Working)
	ws = ws.WithStagedRoot(roots.Staged)

	wsHash, err := ws.HashOf()
	if err != nil {
		return err
	}

	wRef := ws.Ref()
	pRef, err := wRef.ToHeadRef()
	if err != nil {
		return err
	}

	parent, err := ddb.ResolveCommitRef(ctx, pRef)
	if err != nil {
		return err
	}

	parents := []*doltdb.Commit{parent}

	meta, err := datas.NewCommitMeta(d.commiterName, d.commiterEmail, fmt.Sprintf("Successfully stored Dolt CI Workflow: %s", string(*workflow.Name)))
	if err != nil {
		return err
	}

	pcm, err := ddb.NewPendingCommit(ctx, roots, parents, meta)
	if err != nil {
		return err
	}

	wsMeta := &datas.WorkingSetMeta{
		Name:      d.commiterName,
		Email:     d.commiterEmail,
		Timestamp: uint64(time.Now().Unix()),
	}
	_, err = ddb.CommitWithWorkingSet(ctx, pRef, wRef, pcm, ws, wsHash, wsMeta, nil)
	return err
}
