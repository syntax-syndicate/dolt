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
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

type WorkflowReader interface {
	GetWorkflowAtRef(ctx context.Context, refName, cSpecStr string) (Workflow, error)
	ListWorkflowsAtRef(ctx context.Context, refName, cSpecStr string) ([]Workflow, error)
}

type doltWorkflowReader struct{}

var _ WorkflowReader = &doltWorkflowReader{}

func NewWorkflowReader() *doltWorkflowReader {
	return &doltWorkflowReader{}
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

func (d *doltWorkflowReader) newWorkflow(ctx context.Context, cvs ColumnValues) (*Workflow, error) {
	wf := &Workflow{
		Events: make([]*WorkflowEvent, 0),
		Jobs:   make([]*WorkflowJob, 0),
	}

	for _, cv := range cvs {
		switch cv.ColumnName {
		case DoltCIWorkflowsCreatedAtColumn:
			t, err := time.Parse(doltCITimeFormat, cv.Value)
			if err != nil {
				return nil, err
			}
			wf.CreatedAt = t
		case DoltCIWorkflowsNameColumn:
			wf.Name = cv.Value
		case DoltCIWorkflowsUpdatedAtColumn:
			continue
		default:
			return nil, errors.New(fmt.Sprintf("unknown dolt_ci workflows column: %s", cv.ColumnName))
		}
	}

	return wf, nil
}
