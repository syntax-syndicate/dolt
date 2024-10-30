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
	"gopkg.in/yaml.v3"
	"io"
)

var ErrWorkflowNameNotDefined = errors.New("workflow name not defined")

type Step struct {
	Name            string `yaml:"name"`
	SavedQueryName  string `yaml:"saved_query_name"`
	ExpectedColumns string `yaml:"expected_columns"`
	ExpectedRows    string `yaml:"expected_rows"`
}

type Job struct {
	Name  string `yaml:"name"`
	Steps []Step `yaml:"steps"`
}

type Push struct {
	Branches []string `yaml:"branches"`
}

type PullRequest struct {
	Branches   []string `yaml:"branches"`
	Activities []string `yaml:"activities"`
}

type WorkflowDispatch struct{}

type On struct {
	Push             Push             `yaml:"push"`
	PullRequest      PullRequest      `yaml:"pull_request"`
	WorkflowDispatch WorkflowDispatch `yaml:"workflow_dispatch"`
}

type Workflow struct {
	Name string `yaml:"name"`
	On   On     `yaml:"on"`
	Jobs []Job  `yaml:"jobs"`
}

//func (w *Workflow) getWorkflowInsertUpdates() ([]string, error) {
//	statements := make([]string, 0)
//	if w.Name == "" {
//		return nil, ErrWorkflowNameNotDefined
//	}
//	insertUpdateStatement := fmt.Sprintf("insert ignore into %s (`name`, `created_at`, `updated_at`) values ('%s', now(), now());", doltdb.WorkflowsTableName, w.Name)
//	statements = append(statements, insertUpdateStatement)
//	return statements, nil
//}
//
//func (w *Workflow) getWorkflowEventTriggerActivitiesInsertUpdates(triggerID WorkflowEventTriggerId, activities []*WorkflowEventTriggerActivity) ([]string, error) {
//	statements := make([]string, 0)
//	for _, activity := range activities {
//		id := string(*activity.Id)
//		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `workflow_event_triggers_id_fk`, `activity`) values ('%s', '%s', '%s') on duplicate key update `workflow_event_triggers_id_fk` = '%s', `activity` = '%s';", DoltCIWorkflowEventTriggerActivitiesTableName, id, triggerID, activity.Activity, triggerID, activity.Activity)
//		statements = append(statements, insertUpdateStatement)
//	}
//	return statements, nil
//}
//
//func (w *Workflow) getWorkflowEventTriggerBranchesInsertUpdates(triggerID WorkflowEventTriggerId, branches []*WorkflowEventTriggerBranch) ([]string, error) {
//	statements := make([]string, 0)
//	for _, branch := range branches {
//		id := string(*branch.Id)
//		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `workflow_event_triggers_id_fk`, `branch`) values ('%s', '%s', '%s') on duplicate key update `workflow_event_triggers_id_fk` = '%s', `branch` = '%s';", DoltCIWorkflowEventTriggerBranchesTableName, id, triggerID, branch.Branch, triggerID, branch.Branch)
//		statements = append(statements, insertUpdateStatement)
//
//	}
//	return statements, nil
//}
//
//func (w *Workflow) getWorkflowEventTriggersInsertUpdates(eventID WorkflowEventId, triggers []*WorkflowEventTrigger) ([]string, error) {
//	statements := make([]string, 0)
//	for _, trigger := range triggers {
//		id := string(*trigger.Id)
//		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `workflow_event_id_fk`, `event_trigger_type`) values ('%s', '%s', %d) on duplicate key update `workflow_event_id_fk` = '%s', `event_trigger_type` = %d;", DoltCIWorkflowEventTriggersTableName, id, eventID, trigger.EventTriggerType, eventID, trigger.EventTriggerType)
//		statements = append(statements, insertUpdateStatement)
//
//		activityInsertUpdateStatements, err := w.getWorkflowEventTriggerActivitiesInsertUpdates(*trigger.Id, trigger.Activities)
//		if err != nil {
//			return nil, err
//		}
//		statements = append(statements, activityInsertUpdateStatements...)
//
//		branchesInsertUpdateStatements, err := w.getWorkflowEventTriggerBranchesInsertUpdates(*trigger.Id, trigger.Branches)
//		if err != nil {
//			return nil, err
//		}
//		statements = append(statements, branchesInsertUpdateStatements...)
//	}
//	return statements, nil
//}
//
//func (w *Workflow) toEventTypes() []Event {
//	events := make([]Event, 0)
//	return events
//}
//
//func (w *Workflow) getWorkflowEventInsertUpdates() ([]string, error) {
//	statements := make([]string, 0)
//
//	for _, event := range events {
//		id := string(*event.Id)
//		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `workflow_name_fk`, `event_type`) values ('%s', '%s', %d) on duplicate key update `workflow_name_fk` = '%s', `event_type` = %d;", DoltCIWorkflowEventsTableName, id, workflowName, event.EventType, workflowName, event.EventType)
//		statements = append(statements, insertUpdateStatement)
//
//		eventTriggerStatements, err := w.getWorkflowEventTriggersInsertUpdates(*event.Id, event.Triggers)
//		if err != nil {
//			return nil, err
//		}
//
//		statements = append(statements, eventTriggerStatements...)
//	}
//	return statements, nil
//}
//
//func (w *Workflow) getWorkflowSavedQueryStepExpectedRowColumnResultInsertUpdates(savedQueryStepID WorkflowSavedQueryStepId, result *WorkflowSavedQueryExpectedRowColumnResult) ([]string, error) {
//	statements := make([]string, 0)
//	insertUpdateStatement := fmt.Sprintf("insert into %s (`saved_query_step_id_fk`, `expected_row_count_comparison_type`, `expected_column_count_comparison_type`, `expected_row_count`, `expected_column_count`) values ('%s', %d, %d, %d, %d)", DoltCISavedQueryStepExpectedRowColumnResultsTableName, savedQueryStepID, result.ExpectedRowCountComparisonType, result.ExpectedColumnCountComparisonType, result.ExpectedRowCount, result.ExpectedColumnCount)
//	statements = append(statements, insertUpdateStatement)
//	return statements, nil
//}
//
//func (w *Workflow) getWorkflowStepSavedQueryStepInsertUpdates(stepID WorkflowStepId, savedQueryStep *WorkflowSavedQueryStep) ([]string, error) {
//	statements := make([]string, 0)
//	id := string(*savedQueryStep.Id)
//	insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `step_id_fk`, `saved_query_name`, `saved_query_expected_results_type`) values ('%s', '%s', '%s', %d) on duplicate key update `step_id_fk` = '%s', `saved_query_name` = '%s', `saved_query_expected_results_type` = %d;", DoltCISavedQueryStepsTableName, id, stepID, savedQueryStep.SavedQueryName, savedQueryStep.SavedQueryExpectedResultsType, stepID, savedQueryStep.SavedQueryName, savedQueryStep.SavedQueryExpectedResultsType)
//	statements = append(statements, insertUpdateStatement)
//	if savedQueryStep.ExpectedRowColumnResult != nil {
//		resultStatements, err := w.getWorkflowSavedQueryStepExpectedRowColumnResultInsertUpdates(*savedQueryStep.Id, savedQueryStep.ExpectedRowColumnResult)
//		if err != nil {
//			return nil, err
//		}
//		statements = append(statements, resultStatements...)
//	}
//	return statements, nil
//}
//
//func (w *Workflow) getWorkflowStepInsertUpdates(jobID WorkflowJobId, steps []*WorkflowStep) ([]string, error) {
//	statements := make([]string, 0)
//	for _, step := range steps {
//		id := string(*step.Id)
//		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `name`, `job_id_fk`, `step_order`, `step_type`, `created_at`, `updated_at`) values ('%s', '%s', '%s', %d, %d, now(), now()) on duplicate key update `name` = '%s', `job_id_fk` = '%s', `step_order` = %d, `step_type` = %d;", DoltCIStepsTableName, id, step.Name, jobID, step.StepOrder, step.StepType, step.Name, jobID, step.StepOrder, step.StepType)
//		statements = append(statements, insertUpdateStatement)
//		if step.SavedQueryStep != nil {
//			savedQueryStepInsertUpdateStatement, err := w.getWorkflowStepSavedQueryStepInsertUpdates(*step.Id, step.SavedQueryStep)
//			if err != nil {
//				return nil, err
//			}
//			statements = append(statements, savedQueryStepInsertUpdateStatement...)
//		}
//	}
//	return statements, nil
//}
//
//func (w *Workflow) getWorkflowJobInsertUpdates(workflowName string, jobs []*WorkflowJob) ([]string, error) {
//	statements := make([]string, 0)
//	for _, job := range jobs {
//		id := string(*job.Id)
//		insertUpdateStatement := fmt.Sprintf("insert into %s (`id`, `name`, `workflow_name_fk`, `created_at`, `updated_at`) values ('%s', '%s', '%s', now(), now()) on duplicate key update `name` = '%s', `workflow_name_fk` = '%s', `updated_at` = now();", DoltCIJobsTableName, id, job.Name, workflowName, job.Name, workflowName)
//		statements = append(statements, insertUpdateStatement)
//
//		stepInsertUpdateStatements, err := w.getWorkflowStepInsertUpdates(*job.Id, job.Steps)
//		if err != nil {
//			return nil, err
//		}
//		statements = append(statements, stepInsertUpdateStatements...)
//	}
//	return statements, nil
//}
//
//func (w *Workflow) AsSqlStatements() ([]string, error) {
//	statements := make([]string, 0)
//	workflowInsertUpdates, err := w.getWorkflowInsertUpdates()
//	if err != nil {
//		return nil, err
//	}
//	statements = append(statements, workflowInsertUpdates...)
//
//	eventInsertUpdates, err := w.getWorkflowEventInsertUpdates()
//	if err != nil {
//		return nil, err
//	}
//	statements = append(statements, eventInsertUpdates...)
//
//	jobInsertUpdates, err := w.getWorkflowJobInsertUpdates(workflow.Name, workflow.Jobs)
//	if err != nil {
//		return nil, err
//	}
//	statements = append(statements, jobInsertUpdates...)
//	return statements, nil
//}

func ParseWorkflow(r io.Reader) (workflow *Workflow, err error) {
	workflow = &Workflow{}

	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)

	err = decoder.Decode(workflow)
	return
}
