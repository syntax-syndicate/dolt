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

//func Merge(config *WorkflowConfig, workflow *Workflow) (*Workflow, error) {
//	if workflow == nil {
//		return nil, errors.New("workflow cannot be nil")
//	}
//	if config == nil {
//		return nil, errors.New("config cannot be nil")
//	}
//
//	updated := workflow
//
//	if updated.Name == nil {
//		name := WorkflowName(config.Name)
//		workflow.Name = &name
//	}
//
//	// handle events
//	events, err := mergeWorkflowEvents(config.On, workflow.Events)
//	if err != nil {
//		return nil, err
//	}
//
//	updated.Events = events
//
//	// handle jobs
//	jobs, err := mergeWorkflowJobs(config.Jobs, workflow.Jobs)
//	if err != nil {
//		return nil, err
//	}
//
//	updated.Jobs = jobs
//
//	return updated, nil
//}
//
//func mergeWorkflowEventsPush(workflowName *WorkflowName, push *Push, events []*WorkflowEvent) ([]*WorkflowEvent, error) {
//	updated := make([]*WorkflowEvent, 0)
//
//	// handle no push events
//	// remove any existing
//	if push == nil {
//		return updated, nil
//	}
//
//	// handle only push events no branches
//	// remove any existing pushes with branches
//	if len(push.Branches) == 0 {
//		// if there are not existing events
//		// create the correct ones
//
//		foundMatch := false
//
//		// search for matching event first
//		for _, event := range events {
//			if event.EventType == WorkflowEventTypePush {
//				triggers := event.GetTriggers()
//				if len(triggers) == 0 {
//
//					// if found add to updated
//					foundMatch = true
//					updated = append(updated, event)
//				}
//			}
//		}
//
//		if !foundMatch {
//			// create event here
//			eventId := WorkflowEventId(uuid.NewString())
//			triggerId := WorkflowEventTriggerId(uuid.NewString())
//
//			triggers := make([]*WorkflowEventTrigger, 0)
//			triggers = append(triggers, &WorkflowEventTrigger{
//				Id:                &triggerId,
//				WorkflowEventIdFK: &eventId,
//				EventTriggerType:  WorkflowEventTriggerTypeUnspecified,
//			})
//
//			updated = append(updated, &WorkflowEvent{
//				Id:             &eventId,
//				WorkflowNameFK: workflowName,
//				Triggers:       triggers,
//			})
//		}
//
//		return updated, nil
//	}
//
//	// handle push events with branches
//	if len(push.Branches) > 0 {
//
//		foundMatch := false
//
//		// search existing events
//		for _, event := range events {
//			if event.EventType == WorkflowEventTypePush {
//				triggers := event.GetTriggers()
//				for _, trigger := range triggers {
//					if trigger.EventTriggerType == WorkflowEventTriggerTypeBranches {
//						existing := make(map[string]struct{})
//						for _, branch := range trigger.Branches {
//							existing[branch.Branch] = struct{}{}
//						}
//
//						for _, branch := range push.Branches {
//
//						}
//					}
//				}
//			}
//		}
//	}
//
//	return updated, nil
//}
//
//func mergeWorkflowEvents(on On, events []*WorkflowEvent) ([]*WorkflowEvent, error) {
//	updated := make([]*WorkflowEvent, 0)
//
//	pushEvents := make([]*WorkflowEvent, 0)
//	pullRequestEvents := make([]*WorkflowEvent, 0)
//	workflowDispatchEvents := make([]*WorkflowEvent, 0)
//	for _, event := range events {
//		if event.EventType == WorkflowEventTypePush {
//			pushEvents = append(pushEvents, event)
//		} else if event.EventType == WorkflowEventTypePullRequest {
//			pullRequestEvents = append(pullRequestEvents, event)
//		} else if event.EventType == WorkflowEventTypeWorkflowDispatch {
//			workflowDispatchEvents = append(workflowDispatchEvents, event)
//		}
//	}
//
//	pushEvents, err := mergeWorkflowEventsPush(on.Push, pushEvents)
//	if err != nil {
//		return nil, err
//	}
//	updated = append(updated, pushEvents...)
//
//	return updated, nil
//}
