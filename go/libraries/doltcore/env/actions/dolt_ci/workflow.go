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
	"time"
)

type WorkflowName string

type Workflow struct {
	Name      *WorkflowName `db:"name"`
	CreatedAt time.Time     `db:"created_at"`
	UpdatedAt time.Time     `db:"updated_at"`
	Events    []*WorkflowEvent
	Jobs      []*WorkflowJob
}

func (w *Workflow) GetEvents() []*WorkflowEvent {
	if w.Events != nil {
		return w.Events
	}
	return make([]*WorkflowEvent, 0)
}

func (w *Workflow) GetJobs() []*WorkflowJob {
	if w.Jobs != nil {
		return w.Jobs
	}
	return make([]*WorkflowJob, 0)
}
