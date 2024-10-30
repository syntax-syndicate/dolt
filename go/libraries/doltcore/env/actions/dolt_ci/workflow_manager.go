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

import "context"

type WorkflowManager interface {
	GetWorkflow(ctx context.Context, workflowName string) (*WorkflowConfig, error)
	StoreWorkflow(ctx context.Context, workflow *WorkflowConfig) error
}

type doltWorkflowManager struct{}

var _ WorkflowManager = &doltWorkflowManager{}

func NewDoltWorkflowManager() *doltWorkflowManager {
	return &doltWorkflowManager{}
}

func (d doltWorkflowManager) GetWorkflow(ctx context.Context, workflowName string) (*WorkflowConfig, error) {
	//TODO implement me
	panic("implement me")
}

func (d doltWorkflowManager) StoreWorkflow(ctx context.Context, workflow *WorkflowConfig) error {
	//TODO implement me
	panic("implement me")
}
