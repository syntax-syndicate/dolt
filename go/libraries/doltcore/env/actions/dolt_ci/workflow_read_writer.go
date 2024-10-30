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

type WorkflowWriter interface {
	StoreWorkflow(ctx context.Context, workflow Workflow) error
}

type WorkflowReadWriter interface {
	WorkflowReader
	WorkflowWriter
}

type doltWorkflowReadWriter struct{}

var _ WorkflowReadWriter = &doltWorkflowReadWriter{}

func NewDoltWorkflowReadWriter() *doltWorkflowReadWriter {
	return &doltWorkflowReadWriter{}
}

func (d doltWorkflowReadWriter) GetWorkflow(ctx context.Context, workflowName string) (Workflow, error) {
	//TODO implement me
	panic("implement me")
}

func (d doltWorkflowReadWriter) StoreWorkflow(ctx context.Context, workflow Workflow) error {
	//TODO implement me
	panic("implement me")
}
