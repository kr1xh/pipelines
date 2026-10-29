// Copyright 2026 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package component

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	apiv2 "github.com/kubeflow/pipelines/backend/api/v2/go_client"
	apiclient "github.com/kubeflow/pipelines/backend/src/v2/apiclient"
	"github.com/kubeflow/pipelines/backend/src/v2/apiclient/kfpapi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type orderingMockAPI struct {
	*kfpapi.MockAPI
	order []string
}

type flakyArtifactTaskMockAPI struct {
	*kfpapi.MockAPI
	createArtifactsBulkCalls int
	createArtifactTaskCalls  int
	updateTasksBulkCalls     int
}

type blockingArtifactService struct {
	apiv2.UnimplementedArtifactServiceServer

	callStarted     chan struct{}
	contextCanceled chan struct{}
}

func (s *blockingArtifactService) CreateArtifactsBulk(
	ctx context.Context,
	_ *apiv2.CreateArtifactsBulkRequest,
) (*apiv2.CreateArtifactsBulkResponse, error) {
	close(s.callStarted)
	<-ctx.Done()
	close(s.contextCanceled)

	return nil, status.FromContextError(ctx.Err()).Err()
}

type deadlineArtifactService struct {
	apiv2.UnimplementedArtifactServiceServer

	callStarted     chan struct{}
	contextCanceled chan struct{}
}

func (s *deadlineArtifactService) CreateArtifactsBulk(
	ctx context.Context,
	_ *apiv2.CreateArtifactsBulkRequest,
) (*apiv2.CreateArtifactsBulkResponse, error) {
	close(s.callStarted)

	<-ctx.Done()
	close(s.contextCanceled)

	return nil, status.FromContextError(ctx.Err()).Err()
}

type shutdownRunService struct {
	apiv2.UnimplementedRunServiceServer

	callStarted chan struct{}
}

func (s *shutdownRunService) GetRun(
	ctx context.Context,
	_ *apiv2.GetRunRequest,
) (*apiv2.Run, error) {
	close(s.callStarted)
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func (m *orderingMockAPI) CreateArtifactsBulk(ctx context.Context, req *apiv2.CreateArtifactsBulkRequest) (*apiv2.CreateArtifactsBulkResponse, error) {
	m.order = append(m.order, "artifacts")
	return m.MockAPI.CreateArtifactsBulk(ctx, req)
}

func (m *orderingMockAPI) CreateArtifactTasks(ctx context.Context, req *apiv2.CreateArtifactTasksBulkRequest) (*apiv2.CreateArtifactTasksBulkResponse, error) {
	m.order = append(m.order, "artifact-tasks")
	return m.MockAPI.CreateArtifactTasks(ctx, req)
}

func (m *orderingMockAPI) UpdateTasksBulk(ctx context.Context, req *apiv2.UpdateTasksBulkRequest) (*apiv2.UpdateTasksBulkResponse, error) {
	m.order = append(m.order, "tasks")
	return m.MockAPI.UpdateTasksBulk(ctx, req)
}

func (m *flakyArtifactTaskMockAPI) CreateArtifactsBulk(ctx context.Context, req *apiv2.CreateArtifactsBulkRequest) (*apiv2.CreateArtifactsBulkResponse, error) {
	m.createArtifactsBulkCalls++
	return m.MockAPI.CreateArtifactsBulk(ctx, req)
}

func (m *flakyArtifactTaskMockAPI) CreateArtifactTasks(ctx context.Context, req *apiv2.CreateArtifactTasksBulkRequest) (*apiv2.CreateArtifactTasksBulkResponse, error) {
	m.createArtifactTaskCalls++
	if m.createArtifactTaskCalls == 1 {
		return nil, fmt.Errorf("transient artifact-task failure")
	}
	return m.MockAPI.CreateArtifactTasks(ctx, req)
}

func (m *flakyArtifactTaskMockAPI) UpdateTasksBulk(ctx context.Context, req *apiv2.UpdateTasksBulkRequest) (*apiv2.UpdateTasksBulkResponse, error) {
	m.updateTasksBulkCalls++
	return m.MockAPI.UpdateTasksBulk(ctx, req)
}

func TestBatchUpdater_FlushDeduplicatesArtifactTasks(t *testing.T) {
	mockAPI := kfpapi.NewMockAPI()
	updater := NewBatchUpdater()

	artifactTask := &apiv2.ArtifactTask{
		ArtifactId: "artifact-1",
		TaskId:     "task-1",
		RunId:      "run-1",
		Key:        "model",
		Type:       apiv2.IOType_OUTPUT,
	}

	updater.QueueArtifactTask(artifactTask)
	updater.QueueArtifactTask(&apiv2.ArtifactTask{
		ArtifactId: "artifact-1",
		TaskId:     "task-1",
		RunId:      "run-1",
		Key:        "model",
		Type:       apiv2.IOType_OUTPUT,
	})

	require.NoError(t, updater.Flush(context.Background(), mockAPI))

	resp, err := mockAPI.ListArtifactTasks(context.Background(), &apiv2.ListArtifactTasksRequest{})
	require.NoError(t, err)
	require.Len(t, resp.ArtifactTasks, 1)
}

func TestBatchUpdater_FlushKeepsDistinctArtifactTasksForDifferentKeys(t *testing.T) {
	mockAPI := kfpapi.NewMockAPI()
	updater := NewBatchUpdater()

	updater.QueueArtifactTask(&apiv2.ArtifactTask{
		ArtifactId: "artifact-1",
		TaskId:     "task-1",
		RunId:      "run-1",
		Key:        "input-a",
		Type:       apiv2.IOType_COMPONENT_INPUT,
	})
	updater.QueueArtifactTask(&apiv2.ArtifactTask{
		ArtifactId: "artifact-1",
		TaskId:     "task-1",
		RunId:      "run-1",
		Key:        "input-b",
		Type:       apiv2.IOType_COMPONENT_INPUT,
	})

	require.NoError(t, updater.Flush(context.Background(), mockAPI))

	resp, err := mockAPI.ListArtifactTasks(context.Background(), &apiv2.ListArtifactTasksRequest{})
	require.NoError(t, err)
	require.Len(t, resp.ArtifactTasks, 2)
}

func TestBatchUpdater_FlushKeepsDistinctArtifactTasksForDifferentProducerIterations(t *testing.T) {
	mockAPI := kfpapi.NewMockAPI()
	updater := NewBatchUpdater()
	iteration0 := int64(0)
	iteration1 := int64(1)

	updater.QueueArtifactTask(&apiv2.ArtifactTask{
		ArtifactId: "artifact-1",
		TaskId:     "task-1",
		RunId:      "run-1",
		Key:        "model",
		Type:       apiv2.IOType_ITERATOR_OUTPUT,
		Producer:   &apiv2.IOProducer{TaskName: "loop-body", Iteration: &iteration0},
	})
	updater.QueueArtifactTask(&apiv2.ArtifactTask{
		ArtifactId: "artifact-1",
		TaskId:     "task-1",
		RunId:      "run-1",
		Key:        "model",
		Type:       apiv2.IOType_ITERATOR_OUTPUT,
		Producer:   &apiv2.IOProducer{TaskName: "loop-body", Iteration: &iteration1},
	})

	require.NoError(t, updater.Flush(context.Background(), mockAPI))

	resp, err := mockAPI.ListArtifactTasks(context.Background(), &apiv2.ListArtifactTasksRequest{})
	require.NoError(t, err)
	require.Len(t, resp.ArtifactTasks, 2)
}

func TestBatchUpdater_QueueTaskUpdateDoesNotSelfDuplicateOutputs(t *testing.T) {
	updater := NewBatchUpdater()
	task := &apiv2.PipelineTask{
		TaskId: "task-1",
		Outputs: &apiv2.PipelineTask_InputOutputs{
			Parameters: []*apiv2.PipelineTask_InputOutputs_IOParameter{
				{ParameterKey: "result"},
			},
			Artifacts: []*apiv2.PipelineTask_InputOutputs_IOArtifact{
				{ArtifactKey: "model"},
			},
		},
	}

	updater.QueueTaskUpdate(task)
	updater.QueueTaskUpdate(task)

	require.Len(t, updater.taskUpdates["task-1"].GetOutputs().GetParameters(), 1)
	require.Len(t, updater.taskUpdates["task-1"].GetOutputs().GetArtifacts(), 1)
}

func TestBatchUpdater_FlushCreatesArtifactLinksBeforeTerminalTaskUpdate(t *testing.T) {
	mockAPI := &orderingMockAPI{MockAPI: kfpapi.NewMockAPI()}
	updater := NewBatchUpdater()
	_, err := mockAPI.CreateTask(context.Background(), &apiv2.CreateTaskRequest{
		RunId: "run-1",
		Task: &apiv2.PipelineTask{
			TaskId: "task-1",
			RunId:  "run-1",
			Name:   "task-1",
			State:  apiv2.PipelineTask_RUNNING,
		},
	})
	require.NoError(t, err)

	updater.QueueArtifact(&apiv2.CreateArtifactRequest{
		RunId:       "run-1",
		TaskId:      "task-1",
		ProducerKey: "model",
		Artifact: &apiv2.Artifact{
			Name: "model",
		},
	})
	updater.QueueArtifactTask(&apiv2.ArtifactTask{
		TaskId: "task-1",
		RunId:  "run-1",
		Key:    "model",
		Type:   apiv2.IOType_OUTPUT,
	})
	updater.QueueTaskUpdate(&apiv2.PipelineTask{
		TaskId: "task-1",
		RunId:  "run-1",
		State:  apiv2.PipelineTask_SUCCEEDED,
	})

	require.NoError(t, updater.Flush(context.Background(), mockAPI))
	require.Equal(t, []string{"artifacts", "artifact-tasks", "tasks"}, mockAPI.order)
}

func TestBatchUpdater_FlushRetriesOnlyFailedTail(t *testing.T) {
	mockAPI := &flakyArtifactTaskMockAPI{MockAPI: kfpapi.NewMockAPI()}
	updater := NewBatchUpdater()
	_, err := mockAPI.CreateTask(context.Background(), &apiv2.CreateTaskRequest{
		RunId: "run-1",
		Task: &apiv2.PipelineTask{
			TaskId: "task-parent",
			RunId:  "run-1",
			Name:   "task-parent",
			State:  apiv2.PipelineTask_RUNNING,
		},
	})
	require.NoError(t, err)

	updater.QueueArtifact(&apiv2.CreateArtifactRequest{
		RunId:       "run-1",
		TaskId:      "task-output",
		ProducerKey: "model",
		Artifact: &apiv2.Artifact{
			Name: "model",
		},
	})
	updater.QueueArtifactTask(&apiv2.ArtifactTask{
		TaskId: "task-parent",
		RunId:  "run-1",
		Key:    "model",
		Type:   apiv2.IOType_OUTPUT,
	})
	updater.QueueTaskUpdate(&apiv2.PipelineTask{
		TaskId: "task-parent",
		RunId:  "run-1",
		State:  apiv2.PipelineTask_SUCCEEDED,
	})

	err = updater.Flush(context.Background(), mockAPI)
	require.Error(t, err)
	require.Equal(t, 1, mockAPI.createArtifactsBulkCalls)
	require.Equal(t, 1, mockAPI.createArtifactTaskCalls)
	require.Equal(t, 0, mockAPI.updateTasksBulkCalls)
	require.Empty(t, updater.artifacts)
	require.Len(t, updater.artifactTasks, 1)
	require.Len(t, updater.taskUpdates, 1)

	require.NoError(t, updater.Flush(context.Background(), mockAPI))
	require.Equal(t, 1, mockAPI.createArtifactsBulkCalls)
	require.Equal(t, 2, mockAPI.createArtifactTaskCalls)
	require.Equal(t, 1, mockAPI.updateTasksBulkCalls)
	require.Empty(t, updater.artifacts)
	require.Empty(t, updater.artifactTasks)
	require.Empty(t, updater.taskUpdates)
}

func TestBatchUpdater_QueueTaskUpdateClonesAndMergesStatusMetadata(t *testing.T) {
	updater := NewBatchUpdater()
	originalTask := &apiv2.PipelineTask{
		TaskId: "task-1",
		RunId:  "run-1",
		State:  apiv2.PipelineTask_RUNNING,
	}

	updater.QueueTaskUpdate(originalTask)
	originalTask.State = apiv2.PipelineTask_FAILED
	originalTask.StatusMetadata = &apiv2.PipelineTask_StatusMetadata{Message: "mutated-after-queue"}

	updater.QueueTaskUpdate(&apiv2.PipelineTask{
		TaskId: "task-1",
		RunId:  "run-1",
		State:  apiv2.PipelineTask_FAILED,
		StatusMetadata: &apiv2.PipelineTask_StatusMetadata{
			Message: "final failure",
		},
	})

	queuedTask := updater.taskUpdates["task-1"]
	require.NotNil(t, queuedTask)
	require.Equal(t, apiv2.PipelineTask_FAILED, queuedTask.GetState())
	require.NotNil(t, queuedTask.GetStatusMetadata())
	require.Equal(t, "final failure", queuedTask.GetStatusMetadata().GetMessage())
}

func TestBatchUpdater_GetMetricsAccumulatesAcrossFlushes(t *testing.T) {
	mockAPI := kfpapi.NewMockAPI()
	updater := NewBatchUpdater()
	_, err := mockAPI.CreateTask(context.Background(), &apiv2.CreateTaskRequest{
		RunId: "run-1",
		Task: &apiv2.PipelineTask{
			TaskId: "task-1",
			RunId:  "run-1",
			Name:   "task-1",
			State:  apiv2.PipelineTask_RUNNING,
		},
	})
	require.NoError(t, err)

	updater.QueueTaskUpdate(&apiv2.PipelineTask{
		TaskId: "task-1",
		RunId:  "run-1",
		State:  apiv2.PipelineTask_SUCCEEDED,
	})
	require.NoError(t, updater.Flush(context.Background(), mockAPI))

	updater.QueueTaskUpdate(&apiv2.PipelineTask{
		TaskId: "task-1",
		RunId:  "run-1",
		State:  apiv2.PipelineTask_FAILED,
	})
	require.NoError(t, updater.Flush(context.Background(), mockAPI))

	metrics := updater.GetMetrics()
	require.Equal(t, 2, metrics["actual_task_update_calls"])
}

func TestBatchUpdater_FlushStopsOnCallerCancellation(t *testing.T) {
	grpcServer := grpc.NewServer()
	artifactService := &blockingArtifactService{
		callStarted:     make(chan struct{}),
		contextCanceled: make(chan struct{}),
	}

	apiv2.RegisterArtifactServiceServer(grpcServer, artifactService)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	client, err := apiclient.New(
		&apiclient.Config{Endpoint: listener.Addr().String()},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	api := kfpapi.New(client)
	updater := NewBatchUpdater()

	updater.QueueArtifact(&apiv2.CreateArtifactRequest{
		RunId:       "run-1",
		TaskId:      "task-1",
		ProducerKey: "output",
		Artifact:    &apiv2.Artifact{Name: "artifact-1"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- updater.Flush(ctx, api)
	}()

	<-artifactService.callStarted
	cancel()

	<-artifactService.contextCanceled
	err = <-flushDone

	require.Equal(t, codes.Canceled, status.Code(err))
	require.Len(t, updater.artifacts, 1)
}

func TestBatchUpdater_FlushTerminatesWhenNativeRPCDeadlineElapses(t *testing.T) {
	grpcServer := grpc.NewServer()
	artifactService := &blockingArtifactService{
		callStarted:     make(chan struct{}),
		contextCanceled: make(chan struct{}),
	}

	apiv2.RegisterArtifactServiceServer(grpcServer, artifactService)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	client, err := apiclient.New(
		&apiclient.Config{Endpoint: listener.Addr().String()},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	api := kfpapi.New(client)
	updater := NewBatchUpdater()

	updater.QueueArtifact(&apiv2.CreateArtifactRequest{
		RunId:       "run-1",
		TaskId:      "task-1",
		ProducerKey: "output",
		Artifact:    &apiv2.Artifact{Name: "artifact-1"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	flushDone := make(chan error, 1)
	go func() {
		flushDone <- updater.Flush(ctx, api)
	}()

	<-artifactService.callStarted
	err = <-flushDone

	require.Equal(t, codes.DeadlineExceeded, status.Code(err))

	<-artifactService.contextCanceled

	require.Len(t, updater.artifacts, 1)
	require.Empty(t, updater.artifactTasks)
	require.Empty(t, updater.taskUpdates)
}

func TestBatchUpdater_GetRunFailsWhenNativeAPIServerStopsDuringRead(t *testing.T) {
	grpcServer := grpc.NewServer()
	runService := &shutdownRunService{
		callStarted: make(chan struct{}),
	}
	apiv2.RegisterRunServiceServer(grpcServer, runService)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		_ = grpcServer.Serve(listener)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	client, err := apiclient.New(
		&apiclient.Config{Endpoint: listener.Addr().String()},
		nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
	})

	api := kfpapi.New(client)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	readDone := make(chan error, 1)
	go func() {
		_, err := api.GetRun(ctx, &apiv2.GetRunRequest{
			RunId: "run-1",
		})
		readDone <- err
	}()

	select {
	case <-runService.callStarted:
	case <-ctx.Done():
		t.Fatal("GetRun request did not reach the server")
	}

	grpcServer.Stop()

	select {
	case err := <-readDone:
		require.Error(t, err)
		require.Contains(t, err.Error(), "Unavailable")
	case <-ctx.Done():
		t.Fatal("GetRun did not fail after the server stopped")
	}
}
