/*
 *  Copyright (c) 2020 Temporal Technologies, Inc. All Rights Reserved
 *
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package io.temporal.samples.mutex;

import static io.temporal.samples.mutex.IncrementalStarter.TASK_QUEUE;

import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.common.RetryOptions;
import java.time.Duration;
import java.util.Optional;

public class MutexActivityImpl implements MutexActivity {
  private final WorkflowClient workflowClient;

  public MutexActivityImpl(WorkflowClient workflowClient) {
    this.workflowClient = workflowClient;
  }

  @Override
  public WorkflowExecution signalWithStartMutexWorkflow(
      String integrationID, Duration unlockTimeout, boolean waitForLockToBeAcquired) {
    ActivityExecutionContext ctx = Activity.getExecutionContext();
    String currentWorkflowID = ctx.getInfo().getWorkflowId();
    String currentRunID = ctx.getInfo().getRunId();
    String namespace = ctx.getInfo().getNamespace();

    String workflowID = String.format("%s:%s:%s", "mutex", namespace, integrationID);

    WorkflowOptions options =
        WorkflowOptions.newBuilder()
            .setTaskQueue(TASK_QUEUE)
            .setWorkflowId(workflowID)
            .setRetryOptions(
                RetryOptions.newBuilder()
                    .setInitialInterval(Duration.ofSeconds(1L))
                    .setBackoffCoefficient(2.0)
                    .setMaximumInterval(Duration.ofMinutes(1L))
                    .setMaximumAttempts(5)
                    .build())
            .build();

    MutexWorkflow mutexWorkflowStub = workflowClient.newWorkflowStub(MutexWorkflow.class, options);

    BatchRequest request = workflowClient.newSignalWithStartRequest();
    request.add(
        mutexWorkflowStub::tryAcquireLock,
        currentWorkflowID,
        currentRunID,
        unlockTimeout,
        waitForLockToBeAcquired);

    return workflowClient.signalWithStart(request);
  }

  @Override
  public boolean isLockAcquired(WorkflowExecution mutexWorkflowExecution) {
    ActivityExecutionContext ctx = Activity.getExecutionContext();
    String currentWorkflowID = ctx.getInfo().getWorkflowId();

    WorkflowStub workflow =
        workflowClient.newUntypedWorkflowStub(mutexWorkflowExecution, Optional.empty());
    return workflow.query("isLockAcquiredByWorkflow", Boolean.class, currentWorkflowID);
  }
}
