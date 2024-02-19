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

import io.temporal.activity.ActivityOptions;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.ExternalWorkflowStub;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import org.slf4j.Logger;

public class TNLWorkflowImpl implements TNLWorkflow {
  private final Logger logger = Workflow.getLogger(TNLWorkflowImpl.class);
  private boolean isReadyForExecution = false;
  private final MutexActivity activities =
      Workflow.newActivityStub(
          MutexActivity.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofMinutes(1))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofSeconds(1L))
                      .setBackoffCoefficient(2.0)
                      .setMaximumInterval(Duration.ofMinutes(1L))
                      .setMaximumAttempts(5)
                      .build())
              .build());

  @Override
  public void execute(String integrationID) {
    String workFlowID = Workflow.getInfo().getWorkflowId();
    Duration workflowTimeoutDuration = Workflow.getInfo().getWorkflowExecutionTimeout();
    logger.info("started workflowID: {}, resourceID {} ", workFlowID, integrationID);

    var mutexWorkflowExecution = tryAcquireLock(integrationID, workflowTimeoutDuration);
    Workflow.await(() -> isReadyForExecution);

    logger.info("critical operation started");
    Workflow.sleep(Duration.ofSeconds(60L));
    logger.info("critical operation finished");

    releaseLock(mutexWorkflowExecution);
    logger.info("finished workflowID: {}, integrationID {} ", workFlowID, integrationID);
  }

  @Override
  public void lockAcquired() {
    this.isReadyForExecution = true;
  }

  private WorkflowExecution tryAcquireLock(String resourceID, Duration lockDuration) {
    return activities.signalWithStartMutexWorkflow(resourceID, lockDuration, true);
  }

  private void releaseLock(WorkflowExecution mutexWorkflowExecution) {
    ExternalWorkflowStub externalWorkflowStub =
        Workflow.newUntypedExternalWorkflowStub(mutexWorkflowExecution);
    externalWorkflowStub.signal("releaseLock", Workflow.getInfo().getWorkflowId());
  }
}
