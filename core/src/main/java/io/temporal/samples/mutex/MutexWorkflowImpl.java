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

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.workflow.ExternalWorkflowStub;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;

public class MutexWorkflowImpl implements MutexWorkflow {
  private record LockHolder(String workflowID, String runID) {}

  private final Logger logger = Workflow.getLogger(MutexWorkflowImpl.class);
  private final Queue<LockHolder> waitingQueue = new ConcurrentLinkedQueue<>();
  private AtomicReference<LockHolder> currentLockerHolder;
  private final AtomicInteger lockContention = new AtomicInteger(0);

  @Override
  public void mutex() {
    String workFlowID = Workflow.getInfo().getWorkflowId();
    logger.info("started workflowID: {}", workFlowID);

    Workflow.await(() -> lockContention.get() == 0);
  }

  @Override
  public void tryAcquireLock(
      String callerWorkflowID,
      String callerWorkflowRunID,
      Duration lockDuration,
      boolean waitForLockToBeAcquired) {
    if (currentLockerHolder == null) {
      currentLockerHolder =
          new AtomicReference<>(new LockHolder(callerWorkflowID, callerWorkflowRunID));
      lockContention.addAndGet(1);
      notifyLockAcquiredToCaller(callerWorkflowID, callerWorkflowRunID);
    } else if (waitForLockToBeAcquired) {
      waitingQueue.add(new LockHolder(callerWorkflowID, callerWorkflowRunID));
      lockContention.addAndGet(1);
    }
  }

  @Override
  public void releaseLock(String callerWorkflowID) {
    if (currentLockerHolder == null
        || !currentLockerHolder.get().workflowID.equals(callerWorkflowID)) {
      logger.error("Caller {} is not the current lock holder", callerWorkflowID);
      return;
    }

    currentLockerHolder = null;
    lockContention.addAndGet(-1);

    LockHolder nextLockHolder = waitingQueue.poll();
    if (nextLockHolder != null) {
      currentLockerHolder = new AtomicReference<>(nextLockHolder);
      notifyLockAcquiredToCaller(nextLockHolder.workflowID, nextLockHolder.runID);
    }
  }

  @Override
  public boolean isLockAcquiredByWorkflow(String workflowID) {
    return this.currentLockerHolder != null
        && this.currentLockerHolder.get().workflowID.equals(workflowID);
  }

  private void notifyLockAcquiredToCaller(String workflowID, String runID) {
    ExternalWorkflowStub externalWorkflowStub =
        Workflow.newUntypedExternalWorkflowStub(
            WorkflowExecution.newBuilder().setWorkflowId(workflowID).setRunId(runID).build());
    externalWorkflowStub.signal("lockAcquired");
  }
}
