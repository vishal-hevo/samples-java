package io.temporal.samples.mutex;

import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface TNLWorkflow {
  @WorkflowMethod
  void execute(String integrationID);

  @SignalMethod
  void lockAcquired();
}
