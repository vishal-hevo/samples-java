package io.temporal.samples.mutex;

import static io.temporal.samples.mutex.IncrementalStarter.INTEGRATION_ID;
import static io.temporal.samples.mutex.IncrementalStarter.TASK_QUEUE;

import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

public class TNLStarter {
  public static void main(String[] args) throws Exception {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    WorkflowOptions workflowOptions =
        WorkflowOptions.newBuilder()
            .setTaskQueue(TASK_QUEUE)
            .setWorkflowId(String.format("tnl_workflow:%s", INTEGRATION_ID))
            .build();

    TNLWorkflow workflow = client.newWorkflowStub(TNLWorkflow.class, workflowOptions);

    WorkflowExecution workflowExecution = WorkflowClient.start(workflow::execute, INTEGRATION_ID);

    System.out.println(workflowExecution.toString());
    System.exit(0);
  }
}
