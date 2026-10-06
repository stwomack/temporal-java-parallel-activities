package com.example.parallel;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.util.UUID;

public class Starter {

  public static void main(String[] args) throws Exception {
    ClientConfigProfile profile = ClientConfigProfile.load();
    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(profile.toWorkflowServiceStubsOptions());
    WorkflowClient client = WorkflowClient.newInstance(service, profile.toWorkflowClientOptions());

    ParallelWorkflow workflow =
        client.newWorkflowStub(
            ParallelWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId("parallel-" + UUID.randomUUID())
                .setTaskQueue(Shared.TASK_QUEUE)
                .build());

    long start = System.currentTimeMillis();
    String result = workflow.run("hello");
    long elapsed = System.currentTimeMillis() - start;

    System.out.println("Result: " + result);
    System.out.println("Elapsed: " + elapsed + " ms (about 3s parallel vs 5s sequential)");
    System.exit(0);
  }
}
