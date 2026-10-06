package com.example.parallel;

import io.temporal.client.WorkflowClient;
import io.temporal.envconfig.ClientConfigProfile;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

public class WorkerMain {

  public static void main(String[] args) throws Exception {
    ClientConfigProfile profile = ClientConfigProfile.load();
    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(profile.toWorkflowServiceStubsOptions());
    WorkflowClient client = WorkflowClient.newInstance(service, profile.toWorkflowClientOptions());

    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker = factory.newWorker(Shared.TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(ParallelWorkflowImpl.class);
    worker.registerActivitiesImplementations(new ParallelActivitiesImpl());

    factory.start();
    System.out.println("Worker polling task queue: " + Shared.TASK_QUEUE);
  }
}
