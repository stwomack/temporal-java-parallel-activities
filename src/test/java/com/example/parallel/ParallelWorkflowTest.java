package com.example.parallel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.ApplicationFailure;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.TestWorkflowExtension;
import io.temporal.testing.WorkflowReplayer;
import io.temporal.worker.Worker;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ParallelWorkflowTest {

  @RegisterExtension
  static final TestWorkflowExtension testWorkflow =
      TestWorkflowExtension.newBuilder()
          .setWorkflowTypes(ParallelWorkflowImpl.class)
          .setDoNotStart(true)
          .build();

  /**
   * Activities 2, 3 and 4 each wait on a latch that only opens once all three have started. If the
   * workflow ran them one at a time, the first would time out and fail the test.
   */
  static class RecordingActivities implements ParallelActivities {
    final List<String> calls = new CopyOnWriteArrayList<>();
    final CountDownLatch allThreeStarted = new CountDownLatch(3);
    final String failOn;

    RecordingActivities(String failOn) {
      this.failOn = failOn;
    }

    @Override
    public String activity1(String input) {
      calls.add("activity1");
      return "r1";
    }

    @Override
    public String activity2(String input) {
      return parallel("activity2", input);
    }

    @Override
    public String activity3(String input) {
      return parallel("activity3", input);
    }

    @Override
    public String activity4(String input) {
      return parallel("activity4", input);
    }

    @Override
    public String activity5(String r2, String r3, String r4) {
      calls.add("activity5");
      return r2 + "," + r3 + "," + r4;
    }

    private String parallel(String name, String input) {
      calls.add(name);
      allThreeStarted.countDown();
      try {
        if (!allThreeStarted.await(10, TimeUnit.SECONDS)) {
          throw ApplicationFailure.newNonRetryableFailure(
              name + " did not overlap with the other parallel activities", "NotParallel");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      if (name.equals(failOn)) {
        throw ApplicationFailure.newNonRetryableFailure(name + " failed", "Boom");
      }
      return name + "<" + input + ">";
    }
  }

  private static ParallelWorkflow newStub(WorkflowClient client, Worker worker, String id) {
    return client.newWorkflowStub(
        ParallelWorkflow.class,
        WorkflowOptions.newBuilder().setWorkflowId(id).setTaskQueue(worker.getTaskQueue()).build());
  }

  @Test
  void runs2Through4InParallelBetween1And5(
      TestWorkflowEnvironment env, Worker worker, WorkflowClient client) {
    RecordingActivities activities = new RecordingActivities(null);
    worker.registerActivitiesImplementations(activities);
    env.start();

    String result = newStub(client, worker, "happy-path").run("in");

    assertEquals("activity2<r1>,activity3<r1>,activity4<r1>", result);
    assertEquals("activity1", activities.calls.get(0));
    assertEquals("activity5", activities.calls.get(activities.calls.size() - 1));
    assertEquals(5, activities.calls.size());
  }

  @Test
  void failureInParallelStepSkipsActivity5(
      TestWorkflowEnvironment env, Worker worker, WorkflowClient client) {
    RecordingActivities activities = new RecordingActivities("activity3");
    worker.registerActivitiesImplementations(activities);
    env.start();

    WorkflowFailedException e =
        assertThrows(
            WorkflowFailedException.class, () -> newStub(client, worker, "failure").run("in"));

    assertInstanceOf(ActivityFailure.class, e.getCause());
    assertFalse(activities.calls.contains("activity5"));
  }

  @Test
  void replaysDeterministically(TestWorkflowEnvironment env, Worker worker, WorkflowClient client)
      throws Exception {
    worker.registerActivitiesImplementations(new RecordingActivities(null));
    env.start();

    newStub(client, worker, "replay").run("in");
    WorkflowExecutionHistory history = client.fetchHistory("replay");

    WorkflowReplayer.replayWorkflowExecution(history, ParallelWorkflowImpl.class);
  }
}
