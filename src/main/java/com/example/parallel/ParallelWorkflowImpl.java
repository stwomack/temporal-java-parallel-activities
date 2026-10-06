package com.example.parallel;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;

public class ParallelWorkflowImpl implements ParallelWorkflow {

  private final ParallelActivities activities =
      Workflow.newActivityStub(
          ParallelActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(30)).build());

  @Override
  public String run(String input) {
    // 1: sequential
    String r1 = activities.activity1(input);

    // 2, 3, 4: parallel
    Promise<String> p2 = Async.function(activities::activity2, r1);
    Promise<String> p3 = Async.function(activities::activity3, r1);
    Promise<String> p4 = Async.function(activities::activity4, r1);
    Promise.allOf(p2, p3, p4).get();

    // 5: runs after all three complete
    return activities.activity5(p2.get(), p3.get(), p4.get());
  }
}
