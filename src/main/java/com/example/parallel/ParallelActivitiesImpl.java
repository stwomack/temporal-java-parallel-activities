package com.example.parallel;

import io.temporal.activity.Activity;

/** Each activity simulates one second of work so the parallel speedup is visible. */
public class ParallelActivitiesImpl implements ParallelActivities {

  @Override
  public String activity1(String input) {
    return work("activity1", input);
  }

  @Override
  public String activity2(String input) {
    return work("activity2", input);
  }

  @Override
  public String activity3(String input) {
    return work("activity3", input);
  }

  @Override
  public String activity4(String input) {
    return work("activity4", input);
  }

  @Override
  public String activity5(String r2, String r3, String r4) {
    return work("activity5", r2 + " + " + r3 + " + " + r4);
  }

  private static String work(String name, String input) {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Activity.wrap(e);
    }
    return name + "(" + input + ")";
  }
}
