package com.example.parallel;

import io.temporal.activity.ActivityInterface;

@ActivityInterface
public interface ParallelActivities {
  String activity1(String input);

  String activity2(String input);

  String activity3(String input);

  String activity4(String input);

  String activity5(String r2, String r3, String r4);
}
