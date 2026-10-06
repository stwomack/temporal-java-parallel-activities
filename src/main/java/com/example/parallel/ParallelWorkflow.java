package com.example.parallel;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface ParallelWorkflow {
  @WorkflowMethod
  String run(String input);
}
