# Parallel Activities in a Temporal Java Workflow

A minimal, tested example of a Workflow that runs five Activities in this order:

1. `activity1` runs first.
2. `activity2`, `activity3`, and `activity4` run in parallel, each using the result of `activity1`.
3. `activity5` runs after all three parallel Activities complete, using their results.

Built against Temporal Java SDK 1.40.0 and Java 17.

## The Workflow

```java
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
```

## How it works

- `Async.function` schedules an Activity and returns a `Promise` immediately, so all three Activities are scheduled in the same Workflow Task and run concurrently on your Workers.
- `Promise.allOf(...).get()` blocks the Workflow until all three complete.
- If any parallel Activity fails after exhausting its retries, `get()` throws an `ActivityFailure` and `activity5` never runs. Catch it if you want compensation logic.
- Use `Promise.anyOf` instead if `activity5` should start as soon as the first one finishes.
- Do not use Java threads, `ExecutorService`, or `CompletableFuture` in Workflow code. They bypass the Workflow scheduler and break deterministic replay. `Async` and `Promise` are the replay-safe equivalents.
- By default the Activity Type name is the method name with the first letter capitalized (`activity1` registers as `Activity1`). Use `@ActivityMethod(name = "...")` to set it explicitly.

## Run the tests

```bash
mvn test
```

The tests use Temporal's in-memory test server, so nothing else needs to be running. They verify:

- **Parallelism:** Activities 2, 3, and 4 each wait on a latch that only opens once all three have started. If they ran sequentially the test would fail.
- **Ordering:** `activity1` runs first and `activity5` runs last.
- **Failure handling:** a non-retryable failure in `activity3` fails the Workflow and `activity5` is never called.
- **Determinism:** the Workflow's recorded history replays cleanly through `WorkflowReplayer`.

## Run it against a Temporal server

Start a local dev server:

```bash
temporal server start-dev
```

In one terminal, start the Worker:

```bash
mvn compile exec:java -Dexec.mainClass=com.example.parallel.WorkerMain
```

In another terminal, start the Workflow:

```bash
mvn compile exec:java -Dexec.mainClass=com.example.parallel.Starter
```

Expected output (each Activity sleeps one second, so parallel execution takes about 3 seconds instead of 5):

```
Result: activity5(activity2(activity1(hello)) + activity3(activity1(hello)) + activity4(activity1(hello)))
Elapsed: 3402 ms (about 3s parallel vs 5s sequential)
```

### Temporal Cloud

Connection settings are loaded with `ClientConfigProfile.load()`, so you can point the same code at Temporal Cloud with environment variables and no code changes:

```bash
export TEMPORAL_ADDRESS=<namespace>.<account>.tmprl.cloud:7233
export TEMPORAL_NAMESPACE=<namespace>.<account>
export TEMPORAL_API_KEY=<your-api-key>
```

See [Environment configuration](https://docs.temporal.io/develop/environment-configuration) for all options, including mTLS.
