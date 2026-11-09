# Part 10: Where next

Optional. You now know the core of Orcapod: sources, function pods, operators, jobs, and how
results are stored and reused. This part is a short tour of features the tutorial did not
need. Each section is independent; read the ones that match what you want to do next.

```python
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import CSVSource

data_dir = make_dataset()
sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"],
    source_id="sessions",
)
durations = sessions.select_data_columns(["duration_s"])
```

## Steps that act instead of compute

A function pod returns a value. Some steps exist for their effect: send a notification,
upload a file, write a row to a lab database. These are **side-effect pods**. The most
useful kind is the **sink**, made with `@sink_pod`: it runs once for each row and remembers
which rows it has already handled, so a re-run does not repeat the effect.

```python
from orcapod import InvocationContext, sink_pod

delivered = []


@sink_pod
def announce(duration_s: float, ctx: InvocationContext) -> None:
    delivered.append(duration_s)  # stands in for "send a message"


store = InMemoryArrowDatabase()


def run_announcements():
    job = PipelineJob(name="announcements", store=store)
    with job:
        announce(durations, label="announce")
    job.run()


run_announcements()
print(len(delivered))
# 8
run_announcements()
print(len(delivered))
# 8
```

Eight sessions, eight deliveries, and none on the second run.

Two differences from a function pod. A sink is applied by calling it directly
(`announce(stream)`, no `.pod`). And its function **must** take a second parameter named
`ctx`, which receives an `InvocationContext` describing the call; without it the run fails
with `TypeError: ... got an unexpected keyword argument 'ctx'`. `@tap_pod` and
`@side_effect_pod` are variants with different defaults for tracking and failure handling.

## Pods without the decorator

When the function comes from a library, or you want to keep it undecorated, build the pod
with `FunctionPod.from_fn`:

```python
from orcapod import FunctionPod


def to_minutes(duration_s: float) -> float:
    return duration_s / 60


minutes_pod = FunctionPod.from_fn(to_minutes, output_keys="duration_min")
print(minutes_pod(durations).as_pandas_df().head(2))
#   subject_id session  duration_min
# 0   mouse_01      s1           1.0
# 1   mouse_01      s2           2.0
```

One trap: `FunctionPod.from_fn` defaults to `version="v1.0"`, while `@function_pod` defaults
to `"v0.0"`. Switching a function from one form to the other therefore changes its major
version and recomputes its results (Part 6). Pass `version=` explicitly when you care.

## Saving a job

`job.save(path)` writes a description of a job to a JSON file: its steps, how they connect,
and whether it has run. `PipelineJob.load(path)` reads it back.

```python
@function_pod(output_keys="duration_min")
def minutes(duration_s: float) -> float:
    return duration_s / 60


job = PipelineJob(name="durations", store=store)
with job:
    minutes.pod(durations, label="minutes")
job.run()
job.save("durations_job.json")

loaded = PipelineJob.load("durations_job.json", store=store)
print("minutes" in loaded.nodes)
# True
print(loaded.nodes["minutes"].get_all_records().num_rows)
# 8
```

A loaded job is for **looking at results**. It does not contain your functions, only their
names, so it cannot compute anything: `loaded.run()` raises `RuntimeError`, and on a loaded
function node use `get_all_records()` (not `as_table()`). To compute, run the script that
defines the pipeline. More in [Pipelines and jobs](../concepts/pipeline-job.md).

## Further topics

Not covered here, with pointers:

- **Pipeline blueprints.** Define a pipeline once with `Pipeline`, then create jobs from it
  with different sources or stores using `PipelineJob.from_pipeline(...)` and
  `job.bind(...)`. See [Pipelines and jobs](../concepts/pipeline-job.md).
- **Other sources.** pandas and Polars DataFrames, Delta tables, SQLite and PostgreSQL
  tables, and `PollingSource` for data that keeps arriving. See
  [Sources](../concepts/sources.md) and the [API reference](../api/sources.md).
- **Running in parallel.** The `orcapod[ray]` extra adds an executor that runs function
  calls on a [Ray](https://www.ray.io/) cluster. `AsyncPipelineOrchestrator` in
  `orcapod.pipeline` runs independent branches of a pipeline concurrently.
- **How identity works.** [Identity & Hashing](../concepts/identity.md) explains the two
  hashes behind caching.
- **The API reference.** Every public class and function is listed under
  [API Reference](../api/index.md).

## Exercises

**1.** Change `announce` so that it only "delivers" sessions longer than 100 seconds, give
the job a new name, and run it twice. How many deliveries are there after each run?

??? success "Solution"
    First, how many sessions qualify? Four have a duration of 120 seconds:

    ```python
    import polars as pl

    long_sessions = pl.from_arrow(durations.as_table()).filter(pl.col("duration_s") > 100)
    print(len(long_sessions))
    # 4
    ```

    The natural design is to filter the stream before the sink. Inside a job an expression
    filter (`duration_s > 100`) cannot be used yet (Part 3), so make the decision in the
    function instead:

    ```python
    delivered_long = []


    @sink_pod
    def announce_long(duration_s: float, ctx: InvocationContext) -> None:
        if duration_s > 100:
            delivered_long.append(duration_s)


    def run_long():
        job = PipelineJob(name="long_announcements", store=store)
        with job:
            announce_long(durations, label="announce_long")
        job.run()


    run_long()
    print(len(delivered_long))
    # 4
    run_long()
    print(len(delivered_long))
    # 4
    ```

    Four after the first run, still four after the second. The sink ran for all eight rows
    the first time (it decided to do nothing for four of them) and for none the second
    time.

**2.** Wrap Python's built-in `round` with `FunctionPod.from_fn`? Try it, and explain what
happens.

??? success "Solution"
    It fails. Orcapod needs type annotations to build a pod (Part 2, Rule 1), and built-in
    functions do not carry them. Wrap the built-in in a small annotated function of your
    own:

    ```python
    try:
        FunctionPod.from_fn(round, output_keys="rounded")
    except ValueError as err:
        print(err)
    # Parameter 'number' has no type annotation and is not specified in input_types.


    def round_duration(duration_s: float) -> int:
        return round(duration_s)


    rounded = FunctionPod.from_fn(round_duration, output_keys="duration_rounded")
    print(rounded(durations).output_schema()[1])
    # Schema({'duration_rounded': int})
    ```

That is the end of the tutorial. The [Concepts](../concepts/sources.md) pages go deeper on
each topic, and [Troubleshooting](../troubleshooting.md) is the page to keep open while you
write your first pipeline.
