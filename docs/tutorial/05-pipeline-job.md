# Part 5: Keeping results

The results of Parts 1 to 4 were held in memory. They are gone when Python exits, and running
the script again runs every function again. With long computations or many rows, you want to
keep the results.

A **`PipelineJob`** stores each result in a database, with a record of the inputs it was
computed from. The next time the pipeline runs, results that are already stored are read back
instead of being computed again.

You will:

- record a pipeline inside a `PipelineJob` and run it
- read results from the job's **nodes**
- keep the results on disk, and see a second run compute nothing

```python
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.sources import CSVSource

data_dir = make_dataset()

sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"], tag_only_columns=True,
    source_id="sessions",
)
units = CSVSource(
    str(data_dir / "units.csv"),
    tag_columns=["subject_id", "session", "probe", "unit"], tag_only_columns=True,
    source_id="units",
)

calls = []  # every function call adds an entry, so we can see when functions really run


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    calls.append("firing_rate")
    return n_spikes / duration_s


@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    calls.append("mean_rate")
    return sum(rate_hz) / len(rate_hz)
```

These are the two functions from Part 4. The `calls` list counts how often each function was
called.

## Run a recorded job

A job needs a **database** for its results. Start with one in memory, and write the pipeline
inside a `with` block:

```python
from orcapod.databases import InMemoryArrowDatabase

database = InMemoryArrowDatabase()

job = PipelineJob(name="workshop", database=database)
with job:
    rates = firing_rate.pod(
        units.select_data_columns(["n_spikes"]),
        sessions.select_data_columns(["duration_s"]),
        label="rates",
    )
    mean_rate.pod(
        rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"]),
        label="session_means",
    )

print(len(calls))
# 0
```

No function was called in this block. The job only **records** which operators and pods are
applied to which streams. `label=` names a step, so that you can find its results.

`run()` executes what was recorded and writes every result to the database:

```python
job.run()

print(len(calls))
# 71
print(calls.count("firing_rate"), calls.count("mean_rate"))
# 63 8
```

`mean_rate` ran 8 times, once per session. `firing_rate` ran 63 times for 64 units, because two
units have the same spike count and the same session duration. Orcapod stores a result under
the function and the *values* of its inputs, so the second of those two units found its result
already stored.

## Nodes

Each recorded step is a **node** of the job. `job.nodes` lists them by name:

```python
print(sorted(job.nodes))
# ['GroupBy', 'Join', 'SelectDataColumns_1', 'SelectDataColumns_2', 'SelectDataColumns_3', 'rates', 'session_means', 'sessions', 'units']
```



- A source is listed under its `source_id`.
- A function pod is listed under the `label` you gave it. Without a label it gets the
  function's name.
- Operators are named after their kind, numbered when there are several.

To see how the nodes connect, put `job` on the last line of a notebook cell: the job draws
itself as a diagram. [Working in notebooks](../concepts/notebooks.md#the-pipeline-diagram)
has a picture of this job's diagram and what each display in a notebook shows.

!!! caution "In development"
    `Pipeline.diagram()`, `PipelineJob.diagram()`, `PipelineDiagram` and its methods and
    arguments, the look of the picture and the notebook display may change in any release
    without a deprecation period. The first diagram in a Python session issues
    `orcapod.errors.InDevelopmentWarning` once, as a reminder; to silence it, run
    `warnings.filterwarnings("ignore", category=InDevelopmentWarning)` after importing
    it from `orcapod.errors`.

Outside a notebook, `print(job.diagram())` gives the same graph as text, one node per line
with the nodes it reads from:

```python
print(job.diagram())
# Job 'workshop': 9 nodes
#   sessions (source · CSVSource · bound)
#   SelectDataColumns_2 (operator · current) <- sessions
#   units (source · CSVSource · bound)
#   SelectDataColumns_1 (operator · current) <- units
#   Join (operator · current) <- SelectDataColumns_2, SelectDataColumns_1
#   rates (function pod · firing_rate · current) <- Join
#   GroupBy (operator · current) <- rates
#   SelectDataColumns_3 (operator · current) <- GroupBy
#   session_means (function pod · mean_rate · current) <- SelectDataColumns_3
```

The `Join` is there because the pod joins the two streams given to `firing_rate`. `current`
means the node holds the rows of the latest run. Before `job.run()`, every node but the
sources says `not run`. `job.diagram(collapse=True)` hides the steps that only choose or join
columns. The sources, the function pods and the `GroupBy` remain:

```python
print(job.diagram(collapse=True))
# Job 'workshop': 5 nodes
#   sessions (source · CSVSource · bound)
#   units (source · CSVSource · bound)
#   rates (function pod · firing_rate · current) <- sessions, units
#   GroupBy (operator · current) <- rates
#   session_means (function pod · mean_rate · current) <- GroupBy
```

Read the node of a function pod or an operator like any other stream. The node of a source has
`as_table()`, `keys()` and `output_schema()`, but no `for` loop and no DataFrame methods:

```python
means = job.nodes["session_means"]
print(means.output_schema())
# (Schema({'subject_id': str, 'session': str}), Schema({'mean_rate_hz': float}))
print(means.as_pandas_df().sort_values(["subject_id", "session"], ignore_index=True))
#   subject_id session  mean_rate_hz
# 0   mouse_01      s1      6.033333
# 1   mouse_01      s2      6.793750
# 2   mouse_02      s1      5.845833
# 3   mouse_02      s2      7.506250
# 4   mouse_03      s1      4.906944
# 5   mouse_03      s2      5.059375
# 6   mouse_04      s1      5.136458
# 7   mouse_04      s2      5.129167
print(job.nodes["rates"].as_table().num_rows)
# 64
```

!!! warning "Read nodes after `run()`"
    A node shows the rows of the job's run in this Python session. Before `run()`, every node
    except the sources gives an empty table and raises no error. Only `head()` and the notebook
    preview say "not run". If a node looks empty, check that you ran the job.

## Run it again

Record the same pipeline in a new job over the same database:

```python
def build_job(database):
    job = PipelineJob(name="workshop", database=database)
    with job:
        rates = firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )
        mean_rate.pod(
            rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"]),
            label="session_means",
        )
    return job


job = build_job(database)
job.run()

print(len(calls))
# 71
print(job.nodes["session_means"].as_table().num_rows)
# 8
```

Still 71: the second run called no function. Every result was found in the database.

## Keep results on disk

`InMemoryArrowDatabase` is gone when Python exits. `DeltaTableDatabase` writes to a directory:

```python
import shutil

from orcapod.databases import DeltaTableDatabase

shutil.rmtree("workshop_db", ignore_errors=True)  # start empty, also when you rerun this page
job = build_job(DeltaTableDatabase("workshop_db"))
job.run()
print(len(calls))
# 142
```

This database was empty, so everything was computed: 71 further calls.

Now make a new database object for the same directory, as a new Python session would, and
build and run the job:

```python
job = build_job(DeltaTableDatabase("workshop_db"))
job.run()

print(len(calls))
# 142
means = job.nodes["session_means"].as_pandas_df()
print(means.sort_values(["subject_id", "session"], ignore_index=True).head(2))
#   subject_id session  mean_rate_hz
# 0   mouse_01      s1      6.033333
# 1   mouse_01      s2      6.793750
```

No calls. The results came from the files in `workshop_db/`:

```python
from pathlib import Path

print(sorted(p.name for p in Path("workshop_db").iterdir()))
# ['workshop']
print(sorted(p.name for p in Path("workshop_db/workshop").iterdir()))
# ['_result', 'firing_rate', 'mean_rate']
```

The first level is the job's `name`. Below it, `_result` holds each function's results, and
the directory named after each function records which inputs the job gave it. By default, two
jobs with different names over the same directory do not share results. If you delete the
directory, the next run computes everything again.

Rows read back from a database are not guaranteed to come in the order they were computed in.
Sort them whenever the order matters, as the samples here do.

## Key points

- `PipelineJob(name=..., database=...)`, a `with` block to record, `job.run()` to execute.
- Give pods a `label`; read results from `job.nodes[label]` after `run()`.
- Results are stored per function and per input values. A second run over the same
  database computes only what is missing.
- `InMemoryArrowDatabase` for trying things out; `DeltaTableDatabase("directory")` to keep
  results between sessions.

## Exercises

**1.** Run a pipeline, quit Python, and run it again. Put the script below in a file
`run_pipeline.py` and run `uv run python run_pipeline.py` twice (with `pip`, in the activated
environment: `python run_pipeline.py`). What does each run print?

```python
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import CSVSource

data_dir = make_dataset()
n_calls = 0


@function_pod(output_keys="depth_mm")
def to_mm(depth_um: int) -> float:
    global n_calls
    n_calls += 1
    return depth_um / 1000


job = PipelineJob(name="depths", database=DeltaTableDatabase("exercise_db"))
with job:
    units = CSVSource(
        str(data_dir / "units.csv"),
        tag_columns=["subject_id", "session", "probe", "unit"], tag_only_columns=True,
        source_id="units",
    )
    to_mm.pod(units.select_data_columns(["depth_um"]), label="depth_mm")
job.run()

print("rows:", job.nodes["depth_mm"].as_table().num_rows)
# rows: 64
print("function calls in this run:", n_calls)
# function calls in this run: 54
```

??? success "Solution"
    The first run prints `rows: 64` and `function calls in this run: 54`. It is below
    64 because some units share a depth, and identical inputs are computed once.

    The second run prints `rows: 64` and `function calls in this run: 0`. It shares only the
    `exercise_db` directory with the first run.

    Delete `exercise_db/` and run the script once more: 54 calls again.

**2.** In the main example, which names would `job.nodes` hold if you removed both `label=`
arguments? Predict, then try it with a new job over an `InMemoryArrowDatabase`.

??? success "Solution"
    The two function nodes are listed under their function names.

    ```python
    unlabelled = PipelineJob(name="unlabelled", database=InMemoryArrowDatabase())
    with unlabelled:
        rates = firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
        )
        mean_rate.pod(
            rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"])
        )
    print(sorted(unlabelled.nodes))
    # ['GroupBy', 'Join', 'SelectDataColumns_1', 'SelectDataColumns_2', 'SelectDataColumns_3', 'firing_rate', 'mean_rate', 'sessions', 'units']
    ```

    Labels tell the nodes apart when one function is applied in two places of a pipeline.

**3.** Create a job without a database, record one step, and call `run()`. Read the error and do
what it suggests.

??? success "Solution"
    ```python
    no_database = PipelineJob(name="no_database")
    with no_database:
        firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )

    try:
        no_database.run()
    except ValueError as err:
        print(err)
    # PipelineJob.run() requires a database. Call job.bind(database=db) before run(). For transient (in-memory) storage use job.bind(database=InMemoryArrowDatabase()).

    no_database.bind(database=InMemoryArrowDatabase())
    no_database.run()
    print(no_database.nodes["rates"].as_table().num_rows)
    # 64
    ```
