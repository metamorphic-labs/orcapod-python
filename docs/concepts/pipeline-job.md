# Pipelines and Jobs

Applying a [function pod](function-pods.md) or an [operator](operators.md) to a
[stream](streams.md) directly computes in memory and keeps nothing. A **`PipelineJob`** runs
the same steps against a database: every result is stored, and a later run computes only
what is not stored yet. A **`Pipeline`** is the description of the steps on its own, without
data or a database, from which several jobs can be made.

For a guided introduction see the tutorial, [Part 5](../tutorial/05-pipeline-job.md) and
[Part 6](../tutorial/06-reruns-and-caching.md). This page is the reference for the same
material.

## A job: record, then run

```python
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource


@function_pod(output_keys="bmi")
def compute_bmi(weight: float, height: float) -> float:
    return weight / height**2


def mice(rows):
    return DictSource(data=rows, tag_columns=["subject_id"], source_id="mice")


cohort_a = [
    {"subject_id": "mouse_01", "weight": 25.3, "height": 0.12},
    {"subject_id": "mouse_02", "weight": 22.1, "height": 0.10},
]

store = InMemoryArrowDatabase()

job = PipelineJob(name="bmi", store=store)
with job:
    compute_bmi.pod(mice(cohort_a), label="bmi")
job.run()

print(job.nodes["bmi"].as_table().to_pylist())
# [{'subject_id': 'mouse_01', 'bmi': 1756.9444444444446}, {'subject_id': 'mouse_02', 'bmi': 2209.9999999999995}]
```

The three stages:

1. **Create** the job with a `name` and a `store`.
2. **Record** inside the `with` block. Pods and operators applied there are not executed;
   the job notes which step is applied to which stream. Sources may be created inside or
   outside the block.
3. **Run** with `job.run()`. The recorded steps execute in dependency order and each
   function result is written to the store. `run()` returns the job itself.

`job.run()` accepts two optional arguments that the tutorial's
[Part 9](../tutorial/09-when-things-go-wrong.md) demonstrates: `orchestrator=` (for example
`SyncPipelineOrchestrator(error_policy="fail_fast")` to stop at the first failing row; the
default policy skips failed rows and continues) and `observer=` (`StatusObserver`,
`LoggingObserver`) to record what happened during the run.

## Nodes

Each recorded step, and each source, is a **node**. `job.nodes` maps a node's name to the
node:

| Kind of step | Name in `job.nodes` |
|---|---|
| Source | Its `source_id` |
| Function pod | The `label=` passed when it was applied; otherwise the function's name |
| Operator | The operator's class name (`Join`, `GroupBy`), numbered when there are several (`SelectDataColumns_1`) |

```python
print(sorted(job.nodes))
# ['bmi', 'mice']
```

The order in which `job.nodes` lists its names is not meaningful and can differ between
Python sessions; look nodes up by name.

A node is a stream: `output_schema()`, `keys()`, `iter_data()`, `as_table()` and
`as_pandas_df()` work on it. Three more things a node of a run job offers:

- **`get_all_records()`** returns everything stored for the function in this job's part of
  the store, including results of earlier runs over other inputs. `as_table()` returns only
  the results for this job's inputs. Pass `columns={"meta": True}` to include the record of
  each call (function name, time, Python version).
- **`as_source()`** returns a `DerivedSource` over the stored results, to use as the input
  of another pipeline.
- **`upstreams`** and **`label`** let you walk the pipeline backwards.

Read nodes after `run()`. Before it, a function node has no results and returns an empty
table.

## The store

The store is any Orcapod database. Two are used most:

- **`InMemoryArrowDatabase()`** keeps results for the lifetime of the Python process. Use it
  for experiments and tests.
- **`DeltaTableDatabase("some/directory")`** writes [Delta Lake](https://delta.io/) tables
  under a directory, and is what makes results survive between sessions.

On disk the layout is `<directory>/<job name>/<function name>/...` for each function, plus
`<directory>/<job name>/_result/...`. The job's `name` is the top level, so jobs with
different names over one directory do not see each other's results. Deleting the directory,
or a job's subdirectory, is safe: the next run recomputes what is missing.

A job created without a store can be recorded but not run. Give it one later with
`job.bind(store=...)`; `run()` without a store raises `ValueError`.

## What a re-run computes

A stored result is reused when the function's identity (its name, its output schema and its
**major** version), the input values of the row, the structure of the pipeline upstream of
the function, and the job's name and store are all unchanged.

```python
cohort_a.append({"subject_id": "mouse_03", "weight": 27.8, "height": 0.11})

calls = []


@function_pod(output_keys="bmi")
def compute_bmi(weight: float, height: float) -> float:
    calls.append(weight)
    return weight / height**2


job = PipelineJob(name="bmi", store=store)
with job:
    compute_bmi.pod(mice(cohort_a), label="bmi")
job.run()

print(job.nodes["bmi"].as_table().num_rows, len(calls))
# 3 1
```

Three rows in the result, one call: the two rows stored by the first job were reused, even
though `compute_bmi` was defined again. The function's source code is not part of its
identity. **Editing a function body does not recompute its results**; raise the major
version (`@function_pod(..., version="v1.0")`; the default is `"v0.0"`) when you change what
a function computes. See [Identity & Hashing](identity.md#change-detection).

## `Pipeline`: a blueprint without data

A `Pipeline` records steps the same way, but has no store and never runs. It is a
**blueprint**. Its sources become named slots, and a job is made from it by filling them:

```python
from orcapod import Pipeline

pipeline = Pipeline("bmi")
with pipeline:
    compute_bmi.pod(mice(cohort_a), label="bmi")

cohort_b = [{"subject_id": "mouse_11", "weight": 24.0, "height": 0.11}]

job_a = PipelineJob.from_pipeline(pipeline, store=store, sources={"mice": mice(cohort_a)})
job_b = PipelineJob.from_pipeline(pipeline, store=store, sources={"mice": mice(cohort_b)})
job_a.run()
job_b.run()

print(job_a.nodes["bmi"].as_table().num_rows, job_b.nodes["bmi"].as_table().num_rows)
# 3 1
```

`sources` maps a source slot's name (the `source_id` used when the blueprint was recorded)
to the source to use. A source bound to a slot must have the slot's columns and types; a
mismatch raises `SourceSpecMismatchError`, and an unknown slot name raises `ValueError`.

A job made without sources has **unbound** slots. It can still be run: nodes that depend on
an unbound source are skipped, and `job.unbound_sources` lists what is missing. Bind later
with `job.bind(...)`:

```python
job_c = PipelineJob.from_pipeline(pipeline, store=store)
print(job_c.unbound_sources)
# ['mice']

job_c.bind(sources={"mice": mice(cohort_b)})
print(job_c.unbound_sources)
# []
job_c.run()
print(job_c.nodes["bmi"].as_table().num_rows)
# 1
```

Every job also carries its blueprint as `job.pipeline`. That object has `show_graph()`,
which draws the pipeline and needs the Graphviz `dot` program to be installed.

## Saving and loading

`job.save(path)` writes a JSON description of the job: its nodes, their connections and
schemas, and whether the job has run. `PipelineJob.load(path, store=...)` reads it back.

```python
job_a.save("bmi_job.json")

loaded = PipelineJob.load("bmi_job.json", store=store)
print(loaded.nodes["bmi"].get_all_records().num_rows)
# 4
```

A loaded job is for **inspecting stored results**. The file holds the names of your
functions, not their code, so the loaded function nodes are read-only:

- `loaded.nodes[label].get_all_records()` works. It returns everything stored for that
  function: here the three rows of cohort A and the one of cohort B.
- `loaded.nodes[label].as_table()` raises `RuntimeError`.
- `loaded.run()` raises `RuntimeError`. To compute, run the code that defines the pipeline.

`Pipeline.save(path)` and `Pipeline.load(path)` do the same for a blueprint.

## How it connects to other concepts

- A job records applications of [Function Pods](function-pods.md) and
  [Operators](operators.md) to [Streams](streams.md), starting from [Sources](sources.md).
- What is reused and what is recomputed follows from [Identity & Hashing](identity.md).
- Files as pipeline inputs and outputs are covered in [File and Path Types](file-types.md).
