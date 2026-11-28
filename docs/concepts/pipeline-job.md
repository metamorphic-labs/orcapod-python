# Pipelines and Jobs

Applying a [function pod](function-pods.md) or an [operator](operators.md) to a
[stream](streams.md) directly computes in memory and keeps nothing. A **`PipelineJob`** runs
the same steps against a database: every function result is stored, and a later run computes
only what is not stored yet. Operator steps are not stored by default; they run again on
every run. A **`Pipeline`** is the description of the steps on its own, without
data or a database, from which several jobs can be made.

For a guided introduction see the tutorial, [Part 5](../tutorial/05-pipeline-job.md) and
[Part 6](../tutorial/06-reruns-and-caching.md). This page gives an overview of jobs and
blueprints. How a run behaves in detail (the run summary, failed rows, load-only runs, what
a second run runs, commits, saving and loading) is in the
[`PipelineJob` section of Function Pods](function-pods.md#pipelinejob-database-backed-cached-execution).

## A job: record, then run

```python
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource


@function_pod(output_keys="bmi")
def compute_bmi(weight: float, height: float) -> float:
    return weight / height**2


def mice(rows):
    return DictSource(data=rows, tag_columns=["subject_id"], tag_only_columns=True, source_id="mice")


cohort_a = [
    {"subject_id": "mouse_01", "weight": 25.3, "height": 0.12},
    {"subject_id": "mouse_02", "weight": 22.1, "height": 0.10},
]

database = InMemoryArrowDatabase()

job = PipelineJob(name="bmi", database=database)
with job:
    compute_bmi.pod(mice(cohort_a), label="bmi")
job.run()

print(job.nodes["bmi"].as_table().sort_by("subject_id").to_pylist())
# [{'subject_id': 'mouse_01', 'bmi': 1756.9444444444446}, {'subject_id': 'mouse_02', 'bmi': 2209.9999999999995}]
```

The three stages:

1. **Create** the job with a `name` and a `database`.
2. **Record** inside the `with` block. Pods and operators applied there are not executed;
   the job notes which step is applied to which stream. Sources may be created inside or
   outside the block.
3. **Run** with `job.run()`. The recorded steps execute in dependency order and each
   function result is written to the database. `run()` returns a `RunSummary` (also kept as
   `job.last_run`) that counts, per function node, the rows computed, read from the database
   and failed; see
   [Reading the run summary](function-pods.md#reading-the-run-summary).

Among `job.run()`'s optional arguments, the tutorial
[Handling errors](../tutorial/handling-errors.md) demonstrates `orchestrator=` (for example
`SyncPipelineOrchestrator(error_policy="fail_fast")` to stop at the first failing row; the
default policy skips failed rows and continues) and `observer=` (`StatusObserver`,
`LoggingObserver`) to record what happened during the run. Both observers raise
`SpiralDBObserverNotSupportedError` at setup if given a SpiralDB store, so give them
another database such as Delta (ITL-1540). An observer only watches: one
that changes the job, its nodes or its bound sources during a run is unsupported, and what
the run then does is undefined (ITL-1369). `load_only=True` fills the nodes
from what is already stored and computes nothing; see
[Loading without computing](function-pods.md#loading-without-computing-runload_onlytrue).

Use `await job.run_async()` or pass an `AsyncPipelineOrchestrator` to `job.run()` to
execute nodes concurrently. Each function pod defaults to a limit based on the usable
CPU count, clamped to 1–16 in-flight calls during an async run. The
[function pod concurrency settings](function-pods.md#limiting-in-flight-calls) show how
to choose a limit for a particular pod.

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
`as_pandas_df()` work on it, and show the rows of the job's **current run** in this Python
process. Read nodes after `run()`: before it, every node other than a source is empty
(`as_table()` returns no rows; `head()` and the notebook preview say the node has not run).
A run fills a node also when every result is read from the database and nothing is computed.

Three more things a job node offers:

- **`get_all_records()`** returns every record this step of the job has stored, including
  rows computed by earlier runs of the same job over the same source with other inputs.
  Pass `columns={"meta": True}` to include the record of each call (function name and
  version, time, Python version).
- **`as_source()`** returns a `DerivedSource` over the same stored records, whether or not
  the node has run in this process, to use as the input of another pipeline.
- **`upstreams`** and **`label`** let you walk the pipeline backwards.

As a stopgap pending ITL-1559, a job cannot absorb steps from another job by
capturing compiled nodes or other streams that depend on the other job's
bindable source slots (`SourceJobNode`). The same restriction applies to
standalone `SourceJobNode` objects, even when unbound. Pass the source itself to
record a new chain, or use `node.as_source()` to read the stored results of an
earlier job. A job can use its own slots while their original bindings are in
place. A schema-only `SourceNode` declares an independent slot for each job.

## The database

Any Orcapod database can hold a job's results. Two are used most:

- **`InMemoryArrowDatabase()`** keeps results for the lifetime of the Python process. Use it
  for experiments and tests.
- **`DeltaTableDatabase("some/directory")`** writes [Delta Lake](https://delta.io/) tables
  under a directory, and is what makes results survive between sessions. It keeps every
  type Orcapod uses: where Delta has no column type or would change it (an unsigned
  integer, a nanosecond timestamp, `float16`, a fixed-size list, a list's item name), the
  column's Arrow type is recorded and restored on read. It refuses, naming the column, the
  few types Delta cannot hold at all (`decimal256`, an interval), a map whose key or item
  field is not named `key` / `value`, and a new column holding only nulls with no type.

No database gives a column back as another type: each keeps a type exactly or refuses it
before anything of the table is written. [Data types per database](../reference/database-types.md)
lists every type for every database.

On disk the layout is `<directory>/<job name>/<function name>/...` for each function, plus
`<directory>/<job name>/_result/...`. The job's `name` is the top level, so jobs with
different names over one directory do not see each other's results, unless they are given
one result database (below). Deleting the directory,
or a job's subdirectory, is safe: the next run of a newly built job, for example in a new
Python session, recomputes what is missing. A job object that has already run holds its
run and computes nothing on a second `run()`.

A job created without a database can be recorded but not run. Give it one later with
`job.bind(database=...)`; `run()` without a database raises `ValueError`.

### Sharing function results between jobs

A function's results depend only on the function and its input values, not on the job
that computed them. Give several jobs one **result database** and they share them:
`PipelineJob(name=..., database=..., result_database=...)`. The function results are
stored in the result database as it is given, at `<function name>/...`, with no job name, so
a job computes only the inputs no job sharing it has stored. Pass a view, such as
`results.at("shared")`, to put them under a folder of your choice.

```python
results = InMemoryArrowDatabase()
shared_calls = []


@function_pod(output_keys="bmi")
def shared_bmi(weight: float, height: float) -> float:
    shared_calls.append(weight)
    return weight / height**2


for study_name in ["study_1", "study_2"]:
    study = PipelineJob(
        name=study_name, database=InMemoryArrowDatabase(), result_database=results
    )
    with study:
        shared_bmi.pod(mice(cohort_a), label="bmi")
    study.run()
    print(study_name, study.nodes["bmi"].as_table().num_rows, len(shared_calls))
# study_1 2 2
# study_2 2 2

print(sorted({path[0] for path in results.list_table_paths(())}))
# ['shared_bmi']
```

`study_2` computed nothing: both rows were in the result database. Without
`result_database` (the default) the results are kept in the job's own database at
`<job name>/_result/...`, as above. `job.bind(result_database=other)` changes it, and
`job.bind(result_database=None)` puts the results back in the job's own database.

Only function results are shared. What the job's nodes store stays the job's own, under
its database and name, whatever `result_database` is: each function node's pipeline records
(which rows the job read), an operator's cache and a side-effect pod's log. So a job still
reads only its own rows: two jobs over different sources share one result table, and each
job's nodes, their `as_source()` and a loaded job show the rows of that job's inputs only.
A `CachedSource` is not a node of the job: its cache is kept in the cache database and at
the path it was created with, whatever job reads it, so two jobs given one `CachedSource`
(or one cache database and path) share its cache.

Every run reads a function node's stored tables (its result table and its pipeline
records) as they are when the node starts: results that another job, or another handle on
the same database (a second `DeltaTableDatabase` over the same directory, another
process), committed before then are reused, not computed again. A read of what is stored
(`as_source()`, `get_all_records()`) likewise reads the tables as they are at that read.
For a `DeltaTableDatabase` each costs one read of each of the node's tables' logs: once
per node and run, or per read call, never per row. Each node looks at its own tables, so
two nodes of one job that share a result table each read its log once in a run.

Results committed **while** a node runs are another matter. The window opens when the node
starts and reads its tables, and closes at its next run or stored read, or when it
commits that table itself: a result another job commits in between is not seen by this
run, which computes that input again. For an ordinary function this costs only time
(both jobs store the same result). For a function that takes the invocation context, it
means **each of two jobs that run at the same time over the same input can perform the
action**: nothing reserves an input while it is being computed. Do not run two such jobs
over the same inputs at once.

A `DeltaTableDatabase` notices a newer version of a table, but not a table deleted and
recreated at the same path that happens to have the same version number: a handle that
had loaded the old table keeps reading it (ITL-1413). Open a new database object after
replacing a table.

A function that takes the invocation context (`ctx_arg`) performs an external action, and
its result table records which inputs it was performed for. Jobs that share a result
database share that record too: a job performs the action only for inputs no job sharing
the database has performed it for.

### Result commit ordering

A function node publishes its pipeline records after the referenced results
commit, also when the results are in a separate result database. Failed result batches
withdraw their pending pipeline records.
`NoOpArrowDatabase` explicitly discards results and still allows pipeline
bookkeeping to be written.

### Connector databases

A `ConnectorArrowDatabase` stores a column only when its connector can store it as written.
Each connector states which columns those are, and anything else is refused with
`UnstorableColumnError` before anything of the table is written.

The SQL connectors store a column only when they read it back with the same Arrow type (text
and binary columns come back as `large_string` and `large_binary`, the types Orcapod uses):

| Connector | Column types it stores |
|---|---|
| `SQLiteConnector` | `bool`, `int64`, `double`, `large_string`, `large_binary` |
| `PostgreSQLConnector` | `bool`, `int32`, `int64`, `float`, `double`, `large_string`, `large_binary`, `date32`, `timestamp[us]`, `timestamp[us, tz=UTC]` |

A non-null column is created `NOT NULL` and reads back non-null. SQLite refuses a
`double` column holding a NaN or `-0.0`, which it would read back as NULL and `0.0`. Any
other type (lists,
structs, dates or timestamps on SQLite, narrower integers and floats, logical types) is
refused. PostgreSQL also refuses a column name or a table name longer than
63 bytes, which it would truncate (`UnstorableTableNameError` for a table). A database on
`SpiralDBConnector` refuses every write for now (`SpiralDBWriteNotSupportedError`): Orcapod
reads SpiralDB only. The full list,
type by type, is [Data types per database](../reference/database-types.md).

Orcapod's own stored tables hold such columns. A function node's result table holds
list-valued executor columns and `__`-prefixed columns. A side-effect node's log holds a UTC
timestamp and `__data_context`. An operator cache holds `__data_context`, the row's entry id
and a UTC timestamp, the time the row was first stored (`__recorded_at`). A cached source
holds `__data_context` too. So a job refuses these nodes when it is built, or when a database
is attached to them, before anything runs:

- every function node, on SQLite and PostgreSQL;
- every side-effect node, on SQLite;
- every operator node with caching on (`CacheMode.LOG` or `REPLAY`), on SQLite: the SQLite
  connector has no column type for a timestamp (adding one is ITL-1482);
- every node that stores anything, on SpiralDB.

An operator cache or cached source on a SQL connector is also refused when it is written, before
anything is stored, if one of its own columns has a type or name the connector does not store. On
PostgreSQL that is every operator cache with caching on: its system-tag column names
(`_tag_source_id::` and a 64-character hash) are longer than 63 bytes.
A table's name is its Orcapod path joined by `__`, and every node table and cached source path
holds a 64-character hash, so on PostgreSQL only tables you write through `ConnectorArrowDatabase`
yourself under short paths fit; reading an existing table with `PostgreSQLTableSource` is
unaffected. Use
`InMemoryArrowDatabase` or `DeltaTableDatabase` for such jobs. Storing these columns on
connector databases is planned for a later release.

```python
from orcapod.databases import ConnectorArrowDatabase, SQLiteConnector
from orcapod.errors import UnstorableColumnError

sqlite_database = ConnectorArrowDatabase(SQLiteConnector(":memory:"))
sql_job = PipelineJob(name="bmi", database=sqlite_database)
try:
    with sql_job:
        compute_bmi.pod(mice(cohort_a), label="bmi")
except UnstorableColumnError as exc:
    print(exc)
# node 'bmi': a SQLiteConnector database cannot store column '__pf_exec_executor_info' of Arrow type large_list<...>: SQLite has no column type for it. Use an InMemoryArrowDatabase instead; docs/reference/database-types.md lists what each database keeps.
```

## What a re-run computes

A stored result is reused when the function's identity (its name, its output schema and its
**major** version), the input values of the row, and the job's name and database (or the
result database it shares, see above) are all unchanged. Which source, and which steps before the function, the values came through does
not matter.

```python
cohort_a.append({"subject_id": "mouse_03", "weight": 27.8, "height": 0.11})

calls = []


@function_pod(output_keys="bmi")
def compute_bmi(weight: float, height: float) -> float:
    calls.append(weight)
    return weight / height**2


job = PipelineJob(name="bmi", database=database)
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

Running the same job object again does even less. A node that already holds a current run
is not run again, so a second `job.run()` computes, reads and writes nothing:

```python
print(job.run())
# Run of job 'bmi': nothing to do; every node holds a current run.
#   bmi: 3 rows: held from the previous run
```

Each run first asks every source it reads whether its data changed (a `CSVSource` checks
its file, for example), and the nodes below a source that changed run again. An in-memory
source such as `DictSource` cannot see its data change: build a new source object and bind
it with `job.bind(sources=...)`. See
[Running again: what a run runs](function-pods.md#running-again-what-a-run-runs) and
[What a source does at a run](sources.md#what-a-source-does-at-a-run).

## `Pipeline`: a blueprint without data

A `Pipeline` records steps the same way, but has no database and never runs. It is a
**blueprint**. Its sources become named slots, and a job is made from it by filling them:

```python
from orcapod import Pipeline

pipeline = Pipeline("bmi")
with pipeline:
    compute_bmi.pod(mice(cohort_a), label="bmi")

cohort_b = [{"subject_id": "mouse_11", "weight": 24.0, "height": 0.11}]

job_a = PipelineJob.from_pipeline(pipeline, database=database, sources={"mice": mice(cohort_a)})
job_b = PipelineJob.from_pipeline(pipeline, database=database, sources={"mice": mice(cohort_b)})
job_a.run()
job_b.run()

print(job_a.nodes["bmi"].as_table().num_rows, job_b.nodes["bmi"].as_table().num_rows)
# 3 1
```

`sources` maps a source slot's name (the `source_id` used when the blueprint was recorded)
to the source to use. A source bound to a slot must have the slot's tag and data columns
with the same Arrow types (a column the slot declares nullable also accepts a non-null one);
any other source raises `SourceSpecMismatchError`, and an unknown slot name raises
`ValueError`. A slot made from a source keeps that source's Arrow types, so an `int32`
column refuses an `int64` source. A slot declared with Python types (`SourceNode(name,
tag_schema=Schema({...}), data_schema=Schema({...}))`) means one Arrow type per Python type
(`int` is `int64`, `datetime` is `timestamp[us, tz=UTC]`); to accept other widths, units or
zones, declare the slot with Arrow schemas (`tag_schema=pa.schema([...])`). Binding a source
never changes a node's schema or where its results are stored.

A job made without sources has **unbound** slots. It can still be run: nodes that depend on
an unbound source are skipped, and `job.unbound_sources` lists what is missing. Bind later
with `job.bind(...)`:

```python
job_c = PipelineJob.from_pipeline(pipeline, database=database)
print(job_c.unbound_sources)
# ['mice']

job_c.bind(sources={"mice": mice(cohort_b)})
print(job_c.unbound_sources)
# []
job_c.run()
print(job_c.nodes["bmi"].as_table().num_rows)
# 1
```

Every job also carries its blueprint as `job.pipeline`.

## Seeing the graph

!!! caution "In development"
    `Pipeline.diagram()`, `PipelineJob.diagram()`, `PipelineDiagram` and its methods and
    arguments, the look of the picture and the notebook display may change in any release
    without a deprecation period. The first diagram in a Python session issues
    `orcapod.errors.InDevelopmentWarning` once, as a reminder; to silence it, run
    `warnings.filterwarnings("ignore", category=InDevelopmentWarning)` after importing
    it from `orcapod.errors`.

In a notebook, a job or a pipeline shown at the end of a cell draws itself: a diagram of its
nodes (sources, operators, function pods, side-effect pods) and the streams between them,
laid out left to right. It is drawn as wide as the cell, at most 1.25 times its own size; a
diagram that would then be drawn below 0.8 times its size, where its labels get hard to
read, starts at 0.8 times from its left (or top) end, and a shaded edge with an arrow shows
that there is more: click it or drag to move along, or use `fit` to see all of it. Drag to
move it and use the `+`, `−` and `fit` buttons (or Ctrl or Cmd with the mouse wheel) to zoom; from the keyboard, focus the diagram and use the arrow keys
to move it, `+` and `-` to zoom and `0` to fit, and Tab to reach the nodes. Click a node (or focus it and press Enter) to see its
schema: its tag and data columns, the nodes it reads from and, for a job, its run status and
the row counts of the job's last run. `diagram()` makes the same diagram with options, and
`print` shows it as text:

```python
print(job_c.diagram())
# Job 'bmi': 2 nodes
#   mice (source · DictSource · bound)
#   bmi (function pod · compute_bmi · current) <- mice
print(pipeline.diagram())
# Pipeline 'bmi': 2 nodes
#   mice (source)
#   bmi (function pod · compute_bmi) <- mice
```

Each line is a node: its name, its kind, what it is when the name does not say it (the
function of a function pod, the class of a bound source), and for a job its status:

- `not run`: the node has not run in this Python session, or was cleared (`clear_cache()`).
- `current`: it holds the rows of its last run, and nothing it depends on has changed (also
  when the job's last run kept it without running it again; a function pod's details then
  count those rows as held).
- `stale`: something it depends on changed after its run (a source was bound again, a node
  above it was cleared); the next run computes it again.
- `failed`: its last run (the job's, or the node's own `run()`) had rows whose function
  raised; select the node to see the first error of the job's last run.
- `loaded only`: filled by `job.run(load_only=True)`.
- `partial`: it ran, but a node above it had failed rows, so it holds the results of the
  rows that did not fail; the next run computes it again.
- `bound` or `unbound`, for a source slot.

A selected node also shows the row counts of the job's last run (`job.last_run`).

A source bound with a label (`DictSource(..., label="mice")`, `SpiralDBTableSource(...,
label="units")`) is shown in a job's diagram, in its node's preview, and in
`RestrictedAggregationWarning` by that label; the diagram's details (`source id`) and the
warning give its full source id. `explain()` and `trace()` name it by its id. Two sources with one
label, or a label that is another node's name, are shown as `label (part)`: a SpiralDB
table's dataset, else its project, else the source's name. A single node's preview shows the label
alone. This is display only: the slot
keeps its name, the one `job.nodes[...]` and `bind` take. In a diagram, a source without a label is shown
by its slot's name, as is every source of a `Pipeline` (which binds none) and of a loaded
job (a source's label is not saved).

The options:

- `direction="TB"` lays the graph out top to bottom instead of left to right.
- `collapse=True` folds the steps that only choose, rename or join columns (joins, column
  selections, drops and maps, among them the join and select a function pod adds for you)
  into the edge they lie on: the edge is dashed, and the node it leads to lists them. A step
  is folded by what its operator is, whatever label it was given.
- `max_nodes` (400 by default): a larger graph is shown as the text listing instead.

In a notebook that runs scripts, two checkboxes next to the zoom buttons switch the layout
in place: `vertical` (top to bottom, or left to right when unticked) and `collapse` (which
appears only when there is something to fold). `direction=` and `collapse=` set the layout
shown first, and the one shown where scripts do not run. A selected node stays selected
when it is in the new layout. The checkboxes appear for graphs of up to 100 nodes: a larger
diagram carries only the layout its arguments ask for, which keeps a notebook small.

An input that only decides which rows of another input pass (the right input of a
`SemiJoin`) is drawn as a dashed edge that enters the box from the side (from above or below
when the graph runs left to right); the data input enters as every other edge does. The node's
details give the tags the rows match on, and the text form lists such an input after the
others: `<- spikes; constrained by units`. `collapse=True` never hides it: folded steps on its
way are drawn dash-dotted.

In each node, the kind line under the name (kind, what it is, status) is wrapped onto
short lines, so a node is about as wide as its name.

Drawing a diagram never computes anything. Like `head()`, it reads only what is already
known: each node's columns (which Orcapod knows without computing) and the run state the job
holds in memory. It runs no function, reads no source and no database. It needs no Graphviz
and nothing from the network: the picture is part of the cell's output, so it stays in a saved
notebook and in an HTML export. Where a notebook does not run scripts (an untrusted notebook,
a preview on a website), the picture is still shown, without pan, zoom or selection; open
the **Nodes** table below the picture for each node's columns and status.
`job.diagram().save("pipeline.html")` writes it as a page that opens in any browser.

## Saving and loading

`job.save(path)` writes the job to a JSON file: the graph, the database's location (and the
result database's, when one was given) and how each source was configured. `PipelineJob.load(path)` reads it back, for example in a later
Python session. An `InMemoryArrowDatabase` has no location to record, so here the database
is passed again; `PipelineJob.load(path, database=...)` is also how you point a loaded job
at a database that has moved, and `PipelineJob.load(path, result_database=...)` at a result
database. A connector database (SQLite, PostgreSQL, SpiralDB) is never rebuilt from the
file: pass it, built over your connector, or the load raises an error that names what to
pass. Saving a loaded job writes the database actually bound, the one you passed, in place of
the recorded one. The recorded one is kept only while nothing is bound in its slot, which
happens when a database of another kind could not be rebuilt and none was passed. The file records each node's own Arrow types, so saving types
every node from its inputs' types, as previewing an empty job does, and costs what that
preview costs: a pipeline whose joins keep many system-tag columns saves as slowly as it
previews.

```python
job_a.save("bmi_job.json")

loaded = PipelineJob.load("bmi_job.json", database=database)
print(loaded.nodes["bmi"].as_table().num_rows)  # not run in this process: empty
# 0
print(loaded.nodes["bmi"].get_all_records().num_rows)
# 4
```

A loaded job is for **reading stored results**. The file holds the names of your functions,
not their code, so a loaded job cannot compute:

- Its nodes have not run in this process, so they are empty. `get_all_records()` and
  `as_source()` read what the database holds for the node: here the three rows of cohort A
  and the one of cohort B, because both cohorts were read through a source named `"mice"`.
- `loaded.run()` raises `RuntimeError`. To compute, run the code that defines the pipeline.
- `loaded.run(load_only=True)` fills the nodes with the stored results for the sources'
  current rows, computing nothing. An in-memory source such as `DictSource` is not saved
  with the job, so bind it first:

```python
try:
    loaded.run()
except RuntimeError:
    print("a loaded job cannot compute")
# a loaded job cannot compute

loaded.bind(sources={"mice": mice(cohort_a)})
summary = loaded.run(load_only=True)
print(loaded.nodes["bmi"].as_table().num_rows, summary.nodes["bmi"].not_stored)
# 3 0
```

As a stopgap (ITL-1560), a loaded job and its cached `job.pipeline` blueprint raise
`RuntimeError` if you try to capture steps into either or call `compile()`.
Build the job from its pipeline to change the graph.
Binding sources and running with `load_only=True` remain supported.

The full rules (which sources are rebuilt from the file, operators, side effects, a
database that holds other rows than the file expects) are in
[Saving and loading a job](function-pods.md#saving-and-loading-a-job).

`Pipeline.save(path)` and `Pipeline.load(path)` save and load a blueprint.

The file lists each operator's inputs in the order they were given, so a loaded
`MergeJoin(left, right)` or `SemiJoin(left, right)` takes `left` first, as the live job did.
A file saved by an Orcapod older than this rule (before ITL-1417) cannot say which input
comes first, so loading it raises `ValueError` when it holds an operator over two or more
inputs that is not known to be commutative (`MergeJoin`, `SemiJoin`, or an operator that
cannot be rebuilt in this process); build the job from its code and save it again. A file
whose multi-input operators are all commutative (`Join`, `Rejoin`) loads as before.

## How it connects to other concepts

- A job records applications of [Function Pods](function-pods.md) and
  [Operators](operators.md) to [Streams](streams.md), starting from [Sources](sources.md).
- What is reused and what is recomputed follows from [Identity & Hashing](identity.md).
- Files as pipeline inputs and outputs are covered in [File and Path Types](file-types.md).
