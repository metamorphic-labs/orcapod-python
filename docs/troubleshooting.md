# Troubleshooting

Symptoms a newcomer is likely to meet, each with its cause and what to do. The first sections
list messages; [Problems that give no error](#problems-that-give-no-error) lists the ones that
produce **no** message, which cost the most time.

If your problem is not here, print `stream.keys()` and `stream.output_schema()` for the
stream involved. Most problems come down to a column that is not where, or what, you think
it is.

## Errors when building a pipeline

These are raised at the line that makes the mistake, before any data is processed.

### `ValueError: tag_columns not found in table: {...}. Available columns: [...]`

**Cause.** A name in `tag_columns` is not a column of the data.

**Fix.** Compare with the "Available columns" list in the message. Usually a typo.

### `ValueError: Table is empty: the input has no columns, so its tag and data columns cannot be found.`

**Cause.** The source was given **no rows**, for example `DictSource(data=[], ...)`. Without
rows there are no columns to find.

**Fix.** Check that your data is not empty. To build a source with no rows on purpose, give
it an explicit schema (for example `data_schema=` on `DictSource`).

### `ValueError: No data columns found in the table. At least one data column is required.`

**Cause.** Every column was named as a tag.

**Fix.** Leave at least one column out of `tag_columns`. A stream needs something to carry.

### `InputValidationError: dictionary-encoded columns are not currently supported in Orcapod: - tag column '...' has type dictionary<...> (decoded: ...)`

**Cause.** A column, tag or data, is dictionary-encoded, or holds a dictionary inside it (for
example a list of dictionary values). This comes from an Arrow table built with
`dictionary_encode()`, a pandas `Categorical` of strings, or a polars `Categorical` or `Enum`.
Orcapod does not support such columns yet; the message names every one, with its role and the
type it decodes to.

**Fix.** Decode the column before building the source.

With pyarrow, keep the field's name, nullability and metadata, and keep the table that
`set_column` returns:

- Take the field: `field = table.schema.field(name)`.
- Take the decoded type: `decoded = field.type.value_type`. For a column that holds a
  dictionary inside it (a struct with a dictionary field, a list of dictionary values), use
  the decoded type the message gives instead, for example
  `pa.struct([("k", pa.string()), ("n", pa.int64())])`.
- Replace the column:
  `table = table.set_column(table.schema.get_field_index(name), field.with_type(decoded), table[name].cast(decoded))`.

Three kinds of column cannot be cast to their decoded type at once, so they need a step
first (each recipe is tested in `tests/test_core/sources/test_dictionary_columns_refused.py`):

- A dictionary whose values hold a dictionary: decode the top level of each chunk first,
  `pa.chunked_array([chunk.dictionary_decode() for chunk in column.chunks]).cast(decoded)`.
- A list view: cast it to a large list of the decoded items,
  `column.cast(pa.large_list(<decoded item type>))` (a source refuses list views whatever they
  hold).
- A run-end-encoded column: cast its values, then expand its runs, chunk by chunk:
  `pc.run_end_decode(pa.RunEndEncodedArray.from_arrays(chunk.run_ends, chunk.values.cast(<decoded value type>)))`.

With polars, keep the frame `with_columns` returns:
`df = df.with_columns(pl.col(name).cast(pl.String))`. Or keep the data frame as a value in a cell:
a pandas or polars `DataFrame` stored whole in one column is a library value, not a dictionary
column. See [Arrow types of a source](concepts/sources.md#arrow-types-of-a-source).

### `TypeError: 'PosixPath' object is not iterable` from `CSVSource`

**Cause.** `CSVSource` was given a `pathlib.Path` and no `source_id`. The file path is
typed as a string; without a `source_id` it is used to build the default one, which fails
for a `Path`. This is a known defect.

**Fix.** `CSVSource(str(path), ...)`.

### `ValueError: Parameter 'x' has no type annotation and is not specified in input_types.`

**Cause.** A function passed to `@function_pod` has a parameter without a type annotation.
Orcapod builds the function's input schema from the annotations.

**Fix.** Annotate every parameter and the return value: `def f(x: int) -> float:`.
Built-in functions have no annotations; wrap them in a small function of your own.

### `ValueError: Type for return item '...' is not specified in output_types and has no type annotation in function signature.`

**Cause.** The function has no return annotation.

**Fix.** Add one: `-> float`, or `-> tuple[float, bool]` for several outputs.

### `ValueError: Multiple return keys were specified but return type annotation <class 'float'> is not a sequence type (list, tuple, Collection).`

**Cause.** `output_keys` is a list of several names, but the function is annotated as
returning a single value.

**Fix.** Return a tuple and annotate it: `-> tuple[float, bool]`. If you get
`ValueError: Values returned by function must be sequence-like if multiple output keys are
specified` when the function runs, the annotation says tuple but the function returned one
value.

### `ValueError: Incoming data schema Schema({...}) is not compatible with expected input schema Schema({...}): ...`

**Cause.** The data columns of the stream do not fit the parameters of the function. The
first schema is what the stream offers; the second is what the function takes. The end of
the message names the first offending column:

- `required parameter 'm' has no matching column`: the stream has no column for a
  parameter that has no default.
- `column 'n' has type int, the function expects float`: a column has a different type.
  A common case is `int | None` where the function says `int`: a source built from a
  `pa.table(...)` without a schema has nullable columns.
- `column 'q' is not a parameter of the function`: the stream has a column the function
  does not take. `pod(stream)` drops such columns for you (it adds a `SelectDataColumns`
  step), so you see this only from `pod.process(stream)`, from `pod(stream, strict=True)`,
  or when **no** column of the stream is a parameter.

**Fix.** For a missing column, rename with `stream.map_data({"old": "new"})`. For a type
mismatch, change the annotation to the column's type (`n: int | None`), or narrow a nullable
column with [`DropNulls`](concepts/operators.md#dropnulls), which drops the null rows.

### `InputValidationError: Input streams are not compatible: Datas should not have overlapping keys ...`

**Cause.** `join` was given streams that both have a data column of the same name.

**Fix.** Drop the column from one side, rename one side (`map_data({"n": "n_left"})`), or
use `MergeJoin` if the two columns are measurements of the same thing and should be merged
into a list.

### `InputValidationError: GroupBy: [...] are not tag columns of the input stream.`

**Cause.** `group_by` was given a data column or an unknown name. Only tag columns can be
grouping keys.

**Fix.** Group by tags. If the column you want to group by is data, it should have been a
tag: name it in `tag_columns` when you create the source. If the message says the columns
are *list-valued tag columns*, the stream came from `batch`; group before batching.

### `ValueError: Cannot hash the Polars expression '...' for an Orcapod identity`

**Cause.** A `polars_filter` predicate calls a Python function (`map_elements`,
`map_batches`) or uses another expression Orcapod cannot identify. The message lists what it
can. Ordinary comparisons such as `pl.col("x") > 1` work everywhere.

**Fix.** Use keyword constraints (`polars_filter(column=value)`) for equality filters, or
compute a boolean column in a function pod and filter on it. See
[PolarsFilter](concepts/operators.md#polarsfilter).

### `AttributeError: 'PipelineJob' has no attribute 'show_graph'. Available node labels: [...]`

**Cause.** `show_graph()` (and `render_graph()`, the Graphviz renderer) was removed,
with Orcapod's dependencies on Graphviz and matplotlib. A `Pipeline` gives the same error,
naming `'Pipeline'`.

**Fix.** Show `job` (or `job.diagram()`) at the end of a notebook cell, `print(job.diagram())`
for a text listing, or `job.diagram().save("pipeline.html")` for a page; the same works for a
`Pipeline`. See [Seeing the graph](concepts/pipeline-job.md#seeing-the-graph).

### `FileNotFoundError: File: path does not exist: ...` / `IsADirectoryError: File: path is a directory: ...`

**Cause.** `op.File(path)` checks the path when it is created.

**Fix.** Correct the path, or use `op.Directory` for a folder.

## Errors when running a job

### `ValueError: PipelineJob.run() requires a database.`

**Cause.** The job was created without `database=`.

**Fix.** `PipelineJob(name=..., database=InMemoryArrowDatabase())`, or
`job.bind(database=...)` before `run()`.

### `RowFailuresWarning: N of M rows failed in node '...' (first error: ...)`

**Cause.** A function raised for some rows. By default a job skips a failing row and carries
on; `run()` does not raise. Steps downstream compute on the rows that are left, so the
results have fewer rows than you expect. The run summary that `job.run()` returns says the
same (`Run of job '...': 1 row failed.`).

**Fix.** The message says how to see the full traceback:
`job.last_run.print_traceback("label")`. While developing, run with
`job.run(orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"))` (from
`orcapod.pipeline`) so that the first failure stops the run and raises. See the tutorial,
[Part 9](tutorial/09-when-things-go-wrong.md).

### `TypeError: ...() got an unexpected keyword argument 'ctx'` from a `sink_pod`

**Cause.** A function decorated with `@sink_pod`, `@tap_pod` or `@side_effect_pod` must
accept the invocation context as a parameter named `ctx`. It is checked when the job runs,
not when the function is decorated. This is a known defect.

**Fix.** Add a `ctx` parameter: `def show(ctx, n: int) -> None:`. It receives an
`InvocationContext` (`from orcapod import InvocationContext`); ignore it if you do not need
it.

### `RuntimeError: PipelineJob.run() cannot execute a loaded job: its function pods are not saved with it, so it cannot compute.`

**Cause.** `run()` was called on a job read with `PipelineJob.load()`. The saved file holds
the names of your functions, not their code.

**Fix.** To read the stored results, use `loaded.nodes[label].as_source()`, or
`loaded.run(load_only=True)` to fill the nodes from the database (bind in-memory sources
first). To compute, run the script that defines the pipeline.

## Problems that give no error

### I edited my function and the results did not change

**Cause.** Stored results are looked up by the function's name, its output schema and its
**major** version, not by its source code. Editing the body changes none of these, so the
old results are returned.

**Fix.** Bump the major version: `@function_pod(output_keys="x", version="v1.0")`. The
default is `"v0.0"`, and a minor bump (`"v0.1"`) is not enough. Renaming the function also
works. See the tutorial, [Part 6](tutorial/06-reruns-and-caching.md).

### A node's table is empty

**Cause.** A job node shows the results of its current run, and it has none yet:

- The job has not been run in this session. Before `job.run()`, a function node reads
  empty, even when the database holds results from an earlier run. A preview of the node
  (`node.head()`, or the node shown in a notebook) says `not run`.
- The job was read with `PipelineJob.load()`. A loaded job's nodes read empty until
  `loaded.run(load_only=True)`.
- A source of the job is not bound. `job.unbound_sources` lists such sources; the nodes
  below them do not run.

**Fix.** Call `job.run()`, then read `job.nodes[label]`. To see everything the database
holds for a node, whatever run it came from, use `job.nodes[label].as_source()`.

### My file changed but nothing was recomputed

**Cause.** The column holds `pathlib.Path` values. A `Path` is identified by the path
string, not by the file's content.

**Fix.** Use `op.File(path)` in the source and `op.File` as the parameter type. See
[File and Path Types](concepts/file-types.md).

### Everything is recomputed although nothing changed

Check, in this order:

- **The job's `name`.** Results are stored under it. A different name is a different,
  empty place.
- **The database.** A new `InMemoryArrowDatabase()` is empty. So is a `DeltaTableDatabase`
  pointing at a different directory; a relative directory depends on where the script is
  run from.
- **The function's identity.** A renamed function, a renamed output column or a different
  major version starts from nothing.
- **The input values.** Results are filed by function and by the values the function
  receives, not by the steps upstream of it. Adding or reordering an upstream step does not
  recompute a function by itself; only rows whose input values changed are computed again.

### A function ran fewer times than there are rows

**Not a problem.** Results are stored per function and per input values. Rows whose inputs
are identical share one computation.

### The rows of a result come in a different order than before

**Not a problem, but be aware of it.** Rows read back from a database are not guaranteed to
be in the order they were first computed in. Sort when the order matters:
`df.sort_values([...tag columns...])`.

### An output file is missing although the pipeline "succeeded"

**Cause.** A function that writes a file stores the file's path as its result. If the file
is deleted later, a re-run finds the stored result, does not call the function, and returns
a path to nothing.

**Fix.** Delete output files and the database together, or bump the function's major
version to regenerate them.

## Messages you can ignore

- `WARN deltalake_core::writer::stats] Skipping column ... because it's a binary field.`
  whenever a `DeltaTableDatabase` writes to a table that an earlier version of Orcapod
  created: one line per binary column (record ids and hashes) per commit. The deltalake
  library keeps no minimum and maximum statistics for binary columns and says so on every
  write; nothing is wrong with the table. A table that `DeltaTableDatabase` creates now names
  the columns it keeps statistics for (the `delta.dataSkippingStatsColumns` table property),
  and writes to it log nothing. Orcapod never changes the property of an existing table. To
  hide only this line for an older table, set `RUST_LOG` in the environment before Python
  starts, keeping deltalake's other warnings:

  ```
  RUST_LOG="warn,deltalake_core::writer::stats=error" python my_pipeline.py
  ```

  deltalake reads `RUST_LOG` once, when it is first imported, so setting it later in the
  same process (for example with `os.environ`) has no effect.

## A script finishes its work but the process never exits

**Symptom.** A Python script that used a Delta database prints all of its output and then hangs,
and has to be killed. On Linux with Python 3.11 or 3.12 the process may instead end with
`terminate called without an active exception` and exit code 134 (SIGABRT). Notebooks are not
affected in the same way, because the kernel keeps running.

**Cause.** This is a bug in pyarrow, not in your pipeline
([apache/arrow#50482](https://github.com/apache/arrow/issues/50482),
[apache/arrow#54536](https://github.com/apache/arrow/issues/54536)). deltalake's default reader
goes through a filesystem written in Python. After a read returns, Arrow finishes cleaning it up
on a worker thread. If that cleanup runs while the interpreter is shutting down, the worker
cannot take the GIL and never finishes, and Arrow's thread pool waits for it forever. Your data
is not affected: the hang happens after every write has completed.

**What Orcapod does.** `DeltaTableDatabase` and `DeltaTableSource` read **local** Delta tables
through a native pyarrow filesystem, so they are not affected. Tables on cloud storage
(`s3://`, `gs://`, `az://` and similar) still use deltalake's reader and can still hang at exit.
Reading a Delta table yourself with `deltalake.DeltaTable(...).to_pyarrow_table()` or
`to_pyarrow_dataset()` is affected too, whatever the storage.

**Workarounds.**

- Upgrade pyarrow once a release includes the fix
  ([apache/arrow#54467](https://github.com/apache/arrow/pull/54467)).
- In a script that reads a cloud Delta database, end it by flushing your outputs and calling
  `os._exit(0)`. That skips interpreter shutdown, where the hang happens. It also skips
  `atexit` handlers and other cleanup, so close files and flush databases first.
- When reading local Delta tables directly with deltalake, pass a native filesystem. Resolve
  `dt.table_uri` first: it is percent-encoded, and newer deltalake versions return a
  `file://` URI.

  For example, with a small local Delta table opened as `dt`:

  ```python
  import deltalake
  import pyarrow
  import pyarrow.fs

  deltalake.write_deltalake("mice", pyarrow.table({"subject_id": ["mouse_01", "mouse_02"]}))
  dt = deltalake.DeltaTable("mice")
  ```

  ```python
  uri = dt.table_uri if "://" in dt.table_uri else "file://" + dt.table_uri
  fs, root = pyarrow.fs.FileSystem.from_uri(uri)
  dataset = dt.to_pyarrow_dataset(filesystem=pyarrow.fs.SubTreeFileSystem(root, fs))
  print(dataset.to_table().sort_by("subject_id").to_pylist())
  # [{'subject_id': 'mouse_01'}, {'subject_id': 'mouse_02'}]
  ```

## Installation

- **`pygraphviz` fails to build** (`fatal error: 'graphviz/cgraph.h' file not found`). You
  are installing a version of Orcapod older than 0.2, which required the Graphviz
  development headers. Install Orcapod 0.2 or newer. Until 0.4.0 is released, a plain
  `uv add orcapod` installs 0.1.4 (0.3.0 was withdrawn): install the development version,
  `uv add git+https://github.com/metamorphic-labs/orcapod-python`.
- **Python version.** Orcapod needs Python 3.11 or newer.
- **A release candidate is not installed.** `uv add orcapod` (like `pip install orcapod`)
  takes the latest stable release and skips pre-releases. Name the release candidate:
  `uv add "orcapod==0.4.0rc1"`.
