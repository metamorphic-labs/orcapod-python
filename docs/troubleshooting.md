# Troubleshooting

Symptoms a newcomer is likely to meet, each with its cause and what to do. The first table
lists messages; the second lists problems that produce **no** message, which are the ones
that cost the most time. Every entry was observed on the current code.

If your problem is not here, print `stream.keys()` and `stream.output_schema()` for the
stream involved. Most problems come down to a column that is not where, or what, you think
it is.

## Errors when building a pipeline

These are raised at the line that makes the mistake, before any data is processed.

### `ValueError: tag_columns not found in table: {...}. Available columns: [...]`

**Cause.** A name in `tag_columns` is not a column of the data.

**Fix.** Compare with the "Available columns" list in the message. Usually a typo.

The same message appears when a source is given **no rows**: with an empty table there are
no columns at all, so every tag column is reported missing
(`Available columns: []`). Check that your data is not empty.

### `ValueError: No data columns found in the table. At least one data column is required.`

**Cause.** Every column was named as a tag.

**Fix.** Leave at least one column out of `tag_columns`. A stream needs something to carry.

### `TypeError: 'PosixPath' object is not iterable` from `CSVSource`

**Cause.** `CSVSource` was given a `pathlib.Path`. It currently accepts the file path only
as a string.

**Fix.** `CSVSource(str(path), ...)`.

### `ValueError: Parameter 'x' has no type annotation and is not specified in input_types.`

**Cause.** A function passed to `@function_pod` has a parameter without a type annotation.
Orcapod builds the function's input schema from the annotations.

**Fix.** Annotate every parameter and the return value: `def f(x: int) -> float:`.
Built-in functions have no annotations; wrap them in a small function of your own.

### `ValueError: Type for return item '...' is not specified in output_types and has no type annotation in function signature.`

**Cause.** The function has no return annotation.

**Fix.** Add one: `-> float`, or `-> tuple[float, bool]` for several outputs.

### `ValueError: Incoming data data type Schema({...}) is not compatible with expected input schema Schema({...})`

Often preceded by a line such as `Key 'age' not found in parameter types.` or
`Type mismatch for key 'age': ...`.

**Cause.** The data columns of the stream are not exactly the parameters of the function. A
function pod does not pick the columns it needs out of a wider stream. The first schema in
the message is what the stream has; the second is what the function takes. The stream has
an extra column, lacks one, or has one of a different type (`int` where the function says
`float`, for example).

**Fix.** Narrow the stream first:
`stream.select_data_columns(["a", "b"])` or `stream.drop_data_columns([...])`. For a type
mismatch, change the annotation to the column's type. For a name mismatch, rename with
`stream.map_data({"old": "new"}, drop_unmapped=False)`.

### `ValueError: Multiple return keys were specified but return type annotation <class 'float'> is not a sequence type`

**Cause.** `output_keys` is a list of several names, but the function is annotated as
returning a single value.

**Fix.** Return a tuple and annotate it: `-> tuple[float, bool]`. If you get
`ValueError: Values returned by function must be sequence-like if multiple output keys are
specified` at run time, the annotation says tuple but the function returned one value.

### `InputValidationError: Input streams are not compatible: Datas should not have overlapping keys ...`

**Cause.** `join` was given streams that both have a data column of the same name.

**Fix.** Drop the column from one side, rename one side
(`map_data({"n": "n_left"}, drop_unmapped=False)`), or use `MergeJoin` if the two columns
are measurements of the same thing and should be merged into a list.

### `InputValidationError: GroupBy: [...] are not tag columns of the input stream.`

**Cause.** `group_by` was given a data column or an unknown name. Only tag columns can be
grouping keys.

**Fix.** Group by tags. If the column you want to group by is data, it should have been a
tag: name it in `tag_columns` when you create the source. If the message says the columns
are *list-valued*, the stream came from `batch`; group before batching.

### `ValueError: PipelineJob.run() requires a store.`

**Cause.** The job was created without `store=`.

**Fix.** `PipelineJob(name=..., store=InMemoryArrowDatabase())`, or
`job.bind(store=...)` before `run()`.

### `AttributeError: 'PipelineJob' has no attribute 'show_graph'`

**Cause.** `show_graph` is a method of the job's pipeline.

**Fix.** `job.pipeline.show_graph()`. It needs the Graphviz `dot` program; without it you
get `ExecutableNotFound`.

### `AttributeError: module 'orcapod' has no attribute 'enable_file_hash_caching'`

**Fix.** `from orcapod.contexts import enable_file_hash_caching`.

### `FileNotFoundError: File: path does not exist` / `IsADirectoryError: File: path is a directory`

**Cause.** `op.File(path)` checks the path when it is created.

**Fix.** Correct the path, or use `op.Directory` for a folder. If this appears when
*reading results*, a function returned an `op.File` whose file has since been deleted.

## Errors from known bugs

These are defects in the library, not mistakes in your code. Each has a spelling that works
today.

### `TypeError: SemanticAwarePythonHasher (strict): no implementation of PythonTypeHandlerProtocol registered for type 'polars.expr.expr.Expr' ...`

**Cause.** A `polars_filter` built from a Polars expression (`pl.col("x") > 1`) cannot be
identified, so it fails when the filtered stream is joined or used anywhere inside a
`PipelineJob`.

**What works.** Equality filters, `stream.polars_filter(column=value)`, work everywhere.
An expression filter works as the very last step, when you only look at its result. Inside
a pipeline, move a threshold decision into a function pod, or filter the final table with
Polars or pandas after taking it out with `as_polars_df()` / `as_pandas_df()`.

### `ArrowInvalid: Incompatible data types for corresponding join field keys: ... of type string and ... of type large_string`

**Cause.** `semi_join` between a stream from a `CSVSource` and a stream from a `DictSource`,
or one that passed through `polars_filter`. The two store text in different Arrow types and
`semi_join` does not reconcile them.

**What works.** `semi_join` between two CSV sources. Otherwise use `join`, which handles the
mixture, followed by a filter or `select_data_columns`.

### `TypeError: ...() got an unexpected keyword argument 'ctx'` from a `sink_pod`

**Cause.** A function decorated with `@sink_pod`, `@tap_pod` or `@side_effect_pod` must
accept the context parameter. It is checked when the job runs, not when the function is
decorated.

**Fix.** Add `ctx: InvocationContext` as the last parameter
(`from orcapod import InvocationContext`).

### `RuntimeError: FunctionJobNode '...' is unavailable: no function pod and no database attached`

**Cause.** `as_table()` (or `iter_data()`) was called on a function node of a job read with
`PipelineJob.load()`. A loaded job has no function code.

**What works.** `loaded.nodes[label].get_all_records()`. To compute, run the script that
defines the pipeline.

## Problems that give no error

### I edited my function and the results did not change

**Cause.** Stored results are looked up by the function's name, its output schema and its
**major** version, not by its source code. Editing the body changes none of these, so the
old results are returned.

**Fix.** Bump the major version: `@function_pod(output_keys="x", version="v1.0")`. The
default is `"v0.0"`, and a minor bump (`"v0.1"`) is not enough. Renaming the function also
works. See the tutorial, [Part 6](tutorial/06-reruns-and-caching.md).

### A node's table is empty

**Cause.** The job has not been run. Before `job.run()`, a function node has no results and
returns an empty table.

**Fix.** Call `job.run()`, then read `job.nodes[label]`.

### The job finished, but rows are missing from the result

**Cause.** A function raised for those rows. By default a job skips a failing row, logs the
traceback, and carries on; `run()` does not raise. Steps downstream compute on the rows
that are left.

**Fix.** While developing, run with
`job.run(orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"))` (from
`orcapod.pipeline`) so that the first failure stops the run. To find the failed rows of a
finished run, pass a `StatusObserver`. See the tutorial,
[Part 9](tutorial/09-when-things-go-wrong.md).

### My file changed but nothing was recomputed

**Cause.** The column holds `pathlib.Path` values. A `Path` is identified by the path
string, not by the file's content.

**Fix.** Use `op.File(path)` in the source and `op.File` as the parameter type. See
[File and Path Types](concepts/file-types.md).

### Everything is recomputed although nothing changed

Check, in this order:

- **The job's `name`.** Results are stored under it. A different name is a different,
  empty place.
- **The store.** A new `InMemoryArrowDatabase()` is empty. So is a `DeltaTableDatabase`
  pointing at a different directory; a relative directory depends on where the script is
  run from.
- **The function's identity.** A renamed function, a renamed output column or a different
  major version starts from nothing. `FunctionPod.from_fn` defaults to `version="v1.0"`
  while `@function_pod` defaults to `"v0.0"`.
- **The pipeline's structure.** Adding, removing or reordering a step upstream of a function
  gives that function a new place in the store.

### `map_data` / `map_tags` dropped my other columns

**Cause.** The `stream.map_data(...)` and `stream.map_tags(...)` methods default to
`drop_unmapped=True`: columns you did not rename are removed. (The operator classes
`MapData` and `MapTags` default to keeping them.)

**Fix.** Pass `drop_unmapped=False`.

### A function ran fewer times than there are rows

**Not a problem.** Results are stored per function and per input values. Rows whose inputs
are identical share one computation.

### The rows of a result come in a different order than before

**Not a problem, but be aware of it.** Rows read back from a store are not guaranteed to be
in the order they were first computed in. Sort when the order matters:
`df.sort_values([...tag columns...])`.

### An output file is missing although the pipeline "succeeded"

**Cause.** A function that writes a file stores the file's path as its result. If the file
is deleted later, a re-run finds the stored path, does not call the function, and returns a
path to nothing.

**Fix.** Delete output files and the store together, or bump the function's major version
to regenerate them.

## Messages you can ignore

- `UserWarning: Protobuf gencode version ... is exactly one major version older than the
  runtime version ...` when Orcapod is imported.
- `WARN deltalake_core::writer::stats] Skipping column ... because it's a binary field.`
  whenever a `DeltaTableDatabase` is written to.

## Installation

- **`pygraphviz` fails to build** (`fatal error: 'graphviz/cgraph.h' file not found`). You
  are installing a version of Orcapod older than 0.2, which required the Graphviz
  development headers. Install Orcapod 0.2 or newer.
- **`ExecutableNotFound: failed to execute 'dot'`** when calling `show_graph()`. Install
  Graphviz (`brew install graphviz`, `apt install graphviz`, or `conda install graphviz`),
  or do without the picture.
- **Python version.** Orcapod needs Python 3.11 or newer.
