# Data Function Types

A function pod runs its function through a **data function type**: today the only one is
the Python function type, `python.function.v1`. The type says what a function of the type
means and how it is run, and what its result table stores about each row. This page states
both, for the type at its current major and revision, and how a change to either is
recorded (ITL-1259).

## The result table path

A function stores each result in its result table, at

```text
<job>/_result/<function>/<output schema hash>/v<function major>/python.function.v1/rev1/rdb_v1
```

For a job given a separate result database (`PipelineJob(..., result_database=...)`), the
path starts at `<function>`, in that database, with no `<job>/_result`.

The first four components after `_result` are the function's uri. The last three each
have one owner:

| Component | What it controls | Owner | In a hash |
|---|---|---|---|
| `python.function.v1` | The type and its **major**: what a function of the type means and how it is run | The data function type | Yes: the last part of every function's uri, so of every pod and node hash |
| `rev1` | The **revision** of the type's own record: the columns it stores about the function and its execution (`pf_var_*`, `pf_exec_*`) | The data function type | No: the path only |
| `rdb_v1` | The **layout** of the columns every result table has, whatever its type | The result cache, for all types | No: the path only |

The test between a new major and the other two: **is a row stored before the change still a
correct result for the same input?** If not, the change is a new major: the function's
identity moves and every result is computed again. If it is, the change is a new revision
(when it changes what the type records) or a new `rdb` version (when it changes the common
columns): a new result table, and no hash moves.

The function's pipeline table carries no revision:

```text
<job>/<function>/<output schema hash>/v<function major>/python.function.v1/schema:<pipeline hash>/pdb_v2
```

A saved job names the type and its major only (`"data_function_type_id":
"python.function.v1"`); the revision is the one the installed version of Orcapod declares.

```python
from orcapod import PipelineJob, function_pod
from orcapod.core.data_function import PYTHON_FUNCTION
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource


@function_pod(output_keys=["total", "label"])
def describe_pair(x: int, y: int) -> tuple[int, str]:
    return x + y, f"{x}+{y}"


types_store = InMemoryArrowDatabase()
types_job = PipelineJob(name="types_demo", database=types_store)
with types_job:
    pairs = DictSource(
        data=[{"k": "a", "x": 1, "y": 2}], tag_columns=["k"], tag_only_columns=True
    )
    _ = describe_pair.pod(pairs, label="describe")
types_job.run()

print(PYTHON_FUNCTION.type_id, PYTHON_FUNCTION.revision_component)
# python.function.v1 rev1
print(describe_pair.pod.uri[-1])
# python.function.v1
for path in sorted(types_store.list_sources()):
    print(path[-4:])
# ('v0', 'python.function.v1', 'rev1', 'rdb_v1')
# ('v0', 'python.function.v1', 'schema:...', 'pdb_v2')
```

## The Python function type, `python.function.v1`

### Identity

A function's uri has four parts, in order:

1. the function's name: its `__name__`, or the `function_name` given;
2. the hash of the output schema: each output key with its type, in no order;
3. `v<major>` of the function's own version (the part before the first dot);
4. the type and its major: `python.function.v1`.

### How a function is run (what the major stands for)

- Inputs are bound to the function's parameters by name, as keyword arguments.
- A parameter with a default may be left out of the inputs.
- The returned value is mapped to the output keys by position: one key takes the whole
  value, several keys take the items of a returned sequence.
- A function with no output keys (`-> None`, or no return annotation) has no outputs; a
  returned value other than `None` is refused.
- A returned value must conform to its declared output type.
- Variadic (`*args`, `**kwargs`) and positional-only parameters are refused.

### Its record, revision 1

Each result row holds these columns about the function (`__pf_var_` prefix):

| Column | Python type | Arrow type | Nullable | Meaning |
|---|---|---|---|---|
| `function_name` | `str` | `large_string` | yes | the function's name, as in the uri |
| `function_version` | `str` | `large_string` | yes | the full version string the function was declared with (`v3.7`) |
| `function_module` | `str \| None` | `large_string` | yes | the function's module (`__module__`) |
| `function_signature_hash` | `bytes` | `large_binary` | yes | the hash of the function's declared interface |
| `function_content_hash` | `bytes \| None` | `large_binary` | yes | the hash of the function as written (a lambda: its enclosing lines); absent when its source cannot be read (a plain REPL, `python -c`, `exec`) or, for a function other than a lambda, does not start at a `def` statement |
| `git_hash` | `str` | `large_string` | yes | the git commit of the function's file or working directory, with `-dirty`, `-untracked` and `-<source>` suffixes; `unknown` outside git |

and these about how it was computed (`__pf_exec_` prefix):

| Column | Python type | Arrow type | Nullable | Meaning |
|---|---|---|---|---|
| `executor_type` | `str` | `large_string` | yes | the executor that ran the function (`local`, `ray.v0`, `none`) |
| `executor_info` | `dict[str, str]` | `large_list<item: struct<key: large_string, value: large_string>>` | yes | the executor's own settings, as text |
| `python_version` | `str` | `large_string` | yes | the Python version that ran the function (`3.12.3`) |
| `extra_info` | `dict[str, str]` | `large_list<item: struct<key: large_string, value: large_string>>` | yes | further facts about the run, as text; empty today |

The two hashes, each stored as `<label>:` followed by its digest:

| Column | Label | What it covers |
|---|---|---|
| `function_signature_hash` | `function_signature_v1` | `{"inputs": {<name>: {"type": <type hash>, "default": <value hash>}}, "outputs": [{"key": <output key>, "type": <type hash>}]}`: the effective types (`input_schema=` / `output_schema=`, else the annotations); inputs keyed by name in no order, a `default` only for a parameter that has one; outputs in declared order (one key is a one-element list, none an empty one). Not in it: the function's name and module (their own columns), the parameters' kind and order, the version, the body |
| `function_content_hash` | `function_content_v1` | `{"source": <text>}`: the source text from the `def` line on, as written (docstring, comments and whitespace included; decorator lines excluded); for a lambda, the enclosing lines `inspect` gives, always present when they can be read. The `def` line is found by tokenizing the text as written, so nothing in the body moves the cut. Absent (null) when the source cannot be read, or when the source of a function other than a lambda does not start at a `def` statement: never a text with a decorator line |

Results are reused by the function's identity (its uri) and the input data hash alone;
these columns record what produced each row and are not compared on a cache hit.

## The common columns, `rdb_v1`

Every result table has, besides its type's record:

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `__input_data_hash` | `large_binary` | yes | the hash of the input data (`arrow_v1`): the key a result is looked up by |
| each output key | its declared type | as declared | the output value |
| `_source_<key>` | `large_string` | yes | the output's source-info token, `<uri parts joined by ":">::<result record id>::<key>` |
| `__pod_ts` | `timestamp[us, tz=UTC]` | yes | when the result was stored. Lookup by input data hash uses the newest timestamp and breaks ties by greater result id |
| `__data_context` | `large_string` | no | the data context that wrote the row (`std:v1:default`) |

The database keeps the row's record id, a UUIDv7 of 16 bytes, as its record id column.
The result cache compares those bytes to break timestamp ties.

The type of a token column is declared, once and for every function
(`RESULT_TOKEN_COLUMN` in `orcapod.core.result_cache`): a nullable `large_string`. It is
never taken from the values of a write, and a missing token is a `large_string` null. A
function result's source-info token is one string: a result whose token column has any
other type (a hand-built output given to `CachedDataFunction.record_data` with a list
token) is refused with `InputValidationError`, naming the column and both types, before
anything is written. A token type of your own cannot be declared.

Every stored row has the token column of every output. A hand-built output that gives no
token for an output (its row has no `_source_<key>` column) is stored with a null token
there, on every database, in either order of the writes. A token column that names no
output of the result is refused the same way, before anything is written.

```python
stored = types_store.get_all_records(next(p for p in types_store.list_sources() if "_result" in p))
print([name for name in stored.column_names if not name.startswith("__pf_")])
# ['__input_data_hash', 'total', 'label', '_source_total', '_source_label', '__pod_ts', '__data_context']
print([name for name in stored.column_names if name.startswith("__pf_var_")])
# ['__pf_var_function_name', '__pf_var_function_version', '__pf_var_function_module', '__pf_var_function_signature_hash', '__pf_var_function_content_hash', '__pf_var_git_hash']
```

## The pipeline columns, `pdb_v2`

A function node's pipeline table stores the input tags and source-info tokens,
the input and output hashes, the referenced result record ID, and bookkeeping
for the entry, including:

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `__pipeline_instance_id` | `large_binary` | yes | The row's instance id: the hash of its entry id (tags, system tags, input data hash) and the input's source-info tokens. The node looks a row up by it. |
| `__pipeline_record_id` | `large_binary` | yes | The row's record id: the hash of the node's content hash (`__node_content_hash`), its instance id and the id of its result. It is the table's key. |
| `__pipeline_run_id` | `large_string` | yes | The ID of the job run that recorded the entry. A node run outside a job stores null. |
| `__recorded_at` | `timestamp[us, tz=UTC]` | no | The UTC time the node recorded the entry. |

The node writes a pipeline entry only when processing yields an output. The
output can come from a new function call or from a result already in the result
database. A null value in a declared nullable output still counts as an output.
Reusing a pipeline entry preserves its original run ID and recording
time. If the result is lost and the node recomputes it, the new entry has its
own run ID and recording time.

A row is named by its record id, the hash of the node's content hash, its
instance id and the id of its result
([the three ids](hashing.md#3-pipeline-db-entry-keys)), stored in
`__pipeline_record_id`; it is the table's key. One table holds the rows of every
node with one pipeline hash, told apart by `__node_content_hash`, and the record
id covers that hash: two such nodes that record one instance with one stored
result have two record ids, and each keeps its row. A row is written unless this node already
stored that record, so the table keeps every distinct record of every node:

- the same instance recorded again with the same result is one row;
- a recompute has a new result, so it is a new row. The instance's current record
  has the latest `__recorded_at`. On a tie, a record with an available result
  wins over one whose ephemeral result is gone. If both results are available
  or both are gone, the greater result id wins;
- an input that arrives with other source-info tokens (its upstream result was
  computed again) is a new instance: it gets a new row. Its result is looked up
  in the result database by the input data hash: when a result is stored there
  the function is not called, and when none is (it was removed, or was an
  ephemeral result that is gone) the function is called.

"Recorded last" is read from `__recorded_at`, the recording process's UTC clock at
the write. So among the records of one instance the current one is the one with
the latest time: a clock set back between two writes of one instance leaves the
earlier record current until a later write.

During a sync run, a repeated identical input without system tags reuses the
first entry. During an async run, both copies can be scheduled before either
entry is available; when each then computes its own result, each has its own row.
A load-only run writes nothing. Neither recording column enters any hash.

Read the two recording columns (`__pipeline_run_id`, `__recorded_at`) on joined
records with `node.get_all_records(columns={"meta": True})` or
`node.get_all_records(all_info=True)`. The joined records do not hold the two
ids: `__pipeline_instance_id` and `__pipeline_record_id` are columns of the
pipeline table itself, read from the database by the table's path:

```python
pipeline_path = next(p for p in types_store.list_sources() if p[-1] == "pdb_v2")
pipeline_rows = types_store.get_all_records(pipeline_path)
print([name for name in pipeline_rows.column_names if name.startswith("__pipeline_")])
# ['__pipeline_instance_id', '__pipeline_record_id', '__pipeline_run_id']
joined = types_job.nodes["describe"].get_all_records(all_info=True)
print([name for name in joined.column_names if name.startswith("__pipeline_")])
# ['__pipeline_run_id']
```

Pipeline tables written by a release candidate before the three ids are at the
layout `pdb_v1`: they are reported as tables of another layout and are not read.

## Where it is declared, and how a released declaration is kept

The declaration is code: `PYTHON_FUNCTION` in `orcapod.core.data_function`, beside the
Python function class, a `DataFunctionTypeDeclaration` (`orcapod.core.data_function_types`) with the type name, major, revision, the identity and run
rules above and its record, one `StoredColumn` per column (name, Python type, Arrow type,
nullability, meaning, and for a hash its label constant and what it covers). The data
function builds its stored columns from it, the result cache takes the revision from it,
and a loaded job finds it by the type id its saved uri names. The common columns are
declared once, as `COMMON_RESULT_COLUMNS` in `orcapod.core.result_cache`, and the token
column of an output beside them, as `RESULT_TOKEN_COLUMN`.

Each declaration is also written out as a snapshot, `orcapod/data_function_types/<type
name>/v<major>-rev<revision>.json` (`python.function/v1-rev1.json`), listed with its sha256
in `orcapod/data_function_types/released.json`. Until the release that ships it, a snapshot
is open and must equal what the code writes; the release step pins its sha256, and from then
on it never changes. A change to what a type stores is then a new revision, and a change to
what its functions mean a new major, each with a snapshot of its own.
