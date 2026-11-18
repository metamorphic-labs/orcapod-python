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
<job>/<function>/<output schema hash>/v<function major>/python.function.v1/schema:<pipeline hash>/pdb_v1
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
# ('v0', 'python.function.v1', 'schema:...', 'pdb_v1')
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
| `function_signature_hash` | `bytes` | `large_binary` | yes | the hash of the function used as a value |
| `function_content_hash` | `bytes` | `large_binary` | yes | the hash of the function's code |
| `git_hash` | `str` | `large_string` | yes | the git commit of the function's file or working directory, with `-dirty`, `-untracked` and `-<source>` suffixes; `unknown` outside git |

and these about how it was computed (`__pf_exec_` prefix):

| Column | Python type | Arrow type | Nullable | Meaning |
|---|---|---|---|---|
| `executor_type` | `str` | `large_string` | yes | the executor that ran the function (`local`, `ray`, `none`) |
| `executor_info` | `dict[str, str]` | `large_list<item: struct<key: large_string, value: large_string>>` | yes | the executor's own settings, as text |
| `python_version` | `str` | `large_string` | yes | the Python version that ran the function (`3.12.3`) |
| `extra_info` | `dict[str, str]` | `large_list<item: struct<key: large_string, value: large_string>>` | yes | further facts about the run, as text; empty today |

The two hashes, each stored as `<label>:` followed by its digest:

| Column | Label | What it covers |
|---|---|---|
| `function_signature_hash` | `semantic_v1` | the function's module and name, each parameter's name, kind, annotation and default (the default's own hash), and the return annotation ([Hashing](hashing.md#1d-function-values-and-parameter-defaults)) |
| `function_content_hash` | `semantic_v1` | the function's name and module, its source text as written (decorator lines, docstring, comments and whitespace included), its annotations sorted by name, and its code properties (argument counts, number of locals, argument names) |

Results are reused by the function's identity (its uri) and the input data hash alone;
these columns record what produced each row and are not compared on a cache hit.

## The common columns, `rdb_v1`

Every result table has, besides its type's record:

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `__input_data_hash` | `large_binary` | yes | the hash of the input data (`arrow_v1`): the key a result is looked up by |
| each output key | its declared type | as declared | the output value |
| `_source_<key>` | `large_string` | yes | the output's source-info token, `<uri parts joined by ":">::<result record id>::<key>` |
| `__pod_ts` | `timestamp[us, tz=UTC]` | yes | when the result was stored; the newest wins when two rows share an input |
| `__data_context` | `large_string` | no | the data context that wrote the row (`std:v1:default`) |

The database keeps the row's record id, a UUID of 16 bytes, as its record id column.

```python
stored = types_store.get_all_records(next(p for p in types_store.list_sources() if "_result" in p))
print([name for name in stored.column_names if not name.startswith("__pf_")])
# ['__input_data_hash', 'total', 'label', '_source_total', '_source_label', '__pod_ts', '__data_context']
print([name for name in stored.column_names if name.startswith("__pf_var_")])
# ['__pf_var_function_name', '__pf_var_function_version', '__pf_var_function_signature_hash', '__pf_var_function_content_hash', '__pf_var_git_hash']
```

## Where it is declared, and how a released declaration is kept

The declaration is code: `PYTHON_FUNCTION` in `orcapod.core.data_function`, beside the
Python function class, a `DataFunctionTypeDeclaration` (`orcapod.core.data_function_types`) with the type name, major, revision, the identity and run
rules above and its record, one `StoredColumn` per column (name, Python type, Arrow type,
nullability, meaning, and for a hash its label constant and what it covers). The data
function builds its stored columns from it, the result cache takes the revision from it,
and a loaded job finds it by the type id its saved uri names. The common columns are
declared once, as `COMMON_RESULT_COLUMNS` in `orcapod.core.result_cache`.

Each declaration is also written out as a snapshot, `orcapod/data_function_types/<type
name>/v<major>-rev<revision>.json` (`python.function/v1-rev1.json`), listed with its sha256
in `orcapod/data_function_types/released.json`. Until the release that ships it, a snapshot
is open and must equal what the code writes; the release step pins its sha256, and from then
on it never changes. A change to what a type stores is then a new revision, and a change to
what its functions mean a new major, each with a snapshot of its own.
