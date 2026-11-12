# Sources

Sources are the entry points for external data into an Orcapod pipeline. Every pipeline begins
with one or more sources that load raw data -- from Python dicts, lists, CSV files, Delta Lake
tables, or Pandas DataFrames -- and present it as an immutable
[stream](streams.md) of (Tag, Data) pairs. Sources also attach provenance metadata
(source-info columns and system tag columns) so that every downstream value can be traced back
to its origin.

## Key classes

### `RootSource` (abstract base)

All sources inherit from `RootSource`. A root source is a pure stream with no upstream
dependencies -- it sits at the root of the computational graph. Key properties:

- `source.producer` returns `None` (no upstream pod)
- `source.upstreams` is always an empty tuple
- `source.source_id` is a canonical name used for provenance tracking and the source registry

### Concrete source types

All sources follow the same pattern: convert input data to a PyArrow Table, then pass it
through `SourceStreamBuilder` which handles enrichment (provenance columns, system tags,
hashing) and produces the final immutable stream.

| Source | Input type | Notes |
|---|---|---|
| `ArrowTableSource` | PyArrow `Table` | Accepts an Arrow table directly |
| `DictSource` | `list[dict]` | Each dict becomes one (Tag, Data) pair |
| `ListSource` | `list[Any]` | Each element stored under the data column given by `name`; the tag defaults to `element_index` |
| `DataFrameSource` | Pandas or Polars `DataFrame` | Converts via Arrow |
| `CSVSource` | File path (string) | Reads CSV into Arrow |
| `DeltaTableSource` | Directory path | Reads Delta Lake table |

`orcapod.sources` also provides database-backed sources (`SQLiteTableSource`,
`PostgreSQLTableSource`, `SpiralDBTableSource`) and `PollingSource` for data that arrives over
time; see the [API reference](../api/sources.md).

Two construction rules apply to every source and are the most common first errors:

- Every name in `tag_columns` must be a column of the data, otherwise the source raises
  `ValueError: tag_columns not found in table`.
- At least one column must be left over as a data column, otherwise it raises
  `ValueError: No data columns found in the table`.

Data with no rows has no columns to read, so `DictSource(data=[], ...)` raises
`ValueError: Table is empty`. To build a source with no rows, pass `data_schema=` so the
columns are known.

### `DerivedSource`

A `DerivedSource` is a source whose rows come from something Orcapod already has: another
source, or a node of a job. It has two uses.

**Reading a job's results.** A `DerivedSource` reads the stored results of a node of a
[`PipelineJob`](function-pods.md#pipelinejob-database-backed-cached-execution) that has run
and presents them as a new source. You get one by calling `as_source()` on the node, for
example `job.nodes["bmi"].as_source()`. This is useful for chaining pipeline stages: run a
job, then use one of its outputs as the input of a new pipeline.

**Using a tag value as an input to a computation.** Tags identify a row and data is what a
function computes on, so a function pod never receives a tag. Sometimes one value is needed
for both: a session's start time identifies the session, and it is also what you subtract to
get an elapsed time. Pass `tags_as_data` to get a source that keeps the tag and also offers
its value as a data column under a name you choose:

```python
from datetime import datetime, timezone

from orcapod import function_pod
from orcapod.sources import DerivedSource, DictSource

start_1 = datetime(2026, 1, 5, 9, 0, tzinfo=timezone.utc)
start_2 = datetime(2026, 1, 6, 9, 30, tzinfo=timezone.utc)

experiments = DictSource(
    data=[
        {"subject_id": "m1", "session_start": start_1, "experiment_index": 0,
         "experiment_start": datetime(2026, 1, 5, 9, 1, tzinfo=timezone.utc)},
        {"subject_id": "m1", "session_start": start_1, "experiment_index": 1,
         "experiment_start": datetime(2026, 1, 5, 9, 15, tzinfo=timezone.utc)},
        {"subject_id": "m1", "session_start": start_2, "experiment_index": 0,
         "experiment_start": datetime(2026, 1, 6, 9, 32, tzinfo=timezone.utc)},
    ],
    tag_columns=["subject_id", "session_start", "experiment_index"],
    source_id="experiments",
)

with_start = DerivedSource(
    experiments, tags_as_data={"session_start": "session_start_value"}
)

tag_keys, data_keys = with_start.keys()
print(tag_keys)
# ('subject_id', 'session_start', 'experiment_index')
print(data_keys)
# ('experiment_start', 'session_start_value')

@function_pod(output_keys="onset_s")
def onset_s(session_start_value: datetime, experiment_start: datetime) -> float:
    return (experiment_start - session_start_value).total_seconds()

onsets = onset_s.pod(with_start)
print(onsets.as_table().select(["subject_id", "experiment_index", "onset_s"]).to_pandas())
#   subject_id  experiment_index  onset_s
# 0         m1                 0     60.0
# 1         m1                 1    900.0
# 2         m1                 0    120.0
```

The result still has `session_start` as a tag, so it joins with other tables on that key as
before. Things to know:

- The mapping is `{tag column: new data column}`. The new name must not already be a column
  and must not start with `_`; a wrong name raises `InputValidationError` when the source is
  built.
- The copy has exactly the tag's type, including whether it may be null. If the tag may be
  null, annotate the function parameter as optional (`datetime | None`).
- The copy equals the tag when the source is read. After that it is ordinary data: a function
  may replace it and `map_data` may rename it, while the tag stays as it was.
- It works on any source (`DerivedSource(source, tags_as_data=...)`) and on a job node
  (`job.nodes["bmi"].as_source(tags_as_data=...)`).
- It is a **new source**, not a step in the origin's pipeline: `upstreams` is empty, and it
  has provenance columns of its own, like the result of handing the same rows to
  `ArrowTableSource`. Use it in place of the origin. Its default `source_id` is
  `derived:` followed by a labelled hash built from the origin's identity and the mapping, so
  it changes when either changes; pass `source_id=` to choose a name.
- A saved job reloads a derived source whenever its origin can be reloaded (a CSV file, a
  Delta table, a database table). Over an in-memory source it is saved but not reloaded, like
  the in-memory source itself.

```python
print(with_start.upstreams)
# ()
print(with_start.source_id)
# derived:...:...
```

Only a source can do this. An operator rearranges rows and columns, and every data value it
emits was already a data value of its input; turning a tag value into a data value creates
new data, which is what a source is for.

## How sources add provenance columns

When you create a source, Orcapod automatically adds two kinds of hidden columns to track
data lineage:

**Source-info columns** (prefix `_source_`) store a provenance token for each data column.
For example, a data column `weight` gets a companion `_source_weight` column. These tokens
identify which source originally produced each value.

**System tag columns** track which source and which source row contributed each row. Every
source adds two of them, `_tag_source_id::<hash>` and `_tag_record_id::<hash>`, where `<hash>`
is the source's pipeline hash. These columns are used internally during
[joins](operators.md) to maintain provenance through multi-stream operations.

Each row's system tag carries a record ID derived from the source's `source_id` and a
per-row token. By default that token is the row's position, so a row that moves to another
position under the same `source_id` gets a different record ID. Downstream pipeline entries
and `sink_pod` completion records are keyed on the system tags together with a hash of the
row's data, so the moved row is treated as a new entry: a function node records it again,
and its stored table then shows two entries with identical tag values but distinct record
IDs, and a `sink_pod` delivers it again. Because every key includes the hash of the row's own
data, no result is ever paired with the wrong row; a function's stored result for that data
is reused rather than recomputed.

Sources that accept `record_id_column` use that column's value instead, so the record ID
follows the row. Both halves of the record ID must be stable: when you omit `source_id` it
defaults to a hash of the table, which changes when the rows are reordered. Whenever row
order can vary between runs, pass an explicit, stable `source_id` **and** set
`record_id_column` to a column that is unique per row; this avoids both the duplicate entries
and the repeated deliveries.

These columns are hidden by default. You can reveal them using `ColumnConfig`:

```python
from orcapod.sources import DictSource

source = DictSource(
    data=[{"subject_id": "mouse_01", "age": 12, "weight": 25.3}],
    tag_columns=["subject_id"],
    source_id="mice",
)

# Show source-info columns
table = source.as_table(columns={"source": True})
print(table.column_names)
# ['subject_id', 'age', 'weight', '_source_age', '_source_weight']
print(table.column("_source_weight").to_pylist())
# ['mice::row_0::weight']

# Show system tag columns
tag_schema, data_schema = source.output_schema(columns={"system_tags": True})
print(list(tag_schema))
# ['subject_id', '_tag_source_id::...', '_tag_record_id::...']

# Show everything
table = source.as_table(all_info=True)
print(table.column_names)
# ['subject_id', 'age', 'weight', '_tag_source_id::...', '_tag_record_id::...', '_content_hash', '_context_key', '_source_age', '_source_weight']
```

Pass `source_id` to give the source a readable name in these provenance tokens. Without it,
Orcapod derives an id from the source's content hash.

## Code example

Create a `DictSource`, inspect its schema, and iterate over its stream:

```python
from orcapod.sources import DictSource

source = DictSource(
    data=[
        {"subject_id": "mouse_01", "age": 12, "weight": 25.3},
        {"subject_id": "mouse_02", "age": 8, "weight": 22.1},
        {"subject_id": "mouse_03", "age": 15, "weight": 27.8},
    ],
    tag_columns=["subject_id"],
)

# Inspect the schema
tag_schema, data_schema = source.output_schema()
print("Tag schema:", dict(tag_schema))
# Tag schema: {'subject_id': <class 'str'>}
print("Data schema:", dict(data_schema))
# Data schema: {'age': <class 'int'>, 'weight': <class 'float'>}

# Get column names
tag_keys, data_keys = source.keys()
print("Tag keys:", tag_keys)
# Tag keys: ('subject_id',)
print("Data keys:", data_keys)
# Data keys: ('age', 'weight')

# Iterate over (Tag, Data) pairs
for tag, data in source.iter_data():
    print(f"  Tag: {tag.as_dict()}, Data: {data.as_dict()}")
# Tag: {'subject_id': 'mouse_01'}, Data: {'age': 12, 'weight': 25.3}
# Tag: {'subject_id': 'mouse_02'}, Data: {'age': 8, 'weight': 22.1}
# Tag: {'subject_id': 'mouse_03'}, Data: {'age': 15, 'weight': 27.8}

# Convert to a PyArrow table
table = source.as_table()
print(table.to_pandas())
#   subject_id  age  weight
# 0   mouse_01   12    25.3
# 1   mouse_02    8    22.1
# 2   mouse_03   15    27.8
```

## How it connects to other concepts

- Sources produce [Streams](streams.md) -- immutable sequences of (Tag, Data) pairs
- Streams flow into [Operators](operators.md) for structural transforms (joins, filters,
  column selection)
- Streams flow into [Function Pods](function-pods.md) for value-level transforms
- Every source has a `content_hash()` and `pipeline_hash()` -- see
  [Identity & Hashing](identity.md) for how these work
