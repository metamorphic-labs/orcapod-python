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
time (provisional in this release, see [Provisional sources](#provisional-sources)); see the
[API reference](../api/sources.md). Their default `source_id` names the table
where it lives (`sqlite_table_v1:<db path>:main.<table>`,
`postgresql_table_v1:<host>:<port>/<database>.<schema>.<table>`, with the server the
connection reached; the
[hashing reference](../reference/hashing.md#default-source-ids-and-row-identifiers) has
every form). A PostgreSQL DSN that names several servers (`host=a,b`), or gives a host
beside a different `hostaddr` (`host=db.example.org hostaddr=192.0.2.1`; with
`host=127.0.0.1 hostaddr=127.0.0.1` the default stays), has no default; the same holds when
`PGHOST`, `PGHOSTADDR` or a service file gives either value. Give such a source a
`source_id`. `DBTableSource`, the base they share, can also be used directly
over any connector; a connector gives no address of its database, so it then has no default
and needs a `source_id`.

### Source ids name the kind that reads the data

Every source id is `<kind>_v<N>:<name>`. The **kind part**, `<kind>_v<N>`, names the kind
of source that read the data (`csv`, `delta_table`, `sqlite_table`, `arrow_table`, ...; the
same word as the source's config type) and the version of that kind's implementation.
Orcapod always adds it. The **name** is the `source_id` you give, kept exactly, or the
source's default: where the data lives (a file path, a table) or a hash of an in-memory
table.

```python
import pyarrow as pa
from orcapod.sources import ArrowTableSource

units_table = pa.table({"unit": ["u1", "u2"], "rate": [3.5, 7.25]})
units = ArrowTableSource(
    units_table, tag_columns=["unit"], tag_only_columns=True, source_id="units"
)
print(units.source_name)
# units
print(units.source_id)
# arrow_table_v1:units
```

The source id, kind part included, is what the source's system tags and source-info tokens
hold (`arrow_table_v1:units::row_0::rate`), and it is part of the source's identity, so the
same rows read by another kind, or by another version of one kind, are another source. The
record id names a row within its source and never holds the source id. A job names an input
slot by the source's name (`job.bind(sources={"units": ...})`).

Each source content hash is registered to one Python object per pipeline or job.
Build the source once and reuse that object wherever you read it. For sources
whose hash includes their `source_id`, each source can instead have its own ID.
Two distinct objects with the same content hash raise
`InconsistentSourceError` even when their rows agree, including sources that read
rows later. Capture, binding, loading and execution all check for conflicts.
Rebinding replaces a slot's source. On load, slots with identical saved source
configurations share one reconstructed object.

A job accepts new steps only while every slot holds the exact source object
it held when that slot was first compiled. A slot originally compiled unbound
must be unbound. Rebinding or unbinding a slot blocks recording until all
original bindings are restored. InconsistentSourceError names the changed
slots. Restore the original bindings or build a new job. A job can compile
and run with its replacement bindings. ITL-1559 tracks capture after rebinding.

As a stopgap pending ITL-1559, a job refuses bindable source slots
(`SourceJobNode`) from another job or created standalone, including through a
stream built from one. Pass the source itself, or use `node.as_source()` to read
another job's stored results. A schema-only `SourceNode` declares a new unbound
slot for each job that captures it.

Capturing into a loaded job or recompiling it can lose its restored graph
(ITL-1560). Recompilation preserves the loaded job's public `sources` mapping.

The same rule applies to wrappers. Each of these pairs raises
`InconsistentSourceError` when the objects share one content hash.

| Sources used together | How to share the source |
| --- | --- |
| A source and a `CachedSource` over it | Build the cache once and use it in both places. |
| Two caches over one source, or a cache and its inner cache | Choose one cache object and reuse it. |
| Two calls to `node.as_source()` on one node | Call `as_source()` once and reuse the result. |
| Two `DerivedSource(tags_as_data=...)` objects with the same hash | Build one derived source and reuse it. |
| A source or its cache paired with a `SourceProxy` restored for it | Choose one source object and reuse it. |

ITL-1553 tracks accepting distinct objects that represent the same source.
ITL-1531 tracks sources that read their rows later.

```python
from orcapod import Pipeline, function_pod
from orcapod.errors import InconsistentSourceError
from orcapod.sources import DictSource


@function_pod(output_keys="adjusted")
def adjust(rate: float) -> float:
    return rate + 1


first = DictSource([{"rate": 3.5}], source_id="rates")
second = DictSource([{"rate": 3.5}], source_id="rates")
try:
    with Pipeline("rates"):
        adjust.pod(first, label="first")
        adjust.pod(second, label="second")
except InconsistentSourceError as error:
    print(error)
# Source 'dict_v1:rates' has distinct source objects with one content hash in step 'first' and step 'second'. Build the source once and reuse the object. If the source ID contributes to the hash, give each source its own source_id.
```

A kind's version moves when a row read before a change of its implementation could read
differently after it (other values, other Arrow types, another record id). Saved jobs keep
each source's name and version apart; a job loaded where a kind's version moved rebuilds the
source with the new version's id and warns (`SourceVersionWarning`).

Two construction rules apply to every source and are the most common first errors:

- Every name in `tag_columns` must be a column of the data, otherwise the source raises
  `ValueError: tag_columns not found in table`.
- At least one data column must remain, otherwise the source raises
  `ValueError: No data columns found in the table`. A data-tag is also a data column, so
  this happens only when every tag is kept plain (`tag_only_columns`) and no other column
  is left.

**Data-tags and plain tags: `tag_only_columns`.** The sources built from a table
(`ArrowTableSource`, `DictSource`, `DataFrameSource`, `CSVSource`, `DeltaTableSource` and the
database sources) take the keyword-only option `tag_only_columns`. By default every declared
tag is a data-tag: it is also the data column of the same name (see
[Data-tags](data-tags.md)). `tag_only_columns` names the tags kept plain, a tag
only and never a data column, as every tag was in v0.3; `tag_only_columns=True` keeps all of
them plain. A source with plain tags and one with data-tags over one table have the same source id and record
ids, but different schemas and so different hashes.

```python
from orcapod.sources import DictSource

with_data_tags = DictSource(data=[{"k": "a", "x": 1}], tag_columns=["k"])
print(with_data_tags.keys())
# (('k',), ('k', 'x'))
plain = DictSource(
    data=[{"k": "a", "x": 1}], tag_columns=["k"], tag_only_columns=True
)
print(plain.keys())
# (('k',), ('x',))
```

Data with no rows has no columns to read, so `DictSource(data=[], ...)` raises
`ValueError: Table is empty`. To build a source with no rows, pass `data_schema=` so the
columns are known.

### Arrow types of a source

Every source gives the large variants of string, binary and list types (`large_string`,
`large_binary`, `large_list`, recursively), whatever its input: an Arrow table of your own
(`ArrowTableSource`), an Arrow schema you pass (`schema=` or `data_schema=`), Python values,
a data frame, a CSV file or a database table. A `string` column you give comes back as
`large_string`. Only the layout changes: names, nullability, widths (`int8` stays `int8`) and
logical types are kept, and the two variants hash identically, so no hash depends on which
one you gave. The Arrow `list_view` and `large_list_view` types, unions (dense or sparse) and
run-end-encoded types are not supported: a source, and a stream built from an Arrow table of
your own, refuses a column whose type is or holds one, naming it (a stream's identity is its
Arrow schema hash, which cannot hold them). Cast a list view to the large list of the same
items, and expand a run-end-encoded column with `pyarrow.compute.run_end_decode`.

Dictionary-encoded columns are not currently supported in Orcapod, tags and data alike. A
column whose Arrow type is a `dictionary`, or holds one inside it (a list of dictionary values,
a struct with a dictionary field), is refused with `InputValidationError` when the source or
stream is built. The message names every such column with its role (tag, data-tag or data), its
type and the type it decodes to, then says how to decode it. Such columns come from an Arrow
table of your own, a pandas `Categorical` of strings, or a polars `Categorical` or `Enum`; CSV
files, Delta tables, database tables and Python values never give one. If keeping the encoding
is not important, decode the column before building the source:

```python
import pyarrow as pa

from orcapod.errors import InputValidationError
from orcapod.sources import ArrowTableSource

coded = pa.table(
    {
        "session": pa.array(["s1", "s2", "s1"]).dictionary_encode(),
        "trial": [1, 1, 2],
        "response": pa.array([0.4, 0.7, 0.5]).dictionary_encode(),
    }
)
try:
    ArrowTableSource(coded, tag_columns=["session", "trial"], tag_only_columns=True)
except InputValidationError as exc:
    print("\n".join(str(exc).splitlines()[:3]))
# dictionary-encoded columns are not currently supported in Orcapod:
#   - tag column 'session' has type dictionary<values=string, indices=int32, ordered=0> (decoded: string)
#   - data column 'response' has type dictionary<values=double, indices=int32, ordered=0> (decoded: double)

decoded = coded
for name in ("session", "response"):
    field = decoded.schema.field(name)
    value_type = field.type.value_type
    # The field keeps its name, nullability and metadata; set_column returns a new table.
    decoded = decoded.set_column(
        decoded.schema.get_field_index(name),
        field.with_type(value_type),
        decoded[name].cast(value_type),
    )
source = ArrowTableSource(decoded, tag_columns=["session", "trial"], tag_only_columns=True)
print(source.as_table().schema.field("session").type)
# large_string
```

With polars, `pl.col("session").cast(pl.String)` decodes a `Categorical` or `Enum` column.
With pandas, `astype` with the categories' dtype decodes a `Categorical` of strings. Both
return a new frame or column: keep it before building the source.

```python
import pandas as pd
import polars as pl

from orcapod.sources import DataFrameSource

frame = pl.DataFrame({"session": ["s1", "s2", "s1"], "trial": [1, 1, 2]}).with_columns(
    pl.col("session").cast(pl.Categorical())
)
frame = frame.with_columns(pl.col("session").cast(pl.String))
print(DataFrameSource(frame, tag_columns=["session"], tag_only_columns=True).keys())
# (('session',), ('trial',))

table = pd.DataFrame({"session": ["s1", "s2", "s1"], "trial": [1, 1, 2]})
table["session"] = table["session"].astype("category")
table["session"] = table["session"].astype(table["session"].cat.categories.dtype)
print(DataFrameSource(table, tag_columns=["session"], tag_only_columns=True).keys())
# (('session',), ('trial',))
```

A pandas `Categorical` of integers that holds a missing value cannot be cast to an integer
dtype this way (fill or drop the missing values first). A pandas `Categorical` of integers
reaches the source as plain integers, so it needs no decoding.

A data frame kept whole as a value in a cell (a pandas or polars `DataFrame` holding a
categorical column) is not a dictionary column: it is a library value, stored and hashed as one
(see [Arrays and other library values](arrays.md)), and is accepted. Supporting
dictionary-encoded columns themselves is future work (ITL-1277).

### A CSV file's types: `schema=`

A CSV file holds text only, so `CSVSource` infers each column's type from the file, and
infers it again on every read: a column of whole numbers is `int64`, and it may be null only
if the file has an empty cell in it. Pass an Arrow `schema=` to choose the types instead: its
fields name the file's columns in the file's order, each with its type, width and
nullability. The source then has exactly those types (in their large variants, as above).

Each cell's text is read as its column's declared type, never as an inferred type converted
afterwards: `007` under a string type stays `"007"`, `1e3` stays `"1e3"`, and `1.005` under
`decimal128(10, 3)` is exactly `1.005`. A cell that is not a value of its declared type (`1.0`
under an integer, a date and time under `date32`, `1.005` under two decimal places) raises
`ValueError`, naming the column and the file. The schema names every column of the file, in
the file's order. A string or binary column (`string`, `large_string`, `binary`,
`large_binary`, `fixed_size_binary`) holds a cell's text as written; what reads as null:

| Cell in the file | Under a string or binary type | Under any other type |
|---|---|---|
| empty: `a,,b` | null | null |
| quoted empty: `a,"",b` | `""` (`b""`) | null |
| `NA`, `null`, `N/A`, `NaN` | the text as written | null |
| any other text | the text as written, spaces included | parsed as the type, or raises |

A float column cannot take a NaN from the text `NaN`, which reads as null; an infinity can be
spelled (`inf`, `-Infinity`).

A null in a field declared non-nullable raises. Under a float type, an infinity spelled in the
file (`inf`, `-Infinity`) is kept, a finite value is rounded to the type as usual, and a finite
value too large for the type (`70000` under `float16`, `1e39` under `float32`) raises instead
of becoming an infinity. `float16` and `decimal256` (which pyarrow's CSV reader does not
parse) are read as text and converted with a checked conversion. A type the reader cannot
parse at all (`decimal32`, `string_view`, an interval) and a list, struct or other nested type
raise, naming the column. The file is read once into memory and parsed from that copy, so a
file replaced while it is read cannot mix two versions.

The schema is part of the source's configuration: `to_config()` saves it, field by field
(Arrow type text, nested fields included, and which fields may be null), and `from_config()`
and a saved job (`PipelineJob.save` / `PipelineJob.load`) read the file with it again, so the
reloaded source has the same schema, content hash and pipeline hash as the one you built. A
source built without `schema=` saves no schema and infers again when it is reloaded. Only
a missing entry means that: a saved entry that is not what `to_config()` writes (`null`, a
string where the list of nullable fields belongs, an unknown key) raises `ValueError`, and
`PipelineJob.load` refuses the file.

```python
import json
import tempfile
from pathlib import Path

import pyarrow as pa

from orcapod.sources import CSVSource

csv_dir = Path(tempfile.mkdtemp())
(csv_dir / "trials.csv").write_text("session,trial,response\ns1,1,0.4\ns1,2,0.7\n")
trial_schema = pa.schema(
    [
        pa.field("session", pa.string(), nullable=False),
        pa.field("trial", pa.int16(), nullable=False),
        pa.field("response", pa.float32()),  # nullable: a later file may lack a value
    ]
)
trials = CSVSource(
    str(csv_dir / "trials.csv"),
    tag_columns=["session", "trial"],
    tag_only_columns=True,
    schema=trial_schema,
)
print(trials.to_config()["schema"])
# {'columns': {'session': 'string', 'trial': 'int16', 'response': 'float'}, 'nullable': ['response']}
reloaded = CSVSource.from_config(json.loads(json.dumps(trials.to_config())))
print(reloaded.output_schema())
# (Schema({'session': str, 'trial': int}), Schema({'response': float | None}))
print(reloaded.content_hash() == trials.content_hash())
# True
```

Arrow metadata cannot be saved. A schema whose fields (at any depth) or whose schema itself
carry Arrow metadata is accepted by the constructor, and the source's table keeps it, but
`to_config()` and so `PipelineJob.save` raise `ValueError` naming the field, and the job file
is not written: build the schema without the metadata to save the source, or do not save it.
A source never saves a schema it would read back differently.

### SpiralDB tables

`SpiralDBTableSource` exposes a table in a [SpiralDB](https://spiraldb.com) project as a
source. It reads SpiralDB's keyed tables through
[spiral-sdk](https://docs.spiraldb.com/docs/sdks/python). Install the extra and log in once;
the `spiral` command comes with the extra:

```bash
uv add "orcapod[spiraldb]"      # or, without uv: pip install "orcapod[spiraldb]"
uv run spiral login
```

`pyspiral`, the older client, provides a Python module of the same name: never install both
in one environment. spiral-sdk has wheels for macOS on Apple silicon and for Linux.

spiral-sdk takes credentials from the environment (`SPIRAL_TOKEN`, or `SPIRAL_CLIENT_ID` with
`SPIRAL_PRIVATE_KEY` for a workload, or `SPIRAL_WORKLOAD_ID` with a platform identity), then
from the saved login. With neither, Orcapod raises `SpiralDBSignInError` instead of letting
the client start a sign-in. A saved login that has expired is not detected: the client may
then print a sign-in prompt and wait. Stop it and run `uv run spiral login` again.

Orcapod only reads SpiralDB for now. Writing a SpiralDB table (`SpiralDBConnector`, or a job
whose database is on one) raises `SpiralDBWriteNotSupportedError`; store a job's results in
another database, such as `DeltaTableDatabase`.

To find the tables of a project and their key columns:

<!-- docs-test: skip -->
```python
from orcapod.databases import SpiralDBConnector

with SpiralDBConnector("edi-sample-v1-855632") as spiral:
    print(spiral.get_table_names())
    print(spiral.get_pk_columns("session"))
```

The table's key columns become the tag columns, and every other column is a data column:

<!-- docs-test: skip -->
```python
from orcapod.sources import SpiralDBTableSource

sessions = SpiralDBTableSource("edi-sample-v1-855632", "session")
experiments = SpiralDBTableSource("edi-sample-v1-855632", "experiment")
print(sessions.output_schema())
sessions.as_df()  # also as_polars_df(), as_pandas_df(), as_lazy_frame(), as_table()
```

Things to know about Spiral tables:

- **The schema is recorded beside the tables.** See
  [The schema of a Spiral table](#the-schema-of-a-spiral-table): a column recorded as
  non-nullable has the type `T`, every other column `T | None`.
- **Types are kept.** Arrow types come through unchanged (`uint32` stays `uint32`), except
  that strings, binaries and lists become their large variants. Joining two Spiral tables on
  their shared key columns works; joining with another source requires the same Arrow types on
  both sides (`uint32` against `int64` is an error, not a conversion).
- **Not supported:** columns of type `map`, which the Arrow hasher does not hash. Building
  the source raises `ValueError` naming each such column and its type; drop or convert
  those columns first. `fixed_size_list` columns are supported.
- **Source identity.** A Spiral source is identified by where its table is, never by its
  rows. The default `source_id` is `spiraldb_table_v1:<project id>.<dataset>.<table>@<Spiral
  table id>`, for example
  `spiraldb_table_v1:edi-tutorial-v1-841226.default.session@table_Jo7J0lpkLcOM`. The table id is
  SpiralDB's own, so tables with the same name in different projects stay apart, and a table
  that is dropped and created again under the same name is a different source: nothing
  recorded for the old table is reused. A renamed table also gets a new `source_id`.
- **Record identity.** A row is identified by its primary key: its record ID is the
  canonical JSON of its key columns (`{"session_id":"s1"}`). A row keeps its record ID when
  other rows are added, and has
  the same one whichever read returned it.

#### Reading a Spiral table

Building a `SpiralDBTableSource` reads no rows. It reads the table's schema, key columns and
table id, which takes about a second whatever the table's size. `output_schema()`, `keys()`,
`content_hash()` and `pipeline_hash()` read no rows either. Rows are read when you ask for
them, and every read scans the table again:

- `iter_data()` and a pipeline job hand the rows on in batches of at most `batch_rows`
  rows (default 8,192). SpiralDB sends a scan in batches that follow its stored fragments,
  whose sizes cannot be chosen; one of them is held while its rows are handed on.
- `as_table()`, `as_df()`, `as_polars_df()`, `as_pandas_df()` and `as_lazy_frame()` read
  the rows into memory and return them all.

To read part of a table, say so when you build the source. SpiralDB applies both
restrictions before any row is transferred (except a condition on a `float16` column, which
Orcapod applies to the rows it receives, and then the limit):

<!-- docs-test: skip -->
```python
first = SpiralDBTableSource("edi-tutorial-v1-841226", "spike_event", limit=1000)
unit = SpiralDBTableSource(
    "edi-tutorial-v1-841226", "spike_event", where={"unit_id": 106}
)
print(first.row_count_estimate)  # rows in the whole table; reads every key to count them
```

- `limit=n` reads the first `n` rows in key order.
- `where={column: value, ...}` reads the rows in which every named column equals its value
  (`source.where(...)` adds conditions to a built source; see
  [Restricting a source versus filtering in the pipeline](#restricting-a-source-versus-filtering-in-the-pipeline)).
- `asof=` reads the table as of an earlier version, and keeps reading it. Without it, a
  source reads the version the table had when the source was built, in every read, until a
  run finds a newer one (see [What a source does at a run](#what-a-source-does-at-a-run)):
  `head()`, `as_table()` and the preview outside a run never move it.

**The row cap.** One read returns at most `max_rows` rows (default 100,000). A read that
would return more raises `SourceTooLargeError`, which says so and gives the ways out:
`limit=`, `where=`, or a larger `max_rows` (`max_rows=None` removes the cap). When nothing
but `limit=` bounds the read, the source decides before it hands on any row, by counting the
table's keys. SpiralDB keeps no row count of a keyed table, so the count is a read of one key
column; it stops one past the cap and is remembered for the version. The cap is there
because a pipeline job keeps every row it processes in memory, about 5 KB per row for each
node, so 100,000 rows cost about 0.5 GB per node. A table of millions of rows can be iterated
with `max_rows=None`, but it cannot yet be run through a job on a laptop.

**Looking at a table in a notebook.** A Spiral source shown at the end of a notebook cell
(its `head()`, see [Looking at data](streams.md#looking-at-data)) reads only its first 10
rows, in one scan to which the row cap does not apply, so a table of any size can be looked
at; `head(n)` reads `n`. The preview shows the rows it read in tag order (it asks for
`sort_by_tags`, as does `head(columns={"sort_by_tags": True})` or `all_info=True`): the rows
read, sorted, not the first `n` of the table in tag order. The header line says what is
shown, for example `SpiralDBTableSource[spike_event] — 10 rows read, sorted by tags, of
about 63,349` (the table's row count, from its keys; above 100,000 rows, or `max_rows` when
that is smaller, it says `of more than 100,000`), `all 4 rows, sorted
by tags` when the source reads no more than that, or `10 matching rows read, sorted by tags
(where unit_id=106); the table has about 63,349 rows` with `where=`. The unsorted `head()`
says `first 10 rows of about 63,349`. Tag columns come first, and the line above the table
names them. To see more rows, use `as_df()` with a `limit=` or `where=` source.

**Restrictions are not part of identity.** `where`, `limit` and `asof` choose rows of the
same table, so two sources on one table have the same `source_id` and `content_hash`
whatever their restrictions, and a row's result is computed once and shared. One
consequence: with a shared store, reading a job node's stored records back
(`get_all_records()`, `node.as_source()`) returns every row that any job with the same
pipeline has read from that table, not only the rows of the latest restriction. The rows
sent downstream during a run are only the rows that run read.

#### Restricting a source versus filtering in the pipeline

`source.where(column=value, ...)` returns a new source on the same table that reads only the
rows in which every named column equals its value. Several conditions can be given in one
call, as keywords or as a mapping, or across chained calls:
`events.where(unit_id=106, session_index=2)` and
`events.where(unit_id=106).where(session_index=2)` are the same restriction, reading the
same rows. The conditions are a `Condition` (`orcapod.types`): `where(...)` builds one with
`Condition.of(...)`, which takes the same arguments, and a `Condition` built beforehand can be
given instead (`events.where(Condition.of(unit_id=106, session_index=2))`). A source
restricted by a condition, a `limit=`, or both is **restricted**. Every source states its
restriction through two read-only properties: `condition` (the empty `Condition` when it
reads every row) and `row_limit` (`None` without a limit). A restriction is not an
operator:

- An operator (`Filter`, `SemiJoin`, `PolarsFilter`) is a step of the pipeline. It is a node of the
  graph and part of the pipeline's identity: a pipeline with a filter computes something
  else than one without, and its nodes have other hashes and other stored tables.
- A restriction is attached to the source. It chooses which of the source's rows flow and
  changes nothing else: the restricted source has the same `source_id`, `content_hash`,
  `pipeline_hash`, schema and system-tag columns as the unrestricted one, and every row it
  reads is a row of the table with every column, record ID included, as the unrestricted
  source reads it. With `limit=`, the conditions apply before the limit, so a restricted read
  with a limit is not a part of an unrestricted read with the same limit; each matching row
  still keeps its record ID. It adds no node, so a job bound to a restricted source is the
  same job, run over fewer rows.

Use a restriction to work on part of a table (one subject, one session) with the pipeline you
will later run on all of it: results computed under the restriction are stored under the same
identities and reused by the full run. Use an operator when selecting rows is part of what
the pipeline computes. Restrict on tag (key) columns where you can; a condition on a data
column is allowed.

`where` and `stream.filter` take the same equalities, written the same way, and keep the same
rows (for a float column, `where` uses the table system's own equality for NaN and `-0.0`,
`filter` Orcapod's: see [Filter](operators.md#filter)). One spelling differs: `filter` takes
`label=` (the step's label, as every stream method does), so a column named `label` is written
`filter({"label": "x"})` or `filter(col("label") == "x")`, while `where(label="x")` restricts
that column. Neither takes `None` as a value: both raise `ValueError` and point to
`col(c).is_null()`, a null test that only `filter` takes. The difference is where the
selection lives:

<!-- docs-test: skip -->
```python
from orcapod import col

# Exploring: the source reads only these rows; no step is added and no hash moves.
explore = events.where(unit_id=106, session_index=2)

# The pipeline: the same selection as a step, part of every downstream identity.
kept = events.filter(unit_id=106, session_index=2)

# Anything but equality is a filter; where refuses it and points to filter.
late = events.filter(col("spike_time_us") >= 1_000_000, col("unit_id").is_in([106, 107]))
```

A filter directly over a source reads every row the source reads (the whole table, or what
the source's own `where` and `limit` select) and then filters them; its condition is never
pushed into the source. `where` reads only the matching rows. On a large table, keep `where` on the source for speed and put the same
`filter` in the pipeline for its identity: the rows are the same, and the restriction rules
above apply to the source. This holds for equalities on integer, string, bytes, bool, date and
timestamp columns, on a source without a `limit`. With a `limit` it does not: the source
reads the first rows that match the `where`, so the `filter` sees other rows than it would
over the source alone (with `limit=2` and unit ids `[1, 2, 2]`, `filter(unit_id=2)` alone
keeps one row, the `where` and the `filter` together keep two). On a float column it does
not either when the value is a NaN or a signed zero:
`where` matches by the table system's equality (a NaN may match no row), `filter` by
Orcapod's (a NaN matches the NaN rows), so `where` can drop rows the `filter` alone would
keep. For such a value, filter without the `where`.

<!-- docs-test: skip -->
```python
events = SpiralDBTableSource("my-project-123456", "spike_event", source_id="spikes")
one_unit = events.where(unit_id=106)  # or events.where({"unit_id": 106})
one_session = one_unit.where(session_index=2)  # conditions add up
same_session = events.where(unit_id=106, session_index=2)  # both in one call
print(one_session.condition)
# unit_id=106, session_index=2
print(one_session.row_limit)
# None

assert same_session.to_config() == one_session.to_config()
assert one_unit.content_hash() == events.content_hash()
job.bind(sources={"spikes": one_unit})  # the slot the unrestricted source fills
summary = job.run()
print(summary.conditions)
# {'spikes': Condition.of({'unit_id': 106})}
print(summary.restricted_source_ids)
# {'spikes': 'spikes'}
```

(This sample needs a SpiralDB project, so it is not run with the documentation; `job` is a
job built over `events`, whose slot is named by the source id.)

To restrict on two columns, pass separate equality keywords such as
`events.where(unit_id=106, session_index=2)`. Combining comparisons as
`(col("unit_id") == 106) and (col("session_index") == 2)` raises `TypeError`.
When building a `Condition` to pass to `where()`, combine parenthesized equalities with `&`.

```python
from orcapod import col
from orcapod.types import Condition

try:
    Condition.of((col("unit_id") == 106) and (col("session_index") == 2))
except TypeError as error:
    print(str(error).split(".")[0])
# Condition has no truth value

both = (col("unit_id") == 106) & (col("session_index") == 2)
assert both == Condition.of(unit_id=106, session_index=2)
```

A date or a time written as text is converted explicitly, with the function of its column's
type: `date.fromisoformat` for a date column, `datetime.fromisoformat` for a timestamp
column, with an offset (an aware value) for a column with a time zone.

```python
import datetime as dt

day = dt.date.fromisoformat("2026-11-20")  # a date column
started = dt.datetime.fromisoformat("2026-11-20T09:30:00")  # a timestamp, no zone
recorded = dt.datetime.fromisoformat("2026-11-20T09:30:00+00:00")  # a zoned timestamp
print(Condition.of(day=day, started=started, recorded=recorded))
# day=datetime.date(2026, 11, 20), started=datetime.datetime(2026, 11, 20, 9, 30), recorded=datetime.datetime(2026, 11, 20, 9, 30, tzinfo=datetime.timezone.utc)
```

- **One mechanism.** `where()` adds to the conditions given with `where=` (a mapping or a
  `Condition`) when the source was built (`Condition.combined`), and SpiralDB applies them
  in the scan. A column already restricted to another value
  is refused, as is a column the table does not have, `None` (a null equals nothing) and
  `where()` with no condition. Everything else of the source is carried over: the pinned
  table version, `limit`, `max_rows`, the tags and data-tags, `tags_as_data`, the key
  columns, the source id and the label. The original source is not changed.
- **No conversion.** A value must be exactly of its column's Python type: an `int` for an
  integer column (not `True`, not `1.0`, not a NumPy scalar: use `.item()`), a `float` for a
  float column, a `datetime.date` for a date column and a `datetime.datetime` for a timestamp
  column (not a string; see below), and it must fit the column's Arrow type. A timestamp
  column with a time zone takes an aware `datetime`, one without a zone a naive `datetime`.
  Anything else raises `TypeError`, naming the column and both types. Columns of other types
  (lists, structs) take no condition.
- **The table system's equality.** Each condition is sent to SpiralDB as `column == value`,
  and which rows it selects is SpiralDB's own equality, not Orcapod's. For a float column, NaN
  selects the NaN rows and `0.0` does not select the `-0.0` rows (seen on the service, not
  documented by it). A value that no `float32` is exactly, such as `0.1`, selects no row of a
  `float32` column. SpiralDB cannot compare a `float16` column, so Orcapod reads the rows the
  other conditions select and compares them itself, by the same rule and before `limit`. To
  select rows by such values as Orcapod matches them, filter
  inside the pipeline (`PolarsFilter`). A NaN or infinite value is saved as
  `{"float": "nan"}`, `{"float": "inf"}` or `{"float": "-inf"}`, since JSON has no such number.
- **One restriction per table in a pipeline.** A restricted source has the unrestricted
  source's content hash, and each call to `where()` returns a new source object.
  Call `where()` once and reuse its result when several parts of the pipeline need
  the same restriction. If two distinct source objects with the same content hash
  enter a pipeline, Orcapod raises `InconsistentSourceError` even when the two
  restrictions are equal. The one-restriction check also looks through wrappers
  such as `CachedSource` and bound `SourceProxy` objects. It rejects different
  restrictions on the same source, including a restricted and an unrestricted read.
- **With `limit=`.** A source built with `limit=n` reads the first `n` rows that match, in
  key order: the conditions are applied first.
- **Conditions as values.** Two conditions are equal when they require the same columns to
  equal the same values, in any order. Values of different types differ (`1`, `True` and
  `1.0`); floats compare as Orcapod matches them (`0.0` equals `-0.0`, NaN equals NaN); an
  aware `datetime` is its instant, so one instant written in two zones is one value, and the
  two 01:30 of a daylight-saving overlap (`fold`) are two. This equality decides whether two
  restrictions are one restriction (for the one-restriction rule above); it does not decide
  which rows a source returns.
  `str(condition)` (or `condition.text()`) reads `unit_id=106, session_index=2`, and
  `condition.equalities` maps each column to its value. A `Condition` has a Python hash,
  consistent with that equality, so it can be a dictionary key or a set member; it never
  enters an Orcapod source or pipeline hash.
- **Recorded.** `RunSummary.conditions` maps the slot of each source read under a condition
  to its `Condition`, `RunSummary.limits` the slot of each source read with a `limit=` to the
  limit, and `RunSummary.restricted_source_ids` every such slot to its source id; the
  summary shows them in alphabetical order by slot name or direct source label
  (`spikes (spikes): where unit_id=106, limit 3`). A saved job records them in
  the source's config, and a loaded job reads with them. A slot's new restriction need
  not match its previous one: a restricted source fits the slot of the unrestricted one, and
  `bind` does not compare the two. But a bind after which two slots would read one source
  identity under different restrictions is refused before any binding is replaced (one
  restriction per table, above). A slot keeps one entry: a slot bound to a stream over two
  restricted tables (a `semi_join` of two restricted sources) reports one condition and one
  limit, those of the first table found (a known limit, ITL-1293), although every check reads
  both.
- **No cache over a restriction.** `CachedSource` (and `source.cached(...)`) refuses a
  restricted source with `ValueError`, naming the restriction: a cache keeps the table's rows
  under the table's identity, which a restriction is not part of, so a restricted read would
  be read back as the whole table by every cache of it. Cache the unrestricted source, or read
  without a cache. For the same reason an operator node that caches (`CacheMode.LOG` or
  `CacheMode.REPLAY`) is refused when a source feeding it is restricted: its cache is kept
  under the node's identity, so a log written from a restricted read would be replayed for
  another restriction or none. The `ValueError` names the node and each restriction, and is
  raised when both are known, before anything is read or written: when the cache mode is set
  over a restricted source, when `bind` binds a restricted source under a caching node, or
  else when a run starts. Use `CacheMode.OFF` there, or read the unrestricted source. A
  function node stores its results row by row and is not concerned.
- **Under an aggregation.** A `GroupBy`, `Batch` or `MergeJoin` below a restricted source
  builds its groups from the rows the restriction lets through. That is allowed (each
  aggregated row lists its members), and a job run issues one
  `RestrictedAggregationWarning` listing each source and step in alphabetical order by slot
  name or direct source label, then by step label.
- **Stored rows stay.** A run over a restricted source deletes nothing: a job node shows the
  rows of its current run, and `node.as_source()` shows everything stored for it.

`where()` is a copy of the source with the restriction applied to the copy; a later version
offers the two steps on their own, and restrictions on other kinds of source.

**Running again.** A run first checks the source: when the table has a newer version (rows
added since the source was built or last checked), the source reads that version from then
on, and the job's nodes below it run. The added rows are new keys, the only rows computed;
the rows already seen are reused. With nothing added, a second run computes nothing. A
source given `asof=` keeps its version: build a new source to read a later one.

**Key columns as data.** A function pod never receives a tag, and in a time-series table the
time is part of the key, so it is a tag. `tags_as_data` also offers named key columns as data
columns, copied as each batch is read (so the source still reads no rows when built, and
`limit=`, `where=`, `max_rows` and the notebook preview work as before). The tag keeps its
name; you name the copy, which comes after the table's data columns, with the key column's
type, never null:

<!-- docs-test: skip -->
```python
spikes = SpiralDBTableSource(
    "edi-tutorial-v1-841226", "spike_event", tags_as_data={"timestamp": "spike_time_us"}
)
unit_key = [
    "spike_extraction_hash", "subject_id", "session_start_datetime",
    "recording_index", "probe_index", "unit_id",
]
per_unit = spikes.group_by(unit_key)
print(per_unit.keys())
# (('spike_extraction_hash', 'subject_id', 'session_start_datetime', 'recording_index', 'probe_index', 'unit_id'), ('spike_amplitude_uv', 'spike_time_us'))
```

`per_unit` has one row per unit, and its `spike_time_us` list is in time order: `group_by`
orders the members of a group by the tags outside the group key, and on the full unit key
the only such tag is `timestamp`. The `timestamp` tag itself is dropped, as `group_by` drops
every tag outside the key; its copy `spike_time_us` is data and stays. `group_by(["unit_id"])` alone sorts by
`spike_extraction_hash`, subject, session, recording, probe, then `timestamp`, so time order
holds only within one session and probe; group on the full unit key.

The copies are data columns, so they are part of the schema, the `content_hash` and the
`pipeline_hash`; the mapping itself is part of the `content_hash` too, because two mappings
can give the same schema while the copy holds different values. The `source_id` and every
row's record id are the same as without the option. Compared with
`DerivedSource(spikes, tags_as_data=...)`, which reads the whole table as soon as its schema
or rows are asked for (building a job asks for the schema), the option keeps a Spiral source,
whose schema is known without a scan.

**Known limitations.**

- Each row's key is hashed with one call, about 0.2 ms per row (about 18 s per 100,000
  rows), in addition to about 0.1 ms per row to build the row.
- A job keeps its rows in memory (see the row cap above).
- Only equality on column values can be pushed down; there is no key range, no column
  selection (use `SelectDataColumns` after the source), and no polling for new rows within
  one run.
- A table that has only its key columns (for example one never written to) has no data
  column and cannot be a source.

#### The schema of a Spiral table

SpiralDB reports every column as nullable, key columns included, and every field of a struct
column too, whatever nullability the data was written with; and it keeps no Arrow metadata.
Orcapod therefore reads each table with a schema that is recorded in one more table of its
dataset, `__orcapod_schemas__`: a row per table, with its column names, types, nullability at
every depth and metadata. The name is reserved, and listings leave it out.

- **A recorded table** is read with exactly that schema. A non-nullable column has the type
  `T`, so a function pod can take `x: float`. Every batch read is checked: a null in such a
  column raises `NullInNonNullableColumnError` naming the table and the column, so a wrong
  record never hides a null.
- **A table that disagrees with its row** is refused with `TableSchemaError`, which names both
  sides: other columns or types, other key columns, another name. The row belongs to the
  table's id, so a table dropped and created again is not read with the old row. An older
  version (`asof=`) whose columns differ from the row is refused too.
- **A table that is not recorded** (its dataset has no `__orcapod_schemas__`, or no row for
  it) reads as SpiralDB reports it: every column has the type `T | None` and no metadata, and
  a function pod must accept `None` (`x: float | None`). `UnrecordedTableSchemaWarning` says
  so, usually once per table and process (threads reading it at the same time may each
  show it; once shown, it is not shown again).
- **A record that is not complete** is refused: the last row written says for which project
  and dataset, how many tables, and a digest of their rows. When it is missing or does not
  fit, every read that opens a table of the dataset raises `TableSchemaError`, and so does a
  schema table that cannot be read. A connector's table handle that has already read one
  version keeps the schema it read it with.
- **Joins are type strict.** A key column that is `T` in a recorded table and `T | None` in one
  that is not is an error, not a conversion.

The schema is part of a source's `pipeline_hash` and `content_hash`. So a table that becomes
recorded with non-nullable columns gets other hashes, as do the nodes built on it: they compute
again, and nothing stored is changed or deleted.

**Tables written by an Orcapod release on pyspiral** kept this in the table's own metadata,
which spiral-sdk does not read. Until their dataset is migrated they read as not recorded.
`scripts/migrate_spiral_schemas.py` migrates a dataset once: it records for every table the
schema that release read it with, so schemas, hashes and source ids stay the same
([the steps](https://github.com/metamorphic-labs/orcapod-python/blob/main/RELEASING.md#migrating-a-spiraldb-dataset-from-pyspiral)).

### `DerivedSource`

A `DerivedSource` is a source whose rows come from something Orcapod already has: another
source, or a node of a job. It has two uses. It is [provisional](#provisional-sources) in both
forms: its identity may change in a later release.

**Reading a job's results.** A `DerivedSource` reads the stored results of a node of a
[`PipelineJob`](function-pods.md#pipelinejob-database-backed-cached-execution) that has run
and presents them as a new source. You get one by calling `as_source()` on the node, for
example `job.nodes["bmi"].as_source()`. This is useful for chaining pipeline stages: run a
job, then use one of its outputs as the input of a new pipeline. The `as_source()` of a node
downstream of a `Join` or `MergeJoin` of two sources cannot yet be bound as a source of a new
`PipelineJob`: its schema is the node's, which declares a pair of system tags for each joined
source, and binding accepts exactly one pair, so it is refused with `SourceSpecMismatchError`
(support is planned, ITL-1127).

**Using a tag value as an input to a computation.** Tags identify a row and data is what a
function computes on. A data-tag (the default for a declared tag) is both: a function
reads it through its data role, under the same name (see
[Data-tags](data-tags.md)). A plain tag is a tag only, and a function pod
never receives it. When a plain tag's value is also needed as data (a session's start time
identifies the session, and it is also what you subtract to get an elapsed time), or you want
the value as a data column under another name, pass `tags_as_data` to get a source that keeps
the tag and also offers its value as a data column under a name you choose:

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
    tag_columns=["subject_id", "session_start", "experiment_index"], tag_only_columns=True,
    source_id="experiments",
)

with_start = DerivedSource(
    experiments,
    tags_as_data={"session_start": "session_start_value"},
    source_id="experiments_with_start",
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
- The copies follow the origin's data columns, in the order of their new names. The mapping
  names a set of copies: the order you give it in changes neither the columns, their order,
  nor any hash.
- The copy has exactly the tag's type, including whether it may be null. If the tag may be
  null, annotate the function parameter as optional (`datetime | None`).
- The copy equals the tag when the source is read. After that it is ordinary data: a function
  may replace it and `map_data` may rename it, while the tag stays as it was.
- It works on any source (`DerivedSource(source, tags_as_data=...)`) and on a job node
  (`job.nodes["bmi"].as_source(tags_as_data=...)`).
- It is a **new source**, not a step in the origin's pipeline: `upstreams` is empty, and it
  has provenance columns of its own, like the result of handing the same rows to
  `ArrowTableSource`. Use it in place of the origin. It needs a `source_id`: there is no
  default, and building one without it raises `ValueError` before the origin is read
  (ITL-1313); `as_source(tags_as_data=..., source_id=...)` takes it too. The source is
  identified by that name and the mapping, like any named source: the mapping is part of its
  `content_hash`, so two mappings never share one.
- A saved job reloads a derived source whenever its origin can be reloaded (a CSV file, a
  Delta table, a database table). Over an in-memory source it is saved but not reloaded, like
  the in-memory source itself.

```python
print(with_start.upstreams)
# ()
print(with_start.source_id)
# derived_v1:experiments_with_start
```

Only a source can do this. An operator rearranges rows and columns, and every data value it
emits was already a data value of its input; turning a tag value into a data value creates
new data, which is what a source is for.

### Provisional sources

Data context v1 freezes the hashes of Orcapod at the 0.4.0 release. A provisional source is
outside that freeze: its default `source_id` and its content hash are still being decided, so
a later release may change them, and with them the system tags, stored paths and record ids
of its rows and of every step downstream of it, which then compute their results again
(nothing stored is lost or wrong). It works as any source does otherwise.

```python
from orcapod.sources import PROVISIONAL_SOURCES

print(sorted(PROVISIONAL_SOURCES))
# ['DerivedSource', 'PollingSource']
```

| Source | What is still open |
|---|---|
| `PollingSource` | its default source id hashes the implementation's own identity, and its content hash leaves out the declared schema (ITL-1316); it holds the source id (ITL-1425) |
| `DerivedSource`, both forms | the plain form (`node.as_source()`) hashes only its origin's content hash and accepts a `source_id` its identity does not use; the `tags_as_data` form changed just before this release (ITL-1317) |

So everything built on a `DerivedSource`, whether it came from `node.as_source()` or from
`DerivedSource(origin, tags_as_data=...)`, is provisional too: every step downstream of it
keeps its hashes only until its identity is decided.

Building one, directly, through `node.as_source()` on a function or operator node, from a
saved config or by loading a saved job, issues `orcapod.errors.ProvisionalHashWarning` once
per source kind per process; filter its class to silence it, as for
[provisional operators](operators.md#provisional-operators).

## What a source does at a run

A job node that has run holds its rows until it is stale, and staleness comes from modified
times only (see [Running one node](function-pods.md)). So every run (`job.run()`,
`run_async()`, a job node's `run()`, a load-only run too) first asks each source feeding it,
once, whether its data changed: `source.check_for_new_data()` (developer, November 14, 2026,
ITL-1102). A source that finds new data takes it in and moves its modified time to that
moment, so every node below it runs; a source that finds nothing changes nothing, so a job
whose sources are unchanged computes nothing. `head()` and the notebook preview never check,
and neither does a source's own `run()` outside a job (`source.run()` does what
`source.as_table()` does); call `source.check_for_new_data()` to check a source by hand.

| Source | What the check does |
|---|---|
| `ArrowTableSource`, `DictSource`, `ListSource`, `DataFrameSource` | nothing: data held in memory cannot change; build a new source and bind it to run on new data |
| `PollingSource` | polls once (and fetches when the poll reports new data); its modified time moves only when the fetch returned rows, to the moment it found them (a cursor's `modified_at` is informational) |
| `DeltaTableSource` | reads the Delta log only; when the table's version moved, reads the table again and, if its rows changed (row order counts only without key columns, see below; a version that only compacted files changes nothing), holds the new rows |
| `SpiralDBTableSource` | holds no rows (every read scans the pinned version); reads the table's current version (no row of it, and `max_rows` does not apply) and pins it when it is newer, so the next read returns the new rows; an `asof` you gave never moves; record ids are the canonical JSON of the key columns, so only new or changed rows are new entries (a version that moves without a change, if SpiralDB moves it on compaction, re-runs the nodes below and reuses every stored result) |
| `CSVSource` | reads the file's modification time and size; when either changed, reads the file again and holds the new rows if they changed (row order counts only without key columns) |
| `SQLiteTableSource`, `PostgreSQLTableSource`, `DBTableSource` | reads the whole table again and holds the new rows if they changed (with key columns in any order, so an unordered `SELECT` is not a change) |
| `CachedSource` | checks the source it wraps; when that source's modified time moved since the cache last took in its rows (also when another source over it made the check), compares the rows' record hashes with the rows it presents (before its first read, with the cache: a read; nothing is written) and moves only when it will present new rows, (a row removed from the wrapped source stays cached); the check guarantees nothing for two cached sources that share one cache (the same `cache_path` or table): give each its own cache (ITL-1143) |
| `DerivedSource` | over a source, checks it; reads the origin's rows again when the origin's modified time moved since this source read them (a source that took in new data, a job node that ran again); holds the new rows if they changed; one that holds no rows (over a job node that had not run) moves when its origin's time moves |
| `SourceProxy` | checks the source bound to it; its modified time follows that source's, and binding or unbinding moves it |

Rows read again are compared with the rows read before, as read from the table or file
(every column, a `system_tag_columns` column too), value by value, with a
NaN equal to a NaN (and -0.0 equal to 0.0), so a table holding a NaN is not a change at every
check. A check that raises part way changes nothing, so the next run checks again.

Row order follows the source's record id rule (developer, November 14, 2026). Without
key columns a row's record id is its position: a change of row order is a change, so
the source takes the rows in their new order and every row gets the id of its new position
at once (the nodes below run again and reuse every stored function result, because results
are stored by the hash of the row's data). With key columns, only content counts:
the same rows in another order are no change. A source that cannot tell which rule applies
counts order. The model is deliberately simple; a fuller design is ITL-1103 / ITL-1104.

A database table has no cheap and exact sign of a change, so each check of a SQLite or
PostgreSQL source reads the whole table, one full read per run, as building the source did.
For a large table, build the source over a narrower table or view, or keep the data in a
store that records versions: a Delta check whose version did not move reads only the Delta
log (one whose version moved reads the table again, as above), and a SpiralDB check reads
only the table's metadata.

A table or file whose columns changed (a CSV file's types are inferred again on each read,
so a null in a column that had none is a change of type), a table that is gone or empty, or a
SpiralDB table dropped and created again, fails the check: the source's schema and table are
part of its identity, so build a new source to read it. A column that loses its nulls keeps
its nullable type (where nullability is inferred from the rows: a CSV file read without
`schema=`, a database whose connector gives no column information), so it is not a
change of columns.

A check that raises does not fail the run. The run issues one `SourceCheckFailedWarning` per
source, naming the source and the error; the source keeps its data and modified time; the
run proceeds with the data the source already holds, and its `RunSummary` lists the source
in `unchecked_sources` ("sources not checked for new data (see the warning): ..."). A job
node's `run()` that then runs nothing (the node holds a current run) makes no summary and
keeps its `last_run`: the warning is the report. A later failure to read the source raises
as it always has. A `PollingSource` reads by polling: a run that has to read it (a node
below it runs) polls it again, and a poll that fails again raises, as it did before the
check existed; a run whose nodes are all current proceeds. Reading a polling source's held
rows after a failed check is ITL-1139.

A source that takes in new data keeps its record id rule. Without key columns, a
row's record id comes from its position, so a row that moves (a reorder or rows inserted
before it) gets another id and is recorded as a new entry; its function result is still reused, because
results are stored by the hash of the row's data. Give a source that will take in new data
key columns that are unique per row (ITL-1104). A `PollingSource` requires them: its
rows arrive in batches, so a key identifies a polled row, and a batch that repeats a key
already polled is refused. Further revisions of what each kind checks are collected in
ITL-1103.

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

Each row carries two system tags: the source's `source_id` and the row's record ID, the row
identifier itself (ITL-1154). By default the record ID of a row of an in-memory, CSV or Delta
source is its position, `row_<n>`, so a row that moves to another position under the same
`source_id` gets a different record ID; a database or SpiralDB table names its rows by their
primary key, as canonical JSON (`{"trial":17}`). Downstream pipeline entries
and `sink_pod` completion records are keyed on the system tags together with a hash of the
row's data, so the moved row is treated as a new entry: a function node records it again,
and its stored table then shows two entries with identical tag values but distinct record
IDs, and a `sink_pod` delivers it again. Because every key includes the hash of the row's own
data, no result is ever paired with the wrong row; a function's stored result for that data
is reused rather than recomputed.

Every source accepts `record_id_columns`, one or several key columns whose values name the
row instead (`{"unit":"u1","time":1700000000}`), so the record ID follows the row. Both
system tags must be stable: when you omit `source_id` an in-memory source is named after a
hash of its table, as `<kind part>:<algorithm label>:<hex>` (for example
`dict_v1:arrow_v1:000002fe…`), which changes when any row changes or is reordered. Whenever row
order or content can vary between runs, pass an explicit, stable `source_id` **and** name
key columns that are unique per row; this avoids both the duplicate entries and the repeated
deliveries.

Sources with one `source_id` must agree on their Arrow schema and key columns.
Differences raise `InconsistentSourceError` during capture, binding, loading and
execution through `job.run()` or a node's `run()`. ITL-1506 tracks support for
distinct readings of one dataset in distinct named slots. Give each reading its
own `source_id` for now.

An in-memory source with key columns and no `source_id` can instead hash its rows in key
order: pass `hash_in_key_order=True` (`ArrowTableSource`, `DictSource`, `DataFrameSource`,
`ListSource`; ITL-1272). The table is hashed sorted by the key columns, in the order you list
them, so the same rows in any order give one default `source_id` and one content hash; the
rows are still presented in the order you gave. It is off by default, because the order you
gave may matter to you. It is refused without key columns (there is no key order) and with a
`source_id`: the table hash is still computed then, but it does not determine a given
`source_id` and enters no identity, so the argument would change nothing. The order of the key
columns is part of the content hash either way; it changes the default `source_id` only when
the two orders sort the table differently.

```python
from orcapod.sources import DictSource

rows = [
    {"subject": "s1", "trial": 1, "weight": 20.5, "count": 3},
    {"subject": "s1", "trial": 2, "weight": 22.25, "count": 1},
    {"subject": "s2", "trial": 1, "weight": 19.0, "count": 4},
]
orders = (rows, rows[::-1], [rows[1], rows[2], rows[0]])
keyed = [
    DictSource(order, tag_columns=["subject", "trial"],
               record_id_columns=["subject", "trial"], hash_in_key_order=True)
    for order in orders
]
print(len({s.source_id for s in keyed}), len({s.content_hash() for s in keyed}))
# 1 1
print(keyed[1].as_table().column("weight").to_pylist())
# [19.0, 22.25, 20.5]
other_key_order = DictSource(rows, tag_columns=["subject", "trial"],
                             record_id_columns=["trial", "subject"],
                             hash_in_key_order=True)
print(other_key_order.source_id == keyed[0].source_id)
# False
as_given = [
    DictSource(order, tag_columns=["subject", "trial"],
               record_id_columns=["subject", "trial"])
    for order in orders
]
print(len({s.source_id for s in as_given}))
# 3
```

A source given a `source_id` is an address: its content hash is built from its class, schema,
`source_id` and key columns, never from its rows, so changing the rows changes it only when the
schema changes (a column of another type). This holds for `ListSource` too (developer,
November 21, 2026, ITL-1272): its elements and its tag function enter its identity only through
its default `source_id` and its output schema.

These columns are hidden by default. You can reveal them using `ColumnConfig`:

```python
from orcapod.sources import DictSource

source = DictSource(
    data=[{"subject_id": "mouse_01", "age": 12, "weight": 25.3}],
    tag_columns=["subject_id"], tag_only_columns=True,
    source_id="mice",
)

# Show source-info columns
table = source.as_table(columns={"source": True})
print(table.column_names)
# ['subject_id', 'age', 'weight', '_source_age', '_source_weight']
print(table.column("_source_weight").to_pylist())
# ['dict_v1:mice::row_0::weight']

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
Orcapod derives an id from a hash of the source's table, as described above.

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
    tag_columns=["subject_id"], tag_only_columns=True,
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
for tag, data in source:
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
