# Missing values

Real tables have gaps: a measurement that was never taken, a cell someone forgot to fill in.
Orcapod keeps track of which columns may hold a missing value, which it calls a **null**, and
refuses to pass one to a function that does not expect it. This tutorial shows where nulls
come from and what to do about them.

**Before you start:** Parts 0 to 3 of the [core course](index.md#core-course).

You will:

- read a type that may be null, `T | None`
- drop the rows with a null, with `drop_nulls`
- see what `join` does with a null in a tag column
- write a function that receives and returns `None`

```python
from workshop_data import make_dataset
from orcapod.sources import CSVSource

data_dir = make_dataset()

sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"], tag_only_columns=True,
    source_id="sessions",
)
```

## A table with gaps

The workshop dataset has no missing values, so add a small file with some. Suppose the lab
measured, after the experiment, the length of each probe's track through the brain. One
probe's track was not measured, and one row lost its session:

```python
(data_dir / "histology.csv").write_text(
    "subject_id,session,probe,track_um\n"
    "mouse_01,s1,imec0,3100\n"
    "mouse_01,s1,imec1,\n"
    "mouse_01,s2,imec0,2950\n"
    "mouse_02,,imec0,2800\n"
)
histology = CSVSource(
    str(data_dir / "histology.csv"),
    tag_columns=["subject_id", "session", "probe"], tag_only_columns=True,
    source_id="histology",
)
print(histology.output_schema())
# (Schema({'subject_id': str, 'session': str, 'probe': str}), Schema({'track_um': int | None}))
```

`track_um` has the type `int | None`: an integer, or a null. The empty cell in that column is
read as a null. The empty `session` cell is not: `session` holds text, and an empty cell in a
text column read this way is the empty string `""`. The next sections come back to that.

`head()` shows the rows; a null shows as `null`:

```python
print(histology.head())
# CSVSource[histology] — all 4 rows
# * = tag
# shape: (4, 4)
# ┌─────────────┬──────────┬────────┬──────────┐
# │ *subject_id ┆ *session ┆ *probe ┆ track_um │
# │ ---         ┆ ---      ┆ ---    ┆ ---      │
# │ str         ┆ str      ┆ str    ┆ i64      │
# ╞═════════════╪══════════╪════════╪══════════╡
# │ mouse_01    ┆ s1       ┆ imec0  ┆ 3100     │
# │ mouse_01    ┆ s1       ┆ imec1  ┆ null     │
# │ mouse_01    ┆ s2       ┆ imec0  ┆ 2950     │
# │ mouse_02    ┆          ┆ imec0  ┆ 2800     │
# └─────────────┴──────────┴────────┴──────────┘
```

## A function that does not expect a null

This function converts a track length to millimetres. Its parameter is a plain `int`:

```python
from orcapod import function_pod


@function_pod(output_keys="track_mm")
def to_mm(track_um: int) -> float:
    return track_um / 1000


try:
    to_mm.pod(histology)
except ValueError as err:
    print(err)
# Incoming data schema Schema({'track_um': int | None}) is not compatible with expected input schema Schema({'track_um': int}): column 'track_um' has type int | None, the function expects int
```

The pod is refused when you build it, before any row is read: the column may hold a null, and
the function says it cannot take one. You have two choices: drop the rows with a null, or let
the function take `None`.

## Dropping the rows with a null

`drop_nulls` keeps the rows where the named columns hold a value. The column's type loses its
`| None`, so the function now accepts the stream:

```python
measured = histology.drop_nulls("track_um")
print(measured.output_schema())
# (Schema({'subject_id': str, 'session': str, 'probe': str}), Schema({'track_um': int}))
print(measured.as_table().num_rows)
# 3

lengths = to_mm.pod(measured)
print(lengths.run().sort_by_tags())
# FunctionPodStream[to_mm] — all 3 computed rows, sorted by tags
# * = tag
# shape: (3, 4)
# ┌─────────────┬──────────┬────────┬──────────┐
# │ *subject_id ┆ *session ┆ *probe ┆ track_mm │
# │ ---         ┆ ---      ┆ ---    ┆ ---      │
# │ str         ┆ str      ┆ str    ┆ f64      │
# ╞═════════════╪══════════╪════════╪══════════╡
# │ mouse_01    ┆ s1       ┆ imec0  ┆ 3.1      │
# │ mouse_01    ┆ s2       ┆ imec0  ┆ 2.95     │
# │ mouse_02    ┆          ┆ imec0  ┆ 2.8      │
# └─────────────┴──────────┴────────┴──────────┘
```

Give `drop_nulls` several names to test several columns, as a list
(`drop_nulls(["session", "track_um"])`). With no names it tests every tag and data column. It
reads only whether a value is there, never the value itself. See
[DropNulls](../concepts/operators.md#dropnulls) for the full rules.

## A function that takes `None`

Dropping rows loses them. To keep every row, annotate the parameter as `int | None` and decide
in the function what a missing length gives. Here it gives a missing result: the function
returns `None`, so its output is declared `float | None` too:

```python
@function_pod(output_keys="track_mm")
def to_mm_or_none(track_um: int | None) -> float | None:
    if track_um is None:
        return None
    return track_um / 1000


all_lengths = to_mm_or_none.pod(histology)
print(all_lengths.output_schema())
# (Schema({'subject_id': str, 'session': str, 'probe': str}), Schema({'track_mm': float | None}))
print(all_lengths.run().sort_by_tags())
# FunctionPodStream[to_mm_or_none] — all 4 computed rows, sorted by tags
# * = tag
# shape: (4, 4)
# ┌─────────────┬──────────┬────────┬──────────┐
# │ *subject_id ┆ *session ┆ *probe ┆ track_mm │
# │ ---         ┆ ---      ┆ ---    ┆ ---      │
# │ str         ┆ str      ┆ str    ┆ f64      │
# ╞═════════════╪══════════╪════════╪══════════╡
# │ mouse_01    ┆ s1       ┆ imec0  ┆ 3.1      │
# │ mouse_01    ┆ s1       ┆ imec1  ┆ null     │
# │ mouse_01    ┆ s2       ┆ imec0  ┆ 2.95     │
# │ mouse_02    ┆          ┆ imec0  ┆ 2.8      │
# └─────────────┴──────────┴────────┴──────────┘
```

The function receives `None` for the missing cell, and the `None` it returns is stored as a
null. A function declared `-> float` that returns `None` fails for that row instead
([Handling errors](handling-errors.md) shows what a job does with a failed row).

## A null in a tag column

Back to the row that lost its session. To read an empty text cell as a null, give the source
the column types with `schema=` (an Arrow schema, one field per column of the file, in the
file's order). A field is nullable unless you say `nullable=False`:

```python
import pyarrow as pa

histology_schema = pa.schema([
    pa.field("subject_id", pa.string(), nullable=False),
    pa.field("session", pa.string()),
    pa.field("probe", pa.string(), nullable=False),
    pa.field("track_um", pa.int64()),
])
histology = CSVSource(
    str(data_dir / "histology.csv"),
    tag_columns=["subject_id", "session", "probe"], tag_only_columns=True,
    source_id="histology",
    schema=histology_schema,
)
print(histology.output_schema())
# (Schema({'subject_id': str, 'session': str | None, 'probe': str}), Schema({'track_um': int | None}))
```

Now `session` is a tag that may be null, and the fourth row's session is `None`. Join the table
with `sessions`, which gives each session its date and duration:

```python
with_sessions = histology.join(sessions)
print(with_sessions.output_schema()[0])
# Schema({'subject_id': str, 'session': str, 'probe': str})
print(with_sessions.run().sort_by_tags())
# DynamicPodStream[Join] — all 3 computed rows, sorted by tags
# * = tag
# shape: (3, 6)
# ┌─────────────┬──────────┬────────┬──────────┬────────────┬────────────┐
# │ *subject_id ┆ *session ┆ *probe ┆ track_um ┆ date       ┆ duration_s │
# │ ---         ┆ ---      ┆ ---    ┆ ---      ┆ ---        ┆ ---        │
# │ str         ┆ str      ┆ str    ┆ i64      ┆ date       ┆ f64        │
# ╞═════════════╪══════════╪════════╪══════════╪════════════╪════════════╡
# │ mouse_01    ┆ s1       ┆ imec0  ┆ 3100     ┆ 2026-10-05 ┆ 60.0       │
# │ mouse_01    ┆ s1       ┆ imec1  ┆ null     ┆ 2026-10-05 ┆ 60.0       │
# │ mouse_01    ┆ s2       ┆ imec0  ┆ 2950     ┆ 2026-10-12 ┆ 120.0      │
# └─────────────┴──────────┴────────┴──────────┴────────────┴────────────┘
```

The row with no session is gone: **a null in a tag column matches nothing**, not even a null in
the other stream. The join is inner, so a row without a partner is dropped, and no error is
raised. The other null, in the data column `track_um`, is carried through.

The joined `session` is `str`, not `str | None`: `sessions` declares it `str`, and a row whose
key is null cannot be in the result. [Join](../concepts/operators.md#join) has the full rule for
joins on keys that may be null.

If a row disappears from a join and you do not see why, look for a null in the tag columns the
two streams share. In a job, `node.why_missing` names the step that removed a row
([Tracing a value](tracing-a-value.md)).

## Key points

- A column that may hold a null has the type `T | None`.
- A function pod whose parameter is `T` refuses a `T | None` column when you build it.
- `drop_nulls` keeps the rows where the named columns hold a value and makes their type `T`.
- A function annotated `T | None` receives `None`; one that returns `None` declares its output
  `T | None`, and the `None` is stored as a null.
- A null in a tag column matches nothing in a join, and the row is dropped without an error.
- An empty cell in a CSV text column is the empty string unless you declare the column's type
  with `schema=` ([Sources](../concepts/sources.md#a-csv-files-types-schema) has the full table of what reads as null).

## Exercises

**1.** Drop the rows of the second `histology` (the one read with `schema=`) that have a null
in any column. How many rows are left, and what is the type of `session` afterwards?

??? success "Solution"
    With no names, `drop_nulls` tests every tag and data column.

    ```python
    complete = histology.drop_nulls()
    print(complete.as_table().num_rows)
    # 2
    print(complete.output_schema()[0])
    # Schema({'subject_id': str, 'session': str, 'probe': str})
    ```

**2.** Write a function pod that takes `track_um` (which may be null) and returns `True` when
the track was measured, `False` when it was not. How many rows does its output have?

??? success "Solution"
    A function that takes `None` can return an ordinary value for it, so the output is not
    nullable.

    ```python
    @function_pod(output_keys="measured")
    def was_measured(track_um: int | None) -> bool:
        return track_um is not None


    flags = was_measured.pod(histology)
    print(flags.output_schema()[1])
    # Schema({'measured': bool})
    print(flags.as_table().num_rows)
    # 4
    ```
