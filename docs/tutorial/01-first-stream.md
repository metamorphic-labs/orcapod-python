# Part 1: Your first stream

An analysis usually starts with tables. Orcapod needs to know two things about each table:
which columns identify a row, and which columns hold the values. This part shows how to load
a table and tell Orcapod both.

You will:

- load a CSV file and a list of Python dictionaries as **sources**
- choose **tag columns** and **data columns**
- look at a **stream** four ways: its schema, its column names, row by row, and as a DataFrame

```python
from workshop_data import make_dataset

data_dir = make_dataset()
```

## Loading a table

A **source** is where data enters Orcapod. `CSVSource` reads a CSV file:

```python
from orcapod.sources import CSVSource

subjects = CSVSource(
    str(data_dir / "subjects.csv"),
    tag_columns=["subject_id"], tag_only_columns=True,
    source_id="subjects",
)
print(subjects.as_pandas_df())
#   subject_id sex  age_weeks  genotype
# 0   mouse_01   F         12  wildtype
# 1   mouse_02   M         14  wildtype
# 2   mouse_03   F         11  knockout
# 3   mouse_04   M         13  knockout
```



- The file path is a string, so a `pathlib.Path` needs `str(...)`.
- `tag_columns` names the columns that identify a row.
- `tag_only_columns=True` makes those columns tags only, as the next section explains.
- `source_id` names the source. Orcapod uses it when it records where a value came from.
  Give every source a short, stable name.

## Tags and data

- **Tag columns** identify a row, like a primary key in a database. Orcapod uses them to
  match rows when it combines tables.
- **Data columns** hold the values.

Here a column is either a tag or data. `tag_only_columns=True` sets that; see
[Data-tags](../concepts/data-tags.md) for columns with both roles. You name the tag columns,
and all remaining columns are data columns. `output_schema()` returns both groups, with their
types:

```python
tag_schema, data_schema = subjects.output_schema()
print(tag_schema)
# Schema({'subject_id': str})
print(data_schema)
# Schema({'sex': str, 'age_weeks': int, 'genotype': str})
```

A **schema** is the list of column names and their types. `CSVSource` infers the types from
the file: `age_weeks` is an `int` because every value in that column is a whole number.

`keys()` gives only the names:

```python
tag_keys, data_keys = subjects.keys()
print(tag_keys)
# ('subject_id',)
print(data_keys)
# ('sex', 'age_weeks', 'genotype')
```

A table can need more than one tag column. Every mouse has a session called `s1`, so a
session is identified by the mouse and the session name together:

```python
sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"], tag_only_columns=True,
    source_id="sessions",
)
print(sessions.output_schema())
# (Schema({'subject_id': str, 'session': str}), Schema({'date': date, 'duration_s': float}))
```

## Looking at a stream

A **stream** is a read-only sequence of rows, each one a pair of (tag, data). Every step in
Orcapod takes streams and returns a stream. A source is the stream that brings data in.
`output_schema()` and `keys()`, and everything below, work on every stream.

Loop over a stream to get its rows. Call `.as_dict()` on a tag or a data object to get a
plain dictionary:

```python
for tag, data in sessions:
    print(tag.as_dict(), "->", data.as_dict())
# {'subject_id': 'mouse_01', 'session': 's1'} -> {'date': datetime.date(2026, 10, 5), 'duration_s': 60.0}
# {'subject_id': 'mouse_01', 'session': 's2'} -> {'date': datetime.date(2026, 10, 12), 'duration_s': 120.0}
# {'subject_id': 'mouse_02', 'session': 's1'} -> {'date': datetime.date(2026, 10, 5), 'duration_s': 90.0}
# {'subject_id': 'mouse_02', 'session': 's2'} -> {'date': datetime.date(2026, 10, 12), 'duration_s': 120.0}
# {'subject_id': 'mouse_03', 'session': 's1'} -> {'date': datetime.date(2026, 10, 5), 'duration_s': 90.0}
# {'subject_id': 'mouse_03', 'session': 's2'} -> {'date': datetime.date(2026, 10, 12), 'duration_s': 120.0}
# {'subject_id': 'mouse_04', 'session': 's1'} -> {'date': datetime.date(2026, 10, 5), 'duration_s': 120.0}
# {'subject_id': 'mouse_04', 'session': 's2'} -> {'date': datetime.date(2026, 10, 12), 'duration_s': 60.0}
```

To see the whole stream at once, turn it into a DataFrame. `as_pandas_df()` gives a pandas
DataFrame, `as_polars_df()` a Polars one, and `as_table()` a PyArrow table:

```python
print(sessions.as_pandas_df().head(3))
#   subject_id session       date  duration_s
# 0   mouse_01      s1 2026-10-05        60.0
# 1   mouse_01      s2 2026-10-12       120.0
# 2   mouse_02      s1 2026-10-05        90.0
print(sessions.as_table().num_rows)
# 8
```

A stream cannot be changed in place. To get different data, you build a new stream from it,
as Parts 2 to 4 show.

## Sources from Python data

`DictSource` takes a list of dictionaries, one per row:

```python
from orcapod.sources import DictSource

probes = DictSource(
    data=[
        {"probe": "imec0", "region": "V1", "n_channels": 384},
        {"probe": "imec1", "region": "CA1", "n_channels": 384},
    ],
    tag_columns=["probe"], tag_only_columns=True,
    source_id="probes",
)
print(probes.as_pandas_df())
#    probe region  n_channels
# 0  imec0     V1         384
# 1  imec1    CA1         384
```

Orcapod has more source types, for pandas and Polars DataFrames, Delta tables and SQL tables.
See [Sources](../concepts/sources.md).

## Key points

- Every step takes **streams** of (tag, data) rows and returns a stream. A **source** is the
  stream that brings data in.
- **Tag columns** identify a row; **data columns** hold the values. You choose the tags.
- Look at any stream with `output_schema()`, `keys()`, a `for` loop and
  `as_pandas_df()`.
- Streams are read-only.

## Exercises

**1.** `workshop_data/units.csv` has one row per sorted unit, with columns `subject_id`,
`session`, `probe`, `unit`, `region`, `depth_um` and `n_spikes`. Unit numbers restart at 0 on
every probe, probe names repeat in every session, and session names repeat for every mouse.
`region`, `depth_um` and `n_spikes` describe the unit. Load the file as a source called
`units`, with the tags that identify a row. How many rows does it have?

??? success "Solution"
    All four of `subject_id`, `session`, `probe` and `unit` are needed. The other columns
    are data.

    ```python
    units = CSVSource(
        str(data_dir / "units.csv"),
        tag_columns=["subject_id", "session", "probe", "unit"], tag_only_columns=True,
        source_id="units",
    )
    print(units.output_schema())
    # (Schema({'subject_id': str, 'session': str, 'probe': str, 'unit': int}), Schema({'region': str, 'depth_um': int, 'n_spikes': int}))
    print(units.as_table().num_rows)
    # 64
    ```

    `region` could also be a tag. It depends only on `probe` here, so it is not needed to
    identify a row. Part 3 shows how tags decide which rows are combined.

**2.** Two mistakes are common when a source is created. Make each one on purpose and read
the message: (a) name a tag column that does not exist in `subjects.csv`; (b) make every
column of `subjects.csv` a tag.

??? success "Solution"
    Both raise `ValueError` as soon as the source is created.

    ```python
    try:
        CSVSource(str(data_dir / "subjects.csv"), tag_columns=["mouse"], tag_only_columns=True)
    except ValueError as err:
        print(err)
    # tag_columns not found in table: {'mouse'}. Available columns: ['subject_id', 'sex', 'age_weeks', 'genotype']

    try:
        CSVSource(
            str(data_dir / "subjects.csv"),
            tag_columns=["subject_id", "sex", "age_weeks", "genotype"], tag_only_columns=True,
        )
    except ValueError as err:
        print(err)
    # No data columns found in the table. At least one data column is required.
    ```

    The first message lists the columns that exist. The second states a rule: a stream needs
    at least one data column.

**3.** Print the first three rows of `units` with a loop.

??? success "Solution"
    A stream is an ordinary Python iterable, so `itertools.islice` works on it.

    ```python
    from itertools import islice

    for tag, data in islice(units, 3):
        print(tag.as_dict(), "->", data.as_dict())
    # {'subject_id': 'mouse_01', 'session': 's1', 'probe': 'imec0', 'unit': 0} -> {'region': 'V1', 'depth_um': 1580, 'n_spikes': 149}
    # {'subject_id': 'mouse_01', 'session': 's1', 'probe': 'imec0', 'unit': 1} -> {'region': 'V1', 'depth_um': 2560, 'n_spikes': 326}
    # {'subject_id': 'mouse_01', 'session': 's1', 'probe': 'imec0', 'unit': 2} -> {'region': 'V1', 'depth_um': 600, 'n_spikes': 662}
    ```
