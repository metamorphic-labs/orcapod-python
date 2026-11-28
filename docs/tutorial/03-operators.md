# Part 3: Reshaping with operators

Function pods compute new values. **Operators** rearrange streams: they combine two tables,
keep some rows, and rename or drop columns. When a number in a result looks wrong, it was
computed by one of your functions or was already in the source.

You will:

- combine tables with `join`
- keep a subset of rows with `filter` and `semi_join`
- rename columns with `map_data`
- combine tables that have a data column of the same name

```python
from workshop_data import make_dataset
from orcapod.sources import CSVSource

data_dir = make_dataset()

subjects = CSVSource(
    str(data_dir / "subjects.csv"), tag_columns=["subject_id"], tag_only_columns=True, source_id="subjects"
)
sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"], tag_only_columns=True,
    source_id="sessions",
)
units = CSVSource(
    str(data_dir / "units.csv"),
    tag_columns=["subject_id", "session", "probe", "unit"], tag_only_columns=True,
    source_id="units",
)
```

Most operators are available as methods on a stream, which is the form used here. All of them
are classes in `orcapod.operators` (`Join`, `Filter`, ...); see
[Operators](../concepts/operators.md).

## Join

`join` matches rows of two streams that have the same values in the tag columns they share,
and puts their data columns side by side:

```python
unit_info = units.join(sessions)

print(unit_info.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('region', 'depth_um', 'n_spikes', 'date', 'duration_s'))
unit_tags = ["subject_id", "session", "probe", "unit"]
narrow = unit_info.select_data_columns(["n_spikes", "duration_s"])
print(narrow.as_pandas_df().sort_values(unit_tags, ignore_index=True).head(3))
#   subject_id session  probe  unit  n_spikes  duration_s
# 0   mouse_01      s1  imec0     0       149        60.0
# 1   mouse_01      s1  imec0     1       326        60.0
# 2   mouse_01      s1  imec0     2       662        60.0
```

The join uses every tag column the two streams share, here `subject_id` and `session`. The
result has the tags and the data columns of both.

- It is an **inner** join: a row with no partner in the other stream is dropped.
- If the streams share **no** tag column, every row is paired with every row.
- `units.join(sessions)` and `sessions.join(units)` give the same rows and columns.
- The order of rows and columns is not promised. Sort the rows when their order matters, as
  the samples here do, and refer to columns by name.

Joins chain. This attaches both the session and the animal to every unit:

```python
everything = units.join(sessions).join(subjects)
print(everything.keys()[1])
# ('region', 'depth_um', 'n_spikes', 'date', 'duration_s', 'sex', 'age_weeks', 'genotype')

```

## Filter

`filter` keeps the rows that satisfy a condition. Keyword arguments test equality:

```python
knockouts = subjects.filter(genotype="knockout")
print(knockouts.as_pandas_df())
#   subject_id sex  age_weeks  genotype
# 0   mouse_03   F         11  knockout
# 1   mouse_04   M         13  knockout
```

Join first to filter on columns from two tables. Several keywords must all hold:

```python
knockout_units = units.join(subjects).filter(genotype="knockout", probe="imec1")
print(knockout_units.as_table().num_rows)
# 16
```

For any other condition, name the column with `col`:

```python
from orcapod import col

busy = units.filter(col("n_spikes") > 1200)
print(busy.as_pandas_df().sort_values(unit_tags, ignore_index=True))
#   subject_id session  probe  unit region  depth_um  n_spikes
# 0   mouse_01      s2  imec0     2     V1       920      1221
# 1   mouse_01      s2  imec1     2    CA1       120      1205
# 2   mouse_02      s2  imec1     3    CA1      2360      1233
```

`col` offers `==`, `!=`, `<`, `<=`, `>`, `>=`, `is_in` for a set of values, `between` for a
range (both ends included), and `is_null` / `is_not_null`. Given several conditions and
keywords, `filter` keeps the rows where all of them hold. Combine conditions with `&` (and),
`|` (or) and `~` (not):

```python
shallow_s1 = units.filter(
    col("probe").is_in(["imec0", "imec1"]),
    col("depth_um").between(100, 500),
    session="s1",
)
print(shallow_s1.as_table().num_rows)
# 4
v1_or_busy = units.filter((col("region") == "V1") | (col("n_spikes") > 1200))
print(v1_or_busy.as_table().num_rows)
# 34
```

As in Polars, write `&`, `|` and `~` instead of `and`, `or` and `not`, and put each comparison
in parentheses. Python applies `&`, `|` and `~` before a comparison, so leaving the
parentheses out raises `TypeError`. With `and`, the first condition is lost without an error.

A value must have its column's type: `n_spikes` holds integers, so `col("n_spikes") > 1200.0`
is refused (`InputValidationError`). Write `col("n_spikes") > 1200`.

`explain()` shows which conditions each source applies itself and which are applied after
reading. Here the source applies none: the whole table is read, and the `Filter` step then
keeps the matching rows.

```python
print(busy.explain())
# DynamicPodStream[Filter] reads:
#   source 'csv_v1:units' (CSVSource)
#     applied by the source: nothing (the whole table is read)
#     applied after reading: n_spikes > 1200 (Filter)
```

`filter` compares a column with a value or with another column. A test it cannot say
(arithmetic between columns, such as spikes per second, or a string function) is computed
first: write a function pod that returns the test's result as a column (Part 2), then
`filter` on that column.

## Semi-join

`semi_join` keeps the rows of the first stream whose tags appear in the second, and adds no
columns. Use it when the list of rows to keep is itself a table. Suppose three sessions passed
quality control, and the list is in a CSV file:

```python
(data_dir / "passed_qc.csv").write_text(
    "subject_id,session,checked_by\n"
    "mouse_01,s1,ana\n"
    "mouse_03,s2,ana\n"
    "mouse_04,s1,ben\n"
)
passed = CSVSource(
    str(data_dir / "passed_qc.csv"),
    tag_columns=["subject_id", "session"], tag_only_columns=True,
    source_id="passed_qc",
)

good_units = units.semi_join(passed)
print(good_units.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('region', 'depth_um', 'n_spikes'))
print(good_units.as_table().num_rows)
# 24
```

Three sessions, eight units each. Unlike `join`, the order matters: the first stream is the
one being filtered.

Rows are matched only on the tag columns the two streams share, here `subject_id` and
`session`. If the two streams share a data column name, that column is not used for matching,
and Orcapod issues a `SemiJoinIgnoredColumnWarning`.

The second stream can be any stream, for example the filtered `knockouts` from above, which
shares only `subject_id` with `units`:

```python
units_of_knockouts = units.semi_join(knockouts)
print(units_of_knockouts.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('region', 'depth_um', 'n_spikes'))
print(units_of_knockouts.as_table().num_rows)
# 32
```

A `join` followed by an equality filter selects the same 32 rows, but also carries the
columns of `subjects` along:

```python
print(units.join(subjects).filter(genotype="knockout").keys()[1])
# ('region', 'depth_um', 'n_spikes', 'sex', 'age_weeks', 'genotype')
```

## Rename and select

`select_tag_columns` and `drop_tag_columns` do for tags what `select_data_columns` and
`drop_data_columns` (Part 2) do for data columns.

`map_data` renames data columns and `map_tags` renames tag columns. Both take a dictionary of
old name to new name:

```python
renamed = subjects.map_data({"age_weeks": "age"})
print(renamed.keys())
# (('subject_id',), ('sex', 'age', 'genotype'))
```

or the same renames as keyword arguments, which build the same step:

```python
print(subjects.map_data(age_weeks="age").keys())
# (('subject_id',), ('sex', 'age', 'genotype'))
```

A name that is not a Python identifier, such as `"spike count"`, needs the dictionary.

Columns you do not rename are kept. A name that is not a column of that kind is refused: a
`map_data` of a tag column raises `InputValidationError` and says to use `map_tags`.
`map_columns` renames tag and data columns in one call. Pass `drop_unmapped=True` to keep only
the renamed ones:

```python
print(subjects.map_data({"age_weeks": "age"}, drop_unmapped=True).keys())
# (('subject_id',), ('age',))
```

## When two tables have the same data column

`join` refuses streams that both have a data column of the same name. To see this, split the
units by probe. Each half has an `n_spikes` column:

```python
imec0 = (
    units.filter(probe="imec0")
    .drop_tag_columns(["probe"])
    .select_data_columns(["n_spikes"])
)
imec1 = (
    units.filter(probe="imec1")
    .drop_tag_columns(["probe"])
    .select_data_columns(["n_spikes"])
)

from orcapod.errors import InputValidationError

try:
    imec0.join(imec1)
except InputValidationError as err:
    print(err)
# Input streams are not compatible: Datas should not have overlapping keys, but ['n_spikes'] found in SelectDataColumns and SelectDataColumns.
```

Overlapping data on the same tags can be meaningful, for example the spike counts of one unit
from two analysis runs. In this example it is not: unit 0 of `imec0` and unit 0 of `imec1` are
different units. There are two ways to combine such streams:

**Rename** when the two columns are different quantities:

```python
side_by_side = imec0.map_data({"n_spikes": "n_imec0"}).join(
    imec1.map_data({"n_spikes": "n_imec1"})
)
pair_tags = ["subject_id", "session", "unit"]
print(side_by_side.as_pandas_df().sort_values(pair_tags, ignore_index=True).head(3))
#   subject_id session  unit  n_imec1  n_imec0
# 0   mouse_01      s1     0      215      149
# 1   mouse_01      s1     1      299      326
# 2   mouse_01      s1     2      135      662
```

**`MergeJoin`** when they are repeated measurements of one quantity, to collect them into a
list:

```python
from orcapod.operators import MergeJoin

merged = MergeJoin()(imec0, imec1)
print(merged.output_schema()[1])
# Schema({'n_spikes': list[int]})
print(merged.as_pandas_df().sort_values(pair_tags, ignore_index=True).head(3))
#   subject_id session  unit    n_spikes
# 0   mouse_01      s1     0  [149, 215]
# 1   mouse_01      s1     1  [326, 299]
# 2   mouse_01      s1     2  [662, 135]
```

Each list holds the first input's value, then the second's. `MergeJoin()(imec1, imec0)` puts
`imec1` first and is a different step.

## Key points

- Operators rearrange streams.
- `join` matches on shared tags, keeps only matching rows, and needs distinct data column
  names. Rename, or use `MergeJoin`, when names clash.
- `filter` keeps rows by equality (`column=value`) or by conditions built with `col`
  (comparisons, `is_in`, `between`, null tests, `&`, `|`, `~`), with each comparison in parentheses.
  A test `filter` cannot say is computed as a column by a function pod, then filtered on.
- `semi_join` filters one stream by the shared tags of another and adds no columns.
- `map_data` and `map_tags` rename columns and keep the rest unless you pass
  `drop_unmapped=True`.

## Exercises

**1.** Build a stream with one row per unit recorded on probe `imec1` in a knockout animal,
with exactly two data columns: `n_spikes` and `age_weeks`. How many rows does it have?

??? success "Solution"
    Join units to subjects, filter on one column from each, then narrow.

    ```python
    answer = (
        units.join(subjects)
        .filter(genotype="knockout", probe="imec1")
        .select_data_columns(["n_spikes", "age_weeks"])
    )
    print(answer.as_pandas_df().sort_values(unit_tags, ignore_index=True).head(4))
    #   subject_id session  probe  unit  n_spikes  age_weeks
    # 0   mouse_03      s1  imec1     0       399         11
    # 1   mouse_03      s1  imec1     1       483         11
    # 2   mouse_03      s1  imec1     2       357         11
    # 3   mouse_03      s1  imec1     3       457         11
    print(answer.as_table().num_rows)
    # 16
    ```

**2.** `sessions` has a data column `date`. Rename it to `recorded_on`, keeping `duration_s`.

??? success "Solution"
    ```python
    print(sessions.map_data({"date": "recorded_on"}).keys()[1])
    # ('recorded_on', 'duration_s')
    ```

**3.** Using the `firing_rate` function from Part 2, compute firing rates only for the sessions
listed in `passed_qc.csv`. Use `semi_join` and a single-stream call to the pod.

??? success "Solution"
    Filter the units first, then join the durations.

    ```python
    from orcapod import function_pod


    @function_pod(output_keys="rate_hz")
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        return n_spikes / duration_s


    inputs = (
        units.semi_join(passed)
        .join(sessions)
        .select_data_columns(["n_spikes", "duration_s"])
    )
    rates = firing_rate.pod(inputs)
    print(rates.as_table().num_rows)
    # 24
    ```
