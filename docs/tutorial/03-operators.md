# Part 3: Reshaping with operators

Function pods compute new values. Everything else you do to a table (combine it with another
one, keep some rows, rename or drop columns) is done by **operators**. An operator
rearranges a stream; it never computes a new value from the data. Keeping the two apart is
deliberate: when a number in your result looks wrong, it can only have come from one of your
functions.

You will:

- combine tables with `join`
- keep a subset of rows with `polars_filter` and `semi_join`
- rename columns with `map_data`
- see what to do when two tables have a column of the same name

```python
from workshop_data import make_dataset
from orcapod.sources import CSVSource

data_dir = make_dataset()

subjects = CSVSource(
    str(data_dir / "subjects.csv"), tag_columns=["subject_id"], source_id="subjects"
)
sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"],
    source_id="sessions",
)
units = CSVSource(
    str(data_dir / "units.csv"),
    tag_columns=["subject_id", "session", "probe", "unit"],
    source_id="units",
)
```

Every operator is available as a method on a stream, and that is the form used here. The
same operators exist as classes in `orcapod.operators` (`Join`, `PolarsFilter`, ...); see
[Operators](../concepts/operators.md).

## Join: combine tables on their shared tags

`join` matches rows of two streams that have the same values in the tag columns they share,
and puts their data columns side by side:

```python
unit_info = units.join(sessions)

print(unit_info.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('region', 'depth_um', 'n_spikes', 'date', 'duration_s'))
print(unit_info.select_data_columns(["n_spikes", "duration_s"]).as_pandas_df().head(3))
#   subject_id session  probe  unit  n_spikes  duration_s
# 0   mouse_01      s1  imec0     0       149        60.0
# 1   mouse_01      s1  imec0     1       326        60.0
# 2   mouse_01      s1  imec0     2       662        60.0
```

You do not say what to join on. `units` has tags `subject_id, session, probe, unit`;
`sessions` has `subject_id, session`; the shared ones are used. The result has the union of
the tags and all data columns of both. This is the step the two-stream function pod in Part 2
did for you.

Three things to know:

- It is an **inner** join: a row with no partner in the other stream is dropped.
- The order does not matter. `units.join(sessions)` and `sessions.join(units)` give the same
  rows.
- If the streams share **no** tag column, every row is paired with every row.

Joins chain. This attaches both the session and the animal to every unit:

```python
everything = units.join(sessions).join(subjects)
print(everything.keys()[1])
# ('region', 'depth_um', 'n_spikes', 'date', 'duration_s', 'sex', 'age_weeks', 'genotype')
print(everything.as_table().num_rows)
# 64
```

## Filter: keep some rows

`polars_filter` keeps the rows that pass a test. The simplest form is equality on one or more
columns, written as keyword arguments:

```python
knockouts = subjects.polars_filter(genotype="knockout")
print(knockouts.as_pandas_df())
#   subject_id sex  age_weeks  genotype
# 0   mouse_03   F         11  knockout
# 1   mouse_04   M         13  knockout
```

A filter may test tag columns and data columns alike. Join first, and you can filter on
columns from either table:

```python
knockout_units = units.join(subjects).polars_filter(genotype="knockout", probe="imec1")
print(knockout_units.as_table().num_rows)
# 16
```

For anything other than equality, pass a [Polars](https://docs.pola.rs/) expression:

```python
import polars as pl

busy = units.polars_filter(pl.col("n_spikes") > 1200)
print(busy.as_pandas_df())
#   subject_id session  probe  unit region  depth_um  n_spikes
# 0   mouse_01      s2  imec0     2     V1       920      1221
# 1   mouse_01      s2  imec1     2    CA1       120      1205
# 2   mouse_02      s2  imec1     3    CA1      2360      1233
```

!!! warning "Known bug: expression filters only work as the last step"
    A filter built from a Polars expression (`pl.col(...) > ...`) works when you look at its
    result directly, as above. It currently **fails** as soon as anything needs to identify
    the filtered stream: joining it with another stream, or using it anywhere inside a
    `PipelineJob` (Part 5). The error is:

    ```
    TypeError: SemanticAwarePythonHasher (strict): no implementation of
    PythonTypeHandlerProtocol registered for type 'polars.expr.expr.Expr' ...
    ```

    Equality filters (`polars_filter(genotype="knockout")`) do not have this problem and work
    everywhere. Until this is fixed, use equality filters inside pipelines, and keep
    expression filters for looking at results at the end.

Here is that failure, so that you recognise it:

```python
try:
    busy.join(subjects)
except TypeError as err:
    print(str(err)[:60])
# SemanticAwarePythonHasher (strict): no implementation of Pyt
```

## Semi-join: keep the rows that appear in another table

Sometimes the list of rows to keep is itself a table: a curation sheet, a list of sessions
that passed quality control. `semi_join` keeps the rows of the first stream whose tags appear
in the second, and adds nothing from the second.

Suppose three sessions passed quality control, and the list is in a CSV file:

```python
(data_dir / "passed_qc.csv").write_text(
    "subject_id,session,checked_by\n"
    "mouse_01,s1,ana\n"
    "mouse_03,s2,ana\n"
    "mouse_04,s1,ben\n"
)
passed = CSVSource(
    str(data_dir / "passed_qc.csv"),
    tag_columns=["subject_id", "session"],
    source_id="passed_qc",
)

good_units = units.semi_join(passed)
print(good_units.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('region', 'depth_um', 'n_spikes'))
print(good_units.as_table().num_rows)
# 24
```

Three sessions, eight units each. The columns are those of `units`; `checked_by` was used
for nothing. Unlike `join`, the order matters: the first stream is the one being filtered.

!!! warning "Known bug: `semi_join` between a CSV source and other streams"
    `semi_join` currently works when both sides are CSV sources, as above. It **fails** when
    one side comes from a `CSVSource` and the other from a `DictSource` or from a stream that
    went through `polars_filter`:

    ```
    ArrowInvalid: Incompatible data types for corresponding join field keys:
    FieldRef.Name(subject_id) of type string and FieldRef.Name(subject_id) of type large_string
    ```

    The two kinds of stream store text in slightly different formats and `semi_join` does not
    reconcile them yet. `join` does, so until this is fixed, use `join` followed by a filter
    when your streams are of mixed origin.

```python
try:
    units.semi_join(knockouts).as_table()
except Exception as err:
    print(type(err).__name__)
# ArrowInvalid
```

The same selection with `join` and an equality filter works:

```python
print(units.join(subjects).polars_filter(genotype="knockout").as_table().num_rows)
# 32
```

## Rename and select

`select_data_columns` and `drop_data_columns` you know from Part 2. There are matching
`select_tag_columns` and `drop_tag_columns`.

`map_data` renames data columns and `map_tags` renames tag columns. Both take a dictionary of
old name to new name:

```python
renamed = subjects.map_data({"age_weeks": "age"}, drop_unmapped=False)
print(renamed.keys())
# (('subject_id',), ('sex', 'age', 'genotype'))
```

!!! note "Always pass `drop_unmapped`"
    Without `drop_unmapped=False`, the `map_data` and `map_tags` methods **drop** every
    column you did not rename: `subjects.map_data({"age_weeks": "age"})` leaves only `age`.
    Pass `drop_unmapped` explicitly so that the call says what you mean.

## When two tables have the same data column

`join` refuses to combine streams that both have a data column of the same name, because it
cannot keep both under one name. To see this, split the units by probe. Each half has an
`n_spikes` column:

```python
imec0 = (
    units.polars_filter(probe="imec0")
    .drop_tag_columns(["probe"])
    .select_data_columns(["n_spikes"])
)
imec1 = (
    units.polars_filter(probe="imec1")
    .drop_tag_columns(["probe"])
    .select_data_columns(["n_spikes"])
)

from orcapod.errors import InputValidationError

try:
    imec0.join(imec1)
except InputValidationError as err:
    print(err)
# Input streams are not compatible: Datas should not have overlapping keys, but KeysView(Schema({'n_spikes': int})) found in SelectDataColumns and SelectDataColumns.
```

There are two ways out, and they mean different things.

**Rename**, when the two columns are different quantities that happen to share a name:

```python
side_by_side = imec0.map_data({"n_spikes": "n_imec0"}, drop_unmapped=False).join(
    imec1.map_data({"n_spikes": "n_imec1"}, drop_unmapped=False)
)
print(side_by_side.as_pandas_df().head(3))
#   subject_id session  unit  n_imec0  n_imec1
# 0   mouse_01      s1     0      149      215
# 1   mouse_01      s1     1      326      299
# 2   mouse_01      s1     2      662      135
```

**`MergeJoin`**, when they are repeated measurements of the same quantity and you want them
collected into a list:

```python
from orcapod.operators import MergeJoin

merged = MergeJoin()(imec0, imec1)
print(merged.output_schema()[1])
# Schema({'n_spikes': list[int]})
print(merged.as_pandas_df().head(3))
#   subject_id session  unit    n_spikes
# 0   mouse_01      s1     0  [149, 215]
# 1   mouse_01      s1     1  [299, 326]
# 2   mouse_01      s1     2  [135, 662]
```

The merged lists are sorted, so you can no longer tell which value came from which probe.
That is the point: `MergeJoin` is for values that are interchangeable.

## What to remember

- Operators rearrange streams; they never compute new values.
- `join` matches on shared tags, keeps only matching rows, and needs distinct data column
  names.
- `polars_filter(column=value)` works everywhere. Expression filters work only as a final
  step (known bug).
- `semi_join` filters one stream by another; mixed CSV and non-CSV inputs fail (known bug),
  so use `join` plus a filter.
- Pass `drop_unmapped` explicitly to `map_data` and `map_tags`.

## Exercises

**1.** Build a stream with one row per unit recorded on probe `imec1` in a knockout animal,
with exactly two data columns: `n_spikes` and `age_weeks`. How many rows does it have?

??? success "Solution"
    Join units to subjects, filter on one column from each, then narrow.

    ```python
    answer = (
        units.join(subjects)
        .polars_filter(genotype="knockout", probe="imec1")
        .select_data_columns(["n_spikes", "age_weeks"])
    )
    print(answer.as_pandas_df().head(4))
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
    print(sessions.map_data({"date": "recorded_on"}, drop_unmapped=False).keys()[1])
    # ('recorded_on', 'duration_s')
    ```

**3.** Using the `firing_rate` function from Part 2, compute firing rates only for the sessions
listed in `passed_qc.csv`. Use `semi_join` and a single-stream call to the pod.

??? success "Solution"
    Filter the units first, then join the durations, then narrow to the function's two
    parameters.

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

Next: [Part 4: Many rows into one](04-many-rows-into-one.md).
