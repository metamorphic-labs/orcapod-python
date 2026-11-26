# Coming from pandas or Polars

If you already work with data frames, most of Orcapod will look familiar: a stream is a table,
and you select, filter, join and group it. This page shows the Orcapod form of what you
already do, then the three places where Orcapod works differently. The same analysis written
in pandas and in Polars is in the [appendix](#appendix-the-same-analysis-in-pandas-and-polars),
so you can compare the two line by line.

**Before you start:** Part 0 of the [core course](index.md#core-course). The core course
explains each step on this page in more detail.

```python
from workshop_data import make_dataset
from orcapod.sources import CSVSource

data_dir = make_dataset()

units = CSVSource(
    str(data_dir / "units.csv"),
    tag_columns=["subject_id", "session", "probe", "unit"], tag_only_columns=True,
    source_id="units",
)
sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=["subject_id", "session"], tag_only_columns=True,
    source_id="sessions",
)
```

Where pandas has `pd.read_csv` and Polars `pl.read_csv`, Orcapod has a source. What you add is
`tag_columns`: here, the columns that say which unit or session a row is about. The next
section explains why.
`tag_only_columns=True` keeps those columns out of the data, as in the tutorial.

## Tags and data

Each column of these streams is either a **tag** or **data** (a column can also be both, a
*data-tag*: see [Data-tags](../concepts/data-tags.md)). `keys()` gives the two groups:

```python
print(units.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('region', 'depth_um', 'n_spikes'))
```

Here the tags name a unit of a probe in a session of a mouse. Data are what was measured
about it. A pandas index or the `on=` columns of a join play a similar part, but in Orcapod the
roles are part of the stream and every step uses them:

- `join` matches rows on the tags the two streams share. There is no `on=` argument.
- `group_by` groups by tags only.
- A function receives data columns and returns new data columns. The tags on this page never
  reach it; the tags of each row are carried to its result.

[Part 1](01-first-stream.md) explains how to choose tags.

## Select

`select_data_columns` keeps the data columns you name, like `df[["n_spikes"]]` in pandas or
`df.select("n_spikes")` in Polars. The tags stay:

```python
counts = units.select_data_columns(["n_spikes"])
print(counts.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('n_spikes',))
```

`drop_data_columns`, `select_tag_columns` and `drop_tag_columns` work the same way, and
`map_data` renames data columns, like `rename(columns=...)` ([Part 3](03-operators.md)).

## Filter

`filter` keeps the rows where a condition holds. `col` names a column, as `pl.col` does in
Polars:

```python
from orcapod import col

busy = units.filter(col("n_spikes") > 1200)
print(busy.as_table().num_rows)
# 3
```

Keyword arguments test equality: `units.filter(probe="imec0")`. [Part 3](03-operators.md#filter)
lists the conditions `filter` takes, and `polars_filter` takes a Polars expression.

## Join

`join` matches the rows of two streams on every tag column they share, here `subject_id` and
`session`:

```python
durations = sessions.select_data_columns(["duration_s"])
joined = counts.join(durations)
print(joined.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('duration_s', 'n_spikes'))
```

It is always an inner join: a row with no partner is dropped. `units` and `counts` both have
the data column `n_spikes`, so joining them is refused, where pandas would add `_x` and `_y`
suffixes:

```python
from orcapod.errors import InputValidationError

try:
    units.join(counts)
except InputValidationError as err:
    print(err)
# Input streams are not compatible: Datas should not have overlapping keys, but ['n_spikes'] found in CSVSource and SelectDataColumns.
```

Rename one of the columns first, with `map_data`. [Part 3](03-operators.md#join) has more.

## Add a column through a function

In pandas you would write `df.assign(rate_hz=df.n_spikes / df.duration_s)`; in Polars,
`df.with_columns(rate_hz=pl.col("n_spikes") / pl.col("duration_s"))`. In Orcapod, every new
value is computed by a Python function, made into a **function pod**:

```python
from orcapod import function_pod


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    return n_spikes / duration_s


rates = firing_rate.pod(joined)
print(rates.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('rate_hz',))
```

The function is applied to each row, with the data columns whose names match its parameters.
Its result is a new stream with the same tags and the columns named in `output_keys`. The
input's data columns are not kept: join them back if you need them.

Operators such as `filter` and `join` never compute a new value; functions do. Because
Orcapod calls the function, it knows which inputs produced which output
([Part 2](02-function-pods.md)).

## Group

`group_by` gathers the rows that share the tags you name. Where the pandas and Polars versions
of this analysis (in the appendix) call `.mean()` on each group, here `group_by` gives each
group's `rate_hz` values as a list, and a function reduces the list:

```python
per_session = rates.group_by(["subject_id", "session"])
print(per_session.output_schema())
# (Schema({'subject_id': str, 'session': str}), Schema({'rate_hz': list[float]}))


@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    return sum(rate_hz) / len(rate_hz)


means = mean_rate.pod(per_session)
print(means.run().sort_by_tags().head(3))
# FunctionPodStream[mean_rate] — first 3 of 8 computed rows, sorted by tags
# * = tag
# shape: (3, 3)
# ┌─────────────┬──────────┬──────────────┐
# │ *subject_id ┆ *session ┆ mean_rate_hz │
# │ ---         ┆ ---      ┆ ---          │
# │ str         ┆ str      ┆ f64          │
# ╞═════════════╪══════════╪══════════════╡
# │ mouse_01    ┆ s1       ┆ 6.033333     │
# │ mouse_01    ┆ s2       ┆ 6.79375      │
# │ mouse_02    ┆ s1       ┆ 5.845833     │
# └─────────────┴──────────┴──────────────┘
```

`group_by` keeps only the tags you group by; the other tags (`probe`, `unit`) are dropped.
[Part 4](04-many-rows-into-one.md) explains why, and how the members of a group are ordered.

`run()` computes the stream and returns a view of its rows. Building `rates` and `means`
called neither function: the functions are called when you ask for rows (`run()`,
`as_table()`), not when you write the step.

## Where Orcapod differs

### Identity and stored results

An Orcapod stream also has an **identity**, a hash of its steps and of the identities of the
sources it reads. Build the same steps again and you get the same identity:

```python
again = mean_rate.pod(
    firing_rate.pod(counts.join(durations)).group_by(["subject_id", "session"])
)
print(again.content_hash() == means.content_hash())
# True
```

Storing results and computing only what is new is a job's work: see
[Part 5](05-pipeline-job.md) and [Part 6](06-reruns-and-caching.md).
[Identity & Hashing](../concepts/identity.md) explains the identities.

### Types must match

pandas and Polars both accept `> 1200.0` on an integer column. Orcapod's `filter` refuses a
value whose type is not its column's:

```python
try:
    units.filter(col("n_spikes") > 1200.0)
except InputValidationError as err:
    print(err)
# Filter: column 'n_spikes' holds int, but the value 1200.0 is a float; convert it explicitly: Orcapod converts no value
```

A function pod's input columns must be compatible with its parameter annotations. `n_spikes`
holds integers, so a function that declares `n_spikes: float` is refused when you build the
pod:

```python
@function_pod(output_keys="rate_hz")
def firing_rate_float(n_spikes: float, duration_s: float) -> float:
    return n_spikes / duration_s


try:
    firing_rate_float.pod(joined)
except ValueError as err:
    print(err)
# Incoming data schema Schema({'duration_s': float, 'n_spikes': int}) is not compatible with expected input schema Schema({'n_spikes': float, 'duration_s': float}): column 'n_spikes' has type int, the function expects float
```

A function must also return the type it declares: a `float` returned from a function declared
`-> int` fails ([Function Pods](../concepts/function-pods.md#return-values-must-match-the-declared-type)).
A column that may hold a missing value has its own type, `T | None`; see
[Missing values](missing-values.md).

### Tags against data

The tag columns decide what a join matches and what a group keeps, so choose them with care.
A value you want to compute with belongs in a data column, because a function receives data
columns; a column that must be both is a data-tag ([Data-tags](../concepts/data-tags.md)).

## Appendix: the same analysis in pandas and Polars

`as_pandas_df()` and `as_polars_df()` read a stream as a data frame. Here
are the mean firing rates computed in pandas and in Polars, then read from the Orcapod stream
`means` for comparison. The three give the same numbers.

```python
import pandas as pd

pd_units = pd.read_csv(data_dir / "units.csv")
pd_sessions = pd.read_csv(data_dir / "sessions.csv")
pd_means = (
    pd_units.merge(pd_sessions, on=["subject_id", "session"])
    .assign(rate_hz=lambda df: df["n_spikes"] / df["duration_s"])
    .groupby(["subject_id", "session"], as_index=False)["rate_hz"]
    .mean()
    .rename(columns={"rate_hz": "mean_rate_hz"})
)
print(pd_means.head(3))
#   subject_id session  mean_rate_hz
# 0   mouse_01      s1      6.033333
# 1   mouse_01      s2      6.793750
# 2   mouse_02      s1      5.845833
```

```python
import polars as pl

pl_units = pl.read_csv(data_dir / "units.csv")
pl_sessions = pl.read_csv(data_dir / "sessions.csv")
pl_means = (
    pl_units.join(pl_sessions, on=["subject_id", "session"])
    .with_columns(rate_hz=pl.col("n_spikes") / pl.col("duration_s"))
    .group_by(["subject_id", "session"])
    .agg(mean_rate_hz=pl.col("rate_hz").mean())
    .sort(["subject_id", "session"])
)
print(pl_means.head(3))
# shape: (3, 3)
# ┌────────────┬─────────┬──────────────┐
# │ subject_id ┆ session ┆ mean_rate_hz │
# │ ---        ┆ ---     ┆ ---          │
# │ str        ┆ str     ┆ f64          │
# ╞════════════╪═════════╪══════════════╡
# │ mouse_01   ┆ s1      ┆ 6.033333     │
# │ mouse_01   ┆ s2      ┆ 6.79375      │
# │ mouse_02   ┆ s1      ┆ 5.845833     │
# └────────────┴─────────┴──────────────┘
```

```python
print(means.as_polars_df().sort(["subject_id", "session"]).head(3))
# shape: (3, 3)
# ┌────────────┬─────────┬──────────────┐
# │ subject_id ┆ session ┆ mean_rate_hz │
# │ ---        ┆ ---     ┆ ---          │
# │ str        ┆ str     ┆ f64          │
# ╞════════════╪═════════╪══════════════╡
# │ mouse_01   ┆ s1      ┆ 6.033333     │
# │ mouse_01   ┆ s2      ┆ 6.79375      │
# │ mouse_02   ┆ s1      ┆ 5.845833     │
# └────────────┴─────────┴──────────────┘
```

The rows of an Orcapod stream come in no promised order, so sort a data frame read from one
before you compare it.
