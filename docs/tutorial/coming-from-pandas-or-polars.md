# Coming from pandas or Polars

Follow one analysis in Orcapod and compare each step with pandas and Polars. You will compute
the mean firing rate of the units in each recording session.

**Before you start:** Part 0 of the [core course](index.md#core-course). The core course
explains each step on this page in more detail.

## Tags and data

Tags identify what each row is about, here a unit or a session. With `tag_only_columns=True`,
the remaining columns hold the data that functions can use ([Part 1](01-first-stream.md)).

```python
from workshop_data import make_dataset
from orcapod.sources import CSVSource

data_dir = make_dataset()
unit_keys = ["subject_id", "session", "probe", "unit"]
session_keys = ["subject_id", "session"]

units = CSVSource(
    str(data_dir / "units.csv"),
    tag_columns=unit_keys,
    tag_only_columns=True,
    source_id="units",
)
sessions = CSVSource(
    str(data_dir / "sessions.csv"),
    tag_columns=session_keys,
    tag_only_columns=True,
    source_id="sessions",
)
print(units.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('region', 'depth_um', 'n_spikes'))
```

!!! note "Equivalent steps in pandas / Polars"

    Both frames read the same rows and keep the keys as ordinary columns.

    **pandas**

    ```python
    from datetime import date

    import pandas as pd

    pd_units = pd.read_csv(data_dir / "units.csv")
    pd_sessions = pd.read_csv(
        data_dir / "sessions.csv", converters={"date": date.fromisoformat}
    )
    ```

    **Polars**

    ```python
    import polars as pl

    pl_units = pl.read_csv(data_dir / "units.csv")
    pl_sessions = pl.read_csv(data_dir / "sessions.csv", try_parse_dates=True)
    ```

## Select

`select_data_columns` keeps the data columns you name and carries the tags along.
[Part 3](03-operators.md) also covers dropping columns and renaming data columns with `map_data`.

```python
counts = units.select_data_columns(["n_spikes"])
print(counts.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('n_spikes',))
```

!!! note "Equivalent steps in pandas / Polars"

    **pandas**

    ```python
    pd_counts = pd_units[unit_keys + ["n_spikes"]]
    ```

    **Polars**

    ```python
    pl_counts = pl_units.select(unit_keys + ["n_spikes"])
    ```

## Filter

[`filter`](03-operators.md#filter) keeps the rows where a condition holds. Here we filter the
original units to see how many exceed 1,200 spikes, then continue the rate analysis with all units.

```python
from orcapod import col

busy = units.filter(col("n_spikes") > 1200)
print(busy.as_table().num_rows)
# 3
```

!!! note "Equivalent steps in pandas / Polars"

    **pandas**

    ```python
    pd_busy = pd_units[pd_units["n_spikes"] > 1200]
    print(len(pd_busy))
    # 3
    ```

    **Polars**

    ```python
    pl_busy = pl_units.filter(pl.col("n_spikes") > 1200)
    print(pl_busy.height)
    # 3
    ```

## Join

[`join`](03-operators.md#join) does an inner join on every tag the streams share, here
`subject_id` and `session`. Select `duration_s` from the sessions first so the joined data
contains just the inputs to the rate calculation.

```python
durations = sessions.select_data_columns(["duration_s"])
joined = counts.join(durations)
print(joined.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('duration_s', 'n_spikes'))
```

!!! note "Equivalent steps in pandas / Polars"

    The frames keep `n_spikes` before `duration_s` because it comes from the left frame.
    Orcapod's join presents the data columns in name order. Sort on the unit keys to compare rows
    across the three joined results.

    **pandas**

    ```python
    pd_durations = pd_sessions[session_keys + ["duration_s"]]
    pd_joined = pd_counts.merge(pd_durations, on=session_keys, how="inner")
    ```

    **Polars**

    ```python
    pl_durations = pl_sessions.select(session_keys + ["duration_s"])
    pl_joined = pl_counts.join(pl_durations, on=session_keys, how="inner")
    ```

## Add a column through a function

A [function pod](02-function-pods.md) computes a value for each row from the data columns whose
names match its parameters. The output carries the original tags and the data named in
`output_keys`.

```python
from orcapod import function_pod


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    return n_spikes / duration_s


rates = firing_rate.pod(joined)
print(rates.keys())
# (('subject_id', 'session', 'probe', 'unit'), ('rate_hz',))
```

!!! note "Equivalent steps in pandas / Polars"

    Keep just the unit keys and `rate_hz` to match the function pod's output.

    **pandas**

    ```python
    pd_rates = pd_joined.assign(
        rate_hz=lambda df: df["n_spikes"] / df["duration_s"]
    )[unit_keys + ["rate_hz"]]
    ```

    **Polars**

    ```python
    pl_rates = pl_joined.with_columns(
        rate_hz=pl.col("n_spikes") / pl.col("duration_s")
    ).select(unit_keys + ["rate_hz"])
    ```

## Group

`group_by` keeps the named tags and gathers each group's `rate_hz` values into a list
([Part 4](04-many-rows-into-one.md)). A function reduces each list to a mean when `run()`
computes the stream.

```python
per_session = rates.group_by(session_keys)
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

!!! note "Equivalent steps in pandas / Polars"

    Both frames compute the mean directly. The previews show sessions in key order.
    An Orcapod stream has no promised row order, so sort after reading it with `as_polars_df()`.

    **pandas**

    ```python
    pd_means = (
        pd_rates.groupby(session_keys, as_index=False, sort=True)["rate_hz"]
        .mean()
        .rename(columns={"rate_hz": "mean_rate_hz"})
    )
    print(pd_means.head(3))
    #   subject_id session  mean_rate_hz
    # 0   mouse_01      s1      6.033333
    # 1   mouse_01      s2      6.793750
    # 2   mouse_02      s1      5.845833
    ```

    **Polars**

    ```python
    pl_means = (
        pl_rates.group_by(session_keys)
        .agg(mean_rate_hz=pl.col("rate_hz").mean())
        .sort(session_keys)
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

    **Orcapod as a Polars frame**

    ```python
    print(means.as_polars_df().sort(session_keys).head(3))
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

pandas and Polars both accept `> 1200.0` on an integer column. Orcapod's `filter` refuses it
on `n_spikes`, which holds integers:

```python
from orcapod.errors import InputValidationError

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

Orcapod also requires distinct data column names in a join. Both `units` and `counts` have
`n_spikes`, so rename one with `map_data` before joining them.

```python
try:
    units.join(counts)
except InputValidationError as err:
    print(err)
# Input streams are not compatible: Datas should not have overlapping keys, but ['n_spikes'] found in CSVSource and SelectDataColumns.
```
