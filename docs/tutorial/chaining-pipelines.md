# Chaining pipelines

The results of one pipeline are often the input of the next. `as_source()` turns the stored
results of a node into a source, so a later stage reads them without running the first stage
again.

**Before you start:** Parts 0 to 5 of the [core course](index.md#core-course).

You will:

- make a source from the stored results of a node
- build a second stage on it
- see which stored results a new job cannot use yet

```python
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import CSVSource

data_dir = make_dataset()
database = InMemoryArrowDatabase()


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    return n_spikes / duration_s


@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    return sum(rate_hz) / len(rate_hz)


job = PipelineJob(name="workshop", database=database)
with job:
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
    rates = firing_rate.pod(
        units.select_data_columns(["n_spikes"]),
        sessions.select_data_columns(["duration_s"]),
        label="rates",
    )
    mean_rate.pod(
        rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"]),
        label="session_means",
    )
job.run()
```

This is the pipeline of [Part 5](05-pipeline-job.md): the first stage.

## A source from stored results

```python
session_means = job.nodes["session_means"].as_source()
print(type(session_means).__name__)
# DerivedSource
print(session_means.keys())
# (('subject_id', 'session'), ('mean_rate_hz',))
```

A `DerivedSource` is a source like those of Part 1, so it can be joined, filtered and given to
pods. The first one you build prints a `ProvisionalHashWarning`, once (see
[Messages you can ignore](00-setup.md#messages-you-can-ignore)).

`as_source()` reads what the database holds, whether or not the node has run in this Python
session. For a node whose results were never stored, it has no rows. It reads every stored
result of the node, including those of earlier runs over other inputs. When you want only
this run's output, use `as_table()`.

## The next stage

The next stage flags sessions with a low mean rate:

```python
@function_pod(output_keys="is_low")
def is_low(mean_rate_hz: float) -> bool:
    return mean_rate_hz < 5.5


flags = is_low.pod(session_means).as_pandas_df()
print(flags.sort_values(["subject_id", "session"], ignore_index=True))
#   subject_id session  is_low
# 0   mouse_01      s1   False
# 1   mouse_01      s2   False
# 2   mouse_02      s1   False
# 3   mouse_02      s2   False
# 4   mouse_03      s1    True
# 5   mouse_03      s2    True
# 6   mouse_04      s1    True
# 7   mouse_04      s2    True
```

The next stage reads what the first one stored. This is how you split a large analysis into
stages that can be owned by different people.

!!! warning "A new job cannot use this source yet"
    The stage above runs in memory, so its own results are not stored. Recording it in a new
    `PipelineJob` does not work yet for `session_means`, or for any node downstream of a
    `Join` or `MergeJoin` of two sources: the job refuses the source with
    `SourceSpecMismatchError` when it is bound. An input of a job must declare one source-id
    and one record-id system tag, and this source declares a pair for each joined source.
    Support is planned (ITL-1127). Until then, to store a later stage, add it to the first
    job.

## Key points

- `node.as_source()` makes a source from the stored results of a node.
- A stage built on it reads the stored results and does not run the first stage.
- `as_source()` reads everything stored for the node; `as_table()` returns this run's output.
- A new job cannot yet use the stored results of a node downstream of a join of two sources.

## Exercises

**1.** Using `as_source()`, compute the mean of the session means per animal (one row per
`subject_id`). Reuse `group_by` from Part 4.

??? success "Solution"
    ```python
    @function_pod(output_keys="animal_mean_hz")
    def mean_of_means(mean_rate_hz: list[float]) -> float:
        return sum(mean_rate_hz) / len(mean_rate_hz)


    means = job.nodes["session_means"].as_source()
    result = mean_of_means.pod(means.group_by(["subject_id"])).as_pandas_df()
    print(result.sort_values("subject_id", ignore_index=True))
    #   subject_id  animal_mean_hz
    # 0   mouse_01        6.413542
    # 1   mouse_02        6.676042
    # 2   mouse_03        4.983160
    # 3   mouse_04        5.132812
    ```

**2.** The `rates` node is also downstream of the join. How many rows and which columns does
`job.nodes["rates"].as_source()` have?

??? success "Solution"
    ```python
    rates_source = job.nodes["rates"].as_source()
    print(rates_source.keys())
    # (('subject_id', 'session', 'probe', 'unit'), ('rate_hz',))
    print(rates_source.as_table().num_rows)
    # 64
    ```

    One row per unit, with the tags and the data column of the node.
