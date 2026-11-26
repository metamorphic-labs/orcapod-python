# Inspecting results

After a run, a job holds the result of every step, and the database holds a record of every
function call. This tutorial shows where to read them.

**Before you start:** Parts 0 to 6 of the [core course](index.md#core-course).

You will:

- list the steps of a job and what each one reads
- show the hidden columns that say which source rows a row came from
- read the record Orcapod keeps of each function call
- tell a node's output from everything stored for it

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

This is the pipeline of [Part 5](05-pipeline-job.md).

## Tracing the steps

`print(job.diagram())` lists the steps of the job, one node per line. After `<-` come the nodes
a node reads from:

```python
print(job.diagram())
# Job 'workshop': 9 nodes
#   sessions (source · CSVSource · bound)
#   SelectDataColumns_2 (operator · current) <- sessions
#   units (source · CSVSource · bound)
#   SelectDataColumns_1 (operator · current) <- units
#   Join (operator · current) <- SelectDataColumns_2, SelectDataColumns_1
#   rates (function pod · firing_rate · current) <- Join
#   GroupBy (operator · current) <- rates
#   SelectDataColumns_3 (operator · current) <- GroupBy
#   session_means (function pod · mean_rate · current) <- SelectDataColumns_3
```

To find the steps that produced a result, start at its node and follow `<-` until you reach
the sources: `session_means` reads `SelectDataColumns_3`, which reads `GroupBy`, and so on back
to `units` and `sessions`. When a job has several steps of the same kind, it numbers them
(`SelectDataColumns_1`, ...). The numbers tell the steps apart and do not follow the order you
wrote them in.

In a notebook, `job` as the last line of a cell draws the same graph as a picture. Clicking a
node there shows its columns and its row counts in the last run.
[Working in notebooks](../concepts/notebooks.md) shows it, and
[Tracing a value](tracing-a-value.md#on-the-diagram) marks the path of one value on it.

## Tracing rows

Every stream carries hidden columns that record where its rows and values came from. Ask for
them with the `columns` argument of `as_table`.

**Source-info columns.** For each data column there is a column named `_source_<column>`. On
the `units` source it holds the source id, the row's record id (its position, counted from
zero) and the column:

```python
table = job.nodes["units"].as_table(columns={"source": True})
print(table.column_names)
# ['subject_id', 'session', 'probe', 'unit', 'region', 'depth_um', 'n_spikes', '_source_region', '_source_depth_um', '_source_n_spikes']
print(table.column("_source_n_spikes").to_pylist()[5])
# csv_v1:units::row_5::n_spikes
```

This is row 5 of the source you named `units`, column `n_spikes`. Every source id starts with
the kind of source and the version of its reader, here `csv_v1`.

**System tags.** Each source also adds two hidden tag columns to every row: one with the
source's id, one with the row's record id. They travel with the row through every operator and
function pod, and a joined row carries the system tags of both inputs:

```python
import polars as pl

table = pl.from_arrow(job.nodes["rates"].as_table(columns={"system_tags": True}))
source_columns = [c for c in table.columns if c.startswith("_tag_source_id")]
print(len(source_columns))
# 2
print(table.select(source_columns).unique().rows())
# [('csv_v1:sessions', 'csv_v1:units')]
```

Since two sources feed `rates`, every row depends on one row of `sessions` and one row of
`units`. The column names end in a long hash that identifies the source's position in the
pipeline, so the sample selects them by prefix.

The `_tag_record_id::...` columns hold the record id of the source row, as text:

```python
units_table = job.nodes["units"].as_table(columns={"system_tags": True})
record_id_column = next(
    name for name in units_table.column_names if name.startswith("_tag_record_id::")
)
print(units_table.column(record_id_column)[5].as_py())
# row_5
```

For a source with key columns (`record_id_columns`), the record id is a JSON object of the key
columns and their values, in the order you declared them.

!!! note "After `group_by`"
    A grouped row comes from many source rows. Its system tags are folded into one digest per
    column, so they tell one group from another and no longer name the individual rows. To see
    which rows went into a group, look at the stream before the grouping
    (`job.nodes["rates"]`). The list is made up of every row with the same `subject_id` and
    `session`.

`as_table(all_info=True)` shows every hidden column at once.

## Call records

For each function, the database holds one record per computed result.
`get_all_records(columns={"meta": True})` returns them with the details of the call:

```python
records = pl.from_arrow(job.nodes["rates"].get_all_records(columns={"meta": True}))
meta = [c for c in records.columns if c.startswith("__")]
print(meta)
# ['__data_id', '__input_data_hash', '__output_data_hash', '__data_context', '__computed', '__is_ephemeral', '__pipeline_run_id', '__recorded_at', '__input_data_hash_right', '__pf_var_function_name', '__pf_var_function_version', '__pf_var_function_module', '__pf_var_function_signature_hash', '__pf_var_function_content_hash', '__pf_var_git_hash', '__pf_exec_executor_type', '__pf_exec_executor_info', '__pf_exec_python_version', '__pf_exec_extra_info', '__pod_ts']
```

Some important columns:

| Column | Meaning |
|---|---|
| `__pf_var_function_name` | The function that produced the result |
| `__pod_ts` | When the result was computed |
| `__pf_exec_python_version` | The Python version it ran under |
| `__pf_var_git_hash` | The git commit of the code, when run inside a git repository |
| `__computed` | `True` if this row was computed, `False` if it reused the result of an identical input |
| `__pipeline_run_id` | The job run that recorded the entry |
| `__recorded_at` | When the entry was recorded, in UTC |
| `__input_data_hash` | The hash of the input values: the key the result is stored under ([Part 6](06-reruns-and-caching.md#reuse-rule)) |

```python
print(records.select("__pf_var_function_name", "__pf_exec_executor_type").unique().rows())
# [('firing_rate', 'local')]
print(records["__computed"].sum(), "of", len(records))
# 63 of 64
print(records["__pod_ts"].dtype)
# Datetime(time_unit='us', time_zone='UTC')
```

63 of 64 rows were computed: one row reused the result of another row with the same inputs,
as in Part 5.

### `as_table` and `get_all_records`

- `node.as_table()` returns the node's output **for the inputs of this job's run**: 64 rows
  here. Before the job has run in this Python session it returns no rows, even when the
  database already holds results.
- `node.get_all_records()` returns **everything stored** for this function in this job's part
  of the database, including results from earlier runs over other inputs. It does not need a
  run.

After the run with two more units in Part 6, a job over the original file has 64 rows in
`as_table()` once it has run, while `get_all_records()` has 66.

## Key points

- `print(job.diagram())` lists the steps of a job and what each one reads.
- `as_table(columns={"source": True})` and `columns={"system_tags": True}` show where values
  and rows came from; `all_info=True` shows every hidden column.
- `get_all_records(columns={"meta": True})` shows how and when each result was computed.
- `as_table()` returns the output of this job's run; `get_all_records()` returns everything
  stored.

## Exercises

**1.** Print the diagram with `collapse=True`, which hides the joins and column selections.
Which sources does `rates` read?

??? success "Solution"
    ```python
    print(job.diagram(collapse=True))
    # Job 'workshop': 5 nodes
    #   sessions (source · CSVSource · bound)
    #   units (source · CSVSource · bound)
    #   rates (function pod · firing_rate · current) <- sessions, units
    #   GroupBy (operator · current) <- rates
    #   session_means (function pod · mean_rate · current) <- GroupBy
    ```

    Two: `sessions` and `units`.

**2.** Which row of `sessions.csv` did the duration of `mouse_03`, session `s2` come from?
Use the source-info column of the `sessions` node.

??? success "Solution"
    ```python
    table = pl.from_arrow(job.nodes["sessions"].as_table(columns={"source": True}))
    row = table.filter((pl.col("subject_id") == "mouse_03") & (pl.col("session") == "s2"))
    print(row["_source_duration_s"].to_list())
    # ['csv_v1:sessions::row_5::duration_s']
    ```

    Row 5, counting data rows from 0: the sixth line after the header.

    This is a Polars filter on a table taken out of Orcapod. Inside a pipeline, write the same
    test as `stream.filter(...)` (Part 3).
