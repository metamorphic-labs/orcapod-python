# Part 8: Inspecting and tracing

A number in a figure raises questions. Which steps produced it? From which rows of which
table? When was it computed, and with what? Because Orcapod runs every step itself, it
keeps the answers. This part shows where to find them.

You will:

- walk backwards from a result to the sources it came from
- trace one value to the source cells it was made from, and find why a row is missing
- reveal the provenance columns that every stream carries
- read the record Orcapod keeps of each function call
- start a second pipeline from the stored results of the first

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

This is the full pipeline from Part 5. `group_by`, and `filter` further down, each print a
`ProvisionalHashWarning` once (see [Messages you can ignore](00-setup.md#messages-you-can-ignore)).

## Which steps produced this?

Every node knows the nodes it reads from, in `node.upstreams`, and its own name, in
`node.label`. Following `upstreams` until there are none left takes you from a result back
to the sources:

```python
def print_lineage(node, depth=0):
    print("  " * depth + node.label)
    for upstream in node.upstreams:
        print_lineage(upstream, depth + 1)


print_lineage(job.nodes["session_means"])
# session_means
#   SelectDataColumns_3
#     GroupBy
#       rates
#         Join
#           SelectDataColumns_1
#             units
#           SelectDataColumns_2
#             sessions
```

Read from the bottom up, this is the pipeline as you wrote it, including the join that the
two-stream call to `firing_rate` added for you. When a job has several steps of the same
kind, it numbers them (`SelectDataColumns_1`, ...); the numbers tell the steps apart but do
not follow the order you wrote them in.

The whole graph, rather than one node's lineage, is what the job draws when it is the last
line of a notebook cell (see [Part 5](05-pipeline-job.md#nodes)).
Clicking a node there shows its columns and its row counts in the last run.
`job.diagram(direction="TB")` lays it out top to bottom (in the notebook, the `vertical`
checkbox above the diagram does the same), and `print(job.diagram())` lists it as text.
[Working in notebooks](../concepts/notebooks.md) shows the diagram, the table preview of a
node and the run summary as they appear in a notebook.

## Which rows did a value come from?

Every stream carries hidden columns that record where its rows and values came from. They
are left out by default. Ask for them with the `columns` argument of `as_table`.

**Source-info columns.** For each data column there is a companion column named
`_source_<column>`. On the `units` source it holds the source id, the row's record id
(its position, counted from zero) and the column:

```python
table = job.nodes["units"].as_table(columns={"source": True})
print(table.column_names)
# ['subject_id', 'session', 'probe', 'unit', 'region', 'depth_um', 'n_spikes', '_source_region', '_source_depth_um', '_source_n_spikes']
print(table.column("_source_n_spikes").to_pylist()[5])
# csv_v1:units::row_5::n_spikes
```

Row 5 of the source you named `units`, column `n_spikes`, read by the CSV source
(`csv_v1`: the kind of source and the version of its reader come first in every source id).
This is what the `source_id` you have been passing since Part 1 is for.

**System tags.** Each source also adds two hidden tag columns to every row: one with the
source's name, one with an identifier of the row. They travel with the row through every
operator and function pod. When two streams are joined, the joined row carries the system
tags of both. So for any row of `rates` you can read off which sources it depends on:

```python
import polars as pl

table = pl.from_arrow(job.nodes["rates"].as_table(columns={"system_tags": True}))
source_columns = [c for c in table.columns if c.startswith("_tag_source_id")]
print(len(source_columns))
# 2
print(table.select(source_columns).unique().rows())
# [('csv_v1:sessions', 'csv_v1:units')]
```

Two system-tag columns, because two sources feed `rates`; every row depends on one row of
`sessions` and one row of `units`. The column names end in a long hash that identifies the
source's position in the pipeline, which is why the sample selects them by prefix.

The companion `_tag_record_id::...` columns identify the individual source row within its
source, as text. By default a record id is the row's position, counted from zero:

```python
units_table = job.nodes["units"].as_table(columns={"system_tags": True})
record_id_column = next(
    name for name in units_table.column_names if name.startswith("_tag_record_id::")
)
print(units_table.column(record_id_column)[5].as_py())
# row_5
```

For a source with key columns (`record_id_columns`), the record id is instead a JSON object
of the key columns and their values, in the order you declared them.

!!! note "After `group_by`"
    A grouped row comes from many source rows. Its system tags are folded into a single
    digest per column, so they still distinguish one group from another but no longer name
    the individual rows. To see which rows went into a group, look at the stream before the
    grouping (`job.nodes["rates"]`): every row there with the group's `subject_id` and
    `session` is a member, and the list elements are in the order of their `probe` and
    `unit`, as you saw in Part 4.

`as_table(all_info=True)` shows every hidden column at once.

## How was it computed?

For each function, the database holds one record per computed result, with details of the
call. `get_all_records` returns them; ask for the `meta` columns to see the details:

```python
records = pl.from_arrow(job.nodes["rates"].get_all_records(columns={"meta": True}))
meta = [c for c in records.columns if c.startswith("__")]
print(meta)
# ['__data_id', '__input_data_hash', '__output_data_hash', '__data_context', '__computed', '__is_ephemeral', '__pipeline_run_id', '__recorded_at', '__input_data_hash_right', '__pf_var_function_name', '__pf_var_function_version', '__pf_var_function_module', '__pf_var_function_signature_hash', '__pf_var_function_content_hash', '__pf_var_git_hash', '__pf_exec_executor_type', '__pf_exec_executor_info', '__pf_exec_python_version', '__pf_exec_extra_info', '__pod_ts']
```

The ones you are most likely to want:

| Column | Meaning |
|---|---|
| `__pf_var_function_name` | The function that produced the result |
| `__pod_ts` | When the result was computed |
| `__pf_exec_python_version` | The Python version it ran under |
| `__pf_var_git_hash` | The git commit of the code, when run inside a git repository |
| `__computed` | `True` if this row was computed, `False` if it reused the result of an identical input |
| `__pipeline_run_id` | The job run that recorded the pipeline entry |
| `__recorded_at` | When the pipeline entry was recorded, in UTC |
| `__input_data_hash` | The hash of the input values: the key the result is stored under (Part 6) |

```python
print(records.select("__pf_var_function_name", "__pf_exec_executor_type").unique().rows())
# [('firing_rate', 'local')]
print(records["__computed"].sum(), "of", len(records))
# 63 of 64
print(records["__pod_ts"].dtype)
# Datetime(time_unit='us', time_zone='UTC')
```

63 of 64: the count from Part 5 again, now visible in the records. One row reused the
result of another row with the same inputs. (Like there, it is the count of `job.run()`.)

### `as_table` versus `get_all_records`

They answer different questions.

- `node.as_table()` is the node's output **for the inputs of this job's run**: 64 rows here.
  Before the job has run in this Python session it has no rows at all, even when the
  database already holds results.
- `node.get_all_records()` is **everything stored** for this function in this job's part of
  the database, including results from earlier runs over other inputs. It does not need a
  run.

After the "two more units" run of Part 6, a job over the original file, once run, has 64
rows in `as_table()`, while `get_all_records()` has 66.

## Tracing one value to its sources

The columns above hold every link from a result back to its sources. `trace` follows them
for you, for one cell: give the node, the tags of the row and the column.

```python
means = job.nodes["session_means"]
trace = means.trace({"subject_id": "mouse_03", "session": "s2"}, "mean_rate_hz")
print(trace)
# trace of mean_rate_hz at node session_means, for subject_id='mouse_03', session='s2': 1 row matches
# session_means (subject_id='mouse_03', session='s2'): mean_rate_hz = 5.059375
#   mean_rate v0.0 (body function_content_v1:...), computed here, computed ... on local, run ..., recorded ...
#   GroupBy (subject_id='mouse_03', session='s2'): rate_hz = [4.791666666666667, 5.058333333333334, 7.366666666666666, 4.683333333333334, 5.0, 3.3333333333333335, 6.916666666666667, 3.325]  (via SelectDataColumns)
#     [0] rates (subject_id='mouse_03', session='s2', probe='imec0', unit=0): rate_hz = 4.791666666666667
#       firing_rate v0.0 (body function_content_v1:...), computed here, computed ... on local, run ..., recorded ...
#       csv_v1:sessions row_5 duration_s = 120.0  (via Join, SelectDataColumns)
#       csv_v1:units row_40 n_spikes = 575  (via Join, SelectDataColumns)
#     [1] rates (subject_id='mouse_03', session='s2', probe='imec0', unit=1): rate_hz = 5.058333333333334
#       firing_rate v0.0 (body function_content_v1:...), computed here, computed ... on local, run ..., recorded ...
#       csv_v1:sessions row_5 duration_s = 120.0  (via Join, SelectDataColumns)
#       csv_v1:units row_41 n_spikes = 607  (via Join, SelectDataColumns)
#     [2] rates (subject_id='mouse_03', session='s2', probe='imec0', unit=2): rate_hz = 7.366666666666666
#       firing_rate v0.0 (body function_content_v1:...), computed here, computed ... on local, run ..., recorded ...
#       csv_v1:sessions row_5 duration_s = 120.0  (via Join, SelectDataColumns)
#       csv_v1:units row_42 n_spikes = 884  (via Join, SelectDataColumns)
#     [3] rates (subject_id='mouse_03', session='s2', probe='imec0', unit=3): rate_hz = 4.683333333333334
#       firing_rate v0.0 (body function_content_v1:...), computed here, computed ... on local, run ..., recorded ...
#       csv_v1:sessions row_5 duration_s = 120.0  (via Join, SelectDataColumns)
#       csv_v1:units row_43 n_spikes = 562  (via Join, SelectDataColumns)
#     [4] rates (subject_id='mouse_03', session='s2', probe='imec1', unit=0): rate_hz = 5.0
#       firing_rate v0.0 (body function_content_v1:...), computed here, computed ... on local, run ..., recorded ...
#       csv_v1:sessions row_5 duration_s = 120.0  (via Join, SelectDataColumns)
#       csv_v1:units row_44 n_spikes = 600  (via Join, SelectDataColumns)
#     [5] rates (subject_id='mouse_03', session='s2', probe='imec1', unit=1): rate_hz = 3.3333333333333335
#       firing_rate v0.0 (body function_content_v1:...), computed here, computed ... on local, run ..., recorded ...
#       csv_v1:sessions row_5 duration_s = 120.0  (via Join, SelectDataColumns)
#       csv_v1:units row_45 n_spikes = 400  (via Join, SelectDataColumns)
#     [6] rates (subject_id='mouse_03', session='s2', probe='imec1', unit=2): rate_hz = 6.916666666666667
#       firing_rate v0.0 (body function_content_v1:...), computed here, computed ... on local, run ..., recorded ...
#       csv_v1:sessions row_5 duration_s = 120.0  (via Join, SelectDataColumns)
#       csv_v1:units row_46 n_spikes = 830  (via Join, SelectDataColumns)
#     [7] rates (subject_id='mouse_03', session='s2', probe='imec1', unit=3): rate_hz = 3.325
#       firing_rate v0.0 (body function_content_v1:...), computed here, computed ... on local, run ..., recorded ...
#       csv_v1:sessions row_5 duration_s = 120.0  (via Join, SelectDataColumns)
#       csv_v1:units row_47 n_spikes = 399  (via Join, SelectDataColumns)
```

The first line repeats the question: the column traced, the node it was asked at and the
row selector, and how many held rows match it. Read the rest from the top. The mean of
`mouse_03`, session `s2`, was computed by `mean_rate`
(its version, the hash of its body, when and where it ran, and the job run that
recorded the entry, with when). Its input was the grouped row
of eight rates. Each rate was computed by `firing_rate` from one row of `units.csv` and the
one row of `sessions.csv` for that session; the operators it passed through are listed
after `via`.

Each step is found from what the job holds and stores, not guessed:

- A function's row is matched to its stored record (Part 8, "How was it computed?") by
  its system tags, the hidden tags that name the source rows it came from.
- The members of a group are the rows before the grouping with the group's
  `subject_id` and `session`, in `probe`, `unit` order (Part 4). Orcapod checks them
  against the group's folded system tags and raises an error if they do not agree.
- The last step reads the source-info columns: `csv_v1:units row_40` is row 40 of the
  source named `units` (its id names the kind of reader, a CSV file read by version 1,
  then its name).

Nothing is computed: the trace only reads.

`source_cells()` lists the source cells alone, each once:

```python
for cell in trace.source_cells():
    print(cell.text())
# csv_v1:sessions row_5 duration_s = 120.0
# csv_v1:units row_40 n_spikes = 575
# csv_v1:units row_41 n_spikes = 607
# csv_v1:units row_42 n_spikes = 884
# csv_v1:units row_43 n_spikes = 562
# csv_v1:units row_44 n_spikes = 600
# csv_v1:units row_45 n_spikes = 400
# csv_v1:units row_46 n_spikes = 830
# csv_v1:units row_47 n_spikes = 399
```

**Computed here, or reused.** In session `s1` of `mouse_02`, two units have the same spike
count, so `firing_rate` received equal inputs twice and ran once (the "63 of 64" above).
The trace still tells the two units apart, and says which of them computed the result and
which reused it:

```python
shared = means.trace({"subject_id": "mouse_02", "session": "s1"}, "mean_rate_hz")
for step in shared.root.walk():
    if step.node == "rates":
        tags = dict(step.tags)
        print(tags["probe"], tags["unit"], step.function.computed_here)
# imec0 0 True
# imec0 1 True
# imec0 2 True
# imec0 3 True
# imec1 0 True
# imec1 1 True
# imec1 2 False
# imec1 3 True
```

`imec1` unit 2 reused the result that `imec0` unit 0 computed: one stored result, two
rows, both traced to their own row of `units.csv`.

**Several rows at once.** The selector need not pick one row: every held row whose columns
equal the values given is traced, and `{}` traces every row. Each matching row has its own
tree, in `trace.roots`, in tag order; `source_cells()` and the highlight cover them all:

```python
both = means.trace({"subject_id": "mouse_02"}, "mean_rate_hz")
print(both.header())
# trace of mean_rate_hz at node session_means, for subject_id='mouse_02': 2 rows match
print([dict(root.tags) for root in both.roots])
# [{'subject_id': 'mouse_02', 'session': 's1'}, {'subject_id': 'mouse_02', 'session': 's2'}]
print(len(both.source_cells()))
# 18
```

A selector that matches no held row gives a trace that says so (`no held row matches`,
status `no_match`); one that names a column the node does not have is an error.

!!! note "The first diagram"
    The first diagram in a Python session issues `InDevelopmentWarning`, once. It is
    harmless: see the note in [Part 5](05-pipeline-job.md#nodes).

**On the diagram.** `trace.highlight()` marks the trace's path on the job's diagram. The
listing shows the provenance question only, with no run status. It marks the nodes on the
path with `*`, the node the trace was asked at with `>` and `[traced from here]`, and each
function node on the path with how its rows on the path got their results: `[computed]`,
`[reused]` (a result stored for equal inputs), or `[computed + reused]` (some rows each):

```python
print(job.diagram(highlight=trace.highlight()))
# Job 'workshop': 9 nodes
#   highlight (*): trace of mean_rate_hz at node session_means, for subject_id='mouse_03', session='s2': 1 row matches
#  * sessions (source · CSVSource)
#  * SelectDataColumns_2 (operator) <- sessions
#  * units (source · CSVSource)
#  * SelectDataColumns_1 (operator) <- units
#  * Join (operator) <- SelectDataColumns_2, SelectDataColumns_1
#  * rates (function pod · firing_rate) [computed] <- Join
#  * GroupBy (operator) <- rates
#  * SelectDataColumns_3 (operator) <- GroupBy
# >* session_means (function pod · mean_rate) [computed] [traced from here] <- SelectDataColumns_3
```

A mean is made through every step of the job, so its path covers the whole diagram. The
trace of one firing rate covers only part of it: the two sources, their column selections,
the join and `rates`. Take the unit that reused a stored result:

```python
rate = job.nodes["rates"].trace(
    {"subject_id": "mouse_02", "session": "s1", "probe": "imec1", "unit": 2}, "rate_hz"
)
print(job.diagram(highlight=rate.highlight()))
# Job 'workshop': 9 nodes
#   highlight (*): trace of rate_hz at node rates, for subject_id='mouse_02', session='s1', probe='imec1', unit=2: 1 row matches
#  * sessions (source · CSVSource)
#  * SelectDataColumns_2 (operator) <- sessions
#  * units (source · CSVSource)
#  * SelectDataColumns_1 (operator) <- units
#  * Join (operator) <- SelectDataColumns_2, SelectDataColumns_1
# >* rates (function pod · firing_rate) [reused] [traced from here] <- Join
#    GroupBy (operator) <- rates
#    SelectDataColumns_3 (operator) <- GroupBy
#    session_means (function pod · mean_rate) <- SelectDataColumns_3
```

In a notebook the same call draws the picture. The caption is the question; the path is
outlined in the text colour and the rest is dimmed (no status of a node is shown in that
colour: red is kept for failed). The node it was asked at has a second outline and the
bold line "traced from here"; the mark is the bold line "reused":

![The diagram of the workshop job, captioned "highlighted: trace of rate_hz at node rates, for subject_id='mouse_02', session='s1', probe='imec1', unit=2: 1 row matches": the sources sessions and units, their column selections, the Join and the function pod rates outlined in a dark line; rates has a second outline and the bold lines "reused" and "traced from here"; GroupBy, its selection and the function pod session_means dimmed](provenance-diagram-trace.svg)

**Computed when?** `computed here`, `[computed]` and `[reused]` describe the **stored
entry**, in the run that recorded it. They do not describe the run you just made. For that,
read the run summary. Build the same job again over the same database, as a new Python session
would, and run it:

```python
again = PipelineJob(name="workshop", database=database)
with again:
    rates = firing_rate.pod(
        units.select_data_columns(["n_spikes"]),
        sessions.select_data_columns(["duration_s"]),
        label="rates",
    )
    mean_rate.pod(
        rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"]),
        label="session_means",
    )
print(again.run())
# Run of job 'workshop': all rows succeeded.
#   rates: 64 rows: 0 computed, 64 from store, 0 failed
#   session_means: 8 rows: 0 computed, 8 from store, 0 failed
warm = again.nodes["session_means"].trace(
    {"subject_id": "mouse_03", "session": "s2"}, "mean_rate_hz"
)
print([run.computed_here for label, run in warm.function_runs() if label == "session_means"])
# [True]
print(warm.highlight().marks == trace.highlight().marks)
# True
```

This run computed nothing: the summary counts 0 computed and 72 from the store. The trace and
its marks are those of the first job, because the entries are: they were recorded by the first run,
whose id and time the trace shows. A loaded job (`PipelineJob.load`) shows the same.

**Why is a row missing?** A second job keeps only the `imec0` probe and leaves out the
sessions of `mouse_04`. Row 4 of `units.csv` is an `imec1` unit, and row 48 is a unit of
`mouse_04`. Neither has a firing rate. `why_missing` says which step removed each:

```python
from orcapod.types import col

imec0 = PipelineJob(name="imec0", database=database)
with imec0:
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
    firing_rate.pod(
        units.filter({"probe": "imec0"}).select_data_columns(["n_spikes"]),
        sessions.filter(col("subject_id") != "mouse_04").select_data_columns(
            ["duration_s"]
        ),
        label="imec0_rates",
    )
imec0.run()
print(imec0.nodes["imec0_rates"].why_missing("units", "row_4"))
# why record row_4 of units is missing at node imec0_rates
# units row_4 is removed by Filter[...]: probe='imec0'
#   no successful entry of function node 'imec0_rates' is stored for it (the function raised on it, or never received it)
#   present in: units
print(imec0.nodes["imec0_rates"].why_missing("units", "row_48"))
# why record row_48 of units is missing at node imec0_rates
# units row_48 is removed by Join: rows without a match in 'sessions' are dropped
#   no successful entry of function node 'imec0_rates' is stored for it (the function raised on it, or never received it)
#   present in: units, ...
#   because: no row with session='s1', subject_id='mouse_04' in ... (reads sessions)
```

The first line repeats the question. The second answers it: which step removed the row.
The third is read from the stored records of `imec0_rates`: no run of this job stored a
result for the row. On its own that does not prove the function never saw it (a function
that raised on a row stores no result for it); the second line says why it did not.
`why_missing(...).highlight()` marks the path up to the step that removed the row; the
other nodes are dimmed, except the node asked at, marked "asked here". For `units row_4`,
the path ends at the filter on `probe`:

![The diagram of the imec0 job, captioned "highlighted: why record row_4 of units is missing at node imec0_rates: ...": the source units and the node Filter_2 outlined in a dark line; the sessions branch and the Join dimmed; the function pod imec0_rates with a second outline and the bold line "asked here"](provenance-diagram-missing-filter.svg)

For `units row_48`, the row passes the filter and its column selection and ends at the
Join, which has no session `s1` of `mouse_04` to match it with:

![The diagram of the imec0 job, captioned "highlighted: why record row_48 of units is missing at node imec0_rates: ...": the source units, Filter_2, SelectDataColumns_1 and the Join outlined in a dark line; the sessions branch dimmed; the function pod imec0_rates with a second outline and the bold line "asked here"](provenance-diagram-missing-join.svg)

The pictures are drawn by `job.diagram(...)` itself, from the job above.

## Starting a second pipeline from stored results

A finished analysis is often the input of the next one. `as_source()` turns the stored
results of a node into a source:

```python
session_means = job.nodes["session_means"].as_source()
print(type(session_means).__name__)
# DerivedSource
print(session_means.keys())
# (('subject_id', 'session'), ('mean_rate_hz',))
```

A `DerivedSource` is a source like those of Part 1, so it can be joined, filtered and fed to
pods. The first one you build prints a `ProvisionalHashWarning`, once: its identity is not
frozen yet, so a later release of Orcapod may compute the stages built on it again instead of
reusing what they stored (see [Provisional sources](../concepts/sources.md#provisional-sources)).
Here the next stage flags sessions with a low mean rate:

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

The next stage does not re-run the first. It reads what the first one stored. This is how
you split a large analysis into stages that different people, or different days, can own.
`as_source()` reads what the database holds, whether or not the node has run in this Python
session; for a node whose results were never stored, it has no rows. It reads every stored
result of the node, including those of earlier runs over other inputs, like
`get_all_records()` above; when you want only this run's output, use `as_table()`.

!!! warning "Not yet: a stored joined result as the source of a new job"
    The stage above runs in memory, so its own results are not stored. Recording it in a new
    `PipelineJob` does not work yet for `session_means`, or for any node downstream of a
    `Join` or `MergeJoin` of two sources. An input of a job must declare exactly one
    source-id and one record-id system tag. The source `session_means` has the schema
    of its node, which declares a pair for each joined source, so a job refuses it with
    `SourceSpecMismatchError` when it is bound. (Its rows hold only the stored tag and data
    columns: no system tags, and null source-info tokens.) Support is planned (ITL-1127).
    Until then, to store a later stage, add it to the first job.

## Key points

- `node.upstreams` and `node.label` walk the pipeline backwards from any result.
- `as_table(columns={"source": True})` and `columns={"system_tags": True}` reveal where
  values and rows came from; `all_info=True` shows everything.
- `get_all_records(columns={"meta": True})` shows how and when each result was computed.
- `as_table()` is the output of this job's run; `get_all_records()` is everything stored.
- `node.trace(row, column)` follows one value back to its source cells, and says which
  function computed each step and whether it reused a stored result.
- `node.why_missing(source, record_id)` says which step removed a row.
- `job.diagram(highlight=...)` marks a trace's or a missing row's path.
- `node.as_source()` starts a new pipeline from stored results.

## Exercises

**1.** Print the lineage of the `rates` node. How many sources does it depend on?

??? success "Solution"
    ```python
    print_lineage(job.nodes["rates"])
    # rates
    #   Join
    #     SelectDataColumns_1
    #       units
    #     SelectDataColumns_2
    #       sessions
    ```

    Two: `units` and `sessions`.

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

    Note the plain Polars filter on a table you have already taken out of Orcapod. Inside a
    pipeline you would write the same test as `stream.filter(...)` (Part 3).

**3.** Using `as_source()`, compute the mean of the session means per animal (one row per
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
