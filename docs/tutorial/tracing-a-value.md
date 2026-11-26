# Tracing a value

`node.trace` follows one value of a result back to the source cells it was made from, and
names the function call behind every step. `node.why_missing` says which step removed a row
that has no result. Both read what the job holds and the database stores, and compute nothing.

**Before you start:** Parts 0 to 6 of the [core course](index.md#core-course).
[Inspecting results](inspecting-results.md) shows the columns and records a trace reads.

You will:

- trace one value to its source cells
- see which rows computed their result and which reused a stored one
- mark the path of a trace on the job's diagram
- find the step that removed a row

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

## Tracing one value

Give `trace` the tags of the row and the column:

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

The first line is the question: the column, the node, the row selector, and how many rows
match. Below it, the mean of `mouse_03`, session `s2`, was computed by `mean_rate`: the line
gives its version, the hash of its body, when and where it ran, and the job run that recorded
the entry. Its input was the grouped row of eight rates. Each rate was computed by
`firing_rate` from one row of `units.csv` and the row of `sessions.csv` for that session. The
operators a value passed through are listed after `via`.

- A function's row is matched to its stored record by its system tags, the hidden tags that
  name the source rows it came from.
- The members of a group are the rows before the grouping with the group's `subject_id` and
  `session`. Orcapod checks them against the group's folded system tags and raises an error
  if they do not agree.
- The last step reads the source-info columns: `csv_v1:units row_40` is row 40 of the source
  named `units`.

`source_cells()` lists the source cells, each once:

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

## Reused results

In session `s1` of `mouse_02`, two units have the same spike count, so `firing_rate` received
equal inputs twice and ran once. The trace says which of the two rows computed the result and
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

`imec1` unit 2 reused the result that `imec0` unit 0 computed. One stored result serves two
rows, and each is traced to its own row of `units.csv`.

## Several rows

Every row whose columns equal the values given is traced, and `{}` traces every row. Each
matching row has its own tree, in `trace.roots`, in tag order. `source_cells()` and the
highlight cover all of them:

```python
both = means.trace({"subject_id": "mouse_02"}, "mean_rate_hz")
print(both.header())
# trace of mean_rate_hz at node session_means, for subject_id='mouse_02': 2 rows match
print([dict(root.tags) for root in both.roots])
# [{'subject_id': 'mouse_02', 'session': 's1'}, {'subject_id': 'mouse_02', 'session': 's2'}]
print(len(both.source_cells()))
# 18
```

A selector that matches no row gives a trace that says so (`no held row matches`, status
`no_match`). A selector that names a column the node does not have is an error.

## On the diagram

`trace.highlight()` marks the path of a trace on the job's diagram. The listing marks the
nodes on the path with `*` and the node the trace was asked at with `>` and
`[traced from here]`. Each function node on the path says how its rows on the path got their
results: `[computed]`, `[reused]` (a result stored for equal inputs), or
`[computed + reused]`. It shows no run status. The first diagram in a Python session issues
`InDevelopmentWarning` once (see [Part 5](05-pipeline-job.md#nodes)).

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

A mean is made through every step of the job, so its path covers the whole diagram. The trace
of one firing rate covers the two sources, their column selections, the join and `rates`. Take
the unit that reused a stored result:

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

In a notebook the same call draws the picture. The caption is the question. The path is
outlined and the rest is dimmed. The node the trace was asked at has a second outline and the
line "traced from here", and `rates` has the line "reused":

![The diagram of the workshop job, captioned "highlighted: trace of rate_hz at node rates, for subject_id='mouse_02', session='s1', probe='imec1', unit=2: 1 row matches": the sources sessions and units, their column selections, the Join and the function pod rates outlined in a dark line; rates has a second outline and the bold lines "reused" and "traced from here"; GroupBy, its selection and the function pod session_means dimmed](provenance-diagram-trace.svg)

## Stored entries and the last run

`computed here`, `[computed]` and `[reused]` describe the **stored entry**, in the run that
recorded it. What the last run computed is in its run summary. Build the same job again over
the same database, as a new Python session would, and run it:

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
its marks are those of the first job, because the entries were recorded by the first run. A
loaded job (`PipelineJob.load`) shows the same.

## Missing rows

A second job keeps only the `imec0` probe and leaves out the sessions of `mouse_04`. Row 4 of
`units.csv` is an `imec1` unit, and row 48 is a unit of `mouse_04`. Neither has a firing rate.
`why_missing` says which step removed each:

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

The first line is the question. The second names the step that removed the row. The third is
read from the stored records of `imec0_rates`: no run of this job stored a result for the row.
A function that raised on a row stores no result for it either, so the third line alone does
not say the function never received the row; the second line does.

`why_missing(...).highlight()` marks the path up to the step that removed the row. The other
nodes are dimmed, except the node asked at, marked "asked here". For `units row_4`, the path
ends at the filter on `probe`:

![The diagram of the imec0 job, captioned "highlighted: why record row_4 of units is missing at node imec0_rates: ...": the source units and the node Filter_2 outlined in a dark line; the sessions branch and the Join dimmed; the function pod imec0_rates with a second outline and the bold line "asked here"](provenance-diagram-missing-filter.svg)

`units row_48` passes the filter and its column selection and ends at the Join, which has no
session `s1` of `mouse_04` to match it with:

![The diagram of the imec0 job, captioned "highlighted: why record row_48 of units is missing at node imec0_rates: ...": the source units, Filter_2, SelectDataColumns_1 and the Join outlined in a dark line; the sessions branch dimmed; the function pod imec0_rates with a second outline and the bold line "asked here"](provenance-diagram-missing-join.svg)

## Key points

- `node.trace(row, column)` follows one value back to its source cells, and says which
  function computed each step and whether it reused a stored result.
- `trace.source_cells()` lists the source cells.
- `computed` and `reused` describe the stored entry; the run summary describes the last run.
- `node.why_missing(source, record_id)` says which step removed a row.
- `job.diagram(highlight=...)` marks the path of a trace or of a missing row.

## Exercises

**1.** Trace the firing rate of unit 0 on probe `imec0` in session `s1` of `mouse_01`, at the
`rates` node. Which source cells was it computed from?

??? success "Solution"
    ```python
    first = job.nodes["rates"].trace(
        {"subject_id": "mouse_01", "session": "s1", "probe": "imec0", "unit": 0}, "rate_hz"
    )
    for cell in first.source_cells():
        print(cell.text())
    # csv_v1:sessions row_0 duration_s = 60.0
    # csv_v1:units row_0 n_spikes = 149
    ```

    One cell of each source: the session's duration and the unit's spike count.

**2.** Row 56 of `units.csv` is an `imec0` unit of `mouse_04`, session `s2`. Which step of the
`imec0` job removes it?

??? success "Solution"
    ```python
    print(imec0.nodes["imec0_rates"].why_missing("units", "row_56"))
    # why record row_56 of units is missing at node imec0_rates
    # units row_56 is removed by Join: rows without a match in 'sessions' are dropped
    #   no successful entry of function node 'imec0_rates' is stored for it (the function raised on it, or never received it)
    #   present in: units, ...
    #   because: no row with session='s2', subject_id='mouse_04' in ... (reads sessions)
    ```

    The row passes the filter on `probe`. The Join removes it, because the filter on
    `sessions` left no session of `mouse_04` to match it with.
