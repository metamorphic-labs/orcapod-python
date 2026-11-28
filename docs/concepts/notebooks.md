# Working in notebooks

Orcapod's streams, sources, job nodes, run summaries, jobs and pipelines display themselves
when they are the last line of a notebook cell. This page is where a notebook user starts:
what each display shows, how to see more, and which page states each rule in full. The
pictures come from JupyterLab. Outside a notebook, `print(stream.head())`, `print(summary)` and
`print(job.diagram())` give the same displays as text (`print(stream)` or `print(job)` does
not), and the samples below show that text.

The samples use the job from [Part 5 of the tutorial](../tutorial/05-pipeline-job.md), not run
yet, over the tutorial's dataset: to run them, first put `workshop_data.py` in your notebook's
working directory, as [Get the dataset](../tutorial/00-setup.md#get-the-dataset) in Part 0
shows.

```python
from workshop_data import make_dataset
from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import CSVSource

data_dir = make_dataset()

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


@function_pod(output_keys="rate_hz")
def firing_rate(n_spikes: int, duration_s: float) -> float:
    return n_spikes / duration_s


@function_pod(output_keys="mean_rate_hz")
def mean_rate(rate_hz: list[float]) -> float:
    return sum(rate_hz) / len(rate_hz)


job = PipelineJob(name="workshop", database=InMemoryArrowDatabase())
with job:
    rates = firing_rate.pod(
        units.select_data_columns(["n_spikes"]),
        sessions.select_data_columns(["duration_s"]),
        label="rates",
    )
    mean_rate.pod(
        rates.group_by(["subject_id", "session"]).select_data_columns(["rate_hz"]),
        label="session_means",
    )
```

## A stream at the end of a cell

In a notebook, a stream, source or job node shown at the end of a cell renders a short
table: its first rows in tag order, with a line saying what they are. Here is `units` as the
last line of a cell:

![The preview of the units source in JupyterLab: the line "CSVSource[units] — first 10 of 64 rows, sorted by tags", the line "bold = tag", and a table of ten rows whose four tag columns have bold blue headers and are separated from the three data columns by a vertical rule](notebooks-preview-light.png#gh-light-mode-only)
![The preview of the units source in JupyterLab's dark theme](notebooks-preview-dark.png#gh-dark-mode-only)

What it shows ([Looking at data](streams.md#looking-at-data) states each rule in full):

- **The header line** names the object (its kind and its name) and says which rows are shown:
  here `first 10 of 64 rows, sorted by tags`. A source that reads only part of its table
  says so, as a [SpiralDB source](sources.md) does ("10 rows read, sorted by tags, of about
  63,349").
- **Tag columns** come first and are marked, and so are data columns: in a notebook a tag's
  header is bold (in an accent colour), a data column's is underlined, a vertical rule
  separates the tags from the data columns, and a line under the header names the marks,
  "bold = tag; underlined = data". A [data-tag](data-tags.md) is one column, a tag that is
  also data: the preview shows it once, among the tags, with both marks (bold, in the
  accent colour, and underlined), and the line adds
  "bold underlined = data-tag (a tag that is also data)". The line names only the marks
  shown: the tags of `units` are all plain. (The pictures were taken before data headers
  were underlined.) When `columns` asks for the hidden column groups, system tags are
  marked as tags and the source-info, context, content-hash and meta columns are left unmarked.
- **Large cells** are summarised (an array as `ndarray float64 (2, 3)`, bytes as
  `bytes (300 B)`, long text and long lists shortened); `head().table` keeps the full values.

The preview is `head()` asked for `sort_by_tags`, and neither ever computes anything: they
show only what is readily available, never compute, poll or deliver a row (they read only
what is held or stored, or make a bounded table read), never call your code (a function, a
predicate, a side effect) and never write to a database. So a job node shown before the job
has run has no rows: its header says `not run`, and the table shows its columns only.

In a terminal, `print(stream.head())` prints the same preview as text. There, each plain tag
column's name starts with `*` ("* = tag"), and each data-tag's with `+`
("+ = data-tag (tag and data)"):

```python
print(units.head(3))
# CSVSource[units] — first 3 of 64 rows
# * = tag
# shape: (3, 7)
# ┌─────────────┬──────────┬────────┬───────┬────────┬──────────┬──────────┐
# │ *subject_id ┆ *session ┆ *probe ┆ *unit ┆ region ┆ depth_um ┆ n_spikes │
# │ ---         ┆ ---      ┆ ---    ┆ ---   ┆ ---    ┆ ---      ┆ ---      │
# │ str         ┆ str      ┆ str    ┆ i64   ┆ str    ┆ i64      ┆ i64      │
# ╞═════════════╪══════════╪════════╪═══════╪════════╪══════════╪══════════╡
# │ mouse_01    ┆ s1       ┆ imec0  ┆ 0     ┆ V1     ┆ 1580     ┆ 149      │
# │ mouse_01    ┆ s1       ┆ imec0  ┆ 1     ┆ V1     ┆ 2560     ┆ 326      │
# │ mouse_01    ┆ s1       ┆ imec0  ┆ 2     ┆ V1     ┆ 600      ┆ 662      │
# └─────────────┴──────────┴────────┴───────┴────────┴──────────┴──────────┘
print(job.nodes["rates"].head())
# FunctionJobNode[rates] — not run; run() fills it; node.as_source() reads stored results
# * = tag
# shape: (0, 5)
# ┌─────────────┬──────────┬────────┬───────┬─────────┐
# │ *subject_id ┆ *session ┆ *probe ┆ *unit ┆ rate_hz │
# │ ---         ┆ ---      ┆ ---    ┆ ---   ┆ ---     │
# │ str         ┆ str      ┆ str    ┆ i64   ┆ f64     │
# ╞═════════════╪══════════╪════════╪═══════╪═════════╡
# └─────────────┴──────────┴────────┴───────┴─────────┘
```

The same table read with `probe` and `unit` as data-tags (only `subject_id` and `session`
kept plain) marks the two kinds of tag apart:

```python
units_by_probe = CSVSource(
    str(data_dir / "units.csv"),
    tag_columns=["subject_id", "session", "probe", "unit"],
    tag_only_columns=["subject_id", "session"],
    source_id="units",
)
print(units_by_probe.head(2))
# CSVSource[units] — first 2 of 64 rows
# * = tag; + = data-tag (tag and data)
# shape: (2, 7)
# ┌─────────────┬──────────┬────────┬───────┬────────┬──────────┬──────────┐
# │ *subject_id ┆ *session ┆ +probe ┆ +unit ┆ region ┆ depth_um ┆ n_spikes │
# │ ---         ┆ ---      ┆ ---    ┆ ---   ┆ ---    ┆ ---      ┆ ---      │
# │ str         ┆ str      ┆ str    ┆ i64   ┆ str    ┆ i64      ┆ i64      │
# ╞═════════════╪══════════╪════════╪═══════╪════════╪══════════╪══════════╡
# │ mouse_01    ┆ s1       ┆ imec0  ┆ 0     ┆ V1     ┆ 1580     ┆ 149      │
# │ mouse_01    ┆ s1       ┆ imec0  ┆ 1     ┆ V1     ┆ 2560     ┆ 326      │
# └─────────────┴──────────┴────────┴───────┴────────┴──────────┴──────────┘
```

(`head()` does not sort, so its header does not say `sorted by tags`; the preview asks for
the sorted head.) What each kind of object shows before and after it is computed (a source,
a computed stream, a job node, a side-effect node) is the table "What each kind of object
shows" in [Streams](streams.md#computing-a-stream-outside-a-job-run).

## The run summary

`job.run()` (and `await job.run_async()`) returns a `RunSummary`: for each function node, how
many rows were computed, how many were read from the database, and how many failed. At the end
of a notebook cell it is shown as a table; `print` shows the same as text:

```python
summary = job.run()
print(summary)
# Run of job 'workshop': all rows succeeded.
#   rates: 64 rows: 63 computed, 1 from store, 0 failed
#   session_means: 8 rows: 8 computed, 0 from store, 0 failed
```

![The run summary of the tutorial job in JupyterLab: the line "Run of job 'workshop': all rows succeeded." over a table with the columns node, rows, computed, from store, failed and first error, one row each for rates and session_means](notebooks-run-summary-light.png#gh-light-mode-only)
![The run summary of the tutorial job in JupyterLab's dark theme](notebooks-run-summary-dark.png#gh-dark-mode-only)

One row per function node: `rows` is every input row the run accounted for in the node
(computed, from store and failed, and for a node the run held, its held rows), `computed`
how many its function computed in this run, `from store` how many it read back from the
database instead, `failed` how many raised, and `first error` the first of those errors, with the call that
prints its full traceback (`job.last_run.print_traceback("rates")`). Here one `rates` row
was found in the store during the run itself ([Part 5](../tutorial/05-pipeline-job.md#run-a-recorded-job)
of the tutorial explains why). A load-only run (`job.run(load_only=True)`) shows how many
rows each node loaded and how many had no stored result instead. The job keeps the summary as
`job.last_run`. A cell that ends with `job.run();` (a semicolon) shows nothing. What a run
does when a function raises for a row is
[Reading the run summary](function-pods.md#reading-the-run-summary).

## Seeing more rows

After the run, every node holds the rows of that run, and its preview shows them. To see
other rows:

- `node.head(n)` shows the first `n` rows; `head(n, columns={"sort_by_tags": True})` gives
  the rows the preview shows.
- `node.run()` at the end of a cell computes what is not computed yet and shows every row:
  in a notebook, `stream.run()` at the end of a cell computes and displays. A view of more
  than ten rows that nobody cut shows its first and last five rows, and its header gives the
  count.
- The view that `head()` or `run()` returns cuts and orders its own rows, without reading or
  computing anything: `view.head(n)`, `view.tail(n)`, `view.sort_by_tags()`, and chains of
  them such as `view.head(20).tail(10)`.
- For every row, as a data frame or a table, use `as_polars_df()`, `as_pandas_df()` or
  `as_table()`. A data frame at the end of a cell is shown by its own library.

```python
means = job.nodes["session_means"]
view = means.run()  # the node holds a current run: this computes nothing
print(view.header())
# FunctionJobNode[session_means] — all 8 rows
print(view.sort_by_tags().tail(3))
# FunctionJobNode[session_means] — last 3 of 8 rows, sorted by tags
# * = tag
# shape: (3, 3)
# ┌─────────────┬──────────┬──────────────┐
# │ *subject_id ┆ *session ┆ mean_rate_hz │
# │ ---         ┆ ---      ┆ ---          │
# │ str         ┆ str      ┆ f64          │
# ╞═════════════╪══════════╪══════════════╡
# │ mouse_03    ┆ s2       ┆ 5.059375     │
# │ mouse_04    ┆ s1       ┆ 5.136458     │
# │ mouse_04    ┆ s2       ┆ 5.129167     │
# └─────────────┴──────────┴──────────────┘
print(job.nodes["rates"].run().header())
# FunctionJobNode[rates] — all 64 rows
```

A view holds the default columns only (tags and data). For system tags, source info and
the other hidden columns, ask the node: `as_table(columns=...)`, as in
the tutorial [Inspecting results](../tutorial/inspecting-results.md#tracing-rows). `run()` on each kind of
object is the table "What `run()` does on each kind of object" in
[Streams](streams.md#computing-a-stream-outside-a-job-run).

## The pipeline diagram

!!! caution "In development"
    `Pipeline.diagram()`, `PipelineJob.diagram()`, `PipelineDiagram` and its methods and
    arguments, the look of the picture and the notebook display may change in any release
    without a deprecation period. The first diagram in a Python session issues
    `orcapod.errors.InDevelopmentWarning` once, as a reminder; to silence it, run
    `warnings.filterwarnings("ignore", category=InDevelopmentWarning)` after importing
    it from `orcapod.errors`.

```python
import warnings

from orcapod.errors import InDevelopmentWarning

warnings.filterwarnings("ignore", category=InDevelopmentWarning)
```

In a notebook, a job or a pipeline shown at the end of a cell draws itself: a diagram of its
nodes (sources, operators, function pods, side-effect pods) and the streams between them,
laid out left to right. Here is `job` as the last line of a cell, after its run, with the
`rates` node selected:

![The diagram of the tutorial job in JupyterLab: the sources sessions and units on the left, through column selections and a join into the function pod rates, then GroupBy, a selection and the function pod session_means on the right; the sources are marked bound and every other node current, the vertical and collapse checkboxes next to the zoom buttons are unticked, and the details panel of the selected node rates lists its tag and data columns, the node it reads from and the row counts of the last run](notebooks-diagram-light.png#gh-light-mode-only)
![The diagram of the tutorial job in JupyterLab's dark theme: the sources sessions and units on the left, through column selections and a join into the function pod rates, then GroupBy, a selection and the function pod session_means on the right; the sources are marked bound and every other node current, the vertical and collapse checkboxes next to the zoom buttons are unticked, and the details panel of the selected node rates lists its tag and data columns, the node it reads from and the row counts of the last run](notebooks-diagram-dark.png#gh-dark-mode-only)

[Seeing the graph](pipeline-job.md#seeing-the-graph) states each of these in full:

- **Moving and zooming.** Drag to move it and use the `+`, `−` and `fit` buttons (or Ctrl or
  Cmd with the mouse wheel) to zoom; from the keyboard, focus the diagram and use the arrow
  keys to move it, `+` and `-` to zoom and `0` to fit, and Tab to reach the nodes.
- **A node's details.** Click a node (or focus it and press Enter) to see its schema: its tag
  and data columns, the nodes it reads from and, for a job, its run status and the row counts
  of the job's last run.
- **Constraining inputs.** The right input of a `SemiJoin` only decides which rows pass: a
  dashed edge entering from the side of the box.
- **Statuses.** Each node of a job shows one: `not run`, `current`, `stale`, `failed`,
  `loaded only`, `partial`, or `bound` or `unbound` for a source slot. Before `job.run()`
  every node but the sources says `not run`.
- **The `vertical` and `collapse` checkboxes.** `direction="TB"` lays the graph out top to
  bottom, and `collapse=True` folds the steps that only choose, rename or join columns into
  the edge they lie on. In a notebook that runs scripts, the `vertical` and `collapse`
  checkboxes next to the zoom buttons switch between these layouts in place (`collapse`
  appears only when there is something to fold; neither appears above 100 nodes).
- **Nothing is computed.** Drawing a diagram never computes anything.
- **Saved with the notebook.** The picture is part of the cell's output, so it stays in a
  saved notebook and in an HTML export. `job.diagram().save("pipeline.html")` writes it as a
  page that opens in any browser.

Here is the same diagram after ticking `vertical` and then `collapse`: five nodes, top to
bottom, with the column selections and the join folded into the dashed edges. `rates` stays
selected, and its details now list the steps folded into its inputs:

![The same diagram with the vertical and collapse checkboxes ticked: five nodes from top to bottom, the sources sessions and units joined by dashed edges into the function pod rates, then GroupBy and, by a dashed edge, the function pod session_means; the details panel of the selected node rates lists sessions and units as its inputs and SelectDataColumns_1, Join and SelectDataColumns_3 as its folded steps](notebooks-diagram-vertical-light.png#gh-light-mode-only)
![The same diagram in JupyterLab's dark theme with the vertical and collapse checkboxes ticked: five nodes from top to bottom, the sources sessions and units joined by dashed edges into the function pod rates, then GroupBy and, by a dashed edge, the function pod session_means; the details panel of the selected node rates lists sessions and units as its inputs and SelectDataColumns_1, Join and SelectDataColumns_3 as its folded steps](notebooks-diagram-vertical-dark.png#gh-dark-mode-only)

`job.diagram()` makes the same diagram with options (`direction="TB"`, `collapse=True`), and
`print` shows it as text, one node per line with the nodes it reads from. A `Pipeline`, such
as the job's blueprint `job.pipeline`, draws itself the same way, without run statuses:

```python
print(job.diagram(collapse=True))
# Job 'workshop': 5 nodes
#   sessions (source · CSVSource · bound)
#   units (source · CSVSource · bound)
#   rates (function pod · firing_rate · current) <- sessions, units
#   GroupBy (operator · current) <- rates
#   session_means (function pod · mean_rate · current) <- GroupBy
```

### An untrusted notebook

Where a notebook does not run scripts (an untrusted notebook, a preview on a website), the
picture is still shown, without pan, zoom or selection; open the **Nodes** table below the
picture for each node's columns and status. JupyterLab runs the scripts of outputs you made
by running a cell. A notebook saved by someone else (or on another machine) opens untrusted:
run its cells again, or trust it (the **Trust Notebook** command, or
`jupyter trust <notebook>.ipynb` in a terminal), to get pan, zoom and selection back.

## Running asynchronously

`job.run(orchestrator=AsyncPipelineOrchestrator())` is written the same way in a script and in
a notebook. A notebook kernel already runs an event loop. There, `job.run(...)` runs the job
on a separate thread and waits for it, so the cell blocks until the run ends; the interrupt
button cancels the run. `await job.run_async()` also works at the top level of a notebook
cell. What the interrupt button does to a function in the middle of a row, and to an
`async def` function that blocks, is
[Running asynchronously, in a script or a notebook](function-pods.md#running-asynchronously-in-a-script-or-a-notebook).

## Classes defined in a notebook cell

A class defined in a notebook is named under `__main__`, so running its cell again makes a
new class. Define the classes your pods pass around with `@op.dataclass`: run again, the class
is still the same type, and pods and stored results keep working. An ordinary `@dataclass` or
a pydantic model run again is a new class, and pods built for the earlier one refuse it. See
[`op.dataclass` in a notebook](your-own-data-types.md#opdataclass-in-a-notebook).

## Other notebook front ends

The displays are HTML in the cell's output, with the diagram's pan, zoom, selection and
`vertical` and `collapse` checkboxes done by a script in that output; the pictures on this page come from
JupyterLab, the front end they are checked in.

- **Jupyter Notebook 7** is built on JupyterLab and runs the scripts of trusted outputs as
  JupyterLab does; it has not been checked separately.
- **VS Code** and **Google Colab** show HTML output and run its scripts in their own output
  frame; the displays have not been checked there.
- **Classic Notebook (6)** runs the scripts of trusted outputs; it has not been checked.

Where a front end does not run the diagram's script, the diagram shows as in an untrusted
notebook.
