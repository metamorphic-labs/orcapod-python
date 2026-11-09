# Workshop Facilitator Notes (PLT-4864)

Date: 2026-11-09. For the Orcapod workshop with external collaborators next week.
Material: `docs/tutorial/` (parts 0-10), `docs/troubleshooting.md`.

**Bottom line:** the tutorial is about 3.5 hours of self-paced work; a live session of 90 to
120 minutes should demo parts 1-6 and run four exercises live, and leave parts 7-10 as
self-study. Three decisions are needed from you before the day (first three open
questions), and two library bugs will be visible to attendees in part 3.

## Before the workshop: send this to attendees

Ask them to do this the day before; it takes ten minutes and surfaces install problems
while there is still time. It is the content of `docs/tutorial/00-setup.md`.

1. Python 3.11 or newer.
2. A fresh environment with Orcapod:

   ```bash
   mkdir orcapod-workshop && cd orcapod-workshop
   python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
   pip install orcapod
   ```

   (`uv init && uv add orcapod` works as well.)
3. The dataset generator, in that directory:

   ```bash
   curl -O https://raw.githubusercontent.com/metamorphic-labs/orcapod-python/main/docs/tutorial/workshop_data.py
   python workshop_data.py
   ```

   Expected output: `Wrote 27 files (357 kB) to .../workshop_data`.
4. The check command:

   ```bash
   python -c "
   from workshop_data import make_dataset
   from orcapod import PipelineJob, function_pod
   from orcapod.databases import InMemoryArrowDatabase
   from orcapod.sources import CSVSource

   @function_pod(output_keys='rate_hz')
   def rate(n_spikes: int) -> float:
       return n_spikes / 60

   d = make_dataset()
   job = PipelineJob(name='check', store=InMemoryArrowDatabase())
   with job:
       units = CSVSource(str(d / 'units.csv'), tag_columns=['subject_id', 'session', 'probe', 'unit'], source_id='units')
       rate.pod(units.select_data_columns(['n_spikes']), label='rate')
   job.run()
   print('OK', job.nodes['rate'].as_table().num_rows, 'rows')
   "
   ```

   Expected last line: `OK 64 rows`. Anything else: reply with the full output.

Optional: Graphviz (`dot` on the `PATH`), only for the one `show_graph()` sample in part 8.

Tell them in advance that `Protobuf gencode version` warnings and
`WARN deltalake_core::writer::stats` lines are harmless.

## Suggested live path, 105 minutes

Times are for a room where everyone has completed the checklist. Demo by typing, not by
showing slides; attendees follow on their own laptops with the page open.

| Min | Part | Do live | Skip or mention |
|---|---|---|---|
| 0-5 | Framing | One slide or two sentences: the goal is the per-session mean firing rate, stored and re-runnable. Show the dataset tree from the tutorial front page. | |
| 5-20 | 1. First stream | `CSVSource` for subjects and sessions; tags versus data; `output_schema`, `as_pandas_df`. **Exercise 1 live** (choose tags for `units.csv`, 4 min), then discuss the "is `region` a tag?" point. | `DictSource`: mention. Exercise 2 (the two errors): show both yourself in one minute. |
| 20-40 | 2. Function pods | `to_mm`, then the exact-column error **on purpose** before `select_data_columns`. `firing_rate` over two streams. **Exercise 1 live** (fix the failing call, 3 min). | Multiple outputs: show `describe_depth`, no exercise. |
| 40-50 | 3. Operators | `join` on shared tags; equality `polars_filter`. Say plainly that expression filters and mixed-origin `semi_join` are broken today and show the page's two warning boxes. | `MergeJoin`, `map_data`: mention only. No live exercise. |
| 50-62 | 4. Many rows into one | `group_by`, the list-valued row, `mean_rate`. The result table with knockouts lower. | `batch`: one sentence. Exercise 1 (per probe) as a 3-minute warm-up if the room is fast. |
| 62-80 | 5. Keeping results | The `with` block, `run()`, `calls` going 0 → 71 → 71. Then `DeltaTableDatabase` and the second job that computes nothing. **Exercise 1 live** (`run_pipeline.py` twice, 5 min): this is the moment the product makes sense. | Directory layout: show `ls`, do not explain. |
| 80-97 | 6. Re-runs | Add two rows (2 calls). Then **edit the function and get the old numbers**: let it land, ask the room why. Then `version="v1.0"`. **Exercise 1 live** as a show of hands (predict a-d), then run it. | The hashes section: one sentence, point to the concept page. |
| 97-105 | Wrap-up | Open `docs/troubleshooting.md` and scroll it. Point to parts 7-10 by need: files (7) for anyone with per-session folders, tracing (8), failures (9). | |

For a 90-minute slot: drop the part 4 warm-up and do part 3 in five minutes (join and
equality filter only). For 120 minutes: add part 7 up to "What makes a file result
recompute" (15 min); it is the most persuasive demo for people whose data is files.

### Things that will come up, and the answer

- **"I changed my function and nothing happened."** Part 6. Major version. This will
  happen to someone during the session whether or not you have reached part 6; when it does,
  use it.
- **"It says my schema is not compatible."** Part 2, rule 2. `select_data_columns`.
- **"Why 63 calls for 64 rows?"** Identical inputs share one result. It is in part 5; it is
  a good question to take from the room.
- **"My output has the rows in a different order."** Rows read back from a store are not
  ordered. Sort. (Part 5 says so.)
- **A wall of `WARN deltalake_core` lines** on the first Delta run. Harmless; warn people
  before you press enter, because it looks like a failure on a projector.
- **Tracebacks during part 9.** The default error policy prints a full traceback per failed
  row and carries on. On a projector this looks like a crash. Say "the run is still going"
  before running it.

### What is deliberately not in the live path

Side-effect pods, `Pipeline` blueprints and `bind`, save/load, observers, Ray. They are in
part 10 and `docs/concepts/pipeline-job.md`. Each needs explanation that does not pay off
in a first session.

## State of the material

- Every `python` sample in the tutorial, the two new concept pages and the troubleshooting
  page runs in the test suite (`tests/test_docs/`): 135 samples on the new pages, one
  skipped (`show_graph()`, needs Graphviz). The whole `tests/test_docs` directory takes
  about 30 seconds.
- The tutorial's central claim, "a second run computes nothing", is pinned by
  `tests/test_docs/test_rerun_computes_nothing.py` under five hash seeds. It failed on about
  half of all Python processes before PR #272; **the workshop depends on #272 being in
  v0.2**.
- The dataset generator is tested for byte-identical output
  (`tests/test_docs/test_workshop_data.py`).
- Not tested: Windows; a real `pip install orcapod` of the released v0.2 wheel (the samples
  ran against the source tree of this branch).

## Library problems attendees will see

Described as-is in the tutorial and on the troubleshooting page; none is worked around
silently.

| Problem | Where it shows | What the tutorial says |
|---|---|---|
| `polars_filter` with a Polars expression cannot be hashed: fails before a join and anywhere in a `PipelineJob` | Part 3 | Known bug; use equality filters in pipelines |
| `semi_join` between a `CSVSource` and a `DictSource` or filtered stream: `ArrowInvalid ... string and ... large_string` | Part 3 | Known bug; use `join` plus a filter |
| `CSVSource` rejects `pathlib.Path` | Part 1 onwards | Wrap in `str(...)` |
| Editing a function body does not invalidate results | Part 6 | Bump the major version |
| Function pods reject extra columns (ITL-807) | Part 2 | `select_data_columns` |
| `list(job.nodes)` order varies with the hash seed | Part 5 | Samples print `sorted(job.nodes)` |
| `job.show_graph()` does not exist; it is `job.pipeline.show_graph()` | Part 8 | Stated |
| A `@sink_pod` function without a `ctx` parameter fails at run time | Part 10 | Stated |
| A loaded job cannot `as_table()` a function node, and its operator nodes are named `SelectDataColumns:semantic_v0.1:...` | Part 10 | Use `get_all_records()` |
| Under the default error policy a failed row silently shrinks a downstream group | Part 9, exercise 3 | Check row counts; `fail_fast` |
| Default record IDs are positional (ITL-817, ST2): reordering the rows of a source file changes row identity, so sinks re-deliver | Not shown; could surface if an attendee sorts a CSV | Not covered; `record_id_column` is documented in `docs/concepts/sources.md` |
| `FunctionPod.from_fn` defaults to `v1.0`, `@function_pod` to `v0.0`; `map_data` method drops unmapped columns, `MapData` class keeps them | Parts 10 and 3 | Stated |

## Open questions for the developer

1. **Is `pip install orcapod` the v0.2 release by workshop day?** The setup page says
   `pip install orcapod` / `uv add orcapod`, with the GitHub install as the fallback. If the
   release is not on PyPI in time, the checklist above must say
   `pip install git+https://github.com/metamorphic-labs/orcapod-python.git` instead (and
   attendees then need `git`).
2. **Where will attendees read the tutorial?** The pages assume a published docs site, from
   which `workshop_data.py` can be downloaded (MkDocs serves it next to the pages). If the
   site is not published by then, the `curl` line from the repository works as long as the
   repository is public under `metamorphic-labs`; otherwise hand out the file. Is the
   repository public, and has the move from `nauticalab` happened?
3. **Should the generator ship in the package?** It lives in `docs/tutorial/workshop_data.py`
   so that this PR touches nothing under `src/`. Shipping it as, say,
   `python -m orcapod.tutorial_data` would remove the download step and the one command
   attendees are most likely to get wrong. It needs no dependency. Your call for v0.2 or
   later.
4. **Will the two part-3 bugs be fixed before the workshop?** If the expression-filter and
   `semi_join` bugs are fixed, two warning boxes and two `try/except` samples in
   `03-operators.md` (and three entries in `troubleshooting.md`) should be removed; the
   samples will fail the docs test when the behaviour changes, which is the reminder.
5. **Does #267 (no `pygraphviz`) make v0.2?** The setup and troubleshooting pages assume
   it does: they say Graphviz is optional and that the `pygraphviz` build failure means an
   older version.
6. **`README.md` and `mkdocs.yml` still say `nauticalab`** (`repo_url`, the install line in
   `docs/index.md`, `pyproject.toml` homepage). New text in this PR uses `metamorphic-labs`.
   The existing lines belong to the release PR (#269) and were left alone.
7. **Ray.** The stale Ray notebook was removed. Part 10 mentions the `orcapod[ray]` extra in
   one sentence and nothing more. If collaborators are expected to use a cluster soon, that
   needs its own tested page.
8. **Windows.** Nothing here was run on Windows. If any attendee is on Windows, someone
   should run the checklist on one machine before the day.
