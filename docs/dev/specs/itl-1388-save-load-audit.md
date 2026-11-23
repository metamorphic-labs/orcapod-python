# ITL-1388 audit: saving and loading a pipeline and a pipeline job

Audit only: no library code changed. Measured on `main` at `fc1df420` (2026-11-22). Every
claim is reproduced by `docs/dev/scripts/itl_1388/` (see its `README.md`); each reader runs in
a fresh process that never imported the module defining the pods. Synthetic data only.

**The developer's goal (2026-11-22):** anyone can load a saved job file and, even without the
function pod definitions, view each node's loaded content; a loaded node applies the chain of
constraints from its upstreams' content and does not show every row of its tables.

## Summary

- **The goal is met by one path: `job.run(load_only=True)` (or `node.run(load_only=True)`).**
  In every case measured, with the pods importable, absent or changed, it gives exactly the
  rows the live job showed for every function, operator and side-effect node, and after the
  source changes it gives the rows of the source as it is now (counting the inputs with no
  stored result). It needs the sources to be rebuildable from the file or bound again.
- **`node.as_source()` / `get_all_records()` show what is stored, by design.** They show
  every row stored under the node's content hash: the rows of every run of the job over the
  same source ids (in the main case 7 rows where the live job shows 5, including two values
  for one tag, `s1/2` = 4.0 and 5.0), and with a `where`-restricted source the unrestricted
  runs' rows too. They separate jobs whose source ids differ (case i: no row of the other job
  ever appears). **Not a defect** (developer, 2026-11-22: "node.as_source IS meant to be
  what's stored, so that's fine: it is not tied to a specific run"). Whether to offer a
  second view that keeps the run's upstream restriction is an open question, ITL-1399
  (low priority).
- **Whether the pods are importable or changed makes no difference**: the loader never
  imports the pods module (checked: `pods_imported_by_loaded_job = False` in every
  situation), and nothing in a loaded job runs user functions.
- **Issues filed** (A), with the developer's decisions of 2026-11-22: rebuilding an
  `orcapod.dataclass` column from a saved schema, a missing feature (ITL-1390); connector
  databases (SQLite) saved but never rebuilt (ITL-1391); `Pipeline.load` + `from_pipeline`
  unusable with any function node (ITL-1392); `PollingSource` saved but never rebuilt
  (ITL-1393); a missing database location must raise, never be created (ITL-1395); a
  load-only run with unbound sources says nothing (ITL-1398); a `CachedSource` presenting two
  rows for one key (ITL-1396, live, found in passing); two misleading messages (ITL-1397);
  the run-restricted view question (ITL-1399).

## The jobs measured

`writer.py` builds the job `main` over three CSV sources (`readings` with tags `subject`,
`session`; `meta`; `selected`), all with key columns:

| Node | Kind | Above it |
|---|---|---|
| `score`, `maybe`, `summary` (exact `@dataclass` output), `pair` (`orcapod.dataclass` output) | function | `readings` |
| `filt` (`polars_filter`, **LOG**) → `label_after_filter` | operator → function | `score` |
| `dropnulls` (OFF) → `kept_after_dropnulls` | operator → function | `maybe` |
| `join` (with `meta`, **LOG**) → `after_join` | operator → function | `score` |
| `semijoin` (with `selected`, OFF) → `after_semijoin` | operator → function | `score` |
| `groupby` (OFF) → `after_groupby`; `batch` (OFF) → `after_batch` | operator → function | `score` |
| `tap` (`tap_pod`) → `after_tap` | side effect → function | `score` |
| `readings_c` (`CachedSource`) → `score_cached` | source → function | |

`main` is run three times, each time a fresh job object built from code over a new
generation of `readings.csv` (generation 2 changes `s1/2` and adds `s3/1`; generation 3 drops
`s1/2` and adds `s3/2`), then saved. Then `other` (same job name, same database, another CSV
with source id `readings_other`) and `separate` (the same with job name `separate`) run.
`downstream` reads `main`'s `score` through `node.as_source()` (`DerivedSource`); `polling`
reads a `PollingSource`. Databases: Delta (all jobs); SQLite through `ConnectorArrowDatabase`
for an operator-only form of `main` (a connector database refuses a function node's tables,
as `docs/concepts/pipeline-job.md` documents); in-memory in one process. PostgreSQL: no
local server, not run.

Situations: **a** pods importable and unchanged; **a_imported** the same, imported before
loading; **b** pods module absent; **c** pods importable but changed (body of `score`,
parameter name of `maybe`, output type of `label`, major version of `mean_score`).

## 1. What `save` writes

`job.save(path)` writes JSON (`orcapod_pipeline_job_version` 0.4.0, `orcapod_version`):

- `pipeline`: the blueprint (`Pipeline.save`'s content): nodes by blueprint hash, `edges`.
  Every node: `node_type`, `label`, blueprint `content_hash`, `pipeline_hash`,
  `output_schema` (Arrow type text per column, plus `nullable`), `node_uri`,
  `data_context_key`.
  - source: `source_config` of the slot (`name`, tag and data Arrow types, nullability).
  - function: `function_config` with the uri, `data_function_type_id`, and the
    `PythonDataFunction` config: `module_path`, `callable_name`, `version`,
    `input_data_schema` / `output_data_schema` as Python repr text (`"<class 'float'>"`),
    `output_keys`; `table_scope`, `fall_through_tags`. No code.
  - operator: `operator_config` (`class_name`, `module_path`, its config: `PolarsFilter`'s
    predicates as Polars expression JSON, `GroupBy.by`, `Batch.batch_size`, ...). Not its
    cache mode (ITL-977).
  - side effect: label, hashes, output schema only; no function, no `SideEffectPodConfig`.
  - a user-class column: its full identifier, `extension<name@hash;hex(record)>`.
- `run`: `run_id`, `status`, `unbound_sources`, `node_job_content_hashes` (the run's content
  hash of every function, operator and side-effect node: what the stored rows carry) and
  the advisory `expected_rows`.
- `bindings.sources`: each source's `to_config()`: CSV and Delta paths **as absolute paths**,
  tag and key columns, source id, hashes, schemas; a `CachedSource` embeds its inner source
  and its cache database config; a `PollingSource` its `impl_module` / `impl_class`; a plain
  `DerivedSource` no origin (so it cannot be rebuilt, ITL-956); in-memory sources only their
  schema (no data).
- `bindings.database`: the database's `to_config()`: Delta `root_uri` (**absolute path**);
  SQLite the `db_path`; in-memory just `{"type": "in_memory"}`. What a job file holds about
  database locations is covered in a private issue.

Machine-specific content: every CSV, Delta source and database path, and the cache database
path of a `CachedSource`.

`Pipeline.save` writes the `pipeline` part alone.

## 2-3. What `load` gives back, and what each read shows

A loaded job rebuilds: source slots (`SourceJobNode`, bound when the source config can be
rebuilt), function nodes as stubs without a pod (`FunctionJobNode`, `function_pod=None`),
operators from their config (`OperatorJobNode`), side-effect nodes read-only
(`LoadedSideEffectJobNode`). Situations a, b and c give identical records for every node
(`run_audit.py`, `analyze.py`).

Reads per node (Delta, situation b; `reader.py`, `analyze.py`):

| Read | Before a run | After `job.run(load_only=True)` |
|---|---|---|
| `as_table()`, `iter_data()` | 0 rows (not run in this process) | the live job's rows, exactly |
| `head()` | `NOT_COMPUTED`: "not run; run() fills it; node.as_source() shows every stored result" | `LOADED_ONLY`: "all 5 rows; loaded only: 5 loaded, 0 inputs without a stored result" |
| notebook preview | renders, says not run | renders the rows |
| `output_schema()`, `keys()` | the saved schema (except user classes, below) | same |
| `as_source()` (function node) | every stored row of the node's key (B1) | same |
| `as_source()` (operator node) | LOG: every logged row of the key; OFF: 0 rows | same |
| `node.run(load_only=True)` (fresh load) | the live rows, running what it depends on first | |
| plain `job.run()` | `RuntimeError: PipelineJob.run() cannot execute a loaded job: its function pods are not saved with it, so it cannot compute. Fill its nodes from the database with run(load_only=True) (bind in-memory sources first), or read a node's stored results with node.as_source(). ...` | |

Source and side-effect nodes have no `as_source()` (`AttributeError`). A side-effect node
passes its input rows on in a load-only run and delivers nothing.

**User-class columns.** `summary` (exact `@dataclass`) and `pair` (`orcapod.dataclass`):
when the class is not defined in the process (situations a, b, c; the module importable or
not), `output_schema()`, `as_table()`, `head()` and `as_source()` raise `ValueError: Cannot
restore the output schema of loaded node 'pair': Column 'pair' has the saved type
'extension<Pair@...>', which cannot be restored to a Python type.` After a load-only run
`as_table()` returns the rows (as plain structs) while `output_schema()` still raises. When
the module is imported before loading (a_imported) every read works. For the structural class
this contradicts `docs/concepts/your-own-data-types.md` ("rebuilt from stored metadata"):
ITL-1390.

**Operator caches (case viii).** The docs say to attach a loaded operator in `REPLAY` to read
its log (`docs/concepts/function-pods.md`, "Saving and loading a job"). Measured: a loaded
LOG operator's `as_source()` reads its log without that (5 rows for `filt`); attaching
`REPLAY` gives the same. An OFF operator stores nothing: its `as_source()` is empty, and only
a load-only run shows its rows (by re-running it over the loaded upstream rows). The doc
sentence is out of date (minor; not filed separately).

### Works / works with a limit / does not work

| Node kind | Delta, a/b/c | SQLite (operators) | In-memory DB |
|---|---|---|---|
| Source node (CSV) | works (rebuilt from absolute path) | works | n/a |
| Source node (in-memory `DictSource`) | limit: not saved; bind it again, then works | | limit: same |
| `CachedSource` | works (reads its cache; see ITL-1396 for what the cache holds) | not run | |
| `DerivedSource` (plain, over a job node) | limit: not rebuilt; nodes below stay empty; message says "in memory" (ITL-956, ITL-1397) | | |
| `PollingSource` | does not work: never rebuilt (ITL-1393) | | |
| Function node, load-only run | works | n/a (refused by the database) | works with `database=` given |
| Function node, `as_source()` | works: everything stored, every run's rows (by design, B1) | n/a | works with `database=` given |
| Function node, user-class output | does not work without the class (ITL-1390); works when imported first | n/a | |
| Operator node, load-only run | works (re-run over loaded inputs) | works only with `database=` given (ITL-1391) | |
| Operator node, `as_source()` | LOG: every logged run (by design); OFF: empty | same, with `database=` | |
| Side-effect node | works (passes rows on, never delivers) | | |
| Database rebuilt from the file | works | does not work (ITL-1391) | limit: rebuilt empty, warns |
| Pipeline blueprint → job | does not work with a function node (ITL-1392) | | |

## 4. The constraint chain

Rows shown vs the live job after its third run, as multisets (`analyze.py`; Delta, b; SQLite
with `database=` given). "load-only" is `job.run(load_only=True)` then `as_table()`;
`node.run(load_only=True)` gives the same in every row of this table.

| Case | Node | load-only | `as_source()` | Mechanism |
|---|---|---|---|---|
| (i) two jobs, one database and name, different source ids | `score` of `other` | = live (3) | = live (3): no row of `main` | `__node_content_hash` (source id enters the content hash) |
| (i) contrast: other job name | every node of `separate` | = live | = live | nothing shared: every table path is relative to the job's database scoped by the job name, `database.at(<job name>)`; the result table too (`<name>/_result/...`). Only the `CachedSource` cache, at the path its own database was given, is outside it |
| (iii) filter above a function | `label_after_filter` | = live (4) | 5 (+1 earlier run) | load-only: re-run of the rebuilt `PolarsFilter` over loaded `score` rows, then entry-id lookup |
| (iii) drop_nulls | `kept_after_dropnulls` | = live (4) | = live (4) (earlier runs gave the same rows) | same |
| (iv) join | `after_join`, `join` (LOG) | = live (5) | 7 (+2 earlier runs) | same |
| (iv) semijoin | `after_semijoin`; `semijoin` (OFF) | = live (3) | 5 (+2); OFF operator: 0 | same |
| (v) group_by / batch | `after_groupby`, `after_batch` | = live (3) | 6 / 7 (+3 / +4 earlier runs) | same; the folded system tags give the entry ids |
| (vi) three runs, source changed between them | `score` | = live (5) | 7 (+2): `s1/2` both 4.0 and 5.0 | load-only reads the CSV as it is now |
| (vi-b) file back at generation 1 after the save | `score` | = generation 1 (4), not_stored 0 | 7 | `scenarios.py` |
| (vi-c) file with a row no run saw (`s4`) | `score` | 5 loaded, `not_stored` 1; nodes below report `inputs_incomplete: ['score']` | 7 | same |
| (vii) upstream function not importable | every node below `score` | = live | as above | the chain needs no function: it uses the stored rows of `score` |
| (viii) operator with / without cache | `filt`, `join` (LOG); `semijoin`, `groupby`, `batch`, `dropnulls` (OFF) | = live | LOG: every logged run; OFF: 0 | |
| SQLite, operator-only form | `filt`, `join` (LOG) | = live | 6 (+2 earlier runs) | same as Delta |
| (ii) source restricted with `where(subject_id="m2")`, after an unrestricted run of the same job (`where_case.py`, the test suite's SpiralDB double) | `double` | = live restricted (2: `m2` only), summary `conditions` `{'events': subject_id='m2'}` | 5: the unrestricted run's `m1` rows too, live and loaded alike | the saved `where` is rebuilt with the source and the load-only run reads through it; a restriction is in no identity (restricted and unrestricted content hashes equal), so `as_source()` shows both runs: by design |

**No read showed a row of another job sharing the tables.** No case found a node showing every
row of a shared table. The limit is across runs of one job over one source id: the content
hash of a source is an address, not a digest of its rows, so every run's rows share one key.

### Why a SQLite database does not come back (ITL-1391)

`connector_database_case.py` (ten lines of job, run once):

1. `job.save` writes the database as `{"type": "connector_arrow_database", "connector":
   {"connector_type": "sqlite", "db_path": "<absolute path>/store.sqlite"}, "base_path": [],
   "max_hierarchy_depth": 10}`.
2. `PipelineJob.load` looks `type` up in `DATABASE_REGISTRY` (`pipeline/serialization.py`),
   which holds `delta_table`, `in_memory` and `noop` only. It logs `Could not reconstruct the
   database from its saved config: Unknown database type: 'connector_arrow_database'. Known
   types: ['delta_table', 'in_memory', 'noop']. Node results cannot be read until a database is
   given: PipelineJob.load(path, database=...) or job.bind(database=...).`
3. The loaded job's `database` is `None`: `as_source()` raises `Cannot create a DerivedSource
   without a database`, `run(load_only=True)` raises `PipelineJob.run() requires a database`.
4. Saving the loaded job again writes `"database": null`: the file has lost its database.

Database kinds with a loader: Delta, in-memory (rebuilt empty, with a warning), no-op.
Without one: every `ConnectorArrowDatabase` (SQLite, PostgreSQL, SpiralDB connectors), though
each writes a config. Passing `PipelineJob.load(path, database=ConnectorArrowDatabase(
SQLiteConnector(...)))` makes every read work (section 4, SQLite row).

## 5. What a loaded job refuses or cannot do

| Situation | What happens | Tells what to do? |
|---|---|---|
| plain `job.run()` | `RuntimeError` (text in section 2-3) | yes |
| node with an unbound source, `node.run(load_only=True)` | `UnboundSourceError: SourceJobNode 'readings' has no concrete source bound. Call job.bind(sources={'<name>': source}) before running.` | partly: a literal `<name>` (ITL-1397) |
| unbound source, `job.run(load_only=True)` | returns a summary with no nodes, no warning | no: "unfriendly and should be improved" (developer), ITL-1398 (related: ITL-1318) |
| database moved / missing (Delta) | loads; load creates the missing root, reads create table folders; reads give 0 rows; one `OrcapodDatabaseContentWarning` per node when `expected_rows` is saved ("the job file records 7 rows ... holds 0") | no. Decided (developer): when the saved location does not exist, raise naming the path; never create it on load or read (ITL-1395) |
| moved, `database=` given | `as_source()` works; sources still point at the old absolute paths, so the load-only run runs nothing until the sources are bound again; then = live | the log says to bind |
| SQLite database in the file | `WARNING Could not reconstruct the database ... Unknown database type: 'connector_arrow_database' ... Node results cannot be read until a database is given: PipelineJob.load(path, database=...)` | yes, but it should not happen (ITL-1391) |
| in-memory database, no `database=` | `WARNING Loading pipeline with in-memory database. Cached data from the original run is not available`; reads empty | yes |
| source not rebuildable (`PollingSource`, plain `DerivedSource`, in-memory) | logged (WARNING / INFO), slot unbound | yes, says to bind (wrong reason for `DerivedSource`, ITL-1397) |
| user class not defined | `ValueError ... cannot be restored to a Python type` | no (ITL-1390) |
| data context not known (`std:v9:default`) | `ContextResolutionError: Unknown context version 'v9' (resolved from 'std:v9:default'). Available: v1` | names the cause only |
| file of format 0.3.0 | `ValueError: Unsupported format version '0.3.0' ... this Orcapod reads only format 0.4.0. Save the pipeline again with this version of Orcapod.` | yes |
| run content hash missing | `ValueError: <path>: saved nodes ['score'] have no run content hash text ... Save the job again with this version of Orcapod.` | yes |
| `Pipeline.load` → `from_pipeline` | `RuntimeError: to_invocations() cannot serialise FunctionNode ...` | points to `PipelineJob.load` (ITL-1392) |

A loaded node whose rows cannot be named (ITL-1043) was not met: every saved node had its run
content hash.

## 6. Round trips

`roundtrip.py`, after a reader's load-only run and plain run:

- `main` (Delta): the second file equals the first except `run.run_id` (the load-only run
  gave the job a new run id). Every node's content hash, pipeline hash and table path of the
  loaded job equals the live job's.
- `downstream`, `polling`: the second file has lost the source the loader could not rebuild
  (`bindings.sources` entry gone, `unbound_sources` lists it, `status` `partial`): saving a
  loaded job is lossy (covered by ITL-1391 / ITL-1393's goals; the general rule is B5).
- SQLite: the second file has `"database": null` (ITL-1391).
- `Pipeline.save` → `Pipeline.load` → `save`: identical.

## 7. The blueprint path

`Pipeline.load(main_pipeline.json)` works and saves back identically. `PipelineJob.
from_pipeline(pipeline, database=..., sources=...)` raises for a pipeline with a function
node, with the pods importable or not (ITL-1392). So a blueprint cannot today become a job,
read-only or runnable. Operator-only blueprints were not tried.

## (A) Issues filed

| Issue | Kind | What |
|---|---|---|
| ITL-1390 | missing feature (Medium) | rebuild an `orcapod.dataclass` column from its saved identity record ("a clear gap in implementation", developer); an exact class needs an import the message does not mention |
| ITL-1391 | defect | connector database (SQLite) saved but never rebuilt; resave drops it (explained above) |
| ITL-1392 | defect | `Pipeline.load` + `from_pipeline` fails with any function node |
| ITL-1393 | defect | `PollingSource` saved but never rebuilt (unknown source type) |
| ITL-1395 | defect, behaviour decided | a missing saved database location must raise naming the path; load and reads never create it |
| ITL-1398 | defect | a load-only run with unbound sources returns an empty summary and says nothing |
| ITL-1396 | defect (live) | `CachedSource` with key columns presents two rows for one key |
| ITL-1397 | defect (Low) | messages: `DerivedSource` called "in memory"; literal `<name>` in `UnboundSourceError` |
| ITL-1399 | question (Low) | whether to offer a stored view keeping the run's upstream restriction, beside `as_source()` |

Not a defect: `as_source()` / `get_all_records()` showing everything stored for a node (B1).

Existing issues that cover further findings: ITL-1318 (a load-only run with an unbound
source misreports its nodes), ITL-956 (a plain `as_source()` source cannot be rebuilt), ITL-977
(operator cache mode not saved), ITL-1129 (`as_source()` and superseded results).

## (B) Design gaps against the goal

**B1. No view of the saved run without the sources.** `as_source()` and `get_all_records()`
show what is stored for a node, not tied to a run: that is their intended meaning (developer,
2026-11-22), not a defect. The rows one run produced are shown today only by re-reading the
sources (`run(load_only=True)`). Whether a run-restricted view should exist at all is not
clear ("not clear what that would mean", developer) and is filed as a low-priority question,
ITL-1399. If it is wanted, one way: a run table (run id → entry ids per node) written at the
end of a run, read by the loaded job without the sources.

**B2. The chain needs the sources.** The load-only path applies the chain only through
sources the file can rebuild (CSV and Delta paths). In-memory sources, a plain
`DerivedSource`, a `PollingSource` and anything whose path moved leave every node below empty.
With B1 the chain is unnecessary for the saved run; for a new source state it stays
necessary. **Recommendation:** B1 first; then saved paths relative to the job file
(optional) and a single "rebind everything under this folder" helper.

**B3. User classes need their definitions.** A loaded job is meant to be readable without
the code; an exact class column is unreadable without its module, and a structural one is
too: the missing feature ITL-1390. **Recommendation:** implement ITL-1390, and read an exact class column whose class
cannot be found as its storage struct, with a warning, instead of failing every read.

**B4. A job file is not shareable as it stands.** It holds absolute paths, and a connector
database cannot be reopened (ITL-1391). **Recommendation:** database and source locations either relative or overridable in one
call (`PipelineJob.load(path, database=..., sources_root=...)`).

**B5. Saving a loaded job drops what the loader could not rebuild.** **Recommendation:** keep
the saved config of an unbound source and database verbatim and write it back.

**B6. The blueprint cannot become a job** (ITL-1392). **Recommendation:** let
`from_pipeline` accept the pods (a mapping by uri), and build read-only stubs when they are
absent, as `PipelineJob.load` does.

**B7. The loader never imports user code.** That is what makes situations a, b and c
identical, and it is a property worth keeping (a job file from someone else runs none of their
code). It should be a stated rule, and any rebuild that needs a user module (`PollingSource`,
an exact class) should require an explicit opt-in.

## (C) Not tested

- PostgreSQL and SpiralDB databases (no local server); the `where` case ran against the
  test suite's SpiralDB double, not a live service.
- The async orchestrator (`run_async(load_only=True)`).
- `DeltaTableSource`, `DataFrameSource`, `ListSource`, `DBTableSource` sources; a
  `DerivedSource` with `tags_as_data`; data-tags; logical types other than user classes
  (numpy, `Path`, `set[T]`); MergeJoin, Rejoin, MapTags and other operators.
- Function nodes with a `ctx_arg`, post-run hooks, executors, ephemeral results.
- Very large tables, concurrent writers, a database written by another Orcapod version.
- Operator-only blueprints through `from_pipeline`.
