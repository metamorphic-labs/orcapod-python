# Changelog

## [0.2.0] — unreleased

<!-- RELEASE-DAY: replace "unreleased" with the publish date (YYYY-MM-DD), resolve the
     publish target open item below, then delete this comment. See
     superpowers/plans/2026-11-09-v0.2-release-checklist.md. -->

> **OPEN ITEM — Publish target: pending the internal registry. The release is BLOCKED until
> the organization's internal Python package registry is available.** `0.2.0` will be
> published there, not to public PyPI, until public development resumes in January 2027.
> The registry URL, authentication and install command will be filled in once the registry
> exists. Publishing to PyPI / TestPyPI is superseded; the Release workflow still targets
> them, and switching it to the internal registry is not yet possible.

Everything merged to `main` since `v0.1.2`. `0.1.3` and `0.1.4` were patch releases cut
from a separate branch; their one fix is also in this release (see
*Fixed*), so upgrading from `0.1.4` loses nothing.

**Read *Breaking changes and migration* before upgrading an existing pipeline database.**
A pipeline or result database written by any `0.1.x` release cannot be read by `0.2.0`
until it is migrated or explicitly ignored.

### Breaking changes and migration

#### Stored hash columns are now `large_binary`; pipeline and result databases are versioned (ITL-535 #232, ITL-539 #241)

Every orcapod-produced hash column in the pipeline database (pdb) and result database
(rdb) is now stored as `large_binary` (the prefixed digest) instead of `large_string`:
`__node_content_hash`, `__input_data_hash`, `__output_data_hash`, the function variation
hashes `function_signature_hash` / `function_content_hash`, the operator node's
`__node_content_hash`, and the side-effect invocation log's `record_id_hash`.

Storage paths now carry a schema version: pipeline tables live under
`<node identity path>/pdb_v1`, result tables under `<pod uri>/rdb_v1`, and side-effect
tracking tables under a `tdb_v1` suffix. A table written by `0.1.x` (no suffix) is a
"v0" table.

**What you will see.** The first time a function node or result cache touches a v0 table
it raises `orcapod.errors.SchemaVersionError`. Nothing is read or overwritten.

**Migration — choose one per node:**

1. *Migrate in place* (keeps cached results). For Delta Lake databases, from the shell:

   ```bash
   orcapod migrate result-db   <result_db_path> <record_path> [<record_path> ...]
   orcapod migrate pipeline-db <pipeline_db_path> <result_db_path> <node_path> [<node_path> ...]
   ```

   Both accept `--dry-run` (count without writing) and `--json-summary`. For any other
   database backend, or from Python:

   ```python
   from orcapod.migrations import migrate_node, migrate_result_v0_to_v1

   migrate_result_v0_to_v1(result_db, result_path)   # the pod's result table
   migrate_node(function_job_node)                    # the node's pipeline table
   ```

   Migration copies rows to the versioned path and leaves the v0 table untouched; it is
   idempotent, so it can be re-run.

2. *Ignore and recompute.* Opt a node out of the check and let it recompute from scratch:

   ```python
   from orcapod.types import NodeConfig
   node.node_config = node.node_config.merge(NodeConfig(ignore_schema=("v0",)))
   ```

   `merge` keeps the node's other settings; assigning a bare `NodeConfig(...)` would reset
   `is_result_ephemeral` to its default.

**Not covered by the migration tool:** tables written by an *operator* node
(`OperatorJobNode`). Those have no version check and no migration; an existing table
fails with an Arrow schema mismatch on the next write and must be dropped and recreated.
See *Known limitations*.

`ArrowDatabaseProtocol` gains a required `table_exists(path) -> bool` method. Custom
database backends must implement it.

#### `record_id` no longer includes the node content hash (ITL-533 #236)

The preimage of a pipeline record's `record_id` is now the system tag columns plus
`__input_data_hash` only. `__node_content_hash` is no longer part of it (it is still
stored on each row, and still used to isolate nodes that share a table).

**Effect:** every `record_id` and `__pipeline_base_entry_id` value differs from the one
`0.1.x` computed for the same input. `orcapod migrate pipeline-db` /
`migrate_pipeline_v0_to_v1` recompute both, so a migrated database is consistent. Any
`record_id` you stored *outside* orcapod (logs, external tables, filenames) no longer
matches.

#### Function-pod signature hashes changed for orcapod logical types (ITL-638 #262)

A function pod's signature hash used to embed the full import path of an annotated
orcapod type (`orcapod.logical_types.file_type.File`). It now uses the type's stable
registered name (`orcapod.file`), so moving a module inside orcapod can no longer
invalidate caches.

**Effect — one-time cache invalidation:** any function pod whose parameters or return
annotation mention `op.File`, `op.Directory`, `op.Path` or `op.UUID` (directly or inside
`list[...]`, `X | None`, etc.) gets a new signature hash. Its previously cached results
are not found and are recomputed on the next run. Migrating the database does not avoid
this. Functions annotated only with builtins and user-defined types are unaffected — their
hashes are byte-identical (enforced by golden-value tests).

#### `Schema` objects are hashed by `SchemaHandler` (ITL-639 #263)

`Schema` values now reach their dedicated handler instead of being hashed as a plain
mapping. **No hash value changes** — the eight golden schema hashes are byte-identical.
The one behavioural difference: hashing a `Schema` that contains a Python type with no
Arrow translation now raises a descriptive error at hash time instead of producing a
hash for a schema that could never be stored.

#### `op.Directory(ignore=...)` matches relative POSIX paths (ITL-603 #247)

Glob patterns are matched against the path relative to the directory root with
`PurePosixPath.match()`, not against the bare entry name. Bare patterns such as `"*.pyc"`
behave as before. Patterns containing `/` (for example `"build/*.o"`), which previously
never matched anything, now exclude files — which changes the directory's content hash.
A callable passed as `ignore` now receives a relative `PurePosixPath` rather than an
absolute `UPath`; callables that only read `.name` are unaffected.

#### `SideEffectFunctionPod` removed; context injection moved into `FunctionPod` (ITL-532 #231)

Only relevant if you tracked `main` between releases — none of this was in a published
version. The `orcapod.core.side_effect_function` module, `SideEffectFunctionNodeProtocol`,
`SideEffectFunctionInvocation` and `record_side_effect_function_pod_invocation` are gone.
Use `@function_pod(ctx_arg=...)` / `FunctionPod.from_fn(..., ctx_arg_name=...)`.
`@side_effect_function_pod` remains as a thin alias.

#### Operator outputs keep their inputs' Arrow types, normalized to large variants (ITL-813, ITL-832 #278)

`MergeJoin` no longer infers Arrow types from Python values, and it no longer fails on a
zero-row input: a result with a zero-row input has the same column types as a non-empty
one. Every output column of `MergeJoin` and `PolarsFilter` takes the large-normalized form
of its input Arrow type: `string` becomes `large_string`, `binary` becomes `large_binary`
and `list` becomes `large_list`, recursively. Width, nullability and logical types are
never changed. List columns built by `MergeJoin` (merged columns), `Batch` and `GroupBy`
are `large_list` of the normalized element, or, for a logical element such as `Path`, that
element's logical list type (`list[orcapod.path]`). Merged source-info columns are
`large_list<large_string>`.

| Column | 0.1.x | 0.2.0 |
|---|---|---|
| `MergeJoin` merged column, `int8` input | `list<int64>` | `large_list<int8>` |
| `MergeJoin` merged column, `float32` input | `list<double>` | `large_list<float>` |
| `MergeJoin` merged column, `timestamp[ms]` input | `list<timestamp[us]>` | `large_list<timestamp[ms]>` |
| `MergeJoin` merged column, `decimal128(5, 2)` input | precision taken from the data (e.g. `decimal128(3, 2)`) | `large_list<decimal128(5, 2)>` |
| `MergeJoin` merged column, `string` input | `list<string>` | `large_list<large_string>` |
| `MergeJoin` result with a zero-row input | raised `ValueError` | same types as a non-empty result |
| `Batch` / `GroupBy` list column, `int8` input | `list<int8>` | `large_list<int8>` |
| `MergeJoin` / `PolarsFilter` system-tag record ID | `large_binary` | `fixed_size_binary[16]`, as the input has |

**Inputs that disagree:** when the two inputs of `MergeJoin` give a colliding data column
or a shared tag column different Arrow types, both are normalized first. If they are then
the same (for example `string` and `large_string`), the join proceeds. Otherwise (for
example `int8` and `int64`, two timestamp units, or two decimal precisions) it raises
`InputValidationError` naming the column and both types; cast one input explicitly. This
used to produce a type inferred from the values, or a Polars `SchemaError`. The check runs
when the join executes: `output_schema()` reports Python types, so such a disagreement is
not visible when the graph is built.

**Hashes:** large and non-large variants hash identically, so normalization and `list` →
`large_list` change no hash. The change is confined to `MergeJoin` merged columns whose
inputs used to be re-inferred to a different type: integers and floats narrower than
`int64` / `double` (which used to widen), timestamps with a unit other than microseconds
(which became `timestamp[us]`), and decimals (whose precision came from the data). The data
content hash of each row containing such a merged column changes, and so does the stored
record of any operator node or function node that consumes it; a function pod downstream
misses its cache once and recomputes. Tag hashes, stream `content_hash` and `pipeline_hash`
are unchanged.

**Stored schemas:** an existing table written with `list<...>` aggregates (from `Batch`,
`GroupBy` or `MergeJoin`) will receive `large_list<...>` batches.

**Nested nullability:** `normalize_to_large_types` now keeps every nested field's
nullability and metadata. A column such as `list<item: int64 not null>` keeps its non-null
item flag through `Data`, operators and storage instead of losing it. `Data`, `Tag`, stream
and source hashes are unchanged; only the stored Arrow schema of such columns changes.

**Struct Python types:** `struct<b: string>` and `struct<b: large_string>` now map to one
synthesized Python class. Hashes are unchanged.

**Logical list elements:** an aggregating operator (`Batch`, `GroupBy`, `MergeJoin`) over a
column whose extension type has no logical list type in the type converter (for example
PyArrow's canonical `arrow.uuid`) raises a `ValueError` naming the column. It used to fail
with `ArrowNotImplementedError: extension`.

#### `Index` and `Pick` keep their input Arrow types (ITL-834 #279)

`Index` and `Pick` rebuilt every data column from Python values, so columns they only pass
through widened (`int8` → `int64`, `float32` → `double`, `list<int8>` →
`large_list<int64>`). They now keep the input Arrow types of every tag and data column, and
the extracted column takes the element's type (`Index` on `list<int8>` gives `int8`; `Pick`
gives the dict's value type). Outputs are large-normalized as above. An in-place `Index` /
`Pick` keeps the column in its position, and a result in which every row was skipped has
the input's Arrow schema. Extracting a `None` element where the predicted output type is
not optional raises `ValueError` naming the operator, the column and the row's tag.

**Hashes:** rows that pass through `Index` or `Pick` with columns narrower than `int64` /
`double` (or lists and structs of such types) get different data content hashes than on
`0.1.x`, and so does every function pod downstream of them: such pipelines miss their
cache once and recompute. Inputs that are already `int64`, `double` or strings give the
same types and hashes as before. Stream `content_hash` and `pipeline_hash` are unchanged.

The underlying datagram fixes:

- `Datagram.with_columns` and `update` keep the Arrow table whenever it is loaded: existing
  columns keep their exact Arrow fields, and `update` keeps each updated column's field.
  `with_columns` (and `Data.with_columns` / `DatagramProtocol.with_columns`) accepts a
  `pa.DataType` or a `pa.Field` in `column_types`; Python types behave as before.
- Meta (`__`-prefixed) columns keep their Arrow types through every mutator.
- `arrow_schema()` is no longer stale after a mutation of a datagram built from a dict.
  `FunctionPodStream.as_table()` could drop such a row's added tag column silently.
- `Index` and `Pick` use the input stream's data context to type the extracted column.

#### `Join` and `MergeJoin` column order follows argument symmetry (ITL-817 #272)

The order of tag and data columns in a `Join` or `MergeJoin` output now follows the
operator's declared argument symmetry. Both are commutative, so they take their inputs in
canonical `pipeline_hash` order and list columns by first appearance across them:
`Join()(a, b)` and `Join()(b, a)` give identical `keys()`, `output_schema()`, `as_table()`
columns and per-row `Tag` / `Data` keys, in every process. Ordered operators such as
`SemiJoin` keep operand order. Code that reads these columns by position may see a
different order than on `0.1.x`; address columns by name. No hash, system tag, record ID
or stored value changes, because hashing does not depend on column order.

#### Shipped in 0.1.2 but never recorded here

These two breaking changes were released in `0.1.2` without a changelog entry. They are
listed for anyone upgrading from `0.1.1` or earlier.

**`NodeConfig` split into `PodConfig` and `NodeConfig` (ITL-512 #221).**

| Before | After |
|---|---|
| `NodeConfig(max_concurrency=N)` | `PodConfig(max_concurrency=N)` |
| `FunctionPod(pf, node_config=NodeConfig(...))` | `FunctionPod(pf, pod_config=PodConfig(...))` |
| `pod.node_config` | `pod.pod_config` |
| `resolve_concurrency(node_config, pipeline_config)` | `resolve_concurrency(pod_config, pipeline_config)` |
| `NodeConfig.is_result_ephemeral: bool` | `NodeConfig.is_result_ephemeral: bool \| None` |

`is_result_ephemeral` can no longer be set when constructing a `FunctionPod`. Set it on
the node (`node.node_config = node.node_config.merge(NodeConfig(is_result_ephemeral=True))`)
or for a whole job
(`job.apply_node_config(NodeConfig(is_result_ephemeral=True))`).

**SpikeInterface support moved to a separate package (ITL-473 #220).** Core `orcapod` no
longer contains or depends on any SpikeInterface code: `LogicalSIRecording`, the
`BaseSorting`, `Motion` and `SortingAnalyzer` logical types, and the `spikeinterface`
optional dependency are gone. They live in
[`orcapod-extension-spikeinterface`](https://github.com/nauticalab/orcapod-extension-spikeinterface),
which registers itself through the new `op.register_extension()` hook. That package is
not on PyPI at the time of writing; install it from its repository.

### Added

- **`GroupBy` operator** — the first many→one operator. `GroupBy(by=[...])` (also
  `stream.group_by(...)`) collapses rows sharing a tag tuple into one row; group keys stay
  scalar tags and every other column becomes list-valued. Output is deterministic across
  runs and input orderings. (NPIPE-204 #250)
- **`SideEffectPod`** with `track_completion` and `drop_on_failure`, plus the `@sink_pod`,
  `@tap_pod` and `@side_effect_pod` decorators. The user function receives an
  `InvocationContext` on every call. (ITL-525 #228)
- **Context injection for function pods** — `@function_pod(ctx_arg=...)` /
  `FunctionPod.from_fn(..., ctx_arg_name=...)` passes an `InvocationContext` to the
  function. (ITL-532 #231, #234)
- **Post-run hooks** — `FunctionPod.add_post_run_hook()` and
  `@function_pod(post_run_hooks=[...])` fire after every invocation (computed, cache hit,
  or error). New public types: `InvocationStatus`, `RunStats`, `PodContext`,
  `PostRunPayload`, `HookConfig`, `PostRunHook`. (ITL-523 #226)
- `PostRunPayload.invocation_context`; `InvocationContext` and `InvocationHashConfig` are
  exported from `orcapod`. (ITL-531 #237)
- **`EmptyData`** — a payload-free token emitted when an upstream ephemeral result is
  missing, so downstream nodes can still be served from their own cache. New errors
  `EmptyDataAccessError`, `EmptyDataHashMissingError`, `EphemeralResultMissingError`.
  Pipeline rows now persist `__output_data_hash`. (ITL-534 #229)
- **Schema versioning and migration** — `orcapod.migrations` package
  (`migrate_node`, `migrate_pipeline_v0_to_v1`, `migrate_result_v0_to_v1`,
  `MigrationResult`), the `orcapod migrate pipeline-db` / `result-db` CLI commands,
  `SchemaVersionError`, `NodeConfig.ignore_schema`, `ContentHash.from_prefixed_digest()`,
  and `ArrowDatabaseProtocol.table_exists()`. (ITL-535 #232)
- **`ListLogicalType`** — `list[T]` and `set[T]` columns whose element is a logical type
  (`list[uuid.UUID]`, `list[File]`, `list[MyDataclass]`, nested lists) now round-trip
  through Parquet and Delta Lake with the element type preserved. (ITL-173 #251)
- `match_mtime` flag on the file-hash cache backends and
  `enable_file_hash_caching(match_mtime=...)`; with `False`, lookups match on path and
  size only, for filesystems with unreliable mtimes. (ITL-522 #225)
- Hash computation site reference in the documentation (ITL-529 #238) and the
  side-effect pods design spec (ITL-524 #227).

### Changed

- All hash columns stored in pipeline, result and tracking tables are `large_binary`. See
  *Breaking changes and migration*. (ITL-535 #232, ITL-539 #241)
- `record_id` preimage no longer includes `__node_content_hash`. See *Breaking changes
  and migration*. (ITL-533 #236)
- `op.Directory(ignore=...)` matches relative POSIX paths. See *Breaking changes and
  migration*. (ITL-603 #247)
- Release workflow accepts an optional `branch` input (defaults to `main`). (#239)
- CI: the Python 3.11 release test job no longer destroys its own virtualenv; every
  GitHub Action is pinned to a commit SHA and upgraded to its Node 24 major.
  (ITL-618, ITL-619 #254; ITL-621 #256)
- `pygraphviz` removed from the runtime dependencies. It was declared but never
  imported, and it is built from source against the Graphviz C headers, so installing
  `orcapod` failed on systems without `libgraphviz-dev` (or with a Graphviz newer than
  `pygraphviz` supports). Installing `orcapod` no longer needs a C toolchain or Graphviz
  headers. Rendering pipeline graphs still uses the pure-Python `graphviz` package and
  therefore the optional Graphviz `dot` binary at render time. (ITL-134 #267)
- **Python 3.13 is supported.** Canonical type names, and the hashes derived from them,
  are identical on Python 3.11, 3.12 and 3.13, and unchanged on 3.11 and 3.12.
  (ITL-825 #275, merged through #276)
- The repository meets the organization's required CI: `ruff` lint and format,
  `basedpyright` with a baseline, and tests on Python 3.11–3.13. Six tests that had been
  hidden by duplicated test-class names now run. (ITL-826 #276)
- `MergeJoin`, `PolarsFilter`, `Batch` and `GroupBy` keep their inputs' Arrow types,
  normalized to large variants, and `MergeJoin` accepts zero-row inputs. See *Breaking
  changes and migration*. (ITL-813, ITL-832 #278)
- `Index` and `Pick` keep their input Arrow types, and `Datagram.with_columns` accepts Arrow
  types. See *Breaking changes and migration*. (ITL-834 #279)
- `Join` and `MergeJoin` column order follows argument symmetry. See *Breaking changes and
  migration*. (ITL-817 #272)
- Packaging: the license is proprietary (`LicenseRef-Metamorphic-Proprietary`), and the
  `LICENSE` file ships in the wheel and the sdist; `pymongo` and `pyyaml` are removed from
  the runtime dependencies; `basedpyright` and `typing_extensions` move to the `dev` group;
  the sdist contains only the package source, `README.md`, `LICENSE` and the package
  metadata; `orcapod.__version__` is added. (ITL-827 #277)
- Documentation: every Python sample in `README.md`, `docs/index.md`,
  `docs/getting-started.md`, `docs/concepts/` and `docs/reference/` is executed by the test
  suite, and `mkdocs build --strict` runs in the tests. (PLT-4816 #271) A runbook for
  coordinating issue-specific agents is added for contributors (`AGENT_COORDINATION.md`).
  (ITL-818 #266)

### Fixed

- `RayExecutor` no longer fails when it has no remote options: `ray.remote` is applied
  directly instead of being called with an empty option set. This is the fix released as
  `0.1.3` / `0.1.4`. (#233, ITL-544 #235)
- Function-pod signature hashes no longer depend on orcapod's internal module layout.
  (ITL-638 #262)
- `Schema` hashing is routed through `SchemaHandler`; the handler was unreachable and was
  missing from the `v0.1` context spec. (ITL-639 #263)
- Empty Arrow tables keep the declared field nullability. Previously a function that
  failed under `error_policy="continue"` produced an all-nullable empty table, and a
  downstream `Join` aborted the whole pipeline. (ITL-563 #242)
- Extension-typed columns can be read back after Delta Lake compaction, which rewrites
  `large_string` storage as `string`. (ITL-602 #246)
- List-backed logical types (`list[File]`, `set[File]`, …): the Polars round-trip in
  `Join` / `MergeJoin` no longer raises, content hashing hashes file contents rather than
  path strings, and `MergeJoin` keeps the extension type on merged columns. (ITL-627 #257)
- `Data` can represent list-valued source-info provenance; `MergeJoin` inside a pipeline
  job no longer fails with `ArrowTypeError`. `Batch` and `GroupBy` can aggregate
  logical-typed columns such as `Path`. (NPIPE-204 #250)
- `PollingSource`:
  - runs its full asynchronous polling loop inside a `PipelineJob` instead of a single
    synchronous snapshot (ITL-615 #255);
  - no longer leaks a synthetic `_content_hash` column into its data schema, which raised
    `SchemaInconsistencyError` on the third fetch (ITL-616 #259);
  - no longer loses rows when `iter_data()` / `as_table()` are called while the async loop
    is running (ITL-617 #260);
  - no longer crashes on a zero-row batch; the Arrow schema is established once, from the
    declared schema or the first non-empty batch (ENG-952 #261).
- `ArrowTableStream.iter_data()` no longer drops rows or pairs a tag with another row's
  data when the tag and data columns have different chunk layouts (which result-cache
  hits could produce), and rows after the first record batch get their own source info.
  Public API, schemas, hashes and record ids are unchanged. (ITL-816 #270)
- `MergeJoin` no longer raises on a zero-row input. (ITL-813 #278)
- A function node whose output mixed cached and freshly computed rows could fail in the
  orchestrator with `ArrowInvalid: Schema at index N was different`, because cached rows
  came back with a different column order. Buffer materialization now aligns rows by
  column name. (ITL-817 #272)

### Known limitations

- **Operator-node tables have no migration path.** `OperatorJobNode` stores
  `__node_content_hash` as `large_binary` but has neither a schema-version check nor a
  migration utility. An operator-node table written by `0.1.x` fails with an Arrow schema
  mismatch on the next write; drop and recreate it. (ITL-539; `DESIGN_ISSUES.md` ON1)
- **The migration CLI only opens Delta Lake databases.** Other backends must use the
  Python API in `orcapod.migrations`.
- **Migration does not rescue caches invalidated by ITL-638.** Pods annotated with
  orcapod logical types recompute once regardless.
- **Hash-stability regression checks are incomplete.** Golden-value tests cover schema
  hashes and type-annotation hashing; broader coverage is tracked in ITL-626 and may land
  after this release.
- **`dict[str, list[T]]` with a logical-type `T`** (a list nested inside a dict value)
  does not round-trip; two tests are marked expected-failure. (PLT-1732)
- **`set[T]` round-trips only for logical-type elements**, not primitives such as
  `set[int]`. (ITL-611, open)
- **Rendering a pipeline graph needs the Graphviz `dot` executable** on `PATH`. Everything
  else works without Graphviz.
- **Delta Lake on S3 is not covered by automated tests.** The S3 integration tests do not
  run in CI; they skip in every environment because the MinIO image they start no longer
  exists. (ITL-819)
- **Arrow types are not part of `Schema`, and the rule for inputs whose Arrow types disagree
  is not settled for every operator.** `output_schema()` reports Python types, so a
  disagreement such as `int8` vs `int64` or `string` vs `large_string` is not visible when
  the graph is built. `MergeJoin` normalizes both inputs and raises `InputValidationError`
  if they still differ (see *Breaking changes and migration*); the other multi-input
  operators do not apply that rule yet. (ITL-835)
- **Large-variant normalization is not applied everywhere.** `MergeJoin`, `PolarsFilter`,
  `Index`, `Pick`, `Batch` and `GroupBy` emit large variants; `Join`, `SemiJoin`, `MapTags`,
  `MapData`, the column-selection operators and sources keep whatever layout their input
  has (`string` stays `string`). This is layout only and changes no hash. (ITL-841)
- **`MergeJoin` cannot merge non-empty struct columns**: a colliding data column holding
  struct values raises `TypeError`. Zero-row inputs work. (ITL-839)
- **`Data.select` and `Data.drop` leave the removed column's source-info column**
  (`_source_<column>`) behind, visible with `all_info=True`. (ITL-838)
- **Arrow schema and data preservation is not yet audited across every operation**, sync and
  async. Each operator fixed in this release was checked by hand; a test harness covering
  every operator, pod, source, node and execution path is deferred (ITL-846), including its
  nullability part (ITL-842).
- **There is no written rule for which data context an element uses.** Operators use their
  input stream's context for data, but an operator's own context still decides its
  `content_hash` and `pipeline_hash`, and `MergeJoin` uses its left input's context without
  checking the right one. (ITL-843, ITL-844)
- **Column order cannot be chosen.** `Join` and `MergeJoin` list columns in their canonical
  order, which may differ from the order of the call; address columns by name. (ITL-840)
- **A source's default record ID is positional.** Without `record_id_column`, a row's
  record ID comes from its `source_id` and its position. A row that moves to another
  position under the same `source_id` is treated as a new entry: a function node records it
  again (same tag values, a different record ID) and a `sink_pod` delivers it again. A
  function's stored result for that data is reused, not recomputed, and no result is
  paired with the wrong row. When row order can vary between runs, pass a stable
  `source_id` and set `record_id_column` to a column unique per row. (ITL-817,
  `DESIGN_ISSUES.md` RS2)
- **CI tests only the locked dependency versions.** Users get the newest versions the
  `pyproject.toml` ranges allow, which CI does not exercise (ITL-823), and the policy that
  every install uses `uv.lock` is not yet enforced everywhere (ITL-822).
- **The SpiralDB backend is not verified by this release.** Its integration tests have
  been failing on `main` since at least 2026-08-26 with `403 Permission table:view
  denied` from the SpiralDB test project — a credentials problem on the test project, not
  a code change — so the `spiraldb` extra ships untested against a live service.
- **Tested on Python 3.11, 3.12 and 3.13 only.** `requires-python` is `>=3.11` with no
  upper bound; newer interpreters are not exercised in CI.
- **`ray` is pinned to `2.48.0`** in the `ray` extra.
- The SpikeInterface extension is not published on PyPI.

## [0.1.4] — 2026-07-17

Patch release cut from a maintenance branch, not from `main`.

### Fixed

- `RayExecutor`: build the Ray remote function inside the cache-miss branch so a new
  `ray.remote` decorator is not created on every call.

## [0.1.3] — 2026-07-17

Patch release cut from a maintenance branch, not from `main`.

### Fixed

- `RayExecutor`: `ray.remote` cannot be pre-configured with an empty option set; apply it
  directly when there are no remote options.

## [0.1.2] — 2026-07-10

Not itemised at the time. Its two breaking changes — the `NodeConfig` → `PodConfig` /
`NodeConfig` split (ITL-512) and the move of SpikeInterface support to a separate package
(ITL-473) — are described under *Breaking changes and migration* in `0.2.0`. For the full
list of changes see the
[v0.1.1...v0.1.2 comparison](https://github.com/metamorphic-labs/orcapod-python/compare/v0.1.1...v0.1.2).

## [0.1.1] — 2026-06-29

### New

#### `orcapod.File` — content-identified file type (ITL-450)

`orcapod.File` is a new first-class type for materialized files. It subclasses
`upath.extensions.ProxyUPath`, giving it full UPath-like behaviour across all
backends (local, S3, GCS, etc.) without polluting the global protocol registry.

**Construction validates eagerly:** `File(path)` raises `FileNotFoundError` if the path
does not exist, `IsADirectoryError` if it is a directory, and `ValueError` if it is not
a regular file. This means a `File` instance always refers to a file that existed at
construction time.

**Symlink handling:** By default (`follow_symlinks=True`) symlinks are followed. Pass
`follow_symlinks=False` to raise `ValueError` instead of following a symlink.

**Content hashing:** `orcapod.File` is the only type whose hash is derived from the
file's *content*. `pathlib.Path` and `upath.UPath` now hash from the path string only
(see Breaking Changes below).

**Arrow extension type:** `LogicalFile` stores `File` values as `large_string` (the path
string) with extension name `"orcapod.file"`. Re-reading a stored `File` column
re-validates existence, so missing files surface at read time.

```python
from orcapod import File

f = File("/data/results.csv")        # validates existence
f2 = File("s3://my-bucket/out.parquet")  # works with any UPath backend
```

**Exports:**
- `orcapod.File` — the `File` class (top-level package)
- `orcapod.extension_types.LogicalFile` — the Arrow extension type
- `orcapod.hashing.semantic_hashing.FileHandler` — the semantic hasher

### Changed

#### starfix v0.3.0 adoption + schema-metadata cleaner (PLT-1737)

Bumped `starfix` dependency to `~=0.3.0` and introduced a schema-metadata
cleaning step before all Arrow schema and table hashes.

**What changed:** `StarfixArrowHasher.hash_schema` and `hash_table` now strip
every metadata key that does not start with `ARROW:extension:` before passing
the schema to starfix. Only identity-bearing extension metadata (e.g.
`ARROW:extension:name`) is included in the hash. Unrelated keys (comments,
vendor annotations, source-file provenance, etc.) are ignored.

**Stability invariant:** Schemas with no `ARROW:extension:*` metadata anywhere
continue to produce byte-for-byte identical hashes to pre-v0.3.0 Orcapod.

**One-time hash invalidation:** Pipelines whose schemas contained
`ARROW:extension:*` keys *alongside* unrelated field metadata (e.g. a
`comment` key on the same field as `ARROW:extension:name`) will see a changed
hash. On-disk caches for those pipelines should be treated as stale and
recomputed.

**New utility:** `orcapod.hashing.schema_cleaner.clean_schema_for_hashing` and
`has_extension_metadata` are available as semi-public utilities for other
Orcapod code that needs to inspect or clean Arrow schema metadata.

### Breaking Changes

#### `pathlib.Path` and `upath.UPath` no longer hash file content (ITL-450)

`pathlib.Path` and `upath.UPath` values now hash from the **path string** only —
no file is read. This matches the semantics of all other string-like types in orcapod.

**Migration:** Replace any `Path`-typed pipeline column that requires content-based
identity with `orcapod.File`. `File` validates existence at construction and hashes
from file content, exactly as `Path` did before this change.

```python
# Before (Path hashed file content — now hashes path string instead)
{"input": Path("/data/results.csv")}

# After (File hashes file content, as Path previously did)
from orcapod import File
{"input": File("/data/results.csv")}
```

**Hash invalidation:** Any on-disk cache entry produced with a `Path`-typed data column
will no longer match. Treat those entries as stale and allow recomputation.

**Removed:** `PathHandler` and `UPathHandler` have been removed from
`orcapod.hashing.semantic_hashing`. Use `FileHandler` for content-based hashing.

#### `packets` → `data` rename (hard break)

All identifiers containing `packet`/`packets`/`Packet` have been renamed to
`data`/`Data`. No deprecation aliases. Pre-v0.1 artifacts will not load.

| Old name | New name |
|---|---|
| `Packet` | `Data` |
| `PacketProtocol` | `DataProtocol` |
| `PacketFunction` | `DataFunction` |
| `PacketFunctionBase` | `DataFunctionBase` |
| `PacketFunctionProtocol` | `DataFunctionProtocol` |
| `PacketFunctionProxy` | `DataFunctionProxy` |
| `PythonPacketFunction` | `PythonDataFunction` |
| `CachedPacketFunction` | `CachedDataFunction` |
| `PacketFunctionWrapper` | `DataFunctionWrapper` |
| `PacketFunctionExecutorProtocol` | `DataFunctionExecutorProtocol` |
| `PacketExecutionLoggerProtocol` | `DataExecutionLoggerProtocol` |
| `PacketLogger` | `DataLogger` |
| `SelectPacketColumns` | `SelectDataColumns` |
| `DropPacketColumns` | `DropDataColumns` |
| `MapPackets` | `MapData` |
| `iter_packets()` | `iter_data()` |
| `process_packet()` | `process_data()` |
| `async_process_packet()` | `async_process_data()` |
| `execute_packet()` | `execute_data()` |
| `map_packets()` | `map_data()` |
| `select_packet_columns()` | `select_data_columns()` |
| `drop_packet_columns()` | `drop_data_columns()` |
| `on_packet_start()` | `on_data_start()` |
| `on_packet_end()` | `on_data_end()` |
| `on_packet_crash()` | `on_data_crash()` |
| `INPUT_PACKET_HASH_COL` | `INPUT_DATA_HASH_COL` |
| `PACKET_RECORD_ID` | `DATA_RECORD_ID` |
