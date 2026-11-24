# ITL-1186 — Move SpiralDB support to spiral-sdk

**Draft for decision.** Replace the Python client with `spiral-sdk==0.16.74`.
Use its public `KeyedTable` API to read existing tables in place. Use the
branch-and-commit `Table` API for new pipeline stores, after proving its
write, schema and publication contracts. Make the resource kind explicit for
new configurations. Keep the existing keyed source configuration readable
with its original identity. The client decision is made. This draft proposes
where to use each table kind and records the evidence that must precede the
switch.

## Evidence and scope

- Orcapod baseline `7a2248bfb3867a3ccb6dd55e21cb0e2a90769a9f`.
  Below, **C** is [the connector](../../../src/orcapod/databases/spiraldb_connector.py),
  **S** is [the source](../../../src/orcapod/core/sources/spiraldb_table_source.py),
  **N** is [the nullability codec](../../../src/orcapod/databases/spiraldb_nullability.py),
  and **F** is [the existing fake](../../../tests/test_databases/fake_pyspiral.py).
  Numbers refer to the baseline. ITL-1186 supplies the migration requirements.
  ITL-931 describes the older private, paged reader. The pin now exposes
  public keyed lookup, streaming scans, and writers. The new public reader
  takes an optional limit with no Python-side 10,000-row limit or offset loop.
  Native and hosted limits still need measurement.
- SDK line numbers refer to files under `spiral/` in the published macOS
  arm64 wheel. The wheel's SHA-256 is
  `3f624ce767c074fbc04e0d2c96db6b4587fae2f8b653f1b15a8c9d35bc524586`.
  Read the shipped Python wrappers, the `_lib.pyi` native boundary, generated
  service clients, entry points and license files. A wrapper delegating to
  native code establishes the call shape, not its remote cost, type fidelity or
  exception mapping. The public docs track the service independently of the
  wheel. The pin governs API availability. [Pinned distribution](https://pypi.org/project/spiral-sdk/0.16.74/),
  [Python SDK](https://docs.spiraldb.com/docs/sdks/python).
- Live evidence below is work for a subsequent opt-in run. No service call,
  sign-in or SDK runtime test was made for this study. The SDK/server support
  statement should initially name only this pin and the hosted environment
  where the conformance suite passes for each table kind. The old keyed WAL
  and new catalog/data plane are separate capabilities. Ask Spiral for the
  support window for the keyed service and the approved dependencies for the
  metadata and lifecycle gaps below. No self-hosted or remote ScanDAL support
  until verified. External consumers follow separately.

## Calls and behavior

In the matrix, a lookup, metadata operation, scan plan, reader and publication
are separate logical operations. The Python code alone does not give HTTP
request counts. An opened SDK handle pins an immutable version. Reuse one
handle for schema, keys, scan and identity. Resolve again to observe a new head.

| Orcapod today | SDK `KeyedTable` | SDK keyless `Table` | Result, failure, work and concurrency |
| --- | --- | --- | --- |
| `sp.Spiral(overrides=...)`, `.project(project_id)`, shallow `share_client()`, `close()` drops references (C 553–574, 1232–1244) | `spiral.Client(...)` and `client.project(id)`; keyed lookup takes project, dataset and name separately. | Same client/project; branch selection is a separate operation. | `project()` is local. `Client` defaults to env, saved login, then device sign-in. It documents keyword endpoints, token and cache rather than a free-form override dict. Translate known nonsecret settings, reject unknown ones explicitly, inject auth from the environment, and keep each wrapper independently closable. No public close in the wrapper/stub. Verify native client sharing and close/cancellation under concurrent scans. `client.py:81–137,174–208,311–339`, `_lib.pyi:64–99`. |
| `project.list_tables()` then dataset filter and sort unqualified names (C 584–596) | No Python list operation in the pin. Documented Keyed HTTP list pages with exact dataset filter, stable IDs and opaque continuation tokens. An adapter needs a supported authenticated transport or a vendor Python binding. `client.py:152–155` wires credential renewal into generated clients via native access. Do not depend on private token or transport properties for the HTTP bridge. | Resolve branch once, then `client.catalog.list_namespace_items(project_id, head.commit_id, request)`; filter resource kind and namespace, return sorted plain names. | Keyed usage makes one logical request per page, including empty pages with a token. New listing reads a commit-consistent namespace. Confirm missing/denied semantics and reuse a single origin/base across pages. Avoid listing before every lookup/write. `services/__init__.py:43–81`, `services/tables/raw.py` (`SyncTablesClient`, lines 80–230), `services/catalog/raw.py:532–594`. [Keyed list](https://docs.spiraldb.com/docs/services/tables/api/keyed-tables/list-tables). |
| `project.table(dataset.table)`; preflight invalid names, match one precise old `ValueError` as `SpiralDBTableNotFoundError` (C 598–627); `get_column_info` and writes also call `project.table` directly | `client.keyed_table(project,dataset,name,asof=...)`; `keyed_table_by_id(id,asof=...)` for a previously verified ID. | `client.table(project,address,ref=...)` or `head.table(path)`; `table_by_id(resource_id,version_id)` for exact version. | One name resolution per operation. Separate namespaces and explicit dispatch avoid masking permission/transport failures with a fallback. Translate only the verified missing-table diagnostic. Native stubs expose `SpiralError` with publication subclasses and no dedicated missing-table subclass. Probe denied/missing, invalid address/version and network error separately. `client.py:174–210`, `projects.py:129–148`, `_lib.pyi:19–30,84–89`. |
| `.table_id`, `.key_schema.names`, `.last_modified_at()` (C 477–504, 629–638, 865–889) | `.resource_id`, `.key_columns`, `int(.version_id)` (decimal Unix microseconds). | `.resource_id`, empty physical key list, opaque `.version_id`; `BranchHead.commit_id` selects project state, not table version. | SDK handle properties expose pinned values. Compare keyed timestamps only after confirming their meaning against pyspiral. Keyless refresh compares resource identity and version change on the followed branch, never numeric or lexical version ordering. Define behavior for branch moves/rewinds. Configured application keys stay distinct from server constraints. `keyed_tables.py:80–106`, `tables.py:359–383`, `projects.py:129–148,207–241`. |
| `.schema().to_arrow()`, `.get_metadata()`, then restore metadata and nullability and normalize layouts (C 486–499, 672–687, 752–812, 849–863) | `.schema()` yields Arrow after planning, with no rows. No public keyed metadata getter in wrapper/stub. Historic Orcapod records require a vendor-supported bridge or explicitly exported metadata keyed by original table ID. | `.schema()` is logical Arrow, with extension metadata where host supports table descriptions. Check against the scan schema. | Metadata calls may plan/read manifests. Surface host errors and malformed records. No silent fallback to changed annotations, invalid nulls, or key/order changes. Schema must belong to the same pinned version as rows. Existing metadata was independently mutable, so a keyed export represents its capture time, not time travel. `keyed_tables.py:163–169`, `_lib.pyi:289–319`, `tables.py:519–528,730–739`. |
| `table[column] == value`, `&`, `table.select()`, `spiral.scan(...,where=...,limit=...,asof=...)`, `.to_record_batches()` (C 506–526, 690–750, 814–847) | `table.scan(filter=expr,limit=n,batch_rows=...,read_ahead_bytes=...).to_reader()`; select time when opening handle. Preserve primary-key scan order. Optional `KeyRange` with typed bounds for proven prefix lookups. | Same reader with keyless scan options. Stored-order limit. Explicit projection with `columns` or `projection`; `top_k` is a separate bounded sorted operation and cannot be combined with limit. | Build `eq(column(name),literal(value,physical_type))` and combine with `and_`. Python inference alone changes integer/temporal types and `==` on expressions is not the filter API. Check null, NaN, signed zero, narrow ints, temporal precision, normalization of literal types and limit-after-filter. Key ranges retain overlaps, so do not use overlapping ranges for source scans. Planning can reject predicates. Reader errors can occur after batches have begun. Keep SQL-shaped `iter_batches` as the connector's simple full read and its ignored params/batch-size warnings unless the public contract changes. SDK batch/read-ahead controls do not replace Orcapod slicing or constitute a hard memory cap. `expressions.py:54–116,170–201,357–385`, `keyed_tables.py:18–38,133–169`, `tables.py:459–517,724–787`. |
| `table.core.get_snapshot(asof=...)`, `spiral.core.manifest_stats(...).key_space_stats.total_rows` (C 528–534, 891–905) | No public manifest count in pinned `KeyedTable`. The documented snapshot response has opaque data. Do not parse its internals as a count contract. | `scan().splits()` provides planning descriptors, not an established visible-row count. `Scan.count_rows(items)` is per work item. Aggregate count may execute a scan. | Need a proven conservative metadata upper bound at the same pin for the preflight cap. Validate overwrites, deletes, uninitialized/empty tables, and compaction. For bounded reads use the caller's effective limit. For an otherwise unbounded capped read, recommend a supported bound or a safe refusal. A streaming cap can raise after earlier rows already reached the job, so it cannot replace this preflight contract. A bounded pre-read would change latency and memory and needs a decision. Never substitute an unnoticed whole-table count. `_lib.pyi:289–319`, `tables.py:52–71,755–768,789–839`. [Keyed snapshot](https://docs.spiraldb.com/docs/services/tables/api/keyed-tables/read-snapshot). |
| `project.create_table(dataset.table,key_schema=[(pk,type)],exist_ok=True)` (C 918–956) | `project.create_keyed_table(dataset,name,pa.schema([...]),exist_ok=True)` returns a pinned handle. | Build a typed empty version using `project.table_writer(schema).finish()` or `write_table(schema.empty_table())` after live validation. Stage `head.commit().create_table(name,table)`, publish. | Keyed create checks same key schema per SDK contract. Test mismatched keys, duplicate names, creation races, permissions, and zero-row initialization. Keyless creates a version then binds a name at commit; an abandoned allocation does not name a visible table. Race losers reopen and check both resource kind and schema. Confirm name grammar and cleanup of abandoned versions with vendor. `projects.py:50–61,113–126,244–276,322–377`, `tables.py:855–917`. |
| `tbl.write(rows)` keyed upsert; validates `id_column` and all composite key names, schema and value refusals before write (C 958–1140, 1161–1229) | `table.append(rows)` upserts by entire primary key and returns a new snapshot. `writer()`, `.append()`, `.finish()` stage one WAL transaction; `.abort()` discards local staging. | `table.merge_rows(rows,on=keys).table`, stage `commit.update_table(next_table)` then publish. Plain `append` adds rows; it does not upsert. | Retain Orcapod's usable keys, non-null and duplicate validation and prove same-key replacement including partial columns. Keyless merge defaults to patch/insert/keep, holds source in memory, rejects duplicate incoming keys, nulls match nothing, and returns counts plus next version. Reject null keys early. There is no declared uniqueness constraint, so require a controlled writer and verify target uniqueness before adopting a table. Measure target work on growing history and probe two overlapping publications. A keyed WAL transaction is per table and has ambiguous outcome on lost response. Native same-key races and WAL conflicts need evidence. `keyed_tables.py:171–209`, `tables.py:544–558,602–631`, `projects.py:265–271,322–377`. [Keyed WAL publication](https://docs.spiraldb.com/docs/services/tables/api/keyed-tables/publish-wal). |
| `skip_existing=True`: `spiral.scan(tbl.select()).to_table()`, make a Python composite-key set, filter, then write only novel rows (C 1141–1153) | Candidate is projected key presence via bounded `KeyRange` or typed predicates, then append. Native writer has no skip-existing option. | `merge_rows(...,matched="ignore",not_matched="insert")` using the selected base version, followed by publish. | Measure target pruning. For Keyed, read then append has a race even with a bounded probe. Require exclusive writer ownership or document and gate the unsupported concurrency mode. A keyless publish conflict must re-resolve and recompute the merge against the new base. An unknown publication outcome instead needs recovery of the same operation. Preserve no-write behavior for all hits. Never retry a stale candidate as an append. `keyed_tables.py:133–159,171–209`, `tables.py:602–631`, `projects.py:322–377`. |
| `get_metadata`, `set_metadata`, `drop_metadata`; N writes its chunks/last marker and C stages Arrow metadata before rows, rolls it back on failure (C 1038–1133, N 179–299) | No methods in this pin. Public snapshot schema is opaque. An approved bridge must read existing metadata and provide explicit safe write semantics if retaining direct keyed writes with metadata. | Put supported Arrow metadata and field flags in the versioned schema. If extra Orcapod metadata is required, a separately versioned manifest Table can be published in the same commit, subject to live evidence. | Old KV and rows are separate writes, so failed/racing writes can expose inconsistent metadata. New keyed WAL only stages row/column changes. Keyless commit binds versions atomically on one branch and project. Retain existing record validation and refusal behavior. Do not set `keeps_field_metadata=True` until data tags, schema bytes and nested layouts are proven. Test altered metadata, invalid records, failed writes and no half-published rows. `_lib.pyi:289–319`, `tables.py:519–524,633–642`, `projects.py:244–271,322–377`. |
| `project.drop_table(dataset.table)` after listing, missing is a no-op (C 907–916) | No exposed Python drop. Listed public keyed HTTP operations in this study have no drop. Vendor-supported deletion by verified ID is an explicit gate for connector parity and automated fixture cleanup. | Resolve the exact resource by path at head, `commit.drop_table(table)`, publish. | Ignore only confirmed absence. Check that a concurrent replacement is never retired by an old lookup, and determine what readers of old versions can still open after retirement. Do not use rename/destroy/recreate as an idempotency strategy. `_lib.pyi:289–341`, `projects.py:294–300,322–377`. [Tables API](https://docs.spiraldb.com/docs/services/tables/api). |

The wheel exposes `Matched = Literal["patch", "delete", "ignore"]` in
`tables.py:30–38`, so the insert-only merge is available to test without a pre-scan
in application code. The service's commit conflict behavior needs a specific
probe. The `BranchHead` and `moved()` docstrings describe head movement
differently (`projects.py:207–235`). Treat `BranchMoved` and `PublishConflict`
as definitive rejection and recompute on a newly resolved head. Persist the
`FrozenCommit.save()` payload securely where durable recovery is required.
On `PublishOutcomeUnknown`, resume/recover or replay the same frozen commit.
Do not serialize credentials or that recovery object into job configuration.
[Commit model](https://docs.spiraldb.com/docs/platform/concepts/commits-and-branches),
[retry contract](https://docs.spiraldb.com/docs/platform/http-api/retries).

## Keep existing keyed sources readable

1. Keep `source_type="spiraldb_table"`, existing project/dataset/table
   selection, and natural-name format
   `spiraldb_table_v1:<project>.<dataset>.<table>@<table id>`. Read the keyed
   table in place with `Client.keyed_table`. Require a byte-for-byte match
   of the SDK `resource_id` with the pyspiral table ID on a signed-off
   synthetic fixture. If IDs differ, get a vendor-backed old-ID mapping and
   explicitly validate both locators before choosing the original ID for
   identity. Never derive an old ID from a new Table or the bare name.
   Keep explicit source IDs/labels through the same configuration rules.
   When a name resolves to a different resource, preserve refusal on
   reads, refresh, and saved-job reconstruction. S 156–190, 363–407,
   665–681, 726–736, 1104–1182.
2. Keep key order, or the ordered user-supplied `record_id_columns`, plus
   default tags, tag-only fields, `tags_as_data`, system tags, and schema.
   Use the existing Arrow key encoding, including integer-width-aware
   validation and lossless temporal extraction. Record IDs are the canonical
   row identifier text attached by the stream builder. Provenance also
   includes the source ID and column. Keep identical names, widths, Arrow
   nullability, field/schema metadata and normalizations before hashing.
   `where`, `limit` and `asof` still select rows of that identity. Test
   per-key record IDs, source-info tokens, data/context hashes, source and
   pipeline hashes, and unchanged recomputation on a saved job. S 389–453,
   657–681, 801–816, 876–919.
   [Canonical key encoding and provenance](../../../src/orcapod/core/sources/stream_builder.py).
3. Open at the correct snapshot. A historical `asof` is a Unix-microsecond
   integer and remains fixed. An implicit pin refreshes only after a newer
   version with the same identity and schema. Preserve implicit latest on
   reload and explicit historical pin on reload. Compare old `last_modified_at`
   with new `version_id` on empty, historical, overwritten and compacted
   resources. Test schema drift across historical versions. The current
   constructor can get current schema before scanning historical rows
   (S 375–390, 888–895). Use the selected version for both, retaining an
   explicit refusal if the historical ABI cannot be recovered. Gate on
   retained history.
4. Before cutover, export the existing Orcapod KV through a separately
   authorized legacy process if vendor access through the new client is
   unavailable. Store an authenticated, size-bounded metadata record
   associated with the original project, table ID, schema fingerprint and
   capture point in an approved resolver. Preserve the intact N chunks and
   marker and the Arrow metadata tree. Verify the capture against real
   schema/batches and refuse malformed or unverifiable metadata. Prefer a
   vendor-supported read path that can also distinguish an unrecorded table
   from a recorded table. A schema guessed from values or an empty KV
   substituted on error can silently move hashes. Existing tables with
   metadata stay a cutover blocker until an approved read path is proven.
5. Give a new keyless source an explicit resource kind and path/ref config,
   an approved identity distinct from the keyed locator, and required
   `record_id_columns`. Physical `ROW_ID_COLUMN` is a stable SDK
   `uint64` across rewrites but is not the Orcapod logical key. Stored
   order is not primary-key order. Document the different limit semantics
   before exposing this source. Keyless creation or conversion must never
   silently take over an existing keyed source's saved locator.
   `tables.py:459–495`.

For async reads keep native calls off the event loop. S 921–944 advances one
generator using `asyncio.to_thread`, potentially on different threads over
its life. Until cross-thread reader use is verified, own each reader in one
worker with bounded handoff, explicit draining/close on cancellation, and one
reader per consumer. Test independent closable source copies (S 576–603,
687–719), simultaneous source and refresh, and cancellation while a native
read is in flight. The pipeline can still materialize all delivered rows.
The cap/preview behavior in S 835–919, 957–1043 stays a separate test from
SDK read-ahead.

## Other Orcapod entry points and saved artifacts

The inventory starts with `git grep -ilE 'pyspiral|spiraldb|\bspiral\b'` on the
baseline, then inspect executable imports, transitive callers and test
fixtures. The following covers active integrations. Historical changelog,
design notes and recorded measurements can continue to name the client
they used. Any retained executable probe must run with the new dependency
or be removed from the runnable probe set.

| Entry point or surface | Migration and evidence | Saved artifact |
| --- | --- | --- |
| Public exports `orcapod.sources`, `orcapod.databases`, root import path and optional-import guard (C 50–59 and package `__init__.py` files) | Keep imports usable without the extra. Resolve the pinned SDK at integration use, with a clear install error for an absent package and an actionable wrong-distribution error for old `spiral`. Use distribution ownership/version plus expected symbols, since both distributions import under the same name. Test subprocesses with neither, only SDK, only old package and shadowing/broken native import. Preserve unrelated errors and avoid import-time client creation. Consult `tests/test_utils/test_lazy_module_use.py` before introducing another lazy proxy. | Import names and documented constructors stay available. |
| Source build, sync and async streams, refresh and preview (S 298–453, 739–1056) | Resolve existing Keyed source with the pinned SDK. Port lookup-count, schema-only, preview/cap, filtered reads, empty typed results, cancellation and drop/recreate. Compare deterministic sync rows exactly and async rows as sets, with schema metadata, IDs and hashes on both paths. Test a new version computing only new/changed rows in a job. | Keep `spiraldb_table` and explicit/natural identities as above. |
| `PollingSource`, `DerivedSource`, `CachedSource`, restrictions and operator caching | The Spiral source's dynamic behavior is `check_for_new_data`. `PollingSource` is a separate user-fetch abstraction with no built-in Spiral binding and no entry in the baseline source registry. A custom fetcher that uses this connector must be tested through its normal cursor and resume flow. Preserve check-on-run and pin isolation. Keep restricted sources' cache rules and single-condition validation. Port `test_source_check_for_new_data.py`, `test_filter_pipeline.py`, `test_order_invariance.py`, `test_explain.py`, tags-as-data, tag-only, and alignment tests. Exercise wrapped source restoration, unchanged rerun, new rows, restarts and refusal for invalid restrictions. | Old jobs with supported wrapped keyed sources must rebuild a readable source and retain matching IDs and hashes. Preserve existing limitations for saving and restoring user-defined polling fetchers. A proxy is useful for inspection but does not pass executable parity. |
| Source registry and `PipelineJob.load` | `pipeline/serialization.py:244–275,530–588` dispatches the saved source kind and may fall back to `SourceProxy`. Capture a pre-switch job file and load under an SDK-only interpreter through the real entry point. Assert concrete source, exact key/order/metadata and same graph, source/pipeline hashes and cache hits. Test unpinned, pinned, explicit ID, nested wrapper, unavailable table, wrong ID and missing extra. | Keep these saved jobs readable without a legacy package. |
| Connector config, store registry, catalog | C 1246–1285 round-trips `connector_type="spiraldb"`; keep meaning of old dataset config, map only approved nonsecret settings and add explicit kind/path/branch fields for new keyless configurations. `ConnectorArrowDatabase.to_config` and `from_config` are distinct from direct connector serialization (`connector_arrow_database.py:923–944`). The emitted connector-database config currently cannot be restored. The job database registry registers Delta, in-memory and noop only (`serialization.py:227–241,409–450`). Test connector config separately from saved source and handle the pipeline store's registration under ITL-1391/ITL-1446. SDK Catalog governs keyless namespace. A legacy dataset list stays a keyed HTTP operation. The proposed Orcapod store catalog in PR 542 is another layer, with its own design decision. | Direct keyed connector config retains keyed semantics. There is no supported saved SpiralDB pipeline store schema on this baseline. A new store gets an explicit format and round-trip tests before exposure. |
| `ConnectorArrowDatabase` reads, staging/flush, capabilities | It calls DBConnectorProtocol to list/create/scan/upsert/delete, read schema and validate metadata preservation. Keep shared queues, record alignment and retry/refusal ordering, then run `test_append_by_name.py`, `test_appended_field_attributes.py`, `test_logical_identity_appends.py`, `test_unstorable_columns.py`, data tags and type matrix. Inspect keyed and keyless schema differences independently. Keep existing refusals until the type is proved exact. | Stored tables carry logical column and context information. Any changed representation requires an explicit migration decision. |
| Data function code and packaging | The Python function type manifest and function config registry have no direct Spiral SDK imports. A function which consumes a Spiral source gets Arrow-derived inputs. Preserve callable identity, serialized descriptors, and the library wheel's dependency boundary. Test a real installed-wheel save/load (functions importable by name), function call counts, result/cache/row hashes, and fresh worker imports for async/distributed jobs. User functions importing pyspiral APIs directly must switch their own code and environment. | Existing Orcapod function descriptors keep their format. A legacy client's serialized runtime object or import is outside the descriptor contract. |
| Current fake and consumers | Replace `fake_pyspiral.py` and migrated monkeypatch call sites. Inventory includes source build/lookups/streaming/`where`/tags-as-data/large normalization/refresh and tag-only tests, core alignment/capability/filter/explain/order-invariance, connector fake/nullability/logical-identity/unstorable tests, type matrix, and `test_run_warnings_caller_globals.py`. Port raw setup to a fixture that seeds snapshots through the appropriate writer or a read-only fixture import. Keep the shape of the example Orcapod pipelines. | No fake disk layout is public. Synthetic serialized job and hash fixtures should remain readable and stable. |
| Opt-in integration tests and runner | `test_spiraldb_connector_integration.py` also has raw legacy client use through connector internals. `test_spiraldb_table_source_live.py` directly instantiates legacy clients for its write cases and is outside today's CI command. Replace both with named synthetic fixture setup and include shared source/connector conformance in the opt-in job. Exercise offline integration skips as well as fail-fast misconfiguration on the authorized live runner. | Reuse an independently seeded keyed resource to prove old-table reads. Create mutable fixtures only in the isolated test scope. |
| Published docs and examples | Update README extras and refusal notes, `docs/concepts/sources.md` install/login, examples, count, limit, identity, refresh and nullability; `pipeline-job.md`, `streams.md`, `notebooks.md`, `operators.md`, `data-tags.md` and `docs/tutorial/00-setup.md`; regenerate `docs/reference/database-types.md` from the tested matrix, identifying both resource kinds, versions and evidence. Run `tests/test_docs/test_doc_samples.py` and docs site checks. Hosted examples need their existing infrastructure skips. Checked-in `examples/` have no direct legacy client calls. | Examples which write/read saved jobs must show the same source kind for keyed tables. |
| Tooling and executable research | Port or retire direct imports in the ITL-926 probes (`inspect_tables.py`, `live_verify.py`, `scan_cost_probe.py`, `size_probe.py`) and ITL-936 `live_nullability.py`. Review other Spiral dev scripts around ITL-186/939/1371 and historical auth notes in `CLAUDE.md`/`.zed/rules` and `RELEASING.md`. No dedicated tracked Spiral skill or dependency-license allow-list entry was found. Review type baseline entries when changing their originating code, following repository rules. | Historical reports remain tied to their measured client. |
| Extra, lock, builds and license jobs | Keep the `spiraldb` and aggregate `all` extras, change the dependency in the implementation PR and regenerate `uv.lock`. Lock check and build wheel/sdist, verify all-extras resolution and optional installation, dependency review and separate SDK license reporting. | Existing installs can move with a clean environment using the same extra name. |

For the cutover, enforce both distribution and API cleanup. Check
`pyproject.toml`, the resolved lock, built wheel metadata and clean
installation with `importlib.metadata.packages_distributions()`.
Search all active source, tests, workflow, scripts, examples, and executable
doc probes for `pyspiral`, `spiral.Spiral`, `sp.Spiral`, old raw
`.core`/KV calls and old authentication variables. Review matches for
historical evidence, the wrong-distribution check or the explicit conversion
of saved nonsecret options. `import spiral` itself stays. Tests run without
either client installed by injecting the fake at Orcapod's client boundary.

## Fake and conformance

Build an Arrow-backed, disk-scoped fake for only the operations above. Reuse
the fixture isolation and call-observation idea from F 237–356, 800–881.
Keep mutable catalog state distinct from immutable snapshots. Exercise the
same Orcapod adapter and types with both providers.

| Fake rule | Evidence and check |
| --- | --- |
| Keyed name resolves to ID, ordered key columns and pinned timestamp. By-ID can reopen a retained version. Dropping/recreating in the test catalog changes ID. Version commits don't edit existing handles. | SDK `client.py:190–204`, `keyed_tables.py:41–106`; S 726–736. Verify numeric semantics and ID parity against separately captured legacy fixture. |
| Keyed filter, projection, limit and streaming. A default range is in primary-key order. Explicit ranges retain overlaps and input order. Aligned ranges can yield an empty batch. | `keyed_tables.py:18–38,133–159`, `tables.py:770–787`. Verify typed expressions and limit before consuming a large result. Keyed batch size and transport yield patterns require live evidence before the fake asserts them. |
| Keyed writer stages changes in a private pending transaction. Finish upserts full keys and yields a new snapshot. Abort exposes no mutations. | `keyed_tables.py:171–209`, [WAL contract](https://docs.spiraldb.com/docs/services/tables/api/keyed-tables/publish-wal). Do not assume cross-table transaction, retry safety, accepted partial rows or conflict precedence. |
| Keyless namespace snapshots bind path, kind, ID and version at a commit. Ordered opaque pagination; recreate allocates new ID. Staged versions are separate from visibility. | `client.py:180–210`, `projects.py:129–148,244–276,294–300`, `services/catalog/raw.py:532–594`. Conformance holds old and fresh readers either side of publication. |
| Keyless append preserves duplicates, merge uses columns supplied by the caller, duplicate incoming keys fail, nulls match nothing, unchanged merge returns the base. Keep insert/update/delete counts. Reader provides stored order and a typed empty schema. | `tables.py:459–495,544–558,602–631,724–787`. Orcapod refuses invalid logical keys before SDK writes. Prove native target duplicate behavior and equality separately. |
| Keyless publication is atomic at the branch. Support deterministic conflict and ambiguous-outcome injection, durable operation identity, recover and replay same operation. | `projects.py:244–249,322–377`, `_lib.pyi:19–28`. Model overlapping changes as rejected until exact server behavior is established. The fake controls injections. Hosted runs assert externally observable state under real races. |
| Arrow schema and batches preserve the fields the adapter promises. Compatibility fixtures can include recorded, unrecorded, malformed and mismatched metadata. | `keyed_tables.py:163–169`, `tables.py:519–528`, C 752–812, N 179–299, 340–431. Encode only type conversions or refusals verified for this resource kind/pin. `StorageBehaviour` switches and PR 544 evidence describe the old writer and are probes for the new one. Test a verified count upper bound only if the service supplies one. |
| Auth, transport errors, blocking and lifecycle can be injected at boundaries. Distinguish missing, denied, invalid input and unknown outcomes. | `client.py:103–137`, `_lib.pyi:19–30`, `services/catalog/raw.py:532–556`. Fake clocks, faults and counters support Orcapod invariants. Live authorization, network/request counts, cancellation and native scheduling are separate evidence. |

Run one conformance module unchanged with two fixture providers. The fake
provider is the default. The live provider is opt-in, assigns unique
synthetic names and scoped credentials, and seeds through documented SDK
calls or a pre-existing read-only keyed fixture. The cases, inputs and
assertions are identical given a table kind. Select kind explicitly and
fail a configured live run if a required capability is absent. Assertions
for unsupported operations check an explicit error and zero visible
mutations. Fixture-only failpoints and call-count assertions are additional
offline tests. Fixtures must clean up by verified ID or use an approved
disposable scope. Until a keyed delete path exists, keep keyed writes out of
persistent integration scopes. Do not skip required cases and count the
run as conformance.

| Group | Required cases and live evidence |
| --- | --- |
| Native SDK contract and capabilities | Same-name create/open and mismatched key schema, absence/permission distinction, exact lookup by ID/version, dropped/recreated name, list boundaries/empty pages, empty and composite-key snapshots, >10,000 rows, filters/projection, blank or malformed arguments, stored order and order after compaction. Own and close readers early and after errors. Confirm precise exception class/code. |
| Read parity and identity | Differential baseline fixtures from old client in a separate environment, acquired before switching. Cases for empty/normal/overwritten/compacted versions, explicit `asof`, latest refresh, metadata, nullability, limits, nested values, tag/source-info, stable IDs and hashes. Capture sync order exactly and compare async output as sets of rows. Check duplicate keys within/across batches before result reuse. Compare saved jobs on the real loader. Golden `--check` must stay clean. |
| Storage/type conformance | Round-trip table/schema metadata and field metadata under structs/lists/maps, data-tag markers, nullability paths, timestamps and zone/precision, signed zero and NaN bits, extensions and registered logical IDs, map names, fixed-size binary/list, duration, null and empty structs, key-only and empty writes, reserved names and NUL/length, append reorder/addition and different metadata/type refusal. Use the same exact-value/schema oracle on both clients and backend kinds. A refusal leaves visible table state unchanged. Verify no silent coercion before exposing capabilities. |
| Writes and publication | Upsert, skip-existing hit/miss and all hits, duplicate/incomplete keys, same-key and disjoint-key races, failed first write before/after metadata staging, lost reply/WAL reconciliation and frozen-commit recovery. Prove retries don't duplicate a record or publish a pipeline record before its result. Probe two readers with two writers, same resource vs different resources, different branches and projects. |
| Cost and memory | Record control-plane calls, object requests/bytes, planner time, first-row latency, wall time and peak RSS for cold/warm lookup, latest check, bounded head, full stream/count, selectivity, point/batched lookups, upsert, and reruns with growing history. Scale past 10,000 rows and 100,000 cap. Compare with legacy synthetic baseline. Use `Scan.metrics()`/planning time for keyless where available and redacted runner measurements for both. Verify selective reads avoid full row materialization, set an accepted lookup and memory budget before enabling the store. |
| Whole job parity | Compare to Delta for first/unchanged/added rows, changed function version, sync/async, save/load, `load_only`, `clear_cache`, split result store, trace, operator and cached-source paths. Seed only synthetic inputs. Reuse ITL-1446 cases. The same rows, types, source/record IDs and hashes are the release gate. |
| Package, docs and CI | Clean base import, SDK-only/absent/wrong distributions, optional guard error messages, binary wheels per claimed platform, executed samples, lint/type/golden, least-privilege sign-in and fail-fast missing runner settings. Publish a sanitized evidence table with pin, capabilities and counts. |

## CI sign-in and distribution

The current job runs the connector integration file on Python 3.11, serially,
only on push/manual triggers in the canonical repository. It hands a workload
ID and JWK to pyspiral using `SPIRAL_CLIENT_ID` and `SPIRAL_PRIVATE_KEY`.
The old OIDC exchange failed. The SDK documents automatic GitHub OIDC and the
JWT-bearer grant at its authorization service.
[Current workflow](../../../.github/workflows/run-tests.yml),
[release runbook](../../../RELEASING.md),
[platform identity](https://docs.spiraldb.com/docs/services/workloads/platform-identity),
[authentication grants](https://docs.spiraldb.com/docs/platform/http-api/authentication).

1. The developer provisions a test-project workload and grants the minimum
   roles that cover the conformance operations. Use separate read-only
   credentials if validating reader permissions. Confirm the storage access
   and operations that require write permission or task submission. Configure
   a GitHub trust policy with the canonical repository and
   `ref=refs/heads/main`, and constrain the workflow claim where supported.
   Use the documented default issuer audience for automatic token discovery.
   If the policy specifies a custom audience, request a token for that
   audience and supply it only as `SPIRAL_OIDC_PROVIDER_TOKEN` to the SDK
   process. Neither policy needs a stored secret.
2. Give only the gated job `id-token: write`, keep `contents: read`, and
   require canonical repository, push or dispatch, and main ref. Use
   `SPIRAL_WORKLOAD_ID`, `SPIRAL_PROJECT_ID`, and approved SDK endpoint
   settings as required. Pass settings to the test step and report missing
   names without printing values or tokens. An absent platform token with
   a workload ID is a hard error according to the docs. Prove the pinned
   SDK selects this path without device fallback. Avoid logging headers and
   credential-bearing exceptions.
3. Run both source and connector conformance serially with the live provider.
   CI on pull requests runs the unchanged tests with the fake and no SpiralDB
   identity. Validate positive and negative trust cases, including fork/ref
   isolation, with the developer before marking ITL-1185 resolved. Remove
   obsolete JWK variables and retire the old key only after a successful
   OIDC run. Update `RELEASING.md` and user docs. Human sign-in uses the
   CLI supplied by the pinned wheel's `entry_points.txt` and the documented
   `spiral login` flow. Test credential lookup with an SDK-specific environment
   before telling users they can reuse any saved login.
   [User sign-in](https://docs.spiraldb.com/docs/services/authentication/sign-in).

**Dependency.** Keep `orcapod[spiraldb]` and replace
`pyspiral>=0.16.33` with `spiral-sdk==0.16.74` in the implementation.
The older published `spiraldb` distribution has been superseded. Both
the current pyspiral wheel and the SDK wheel own `spiral/` and the
`spiral` console command. Installing both creates file and entry point
collisions even if a resolver accepts the two distribution names.
Uninstalling one can remove files of the other. Use a clean environment.
Remove pyspiral as a supported runtime client and avoid dual imports or
import manipulation. Retain its service format through SDK `KeyedTable`.
An isolated old environment can capture migration evidence or an approved
one-time metadata export. It is external to Orcapod's supported runtime.
[Pinned SDK](https://pypi.org/project/spiral-sdk/0.16.74/),
[old client](https://pypi.org/project/pyspiral/0.16.33/),
[superseded distribution](https://pypi.org/project/spiraldb/0.17.0/).

The wheel requires Python 3.11+ and `pyarrow>=19.0.0` (Orcapod already
requires 22+) plus `opentelemetry-api>=1.38,<2`. Its extras are
`adbc`, `harlequin` and `torch`. Request none. The wheel includes the
`spiral` and `spiral-sql` entry points, even though some docs describe
separate CLI/SQL packaging. The release has only cp311-abi3 macOS arm64
and manylinux glibc 2.28+ x86_64/aarch64 wheels, without an sdist.
The old pin advertises the same platforms. Test installs on claimed Python
versions 3.11–3.13 and document unsupported platforms for this extra and
`all`. Recompute and review the lock graph to see which old dependencies
actually disappear. Ordinary Orcapod remains SDK-free. Keep bundled
binaries out of the Orcapod wheel and sdist.

**License.** SDK `METADATA` declares
`MIT AND LicenseRef-Spiral-libspiral`. Shipped `LICENSE` covers SDK
wrappers and bindings and `LICENSE-libspiral` v1.1 covers proprietary
compiled runtime code, including code linked into native Python modules.
Its terms allow use and redistribution with license and applicable notices
and restrict reverse engineering. Orcapod stays MIT. The old wheel declares
a proprietary license. Current `_license-check.yml:28–36` installs dev
without the Spiral extra and uses partial string matching. A transitive
MIT match must not count as approval of the runtime. Audit the actual
SDK-enabled resolution and both license files, require an explicit narrow
developer decision for this optional distribution, and keep dependency
review and base license CI intact. Do not broaden a license allow-list to
all proprietary dependencies. [Pinned wheel and metadata](https://pypi.org/project/spiral-sdk/0.16.74/).

## What survives from ITL-1446

| Prior work | Keep | Rework or defer |
| --- | --- | --- |
| [PR 542](https://github.com/metamorphic-labs/orcapod-python/pull/542), `itl-1446-spiral-columns.md` §2–3, §7–10 | Exact-or-refuse contract, separation of logical and stored schema, deterministic name mapping where needed, manifest binding and validation, app-level record IDs and hashes, order policy, Delta parity, failure/interleaving model, cost budgets, least privilege and no credentials in saved files. Keep generic batched fetch, queue retention and all conformance goals. | New type and identifier evidence per kind. §6 cost numbers and the legacy point-key input/table null-placeholder behavior are historical. Replace access patterns with SDK typed scans/merge or prove an equivalent. Native Table stores versions; choose a stable synchronous store read order where callers depend on it. |
| PR 542 §4–6, lease/session/catalog/publication | Session admission and drain, exclusion of uncontrolled writers, cleanup by confirmed identity, and crash models remain design requirements where applicable. Keep record/result publication invariants, retention and controlled repair. | For new keyless stores prototype atomic result/index/record publication on one branch and project with frozen-commit recovery instead of three separately visible operations and an exclusive-create lease. Keep lease design only where the chosen backend lacks an equivalent or external side effects require admission before work. Recompute on overlapping conflicts, and preserve result-first or explicit refusal across projects. Re-evaluate whether the catalog needs a logical path index, generation isolation or tombstones once native atomic namespace and versions are available. Do not transplant the keyed lease takeover, control table bootstrap, KV publication or scans as implementation defaults. |
| [PR 544](https://github.com/metamorphic-labs/orcapod-python/pull/544), stricter fake and refusals | Preserve preflight validation, fixture cases, absence checks, no mutation after rejection and matrix documentation discipline. Treat measured old behavior as the baseline for differential tests. | Replace pyspiral mock signatures and ordering/placeholder/409 assumptions. Repeat probes for NUL, 255/256 names, key-only and empty writes, depth of struct restrictions, duration/fixed-size binary, duplicate keys and field order. Keep current public refusals during cutover unless parity proves a change is safe and the developer elects to widen support. |
| [PR 545](https://github.com/metamorphic-labs/orcapod-python/pull/545), pure store codec | Keep pure functions/tests around logical and physical schema, names, exact Arrow values, logical identifiers, structural validation, error paths, and format vectors as candidates. The codec has no pyspiral import. Its end-to-end tests still need the actual storage path. | Gate the on-disk format. Revisit `encode_name` and `physical_schema` if keyless accepts Orcapod's actual names and types. Retain versioned/authenticated manifest semantics while replacing `manifest_entries`/`read_manifest` legacy KV transport and size/chunk assumptions where unsupported. `__record_id` stays logical identity even where it is only an application merge key. An unused codec need not freeze names or type encodings before the live matrix. |

ITL-1164 and ITL-1176 still apply to keyed naming restrictions until
verified otherwise. The new Table can carry a different schema and declares
no native key, but its Arrow/namespace rules alone do not prove that it
accepts dotted/system columns or exact logical metadata. Probe the actual
bookkeeping column names, dotted names, nested fields and full job table
paths. Prefer native storage if exact, and retain reversible mapping only
where the verified backend requires it. A connector migration by itself
does not resolve the ITL-1162 function-node, side-effect, operator cache
and cached-source refusals.

## Pull requests and decisions

| Order | PR and size | Merge evidence |
| --- | --- | --- |
| 1 | This study, S | Review the API and decisions. Link as part of ITL-1186. |
| 2 | SDK boundary, synthetic legacy fixture capture, fake and shared conformance harness, M | Add the narrow contracts and tests before any default cutover. Establish separately provisioned live probe scope, compare old/new schemas and IDs, and get vendor answers on keyed count, metadata, enumeration and retirement. Keep the clients in separate processes/environments while comparing. |
| 3 | SDK keyed source, saved-job parity and direct connector cutover, L; split source and write work if needed | Land testable preparatory changes with no new default. Replace the extra, adapters and remaining imports together at the switch. The keyed source release gate is identity, metadata, preflight cap and version parity. Direct keyed connector additionally needs list/create/upsert/delete, lossless metadata, permission mapping and an approved concurrency contract. If these are blocked, keep the switch gated and take the scope question back to the developer. |
| 4 | Trusted live runner, dependency/packaging and author-facing docs, M | Operational setup can proceed alongside PRs 2–3. Turn on the normal SDK conformance runner before the switch is considered ready. Check the base/extra installs, license decision, signed runner, docs samples and exact golden values. Make the accompanying cutover docs available with the API switch. |
| 5 | Keyless pipeline store under ITL-1446, L for adapter/publication and L for pipeline wiring | Start with write probes and the pure codec decision. A native transaction across needed resources, real-store parity, accepted cost, recovery, and explicit backend registration/serialization gate enabling it. Split cross-project semantics or further optimizations into separate work after the same-project contract is proven. |

| Decision for developer | Recommendation |
| --- | --- |
| Resource model and client coexistence | Switch the extra to one pinned SDK. Keep keyed sources in place and select store kind explicitly. Use keyless for new pipeline stores. Preserve direct keyed connector operations where verified. |
| Missing native keyed capabilities | Ask Spiral for supported list/drop/count/KV read and write paths and service longevity. Treat count, metadata and identity as keyed source cutover gates. For existing metadata prefer documented access, then a deliberate export + resolver with integrity checks. Do not add an opaque protocol parser or an automatic prior-client fallback. If direct keyed CRUD cannot be preserved, approve an explicit change and migration instructions before release. |
| Keyless default identity and ordering | Require logical application keys and explicit resource-kind config. Keep original keyed IDs exactly. Specify keyless limit and deterministic sync ordering before exposing it. No change to key encodings or golden values as part of dependency replacement. |
| Concurrent writes and visibility | Limit keyed skip-existing to controlled writers until proven. Use keyless atomic commits for the same project/branch, recompute conflicts and recover unknown outcomes. Defer cross-project publication and avoid promising exactly-once external side effects. |
| Schema policy and codec | Keep the existing refusal envelope during migration. Remove restrictions one at a time with exact round trips and decision-backed documentation. Carry PR 545's logic forward only where it makes a verified difference and explicitly review any persistent format. |
| OIDC, license and platforms | Approve a scoped main-only workload and explicit optional-runtime license review. Publish the exact platform and server support tested under the pin. Keep clean installs as the supported transition. |
| Cost and release bar | Require unchanged-suite fake/live conformance, no full materialization for bounded reads, a proven preflight bound or approved failure behavior, published measurement budgets and an unchanged golden check. Hold release if old saved keyed sources cannot run without identity or schema drift. |
