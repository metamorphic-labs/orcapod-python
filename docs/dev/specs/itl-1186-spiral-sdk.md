# ITL-1186 — Move SpiralDB support to spiral-sdk

**Draft implementation plan.** Switch to `spiral-sdk==0.16.74` in one
independent PR from `origin/main`. Scenario 1 preserves Keyed reads and writes.
Scenario 2 switches to new Tables and treats Keyed reads as a migration
bridge. Spiral has yet to confirm a retirement timeline. Neither scenario
takes commits, merges or cherry-picks from #541, #542, #544, #545, #546 or
#547.

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
- This study remains offline. The live results below were supplied by the
  coordinator from separate Keyed probes. They are evidence for the pinned
  SDK and the probed service, with the remaining release gates listed below.
  Publish the exact tested pin and hosted Keyed support after conformance.
  Ask Spiral for the keyed support window and supported metadata and
  lifecycle paths. External consumers follow separately.

### Separately measured Keyed results

The coordinator reported that the existing Keyed tables open by dataset and
name, with `resource_id` equal to pyspiral's table ID and `version_id` equal
to `last_modified_at`. Historical opens, primary-key order, client-to-client
read/write interoperability, upsert, streaming, typed filters and exclusive
create were exercised successfully. The old 10,000-row cap was absent.
Key ranges omitted absent keys. No write conflict check was observed. Keep
the existing `skip_existing` cost and concurrency limits.

The SDK exposed no Keyed list, drop, rename, row count or table KV.
`append` left old KV intact, but the SDK could neither read nor write the
nullability record or `__arrow_metadata__`. Field metadata was absent from
the SDK readback. Key creation refused date, timestamp, decimal and fixed-size
binary in the reported probes. Probe the precise type variants and the
ability to read existing tables with those key types. SDK errors obscured
response bodies. The documented HTTP list remains a candidate to validate
independently. Rename was observed as a gap but is outside today's Orcapod
connector API.

## Scenario 1 — Keep Keyed tables

### Calls and behavior

The table handle pins an immutable snapshot. Reuse it within reads, and
resolve again to observe a new version. Lookups, metadata operations, plans,
reads and publications are logical operations. Measure actual request counts
and memory rather than inferring them from wrapper calls.

| Orcapod today | SDK Keyed equivalent and parity check |
| --- | --- |
| `sp.Spiral(overrides=...)`, `.project(project_id)`, shallow `share_client()`, `close()` drops references (C 553–574, 1232–1244) | `spiral.Client(...)` and `client.project(id)`. Project selection is local and the wrapper has no public close. The client accepts named endpoint/token/cache options and discovers credentials from env, saved login or device sign-in. Translate supported nonsecret saved overrides, keep wrappers independently closable, and verify native sharing and cancellation. `client.py:81–137,174–204,311–339`. |
| `project.list_tables()`, exact dataset filter and sorted plain names (C 584–596) | The pin has no Python keyed list method. The public HTTP list accepts a dataset and uses opaque page tokens, including empty intermediate pages. Retain sorted names and isolate one supported authenticated bridge if needed. No listing before each name lookup. `services/__init__.py:43–81`, `services/tables/raw.py:80–230`. [Keyed list](https://docs.spiraldb.com/docs/services/tables/api/keyed-tables/list-tables). |
| `project.table(dataset.table)`, invalid names and a precise old missing-table `ValueError` (C 598–627, 672–675, 1037–1041) | `client.keyed_table(project,dataset,name,asof=...)` and `keyed_table_by_id` for a previously verified ID. Preserve name and dataset guards and translate only verified absence. `SpiralError` has no dedicated missing-table subclass in the stub. Probe denied, invalid name/version, absent and network failure. `client.py:190–204`, `_lib.pyi:19–30,84–89`. |
| `.table_id`, `.key_schema.names`, `.last_modified_at()` (C 477–504, 629–638, 865–889) | `.resource_id`, `.key_columns` in schema order, `int(.version_id)`. The version is a decimal Unix-microsecond timestamp. Prove exact resource ID, key order, and refresh behavior against pyspiral. These properties come from the pinned handle. `keyed_tables.py:80–106`. |
| `.schema().to_arrow()` and `.get_metadata()`; restore metadata and nullability, normalize Arrow layouts (C 486–499, 672–687, 752–812, 849–863) | `.schema()` returns Arrow from scan planning without row reads. Preserve the batch/schema preparer and align it with the pin. The SDK does not expose old KV methods. Prove old metadata visibility, prepared schema and typed empties, including malformed-record errors. Metadata may cost its own request. `keyed_tables.py:163–169`, `_lib.pyi:289–319`. |
| `table[column] == value`, `&`, `table.select()`, `spiral.scan(...,where=...,limit=...,asof=...)`, `.to_record_batches()` (C 506–526, 690–750, 814–847) | Resolve `asof` at open, then call `table.scan(filter=expr,limit=n).to_reader()`. Use `eq(column(name),literal(value,physical_type))` and `and_`. Probe equality, typed literals, filter-before-limit and primary-key order. Keep the SQL-shaped full read, ignored params/batch-size warnings, existing slicing and typed empty behavior. SDK batching/read-ahead do not bound process memory. Reader failures may surface after earlier batches. `expressions.py:54–116,170–201,357–385`, `keyed_tables.py:133–169`, `tables.py:724–787`. |
| `table.core.get_snapshot(asof=...)` and `spiral.core.manifest_stats(...).key_space_stats.total_rows` (C 528–534, 891–905) | No public SDK equivalent. The documented snapshot contains opaque metadata. Require a supported conservative count for the same pin that needs no row scan. Verify upper-bound behavior through overwrites, deletes and compaction. Preserve preflight cap before delivering any rows. `_lib.pyi:289–319`. [Keyed snapshot](https://docs.spiraldb.com/docs/services/tables/api/keyed-tables/read-snapshot). |
| `project.create_table(dataset.table,key_schema=[(pk,type)],exist_ok=True)` (C 918–956) | `project.create_keyed_table(dataset,name,pa.schema([...]),exist_ok=True)` returns a pinned handle. The SDK documents opening an existing table with the same key schema. Test empty typed tables, same-name retries, mismatched schemas and create races, and adapt any proven difference in the previous no-op behavior. `projects.py:50–57`. |
| `tbl.write(rows)` upserts by full composite key, after Orcapod validates `id_column`, all key names, type and value constraints (C 958–1140, 1161–1229) | `table.append(rows)` upserts by primary key and returns the new snapshot. `writer().append/finish/abort` is for staging a single table's row changes in one WAL transaction. The simple path needs one append. Test same-key replacements and partial-column updates, multiwriter behavior and failed writes. A lost reply can leave the outcome unknown. Do not promise replay safety or strengthen concurrent behavior on the strength of the wrapper. `keyed_tables.py:171–209`. [Keyed WAL publication](https://docs.spiraldb.com/docs/services/tables/api/keyed-tables/publish-wal). |
| `skip_existing=True` reads the entire table with `spiral.scan(tbl.select()).to_table()`, then filters incoming rows by full key (C 1141–1153) | Preserve the full read with `table.scan().to_table()` and the existing key filter before append. Keep the all-hit no-write path, existing cost and existing read-before-write race. The SDK has no atomic skip-existing option. Check hit/miss, composite keys and no-write behavior. `keyed_tables.py:133–172`, `tables.py:770–787`. |
| `get_metadata`, `set_metadata`, `drop_metadata`; N writes chunks then the marker, C stages Arrow metadata before rows and cleans up on failure (C 1038–1133, N 179–299) | No public methods in the pin; Keyed WAL writer stages rows and column changes only. Keep the existing record format and failure/cleanup semantics through a supported KV bridge. Verify old records, new writes, first-write failure, existing records on failure, and races. Retain current refusal and metadata capability reporting until proved. `_lib.pyi:289–319`, `keyed_tables.py:184–209`. |
| `project.drop_table(dataset.table)` after list, missing is a no-op (C 907–916) | The pinned SDK and current public Keyed HTTP reference expose no drop. Require a vendor-supported delete-by-ID path, check absence without masking other errors, and test lookup/drop concurrent with name reuse. Use authorized disposable fixtures while investigating retirement. `_lib.pyi:289–341`. [Tables API](https://docs.spiraldb.com/docs/services/tables/api). |

### Connector gaps and proposed behavior

The SDK wheel alone cannot implement the following current calls. A
documented, vendor-supported transport is acceptable inside the same small
adapter. It must share the SDK client's approved authentication model and
must not depend on private native symbols or parse opaque snapshot JSON.

| Current behavior without a direct SDK equivalent | Smallest parity path | If it cannot be kept |
| --- | --- | --- |
| Dataset enumeration | Implement the documented Keyed list endpoint with all pages and exact dataset selection. Confirm auth is compatible with user login and CI OIDC. | Fail listing explicitly if auth or enumeration cannot be supported. Returning an empty list would misrepresent existing tables. This exception to parity requires developer approval. |
| Table deletion, including missing no-op | Obtain a supported Keyed retirement method by stable ID. Translate verified missing to no-op and preserve other failures. | Existing table deletion raises an explicit unsupported-operation error. Confirmed absence can still be a no-op. Never return success for an undeleted table. Requires a decision before switching. |
| Manifest estimate and preflight cap | Obtain a supported same-pin conservative metadata count, with no row read. | An explicit cap-safe limit still works. Refuse an otherwise capped read before delivering rows if the bound cannot be established, and make the estimate itself explicitly unavailable. No silent full scan or delayed cap failure. Requires a decision before switching. |
| Legacy read/write KV and write cleanup | Keep both existing record formats and their read, set, delete and failure semantics using supported endpoints. Test new writes as well as old data. See the metadata options below. | Refuse ambiguous reads and writes requiring unavailable KV before mutation. Advertise only proven capability. The developer must accept any narrower coverage before the switch, and must decide any change to a released source's schema. |
| Key creation with previously accepted Arrow key types | Compare pyspiral and the SDK on date, timestamp, decimal and fixed-size binary and separately open existing tables with those schemas. Prove the current supported set and preserve read/record-ID parity. | If a previously supported type can no longer be created, reject it before allocation with an actionable type error. Keep reads if parity passes. An unreadable existing keyed source blocks the switch unless the developer approves the impact. |
| Saved, free-form pyspiral client overrides | Translate explicitly understood nonsecret settings (such as `server.url` to `api_url`) and use SDK auth discovery. | Unknown/obsolete settings get an actionable configuration error. Never ignore an unknown endpoint or forward credentials in a saved artifact. Verify which existing public settings need translation. |
| Exact SDK-dependent failure behavior | Translate only recognized absence, validation and authorization cases to preserve Orcapod's current public errors and no-op paths. Check mismatched `exist_ok` schemas and old write constraints on the live fixtures. | Surface other native or transport failures with context. If the service now accepts an input formerly refused, keep Orcapod's documented refusal. If an accepted input is now refused or loses data, refuse before mutation with the failing capability named and get approval for the change. Under an ambiguous WAL outcome, report uncertainty and reconcile before retry. |

At this point list has a documented HTTP path, and count, KV and retirement
lack a proven parity path. The right default is to hold the switch until
these are resolved or the developer explicitly approves a behavior change.
Live tests for such a change must assert the new explicit failure and the
absence of unintended mutations.

#### Metadata is the main gate

A keyed table can store a nullability record that restores a field's
declared `T`. Reading through the SDK without that record can produce
`T | None`, change the pipeline hash and stored paths, and invalidate
result reuse or function input checks. Losing `__arrow_metadata__` can
also remove field/schema attributes and data-tag information. SDK append
leaving the old records in place does not make them visible or establish
safe first writes. A released source's schema may change only with an
explicit developer decision.

| Option | Cost and coverage |
| --- | --- |
| Documented HTTP KV bridge | Preferred if Spiral documents and supports get, set and delete under SDK user and workload auth. Adds control-plane requests, credential integration and failure/race tests. Preserves old formats and can cover reads, fresh writes and cleanup. The public Keyed HTTP reference inspected for this study does not document these KV operations. |
| One-time pyspiral export into a companion table | An isolated legacy process copies the intact records, tied to stable table ID, schema fingerprint and capture point. Adds an export/check step for each covered table, an extra read, permissions and operational ownership. It can preserve reads for a captured set after exact parity checks, but can go stale and does not preserve the original write/rollback contract. This is a fallback requiring developer approval, not part of the smallest parity switch. |
| Wait for Spiral | Keep today's client until a supported KV path or SDK release exists, then repeat the pinned conformance. Avoids a second metadata representation and keeps compatibility, at the cost of delaying the switch and retaining exposure to the old client's support window. Prefer waiting over silently changing schemas or hashes. |

### Keep existing keyed sources readable

1. Keep `source_type="spiraldb_table"`, existing project/dataset/table
   selection, and natural-name format
   `spiraldb_table_v1:<project>.<dataset>.<table>@<table id>`. Read the keyed
   table in place with `Client.keyed_table`. Require a byte-for-byte match
   of the SDK `resource_id` with the pyspiral table ID on the release fixture,
   as in the supplied live evidence. A mismatch blocks reuse of the old
   source identity.
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
   (S 375–390, 888–895). Cover schema drift explicitly and retain current
   failure behavior when the declared and scanned schemas differ. Gate on
   retained history.
4. Restore the intact N chunks and marker and the Arrow metadata tree by
   the chosen read path. Distinguish a genuinely unrecorded table from
   unavailable, malformed or unverifiable metadata.

For async reads keep native calls off the event loop. S 921–944 advances one
generator using `asyncio.to_thread`, potentially on different threads over
its life. Until cross-thread reader use is verified, own each reader in one
worker with bounded handoff, explicit draining/close on cancellation, and one
reader per consumer. Test independent closable source copies (S 576–603,
687–719), simultaneous source and refresh, and cancellation while a native
read is in flight. The pipeline can still materialize all delivered rows.
The cap/preview behavior in S 835–919, 957–1043 stays a separate test from
SDK read-ahead.

### Other Orcapod entry points and saved artifacts

The inventory starts with `git grep -ilE 'pyspiral|spiraldb|\bspiral\b'` on the
baseline and covers executable imports, transitive callers and test fixtures. The following covers active integrations. Historical changelog,
design notes and recorded measurements can continue to name the client
they used. Any retained executable probe must run with the new dependency
or be removed from the runnable probe set.

| Entry point or surface | Migration and evidence | Saved artifact |
| --- | --- | --- |
| Public exports `orcapod.sources`, `orcapod.databases`, root import path and optional-import guard (C 50–59 and package `__init__.py` files) | Keep imports usable without the extra. Resolve the pinned SDK at integration use, with a clear install error for an absent package and an actionable wrong-distribution error for old `spiral`. Use distribution ownership/version plus expected symbols, since both distributions import under the same name. Test subprocesses with neither, only SDK, only old package and shadowing/broken native import. Preserve unrelated errors and avoid import-time client creation. Consult `tests/test_utils/test_lazy_module_use.py` before introducing another lazy proxy. | Import names and documented constructors stay available. |
| Source build, sync and async streams, refresh and preview (S 298–453, 739–1056) | Resolve existing Keyed source with the pinned SDK. Port lookup-count, schema-only, preview/cap, filtered reads, empty typed results, cancellation and drop/recreate. Compare deterministic sync rows exactly and async rows as sets, with schema metadata, IDs and hashes on both paths. Test a new version computing only new/changed rows in a job. | Keep `spiraldb_table` and explicit/natural identities as above. |
| `PollingSource`, `DerivedSource`, `CachedSource`, restrictions and operator caching | The Spiral source's dynamic behavior is `check_for_new_data`. `PollingSource` is a separate user-fetch abstraction with no built-in Spiral binding and no entry in the baseline source registry. A custom fetcher that uses this connector must be tested through its normal cursor and resume flow. Preserve check-on-run and pin isolation. Keep restricted sources' cache rules and single-condition validation. Port `test_source_check_for_new_data.py`, `test_filter_pipeline.py`, `test_order_invariance.py`, `test_explain.py`, tags-as-data, tag-only, and alignment tests. Exercise wrapped source restoration, unchanged rerun, new rows, restarts and refusal for invalid restrictions. | Old jobs with supported wrapped keyed sources must rebuild a readable source and retain matching IDs and hashes. Preserve existing limitations for saving and restoring user-defined polling fetchers. A proxy is useful for inspection but does not pass executable parity. |
| Source registry and `PipelineJob.load` | `pipeline/serialization.py:244–275,530–588` dispatches the saved source kind and may fall back to `SourceProxy`. Capture a pre-switch job file and load under an SDK-only interpreter through the real entry point. Assert concrete source, exact key/order/metadata and same graph, source/pipeline hashes and cache hits. Test unpinned, pinned, explicit ID, nested wrapper, unavailable table, wrong ID and missing extra. | Keep these saved jobs readable without a legacy package. |
| Connector config and database registry | C 1246–1285 round-trips `connector_type="spiraldb"`. Keep current keys and keyed semantics. Translate supported nonsecret settings and reject unknown ones clearly. Test this path separately from saved sources. `ConnectorArrowDatabase.from_config` is unimplemented and the wrapper is absent from the database registry (`connector_arrow_database.py:923–944`, `serialization.py:227–241`). | Direct connector configs and jobs using a Spiral source with a supported database continue to load. Preserve the current connector-database serialization limitation. |
| Existing `ConnectorArrowDatabase` callers | It uses DBConnectorProtocol to list/create/scan/upsert/delete, read schema and validate metadata preservation. Keep existing queues, record alignment, flush, capability and refusal behavior. Run `test_append_by_name.py`, `test_appended_field_attributes.py`, `test_logical_identity_appends.py`, `test_unstorable_columns.py`, data-tag tests and the type matrix. | Existing supported database contents remain readable with the same metadata and data context. |
| Data function code and packaging | The Python function type manifest and function config registry have no direct Spiral SDK imports. A function which consumes a Spiral source gets Arrow-derived inputs. Preserve callable identity, serialized descriptors, and the library wheel's dependency boundary. Test a real installed-wheel save/load (functions importable by name), function call counts, result/cache/row hashes, and fresh worker imports for async/distributed jobs. User functions importing pyspiral APIs directly must switch their own code and environment. | Existing Orcapod function descriptors keep their format. A legacy client's serialized runtime object or import is outside the descriptor contract. |
| Current fake and consumers | Replace `fake_pyspiral.py` and migrated monkeypatch call sites. Inventory includes source build/lookups/streaming/`where`/tags-as-data/large normalization/refresh and tag-only tests, core alignment/capability/filter/explain/order-invariance, connector fake/nullability/logical-identity/unstorable tests, type matrix, and `test_run_warnings_caller_globals.py`. Port raw setup to a fixture that seeds snapshots through the appropriate writer or a read-only fixture import. Keep the shape of the example Orcapod pipelines. | No fake disk layout is public. Synthetic serialized job and hash fixtures should remain readable and stable. |
| Opt-in integration tests and runner | `test_spiraldb_connector_integration.py` also has raw legacy client use through connector internals. `test_spiraldb_table_source_live.py` directly instantiates legacy clients for its write cases and is outside today's CI command. Replace both with named synthetic fixture setup and include shared source/connector conformance in the opt-in job. Exercise offline integration skips as well as fail-fast misconfiguration on the authorized live runner. | Reuse an independently seeded keyed resource to prove old-table reads. Create mutable fixtures only in the isolated test scope. |
| Published docs and examples | Update README extras and refusal notes, `docs/concepts/sources.md` install/login, examples, count, limit, identity, refresh and nullability; `pipeline-job.md`, `streams.md`, `notebooks.md`, `operators.md`, `data-tags.md` and `docs/tutorial/00-setup.md`; regenerate `docs/reference/database-types.md` from the tested matrix, identifying the tested keyed SDK and service evidence. Run `tests/test_docs/test_doc_samples.py` and docs site checks. Hosted examples need their existing infrastructure skips. Checked-in `examples/` have no direct legacy client calls. | Examples which write/read saved jobs must show the same source kind for keyed tables. |
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

### Fake and conformance

Build an Arrow-backed, disk-scoped fake for only the operations above. Reuse
the fixture isolation and call-observation idea from F 237–356, 800–881.
Keep namespace entries distinct from immutable snapshots. Exercise the same
Orcapod adapter and contract with both providers.

| Fake rule needed by the switch | Evidence and check |
| --- | --- |
| Resolve Keyed names to stable IDs, ordered keys and pinned timestamps. Reopen retained versions by ID. A new resource at the same name gets a different ID. | SDK `client.py:190–204`, `keyed_tables.py:41–106`, S 726–736. Verify ID/version parity against independently captured legacy fixtures. |
| Apply typed filters, then limit, and stream in primary-key order. Support zero-row schema discovery and inject a failure after yielding a batch. | `keyed_tables.py:133–169`, `tables.py:724–787`, and S 835–919. Use configurable batch boundaries without claiming the SDK's native chunk sizes. |
| Upsert full keys, preserve earlier snapshots and return the new pin. Model current validation and metadata cleanup separately from row writes. | `keyed_tables.py:171–209`, C 958–1153, N 179–299, [WAL contract](https://docs.spiraldb.com/docs/services/tables/api/keyed-tables/publish-wal). Don't invent same-key conflict precedence or replay guarantees. |
| Restore the old fixture's schema and nullability through the existing preparer. Keep default fake type conversions only where the SDK readback confirms them. | C 752–812, N 340–431, F 90–164. Check large layout normalization and declared/recorded flags through both schema and batches. The supplied probes show raw SDK readback lacks field metadata. Test restoration through the chosen KV path. |
| Model the selected list/count/KV/drop path, including page tokens, absent vs denied, malformed old records, pinned metadata bound, and cleanup errors. | [Keyed list](https://docs.spiraldb.com/docs/services/tables/api/keyed-tables/list-tables), C 584–627, 752–812, 891–916, N 179–299. Use live-confirmed contracts for the gaps. The fake must not fabricate a bridge or certify a metadata approximation. |
| Inject blocking and auth/transport failures at the adapter boundary. | `client.py:103–137`, `_lib.pyi:19–30`, S 926–944. Unit tests cover cleanup and cancellation. Verify authorization and native scheduling separately. |

#### What PR 544 established for the fake

Use its [probe record](https://github.com/metamorphic-labs/orcapod-python/blob/fd72822faf0d58464fc8530ec294c659a3c18409/docs/dev/plans/2026-11-24-itl-1446-strict-fake.md)
as evidence about pyspiral 0.16.33 and the then-running Keyed service. PR 544
itself contributes no code to the switch. Separate Orcapod's existing
validation from native SDK behavior. The fake must reproduce existing
Orcapod guarantees immediately. Apply native restrictions to its SDK layer
only when the pinned SDK documents them or the shared live cases confirm
them.

| PR 544 evidence | New fake and conformance decision |
| --- | --- |
| Live writes refused fixed-size binary, duration, interval, empty struct and null struct values, including null struct children of a struct. The offline legacy converter also refused union, and fixed-size binary/duration inside containers. | Keep the current connector's recursive null-struct refusal with no mutation. Probe the other shape/type restrictions before modeling them as SDK refusals, including nested combinations and the distinction between an empty struct type and a null struct value. Do not adopt PR 544's blanket converter rule or its exception text. |
| The legacy service kept untyped Arrow `null` values and sorted the immediate fields of a top-level struct by name. Prior live evidence (ITL-1367, cited in PR 544's fake) established top-level keys in declaration order then value names in sorted order, with appends matched by name. | Reproduce the exact prepared schema and values on legacy fixtures, probe fresh SDK write/read behavior, and model verified ordering where it matters to current reads. Do not extrapolate struct sorting inside other containers. Keep untyped value null distinct from a Null key type. |
| Live legacy writes truncated a column at NUL. The actual table-name path accepted length 128, 129 and 255 and rejected 256, contradicting the old 128-character annotation. Other character restrictions were unverified. | Add parity probes for NUL and collisions before any real mutation, and name-boundary cases on the table component. If the SDK also truncates, guard against loss and assert refusal with zero mutation. Adopt the measured SDK name limit only after confirmation. Do not normalize or encode names, or assume arbitrary Unicode/ASCII rules from these probes. |
| A live legacy zero-row write caused a native panic. PR 544 used a normal exception as its fake substitute. Key-only write refusal, duplicates within a batch, `$` on a value name and Null key-schema type had legacy native diagnostics. Across-batch duplicates were unverified. | Probe SDK empty append and key-only write separately from empty table creation. Model an ordinary error for an SDK refusal and never a synthetic panic. Do not equate key-only schema with zero rows unless the new writer confirms it. Test single and multiple batches, reserved value names and Null keys before inheriting those native restrictions. |
| Legacy `Scan.to_table(key_table=...)` emitted a null placeholder for an absent key. The coordinator's SDK probe saw absent keys omitted from key ranges. PR 544 also modeled legacy existing-name errors and exact strings. | Omit the unused key-table request and placeholder rule. The current connector has no key-range lookup to add to the fake. Model exclusive create and test public Orcapod errors without copying the old client strings or assuming the SDK exposes error bodies. |

#### Live gates for merging the switch

Run one Keyed conformance module unchanged with two fixture providers. The
default Arrow fake needs no Spiral package. The opt-in live provider uses
scoped credentials and synthetic resources, including a read-only fixture
created before the switch and disposable fixtures for writes. Fixture-only
fault injection and call counters are additional offline tests. The live
run fails if required capabilities cannot be exercised. Capture the old
client baseline in a separate environment before uninstalling it. A fake
pass or an SDK wrapper signature alone cannot satisfy these gates.

| Group | Required evidence on Keyed tables |
| --- | --- |
| Existing sources and saved jobs | Same table ID, key order, prepared schema (types, field/schema metadata and nullability), record IDs, source-info tokens, row and pipeline hashes as pyspiral. Cover empty, historical and latest snapshots, refresh, drop/recreate refusal, filtered/limited reads and source caps. Load an existing job through the real loader and check that it reads through a concrete source and reuses results. Sync outputs exact, async outputs as sets. Golden `--check` remains clean. |
| Connector and metadata | Enumerate a dataset across pages, resolve ordinary and composite-key tables, create idempotently, upsert, skip existing, delete and ignore true absence. Round-trip old and new KV/nullability records and Arrow layout, confirm malformed records raise, and verify that failure cleanup keeps earlier rows and metadata intact. Run the exact-value type matrix and the PR 544 probes above. Test same and disjoint key races and distinguish definite failure from unknown WAL outcome. Any approved exception to parity must be explicit and tested. |
| Bounded read, cancellation and cost | Show count/metadata lookup needs no rows, cap refusal precedes any row delivery, and filter/limit reads remain streaming in key order. Measure cold/warm calls, bytes and memory on selective and full reads, including above 10,000 rows and the default 100,000 cap. Evaluate the existing full-table cost of `skip_existing`; this PR does not change its algorithm. Exercise simultaneous readers, cancellation and close. |
| CI, package and docs | Complete serial live connector and source conformance on the trusted OIDC runner, including user-vs-workload permission checks and explicit failure on missing settings. Prove clean base and SDK-only installs, wrong-distribution guards, successful platform builds, executed docs samples and type/format/golden checks. |

### CI sign-in and distribution

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

### Smallest switch and PR order

PR #548 is the offline study. Implement its switch as one reviewable PR,
size **L**, with the following changes together. Start it from then-current
`origin/main` and verify the base and diff exclude #541, #542, #544, #545,
#546 and #547. The implementation can use separate commits for review.

1. Replace only the Spiral client dependency, optional guard and connector
   SDK calls. Keep one Keyed adapter for required service gaps, with the
   existing connector API, behavior and configuration.
2. Keep the source's construction, streaming, refresh, IDs and serialized
   interface. Port the fake and existing tests to the new boundary. Add
   the shared Keyed conformance and historical job/identity fixtures.
3. Switch the existing opt-in CI job to SDK OIDC. Update installation,
   sign-in and parity documentation, refresh the lock and check licensing.
   Remove the legacy runtime dependency and calls in the same PR.

Do the Keyed probes before asking for merge approval. **One PR is
recommended.** The dependency swap, connector, source and fake must agree
at every mergeable point because the clients share the same import name.
The small expected bridge belongs with the switch. Split only if proving
or implementing a supported bridge requires a separately reviewable
transport/auth change or verification harness. A preparatory PR in that
case may add isolated tests or an unused boundary and must leave today's
runtime and dependency selection intact. The final switch still lands
atomically. A vendor gap is a release decision, not a reason to expand the
switch.

| Decision | Recommendation |
| --- | --- |
| Keyed gaps and support window | Ask Spiral for supported authenticated list, count, KV and retirement paths. Hold the switch until live parity passes or the developer approves the specific behavior changes listed above. |
| SDK/client support | Keep the extra name, use one pinned SDK in a clean environment, and report the hosted Keyed support and platforms actually verified. Keep saved IDs and hashes and test them before switching. |
| CI and licensing | Approve a main-only OIDC workload and a narrow review of the optional native runtime license. Require a successful live conformance run and preserve existing offline CI. |

## Scenario 2 — Switch to new Tables, migrate Keyed tables

This alternative replaces Keyed writes with the SDK's branch-and-commit
`Table` model. Existing Keyed sources are readable only during a documented
migration window, subject to service availability and verified metadata
access. The developer suspects Keyed tables may be retired. Spiral has
yet to confirm whether or when reads, writes, or historical versions will
end. Ask for that timeline, supported export options and an escalation path.
The separate live observation supplied for this study is that new Tables
preserve and enforce declared Arrow nullability. Keyed reads lose it, even
for data written by the SDK. The transition cannot be justified by assuming
that `KeyedTable.append` repairs the schema.

### Names, identities and existing jobs

- **Physical locator.** Use project, Table path and an explicit branch or
  version selector. Start with `main`. A Table path has no dataset.
  The service allows slash-separated namespace segments and reserves
  `@` for a ref. At this SDK pin, `Commit.create_table(name, table)`
  documents a root binding. Verify a supported creation path before
  promising nested creation. For migration, let the owner select a unique
  destination path, preflight collisions across legacy datasets, and record
  the old project/dataset/name/ID and new project/path/ID/version. Never
  resolve an old saved dataset by guessing a similarly named Table.
  [Resource names and refs](https://docs.spiraldb.com/docs/platform/concepts/resources).
- **Default source ID.** Today S 156–190 includes the old native table ID
  in the default. A newly constructed source over a copy uses a distinct
  location name built from the new project/path and resource ID, under the
  existing `spiraldb_table_v1` kind if the developer approves. The default
  must be unambiguous and include the resource ID so drop/recreate creates
  a different default. Its pin belongs to read state. Verify path/ref
  semantics before supporting more than `main`. A copied table has a new
  resource ID even when its rows are identical.
- **Preserved source ID.** Retain an explicit source name, or deliberately
  reuse the old default's saved `source_name` after verification. Preserve
  `source_type` and `source_version` as well. The user-facing
  `source_id=` parameter is the name part. Passing the full old ID as that
  argument prefixes it again. The preserved name keeps the source ID,
  its system-tag value and the source component of provenance tokens.
  It does not change the destination's physical ID or replace the
  drop/recreate check. Record IDs independently follow the same ordered
  logical keys and values. Keep the schema, nullability, metadata, tag
  configuration and data context to retain full provenance, source
  content hash and cache identity.
  The root pipeline hash is schema-only. Merely keeping the source name
  neither repairs schema drift nor guarantees downstream cache reuse.
  [Root source identity and saved names](../../../src/orcapod/core/sources/base.py),
  [record IDs and provenance](../../../src/orcapod/core/sources/stream_builder.py).
- **Record IDs without a primary key.** Require a nonempty, ordered
  `record_id_columns` for a keyless source, and propose no positional
  fallback. On copy, freeze the previous effective key list in this field,
  including its order and any override. Preserve the previous effective
  tag columns separately. On construction, check key names exist once
  each, preserve types and reuse Orcapod's allowed-type check (strings,
  integers, booleans, dates and timestamps). For each pinned copy, scan
  the entire key projection for nulls or duplicates of the canonical
  composite identifier, including across batches. The ordinary streaming
  reader still checks all delivered batches. Restricted reads alone
  cannot establish global uniqueness. Neither the SDK's storage row ID
  nor its stored order is the Orcapod record ID. The direct connector
  also needs a declared merge key per Table in caller configuration or a
  verified native schema property. Validate it and the target's uniqueness
  on create/adoption and maintain that invariant on all writes. SDK
  `merge_rows` can update multiple target rows for one input key.
  [Key validation and encoding](../../../src/orcapod/core/sources/stream_builder.py),
  [SDK merge behavior](https://docs.spiraldb.com/docs/services/tables/write-rows#merge-rows-by-key).
- **Saved jobs.** Read unchanged Keyed descriptors through the temporary
  reader while possible. To switch one, back up the job, explicitly
  replace its physical locator and expected table ID with the independently
  verified destination, preserve the name/kind/version and other source
  settings, and test the real loader. S 1104–1182 already refuses a
  different table ID, so there is no automatic fallback by name. A saved
  integer `asof` requires a copy of that exact old snapshot and a recorded
  destination version. Preserve implicit-current and explicit-pin
  semantics separately. If there is no verified copy, the job cannot be
  reconstructed as a readable source. Where the loader supports it, its
  existing proxy is for inspection.

### Copy and verify each snapshot

1. **Inventory and freeze.** Publish a migration runbook for user-owned
   tables. The owner chooses the timing, target project/path, permissions
   and whether to retain prior source identity. Inventory active and saved
   jobs, key declarations, source IDs, schema, KV metadata and all
   historical pins needed. Stop writes for final capture, or arrange an
   explicitly validated catch-up and choose one final snapshot. Old KV
   can change independently of the row snapshot, so capture it while
   quiescent and record its capture point. Each owner brings their own
   read and destination-write access.
2. **Export with pyspiral, write with the SDK.** Use separate clean
   environments because both distributions own `spiral`. Read the
   chosen Keyed snapshot with pyspiral and restore its Orcapod
   nullability and Arrow metadata records using C/N. Validate every
   recorded non-nullable path before exporting first-class Arrow fields
   and batches, for example in a protected Arrow IPC export. A table
   without a record retains its observed flags. Never infer `NOT NULL`
   from a sample. In the SDK environment, create using the restored Arrow
   schema and stream the batches with `Project.table_writer(schema)`
   or `Project.write_table(reader)`. Finish a candidate version, then
   verify and publish it with the documented naming/commit API. Copying
   through raw SDK Keyed reads would miss the old KV restoration.
   `projects.py:113–126,273–276,322–377`.
3. **Refusals and type fidelity.** An offline probe of the pinned macOS
   arm64 wheel wrote small, synthetic Arrow batches into a new temporary
   directory per case with `Client._without_control_plane()`,
   `_create_table_at`, `append`, `finish` and `to_arrow()`
   (`client.py:139–144,226–237`, `tables.py:883–918`).
   These private entry points isolated native conversion. The proposed
   implementation uses the public hosted writer. The probe used PyArrow
   23.0.1 and no authentication or service. It found the following.

   | Observed result | Example input in the local native Table probe |
   | --- | --- |
   | Rejected at append | Fixed-size binary (2 bytes), nanosecond duration, month-day-nano interval, dense union, nonempty map and a one-row empty struct. A null in a declared non-nullable int64 field also failed. |
   | Values read back in the tested type | Arrow null, decimal128(12, 2), decimal256(40, 2), fixed-size list of int32, UTC nanosecond timestamp, a null struct with an int64 child, float16, and Arrow UUID. These examples do not certify nested combinations or every value. |
   | Read back with a different layout | String view and list view became ordinary string and list, and dictionary and run-end string examples became plain strings. Require exact prepared schema and bit parity before accepting even a layout conversion. |
   | Flags and metadata | A non-null int64 stayed non-null in both description and scan. Its arbitrary field metadata appeared in the description only. Arbitrary schema metadata was absent from both. A native nullability pass alone is insufficient for a metadata-bearing Orcapod source. |

   This is evidence about native conversion at the pin, with hosted
   validation still required. The documented continuous `ingest`
   restrictions are separate. Ordinary appends require the table's
   columns and stored types, and merges reject duplicate input keys.
   Repeat these cases on the hosted ordinary writer, plus typed empties,
   nested nullability, field/schema metadata and extension identity.
   Verify direct writes and merge keys separately. Preserve current
   Orcapod refusals unless the developer agrees to change them. Refuse
   any unproved, rejected or lossy column before publishing the copy.
   If the hosted description preserves metadata that the scan omits,
   test safe restoration from that exact version. If no hosted path
   preserves released schema semantics, hold the migration or seek
   approval for the explicit schema and hash change.
   [Documented write constraints](https://docs.spiraldb.com/docs/services/tables/write-rows).
4. **Read back and compare.** Independently count every visible row on
   both exact versions, including the empty case. Compare the key sets
   and ensure one row per canonical composite key. Align batches by key
   and use the repository type matrix's exact field and bit-level
   comparison rules. Compare null validity, ordered schema and metadata,
   nested values and temporal integers, and compare floats by bits
   including NaN payloads and signed zero. Chunking and physical layout
   may differ, with only Orcapod's existing normalization allowed before
   the logical comparison. Compare record IDs, source-info tokens, data
   and context hashes, source content and pipeline hashes, and stored
   result paths using the preserved source name. Then load and run a
   representative saved job and prove result reuse. A new default name
   should change the source ID and provenance as designed. Verify deterministic
   sync order and prefix/limit behavior separately from equality by key.
   Async comparisons use sets of rows. Fail the cutover on any unexplained
   mismatch. Keep the restorable old artifact and the verified locator
   manifest until owners accept the copy. [Type-matrix oracle](../../../tests/test_databases/type_matrix.py).
5. **If Keyed reads stop early.** Prioritize permissioned exports and
   historical pins now. If reads end before an owner has a verified copy,
   restore from a validated Arrow export or backup with the original
   schema, KV and key/identity information. Ask Spiral for a supported
   recovery/export path if none is available. Keep the affected job
   unavailable and report which tables and pins cannot be recovered.
   A proxy, partial backup or guessed schema cannot substitute for data
   or justify reusing the old identity. Never promise history the service
   no longer retains.

### Independent PR and release gates

This alternative is **larger than Scenario 1**. It drops the need for
general Keyed write/retirement parity, but adds a new connector contract,
identity and saved-job migration, two kinds of reads during the transition,
and a user-controlled copy and verification workflow. Keep the switch
from `origin/main` and independent of ITL-1446 and the same six PRs. Review
it as one atomic default change if feasible. If the migration workflow
needs its own review, ship its read/export/verify tooling independently
first. It must leave the current default client unchanged. The switch
would include the following.

- A new Table connector for typed create, lookup/list/drop, scan, row-count
  and version handling, and upsert/skip-existing through a declared key.
  Test typed empty initialization, namespace pagination and missing/no-op
  semantics. Use `merge_rows(..., matched="ignore")` for skip-existing and
  prove the all-hit no-write behavior. Publish writes through branch
  commits. Verify merge behavior,
  non-null and unique keys, recomputation on known conflicts and recovery
  of unknown publication outcomes. Plain append is not upsert.
- Source path/ref configuration, verified rebinding of saved Keyed
  sources, and the time-limited read-only Keyed path. Preserve count
  preflight and old first-by-key semantics for migrated sources or get
  approval for explicit differences. New Table scans return stored order.
  Verify an exact bounded key-order read before promising former
  `head` and `limit` behavior. Confirm a conservative metadata count for
  `max_rows`, or document and approve a fail-closed difference.
- Fake/native conformance, new Table copy fixtures and the existing
  legacy identity oracle, SDK dependency and import guard, OIDC CI,
  platform/license checks and migration documentation. Release after
  native nullability and exact schema/value parity, read/write and
  conflict tests, and a successful saved-job migration through the
  live runner. A released source's schema or identity changes only
  after the developer agrees to the specific impact.

## Afterwards

ITL-1446 starts from the merged SDK switch. The former PR 5 belongs there.
No name mapping, store catalog, lease, batched lookup or new `__` bookkeeping
columns enter the switch. PR 542's exact-or-refuse, publication, recovery,
ordering and cost requirements and PR 545's pure codec and format tests
remain candidates for that follow-on. Re-evaluate physical name mapping
and the legacy KV manifest before fixing a new store format. The keyless
`Table` API has stored-order scans and caller-selected merge keys
(`tables.py:459–495,602–631`), and a project commit can publish several table
versions together (`projects.py:244–276,322–377`). Multi-table store
publication and its failure tests belong to the subsequent store work.
