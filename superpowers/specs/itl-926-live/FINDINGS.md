# ITL-926 — Phase 1 findings (November 11, 2026)

Audit of `SpiralDBTableSource` / `DBTableSource` / `SpiralDBConnector`, an end-to-end run of
the workshop path through a disk-backed pyspiral fake (`tests/test_databases/fake_pyspiral.py`),
a pyspiral 0.14.9 → 0.16.33 API check, and one read-only live call against
`edi-sample-v1-855632`. Repro scripts: `superpowers/specs/itl-926-live/` and the session
scratchpad (listed in the plan).

## Live facts (EDI sample V1, read-only)

* Schema dump (developer): **every column is nullable, key columns included** (`utf8?`,
  `u32?`, `vortex.timestamp[µs](i64?)`). Keys are composite: string hashes, `subject_id`,
  `session_start_datetime` (timestamp µs), `u32` indexes. Value types include `u32`/`u64`,
  `f32`, `list(...)`, `fixed_size_list(...)[n]`, structs with nested lists (`unit`),
  `binary` (`frame_jpeg`), `date[days]`.
* pyspiral **0.16.33** (overlay env, repo code unchanged) authenticates with the developer's
  login, lists the 27 tables in 1.2 s and scans `atlas` (limit 3) in 1.2 s.
* pyspiral **0.14.9** (the repo lock) does **not** use that login: it started a device-login
  flow, which was aborted at once.
* A second read-only probe was blocked by the Claude Code permission classifier in this
  terminal, so the per-table Arrow types from a scan are still unknown (in particular the
  Arrow type and timezone of `vortex.timestamp`).

## Findings by severity

### Critical: workshop blockers

1. **All-nullable columns make every function pod with a plain type fail.** With the EDI
   schemas every tag and data column is `T | None`. `@function_pod def f(x: float)` on such a
   stream raises `ValueError: ... column 'x' has type float | None, the function expects float`.
   Pods annotated `x: float | None` work. Repro: `e2e/edi_sim.py`.
2. **`fixed_size_list` columns crash source construction.** `starfix` 0.4.0 (latest) raises
   `NotImplementedError: Unsupported leaf type: fixed_size_list<...>` from
   `arrow_hasher.hash_table`. General: `ArrowTableSource` fails the same way. EDI tables hit:
   `experiment_display_params`, `experiment_freeviewing_params`,
   `experiment_screen_capture_event`, `lfp_timeseries`.
3. **Large tables cannot be used.** The source reads the whole table eagerly and
   `SourceStreamBuilder` calls `table.to_pylist()` for provenance tokens. Measured: 200k
   spike_event-shaped rows take 0.9 s and add about 210 MB of peak RSS for 8.6 MB of Arrow
   data (about 25×). There is no column projection, row filter or limit on the source.
   `spike_event`, `lfp_timeseries`, `experiment_gaze_online_timeseries` and
   `experiment_screen_capture_frame_270p` are at risk.
4. **pyspiral pin vs. login.** See "pyspiral" below.

### High

5. **Naive timestamp tags become UTC on one read path.** `FunctionJobNode.as_table()` returns
   a `timestamp[us]` tag as `timestamp[us, tz=UTC]`; `get_all_records()` and an in-memory pod
   keep `timestamp[us]`. General (not Spiral); repro `e2e/tz_repro.py`. EDI keys include
   `session_start_datetime`.
6. **Record identity is positional by default.** Inserting a row whose key sorts first
   re-keys every later row, giving duplicate pipeline entries; function results are still
   cached (one call). `record_id_column` takes one column only, so a composite primary key
   cannot be used, and a non-unique column is accepted silently, giving two rows the same
   record ID (`comp::s=a` twice).
7. **Reading a loaded job.** After `PipelineJob.load(path, store=...)` in a fresh process,
   `nodes[label].as_table()` raises `FunctionJobNode ... unavailable: no function pod and no
   database attached`; `get_all_records()` works. General (CSV control repro).
8. **`get_all_records()` after the source data changed** returns the previous run's rows (3)
   while `as_table()` shows 4. General; needs triage.

### Medium

9. **Timezone:** SpiralDB drops the timezone of `timestamp[tz]` columns (seen live earlier).
   It cannot be fixed in Orcapod for tables written outside it; document it.
10. **Logical (extension) types do not survive a Spiral source.** An Orcapod `Path` column
    comes back as `large_string`: the connector's KV metadata (ITL-471) stores
    `field.metadata` only (the extension lives in the type), and `DBTableSource` rebuilds
    fields without metadata. The EDI tables have none.
11. **ITL-40:** the asymmetry is fixed at top level (`get_column_info` and `iter_batches`
    both map `string`/`binary` to the large variants). Nested types (`list<string>`,
    struct children) are normalized by neither, which breaks the "normalize recursively"
    rule; hashes are unaffected. Mechanical fix: `arrow_utils.normalize_to_large_types`
    in both.
12. **Saved source config narrows integer widths:** `tag_schema` / `data_schema` in
    `to_config()` go through Python types, so an `int32`/`uint32` key is saved as `int64`,
    and a `SourceProxy` fallback gets the wrong width. General.
13. **`source_id` defaults to the bare table name.** Same-named tables in two projects or
    datasets (for example, two EDI sample projects) get the same `source_id`, and therefore
    the same record IDs and provenance tokens.
14. **Docs:** credentials live at `typer.get_app_dir("pyspiral")/auth.json`
    (`~/Library/Application Support/pyspiral/` on macOS), not `~/.config/pyspiral/` as the
    docstrings say.

### Low

15. `decimal128` and dictionary columns map to `typing.Any` (a general converter gap; the EDI
    tables have none). Spiral refuses struct-level nulls on write (write side only).

## Verified working (fake; same code path as live)

* `SpiralDBTableSource` with a composite key → `Join` (two Spiral tables on shared string,
  timestamp and `u32` keys; or one Spiral table with a `DictSource`) → function pod →
  `DeltaTableDatabase`. A re-run makes 0 function calls.
* `content_hash` and `pipeline_hash` are identical across processes. System-tag and
  source-info columns are present and correctly named (`<source_id>::row_<i>::<col>`).
* `save` → `PipelineJob.load` in a fresh process: the Spiral source is rebuilt from config
  (a `DictSource` comes back unbound, as designed), and `get_all_records()` returns every row.
* Zero rows and a missing key schema give clear errors. A Join on mismatched key widths
  (`int32` vs `int64`) gives a clear `InputValidationError`.

## pyspiral

* Every symbol and signature the connector uses is identical in 0.14.9 and 0.16.33 on
  Python 3.11, 3.12 and 3.13, except `Table.key_schema`, which now returns a Spiral `Schema`;
  the connector uses only `.names`, which works.
* `uv lock --upgrade-package pyspiral==0.16.33` moves only pyspiral. The Spiral unit tests
  pass on 0.16.33.
* Attendees installing `orcapod[spiraldb]` get 0.16.33 anyway (the floor is `>=0.14.0`).
* 0.14.9 does not use the login written by the 0.16.33 CLI.

Recommendation: move the lock to 0.16.33. Keep the floor at `>=0.14.0`, or raise it to
`>=0.16`.

## Integration tests vs. the workshop path

`tests/test_databases/test_spiraldb_connector_integration.py` covers the connector and
`ConnectorArrowDatabase` round trips plus ITL-471 metadata. It does not cover
`SpiralDBTableSource`, `PipelineJob`, `Join`, caching, save/load, nullability, composite keys,
timestamp keys, or large tables. The `spiral-integration` job needs the `SPIRAL_WORKLOAD_ID`
secret (GitHub OIDC), `vars.SPIRAL_PROJECT_ID` and `vars.SPIRAL_SERVER_URL`
(default `http://api.spiraldb.dev`), and runs on push and `workflow_dispatch` only.
