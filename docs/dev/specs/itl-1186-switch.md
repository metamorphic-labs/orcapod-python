# ITL-1186: the switch to spiral-sdk — design

Status: superseded in part. After an independent design review the first step is narrowed
(November 24, 2026): **reading only**. The schema table is written by the migration tool
alone, one row per table keyed by SpiralDB's table id, one schema per table, and a completion
marker last (`src/orcapod/databases/spiraldb_schema_table.py` is the statement). The connector
refuses writes; sections 1 and 2 below (a row per write, `earlier_schemas`) are not built.
"Measured" means measured on the service with spiral-sdk 0.16.74 (and pyspiral 0.16.33 for
comparison) on throwaway tables.

## 1. The schema table

One keyed table per dataset, `__orcapod_schemas__` (reserved). Key: `table_name`.

| Column | Content |
|---|---|
| `table_name` (key), `table_id` | the table, and SpiralDB's id of it |
| `format` / `format_version` | `orcapod.table_schema` / 1; another value is refused |
| `nullability` | `recorded`, or `service` (metadata only: flags as the SDK reports them) |
| `schema` | Arrow IPC bytes of the newest schema: names, order, types, flags at every depth, field and schema metadata, extension identity in storage form |
| `earlier_schemas` | list of IPC bytes: the table's earlier column sets, oldest first |
| `key_columns`, `nullable_columns` | readable form (list, JSON of paths); must agree with `schema`, else refused |
| `written_at`, `written_by` | ISO UTC text; `orcapod <version>, spiral-sdk <version>` or the tool (no user, host) |

- The schema is the one Orcapod **presents today**: large variants, key columns in key order,
  then the others by name, fields of a struct (and of a struct in it) by name. Anything else
  would move hashes or row layout. The declared order can be a later format version.
- Deserialising brings a registered extension type to life; the reader puts every schema
  through `canonical_form.storage_form_schema`, so metadata stays what was written.
- Size: a 5 MB cell was written and read back (measured), so the 8 KiB metadata limit is gone.
- Reserved: lookup, source, create, write and delete of the name raise `ValueError`;
  `get_table_names` leaves it out. It has no row of its own; its columns are fixed and checked.
- **Cost.** Each lookup opens the schema table (0.1 to 0.2 s) and reuses the rows it read while
  the version is unchanged (a snapshot is immutable). A changed version costs one whole read,
  about 2 s. One cache per shared client, under a lock. Today's metadata read was 0.1 s.
  A dataset with no schema table costs one refused lookup (0.1 s).
- The table is opened **before** the row is read, so a row is never older than its table.
- **Versions.** The current row serves every version. A snapshot must equal exactly one listed
  column set, so a pinned read finds its own. Reading the schema table `asof` would lose every
  pin older than the migration.

## 2. Writes

1. Validate as today; refuse a column name holding NUL (silently cut: measured).
2. Stage the rows (`writer().append`): the client's own refusals come here, nothing written.
3. If the row must change, write it. 4. `finish()`. 5. If the row changed, compare the returned
   snapshot with it and raise on a difference, saying the rows are stored.

A snapshot holding only key columns was never written (the service refuses key-only writes):
its row, **of any id or content**, is ignored on read and replaced on the first write.

| Event | Result |
|---|---|
| killed or failed between create, row and first rows | key-only table; the leftover row is inert; the retry works. The `NullabilityRecordError` brick is gone |
| key-only table in `ConnectorArrowDatabase` | today refused for good (`Schema mismatch existing={__record_id}`). Proposed: the connector reports no columns yet through one method and the flush treats the table as new. Small; see Q3 |
| added nullable column, killed before the rows | the new set is listed, the table equals an earlier one: reads work |
| append that loosens a recorded column | refused (`NullabilityRecordMismatchError`), as today |
| changed metadata only | the row is replaced, put back if the rows fail, as today |
| non-empty table without a row | gets no nullability, as today; if the rows carry metadata, a `service` row (today's metadata entry) |
| two writers, different tables | two keys, independent |
| two writers, one table, equal schemas | equal rows; no harm |
| two writers, one table, different new columns | the rows race, last wins; the union is in neither row: the writer's check or the next read refuses, naming both. Nothing is mis-read. Repair: the tool |
| same data key, two writers | last wins, no conflict (measured), as today |
| lost reply of a commit | unknown outcome is said; an identical retry is safe |

No row is written at create time. Zero rows: validated, then nothing written (pyspiral
panicked). `exist_ok` with another key gives a bare 409: the connector compares the keys and says so.

## 3. Reads

- Row found, id equal, columns (names, types) equal one listed set: that schema. Nulls in a
  non-null path refused per batch, as now.
- Other id, extra, missing or differently typed column, unknown format, disagreeing readable
  form: refused, naming the table's and the row's side.
- No row: as the SDK reports it. **An Orcapod table not yet migrated therefore reads nullable
  and changes its pipeline and content hash, silently: migrate before upgrading.**
- "No schema table" is accepted only for the exact text `resource not found:
  <project>.<dataset>.__orcapod_schemas__`. 401, 404 of a project, transport failure: raised
  (measured: each has another text). Wrapped errors keep the SDK's text.

## 4. What does not change, and the exceptions

Unchanged: `table_<id>` (resource_id equals it: reported by the earlier live study, rechecked by the conformance run), so default source
ids; `SOURCE_VERSION` 1; record ids; hashes; schema; key order; `limit`, `where`, `asof`,
streaming, pinning; saved jobs and configs. The SDK's schema of pyspiral tables equals
pyspiral's, with zoned timestamps, times and timestamp/date keys (measured).

`where`: literals take the column's storage type (the SDK refuses a mismatch; `large_string`
becomes `string`). Results equal pyspiral's on 0.0, -0.0, NaN, bool, text, int, date32/64, zoned
and ns timestamps. A value a float32 column cannot hold exactly matches nothing in pyspiral but
matches after rounding in the SDK: the connector yields no row, as today.

Exceptions, all user-visible:
1. `delete_table` of an existing table is refused (section 5).
2. Row counts (section 5): exact, dearer, and unknown above a bound in a preview.
3. `overrides`: `server.url` maps to `api_url`; any other key is refused. `~/.spiral.toml` and
   `SPIRAL__*` are not read; sign-in is the SDK's.
4. New tables cannot have timestamp, date, fixed-size binary (or decimal) keys. Existing ones read.
5. An unmigrated table (section 3); a pinned version whose columns predate the migration.
6. Two processes by fork cannot share a client (hangs, measured); spawn works.

## 5. What the SDK lacks

| Need | Becomes |
|---|---|
| list | documented `GET /api/keyed-tables/projects/<p>/tables?dataset=`, all pages (also empty ones), token from the SDK client. One function; the token accessor is not public API, hence the exact pin |
| drop | `NotImplementedError` naming SpiralDB's own tools; absent table stays a no-op. Nothing in `src` calls it |
| rename | not offered today, not offered |
| count | a read of one key column. `row_count_estimate` is now exact but reads every key (1.3 s at 20,000 rows; grows). The `max_rows` check reads at most `max_rows + 1` keys (1 to 3 s) and remembers it per table version. `head()` counts at most as many; above that the total is unknown ("more than 100,000 rows") |
| key types | refused at create, naming the column |
| error bodies | status only; the connector adds operation and table as a note |
| threads, first call | 8 threads opening on a fresh client: 8 of 8 (one trial; opens, whereas the 7 of 8 pyspiral failures were first writes, not yet tried). A test; a lock around client creation only if it is ever seen |
| maps, null structs, zoned or extension columns, `__`, `.` | today's refusals kept, also where the SDK would store more |

## 6. Fake and tests

`tests/test_databases/fake_spiral_sdk.py`: `Client`, `Project`, `KeyedTable`, writer, scan,
expressions, HTTP list, `SpiralError`; every rule cites its evidence. `fake_pyspiral.py` goes.
One conformance module runs unchanged on the fake and, opt-in, the service. Live tests cannot
drop tables: they reuse fixed names, reset by `delete_keys` and column drops (documented in
the SDK, to be probed first); throwaway tables of my runs are dropped with pyspiral in its own
environment. Tests: every row of sections 2 to 5, many tables and columns, composite keys in
several orders, nested nullability, sync and async (sets), saved jobs from the pyspiral release,
crash at each write step, two writers, threads, early close, clean exit.

## 7. Packaging

`spiral-sdk==0.16.74`, exact: pre-alpha, six releases on one day, one private accessor. A bump
is a PR that reruns the live conformance. abi3 wheels cover 3.11 to 3.13 on macOS arm64 and
glibc Linux; no sdist. pyspiral leaves pyproject, lock, docs, probes' imports. A plain error
names pyspiral if it is installed instead.

CI sign-in: `SPIRAL_CLIENT_ID` and `SPIRAL_PRIVATE_KEY` work unchanged (measured as the workload);
no workflow edit needed except comments that name pyspiral (Q5). Licence `MIT AND
LicenseRef-Spiral-libspiral`: the licence job installs no extras so never sees it, and its
partial match would accept it for the "MIT": a decision, not a pass (Q5).

Telemetry: the SDK has a usage reporter, flushed at exit, with no documented off switch. One
keyed read through a proxy made 3 API calls, none to a usage route, also after a forced flush;
other hosts were not proxied, so "sends nothing" is **not** established.

## 8. Migration tool, run by the developer

`scripts/migrate_spiral_schemas.py`, three pinned commands, explicit project and datasets, no
login step; the principal found is printed first, and with no credentials it stops (the SDK
would start a device sign-in).

1. `dry-run` (`uv run --with` Orcapod's last pyspiral commit): reads schema, record and metadata
   with that Orcapod, records schema, content hash, pipeline hash, source id and table id per
   table into a local file; prints per dataset the count and table names; prints a digest.
2. `write --digest <d>` (SDK environment): recomputes the summary from the live tables and the
   schema table, refuses another digest, writes one row at a time, reading each back; stops at
   the first disagreement, leaving whole rows; equal rows are skipped, so a rerun finishes.
3. `verify` (SDK only): builds each source and compares with the file, every value.

It touches the schema table only and refuses what it cannot represent (broken record, record
naming absent columns, a row of another id or content). Tables with no record and no metadata
get no row. Tested on the throwaway project, with a run stopped half way and resumed.

## Questions

1. Branch: the issue's branch is PR 548's head. I propose `eywalker/itl-1186-spiral-sdk-switch`.
2. Counts as in section 5 (bounded check, unknown total in a preview)? Recommended.
3. Fix `ConnectorArrowDatabase` for a key-only table here? Recommended (one method).
4. Exact pin? Recommended.
5. May I edit only the comments of `run-tests.yml` that name pyspiral (else I leave them), and
   who decides the licence and the unproven telemetry?
