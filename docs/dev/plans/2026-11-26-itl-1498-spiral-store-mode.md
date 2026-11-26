# ITL-1498: the SpiralDB connector's store mode (stage 3)

Stage 3 of `docs/dev/specs/itl-1446-round2-spiral-sdk.md` (PR 588), stacked on PR 590
(ITL-1497, the codec) and PR 589 (ITL-1495, names and metadata). Q1 to Q4 approved as
recommended by the coordinator on the developer's behalf (2026-11-26); Q2 revises decision 4.
Implemented in `databases/table_names.py`, `databases/spiraldb_store_tables.py`, the
connector's `_store` flag, and the chunked manifest of `spiral_store.manifest_entries`.

## What the code does today

- `ConnectorArrowDatabase` owns the path-to-name rule (`_path_to_table_name`: sanitize, join
  with `__`), tests existence with `name in connector.get_table_names()`, and lists paths by
  splitting names back (`list_table_paths`). A connector sees table names only, never a path.
- Every read is a whole-table read (`SELECT * FROM "<name>"`, then filtered in Arrow); there
  is no keyed read.
- `SpiralDBConnector` reads only; every write raises `SpiralDBWriteNotSupportedError`.

## Proposal

**Q1. Naming moves into the connector.** `DBConnectorProtocol` gains three required methods
(protocols are internal, so no optional capability):

- `table_name(path) -> str`: the stored name of the table at a full path;
- `has_table(table_name) -> bool`: whether it exists;
- `table_paths(prefix, pending_names) -> list[path]`: the paths of the stored tables under
  `prefix`.

`ConnectorArrowDatabase` calls these instead of its own rule. SQLite, PostgreSQL and a plain
SpiralDB connector implement them with today's rule, moved unchanged into one shared helper,
so nothing they do changes. The other shape, a separate database class for SpiralDB that
copies `ConnectorArrowDatabase`'s pending, flush and guard machinery, is not recommended: two
copies of that machinery can drift.

**Store mode** (`SpiralDBConnector(project, dataset, _store=True)`):

- `table_name(path)`: `spiral_store_names.store_table_name`. The connector remembers each
  name's path. A name it has not computed is not resolved from the table's metadata: every
  other call on a name raises `KeyError` unless `table_name(path)` was called on the same
  connector first (as `ConnectorArrowDatabase` does), never with a name taken from another
  connector.
- `has_table(name)`: opens the table by name, with no listing. Its record is read and checked:
  another path or format raises `SpiralStoreTableCollisionError`.
- `table_paths(prefix)`: one listing, then the names starting with
  `store_table_name_prefix(prefix)`, then each candidate's metadata (cached by table id), then
  the recorded paths under `prefix`. Stale-layout checks use this unchanged (ITL-954).
- `create_table_if_not_exists`: checks the key column and that the name is one this connector
  computed; it creates no table (as built, after the review of fe4145c7: every check runs
  before a table is created).
- First non-empty `upsert_records` on a table: after every check of the rows, create the table
  (`create_keyed_table` with the key column; a 409 means it was made meanwhile and is judged
  again), or adopt the empty one found; write the identity record and the manifest together,
  in one metadata upsert; then append the rows. If that write stops part way, what can remain
  is an empty table with no record, or a recorded table with no rows: a later first writer
  adopts either one, since a table with no rows is new (N42). **Q4**: the data context recorded is the one of the rows' `__data_context`, which must
  be a single value. A table found without a record, and with no rows and the expected key, is
  adopted; otherwise it is refused (decision 3).
- `upsert_records`: rows through `spiral_store.encode_rows`; `skip_existing` reads the key
  column once. A write of no rows is never sent.
- `get_column_info` and `iter_batches`: the logical columns, rows through
  `spiral_store.decode_rows` against the manifest.
- Plain tables are not store tables: a store-mode connector sees only `o1_` tables.

**Q2. Where the manifest lives (revisits decision 4).** On main, the dataset's schema table
(`__orcapod_schemas__`) is all or nothing: one completion marker holds a digest of every
row, and a dataset whose marker does not fit its rows is refused for every read. Adding a
store table would mean reading the whole schema table and writing its row plus a new marker
(one append, so atomic), and every plain reader would parse store rows too. Recommended: keep
the manifest in the table's own metadata, in chunks of at most 8 KiB (PR 545's chunked format,
which PR 590 replaced with one record), beside the identity keys. Store tables then stay
independent of each other and of the schema table. Measured: there is no total metadata limit
in sight; reads slow down only past a megabyte.

**Q3. Store mode stays private until stage 5.** The flag is underscored, `to_config` refuses a
store-mode connector, and there is no lease yet (stage 4). Writes happen only in tests on the
fake and in the live conformance dataset.

## Measured (live, spiral-sdk 0.16.78)

- A key-range scan returns no row for an absent key, and rows in the order of the ranges
  asked: no null rows (ITL-1476's second finding does not apply to spiral-sdk).
- Table names: the store's names are at most 164 characters in `[a-z0-9_]`, within the
  service's 255 (ITL-1476's first finding).
- A write of no rows publishes nothing (ITL-1476's third finding); the store sends none.

## Not in this stage

The lease (stage 4), keyed reads in place of whole-table reads (a later performance stage),
and job integration with switch-on (stage 5).
