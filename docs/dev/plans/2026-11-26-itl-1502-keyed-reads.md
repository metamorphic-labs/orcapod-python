# ITL-1502: keyed reads, one existence answer per run, one listing per run

Stacked on PR 595 (ITL-1498). Decided on the developer's behalf (N43): the three parts below;
Q1 approved as `run_scope()` on the protocols (N44: re-entrant, one held state until the
outermost scope exits, dropped also when the run raises).
The gate: `docs/dev/scripts/itl_1446_store_evaluation.py` compares every hash, row and record
id with Delta, on the fake and live, exactly as at 0a4189c3.

## What the job reads today (traced on the fake)

| Run | `get_all_records` | `get_records_with_column_value` | `get_record_by_id` | `list_table_paths` |
|---|---|---|---|---|
| first | 33 | 60 | 3 | 5 |
| unchanged rerun | 5 | 0 | 3 | 3 |
| two rows added | 13 | 16 | 3 | 5 |

`ConnectorArrowDatabase` answers every one of them by reading the whole table and filtering in
Arrow. `get_records_with_column_value` filters on value columns (a result's input hash, a
pipeline entry's base id or node hash, the data context), not on the key; `get_record_by_id`
and `get_records_by_ids` filter on the key.

## 1. Reads narrowed at the scan

- `DBConnectorProtocol` gets one more required method:
  `read_candidates(table_name, *, keys=None, equal=None) -> pa.Table | None`. It returns the
  table's rows that may match: at least every row whose key is in `keys` and whose columns
  equal `equal`. It returns `None` when the table is absent.
- `ConnectorArrowDatabase` still applies its exact Arrow filter to what comes back, so no
  result can change; a connector may only read less.
- SQLite, PostgreSQL and a plain SpiralDB connector read the whole table, as today.
- A store scans with one key range per asked key, and with a pushed-down equality filter on
  the stored names of the string, binary, integer and boolean columns. Floats, temporal and
  nested columns are not pushed down; the Arrow filter applies to them.
- `get_all_records` is unchanged: it means every row.
- `skip_existing` reads only the keys being written (key ranges), not the whole key column.

## 2. One existence answer per table per run

- An existence map is kept per store connector, per run: in one place, `StoreTables`.
- A run's scope, opened by `job.run`, `run_async` and a job node's `run`, starts it; leaving
  the scope drops it.
- **Q1**: the scope needs a run boundary that databases do not get today. Proposed: a
  required `run_scope()` context manager on `ArrowDatabaseProtocol`.
  - It is a no-op for in-memory, Delta and no-op databases, and for the test doubles.
  - `ExtensionAwareDatabase` and `ConnectorArrowDatabase` pass it to their backend or
    connector; a store connector's opens the map.
  - The job enters it once per distinct database of the run.
  - Outside a run (a database used alone), nothing is held: today's behaviour (N42).
- Within a run, the map records:
  - a table found, as present;
  - a table this process creates, as present;
  - a table found absent, as absent until this process creates it.
- A listing never records an absent table.
- Docstring: a second writer is not supported until the lease (O37); with one, an absent
  answer may be stale for the rest of the run.

## 3. One listing per run

The store's dataset listing is held in the same run map: one listing per run. It gains the
tables this process creates during the run. Outside a run, each `list_table_paths` lists the
dataset, as today.

## Tests

- Call-count tests on the fake:
  - a lookup by key and by value reads no whole table;
  - `skip_existing` reads only its keys;
  - one opening by name per absent table per run;
  - one listing per run;
  - nothing is held after the run.
- Equality of results: the existing suites and the evaluation, fake and live, before and
  after.
