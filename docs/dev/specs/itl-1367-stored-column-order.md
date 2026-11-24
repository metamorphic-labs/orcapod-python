# ITL-1367 step 2 (proposal): a canonical column order for a new stored table

Status: ITL-1367 is merged and ITL-1370 implements canonical writes and reader
layouts. The exact stored order is awaiting developer approval in PR #496.
The reader layouts are listed in `CLAUDE.md` under “Stored column order”.

The developer's decision (2026-11-22): "We should always try to save to match the existing
database order if already present, but if not yet, then use canonicalized order for new
database." Step 1 is the first half. This document proposes the second half.

## What main writes today

Measured with `docs/dev/scripts/itl_1367_stored_column_orders.py`: a job over a source with
two tags declared `z_tag, a_tag` and two data columns `y, b`, running a function pod, a
logging `Join` node, a cached source and a side-effect pod. The order of each table's first
write (system-tag hashes shortened):

| Table kind | Layout | Order written today |
|---|---|---|
| Function pipeline | `pdb_v1` | `__record_id`, `z_tag`, `a_tag`, `_tag_source_id::…`, `_tag_record_id::…`, `_source_y`, `_source_b`, `__data_id`, `__node_content_hash`, `__input_data_hash`, `__output_data_hash`, `__data_context`, `__computed`, `__is_ephemeral`, `__pipeline_base_entry_id`, `__pipeline_recomputation_index` |
| Function result | `rdb_v1` | `__record_id`, `__input_data_hash`, `__pf_var_*` (6), `__pf_exec_*` (4), `w_out`, `c_out`, `_source_w_out`, `_source_c_out`, `__pod_ts`, `__data_context` |
| Side-effect log | `tdb_v1` | `__record_id`, `record_id_hash`, `pipeline_run_id`, `executed_at`, `__data_context` |
| Operator cache | `odb_v1` | `_record_hash`, `z_tag`, `a_tag`, system tags (input 0, then input 1), `y`, `b`, `x`, `e`, `_source_y`, `_source_b`, `_source_x`, `_source_e`, `__node_content_hash`, `__data_context` |
| Cached source | `sdb_v1` | `_record_hash`, `z_tag`, `a_tag`, `y`, `b`, system tags, `_source_y`, `_source_b`, `__data_context` |

(The record id column is renamed to `__record_id` inside every database; `_record_hash`
is the caller's name for it.)

Two things follow. The user columns follow the declared tag order and the table layout
(`z_tag` before `a_tag`), which the rule that order never counts (ITL-1271) allows only as
presentation; and the kinds disagree with each other (the cached source puts data before
the system tags, the operator cache after). SpiralDB stores none of these orders: it keeps
the key column, then the other columns by name.

## SpiralDB already imposes an order

Measured live (2026-11-22, workload account, throwaway tables, dropped): SpiralDB accepts a
write in any column order, matches it by name, and stores and reads the table as the key
columns in key-schema order, then the value columns sorted by name by code point (`B`,
`C`, `a`), whatever the order written. A later append in another order changes nothing; an
added column takes its place by name. Struct children are sorted by name too.

The evidence, two live sessions as the agent workload on the test project, every table a
throwaway one dropped at the end (the final listing showed none left). Each probe wrote
with pyspiral's `Table.write` (or `SpiralDBConnector.upsert_records`), then read the
table's schema (`Table.schema()`) and its rows (`SELECT *` through the connector):

| Probe | Written | Schema and rows read back |
|---|---|---|
| 1. one key `k`, then appends in other orders | first `k, z, a, m`; then `m, k, a, z`; then `b, k, z, a, m` (adds `b`) | `k, a, m, z` after the first write and after the reordered append, values by name (`z` = 1 and 11, `a` = 2 and 12); `k, a, b, m, z` after the append that adds `b` |
| 2. names in mixed case | `k, B, a, C` | `k, B, C, a`: code point order, uppercase first |
| 3. composite key declared `(k2, k1)` | `y, k1, b, k2` | `k2, k1, b, y`: the keys in key-schema order (not by name), then the values by name |
| 4. a struct column | `s: struct<z, a>`, then an append with `s: struct<a, z>` | `s: struct<a, z>` both times; values by child name (`a` = 2 and 12, `z` = 1 and 11) |

The first session also wrote `__record_id, a, c, b` and then, through the connector's own
`upsert_records` with no reordering by Orcapod, `b, c, __record_id, a`: accepted, read back
as `__record_id, a, b, c` with every value under its own name. Through
`ConnectorArrowDatabase` with step 1, three orders read back the same way.

So an order Orcapod chooses cannot be what SpiralDB physically stores. "Canonical" can mean
two things per database:

- **the order Orcapod writes** (the order of the first `add_records` that creates the
  table): in-memory, Delta, SQLite and PostgreSQL store it as written, so for them it is
  also the order a read returns;
- **the order a read returns**: for SpiralDB, always SpiralDB's own.

The proposal below makes the two meanings coincide in all five databases.

## The proposal

**A new table's columns are written in this order: the record id column, then every other
column sorted by name, by code point.** No other rule.

- **Where:** in the database, in the shared step of step 1
  (`databases/utils.in_held_column_order`): with no held table (`held is None`), it returns
  the rows in the canonical order instead of as given. Its callers do not change. The
  database is the one place that sees every write of every table kind, so no table kind can
  be forgotten, and a future table kind gets the rule without doing anything.
- **Why not role groups (record id, user tags, system tags, data, source info, meta):** a
  database does not know a tag from a data column, so a role-grouped order can only be
  produced by each caller (five table kinds, each building its table its own way, plus every
  future one). Within a group the order would still have to be by name, since the declared
  tag order may not count. Prefix groups that a database could see (`_tag_`, `_source_`,
  `__`) do not separate user tags from data. Sorting every column by name is the one order
  every database can apply alone, and it is SpiralDB's order (the record id is SpiralDB's
  only key column in every Orcapod table), so all five databases then store and read the
  same order.
- **Code point:** Python's `sorted` on `str`, the order SpiralDB was measured to use
  (uppercase before `_` before lowercase). Names that differ in case only are one
  column to SQLite, which resolves names without case; the PostgreSQL connector quotes
  every name, so it should keep them apart (not measured); the in-memory and Delta
  databases keep them apart. Code point order is total, so the order is decided either
  way.

## Which stored tables change

Every table **created** after the change, of every kind in the table above, in the
in-memory, Delta, SQLite and PostgreSQL databases: each is written in an order no table
kind uses today (the record id, then for example `__data_context`, `__input_data_hash`,
`__pf_exec_…`, `__pf_var_…`, `__pod_ts`, `_source_c_out`, `_source_w_out`, `c_out`,
`w_out` for a function result table). SpiralDB tables change in nothing: they are read in
that order already.

A table **already stored** keeps its order: step 1 puts every later append in the held
order. Nothing is rewritten.

## What else changes

- **Readers that present stored rows in the stored order.** An operator node puts a stored
  read in row order (`_in_row_order_on_read`). Others present the stored order as it is:
  `CachedSource` (its stream is the cache read), `DerivedSource` (a node's stored records),
  a function node's stored view and anything that calls `get_all_records`. Under the
  proposal their column order would change (presentation only, but visible, and different
  for a table created before the change and one created after). The proposal therefore
  includes one helper that puts a stored read in row order (user tags, system tags, data,
  source info; CLAUDE.md, "Important implementation details"), used by every reader, so
  what a reader presents never depends on the stored order. On SpiralDB this is already
  needed today.
- **The ITL-1347 differential** (`docs/dev/scripts/itl_1347/`, on the job-speed branch):
  its `exact` mode compares stored tables column order included
  (`compare.py`, `_schema_differences`: `column order: …`). A run of the branch against
  pinned main would report every stored table created in the run. It would need stored
  tables compared with columns matched by name (the `set` mode's `select(sorted(...))`, rows
  still compared in order), while node tables (what a node keeps and presents) keep their
  column order comparison, once the readers above present row order.
- **Golden values and paths:** none should move. No hash reads a stored table's column
  order: a schema enters an identity through `hash_schema`, which ignores column order
  (ITL-640); a row's hashes are built from its values by name; a path is built from
  identities, never from a stored table's layout; record ids are built before a row is
  written. The golden check (`generate_golden_hashes.py --check`) and the differential's
  `set` mode are the evidence to collect when it is implemented.
- **Layout versions:** recommended none. A layout version moves when a table kind's
  columns are added, renamed or retyped (CLAUDE.md, "Stored tables"), because a reader of
  the old layout would misread the new one. Here no column changes, every read is by name,
  and step 1 lets old and new tables take appends in any order. A table written in the old
  order is read as current, as it would be by a SpiralDB read today. The developer may
  still prefer a new marker so that a table's order tells its generation; it would move
  every stored path, which nothing else here needs.
- **Data function type snapshots** (`src/orcapod/data_function_types/`) list a type's
  record columns; the list is the declaration, not the stored order, and does not change.

## Struct fields (input for ITL-1368)

The same rule extends to struct fields at every depth: a new table's struct columns are
written with their children sorted by name (code point), recursively through lists, maps
and structs, which is again what SpiralDB was measured to store. An append would match a
struct's children by name, as step 1 matches top-level columns, so the recursive step
replaces `in_held_column_order` without changing its callers. `hash_table` and
`hash_schema` already leave struct field order out (`hashing/arrow_hashers.py`), but
`hash_table_ordered` (the pandas value hash, `arrow_ordered_v1`) includes it, and a struct
value may reach the semantic hasher in field order: ITL-1368 has to establish first
whether any hash depends on it, and a struct whose fields are reordered on write must read
back, and hash, as its type declares.

## Decision needed

1. Adopt the canonical order (record id, then every column by name, code point) for new
   tables, in the database's shared step? Or a role-grouped order built by each caller?
2. Readers put stored reads in row order (one helper), so presentation never follows the
   stored order?
3. No layout version change?
