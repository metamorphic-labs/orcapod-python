# ITL-1367 step 2: a canonical column order for a new stored table

Status: ITL-1367 is merged and ITL-1370 implements canonical writes and reader
layouts. The developer approved the exact stored order in PR #496 on 2026-11-22
and confirmed it on 2026-11-24 and 2026-11-27.
The reader layouts are listed in `CLAUDE.md` under “Stored column order”.

The developer's decision (2026-11-22): "We should always try to save to match the existing
database order if already present, but if not yet, then use canonicalized order for new
database." Step 1 implements the first half. ITL-1370 implements the second half.

## What main wrote when the order was approved

Measured with `docs/dev/scripts/itl_1367_stored_column_orders.py`: a job over a source with
two tags declared `z_tag, a_tag` and two data columns `y, b`, running a function pod, a
logging `Join` node, a cached source and a side-effect pod. The order of each table's first
write (system-tag hashes shortened). ITL-1471 subsequently moved the function pipeline,
side-effect log and operator cache to `pdb_v2`, `tdb_v2` and `odb_v2`. The approved order
applies to the columns of those layouts as well.

| Table kind | Layout | First-write order at the time |
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

The canonical order below matches the order SpiralDB reads for a table with one
record id key.

## The approved order

**By default, a new table is written with the record id column first and every other
column sorted by name in code-point order.**

A new table keeps its entire first-write order if any column at any depth has a type
the append comparison cannot handle (a dictionary-encoded type, a view type, a list
view, a union, an interval, or a run-end-encoded type), a column name repeats, or the
record id column is absent. Matching a later append by name requires the comparison
to confirm every field. Reordering even part of such a table could leave the writer's
own next append unmatched. Delta normalizes view types before checking the schema.
ITL-1529 tracks the follow-up.

- **Where:** in the database, in the shared step of step 1
  (`databases/utils.in_held_column_order`): with no held table (`held is None`), it returns
  the rows in the canonical order when the schema qualifies. The shared step applies the
  same rule to every table kind.
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

The rule applies to qualifying tables of every kind **created** after the change in the
in-memory, Delta, SQLite and PostgreSQL databases. A qualifying function result table
starts with the record id, followed by `__data_context`, `__input_data_hash`,
`__pf_exec_…`, `__pf_var_…`, `__pod_ts`, `_source_c_out`, `_source_w_out`, `c_out`,
and `w_out`. SpiralDB already reads single-key tables in that order.

A table **already stored** keeps its order: step 1 puts every later append in the held
order. Nothing is rewritten.

## What else changes

- **Readers that present stored rows in the stored order.** An operator node puts a stored
  read in row order (`_in_row_order_on_read`). Others present the stored order as it is:
  `CachedSource` (its stream is the cache read), `DerivedSource` (a node's stored records),
  a function node's stored view and anything that calls `get_all_records`. ITL-1370 uses
  one helper to apply each reader's presentation order (see “Stored column order” in
  `CLAUDE.md`). This gives readers the same presentation for tables created before and
  after the change, including SpiralDB tables.
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
- **Layout versions:** no change. A layout version moves when a table kind's
  columns are added, renamed or retyped (CLAUDE.md, "Stored tables"), because a reader of
  the old layout would misread the new one. Here no column changes, every read is by name,
  and step 1 lets old and new tables take appends in any order. A table written in the old
  order is read as current, as it would be by a SpiralDB read today.
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
