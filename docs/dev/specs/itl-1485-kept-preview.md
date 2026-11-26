# ITL-1485: a source keeps its preview

Developer, November 26, 2026: "sources should actually cache their preview and use the
system very much like that of the stream's staleness check to see if their preview is stale
OR if user is requesting a preview that cannot be achieved with what's been cached, ONLY
THEN it should bust the cache and stream again (minimally) from the source to form the
preview."

## What a preview reads today

Only two kinds of source read anything for `head()` (and the notebook preview, which is
`head()`): `SpiralDBTableSource`, and a `DerivedSource` over a job node (its stored rows,
read whole and not kept). Every other source holds its rows (a slice), delegates
(`SourceProxy`, a `DerivedSource` over a source, a source job node) or reads nothing
(`PollingSource` before a poll, `CachedSource` before its first read).

A SpiralDB source's `head()` on `main`, every time, on the fake: open the table by name,
open the dataset's schema table, the snapshot's `schema()`, one scan limited to `n` rows;
the first one also counts the keys (the count is then remembered per version by the
connector). A preview of an unchanged table repeats all of it.

What the source already knows for free: it reads one pinned version (`_asof`), set when it
is built or by a given `asof`, and moved only by `check_for_new_data` (a run's source check).
Between two moves every read returns the rows of one immutable snapshot. So whether a kept
preview is still the source's preview needs no call at all: it is if it was read at the
version, restriction and table the source reads now.

## The design

1. **What is kept.** One record on the source object: the rows of the last preview that
   read rows, with every column group (tags, data, system tags, source info, context), as
   one stream; the number of rows the read asked for and whether it returned fewer (then it
   holds every row the source reads); the total and counted bound the header shows; and the
   key it was read under: the pinned version, the condition, the limit.
2. **When it is used.** `head(n)` with the same key, and `n` at most the rows it holds or
   the record holds every row. Any `columns` / `all_info` / `sort_by_tags` is answered from
   it (they are projections of the kept columns; sorting sorts the first `n` rows read, as
   on `main`). No service call is made. The view is the one `main` returns.
3. **When it reads again.** Another key (a run's check moved the version; a `where()` copy
   starts with none), or more rows than it holds. It then reads `n` rows from the start, as
   on `main`, and the new record replaces the old one.
4. **"Changed" per kind.** SpiralDB: the pinned version, held in memory; the check costs
   nothing. A live change of the table is seen only after a run's check moves the version,
   exactly as `main`'s preview (which reads the pinned version) sees it. Sources that hold
   their rows: nothing to keep. `PollingSource`: unchanged, never polls.
5. **Restriction.** The key holds the condition and the limit: kept rows belong to one
   restriction. `where()` returns a copy, which keeps nothing.
6. **A job.** A run's source check that takes in a new version moves the pin, so the next
   preview reads. A run that finds nothing leaves the kept preview in use. Rebinding binds
   another source object, with its own record; nothing is kept on a node or a slot.
7. **No identity changes.** Nothing kept enters a hash, a config, a stored path or a
   stored table.

## Decisions

Approved as recommended by the coordinator on the developer's behalf (November 26,
2026).


- **D1. Scope: SpiralDB only.** Recommended. A `DerivedSource` over a job node also reads
  on every preview (its whole stored table); its change signal would be the node's modified
  time, which misses another process writing the same store. Follow-up issue, not this PR.
- **D2. Reading more rows: read `n` from the start, not only the missing ones.**
  Recommended. A SpiralDB scan has no offset, and "after this key" over a composite key
  needs an `or`/`>` predicate the source does not build (the `where()` path is being
  changed by ITL-998). The second read costs what `main` pays for every preview.
- **D3. Memory bound: keep a preview of at most 10,000 rows and 64 MiB** (the Arrow
  buffers of the kept table, every column group); a larger `head(n)` is read and shown,
  not kept. The default preview is 10 rows. Rows can be wide (a list, binary or library
  value column holds any number of bytes per row), so the row bound alone does not bound
  memory: 10,000 rows of a 1 MB array column are 10 GB. The byte bound does. It counts
  every buffer the kept table references (`get_total_buffer_size`), not its values'
  bytes: a scan's limit slices a batch, and a slice keeps the whole batch's buffers, so
  rows that hold more than their values are first copied out (review of af670090).
- **D4. A closed source keeps nothing.** `close()` drops the record, so a closed source's
  `head()` raises as on `main`. Recommended.
- **D5. A table dropped and created again, or a schema-table change, is not seen by a kept
  preview** (on `main` the preview's lookup raises). It is seen by the next run's check,
  read or uncached preview. Consistent with `main`: a preview already shows the version
  the source last checked (the version moves only by a run's source check). The docs say
  so: a preview shows the version the source last checked; run the job (or check the
  source) to see newer rows.
- **D6. Full reads (`as_table`, `iter_data`, `run()`) neither use nor fill the record.**
  Recommended: they keep `main`'s "every read scans again"; a run's rows are a job's
  business. The computed `preview(n)` of ITL-1464 (#571, not merged) can answer a source's
  preview from the record later.
- **D7. The total is kept with the rows** (amended after the review of af670090; decided
  on the developer's behalf: "a repeated preview of an unchanged source takes no time").
  The read that fills the record also counts the table's keys whenever a preview answered
  from it shows the table's row count (its rows are not every row the source reads, or a
  condition chose them), at the same version and through the same handle, to the largest
  bound such a preview counts to. Every preview the record answers then makes no call,
  its total included. The one call `main` does not make: a preview whose condition
  returned every matching row also counts, once per version (the connector remembers it).
  That count is for later previews only (review of 854bbf94): it is made only when the
  rows will be kept, never delays or fails the preview it follows (its total is exact),
  and when it fails nothing is kept. A record is kept only while the source still reads
  its version, condition and limit, so a late read of an older version never replaces a
  newer record. Rows are copied out only when they are a real slice (their buffers hold
  over 1 MiB beyond their values), after the row bound is checked.
- **One version, one read, a closed source** (review of af670090). The pinned version
  is read once per preview: the rows, the count and the record's key are all of it, so a
  source check that moves the version during a preview neither mixes versions nor keeps
  rows under the new one. A source closed during a preview keeps nothing, and a closed
  source's preview raises before it looks at a record.

## Tests (fake SpiralDB, by SDK call counts)

A second preview makes no service call; `head(5)` after `head(10)` none; other columns and
`sort_by_tags` none; `head(20)` after `head(10)` reads 20; a table written to and a run's
check (sync `job.run()` and `run_async()`) make the next preview read the new rows; a run
that finds nothing does not; a `where()` copy reads its own; a given `asof` keeps its
record; `close()` drops it; a 3-row table answers any `n`; a preview over 10,000 rows is not
kept; every view equals `main`'s for the same arguments.
