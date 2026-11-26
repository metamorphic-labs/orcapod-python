# Computation that never holds everything in memory (ITL-1464, phase 2: design)

Status: draft for approval, November 25, 2026. Written against `origin/main` 0d1e017c. The
measurements it rests on are in `itl-1464-memory-diagnosis.md`. Decisions the developer
has made are marked **Decided**; decisions still open are marked **OPEN** and listed again
at the end.

Tables whose rows hold long sample arrays are out of scope (developer, November 25, 2026):
a session of them is a different problem. Nothing here is designed or tested for them.

## The governing rules

- **No hash, identity, stored layout or golden value changes.** Everything here is about
  what is held in memory and when. If a step turns out to need such a change, it stops
  and is asked about.
- **Sync results stay exact and deterministic**: every value, type, row order, stored row
  and error of the sync path stays as it is today (the rule of ITL-1347 and ITL-1441).
- **Async results equal the sync results as a set of rows** (each row as often as it
  occurs); row order is not a result on the async path (developer, November 22, 2026).
- **The async path is the primary memory-bounded path** (developer, November 25, 2026).
  The sync path gets what it can get without giving up its order.
- **Fewest special cases.** One mechanism for rows that are not in memory (the local
  cache, below), used by every part that needs one.

## What the diagnosis found, in one paragraph

A row of the measured table is 120 bytes. Held as a `(Tag, Data)` pair it costs 18 to
110 KiB; the async path passes rows that way and every operator job node keeps all of
them. Its source-info tokens are 4.2 KiB (35 times the data), built at every source
read. Every sync operator holds its whole input and whole output, tokens included. So the
design has three parts, in this order of worth: rows move and are kept as Arrow record
batches, never as objects; a node does not keep its rows in memory; tokens are not
materialized per row where they can be derived.

## 1. The `run()` contract

`run()` is the way to trigger computation (developer, November 24, 2026). It promises:

- every row of the stream is computed, with every effect and every write a job makes;
- on the sync path, in the deterministic order of today;
- it returns a `StreamView`.

It promises nothing about how the rows are produced or what is held meanwhile: not one
table, not batches of a given size, not a thread. "Does exactly the work `as_table()`
does" (ITL-1081) is replaced by "computes every row; `as_table()` afterwards does not
compute them again" (section 3 says how).

`as_table()`, `as_polars_df()` and iteration stay the way to get all rows, and
`as_table()` still returns one Arrow table: asking for it is asking for the rows in
memory. `head()` and the notebook display still never compute.

## 2. The local cache

One mechanism holds rows that are computed but not in memory.

- **What it is.** Arrow IPC files (the file format, uncompressed, so they can be
  memory-mapped), one per stream of rows, written a record batch at a time as the rows
  are produced, in the form the rows have on a job's edges (ITL-1347's row-built form).
  A row is addressed by `(batch number, row number)`.
- **Where.** A directory per process run, `<cache root>/orcapod-<pid>-<random>/`. The
  cache root is `OrcapodConfig.local_cache_dir`, by default the system's temporary
  directory. **OPEN (L1)**: the default location, and whether a job's cache may live
  beside the job's database.
- **The cache root must be on disk.** The memory promise of section 8 holds only when the
  cache root is on storage backed by a disk. On a memory-backed file system (`tmpfs` or
  `ramfs`, which some Linux systems mount as the temporary directory) a cache file's
  bytes are memory however the file is read, and dropping mapped pages frees nothing: the
  job then holds every cached row in memory, as before this design. Proposed (part of
  L1): the default stays the system's temporary directory; where Orcapod can tell that
  the cache root is memory-backed (Linux: the file system type of the directory), it
  issues one warning per process naming the directory and `local_cache_dir`, and the
  stated bound then does not apply. It never moves the cache to another place on its own.
- **Reading.** A reader memory-maps the file. Pages of a mapped file are the operating
  system's to drop under pressure, so they are not memory the process holds: this is
  what makes "fetch the row by reference, emit it, let it go" cheap. Rows are fetched in
  batches of references (`take` on the mapped batches), never one by one.
- **Lifetime.** A file belongs to the object that wrote it (an operator's run, a job
  node's current run, a stream's computed result) and is deleted when that object lets
  go of it: an operator when it finishes or fails (`finally`), a node at
  `clear_cache()`, at its next run and when it is collected, a stream when it is
  collected. The process directory is removed at exit. A directory whose process no
  longer exists is removed at the next start (the directory name holds the process id
  and a lock file).
- **Small results skip it.** Rows below a threshold (`OrcapodConfig.keep_in_memory_bytes`,
  proposed 64 MiB per stream) stay in memory as one Arrow table, as today; nothing is
  written. Tutorial-sized work never touches the disk. **OPEN (L2)**: the threshold.
- **What it never is.** It is not a store: nothing in it is an identity, nothing survives
  the process, no result is read from it by a later process, and it holds no hash.

Without source-info tokens materialized (section 7) a cache file is about the size of the
rows' Arrow data: 248 MiB for the measured 2.2 million row session.

## 3. What a job node keeps, and the view

Today a job node keeps its current run's rows in memory (ITL-1095). That is one whole
tokenized table per node, for as long as the job lives.

**Proposed (OPEN, N1: a public promise changes).** A job node keeps a *reference* to its
current run's rows, not the rows:

- a function node, and an operator node in `LOG` mode, read them from the database, a
  page at a time (their rows are stored there by the run; the node knows which entries
  are this run's);
- an operator node in `OFF` mode, and a side-effect node, read them from the local
  cache, where the run wrote them;
- below the in-memory threshold, the rows are kept as today.

Every read keeps its meaning: `node.as_table()` returns the current run's rows (now read
from the reference, memory-mapped where it is the cache), iteration yields them a batch
at a time, a downstream node's read is a batch reader. "A job node shows its current
run" stays true; "holds them in memory" does not.

**The view** (`StreamView`) follows the same rule and needs no special case:

- inside a job, it reads pages from the node's reference;
- outside a job, `run()` writes the computed rows to the local cache (or keeps them in
  memory below the threshold) and the stream keeps that reference, as a pod's stream
  keeps its computed table today. So nothing is computed twice: `as_table()` after
  `run()` reads the reference.
- `head(n)` and `tail(n)` read the first and last `n` rows; `total_rows` and the schema
  are recorded while the rows are written;
- `sort_by_tags()` on a result in memory sorts as today; on a cached result it sorts an
  index of the tags (the same keys-only index a join uses, section 6) and fetches the
  rows shown. The view then shows exactly what it shows today.
- The notebook display of a view is its `head` in tag order plus the row count, as
  today.

The issue's alternative, a view that keeps only the first and last rows, the count and
the schema, would make `as_table()` after `run()` compute everything again (calling
every function again outside a job) and would need a rule for every view method beyond
the kept rows. The cache-backed view has neither problem. **OPEN (N2)**: confirm the
cache-backed view over the bounded-window view.

A source that holds no rows (`SpiralDBTableSource`) is the one case where "keep a
reference" means "read again": its `run()` view may keep a cache file of the read, or
the first and last rows and the count. **OPEN (N3)**; proposed: the cache file, so a
source behaves as every other stream.

## 4. A bounded preview: `stream.preview(n)`

`head()` never computes, so it cannot show the first rows of a derived stream that was
never run. `preview(n)` computes only what `n` rows need:

- the limit is passed down: a source reads at most the rows asked of it (a SpiralDB or
  database source as a pushed-down `limit`, a file source by stopping the read);
- row-by-row steps (section 5) are applied to batches until `n` rows have come out; the
  batch size doubles when a batch yields too few (a selective filter);
- a step that needs its whole input (`GroupBy`, `Batch`, a join) is **computed in full**
  under `preview`, with a note in the view saying so. **OPEN (P1)**: compute in full, or
  refuse and point to `run()`. Proposed: refuse by default with
  `preview(n, compute_all=True)` to allow it, because a preview that silently reads
  2 million rows is the surprise this method exists to avoid.

Decisions the issue lists:

- **OPEN (P2)**: does a preview call user functions? Proposed: yes; it is the point of
  the method, and the name says it computes. `head()` stays the method that never does.
- **OPEN (P3)**: are a preview's results stored? Proposed: outside a job nothing is
  stored, as for any stream outside a job. A job node's `preview` is not offered in the
  first version: `job.run()` and `node.head()` cover it.
- **OPEN (P4)**: is the order defined? Proposed: the preview's rows are the first `n` the
  sync path would produce when every source has a defined read order (in-memory, CSV,
  Delta: the order given; SpiralDB and database tables: key order). A source without one
  (none today) would say "some `n` rows".
- A preview refuses a `SideEffectPod` in the chain. That is all it enforces: it does not
  exclude what a user function in a function pod does outside Orcapod (a write to an
  external service). Whether a preview runs user functions is open (P2).

## 5. Batch-wise execution, per operator kind

Rows move between job nodes as Arrow record batches (ITL-1347, already designed and
partly built: `InputChannel`, `OutputChannel`, `async_execute_batches`, the streaming
join kernels). This design finishes the wiring and adds what the memory bound needs.

| Kind | Operators | Holds |
|---|---|---|
| Row by row (`ROW_BY_ROW`) | `Select*`/`Drop*` columns, `MapTags`, `MapData`, `MapColumns`, `DropNulls`, `Filter`, `PolarsFilter`, `AddTags`, `TagBy`, `Pick`, `Index` | the batch in hand |
| Per row, user code | function pods, side-effect pods | the batch in hand; results go to the database as they are computed |
| Probe against one small or indexed side | `SemiJoin` | a keys-only index of the right input (section 6) |
| Joins | `Join`, `MergeJoin`, `Rejoin` | a keys-only index, section 6 |
| Grouping by key | `GroupBy` | a keys-only index of the input and one group at a time (below) |
| Grouping by arrival | `Batch` with a size | one batch of `size` rows |
| Whole input, by definition | `Batch` without a size | one row holding everything: its output is the input |

Notes:

- `PolarsFilter` is row by row but has no async form today, so it gathers its input; it
  gets `ROW_BY_ROW`. Its predicate is applied per batch; a predicate is an expression
  over one row's columns (an expression that reads other rows, such as a window, is
  already outside what the sync path's identity describes). **OPEN (B1)**: refuse
  expressions that are not row-wise (they exist in Polars), or document.
- `GroupBy` sorts its input by key and member order. Batch-wise: the input goes to the
  local cache while a keys-only index is built (the prepared key columns, the
  member-order tags, record id and source id, and the row's reference); the index is
  sorted exactly as today's sort frame is; groups are emitted one at a time by fetching
  their members. It holds the index (the size of the tag columns) and one group. Its
  output order is today's, on both paths. A group larger than memory is a row larger
  than memory: out of scope.
- A function pod over grouped rows receives one group's lists at a time; that row is as
  large as the group, by the user's choice.

### How sync stays deterministic

The sync orchestrator runs nodes in topological order, each to completion. Batch-wise it
does the same, with these rules:

- a row-by-row operator applied to the batches of its input in order, with the results
  concatenated in order, gives exactly its result over the whole input, because each
  output row comes from one input row; the differential test of ITL-1347 (sync against
  pinned `main`, byte for byte) is the gate;
- every other operator runs its one sync body (`static_process`) as today; what changes
  is only where its inputs' rows live (the cache instead of memory) and, for `GroupBy`
  and the joins, that the body fetches rows by reference. The matching, the order of the
  output rows and every value are the present code's: `Join` and `SemiJoin` already
  match on key columns and row numbers and take every value from the input by row number
  (ITL-1284), and a row number is a reference;
- the order of a sync read is the order the run emitted (ITL-1441): the cache file is
  written in that order and read in it.

So the sync path gets the same bounds as async for row-by-row chains and grouping, and
for joins the bound of "keys-only index plus the output written to the cache". It does
not get the early-release rules of section 6 that depend on arrival order.

## 6. Joins

Joins are where rows multiply and where an operator must remember rows. The aim is a
stated bound per case, not a join that is free (developer, November 25, 2026).

**The target (Decided).** An async join's memory is, in the worst case, linear in the
size of one of its inputs: never the product, never all inputs plus the output at once.
Everything else on the async path is bounded by the batches in flight.

### What a join holds today

- Sync `Join`: both inputs whole, as tokenized Arrow tables (about 4 KiB per row here), a
  Polars frame of the prepared key columns and row numbers, and the whole output. It is
  already a keys-only match: Polars sees keys and row numbers, and the rows are taken
  from the tables by row number.
- Async `Join` (today's row form): both inputs as lists of `(Tag, Data)` pairs (tens of
  KiB per row) until the other side closes.
- The streaming join kernels of ITL-1362 (built, not wired): each side's rows as Arrow
  tables plus a key frame, a side dropped when the other closes.

### Keys only in memory, rows in the local cache (Decided direction)

Each input of an async join is written to the local cache as it arrives. In memory the
join keeps, per row that can still match, an index entry:

- the row's join key in prepared form (`arrow_keys.polars_ready`, the present
  preparation), as a Polars frame, so matching stays one vectorized join per arriving
  batch with the sync path's own rule (`join._matched_pairs`); and
- the row's reference, `(batch number, row number)`: 8 bytes.

**The pairs of one match are bounded too.** The size of an arriving batch does not bound
the number of pairs it matches: rows of the batch that share a key with many indexed rows
give their product. So no match builds its whole pair frame at once. Before matching, the
join counts the pairs per arriving row (the batch's keys against a count of index entries
per key, kept beside the index: at most one count per arriving row) and cuts the batch
into row ranges whose pairs fit a pair budget (proposed 1,048,576 pairs, 16 MiB of row
references). One arriving row whose matches alone pass the budget is matched against
successive ranges of that key's index entries. Each range is matched, fetched, sent and
let go before the next, so the transient is `16 bytes × pair budget` plus one output
batch, whatever the keys hold. **OPEN (J3)**: the pair budget.

For each batch of matches the join fetches the matched rows of the other side from the
cache by reference (one `take` per input per batch of pairs), builds the output batch
with the sync path's column work, sends it, and lets the rows go.

Cost per indexed row: the key's own size plus 8 bytes. For the measured tables the
prepared key is 60 to 90 bytes (two short strings, a timestamp, two integers), against
4.2 KiB for the tokenized row today and 120 bytes for the bare row. **OPEN (J1)**: index
a 128-bit hash of the key instead of the key (24 bytes per row whatever the key's
width); matches are then confirmed on the fetched rows, so a hash collision costs a
fetch, never a wrong row. Proposed: start with the key itself (it is the present code),
measure, and switch only if the index is the limit.

Compared with today's sync `Join`: the same match, on the same prepared keys, with a
reference where today there is a row number; the difference is that the rows behind the
references are in a mapped file, not in process memory.

### Uniqueness certificates

A stream can be certified to have unique tags: each tag tuple occurs at most once.

**Where a certificate comes from (Decided).** Certificates originate inside Orcapod and
are trusted by every step that derives them; no step re-checks. The one claim that is
verified is a source's own.

The certificate is a set of *unique keys*: sets of tag names such that no two rows agree
on all of them. A stream has unique tags when one of its unique keys is a subset of its
tags. (**OPEN (C1)**: this form, or a single yes/no. The key form survives dropping a
tag that is not part of the key, and lets a join say more; the yes/no form is simpler
and must be given up whenever a tag is dropped. Proposed: the key form.)

| Step | Certificate of the output |
|---|---|
| Source with key columns `K` that are all tags | `{K}` (verified or store-guaranteed, below) |
| Source identified by position, or whose key columns are not all tags | none |
| Select/drop data columns, `MapData`, `DropNulls`, `Filter`, `PolarsFilter`, function pod, side-effect pod, `Pick`, `Index` | the input's (rows are removed or data changes; tags stay) |
| `AddTags`, `TagBy` | the input's (a superset of a unique key is unique) |
| Select/drop tag columns | the input's keys that are still tags |
| `MapTags`, `MapColumns` | the input's keys, renamed |
| `SemiJoin` | the left input's |
| `GroupBy(by)` | `{by}`: one row per key |
| `Batch` | none (its tags are lists) |
| `Join`, `MergeJoin` of inputs with keys `Ka`, `Kb`, joined on shared tags `S` | `Ka ∪ Kb`; also `Ka` when `Kb ⊆ S` (each left row matches at most one right row), and `Kb` when `Ka ⊆ S` |
| `Rejoin` | the inputs' common keys (a result row is its origin row) |

**How it travels (Decided: not in any hash or identity).** It is a read-only member of
the stream's description, beside `condition` and `row_limit`: `StreamProtocol.unique_keys`.
Each operator derives it from its inputs' certificates when the graph is built; no data
is read. It is not Arrow metadata, enters no hash, identity, config, stored path or
stored row, and a loaded job derives it again from its sources. Nothing in the design
requires it in an identity: it never changes a result, only how much a join remembers.

**Verification as data flows (Decided).** A source that claims a key keeps the keys it
has emitted and checks each batch before it is sent on. On a duplicate it raises, and
the duplicate row never proceeds, so nothing downstream computes on a wrong assumption.

- *Memory*: keys only. Today `SpiralDBTableSource` already does this with a Python set
  of record-id strings (about 250 bytes per row, 0.5 GiB for the measured session). The
  proposal keeps 128-bit digests of the record id (16 bytes per row, in a sorted
  numeric structure checked per batch): 35 MiB for the measured session. A digest
  collision would raise a false duplicate (never a wrong result); at 100 million rows
  its probability is below 10^-20.
- *What has happened when a duplicate appears late*: the rows before it were emitted, and
  downstream nodes may have computed and stored results for them. Those stored results
  are correct for their rows (a function result is kept by its input row's hash), exactly
  as after any run that fails part-way.
- *The error*: `RowIdentifierError`, as today, naming the source (its id and label), the
  key, and that an earlier row of the read holds it. (Naming the earlier row's position
  needs 8 more bytes per key. **OPEN (C2)**; proposed: not kept.)
- *The error policy*: a uniqueness violation is a condition of the whole input, not of a
  row, so it is outside the row error policy, as the data-context check is: it stops the
  source's node and fails the run under `continue` and `fail_fast` alike. Every node
  downstream of the source holds rows computed on a claim that turned out false; a run
  that raised defines no rows for the nodes that ran, which is the present rule.
- In-memory sources check their keys when they are built (they hold every row), so their
  certificate is verified before the first read; nothing changes for them.

**Keys the store guarantees (Decided).** Where the source's own system guarantees
uniqueness over the key, Orcapod trusts it without the running check.

- *How a source states it*: a class-level declaration, never a user flag:
  `RootSource.key_enforced_by_store()`, answered by the source class from what it knows
  of its table. A user's claim on a file is a claim to verify.
- *The rule*: the key columns the source identifies rows by must be exactly a key the
  store enforces, and, for the certificate, the tags must contain them (a superset of a
  unique key is unique; a subset is not).
- *Which kinds*:

| Source | Guaranteed by the store? |
|---|---|
| PostgreSQL table, key = the table's primary key | yes: the database enforces it (a primary key column is never null) |
| SQLite table, key = the table's primary key | yes when every key column is declared `NOT NULL`, or is the `INTEGER PRIMARY KEY`, or the table is `WITHOUT ROWID`; otherwise no (such a column can hold NULL, and SQLite keeps rows with a NULL key apart): running check |
| The same, key = columns of a `UNIQUE` constraint, all `NOT NULL` | yes, once the connector reads constraints (not today: running check until then) |
| The same, `record_id_columns` given and not such a key | no: running check |
| `DBTableSource` over a generic connector | only if the connector reports an enforced key; otherwise running check |
| SpiralDB keyed table, key = its key columns, one pinned version | yes, with one reservation (below) |
| CSV, Delta, in-memory (Arrow table, dict, list, data frame) | never: checked at construction (in memory) or as the rows flow (files) |
| `PollingSource` | never trusted; it already refuses a key an earlier batch held |
| `CachedSource` | its wrapped source's answer |
| `DerivedSource` over a job node's stored records | **OPEN (C3)**: the records are kept by record id, which is unique per row of a run, but its tags are the node's output tags, unique only if the node's certificate says so. Proposed: the node's certificate, verified by the running check on read |

  SpiralDB: a keyed table stores at most one row per key; a write of an existing key
  replaces that row (upsert), and a repeated key within one write is refused. This was
  observed on the service with spiral-sdk 0.16.74 and 0.16.75 (the rules `upsert` and
  `dup` of the test fake record it). I did not find a statement of it in documentation
  I could cite, so the confidence is "observed, and the meaning of a keyed table", not
  "documented guarantee". A source reads one pinned version, so no concurrent write can
  show two versions of a row. Proposed: trust it, and keep the conformance test that
  would notice a change.

### The bound for each case

`n` and `m` are the rows of the two inputs; "index" is the per-row cost above (key plus
8 bytes); rows themselves are in the cache, on disk.

| Case | What is held in memory | Bound |
|---|---|---|
| Both inputs certified unique on the join key (the tags fully overlap): 1:1 | the index of rows not yet matched; an entry is dropped the moment it matches, and both rows are emitted and let go | index × (unmatched rows). Worst case, one input arriving entirely before the other: index × max(n, m). With both inputs in key order: the out-of-order window only |
| One input certified unique on the join key (a large table against its lookup table) | the unique side's index for the whole join; rows of the other side that arrive before the unique side has closed are indexed until then, after which each arriving row matches at most one row, is emitted and is not kept | index × (unique side) + index × (rows of the other side that arrived early). Worst case index × (n + m): still keys only |
| Neither unique (many to many) | the index of both sides | index × (n + m). The output can be n × m rows; it is sent a batch at a time and never held |
| Any case, a side whose other side has closed | nothing more is kept for it (the kernels' present rule) | |
| Any case, while one range of pairs is matched | the pairs of that range (two row references each) and its output batch | 16 bytes × pair budget + one output batch, whatever the keys hold |

In every case the memory is keys only, so it is below the full size of one input by the
ratio of row size to key size (here 120 bytes against 70 to 100, without tokens; 4.2 KiB
against 70 to 100 with today's tokens). The developer's worst case, "linear in the size
of one input", holds with room: the worst case is linear in the *keys* of both.

**Key-ordered inputs.** Two inputs sorted by their tags can be merged holding only the
rows between the two read positions. That needs a second certificate, "in tag order",
which a source can give (a SpiralDB or database scan in key order) and which few
operators preserve on the async path, where arrival order is not a result. **OPEN (J2)**:
proposed not in the first version; the 1:1 early release already gives the window bound
whenever the inputs happen to arrive in step.

**Three or more inputs.** `Join` folds pairwise in its canonical order, as today; each
step is one of the cases above, and a step's output goes to the next step as batches,
not to a cache file, unless that next step must index it.

**Diamonds.** When both inputs come from one upstream through a broadcast, one input
cannot be read to its end while the other waits: the wait would block the upstream. This
is why rows that arrive "early" are indexed and cached rather than refused by
back-pressure, and why the worst case counts both sides.

### Order and determinism

- Async: the output is the sync join's rows as a set; batches are sent as matches are
  found.
- Sync: `Join.static_process` as today, over references. Its row order is today's (the
  order Polars gives the match, kept by ITL-1355's rule), so the stored and returned
  tables are identical.

### Test plan that proves the bound

- A measurement in `measure.py` style for each case (1:1 in step, 1:1 one side first,
  unique side small, many to many), at two sizes a factor of 10 apart, recording peak
  resident memory less mapped file pages, and the peak of the Arrow pool: the peak must
  grow as the index does (key bytes × rows), not as the rows do, and a 10 times larger
  non-indexed side must not move it.
- The differential of ITL-1347 for results: async equals sync as a set of rows, sync
  equals pinned `main` byte for byte.
- A violation test per certificate rule: a source that claims a key and repeats it late
  raises, the duplicate reaches no downstream node, and the run fails under both error
  policies.

## 7. Source-info tokens that are derived, not stored per row

A source row's token for column `c` is `<source id>::<record id>::<c>`, and the row's two
system tags hold the source id and the record id. So the 35-fold cost is a copy of what
the row already carries.

**Proposed (OPEN, T1).** A stream may hold a column's tokens as a *recipe* instead of a
column: "the token of `c` is built from system-tag columns `S` and `R` and the name
`c0`" (`c0` is the column's name at the source, which a rename does not change). Record
batches then carry no token column for it. The tokens are materialized, by one helper,
exactly where they are read:

- a table or row that shows them (`as_table(columns={"source": True})`, `all_info`,
  `data.source_info()`);
- a function node writing its pipeline records, a database write of an operator's
  output: the stored rows hold the same tokens as today, byte for byte;
- an operator that computes with tokens: `MergeJoin` (token pairs), `Batch` and
  `GroupBy` (token lists), a function pod's output (new tokens).

Operators that pass tokens through (select, map, filter, `DropNulls`, `Join`, `SemiJoin`)
pass the recipe; `Join` renames the system-tag columns a recipe names as it renames the
columns.

This is an in-memory representation only: every stored token, hash and layout is
identical. It touches how every operator treats `_source_` columns, so it is the largest
step and comes after the transport. Its worth, from the measured column groups: a
tokenized table falls from 911 MiB to about 107 MiB per 200,000 rows (8.5 times).

Second step (**OPEN, T2**): `GroupBy` and `Batch` emit one token list per column, each
with one token per member: the same 35-fold cost inside the lists. A recipe for those
("the tokens of `c` are built from the members' record ids", one hidden list per group
instead of one per column) removes it. Proposed: after T1, measured first.

## 8. The memory we can promise

With sections 2 to 7 in place, for a job run by the async orchestrator:

| Part | Held in memory |
|---|---|
| A source | one batch read (65,536 rows at most), plus 16 bytes per row read if its key is verified as data flows, nothing if the store guarantees it |
| A row-by-row operator, a function pod | the batch in hand |
| A channel between two nodes | its buffered batches (a fixed number) |
| `GroupBy` | its keys-only index, and one group |
| A join | its keys-only index (section 6), and the pairs of one range being matched (at most the pair budget) |
| A job node after the run | a reference (a file name or a set of stored entries), or its rows when they are below the in-memory threshold (section 2) |
| A view | the rows shown |

**The bound to state to users**: peak memory of an async job is
`(batches in flight) × (batch bytes) + (key bytes + 8) × (rows indexed by joins and groupings) + 16 × (rows of verified sources) + 16 × (pair budget) × (joins matching at once) + (small results kept in memory) + the largest single row`.
For the measured session (2.2 million rows of 120 bytes, one grouping, one function
pod) that is under 0.5 GiB, against about 210 GiB today.

Small results kept in memory are part of the bound, and a per-stream threshold alone
does not limit them: 100 nodes each keeping 63 MiB hold 6.2 GiB. So the threshold comes
with a total for the process (proposed 512 MiB, part of L2): a result that would take the
kept total past it goes to the cache whatever its size, and the term is at most that
total. The bound also assumes a cache root on disk (section 2).

**Proposed budget for the workshop** (**OPEN, M1**): a job over every table in scope of a
sample dataset peaks below 4 GiB on the async path, checked on synthetic tables of the
dataset's shapes and then once live, read-only.

The sync path: the same for row-by-row chains, `GroupBy` and joins' inputs; a sync join
additionally holds nothing more than async's worst case. Outside a job, `as_table()` of
a large result returns a memory-mapped table.

## 9. What changes for users

- `run()` no longer means "one table in memory". Its view shows the same things.
- A job node no longer holds its rows in memory; every read of it gives the same rows.
  Reading a large node with `as_table()` maps a file or reads the database.
- Large results outside a job are written to a temporary directory, which must have
  room; small ones are not. A new config value names the directory, another the size
  below which rows stay in memory.
- `stream.preview(n)` is new.
- `flow()` and iteration of a large stream yield rows a batch at a time and keep none
  (`ArrowTableStream.iter_data` stops keeping every pair it yielded). **OPEN (U1)**:
  two iterations of one stream then give equal rows but not the same objects; proposed:
  accept.
- A source that repeats a key fails when the repeat is read, as `SpiralDBTableSource`
  does today, not always before the first row.
- Async runs get faster as well as smaller: the per-row object work is what makes the
  async path 25 times slower than sync today (127 s against 5 s at 200,000 rows).

## 10. Order of work

Each step is a draft pull request against `main` with tests and before/after numbers on
the synthetic tables of the diagnosis; none changes a hash, a stored layout or a golden
value.

1. **Done (PR 558)**: a SpiralDB read that returns no tokens builds none.
2. **First part done (PR 562)**: rows kept on the async path as Arrow tables of at most
   8,192 rows, never as a list of pairs (async now costs about what sync does). Remaining:
   rows on the async path as record batches: wire `async_execute_batches` and the
   record-batch channels into the nodes and the async orchestrator (ITL-1347's remaining
   stages), give `PolarsFilter` `ROW_BY_ROW`. Removes the 18 to 110 KiB per row.
3. The local cache, and job nodes that keep a reference (section 3). Removes "every node
   holds its output".
4. The view and `run()` on the cache; `preview(n)`.
5. Certificates and the running check with digests; store-guaranteed keys.
6. Joins on a keys-only index over the cache, with the early-release rules; `GroupBy`
   on the index.
7. Derived tokens (T1), then token lists (T2).

Steps 2 and 3 together give the bound for row-by-row chains and function pods; step 6
for joins and grouping; step 7 removes the 35-fold factor from everything that still
holds rows (cache files, `as_table()`, the sync path).

## Open decisions for the developer

| Id | Decision | Proposed |
|---|---|---|
| N1 | A job node keeps a reference to its run's rows (database or local cache), not the rows in memory: changes the ITL-1095 promise | yes |
| N2 | The view is backed by the cache (keeps every method's meaning, nothing computed twice) rather than a bounded window of first and last rows | cache-backed |
| N3 | A source that holds no rows: its `run()` view keeps a cache file of the read | yes |
| L1 | Where the local cache lives by default; what happens when that place is memory-backed | the system temporary directory, configurable; a warning when it is memory-backed, and no bound then |
| L2 | The size below which rows stay in memory, and the total kept in memory per process | 64 MiB per stream, 512 MiB in total |
| P1 | `preview(n)` over a step that needs its whole input: compute in full or refuse | refuse unless `compute_all=True` |
| P2 | `preview` calls user functions | yes |
| P3 | `preview` results are stored | no; not offered on job nodes at first |
| P4 | `preview` order | the first `n` of the sync order |
| B1 | `PolarsFilter` with an expression that is not row-wise | refuse |
| J1 | The join index holds the key or a 128-bit hash of it | the key first; measure |
| J2 | An "in tag order" certificate for a streaming merge | not in the first version |
| J3 | The pair budget of one match range | 1,048,576 pairs (16 MiB) |
| C1 | Certificate as a set of unique keys or as yes/no | set of unique keys |
| C2 | The duplicate-key error names the earlier row's position (8 bytes per key) | no |
| C3 | `DerivedSource` over a node's records: certificate | the node's, verified on read |
| T1 | Source tokens held as a recipe until read | yes, after the transport |
| T2 | Token lists of `GroupBy` and `Batch` as a recipe | after T1, measured first |
| U1 | Iteration keeps no row objects | accept |
| M1 | The workshop budget | 4 GiB peak, async, tables in scope |

Approved by the coordinator on the developer's delegation (November 25, 2026), each for
the developer's review: N1 (a job node reads its run's rows from storage or a local
spill file, a page at a time; spill files under a temporary directory per job run,
removed with the job or the process, never part of a stored path or hash; same rows,
same order on sync), T1 as a direction (design first, implemented after the transport),
and the interim transport step of PR 562. The local cache of section 2 is one shared
module, `core/row_spool.py` (PR 559), used by joins, views and job nodes alike.

Decided by the developer (November 24 and 25, 2026), recorded above: `run()` promises
computation and nothing about how; async is the primary memory-bounded path; joins keep
keys only, with rows fetched from a local cache; certificates originate in Orcapod and
are trusted by derivation, a source's claim is verified as data flows, and a key the
store enforces is trusted; a certificate enters no hash or identity; an async join's
memory is at worst linear in one input; tables of long sample arrays are out of scope.
