# Source identity, implementation versions and exact stand-ins (ITL-1425)

A decision document. Nothing is implemented. Every statement about today's behaviour is
measured by a script in [`itl-1425-source-identity/scripts/`](itl-1425-source-identity/scripts/),
run on `main` at `8ee6c6d4` (identity code unchanged at `83c54764`) with the public API, from
that folder: `uv run python -m scripts.<name>`. Synthetic data only. The measurements of
"today" are of `main` before this PR; run on the PR branch, `identity_today` and
`stand_in_job` show the implemented ids, and `golden_moves` runs only at `8ee6c6d4`. A design that is not
implemented is simulated by patching the library inside the script's own process; nothing
under `src/` changes.

## Decisions (developer, 2026-11-23; implemented in this PR)

The moved golden list (section 3.7, regenerated against the implementation in the PR body)
is still to be approved before any golden file changes.

| # | Decision | Where |
|---|---|---|
| 1 | Every source id is `<kind>_v<N>:<name>`. The name is the given `source_id` or the default (a location, a qualified table, `<hash label>:<hex>`). Orcapod always adds the kind part, to given ids too (`units` read by Delta v1 is `delta_table_v1:units`). This reverses "a given id is used exactly as given" (ITL-957, ITL-1154) to "a given **name** is used exactly as given" | `source_ids.build_source_id`, `RootSource.source_id` |
| 2 | Kind words stay today's `SOURCE_KIND` names (`delta_table`, `sqlite_table`, `csv`, ...); each class declares `SOURCE_VERSION` beside it (all 1). Bump it when a row read before a change could read differently after it | every source class |
| 3 | A given name that starts like `<registered kind>_v<N>:` is accepted as the name (nested: `csv_v1:delta_table_v1:units`) with `SourceNameWarning`; never refused | `source_ids.warn_if_name_looks_like_an_id` |
| 4 | The kind part is the text before the first `:` and never holds one; the name may. No `::`, no `:` at either end of the name, no empty name | `source_ids.parse_source_id`, `require_source_name` |
| 5 | Saved configs hold `source_name` and `source_version` apart (kind = `source_type`); the id is rebuilt, never parsed. Loaded under another declared version, the source has the installed version's id and `SourceVersionWarning` says so; nothing is rewritten. A config saved before this change (a `source_id`, no `source_name`) is refused | `RootSource._source_id_config`, `saved_source_name`, `serialization.saved_source_id` |
| 6 | Tokens, system tags and `source_id` show the full id. Record ids never hold the source id (unchanged; tested) | section 1.3 |
| 7 | The class name leaves the content hash: identity `(source id, Arrow schema identity, key columns)` plus each kind's extras (SpiralDB `tags_as_data`; a restriction adds nothing). `PollingSource`: its source id is added to its identity. Plain `DerivedSource`: identity stays the origin's content hash; its default id is `derived_v1:semantic_v1:<hex>`. `CachedSource`: the wrapped source's identity **and id** (no `source_id` argument any more). `SourceProxy`: the saved hashes and the saved id | section 3.3 |
| 8 | Out of scope, filed as **ITL-1426** (deferred): the explicit stand-in operation and the row-hash ledger. Sections 3.4 and 5 are their design | ITL-1426 |
| 9 | Confirmed by the developer (2026-11-23; proposed by the worker): a job slot (a `job.nodes` key and a `bind` key) is named by the source's **name**, not its full id, so `bind(sources={"units": ...})` is unchanged and a slot keeps its name whichever kind reads it | `SourceJobNode.from_stream` |

**Before this decision** the developer's direction was stated as: every source id carries,
explicitly and in one fixed place, the kind and implementation version that read the data;
the record id never depends on the source id; a source stands in for another by an explicit
operation that keeps the name and swaps only the kind part, after the rows are verified
equal. Section 3 works this design out; section 4 compares the other alternatives with it.

| Script | What it measures |
|---|---|
| `identity_today` | content hash, pipeline hash, system tags, tokens, record ids and default ids of six kinds holding the same rows; record ids under several source ids |
| `identity_structures` | `identity_structure()` of every kind that can be built here; saved and loaded slots |
| `stand_in_job` | one kind in place of another in a job: bind, and a fresh job over one database; `--without-class-name` runs it under alternative C |
| `stand_in_explicit_kind` | the developer's design simulated: ids, tokens and a job with and without an explicit stand-in |
| `cached_source_today` | what `CachedSource` stores, where, and when it reads its source |
| `types_positions_changes` | inferred CSV types, a narrower Arrow type, positional record ids, a silent reader change |
| `ledger_prototype` | the ledger prototyped outside the library: stand-ins, types, a reader change, positions |
| `row_hash_cost` | cost of hashing every row (tutorial dataset, 1M rows) |
| `golden_moves` | which golden entries each design moves (lists in `golden-moves-<variant>.txt`) |

## 0. The questions

1. Can a source say which version of its kind's implementation read it, for example in
   its source id? (Today: no.)
2. What are the alternatives, with their pros, cons and implications?
3. Requirement: one source can stand in **exactly** for another, also across kinds (a
   CSV export in place of a database table that was migrated away).
4. Idea: make source caching a **record-keeping ledger** (per record: source id, record id,
   the hash of the whole row, implementation columns) instead of a full cache.

## 1. Today, measured

### 1.1 What a source's identity is made of

The same three rows (`subject` key, `weight: double`, `count: int64`) as six kinds, each
with the given `source_id="subjects"` and `record_id_columns=["subject"]`
(`identity_today`, `identity_structures`):

| Kind | `identity_structure()` (content hash input) | Pipeline hash input |
|---|---|---|
| `ArrowTableSource`, `DictSource`, `ListSource`, `DataFrameSource`, `CSVSource`, `DeltaTableSource`, `SQLiteTableSource` (and, by code, `PostgreSQLTableSource`, `DBTableSource`) | `(class name, (tag schema hash, data schema hash), source_id, key columns)` | `(tag schema hash, data schema hash)` |
| `SpiralDBTableSource` (code; needs a server) | the root form, then `("tags_as_data", sorted mapping)` when given; a restriction adds nothing (`_restriction_identity` returns `None`) | the same |
| `CachedSource` | its inner source's structure (wrapping is invisible) | the inner's |
| `DerivedSource`, plain (`node.as_source()`) | `(origin node's content hash,)`: no class name, no source id, no schema | the origin's schema |
| `DerivedSource`, `tags_as_data` | root form with class name `DerivedSource`, then the sorted mapping | schema |
| `PollingSource` (provisional) | `(impl.identity(), sorted tag columns, key columns)`: **no source id and no schema**; a name only if the implementer puts one in `identity()` | schema |
| `SourceProxy` (code) | the saved content hash, as text | the saved pipeline hash |

Measured across the six kinds holding the same rows under the same id and key:

| Compared with `ArrowTableSource` | csv | delta_table | sqlite_table | dict | data_frame |
|---|---|---|---|---|---|
| content hash | differs | differs | differs | differs | differs |
| content hash input without the class name | equal | equal | equal | equal | equal |
| pipeline hash, Arrow schemas | equal | equal | equal | equal | equal |
| system-tag column names and values, source-info tokens, the whole table | equal | equal | equal | equal | equal |

A source-info token is `subjects::{"subject":"s1"}::weight` for every kind; the system-tag
columns are `_tag_source_id::<schema hash>` (value `subjects`) and
`_tag_record_id::<schema hash>` (value `{"subject":"s1"}`).

### 1.2 Where the kind enters a datum's identity today

| Place | Kind in it? | Forced by Orcapod? |
|---|---|---|
| Source content hash (`RootSource.identity_structure`) | the class name | **yes**: no argument removes it |
| Everything downstream of it: node content hashes, `__node_content_hash` in stored pipeline rows, the `instance:<content hash>` path component of a content-scoped table, a `CachedSource`'s table path, a plain `DerivedSource`'s default id (`derived:semantic_v1:<hash of the origin's content hash>`) | the class name, through the hash | **yes**, transitively |
| Default source ids (`csv:<path>`, `sqlite_table:<db>:main.<t>`, `arrow_table:arrow_v1:<hex>`, ...) | the kind | no: a given `source_id` replaces it, used exactly as given |
| Record ids (`row_<n>`, key JSON) | no | - |
| Source-info tokens, system-tag column names | no (only the source id and the schema hash) | - |
| Pipeline hash | no | - |
| Function result memo (input data hash) | no | - |
| Saved job config (`source_type`) | the kind, to rebuild the source | not identity |

So the **class name in the source content hash is the one place that forces the kind into
a datum's identity**; everything else that carries the kind is a default's choice.

Unevenness to settle with any rule: `PollingSource` leaves out the source id and the schema
(a given `source_id` changes no hash: measured, `identity_structures`), and a plain
`DerivedSource` is named by its origin's hash only.

### 1.3 Record ids do not depend on the source id

`identity_today`: the same table under three given source ids (`subjects`,
`csv_v1:subjects`, `something_else`), keyed and positional, and the six kinds:

| | Record ids | Token of row 0 (`weight`) |
|---|---|---|
| keyed, any source id | `{"subject":"s1"}`, `{"subject":"s2"}`, `{"subject":"s3"}` | `<source id>::{"subject":"s1"}::weight` |
| positional, any source id | `row_0`, `row_1`, `row_2` | `<source id>::row_0::weight` |
| six kinds, keyed | equal | equal under one source id |

A record id is the row identifier only (ITL-1154: `row_<n>` or the key JSON); the source id
enters a token, `<source id>::<record id>::<column>`, and the source-id system tag, never the
record id. The developer's design needs no change here.

### 1.4 One kind in place of another in a job

`stand_in_job`: a function pod over a slot; the `ArrowTableSource` first, then each other
kind (same rows, same id and key).

| | Bind into one job (`job.bind`) | A fresh job per kind, one database |
|---|---|---|
| bind accepted | yes, every kind | - |
| function calls after the first kind | 0 (results memoized by input data hash) | 0 |
| output, system tags and tokens included | identical | identical |
| node content hash | **unchanged** (see the bug below) | **differs per kind** |
| stored pipeline rows (`pdb_v1`) | 3 rows, 1 `__node_content_hash` | **12 rows, 4 `__node_content_hash` values** (one set per kind) |
| `node.as_source()` | - | 3 rows each (each kind sees only its own set) |

Under alternative C (`stand_in_job --without-class-name`, section 4), both columns give one node
content hash and 3 stored pipeline rows for all six kinds: an exact stand-in.

**Pre-existing bug found** (reported to the coordinator, not fixed here): after
`job.bind(...)` the bound slot's content hash changes, but a downstream function node keeps
the content hash it computed before the bind (`_content_hash_cache` is not invalidated
downstream). Its new rows are stored under the previous source's `__node_content_hash`.

### 1.5 What `bind` accepts (ITL-1112, ITL-640)

`SourceNodeBase.validate`: per column the declared Python type (or `T` into `T | None`)
and the Arrow type compared by `hash_schema`, with only a top-level non-null column
accepted where the slot is nullable; exactly one source-id and one record-id system-tag
column. `_DeclaredForm` presents the source's rows as the slot's declared schema.
Measured (`types_positions_changes`):

| Stand-in | Accepted? | Recomputes? |
|---|---|---|
| CSV with inferred types (non-null columns) bound into a nullable slot | yes | 0 calls (rows given in the declared form) |
| the same CSV in a fresh job | - | **3 of 3**: another pipeline hash, and the input data hash includes nullability |
| Delta table with `count: int32` into an `int64` slot | refused, `SourceSpecMismatchError` | - |

### 1.6 `CachedSource` today (`cached_source_today`)

| Question | Answer |
|---|---|
| Path | `<prefix>/source/node:<source content hash>/sdb_v1`: one table per kind for the same rows (4 tables for 4 kinds) |
| Columns | `_record_hash`, the user columns, both system-tag columns, the source-info columns, `__data_context`: a full copy |
| Row hash | `arrow_v1` of the one-row table **including the system tags**, so it holds the source id (the same rows under another id share no hash) |
| When the inner source is read | on the first read in a process; then again only when `check_for_new_data()` sees its modified time move |
| Semantics | cumulative union of every row ever read |

### 1.7 Two silent cases

| Case | Content / pipeline hash | What a job does | Signal |
|---|---|---|---|
| Positional record ids, the same rows in another order | unchanged | 0 calls; `s1` becomes `row_2` (tokens change) | none |
| A reader change: one value read differently, same id and key | unchanged | 1 call; the stored view holds both values for `s2` (4 rows for 3 records) | **none** |

The second is R2's failure today: an implementation change that changes what a
row reads as is silent.

### 1.8 Cost of hashing every row (`row_hash_cost`)

Apple machine, 18 CPUs, load average about 4.6 during the runs, one process.

| Input | Rows | Build and read the source | Hash the table once | Hash every row (one `hash_table` per one-row slice, as `CachedSource` does) |
|---|---|---|---|---|
| tutorial spikes (every spikes file stacked, CSV) | 36,616 | 0.049 s | 0.010 s | 1.32 s (36 us/row, 27x the read) |
| tutorial `units.csv` | 64 | 0.001 s | 0.000 s | 0.004 s |
| synthetic, 3 user columns | 1,000,000 | 1.42 s | 0.28 s | **32.8 s** (33 us/row, **23x the read**) |

starfix has no per-row call (`ArrowDigester`: `hash_table`, `hash_record_batch`,
`hash_array`, ...), so a row hash is one Python call per row. Hashing the whole table once
costs a fifth of the read: a vectorized per-row digest in starfix would bring the per-row
cost near that (not measured; it does not exist).

## 2. Requirements

**Guiding principle** (developer, 2026-11-23, as revised the same day): the kind and the
implementation version of the source that read a datum are named **explicitly, in one fixed
and replaceable part of the source id**, and nowhere else in a datum's identity. The record
id never depends on the source id. So a stand-in changes exactly one thing, the kind part,
and changes it only by an explicit, verified operation. Nothing else in Orcapod (a class
name in a hash, a kind hidden in a name) may force the kind in, or a stand-in could not be
exact.

| | Requirement | Test |
|---|---|---|
| R1 | Exact stand-in across kinds and versions | Same rows, same name, key columns, any two kinds or versions, after the explicit stand-in: equal source content hash, node content hashes, tokens, system tags, record ids; a job over one database computes nothing again and stores no second set of pipeline rows |
| R2 | An implementation change that changes what a row reads as is never silent | A reader that now reads one value differently: the next run raises or reports, naming the record |
| R3 | Provenance stays truthful | For a row read through a stand-in, something stored says which kind, version and location actually read it |
| R4 | Order never counts; no hash input from an implementation detail unless decided | Reordering columns or (keyed) rows changes no hash; the kind part is the one decided input |
| R5 | User-given names are kept | The given name appears verbatim after the kind part. (Today's rule "a given id is used exactly as given", ITL-957 and ITL-1154, is **reversed** by the developer's design: Orcapod adds the kind part) |
| R6 | Cost | Per-row work at most a small multiple of the read; measured on the tutorial dataset and 1M rows |

Today: R1 fails (class name; 1.2, 1.4), R2 fails (1.7), R3 holds only as far as an id says,
R4 holds, R5 holds in its old form, R6 holds (nothing is hashed per row except in
`CachedSource`).

## 3. The developer's design: the kind part in every source id

### 3.1 Grammar

| Part | Form | Rules |
|---|---|---|
| Source id | `<kind part>:<name>` | no `::` anywhere and no trailing `:` (today's rules, kept: a token splits at its first and last `::`) |
| Kind part | `<kind>_v<N>`, e.g. `delta_v1`, `csv_v1`, `sqlite_v1` | lower case letters, digits and `_`; ends in `_v` and a positive integer; **never holds `:`**; added by Orcapod only |
| Name | the given id, or the default: a location (`/data/units.csv`, `<db>:main.units`, `<host>:<port>/<db>.<schema>.<table>`) or a hash (`arrow_v1:<hex>`) | not empty; may hold `:` (a SQLite default already does); a source id is split at its **first** `:` |

Measured with the simulation (`stand_in_explicit_kind`, `golden_moves` variant `K_always`):

| Source | Source id | Token of row 0 |
|---|---|---|
| `ArrowTableSource`, given `subjects` | `arrow_table_v1:subjects` | `arrow_table_v1:subjects::{"subject":"s1"}::weight` |
| `CSVSource`, given `subjects` | `csv_v1:subjects` | `csv_v1:subjects::{"subject":"s1"}::weight` |
| `ArrowTableSource`, default | `arrow_table_v1:arrow_v1:<hex>` | |
| `CSVSource`, default | `csv_v1:<path>` | |

The record id is the same in all of them (1.3).

**The hash label beside the kind part.** A hash default reads `arrow_table_v1:arrow_v1:<hex>`:
two parts of the same shape, `<word>_v<N>`. The position decides (the kind part is always
first, a hash label only ever in the name), and a parser never confuses them, since it splits
at the first `:`. A reader may: the documentation must show the anatomy once. A different
shape for the kind part (`delta@1`, `delta.v1`) would remove the look-alike; the developer
chose `delta_v1`, and this document keeps it.

**Which word is the kind.** Today `SOURCE_KIND` is also the config type key: `arrow_table`,
`dict`, `list`, `data_frame`, `csv`, `delta_table`, `sqlite_table`, `postgresql_table`,
`db_table`, `spiraldb_table`, `polling_source`, `derived`, `cached`. The developer's examples
use short words (`delta_v1`, `sqlite_v1`). The simulation used `SOURCE_KIND`; the golden
entries that move are the same whichever spelling is chosen.

### 3.2 Given ids

| Question | Options | Recommendation |
|---|---|---|
| Is a given id prefixed? | (a) always: `units` read by Delta v1 is `delta_v1:units` (developer, settled); (b) defaults only | (a), as settled: the id always names the implementation that read the data, and one grammar holds for every id. It reverses ITL-957/ITL-1154 ("a given id is used exactly as given"): what the user gives is now the **name**, used exactly as given |
| A given id that already starts with something shaped like a kind part (`delta_v1:units`, also `run_v2:x`) | refuse; nest (`csv_v1:delta_v1:units`); strip | **Decided otherwise: nest, with a warning when the prefix is a registered kind (Decisions, 3).** This document had recommended: refuse, with `ValueError` saying the kind part is Orcapod's and pointing to the stand-in operation: nesting would make a stand-in look like it was spelled by hand, and the rule is ours and certain ("when in doubt, refuse"). The cost: a user cannot name a source `run_v2:x`; the message says so |
| An empty name, `::`, a trailing `:` | refuse (as today) | unchanged |

### 3.3 Identity

The kind part names the implementation, so the class name must leave the content hash:
otherwise a stand-in that presents the original's id still has another identity (1.4, and
`stand_in_explicit_kind` part 2 shows the equality only with the class name removed).

| Kind | Identity under the design |
|---|---|
| Every root source (Arrow, Dict, List, DataFrame, CSV, Delta, SQLite, PostgreSQL, DBTable, SpiralDB) | `(Arrow schema identity, source id, key columns)`; SpiralDB adds its sorted `tags_as_data` as today; a restriction adds nothing (as today) |
| `PollingSource` (provisional, ITL-1316) | **changes to the root form**: today `(impl.identity(), tags, keys)` with no source id and no schema (1.1); the default name is the semantic hash of `impl.identity()` as today, so the default id is `polling_v1:semantic_v1:<hex>` |
| `DerivedSource`, `tags_as_data` (provisional, ITL-1317) | root form with `derived_v1:<given name>`, then the sorted mapping |
| `DerivedSource`, plain | today `(origin content hash,)`. It reads stored records, so it is a kind: its id `derived_v1:semantic_v1:<hex of the origin's content hash>`; the identity can stay the origin's hash until ITL-956 gives it system tags and tokens |
| `CachedSource` | the wrapped source's identity and id (wrapping is not a read of another implementation); its table path follows the identity |
| `SourceProxy` | the saved hashes, as today |

What remains in a datum's identity: the source id (kind part and name), the schema, the key
columns. The kind enters exactly once, in the kind part.

### 3.4 The explicit stand-in

Not implemented here: filed as ITL-1426 (deferred). This section is its design.

| Question | Recommendation |
|---|---|
| Shape | a method on the source, returning a copy, as `where()` does: `csv.stand_in_for("sqlite_v1:units")`. Not on `bind`: the tokens are built by the source, so the source must know whose id it presents; and a stand-in is the same outside a job |
| Argument | the original's full source id. Usually the names are equal and only the kind part differs (`csv_v1:units` for `sqlite_v1:units`); for an original with a default name (a path that no longer exists) the stand-in presents the original's whole id. Refuse an id that does not parse (3.1) |
| What it presents | the original's id in `source_id`, the source-id system tag and every token; its own record ids (they never depended on the id); its own schema, which must hash equal to the slot's (`bind` checks it as today) |
| What is recorded | in the ledger (section 5), per read: presented source id, record id, row hash, and the reader's actual kind, version and location, and that it read as a stand-in. Provenance stays truthful through the ledger, not the token |
| Exactness | at the run's source check (ITL-1102), before anything is computed: every record read must hash equal to the ledger's latest row for that record id; a changed hash refuses the run naming the records; a record the ledger lacks is appended (sources grow); a record missing from the read changes nothing (a restriction reads fewer rows) |
| No ledger for the original (it was never read with a ledger) | **refuse**, unless the user passes `unverified=True`, which records the stand-in as unverified in the ledger. "When in doubt, refuse": nothing proves the rows equal |
| A library upgrade (`delta_v1` to `delta_v2`) | the same operation: `src.stand_in_for("delta_v1:units")`, accepted when the rows hash equal. Without it the source is `delta_v2:units`, a new source: its downstream nodes get new content hashes and new pipeline rows (function results are still reused when data is equal: memo by input data hash) |

Measured (`stand_in_explicit_kind`):

| | Source ids | Node content hash | Function calls (second source) | Stored pipeline rows |
|---|---|---|---|---|
| No stand-in: `arrow_table_v1:subjects`, then `csv_v1:subjects` | differ | differ | 0 | 6 (2 sets) |
| CSV stands in for `arrow_table_v1:subjects` (rows verified: 3 same, 0 changed) | equal | equal | 0 | 3 (1 set) |

### 3.5 When a version is bumped

The data function type's test, applied to a reader: **would a row read before the change
read as the same row after it (same values, same Arrow types, same record id)?** If not for
some row, bump `N`. A bump that changed no row costs a stand-in (verified, so no data moves);
a missing bump is caught by the ledger as changed hashes under known record ids (R2). The
version is a constant beside `SOURCE_KIND`; a test can pin it per kind with a golden ledger
of a fixed file.

### 3.6 Saved jobs

| Question | Recommendation |
|---|---|
| What a source's config stores | the name, the kind and the version as separate fields (`"name": "units", "source_type": "delta", "version": 1`), and, for a stand-in, the presented id; never one string to be parsed back |
| Loading under a newer version of the kind (saved 1, installed 2) | the loaded source is `delta_v2:units`, another source: the load reports the mismatch (saved and installed versions, per source) and refuses to bind it into the saved slot as the same identity; the explicit stand-in is the way through |
| Jobs saved before the change | none in use (no users before 0.4.0): refused with a message, no migration |

### 3.7 What moves

`golden_moves`, 2,411 entries computed (frozen-file: in files 0.4.0 will freeze;
provisional: outside the freeze):

| Variant | Moves | schema | datagram | pipeline | torch | frozen-file / provisional |
|---|---|---|---|---|---|---|
| **K_always** (the design: class name out, every id prefixed) | **703** | 120 | 108 | 472 | 3 | **355 / 348** |
| K_defaults (class name out, defaults only prefixed) | 252 | 100 | 34 | 117 | 1 | 165 / 87 |
| C (class name out only) | 199 | 51 | 34 | 113 | 1 | 112 / 87 |

No `semantic` or `arrow` entry moves in any variant. The full list is
`golden-moves-K_always.txt`; the largest groups by case: `rejoin_arrangement` 108,
`hash_in_key_order` 64, `rejoin_job` and `rejoin_job_async` 33 each, `job` 32,
`polars_filter_expr_job` 31, `rejoin` 27, `drop_nulls_job` 25, `group_by_arrangement` 24,
`arrow_source` 24, `row_identifiers` 18. A source id enters system-tag values, tokens and
row hashes, so every case that reads a source's rows moves, not only those that hash a source.

| Also moves | |
|---|---|
| Stored paths | `CachedSource` tables, content-scoped `instance:` tables, a plain `DerivedSource`'s default id; pipeline-table rows (`__node_content_hash`, record ids built from system tags); token columns in result tables |
| Saved job files | the source config gains `name` and `version` (3.6) |
| Documentation | every sample that prints a source id or a token |
| Cost before 0.4.0 | the developer's approval of the exact list (`--remove`, then regenerate); v1 is open, a pre-release freezes nothing (RELEASING.md) |
| Cost after 0.4.0 | a new data context version, in a minor release |

### 3.7a The implementation's moved list (for the developer's approval)

`generate_golden_hashes.py --check` on the implementation, before main's Filter cases were
merged in: **659 entries move**, every one listed with its pinned and live value and its explanation in
`golden-moved-implementation.tsv` beside the scripts.

| Group | Frozen-file | Provisional | Total |
|---|---|---|---|
| schema | 118 | 12 | 130 |
| datagram | 93 | 15 | 108 |
| pipeline | 130 | 288 | 418 |
| torch | 0 | 3 | 3 |
| semantic, arrow | 0 | 0 | 0 |
| **all** | **341** | **318** | **659** |

| Why it moved | Entries | Checked how |
|---|---|---|
| source id text (a source id, a source-id system-tag value): the kind part is added | 424 | mechanically: the live value is the pinned one with `<kind>_v1:` in front, or a default's `<kind>:` prefix become `<kind>_v1:` |
| content hash of a source or of a stream or node downstream of one (class name out, source id holds the kind part) | 200 | every one is a `content_hash`; no `pipeline_hash` moved |
| folded system-tag value (`batch_v1` / `group_v1`) over members' source ids | 28 | the fold label is unchanged; its input is the members' source ids |
| `node.as_source()` default id: `derived_v1:` plus the origin's (moved) content hash | 2 | prefix checked |
| pipeline-table record ids (built from system-tag values) | 5 | the table paths (pipeline hashes) are unchanged |

Against the simulation (`K_always`, 703): 54 simulated moves do not happen, all in job cases
(`job`, `polars_filter_expr_job`, `drop_nulls_job`, `rejoin_job`, `rejoin_job_async`),
because a slot is named by the source's name, not its full id (Decisions, 9); 10 entries move
that the simulation did not (`source_stream_builder`), because that golden case now passes the
builder a kind part, as every source does.

Approved and regenerated (developer, November 23, 2026). The 659 entries above were approved
as listed. After main was merged in (021a565e), `--check` listed those 659, with the same
values, and 25 more, from the provisional Filter golden cases main added in PR 498
(`filter.keyword`, `filter.terms`, `filter.tree`, `filter_job`, all in
`provisional/pipeline.json`): 16 source id texts (`dict_v1:` added), 8 content hashes and 1
pipeline-table record ids, no pipeline hash. The developer approved the 25 too; they are the
last 25 rows of the TSV. All 684 were removed with `--remove` and written again by the
generator. `scripts/verify_regenerated_golden.py` compares every golden entry before and
after: each approved entry was pinned with its TSV `pinned` value and now holds its TSV
`live` value, every other entry is unchanged (output: `golden-regeneration-verification.txt`).
Four regenerated entries are now in `pipeline-02.json` instead of `pipeline-01.json`, because
the generator writes into a group's newest file; no v1 file is released.

### 3.8 How the design meets the requirements

| | |
|---|---|
| R1 | met with the explicit stand-in and the class name out (measured, 3.4) |
| R2 | met by the ledger's row hashes (section 5); the version bump is the declared signal |
| R3 | the id names the reader on every ordinary read; under a stand-in, the ledger does |
| R4 | the kind part is the one decided implementation input; the class name leaves |
| R5 | the name is kept exactly; the full id is no longer what the user typed (reversal, decided) |
| R6 | the id costs nothing; the ledger's per-row hashing costs 33 us/row today (1.8) |

## 4. The other alternatives, compared with the developer's design

The developer's design (section 3, "K") is the central one. The alternatives below were
worked out first, under the earlier principle (the kind not a necessary part of an id);
they stay for comparison. The ledger (D) is a part of K, described in section 5; it is listed
here also on its own.

Golden moves are measured by `golden_moves` (2,411 entries computed; "frozen-file" entries
are in files the 0.4.0 release will freeze, "provisional" ones are outside the freeze; the
full lists are `golden-moves-<variant>.txt` beside the scripts).

| | Moves | semantic | schema | arrow | datagram | pipeline | torch | frozen-file / provisional |
|---|---|---|---|---|---|---|---|---|
| **K** (section 3: class name out, every id `<kind>_v1:<name>`) | **703** | 0 | 120 | 0 | 108 | 472 | 3 | **355 / 348** |
| K_defaults (class name out, defaults only) | 252 | 0 | 100 | 0 | 34 | 117 | 1 | 165 / 87 |
| A | 94 | 0 | 84 | 0 | 0 | 10 | 0 | 92 / 2 |
| B (class name + version) | 199 | 0 | 51 | 0 | 34 | 113 | 1 | 112 / 87 |
| B' (kind + version, no class name) | 199 | same | | | | | | 112 / 87 |
| C (class name out) | 199 | 0 | 51 | 0 | 34 | 113 | 1 | 112 / 87 |
| D (ledger) alone | 0 | | | | | | | new pins for the row hash only |

Every change to the root identity tuple moves the same 199 entries (every case that hashes
a source or anything downstream of one): the cost of B and C is the same; only what they
mean differs. K moves more because a given id changes too, and a source id enters every
system-tag value and token, so every case that reads a source's rows moves.

### A. Implementation version in the default id (`delta_table.v2:<path>`)

| | |
|---|---|
| R1 | no better than today: the class name still forces the kind into the content hash; a default that carries a version also differs between versions of one kind |
| R2 | only for default ids, and only if the implementer bumps the version; a given id never sees it |
| R3 | partly: the id says which version, until a stand-in is given that id |
| R4, R5 | R5 holds (given ids untouched); the version is a decided input |
| R6 | free |
| Pros | readable; cheap; no identity change for given ids |
| Cons | does not answer the question for given ids (most real pipelines); makes defaults less stable; a version bump changes every record's tokens and every downstream hash even when no row reads differently |
| Moves | 94 golden entries (default-id cases); stored paths of default-id sources' downstream nodes |
| User sees | new default ids; recomputation of nothing (memo by data), but new pipeline rows |
| Failure modes | an implementer forgets to bump; a bump that changed nothing still renames every record |
| Principle | allowed (a default's choice), but puts more implementation into defaults |

### B. A declared version beside `SOURCE_KIND`, in the content hash, not the id

| | |
|---|---|
| R1 | **worse**: the kind (and now its version) is forced in twice; two versions of one kind cannot stand in for each other |
| R2 | coarse: every node downstream gets a new content hash after a bump, whether or not any row changed; which rows changed is not said |
| R3 | no record of a read; the hash says only "some version" |
| R4 | the version is a decided input; fine |
| R5, R6 | hold |
| Pros | simple; follows the data function type's major |
| Cons | against the guiding principle; a bump is all-or-nothing; the data function model does not carry over (a function's major says what a value MEANS; a reader that reads the same rows produced the same data) |
| Moves | 199 (112 frozen-file, 87 provisional) |
| Failure modes | forgotten bumps; needless bumps re-address every stored pipeline row |

### C. Identity is the logical source: the class name leaves the identity

Root identity = `(Arrow schema identity, source_id, key columns)` for every root source,
with each kind's own decided additions (`tags_as_data` mapping). Kind and implementation
are recorded elsewhere (D, or the run summary).

| | |
|---|---|
| R1 | **met**: measured with `stand_in_job --without-class-name`: one node content hash, one set of stored pipeline rows, 0 recomputations, for all six kinds, bound or in a fresh job |
| R2 | not by itself: a reader change stays silent (1.7) |
| R3 | not by itself: a stand-in given the original's id writes tokens naming the original |
| R4 | removes an implementation detail from every hash |
| R5, R6 | hold; free |
| Pros | the principle holds everywhere; one rule for all kinds; `CachedSource` and content-scoped tables follow automatically (same path for every kind) |
| Cons | two different sources given the same id, schema and key are one address. That is the stand-in contract itself, and it already holds today within one kind (two CSV files given one id). Without D nothing checks that a stand-in really holds the same rows |
| Moves | 199 golden entries (112 frozen-file, 87 provisional); stored paths: `CachedSource` tables and content-scoped `instance:` tables; `__node_content_hash` values of existing pipeline rows (rows stored before the change are not seen as the current node's) |
| User sees | nothing until a stand-in; then reuse instead of a second copy |
| Migration | none needed before 0.4.0 (no users; v1 open); after 0.4.0 a new data context |
| Failure modes | a user reuses an id for different data: not caught today either (a content hash is an address, not a digest of rows), within one kind or across kinds; D is what catches it |
| Uniformity | make `PollingSource` follow it: `(schema, source_id, key columns)`, its default id built from `impl.identity()` as today; it is provisional (ITL-1316), so outside the freeze. A plain `DerivedSource` stays named by its origin (a computation's output is named by the computation) until ITL-956 |

### D. The record-keeping ledger

Per source, a stored table, one row per (record, observed row content):

| Column | Type | Meaning |
|---|---|---|
| `source_id` | `large_string` | as the source presents it |
| `record_id` | `large_string` | the record-id system tag value |
| `row_hash` | `large_binary` | `arrow_v1` of the record's user columns, prefixed digest (5.1) |
| `kind` | `large_string` | kind of the source that actually read it (under a stand-in, not the presented one) |
| `implementation_version` | `int64` | a declared reader revision of that kind (new constant) |
| `location` | `large_string` | what the source says it read (path, table, `@<table id>`, cursor or snapshot version), as text |
| `observed_at` | `timestamp[us, tz=UTC]` | when this content was first seen |
| `run_id` | `large_string` | the run that saw it (uuid7; not a hash) |
| `stand_in` | `bool` | read through an explicit stand-in (K, 3.4); with `unverified=True` recorded as such |
| `__data_context` | `large_string` | as every stored table |

Prototype (`ledger_prototype`), ledger written by the `ArrowTableSource`:

| Read compared with the ledger | same | changed | new | missing |
|---|---|---|---|---|
| each of the six kinds, same rows | 3 | 0 | 0 | 0 |
| CSV with inferred (non-null) types, hashed as read | 0 | **3** | 0 | 0 |
| the same, hashed in the slot's declared form | 3 | 0 | 0 | 0 |
| a reader change (one value) | 2 | **1** | 0 | 0 |
| positional record ids, rows reversed | 1 | **2** | 0 | 0 |

| | |
|---|---|
| R1 | checks it: a stand-in is exact when every record it reads hashes equal to the ledger's latest row for that record. Alone (without C) the class name still splits content hashes, so the stand-in is checked but not shared |
| R2 | **met**, per record, exactly (a changed hash under a known record id is certain) |
| R3 | **met**: kind, version and location of every observation are stored |
| R4 | the row hash ignores column order (`hash_table`); record ids are explicit; ledger row order is irrelevant |
| R5 | holds |
| R6 | **not met with today's per-row hashing**: 33 us/row, 23x a read of 1M rows. Needs a vectorized per-row digest in starfix, or hashing only when the source's modified time moved (the ITL-1102 check already decides that) |
| Pros | answers R2 and R3 without putting anything into identity; serves provenance (scenarios 4 and 7 of ITL-1421); small (one row per changed record, about 100 bytes) |
| Cons | a new stored table kind and a new hash site; a read hashes every row; a policy for mismatches is needed |
| Moves | no existing golden entry; the row hash gets new pins; a new layout `ldb_v1` in `CURRENT_LAYOUTS`; the hash site inventory |
| User sees | a refusal naming records when a source reads differently from before, with an explicit way to accept |
| Failure modes | cost on very large sources; a ledger shared by two jobs (concurrency, 5.6); positional ids make reorders look like changes (correct under today's rules, but noisy) |

### E. Combinations, and how each compares with K

| Design | R1 | R2 | R3 | Kind in the id | Moves | Compared with K |
|---|---|---|---|---|---|---|
| **K** (section 3, with the ledger) | met, by an explicit verified stand-in | met | met: the id names the reader; the ledger under a stand-in | always, explicit | 703 + ledger pins | the developer's direction |
| K_defaults + D | met; a given id is the user's choice | met | partly: a given id need not name the reader | defaults only | 252 + pins | keeps ITL-957/ITL-1154 for given ids; the id does not always name the implementation |
| C + D | met, with no operation: same name is the same source | met | met through the ledger only | defaults only | 199 + pins | the stand-in is implicit; a version upgrade is silent in identity, caught only by row hashes |
| C alone | met, unchecked | no | no | defaults only | 199 | |
| D alone | checked, not shared (class name) | met | met | defaults only | pins | the class name still forces the kind |
| B + D | no | met | met | hidden in the hash | 199 + pins | the kind is forced in, invisibly |
| A + C | met for given ids | defaults only | no | defaults, with version | 199 + 94 | half of K, without the stand-in |

K and C + D differ in one choice: whether replacing the reader is an explicit, recorded act
(K) or implicit when the names agree (C + D). K is stricter: under C + D a reader upgrade or
another kind under the same name changes no identity and is seen only if a row reads
differently; under K it is a different source until the user says, and the ledger proves,
that it stands in.

### The default-id decision (ITL-957, ITL-1154), under K

ITL-957 and ITL-1154 decided that default ids are readable and name their kind first. K keeps
that and extends it: the kind part is now explicit (`<kind>_v<N>`), carries the version, and
is added to given ids too. Two consequences: a default name no longer repeats the kind
(`csv_v1:<path>`, not `csv_v1:csv:<path>`), and the rule "a given id is used exactly as
given" becomes "a given name is used exactly as given" (R5).
## 5. The ledger, concretely

Not implemented here: filed as ITL-1426 (deferred). This section is its design.

Under K the ledger is what makes a stand-in verifiable and an implementation change
visible; its columns are those of alternative D (section 4).

### 5.1 What a row hash covers

| Question | Answer (recommended) |
|---|---|
| Columns | the user tag and data columns (data-tags once), nothing else: not the system tags (the source id and record id are the ledger's key; `CachedSource` includes them today, so its hash changes with the id), not source info, context or meta |
| Types | the slot's **declared form** (the Arrow schema the pipeline is built on, after large normalization): measured, the inferred CSV hashes differently as read (nullability enters `hash_table`) and equal in the declared form. Outside a job, the source's own Arrow schema |
| Nulls, floats, logical types | as `hash_table` does for Orcapod's tables: nulls hashed with validity, the float rule on, a logical type by its identifier and value hash |
| Label | `arrow_v1` (`hash_table` of a one-row table); stored with its label, `to_prefixed_digest()`. A new label only if the procedure differs from `hash_table` (a vectorized per-row digest would be a new procedure, so a new label: `arrow_row_v1`) |
| Per row or per table | per row: a mismatch must name the record |

### 5.2 Why exact Arrow types

The row hash and the function memo both hash Arrow types, nullability included. A stand-in
that reads `int32` where the original read `int64`, or a CSV whose types are inferred, is a
different datum unless brought to the declared form. Today's rules already give the route:
`CSVSource(schema=...)` reads each cell as its declared type (ITL-1322), and `bind`
presents a top-level non-null source in the slot's nullable form (`_DeclaredForm`). A
narrower width is refused (measured). So: the ledger hashes the declared form, and a
cross-kind stand-in declares its schema in Arrow.

### 5.3 Positional record ids

Measured: reversing three positional rows reports 2 of 3 records changed. That is the
truth under today's rule (a position is the record's name: "reorder is a change when
record ids are positional"), so the ledger need not require key columns. It should say so
in the refusal ("this source names its records by position; give it `record_id_columns`"),
and a stand-in across kinds should be keyed (a database may not return rows in file order).

### 5.4 What a mismatch does

| Observation | Recommended action | Why |
|---|---|---|
| same hash | nothing written | |
| new record id | append | sources grow |
| record missing from this read | nothing (the ledger keeps history) | a restricted read (`where`, `limit`) legitimately reads fewer rows |
| **changed hash under a known record id** | **refuse** the run before anything is computed, naming the source, the count and the first record ids; accept explicitly (`job.run(accept_source_changes=["subjects"])` or a ledger method), which appends the new hashes as the current content | a changed hash is exact and certain ("refuse only what is certain"), and "when in doubt, refuse" |
| kind, version or location differs, hashes equal | append an observation (R3), no refusal | an exact stand-in is what R1 allows |
| a stand-in (3.4) whose original has no ledger | refuse, unless `unverified=True` (recorded) | nothing proves the rows equal |

### 5.5 Where it lives and what reads it

| Question | Recommendation |
|---|---|
| Database | the job's database (as pipeline and result tables) |
| Path | `<job>/source_ledger/node:<source content hash>/ldb_v1`. Under K the content hash is that of the presented id, so a stand-in reads and extends its original's ledger, and a new version without a stand-in starts its own |
| Layout | a new `ldb_v1`, added to `CURRENT_LAYOUTS`; not `sdb_v1` (that is a full copy with another hash) |
| `CachedSource` | stays a separate feature (availability: keeps rows a source may lose). Later it can take the ledger's row hash instead of its own `_record_hash` (a stored-schema change: `sdb_v2`) |
| Who reads and writes | `job.run()`, `run_async()` and a job node's `run()`, at the source check (ITL-1102, `node_run.check_sources`), for each source a run reads; `bind` does not read rows (it stays a schema check); `head()` and the preview never |
| When it hashes | when the run reads the source: if the source's modified time did not move since the last ledgered read in this process, nothing is hashed again |

### 5.6 Concurrency and size

| | |
|---|---|
| Writes | append-only; one row per (record, new content). Two jobs appending to one Delta table meet only at commit, where Delta's optimistic commit lets blind appends both succeed; the current content of a record is its latest `observed_at` |
| Size | about 100 bytes a row (record id, 44-byte prefixed digest `arrow_v1:` + 35 bytes, short text): 1M records about 100 MB uncompressed, written once; then only changed records |
| Read | one table scan per run per source, or a lookup by record id |

### 5.7 The provenance showcase (ITL-1421)

| Scenario | What the ledger gives |
|---|---|
| 4. Reproduce and prove a result | the row hash of every traced source record (source id, record id from the tokens): a manifest can list them, and a verification re-reads and compares |
| 7. New data arrived | which records are new and which changed since the last run, per source, before anything runs; with the function memo, which nodes would recompute |
| 1, 2. Trace back and forward | the kind, location and version that actually read a traced record (R3) |

## 6. Interactions

| With | Under K |
|---|---|
| `DerivedSource` plain, `as_source()`, ITL-956 | id `derived_v1:semantic_v1:<hex of the origin's content hash>`; identity stays the origin's hash until ITL-956 gives it system tags and tokens (new record ids); add it to the ledger after ITL-956 |
| `DerivedSource` `tags_as_data` | `derived_v1:<given name>` (a source id is still required, ITL-1313), root identity plus the mapping; provisional (ITL-1317), outside the freeze |
| `PollingSource` | takes the root identity (3.3); default `polling_v1:semantic_v1:<hex of impl.identity()>`; a cursor or snapshot version is its ledger `location`; provisional (ITL-1316), outside the freeze |
| `SpiralDBTableSource` | `spiraldb_v1:<project>.<dataset>.<table>@<table id>` by default; a re-created table (new table id) under a given name stands in after verification |
| `CachedSource` | the wrapped source's id and identity; one cache table per presented source. It stays a full cache (availability); it may later take the ledger's row hash instead of its `_record_hash`, which today includes the system tags (1.6) |
| `bind` | unchanged: a schema check; it never reads rows. The stand-in is made on the source before binding. Fix first the bug of 1.4 (a bind leaves downstream content hashes stale) |
| Saved jobs | name, kind and version as separate fields; a version mismatch on load is reported, and the stand-in is the way through (3.6) |
| The run's source check (ITL-1102) | where the ledger is compared and appended, before anything is computed |
| Provenance showcase (ITL-1421) | scenario 4 (prove a result) takes the traced records' row hashes from the ledger; scenario 7 (new data) takes new and changed records from it (5.7) |
| Data context and the freeze | v1 is open until 0.4.0; a pre-release freezes nothing (RELEASING.md, ITL-1329); 0.4.0 freezes. K moves 355 frozen-file and 348 provisional entries: the developer approves the exact list (`golden-moves-K_always.txt`), then `--remove` and regenerate. After 0.4.0, K needs a new data context version. The stand-in operation and the ledger move no existing value (new pins only) and can land in any release |

## 7. Recommendation (as written before the decisions; see "Decisions" at the top)

**Adopt K (the developer's direction) with the ledger.** Before 0.4.0, land everything that
moves a hash: the class name out of the source identity, the kind part in every source id,
`PollingSource` on the root identity. After 0.4.0 (or before, if time allows; they move no
existing value), the ledger and the explicit stand-in.

### Decisions for the developer

| # | Decision | Recommendation | Commits to |
|---|---|---|---|
| 1 | Confirm K against its moved list | confirm: 703 entries, 355 frozen-file and 348 provisional (`golden-moves-K_always.txt`) | the reversal of "a given id is used exactly as given" (ITL-957, ITL-1154) to "a given name is used exactly as given" |
| 2 | Kind words | one vocabulary for the kind part and the config type key; choose short words (`delta`, `sqlite`, `postgresql`, `spiraldb`, `csv`, `polling`) or today's `SOURCE_KIND` (`delta_table`, ...). Recommended the short words; **decided: today's `SOURCE_KIND` words** (Decisions, 2) | every default id and saved config; no extra golden move beyond decision 1 |
| 3 | A given id that already has a kind-part shape | refuse (3.2) | users cannot name a source `x_v2:...` |
| 4 | Parsing | the kind part never holds `:`; split at the first `:`; the name may hold `:`; no `::`, no trailing `:`, no empty name | one parser, used by the saved-job reader and the ledger |
| 5 | Class name out of the identity; root identity `(Arrow schema identity, source id, key columns)` for every root source, `PollingSource` included | adopt | part of decision 1's list |
| 6 | Version bump rule | bump when a row read before could read differently after (3.5); a constant beside the kind | a per-kind constant and a test pinning it |
| 7 | Stand-in API | `source.stand_in_for(original_source_id, *, unverified=False)`, returning a copy; checked at the run's source check against the ledger; refuse on a changed record; refuse without a ledger unless `unverified=True` | public API; a new error |
| 8 | Ledger | a new stored table `ldb_v1` in the job database, keyed by the presented source's content hash; row hash of the user columns in the slot's declared form, `arrow_v1` per one-row table now, a vectorized per-row digest in starfix (new label) before it is on by default (R6: 33 us/row today, 23x a 1M-row read) | a new table kind, a new hash site, new pins, a starfix request |
| 9 | Mismatch policy outside a stand-in | changed record under a known record id: refuse, explicit accept appends; new: append; missing: nothing (5.4) | a run argument (public API) |
| 10 | Saved jobs | name, kind, version as separate fields; refuse jobs saved before the change | the job file format |
| 11 | `CachedSource` | stays a full cache now; may adopt the ledger row hash later (`sdb_v2`) | nothing now |
| 12 | The bind bug of 1.4 | fix separately, before 0.4.0 | a Linear issue |

### Before 0.4.0, and after

| Before 0.4.0 (moves golden values) | After 0.4.0, or earlier if ready (moves none) |
|---|---|
| decisions 1-6 and 10: kind part in every id, class name out, `PollingSource` identity, version constants, saved-job fields; decision 12 (the bind bug); documentation of the id anatomy | decisions 7-9: the ledger and the explicit stand-in (until then a stand-in across kinds or versions is impossible: under K a different kind part is a different source); ITL-956 and then the derived source in the ledger; decision 11 |
