# ITL-897: pandas DataFrame hash ignores column order (step 1: investigation and proposal)

Status: proposal, awaiting the coordinator's decision. No library code is changed.
Base: `main` at `b197544e` (v0.2.0 + #291), pandas 2.2.3, pyarrow 23.0.1, Python 3.12.

Developer decision (November 11, 2026): order is identity wherever the type's own equality
depends on order. Only `dict` and Orcapod streams ignore order, deliberately. Added the same day:
a `pa.Table` used as a value must be order dependent too.

Every claim below has a script in `superpowers/scripts/itl_897/`, run with
`uv run python superpowers/scripts/itl_897/<script>.py`. `prototype_v1.py` holds the prototype
hashes; the scripts patch it into their own process only.

| Claim | Script |
|---|---|
| (a) pandas equality vs the hash | `claim_a_equality_vs_hash.py` |
| (b) storage keeps order; the cache does not | `claim_b_storage_roundtrip.py` |
| (d) what a v0.2.0 store sees | `claim_d_migration.py`, `claim_d_source_identity.py` |
| (d) golden entries that move | `claim_d_golden_diff.py` |
| (f) paths by which a `pa.Table` reaches the hasher | `claim_f_table_value_paths.py` |
| (g) `pa.Table` equality vs the hash | `claim_g_arrow_table_value.py` |

## How a pandas value is hashed today

`PandasDataFrameHandler` converts the frame with `pa.Table.from_pandas(df, preserve_index=True)`
and returns the context Arrow hasher's `hash_table` of that table (label `arrow_v0.1`).
`PandasSeriesHandler` does the same for `series.to_frame(name)`, with the sentinel
`__pandas_series_unnamed__` for an unnamed Series. starfix, underneath the Arrow hasher, ignores
field order and struct field order. It does not hash the `pandas` schema metadata, which holds
`columns.name`, the original label types and the pandas dtypes.

A pandas value in a data column is stored as `extension<pandas.dataframe>[large_binary]` (Arrow
IPC bytes). When a datagram is hashed, the Arrow hasher's visitor decodes the value and calls the
pandas handler, and the row's data hash embeds the token `<extension>::<method>:<digest>`. So the
pandas value hash reaches every row-level key that covers that column: input data hashes,
function-pod result keys, record IDs, operator-node row hashes, and a source's default
`source_id`.

## (a) What pandas equality depends on, and whether the hash does

`equals` is `DataFrame.equals` / `Series.equals`. `assert` is `assert_frame_equal` /
`assert_series_equal` with default arguments. "Hash equal" means today's hash is the same for
both values. "Stored" means the IPC round trip keeps the property.

**DataFrame**

| Property changed | equals | assert | hash equal | stored | verdict |
|---|---|---|---|---|---|
| **column order** | differ | differ | **yes** | yes | **collision** (this issue) |
| row order | differ | differ | no | yes | consistent |
| index values | differ | differ | no | yes | consistent |
| index name | equal | differ | no | yes | pandas split |
| index type (RangeIndex vs Index[int64], same values) | equal | equal | yes | yes | consistent |
| index dtype (int64 vs float64, same values) | equal | differ | no | yes | pandas split |
| MultiIndex level names | equal | differ | no | yes | pandas split |
| **MultiIndex level order** (`swaplevel`) | differ | differ | **yes** | yes | **collision** (an order) |
| `columns.name` | equal | differ | yes | yes | collision vs `assert` |
| column label type (`0` vs `"0"`) | differ | differ | yes | yes | collision |
| MultiIndex columns vs flat labels with the same text | differ | differ | yes | yes | collision |
| column dtype (int64 vs float64; int64 vs int32) | differ | differ | no | yes | consistent |
| categorical vs object string (ITL-887) | differ | differ | yes | yes | collision |
| categorical category order | equal | differ | yes | yes | collision vs `assert` |
| categorical unused category | differ | differ | yes | yes | collision |
| categorical `ordered` flag | differ | differ | yes | yes | collision |
| nullable `Int64` vs `int64` (no nulls) | differ | differ | yes | yes | collision |
| nullable `boolean` vs `bool` (no nulls) | differ | differ | yes | yes | collision |
| `string` vs object str | differ | differ | yes | yes | collision |
| `Int64` with `<NA>` vs float64 with NaN | differ | differ | no | yes | consistent |
| datetime64 unit (ns vs us) | differ | differ | no | yes | consistent |
| datetime tz (UTC vs naive) | differ | differ | no | yes | consistent |
| `-0.0` vs `0.0` | equal | equal | no | yes | spurious miss |
| **struct field order** (`ArrowDtype` struct column) | differ | differ | **yes** | **storage raises** | **collision** (an order) |
| key order in an object column of dicts | equal | equal | yes | no (keys reordered) | consistent (dict) |
| `attrs` | equal | equal | yes | yes | consistent |
| `flags.allows_duplicate_labels` | equal | differ | yes | **no** | collision vs `assert` |

**Series**

| Property changed | equals | assert | hash equal | stored | verdict |
|---|---|---|---|---|---|
| name (`"a"` vs `"b"`; `None` vs `"x"`) | equal | differ | no | yes | pandas split |
| name `0` vs `"0"` | equal | differ | yes | yes | collision vs `assert` |
| **MultiIndex level order** (`swaplevel`) | differ | differ | **yes** | yes | **collision** (an order) |
| index name | equal | differ | no | yes | pandas split |
| dtype `int64` vs `Int64` | differ | differ | yes | yes | collision |
| categorical vs object | differ | differ | yes | yes | collision |
| `attrs` | equal | equal | yes | yes | consistent |

"pandas split" means `equals` and `assert_frame_equal` disagree and the hash sides with
`assert_frame_equal`. Rows in bold are orders, which the developer's rule settles: column
order, MultiIndex level order (frame and Series) and struct field order are identity. The other
mismatches are listed for a decision; this proposal does not decide them.

Edge values: a frame with duplicate column labels cannot be hashed or stored on `main`
(`ValueError`). A frame with labels `1` and `"1"` hashes and stores, but the round trip gives
`["1", "1"]`, so the stored value is not the value that was hashed. An `ArrowDtype` struct column
hashes but cannot be stored (`ValueError` from `to_pandas`). These are out of scope (see "Out of
scope findings").

## (b) Storage keeps column order; the cache key does not

`LogicalPandasDataFrame.python_to_storage` writes `pa.Table.from_pandas(df, preserve_index=True)`
as an Arrow IPC stream into `large_binary`. The IPC schema lists the fields in frame order (data
columns, then index levels, e.g. `["b", "a", "__index_level_0__"]`), and the `pandas` metadata
lists the columns in the same order. `storage_to_python` (`to_pandas`) rebuilds the frame in that
order. So column order needs no storage change, and the stored bytes do not change.

Through the public interface (`make(reverse) -> df`, then `first(df) -> df.columns[0]`):

| Store | Orchestrator | Stored column order kept | `first` correct (one run, both rows) | `first` correct (warm cache, `rev` in a later run) |
|---|---|---|---|---|
| in-memory | sync | yes | **no**: `rev` gets `"a"` | **no** |
| in-memory | async | yes | yes (both rows race past the cache) | **no** |
| Delta | sync | yes | **no** | **no** |
| Delta | async | yes | yes (race) | **no** |

Under the prototype hash, every pandas value used in claim (a) (70 storable values) hashes the
same before and after the storage round trip, so a value read back from a store keeps its cache
key.

## (c) Proposal for the new hash

### Mechanism (reused from PR #286)

The same construction as `polars_frame_content_hash`:

```
pandas_dataframe_v1 / pandas_series_v1 = sha256(descriptor + b"\x00" + values_digest)
```

* `values_digest` is the starfix digest of the values. Recommended: the Arrow hasher's digest of
  `pa.Table.from_pandas(value, preserve_index=True)`, which is exactly what is hashed today (the
  visitor already handles pandas columns that Arrow types as extensions).
* `descriptor` is canonical JSON (keys sorted, `","` / `":"` separators, UTF-8, non-ASCII kept) in
  an Orcapod-owned grammar. It carries what starfix does not see. For the order rule it lists
  the fields in order: data columns in frame order, then the index levels in order, and for a
  struct-typed column its fields in order at every depth.
* Labels `pandas_dataframe_v1` and `pandas_series_v1`. The label names the whole definition and
  is the only version marker. Bytes under a label never change; a new definition gets `_v2`.
* PR #286 keeps `_content_hash` private in `polars_type.py`. This change should lift that
  function (descriptor, digest, label) into a shared helper used by both, rather than copy it.

### Which hashes move

The label changes from `arrow_v0.1` to `pandas_dataframe_v1` / `pandas_series_v1`, so **every**
pandas DataFrame and Series value hash moves: frames with one column and frames whose columns
are already sorted included. Whatever embeds a value hash moves with it (see (d)).

### A design that moves fewer, and why not to use it

There is one: keep today's hash (label `arrow_v0.1`, same digest) when the frame's fields are in
a canonical order (sorted labels, sorted index levels), and use the new labelled hash only for
other orders. It is collision-free for column order: two orders of one frame give different
hashes. With it, none of the 3 golden entries move, and only frames whose labels are not sorted
recompute. It is still not recommended:

1. The hash would depend on whether the labels happen to be sorted. "Sorted" would be defined
   on the stringified Arrow field names, the same names that cause the `0` vs `"0"` collision.
   With `preserve_index=True`, `__index_level_0__` sorts before every lowercase label, so the
   canonical-order test has to exclude index fields and then handle MultiIndex levels separately.
2. Values hashed by the old definition keep the `arrow_v0.1` label, so old and new hashes cannot
   be told apart. That breaks the versioned-label convention.
3. Today's definition would be frozen for sorted frames. It ignores `columns.name`, label types,
   categorical vs string (ITL-887) and nullable dtypes, so fixing any of those later moves the
   hashes anyway. The saving only lasts until the next fix.

### Scope of `v1`: decide the other mismatches now

Each later change to the definition moves every released pandas hash again. Recommended: decide
now which of the non-order mismatches in (a) `pandas_dataframe_v1` covers, so released pandas
hashes move once. Candidates the descriptor can carry: column label types (from the `pandas`
metadata), `columns.name`, pandas dtypes (`Int64` vs `int64`, `string` vs object, categorical
with categories, their order and `ordered`; this would settle ITL-887), MultiIndex columns,
`flags`. If the decision is "order only", the minimal descriptor is the ordered field list
(the prototype).

## (d) Migration assessment

What a v0.2.0 store sees after the change (`claim_d_migration.py`: two processes, one Delta
store; `make(bool) -> df`, `first(df)`, `height(df)`):

* **Recomputes.** A function pod whose input row holds a pandas value recomputes that row once
  (`first`, `height`). A pod whose inputs hold no pandas value is a cache hit even if it outputs a
  frame (`make`). After the first new run, everything hits the cache again.
* **Old rows stay and nothing looks dropped.** Pipeline and result tables keep their v0.2.0 rows
  and gain the new ones (`first`: pipeline table 2 to 4 rows, result table 1 to 3). Table paths
  do not change (pipeline hashes do not depend on values). `iter_data` yields exactly the current
  rows (2 per node), with no duplicates and none missing. A v0.2.0 process still finds its own
  rows. Stored frames (IPC bytes) and stored schemas do not change.
* **The wrong result is replaced.** The collided v0.2.0 result (`rev` gets `"a"`) remains in
  the store as an orphaned row; the new run computes `"b"`.
* **Ephemeral upstream.** With `make` ephemeral, the second session recomputes `make` (the
  ephemeral results are gone), then `first` and `height` under the new keys. The stored
  `__output_data_hash` of v0.2.0 rows (old format) is read only by the `EmptyData` flow-through
  path (`function_node.py:2032`). In the scripted runs that path did not serve an old key. I did
  not build a case where it does, so this is not proven.
* **Sources without an explicit `source_id`.** The default `source_id` is the table hash. For a
  source holding a pandas column it changes, so `_tag_source_id` and every `_tag_record_id`
  change (`claim_d_source_identity.py`). Every downstream entry of such a source is then new,
  including pods that never read the pandas column. With an explicit `source_id`, only the row
  data hashes move. This is the widest effect, and the release note should say so.
* **Golden entries** (`claim_d_golden_diff.py`): 3 of 608 move, all pandas:
  `semantic.json` `value.dataframe.basic` and `datagram.json`
  `dataframe.row[{"k":"r1"}].data_hash` and `dataframe.row[{"k":"r2"}].data_hash`. The source
  `content_hash`, `pipeline_hash`, record IDs, schema and type hashes do not move. There is no
  pinned pandas Series entry; the fix should add Series and reordered-column cases (additions
  only).
* **Detecting an old value.** A pandas value hash computed directly carries its label
  (`arrow_v0.1` vs `pandas_dataframe_v1`), so it can be told apart. A stored key cannot: input
  data hashes, record IDs and output data hashes are `arrow_v0.1` digests over the row, and the
  value's label is hashed into them. A tool could recompute the old key for a stored output,
  because the old definition stays computable from the stored frame. It cannot do so for a pod
  input, because pipeline tables store tags and provenance, not input values. No automatic
  repair is proposed.

## (e) Build on PR #286 or on `main`

Shared code: the labelled-hash construction (`_content_hash` in `polars_type.py`), the shared
IPC helpers (`arrow_ipc.py`, which also edits `pandas_type.py`), the label convention in
`docs/reference/hashing.md` §1c and `CLAUDE.md`, the `[0.3.0] — unreleased` CHANGELOG section,
and the polars golden cases. Built on `main` now, this change would conflict with #286 in
`pandas_type.py`, `builtin_handlers.py`, `hashing.md`, `CHANGELOG.md`, `golden/cases.py` and
`DESIGN_ISSUES.md`, and would either duplicate `_content_hash` or move it before #286 lands.

Recommendation: implement after #286 merges, branched from `main`, and lift `_content_hash` into
a shared helper in the same PR. #286 is open and mergeable with green checks. If it is not
merging soon, the alternative is to branch from #286's head and merge `main` once it lands; the
PR then shows #286's diff until then.

## (f) How a `pa.Table` / `RecordBatch` reaches the hasher: value vs stream

Two routes reach starfix:

1. **Type dispatch.** `semantic_hasher.hash_object(obj)` sends a `pa.Table` or `pa.RecordBatch`
   to `ArrowTableHandler`, which returns `arrow_hasher.hash_table(table)`.
2. **Direct calls to `arrow_hasher.hash_table`.** Record-ID and entry-ID preimages
   (`function_pod.py:198`, `function_node.py:1620/1659`, `side_effects.py:231`,
   `migrations/pipeline_db.py`), per-row hashes (`operator_node.py:829`, `cached_source.py:232`),
   a source's default `source_id` (`stream_builder.py:192`), and the pandas handlers.

Route 2 never sees a user value, only Orcapod's own tables, so it stays order independent as it
is. Route 1 is shared. With the handler instrumented over the full test suite
(`claim_f_table_value_paths.py`), its callers are:

CLAIM_F_TABLE

The library's own uses of route 1 are stream identities: `Datagram.identity_structure` returns
the data table (every `Tag` / `Data` content hash, so every input data hash and record ID), and a
producer-less `ArrowTableStream.identity_structure` includes `as_table(all_info=True)`. A user
value reaches route 1 only through `hash_object` on a table, or on a structure (dataclass,
container, identity structure) that contains one. `pa.Table` is not a registered logical type,
so it cannot be a pod input or output, and it cannot be stored in a column.

**How to separate them.** `ContentHash` is terminal in `hash_object` (returned as is at the top
level, embedded as is inside a structure). So `Datagram` and `ArrowTableStream` can return
`self.data_context.arrow_hasher.hash_table(table)` instead of the table. That is exactly what
the handler returns for them today, so their hashes stay byte-identical. `ArrowTableHandler` is
then the value-only path and can take the new definition. Measured with the prototype
(`claim_d_golden_diff.py`):

| Variant | Golden entries that move |
|---|---|
| `arrow_table_v1` in `ArrowTableHandler`, identities not separated | 105 of 608 (datagram 40, pipeline 53, schema 12) |
| `arrow_table_v1`, identities separated | 0 of 608 |
| pandas prototype plus separated `arrow_table_v1` | 3 of 608 (the pandas entries above) |

One difference: `Datagram.content_hash(hasher)` with an explicit hasher from another context
would use the datagram's own context's Arrow hasher for its table, not the one behind that
hasher's `ArrowTableHandler`. The two are the same in the default context.

## (g) Which released hashes move if table values become order dependent

What `pa.Table.equals` depends on vs the value hash (`claim_g_arrow_table_value.py`):

| Property changed | `Table.equals` | hash equal | verdict |
|---|---|---|---|
| **column order** | differ | **yes** | **collision** |
| **struct field order** (in a column; in a list column) | differ | **yes** | **collision** |
| `string` vs `large_string` | differ | yes | deliberate: large and non-large hash the same (developer, Nov 10) |
| row order, values, names, int width, nullability | differ | no | consistent |
| chunking | equal | yes | consistent |
| schema metadata | equal (differs with `check_metadata=True`) | yes | consistent |

A `RecordBatch` hashes like the one-batch `Table` with the same content; today both carry the
`arrow_v0.1` label.

With the separated design, the hashes that move are those of `pa.Table` / `RecordBatch`
**values** passed to the semantic hasher, and anything that embeds one. No golden entry pins
such a value. In the library, no stored key embeds one, because a table cannot be a pod value
and the library's own identities use route 2 or the separated route 1. A user's own object
whose identity structure holds a table would move.

Flagged, not decided: struct field order inside columns of Orcapod rows and streams. The row
hash (route 2) ignores struct field order, so a row whose struct column (for example a
dataclass value stored as a struct) has fields in another order has the same data hash. Streams
ignore column order on purpose; whether that extends to field order inside a column is a
separate decision.

## (h) Proposal for table values

* `ArrowTableHandler` returns `arrow_table_v1 = sha256(descriptor + b"\x00" + values_digest)`.
  The descriptor lists fields in order, with struct fields in order at every depth. The digest is
  the Arrow hasher's digest of the table.
* `Datagram.identity_structure` and the producer-less `ArrowTableStream.identity_structure`
  return `arrow_hasher.hash_table(table)`. They stay order independent and byte-identical.
* Open: one label for `Table` and `RecordBatch` (today they hash the same) or a kind field that
  separates them; whether `string` vs `large_string` stays equal under the value hash (the
  Nov 10 normalization rule says yes).

**One PR or two.** They share the labelled-hash helper and the doc section, so they should share
the helper. They differ in risk: the pandas change moves released hashes (3 golden entries, and
row keys in user stores). The table change, once separated, moves no pinned or stored hash, but
it changes `Datagram` and stream identity code, which every hash passes through. Recommended:
two PRs on one shared helper. The table PR goes first, or with #286, and carries the separation
and the helper; the pandas PR follows with its own golden change and migration note. If the
coordinator prefers one PR, the golden diff stays the same 3 entries.

## Decisions requested

1. Approve the labelled design (`pandas_dataframe_v1` / `pandas_series_v1`, every pandas value
   hash moves) over the sorted-order variant.
2. Scope of `v1`: order only, or also the non-order mismatches in (a), and which ones (ITL-887
   categorical among them).
3. Base: wait for #286 and branch from `main` (recommended), or branch from #286's head.
4. `pa.Table` values: approve the separation and `arrow_table_v1`; one PR or two; `Table` vs
   `RecordBatch`; struct field order inside stream columns (flag only).

## Out of scope findings (to report, not fix here)

* Frames with mixed label types that collide as strings (`1` and `"1"`) store and read back as
  `["1", "1"]`: the stored value differs from the hashed one.
* An `ArrowDtype` struct column hashes but cannot be stored (`to_pandas` raises `ValueError`).
* `flags.allows_duplicate_labels` is lost in storage.
* In the ephemeral run, `make`'s pipeline table grows by 1 row per session although 2 rows
  recompute (seen with the old hash too, so it predates this change).
* `CLAUDE.md` says `FunctionNode.iter_data()` Phase 1 returns all records in the shared table;
  in the migration runs it yielded only the current inputs' records.
