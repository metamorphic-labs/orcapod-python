# ITL-1250: GroupBy groups through Polars, builds from Arrow (plan)

Branch `eywalker/itl-1250-groupby-compares-keys-as-python-values`, from `origin/main` at
`a026d31f`. Status: approved by the coordinator on 2026-11-19 with the decisions at the end;
implemented.

Every number below was measured before the change, on `main`, against a prototype of the
design that replaced `GroupBy.unary_static_process` in the measuring process (the prototype
became the implementation and is not kept). The scripts in `docs/dev/scripts/itl_1250/`
measure the checkout they run in: `measure_keys.py` ((a), GroupBy against Polars),
`measure_data_values.py` (the data lists against the input), `timing.py` ((d), run on each
checkout) and `measure_batch_merge_join.py` ((f)).
Library versions: Polars 1.41.2 (locked; `pyproject.toml` allows `>=1.36.0`), PyArrow
23.0.1, pandas 2.2.3.

## Design

1. **Which rows group, in what order: one Polars sort.** A frame with synthetic column
   names (no user name can collide) holds the row number, the keys (`by`, in name order),
   the member-order tags (`member_order`, then the other non-key tags in name order), the
   record-id columns, then the source-id columns. One stable sort,
   `nulls_last=True, maintain_order=True`, gives the group order and the member order at
   once. Group boundaries are `pl.struct(keys).rle_id()`: Polars' own equality.
2. **Polars needs help with a few types**, prepared in Arrow, in the sort frame only:
   - an extension type becomes its storage; a dictionary is decoded (only until PR 455
     merges, which refuses dictionary tags);
   - `float16` is cast to `float32` (exact for order and equality; the emitted bits are
     read from the original column). Polars 1.41 has `Float16`, but the floor 1.36 may not;
   - `decimal256` (Polars panics: "operator does not support primitive `Int256`") becomes
     four 64-bit limbs (top limb signed), which order and compare as the values do;
   - nulls inside a list sort FIRST in Polars, against the rule (nulls last at every
     depth): a list-valued tag is sorted through a recursive key, `struct(is_null, value)`
     at every depth. Measured equal to main's order on every list case (below).
3. **The output is built from the original Arrow table**: `take()` in the sorted row order,
   then list arrays from group offsets (`LargeListArray.from_arrays` with the exact
   `aggregated_list_type`, or its storage then wrapped for a logical element). No data
   value passes through Python or Polars, so no `restore_column_types` is needed: nothing
   comes back from Polars but row numbers and group sizes. I would add that builder beside
   `build_aggregated_table` in `arrow_utils` (it takes arrays, not Python rows); `Batch`
   and `MergeJoin` could move to it later (see (f)).
4. **The emitted key**: a float key (float storage, logical type over a float) takes,
   per group, the member with the smallest bit pattern (the unsigned view of the original
   storage, `arg_min` per group in Polars); a null key through a null index. Every other
   key takes the group's first member: equal under Polars equality means identical
   values for those types.
5. **Folds**: system tags and a data-tag key's source info fold as today
   (`fold_system_tag_values`, `group_v1`), over the sorted strings; strings lose nothing
   in Python.
6. Validation, identity, config, `unary_output_schema`, the data-tag handling (ITL-1017,
   ITL-1070) and the async path (barrier, through `static_process`) are unchanged.

## (a) Grouping per key type: main (Python dict) against Polars

`measure_keys.py`: partitions of the row numbers.

| Key type | main | Polars | |
|---|---|---|---|
| int8/16/32/64, uint8/16/32/64 (min, max, 0, null) | | | same |
| float16/32/64: NaN, -NaN, payload NaN, signaling NaN, -0.0, 0.0, 1.0, null | all NaN one group; 0.0 = -0.0 | same | same |
| bool, large_string (incl. `é` and `é`: two groups), large_binary, fixed_size_binary(4), date32, null type | | | same |
| **date64** 0 ms, 1 ms, 1 day, 1 day + 1 ms | 2 groups (`datetime.date`) | 4 groups | **DIFFER** |
| time32[s], time32[ms], time64[us] | | | same |
| **time64[ns]** 1, 2, 1000, 1001 ns | 2 groups (`datetime.time`, us) | 4 groups | **DIFFER** |
| timestamp[s/ms/us/ns], tz None / UTC / America/New_York | | | same (ns: pandas `Timestamp` keeps ns) |
| duration[s/ms/us/ns] | | | same (ns: pandas `Timedelta`) |
| decimal128(38,10): 1.0000000001, 1.0000000002, 1, 1.0000000000 | | | same |
| decimal256(76,40) | 4 groups | Polars panics | same after the limb encoding |
| logical Path (large_string), logical UUID (fixed_size_binary(16)) | | | same |
| struct key (a user class tag), `dict[K, V]` tag (`large_list<struct<key, value>>`), Arrow map | bare `TypeError: unhashable type` at run time | Polars groups them | see decision 1 |

Equal decimals at different scales cannot be in one column: the scale is part of the
type. A list-valued key is refused at validation, as today.

## (b) Member order: main (`sorted` + `_null_last_key`) against a Polars sort

`measure_order.py`, a tie shows through a descending second column.

| Ordering tag | Result |
|---|---|
| int64 with nulls; float64 NaN variants, -0.0, 0.0, ±inf, null; float32 -0.0/0.0/NaN; large_string with non-ASCII (UTF-8 byte order = code point order); large_binary; bool | same: Polars ties -0.0 with 0.0 and every NaN with every NaN, puts NaN after +inf and before null |
| large_list<int64>, large_list<float64>, large_list<large_list<int64>>, large_list<large_string>, nulls at every depth | DIFFER with a plain Polars sort (nulls inside a list first); **same** with the recursive `struct(is_null, value)` key |
| time64[ns], date64 (values below Python's precision) | main ties them and falls to the next column; Polars orders them exactly: the rule (order by the tag's value) is Polars' |
| struct (a user class tag), `dict[K, V]` tag | main: bare `TypeError` when a group has two members; Polars sorts structs field by field in declared order: see decision 2 |

## (c) Pinned values that move

- `generate_golden_hashes.py --check` with the prototype installed: "golden values of
  semantic, schema, arrow, datagram, pipeline, torch match". **No golden entry moves.**
- Every test file that names `GroupBy` or `group_by(` (52 files, 15,626 tests) plus
  `tests/test_hashing/golden/`, with the prototype: 1 failure,
  `tests/test_core/operators/test_group_by_nan_keys.py::test_a_time64_ns_key_is_rebuilt_from_its_python_value`,
  the PR 441 pin of the ITL-1250 merge (1 ns and 2 ns as one group emitting 0 ns). It
  becomes the test of the fix: two groups, keys 1 ns and 2 ns.
- The 8 other test files with a 64-hex literal: 291 passed.
- What moves outside the pins: any input whose keys, ordering tags or data values differ
  below Python's precision (time64[ns], date64 off midnight, float32 signaling NaN in
  data). No test or golden value holds one.

## (d) Timing (one process at a time; source built before timing)

| Rows | Groups | main | prototype | main peak RSS | prototype peak RSS |
|---|---|---|---|---|---|
| 1e5 | 10 | 0.38 s | 0.07 s | 272 MB | 252 MB |
| 1e5 | 1e3 | 0.36 s | 0.07 s | 277 MB | 253 MB |
| 1e5 | 1e5 (63,249 distinct) | 0.92 s | 0.23 s | 375 MB | 279 MB |
| 1e6 | 10 | 5.51 s | 0.69 s | 1.15 GB | 683 MB |
| 1e6 | 1e3 | 4.37 s | 0.70 s | 1.15 GB | 682 MB |
| 1e6 | 1e5 | 4.88 s | 1.07 s | 1.38 GB | 731 MB |

Most of what remains is the per-group SHA-256 folds of the system tags, as before.

## Data values in the lists (beyond the keys)

`measure_data_values.py`: main rebuilds the DATA lists from Python too, so it changes
data values, not only keys:

| Data column | main | prototype |
|---|---|---|
| time64[ns] 1, 1001 ns | 0, 1000 (truncated) | 1, 1001 |
| date64 1 ms, 1 day + 1 ms | 0, 1 day (midnight) | exact |
| large_list<time64[ns]> | truncated | exact |
| float32 signaling NaN (ITL-1248) | quieted (0x7f800001 to 0x7fc00001) | bits kept |
| timestamp[ns], duration[ns], float16 / float64 NaN payloads, -0.0, decimal256 | exact | exact |

So this resolves ITL-1248 for `GroupBy`.

## (e) Interactions

- **PR 455** (refuses dictionary tags) edits the dictionary branches of `group_by.py` that
  this rewrite replaces: a textual conflict. Proposal: 455 merges first and I drop the
  dictionary handling. If this PR is ready first, I keep a decode in the sort frame (the
  dictionary tests of `test_group_by_nan_keys.py` pass with the prototype) and say so.
- **PR 453** changes `test_group_by_member_order.py` (content hashes of raw streams), other
  lines than mine; no golden overlap with this PR (no value moves here).
- **PR 456** (float rule in hashing): independent. The emitted key is still the
  smallest-bit member; under 456 the zero and NaN variants may hash alike, which only makes
  that choice invisible to hashes.
- **PR 457, 451**: CLAUDE.md and `docs/concepts/operators.md` text only; a merge, no
  semantic overlap. **PR 450 / ITL-1279**: this PR moves no golden value, so it does not
  depend on the provisional group.
- **ITL-1017** (data-tag key's source info folds to one token), **ITL-1070** (non-key
  tags dropped, data-tags kept as data): kept; their tests pass with the prototype.
- **ITL-1071** (opt-in keep non-key tags): the sort frame already orders by them, so it
  stays simple to add.
- **ITL-1248**: resolved for `GroupBy`; `Batch` and `MergeJoin` remain (f).

## (f) Batch and MergeJoin (report only)

`measure_batch_merge_join.py`: both build their lists from Python values (`Batch`:
`table.to_pylist()` then `build_aggregated_table`; `MergeJoin`: `to_pylist()` of the two
colliding columns, sorted by Python value) and lose the same values: time64[ns] 1, 1001 ns
become 0, 1000; date64 truncates to midnight; a float32 signaling NaN is quieted.
`MergeJoin` also orders a merged pair by Python value, so two values that differ below a
microsecond tie. Neither is changed here.

## Decisions for the coordinator

Answered on 2026-11-19: 1 refuse; 2 refuse too (an order by fields would be a new rule nobody
decided; it can be allowed later, since GroupBy goes into the provisional group); 3 no
design-issue entry (the Linear issue is the record), and ITL-1281 filed for `Batch` and
`MergeJoin`; 4 go first, with the dictionary decode in one small place
(`_without_dictionary`).

1. **Struct-bearing keys** (a user class tag, a `dict[K, V]` tag, an Arrow map): main
   raises a bare `TypeError` at run time. Proposal: refuse at validation with an
   `InputValidationError` naming the column (as a list key is refused today), rather than
   start grouping by them.
2. **Struct-bearing ordering tags** (a non-key user-class or dict tag): main raises a bare
   `TypeError` whenever a group has two members. Proposal: order them, by their fields in
   declared order, nulls last at every depth (the same recursive key the lists need; a
   structural class's field order is part of its identity). Alternative: refuse at
   validation.
3. **Logging**: no design-issue entry matches ITL-1250. Add `docs/dev/design_issues/ITL-1250-GB<n>.md`
   (status `in progress`, then `resolved`)? And open a Linear issue (plus an entry) for the
   `Batch` / `MergeJoin` data loss in (f)?
4. **Order with PR 455**: wait for it, or go first with a temporary decode.
