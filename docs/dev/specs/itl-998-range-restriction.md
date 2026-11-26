# ITL-998: restrict a SpiralDB read to a range of a column

Status: decided and implemented on this branch. Every decision, D1 to D14, is decided (section 8). The plan is `docs/dev/plans/itl-998-plan.md`. Depends on
ITL-1465 (D2).

`SpiralDBTableSource` restricts a read by equality (`where`) and `limit=`. A long table needs a window too,
applied by SpiralDB before rows are transferred. A range is a restriction: it chooses rows of the same data and
enters no identity.

Evidence marks: **[live]** read-only against the installed spiral-sdk 0.16.74 on a real keyed table; **[sdk]**
the SDK's source or a local call; **[fake]** the test fake with a prototype applied; **[code]** read on `main`
(0d1e017c).

## 1. The form: comparisons in `where()`

```python
from orcapod import col

source.where(col("timestamp") >= start, col("timestamp") < stop)               # a window
source.where(col("timestamp") >= start, col("timestamp") < stop, unit_id=106)  # and an equality
source.where(window, col("unit_id") < 20)                                      # two columns
SpiralDBTableSource(project, "events", where=window & Condition.of(unit_id=106))
```

`where(*conditions, **columns)` takes what `filter(...)` takes: mappings, `Condition`s built with `col`, and
keywords, all of which must hold. The alternative, `window=("col", a, b)`:

| | Comparisons in `where` | `window=` |
|---|---|---|
| Two ranges; a range and an equality | another argument | a list of tuples, beside `where` |
| One-sided, or both ends open | written as it reads | `None` for an end, plus a flag |
| Half-open or closed | visible in the operator | a convention to remember |
| Config, equality, hash, run summary | `Condition` has them | a second restriction form to add everywhere |
| Moving a step between source and pipeline | the same `Condition` in `filter(...)` | another spelling |
| Mistakes | the Python pitfall of D2 | swapped ends |

The project rule is one restriction class, `Condition` (ITL-1218), and `Condition` already holds comparisons
(ITL-1374): `window=` would be a second one. **D1: comparisons.**

Cases (each is a test):

- **Two ranges on one column** add up (their intersection). `where(...).where(...)` too.
- **A range and an equality**, on the same or another column: both are sent.
- **Ranges on two columns**: both are sent. [live] a window on each of two columns returned the expected number
  of rows.
- **Order**: `where(a, b)`, `where(b, a)`, `where(b & a)` and `.where(b).where(a)` are equal conditions with one
  hash and one canonical form [code]. [live] the two bounds sent in either order returned identical tables. The
  saved form lists terms in canonical order, the text in the order given (section 3).
- **Refused, with a pointer to `filter`**: `|`, `~`, `!=`, `is_in`, null tests, struct fields, a column against
  a column. Any of these in the tree refuses the whole condition; nothing is applied in part.
- **Contradictions**: two equalities with different values stay refused (as today). Ranges that cannot hold
  together, or a range that excludes an equality, are kept and read zero rows; the source does not reason about
  satisfiability. **D3, approved.**

**D2: the chained comparison.** `where(start <= col("t") < stop)` is how a window is written in mathematics.
Python evaluates it as `(start <= col("t")) and (col("t") < stop)`, and a non-empty `Condition` is true, so the
result is `t < stop` and the lower bound is **silently lost** (confirmed by running it; `c1 and c2` likewise).
`ColumnRef` refuses a truth value, `Condition` does not. For a window it is the likely mistake, and it reads the
wrong rows with no error. Approved, and a separate change: **ITL-1465** makes a `Condition`'s truth value an
error. ITL-998 depends on it and does not contain it: ranges ship after it.

## 2. Bounds, column types and literals

**Operators.** `<`, `<=`, `>`, `>=` exactly as written. The documented way to write a window is half-open, `>=
start` and `< stop`: adjacent windows neither overlap nor leave a gap. `col(c).between(a, b)` keeps the meaning
`Condition` gave it (ITL-1374): both ends included, equal to `>= a` and `<= b`; `a > b` is refused. A half-open
`between` would give one name two meanings. **D4, approved.** A null meets no comparison, as in SQL and in
`filter`.

**Column types (D5, approved).** Unsigned and signed integers of every width; timestamps of every unit, with or
without a zone; `date32` and `date64`. Refused in a source's range: strings and binary (collation is the
service's), booleans, and every type a value condition does not support (decimals, times, durations, lists,
structs, logical types), and floats (**D14**): they were to be allowed (`float32`, `float64`) under
Orcapod's stance (ITL-1251: every NaN is one value and unordered, so a NaN meets no range comparison and a NaN
bound is refused; `-0.0` equals `0.0`), but the service orders floats otherwise ("Floats", below), so a float
range stays refused. Pushing down a rewritten float range that equals the stance is a follow-up, ITL-1477.
Equalities accept what they accept today. A refused range points to `filter`, where Orcapod's own rules
apply.

The check is on the column as Orcapod reads it (`_table_schema`), so a logical type stored as an integer is
refused. The literal takes the type SpiralDB stores (`reading.found`); the connector refuses a comparison on a
stored type with no literal, and never reads and filters locally.

**No conversion.** A bound passes the rule every condition value passes (`check_value_for_column`), when the
condition meets the source, before any scan:

| Column | Takes | Refused (`TypeError` naming the column, its type and the value) |
|---|---|---|
| `uint64` | `int` | `-1`, `2**64`, `2.0`, `True`, `"2"`, a `datetime`, a NumPy scalar |
| `int8` | `int` | `300` |
| `timestamp[us]` | naive `datetime` | an aware one, a `date`, an `int` |
| `timestamp[us, tz]` | aware `datetime`, compared as an instant | a naive one |
| `timestamp[ms]` | a whole number of ms | `...000001` (never rounded, though a bound could be) |
| `timestamp[ns]` | microsecond precision (Python's) | outside the years ns can hold |
| `date32`, `date64` | `date` | a `datetime` |

A bound outside the type's range is refused, not read as "always" or "never" (**D6, approved**): `col("t") <
2**64` is more likely a unit mistake. `date64` values that are not midnight compare as stored; a bound is midnight. Error
wording: `SpiralDB table '<project>/<dataset>.<table>': column 't' holds uint64, which cannot hold -1 ...`.

**The SDK still needs a literal of the column's own type.** [live] An untyped Python `int` and an `int64`
literal against a `uint64` column are both refused: `Comparison requires equal dtypes ignoring nullability, got
u64? and i64`. A `uint64` literal works. [sdk] `literal` takes an explicit Arrow type; both ends of all eight
integer types are accepted and one past either end is refused; every timestamp unit, with and without a zone,
and both date types are accepted; time, duration and decimal types are not. "Scans compare without coercion, so
the unit and timezone must equal the column's" (its docstring). The connector's `_literal` already builds these
for equality, so ranges add only `sp.lt`, `sp.lte`, `sp.gt` and `sp.gte`.

[live] with the prototype: a `uint64` window returned exactly the rows computed locally from the whole table;
with an equality, and with a range of a `uint32` column, the expected number of rows; a range with no match gave zero
rows with the full schema.

[live] A conformance probe on throwaway tables, one per type, dropped afterwards: 23 of 23 stored and read back
unchanged, 2,530 checks, all equal to the answer pyarrow gives on the same rows. It covered:

- `uint8` to `uint64` and `int8` to `int64` at both ends and one inside; `s`, `ms`, `us` and `ns` timestamps, each
  naive, in UTC and in a daylight-saving zone, before and after 1970; `date32`, `date64` and a `date64` holding
  values that are not midnight; a null in the compared column (it met no comparison, whatever the operator).
- The SDK alone: all four operators with each stored value as the bound. A
  literal of another unit or zone was refused for every timestamp type.
- Through the prototype: all four operators, equality, `between` (also of one value), `start == stop` and a
  reversed window (no row), a range with one and with two equalities, an equality and a range on one column, two
  ranges on one column, ranges on two columns in all 24 argument orders and chained, a range of two key columns,
  four limits, an async read, and the identity. Both 01:30 of a daylight-saving overlap are two instants.

**Not shown live:** a date key column, or more than a weak check of a timestamp one: all four operators on an existing
naive `timestamp[us]` key gave the expected counts, but it held one value (SpiralDB creates no such key today,
so the probe's were data columns), a `ns` bound between two microseconds through Orcapod (Python
has none), and how much the service scans. Nothing differed from D5 or D6, so neither changes.

**Floats (D14).** [live] A second probe, on throwaway tables dropped afterwards, stored one keyed table per
float type with: a quiet NaN, two signalling NaNs, a negative NaN, `+0.0` and `-0.0`, both infinities, the
smallest subnormal of each sign, the smallest normal, the largest and smallest finite values, ordinary values,
repeats, and a null. Every type stored and read back bit for bit, NaN payloads included. Then the SDK alone, with
each stored value as the bound: `<`, `<=`, `>`, `>=`, `==`, and `between` (as `>=` and `<=`) of every pair of
non-NaN bounds, each answer compared with pyarrow's on the same rows and with the stance.

The service orders floats by IEEE 754 total order, as stored bits: `-NaN < -inf < ... < -0.0 < +0.0 < ... <
+inf < signalling NaN < quiet NaN` (NaNs ordered by payload). `float64`:

| Case | The service returns | The stance (and pyarrow, except for NaN bounds) |
|---|---|---|
| `col > x`, `col >= x`, any non-NaN `x` (also `x = +inf`) | also every positive NaN | no NaN |
| `col < x`, `col <= x`, any non-NaN `x` (also `x = -inf`) | also the negative NaN | no NaN |
| `col < 0.0`, `col >= -0.0` | `-0.0` below `0.0`: `col < 0.0` holds `-0.0`, `col <= -0.0` lacks `0.0` | `-0.0` equals `0.0` |
| `between(a, b)` with `a = +0.0` or `b = -0.0` | misses the other zero (14 of 169 pairs) | both zeros |
| `between(a, b)`, other non-NaN bounds | the same rows (155 of 169) | |
| `col == 0.0`, `col == -0.0` | the zero of that sign only | both zeros |
| a NaN bound | ordered: `col == NaN` holds that bit pattern only; `col < quiet NaN` holds every number and the lower NaNs | refused (stance); pyarrow: no row |
| a null | meets nothing | meets nothing |

Of 254 answers, 86 differ from pyarrow's; of the 234 with a bound the stance accepts, 68 differ from it, all of
them cases of this table. `float32`, the same rows and bounds: the same order and the same cases, 80 of 254 answers differ from pyarrow's
and 68 of 234 from the stance (every comparison with a non-NaN bound returns a NaN row, as for `float64`). The
SDK takes a `float32` literal only as a Python float, so a signalling NaN bound arrives quieted (it then sorts
above the stored quiet NaN); a bound the stance accepts is not affected. The rewrite of option C (below) was also
sent for every non-NaN bound and the four operators: 52 of 52 equal the stance. `float16`: stored, but the SDK has no `float16` literal, so
no comparison can be sent at all.

So a float range is refused (D14, option A), with the pointer to `filter`, where Orcapod's rules apply. Reading
and comparing locally (B) gives up what a range is for, bounding the read. Pushing down a rewritten range that
equals the stance (C: add `col >= -inf` and `col <= +inf`, which excludes every NaN, and send a zero bound as
`+0.0` for `>` and `<=`, `-0.0` for `<` and `>=`) depends on the service's order staying as observed; it is the
follow-up ITL-1477. D6 would apply to such a bound (`0.1` is refused on a `float32` column).

**Findings (existing behaviour, not changed here):**

- *A float equality value a narrower column cannot hold is accepted, not refused.* `check_value_for_column` takes
  `0.1` for a `float32` column (it refuses only a finite value beyond the type's range), and the connector then
  reads no row, since no `float32` equals `0.1` exactly: `where(f=0.1)` returns nothing, silently. This is the
  one exception case to D6 found so far: a float range under ITL-1477 would refuse the value that an equality
  on the same column accepts.
- *Float equality in `where` follows the service* (documented as "the table system's own equality"): the scan's
  equality ([live] on `float64`, through the SDK) does not match `-0.0` to `0.0`, and matches a NaN to the same
  bit pattern only, so `where(x=0.0)` does not read a row holding `-0.0`, and `where(x=nan)` reads only the NaNs
  with the bits of Python's `nan` (the quiet NaN). `filter` follows Orcapod's rule (`-0.0` equals `0.0`, every
  NaN equals NaN).
- *Changed here after review (D15):* an independent review of `d4115197` found that a `where` call that gives
  one column both `-0.0` and `0.0` (or two NaNs) reads rows that depend on the order of the arguments:
  `Condition.of` keeps one of two values Orcapod counts equal, and SpiralDB matches the one kept by its bits.
  With a `float64` column holding `[-0.0, +0.0]`, `where(col("id") >= 0, {"v": -0.0}, v=0.0)` read id 1 and the
  signs reversed id 0, under identical conditions, canonical forms and saved JSON. So a SpiralDB `where` refuses
  an equality to `-0.0` or a NaN (`TypeError` pointing to `filter`, naming ITL-1480), checked on the arguments as
  given (before merging) and on every restriction a source applies; every other float equality is unchanged.
  One form stays out of reach: a condition the user combines beforehand with `&`
  (`(col("v") == 0.0) & (col("v") == -0.0)`) has already kept `0.0`, which is pushed down as before.
- *The test fake's `float-eq` rule says NaN equals NaN*, any NaN; the probe saw the service match a NaN by bit
  pattern only. The fake's comparison reads Python floats, so it cannot tell NaN payloads apart.

**How it is pushed down.** The scan's `filter`, not `key_ranges=`: a filter works on any column and combines
with equalities. [live] the scan returned only the window's rows. How much the service scans to find them could
not be measured: this keyed scan reported no splits and no counters. The docs promise only which rows are
returned.

## 3. Identity, where it is held, saved form

- **Identity.** In no content hash, pipeline hash, source id, schema or system-tag name;
  `_restriction_identity()` stays `None`. [live] the hashes and source id equal the unrestricted source's and
  every record id read is one of its record ids. No golden value moves.
- **Held** in the source's one `Condition` (`source.condition`). Every rule that reads it needs no change:
  `pipeline/restrictions.py` (one restriction per table in a pipeline, no `CachedSource` and no caching operator
  node over a restriction), the aggregation warning, `RunSummary.conditions`.
- **Canonical form.** `Condition` equality, hash and `canonical_form()` are order-free and unchanged. They
  compare statements, not row sets: `t >= 3` and `t > 2` are two restrictions on an integer column, so one table
  read under both in a pipeline is refused. Redundant bounds are kept, never simplified. **D7, approved.**
- **Commutative.** Every form a source accepts is a conjunction of terms (equalities and the four comparisons;
  `between` is two of them), so the condition is the set of its terms. `canonical_form()` flattens every `all`
  and sorts its children, so the order of arguments, keywords, `&` operands, `combined` and chained `where`
  calls never counts [code]. Checked: the 120 orders of five parts (two bounds of a datetime column, a
  `between` of an integer column, an equality, a float comparison), each built with `&` and with `combined`,
  give one canonical form, one saved form, and load back equal. `|` and `~`, which a source refuses, are
  order-free in `canonical_form()` too (`any` sorted like `all`, `not` unary).
- **Saved, in canonical order (D8, approved as changed).** `to_config` writes `{":condition": {"all": [...]}}`
  for a condition that is not only equalities, with tagged values for datetimes and dates, and `from_config`
  rebuilds it through the public builders and the source's checks [code]. The earlier draft of this section
  said the saved list follows the order given; it does not: since ITL-1374 `_node_to_config` writes the
  children of `all` and `any`, and `is_in` values, in canonical order. So a condition with a range already saves
  canonically, and needs no change. An equality-only condition is saved as a mapping, column to value, in the
  order given: `where(b=1, a=2)` and `where(a=2, b=1)` save `{"b": 1, "a": 2}` and `{"a": 2, "b": 1}`. To be
  canonical it is written in canonical order (the order `canonical_form` sorts the terms, by the JSON text of
  each term; for column names without spaces or punctuation, name order). That changes the bytes of a saved equality-only `where` only when it has two or
  more columns given out of that order; one column, or columns given in order, stay byte for byte. Both forms
  load to equal conditions, and no hash reads a source config (the saved job writes it as JSON; only database
  configs are hashed, with sorted keys). Saved jobs from before load unchanged.
- **Printed.** `head()` and the run summary use `condition.text()`, in the order given; display only.

## 4. Reads

- **Row cap.** The pre-check counts the whole table, so it runs only when nothing but `limit` bounds the read;
  with any condition it is skipped and the cap is applied while reading, to the matching rows [code]. A range
  follows that rule: a window under the cap on a table far over it is read; a window over it raises
  `SourceTooLargeError` after up to `max_rows` rows were handed on, so an async job can compute some rows first,
  as with an equality today. The alternative is to count the matching keys up to `max_rows + 1` first: the SDK
  has no filtered count (`Scan.count_rows` wants splits; a keyed scan has none [live]), so that is a second scan
  on every read. **D9:** today's rule. The error names the window (`source.condition`) and the cap, and says how to
  narrow it or raise `max_rows`.
- **`limit`** counts matching rows, in key order. [live] the limited range equals the first rows of the full
  read that match. A `float16` equality in the same condition is compared locally, and then the limit too (as
  today); the range is still sent.
- **`row_count_estimate`** stays the whole table's count and says so.
- **`head()` and the preview** read the first `n` matching rows in one bounded scan, without `max_rows`. The
  note reads `first 2 matching rows (where timestamp >= ..., timestamp < ...); the table has about N rows`: the
  count is named as the table's. When fewer than `n` match, the total is exact and the note says `all k rows`.
  **D10:** the table's size, labelled as such; no count of matching rows.
- **Zero rows.** `all 0 rows`, no error, with a table of the same schema as any read [live]. Asking for the
  schema, keys or hashes reads no row.
- **`check_for_new_data`** compares table versions (metadata only), so a commit outside the window moves the
  pinned version and the modified time, and the nodes below run again and reuse every stored result. It does not
  look inside the window, as for an equality.
- **Staleness.** `where()` returns a new source modified now; rebinding a slot to a moved window makes its nodes
  stale. Rows of earlier windows stay stored; record ids do not depend on the window, so overlapping windows
  compute each row once.
- **Uniqueness certificates** (PR 567): a source certifies its key columns because every read refuses a repeated
  key. A subset of rows with unique keys has unique keys, so a range keeps the certificate and adds none (an
  equality or one-value range does not make a shorter key unique here). The key check sees only the rows read: a
  repeat outside the window is not found, as with `where` today.

## 5. Overlap with open PRs

Text conflicts were tested with `git merge-tree` against a commit holding the prototype (which changes the files
the implementation will). A clean merge does not prove the behaviour; the last column is what to test.

| PR | Touches in SpiralDB reads | Text | To check |
|---|---|---|---|
| 555 (ITL-1457) | constructor `connector=`, `_for_source`, `_counts` per source; the read path is unchanged | clean | a restricted copy of a source given a connector |
| 558 | `with_source_info` through `_read_batches` and `_whole_table` | clean | a window's tokens equal the full read's |
| 559 | nothing (a new module, `row_spool`) | clean | - |
| 560 | `run()` reads in batches (`_batch_streams_of_rows`) | clean | a window's bounded view; zero rows |
| 571 (on 560) | `_preview_batches` calls `_read_batches` with a limit | clean | `preview(n)` over a window |
| 562 | only 558's commits in this file; async rows as Arrow chunks | clean | async rows of a window |
| 566 | 558's commits in this file; node rows on disk | clean | a moved window is stale, not read from the spool |
| 567 | `_whole_table` becomes `_whole_stream`; certificates in `sources/base.py` | clean | the certificate under a range |
| 570 | `identifiers` becomes `SeenKeys`, next to the scan call | clean | repeated keys inside a window |

562, 566 and 570 contain 558; 566 also contains 559 and 562. Pairwise, 567 conflicts in
`spiraldb_table_source.py` with 558, 560, 562, 566, 570 and 571; these are between those PRs and do not involve
ITL-998. The prototype also merges cleanly with the integration branch (569), and its `where` tests give the
same 488 passes and 2 failures there as on `main`. Two other drafts of ITL-1457, 553 and 554, do conflict with
it in `spiraldb_connector.py`; the conflict is with the prototype's scan lines, so whichever lands second is
rebased.

**D11, merge order (approved).** ITL-1465 (PR 576) first: ITL-998 depends on it. ITL-998 may then merge before
555 and 558; it is independent of the ITL-1464 stack and need not wait for it. The stack follows, with 567 last,
rebuilt on the rest (it rewrites `_whole_table`). If 553 or 554 is chosen over 555, the one that lands second
resolves the conflict in `spiraldb_connector.py`. Checked again on 2026-11-26 against the current heads: the
prototype merges cleanly with 576, 555, 558, 559, 560, 562, 563, 565, 566, 567, 569, 570 and 571, and
conflicts with 553 and 554 as before.

## 6. Test plan

Offline, on the fake. The fake gets the four comparisons and must keep refusing a literal of another type and
treating a null as no match. Extend `test_spiraldb_table_source_where.py` (tables there have several key and
several data columns):

1. Rows: four operators, `between`, one-sided, `start == stop`, empty, both ends of each integer type.
2. Several items: two ranges on one column, ranges on two columns, range and one or two equalities, range and
   equality on one column; every permutation of the arguments and of the chained calls gives the same rows,
   condition, hash and saved-and-loaded condition. In each role: key column (tag and data-tag, `tags_as_data`),
   non-key data column with nulls, first and last key column.
3. Types: eight integer widths, four timestamp units naive and zoned (both 01:30 of a daylight-saving overlap),
   `date32`, `date64`; each refusal of section 2, asserting that no scan ran; a range of a `float16`,
   `float32` and `float64` column refused (D14), while their equalities behave as today.
4. Refused trees: each form of section 1; `start <= col(c) < stop` given to `where` raises (from ITL-1465).
5. Identity: hashes, source id, record ids; chained `where` and the constructor are one mechanism; the original
   source is unchanged.
6. Cap, limit (with a local `float16` equality too), head, preview note, zero rows, `check_for_new_data`, stale
   after rebind, overlapping windows compute each shared row once.
7. Sync and async: source reads and whole jobs give the same rows. One-restriction refusal, caching refusals,
   aggregation warning and `RunSummary.conditions` with a range.
8. Round trip: source config, saved and loaded job, tagged datetimes and dates; an equality-only config is
   unchanged.

Opt-in:

- **Read-only**, in `test_spiraldb_table_source_live.py` (`ORCAPOD_SPIRAL_LIVE_PROJECT`): a window and a window
  with an equality equal the local filter of the full read; record ids a subset; reversed arguments; `limit`;
  `head`; an empty window.
- **Conformance** (`SPIRAL_INTEGRATION_TESTS`, which writes to its own dataset): every type of section 2 and
  nulls, against the answer pyarrow gives, as a kept form of the probe (section 2), run on the final code before
  ready. The fake runs the same cases, so a difference between fake and service shows. **D12.**

## 7. What of the sketch to keep

It applies cleanly to 0d1e017c; `test_spiraldb_table_source_where.py` gives 488 passed, 2 failed. Both failures
are in its tests: one indexes a scan log that holds one entry, the other feeds a nullable column to a `str`
parameter (and its last assertion, `... or loaded is not None`, is always true, so it checks nothing).

Keep:

- `where(*conditions, **columns)` and `check_restriction` (checked alone, then combined with the one in force),
  and `is_range_type`.
- The scan pushing typed literals, and refusing rather than filtering locally.
- The fake's comparisons, and most tests.

Change:

- **One list of terms** to `scan_batches` (`(column, operator, value)`, equality included) instead of `where=`
  plus `comparisons=`: one loop, and `Condition.equalities` (one value per column) is no longer how a scan is
  described.
- `check_restriction` calls `check_equality_conditions` for a range bound: call the value check
  (`check_value_for_column`) under a neutral name; its "where names columns..." text stays.
- `_checked_conditions` is then unused: delete it and its test import.
- The sentence "a range of the first key columns ... is the cheapest to read" was not measured (section 2):
  remove it.
- Add docs (`docs/concepts/sources.md`: the form, half-open, the types), the changelog fragment and the
  tests of section 6 it lacks (permutations, several columns, roles, jobs, staleness).

## 8. Decisions

Decided by the developer on 2026-11-25 and 2026-11-26.

| # | Question | Decision | State |
|---|---|---|---|
| D1 | Form | Comparisons in `where()`, as in `filter`; no `window=` | Approved |
| D2 | `a <= col(c) < b` silently drops `a` | A `Condition`'s truth value is an error; not in this change, which depends on it | Approved, as ITL-1465 |
| D3 | Ranges that cannot hold together | Kept, zero rows; two different equalities stay refused | Approved |
| D4 | Ends | Operators as written; half-open is the documented window; `between` includes both ends | Approved |
| D5 | Column types | Integers, timestamps, dates; floats, strings, binary, booleans and the rest refused | Approved (floats: D14) |
| D6 | A bound the type cannot hold exactly | Refuse with `TypeError`; never round or clamp; for floats too (`0.1` on a `float32` column) | Approved; to be reconsidered if an exception case turns up (section 2, "Floats") |
| D7 | Equality of restrictions | By statement; no simplification, so `t >= 3` and `t > 2` differ | Approved |
| D8 | Order inside the saved config | Canonical order (ITL-1271); `head()` and the run summary keep the order given | Approved as changed; a range already saves canonically, an equality-only mapping changes (section 3) |
| D9 | Row cap with a range | Today's rule, no pre-count; a clear error naming the window and how to narrow it or raise the cap | Approved |
| D10 | Size shown by `head` | The table's size, labelled; no count of matches | Approved |
| D11 | Merge order | ITL-1465 (PR 576) first; ITL-998 may merge before 555 and 558; then the rest of the stack with 567 last | Approved |
| D12 | Live proof of the types | Throwaway tables now; the kept conformance test on the final code before ready | Approved; probes run, section 2 |
| D13 | Scope | `SpiralDBTableSource` only; other sources stay with ITL-1219 | Approved |
| D14 | Float ranges, given the service's float order | A: refused, pointing to `filter` (B, read then compare locally, and C, push down a rewritten range equal to the stance, were the other options) | Approved: A; C is ITL-1477 |
| D15 | Float equality to `-0.0` or NaN in `where` (review of `d4115197`) | Refused, `TypeError` pointing to `filter`, naming ITL-1480; every other float equality as before | Decided on the developer's behalf by the coordinator (logged) |

One restriction per table in a pipeline stays in force; lifting it is ITL-1472, not this change.
