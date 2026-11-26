# ITL-998 implementation plan: range restriction for `SpiralDBTableSource`

The design is `docs/dev/specs/itl-998-range-restriction.md`; its decisions are section 8. This plan implements
D1 to D14: a float range is refused (D14, option A). The follow-up that pushes down a rewritten float range,
ITL-1477, stays an isolated addition: one gate, one function and their tests, listed at the end.

## Branch and dependencies

- Work continues on the branch of PR 574 (`eywalker/itl-998-spiraldbtablesource-restrict-a-read-to-a`),
  which holds the spec and this plan. `main` is merged in when it moves; no rebase, no force-push.
- **ITL-1465 (PR 576).** Merged into `main`, and `main` merged in here (2026-11-26). ITL-998 needs none of its
  code: a range is built with `col`; ITL-1465 turns `start <= col(c) < stop` (and `c1 and c2`) into an error
  instead of a silently dropped bound. Until it was on `main` its branch was not merged into this one, and
  PR 574 stayed a draft (D11). The test that depends on it is in place (`where(start <= col("t") < stop)`
  raises).
- The sketch in the worktree (uncommitted, from the coordinator's handoff) is the starting point; section 7 of
  the spec says what of it is kept and what changes.

## Code

### `src/orcapod/types/condition.py`

- `is_range_type(arrow_type)`: integers of every width, timestamps of every unit and zone, `date32`, `date64`.
  Floats are excluded here, and only here (D14).
- `check_restriction(condition, schema)`: the one check of a source's restriction, run on the condition given
  and again on the combined one. Each top-level child must be a term on a plain column (no field, no other
  column as operand) with `==` or one of `<`, `<=`, `>`, `>=`; otherwise `TypeError` pointing to
  `source.filter(...)`. The column must exist (`ValueError`, today's wording). Every value passes
  `check_value_for_column` (exact type, exact fit: D6). A comparison on a column that is not
  `is_range_type` raises `TypeError` naming the column and its type, pointing to `filter`. Two different
  equalities on one column stay refused (`ValueError`, today's wording); comparisons are kept as given (D3, D7).
  The sketch's call of `check_equality_conditions` per term is replaced by the column check and
  `check_value_for_column`; `_checked_conditions` in the source becomes unused and is deleted.
- `Condition.to_config` (D8): an equality-only condition is written as today's mapping, its keys in canonical
  order (the order `canonical_form` sorts the terms). A condition with any other term is already saved in
  canonical order. `Filter`'s saved config uses the same method, so it follows too; no identity reads it
  (`Filter`'s identity is `canonical_form()`), and no golden value moves.
- Docstrings: `EQUALS`, `only_equalities`, the class docstring (the sketch's wording).

### `src/orcapod/core/sources/spiraldb_table_source.py`

- `where(*conditions, **columns)`, as `filter` and `Condition.of` take them (today `where(mapping=None, /,
  **columns)`; a single positional mapping or condition still works). `Condition.of`, then
  `_apply_restriction` calls `check_restriction` on the new condition and on the combined one.
- The constructor's `where=` goes through the same check.
- The scan is described by one list of terms, `(column, operator, value)` with equality included, passed to
  the connector (spec section 7), built from `condition.terms()`.
- The cap message names the condition and says how to narrow it or raise `max_rows` (D9). `head()` and the
  preview note use `condition.text()` (order given) and label the size as the table's (D10).
- Docstrings and the module example: the half-open window; the types; floats, strings and the rest go to
  `filter` (the docstring names ITL-1477 for floats; the error text does not). The sentence calling a range of the first key columns "the cheapest to read" is removed (not
  measured).

### `src/orcapod/databases/spiraldb_connector.py`

- `scan_batches(where=...)` takes the source's `Condition` (its terms are the one list of `(column, operator,
  value)`) instead of a mapping; done this way rather than a new `terms=` argument, since `Condition` is the one
  restriction class. One loop builds the predicate: an equality as today (`_literal`; a `float16` equality still
  compared locally, an inexact `float32` one still reads no row, as today); a comparison through one new
  function, `_range_predicate(column, operator, value, stored_type)`, which builds `sp.lt`/`lte`/`gt`/`gte` on
  `_literal(value, stored_type)` and refuses (`ValueError`) a stored type that is not `is_range_type`. It never
  reads and filters a range locally. Callers that passed a mapping wrap it in `Condition.of(...)`.
- Every caller of `scan_batches` and `_read_batches` passes the terms; `limit` counts matching rows, as today.

## Tests (offline, on the fake)

`tests/test_databases/fake_spiral_sdk.py`: `lt`, `lte`, `gt`, `gte` with the service's rules as observed live:
a literal of another type is refused, a null meets nothing, and floats compare by IEEE total order (as the
service does; unused while floats are refused, ready for ITL-1477).

`tests/test_core/sources/test_spiraldb_table_source_where.py`, on tables with two or more key columns and two or
more data columns (CLAUDE.md, "Testing conventions"; the shared several-item family where it fits):

1. **Rows.** Each operator, `between` (closed; also of one value), one-sided, `start == stop`, a reversed window
   (zero rows, full schema), both ends of each integer width; each against the rows pyarrow selects from the
   whole table.
2. **Several items, several orders.** Two ranges on one column; ranges on two columns; a range with one and
   with two equalities; an equality and a range on one column. Every permutation of the arguments, of `&`
   operands, of keywords against positional mappings, and of chained `where` calls gives the same rows,
   condition, `canonical_form`, saved config bytes, and loaded condition; `text()` keeps the order given.
3. **Roles.** A range of a key column (first and last), of a data column with nulls, of a data-tag, and with
   `tags_as_data`; `tag_only_columns=True` except where the default is the subject.
4. **Types.** Eight integer widths, four timestamp units naive and zoned (both 01:30 of a daylight-saving
   overlap), `date32`, `date64` (also not midnight). Every refusal of the spec's section 2 table (wrong Python
   type, out of range, inexact, wrong zone kind, `bool` for `int`), a range of `float16`, `float32`,
   `float64`, string, binary, boolean and the unsupported types, each asserting that no scan ran; float
   equalities unchanged.
5. **Refused trees.** `|`, `~`, `!=`, `is_in`, null tests, a field, a column against a column: the whole
   condition refused, nothing applied. Two different equalities refused; contradictory ranges kept.
6. **Identity.** Content hash, pipeline hash, source id, schema, system-tag names and every record id equal the
   unrestricted source's; the constructor and chained `where` are one mechanism; the original source is
   unchanged. `generate_golden_hashes.py --check` reports no drift.
7. **Reads.** Row cap (a window under the cap on a larger table; over it raises naming the window), `limit`
   (also with a local `float16` equality), `head()` and the preview note, zero rows, `check_for_new_data`,
   a rebound slot with a moved window is stale, overlapping windows compute each shared row once.
8. **Sync and async.** Source reads and whole jobs give the same rows on both paths; the one-restriction
   refusal, the `CachedSource` and caching-operator refusals, the aggregation warning and
   `RunSummary.conditions` with a range.
9. **Saved.** Source config and a saved and loaded job, with datetimes and dates tagged; an equality-only
   `where` with two columns given out of order saves in canonical order; a config saved before (keys in the
   order given) loads to the same condition; one-column configs unchanged byte for byte.
10. `where(start <= col("t") < stop)` raises (ITL-1465, on `main`).

`tests/test_types_condition*.py`: `to_config` key order of an equality-only condition (the existing tests compare
dicts, which ignore order, so they keep passing; one new test compares the JSON text).

## Tests (opt-in, live)

- `tests/test_core/sources/test_spiraldb_table_source_live.py` (`ORCAPOD_SPIRAL_LIVE_PROJECT`, read-only): a
  window, and a window with an equality, equal the local selection of the full read; record ids a subset;
  arguments reversed; `limit`; `head`; an empty window.
- `tests/test_databases/test_spiraldb_connector_integration.py` (`SPIRAL_INTEGRATION_TESTS`, writes its own
  tables and drops them): the kept form of the conformance probe for every type of section 2, against pyarrow,
  plus the float order the service uses (so a change in it shows, and ITL-1477 starts from it), run on the
  final code before ready (D12). The fake runs the same cases.

Run locally: the touched test files only, one pytest process; ruff, ruff format, basedpyright with its exit code;
`scripts/design_issues.py --check` and `scripts/assemble_changelog.py --check`.

## Documentation

- `docs/concepts/sources.md`, SpiralDB section: ranges in `where` (the half-open window, `between` closed,
  several ranges, ranges with equalities), the types, why floats (the service's float order; ITL-1477),
  strings and the rest go to `filter`, the cap
  and `head()` with a window. The existing note on float equality (the table system's own) gains the finding:
  `0.0` does not match `-0.0`, and NaN matches by bit pattern. Samples that need SpiralDB stay marked
  `docs-test: skip`.
- The `where` docstrings; the API reference is generated from them.

## Changelog

`changelog.d/ITL-998.md`:

- `### Added`: `SpiralDBTableSource.where` and `where=` take ranges of integer, timestamp and date columns
  (`col("t") >= start, col("t") < stop`, `between`), applied by SpiralDB in the scan; `where` takes several
  conditions, as `filter` does.
- `### Changed`: a saved equality-only condition (a source's `where`, a `Filter`) lists its columns in canonical
  order; earlier saved files load unchanged.

## Conflicts with open PRs

- 555 (ITL-1457), 558 and the ITL-1464 stack (559, 560, 562, 563, 565, 566, 567, 569, 570, 571): no text
  conflict with the prototype (`git merge-tree`, 2026-11-26). What each could change in behaviour is the spec's
  section 5 table; the tests above cover each point (a restricted copy given a connector, tokens of a window,
  bounded views, async rows, spool staleness, certificates, repeated keys inside a window). Whichever lands
  after ITL-998 runs those tests on its own head.
- 553 and 554 (other drafts of ITL-1457) conflict in `spiraldb_connector.py`, in the scan's predicate lines; the
  one that lands second resolves it.
- 576 (ITL-1465): no text conflict; merged in only through `main`.

## Order of work

1. Merge `main` in. Commit the sketch's kept parts with the changes of spec section 7 (condition, connector,
   source), then the fake.
2. Tests 1 to 9, then the condition tests; ruff, basedpyright, the golden check.
3. Docs, changelog fragment.
4. Live tests written; the conformance test run on throwaway tables before ready.
5. After 576 merged: merge `main`, add test 10 (done). Then the PR readiness pipeline.

## ITL-1477, later (isolated)

Not part of ITL-998. What the follow-up changes, so that nothing here is restructured for it:

1. `is_range_type` accepts `float32` and `float64` (not `float16`: the SDK has no `float16` literal).
2. `check_restriction` refuses a NaN bound, and a float bound the column type cannot hold exactly (D6: `0.1` on
   `float32`), for comparisons only (equalities keep today's rule).
3. `_range_predicate`, for a float column only: adds `col >= -inf` and `col <= +inf` (excludes every NaN, which
   the service orders beyond the infinities) and sends a zero bound as `+0.0` for `>` and `<=`, `-0.0` for `<`
   and `>=` (so `-0.0` equals `0.0`).
4. Tests: the fake's total order makes the rewrite testable offline (every operator with each special value as
   row and bound, against the stance); the live conformance test checks it against the service.
