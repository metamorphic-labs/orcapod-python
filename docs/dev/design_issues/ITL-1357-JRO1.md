## `src/orcapod/core/operators/` — join row order

### JRO1 — Sync `Join`, `MergeJoin` and `Rejoin` take their row order from Polars' unspecified inner-join order
**Status:** open
**Severity:** low
**Issue:** ITL-1357

The sync paths of `Join` (`join._inner_join`, `join.py:94`, which `MergeJoin` shares) and
`Rejoin` (`_rejoin_pair`, `rejoin.py:376`) pair rows with a Polars inner join on a key frame
of prepared keys and row numbers, without `maintain_order`, then take every value from the
Arrow inputs by row number. Polars does not guarantee the row order of such a join, so a
Polars upgrade or a different thread count could reorder a join node's rows, and its stored
operator table, on the sync path. Values and hashes would not move: hashes are per row.
`SemiJoin` already sets `maintain_order="left"` (`semijoin.py:242`).

The developer has said row order is not critical (2026-11-22), so this is low priority.
Found while writing the ITL-1347 spec (`docs/dev/specs/itl-1347-job-table-transport.md`,
question 10).

What to do: measure whether `maintain_order="left_right"` gives exactly today's row order on
the test suite and on the ITL-1347 differential, at one Polars thread and at the default
(stage 0 of ITL-1347 reports it). If it does, set it explicitly in every key-frame join. If
it does not, report the differences for a decision: a change of stored row order needs one.
