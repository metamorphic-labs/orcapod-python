## `src/orcapod/core/operators/` — Async execution

### VST1 — Operators keep state written at validation: one operator object on two streams can use the other stream's values
**Status:** open
**Severity:** medium
**Issue:** ITL-1358

Some operators store values computed while validating an input on the operator object
itself and read them later on the async path, where no stream is at hand:

- `SemiJoin._validated_common_keys` (`semijoin.py:287`, read at `:306` and `:381`);
- `Index._element_arrow_type_fallback` (`index.py:82`, `:126`).

One operator object applied to two streams can then use the shared keys or the fallback
type of the stream validated last (ITL-858 inventory, section 1.2). Found again while
writing the ITL-1347 spec (`docs/dev/specs/itl-1347-job-table-transport.md`, difference e6).
It is not yet confirmed by a reproducer.

What to do:

- Write a reproducer: one `SemiJoin` instance validated on stream B, then run async over
  stream A with other shared tag keys; likewise for `Index`. It fails on `main` if the bug is
  real.
- Inventory every operator that writes instance state during validation.
- Derive such values from each input's description when they are used, with no state
  written at validation. Stage 5 of ITL-1347 does this as part of reading descriptions; the
  reproducers then pass, and sync results are unchanged.
