## `src/orcapod/core/operators/` — Async execution

### VST1 — Operators keep state written at validation: one operator object on two streams can use the other stream's values
**Status:** resolved
**Severity:** medium
**Issue:** ITL-1358

Some operators store values computed while validating an input on the operator object
itself and read them later on the async path, where no stream is at hand:

- `SemiJoin._validated_common_keys`.
- `Index._element_arrow_type_fallback` and `Pick._value_arrow_type_fallback`.

One operator object applied to two streams can then use the shared keys or the fallback
type of the stream validated last (ITL-858 inventory, section 1.2). Found again while
writing the ITL-1347 spec (`docs/dev/specs/itl-1347-job-table-transport.md`, difference e6).

**Fix:** Async `SemiJoin` reads the job inputs' tag descriptions when the right input is
empty. Direct row-channel calls use the keys validated for their own pair of pipeline
hashes and raise if that pair was never validated. `Index` and `Pick` keep projected
types and lookup information keyed by the input pipeline hash. Regression tests reuse
each operator across inputs with different tags or projected types.
