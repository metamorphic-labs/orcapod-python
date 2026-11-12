## `src/orcapod/semantic_types/canonical_order.py` — dict and set values in canonical order

### DO1 — A `dict` value's insertion order, or a `set`'s iteration order, changed its row hash
**Status:** resolved
**Severity:** high
**Issue:** ITL-905

A `dict[K, V]` value is stored as `large_list<struct<key, value>>` and a plain `set[T]` as
`large_list<T>`. The converter wrote both in Python iteration order, so `{"a": 1, "b": 2}`
and `{"b": 2, "a": 1}` had different row hashes (and missed each other's function pod cache
entries), while the semantic hasher, which sorts dict keys, gave them the same hash. A
`set[str]` was stored in an order that depends on `PYTHONHASHSEED`, so its row hash changed
from process to process. `set[<logical type>]` already sorted (`ListLogicalType`).

**Fix:** the developer ruled (November 11, 2026) that a dict and a set are order independent,
that Orcapod sorts them before storing them into Arrow, and that hashing an Arrow table never
sorts anything (starfix ignores only column order and struct field order). The converter's
`dict` branch sorts entries by storage key and a new `set` branch sorts elements, both with
the one ordering rule in `semantic_types/canonical_order.py` (None first; numeric; `str` by
code point; `bytes` bytewise; dates and times naturally). NaN, incomparable values and two
keys or elements with the same storage value raise `CanonicalOrderError`, wrapped with the
column name by the caller. `Datagram` built from Python values holds key-ordered copies of
its dict values (inside `list`, `dict`, `Optional`), and `update` and `with_columns` do the
same, so a newly computed output and one served from a `0.3.0` cache iterate alike (one served
from a `0.2.0` cache keeps its stored order until recomputed); the caller's dict is never
mutated. Released hashes move only for values not already
in canonical order (two golden entries, `pipeline:index_pick.index` row data hashes); see
the `0.3.0` changelog. Regressions: `tests/test_hashing/test_dict_order_hash.py` (including a
Delta store written by `0.2.0`) and `tests/test_hashing/test_set_order_hash.py`. Open: a
key/value list that arrives already in Arrow form keeps its stored order and hash, by the
same ruling; dicts inside dataclass or pydantic objects keep their own order in memory
(their stored and hashed form is canonical).
