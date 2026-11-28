## `src/orcapod/semantic_types/universal_converter.py`

### DK1 — A stored dict with a repeated key was read as a dict that lost an entry
**Status:** resolved
**Severity:** critical
**Issue:** ITL-907

A `dict[K, V]` column is stored as a list of key/value structs. A table built outside
Orcapod can hold a cell with one key twice (`[{key: 'a', value: 1}, {key: 'a', value: 2}]`).
`ArrowTableSource` accepted it; the table and its hash held both entries, while `as_dict()`
returned `{'a': 2}` and a function pod reading the column received a dict that had silently
lost an entry. Dicts Orcapod builds from Python values cannot be in this state.

**Fix:** Both refusals (developer, November 28, 2026). Which Arrow type is a dict has one
definition, `UniversalTypeConverter.dict_entry_fields` (a list of `struct<key, value>` is a
dict; nothing else marks one). The source builder refuses a column whose type holds a dict
type when a cell repeats a key (`core/sources/dict_keys.py`, on the Arrow data, only for
such columns), and the conversion to a Python `dict` raises on a repeated key, which covers
the routes that pass no source (a raw `ArrowTableStream`, rows read back from a store).
Both raise `RepeatedDictKeyError`; the caller adds the column. Two keys are repeated exactly
when the Python `dict` would keep one of them (`0.0` with `-0.0`, two nulls; not two NaN).
`ArrowTableStream.__init__` does no value scan: every operator result becomes a stream
there. Nothing stored or hashed changed.
