## `src/orcapod/semantic_types/universal_converter.py`

### NFK1 — A float32 or float16 dict key or set element loses an entry when a float is narrowed
**Status:** resolved
**Severity:** high
**Issue:** ITL-1591 (related: ITL-924, ITL-1613)

A Python float stored under a `float32` or `float16` type is narrowed without notice, and two
distinct floats can narrow to one stored value (`1e-50` and `2e-50` both become `0.0`). For
the keys of a dict and the elements of a set that lost an entry: a `DictSource` with an Arrow
`data_schema` whose column `k` is `large_list<struct<key: float, value: int64>>` stored
`{1e-50: 1, 2e-50: 2}` with two `0.0` keys, a set column `{1e-50, 2e-50}` was stored as
`[0.0, 0.0]` and read back `{0.0}`, and updating a datagram held as an Arrow table did the
same. No warning, no error.

**Fix:** developer decisions D16 and D25 (November 28, 2026): a declared type whose dict key
or set element is `float32` or `float16` is refused where it is declared, in a Python schema
or annotation and in an Arrow schema or table alike, at any nesting depth, by a check of
types that reads no value. One rule, `semantic_types/narrow_float_keys.py`
(`NarrowFloatKeyError`, an `InputValidationError`): the type converter applies it to every
dict and set type it builds (so a source's Python schema, a datagram's, a user-class field
and a function pod's declared inputs and outputs, when the pod is defined), and the source
builder, `ArrowTableStream` and a datagram given an Arrow table apply it to the table's
schema. A dict is the converter's one dict shape (`nested_storage.dict_entry_fields`, now
used by the converter's two readers too) or an Arrow map. `float32` and `float16` stay
accepted as a plain column, a list element, a struct field and a dict value.

Left as it is: a plain value in an Arrow-declared `float32` or `float16` column is still
narrowed silently (ITL-1613). A struct column whose fields are exactly `key` and `value`
becomes the dict shape once `Batch`, `GroupBy` or `MergeJoin` puts it in a list, and is
refused there when its `key` is `float32` or `float16`.
