# Value-based array format for `numpy.ndarray` in v0.3 (ITL-888)

**Date:** 2026-11-10
**Issue:** ITL-888 (fixes ITL-883; refs ITL-874, ITL-871, ITL-465, ITL-882, ITL-884)
**Design basis:** ITL-874 review, PR #285 (`superpowers/specs/2026-11-10-array-tensor-storage.md`,
candidate 4c, sections C4 and C5; experiments `b0`, `c1`, `c2`, `c3`, `b7`).
**Status:** Implemented in PR #289.

**Decisions (developer, November 10, 2026).** The identity rules D1 to D8 are confirmed as
written here: identity is dtype, shape and element bits; memory layout, byte order and
structured field order are not identity; little-endian storage; hashing by bits; extended data
(masked, object, `StringDType`) refused, and if supported later stored as multi-part data, not
as one array; subclasses treated as arrays unless a more specific registered type exists; array
and tensor distinct. Proceeding provisionally, each a one-line change: the names
`orcapod.ndarray` and `ndarray_v1`, D-PATH, and the three extra refusals (`longdouble`,
unit-less datetimes, titled structured dtypes). Old-store detection and migration are
deferred. The PENDING marks below are kept as written for the record.

v0.3 is a public release. The extension name, the storage struct, the dtype tags, the digest
definition and the hash label below are public commitments once v0.3 is tagged.

---

## Summary

- A `numpy.ndarray` value is stored as one Arrow struct per row: `dtype` (an Orcapod dtype tag
  string), `shape` (`large_list<int64>`), `data` (`large_binary`, little-endian, C order,
  packed). The column's extension name is **`orcapod.ndarray`**. The old name `numpy.ndarray` is
  no longer registered and is never written again.
- The hash is defined on the value: SHA-256 over a length-prefixed encoding of exactly those three
  stored fields. Its label is **`ndarray_v1`**, replacing the bare `sha256`. Memory order, strides,
  byte order and structured field order do not change it.
- The canonical form is applied at **storage** time, so a stored row *is* its canonical value: a
  Fortran array reads back C-ordered, `>f4` reads back `<f4`, a structured dtype reads back with
  its fields sorted by name and packed. The digest is a function of the stored row alone, so a
  later migration or a starfix reimplementation can recompute it without NumPy.
- Masked arrays, object dtype, `StringDType`, `longdouble` and unit-less datetimes are refused at
  write time with a `ValueError` naming the column and the dtype. A NumPy scalar returned where
  `np.ndarray` is declared is refused at the pod (ITL-883).
- The new logical type name moves the **pipeline hash of every node that has an array column, and
  only those**. A v0.2 store is therefore never written into: v0.3 builds new tables for the
  array nodes and leaves the v0.2 tables untouched (verified, see E). This is a deliberate table
  path change for array nodes. It needs the developer's yes (D-PATH).
- Old-data detection and migration are deferred (developer, November 10). Section E states the
  known consequence for the CHANGELOG.
- The NumPy-neutral pieces (storage struct, digest) live in one module that `torch.Tensor`
  (ITL-871) will reuse under its own name.

---

## A. Storage layout

### A1. Arrow storage type (CONFIRMED: candidate 4c, developer's decision; nullability per F1)

```
extension<orcapod.ndarray> over
struct<
  dtype: large_string,
  shape: large_list<item: int64>,
  data:  large_binary
>
```

- Field order is `dtype, shape, data`. The field names are part of the format.
- **The children are nullable at the Arrow level (finding F1, approved November 10).** The first
  draft made them non-null. The risk prototype showed that Polars drops nested non-null flags
  when it converts back to Arrow (the nested-nullability gap, ITL-842). The extension type then
  no longer matches, and Orcapod's strict extension deserializer rejects it inside
  `PolarsFilter`, `Join` and `MergeJoin`. So non-null is enforced by the logical type instead:
  writing never produces a null child, and reading a row with a null child raises an error
  naming the column. This can be tightened later under a new storage version, once nested
  nullability survives Polars.
- Nullability of the **column** follows the annotation, as for every other logical type:
  `np.ndarray` gives a non-null column, `np.ndarray | None` a nullable one, and a `None` value is
  a null struct.
- `large_*` variants throughout, so `normalize_to_large_types` is the identity on this type and
  operators never rebuild it.
- `shape` holds one `int64` per dimension, outermost first, every value `>= 0`.
- `data` holds the array's elements in C order, little-endian, packed (no padding):
  exactly `itemsize(dtype) * prod(shape)` bytes.
- The Polars extension type carries the same `ARROW:extension:metadata` as the Arrow one.

### A2. The dtype tag (PENDING: names below)

The tag is an Orcapod string, not NumPy's `descr`. It names the element type without byte order
(storage is always little-endian), so it can be produced by any implementation, including torch
for types NumPy lacks (`bfloat16`, `float8_*` in ITL-871). It maps one to one onto the NumPy
dtypes that are supported:

| NumPy (`dtype.str`, any byte order) | Tag | Element bytes in `data` |
|---|---|---|
| `\|b1` | `bool` | 1 byte, as NumPy holds it (0x00 or 0x01 for values NumPy creates) |
| `i1 i2 i4 i8` | `int8 int16 int32 int64` | two's complement, LE |
| `u1 u2 u4 u8` | `uint8 uint16 uint32 uint64` | LE |
| `f2 f4 f8` | `float16 float32 float64` | IEEE 754 binary16/32/64, LE |
| `c8 c16` | `complex64 complex128` | real then imaginary, each IEEE 754 LE |
| `M8[<n><unit>]` | `datetime64[<n><unit>]`, e.g. `datetime64[ns]`, `datetime64[10ms]` | int64 LE (NaT = int64 min) |
| `m8[<n><unit>]` | `timedelta64[<n><unit>]` | int64 LE |
| `U<n>` | `ucs4[<n>]` | n UCS-4 code units, each uint32 LE (NumPy's own representation, NUL-padded) |
| `S<n>` | `bytes[<n>]` | n raw bytes, NUL-padded, no byte swap |
| `V<n>` (unstructured) | `void[<n>]` | n raw bytes, no byte swap |
| structured | JSON array, see below | packed fields in tag order |

The unit and multiplier are written as NumPy writes them inside the brackets (`ns`, `10ms`, `D`,
`W`, ...); the multiplier is omitted when it is 1.

**Structured dtypes.** The tag is a JSON array with one entry per field, sorted by field name
(Unicode code point order): `[name, tag]`, or `[name, tag, [d0, d1, ...]]` for a subarray field.
A nested structured field has a nested array as its tag. The text is the output of
`json.dumps(value, separators=(",", ":"), ensure_ascii=True)`: no whitespace, non-ASCII and
control characters escaped as lowercase `\uXXXX` (surrogate pairs above U+FFFF), `"` and `\`
escaped, and the short escapes `\b \f \n \r \t`. That rule is fully specified by RFC 8259 plus
those choices, so another implementation can reproduce it. A tag beginning with `[` is
structured; no scalar tag begins with `[`.

Example: `np.dtype([("b", ">f8"), ("a", "<i4", (2,))])` has the tag
`[["a","int32",[2]],["b","float64"]]` and 8 + 8 = 16 bytes per element, `a` first.

The tag relates to NumPy's `descr` as follows: it is `descr` with byte order removed, kind and
width spelled out, fields sorted by name and offsets/padding dropped (fields are packed).

**Refused dtypes** (`ValueError` naming the column and the dtype, at write time):

| Input | Why |
|---|---|
| `object` (`O`), or a structured dtype with an object field | needs pickling |
| NumPy 2 `StringDType` (`T`) | variable width, object-like storage (ITL-883 defect 3) |
| `longdouble`, `clongdouble` (`g`, `G`) | width and format differ by platform (float64 on arm64 macOS, 80-bit on x86-64 Linux), so neither bytes nor hash would be portable. **PENDING** (alternative: accept and tag `longdouble` with platform-dependent hashes) |
| `datetime64`/`timedelta64` without a unit (`M8`, `m8`) | no defined meaning for the int64 value. **PENDING** (rare; alternative: tag `datetime64[generic]`) |
| structured dtype with field titles | titles are aliases that cannot be represented once fields are sorted. **PENDING** (rare) |

### A3. Canonicalisation on write (PENDING with D below)

`python_to_storage(arr)`:

1. Refuse masked arrays (`np.ma.MaskedArray`) and the dtypes above.
2. Build the canonical NumPy dtype: little-endian, packed, structured fields sorted by name,
   recursively.
3. Copy field by field into a C-contiguous array of that dtype. Field-by-field assignment is
   required: NumPy assigns between structured dtypes *by position*, so a plain `astype` would
   pair fields wrongly once they are sorted. Unstructured arrays use one
   `astype(canonical, order="C")`, which is a no-op copy-free view when the input already is
   little-endian C-contiguous.
4. Store `(tag, list(shape), canonical.tobytes())`.

`storage_to_python` builds the canonical NumPy dtype from the tag and returns
`np.frombuffer(data, dtype).reshape(shape)` as a **writeable copy** (a read-only view into Arrow
memory would surprise pods that mutate their input). So a value reads back with the canonical
dtype, C order, and the base class `np.ndarray`.

### A4. Zero-dimensional and empty arrays (CONFIRMED by b4/c1)

- 0-d: `shape = []`, `data` = one element. `np.array(1.5)` reads back as a 0-d `float64` array.
- Empty: `shape` keeps every dimension, `data = b""`. `(0, 3)` and `(3, 0)` are distinct values
  with distinct hashes. Structured and string dtypes work the same way.

---

## B. Extension name and storage version (PENDING: names)

- **Extension name `orcapod.ndarray`**, and the logical type name (`logical_type_name`, which the
  type and pipeline hashes use) is the same string, as for every built-in type
  (`orcapod.file`, `orcapod.uuid`, ...). Justification: C5 rule 1, the old name `numpy.ndarray`
  must never carry new bytes, and the `orcapod.*` prefix says Orcapod defines the layout.
- **Storage version** lives in `ARROW:extension:metadata` as the canonical JSON
  `{"version":1}`. The registry's deserialiser already rejects a stored type whose metadata
  differs from the registered one, so a future layout read by v0.3 code fails loudly instead of
  decoding wrongly. Per C5 rule 2, a later layout change gets a new version (and, if the struct
  changes shape, a new name) plus an explicit migration.
- `numpy.ndarray` is removed from the data context and not registered with pyarrow. The v0.1
  context JSON is edited in place (precedent: ITL-451 changed `orcapod.file` storage in place),
  with a changelog line. Nothing else in the context moves; in particular the semantic hasher
  id `semantic_v0.1` stays, so no unrelated table path changes.

---

## C. Hash

### C1. Value digest, byte level (PENDING: exact framing)

For a stored row with tag `T` (string), shape `(d0, ..., d{n-1})` and data `D` (bytes):

```
P = u64le(len(utf8(T))) || utf8(T)
 || u64le(n) || i64le(d0) || ... || i64le(d{n-1})
 || u64le(len(D)) || D
digest = SHA-256(P)            (32 bytes)
```

`u64le`/`i64le` are 8-byte little-endian unsigned/signed integers. Because every field is
length-prefixed, no two distinct `(T, shape, D)` give the same `P`. The digest depends only on the
stored row, so it can be recomputed from a Parquet or Delta file with any SHA-256 library; the
docs will carry a short reference implementation and test vectors (the golden values in G).

It is computed from the canonical buffer without the hasher copying it again: `hashlib` reads the
C-contiguous canonical array's buffer directly (c1: about 0.1 s for 128 MiB).

### C2. Label and binding to the logical type (PENDING: label spelling)

- The semantic handler (`NumpyArrayHandler`, kept registered for `numpy.ndarray`) returns
  `ContentHash(method="ndarray_v1", digest=<32 bytes>)`. `ndarray_v1` replaces the bare
  `sha256`, so an old and a new array hash can be told apart from the label alone (C5, c2). The
  label contains no `:` (`ContentHash.to_prefixed_digest` uses `:` as its separator).
- In a table hash, the Arrow hasher already folds the extension name in front of the handler's
  result, so a stored array cell hashes as the token
  `b"orcapod:ndarray::ndarray_v1:" + digest`. Before: `b"numpy:ndarray::sha256:" + sha256(.npy)`.
- Binding: the digest body is shared and type-neutral; the type is bound twice, by the label
  (`ndarray_v1`, later `tensor_v1` for torch) and by the extension name in the token. An ndarray
  and a tensor with equal values therefore have different hashes but the same digest body
  (ITL-874 D6 pick).

### C3. Invariances (CONFIRMED for layout by c1; PENDING for byte and field order, see D)

Same hash: C vs Fortran order, any strides or views, a transposed-twice view, big- vs
little-endian copies, structured field order, aligned vs packed structured dtypes, subclass vs
base class.
Different hash: dtype (including width: `int32` vs `int64`), shape (including `(0,3)` vs `(3,0)`
and `()` vs `(1,)`), any element bit (including `-0.0` vs `0.0` and different NaN payloads),
structured field names.

---

## D. Identity decisions (all PENDING the developer's confirmation, relayed by the coordinator)

| # | Decision | Plan | Consequence |
|---|---|---|---|
| D1 | What identity is | The value: dtype tag, shape, C-order little-endian packed buffer. Not the `.npy` file. | Layout-free hash; the digest is reproducible outside NumPy. |
| D2 | Byte order | Not identity. Canonicalised to little-endian on write; `>f4` reads back as `<f4`. | Round trip exact in value, not in `dtype.byteorder`. |
| D3 | Structured field order | Not identity (ITL-465). Fields sorted by name on write; field names and types are identity. | `(b, a)` reads back as `(a, b)`. Padding/alignment is dropped (packed). |
| D4 | `-0.0` vs `0.0`, NaN payloads | Hashed by bits, so different. | Matches the stored bytes; no float normalisation. |
| D5 | Masked arrays | Refused (ITL-883 defect 1). | No silent mask loss. Users store `data` and `mask` as two arrays. |
| D6 | object dtype, `StringDType` | Refused, naming column and dtype. | ITL-883 defect 3 becomes a clear error. |
| D7 | Subclasses (`np.matrix`, `np.recarray`, `np.memmap`, user subclasses) | **Accepted and read back as plain `np.ndarray`**; documented. Hash equals the base-class value's. Rejecting them would break `recarray`/`memmap` users for no gain; the value is fully preserved. | `np.matrix` reads back as a 2-D ndarray. |
| D8 | NumPy scalar returned where `np.ndarray` is declared | Refused at the pod with a `TypeError` naming the output key, the declared type and the returned type (ITL-883 defect 2). Arrays are never wrapped implicitly (type strictness). | `x * 2` on a 0-d array returns a scalar; users write `np.asarray(...)`. |
| D-PATH | Logical type name | `orcapod.ndarray` (see B), which moves the pipeline hash, and so the table path, of every node with an array column, and of nothing else. | v0.2 tables are never mixed with v0.3 rows (E). Old tables stay as they are. |

---

## E. Old data: known consequence (detection and migration deferred by the developer)

No detection, no special error, no Delta read-path change, ITL-884 untouched (coordinator
message, November 10). What a v0.2 store that holds `.npy` arrays does under v0.3:

**With D-PATH as planned (new logical type name), verified by a probe on a real v0.2 Delta store**
(scratch experiment: `source -> double(x)->ndarray -> total(y)->float`,
`direct_sum(x)->float` and an array-free `tag_only` node; the type name changed in a fresh
process):

- Every node with an array column gets a new pipeline hash (`double`, `total`, `direct_sum`);
  `tag_only` keeps its hash and its table.
- Tables that hold array **values** move: the pipeline tables of all three nodes, and the result
  table of `double` (its function identity includes the `-> np.ndarray` output type). The v0.2
  tables are left untouched and stay readable by v0.2.
- Result tables of array-**consuming** pods with no array output (`total`, `direct_sum`) keep
  their path, because the function identity does not change. New rows are appended under the new
  input hashes. Those tables hold no array column, so the append cannot make them unreadable.
- So under v0.3 the array nodes **recompute every row once**, and their v0.2 results are no longer
  returned. Nothing is damaged and no table becomes unreadable.

**Without D-PATH** (keeping the name `numpy.ndarray` for the type hash, so paths stay):
new-format rows are appended into the v0.2 tables. Delta accepts the append, the next read fails,
and Orcapod reports no rows (b7, ITL-884). The array-free downstream tables recompute and hide
their old rows (c3). This damages v0.2 stores, so I do not recommend it.

For the CHANGELOG and docs: "v0.3 changes how arrays are stored and hashed. A store written by
v0.2 that holds `numpy.ndarray` columns is not migrated: under v0.3 every node that handles
arrays recomputes into new tables, and its v0.2 results are no longer returned (they remain on
disk, readable with v0.2). Use a fresh store, or keep using v0.2 for that store. A migration is
planned after v0.3." The tests will pin the D-PATH behaviour on a v0.2-format Delta store fixture
(new tables, old tables byte-identical, no unreadable table).

---

## F. Room for `torch.Tensor` (ITL-871)

New module `src/orcapod/logical_types/array_format.py` with no NumPy import and no NumPy names:

- `ARRAY_STORAGE_TYPE`: the struct of A1.
- `ARRAY_FORMAT_VERSION = 1` and the extension metadata bytes.
- `array_digest_v1(tag: str, shape: Sequence[int], data: bytes | memoryview) -> bytes`: C1.
- `make_array_storage_value(tag, shape, data)` / field accessors for a stored row.

`numpy_type.py` holds only the NumPy side: dtype to tag, tag to dtype, canonicalisation,
`LogicalNumpyArray` (name `orcapod.ndarray`). `NumpyArrayHandler` calls `array_digest_v1`. The
torch type will add its own tag mapping (including `bfloat16`) and its own name and label
(`orcapod.tensor`, `tensor_v1`), reusing the struct and the digest. No torch code in this PR.

---

## G. Golden hash tests (PR #284)

**PR #284 is not on main yet** (open, checked November 10). I will rebase onto it if it merges
before my PR; otherwise I will list the moves in my PR and the second PR to land updates them.
Pinned entries that move, all array entries, nothing else:

| File | Entries | Why |
|---|---|---|
| `semantic.json` | `value.ndarray.{empty,float64,int32,int64}` | new digest and label (`sha256:` becomes `ndarray_v1:`) |
| `semantic.json` | `type.ndarray` | type hash uses the logical type name (`type:orcapod.ndarray`) |
| `schema.json` | `single_field.ndarray.arrow_type`, `single_field.ndarray.schema_hash` | new storage type; type name in the schema hash |
| `datagram.json` | `ndarray.content_hash`, `ndarray.pipeline_hash`, `ndarray.row[...].data_hash` (2), the `ndarray.row[...]._tag_*::<pipeline hash>` keys (4) and `ndarray.system_tag_columns` | value hash; pipeline hash in system tag column names |

`arrow.json` and `pipeline.json` have no array cases and must not move; the test run will show
that every non-array entry is unchanged. I will add golden pins for a few arrays of my own
(0-d, empty, Fortran, big-endian, structured) with the digest computed by an independent
reference implementation of C1 in the test (plain `hashlib` + `struct`, no Orcapod code).

---

## Operator findings from the risk prototype (November 10)

A scratch prototype of the struct format (no `src/` change) was run against `tests/test_arrays`:

- **F1** (approved): nullable struct children, see A1.
- **F2** (moved out of this PR): `SemiJoin` uses pyarrow's Acero hash join, which refuses any
  nested non-key column. It already fails on main for `list[int]` and dataclass columns; today's
  arrays only escape it because `.npy` is `large_binary`. The fix, moving SemiJoin to Polars
  (`how="semi"`, `maintain_order="left"`, input types restored), is ITL-847 and has its own PR.
  Until it merges, the SemiJoin-with-array test is a strict `xfail` citing ITL-847.
- **F3** (approved): `MergeJoin` sorts colliding values as Python storage values, and struct
  storage values (dicts) cannot be ordered (the ITL-839 limitation). For the array logical type
  only, the ordering key is the stored `(dtype tag, shape, data)` tuple. That order is
  deterministic and defined on the stored value. General struct ordering stays in ITL-839.

## H. Risks and cuts

Risks:

1. **Polars and struct-backed extension types.** Prototyped: works with nullable children (F1).
2. **MergeJoin sorting a struct column.** Prototyped: needs the array ordering key (F3).
3. **Delta with the struct column.** Prototyped: in-memory and Delta, sync and async pass;
   the cached re-run in a fresh process is still to be verified with the real type.
4. **Golden churn if #284 lands late.** Mechanical, but must be listed in the PR.
5. **D-PATH is a table path change for array nodes.** Needs the developer's explicit yes.

Cuts if time runs short, in order: the extra golden array pins beyond four values; docs worked
example (keep the format and digest definition); refusal of `longdouble` (fall back to accepting
it with a platform-dependent note). Not cuttable: storage layout, digest and label, operator and
database coverage, the scalar-return check, the CHANGELOG entry.

---

## Implementation outline (for the plan after approval)

1. `array_format.py` (struct, version, digest) with unit tests against an independent reference.
2. `numpy_type.py`: tag mapping, canonicalisation, refusals; `LogicalNumpyArray` renamed to the
   new extension; column name added to conversion errors.
3. `NumpyArrayHandler` on the new digest and label.
4. Function pod output check for declared `np.ndarray` (D8).
5. Context JSON and changelog line; CHANGELOG entry; docs (`docs/reference/hashing.md` plus a
   value-types section; there is none on main, so a new `docs/concepts/` page or a section in
   an existing one, to be decided in the plan).
6. Tests first, through the public interface: every dtype class, round trip, invariances and
   differences, every operator including Batch, GroupBy and MergeJoin with a colliding array
   column, sync and async, in-memory and Delta, cached re-run in a fresh process, cross-process
   and cross-`PYTHONHASHSEED` stability, golden pins, and the v0.2 store fixture for E.
