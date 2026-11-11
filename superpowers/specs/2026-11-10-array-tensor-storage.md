# Array and tensor storage: review of today's NumPy support and candidate designs (ITL-874)

**Date:** 2026-11-10
**Issue:** ITL-874 (related: ITL-871 PyTorch, ITL-465 NumPy hashing, ITL-848 starfix gaps,
ITL-467 out-of-table storage, ITL-869 jaxtyping)
**Status:** For the developer to decide. Nothing here is implemented.

## Summary

- Today, an array column stores the exact `np.save` file (`.npy` header plus raw buffer) in a
  `large_binary` column with extension name `numpy.ndarray`. The content hash is starfix over a
  token `numpy:ndarray::sha256:<sha256 of the .npy file>`.
- Storage is **lossless** for every non-object dtype tried, including structured, datetime,
  strings, 0-d and empty arrays. It is identical on NumPy 1.26.4 and 2.2.6 and identical
  across processes. Arrays survive every operator, the in-memory and Delta stores, the sync
  and async orchestrators, and a cached re-run in a fresh process.
- The **hash is over the file, not the value.** Equal arrays in C and Fortran order, or in
  little- and big-endian byte order, or with structured fields in another order, get
  different hashes. ITL-465 asked for the opposite.
- The **schema says only `numpy.ndarray`.** No dtype or shape reaches the schema or the
  pipeline hash. `NDArray[np.float32]` is rejected as an annotation, so dtype cannot be
  declared or enforced. MergeJoin merges float32 and float64 arrays under one column.
- Defects: masked arrays silently lose their mask. A pod declared `-> np.ndarray` can return
  a NumPy scalar that is not caught. NumPy 2 `StringDType` fails with a misleading pickle
  error. `pl.read_delta` fails on a store once Orcapod is imported.
- Of the candidates, the Arrow canonical tensor types are **not usable today**. Starfix cannot
  hash them. Delta reads of them fail, and the failure is hidden as "no rows". pyarrow 23.0.1
  has no `variable_shape_tensor`. `fixed_shape_tensor` refuses 0-d, bool, complex, datetime,
  strings and structured dtypes.
- The best long-term layout tested is an Orcapod logical type over **explicit fields per row:
  dtype, shape and a little-endian C-order data buffer** (candidate 4c). It is exact for every
  dtype, hashable by starfix today and the same size as `.npy`. Other tools can read its shape.
  A tensor stores to identical bytes, bfloat16 included (as raw bits plus a dtype tag).
- **Hazard for any format change.** The pipeline hash uses the Python type, so it is the same
  before and after. New-format rows would therefore land in the same Delta table as old ones.
  Delta accepts that write, then the read fails and Orcapod returns no rows. Old results look
  dropped. A format change must ship as an explicit migration to new tables.
- **My pick:** ship today's `.npy` format in v0.3 and fix the defects that do not move a valid
  array's hash. Label array storage provisional. Hold PyTorch for v0.4, built on candidate 4c,
  and move to 4c in v0.4 with one explicit migration that also changes the hash once. The
  developer decides; see Part C.

## Evidence

Every script is in `superpowers/specs/array-tensor-storage-experiments/`. Run each from the
worktree root with `uv run python <script>`, unless the table gives another command. Each
prints one line per check: `[OK]`, `[DEFECT]`, `[FAIL]`, `[INFO]` or `[SKIP]`.

| Script | What it establishes |
|---|---|
| `a1_stored_and_hashed_bytes.py` | stored bytes, hashed bytes, stability, 0-d, empty and large arrays |
| `a1b_numpy_versions.py` | NumPy 1.26.4 against 2.2.6, bytes and hashes; runs `uv run --with numpy==1.26.4` itself |
| `a2_dtype_coverage.py` | dtype coverage, field order, object, masked, scalars, subclasses |
| `a3_operators.py` | function pods and every operator, public interface |
| `a3_pipeline_stores.py` | `PipelineJob` on in-memory, Delta and SQLite, sync and async |
| `a3_cached_rerun.py` (+ `a3_cached_rerun_child.py`) | cached re-run in fresh processes; slow (12 child processes) |
| `a4_annotations.py` | `np.ndarray`, `NDArray[...]`, `Optional`, `list[...]` annotations |
| `a4_foreign_readers.py` | what deltalake, Parquet, polars, pandas and plain NumPy see |
| `b0_common.py` | encoders for the candidates (helper, not an experiment) |
| `b1_starfix_hashing.py` | starfix and Orcapod hashing per candidate, schema hash, layout independence |
| `b2_polars_roundtrip.py` | Polars round trips that operators perform |
| `b3_databases.py` | Parquet, IPC, in-memory, Delta, SQLite per candidate |
| `b4_dtype_strictness.py` | 17 dtype and layout cases per candidate |
| `b5_size_zero_copy_readers.py` | size, zero copy, DuckDB, polars, pandas; `uv run --with duckdb python ...` |
| `b6_torch_mapping.py` | torch 2.15 mapping; `uv run --with torch python ...` |
| `b7_migration.py` | `.npy` to each candidate, coexistence, type change in Delta |
| `b8_vst_support_and_out_of_table.py` | `variable_shape_tensor` in pyarrow 23.0.1, out-of-table size |
| `c1_canonical_digest_prototype.py` | a layout-independent array digest, prototype |

Environment: macOS arm64, Python 3.12, NumPy 2.2.6, pyarrow 23.0.1, polars 1.41.2,
starfix 0.4.0, torch 2.15 (experiments only). **Not verified:** Linux, x86-64, Windows,
big-endian hosts, CUDA, NumPy versions other than 1.26.4 and 2.2.6.

---

## Part A: today's NumPy support

Code: `src/orcapod/logical_types/numpy_type.py` (`LogicalNumpyArray`), registered in
`contexts/data/v0.1.json` with `NumpyArrayHandler` (`hashing/semantic_hashing/builtin_handlers.py`).

### A1. What is stored

- Column type: `extension<numpy.ndarray>` over `large_binary`, non-null unless the
  annotation is optional (a1, a4).
- Value: exactly `np.save(buf, arr, allow_pickle=False)`. The magic is `\x93NUMPY\x01\x00`
  and the header is a Python dict literal, e.g. `{'descr': '<f8', 'fortran_order': False,
  'shape': (2, 3), }`, padded so data starts at byte 128. A 48-byte array takes 176 bytes (a1).
- The Arrow extension metadata is empty. Dtype and shape live only inside each value's
  header (a4).
- The memory layout is stored as given: a Fortran-ordered input is stored with
  `fortran_order: True` and read back Fortran-ordered. Byte order is stored as given: `>f4`
  reads back as `>f4` (a1, a2).

### A2. What is hashed

The content hash of a datagram holding an array is starfix `hash_table` over a non-null
`large_binary` column whose value is:

```
b"numpy:ndarray::sha256:" + sha256(np.save(np.load(stored_bytes)))
```

a1 rebuilds that column by hand and gets the same digest as `arrow_hasher.hash_table`. The
hash therefore commits to the **whole `.npy` file, header included**: dtype string with byte
order, `fortran_order` and shape, plus the raw buffer in stored order. The array is decoded
and re-encoded for hashing; peak traced memory was 3.1× the array size for a 128 MiB array (a1).

A bare `ndarray` hashed by the semantic hasher (e.g. as a function argument) gives
`ContentHash("sha256", sha256(.npy))`, the same digest without the starfix wrapper (a1).

### A3. Hash stability

| Variation | Same hash? | Evidence |
|---|---|---|
| Across processes | Yes (3 processes, 1 hash) | a1 |
| NumPy 1.26.4 against 2.2.6 (16 arrays, all kinds) | Yes, bytes and Orcapod hash identical; 1.26 bytes load in 2.2.6 exactly | a1b |
| C against Fortran order, equal values | **No** | a1, c1 |
| Non-contiguous strided view against its C copy | Yes (`np.save` writes it C-order) | a1 |
| Transposed view (F-contiguous) against its C copy | **No** | a1 |
| `<f4` against `>f4`, equal values | **No** | a1 |
| `int32` against `int64`, equal values | No (expected: dtype is identity) | a1 |
| Array read from a v2.0 `.npy` file | Yes: it is re-encoded as v1.0 on store | a1 |
| Header version chosen by `np.save` | v1.0 normally; **v3.0** for non-Latin-1 field names | a1 |
| Structured fields `(a, b)` against `(b, a)`, equal records | **No** (ITL-465 asked for yes) | a2 |
| `0.0` against `-0.0`; two NaN payloads | No: the hash is over bits | a2 |
| 0-d, `(0,)`, `(0, 3)`, `(3, 0)` | Round trip exact; empty shapes hash distinctly | a1 |
| 128 MiB array | Works; 0.11 s; 3.1× peak memory | a1 |
| Values over 2 GiB | Not verified | — |
| Cross-platform (x86-64, Linux) | Not verified. `longdouble` is `float64` on this machine and 80-bit elsewhere, so it is platform-dependent by construction | a2 |

**Assessment.** The hash is deterministic, but it is a hash of a NumPy file, not of an array
value. It is also only reproducible by code that writes `.npy` byte for byte, padding
included. A Rust or tensor implementation would have to emit NumPy's header formatting.

### A4. Dtype coverage and failure modes

| Input | Result | Evidence |
|---|---|---|
| bool, int8, uint64, float16/32/64, complex64/128 | Exact round trip | a2 |
| `datetime64[ns]`, `datetime64[D]`, `timedelta64[s]` | Exact | a2 |
| `U5`, `S3`, `V4` | Exact | a2 |
| Structured, nested structured, structured with subarray field | Exact | a2 |
| Big-endian `>i4` | Exact, stays big-endian | a2 |
| `longdouble` | Exact on this machine (it is `float64` here) | a2 |
| NumPy 2 `StringDType` | **Fails** with "Object arrays cannot be saved when allow_pickle=False" and a pickle warning. Its `dtype.kind` is `T`, so the object-dtype guard misses it | a2 |
| `dtype=object` | Rejected with a clear `ValueError` | a2 |
| Structured with an object field | Rejected by `np.save` (less clear message) | a2 |
| `np.ma.MaskedArray` | **Defect: accepted, the mask is silently dropped.** It reads back as a plain array of the underlying values and hashes as the unmasked data | a2 |
| `np.float32`, `np.int64`, `np.bool_` scalars | Not arrays: typed `float`/`int`/`bool`, Arrow width kept (`float`, `int64`, `bool`) | a2 |
| `np.matrix`, `np.recarray`, user subclass | Accepted, read back as plain `ndarray` (subclass lost) | a2 |

### A5. Pipeline behaviour (public interface)

| Path | Result | Evidence |
|---|---|---|
| Pod returning, taking, and chaining arrays | Dtype, shape, values and Fortran order kept | a3_operators |
| Join, SemiJoin, MapData, Select/Drop (4 operators), PolarsFilter | Arrays kept; column stays `extension<numpy.ndarray>` | a3_operators |
| Batch, GroupBy, MergeJoin with colliding array column | Kept; column becomes `list[numpy.ndarray]` | a3_operators |
| Index over Batch/GroupBy results | Gives `numpy.ndarray` back, values exact | a3_operators |
| Pick on an array column | Raises `NotImplementedError` (not `InputValidationError`) | a3_operators |
| MergeJoin ordering of merged arrays | Sorted by stored `.npy` bytes, not by value (107.5 before 7.5). Deterministic, but tied to header bytes | a3_operators |
| MergeJoin float32 array with float64 array | **Accepted silently** under one column. A plain-type mismatch raises `InputValidationError` | a3_operators |
| `PipelineJob`, in-memory and Delta, sync orchestrator | Arrays exact at every node | a3_pipeline_stores |
| Cached re-run in a fresh process, Delta, sync→sync and async→async | Run 2 calls pods 0 times; arrays exact; source, row, output and node hashes identical across processes | a3_cached_rerun |
| SQLite store | **Fails, also for plain ints** (not array-specific) | a3_pipeline_stores |
| Async run, then `iter_data` on a GroupBy node | **Empty, also for plain ints** (not array-specific) | a3_pipeline_stores |
| Sync run, then async run, same Delta store | **Fails, also for plain ints**: non-nullable `_tag_source_id` violation (not array-specific) | a3_cached_rerun |

### A6. Annotations

| Annotation | Result | Evidence |
|---|---|---|
| `np.ndarray` | Works | a4_annotations |
| `numpy.typing.NDArray[np.float32]`, `np.ndarray[Any, np.dtype[np.float32]]` | **Rejected** at pod build ("Unsupported annotation"), on parameters and returns; also rejected inside a `Schema` | a4_annotations, b1 |
| `Optional[np.ndarray]`, `np.ndarray \| None` | Work; `None` is null in the same column | a4_annotations |
| `list[np.ndarray]`, `tuple[np.ndarray, np.ndarray]` | Work | a4_annotations |
| `dict[str, np.ndarray]` | Rejected with the documented ET1 error | a4_annotations |
| Pod `-> np.ndarray` returning `np.float32` (e.g. `x * 2` on a 0-d array) | **Defect**: not caught at the pod. `as_table()` later fails with "Complex unions…" (mixed) or `ArrowTypeError` (all 0-d) | a3_operators |

So a pod cannot declare an array's dtype or shape, and nothing enforces either. Two sources
with float32 `(2, 3)` and int64 `(5,)` columns have equal schemas and equal pipeline hashes (a2).

### A7. What other tools see

| Reader | Sees | Evidence |
|---|---|---|
| deltalake without Orcapod | `binary`; the Delta log keeps `ARROW:extension:name = numpy.ndarray`, empty extension metadata | a4_foreign_readers |
| Parquet footers | **Mixed within one table**: of 3 data files, some are plain `binary` with no extension name and others are `large_binary` with `ARROW:extension:name = numpy.ndarray`. A tool that reads one data file may not see that the column holds arrays. The cause (different write paths) is not verified | a4_foreign_readers |
| polars, pandas, DuckDB | Opaque bytes (`Binary`, `object`, `BLOB`) | a4_foreign_readers, b5 |
| plain NumPy | Every value decodes with `np.load(io.BytesIO(v), allow_pickle=False)`, big-endian and Fortran included | a4_foreign_readers |
| `pl.read_delta` with Orcapod imported | **Fails**: "data type mismatch … Binary != Extension(numpy.ndarray, Binary)" | a4_foreign_readers |

### A8. Defects (reproductions; none fixed)

Array-specific:

1. **Masked arrays lose their mask silently** (a2).
   ```python
   m = np.ma.array([1.0, 2.0, 3.0], mask=[False, True, False])
   src = DictSource(data=[{"k": "a", "x": m}], tag_columns=["k"], source_id="s")
   next(iter(src.iter_data()))[1]["x"]  # array([1., 2., 3.]), plain ndarray
   ```
2. **A pod declared `-> np.ndarray` may return a NumPy scalar unchecked** (a3_operators). The
   cause is `function_pod.py` inferring types from values in `python_dicts_to_struct_dicts`
   even though the schema is known.
   ```python
   @function_pod(output_keys="y")
   def f(x: np.ndarray) -> np.ndarray: return x * 2
   s = DictSource(data=[{"k": "a", "x": np.ones(2, np.float32)},
                        {"k": "d", "x": np.array(7.5, np.float32)}],
                  tag_columns=["k"], source_id="s")
   f.pod(s).as_table()  # ValueError: Complex unions ... Union[numpy.float32, numpy.ndarray]
   ```
3. **`StringDType` fails with a misleading pickle error** (a2):
   `DictSource(data=[{"k": "a", "x": np.array(["a"], dtype=np.dtypes.StringDType())}], tag_columns=["k"])`.
4. **`NDArray[np.float32]` is rejected** as a pod annotation and in a `Schema` (a4, b1).
5. **MergeJoin merges arrays of different dtypes under one column** (a3_operators). The column
   type carries no dtype, so the disagreeing-types rule cannot apply.
6. **`pl.read_delta` fails on an Orcapod store once Orcapod is imported** (a4_foreign_readers).
7. **Pick on an array column raises `NotImplementedError`** rather than `InputValidationError`
   (a3_operators).

Not array-specific, found on the way (out of ITL-874's scope; reported in the hand-off, not filed):

8. SQLite is unusable as a `PipelineJob` store: ext columns are refused ("does not preserve
   Arrow metadata"), and list columns fail to bind, plain ints included. The sync run only logs
   a warning and ends with empty nodes; the async run raises (a3_pipeline_stores, b3).
9. After an async run, `job.nodes[...].iter_data()` on a GroupBy node is empty, and the
   downstream node sometimes shows one group of two (a3_pipeline_stores).
10. A sync run followed by an async run on the same Delta store fails with a non-nullable
    `_tag_source_id` violation (a3_cached_rerun).
11. `arrow_hasher.hash_schema` raises `TypeError: unhashable type` on every live extension type,
    today's `numpy.ndarray` included (ITL-848, confirmed; b1).
12. Delta read failures are swallowed and returned as `None`, which looks like "no rows" (b3, b7).

### A9. Is today's support compatible with future designs?

- **Storage: yes, as a source.** Every `.npy` value converts exactly into candidate 4c (b7),
  and old and new columns can be told apart by extension name and by the `.npy` magic (b7).
- **Hash: no.** Any layout-independent hash, any field-order-independent structured hash
  (ITL-465) and any hash shared with tensors gives different values from today's. Changing it
  changes the record id of every row that has an array input or output.
- **Schema: neutral.** Today's schema carries no dtype or shape, so adding them later changes
  the column type. Nothing stored today depends on them.

---

## Part B: candidate designs

### B1. Candidates

| # | Name | Storage | Dtype and shape live in |
|---|---|---|---|
| 1 | `.npy` envelope (today) | `large_binary` `.npy` file | each value's header |
| 2 | `arrow.fixed_shape_tensor` | `fixed_size_list<T>[prod(shape)]` | type metadata: shape per column, plus permutation |
| 3 | `arrow.variable_shape_tensor` | `struct<data: list<T>, shape: fixed_size_list<int32>[ndim]>` | value type per column; shape per row |
| 4a | Orcapod type, flat | `large_list<T>` | type metadata: dtype and shape per column |
| 4b | Orcapod type, per row | `struct<shape: large_list<int64>, data: large_list<T>>` | dtype per column; shape per row |
| 4c | Orcapod type, raw | `struct<shape: large_list<int64>, data: large_binary>`, data little-endian C-order | dtype tag (full `.npy` descr) and shape per row |
| 5 | Out of table (ITL-467) | a reference in the row; the bytes in a file | the file |

4a–4c were built as throwaway `pa.ExtensionType` subclasses inside the scripts (b0). They
show layout behaviour; they are not a proposal for exact field names.

### B2. Comparison

| Criterion | 1 npy | 2 fst | 3 vst | 4a flat | 4b row | 4c raw | 5 out |
|---|---|---|---|---|---|---|---|
| pyarrow 23.0.1 support | ext type of ours | built in | **none built in**; definable in Python (b8) | ours | ours | ours | n/a |
| `isinstance(t, pa.ExtensionType)` | yes | **no** (b1, b8) | yes (ours) | yes | yes | yes | — |
| Orcapod `hash_table` today | OK (`.npy` token) | **fails** | **fails** (`fixed_size_list` shape) | OK | OK | OK | refs only |
| starfix on storage form + metadata | OK | **fails** (`fixed_size_list`) | **fails** | OK | OK | OK | — |
| `hash_schema` on live type | fails (ITL-848) | fails | fails | fails | fails | fails | — |
| Schema hash changes with shape / dtype | no / no | yes / yes | no / yes | yes / yes | no / yes | no / no¹ | — |
| Polars join, filter and restore | kept | restored (Polars `Array`, ext dropped) | restored | restored | restored | restored | — |
| Parquet, IPC, in-memory DB | OK | OK | OK | OK | OK | OK | — |
| Delta | OK | **read fails, shown as no rows** | **read fails** | OK | OK | OK | not verified |
| SQLite connector | storage-only OK | fails | fails | fails | fails | fails | not verified |
| bool, datetime, strings | exact | **refused** | strings come back as object | exact | exact | exact | — |
| complex, structured | exact | **refused** | refused | refused | refused | **exact** | — |
| 0-d, empty | exact | **0-d refused** | exact | exact | exact | exact | — |
| Big-endian input | kept as `>f8` | `from_numpy_ndarray` raises; `np.stack` byte-swaps silently | converted to LE | LE by design | LE | LE | — |
| Same hash for C/F order and byte order | **no** | permuted input gives another type | not hashable | yes | yes | yes | — |
| Parquet size, 16 MB payload | 16.00 MB | 18.39 MB | 18.39 MB | 18.39 MB | 18.39 MB | **16.00 MB** | 2.4 KB + 16 MB files |
| Zero-copy decode | `np.load` copies; header parse + `frombuffer` does not | yes | yes | yes | yes | yes | n/a |
| Other tools | opaque bytes | `FLOAT[]`, shape lost without metadata | struct of data and shape | `FLOAT[]` | struct | struct, data as BLOB | files |
| torch tensor and ndarray, same storage | yes | yes | yes | yes | yes | yes | — |
| bfloat16, float8, complex32 | `.numpy()` fails | no Arrow type | no Arrow type | no Arrow type | no Arrow type | **exact as int bits + dtype tag** | — |
| Migrate a mixed-dtype `.npy` column | — | **fails** | **fails** | **fails** | silent cast | exact² | — |

Evidence: b1 (hashing, schema), b2 (Polars), b3 (databases), b4 (dtypes), b5 (size, readers),
b6 (torch), b7 (migration), b8 (vst, out of table).

¹ In the b0 prototype of 4c the dtype is per row, so neither shape nor dtype enters the schema.
Putting the dtype in type metadata instead (as in 4b) is possible when the annotation names it.
² b7 shows 4c value-exact for every case. Big-endian `.npy` values convert `>f4` to `<f4`,
equal in value but a different NumPy dtype, and Fortran order becomes C order. Whether byte order is
identity is a developer decision (D4).

### B3. Observations that drive the choice

1. **The hash can be defined apart from the storage.** When a semantic handler is registered
   for the Python type, starfix only sees the handler's token (a1). A storage change can keep
   hashes, and a hash change can keep storage. Without a handler, starfix hashes the storage
   form plus the extension name, and the hash then follows the layout (b1).
2. **The pipeline hash uses Python types, not Arrow types** (b1: `Schema({'x': np.ndarray})`
   hashes as `type:numpy.ndarray`). Putting dtype or shape in the Arrow type does not reach
   the pipeline hash unless the Python-side type carries them. Today `NDArray[...]` is
   rejected (A6), so per-column dtype or shape needs annotation work first (ITL-869 overlaps).
3. **A storage change under an unchanged pipeline hash writes to the same table.** In Delta,
   appending new-format rows to a `.npy` table is accepted. The next read fails and Orcapod
   returns no rows, so the old results look dropped (b7). Any format change must therefore
   write new tables through an explicit migration (the ITL-535 framework: one migration per
   minor release, version in the path).
4. **`fixed_shape_tensor` fits fixed-shape batches, not Orcapod values.** Shape per column means
   every row in a column has one shape and the shape must be known at schema time. It refuses
   0-d arrays and half of NumPy's dtypes. Its type identity also depends on how it was built
   (default permutation and `[0, 1]` give different IPC bytes; b1).
5. **4c keeps everything `.npy` keeps, in explicit fields.** It is exact for structured,
   complex, datetime, strings and torch-only dtypes, because the dtype is a tag and the data is
   raw bytes. Starfix hashes it today. Size equals `.npy`. Other tools see the shape.

### B4. How a `torch.Tensor` maps (b6, torch 2.15, CPU only)

| Tensor property | Mapping | Evidence |
|---|---|---|
| CPU, plain numeric dtype | Same storage bytes as the equal ndarray in every candidate | b6 |
| `requires_grad=True` | `.numpy()` refuses; needs `detach()`. Proposal: not identity, not stored (ITL-871 expectation) | b6 |
| Device mps | `.numpy()` refuses; needs `.cpu()`, and the device is then lost. CUDA not verified | b6 |
| Sparse layout | `.numpy()` refuses. Proposal: reject | b6 |
| Non-contiguous (`t.T`) | `.npy` bytes differ from its contiguous copy (Fortran flag); 4a–4c canonicalise | b1, b6 |
| bfloat16, float8, complex32 | No NumPy or Arrow type (`.numpy()` raises). Their bits round-trip exactly as int16/uint8 in `.npy` or 4c, but only a format with its own dtype tag (4c) can record that they were bfloat16 | b6 |
| DLPack | Zero-copy both ways (Arrow values to torch, CPU tensor to NumPy) | b6 |

### B5. Should an ndarray and a tensor with equal dtype, shape and values hash the same?

Today the token starts with the extension name (`numpy:ndarray::`). So two logical types hash
differently even if their digests agree. Options:

- **Same hash.** Store and hash both through one logical type. Results computed with NumPy
  cache-hit for a torch pipeline. But the Python type a pod returns would change on read
  unless the annotation decides.
- **Different hashes, same digest body (my pick).** Each type keeps its own name in the
  token, and both use one canonical array digest (c1). The two stay distinct for caching, but
  share code, golden tests and a cross-language definition.

### B6. What starfix and starfix-python would need (from `arrow_digester.py`, confirmed by b1)

- Value hashing for `fixed_size_list`. It falls to the leaf path and raises today. Needed for 2 and 3.
- Extension types: hash the storage plus a canonical `ARROW:extension:name` and metadata,
  including built-in canonical types such as `fixed_shape_tensor`, which are not
  `pa.ExtensionType` instances. Needed for 2 and 3, and for `hash_schema` on any live extension
  type (ITL-848).
- Canonical extension metadata, so a defaulted permutation and `[0, 1]` hash the same. Needed for 2.
- Nothing for 4a–4c: they hash today through Orcapod's normalisation (b1).

### B7. Migration path from today's `.npy` columns, per candidate

Rule: pre-computed results must never appear dropped, must not be recomputed, and there is no
silent version bump.

| Candidate | Lossless conversion | Coexistence on read | Path |
|---|---|---|---|
| 1 `.npy` | — | — | No migration |
| 2 fst | No: mixed shapes and dtypes per column fail; 0-d, bool, complex, structured refused | By extension name | Not viable for existing data |
| 3 vst | No for complex, structured, strings | By extension name | Not viable; pyarrow has no type |
| 4a flat | No: needs one dtype and shape per column | By extension name | Only for homogeneous columns |
| 4b row | No: needs one dtype per column; mixed columns cast silently | By extension name | Only for single-dtype columns |
| 4c raw | **Yes for values** in every b7 case, structured and complex included. Big-endian values become little-endian, a different NumPy dtype (D4). Memory order becomes C | By extension name, plus `\x93NUMPY` magic on old values | Explicit migration to new tables. Old hashes change, so record ids must be re-derived from stored values, not by re-running functions (feasibility not verified) |
| 5 out of table | Yes (write the `.npy` bytes to a file) | Reference column | Same as 4c, and needs ITL-467 first |

For every candidate, writing the new format under the old pipeline path hides old rows in Delta
(B3.3). A reader that keeps `LogicalNumpyArray` registered for `numpy.ndarray` can still
decode old values side by side with the new type (b7). What it cannot do is mix both in one
Delta table.

---

## Part C: recommendation

### C1. What should ship in v0.3 (tag target November 16)

**Option A: ship `.npy` unchanged; fix the defects that do not move hashes; label arrays provisional.**
- Fixes: reject masked arrays and `StringDType` with clear errors, validate the declared
  `-> np.ndarray` return, and make Pick raise `InputValidationError`. A valid array's hash
  does not move.
- Consequences: the workshop gets arrays that work end to end today (A5). Public users store
  `.npy` data with the file-based hash. Moving to the long-term design in v0.4 needs one
  explicit migration that rewrites and rekeys. That conversion is shown lossless (b7).
- Risk now: lowest.

**Option B: keep `.npy` storage, change only the hash to the canonical array digest now.**
- Consequences: the hash becomes layout-, byte-order- and (with ITL-465) field-order-independent
  before public data exists. A v0.4 storage change could then keep hashes, since storage-only
  migrations need no rekeying (B3.1).
- Cost: every hash involving an array moves in v0.3. That is a hash change days before a public
  release, and it needs a v0.2→v0.3 rekeying migration for internal stores, or an explicit
  decision that v0.2 array caches are abandoned. Pipeline hashes do not move. Record ids do, so
  new rows land beside the old ones in the same table. Whether Phase 1 then shows duplicates is
  not verified.

**Option C: move to candidate 4c in v0.3, for NumPy and PyTorch.**
- Consequences: the long-term format ships first, so public data needs no later migration.
- Cost: a new logical type, a new hash, a migration of `.npy` tables to new paths, and golden
  tests, all within six days. That is the most work and the most risk for the workshop.

**Option D: adopt `arrow.fixed_shape_tensor`.** Not recommended. Starfix cannot hash it, Delta
reads fail, and it refuses common dtypes and 0-d arrays (B2).

**My pick: Option A for v0.3, with 4c as the v0.4 target.** In v0.4, storage and hash move
together under one explicit migration. Today's storage is lossless and stable, so v0.3 data is
safe to carry forward (A3, A4, b7). The only thing that later changes is the hash, which is
the same cost whether it changes now or in v0.4, but now it lands on a release a week before
a public workshop. If the developer prefers paying the hash change before there are public
users, Option B is the alternative. Option C is not realistic in the time.

### C2. PyTorch (ITL-871) in v0.3?

- **T1 (my pick): wait for v0.4, built on 4c.** At the workshop, convert at the pod boundary
  (`t.detach().cpu().numpy()` in, `torch.from_numpy(a)` out). This adds no stored format.
- **T2: ship `torch.Tensor` over `.npy` in v0.3.** CPU tensors only. Reject bfloat16, float8,
  complex32, sparse and `requires_grad`, or detach them first. This is the "copy the `.npy`
  envelope" path the developer wanted to avoid. Its data would migrate with NumPy's in v0.4.
- **T3: ship torch on 4c in v0.3.** Only together with Option C.

### C3. Decisions for the developer

| # | Decision | Options | My pick |
|---|---|---|---|
| D1 | v0.3 array format | A / B / C (C1) | A |
| D2 | PyTorch in v0.3 | T1 / T2 / T3 (C2) | T1 |
| D3 | Array identity | file bytes (today) / value: dtype, shape, C-order buffer | value |
| D4 | Is byte order identity? (`>f4` against `<f4`) | yes, keep / no, canonicalise to little-endian | no |
| D5 | Structured field order identity (ITL-465) | order matters / sorted by name | sorted (as ITL-465 asked) |
| D6 | NumPy and tensor with equal values | same hash / distinct (B5) | distinct, shared digest |
| D7 | Shape and dtype in the schema | never / later via annotations (ITL-869), as type metadata | later, optional |
| D8 | Masked arrays | reject / support (mask as a field) | reject now |
| D9 | v0.2 internal stores holding arrays | migrate / declare abandoned (explicitly) | depends on D1; migrate if hashes move |

### C4. Shape of the long-term design (4c), for reference

- One Orcapod logical type per Python type (`numpy.ndarray`, later `torch.Tensor`) over a
  shared storage struct: dtype tag, shape (`large_list<int64>`), data (`large_binary`,
  little-endian, C order). Exact names to be designed.
- Hash: the canonical array digest (c1) under each type's name. It is defined in a few lines
  (a length-prefixed JSON header with dtype and shape, then the buffer), so it can be
  reimplemented outside Python.
- Optional per-column dtype and shape constraints from annotations, recorded in extension
  metadata once annotations can carry them.
- Export to `arrow.fixed_shape_tensor` for tools that read it, as a view, not as storage.
- Large values: the same struct with the data field replaced by a reference, once ITL-467
  exists.
