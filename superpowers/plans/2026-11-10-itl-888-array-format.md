# Value-based array format (ITL-888) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Store and hash `numpy.ndarray` values in the v0.3 value-based format of
`superpowers/specs/2026-11-10-array-format-v0.3.md`.

**Architecture:** A NumPy-free module `logical_types/array_format.py` owns the storage struct and
the value digest. `logical_types/numpy_type.py` maps NumPy dtypes to tags and canonicalises
arrays. `NumpyArrayHandler` hashes through the shared digest. Public-interface tests pin
behaviour across operators, orchestrators, databases and processes.

**Tech Stack:** numpy 2.2.6, pyarrow 23.0.1, polars 1.41.2, deltalake 1.0.2, starfix, pytest.

**Gating:** Phase 1 is approved by the coordinator now (November 10). Phase 2 depends on PENDING
decisions (identity rules, names, label, D-PATH) and starts only when the coordinator relays
them. This plan is updated with the decided values before Phase 2 starts.

---

## File structure

| File | Responsibility |
|---|---|
| `src/orcapod/logical_types/array_format.py` (new) | Storage struct, format version, `array_digest_v1`. No NumPy import. Shared with torch (ITL-871). |
| `src/orcapod/logical_types/numpy_type.py` | dtype to tag and back, canonicalisation, refusals, `LogicalNumpyArray` (Phase 2) |
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | `NumpyArrayHandler` on the new digest and label (Phase 2) |
| `src/orcapod/core/function_pod.py` (or the converter it calls) | declared `np.ndarray` return check (Phase 2) |
| `src/orcapod/contexts/data/v0.1.json` | changelog line (Phase 2) |
| `tests/test_arrays/` (new package) | public-interface array tests (Phase 1 scaffolding, Phase 2 format tests) |
| `tests/test_logical_types/test_array_format.py` (new) | digest against an independent reference |

---

## Phase 1 (approved)

### Task 1: storage struct and value digest

**Files:**
- Create: `src/orcapod/logical_types/array_format.py`
- Test: `tests/test_logical_types/test_array_format.py`

- [ ] **Step 1: Write the failing test.** The reference is written independently with `struct`
  and `hashlib` only, framing per spec C1:

```python
import hashlib
import struct

import pyarrow as pa
import pytest

from orcapod.logical_types.array_format import (
    ARRAY_STORAGE_TYPE,
    array_digest_v1,
)


def reference_digest(tag: str, shape: tuple[int, ...], data: bytes) -> bytes:
    t = tag.encode("utf-8")
    p = struct.pack("<Q", len(t)) + t
    p += struct.pack("<Q", len(shape)) + b"".join(struct.pack("<q", d) for d in shape)
    p += struct.pack("<Q", len(data)) + data
    return hashlib.sha256(p).digest()


CASES = [
    ("float32", (2, 3), bytes(range(24))),
    ("float64", (), b"\x00" * 8),          # 0-d
    ("int8", (0, 3), b""),                 # empty
    ("int8", (3, 0), b""),
    ('[["a","int32",[2]],["b","float64"]]', (1,), bytes(16)),
    ("ucs4[2]", (1,), "hé".encode("utf-32-le")),
]


@pytest.mark.parametrize(("tag", "shape", "data"), CASES)
def test_digest_matches_reference(tag, shape, data):
    assert array_digest_v1(tag, shape, data) == reference_digest(tag, shape, data)


def test_digest_accepts_memoryview():
    data = bytes(range(24))
    assert array_digest_v1("float32", (6,), memoryview(data)) == array_digest_v1(
        "float32", (6,), data
    )


def test_digest_separates_fields():
    assert array_digest_v1("int8", (0, 3), b"") != array_digest_v1("int8", (3, 0), b"")
    assert array_digest_v1("int8", (), b"\x01") != array_digest_v1("int8", (1,), b"\x01")
    assert array_digest_v1("int8", (2,), b"\x01\x02") != array_digest_v1(
        "uint8", (2,), b"\x01\x02"
    )


def test_storage_type():
    assert ARRAY_STORAGE_TYPE == pa.struct(
        [
            pa.field("dtype", pa.large_string(), nullable=False),
            pa.field(
                "shape",
                pa.large_list(pa.field("item", pa.int64(), nullable=False)),
                nullable=False,
            ),
            pa.field("data", pa.large_binary(), nullable=False),
        ]
    )
```

- [ ] **Step 2:** `uv run pytest tests/test_logical_types/test_array_format.py -v`. Expected: FAIL
  (`ModuleNotFoundError: orcapod.logical_types.array_format`).
- [ ] **Step 3: Implement** `array_format.py`: `ARRAY_STORAGE_TYPE` as above;
  `array_digest_v1(tag, shape, data)` feeding `hashlib.sha256` incrementally (tag length and tag,
  ndim and dims, data length, then the buffer itself so a large array is not copied); validate
  that every dim is a non-negative int. Google-style docstrings carrying the byte-level
  definition.
- [ ] **Step 4:** rerun, expected PASS. `uv run ruff format . && uv run ruff check .`,
  `uv run --with basedpyright basedpyright` exit 0.
- [ ] **Step 5: Commit** `feat(arrays): shared array storage struct and value digest v1 (ITL-888)`.

### Task 2: public-interface scaffolding true under every option

**Files:** `tests/test_arrays/__init__.py`, `tests/test_arrays/conftest.py`,
`tests/test_arrays/test_array_round_trip.py`, `test_array_operators.py`,
`test_array_pipeline_stores.py`, `test_array_cached_rerun.py`, `test_array_hash_stability.py`.

Every test uses only arrays that are little-endian, C-ordered, plain `np.ndarray` and not
structured with unsorted fields, so it holds under every pending identity option. They pass on
today's code and must keep passing after Phase 2 (a regression harness, not TDD red tests).

- `conftest.py`: `ARRAYS` dict, one per dtype class: bool, int8, uint64, float16, float32,
  float64, complex64, complex128, `datetime64[ns]`, `datetime64[D]`, `timedelta64[s]`, `U5`,
  `S3`, `V4`, structured with sorted fields `[("a","<i4"),("b","<f8")]`, 0-d, `(0,3)`, `(3,0)`,
  3-D. `assert_arrays_identical(a, b)`: same type `np.ndarray`, dtype, shape, `array_equal`
  (with `equal_nan` for floats), and C-contiguous.
- Round trip: `DictSource` then `iter_data()`; a function pod `identity(x: np.ndarray) ->
  np.ndarray`; `np.ndarray | None` with a `None` row.
- Operators (called `op(stream)`, read through `iter_data`/`as_table`): `Join`, `SemiJoin`,
  `MapData`, `SelectDataColumns`, `DropDataColumns`, `PolarsFilter` on a tag, `Batch`,
  `GroupBy`, `MergeJoin` with a colliding array column (values preserved as a set, column is
  `list[np.ndarray]`).
- Pipeline stores: `PipelineJob` with `double` and `total`, in-memory and Delta, `job.run()` and
  `AsyncPipelineOrchestrator().run(job.dag)`; every node's arrays exact.
- Cached re-run: Delta store, two fresh subprocesses; the second calls no pod (call counter
  file) and returns identical arrays and identical node content hashes.
- Hash stability: semantic hash of each array, datagram content hash, and a source's
  `content_hash()` in three subprocesses with `PYTHONHASHSEED` 0, 1 and random: identical.

Steps: write each file, run it (`uv run pytest tests/test_arrays -v`), expect PASS on today's
code; fix the test, not the library, if a test is wrong; report any library failure as a
finding. Commit `test(arrays): public-interface array scaffolding (ITL-888)`.

### Task 3: draft PR

Push the branch, open a draft PR against `main` (body: `Fixes ITL-888`, `Fixes ITL-883`,
`Refs ITL-874`, the spec link, the "draft: decisions pending" note, the trailer line), then
`orca linear attach` and `orca worktree set --worktree current --pr <n>`.

---

## Phase 2 (after decisions; expanded with the decided values before starting)

Each task is test-first: write the failing tests, show the failure, implement, pass, commit.

4. **Tag mapping and canonicalisation** (`numpy_type.py`): `dtype_to_tag`, `tag_to_dtype`,
   `canonicalise(arr)`. Tests: every row of spec A2, structured with unsorted fields, nested and
   subarray fields, refusals (object, `StringDType`, masked, plus the decided ones) naming the
   dtype. Structured copies go field by field (NumPy assigns structured dtypes by position).
   Byte order handled by one function, so preserved vs canonicalised is a local change.
5. **`LogicalNumpyArray` on the struct**: new extension name and metadata, polars extension,
   `python_to_storage`/`storage_to_python` (writeable copy). Tests: stored Arrow type, round
   trip per dtype, extension name `numpy.ndarray` no longer registered.
6. **Column name in conversion errors**: refusal raised through `DictSource` and a function pod
   names the column and the dtype.
7. **`NumpyArrayHandler`**: `ContentHash(<label>, array_digest_v1(...))`. Tests: invariances and
   differences of spec C3 under the decided identity rules; token prefix in a table hash.
8. **Declared `np.ndarray` return check**: a pod returning a NumPy scalar raises the decided
   error naming the output key.
9. **Context JSON changelog line; golden**: merge main once #284 is in. New array pins may be
   added, checked against the Task 1 reference. Existing entries are regenerated only under the
   golden rules in CLAUDE.md: the developer has approved the hash change (November 10, 2026),
   with the migration assessment in spec section E. Only the listed array entries may change,
   and each one is listed in the PR description.
10. **v0.2 store fixture test** (spec E): a small v0.2-format Delta store committed under
    `tests/test_arrays/data/`, generated by a script kept beside it; run the v0.3 pipeline on a
    copy; assert the decided behaviour (new tables, old tables byte-identical, nothing
    unreadable).
11. **Docs and CHANGELOG**: `docs/concepts/arrays.md` (format, dtype tags, identity, refusals,
    known consequence) added to `mkdocs.yml` nav; `docs/reference/hashing.md` array digest
    section with the byte-level definition and test vectors; CHANGELOG entry. Doc samples
    must pass `tests/test_docs`.
12. **Verification**: full suite on 3.11, 3.12, 3.13; ruff; basedpyright; merge origin/main;
    DESIGN_ISSUES.md entry; PR out of draft.

---

## Resume (2026-11-11, after machine restart; dispatch ctx_c41c7c0e38ed)

State on arrival: worktree clean at 9802a9ab, equal to origin; 3 CodeRabbit rounds answered.
main is b197544e (v0.2.0 tagged at 70f53f64; #291 SemiJoin via Polars, ITL-847, merged).

1. Merge origin/main (no rebase). Remove the strict xfail on `test_semijoin`
   (`tests/test_arrays/test_array_operators.py`); it must pass. Drop the "known gap"
   wording in the PR body, CHANGELOG (`[0.3.0]` section) and spec F2.
2. Rerun: full suite on 3.11/3.12/3.13, golden tests (only the ndarray entries listed in the
   PR body may differ from main), ruff check, ruff format --check, basedpyright.
3. Push, wait for CI, request CodeRabbit once, handle a round (test/doc/wording on merit;
   layout/digest/names/identity/behaviour → ask coordinator).
4. If #286 merges meanwhile: merge main again (golden: keep both sets; docs/reference/hashing.md:
   the hash label convention paragraph appears once).
