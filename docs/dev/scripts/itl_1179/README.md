# ITL-1179: Orcapod on starfix 0.6.0 (the Rust implementation): evidence

Stage 1 moves Orcapod from starfix 0.5.0 (the pure-Python starfix-python) to starfix 0.6.0
(the Rust crate's PyO3 binding) with the float options off, and shows that no hash Orcapod
produces moved. Measured on 2026-11-18, macOS arm64, Python 3.12, pyarrow 23.0.1.

## A. Golden values

- `generate_golden_hashes.py --check` under 0.6.0: "golden values of semantic, schema, arrow,
  datagram, pipeline, torch match". Nothing moved.
- `generate_golden_hashes.py --stdout` (every computed entry, pinned or not; 1,565 values in
  six groups) is byte-identical under 0.5.0 and 0.6.0.
- The tests that pin digests outside the golden folder pass under 0.6.0 (`tests/test_hashing`,
  `tests/test_contexts`, `tests/test_logical_types`, `tests/test_arrays`, `tests/test_torch` and
  the twelve other test files that hold a 64-hex literal): 6,111 passed, 33 skipped, 2 xfailed.

## B. Differential corpus

`starfix_differential_corpus.py` builds 4,400 deterministic calls: every primitive width;
floats with subnormals, infinities and the ordinary NaN (no -0.0, no other NaN); decimals,
dates, times, timestamps of every unit with and without zones, durations; strings and binary,
large and not; fixed-size binary (also of size 0); nulls at every level; lists, large lists and
fixed-size lists, nested; structs, nested, in both field orders; dictionaries of every value
kind under four index types, nested, sliced, in lists and structs; slices with offsets; zero
rows and zero columns; six chunkings of one column; pyarrow's canonical extension types; every
Orcapod logical type (path, UUID, file, directory, numpy, pandas, polars, torch, structural and
exact classes, `set[int]`, `list[Path]`); schema and field metadata; the inputs starfix
refuses. Each table goes through `ArrowDigester.hash_table` under all eight option
combinations, `hash_schema` with and without metadata, and Orcapod's `StarfixArrowHasher`
(`hash_table`, `hash_table_ordered` with and without struct field order, `hash_schema`); library
values go through the semantic hasher.

Run once per version (`uv run --with "starfix==0.5.0" ...` for the old one) and compared with
`compare_corpus_runs.py`: 4,367 equal, 0 unexpected, and

- 22 documented differences, the two inputs starfix 0.6.0 lists against 0.5.0
  (`python/tests/_harness.py` `EXPECTED_DIFFERENCES` in the 0.6.0 sdist; `README.md`, "known
  exception"):
  - ITL-1223: a dictionary over a sliced `fixed_size_list<fixed_size_list<int64, 1>, 2>`.
    0.5.0 decodes it with pyarrow's `take`, which returns wrong values on pyarrow >= 17; its
    digest differs from that of the same values built fresh. 0.6.0's digest equals the
    fresh build's (checked), so 0.6.0 is the correct one.
  - A dictionary whose empty string dictionary has no offsets buffer (valid Arrow): 0.5.0
    raises `TypeError`; 0.6.0 hashes it as the allocated equivalent.
- 11 message-only differences: a null in a TOP-LEVEL column declared non-nullable still raises
  `ValueError`, now with the import check's wording ("Invalid argument error: Column 'x' is
  declared as non-nullable but contains null values"; 0.5.0: "Non-nullable field 'x' contains
  nulls"). A nested field keeps 0.5.0's wording ("Non-nullable field 's/a' contains nulls").
  No Orcapod code reads this message; one test matched it and now matches the column name and
  "non-nullable".

## C. Behaviour

- Every refusal keeps its class (`ValueError`) and message, apart from the one above:
  "Ambiguous node path '<path>': two schema nodes share one path ..." (which
  `logical_types/polars_type.py` matches), "Hashing data of type null (node 'c') is not
  supported", "Unsupported Arrow type for hashing: list_view<item: int64>", map, interval,
  view, union and run-end-encoded types.
- `hash_table_timing.py`, 1e6 rows, best of three:

  | | 0.5.0 | 0.6.0 |
  |---|---|---|
  | starfix `hash_table`, int64 + float64 + large_string | 0.746 s | 0.087 s |
  | starfix `hash_table`, `large_list<int64>` | 0.019 s | 0.065 s |
  | Orcapod `hash_table`, flat | 0.713 s | 0.078 s |
  | Orcapod `hash_table`, `large_list<int64>` | 5.870 s | 6.972 s |

  The list column's time in Orcapod is its semantic preprocessing (a Python round trip of
  every nested column), not starfix.

## D. Wheels

starfix 0.6.0 on PyPI: `cp310-abi3` wheels for macOS `universal2` (x86_64 and arm64, macOS
10.12 / 11.0 and later), manylinux2014 x86_64 and aarch64, musllinux 1.2 x86_64 and aarch64,
and Windows amd64, plus the sdist. abi3 covers Python 3.11, 3.12 and 3.13, so Orcapod's matrix
installs from wheels with no Rust toolchain. No wheel: Windows on ARM64, 32-bit platforms, other
Linux architectures, and free-threaded CPython builds (abi3 does not cover them): those build
from the sdist and need Rust.
