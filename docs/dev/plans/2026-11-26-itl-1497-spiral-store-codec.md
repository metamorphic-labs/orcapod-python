# ITL-1497: the codec of a SpiralDB store table, on spiral-sdk

Stage 2 of `docs/dev/specs/itl-1446-round2-spiral-sdk.md` (PR 588), approved by the
coordinator on the developer's behalf (decision 4). It stacks on PR 589 (ITL-1495: spiral-sdk
0.16.78 and its fake).

## What is carried over from PR 545

`src/orcapod/databases/spiral_store.py` and `tests/test_databases/test_spiral_store.py`, from
PR 545's branch, with these changes:

- **The manifest is one payload**: `manifest_bytes` gives its bytes and `read_manifest_bytes`
  reads them back. Kept in a table's metadata, it is `manifest_entries` (the chunks and their
  header), read back by `read_manifest`, which takes the table's whole metadata mapping, never
  raw payload bytes. Where it is kept is
  stage 3's: decision 4 was revised on 2026-11-26 (ITL-1498), and stage 3 keeps it in the
  table's own metadata, in chunks of at most 8 KiB (a key and its value together), beside the
  identity record (`spiral_store_names`, PR 589). It is not kept in the dataset's schema table,
  which is all or nothing under one completion marker. The format stays a draft
  (`orcapod.spiral_store.draft`, version 0) until stage 3 fixes it as version 1.
- **Timestamps are written as they are**, not as integers.
- **A null fixed-size list of structs is kept**, no longer refused.
- Everything else is unchanged: names, types, nullability, refusals before anything is written,
  reading by the manifest's stored names, no row order.

## Measured live with spiral-sdk 0.16.78 (2026-11-26, throwaway tables)

| What | Result |
|---|---|
| booleans, integers (`uint64` max), floats (signed zeros and NaN payloads, `float16` too), `decimal128/256`, `date32`, `date64` off midnight, `null` | kept bit for bit |
| timestamps of every unit, with and without a zone, every `int64` value (`datetime.max` and `.min` included) | kept bit for bit: **written as they are** (pyspiral's service refused instants after 9999-12-30) |
| `time32` / `time64` within a day | kept; **outside one day: the whole write refused**, so times stay integers |
| `decimal32/64` | come back as `decimal128`: written as such, restored |
| strings, binaries, lists | come back in their small variants: normalized on read |
| struct fields | sorted by name at the top level (not inside a list): restored by name |
| map `keys_sorted` | dropped: restored from the manifest |
| a null fixed-size list of structs | **kept** (the refusal is lifted) |
| a null struct item inside a list | kept by the service; still refused by the codec, which does not yet carry a struct's validity (a later lift) |
| a null struct at the top level, an empty struct, `fixed_size_binary`, `duration`, `interval` | refused |
| column and struct field names | refused: a `__` or `$` prefix, any `.`, exactly `*`, the empty name. Kept: `$`, `*` and `;` inside a name, control characters, case and Unicode normal forms apart, 1,024 characters. NUL is cut silently. The codec's escape rule covers every refusal and NUL, so it is unchanged |
| a write of no rows | publishes nothing; a key-only write is refused |

The fake (`tests/test_databases/fake_spiral_sdk.py`) now refuses the empty name, `*` and times
outside a day as the service does. The conformance tests (`test_spiraldb_connector_integration.py`)
run the codec through spiral-sdk on the fake and live: the rows written are the rows read. They
also check that the service refuses each name and value listed in the refusal test
(`test_what_the_store_codec_avoids_is_refused`). Not every value the codec avoids is refused by
the service: the codec also refuses what the service keeps but the codec cannot yet carry back
(a null struct item inside a list).
