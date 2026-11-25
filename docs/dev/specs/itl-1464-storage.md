# ITL-1464 storage compressibility

| Workload | Current Delta B/row | Zstd 3 inline B/row | Derived B/row | Integer / hash row lookup B/row | List-only lookup B/row |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 output | ~430 | ~220 | ~210 | ~235 / ~290 | ~220 |
| 3 outputs | ~525 | ~265 | ~225 | ~280 / ~335 | ~265 |
| 10 outputs | ~850 | ~410 | ~275 | ~420 / ~480 | ~410 |
| Grouped, 3 outputs | ~860 | ~585 | ~530 | ~615 / ~670 | ~630 |

Keep logical provenance and try Zstd level 1 or 3 with larger commits first, using the existing row order as the baseline. For wide scalar results, deriving tokens from existing record IDs and a source/column dictionary reduced the compressed two-table store by about a third, with a measurable read cost. Use list lookups where lists recur across nodes, which saved about 45–80% in a five-node reuse model.

The table combines pipeline and result Parquet bytes per pipeline row or group. The current Delta column uses the original files. Each alternative uses Zstd 3 with selective prefix encoding.

## Workloads

Synchronous function nodes wrote to fresh local Delta stores. Scalar projections covered workloads such as a time series table and an event table. Inputs ranged from roughly a hundred rows to several million rows and occupied roughly half a megabyte to a hundred megabytes as Parquet. Per-row runs used bounded samples of up to a few tens of thousands of rows. The larger grouped runs processed millions of inputs into on the order of tens to hundreds of groups.

Each scalar projection used a non-null numeric field as a float value and order tag. It mapped a repeated scalar field to integer group keys, preserving equality and row order. A cyclic key provided groups where a suitable repeated field was unavailable. Grouped nodes computed count, mean, and range.

A narrow real-node sample took about two minutes. At that rate, a million-scale per-row run would take hours. The scale measurements instead streamed seeded synthetic batches into the real nodes' on-disk schemas. The generator varies scalar cardinality, adjacent repetition and ordering independently. Repeated values reuse input and output digests, while distinct values have high-entropy digests. Result IDs use a UUIDv7-shaped prefix and suffix.

| Approximate synthetic scale | Outputs | Delta pipeline B/input | Delta result B/input | Zstd 3 total B/input | Savings |
| --- | ---: | ---: | ---: | ---: | ---: |
| About 2.2 million rows | 1 | ~180 | ~90 | ~210 | ~20% |
| About 2.2 million rows | 10 | ~180 | ~400 | ~370 | ~35% |
| About 8 million rows | 1 | ~180 | ~90 | ~215 | ~20% |
| About 8 million rows | 10 | ~180 | ~410 | ~400 | ~35% |

The scale baseline used fresh delta-rs writes. The rewrite used Zstd 3, selective prefix encoding, and batches of up to 64 Ki rows. This setting differs from the real job's smaller commits.

Apple M5 Max, 128 GiB RAM, macOS 27.2, local APFS. Python 3.12.16, PyArrow 23.0.1, Delta Lake Python 1.6.6, NumPy 2.2.6, Polars 1.41.2, Orcapod base 0d1e017c40ba47105292e81cae8f459053e75b26. The suites ran serially under a 24 GiB memory guard.

## Writer settings and column costs

[DeltaTableDatabase](../../../src/orcapod/databases/delta_lake_databases.py) passes no writer properties on creation or insert-only MERGE. The actual files used Snappy and dictionary encoding where supported. The database's pending batch threshold defaults to 1,000, and [PipelineJob.run](../../../src/orcapod/pipeline/job.py) commits after 100 rows per function node by default. Logs and related Delta metadata added about 50 B/row narrow, 75 B/row wide, and 110 B/group in the compact grouped example.

With the same packed file layout and row order, Zstd 3 with selective prefix encoding saved about 25% narrow and 35% wide versus packed Snappy with dictionaries. Savings against the original small-file store were about 50%, including the effect of coalescing.

The [rounded column ratios](../scripts/itl-1464-storage/column-sizes.csv) report bytes per pipeline row for the main column families in Arrow, Delta and the Zstd rewrite. Under Zstd 3 prefix encoding the input hash accounted for about 30% of the narrow and 15% of the wide store. Input and output hashes and the main record identifiers together accounted for about 85% narrow and 45% wide. Distinct 32-byte digests took about 32 B/row after compression. A binary result ID copy took about 11 B/row because its time prefix is correlated. Wide output token columns together took roughly 130 B/row because the same textual UUID appeared in ten independently compressed columns.

The codec grid compared Snappy and Zstd levels 1, 3, and 9 with dictionaries enabled, disabled, and selectively replaced by DELTA_BYTE_ARRAY for byte-array token, tag, hash, and record ID columns. In a wide synthetic trial, Zstd 3 at a 128 Ki-row limit took about 410 B/row with all dictionaries and 375 B/row with prefix encoding. Smaller row groups gave similar results. A single larger row group improved size by about 2%. Sorting by tags increased size by about 2% in that trial. At the 128 Ki-row limit, Snappy with prefix encoding took about 500 B/row and Zstd 1 or 9 each took around 365–375 B/row. Zstd 1 with dictionaries off was comparable.

## Provenance layouts and reconstruction

| Layout | Narrow change vs. inline | Wide change vs. inline | Grouped sample change vs. inline |
| --- | ---: | ---: | ---: |
| Derived | −6% | −33% | −9% |
| 8-byte integer per row | +6% | +3% | +5% |
| 16-byte hash per row | +32% | +17% | +15% |
| List-only hash | 0% | 0% | +7% |
| Omitted, as a storage bound | −6% | −33% | −30% |

Totals include the data and sidecar files. An equality index and row gather rebuilt lookup values. Derived tokens used hexadecimal conversion and Arrow concatenation. Every preserving variant passed a comparison against the original stored values.

For derivable scalar tokens the data table already holds the record ID, and a small dictionary supplies the source prefix and field suffix. In the wider synthetic trial, derivation took on the order of 90 ms to rebuild ten tokens per row, versus around 30 ms for either row lookup, before adding read time. Both row lookups store a key in the data and the sidecar, and the token tuples are nearly unique per row.

The list-only layout uses a 16-byte content key for ordered list tokens and keeps scalar tokens inline. A grouped record ID cannot recover its members' ordered tokens. The larger grouped runs produced about 10–50 MB of pipeline Parquet and about 10–30 KB of result Parquet. After compressing inline tokens, a single list lookup changed total size by about 0–2%. Read and rebuild added on the order of tens to a hundred milliseconds in those runs. The five-node model counts five data writes and one shared sidecar, giving about 45–80% savings when all five nodes have identical lists. The model assumes fixed reuse and excludes sidecar transaction and lifecycle costs.

## Arrow IPC for local spill

These are rounded warm-cache medians from a wide synthetic trial. The timed writes and reads include encoding, decoding, and materialization and exclude durability sync.

| Table | IPC codec | Approximate size | Write ms | Read ms |
| --- | --- | ---: | ---: | ---: |
| Pipeline | Plain | ~80 MB | ~9 | ~8 |
| Pipeline | LZ4 | ~40 MB | ~14 | ~4 |
| Pipeline | Zstd | ~30 MB | ~15 | ~5 |
| Result | Plain | ~320 MB | ~30 | ~35 |
| Result | LZ4 | ~80 MB | ~25 | ~11 |
| Result | Zstd | ~45 MB | ~27 | ~11 |

Zstd was about 30% smaller than LZ4 for pipeline data and about 45% smaller for wide results, with writes about 10–20% slower. LZ4 is a starting point for frequent spills. Test actual join-cache and node-spill schemas when disk pressure makes the Zstd savings important.

## Reproduction

The [measurement scripts](../scripts/itl-1464-storage/) take user-supplied paths. Run workloads.py with a fresh destination and one of row1, row3, row10 or group. Keep any input-backed data, scratch files and raw metrics local. Run measure.py on the Delta destination and pass narrow and wide local metrics files to columns.py for the rounded column summary.

For a synthetic run, build a template without an export. Pass the template store, a fresh destination, and a chosen row count to scale.py along with a locally supplied profile. The rewrite flag measures a whole-scale Zstd rewrite and sample-rows selects a bounded layout and IPC trial. Use sizes-only mode for streaming column accounting.
