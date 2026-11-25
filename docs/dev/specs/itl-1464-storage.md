# ITL-1464 storage compressibility

| Real projection D | Current Delta B/row | Zstd 3 inline B/row | Derived B/row | Integer / hash row lookup B/row | List-only lookup B/row |
| --- | --- | --- | --- | --- | --- |
| 1 output | 432.08 | 221.31 | 208.33 | 234.86 / 291.90 | 221.31 |
| 3 outputs | 526.47 | 266.19 | 226.20 | 279.75 / 336.79 | 266.19 |
| 10 outputs | 850.72 | 408.25 | 275.53 | 421.80 / 478.85 | 408.25 |
| Grouped, 3 outputs | 856.36 | 586.54 | 531.63 | 613.18 / 671.58 | 629.05 |

Keep logical provenance and try Zstd level 1 or 3 with larger commits first, using the existing row order as the baseline. For wide scalar results, deriving tokens from existing record IDs and a source/column dictionary cut a further 33% from the compressed two-table store, so it merits a prototype with measured read cost. Reserve a content-addressed list lookup for workloads with repeated lists across nodes, where modeling five identical copies with one stored list reduced full-group footprints by 46–80%.

All columns above include the pipeline and result Parquet files per pipeline row, or per group in the last row. Delta logs are listed separately below. Each alternative uses the same Zstd 3 selective prefix baseline.

## Workloads

Five read-only Parquet exports replaced live reads. The full-export measurements use file sizes and metadata; the per-row node runs used scalar projections of export prefixes. Two grouped node runs also processed the complete scalar projections of B and D.

| Export | Full rows | Columns | Export bytes | 1-output records | 3-output records | 10-output records | Groups |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A | 279,074 | 7 | 1,732,077 | 25,000 | 3,000 | 3,000 | 12 |
| B | 2,177,155 | 13 | 88,980,206 | 3,000 | 3,000 | 3,000 | 10 |
| C | 1,289,798 | 21 | 18,953,346 | 3,000 | 3,000 | 3,000 | 4 |
| D | 8,087,906 | 8 | 91,418,512 | 3,000 | 3,000 | 3,000 | 100 |
| E | 100 | 10 | 493,978 | 100 | 100 | 100 | 10 |

Each projection used the highest-cardinality non-null numeric column from the selected rows as a float value and order tag. It mapped a repeated scalar field to consecutive integer group keys, preserving equality and row order. A cyclic key with ten groups covered prefixes without a suitable repeated field. The scalar reader excluded nested fields. Real synchronous Orcapod jobs wrote fresh local Delta stores for one, three or ten scalar results per row, and separate jobs computed count, mean and range after grouping.

The 25,000-row narrow run took 122 seconds. At that rate, 2.2 million per-row calls would take about three hours. The larger per-row measurements therefore streamed synthetic records directly into the exact on-disk schemas captured from the real runs. This was a runtime decision; the guarded runs fitted within the memory limit.

The generator calibrated from 250,000-row prefixes of exports B and D, with 250,000 and 248,268 distinct projected values respectively. It produces seeded scalar ranks, shuffles a subset of ranks to approximate local reversals, and advances the categorical key probabilistically to match adjacent repetition. The last two columns show observed calibration and synthetic fractions. The shape uses one result per unique input value. Synthetic UUIDv7-shaped result IDs preserve the time prefix and random suffix. Prefixed binary digests use four independently seeded high-entropy limbs, with repeat values sharing the input or output digest. The synthetic record batches reproduce byte widths and repetition patterns; actual identities, digests and timestamps come from the real jobs.

| Synthetic pipeline rows | Result rows | Outputs | Delta pipeline B/input | Delta result B/input | Zstd 3 total B/input | Savings | Groups calib./synth. | Adjacent group equality | Adjacent value ascent |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2,200,000 | 2,200,000 | 1 | 177.01 | 87.42 | 209.24 | 20.9% | 10 / 10 | 0.000 / 0.000 | 1.000 / 1.000 |
| 2,200,000 | 2,200,000 | 10 | 177.01 | 402.80 | 372.40 | 35.8% | 10 / 10 | 0.000 / 0.000 | 1.000 / 1.000 |
| 8,100,000 | 8,043,883 | 1 | 178.98 | 88.64 | 215.11 | 19.6% | 100 / 100 | 0.032 / 0.032 | 0.500 / 0.500 |
| 8,100,000 | 8,043,883 | 10 | 178.98 | 410.71 | 396.15 | 32.8% | 100 / 100 | 0.032 / 0.032 | 0.500 / 0.500 |

The scale baseline uses fresh delta-rs writes. The full rewrite used Zstd 3, selective prefix encoding and batches of up to 65,536 rows. It did not simulate the real job's 100-row commit cadence. Full-scale codec tests above cover the one- and ten-output schemas; the three-output and grouped results were measured through real nodes.

Apple M5 Max, 128 GiB RAM, macOS 27.2, local APFS. Python 3.12.16, PyArrow 23.0.1, Delta Lake Python 1.6.6, NumPy 2.2.6, Polars 1.41.2, Orcapod base 0d1e017c40ba47105292e81cae8f459053e75b26. Both suites ran serially under the supplied 24 GiB guard.

## Writer settings and column costs

[DeltaTableDatabase](../../../src/orcapod/databases/delta_lake_databases.py) passes no writer properties on creation or on its insert-only MERGE. The actual files used Snappy and dictionary encoding where supported. The database's pending batch threshold is 1,000, and [PipelineJob.run](../../../src/orcapod/pipeline/job.py) commits after 100 rows per function node by default. The D per-row runs each wrote 30 files per table for 3,000 rows. Logs and related Delta metadata contributed 52.70 B/row narrow, 75.30 B/row wide, and 110.55 B/group.

Packed Snappy with dictionaries was 292.10 B/row narrow and 639.04 B/row wide on D. Zstd 3 with selective prefix encoding saved 24% and 36% against those packed baselines with the same rows, schema and row order. Compared with the original small-file store, the savings were 49% and 52%. The latter comparison includes coalescing.

| Column family | Narrow Arrow B/row | Narrow Delta B/row | Narrow Zstd B/row | Wide Arrow B/row | Wide Delta B/row | Wide Zstd B/row |
| --- | --- | --- | --- | --- | --- | --- |
| Input data hash, both tables | 104.00 | 74.81 | 64.57 | 104.00 | 74.81 | 64.57 |
| Pipeline output data hash | 52.00 | 37.41 | 32.32 | 52.00 | 37.41 | 32.28 |
| Pipeline record ID + base entry ID | 104.00 | 74.82 | 64.61 | 104.00 | 74.82 | 64.61 |
| Result record ID + pipeline data ID | 48.00 | 30.86 | 22.37 | 48.00 | 30.90 | 22.38 |
| Pipeline system tags | 51.63 | 6.38 | 0.30 | 51.63 | 6.38 | 0.30 |
| Input provenance tokens | 52.63 | 6.99 | 0.30 | 52.63 | 6.99 | 0.30 |
| Output provenance tokens | 139.00 | 30.58 | 13.13 | 1370.00 | 306.32 | 131.62 |

With Zstd 3 prefix encoding, the input hash accounts for 29% of the combined narrow store and 16% wide. The first four column families total about 184 B/row, or 83% narrow and 45% wide. The shared hash framing compresses, leaving about 32.3 B/row for each distinct 32-byte digest. UUIDv7 time bytes are correlated, leaving about 11 B/row for each binary result ID copy. Source IDs and positional source record IDs are much cheaper. The same UUID rendered as text in ten separate output token columns costs about 132 B/row because the columns compress independently.

The [per-column evidence](../scripts/itl-1464-storage/column-sizes.csv) reports Arrow logical array bytes, compressed Parquet leaf-chunk bytes, bytes per row and shares of each individual file set. Unassigned bytes are file overhead. The grid covers Snappy and Zstd levels 1, 3, and 9 with eligible dictionaries enabled, disabled, and selectively replaced by DELTA_BYTE_ARRAY on token, system tag, hash and ID byte-array leaves. Other nested fields use plain encoding in the prefix trial. The two row orders are stored-file order, including any ordering introduced by MERGE, and tag order. The result permutation follows its first matched pipeline row in tag order.

On the 175,000-row B-like wide sample, Zstd 3 with all dictionaries and with selective prefix encoding gave 409 and 373 B/row at the 131,072-row limit. With prefix encoding, the 16,384 / 131,072 / 1,048,576 limits gave 373.23 / 373.13 / 367.21 B/row. The largest limit used the 175,000 available rows. Sorting by tags gave 380.42 B/row at 131,072, a 2.0% increase. At that limit in stored order, Snappy, Zstd 1, and Zstd 9 with prefix encoding gave 500.00, 364.92, and 372.91 B/row respectively. Zstd 1 with dictionaries off gave 363.79 B/row. The results support skipping dictionaries for high-cardinality columns and testing prefix encoding against plain encoding. Access patterns should inform row-group size and ordering.

## Provenance layouts and reconstruction

| Workload | Layout | Data bytes | Lookup bytes | Total B/row | Change vs inline | Warm read + rebuild ms | Rebuild ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 output | Inline | 663,915 | 0 | 221.31 | +0.0% | 1.87 | 0.00 |
| 1 output | Derived | 622,933 | 2,052 | 208.33 | -5.9% | 3.23 | 1.15 |
| 1 output | 8-byte integer | 643,049 | 61,534 | 234.86 | +6.1% | 2.59 | 0.22 |
| 1 output | 16-byte hash | 728,621 | 147,091 | 291.90 | +31.9% | 2.67 | 0.29 |
| 1 output | Omitted | 622,933 | 0 | 207.64 | -6.2% | 1.25 | — |
| 3 outputs | Inline | 798,574 | 0 | 266.19 | +0.0% | 2.16 | 0.00 |
| 3 outputs | Derived | 676,548 | 2,065 | 226.20 | -15.0% | 3.20 | 1.17 |
| 3 outputs | 8-byte integer | 696,664 | 142,578 | 279.75 | +5.1% | 2.62 | 0.25 |
| 3 outputs | 16-byte hash | 782,232 | 228,135 | 336.79 | +26.5% | 2.79 | 0.29 |
| 3 outputs | Omitted | 676,548 | 0 | 225.52 | -15.3% | 1.28 | — |
| 10 outputs | Inline | 1,224,749 | 0 | 408.25 | +0.0% | 2.73 | 0.00 |
| 10 outputs | Derived | 824,507 | 2,078 | 275.53 | -32.5% | 3.68 | 1.60 |
| 10 outputs | 8-byte integer | 844,611 | 420,793 | 421.80 | +3.3% | 3.20 | 0.43 |
| 10 outputs | 16-byte hash | 930,191 | 506,350 | 478.85 | +17.3% | 3.27 | 0.44 |
| 10 outputs | Omitted | 824,507 | 0 | 274.84 | -32.7% | 1.41 | — |
| Grouped, 3 outputs | Inline | 58,654 | 0 | 586.54 | +0.0% | 1.48 | 0.00 |
| Grouped, 3 outputs | Derived | 51,967 | 1,196 | 531.63 | -9.4% | 1.63 | 0.12 |
| Grouped, 3 outputs | 8-byte integer | 42,254 | 19,064 | 613.18 | +4.5% | 2.15 | 0.16 |
| Grouped, 3 outputs | 16-byte hash | 45,188 | 21,970 | 671.58 | +14.5% | 1.94 | 0.15 |
| Grouped, 3 outputs | List-only hash | 49,852 | 13,053 | 629.05 | +7.2% | 1.61 | 0.09 |
| Grouped, 3 outputs | Omitted | 41,142 | 0 | 411.42 | -29.9% | 1.34 | — |

The two-table totals include both the data and sidecar files. The disk read and in-memory rebuild times sum the medians of three warm runs for the two tables measured separately. The reconstruction uses an equality index and a row gather for lookups, and hexadecimal conversion and Arrow concatenation for derived tokens. All preserving layouts passed comparison to the original stored tables. The omission numbers establish the storage bound.

In the derived layout, the fact already holds the row ID and a per-table dictionary records the fixed source and field suffix. The result ID is rendered once per row and reused for every output column. Variable sources, nulls and input lists need another representation. On the 175,000-row B-like wide sample, derived occupied 44.18 MB of data and a 2.1 KB dictionary, saving 32.3%. Rebuild took 87 ms, including materializing ten strings per row. The integer and hash layouts grew the store by 3.4% and 18.4% and took 28 and 29 ms to rebuild. Both store a key in the fact and the lookup table, and the token tuples are nearly unique per row.

The list-only layout uses a 16-byte content key for the ordered tuple of list-valued tokens while retaining scalar tokens inline. The grouped D pipeline list occupied 158,690 Arrow bytes, 23,248 current Delta Parquet bytes and 10,042 bytes after the Zstd 3 prefix rewrite. A group ID folds its member IDs, so derivation retains the list. To model five nodes with identical lists, count five measured fact writes and one shared sidecar. On D, that is 262,313 bytes versus 293,270 inline, a 10.6% saving, or 524.63 B per node row. Two identical copies reach break-even in this example. This is a modeled chain with unchanged lists and excludes sidecar transaction and lifecycle costs. A single list lookup grew each grouped store in the experiment by 4.7–7.2%.

### Full grouped inputs

The full B run processed 2,177,155 rows into 10 groups in 4.2 seconds. It wrote 11,480,739 bytes of pipeline Parquet and 11,891 bytes of result Parquet. The full D run processed 8,087,906 rows into 100 groups in 16.6 seconds. It wrote 51,057,530 bytes of pipeline Parquet and 27,386 bytes of result Parquet. The five-node column uses the same fixed-list-reuse model. List-only lookup saved about 46% and 80% across five identical nodes over the two full inputs.

| Full grouped input | Groups | Layout | Data B/group | Sidecar B/group | Total B/group | Change vs inline | Five-node change | Warm read + rebuild ms | Rebuild ms |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| B | 10 | Inline Zstd 3 | 5,056.0 | 0.0 | 5,056.0 | +0.0% | +0.0% | 56.1 | 0.0 |
| B | 10 | Derived scalar | 4,746.2 | 119.6 | 4,865.8 | -3.8% | -5.7% | 56.5 | 0.1 |
| B | 10 | 8-byte integer | 1,806.0 | 3,436.0 | 5,242.0 | +3.7% | -50.7% | 81.4 | 22.7 |
| B | 10 | 16-byte hash | 1,832.4 | 3,459.6 | 5,292.0 | +4.7% | -50.1% | 78.7 | 23.1 |
| B | 10 | List-only hash | 2,093.2 | 3,079.8 | 5,173.0 | +2.3% | -46.4% | 76.7 | 21.9 |
| B | 10 | Omitted | 1,734.4 | 0.0 | 1,734.4 | -65.7% | -65.7% | 1.5 | — |
| D | 100 | Inline Zstd 3 | 222,808.0 | 0.0 | 222,808.0 | +0.0% | +0.0% | 297.1 | 0.0 |
| D | 100 | Derived scalar | 222,737.0 | 12.0 | 222,749.0 | -0.0% | -0.0% | 295.1 | 0.1 |
| D | 100 | 8-byte integer | 441.1 | 222,393.3 | 222,834.5 | +0.0% | -79.8% | 384.7 | 95.2 |
| D | 100 | 16-byte hash | 470.5 | 222,422.4 | 222,892.9 | +0.0% | -79.8% | 379.6 | 94.3 |
| D | 100 | List-only hash | 521.2 | 222,329.2 | 222,850.4 | +0.0% | -79.8% | 399.4 | 97.3 |
| D | 100 | Omitted | 430.0 | 0.0 | 430.0 | -99.8% | -99.8% | 1.3 | — |

## Arrow IPC for local spill

These medians use the 175,000-row B-like wide sample. Rates divide Arrow logical bytes by elapsed time. Local writes and warm reads through Arrow OSFile include encoding, decoding and materialization. They exclude durability sync.

| Table | IPC codec | File MB | Write ms | Read ms | Write / read GB/s |
| --- | --- | --- | --- | --- | --- |
| Pipeline | plain | 81.55 | 8.58 | 8.47 | 9.5 / 9.6 |
| Pipeline | lz4 | 38.64 | 13.74 | 4.10 | 5.9 / 19.9 |
| Pipeline | zstd | 27.69 | 15.19 | 4.59 | 5.4 / 17.8 |
| Result | plain | 318.51 | 30.71 | 34.85 | 10.4 / 9.1 |
| Result | lz4 | 80.80 | 23.04 | 11.40 | 13.8 / 27.9 |
| Result | zstd | 43.07 | 27.15 | 10.68 | 11.7 / 29.8 |

Zstd was 28% smaller than LZ4 for pipeline data and 47% smaller for wide results, with 11–18% longer writes. Read times were close and depended on the table. LZ4 is a starting point for frequent spills. Actual join-cache and node-spill schemas should be profiled when disk pressure favors Zstd.

## Reproduction

The [measurement scripts](../scripts/itl-1464-storage/) take user-supplied paths. Run workloads.py with a fresh destination, one of row1, row3, row10 or group, and optionally an authorized export and sample size. Keep export-backed data and scratch files in a private directory. Run measure.py over the Delta destination, then columns.py with anonymous-label=metrics.json arguments to emit the column matrix. Run the large cases under the memory guard.

For an entirely synthetic run, omit the export when building a template with workloads.py. Pass the real job's synthetic template store, a fresh destination and the row count to scale.py. Use either the authorized calibration export or a JSON profile containing rows, group_count, group_adjacent_equal, value_distinct and value_adjacent_ascending. Use the rewrite flag for a full Zstd rewrite and sample-rows 175000 for the layout and IPC grid. Measure whole-scale column sizes with the sizes-only mode of measure.py.
