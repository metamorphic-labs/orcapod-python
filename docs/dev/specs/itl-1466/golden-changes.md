# ITL-1466: the exact golden changes, for approval

The developer is asked to approve these 18 changes to golden entries: 6 record-id pins removed, 6
instance-id pins added in their place, 6 table-path entries moved. The values are as the generator
writes them (`_jsonable`: a labelled digest as `<label>:<hex of the digest bytes>`). They were
computed on the prototype, with:
- option A: the entry id without the recomputation index column;
- `pdb_v2`;
- the instance-id preimage column `__pipeline_entry_id`;
- the golden case change of the implementation plan (`docs/dev/plans/itl-1466-plan.md`, stage 1).

The values hold only if stage 1 builds exactly these preimages. Nothing is written to a golden file
until the developer approves.

How the values were obtained, all on the prototype (local branch `scratch/itl-1466-prototype`):
`generate_golden_hashes.py --stdout --group pipeline` twice, with `PYTHONHASHSEED` 1 and 2:
identical output. Then a trial on a copy of the golden tree (`store.copy_tree`, `--root`): the 12
`--remove` below, then the generator with no option. It wrote 4 entries into `pipeline-02.json`
(2 instance-id pins, 2 table paths) and 8 into `provisional/pipeline.json` (4 and 4), recorded both
files in the open revision of `v1.json`, and `--check` then reported every golden value of every
group matching. The diff of the copy against the repository is exactly the entries below.

Files: `job` and `drop_nulls_job` are in the frozen-group file `pipeline-02.json` (open: v1 is not
released); the other four are provisional (`provisional/pipeline.json`, PolarsFilter, Filter,
Rejoin).

Why each moves: a pipeline table's record id is now hash(instance id, result id) and the result id
is a UUIDv7, new in every fresh store, so it cannot be pinned; the golden cases pin the table's
lookup column, the instance id, instead (as they pin `__input_data_hash` for a result table). The
path's last component is the layout, `pdb_v1` to `pdb_v2`. Nothing else in any path moves.

## 1. `pipeline:job` (`pipeline-02.json`)

**Removed** (`--remove`):

```text
job.table[golden_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:b1212cbf9e4025086e622df7d8e9931ddde18946f29dda37568a13f36dfb39ef/pdb_v1].record_ids
[
  "arrow_v1:000002439c8a6f4f392cd14e4e407536566a3e2036d1f5839bd77ad1d99ce86637af05",
  "arrow_v1:0000024e73709dcc24e69e8fed821be20e818f559e8cd26e4532e7d00ad0e2f83404f3"
]
```

**Added** (written by the generator after approval):

```text
job.table[golden_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:b1212cbf9e4025086e622df7d8e9931ddde18946f29dda37568a13f36dfb39ef/pdb_v2].instance_ids
[
  "arrow_v1:0000024609444ffd64b9f53dd01cc32c3c347d475d3c81ab72bd79ee97852cc9fda584",
  "arrow_v1:000002e0a4225b4f5ecc73b83d2423189934560f2bcd78674edfc15bf894c14ea0fa0f"
]
```

**Moved**, `job.table_paths` (`--remove`, then written again):

```text
before:
[
  "golden_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1",
  "golden_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:b1212cbf9e4025086e622df7d8e9931ddde18946f29dda37568a13f36dfb39ef/pdb_v1"
]
after:
[
  "golden_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1",
  "golden_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:b1212cbf9e4025086e622df7d8e9931ddde18946f29dda37568a13f36dfb39ef/pdb_v2"
]
```

## 2. `pipeline:drop_nulls_job` (`pipeline-02.json`)

**Removed** (`--remove`):

```text
drop_nulls_job.table[golden_drop_null_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:fbe5ea222e164ffeeb59b89b9c4836a3f1b7e5f580dce814091fe75bb60bff20/pdb_v1].record_ids
[
  "arrow_v1:00000208e68d47a17985bf24008c03ed0b445b040297a6895090867117a175faa6a9cc",
  "arrow_v1:0000025960b646e835f382c9fb9b3bcb207eee5f3ca389d86fc20f6486e53e6dfad1f7"
]
```

**Added** (written by the generator after approval):

```text
drop_nulls_job.table[golden_drop_null_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:fbe5ea222e164ffeeb59b89b9c4836a3f1b7e5f580dce814091fe75bb60bff20/pdb_v2].instance_ids
[
  "arrow_v1:0000028c8d0fceb1229d2c165c2ad826c24c504ea25fbab2b353f48144999f2971df1b",
  "arrow_v1:000002be7cbed6d33fd41c6fe43dab281b19f4cffc813d6e3788a127a852c17dd9e918"
]
```

**Moved**, `drop_nulls_job.table_paths` (`--remove`, then written again):

```text
before:
[
  "golden_drop_null_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1",
  "golden_drop_null_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:fbe5ea222e164ffeeb59b89b9c4836a3f1b7e5f580dce814091fe75bb60bff20/pdb_v1"
]
after:
[
  "golden_drop_null_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1",
  "golden_drop_null_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:fbe5ea222e164ffeeb59b89b9c4836a3f1b7e5f580dce814091fe75bb60bff20/pdb_v2"
]
```

## 3. `pipeline:filter_job` (`provisional/pipeline.json`)

**Removed** (`--remove`):

```text
filter_job.table[golden_filter_step_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:1b55d0d2cce6651fc52aed3a13227619ae3ba68ecd21f99ebfde5d68e56f4fa2/pdb_v1].record_ids
[
  "arrow_v1:000002fd7d9af35a038e4d9201d0f1c0dc1a8ed63f800e52eb937da2a4993ea1b51971"
]
```

**Added** (written by the generator after approval):

```text
filter_job.table[golden_filter_step_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:1b55d0d2cce6651fc52aed3a13227619ae3ba68ecd21f99ebfde5d68e56f4fa2/pdb_v2].instance_ids
[
  "arrow_v1:00000294e0adb82b308c36118fa4922a9f5c5da2455aa7f696ce09f3b4db4bfa3976e5"
]
```

**Moved**, `filter_job.table_paths` (`--remove`, then written again):

```text
before:
[
  "golden_filter_step_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1",
  "golden_filter_step_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:1b55d0d2cce6651fc52aed3a13227619ae3ba68ecd21f99ebfde5d68e56f4fa2/pdb_v1"
]
after:
[
  "golden_filter_step_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1",
  "golden_filter_step_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:1b55d0d2cce6651fc52aed3a13227619ae3ba68ecd21f99ebfde5d68e56f4fa2/pdb_v2"
]
```

## 4. `pipeline:polars_filter_expr_job` (`provisional/pipeline.json`)

**Removed** (`--remove`):

```text
polars_filter_expr_job.table[golden_filter_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:d41530868bebc0f11662b61d07ec45e0ae6bcee9b9602116392a103444a02cb7/pdb_v1].record_ids
[
  "arrow_v1:0000023efdc614d3a51bc82f2efc0a7355bf905da5dea9b679e66b9873e367a6593449"
]
```

**Added** (written by the generator after approval):

```text
polars_filter_expr_job.table[golden_filter_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:d41530868bebc0f11662b61d07ec45e0ae6bcee9b9602116392a103444a02cb7/pdb_v2].instance_ids
[
  "arrow_v1:000002e665d85abb2155b8d05406b74b6830fc09c5703c778d067946e692cba85cab7c"
]
```

**Moved**, `polars_filter_expr_job.table_paths` (`--remove`, then written again):

```text
before:
[
  "golden_filter_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1",
  "golden_filter_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:d41530868bebc0f11662b61d07ec45e0ae6bcee9b9602116392a103444a02cb7/pdb_v1"
]
after:
[
  "golden_filter_job/_result/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/rev1/rdb_v1",
  "golden_filter_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:d41530868bebc0f11662b61d07ec45e0ae6bcee9b9602116392a103444a02cb7/pdb_v2"
]
```

## 5. `pipeline:rejoin_job` (`provisional/pipeline.json`)

**Removed** (`--remove`):

```text
rejoin_job.table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1].record_ids
[
  "arrow_v1:000002373cad79440bb534ae0b85745bfbe97278bcf7dac4ad2718df0a13f9b4fc51b9",
  "arrow_v1:0000025d8f7a2f2a2ec39599d0f52172f223126d006a65d4f6ca87b7fd231bad7ce6e8",
  "arrow_v1:000002e5737179621e804294b78ea1190ce0c0e915058c536137b50d85c8e447c6d2ab"
]
```

**Added** (written by the generator after approval):

```text
rejoin_job.table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v2].instance_ids
[
  "arrow_v1:00000264d51dd01edfbb777f7047d25e7cdb50e4f52e6b01454ffc56b70aa5b166b920",
  "arrow_v1:0000029b103c78b5c06ac0fe36d793fe576563a8609b835d662cf78a5828436f9b8aa3",
  "arrow_v1:000002b71bb228680318fe2cc0fc599241136f0fedc28658bda79e2a90915b87c82ef0"
]
```

**Moved**, `rejoin_job.table_paths` (`--remove`, then written again):

```text
before:
[
  "golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1",
  "golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1"
]
after:
[
  "golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1",
  "golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v2"
]
```

## 6. `pipeline:rejoin_job_async` (`provisional/pipeline.json`)

**Removed** (`--remove`):

```text
rejoin_job_async.table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1].record_ids
[
  "arrow_v1:000002373cad79440bb534ae0b85745bfbe97278bcf7dac4ad2718df0a13f9b4fc51b9",
  "arrow_v1:0000025d8f7a2f2a2ec39599d0f52172f223126d006a65d4f6ca87b7fd231bad7ce6e8",
  "arrow_v1:000002e5737179621e804294b78ea1190ce0c0e915058c536137b50d85c8e447c6d2ab"
]
```

**Added** (written by the generator after approval):

```text
rejoin_job_async.table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v2].instance_ids
[
  "arrow_v1:00000264d51dd01edfbb777f7047d25e7cdb50e4f52e6b01454ffc56b70aa5b166b920",
  "arrow_v1:0000029b103c78b5c06ac0fe36d793fe576563a8609b835d662cf78a5828436f9b8aa3",
  "arrow_v1:000002b71bb228680318fe2cc0fc599241136f0fedc28658bda79e2a90915b87c82ef0"
]
```

**Moved**, `rejoin_job_async.table_paths` (`--remove`, then written again):

```text
before:
[
  "golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1",
  "golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1"
]
after:
[
  "golden_rejoin_job/_result/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/rev1/rdb_v1",
  "golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v2"
]
```

The `rejoin_job_async` values equal `rejoin_job`'s, as today (the async run pins the sync run's
values).

## The commands, once approved

```text
uv run python tests/test_hashing/golden/generate_golden_hashes.py \
  --remove 'job.table[golden_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:b1212cbf9e4025086e622df7d8e9931ddde18946f29dda37568a13f36dfb39ef/pdb_v1].record_ids' \
  --remove 'job.table_paths' \
  --remove 'drop_nulls_job.table[golden_drop_null_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:fbe5ea222e164ffeeb59b89b9c4836a3f1b7e5f580dce814091fe75bb60bff20/pdb_v1].record_ids' \
  --remove 'drop_nulls_job.table_paths' \
  --remove 'filter_job.table[golden_filter_step_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:1b55d0d2cce6651fc52aed3a13227619ae3ba68ecd21f99ebfde5d68e56f4fa2/pdb_v1].record_ids' \
  --remove 'filter_job.table_paths' \
  --remove 'polars_filter_expr_job.table[golden_filter_job/golden_adjust/dff0617f9fe7bc2e6d071f4da43376bf59044ed9929bcce9812e9917bc95d39c/v0/python.function.v1/schema:d41530868bebc0f11662b61d07ec45e0ae6bcee9b9602116392a103444a02cb7/pdb_v1].record_ids' \
  --remove 'polars_filter_expr_job.table_paths' \
  --remove 'rejoin_job.table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1].record_ids' \
  --remove 'rejoin_job.table_paths' \
  --remove 'rejoin_job_async.table[golden_rejoin_job/golden_ratio/43ebfd98b0b9497b3b820bb330f4727dd540e67883fd7588fdd98d1f176733b3/v0/python.function.v1/schema:2940d188693a284739b066b85def5c18e849c85c1de5fc9a3444d6dfbb25e7ba/pdb_v1].record_ids' \
  --remove 'rejoin_job_async.table_paths'
uv run python tests/test_hashing/golden/generate_golden_hashes.py
uv run python tests/test_hashing/golden/generate_golden_hashes.py --check
```
