# ITL-938: one coherent versioning system for hashes, hash labels and stored paths

Status: **draft, discussion in progress** (inventory complete; the system, the path decision,
the v0.3 cut list and the follow-up issues are filled in as the developer decides them).

Linear: [ITL-938](https://linear.app/metamorphic/issue/ITL-938). Design only; no library code.

Everything in section 1 was read from the code on `origin/main` at `d61e3e39` and checked by
running it (November 11, 2026). The ITL-910 comparisons ran a snapshot of
`origin/eywalker/itl-910-adopt-starfix-05-hash-format-2-in-orcapod` at `6df680ce`
(starfix 0.5.0). Items read from code but not exercised by a run are marked "(code only)".

Terms used here:

- **Label**: the text in front of a digest that names the algorithm, as in
  `arrow_v0.1:000001d0e4…`. In code it is `ContentHash.method`.
- **Labelled / bare**: a stored hash is *labelled* when the label is stored with the digest,
  and *bare* when only the hex or raw bytes are stored.
- **Preimage**: the exact bytes or structure fed into a hash function.

---

## 1. Inventory

### 1.1 The hashers

| Label | What it hashes | Algorithm | Where the label is set |
|---|---|---|---|
| `semantic_v0.1` | Python objects and structures: schemas, function identity, node identity, every `content_hash()` and `pipeline_hash()` | SHA-256 of canonical JSON of the expanded structure. A nested hash is embedded as its labelled string, so **labels are part of the preimage** of every enclosing hash | `contexts/data/v0.1.json` (`semantic_hasher.hasher_id`); duplicated in `hashing/versioned_hashers.py` (no callers) and `contexts/data/schemas/context_schema.json` |
| `arrow_v0.1` (ITL-910: `arrow_v0.2`) | Arrow tables and schemas: every row's data hash, row keys, pandas values | starfix `ArrowDigester`. The digest is 35 bytes: starfix's own 3-byte format number (`000001` for starfix 0.4, `000002` for 0.5) followed by SHA-256 | same three places |
| `sha256` | file content (`orcapod.File`) | SHA-256 of bytes; the label is the algorithm name, with no version | `hashing/hash_utils.py` |
| `merkle_sha256` | directory content (`orcapod.Directory`) | Merkle tree over entries; child file labels are dropped from the preimage | `hashing/directory_hashers.py` |
| `ndarray_v1` | `numpy.ndarray` values | `array_digest_v1` over dtype tag, shape and bytes | `hashing/semantic_hashing/builtin_handlers.py`, `logical_types/array_format.py` |
| `polars_expr_v1` | `polars.Expr` (filter predicates) | SHA-256 of canonical JSON of an Orcapod-owned expression tree | `hashing/semantic_hashing/polars_expr.py` |
| not on main yet | `torch_tensor_v1` (PR #300); `polars_dataframe_v1`, `polars_series_v1` (PR #286); `pandas_dataframe_v1`, `pandas_series_v1`, `arrow_table_v1` (ITL-897 branches, no PR) | per-type handlers | — |
| no label at all | PR #292 class identity `Point@<sha256 hex>`; the two `uuid5` namespaces for record ids; `combine_hashes` (SHA-256 of concatenation) for folded system tags | fixed algorithms with no name | — |

An extension-typed cell inside an Arrow table is hashed as
`<extension name>::<label>:<digest>`, so per-type labels are inside the preimage of every
Arrow hash that covers such a cell.

### 1.2 Every hash and where it is stored

Probe pipeline: `source1 -> double -> join(source2) -> take_sum`, `PipelineJob("my_pipeline")`
on `DeltaTableDatabase`. Values are from that run on main.

| Hash | Computed from | Hasher | Stored form | Stored where |
|---|---|---|---|---|
| Node `content_hash()` | source: class, schema, `source_id`; operator/function: its identity + upstream content hashes | semantic | labelled string `semantic_v0.1:5da63a88…` | saved job JSON (node keys, edges, `content_hash`, `node_job_content_hashes`); column `__node_content_hash` (labelled bytes); path component `instance:…` when `table_scope="content_hash"`; path of a cached source `source/node:…` (code only) |
| Node `pipeline_hash()` | schemas and topology only, never data | semantic | labelled string `semantic_v0.1:6fe7280a…` | path component `schema:semantic_v0.1:6fe7280a…`; saved job JSON; **bare hex** inside system-tag column names after Join/MergeJoin/Batch/GroupBy |
| Schema hash of a source | `(tag_schema, data_schema)` | semantic | **bare hex**, 64 chars | system-tag column names `_tag_source_id::52eb0af7…`, `_tag_record_id::52eb0af7…` |
| Function `uri` | `(name, semantic hash of output schema, "v"+major, type id)` | semantic | tuple `('double', 'semantic_v0.1:17de3328…', 'v0', 'python.function.v0')` | **four path components** of both function tables; saved job JSON; inside every `_source_<col>` token of function outputs, joined with `:` |
| Operator `uri` | `(ClassName, content hash)` | semantic | **bare hex** `('Join', '295ca933…')` | operator cache table path (code only); saved job JSON |
| Row data hash | the row's data columns as an Arrow table | arrow | labelled bytes `b"arrow_v0.1:" + 35 bytes` | columns `__input_data_hash`, `__output_data_hash`; opt-in `_content_hash` column (labelled string) |
| Pipeline entry id | Arrow table of (system-tag columns + `__input_data_hash`) | arrow | labelled bytes | `__pipeline_base_entry_id`; with the recomputation index, the row key `__record_id` of the pipeline table |
| Result row key / `__data_id` | random `uuid7` of the output datagram | none | 16 raw bytes | `__record_id` in the result table; `__data_id` in the pipeline table; inside `_source_<col>` tokens |
| Source record id | `uuid5(namespace, "{source_id}::{row token}")` | fixed, unnamed | 16 raw bytes | values of `_tag_record_id::…` columns |
| Folded record id / source id (Batch, GroupBy) | `uuid5` / SHA-256 over the members | fixed, unnamed | 16 raw bytes / bare hex | values of system-tag columns |
| Default `source_id` (when the user gives none) | Arrow hash of the source table | arrow | **bare hex**, 70 chars, starts `000001` | `_tag_source_id::…` values, `_source_<col>` tokens, and from there the source's content hash |
| Function signature / content hash | signature, function components | semantic | labelled bytes | `__pf_var_function_signature_hash`, `__pf_var_function_content_hash` |
| Operator cache row key | Arrow hash of the row incl. `__node_content_hash` | arrow | **bare hex** | `_record_hash` (code only) |
| File-hash cache | key `(path, mtime, size)`, value labelled digest | sha256 | key carries no algorithm | SQLite / Postgres cache table |

### 1.3 The store on disk (main today)

```
store/
└── my_pipeline/                                         job name
    ├── double/                                          function name            ┐
    │   └── semantic_v0.1:17de3328…a26939/               hash of output schema    │ function uri
    │       └── v0/                                      function major version   │
    │           └── python.function.v0/                  data function type id    ┘
    │               └── schema:semantic_v0.1:6fe7280a…b9ec94/   node pipeline hash
    │                   └── pdb_v1/                      pipeline-table layout version   <- Delta table
    ├── take_sum/…/schema:semantic_v0.1:1417915f…feff07/pdb_v1/                          <- Delta table
    └── _result/
        ├── double/semantic_v0.1:17de3328…a26939/v0/python.function.v0/rdb_v1/           <- Delta table
        └── take_sum/semantic_v0.1:d50b00b6…1979a6/v0/python.function.v0/rdb_v1/         <- Delta table
```

The join has no table (operator nodes are not cached by default). The result table path has
no pipeline hash: it is shared by every node that uses the function.

Hash-bearing columns, pipeline table (`…/pdb_v1`):

| Column | Type | Example | Label |
|---|---|---|---|
| `__record_id` (row key) | binary | `arrow_v0.1:` + 35 bytes | labelled |
| `__pipeline_base_entry_id` | binary | `arrow_v0.1:` + 35 bytes | labelled |
| `__input_data_hash`, `__output_data_hash` | binary | `arrow_v0.1:` + `000001…` | labelled |
| `__node_content_hash` | binary | `semantic_v0.1:` + 32 bytes | labelled |
| `__data_id` | binary | 16 bytes (uuid7) | none (random) |
| `_tag_source_id::52eb0af7…::6fe7280a…:1` | string | `source1` | column **name** holds two bare semantic hashes |
| `_tag_record_id::52eb0af7…::6fe7280a…:1` | binary | 16 bytes (uuid5) | bare, in name and value |
| `_source_doubled` | string | `double:semantic_v0.1:17de3328…:v0:python.function.v0::<uuid7>::doubled` | label inside the token |
| `__input_data_context_key` | string | `std:v0.1:default` | written, never read |

Result table (`…/rdb_v1`): `__record_id` (bare uuid7), `__input_data_hash` (labelled, the lookup
key), `__pf_var_function_signature_hash`, `__pf_var_function_content_hash` (labelled),
`_context_key` = `std:v0.1:default` (read back: it selects the data context for a cached row),
`_source_<col>` tokens.

**On the ITL-910 branch** the same pipeline gives the same directory tree, the same node
content and pipeline hashes, the same column names. The only differences are the values of
the Arrow-hashed columns: `arrow_v0.1:000001…` becomes `arrow_v0.2:000002…` in
`__record_id` (pipeline table), `__pipeline_base_entry_id`, `__input_data_hash`,
`__output_data_hash`. With a default (table-derived) `source_id`, the bare `000001…` hex
would also change to `000002…` and carry into source content hashes.

### 1.4 Every version, label and format marker

| # | Marker | Values today | What it is meant to capture | Who bumps it, when | Can a reader tell which version wrote a stored value? | What moves when it is bumped |
|---|---|---|---|---|---|---|
| 1 | Semantic hasher label | `semantic_v0.1` | the algorithm for hashing Python structures | never bumped; hash-moving fixes have gone in under the same label (PR #301 proposes another) | yes where labelled; no in column names and operator uri (bare) | every content/pipeline hash value, every function path, every system-tag column name |
| 2 | Arrow hasher label | `arrow_v0.1` -> `arrow_v0.2` (ITL-910) | the algorithm for hashing Arrow data. Rule accepted Nov 11, 2026: bump whenever any hash can change | Orcapod, by editing the context JSON | yes in labelled columns; only by the starfix prefix in bare hex | row hashes and row keys. **No path moves** |
| 3 | starfix format number | `000001`, `000002` (first 3 bytes of the digest) | starfix's own byte format | starfix | yes | same as 2 |
| 4 | Per-type value labels | `sha256`, `merkle_sha256`, `ndarray_v1`, `polars_expr_v1` (+ pending `torch_tensor_v1`, `polars_*_v1`, `pandas_*_v1`, `arrow_table_v1`) | the hash definition for one value type | type author; rule in PR #300: bytes under a label never change | only inside a preimage; the label is not stored next to the cell | any Arrow hash covering such a cell |
| 5 | Data context key and version | `std:v0.1:default`, `v0.1` | the whole bundle: both hashers, the type converter, the registered logical types and handlers | **never bumped**: `v0.1.json` has been edited in place 20 times (see its changelog), including hard cuts | the key is stored in every result row, but it is the same string before and after every change, so it tells nothing | nothing |
| 6 | Table layout suffixes | `pdb_v1`, `rdb_v1`, `tdb_v1` | the column layout of Orcapod's own pipeline, result and side-effect tables | Orcapod, on a layout change | by path only: `SchemaVersionError` fires when a table exists at the path **without** the suffix and none with it; `NodeConfig(ignore_schema=("v0",))` opts out | the last path component. `tdb_v1` and operator cache tables have no check |
| 7 | Function major version | `v0` (from `version="v0.0"`) | the user's statement that the function's results changed | the user | yes, path component and uri | function uri, both function table paths |
| 8 | Data function type id | `python.function.v0` | the kind of function wrapper | Orcapod; hard-coded in four files | yes, path component | function table paths |
| 9 | Stored representation versions | array format `{"version":1}` in the Arrow extension metadata; PR #308 nullability `{"format":"orcapod.nullability","version":1}`; every other logical type: none | the byte layout of a stored value | type author | arrays: yes, and an unknown version raises (verified: `{"version":2}` gives `ValueError … expected metadata b'{"version":1}'`). Other types: no. `orcapod.file` changed layout once (ITL-451) under the same name with no marker | nothing in paths or labels |
| 10 | Saved file formats | `orcapod_pipeline_version: 0.1.0`, `orcapod_pipeline_job_version: 0.1.0` | the JSON layout of a saved pipeline / job | Orcapod | yes; load rejects unknown versions. Hashes inside the file are trusted, not re-verified | nothing |
| 11 | File-hash cache table version | `1` (`PRAGMA user_version`) | the cache table's DDL | Orcapod | yes | nothing |
| 12 | Package version | `0.2.x` -> `0.3.0` (git tag) | the release | release process | **no: it is written nowhere in a store** | nothing |
| 13 | Golden files | 685 pinned values in 5 files | a test that hashes do not move | regenerated only with approval | — | ITL-910 changes 186 of them |
| 14 | `table_scope` | `pipeline_hash` (default) / `content_hash` | not a version: whether a node's table is shared across data (schema-level) or private to one input | user, per node | yes, in saved JSON | adds an `instance:…` path component |

### 1.5 Failure modes, reproduced

| # | Change | What happens today | Evidence |
|---|---|---|---|
| F1 | Arrow label changes (`arrow_v0.1` -> `arrow_v0.2`), nothing else | Same paths. Every lookup misses, everything recomputes, and the new rows are **appended to the same four tables**: 6 rows for 3 inputs, 4 for 2. Reads that scan the table see both generations | main with the label patched, run on a store written by main |
| F1' | Same, on the ITL-910 branch tip | `StoreHashLabelError` naming the table, the node and both labels; nothing is read or written | ITL-910 snapshot on a store written by main |
| F2 | Semantic label changes (`semantic_v0.1` -> `semantic_v0.2`), nothing else | **Every function path moves** (the label is in two path components, and because nested labels are in the preimage the hex changes too: `17de3328…` -> `03141c1e…`). The run succeeds, recomputes everything into new tables, and the old tables stay behind untouched. No error, no warning: stored results silently look dropped | main with the label patched |
| F3 | One hasher's label changes but not the other | The two hashers behave in opposite ways (F1 keeps paths and mixes rows; F2 moves paths and orphans tables). Neither is detected on main | F1 + F2 |
| F4 | A stored representation changes | Arrays: detected, raises on an unknown version. All other logical types carry no version: a layout change under the same extension name is read with the new code as if it were the new layout | `{"version":2}` test; `orcapod.file` history (ITL-451) |
| F5 | The data context changes | The stored `_context_key` is identical before and after, so the one stored marker designed to say "which rules wrote this row" cannot distinguish anything | `v0.1.json` changelog, 20 in-place edits |
| F6 | A hash is stored bare | System-tag column names, operator uri, default source ids and operator-cache row keys would look the same in form under a new algorithm; a reader cannot tell which hasher made them | inventory 1.2 |
| F7 | A hash-moving fix ships without a label bump | Indistinguishable from a cache miss: silent recompute, mixed rows | PR #301 (dict/set order) as proposed |

### 1.6 What the inventory says

1. There are **twelve independent version markers** and no rule relating them. Three name
   conventions coexist: `_v0.1` (hashers), `_v1` (per-type, table layouts), `0.1.0` (files).
2. The one marker meant to tie everything together, the data context version, is the only
   one that has never moved.
3. Whether a hashing change moves paths is an accident of which hasher it touches: paths
   carry the semantic label and never the Arrow label.
4. No reader checks a label on main. `ContentHash.from_string` accepts any label; comparisons
   are byte equality, so a foreign label is a silent miss.
5. A store does not say what wrote it: no package version, no meaningful context version, no
   store-level marker.

---

## 2. The system (to be decided)

_Filled in during the discussion._

## 3. Generation names in paths (to be decided)

_Filled in during the discussion._

## 4. Before the v0.3 tag, and after (to be decided)

_Filled in during the discussion._

## 5. Follow-up issues to file

_Filled in during the discussion._

## 6. Decisions

Decisions made before this discussion (developer, as relayed in ITL-938 and ITL-910):

| Date | Decision |
|---|---|
| Nov 10, 2026 | Hash values must stay identical across releases. |
| Nov 11, 2026 | "A hash bug overrides hash stability: erroneous values move, and the change is marked explicitly as a fix." |
| Nov 11, 2026 | Arrow label rule: bump Orcapod's label whenever any hash can change; label numbers are not kept aligned with starfix's format number. |
| Nov 11, 2026 | v0.3 fails explicitly when it meets v0.2 rows (ITL-910). |
| Nov 11, 2026 | Migration matches stored results by tags, not by reproducing old hashes (ITL-913). |
| Nov 11, 2026 | Open question that started this design: "Should we think about having explicit name so that we have path change everywhere for v0.3?" |

Decisions made in this discussion: _none yet._
