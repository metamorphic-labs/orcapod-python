# ITL-938: one coherent versioning system for hashes, hash labels and stored paths

Status: **draft, discussion in progress.** Parts 1 to 4 (what exists, the axes of change,
their impact, the path-or-column test) are complete. Parts 5 to 8 (the system, the path
layout, the v0.3 cut list, follow-up issues) are filled in as the developer decides them.

Linear: [ITL-938](https://linear.app/metamorphic/issue/ITL-938). Design only; no library code.

**How the facts here were obtained.** Everything was read from the code on `origin/main` at
`d61e3e39` and checked by running it on November 11, 2026. Comparisons with ITL-910 ran a
snapshot of `origin/eywalker/itl-910-adopt-starfix-05-hash-format-2-in-orcapod` at `6df680ce`
(starfix 0.5.0). Every hash string quoted is a real value from those runs. Items read from
code but not exercised by a run are marked "(code only)".

**The probe pipeline** used for all examples:

```python
s1 = DictSource([{"id": 0, "x": 5}, {"id": 1, "x": 10}, {"id": 2, "x": 15}], tag_columns=["id"], source_id="source1")
s2 = DictSource([{"id": 0, "y": 3}, {"id": 2, "y": 6}, {"id": 4, "y": 9}], tag_columns=["id"], source_id="source2")

@function_pod(output_keys="doubled")
def double(x: int) -> int: return 2 * x

@function_pod(output_keys="sum")
def take_sum(doubled: int, y: int) -> int: return doubled + y

job = PipelineJob(name="my_pipeline", store=DeltaTableDatabase("./store"))
with job:
    take_sum.pod(double.pod(s1, label="double").join(s2, label="join"), label="sum")
job.run()
```

**Terms.**

| Term | Meaning |
|---|---|
| Label | The text in front of a digest that names the algorithm, as in `arrow_v0.1:000001d3da…`. In code: `ContentHash.method`. |
| Labelled / bare | A stored hash is *labelled* when the label is stored with the digest, *bare* when only hex or raw bytes are stored. |
| Preimage | The exact bytes or structure fed into a hash function. |
| Key column | A column whose value is used to find a row (a lookup or row key). |
| Axis of change | One independent way in which Orcapod's behaviour can change between two versions of the code. |

---

## Part 1. What exists today

### 1.1 Where hashing happens

```mermaid
flowchart LR
    subgraph VALUE["Per-type value hashers"]
        V1["file bytes<br/><b>sha256:</b>2cf24dba…"]
        V2["directory<br/><b>merkle_sha256:</b>…"]
        V3["numpy array<br/><b>ndarray_v1:</b>8462766b…"]
        V4["polars expression<br/><b>polars_expr_v1:</b>…"]
    end
    subgraph SEM["Structure hasher  semantic_v0.1"]
        S1["schema hash<br/>semantic_v0.1:15816996…"]
        S2["function uri hash<br/>semantic_v0.1:17de3328…"]
        S3["node pipeline_hash<br/>semantic_v0.1:6fe7280a…"]
        S4["node content_hash<br/>semantic_v0.1:5da63a88…"]
    end
    subgraph ARW["Arrow hasher  arrow_v0.1"]
        A1["row data hash<br/>arrow_v0.1:000001d3da…"]
        A2["pipeline entry id / row key<br/>arrow_v0.1:00000189…"]
        A3["default source_id<br/>00000188… (bare)"]
    end
    subgraph FIXED["Fixed, unnamed algorithms"]
        F1["source record id<br/>uuid5, 16 bytes"]
        F2["folded system tags<br/>uuid5 / sha256"]
    end
    VALUE -->|"value inside a structure"| SEM
    VALUE -->|"extension cell inside a row"| ARW
    S1 --> S2 --> S3
    S3 --> S4
    A3 -->|"when the user gives no source_id"| S4
    F1 --> A2
    A1 --> A2
```

| Label | What it hashes | Algorithm | Where the label is set |
|---|---|---|---|
| `semantic_v0.1` | Python structures: schemas, function identity, node identity, every `content_hash()` and `pipeline_hash()` | SHA-256 of canonical JSON of the expanded structure. A nested hash is embedded as its labelled string, so **labels are part of the preimage** of every enclosing hash | `contexts/data/v0.1.json`; duplicated in `hashing/versioned_hashers.py` (no callers) and `contexts/data/schemas/context_schema.json` |
| `arrow_v0.1` (ITL-910: `arrow_v0.2`) | Arrow tables and schemas: each row's data hash, row keys | starfix `ArrowDigester`. The digest is 35 bytes: starfix's own 3-byte format number (`000001` for starfix 0.4, `000002` for 0.5), then SHA-256 | same three places |
| `sha256` | file content | SHA-256 of the bytes. The label is the algorithm name; it has no version | `hashing/hash_utils.py` |
| `merkle_sha256` | directory content | Merkle tree over entries; child file labels are dropped from the preimage | `hashing/directory_hashers.py` |
| `ndarray_v1` | `numpy.ndarray` values | `array_digest_v1` over dtype tag, shape, bytes | `builtin_handlers.py`, `logical_types/array_format.py` |
| `polars_expr_v1` | `polars.Expr` filter predicates | SHA-256 of canonical JSON of an Orcapod-owned expression tree | `semantic_hashing/polars_expr.py` |
| not on main yet | `torch_tensor_v1` (PR #300); `polars_dataframe_v1`, `polars_series_v1` (PR #286); `pandas_dataframe_v1`, `pandas_series_v1`, `arrow_table_v1` (ITL-897 branches) | per-type handlers | — |
| no label | PR #292 class identity `Point@9117beaa…` (SHA-256 of a JSON description); the two `uuid5` namespaces for record ids; `combine_hashes` for folded system tags | fixed algorithms with no name | — |

An extension-typed cell inside an Arrow table is hashed as
`<extension name>::<label>:<digest>`, so per-type labels sit inside the preimage of every
Arrow hash that covers such a cell.

### 1.2 Where each hash is stored

| Hash | Computed from | Hasher | Stored form | Stored where |
|---|---|---|---|---|
| Node `content_hash()` | source: class, schema, `source_id`; operator or function: its identity + upstream content hashes | semantic | labelled string `semantic_v0.1:5da63a88…` | saved job JSON (node keys, edges, `content_hash`, `node_job_content_hashes`); column `__node_content_hash` (labelled bytes); path component `instance:…` when `table_scope="content_hash"`; cached-source path `source/node:…` (code only) |
| Node `pipeline_hash()` | schemas and topology only, never data | semantic | labelled string `semantic_v0.1:6fe7280a…` | path component `schema:semantic_v0.1:6fe7280a…`; saved job JSON; **bare hex** inside system-tag column names after Join, MergeJoin, Batch, GroupBy |
| Schema hash of a source | `(tag_schema, data_schema)` | semantic | **bare hex**, 64 chars | system-tag column names `_tag_source_id::52eb0af7…`, `_tag_record_id::52eb0af7…` |
| Function `uri` | `(name, semantic hash of output schema, "v"+major, type id)` | semantic | `('double', 'semantic_v0.1:17de3328…', 'v0', 'python.function.v0')` | **four path components** of both function tables; saved job JSON; inside every `_source_<col>` token of function outputs |
| Operator `uri` | `(ClassName, content hash)` | semantic | **bare hex** `('Join', '295ca933…')` | operator cache path (code only); saved job JSON |
| Row data hash | the row's data columns as an Arrow table | arrow | labelled bytes `b"arrow_v0.1:"` + 35 bytes | `__input_data_hash`, `__output_data_hash`; opt-in `_content_hash` column (labelled string) |
| Pipeline entry id | Arrow table of (system-tag columns + `__input_data_hash`) | arrow | labelled bytes | `__pipeline_base_entry_id`; with the recomputation index it gives the row key `__record_id` of the pipeline table |
| Result row key, `__data_id` | random `uuid7` of the output datagram | none | 16 raw bytes | `__record_id` in the result table; `__data_id` in the pipeline table; inside `_source_<col>` tokens |
| Source record id | `uuid5(namespace, "{source_id}::{row token}")` | fixed, unnamed | 16 raw bytes | values of `_tag_record_id::…` columns |
| Folded record id / source id (Batch, GroupBy) | `uuid5` / SHA-256 over the members | fixed, unnamed | 16 raw bytes / bare hex | values of system-tag columns |
| Default `source_id` | Arrow hash of the source table | arrow | **bare hex**, 70 chars: `000001883080da70…` | `_tag_source_id::…` values, `_source_<col>` tokens, and through them the source's content hash |
| Function signature / content hash | signature; function components | semantic | labelled bytes | `__pf_var_function_signature_hash`, `__pf_var_function_content_hash` |
| Operator cache row key | Arrow hash of the row incl. `__node_content_hash` | arrow | **bare hex** | `_record_hash` (code only) |
| File-hash cache entry | key `(path, mtime, size)`; value labelled digest | sha256 | the key has no algorithm in it | SQLite / Postgres cache table, outside the store |

### 1.2.1 The two identity chains, source identity and source entry identity

Every node has two hashes, both made by the structure hasher, both chained from its upstreams.
They differ in one thing: what a **source** contributes at the bottom of the chain.

```
                          PIPELINE IDENTITY                       CONTENT IDENTITY
                          (schemas + topology)                    (pipeline identity + which sources)

 source1                  (tag schema, data schema)               (class, tag schema, data schema, source_id)
   id:int, x:int          semantic_v0.1:52eb0af7…                 semantic_v0.1:d644c803…
      │                         │                                       │
      ▼                         ▼                                       ▼
 double                   (function uri, upstream pipeline hash)  (function uri, upstream content hash)
                          semantic_v0.1:6fe7280a…                 semantic_v0.1:5da63a88…
      │                         │                                       │
      │   source2               │   semantic_v0.1:1560128e…             │   semantic_v0.1:c6411778…
      ▼      ▼                  ▼      ▼                                ▼      ▼
 join                     ("Join", {upstream pipeline hashes})    ("Join", {upstream content hashes})
                          semantic_v0.1:87a24180…                 semantic_v0.1:50c289b4…
      │                         │                                       │
      ▼                         ▼                                       ▼
 take_sum                 semantic_v0.1:1417915f…                 semantic_v0.1:48ddd935…

 USED FOR                 the table path:                         telling rows apart inside a shared table:
                            …/schema:semantic_v0.1:1417915f…/       column __node_content_hash
                          system-tag column names (bare):         path component instance:… (table_scope="content_hash")
                            _tag_source_id::52eb0af7…::6fe7280a…:1 node keys and edges in the saved job file
```

So two pipelines with the same shape and schemas but different sources write to the **same
tables**, and their rows are separated by `__node_content_hash`.

**Source identity** has three levels, and each one feeds a different stored thing:

| Level | Computed from | Example value | Feeds |
|---|---|---|---|
| Source pipeline identity | `(tag schema, data schema)` only | `semantic_v0.1:52eb0af7…` (the same for `source1` and for any other source with columns `id:int, x:int`) | every downstream pipeline hash, so every table path; the bare hex in system-tag column names |
| Source content identity | `(class name, schemas, source_id)` | `semantic_v0.1:d644c803…` for `source_id="source1"`; `…9da643df…` for `source_id="other_name"` with the same rows | every downstream content hash; `__node_content_hash`; saved job file |
| `source_id` | given by the user (`"source1"`), or when absent the bare Arrow hash of the whole table (`000001883080da70…`) | — | source content identity; values of `_tag_source_id::…`; the front of every source-info token and of every record id |

The contract (developer, November 11, 2026): a source decides whether its identity reflects
its exact content or is an *address* for it, such as a repository. Inside a source, each entry
carries a unique and reliable record id. Orcapod trusts that the same `source_id` and the same
record id mean exactly the same data entry.

```
 source_id      = which repository          "source1"            -> content identity, _tag_source_id
 record id      = which entry inside it     uuid5("source1::row_2") -> _tag_record_id, entry id
 source-info    = which value of the entry  "source1::row_2::x"  -> _source_x
```

Two measured consequences:

- With a user-given `source_id`, the source's content hash does **not** cover its data, by
  design: changing one value (`x: 5` to `x: 99`) leaves it at `semantic_v0.1:d644c803…`. The
  changed row is still distinguished downstream, by its row data hash.
- With no `source_id`, the source's content hash depends on the **Arrow hasher** through the
  default id: the same rows give `semantic_v0.1:d8eb49fa…` on main and `…5b6ae6d2…` on
  ITL-910. This is the one place where an Arrow-hasher change leaks into the structure-hash
  chain (and into cached-source paths).

**Source entry identity** (the identity of one row of a source) has two parts:

| Part | Computed from | Example | Stored where | Feeds |
|---|---|---|---|---|
| Record id | `uuid5(fixed namespace, "{source_id}::{row token}")`; the token is `row_{i}` (position) or `{column}={value}` | row 2 of `source1`: token `source1::row_2`, id `dbbded85-3b31-5aa1-8070-7b5a41d74c5b` | value of `_tag_record_id::52eb0af7…` (16 bytes, bare) | the pipeline entry id of every downstream row |
| Source-info token | `{source_id}::{row token}::{column}` | `source1::row_2::x` | value of `_source_x` | provenance only; not hashed into keys |
| Row data hash | the row's data columns | `{"x": 15}` -> `arrow_v0.1:000001d0e4…` | `__input_data_hash` | the result-table lookup key and the pipeline entry id |

How these combine into the key of a stored row:

```
 record ids of the contributing source rows ──┐   (system-tag columns: uuid5, bare)
   _tag_record_id::52eb0af7…::6fe7280a…:1     │
   _tag_record_id::1560128e…::1560128e…:0     ├──► Arrow hash ──► __pipeline_base_entry_id ──┬──► __record_id
                                              │                   arrow_v0.1:00000103…       │    arrow_v0.1:0000012f…
 row data hash of the input ──────────────────┘                                              │
   __input_data_hash  arrow_v0.1:00000196…                __pipeline_recomputation_index ────┘
```

The record id is positional today: reordering a source's rows changes `row_2` to another
index and so changes every downstream key. ITL-817 proposes deriving it from content.

Which axis of Part 2 each of these sits on:

| Thing | Axis |
|---|---|
| What a source contributes to each chain (schema only; schema + `source_id`) | I4 Identity composition |
| That the chains are hashed with the structure hasher | I1 Structure hashing |
| The record-id recipe (namespace, token format, positional or content-derived) | I4 Identity composition |
| The default `source_id` being an Arrow table hash | I4 (the choice) and I2 (the value) |
| The entry-id recipe (system tags + input data hash) | I4 Identity composition |
| That the entry id is hashed with the Arrow hasher | I2 Arrow data hashing |
| The source-info token format | III3 Column naming |

### 1.2.2 System tags and source-info columns through an operator chain

Two families of hidden columns travel with every row. They are where source identity
(1.2.1) and the operator chain meet, and both hold hashes.

| Family | Columns | What it records | Is it hashed into row keys? |
|---|---|---|---|
| **System tags** | a pair per contributing source: `_tag_source_id::<suffix>` and `_tag_record_id::<suffix>` | which source, and which row of it, this row descends from. The column **name** records the route the row took through multi-input operators | **yes**: the entry id is the Arrow hash of all system-tag columns (names, types and values) plus the input data hash |
| **Source info** | one per data column: `_source_<column>` | where each data value came from: a source row, or a function invocation | no: provenance only |

Measured at each stage (the row for `id=2`; hashes shortened, all bare `semantic_v0.1` hex):

```
source1                                              pipeline hash 52eb0af7…
   _tag_source_id::52eb0af7…                         "source1"
   _tag_record_id::52eb0af7…                         dbbded85…   fixed_size_binary[16]   = uuid5("source1::row_2")
   _source_x                                         "source1::row_2::x"
        │
        ▼  double  (function pod)                    NAME-PRESERVING
   _tag_source_id::52eb0af7…                         "source1"
   _tag_record_id::52eb0af7…                         dbbded85…   large_binary            <- type changed
   _source_doubled                                   "double:semantic_v0.1:17de3328…:v0:python.function.v0::<uuid7>::doubled"
        │                                            (the source-info token is REPLACED by the function's)
        ▼  join with source2                         NAME-EXTENDING: append ::<input pipeline hash>:<position>
   _tag_source_id::1560128e…::1560128e…:0            "source2"
   _tag_record_id::1560128e…::1560128e…:0            01e2b591…
   _tag_source_id::52eb0af7…::6fe7280a…:1            "source1"         52eb0af7 = source1's schema hash
   _tag_record_id::52eb0af7…::6fe7280a…:1            dbbded85…         6fe7280a = pipeline hash of `double`, the join input
   _source_y                                         "source2::row_1::y"
   _source_doubled                                   unchanged
        │
        ▼  batch                                     REDUCING: values fold to one digest; append ::<input pipeline hash>
   _tag_source_id::1560128e…::1560128e…:0::87a24180… "36f89629…"  (bare SHA-256 of the members' source ids)
   _tag_record_id::1560128e…::1560128e…:0::87a24180… 1ddcf99a…    (uuid5 over the members' record ids)
   _tag_source_id::52eb0af7…::6fe7280a…:1::87a24180… "6baf7889…"
   _tag_record_id::52eb0af7…::6fe7280a…:1::87a24180… 7f463412…
   _source_y                                         ["source2::row_0::y", "source2::row_1::y"]      large_list<large_string>
   _source_doubled                                   ["double:…::doubled", "double:…::doubled"]
```

What the name of a system-tag column encodes, piece by piece:

```
_tag_record_id :: 52eb0af7… :: 6fe7280a… : 1 :: 87a24180…
└──────┬──────┘   └───┬───┘    └───┬───┘  └┬┘   └───┬───┘
  which field    schema hash   pipeline   its     pipeline hash of the
  (source id or  of the        hash of    canonical  stream that Batch /
   record id)    source        the join   position   GroupBy reduced
                               input      in the join
```

Facts measured on this chain:

| Fact | Evidence |
|---|---|
| A join is commutative: `join(double, source2)` and `join(source2, double)` give the same pipeline hash `87a24180…` and identical column names | both orders run |
| Two sources with the same schema are told apart in the name only by position: `merge_join(source1, source3)` gives `…::52eb0af7…::52eb0af7…:0` and `…:1` | run |
| Every hash inside a column name is a bare structure hash. A structure-hasher change renames every system-tag column in every stored table | names above |
| The values are bare too: a record id is 16 raw bytes; a folded source id is 64 hex characters with no label | values above |
| The record-id column is `fixed_size_binary[16]` out of a source and `large_binary` after a function pod. The entry id is an Arrow hash over these columns, so the Arrow type of a system-tag column is part of every downstream row key | types above |
| A function pod replaces the source-info token of its outputs with its own: uri, the output's `uuid7`, the column. The uri inside carries a labelled structure hash, separated by the same `:` the label uses | `_source_doubled` |
| Reducing operators turn source info into lists and keep system tags scalar (a digest) | `batch` |

Which axis of Part 2 each piece sits on:

| Piece | Axis |
|---|---|
| The column-name grammar (`_tag_…::hash::hash:pos::hash`, separators, which hashes are appended and when) | III3 Column naming |
| That the hashes in the names are structure hashes | I1 Structure hashing |
| Which hash is appended (the input's pipeline hash, not its content hash) and the canonical ordering of join inputs | I4 Identity composition |
| The fold recipe for reducing operators (`uuid5` over record ids, SHA-256 over source ids) | I4 Identity composition |
| The Arrow type of system-tag and source-info columns | II1 Conversion rules |
| The source-info token grammar (`{source_id}::{row token}::{column}`; `{uri}::{uuid7}::{column}`) | III3 Column naming |
| That system tags are hashed into the entry id, and source info is not | I4 Identity composition |
| How each operator evolves the two families (preserve, extend, reduce) | IV3 Operator behaviour |

So system tags are the point where four axes meet: a change to structure hashing (I1), to
column naming (III3), to identity composition (I4) or to an operator's behaviour (IV3) each
changes the **names** of stored columns, which by the developer's rule must show in the path,
and each changes the entry id, so old rows cannot be found.

### 1.3 The store on disk

**Anatomy of a path** (the pipeline table of node `double`):

```
store/my_pipeline/double/semantic_v0.1:17de3328…a26939/v0/python.function.v0/schema:semantic_v0.1:6fe7280a…b9ec94/pdb_v1
      └────┬────┘ └─┬──┘ └──────────────┬─────────────┘ └┬┘ └───────┬────────┘ └──────────────────┬───────────────┘ └─┬──┘
        job name  function     hash of the function's  function  function wrapper     pipeline hash of the node     layout of
                   name          output schema          major        kind             (schemas + topology above it)  this table
                                 [structure hasher]     version                       [structure hasher]
                  └──────────────────────── function uri ───────────────────────┘
```

**All tables written by the probe pipeline:**

```
store/
└── my_pipeline/
    ├── double/semantic_v0.1:17de3328…/v0/python.function.v0/schema:semantic_v0.1:6fe7280a…/pdb_v1/    pipeline table, 3 rows
    ├── take_sum/semantic_v0.1:d50b00b6…/v0/python.function.v0/schema:semantic_v0.1:1417915f…/pdb_v1/  pipeline table, 2 rows
    └── _result/
        ├── double/semantic_v0.1:17de3328…/v0/python.function.v0/rdb_v1/                               result table, 3 rows
        └── take_sum/semantic_v0.1:d50b00b6…/v0/python.function.v0/rdb_v1/                             result table, 2 rows
```

- The join has no table: operator nodes are not cached by default.
- The **result table** path has no pipeline hash. It holds the function's outputs keyed by the
  hash of the input row, and is shared by every node that uses that function.
- The **pipeline table** holds one row per (input row, node): its tags, and a pointer
  (`__data_id`) to the output in the result table.
- Other table kinds (code only): operator cache `<Class>/<bare hash>/schema:…` (no layout
  suffix), cached source `source/node:<content hash>`, side-effect log `…/schema:…/tdb_v1`,
  status and log tables `…/execution_status`, `…/execution_logs`.
- Non-Delta backends (SQLite, Postgres, Spiral) turn the same path tuple into a table name.

**Anatomy of a pipeline-table row** (`take_sum`, row for `id=2`), grouped by what each column
is for:

```
 USED TO FIND THE ROW (keys)
   __record_id                  arrow_v0.1:0000012f…        row key = entry id + recomputation index
   __pipeline_base_entry_id     arrow_v0.1:00000103…        hash of (system tags + input data hash)
   __input_data_hash            arrow_v0.1:00000196…        hash of the input row's data; lookup key into the result table
   __node_content_hash          semantic_v0.1:48ddd935…     which node (and so which input data) wrote the row
   __data_id                    16 bytes (uuid7)            pointer to the output row in the result table

 USER-VISIBLE CONTENT (tags; the data columns live in the result table)
   id                           2

 PROVENANCE CARRIED WITH THE ROW (system tags, source info)
   _tag_source_id::1560128e…::1560128e…:0     "source2"         column NAME embeds bare structure hashes
   _tag_record_id::1560128e…::1560128e…:0     16 bytes (uuid5)
   _tag_source_id::52eb0af7…::6fe7280a…:1     "source1"
   _tag_record_id::52eb0af7…::6fe7280a…:1     16 bytes (uuid5)
   _source_y                    "source2::row_1::y"
   _source_doubled              "double:semantic_v0.1:17de3328…:v0:python.function.v0::<uuid7>::doubled"

 DESCRIPTION OF HOW THE ROW CAME TO BE
   __output_data_hash           arrow_v0.1:000001c1…
   __input_data_context_key     "std:v0.1:default"          written, never read
   __computed, __is_ephemeral, __pipeline_recomputation_index
```

The result table adds: `__pf_var_function_name`, `__pf_var_function_signature_hash` and
`__pf_var_function_content_hash` (labelled `semantic_v0.1:`), `__pf_var_git_hash`,
`__pf_exec_executor_type`, `__pf_exec_python_version` (`3.12.15`), `_context_key`
(`std:v0.1:default`; read back to choose the data context for a cached row), `__pod_ts`.

**Main versus the ITL-910 branch.** Same directory tree, same node content and pipeline
hashes, same column names. Only the Arrow-hashed values differ:

| | main (starfix 0.4) | ITL-910 (starfix 0.5) |
|---|---|---|
| Path of `double` pipeline table | `…/schema:semantic_v0.1:6fe7280a…/pdb_v1` | identical |
| Row data hash, `{"x": 5}` | `arrow_v0.1:000001d3daa548b1…` | `arrow_v0.2:000002136ffea0f7…` |
| Default source id | `000001883080da70…` | `00000243d6b9f563…` |
| Content hash of a source with that default id | `semantic_v0.1:d8eb49fa…` | `semantic_v0.1:5b6ae6d2…` |
| `ndarray_v1`, `sha256`, schema and uri hashes | — | identical |

### 1.4 Every version, label and format marker

| # | Marker | Values today | What it is meant to capture | Who bumps it, when | Can a reader tell which version wrote a stored value? | What moves when it is bumped |
|---|---|---|---|---|---|---|
| 1 | Structure hasher label | `semantic_v0.1` | the algorithm for hashing Python structures | never bumped; hash-moving fixes went in under the same label (PR #301 proposes another) | yes where labelled; no in column names and operator uri | every content and pipeline hash, every function path, every system-tag column name |
| 2 | Arrow hasher label | `arrow_v0.1` -> `arrow_v0.2` (ITL-910) | the algorithm for hashing Arrow data. Rule accepted Nov 11, 2026: bump whenever any hash can change | Orcapod, by editing the context JSON | yes in labelled columns; only by the starfix prefix in bare hex | row hashes and row keys. **No path** |
| 3 | starfix format number | `000001`, `000002` | starfix's own byte format | starfix | yes | same as 2 |
| 4 | Per-type value labels | `sha256`, `merkle_sha256`, `ndarray_v1`, `polars_expr_v1`, pending others | the hash definition of one value type | type author. Rule in PR #300: bytes under a label never change | only inside a preimage; not stored next to the cell | any hash covering such a value |
| 5 | Data context key and version | `std:v0.1:default`, `v0.1` | the whole bundle: both hashers, the type converter, the registered types and handlers | **never bumped**: `v0.1.json` has been edited in place 20 times | the key is in every result row but is the same string before and after every change | nothing |
| 6 | Table layout suffixes | `pdb_v1`, `rdb_v1`, `tdb_v1` | the column layout of Orcapod's own tables | Orcapod, on a layout change | by path only: `SchemaVersionError` when a table exists **without** the suffix and none with it; `NodeConfig(ignore_schema=("v0",))` opts out | the last path component. `tdb_v1` and operator cache tables have no check |
| 7 | Function major version | `v0` (from `version="v0.0"`) | the user's statement that results changed | the user | yes | function uri, both function table paths |
| 8 | Function wrapper kind | `python.function.v0` | how a function is called and its outputs packaged | Orcapod; hard-coded in four files | yes | function table paths |
| 9 | Stored representation versions | array format `{"version":1}` in Arrow extension metadata; PR #308 nullability `{"format":"orcapod.nullability","version":1}`; all other types: none | the byte layout of a stored value | type author | arrays: yes, an unknown version raises. Others: no | nothing |
| 10 | Saved file formats | `orcapod_pipeline_version: 0.1.0`, `orcapod_pipeline_job_version: 0.1.0` | the JSON layout of a saved pipeline or job | Orcapod | yes; load rejects unknown versions. Hashes inside are trusted | nothing |
| 11 | File-hash cache table version | `1` | the cache table's layout | Orcapod | yes | nothing |
| 12 | Package version | `0.2.x` -> `0.3.0` | the release | release process | **no: written nowhere in a store** | nothing |
| 13 | Golden files | 685 pinned values, 5 files | a test that hashes do not move | regenerated only with approval | — | ITL-910 changes 186 |
| 14 | `table_scope` | `pipeline_hash` / `content_hash` | not a version: whether a node's table is shared across data or private to one input | user, per node | yes, in saved JSON | adds an `instance:…` path component |

Three naming conventions coexist: `_v0.1` (hashers), `_v1` (per-type labels, table layouts),
`0.1.0` (files).

### 1.5 What stays the same and what changes today (measured)

| Change made | Function uri | Pipeline hash | Table path | Row data hash |
|---|---|---|---|---|
| Baseline `double` | `('double', 'semantic_v0.1:17de3328…', 'v0', 'python.function.v0')` | `…6fe7280a…` | baseline | `arrow_v0.1:000001d3da…` |
| Edit the function body (`x + x`) | same | same | **same** (by design) | same |
| Minor version `v0.7` | same | same | **same** | same |
| Major version `v1.0` | `…, 'v1', …` | `…ea0cf7b3…` | **moves** | same |
| Rename to `twice` | `('twice', …)` | `…0de069df…` | **moves** | same |
| Return type `float` | `…'semantic_v0.1:1f13e266…'…` | `…868ee85c…` | **moves** | same |
| Rename output key to `twice_x` | `…'semantic_v0.1:b680b6b1…'…` | `…0e5702c0…` | **moves** | same |
| Different source name (`other_name`) | same | same (`52eb0af7…` for the source) | **same**; rows are told apart by `__node_content_hash` | same |
| Source column `int32` instead of `int64` | same | same | **same** | `arrow_v0.1:0000014dff…` (**differs**) |
| Arrow label `arrow_v0.1` -> `arrow_v0.2` | same | same | **same** | `arrow_v0.2:…` (**differs**) |
| Structure label -> `semantic_v0.2` | `…'semantic_v0.2:03141c1e…'…` | `semantic_v0.2:800bf59e…` | **moves** | same |

### 1.6 Failure modes, reproduced

| # | Change | What happens today | How it was reproduced |
|---|---|---|---|
| F1 | Arrow label changes, nothing else | Same paths. Every lookup misses, everything recomputes, new rows are **appended to the same tables**: 6 rows for 3 inputs | main with the label patched, on a store written by main |
| F1' | Same, on the ITL-910 branch tip | `StoreHashLabelError` naming the table, node and both labels; nothing read or written | ITL-910 snapshot on a store written by main |
| F2 | Structure label changes, nothing else | **Every function path moves.** The run succeeds, recomputes into new tables, leaves the old ones behind. No error: stored results silently look dropped | main with the label patched |
| F3 | One hasher's label changes but not the other | The two hashers behave in opposite ways (F1 mixes rows, F2 orphans tables) | F1 + F2 |
| F4 | A stored representation changes | Arrays: an unknown version raises `ValueError … expected metadata b'{"version":1}' but got b'{"version":2}'`. Every other type has no version | forged `{"version":2}` column |
| F5 | The data context changes | The stored `_context_key` is the same string before and after | `v0.1.json` changelog |
| F6 | A hash is stored bare | System-tag column names, operator uri, default source ids, operator-cache row keys: a reader cannot tell which algorithm made them | inventory 1.2 |
| F7 | A hash-moving fix ships without a label bump | Looks like a cache miss: silent recompute, mixed rows | PR #301 as proposed |
| F8 | The stored Arrow type of a column differs while its Python type is the same (`int32` vs `int64`) | Same pipeline hash, same path. The second run fails at commit with `StoreWriteError … Cannot append to the stored table`, **after** its rows were already written to the result table (4 rows for 2 inputs) | two `ArrowTableSource`s, same job |

```
F1  Arrow label moves                     F2  Structure label moves

  …/schema:…6fe7280a…/pdb_v1/               …/double/semantic_v0.1:17de3328…/…/pdb_v1/   <- old rows, never read again
    arrow_v0.1 row  id=0  ┐ old             …/double/semantic_v0.2:03141c1e…/…/pdb_v1/   <- new rows
    arrow_v0.1 row  id=1  │ never match
    arrow_v0.1 row  id=2  ┘                 no error, no warning
    arrow_v0.2 row  id=0  ┐ new
    arrow_v0.2 row  id=1  │
    arrow_v0.2 row  id=2  ┘
  a full read returns 6 rows
```

---

## Part 2. The axes of change

Part 1 lists markers. This part sets them aside and asks a different question: **in which
independent ways can Orcapod's behaviour change between two versions of the code?** Each such
way is an axis. A marker is only a name we choose to give to a position on an axis.

### 2.1 The hierarchy

```
Release (0.2.x, 0.3.0)                       any axis below can only move with a release
│
├── I. IDENTITY RULES                        how a hash value is produced
│   ├── I1  Structure hashing                Python structures -> hash          today: semantic_v0.1
│   ├── I2  Arrow data hashing               rows and tables -> hash            today: arrow_v0.1
│   │   └── starfix byte format                                                 today: 000001
│   ├── I3  Per-type value hashing           one value type -> hash             today: ndarray_v1, sha256, polars_expr_v1 …
│   └── I4  Identity composition             WHAT goes into each identity       today: no marker
│           ├── function identity   (name, output-schema hash, major version, wrapper kind)
│           ├── operator identity   (class name + parameters)
│           ├── source identity     (class, schema, source_id)
│           ├── record-id recipe    (uuid5 namespace, "{source_id}::{row token}")
│           ├── entry-id recipe     (system tags + input data hash)
│           └── system-tag fold     (how Batch / GroupBy combine members)
│
├── II. TYPE RULES                           how a Python value becomes stored data
│   ├── II1 Conversion rules                 Python type -> Arrow type          today: no marker
│   │       (int -> int64, str -> large_string, nullability, list -> large_list)
│   └── II2 Per-type representation          stored layout of one type          today: {"version":1} for arrays only
│           (array = struct<dtype, shape, data>; file = JSON {"path": …})
│
├── III. STORE LAYOUT                        how Orcapod arranges its bookkeeping
│   ├── III1 Path layout                     which components, in which order   today: no marker
│   ├── III2 Table layout                    system columns per table kind      today: pdb_v1, rdb_v1, tdb_v1
│   ├── III3 Column naming                   _tag_…::hash:pos, _source_…,       today: no marker
│   │                                        format of source-info tokens
│   └── III4 Backend encoding                how a backend writes Arrow         today: nullability version (PR #308)
│
└── IV. COMPUTATION BEHAVIOUR                what is computed
    ├── IV1 User function                    name, signature, major version     today: in the uri (user-controlled)
    ├── IV2 Function wrapper kind            how a function is called/packaged  today: python.function.v0
    ├── IV3 Operator behaviour               which rows Join, Batch … emit      today: no marker
    └── IV4 Execution environment            Python version, executor, git hash today: columns __pf_exec_*

Outside the store, independent of all of the above:
    saved pipeline / job file format (0.1.0)        file-hash cache table (1)
```

### 2.2 How the axes relate

```mermaid
flowchart TD
    II1["II1 Conversion rules<br/>Python type -> Arrow type"]
    II2["II2 Per-type representation<br/>stored layout of one type"]
    I3["I3 Per-type value hashing"]
    I1["I1 Structure hashing"]
    I2["I2 Arrow data hashing<br/>(contains the starfix format)"]
    I4["I4 Identity composition"]
    III3["III3 Column naming"]
    III2["III2 Table layout"]
    III1["III1 Path layout"]
    IV1["IV1 User function"]
    IV3["IV3 Operator behaviour"]

    DT(["dtypes of result columns"])
    KEYS(["values of key columns<br/>(finding a row)"])
    BYTES(["meaning of stored bytes<br/>(reading a row)"])
    NAMES(["names of system columns"])
    PATH(["table path"])
    ROWS(["which rows a table holds"])

    II2 -. "finer grain of" .-> II1
    I3 -- "feeds" --> I1
    I3 -- "feeds" --> I2
    II1 -- "the Arrow hash covers Arrow types" --> I2
    II1 --> DT
    II2 --> DT
    II2 --> BYTES
    I2 --> KEYS
    I1 --> KEYS
    I4 --> KEYS
    I1 -- "hashes sit in paths" --> PATH
    I1 -- "hashes sit in column names" --> NAMES
    I4 --> PATH
    III3 --> NAMES
    III2 --> NAMES
    III1 --> PATH
    IV1 --> PATH
    IV3 --> ROWS
    IV3 -. "can change list types" .-> DT
```

Reading the diagram:

- **Finer grain.** II2 is a finer grain of II1 (one type's layout versus the rules for all
  types). The starfix format is a finer grain of I2. I3 is a finer grain of both I1 and I2,
  because a value's hash is consumed by whichever hasher covers the thing containing it.
- **Dependency.** I2 depends on II1: the Arrow hash covers Arrow types, so changing how `int`
  is stored changes every row hash even though the hashing algorithm did not change
  (measured: the same values as `int32` hash to `arrow_v0.1:0000014dff…`, as `int64` to
  `arrow_v0.1:000001d3da…`).
- **Paired but independent.** For one type, II2 (how it is stored) and I3 (how it is hashed)
  are separate. The array format was designed so that the hash is by value: a future storage
  change need not move `ndarray_v1` hashes, and a hash fix need not change storage.
- **Independent.** Store layout (III) is independent of identity and type rules, except that
  column names (III3) embed I1 hashes. Computation behaviour (IV) is independent of all three.

### 2.3 Each axis in detail, with examples

For each axis: what it is, a change that has happened or could, and what should stay the same
and what should change.

#### I1 Structure hashing

How a Python structure (a schema, a function's identity, a node's identity) becomes a hash.

- Example values: `Schema({'x': int})` -> `semantic_v0.1:15816996…`; function output schema
  -> `semantic_v0.1:17de3328…`; node pipeline hash -> `semantic_v0.1:6fe7280a…`.
- A change on this axis: PR #301 (a dict's insertion order no longer affects its hash).
  A different canonical JSON form would be another.
- When it changes: every content and pipeline hash changes, so every hash in a path and every
  system-tag column name changes. Row data hashes (`arrow_v0.1:…`) do **not** change.
- Should stay the same when: data changes, a function body is edited, the Arrow hasher changes.

#### I2 Arrow data hashing

How a row or table of Arrow data becomes a hash.

- Example values: row `{"x": 5}` -> `arrow_v0.1:000001d3daa548b1…`.
- A change on this axis: starfix 0.4 -> 0.5 (ITL-910): the same row becomes
  `arrow_v0.2:000002136ffea0f7…`.
- When it changes: every row key changes (`__input_data_hash`, `__pipeline_base_entry_id`,
  pipeline-table `__record_id`), and default source ids. No column name, no dtype, no path.
- Should stay the same when: the structure hasher changes; a function is renamed.

#### I3 Per-type value hashing

How one value of a given type becomes a hash.

- Example values: `np.arange(3, dtype="int16")` -> `ndarray_v1:8462766b…`; the same numbers
  as `int64` -> `ndarray_v1:44ef444a…` (dtype is part of identity); a file containing
  `hello` -> `sha256:2cf24dba…`.
- A change on this axis: ITL-888 moved arrays from hashing `.npy` bytes (label `sha256`) to
  hashing by value (label `ndarray_v1`).
- When it changes: every row hash of a row holding that type, and every structure hash that
  contains such a value (a `polars.Expr` inside a filter operator's identity). Tables that
  never hold the type are unaffected.
- Should stay the same when: the type's stored layout changes but its value does not.

#### I4 Identity composition

What goes into an identity, as opposed to how it is hashed. Nothing marks this axis today.

- Example: a function's identity is the tuple
  `('double', 'semantic_v0.1:17de3328…', 'v0', 'python.function.v0')`. Deciding that the
  input schema should also be part of it is a change on this axis.
- Example: a source's identity is `(class, schema, source_id)`. Measured consequence: two
  `DictSource`s with the same `source_id="source1"` but one different value have the **same**
  content hash `semantic_v0.1:d644c803…`.
- Example: a source row's record id is `uuid5(namespace, "source1::row_2")`. ITL-817 proposes
  deriving it from content instead.
- Example: an operator's identity is its class name and parameters (`Join` ->
  `('Join', '295ca933…')`).
- When it changes: depends on which identity. A function-identity change moves paths; a
  record-id change moves `_tag_record_id` values and, through the entry id, every row key.

#### II1 Conversion rules

How a Python type maps to an Arrow type.

- Example: `int` -> `int64`; `str` -> `large_string`; `list[int]` -> `large_list<int64>`;
  `int` (required) -> non-nullable, `int | None` -> nullable.
- A change on this axis: the November 10 rule "always normalize to large variants".
  A hypothetical one: storing Python `int` as `int32`.
- When it changes: the dtype of result columns, and through I2 every row hash.
- Today the path does **not** move: the schema hash covers Python type names, not Arrow types
  (F8: `int32` and `int64` sources give the same pipeline hash `52eb0af7…` and the same path).

#### II2 Per-type representation

The stored layout of one logical type.

- Example: an array is stored as extension `orcapod.ndarray` over
  `struct<dtype, shape, data>` with metadata `{"version":1}`. Before v0.3 it was extension
  `numpy.ndarray` over `large_binary` holding `.npy` bytes.
- Example: `orcapod.file` changed from a plain path string to JSON `{"path": "…"}` (ITL-451)
  under the same name, with no marker.
- When it changes: the dtype of columns holding that type (if the layout changes shape), or
  the meaning of the bytes (if the shape is the same). Ideally not the hash (see I3).
- Today the path moves only if the type is renamed, because the schema hash uses the type's
  name. A `{"version":2}` layout under the name `orcapod.ndarray` would keep the path.

#### III1 Path layout

Which components a table path has and in what order. Example: adding a generation name at the
front, or putting the pipeline hash into the result table's path. No marker today; a change
is visible only as tables appearing somewhere else.

#### III2 Table layout

The system columns of each kind of table.

- Example: `pdb_v1` introduced labelled-bytes hash columns in place of `"method:hex"` strings
  and renamed `_node_content_hash` to `__node_content_hash`.
- A table without the suffix is "v0"; meeting one raises `SchemaVersionError`. Only the
  pipeline and result tables are checked.

#### III3 Column naming

The names Orcapod gives to columns it adds to streams.

- Example: a system-tag column is named `_tag_source_id::<schema hash>` and after a join
  `_tag_source_id::52eb0af7…::6fe7280a…:1` (source schema hash, then the pipeline hash and
  position of the join input).
- Example: a source-info token is `source1::row_2::x`.
- A change on this axis: truncating the hashes in column names, or a different separator.
- When it changes: names of system columns in every stored table, and (because system tags
  are hashed into the entry id) possibly every row key.

#### III4 Backend encoding

How one backend physically writes an Arrow table. Example: SpiralDB stores every column as
nullable, so PR #308 records nullability in table metadata under `__orcapod_nullability__`
with `"version": 1`. This axis is per backend and invisible to every hash.

#### IV1 User function

The user's function: name, signature, major version. The user controls it; all three are in
the uri and so in the path (table in 1.5). Body edits are excluded by design.

#### IV2 Function wrapper kind

`python.function.v0`: how Orcapod calls a function and packages its outputs. In the uri and
the path.

#### IV3 Operator behaviour

Which rows an operator emits, in what order, with what list types. Nothing marks this axis
today: `Join`'s identity is the string `"Join"`.

- Example of a change: `MergeJoin` sorting its merged lists differently, or `GroupBy` ordering
  members by a different tiebreaker. The pipeline hash and path stay the same while the rows
  written differ.
- Example that also changes a dtype: `Batch` typing its list columns as `large_list` instead
  of `list`.

#### IV4 Execution environment

Python version, executor, git hash of the user's code. Recorded per row
(`__pf_exec_python_version = '3.12.15'`). It affects neither where a row is found nor how it
is read.

---

## Part 3. The impact of each axis

The developer's rule (November 11, 2026): **anything that changes the schema of the result or
tag tables (a column name, a dtype, an added column) must be reflected in the path of the
table.** The last column measures today's code against that rule.

| Axis | Result column names or dtypes | System column names or dtypes | Values of key columns (old rows cannot be found) | Meaning of stored bytes (old rows misread) | Reaches the path today? |
|---|---|---|---|---|---|
| I1 Structure hashing | no | **yes**: system-tag column names embed these hashes | yes | no | Yes, as a side effect: hashes in the path change; old tables are orphaned silently (F2) |
| I2 Arrow hashing | no | no | yes, every row key | no | **No** (F1) |
| I3 Per-type hashing | no | no | yes, where rows hold the type | no | No |
| I4 Identity composition | no | sometimes | yes | no | Only if the changed identity is in the path |
| II1 Conversion rules | **yes** (dtype) | no | yes | possibly | **No** (F8) |
| II2 Per-type representation | **yes** if the layout changes shape | no | no, if the hash is by value | **yes** | Only if the type is renamed |
| III1 Path layout | no | no | no | no | It is the path |
| III2 Table layout | no | **yes** | no | no | Yes for pipeline and result tables; not for the other kinds |
| III3 Column naming | no | **yes** | possibly | no | **No** |
| III4 Backend encoding | as physically stored | as physically stored | no | yes | No |
| IV1 User function | yes (signature) | no | — | — | Yes |
| IV2 Wrapper kind | possibly | possibly | — | — | Yes |
| IV3 Operator behaviour | possibly | possibly | no | **yes**: same path, different rows | **No** |
| IV4 Environment | no | no | no | no | No, and correctly: it is recorded in columns |

Measured against the rule, **four axes break it today**: II1, II2, III3 and IV3. I1 satisfies
it only by accident, and without any way to find the old tables.

How a change reaches a path today, when it does:

```
  (a) by an explicit name      …/v0/…            …/pdb_v1          the reader can look for the old name
  (b) through a hash value     …/semantic_v0.1:17de3328…/…         the old path cannot be derived from the new one
  (c) not at all               arrow label, Arrow types, operator behaviour, column naming
```

---

## Part 4. Path or column: the rule (decided)

**Decision (developer, November 11, 2026):** "Only things that would certainly change the
schema of the table needs to be part of the path. Something that changes the value of a
column while still preserving the schema (same column names, same data types), can happily
reside within the same table (and thus share the path) as long as distinct entries within the
table can be told apart to reflect the difference in how the entry has come out to be (e.g.
minor version or hash of function content exactly makes it in as column values because we
want to keep track of these differences but nothing about schema would change as long as all
tracked components in the path such as output value schema changes)."

This replaces the test first proposed here (which also sent key-value changes to the path).

```mermaid
flowchart TD
    Q0{"Does the change certainly alter the table's schema?<br/>(a column name, a data type, the set of columns)"}
    P["PATH<br/>a new table"]
    C["COLUMN<br/>same table, same path"]
    R["Requirement: entries written before and after<br/>the change can be told apart by a column value"]
    Q0 -- yes --> P
    Q0 -- no --> C --> R
```

### 4.1 The rule applied to each axis

"Told apart today?" asks whether a stored row already carries a value that distinguishes the
two sides of a change on that axis.

| Axis | Changes the schema? | Verdict | Told apart today? | Gap |
|---|---|---|---|---|
| I1 Structure hashing | **yes**: system-tag column names embed structure hashes | path | the path already moves, because the hashes in it change | the old path cannot be derived from the new one; no way to find the old table |
| I2 Arrow hashing | no: values of key columns only | column | **yes**: every Arrow-hashed value carries its label (`arrow_v0.1:` / `arrow_v0.2:`) | bare Arrow hashes (default `source_id`, operator cache key) carry only starfix's prefix |
| I3 Per-type value hashing | no | column | **no**: the row hash `arrow_v0.1:…` does not say which array or file hash definition was used inside it | needs a marker, or the rule "bump the Arrow label whenever any hash can change" must cover per-type changes |
| I4 Identity composition (record-id recipe, entry-id recipe, folds) | no, except where it changes column names | column | **no**: record ids and folded ids are bare bytes | needs a marker |
| I4 Identity composition (function identity, operator identity) | no | column | partly: the function uri is in the path and in `_source_` tokens | — |
| II1 Conversion rules | **yes**: data types | path | — | **the path does not move today** (F8) |
| II2 Per-type representation | **yes** when the stored type changes | path | — | the path moves only if the type is renamed |
| III2 Table layout | **yes** | path | — | `pdb_v1` / `rdb_v1` exist; other table kinds have none |
| III3 Column naming | **yes** | path | — | the path does not move today |
| III4 Backend encoding | as physically stored | per backend | — | — |
| IV1 User function: output schema | **yes** | path | already in the path | — |
| IV1 User function: name | no | path today | identifies which function the table belongs to | — |
| IV1 User function: major version | no | **path today**; a column by the rule | in the path | to decide: keep as a deliberate partition, or make it a column that the lookup must match |
| IV1 User function: minor version | no | column | **no: not stored anywhere** | add a column |
| IV1 User function: body | no | column | **yes**: `__pf_var_function_content_hash` (hash of the source text) | — |
| IV2 Wrapper kind | possibly | path today | in the path | — |
| IV3 Operator behaviour | no (usually) | column | **no** | nothing marks it; and putting it in the operator's identity would change pipeline hashes and so system-tag column names, which makes it a path change |
| IV4 Execution environment | no | column | **yes**: `__pf_exec_python_version`, `__pf_exec_executor_type`, `__pf_var_git_hash` | — |

### 4.2 What "told apart" has to cover

A value-level change that affects **key columns** has two consequences inside a shared table:

```
 one table:  …/schema:semantic_v0.1:6fe7280a…/pdb_v1

   __input_data_hash          __pipeline_base_entry_id      id
   arrow_v0.1:000001d3da…     arrow_v0.1:00000189…           0     ┐ written before the change
   arrow_v0.1:000001b277…     arrow_v0.1:000001a4…           1     ┘
   arrow_v0.2:000002136f…     arrow_v0.2:0000027c…           0     ┐ written after the change
   arrow_v0.2:00000275c5…     arrow_v0.2:000002e1…           1     ┘

 (1) A lookup by key cannot match across the two groups: the key values differ.
     Nothing wrong is ever returned, but nothing old is ever found either.
 (2) A read of the whole table returns both groups: 4 rows for 2 inputs,
     unless the reader selects one group by the distinguishing value.
```

So for each value-level axis the system needs to say (a) which column value distinguishes the
groups, and (b) what a reader does with rows from another group: skip them, fail, or reuse
them. Point (b) is open.

### 4.3 Concrete cases under the rule

| Case | Schema changes? | Verdict | Distinguishing value |
|---|---|---|---|
| starfix 0.4 -> 0.5 (`arrow_v0.1` -> `arrow_v0.2`) | no | same table | the label inside `__input_data_hash`, `__pipeline_base_entry_id`, `__record_id` |
| Arrays move from `.npy` bytes in `large_binary` to `struct<dtype, shape, data>` | yes | new path | — |
| A future array layout `{"version":2}` with the same struct | to decide: is the extension metadata part of the "data type"? | — | — |
| Dict insertion order stops affecting structure hashes (PR #301) | yes for nodes whose hashes move: paths and system-tag column names change | new path | — |
| A SpiralDB table gains the nullability metadata (PR #308) | yes (nullability is part of the type) | new path | — |
| `GroupBy` changes its member tiebreaker | no | same table | none today |
| Record ids become content-derived (ITL-817) | no | same table | none today |
| User edits a function body | no | same table | `__pf_var_function_content_hash` |
| User bumps the minor version | no | same table | none today |
| User upgrades Python 3.12 -> 3.13 | no | same table | `__pf_exec_python_version` |

---

## Part 5. The system (draft, under discussion)

### 5.1 The core logic

A table's path is the name of two things and nothing else:

| | What it is | For a function's pipeline table | For a function's result table |
|---|---|---|---|
| **Schema (S)** | the column names and data types actually stored | tag columns; system-tag columns (their names embed upstream schema and pipeline hashes); Orcapod's bookkeeping columns | the function's output columns; `_source_<col>` columns; bookkeeping columns |
| **Subject (M)** | what one row stands for | "one input entry of this function, at this position in this chain of steps" | "one result of this function": its name, major version, wrapper kind |

Everything else about a row, how it came to be, is a column value.

```mermaid
flowchart TD
    C["A change to Orcapod, or to the user's pipeline"]
    Q1{"Does it change S of this table?<br/>(a column name, a data type, the set of columns)"}
    Q2{"Does it change M of this table?<br/>(which computation the rows belong to)"}
    Q3{"Does it change a value that existing rows carry?"}
    NP["NEW PATH for this table"]
    SP["SAME PATH. Old and new rows share the table.<br/>Required: a column value tells them apart."]
    NO["No effect on this table"]
    C --> Q1
    Q1 -- yes --> NP
    Q1 -- no --> Q2
    Q2 -- yes --> NP
    Q2 -- no --> Q3
    Q3 -- yes --> SP
    Q3 -- no --> NO
```

The logic is applied **per table**. One change can move some tables and leave others alone.

Why a structure-hash change moves paths *by its nature*: the names of system-tag columns embed
the schema hash of each source and the pipeline hash of each join input. When an upstream
schema's hash changes, the column names of every downstream pipeline table change, which is a
change of S. Measured on a pipeline `array source -> size -> inc`, v0.2.0 against main:

| Table | Own columns hold an array? | Change in S | Path |
|---|---|---|---|
| pipeline table of `size` | no (tags and system tags only) | `_tag_record_id::8a81bfa1…` becomes `_tag_record_id::f0b7551d…` | moves (`…7c1aad1b…` -> `…26e8aefa…`) |
| pipeline table of `inc`, one step further down | no | same rename | moves (`…7d689f45…` -> `…67de61d1…`) |
| result table of `inc` (`n: int -> m: int`) | no | none | **stays**, results reused |

### 5.2 Every change from the old version to v0.3: what changed, where it shows, how to migrate

**Baseline.** The old deployed version is `nauticalab/orcapod-python` at `61d0f18` with starfix
0.4.0. The tables below were measured against the tag `v0.2.0`, which contains that commit plus
15 later commits. The same script was then run on `61d0f18` itself: every value in the first
table is identical to `v0.2.0`. See 5.4 for the one stored difference found. **Target.** `main` at `d61e3e39` plus the open pull
requests expected in v0.3.

**Method.** One script builds a source and a function node for each kind of column and prints
the stored type, the row data hashes, the schema and pipeline hashes, and the system-tag
columns of a join and a batch. It was run on `v0.2.0`, on `main`, and on snapshots of PR #309
(ITL-910), PR #292 and PR #301.

**Result in one table.** "same" means byte-identical between `v0.2.0` and the version named.

| Column kind | `main` | + PR #309 starfix 0.5 | + PR #292 class identity | + PR #301 dict order |
|---|---|---|---|---|
| `int`, `str`, `float`, `bool`, `int \| None`, `list[int]`, nested list, `Path`, `File`, `DataFrame` | same: type, hashes, paths | row data hash only: `arrow_v0.1:000001\|8065521a…` becomes `arrow_v0.2:000002\|5682a627…` | same | same |
| `dict[str,int]` | same | row data hash only | same | row data hash of a dict written in another key order: `…63b89b7a…` becomes `…33a846ce…` (equal to the same dict in sorted order) |
| `numpy.ndarray` | **stored type** `numpy.ndarray` over `large_binary` becomes `orcapod.ndarray` over `struct<dtype, shape, data>`; row hash `…45f0d32e…` becomes `…a1920ba5…`; schema hash `21afcd86` becomes `3c83b899`; function output-schema hash `67e09e1a` becomes `272e93f6`; all paths of such nodes move | row data hash label and value | — | — |
| dataclass, pydantic model | same | row data hash only | **logical type name** `inv_models.Point` becomes `inv_models.Point@1a18cd5e…` (storage `struct<x, y>` unchanged); row hash `…16134b5a…` becomes `…07101202…`; schema hash `90a5ffbb` becomes `9fbb8279`; output-schema hash `0c513ac2` becomes `aa6490d2`; all paths of such nodes move | same |
| default `source_id` (no id given) | same | `000001883080…` becomes `00000243d6b9…`; the source's content hash `d8eb49fa` becomes `5b6ae6d2` | same | same |
| system-tag column names and types, join and batch pipeline hashes, folded values, source-info tokens | same | same | same | same |
| table layout (`pdb_v1`, `rdb_v1`), path grammar, bookkeeping columns | same | same | same | same |

**Change by change.**

| # | Change | What changed | Where it shows | Outcome by the logic of 5.1 | Migration |
|---|---|---|---|---|---|
| 1 | Array format (ITL-888, merged) | arrays stored as a value-based struct, not `.npy` bytes; hashed by value | stored column type and logical type name; value-hash label `sha256` becomes `ndarray_v1`; row data hashes; schema, output-schema and pipeline hashes; system-tag column names downstream; result and pipeline table paths | schema changes: new path for every table with an array column and every pipeline table downstream | **Possible from the old store alone.** Read the old table at its old path (the old path is computable: same structure hasher, old type name), decode the `.npy` bytes, write the struct, recompute keys. Not for values v0.3 refuses: a masked array's mask was dropped silently on write, so the stored value is already not what the user had |
| 2 | starfix 0.5 (ITL-910, PR #309) | the Arrow hash algorithm. Format 1 lost information: nested-list lengths, a null struct versus a struct of nulls, booleans (also chunk-dependent) | label and prefix of every Arrow-hashed value: `__input_data_hash`, `__output_data_hash`, `__pipeline_base_entry_id`, the pipeline row key; bare default `source_id` and what derives from it (record ids, `_tag_source_id` values, `_source_` tokens, the source's content hash, content-scoped and cached-source paths) | values only: same path; old and new rows share the table, told apart by the label | **Possible when the inputs can be obtained again** (sources still available, or upstream results in the store). A key is the hash of an *input*, and a function's tables do not store its inputs. Procedure: re-derive each input row, match the old row by its tags (the developer's decision of November 11), write a new pipeline row with the new key pointing at the existing result, and a result row keyed by the new input hash. **Not blindly safe for inputs that hold nested lists, nullable structs or booleans:** under format 1 two different inputs could share one key, so an old row may point at the result of a different input. Detectable during migration: if two old rows shared an input hash and their re-derived inputs get different new hashes, the later one must be recomputed |
| 3 | Polars filter predicates (ITL-828, merged) | a predicate becomes hashable | new values only (`polars_expr_v1`) | nothing existing moves: `v0.2.0` raised `TypeError` for a predicate filter; a filter by `constraints` is unchanged (`98590003` on both) | Nothing to migrate |
| 4 | User class identity (PR #292) | a dataclass or pydantic column's logical type name gains a fingerprint of the class's fields | logical type name in the stored column; row data hashes (the name is part of the hashed cell); schema, output-schema and pipeline hashes; system-tag column names downstream; table paths | schema changes (the logical type is part of the data type): new path | **Possible with the user's class definition.** The stored struct is unchanged; rows are copied under the new type name and re-keyed. The fingerprint comes from the current class, so the class must still match the stored fields; a class that changed since cannot be matched |
| 5 | Dict and set order (PR #301) | equal dicts and sets hash the same whatever their insertion order | row data hashes of rows holding a dict or set written in a non-sorted order; no label of its own | values only: same path. Told apart only because the Arrow label moves in the same release | **Possible.** Under the old rule equal values could have several keys, never the reverse, so no result is attached to the wrong input. Re-key; where two old rows collapse to one key, keep one |
| 6 | Spiral nullability (PR #308) | Spiral tables record which columns are non-nullable | table metadata `__orcapod_nullability__`; for a Spiral source, column types `int \| None` become `int`, so schema hashes and paths | schema changes for those tables: new path | A table without the metadata reads as before; one that gains it is a new schema |
| 7 | New value types (PR #300 torch, PR #286 Polars values) | new logical types | new type names and labels | nothing existing moves | Nothing to migrate |
| 8 | Wrong-hash fixes (ITL-942, in progress) | fold separator; nested nullability hashed consistently; process-independent set hashing; column-selection arguments normalised; unordered inputs no longer collapsed by `source_id`; record-id type; default version | per fix: folded `_tag_source_id` values after Batch or GroupBy; row hashes of nested columns; pipeline hashes and paths of nodes after a column selection or a merge join | mostly values; a few are a pure rename of the path (same schema, same subject) | Per fix. A rename is migratable by moving the table (the old path is computable). The fold collision and the collapsed inputs could have attached one identity to two different things; affected rows are recomputed |
| 9 | Behaviour fixes (merged: ITL-884, ITL-878, ITL-847/829; open: PR #299, PR #293) | Delta refuses a changed column type instead of casting silently; a failed store write fails the run; SemiJoin handles nested columns; outputs must match the declared type; Delta reads return every row | no hash, label, type or path | nothing moves | No migration. Old stores may hold values written under the old behaviour (a silently cast column, an output that did not match its declared type); these cannot be told apart from correct rows by anything stored |

**Conclusion.** The logic does not give v0.3 a distinct path everywhere. It gives a new path
exactly to the tables whose schema changed (arrays, user classes, Spiral nullability, and
their downstream pipeline tables). Every other table is shared by v0.2 and v0.3 rows, which
differ in the Arrow-hashed key values and are told apart by the label inside those values.

### 5.3 Where today's paths are not a faithful function of S and M

| Kind | Case | Evidence |
|---|---|---|
| S changes, path does not | the stored Arrow type of a column (type conversion rules, a representation change under the same type name) | F8 |
| S changes, path does not | the grammar of system-tag and source-info column names | no marker |
| S changes, path moves only for two table kinds | Orcapod's bookkeeping columns | `pdb_v1`, `rdb_v1` only |
| Neither S nor M changes, path moves | `SelectDataColumns(["x"])` versus `(("x",))` | A.9 F |
| Neither S nor M changes, path moves | any change to how a structure hash is computed for an unchanged structure: the table is the same table under a new name | F2 |
| M changes, path does not | two unordered inputs with the same `source_id` collapse | A.9 E |

### 5.4 One global hash version (direction set by the developer; details proposed)

**Requirement (developer, November 11, 2026).** Every row must carry enough to tell exactly
which version of the computation of hashes produced it. Summarising everything that changed is
not sensible, so there is **one number** that changes whenever anything that could alter a
previously computed hash changes. If two rows share the number, they were computed with
exactly the same algorithms, definitions and libraries, and the same entry would have got the
same hash. A lookup, possibly in `orcapod-sanctuary`, maps each number to the actual
combination of tools, so that two numbers can be compared (what differs between them) and a
migration can be planned: cheap where nothing relevant to an entry changed, or at least "hash
under both; if the old entry exists, create the new entry from it". A purely additive change,
under which no previously computed hash changes, does not change the number.

Working name here: **hash version** (the name is open).

**Numbering (decided November 11, 2026).** `H0` is exactly what the deployed
`nauticalab/orcapod-python` at `61d0f18` (starfix 0.4.0) computes. `H1` is what v0.3.0 computes
at its tag. v0.2.0 gets no number of its own: it was just released and has no real users. Only
one manifest has to be written after the fact (`H0`); `H1` is maintained as the ongoing one
until the v0.3.0 tag.

`v0.2.0` is **not** the same code as `61d0f18`: 15 commits and 128 source files lie between
them (fixes to joins, source info, Arrow types of aggregated columns, tag column order). On
the scenarios of 5.2 the two give identical paths, column names, column types and hash values.
One stored difference was found: on `61d0f18` the source-info token of every row that passed
through an operator is the token of the first row (`source1::row_0::x` where `v0.2.0` stores
`source1::row_2::x`; fixed by ITL-845). Source-info tokens are not hashed into keys.

The golden suite of `v0.2.0` (608 pinned cases in five files) was then generated with the code
of `61d0f18`: **all 608 come out identical**. So `v0.2.0` computes `H0`, and the golden files
at the `v0.2.0` tag can serve as the `H0` reference values.

**Relation to the data context (proposed answer to the developer's question).** The hash
version is not finer than the data context key was meant to be, and it is not orthogonal to
it. It is broader than what the data context holds today:

| The hash version must cover | In the data context today? |
|---|---|
| structure hasher, Arrow hasher | yes (`semantic_hasher`, `arrow_hasher`) |
| value hashers (file, directory, array, expression, types, schema) | yes (the handler registry) |
| type conversion rules, logical types and their names | yes (`type_converter`) |
| what each identity is composed of (source, function, operator, node) | **no**: code in the source, pod and operator classes |
| record-id, entry-id and fold recipes | **no**: code in the stream builder, function node, `arrow_utils` |
| the grammar of system-tag column names | **no**: code in `arrow_data_utils` and the operators |
| third-party library behaviour | **no** |

And the data context holds one thing that is not a hash-version matter: the set of registered
logical types can grow (a new type is added) without any existing hash changing.

Proposal: make the data context version **be** the hash version. The context key is already
stored per row and per node, and it is already what a reader uses to pick the hashers for a
stored row, which is exactly what reading an old row under its own rules needs. Two changes
make it true: the version changes whenever an existing golden value changes (and only then),
and its manifest also names the definitions that live in code today.

```mermaid
flowchart LR
    subgraph REG["Registry: one frozen manifest per hash version"]
        M1["H0 (= nauticalab 61d0f18)<br/>structure: canonical JSON, rules s1<br/>arrow: starfix format 1<br/>array: sha256 of .npy bytes<br/>record id: positional uuid5<br/>fold: sha256, no separator<br/>…"]
        M2["H1 (= what v0.3.0 computes)<br/>structure: s1 + fixes<br/>arrow: starfix format 2<br/>array: array_digest_v1<br/>record id: positional uuid5<br/>fold: sha256 with separator<br/>…"]
    end
    ROW["a stored row<br/>hash version = H0"] -->|"look up"| M1
    CUR["current Orcapod<br/>hash version = H1"] --> M2
    M1 -->|"diff"| D["what differs between H0 and H1:<br/>arrow, array, fold, …"]
    M2 --> D
    D --> PLAN["per entry: unaffected -> relabel;<br/>affected -> hash under both, link old to new"]
```

**What the number covers** (anything that can change a previously computed hash; Appendix A
is the list of sites): structure hashing rules; the Arrow hasher, including Orcapod's
preparation step and the starfix format; every value hasher (file, directory, array,
expression, types, schemas); what each identity is composed of (source, function, operator,
node); the record-id, entry-id and fold recipes; the type conversion rules and logical type
names, since stored Arrow types and type names are hashed; the grammar of system-tag column
names, since they are hashed into entry ids; and third-party libraries wherever their
behaviour enters a hashed value (starfix; pyarrow's conversion of pandas data; Polars'
serialisation of expressions).

**How "anything changed" is detected.** The golden hash files are the operational definition
of a hash version: about 685 pinned values today. Proposed rule: a change to any existing
golden value requires a new hash version; adding golden values does not. That makes the
developer's "additive changes do not change the number" a test, not a judgement.

**What exists today, measured against the requirement.**

| Candidate | Stored per row? | Changes with every hash-affecting change? |
|---|---|---|
| Arrow label (`arrow_v0.1` -> `arrow_v0.2`) | in the labelled key values of function tables; not in the operator cache, cached-source or (bare) system-tag values | by the rule accepted for ITL-910 ("bump whenever any hash can change"), yes for Arrow-derived hashes; says nothing about structure hashes |
| Structure label (`semantic_v0.1`) | in paths and labelled columns | never changed, although structure hashes did |
| Data context key (`std:v0.1:default`) | `_context_key` in result rows, `__input_data_context_key` in pipeline rows, per node in saved jobs; not in the operator cache, cached-source or side-effect tables | never changed: its file was edited in place 20 times |
| Registry of what each version means | the context file `contexts/data/v0.1.json` lists the classes and has a changelog | it is not frozen per version, and it names classes whose behaviour changed underneath, not definitions |

---

## Part 6. Generation names in paths (decided: none)

No generation name is added to paths. The developer's question of November 11 ("Should we
think about having explicit name so that we have path change everywhere for v0.3?") is
answered by the logic of Part 5: a path changes when, and only when, the table's schema or
subject changes. Path layout before and after v0.3 is therefore the same:

```
<job>/<function name>/<output-schema hash>/v<major>/<wrapper kind>/schema:<pipeline hash>/pdb_v1
<job>/_result/<function name>/<output-schema hash>/v<major>/<wrapper kind>/rdb_v1
```

**On meeting rows of an older value-level generation** (decided November 11, 2026): v0.3 does
not fail. It writes and uses `arrow_v0.2` rows; `arrow_v0.1` rows stay in the same table,
untouched and distinguishable by the label in their values, so that they can be migrated
later. A read of a whole table must use rows of the current label only.

### 6.1 Consequences found while implementing "do not fail" (ITL-910 worker, November 11, 2026)

| # | Consequence | Why | Options |
|---|---|---|---|
| 1 | **Side effects are delivered again.** On the first v0.3 run against a v0.2 store, every side effect (an upload, a notification) repeats. Verified by the ITL-910 worker | the side-effect log is read only by key, and the key starts with the Arrow label, so `arrow_v0.1` completions are not found | accept and document (provisional), or keep an explicit failure for side-effect logs only |
| 2 | **Two table kinds carry no Orcapod label in their rows**: the operator cache and the cached-source table. Their row key `_record_hash` is bare hex; `__node_content_hash` is the same under both labels when `source_id` is given | 1.2: hashes stored bare | the branch filters by starfix's own prefix (`000001` / `000002`). A future Arrow label that keeps starfix format 2 would be indistinguishable there unless a label is stored, which changes those tables' layout |
| 3 | **Old results look absent until v0.3 writes.** Reading a node on a v0.2 store before any v0.3 run returns no rows; a job saved by v0.2 and loaded by v0.3 shows no rows | reads use current-label rows only | report, once per table, how many rows of another label were skipped |

## Part 7. Before the v0.3 tag, and after (to be decided)

_Filled in during the discussion._

## Part 8. Follow-up issues to file

_Filled in during the discussion._

## Part 9. Decisions

Decisions made before this discussion (developer, as recorded in ITL-938, ITL-910, ITL-913):

| Date | Decision |
|---|---|
| Nov 10, 2026 | Hash values must stay identical across releases. |
| Nov 11, 2026 | "A hash bug overrides hash stability: erroneous values move, and the change is marked explicitly as a fix." |
| Nov 11, 2026 | Arrow label rule: bump Orcapod's label whenever any hash can change; label numbers are not kept aligned with starfix's format number. |
| Nov 11, 2026 | v0.3 fails explicitly when it meets v0.2 rows (ITL-910). |
| Nov 11, 2026 | "As for the migration, we are going to use tags as the source of truth in matching and finding records." (ITL-913) |
| Nov 11, 2026 | Open question that started this design: "Should we think about having explicit name so that we have path change everywhere for v0.3?" |

Decisions made in this discussion:

| Date | Decision (developer's words where quoted) |
|---|---|
| Nov 11, 2026 | Organise by axes of change, not by today's markers: "what we really need is to organize hierarchically all the different elements / ways by which something about the behavior of the Orcapod system can change." |
| Nov 11, 2026 | "Anything that ends up changing the schema of the results (changes the column name/dtype or adds more columns to results / tags table) should have itself reflected in the path of the table." |
| Nov 11, 2026 | "Even if the schema/column doesn't change but the meaning of the columns change, we should critically evaluate whether that should be present as entirely different path or if it should just be reflected in a column that keeps that info." |
| Nov 11, 2026 | Asked for the identity chains (pipeline versus content identity, source identity, source entry identity), system tags and source columns to be laid out explicitly, and for every hash computation site to state precisely what affects its result (sections 1.2.1, 1.2.2, Appendix A). |
| Nov 11, 2026 | **Source identity contract.** "Whether source identity precisely reflects the exact content OR whether it is meant to just serve as the 'address' of how to (supposedly reliably) refer to the very source is up to the design / decision of the source. This is intentional as sometimes we want to treat the 'repository' as the source identity (e.g. a github repo address). Within the repo, we expect distinct entries to carry unique and reliable record/entry ID which will be used for source columns but as far as source ID is concerned, it's just the repo. We trust that the source will maintain that if source ID and record/entry ID is the same, then we are talking about exactly the same data entry." Also: the content hash is the Merkle chain that includes the identity of the bound sources; the pipeline hash captures only schema and the chain of steps. |
| Nov 11, 2026 | **Function major version stays in the path.** "Function major version must stay in the path as that's one big handle that we are giving to the user to explicitly say that function has shifted dramatically enough that it won't make sense for the results to stay within a shared table even if everything else looks the same." |
| Nov 11, 2026 | **Wrong-hash findings (A.9) are fixed before the v0.3 tag.** "I want all of these to be addressed before v0.3 tag as much as possible." |
| Nov 11, 2026 | **A distinct path needs a reason from the nature of the change.** "Typically we want to have things into completely distinct path if the schema changed, OR if what's meant by the table content, even if schema is the same, is fundamentally changed. Otherwise, the two distinct entries could have simply been kept inside a same path (and thus same table) but with one of the columns being sure to change to capture the different version." The system must be able to say, for the changes from v0.2 to v0.3, if and how the path or the table schema changes and why (Part 5). |
| Nov 11, 2026 | **v0.3 does not fail on `arrow_v0.1` rows** (reverses the earlier decision of the same day). "Looking back, I don't think it'd make sense for the branch to raise an error when it meets old rows from arrow_v0.1. As long as just store and make use of arrow_v0.2, then it's ok that arrow_v0.1-based values are present next arrow_v0.2. I do want to make sure that we COULD perform migration from arrow_v0.1-based entries into arrow_v0.2-based entries down the line." And: "For value only changes such as starfix, I think it'd make sense to NOT fail but still be possible/completely clear to distinguish old vs new entries." |
| Nov 11, 2026 | **The logical type is part of the data type.** "Logical data type is absolutely part of the data type -- even if underlying representation technically didn't change, if extension type changed, it should generally be considered to have migrated in data type. Change in logical / extension metadata should be seen as changing schema just as much as actually changing the data type in arrow." |
| Nov 11, 2026 | **What a label is for.** "I'm generally not convinced yet of the need for all hashes to be prefixed with operator/data type specific prefix. I think what's critical is that we have confidence that hash would change if what it identifies had changed. Label may help to make WHY something changed more obvious OR sometime label change is necessary to ensure we don't accidentally collide in hash when we don't have a guarantee that hash would be of different values when it ought to be ... its need heavily depends on what we want to be able to 'easily' identify as the source of change just by looking at the hash string and to much lesser extent, whether we need to further guarantee that hash would change." |
| Nov 11, 2026 | Operator behaviour changes: leaning to record nothing (treat like upstream data changing); to be confirmed with concrete examples. Which value tells generations apart: leaning to labels plus a per-row rules name; to be confirmed with concrete examples. |
| Nov 11, 2026 | **Side-effect logs fail on rows under another label; other tables do not.** "Let's do fail for side-effect log only although I want to separately step into this later and discuss more of what's present." |
| Nov 11, 2026 | **Report skipped older rows as a total count.** "Yes but just total counts I think would be sufficient (eventually we want to detect old entries and suggest to perform migration)." |
| Nov 11, 2026 | **Old results looking absent without migration is accepted; additive changes do not change the hash version.** "I think old values looking absent without explicit migration is something we have to accept. Generally, as long as what we do is simply additive in features, such that previous hash value should NOT have changed under new version, then we don't need to actually alter the hash version number and thus we won't run into suddenly empty table under the new hash version." |
| Nov 11, 2026 | **One global hash version, stored per row, with a registry.** "Anytime anything really that could alter previously computed hash changes between versions, we need information inside the row that would allow us to precisely tell what must have changed. Since summarizing everything that actually changed is not really sensible, the way to do is to have a single number that keeps on varying so that every unique different combo of hashing mechanism would yield different number such that we know for sure that if two rows share this number, they were computed using exactly identical set of algorithms/definitions/libraries such that same entry would have yielded same hash. ... we should have a look up somewhere (possibly as part of orcapod-sanctuary) that maps every exact number to actual combination of specific tools. This way, when given two such numbers (e.g. v15 vs v1), we can look up what is actually different between the two versions ... and consequently decide if we can cheaply migrate ... or minimally we can just rerun all hashing under v1 and v15, and if v1 version exist, take it and create a new entry with v15 hash." Whether this is what `arrow_v0.1` / `arrow_v0.2` should mean, or belongs in the data context, is open (5.4). |
| Nov 11, 2026 | **Numbering of the hash version.** "I'd rather that H0 is what the current nauticalab/orcapod-python exactly points to, forget about v0.2.0 as we just had released it but with no real users anywhere yet and we are about to move to v0.3.0. So let H1 be what v0.3.0 going to be. ... This way we only have to backfill one manifest from H0 and the updated ongoing one as H1." |
| Nov 11, 2026 | Operator-behaviour marker: deferred until the global hash versioning strategy is confirmed. |
| Nov 11, 2026 | **Path or column rule.** "Only things that would certainly change the schema of the table needs to be part of the path. Something that changes the value of a column while still preserving the schema (same column names, same data types), can happily reside within the same table (and thus share the path) as long as distinct entries within the table can be told apart to reflect the difference in how the entry has come out to be." Consequence: a pure hash change such as `arrow_v0.1` -> `arrow_v0.2` does not get a new path. |

Open questions for the developer:

1. Does "data type" include a logical type's extension name and metadata (its stored
   representation version), or only the physical Arrow type?
2. What does a reader do on meeting rows of another value-level generation in the same table:
   skip them, fail, or reuse them? Does the November 11 decision "v0.3 fails explicitly when it
   meets v0.2 rows" stand?
3. Function major version: keep it in the path, or make it a column the lookup must match?
4. Which column tells value-level generations apart: the labels already inside the values,
   a per-row rules name (the data context key, made meaningful), or both?
5. Operator behaviour (IV3): should a change be recorded, and where?
6. Which of the wrong-hash findings in A.9 are fixed before the v0.3 tag?

---

## Appendix A. Every hash computation site: what affects the result

For each site: the algorithm, every input that changes the result, inputs one might expect
to matter that do not, and the label that comes out. Every "affects" and "does not affect"
below was measured by changing one thing and comparing hashes (Python 3.12.15, numpy 2.2.6,
pandas 2.2.3, polars 1.41.2, pyarrow 23.0.1, starfix 0.4.0, macOS arm64), unless marked
"(code only)". The claims flagged in A.9 were re-run a second time independently, on main
and on the ITL-910 snapshot.

Not verified: other Python versions, big-endian hosts, PostgreSQL and SpiralDB sources,
`PollingSource`, the async Join path.

Notation: `S:` stands for `semantic_v0.1:`; hashes are shortened to their first 8 hex digits
(for Arrow hashes, the 8 after the `000001` prefix).

### A.0 Map of sites

```
 VALUE HASHERS (A.2)                STRUCTURE HASHER (A.1)                 ARROW HASHER (A.3)
 file, directory, ndarray,   ──►    hash_object(structure)          ◄──   hash_table(table)
 polars expr, bytes, uuid,          sha256(canonical JSON)                 starfix digest
 type objects, functions,                    │                                    │
 schemas                                     ▼                                    ▼
                         IDENTITY STRUCTURES (A.4 - A.6)             ROW IDENTITIES (A.7 - A.8)
                         source, stream, operator, function,         row data hash, default source_id,
                         node  ->  content_hash / pipeline_hash      record id, system tags, entry id,
                                                                     row keys of every table
```

### A.1 The structure hasher: `hash_object`

`hashing/semantic_hashing/semantic_hasher.py`. Used for every `content_hash()` and
`pipeline_hash()`, schema hashes and function identity.

**Algorithm**

1. A value that is already a hash is returned unchanged, with its own label.
2. `None`, `bool`, `int`, `float`, `str` (and subclasses) go straight to step 6.
3. Otherwise a handler is looked up by exact type, then along the type's base classes
   (A.2). Its result is fed back into step 1.
4. Containers are expanded:

   | Input | Becomes |
   |---|---|
   | `list` | JSON array, order kept |
   | `tuple` | `{"__type__":"tuple","items":[…]}`, order kept |
   | `set`, `frozenset` | `{"__type__":"set","items":[…]}`, items sorted by their `str()` |
   | `dict`, any mapping | JSON object with keys turned into `str()`, sorted |
   | namedtuple | `{"__type__":"namedtuple","name":<class name>,"fields":{…}}` |
   | anything else nested inside | its own hash as the labelled string `"<label>:<hex>"` |

5. An object with its own identity (a source, stream, pod) is replaced by its `content_hash()`
   or `pipeline_hash()` string, depending on which chain is being computed.
6. Digest: `sha256(json.dumps(tree, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))`.
7. Output label: `semantic_v0.1`.

Example: a source's pipeline identity `(tag_schema, data_schema)` is hashed as exactly
`{"__type__":"tuple","items":[{"id":"semantic_v0.1:24be397c…"},{"x":"semantic_v0.1:24be397c…"}]}`,
where `semantic_v0.1:24be397c…` is the hash of the string `"type:builtins.int"`.

| Affects | Example |
|---|---|
| The type of a primitive | `[1]` `S:080a9ed4`, `[1.0]` `S:c2272c0a`, `[True]` `S:1c28f2eb` |
| List order; list versus tuple versus set | `[1,2]` `S:49a64717`, `[2,1]` `S:af1a1fc1`, `(1,2)` `S:ed27331d`, `{1,2}` `S:28654306` |
| Dict keys and values; a key present with `None` versus absent | `{"a":None}` `S:d091f9c8`, `{}` `S:44136fa3` |
| The label **and** digest of every nested hash | a nested `foo_v9:…` versus `bar_v9:…` with the same digest differ |
| The label `semantic_v0.1` itself, whenever the structure holds any non-primitive leaf (a type, schema, function, bytes, another element) | `[b'a']` under another label gives a different digest |
| `0.0` versus `-0.0`; Unicode normalisation form | hashed as written |

| Does NOT affect | Why |
|---|---|
| Dict insertion order | keys are sorted |
| `set` versus `frozenset`; insertion order of a set | same tag; sorted |
| The label, for a flat structure of primitives | the label is not in the JSON |
| Which class a dict, list or `Schema` subclass is | matched through the base class |

Known irregularities (measured):

- A set holding items with the same `str()` (`{1, "1"}`) hashes differently from run to run
  (`PYTHONHASHSEED`).
- Dict keys are compared after `str()`: `{1: "x"}` and `{"1": "x"}` are the same hash.
- A tuple and the dict `{"__type__":"tuple","items":[…]}` are the same hash.
- At top level, `b'a'` and the string `"61"` are the same hash; a UUID and its 32-hex string
  are the same hash.

### A.2 Value hashers (handlers)

| Value type | Algorithm | Affects | Does NOT affect | Label |
|---|---|---|---|---|
| `bytes`, `bytearray` | lowercase hex string, then A.1 | the bytes | bytes versus bytearray | `semantic_v0.1` |
| `uuid.UUID` | its 16 bytes, then as bytes | the 128-bit value | — | `semantic_v0.1` |
| `orcapod.File` | SHA-256 over the file's bytes, read in 64 KiB chunks | file bytes | file name, directory, mtime, permissions, symlink, chunk size | `sha256` |
| `orcapod.Directory` | Merkle tree: each entry is `sha256("file\0"+name+"\0"+file digest)`, `"dir\0"+name+"\0"+subtree digest` or `"symlink\0"+name+"\0"+link target`; entries sorted by name; node = `sha256("directory\0"+entry digests)` | entry names, contents, tree position, empty subdirectories, hidden files (`.DS_Store`), symlink target text, what the ignore filter removes | root directory name and location, creation order, mtime, permissions | `merkle_sha256` |
| `numpy.ndarray` | `sha256(len(tag)‖tag‖ndim‖each dim‖nbytes‖data)`, all little-endian 64-bit; `tag` is the dtype (`int32`, `float64`, `datetime64[ns]`, `ucs4[3]`, …); data is C-order little-endian | dtype kind and width, shape, values, exact float bits (`-0.0`, NaN payload), string width, datetime unit | byte order, C or Fortran order, contiguity, subclass, struct padding; the stored layout and `{"version":1}` | `ndarray_v1` |
| `polars.Expr` | serialise, parse into Orcapod's own tree, `sha256` of its canonical JSON | column names, operators, literal types, operand order, structurally different but equivalent forms | method versus operator spelling (`col.gt(1)` versus `col > 1` versus `1 < col`) | `polars_expr_v1` |
| `pa.Table`, `pa.RecordBatch` | A.3 | see A.3 | see A.3 | `arrow_v0.1` |
| `pandas.DataFrame`, `Series` | converted to an Arrow table with the index as a column, then A.3 | values, index values and name, row order, column names, numeric dtype, Series name | column order; `int64` versus `Int64`; object, category and Arrow strings | `arrow_v0.1` |
| a type (`int`, `Path`, a user class) | the string `"type:<name>"`. For a registered logical type the name is its logical type name (`type:orcapod.path`); otherwise module + qualified name (`type:builtins.int`) | the name | **the Arrow storage type and extension metadata of the logical type** | `semantic_v0.1` |
| `list[int]`, `dict[str,int]` | `{"__type__":"generic_alias","origin":…,"args":[…]}` of nested type hashes | origin, argument order | `typing.List[int]` versus `list[int]` | `semantic_v0.1` |
| `int \| None`, `Optional[int]` | `{"__type__":"union","args":[sorted]}` | the member set | spelling and order | `semantic_v0.1` |
| `Schema` | `{field name: hash of its Python type}`, sorted by name | field names, Python types, `T` versus `T \| None` written in the type | field order; **`optional_fields`**; the Arrow type each field converts to | `semantic_v0.1` |
| a Python function (as a value) | `{"module", "name", "params", "returns"}`; `params` is text such as `a: int, b: str = 'x'` with defaults as `repr()` | module, name, parameter names, order, annotations, defaults, return annotation | **body**, docstring, closure, qualified name, `async` | `semantic_v0.1` |

Note that a function used as a pod is **not** hashed through the last row; see A.6.

### A.3 The Arrow hasher: `hash_table`

`hashing/arrow_hashers.py`, then starfix. Used for row data hashes, every row key, the
default `source_id`, and table-like values.

**Algorithm**

1. Columns of plain types are passed to starfix untouched.
2. Struct, list and extension columns are rebuilt first. A cell of an extension type that has
   a value hasher becomes the bytes `<extension name>::<label>:<digest>`; for an array that is
   `orcapod:ndarray::ndarray_v1:` + 32 bytes. Struct and list types are rebuilt from the first
   non-null row, which **drops nested nullability, nested metadata and list item names**. A
   column with no non-null row keeps its original type.
3. Metadata is ignored, except keys starting `ARROW:extension:`.
4. starfix: a schema digest (SHA-256 of JSON `{column name: {type, nullable}}`, sorted by
   name; `string` and `large_string`, `list` and `large_list`, dictionary and plain are the
   same type), then per column a validity bitmap, list lengths and the value bytes.
5. Output: 3 bytes of format number (`000001`) + SHA-256. Label `arrow_v0.1`.

| Affects | Example |
|---|---|
| Row order | `fb52c5ca` -> `51aa84ec` |
| Column names; struct field names | -> `490fa01f` |
| Integer and float width (`int64` versus `int32`) | -> `81bfc2bd` |
| The top-level nullable flag, even with no nulls | -> `10947bfc` |
| Null values and positions; `""` versus null; `[]` versus null | |
| Timestamp unit and time zone text (`UTC` versus `+00:00`); decimal precision | |
| `large_binary` versus `fixed_size_binary` holding the same bytes | `f5bc0613` -> `2e0b4a7a` |
| For an extension cell: the extension name, the value hasher's label, the value's digest | |
| On starfix 0.4 only: how a top-level `bool` column is split into chunks | `cacf9103` versus `26795810` for equal tables; fixed in starfix 0.5 (ITL-910: both `95edd95c`) |
| Nested nullability, when the column has zero rows or is all null | `fb1f61ef` versus `0853207f`; still so on ITL-910 |

| Does NOT affect | Why |
|---|---|
| Column order; struct field order | sorted by name |
| Chunking of non-bool columns; table versus record batch | streamed |
| `string` versus `large_string`, `binary` versus `large_binary`, `list` versus `large_list`, dictionary encoding | normalised by starfix |
| Ordinary field and schema metadata | ignored |
| Nested nullability, when the column has data | dropped in step 2 (`2dd40745` both ways) |

### A.4 Sources

Structure hasher over these structures:

| Source | Content identity | Pipeline identity |
|---|---|---|
| `DictSource`, `ArrowTableSource`, `DataFrameSource`, `CSVSource`, `DeltaTableSource`, database table sources | `(class name, (tag schema, data schema), source_id)` | `(tag schema, data schema)` |
| `ListSource` | `(class name, name, elements, tag function's module.qualname)` | same as above |
| `CachedSource` | the wrapped source's | the wrapped source's |
| `DerivedSource` | `(origin node's content hash,)` | from the origin's schema |
| `SourceNode` (unbound) | `("source_node", name, tag schema, data schema)` | `(tag schema, data schema)` |

Where `source_id` comes from when the user gives none:

| Source | Default `source_id` |
|---|---|
| `DictSource`, `ListSource`, `DataFrameSource`, `ArrowTableSource` | the Arrow hash of the whole table, 70 bare hex characters |
| `CSVSource` | the file path as passed |
| `DeltaTableSource` | the name of the table's directory |
| SQLite, PostgreSQL, SpiralDB table sources | the table name |

| Affects | Which hash | Example |
|---|---|---|
| `source_id` | content | `S:20630802` -> `S:1e0f359d` |
| The source class name | content | same data and id: `DictSource` `S:20630802`, `ArrowTableSource` `S:a7b3dad3` |
| Which columns are tags | both | |
| Column names; Python types | both | |
| Nullability: one `None` in a column turns `int` into `int \| None` | both | `S:ce67f306` -> `S:fa9b175f` |
| Data, row order: **only with the default table-hash `source_id`** | content | |

| Does NOT affect | Example |
|---|---|
| Data values, row count, row order, when `source_id` is given | all `S:20630802` |
| Column order | |
| Arrow width (`int32` versus `int64`) | |
| `record_id_column`, `label` | |
| CSV content and mtime | same path, different content: `S:c46011a7` both |
| Which database file or which parent directory | two SQLite files with a table `t`: same hashes |

### A.5 Operators and streams

A stream produced by an operator: `(operator, inputs)`. The inputs are an unordered set for
`Join` and `MergeJoin`, an ordered tuple for `SemiJoin` and every single-input operator. The
same structure is used for both chains; the chains differ only in how the inputs resolve.

| Operator | Its identity |
|---|---|
| `Join`, `MergeJoin`, `SemiJoin` | the class name as a string: `"Join"` -> `S:295ca933` |
| `Batch` | `(class name, batch_size, drop_partial_batch)` |
| `GroupBy` | `(class name, by)` |
| `SelectTagColumns`, `SelectDataColumns`, `DropTagColumns`, `DropDataColumns` | `(class name, columns, strict)` |
| `MapTags`, `MapData` | `(class name, name map, drop_unmapped)` |
| `PolarsFilter` | `(class name, predicates, constraints)` |
| `Index`, `Pick` | `(class name, column, index or key, out)` |

| Affects | Example |
|---|---|
| The class name | a subclass named `MyJoin`: `S:2da23cfc` |
| The constructor arguments listed above, including order of `by`, of selected columns, of predicates | `Batch(2)` `S:6e9c57aa`, `Batch(3)` `S:538df39c` |
| Input order, for `SemiJoin` | |
| Nesting of joins | `Join(Join(a,b),c)` differs from `Join(a,b,c)` |
| The container type of `columns` | `SelectDataColumns(["x"])` `S:c4b23b2f`, `(("x",))` `S:85aeda52`, `({"x"})` `S:b09918bc` |

| Does NOT affect | Example |
|---|---|
| The operator's module, implementation or version | a class in another module also named `Join`: `S:295ca933` |
| Input order, for `Join` and `MergeJoin` | |
| `label` | |
| A predicate written another way | `10 < col` equals `col > 10` |

Identity follows how the operator was written, not what it outputs: `DropDataColumns(["y"])`
and `SelectDataColumns(["x"])` on a stream with data columns `x, y` give the same rows and
different hashes.

A stream with no producer (`ArrowTableStream` built from a table): content identity is
`(class name, the whole table including system tags and source info, tag columns)`; pipeline
identity is `(tag schema, data schema)`.

### A.6 Functions and function nodes

A function pod's identity is its `uri`, for both chains:

```
('double', 'semantic_v0.1:17de3328…', 'v0', 'python.function.v0')
    │                 │                 │            │
 function name   hash of the OUTPUT   major      wrapper kind
 (or the         schema only          version
 function_name
 argument)
```

Major version: the first run of digits followed by a dot (`"v0.7"` -> `v0`, `"2.0"` -> `v2`,
`"release-10.3rc1"` -> `v10`; `"v1"` is an error). The default is `v0.0` with
`@function_pod` and `v1.0` with `FunctionPod.from_fn`.

A function node or stream: `(function pod, {input stream})`.

| Affects | Example |
|---|---|
| Function name | `S:c1a0b28b` -> `S:63deeae2` |
| Major version | `v1.0`: `S:9c246eff` |
| Output annotation; output key names | `-> float`: `S:7d6d0cf0` |
| The module of a user-defined **output type** | a dataclass `Point` in module A versus B: different |
| The upstream stream (schema for the pipeline chain; schema and source for the content chain) | |

| Does NOT affect | Example |
|---|---|
| The function body, comments, docstring, `async` | all `S:c1a0b28b` |
| Minor version | `v0.7` equals `v0.0` |
| The input schema: parameter names, annotations, defaults | the pod hash stays `S:c1a0b28b`; the node hash moves only because the upstream schema moved |
| The function's module | same source in another module: `S:c1a0b28b` |
| `label`, `pod_config`, executor, the cache wrappers, the attached database | |
| `table_scope` | changes the path only |

Stored but used for nothing: `__pf_var_function_signature_hash` (module, name, parameters,
return annotation) and `__pf_var_function_content_hash` (name, module, **full source text**,
annotations, a few code-object counts). They do change when the body changes
(`45763798` -> `3b667e66`), but no lookup reads them.

### A.7 Row-level identities

| Identity | Algorithm | Affects | Does NOT affect |
|---|---|---|---|
| **Row data hash** (`__input_data_hash`, `__output_data_hash`, `_content_hash`) | A.3 over the row's data columns only | data values, data column names, Arrow width, the nullable flag of each data column | tags, system tags, source info, `source_id`, row position, the data context key |
| **Default `source_id`** | A.3 over the whole source table (tag and data columns; columns starting with `_` are dropped), as bare hex | every value, row order, nullable flags | which columns are tags, column order |
| **Source record id** | `uuid5(namespace, "{source_id}::row_{i}")`, or `"{source_id}::{column}={value}"` with `record_id_column`. The namespace is `uuid5(NAMESPACE_URL, "https://orcapod.org/namespaces/source-record-id")` | `source_id`; row position (or the column's value) | data values, when `source_id` is given |
| **Source-info token** | `"{source_id}::{row token}::{column}"`; after a function pod `"{uri joined by ':'}::{uuid7 of the output}::{column}"` | — | not a hash |
| **Schema hash in system-tag names** | A.1 over `(tag schema, data schema)`, bare hex. Equal to the source's pipeline hash | column names, tag or data role, Python types, `T` versus `T \| None` | `source_id`, values, Arrow width, column order |
| **System-tag name after Join / MergeJoin** | append `::<pipeline hash of that input, bare hex>:<position>`; position is the rank of the input's pipeline hash | the input's pipeline hash | argument order; the join's own hash; any content hash |
| **System-tag name after Batch / GroupBy** | append `::<pipeline hash of the input stream>` | same | |
| **Folded record id** (Batch, GroupBy) | `uuid5(namespace2, "::".join(member ids as hex))` | members and their order | |
| **Folded source id** (Batch, GroupBy) | `sha256` of the members concatenated **with no separator**, bare hex | members and their order | where one member ends and the next begins: `["ab","c"]` equals `["a","bc"]` |

### A.8 Row keys of stored tables

| Key | Algorithm | Affects | Does NOT affect |
|---|---|---|---|
| **Pipeline entry id** (`__pipeline_base_entry_id`) | A.3 over a one-row table: the **user tag columns**, the system-tag columns, and `__input_data_hash` | user tag names and values; system-tag names (so the route through the operator chain) and values (so `source_id` and record ids); the row data hash | which function or node computes it; source info |
| **Pipeline-table row key** (`__record_id`) | the same table plus `__pipeline_recomputation_index` | as above, plus the recomputation count | |
| **Result-table lookup** | match on `__input_data_hash` only, inside the table at `<job>/_result/<function uri>/rdb_v1`; the newest row wins | the row data hash; function name, output schema, major version; the job name | the function body and its stored hashes; tags; `source_id`; the data context key |
| **Row filter in a shared pipeline table** | `__node_content_hash` equals the node's content hash | the node's content hash (A.4 to A.6) | data, when `source_id` is given |
| **Result-table row key** (`__record_id`), `__data_id` | random `uuid7` | nothing | not a hash |
| **Operator cache row key** (`_record_hash`) | A.3 over the full row: tags, data, source info, system tags and `__node_content_hash`, as bare hex | every column and its Arrow type | row and column order |
| **Cached-source row key** | A.3 over the source row with source info and system tags, bare hex | same | |
| **Side-effect log key** | the pipeline entry id with index 0 | as the entry id | |

Measured cache behaviour that follows from this table:

| Scenario | What happens |
|---|---|
| Same function and data, source renamed | result table unchanged (2 rows), pipeline table gains 2 rows that point at the existing results, nothing recomputed |
| Function body changed from `a + b` to `a - b`, version `v0.0` or `v0.9` | the cached `11` is returned |
| Same, version `v1.0` | recomputed: `9`, in a new table `…/v1/…` |

### A.9 Findings that bear on the design

These came out of the audit. Each was reproduced a second time.

| # | Finding | Why it matters here |
|---|---|---|
| A | **Intended (developer, November 11, 2026).** Content identity is *source identity*, not data content: with a user-given `source_id`, a changed value, an added row and reordered rows leave every stage's content hash unchanged. A source decides whether its identity reflects its exact content or is an address for it; see the source identity contract in Part 9. Rows are still told apart by the entry id (record id + row data hash). `CLAUDE.md` describes `content_hash()` as "data-inclusive, changes when data changes", which should be reworded | the system must name what the content hash covers: the pipeline bound to named sources |
| B | File and database sources default `source_id` to a name: the CSV path, the table name, the Delta directory name. Two SQLite files with a table `t`, or two Delta tables named `tbl` in different directories, have identical hashes. Under the contract this is the source's choice, but here the user stated nothing and the default address is not unique | candidate follow-up: is a bare table or directory name a good enough default address |
| C | The stored Arrow type of a column is in no structure hash. Only its Python type name is | type rules (axes II1, II2) cannot reach a path through any existing hash |
| D | An operator's identity is its class name; nothing about its behaviour or version | axis IV3 has no hook |
| E | `MergeJoin(m1, m1b)`, same `source_id` and different data, equals `MergeJoin(m1, m1)` on both hashes (`S:57f303d7`, `S:69ff5b2e`), and its **pipeline** hash differs from `MergeJoin(m1, m2)` (`S:a91c53db`) although all three inputs share one schema | the pipeline hash is not purely schema plus topology: unordered inputs are de-duplicated by content |
| F | `SelectDataColumns(["x"])`, `(("x",))` and `({"x"})` give three different hashes and so three different downstream paths | identity composition (axis I4) is uneven |
| G | The record-id system tag is `fixed_size_binary[16]` out of a source and `large_binary` after passing through a tag object. Entry ids are unaffected (always built after the conversion); the operator cache key is affected: the same row hashes differently on a direct stream and through the orchestrator | a type drift inside a hashed table |
| H | The folded source id has no separator between members | a collision by construction |
| I | Nested nullability is hashed for an empty or all-null column and dropped for a column with data, so the same schema hashes differently depending on whether data is present. `CLAUDE.md` says the nested non-null flag always matters | data-dependent hashing |
| J | On starfix 0.4 a `bool` column's chunk layout changes the table hash, and so the default `source_id`. Fixed by starfix 0.5 | already addressed by ITL-910 |
| K | The entry id covers user tag names and values; the code comments say system tags only | documentation |
| L | Function defaults enter the function-value hash as `repr()`, which is not stable for sets or objects; a set with `str()`-equal items hashes differently per process | process-dependent hashes |
| M | The structure label is inside the preimage of every nested non-primitive hash, so changing the label alone moves every real identity hash; flat structures are unaffected | a label bump on axis I1 is all-or-nothing |
| N | The default function version differs by entry point (`v0.0` versus `v1.0`) | the same function gets two identities |
| O | `ctx_arg_name` changes a pod's content hash, uri and table path but not its pipeline hash | the two chains disagree about the path |
