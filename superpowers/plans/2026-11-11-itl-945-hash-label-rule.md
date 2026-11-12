# ITL-945 hash label rule: step 1 plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply the nine-point hash label rule (ITL-938 design, section 5.6): labels
`<algorithm>_v1`, each defined once, never part of a hash input, absent from names, present on
stored hash values.

**Architecture:** Each label becomes one constant next to the algorithm it names. Every place
that feeds a nested hash into a parent hash feeds the digest only. Every place that builds a
name (path component, column name, token) uses the bare hex digest. The two bare row keys
become labelled bytes.

**Tech stack:** Python, pyarrow, starfix 0.5 (hash format 2), pytest.

**Branch:** `eywalker/itl-945-apply-the-hash-label-rule-one-format-labels`, from the head of
PR #309 (`b88f7eb3`). PR against `main`, first body line `Stacked on #309`, `Fixes ITL-945`.

**Status:** step 1 (this plan) sent to the coordinator; no library change before approval.

---

## (a) Site inventory

Method: read every use of `ContentHash.to_string`, `to_hex`, `to_prefixed_digest`, `__str__`,
`.digest`, `.method`, `display_name`, `from_string`, `from_prefixed_digest`; then ran the whole
test suite with `ContentHash.__getattribute__` and its dunder methods wrapped to record the
calling line (results in the section "Tracer results").

### A1. Label definitions today

| Label today | Becomes | Written in |
|---|---|---|
| `semantic_v0.1` | `semantic_v1` | `hashing/versioned_hashers.py:37`, `contexts/data/v0.1.json:132`, `contexts/data/schemas/context_schema.json:186` |
| `arrow_v0.2` | `arrow_v1` | `hashing/versioned_hashers.py:42`, `contexts/data/v0.1.json:141`, `contexts/data/schemas/context_schema.json:178`; also `hashing/stored_hash_labels.py:50` (`_LABEL_OF_FORMAT`) |
| `merkle_sha256` | `directory_v1` | `hashing/directory_hashers.py:215`, built as `f"merkle_{algorithm}"` |
| `ndarray_v1` | unchanged | `builtin_handlers.py:330` (`NumpyArrayHandler.HASH_LABEL`), once |
| `polars_expr_v1` | unchanged | `semantic_hashing/polars_expr.py:78` (`POLARS_EXPR_HASH_LABEL`), once |
| `sha256` (file) | unchanged, the stated exception | `hash_utils.py:173-213`: the name of the digest function the file hasher is configured with |

### A2. Hash inputs that contain a label today (rule 6)

| # | Site | Preimage today | Preimage after |
|---|---|---|---|
| 1 | `semantic_hasher.py:250` `_expand_structure`, a `ContentHash` leaf | JSON string `"<label>:<hex>"` | JSON string `"<hex>"` |
| 2 | `semantic_hasher.py:314` `_expand_element`, any non-primitive, non-container element (type, schema, function, bytes, pod, stream, value of a handler) | `"<label>:<hex>"` | `"<hex>"` |
| 3 | `builtin_handlers.py:138-152` `GenericAliasHandler`, `UnionTypeHandler` | `origin` and `args` as `"<label>:<hex>"` | `"<hex>"` |
| 4 | `builtin_handlers.py:251` `SchemaHandler` | `{field: "<label>:<hex>"}` | `{field: "<hex>"}` |
| 5 | `hashing/visitors.py:310` extension cell | `<ext name with . as :>` + `::` + `<label>:` + digest bytes | `<ext name with . as :>` + `::` + digest bytes |
| 6 | `core/nodes/function_node.py:119` entry-id preimage column `__input_data_hash` (used by `compute_base_entry_id`, `compute_pipeline_entry_id`, and the side-effect log key, `side_effects.py:234`) | `b"<arrow label>:"` + digest | digest bytes only (the stored column `__input_data_hash` stays labelled) |
| 7 | `core/nodes/operator_node.py:834-848` operator-cache row key: Arrow hash of the row including `__node_content_hash` | that column holds `b"<semantic label>:"` + digest | hash the row with that column holding the digest only; store the column labelled as today |
| 8 | `pipeline/job.py:861-866` run snapshot hash, `sha256("\n".join(leaf content hash strings))`, shown in the run URI `name@<16 hex>` | `"<label>:<hex>"` lines | `"<hex>"` lines |
| 9 | names that are themselves hash inputs: function `uri` (in the pod identity), default `DerivedSource.source_id`, `SourceNode` slot name | labelled, see A4 | bare, see A4 |

Sites 6, 7 and 8 are not in the brief or the design table. Without them, renaming the Arrow or
structure label moves every entry id, operator-cache key and run URI.

Already label-free: the directory hasher feeds `file_hash.digest` (`directory_hashers.py:96`);
record ids and folds use `uuid5` / `combine_hashes` over bare text.

### A3. Stored hash values (rule 4)

| Value | Form today | After |
|---|---|---|
| `__input_data_hash`, `__output_data_hash`, `__node_content_hash`, `__pipeline_base_entry_id`, pipeline `__record_id`, side-effect `record_id` / `record_id_hash`, `__pf_var_function_signature_hash`, `__pf_var_function_content_hash` | labelled bytes | keep |
| `_content_hash` column (`arrow_table_stream.py:223`) | labelled string | keep |
| file-hash cache value (`hash_cachers.py:317`, `postgres_hash_cacher.py:259`) | labelled bytes | keep |
| saved job JSON: node keys, edges, `content_hash`, `pipeline_hash`, `node_job_content_hashes` (`pipeline/graph.py`, `job.py`, `base.py`, `sources/base.py:125`), read back with `ContentHash.from_string` | labelled string | keep |
| observer `node_hash`, hook `pod_hash`, graph attribute `pipeline_hash` | labelled string | keep (reported values) |
| operator-cache row key `_record_hash` (`operator_node.py:847-853`) | bare hex text in `large_binary` | **add**: `to_prefixed_digest()` |
| cached-source row key (`cached_source.py:240-248`) | bare hex text in `large_binary` | **add**: `to_prefixed_digest()` |

### A4. Names (rule 5)

| Name | Site | Today | After |
|---|---|---|---|
| function uri component 2 (path, pod identity, source-info token of every function output) | `data_function.py:232` | `semantic_v0.1:<hex>` | `<hex>` |
| `schema:<pipeline hash>` | `function_node.py:358`, `operator_node.py:261`, `side_effects.py:852` | labelled | bare |
| `instance:<content hash>` | `function_node.py:360`, `operator_node.py:263` | labelled | bare |
| `node:<content hash>` | `cached_source.py:197` | labelled | bare |
| default `DerivedSource.source_id` | `derived_source.py:51` `derived:<hash>`; `function_node.py:1912`, `operator_node.py:1222` `<path>:<hash>` | labelled | bare |
| `SourceNode` slot name for a non-source stream | `source_node.py:452`, `:711` `<label>:<hash>` | labelled | bare |
| system-tag column names, operator uri, default table-hash `source_id`, list-source tag-function hash | `to_hex` already | bare | unchanged |
| `Join` input ordering key | `join.py:52` sorts by `to_string()` | labelled | `to_hex()` (no value change; same key as `merge_join.py:252`) |

### A5. Record ids (rule 7)

No change. Source record id (`uuid5`), folded record id (`uuid5`), folded source id
(`combine_hashes`), result row key and `__data_id` (`uuid7`) carry no label today.

### Tracer results

Filled in once the traced suite run finishes (see "Notes").

---

## (b) Single definition

Each label is a module-level constant in the module that implements the algorithm:

| Constant | Module |
|---|---|
| `SEMANTIC_HASH_LABEL = "semantic_v1"` | `hashing/semantic_hashing/semantic_hasher.py` |
| `ARROW_HASH_LABEL = "arrow_v1"` | `hashing/arrow_hashers.py` |
| `DIRECTORY_HASH_LABEL = "directory_v1"` | `hashing/directory_hashers.py` |
| `NumpyArrayHandler.HASH_LABEL` | unchanged |
| `POLARS_EXPR_HASH_LABEL` | unchanged |

`hasher_id` stays a constructor argument of `SemanticAwarePythonHasher` and
`StarfixArrowHasher` and now defaults to the constant. The manifest refers to a label by naming
the class in `_class`; the two `"hasher_id": ...` lines are deleted from
`contexts/data/v0.1.json`, the two literals from the examples in `context_schema.json`, and the
two constants from `versioned_hashers.py`. A test reads every shipped file under
`contexts/data/` and fails if one spells a hasher label.

Fit with ITL-944: its branch adds `contexts/data/v1.json` as a copy that still has both
`hasher_id` lines and does not touch `v0.1.json`, so there is no textual conflict. At
integration the two lines are deleted from `v1.json`; until then the test above fails on the
merged tree, which is the intended signal.

## (c) Rule 6 test

`tests/test_hashing/test_hash_label_rule.py`, public interface only. Two default contexts are
built that differ only in labels (`SemanticAwarePythonHasher(hasher_id="renamed_v9", ...)`,
`StarfixArrowHasher(hasher_id="renamed_v9", ...)`). For each, compute: structure hashes of a
nested structure with types, a schema, bytes and an ndarray; the Arrow hash of a table with an
extension-typed column; the content and pipeline hash of a source, a join, a function pod
stream; a function node's `node_identity_path`; entry ids and the operator-cache row key
through a run on an in-memory store. Assert every digest and every name equal between the two
contexts, and every label different.

## (d) Reading older values

v0.2 tables are a skipped generation. Nothing in v0.3 parses a labelled name: the only parsers
of names are `cli/migrate.py:38` (`startswith("schema:")`, prefix unchanged), `join.py:444` and
`arrow_utils.py:1574` (system-tag names, bare already), and `registry.py:214` (context keys).
`ContentHash.from_string` / `from_prefixed_digest` are applied to stored values only.

## (e) Public API

Kept unchanged: `ContentHash.to_string`, `to_hex`, `to_prefixed_digest`, `from_string`,
`from_prefixed_digest`, `display_name`, `__str__`. Behaviour changes: every label string;
`BasicDirectoryHasher` label; function `uri[1]`, paths, default derived `source_id` and slot
names are bare; `get_versioned_*` defaults come from the constants.

## (f) Golden groups expected to move

706 entries in five files. Every entry that prints a label moves at least in its label text
(382 entries). Digests move for: every structure hash with a nested non-primitive leaf, every
schema hash and system-tag column name, every path, every Arrow hash over an extension cell,
every entry id and operator-cache key. Digests that must not move: Arrow hashes of plain tables
and schemas (`arrow.json` `table.*`, `schema.*`), flat structure hashes of primitives, value
hashes of ndarray, Polars expression, file and directory. Exact counts are reported per group
in the PR after implementation.

## (g) Commit order

1. `test(hashing)`: failing tests for the rule.
2. `refactor(hashing)`: rename labels.
3. `refactor(hashing)`: single definition per label.
4. `feat(hashing)!`: labels out of hash inputs.
5. `feat(core)!`: bare digests inside names.
6. `feat(store)!`: labelled row keys for the operator cache and cached source.
7. `test(golden)`: regenerate, diff described by group.
8. `docs`: `docs/reference/hashing.md`, CHANGELOG, DESIGN_ISSUES, CLAUDE.md and `.zed/rules`.

## (h) Open points

See the question sent to the coordinator; answers are recorded under "Decisions".

## Decisions

(pending)

## Notes

- Tracer plugin: scratchpad `plug/chtrace.py`; not committed.
