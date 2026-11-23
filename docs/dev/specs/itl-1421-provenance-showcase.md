# Provenance showcase: audit and plan (ITL-1421)

Status: audit and plan only. Nothing under `src/` changes, and no hash, golden file or stored
layout changes. The evidence is the scripts in
[`itl-1421-provenance/scripts/`](itl-1421-provenance/scripts/). Run each from
`docs/dev/specs/itl-1421-provenance` with `uv run python -m scripts.<name>`. They use the
public API only. Each one prints what it proves.

| Script | What it shows |
|---|---|
| `01_operator_lineage` | the tokens and system tags every exported operator leaves on a plain stream |
| `02_function_pod_lineage` | what function pod and side-effect pod rows carry (plain stream) |
| `03_job_modes` | a job node after `run()`, a loaded job (`run(load_only=True)`), and `as_source()` |
| `04_trace_by_hand` | **scenario 1, by hand**: one output cell traced to its source cells, live and loaded |
| `05_source_kinds` | source id, record id and token for every source kind |
| `06_forward_impact` | scenario 2 |
| `07_function_changed` | scenario 3 |
| `08_manifest` | scenario 4 |
| `09_two_pipelines` | scenario 5 |
| `10_missing_row` | scenario 6 |
| `11_new_data` | scenario 7 |
| `12_explain_pr498` | what `stream.explain()` (PR 498, not on `main`) reports; run against that branch's `src` |

## 1. Audit: what per-row lineage exists

### 1.1 The three carriers

| Carrier | Where | Form | Survives |
|---|---|---|---|
| Source-info token | `_source_<col>`, one per data column | `<source id>::<record id>::<column>`. A function output's token is `<function uri, joined by ":">::<result uuid7 hex>::<output key>` | every operator; a function pod **replaces** it with its own (the input tokens are not in its output rows) |
| System tags | `_tag_source_id::…`, `_tag_record_id::…`, one pair per source | the record id is TEXT: `row_<n>`, or the key columns as JSON | row-wise steps unchanged; joins extend the column NAME; `Batch` and `GroupBy` FOLD the values |
| Stored function record | `node.get_all_records()` (pipeline record joined with result) | tags, system tags, the **input** columns' tokens, `__data_id` (= the output token's record id), `__input_data_hash`, `__output_data_hash`, `__pf_var_*` / `__pf_exec_*`, `__pod_ts`, `__computed` | job nodes with a database only |

### 1.2 Per operator, on a plain stream (`01`)

| Operator | Tokens | System tags | Dropped rows |
|---|---|---|---|
| `SelectTagColumns`, `SelectDataColumns`, `DropTagColumns`, `DropDataColumns`, `SelectColumns`, `DropColumns`, `MapTags`, `MapData` and `MapColumns` (the token keeps the original column name under the new `_source_<new>`), `AddTags`, `TagBy` | pass through (a dropped column's token goes with it) | unchanged (name-preserving) | — |
| `Pick` | the picked value's token extends the column part with the key: `stats::row_0::stats['a']` (a resolver must read that path) | unchanged | a row whose key is missing is skipped, leaving nothing behind |
| `Index` | element `i` of a list column takes element `i` of its token list (after `Batch`: the member's own scalar token) | unchanged | as `Pick` |
| `Filter`, `PolarsFilter`, `DropNulls` | pass through for kept rows | unchanged | **nothing left behind**: the result is an ordinary table of the kept rows |
| `SemiJoin` | left's pass through | left's, unchanged | nothing left behind |
| `Join` | pass through from each input | both inputs' pairs; the names gain `::<pipeline hash>:<position>`, and are equal in both argument orders | nothing left behind (inner join) |
| `Rejoin` | pass through | the origin row's, unchanged | nothing left behind |
| `MergeJoin` | colliding column: `large_list` `[first, second]` | both pairs, extended, ordered by argument | nothing left behind |
| `Batch`, `GroupBy` | `large_list` of member tokens, row-aligned with the values; nested one level per further aggregation (`large_list<large_list<…>>`) | **folded**: `batch_v1:<hex>` / `group_v1:<hex>`, name gains `::<pipeline hash>` | `GroupBy` drops non-key tags |

**What a fold loses.** The digest is one-way: a grouped row no longer names its member
record ids. The member tokens of every data column still name the members, one per member,
in member order. Members of a source **none of whose data columns survive** are named by
nothing in the row. Example: after `Join(units, sessions)` and `select(["n_spikes"])`, a
group's `sessions` members are only in the digest. They can be found again only by
re-reading the input of the `GroupBy` and selecting the rows with the group's key (or the
batch's position). That needs the operator's own rule (section 2.2).

### 1.3 Function pods, side-effect pods (`02`, `03`)

- **Output tokens name the RESULT, not the input row.** On a plain `FunctionPodStream` the
  uuid7 is minted per computation. It stays the same when that stream is read again, a
  second stream over the same inputs gets a new one, and it is stored nowhere.
- **Output row to input row:** the input row's tags and system tags pass through
  unchanged, so on a plain stream the output row matches its input row by its system tags.
  No input data hash and no `__node_content_hash` is in any stream row.
- **In a job,** `get_all_records()` is the one place where an output token
  (`__data_id`) meets its inputs' tokens. The pipeline record's `__node_content_hash` and
  entry ids are dropped from that view on purpose. The view also shows a duplicate
  `__input_data_hash_right`, an artifact of the join (cosmetic).
- **Memoization makes the link one-to-many.** Two input rows with equal values share one
  stored result, so its token is pointed at by both pipeline records. In the tutorial
  dataset this happens inside one group: `mouse_02 s1 imec0 unit 0` and `imec1 unit 2`,
  `04`. The group key cannot tell the two apart; the position in the grouped row's token
  list, under `GroupBy`'s member order, can.
- **Side-effect pods** pass rows, tokens and system tags through unchanged. Their
  invocation log is keyed on system tags and the input data hash. Nothing is added to the
  row.

### 1.4 Job modes (`03`)

| Mode | Tokens and system tags | Link from output to input |
|---|---|---|
| (a) plain stream | as in 1.2 and 1.3 | by system tags within a chain; computing upstream streams to read them |
| (b) job node after `job.run()` | as in (a); held in memory | plus `get_all_records()` |
| (c) loaded job, `run(load_only=True)` | **identical to (b)**, for source, operator and function nodes (same columns and values) | same as (b): the stored results are read, not re-minted |
| (d) `node.as_source()` (plain `DerivedSource`) | **no system tags; the output tokens are NULL** | cut. Re-joinable: tags + `_content_hash` equal tags + `__output_data_hash` of `get_all_records()` (8 of 8 rows in `03`) |
| `DerivedSource(tags_as_data=…, source_id=…)` | a fresh root: `<given id>::row_<n>::<col>` (`05`) | cut by design (a new root) |
| `CachedSource` | its source's tokens and id, unchanged (`05`) | — |

### 1.5 Sources (`05`)

Every kind follows one rule, `<source id>::<record id>::<column>`: `ArrowTableSource`,
`DictSource`, `ListSource`, `DataFrameSource`, `CSVSource`, `DeltaTableSource`,
`SQLiteTableSource` (the primary key by default), `CachedSource`, and
`DerivedSource(tags_as_data)`. `PollingSource` was not run (it needs an implementation);
it requires key columns. A token resolves to its source row through the source id and the
record-id system tag (`SourceJobNode.bound_source.source_id`). With key columns the record
id is readable (`{"subject":"m1","session":"s1"}`); without them it is the position, so
editing or reordering the file moves it.

### 1.6 What `explain()` (PR 498) reports (`12`)

For each source a stream reads, `explain()` reports what the source applies itself
(`where`, `limit`) and which steps above it drop rows (`subject='m1' (Filter)`,
`rows without a match in 'sessions' are dropped (Join)`). It describes the pipeline, never a
row. It is the vocabulary scenario 6 reuses (`Step`).

### 1.7 Findings to report (none fixed here)

| # | Finding | Kind |
|---|---|---|
| F1 | Plain `as_source()` presents `_source_<col>` as null and no system tags: a function node's stored output tokens (`_source_<key>` in the result table) are not carried | design gap: decision D3 |
| F2 | Tutorial Part 8 says the record-id system tags "hold compact binary identifiers … not meant to be read by eye"; they are text (`row_5`, key JSON) since ITL-1154 | doc out of date |
| F3 | Tutorial Part 8's "Not yet" box says a stored joined result is refused because "its rows still carry the provenance of both joined sources"; `as_source()` rows carry no system tags at all (`03`) | doc wording to check (the refusal itself, ITL-1127, stands) |
| F4 | `get_all_records()` shows `__input_data_hash_right` beside `__input_data_hash` | cosmetic |

No code bug was reproduced.

## 2. Verdict per scenario

| # | Scenario | Verdict | Evidence |
|---|---|---|---|
| 1 | Where did this value come from? | **RECOVERABLE TODAY** (job node, live or loaded) through `as_table(columns=…)`, `get_all_records()`, `node_uri`, `bound_source.source_id`: a cell, then `mean_rate` result, then 8 `firing_rate` results, then 9 source cells, re-verified by recomputing the mean. One ambiguity: a shared memoized result (resolved by member position, 2.2) | `04` |
| 2 | What does this bad input touch? | **RECOVERABLE TODAY, exactly, only with per-operator rules** (system tags through row-wise steps, the key for `GroupBy`, folded tags after it). Generic token matching through stored records works but over-includes rows that share a memoized result | `06` |
| 3 | The function changed: what changed? | **RECOVERABLE FROM STORED TABLES** (`get_all_records()` of two versions, paired by tags + `__input_data_hash`). A body edit without a major bump is reused silently; only `__pf_var_function_content_hash` shows it | `07` |
| 4 | Reproduce and prove a result | **RECOVERABLE TODAY** (node hashes, ids, uri, data context, row hashes; equal on the loaded job). Node hashes alone prove nothing about the data: a source hash is an address (an edited CSV value moves no node hash). The proof needs the row hashes of the traced source records (needs 1) | `08` |
| 5 | Two pipelines: shared work, first divergence | **RECOVERABLE TODAY** (equal `content_hash` per node; first divergence = new hash over known upstreams; why = `producer.identity_structure()`). Work is shared only under one job name | `09` |
| 6 | Why is this row missing? | **RECOVERABLE TODAY** on a run job (first node whose rows lack the record id, plus that operator's identity; for a join, the missing key in the other input). Nothing is stored about dropped rows; it is recomputed from the held or loaded node tables (operators re-run in `load_only`, no user code) | `10`, `12` |
| 7 | New data: what is stale, what would run? | **Node level: RECOVERABLE TODAY** (`bound_source.check_for_new_data()`, then `is_stale`). **Row level: exact only up to the first function node with new inputs**; deeper needs those results (computing). The run summary confirms the prediction (2 computed, 64 from store; 2 groups recomputed) | `11` |

Nothing in the seven needs a new stored column or a new hash input. F1 (D3) changes what
`as_source()` presents, not what is stored.

## 3. Design

### 3.1 Rules every tool obeys

- **Reading only.** Never computes a function, never calls user code, never writes, never
  polls. It reads held rows, stored tables and declared structure. Where a node holds no
  run, it says so (`not_computed`) instead of computing. Operators may be re-applied to
  held or stored rows only where `run(load_only=True)` already does that.
- **Order never counts.** The results are frozen and order-free: nodes in topological
  order, then by label; rows by tags; members in the operator's own member order.
- **No new hash input, no stored column** without a decision (D1).
- **Protocols are internal.** One required method per operator,
  `lineage_rule(...)` (section 3.3); no optional capability and no fallback.
- **Refuse only what is certain.** An ambiguous link (a shared result) is reported with
  every candidate, never guessed.
- **One vocabulary.** It extends `Explanation` / `Step` from ITL-1402: a `Step` names an
  operator, its label and its text in the same words.

### 3.2 Public API (proposed)

All return frozen dataclasses with `text()`, `__str__`, `_repr_pretty_` and an HTML
`_repr_html_` (a table plus the diagram highlight). Each takes a job node, or a job and a
node label; none takes a plain stream (decision D2).

| # | Call | Returns | Reads | Text form | Visual form |
|---|---|---|---|---|---|
| 1 | `node.trace(row: Mapping[str, Any], column: str)` | `Trace(cell, root: TraceStep)`; `TraceStep(node, column, value, record: str, inputs: tuple[TraceStep, ...], candidates: int)`; the leaves are `SourceCell(source_id, record_id, column, value, tags)` | held rows, `get_all_records()` of each function node on the way | indented tree, as `04` prints | tree, plus highlighted nodes and edges on `job.diagram()` |
| 2 | `job.impact(source: str, record_id: str)` | `Impact(per_node: tuple[NodeImpact, ...])`; `NodeImpact(node, rows: tuple[Mapping, ...])` | held rows, the per-operator rule | node, then the count and the tags of the rows | table per node; nodes shaded by count on the diagram |
| 3 | `node.compare_results(other: FunctionJobNode)` | `ResultComparison(pairs, only_left, only_right, changed: tuple[ResultDiff, ...])` | `get_all_records()` of both | summary line, then the changed inputs | table (tags, old, new) |
| 4 | `job.manifest(node_label)`, `Manifest.verify(job) -> Verification` | `Manifest(nodes: tuple[NodeEntry, ...], source_rows: tuple[SourceCell, ...], data_context)`; it saves to JSON | hashes, ids, uri, and the source rows of the node's traces | one line per node; then OK, or the mismatches | table |
| 5 | `job.compare(other: PipelineJob)` | `PipelineComparison(shared: tuple[(str, str)], diverging: tuple[Divergence, ...])`; `Divergence(node, other_node, why: Step)` | `content_hash`, `upstreams`, operator identity | shared and divergent nodes, with why | both diagrams; shared nodes grey, first divergence red |
| 6 | `node.why_missing(source: str, record_id: str)` | `Absence(present_in, removed_by: Step \| None, because: str)` | held rows of the nodes on the path; `explain()`'s `Step` | `present in X, removed by Filter[probe == 'imec0']` | path highlighted to the removing step |
| 7 | `job.plan()` | `RunPlan(nodes: tuple[NodePlan, ...])`; `NodePlan(node, will_run, stale_because, new_inputs: int \| None)`, `None` past the first function node with new inputs | `check_for_new_data()` (moves only the source's modified time), `is_stale`, record ids | one line per node | stale nodes highlighted |

`job.diagram(highlight=Highlight(nodes=…, edges=…, note=…))` is the one visual hook, drawn
by the existing SVG writer (`pipeline/diagram.py`); no Graphviz, no matplotlib.

### 3.3 The one new internal rule: how lineage crosses an operator

Tracing backward (1) works on tokens and stored records alone, except for a memoized
result shared by several input rows. Exact forward impact (2), why-missing (6) and the
plan (7) need to know how a row of an operator's output relates to its input rows. Each
operator declares it, as a required method beside `explain`'s:

| Kind | Rule |
|---|---|
| row-wise, filters, `SemiJoin`, `Rejoin` | same system-tag values |
| `Join`, `MergeJoin` | the input's system-tag pair under its extended name |
| `GroupBy` | the key tags (its `by`); members in `member_order` |
| `Batch` | position: batch `i` holds input rows `[i·n, (i+1)·n)` |
| function pod | system tags pass through; the stored record links tokens |

### 3.4 Effort, dependencies, workshop subset

Workshop: Wednesday 2026-11-25. One working day (Monday and Tuesday, with review) is
realistic for about 10 to 14 agent-hours of implementation.

| Item | Depends on | Agent-hours | Workshop |
|---|---|---|---|
| A. `Trace` result types and `node.trace()` (1), the shared-result candidates included | — | 6 | **yes** |
| B. `diagram(highlight=…)` | — | 3 | **yes** |
| C. `node.why_missing()` (6), reusing `Step` | PR 498 merged; the row-presence walk from A | 4 | **yes** |
| D. `job.compare()` (5) | B for the visual | 3 | stretch |
| E. `job.manifest()` and `verify` (4) | A (source rows) | 4 | after |
| F. `job.plan()` (7) | `node_run.plan_run` exists internally | 4 | after |
| G. `node.compare_results()` (3) | — | 3 | after |
| H. `lineage_rule` on every operator, then `job.impact()` (2) | the protocol method on all 22 operators, several-item tests | 10 | after |
| I. `as_source()` carries the stored output tokens (D3) | decision | 2 | after, if D3 is yes |

Order after the workshop: E, F, G, then H (H is the largest and changes every operator).

### 3.5 Five-minute demo on the tutorial dataset

1. Run the Part 8 pipeline. Point at `session_means` for `mouse_03 s2`, 5.06 Hz:
   "where does this number come from?" `node.trace(...)` prints the tree: the mean, then
   8 firing rates, then 8 `units.csv` rows and 1 `sessions.csv` row. The diagram lights up
   the path (A, B).
2. Recompute the cell from the leaves by hand: the same 5.059375. Close the kernel, load
   the job from its file, run `load_only`: the same tree with no computation (A on a loaded
   job).
3. Add `filter(probe == "imec0")` and ask about `row_4`: `why_missing` answers "removed by
   Filter: probe == 'imec0'". A unit of `mouse_04`, with sessions restricted, is removed by
   the join: there is no `mouse_04 s1` row in sessions (C).
4. Stretch: compare the two pipelines. The rates are shared (64 from store) and the filter
   is the first divergence (D).

## 4. Decisions for the developer

| # | Decision | Recommendation |
|---|---|---|
| D1 | Store anything new for provenance (dropped-row records, member record ids beside the folds)? | **No.** Every scenario is recoverable without it; dropped rows are re-derived from held or loaded rows. Revisit only if (6) must work without re-applying operators |
| D2 | Scope of the tools: job nodes only, or plain streams too? | **Job nodes only** at first. A plain stream would have to compute its upstreams to answer, and its function tokens live only in memory |
| D3 | Should plain `as_source()` carry the stored output tokens instead of null (F1)? | **Yes.** It is a presentation change (no hash, no layout) that keeps the chain readable across stages. It needs its own issue |
| D4 | Names: `trace` / `why_missing` / `impact` / `plan` / `compare` / `manifest`, on node and job, frozen result types in `orcapod.types` beside `Explanation` | Accept, or rename before the workshop: these become public API |
| D5 | A shared memoized result: resolve by member position (exact, uses `GroupBy`'s and `Batch`'s rules) or list the candidates? | **List the candidates** for the workshop; resolve exactly once `lineage_rule` (H) exists |
| D6 | Workshop subset | **A + B + C** (about 13 agent-hours), D as a stretch |
| D7 | Fix the tutorial wording F2 and F3 now? | Yes, as a small doc PR (a separate issue) |

## 5. Explicitly later

- Forward impact across every operator (H), and resolving shared results exactly through
  it.
- Per-row prediction of a run beyond the first function node with new inputs: it needs
  results that are not computed yet, so it cannot be exact without computing.
- Lineage across stages joined through `as_source()` (needs D3, and ITL-1127 for binding).
- A body edit without a version bump (scenario 3) is reused without a check (ITL-1253). A
  tool can flag it from `__pf_var_function_content_hash`, but the policy is ITL-1253's.
- `PollingSource` and SpiralDB / PostgreSQL sources were not exercised (they need an
  implementation or infrastructure). They follow the same token rule.
