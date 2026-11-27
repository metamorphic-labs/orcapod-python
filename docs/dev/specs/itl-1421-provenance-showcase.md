# Provenance showcase: audit and plan (ITL-1421)

Status: audit and plan only. Nothing under `src/` changes, and no hash, golden file or stored
layout changes. The evidence is the scripts in
[`itl-1421-provenance/scripts/`](itl-1421-provenance/scripts/). Run each from
`docs/dev/specs/itl-1421-provenance` with `uv run python -m scripts.<name>`. They use the
public API and direct reads of the stored tables. Each one prints what it proves.

**Round two (2026-11-23).** The developer's review: round one did not use all the stored
information, in particular the system tags and the tags. The intended way to trace is the
PIPELINE store for why and from what (the input entry, by its tags and system tags,
that triggered a computation), the RESULT store for which function and which computation,
and the SOURCE INFO for the ultimate sources. This revision adds the information map (1.0,
script `00`), redoes the seven scenarios on it (section 2, scripts `04` and `06` to `11`
rewritten or extended), and revises the design (3, 4). Section 6 lists what changed and
why.

| Script | What it shows |
|---|---|
| `00_information_map` | **every stored table of one run, every column, and every link between them, checked on the rows** |
| `01_operator_lineage` | the tokens and system tags every exported operator leaves on a plain stream |
| `02_function_pod_lineage` | what function pod and side-effect pod rows carry (plain stream) |
| `03_job_modes` | a job node after `run()`, a loaded job (`run(load_only=True)`), and `as_source()` |
| `04_trace_by_hand` | **scenario 1**: one cell to its source cells through pipeline entries, results and tokens; the shared-result group; live and loaded |
| `05_source_kinds` | source id, record id and token for every source kind |
| `06_forward_impact` | scenario 2: a bad input value against a bad computation |
| `07_function_changed` | scenario 3 |
| `08_manifest` | scenario 4: node manifest and per-cell entry manifest, and its check after a CSV edit |
| `09_two_pipelines` | scenario 5, node and row level |
| `10_missing_row` | scenario 6 |
| `11_new_data` | scenario 7: exact prediction at both function nodes |
| `12_explain_pr498` | what `stream.explain()` (PR 498, not on `main`) reports; run against that branch's `src` |

## 1. Audit: what per-row lineage exists

### 1.0 Information map (`00`)

One run of the tutorial job (Part 8) with every operator node given a stored cache
(`CacheMode.LOG`; a job's default `OFF` keeps operator rows in memory only). The tables are
read directly: `database.list_table_paths()`, then
`database.get_all_records(path, record_id_column="__record_id")`. `node.get_all_records()`
joins the pipeline record with its result and drops `__record_id` (the entry id),
`__pipeline_base_entry_id`, `__pipeline_recomputation_index` and `__node_content_hash`
(`00`, section 5).

Nine tables are stored; source nodes store nothing:

```
workshop/firing_rate/<out schema hash>/v0/python.function.v1/schema:<pipeline hash>/pdb_v1   64 rows  rates PIPELINE
workshop/_result/firing_rate/<out schema hash>/v0/python.function.v1/rev1/rdb_v1               63 rows  rates RESULT
workshop/mean_rate/…/schema:<pipeline hash>/pdb_v1                                              8 rows  session_means PIPELINE
workshop/_result/mean_rate/…/rev1/rdb_v1                                                        8 rows  session_means RESULT
workshop/<operator>/<operator hash>/schema:<pipeline hash>/odb_v1    one per operator node (LOG only): 8, 64, 64, 8, 8 rows
```

A pipeline or operator table is shared by every node of its pipeline hash under the job's
name; `__node_content_hash` names the node a row belongs to. Example values are from `00`.

#### Pipeline store (`pdb_v1`): one row per input entry a function node received

| Column | Type | Example | Question it answers | Links to |
|---|---|---|---|---|
| `__record_id` (the key) | `large_binary` | `arrow_v1:2a215f…` | which input entry, which attempt | hash of the input's system tags + `__input_data_hash` + `__pipeline_recomputation_index` (checked 64/64 with `compute_pipeline_entry_id`) |
| user tags (`subject_id`, `session`, `probe`, `unit`) | as the input | `mouse_01`, `s1`, `imec0`, `0` | the readable name of the input row | the input row's tags; downstream `GroupBy` keys |
| `_tag_source_id::…`, `_tag_record_id::…` (one pair per source) | `large_string` | `units`, `row_0`; after a group `group_v1:…` | **from what**: the exact source records the input row derives from | = the input row's system tags (64/64); a record id names a source row |
| `_source_<input column>` | `large_string`, a list after a group | `units::row_0::n_spikes` | the ultimate source cell of each input value | = the input row's tokens (64/64); a function output token's record part = a RESULT `__record_id` |
| `__data_id` | `large_binary` (uuid7) | `…da2f7444` | which result this entry was served | = RESULT `__record_id` (64/64; 64 entries, 63 results) |
| `__node_content_hash` | `large_binary` | `semantic_v1:21fc70…` | which node (pipeline bound to which sources) | = `node.content_hash()` |
| `__input_data_hash` | `large_binary` | `arrow_v1:cba307…` | exactly what the function received | = the input row's `_content_hash` (64/64) = RESULT `__input_data_hash` |
| `__output_data_hash` | `large_binary` | `arrow_v1:1b7873…` | the output row | = the node's output row `_content_hash` (64/64); = `as_source()`'s `_content_hash` |
| `__data_context` | `large_string` | `std:v1:default` | which hashing generation | every stored table |
| `__computed` | `bool` | `True` | did THIS entry run the function, or was it served a stored result | of the two entries sharing one result, one is True |
| `__is_ephemeral` | `bool` | `False` | is the result in an ephemeral (run-only) database | — |
| `__pipeline_base_entry_id` | `large_binary` | `arrow_v1:290f32…` | the input entry, the same for every attempt and every function version | pairs versions (`07`); predicts new work (`11`) |
| `__pipeline_recomputation_index` | `int32` | `0` | which recomputation attempt (one after a lost result) | part of `__record_id` |

#### Result store (`rdb_v1`): one row per computation

| Column | Type | Example | Question it answers | Links to |
|---|---|---|---|---|
| `__record_id` (the key) | `large_binary` (uuid7) | `…da2f7444` | which computation | = PIPELINE `__data_id`; its hex is the record part of the output token |
| `__input_data_hash` | `large_binary` | `arrow_v1:cba307…` | what it was computed from (the memo key) | = PIPELINE `__input_data_hash` |
| `__pf_var_function_name`, `_version`, `_module` | `large_string` | `firing_rate`, `v0.0`, `scripts._common` | which function | the uri in the table path |
| `__pf_var_function_signature_hash`, `__pf_var_function_content_hash` | `large_binary` | `function_content_v1:a07af5…` | which signature, which BODY | the current function's hash (`07`) |
| `__pf_var_git_hash` | `large_string` | `ed8e4570…-untracked` | which code revision | — |
| `__pf_exec_executor_type`, `_executor_info`, `_python_version`, `_extra_info` | text, list of key/value | `local`, `[]`, `3.12.16` | how it ran | — |
| `<output>` (`rate_hz`) | as declared | `2.4833…` | the value | the node's output column |
| `_source_<output>` | `large_string` | `firing_rate:<uri>::<__record_id hex>::rate_hz` | the output token | appears in downstream rows and grouped lists |
| `__pod_ts` | `timestamp[us, tz=UTC]` | `2026-11-23 18:37:22` | when it was computed | — |
| `__data_context` | `large_string` | `std:v1:default` | — | — |

#### Operator cache (`odb_v1`, `CacheMode.LOG` only): the operator's output rows

`__record_id` (hash of the row with `__node_content_hash`), the user tags, the system tags
(names and values as in memory), the data columns, `_source_<col>`, `__node_content_hash`,
`__data_context`. Each table equals the operator's in-memory rows (8/8, 64/64, 64/64, 8/8,
8/8). It says what the operator emitted; nothing about what it dropped, no time, no run.

#### In-memory node tables (`as_table(all_info=True)`)

Every node: user tags, system tags, data, `_source_<col>`, `_content_hash` (`label:hex`
text; the stored binary `__input_data_hash` / `__output_data_hash` are the same hash),
`_context_key`. Source nodes: 2 system-tag columns; after the join and below it: 4.

#### System-tag column names record the path

A name is `_tag_<source_id|record_id>::<schema hash of the source>`, then one extension per
step that combines rows. A join appends `::<pipeline hash of that INPUT>:<position>` and a
`GroupBy` appends `::<pipeline hash of its input>` (`00`, section 3):

```
units (source):  _tag_record_id::4c1a7d61…
Join, rates:     _tag_record_id::4c1a7d61…::ead6e5b5…:1        joined as input 1 = SelectDataColumns_3
GroupBy, means:  _tag_record_id::4c1a7d61…::ead6e5b5…:1::4f30609d…   then folded after 'rates'
```

So a column name says from which source and through which join input the value comes,
and each extension's hash is the pipeline hash of a node of the job (and the `schema:` part
of its stored path).

#### Link diagram

```
session_means output row ──(system tags = )── session_means PIPELINE entry ──__data_id = __record_id──▶ session_means RESULT
   (tags; folded system tags)                    │  __input_data_hash = _content_hash of ▼                (function, body, executor, time, value)
                                                 ▼
                     SelectDataColumns_2 row = GroupBy row (same system tags: row-wise)
                                                 │  GroupBy: members = 'rates' rows with the KEY TAGS;
                                                 │  refold of their system tags in member order = the group's (8/8)
                                                 ▼  list position i = member i (tokens row-aligned, 8/8)
   rates output row ──(system tags = )── rates PIPELINE entry ──__data_id = __record_id──▶ rates RESULT
                                                 │  __input_data_hash = _content_hash of ▼  _source_n_spikes, _source_duration_s
                                                 ▼                                         │
                     Join row (same system tags; tokens equal) ◀───────────────────────────┘
                                                 │  record-id system tag of each source (extended name: which join input)
                                                 ▼
                     units row_<n> / sessions row_<m>   (= the token's <source id>::<record id>)
```

#### What each store can and cannot tell

| Store | Can tell | Cannot tell |
|---|---|---|
| PIPELINE | every input entry a node received, by tags and system tags (so which source records); which result it was served; whether it computed it; which node; the input's exact hash; the history (an edited input adds an entry beside the old one, `08`) | values (only tokens); when, or in which run, an entry was recorded (F5); rows dropped before the node |
| RESULT | the value; which function, body, signature, module, git revision, executor and Python ran; when; on what input data | which input ROW (rows with equal values share a result): only through the pipeline store |
| Operator cache (LOG) | what an operator emitted, per node | what it dropped; when; which run. Off by default |
| Source info | the ultimate source cell of every value, through every operator | for a function output, which input row: the token names a result, which several entries can share |
| System tags | the exact source records of a row, through row-wise steps and joins (names record the join inputs) | a folded value is one-way: membership is checked by refolding candidate members, never read off the value |
| Tags | readable identity; a `GroupBy` group's members (its key) | uniqueness in general (they are user data) |

### 1.1 The carriers, in brief (round one; 1.0 has every column)

| Carrier | Where | Form | Survives |
|---|---|---|---|
| Source-info token | `_source_<col>`, one per data column | `<source id>::<record id>::<column>`. A function output's token is `<function uri, joined by ":">::<result uuid7 hex>::<output key>` | every operator; a function pod **replaces** it with its own (the input tokens are not in its output rows) |
| System tags | `_tag_source_id::…`, `_tag_record_id::…`, one pair per source | the record id is TEXT: `row_<n>`, or the key columns as JSON | row-wise steps unchanged; joins extend the column NAME; `Batch` and `GroupBy` FOLD the values |
| Stored function record | the PIPELINE store and the RESULT store, read directly (section 1.0); `node.get_all_records()` is a joined view of the two that drops the entry ids and `__node_content_hash` | see 1.0 | job nodes with a database only |

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
batch's position). Round two: those candidate rows carry every source's system tags, and
refolding them in member order must give the group's folded values exactly (8 of 8 groups,
`00`), so membership is proven, not assumed (section 3.3).

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
- **Memoization: one result, several pipeline entries.** Two input rows with equal values
  share one stored result: in the tutorial dataset `mouse_02 s1 imec0 unit 0` (units
  `row_16`) and `imec1 unit 2` (`row_22`). They are still two pipeline entries, each
  keyed by its own system tags, and `__computed` says which one ran the function
  (`row_16`: True, `row_22`: False; `00`). Round one called this an ambiguity. It is one
  only for a trace that follows the output token to the result and then looks for "the"
  pipeline record. A trace that goes through the pipeline entries is exact (section 2,
  scenario 1).
- **Side-effect pods** pass rows, tokens and system tags through unchanged. Their
  invocation log is keyed on system tags and the input data hash. Nothing is added to the
  row.

### 1.4 Job modes (`03`)

| Mode | Tokens and system tags | Link from output to input |
|---|---|---|
| (a) plain stream | as in 1.2 and 1.3 | by system tags within a chain; computing upstream streams to read them |
| (b) job node after `job.run()` | as in (a); held in memory | plus the pipeline and result stores (1.0) |
| (c) loaded job, `run(load_only=True)` | **identical to (b)**, for source, operator and function nodes (same columns and values) | same as (b): the stored entries and results are read, not re-minted (`04` traces the loaded job) |
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
| F5 | A pipeline entry has no time and no run id. When an entry REUSED a result (`__computed` False), nothing stored says when, or in which run, that input reached the node; `__pod_ts` is the result's compute time only | gap, decision D8 |
| F6 | After new data, a load-only run shows the OLD group rows of a group whose new member it skipped (the summary says "inputs incomplete"; `11`). A `plan()` must not read a load-only run's rows below a skipped input | note for item F |

No code bug was reproduced.


## 2. Scenarios, redone on the information map

Each procedure is written as "start from X; use column Y of table Z to find W". P marks
the pipeline store (why, from what), R the result store (which function, which
computation), S the source info (ultimate sources), T the tags and system tags.

### Scenario 1: where did this value come from? (`04`)

1. Start from the output row (`session_means`, `mouse_03 s2`, 5.059375); take its system
   tags. **P**: the entry of `session_means` whose system tags equal them (and whose
   `__node_content_hash` is the node's).
2. **R**: the result whose `__record_id` = the entry's `__data_id`: function `mean_rate
   v0.0`, body hash, executor, `__pod_ts`, value; its `__input_data_hash` = the entry's.
3. **T**: the input entry is the upstream row with the same system tags
   (`SelectDataColumns_2`, then `GroupBy`, row-wise); its `_content_hash` = the entry's
   `__input_data_hash`.
4. **T**, `GroupBy`: the members are the `rates` rows with the group's key tags
   (`subject_id`, `session`). Refolding their system tags in member order (non-key tags in
   name order) gives the group's folded system tags: membership and order are proven, and
   list position i is member i.
5. **P**, per member: the `rates` entry with the member's system tags. **R**: its result
   (`firing_rate v0.0`, body hash, value). **S**: the entry's input tokens
   `_source_n_spikes`, `_source_duration_s` name the source cells; **T**: the entry's
   record-id system tags name the same rows.

Output (abridged):

```
session_means mouse_03 s2: mean_rate_hz = 5.059375
 P session_means entry arrow_v1:f4407664ef… (by its 4 folded system tags) -> result …fc30105194, computed by this entry: True
 R result: mean_rate v0.0 body function_content_v1:39871100… ran 18:54:23.136309 on local; value 5.059375
 U input entry: the SelectDataColumns_2 / GroupBy row with those system tags; content hash = __input_data_hash: True (8 rates)
 G 8 members = 'rates' rows with subject_id='mouse_03', session='s2'; refold of their system tags = the group's: True
   [0] imec0 unit 0: P rates entry arrow_v1:0bba38eb36… -> R result …641c3efa43 rate_hz=4.7917 (computed by this entry: True; result served to 1 entry)
       S n_spikes = 575 <- units row_40
       S duration_s = 120.0 <- sessions row_5
   …
reached 9 source cells; recomputed from them: 5.059375
```

On the group with the shared result (`mouse_02 s1`) the procedure is exact:

```
   [0] imec0 unit 0: P rates entry arrow_v1:8ae1535614… -> R result …256c062e36 rate_hz=3.5667 (computed by this entry: True; result served to 2 entries)
       S n_spikes = 321 <- units row_16
   …
   [6] imec1 unit 2: P rates entry arrow_v1:8e5b6335cf… -> R result …256c062e36 rate_hz=3.5667 (computed by this entry: False; result served to 2 entries)
       S n_spikes = 321 <- units row_22
   …
reached units rows (exact, via pipeline records): ['row_16', 'row_17', 'row_18', 'row_19', 'row_20', 'row_21', 'row_22', 'row_23'] (8 distinct)
round one's token-only path reaches:            ['row_16', 'row_17', 'row_18', 'row_19', 'row_20', 'row_21', 'row_23'] (7 distinct; 2x row_16)
loaded job reaches the same source cells: mouse_03 s2 True ; mouse_02 s1 True
```

**Verdict: RECOVERABLE TODAY, EXACT**, live and on a loaded job (`run(load_only=True)`).
Round one's "one ambiguity" was a flaw of the method, not of the data. Its token-only path
was in fact wrong on `mouse_02 s1`: it reached `row_16` twice and never `row_22`.

### Scenario 2: what does this bad thing touch? (`06`)

Two questions, with different answers:

- **(a) A bad input value** (units `row_16`). Start from the record id. **T**: every row
  whose record-id system tag of source `units` is `row_16` (the column found by its
  `_tag_source_id` value, whatever its extensions): the select, the join, the `rates`
  rows. **P**: the `rates` entries with that system tag are exactly the computations it
  TRIGGERED (1 entry). **T**, `GroupBy`: the groups with that entry's key tags; refolding
  the members confirms `row_16` is in the group. **P**: the `session_means` entry with the
  group's folded system tags.
- **(b) A bad computation** (the result that `row_16`'s entry was served is suspect). **R**:
  the result `__record_id`. **P**: every entry whose `__data_id` is that result: here
  `row_16` (computed) AND `row_22` (reused). Then on as in (a) from those entries.

```
(a) rates PIPELINE store: entries triggered by it: [('mouse_02 s1 imec0 0', 'arrow_v1:8ae1535614…')]
    per node (rows): {'units': 1, 'SelectDataColumns_3': 1, 'Join': 1, 'rates': 1, 'GroupBy': 1, 'SelectDataColumns_2': 1, 'session_means': 1}
(b) rates RESULT …585041d27a <- rates PIPELINE entries served it: [('mouse_02 s1 imec0 0', 'row_16', 'computed'), ('mouse_02 s1 imec1 2', 'row_22', 'reused')]
Round one's method (tokens forward): 2 rows at rates: it includes units row_22, which (a) excludes and (b) includes.
```

**Verdict: RECOVERABLE TODAY, EXACT, and generic except for folds.** Round one said exact
impact needed a rule on every operator. The record-id system tag already follows every
row-wise operator and join (the value is kept and the name extended), so (a) is one generic
rule: match the record id under any column whose source-id value is the source. Only the
folding operators (`GroupBy`, `Batch`) need their own rule, and it is a CHECK: refold the
candidate members (key tags for `GroupBy`, position for `Batch`). Round one's token method
answered (b) while asking (a).

### Scenario 3: the function changed: what changed? (`07`)

- **R**, a body edit without a version bump: compare each result's
  `__pf_var_function_content_hash` with the current function's
  (`pod.data_function.get_function_variation_data()`). All 63 results are of the old body,
  and the run reused all of them (`0 computed, 64 from store`).
- **P**, a major bump (`v1.0`): new pipeline and result tables. Pair the two versions'
  entries by `__pipeline_base_entry_id` (system tags + input hash, no function in it);
  **R**: compare the paired results' values, versions and body hashes.

```
RESULT store: 63 results; function_content_hash differs from the current body's: 63
PIPELINE store: v0 entries 64, v1.0 entries 64, paired by base entry id: 64
e.g. mouse_01 s1 imec0 unit 0: 2.4833 (v0.0, body function_content_v1:a07af509…) -> 149.0000 (v1.0, body …b39ac8d7…)
```

**Verdict: RECOVERABLE FROM THE STORES, exact per input row.** It changed in two ways.
The pairing key is the stored base entry id, not tags plus input hash rebuilt by hand. And
the silent reuse after a body edit is detected per result by its stored body hash.

### Scenario 4: reproduce and prove a result (`08`)

- Node level: `content_hash`, `pipeline_hash`, `source_id`, uri, data context. They are
  equal on the loaded job, and they prove nothing about the data: a CSV edit moves none.
- Cell level: start from the cell; the manifest is the chain of **P** entries scenario 1
  visits (entry id, `__input_data_hash`), each with its **R** result (id, body hash,
  executor) and its **S** input tokens. To verify against a later run, for each entry find
  the current input row with the same system tags (**T**) and compare its `_content_hash`
  with the entry's `__input_data_hash`.

```
verify against the same run: OK
-- edit units row_16, run into the same store --
node hashes unchanged after the edit: True
rates PIPELINE store: 65 entries; system tags with two entries: 1 -> mouse_02 s1 imec0 0
session_means PIPELINE store: 9 entries; system tags with two entries: 1 -> mouse_02 s1
verify the old cell manifest against the new run:
  session_means entry arrow_v1:0f9446… : input now arrow_v1:2f6cf9…, was arrow_v1:593e81…
  rates entry arrow_v1:8ae153… (record ids ['row_16', 'row_2']): input now arrow_v1:4a6cee…, was arrow_v1:bc3475…
```

**Verdict: RECOVERABLE TODAY, at cell level.** The proof is the chain of pipeline entries.
After the edit, the store keeps both entries of the edited unit (same system tags, two input
hashes), and the check names exactly the two steps whose input moved.

### Scenario 5: two pipelines (`09`)

- Node level (unchanged): equal `content_hash` means a shared node; the first divergence
  is a node of B whose content hash A lacks while all its upstreams' are in A; why:
  `producer.identity_structure()`.
- Row level: **P**, a shared function node writes to the same pipeline table under the
  same `__node_content_hash`. B wrote no new entry (64 under the shared hash) and reused
  all 63 results. **T**, `GroupBy`: per group, A's members against B's (key tags, refold
  checked). The record ids only A has are in B's shared `rates` rows and missing from B's
  `Filter` rows, so the row-level divergence is the `Filter`.

```
group mouse_01 s1: A's members ['row_0', … 'row_7']; B's members ['row_0', 'row_1', 'row_2', 'row_3']
['row_4', 'row_5', 'row_6', 'row_7']: in B's rates (shared entries): True; in B's Filter: [] -> the row-level divergence is at Filter
```

**Verdict: RECOVERABLE TODAY, at node and row level.** Work is shared only under one job
name (the job name is the top of every path).

### Scenario 6: why is this row missing? (`10`)

- **P** first: no entry of the function node names the record (units `row_4`: 0, `row_48`:
  0, `row_1`: 1). That alone says the record never reached the function, from the store,
  in any later process.
- **T**: then the first node on the path whose rows lack the record id (the record-id
  system tag of that source), and that node's operator identity. For a join, the missing
  key in the other input.

**Verdict: RECOVERABLE TODAY.** Which step dropped the record needs the operators' rows:
held after a run, in an operator cache (`LOG`), or re-derived by a load-only run (no user
code). Nothing stored records a drop.

### Scenario 7: new data arrived (`11`)

- Node level: `bound_source.check_for_new_data()`, then `is_stale`.
- **P** at the first function node: a load-only run applies the operators (no user code,
  nothing written). For each input row, compute its base entry id
  (`node.compute_base_entry_id`) and look it up in `__pipeline_base_entry_id`: the misses
  are exactly what will compute.
- **T + P** past it: a group's entry is keyed by its folded system tags, which fold the
  members' record ids, known without the members' results (a function pod passes system
  tags through). Refold every group from the join's rows; a group whose fold has no entry
  in the next pipeline store will compute.

```
rates: 66 inputs; base entry id not in the PIPELINE store: 2 -> [('mouse_01', 's1', 'imec0', 4), ('mouse_02', 's2', 'imec1', 4)]
session_means: 8 groups; refolded system tags with no entry in its PIPELINE store: [('mouse_01', 's1'), ('mouse_02', 's2')]
run: rates 2 computed, 64 from store; session_means 2 computed, 6 from store
```

**Verdict: RECOVERABLE TODAY, EXACT beyond the first function node when membership
changes** (round one: exact only up to it). Not exact when a member's VALUE changes but
its record id does not (an edited row): the group keeps its folded system tags, and it
computes only if the member's new result differs. Before the run that is an upper bound.

### Summary

| # | Round one | Round two |
|---|---|---|
| 1 | recoverable, one ambiguity (shared result) | **exact** through pipeline entries; the token-only path was wrong on `mouse_02 s1` |
| 2 | exact only with a rule per operator; tokens over-include | **exact**: system tags for every row-wise step and join (generic), a refold check for `GroupBy` / `Batch`; (a) value against (b) computation |
| 3 | pair by tags + input hash | pair by stored `__pipeline_base_entry_id`; body edit flagged per result |
| 4 | needs the source rows' hashes | **cell manifest = pipeline entries**; the check names the moved steps |
| 5 | node level | node and **row level** (shared entries; members by refold) |
| 6 | recoverable on a run job | plus a store-only answer ("never reached the function") in any process |
| 7 | row level exact up to the first function node | **exact past it when membership changes**; an upper bound for value edits |

Nothing in the seven needs a new stored column or a new hash input. F5 (no time and no run
on an entry) is the one gap found (D8).

## 3. Design (revised)

### 3.1 Rules every tool obeys

- **Reading only.** Never computes a function, never calls user code, never writes, never
  polls. It reads held rows, stored tables (directly, every column) and declared structure.
  Where a node holds no run, it says so (`not_computed`) instead of computing. Operators
  may be re-applied to held or stored rows only where `run(load_only=True)` already does.
- **Three stores, three jobs.** A tool crosses a function node through its pipeline store
  (the entry, by system tags), takes what ran from its result store, and takes ultimate
  sources from the source info. It never matches a function's output token alone to
  "the" input row.
- **Order never counts.** Results are frozen and order-free: nodes in topological order,
  then by label; rows by tags; members in the operator's own member order.
- **No new hash input, no stored column** without a decision (D1, D8).
- **Protocols are internal.** One required method, only on the folding operators
  (section 3.3).
- **Refuse only what is certain.** A link that cannot be proven (a fold that does not
  refold) is reported as such, never guessed.
- **One vocabulary.** It extends `Explanation` / `Step` from ITL-1402.

### 3.2 Public API (proposed)

All return frozen dataclasses with `text()`, `__str__`, `_repr_pretty_` and an HTML
`_repr_html_` (a table plus the diagram highlight). Each takes a job node, or a job and a
node label; none takes a plain stream (decision D2).

| # | Call | Returns | Reads | Text form | Visual form |
|---|---|---|---|---|---|
| 1 | `node.trace(row: Mapping[str, Any], column: str)` | `Trace(cell, root: TraceStep)`; `TraceStep(node, entry_id, result_id, function: FunctionRun \| None, value, inputs: tuple[TraceStep, ...])`; `FunctionRun(name, version, body_hash, executor, computed_at, computed_here: bool)`; leaves `SourceCell(source_id, record_id, column, value, tags)` | held rows; each function node's pipeline and result tables; folds refolded | indented tree, as `04` prints | tree, plus the path on `job.diagram()` |
| 2 | `job.impact(source: str, record_id: str)`; `node.impact_of_result(result_id)` | `Impact(per_node: tuple[NodeImpact, ...])`; `NodeImpact(node, rows, entries)` | held rows (system tags), pipeline tables, refold for `GroupBy` / `Batch` | node, then the count and the tags of the rows | table per node; nodes shaded by count |
| 3 | `node.compare_results(other: FunctionJobNode)` | `ResultComparison(pairs, only_left, only_right, changed: tuple[ResultDiff, ...])`; plus `node.results_of_other_bodies()` | both nodes' pipeline tables (paired by base entry id) and result tables | summary line, then the changed inputs | table (tags, old, new) |
| 4 | `node.manifest(row)`, `Manifest.verify(job) -> Verification` | `Manifest(nodes, entries: tuple[EntryRecord, ...], data_context)`; it saves to JSON | the entries of the cell's trace | one line per entry; OK, or the steps whose input moved | table |
| 5 | `job.compare(other: PipelineJob)` | `PipelineComparison(shared, diverging: tuple[Divergence, ...], shared_entries: int)`; `Divergence(node, other_node, why: Step)` | `content_hash`, `upstreams`, operator identity, pipeline tables | shared and divergent nodes, with why | both diagrams; shared nodes grey, first divergence red |
| 6 | `node.why_missing(source: str, record_id: str)` | `Absence(reached_function: bool, present_in, removed_by: Step \| None, because: str)` | the pipeline store first; held rows of the nodes on the path; `explain()`'s `Step` | `never reached rates; removed by Filter[probe == 'imec0']` | path highlighted to the removing step |
| 7 | `job.plan()` | `RunPlan(nodes: tuple[NodePlan, ...])`; `NodePlan(node, will_run, stale_because, new_entries: int, exact: bool)` | `check_for_new_data()`, `is_stale`, base entry ids against the pipeline store, refolds | one line per node | stale nodes highlighted |

`job.diagram(highlight=Highlight(nodes=…, edges=…, note=…))` is the one visual hook, drawn
by the existing SVG writer (`pipeline/diagram.py`); no Graphviz, no matplotlib.

One reader underlies all seven: a typed view of a node's stored tables with every column
(the entry id, the base entry id, `__node_content_hash`), filtered to the node's own rows.
`get_all_records()` stays as it is; the reader is internal.

### 3.3 The one new internal rule: membership of a folded row

Round one proposed a `lineage_rule` on every operator. The information map shows it is not
needed. The record-id system tags follow every row-wise operator and every join (value
kept, name extended), so "the rows that derive from record R of source X" is one generic
match. Function nodes are crossed through their pipeline store. Only the two folding
operators lose the record ids from the row, so only they declare a method:

| Operator | `fold_members(group_row, input_rows) -> tuple[row, ...]` |
|---|---|
| `GroupBy` | the input rows with the group's key tags, in `member_order`; the caller checks the refold equals the group's folded system tags |
| `Batch` | the input rows at positions `[i·n, (i+1)·n)`; checked the same way |

The refold check makes the rule safe: a wrong member set cannot pass it.

### 3.4 Effort, dependencies, workshop subset

Workshop: Wednesday 2026-11-25. One working day (Monday and Tuesday, with review) is
realistic for about 10 to 14 agent-hours of implementation.

| Item | Depends on | Agent-hours | Workshop |
|---|---|---|---|
| R. The stored-table reader (every column, own rows) | — | 2 | **yes** |
| A. `Trace` types and `node.trace()` (1), exact through pipeline entries, with `GroupBy` / `Batch` `fold_members` | R | 6 | **yes** |
| B. `diagram(highlight=…)` | — | 3 | **yes** |
| C. `node.why_missing()` (6), store answer first, reusing `Step` | R; PR 498 merged | 3 | **yes** |
| D. `job.compare()` (5), with shared entries | R, B | 3 | stretch |
| E. `node.manifest()` and `verify` (4) | A | 3 | after |
| F. `job.plan()` (7), base entry ids and refolds | R; `node_run.plan_run` | 4 | after |
| G. `node.compare_results()` and the body-hash flag (3) | R | 3 | after |
| H. `job.impact()` (2): generic system-tag match plus `fold_members` | A | 4 | after (was 10) |
| I. `as_source()` carries the stored output tokens (D3) | decision | 2 | after, if D3 is yes |

Order after the workshop: E, F, G, H.

### 3.5 Five-minute demo on the tutorial dataset

1. Run the Part 8 pipeline. Point at `session_means` for `mouse_03 s2`, 5.06 Hz: "where
   does this number come from?" `node.trace(...)` prints the tree. The mean's entry and its
   result say `mean_rate v0.0`, which body and when. Then the 8 member entries, each with
   its `firing_rate` result, then 8 `units.csv` rows and 1 `sessions.csv` row. The diagram
   lights up the path (A, B).
2. Recompute the cell from the leaves by hand: 5.059375. Load the job from its file and run
   `load_only`: the same tree with no computation.
3. Trace `mouse_02 s1`: two units have equal values, so one stored result serves both. The
   tree shows both rows with "computed here" against "reused", one result and two
   entries: exact.
4. Add `filter(probe == "imec0")` and ask about `row_4`: `why_missing` answers "never
   reached rates; removed by Filter: probe == 'imec0'" (C).
5. Stretch: compare the two pipelines. The rates entries are shared (64 from store) and the
   filter is the first divergence (D).

## 4. Decisions for the developer

| # | Decision | Recommendation |
|---|---|---|
| D1 | Store anything new for provenance (dropped-row records, member record ids beside the folds)? | **No.** Every scenario is recoverable without it. A fold's members are proven by refolding |
| D2 | Scope of the tools: job nodes only, or plain streams too? | **Job nodes only** at first. A plain stream has no pipeline or result store, and its function tokens live only in memory |
| D3 | Should plain `as_source()` carry the stored output tokens instead of null (F1)? | **Yes.** A presentation change (no hash, no layout); its own issue |
| D4 | Names: `trace` / `why_missing` / `impact` / `plan` / `compare` / `manifest`, on node and job, frozen result types in `orcapod.types` beside `Explanation` | Accept, or rename before the workshop: these become public API |
| D5 | A shared memoized result | **Settled by round two: not an ambiguity.** A trace through pipeline entries is exact. The tool shows "computed here" or "reused" per entry (`__computed`), and `impact_of_result` answers the bad-computation question |
| D6 | Workshop subset | **R + A + B + C** (about 14 agent-hours), D as a stretch |
| D7 | Fix the tutorial wording F2 and F3 now? | Yes, as a small doc PR (a separate issue) |
| D8 | F5: record when (and in which run) an entry was recorded, for entries that reused a result? | **Not for the workshop.** It is a stored column (a layout change, `pdb_v2`). Decide with the run-history design. Until then the tools show the result's `__pod_ts` and say whether the entry computed it |

## 5. Explicitly later

- Forward impact (H) and the full plan (F) after the workshop.
- A value edit under an unchanged record id: past the first function node, whether a group
  recomputes depends on the new result; before the run it is an upper bound.
- Lineage across stages joined through `as_source()` (needs D3, and ITL-1127 for binding).
- A body edit without a version bump (scenario 3) is reused without a check (ITL-1253). A
  tool flags it from `__pf_var_function_content_hash`; the policy is ITL-1253's.
- `PollingSource` and SpiralDB / PostgreSQL sources were not exercised (they need an
  implementation or infrastructure). They follow the same token and system-tag rules.

## 6. What changed in round two, and why

The developer's review (2026-11-23): the analyses did not use all the stored information,
in particular the system tags and the tags.

- **Method.** Round one read `get_all_records()` and followed source-info tokens. Round two
  reads every stored table directly (1.0) and crosses each function node through its
  PIPELINE entry (system tags), not through its output token. Every link the procedures
  use is checked on the rows (`00`).
- **Scenario 1.** The "shared memoized result" ambiguity is gone: two entries, one result,
  each entry keyed by its own system tags; `__computed` says which ran. Round one's
  token-only path was wrong on `mouse_02 s1` (it reached `row_16` twice, never `row_22`).
  D5 changes accordingly.
- **Scenario 2.** Split into a bad value (system tags) and a bad computation (result to
  entries). The per-operator `lineage_rule` (old 3.3, 10 hours) shrinks to `fold_members`
  on `GroupBy` and `Batch`, checked by refolding (H: 4 hours).
- **Scenarios 3 and 4.** Pair versions by the stored base entry id; flag old bodies by the
  stored body hash. The manifest is the chain of pipeline entries, and its check names the
  steps whose input moved.
- **Scenario 5.** A row-level comparison: shared entries, and per group the members of each
  pipeline.
- **Scenario 6.** A store-only first answer ("never reached the function").
- **Scenario 7.** Exact past the first function node when membership changes (refolded
  system tags against the next pipeline store).
- **Kept from round one:** sections 1.1 to 1.6 (what each operator, pod, mode and source
  carries), F1 to F4, D1 to D4, D7, and the reading-only rules. **New:** 1.0, F5, F6, D8,
  item R.
