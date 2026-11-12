# ITL-939 design spike: a value that is both a tag and data

**Status:** proposal, awaiting the developer's decisions (section 6).
**Issue:** ITL-939. **Related:** ITL-929 (record ids from primary keys), ITL-938 (versioning).
**Date:** November 11, 2026.
**Experiments:** `superpowers/specs/itl-939-experiments/` (each `expNN_*.py` has its captured
output next to it as `expNN_*.out.txt`; rerun with `uv run python <script>`).

## 1. Summary

* **Today** a name cannot be in both groups, but nothing checks for it when the graph is
  built. `Join`, `MergeJoin` and a function pod whose output key equals a tag name all
  predict a stream with the name in both groups. That stream then fails when it is
  materialised, with a raw `KeyError` or `ValueError`, or (for the pod) yields diverging
  values through `iter_data()`.
* **The need is real but narrow.** In the 27 EDI tables, the key that a pod wants per row as
  data is essentially one column: `session_start_datetime` (a key in 20 tables). It is needed
  for session-level time arithmetic: session duration, experiment onset, age at session,
  days since implant. Index and `timestamp` keys mostly reach pods after a `GroupBy`, which
  already turns non-key tags into list-valued data. Hash keys are never inputs.
* **Workarounds.** Of today's options only one is correct: copy the column under another name
  before building the source (one line). Choosing `tag_columns` so the column stays data, or
  reading the table twice and joining, gives silently wrong joins: 9 rows instead of 5 on the
  sample. For a `SpiralDBTableSource` the correct recipe means leaving the DB source for an
  in-memory `ArrowTableSource`, which loses save and reload.
* **Recommendation:** the smallest safe feature is design C. It is an operator,
  `CopyTagsToData`, that copies named tags into the data group under new names. The tags stay
  where they are. There is no new column model, no change to any existing hash, path or
  stored schema, and it fits the operator boundary: operators may read tags, and `GroupBy`
  already moves tags into data. It is about a day of work with tests. If the developer would
  rather wait, the one-line recipe (section 6.2) is enough for the workshop on in-memory
  sources.
* **Not recommended:** a column that belongs to both groups (A), or two same-named columns
  (B). Both touch every operator and every stored table, and B turns today's crash on
  `Join(penetration, probe)` into a silently wrong cross product. Also not recommended: pods
  that read tags (D). The result cache keys on the input data hash only, so such a pod
  would get wrong cache hits.
* **Related defects found** (outside this issue, see section 7): `Join`/`MergeJoin` miss a
  tag-vs-data name collision across inputs. A pod output key may equal a tag name. Both
  surface as late, unclear errors. The first one hits a natural EDI join (`penetration` with
  `probe` on `probe_id`).

## 2. What happens today (experiments 01, 02 and 04)

Fixtures: EDI-shaped `session` (keys `subject_id`, `session_start_datetime`) and `experiment`
(keys `subject_id`, `session_start_datetime`, `experiment_index`; data
`experiment_start_timestamp` as u64 microseconds, `task_id`). Subject `m1` has two sessions,
each with `experiment_index` 0 and 1, so `experiment_index` alone does not identify an
experiment, as in the real tables. All timestamps are UTC: a naive `datetime` returned by a pod
is rejected by the strict timezone policy.

### 2.1 Where the one-name-one-group rule is enforced (exp01)

| Probe | Result | Where |
|---|---|---|
| Arrow table with the field name twice, the name in `tag_columns` | `KeyError: Field "session_start_datetime" exists 2 times` | `SourceStreamBuilder.build` (`stream_builder.py:185`) |
| `MapData` renames a data column onto a tag name | `InputValidationError`, clear message | `MapData.validate_unary_input` |
| `MapTags` renames a tag onto a data name | `InputValidationError`, clear message | `MapTags.validate_unary_input` |
| Pod output key equals a tag name: `keys()` | **accepted**: `(('subject_id','session_start_datetime'), ('session_start_datetime',))` | none |
| ... `as_table()` | `ValueError: Duplicate column name` | `arrow_utils.hstack_tables` |
| ... `iter_data()` | **works, values differ**: tag is the start time, data is the end time | none |
| `Join(A[tag x], B[data x])`: `keys()`, `output_schema()` | **accepted**, `x` in both groups | none (`Join` checks only data-vs-data) |
| ... `as_table()` and `iter_data()` | `KeyError: Field "x" exists 2 times` | `arrow_utils.split_by_column_groups` |
| `MergeJoin(A, B)` same shapes | same as `Join` | same |
| `SemiJoin(A, B)` | works (B's data never reaches the output) | |
| A pod parameter named after a tag | `ValueError: required parameter ... has no matching column` | pod input validation |

So the rule exists only physically: the Arrow table cannot hold the name twice. The `Datagram`
pair (`Tag`, `Data`) can already hold it, as the pod probe shows. No validator stops it at
graph-build time.

### 2.2 Every way to get a tag value into a pod today (exp02)

The task: elapsed seconds from session start to experiment start, then `Join` with `session` on
the shared keys. The correct answer has 5 rows.

| | Recipe | Extra code | Join with `session` | Hash and provenance consequences |
|---|---|---|---|---|
| W1 | `tag_columns` without `session_start_datetime`, so it stays data | 1 changed argument; for a DB source the user lists the other primary keys by hand | **9 rows: silently wrong.** The pod output lost the session identity; the join pairs every `m1` experiment with both sessions | new schema hash, so new `pipeline_hash` and system-tag column names (`_tag_source_id::a7dc…` instead of `::2f50…`) |
| W2 | copy the column before the source: `t.append_column("session_start", t["session_start_datetime"])` | 1 line, plus pod parameter `session_start` | **5 rows: correct** | new schema hash (extra data column); the copy gets a normal source-info token; record ids unchanged |
| W2' | same, mid-pipeline: `as_table()`, copy, new `ArrowTableSource` | 3 lines | correct | **lineage cut**: `upstreams == ()`; new root, so the downstream `pipeline_hash` no longer chains to the real sources |
| W2 on a DB source | `SpiralDBTableSource(...).as_table()`, copy, `ArrowTableSource(..., source_id="experiment")` | 3 lines | correct | the pipeline cannot be reloaded: `ArrowTableSource.from_config` raises `NotImplementedError`. The `source_id` must be passed by hand, or it defaults to the table hash and record ids move with the data |
| W3 | read the table twice (once with the column as data), `MapData` to a new name, `Join` | 3 lines | **9 rows: silently wrong.** The two reads share only `subject_id`, `experiment_index` as tags | two sources, two schema hashes |
| W4 | an operator that copies a tag into data | none exists: `MapTags`/`MapData` rename within a group; Select/Drop only remove; `Pick`/`Index` read data; `InvocationContext` (`ctx_arg`) carries hashes, not tag values | | |

Post-run hooks do receive the tag (read-only, `hooks.py:82`), but they run after the
computation and are not a way to feed it.

### 2.3 The inverse direction: a data column wanted as a tag (exp04)

Nine EDI tables carry another table's key as data: `experiment.task_id`/`task_version`,
`penetration.probe_id`/`chamber_id`, `lfp_location.chamber_id`, `sample_unit.atlas_label_region_short`,
and others. Joining on such a foreign key needs it as a tag on both sides.

* `Join(penetration, probe)` with primary keys as tags: `probe_id` is a tag in `probe` and
  data in `penetration`. `keys()` is accepted. Materialising raises
  `KeyError: Field "probe_id" exists 2 times`. This is the defect in 2.1, met on a natural
  workshop join.
* Today's recipe works at the source: `tag_columns=[*primary_keys, "probe_id"]` gives the
  correct 2 rows.
* After a pod, no operator can turn a data column into a tag. Only re-rooting can (W2').

## 3. How common is the need?

From `edi_sample_v1` (27 tables, mean key width 4.4, widest 9):

| Key column | Tables keyed on it | Plausibly a per-row computation input? |
|---|---|---|
| `subject_id` | 22 | no (categorical label) |
| `session_start_datetime` | 20 | **yes**: time arithmetic against other timestamps and dates |
| `recording_index`, `probe_index`, `experiment_index`, `unit_id` | 11, 10, 8, 4 | rarely (ordinals; used for ordering after grouping) |
| `*_hash` (preprocessing, extraction, localization) | 18 total | never |
| `timestamp` | 6 (event and time-series tables) | yes, but normally consumed per group (spike trains, gaze traces), where `GroupBy` already hands it to the pod as list data |
| `trial_index`, `frame_index` | 1 each | as for `timestamp` |
| `atlas_id`, `atlas_label_region_short`, `chamber_id`, `probe_id`, `name`, `task_*` | 1 to 5 | as labels, rarely as inputs |

Workshop-style computations that need a key as data:

1. **Session duration**: `session_end_datetime - session_start_datetime` on `session`. Needs the
   key per row.
2. **Experiment onset within the session**: `experiment_start_timestamp - session_start_datetime`
   on `experiment`. Likewise `recording_start_timestamp` on `recording`. Needs the key per row.
3. **Age at session**: `session_start_datetime - date_of_birth` after `Join(session, subject)`.
   Needs the key per row.
4. **Days since chamber implant**: `session_start_datetime - chamber_implant_date`. This needs
   the key as data, and also `chamber_id` promoted from data to tag in `penetration` to join
   `chamber`. Both directions in one exercise.
5. **Spike-train statistics per unit** (rate, ISI, PSTH): `spike_event.timestamp` is a key, but
   `GroupBy(by=[... "unit_id"])` already passes the timestamps to the pod as a `list[int]` data
   column (exp03 section 6 shows the mechanism). **No feature needed.**

Plainly: the need comes down to one column, `session_start_datetime` (and any similar date
key). It shows up in perhaps 2 to 4 workshop exercises, all of them session-level time
arithmetic. It is not a general pattern across keys. The inverse need, a foreign key as a tag
for a join, is more common (9 tables), and the source-level `tag_columns` recipe already
covers it, except after a pod.

## 4. The four designs

### 4.1 Comparison

| | A: one column in both groups | B: two columns, one user-visible name | C: copy operator (no new column model) | D: pod reads declared tags |
|---|---|---|---|---|
| Arrow representation | one physical column, listed in both groups | data twin under a reserved prefix (e.g. `_data_::x`), tag under `x` | ordinary data column under a new name `x2` | unchanged |
| `keys()`, `output_schema()` | `x` in both tuples and schemas | `x` in both | `x` in tags, `x2` in data | unchanged |
| `as_table()`, `as_df()`, `as_polars_df()` | one column `x`; a reader cannot tell it is also data | must rename (leaks the prefix) or fail: names must be unique | ordinary | unchanged |
| `iter_data()` | `x` in `Tag` and in `Data` | `x` in both (the datagrams already allow this, exp01 #10) | `x` in `Tag`, `x2` in `Data` | unchanged |
| Pod binding | parameter `x` | parameter `x` | parameter `x2` | parameter `x` plus a declared `tag_inputs=["x"]` |
| Values equal? | by construction, until an operator must split them | no: independent once created | equal at copy time, independent after (an ordinary data column) | n/a |
| Operators | not closed: `MapData`/`MapTags` rename, `GroupBy` (key stays a scalar tag, data must become a list) and Drop of one side all force a split into B | every name lookup must say which group; `PolarsFilter` (one namespace) becomes ambiguous; `GroupBy` turns a non-key tag `x` into data, colliding with the data twin `x`; `Join` data overlap rule must handle twins | unchanged; the copy is an ordinary data column | unchanged |
| `Join(penetration[data probe_id], probe[tag probe_id])` | still a collision | **legal and silently a cross product** (no shared tag) | unchanged (should be a clear error, section 7) | unchanged |
| Source info, record ids | membership must be stored | twin needs its own `_source_` | copy's `_source_` is null, as `GroupBy` does for tags it moves into data; record ids unchanged | unchanged |
| `content_hash`, `pipeline_hash` | schema hash must encode membership | schema hash sees `x` in both schemas | only for streams that use the operator (a new node) | pod identity must include `tag_inputs`; **the result cache keys on the input data hash only** (`result_cache.py:186`), so the key must gain the tag values, or rows with equal data and different tags share a cached result (exp03 #4) |
| Existing hashes, golden values | unchanged if membership is absent by default | unchanged if the prefix appears only with twins | unchanged | unchanged |
| Stored tables, save and load, `DerivedSource` | stored schema must record membership | a new reserved physical prefix in every stored table: a stored-schema convention for ITL-938 | a data column like any other | result-cache layout changes for such pods |
| Sync and async | every operator, both paths | every operator, both paths | one operator, both paths | function pod and executors |
| Tag `T` vs data `T \| None` | one column, one nullability | twins may diverge after a pod | copy keeps the tag's exact Arrow type and nullability (exp03 #5) | pod sees the tag's type |
| Tag/data principle | blurred: one value is both label and input | kept internally, blurred for the user | **kept**: the label stays a label; a copy is input | **broken**: a deliberate exception |
| Cost | weeks; touches every operator, datagrams, stores | weeks; same, plus name resolution everywhere | about a day with tests and docs | 2 to 4 days, plus the cache-key change |

### 4.2 Design A: one column in both groups

`ArrowTableStream` computes data columns as "every column that is not a tag"
(`arrow_table_stream.py:107`), so A needs explicit membership. Display is pleasant: one
column. It falls apart under operators, because they treat the two groups differently:

* `MapData({"x": "y"})` renames the data side only, so the column must split into a tag `x`
  and a data `y`. `MapTags`, `DropTagColumns` and `DropDataColumns` behave the same way.
* `GroupBy(by=["x"])` keeps `x` as a scalar tag, but every data column becomes a list, so the
  data side must split.
* `Join` keys on `x`. Both inputs also carry `x` as data, which the non-overlap rule rejects,
  so it needs a special case ("equal because it is a key").

So A is B plus a storage optimisation that holds only until the first operator. Not recommended.

### 4.3 Design B: two columns, one visible name

This matches the developer's "more simply" option, and per-row datagrams already allow it
(exp01 #10). The costs are in the table world:

* Physical names must be unique, so one side needs a reserved prefix. That prefix then
  appears in `as_table()` and the dataframes, or those methods must refuse the stream.
* Every operator that addresses a column by name (`Join` keys and data overlap, `MergeJoin`,
  `PolarsFilter` expressions, `GroupBy`'s tag-to-data step, `Batch`, Select, Drop and Map)
  needs a rule for which group it means. `PolarsFilter` sees one namespace, so
  `pl.col("x")` becomes ambiguous.
* `GroupBy` collides with itself: grouping by other keys turns the non-key tag `x` into a
  data `x`, and a data twin `x` already exists.
* The worst problem is that B legalises the state that today crashes on
  `Join(penetration, probe)`. Under B, `probe_id` is a tag in one input and data in the other,
  they share no tag, and the join becomes a silent cross product. Today's accidental error
  would become a silent wrong answer.
* Every stored table (function-node results, operator nodes, `DerivedSource`) gains a reserved
  physical prefix. That is a stored-schema convention, and it belongs in ITL-938's versioning
  system.
* Nothing ties the twins together. A pod that outputs `x` replaces the data twin, and the
  same name then means two different values (exp01 #10 shows this happening today).

Not recommended. The benefit (the pod parameter can keep the key's name) does not justify a
cross-cutting change before the public v0.3.

### 4.4 Design C: a copy operator (recommended)

`CopyTagsToData({"session_start_datetime": "session_start"})` appends one data column per entry:

* It copies the tag's Arrow column unchanged: same type (large-normalized like every operator
  output), same nullability.
* The tags, the system tags (name-preserving rule) and the existing data columns are unchanged.
* The copy's `_source_` value is null, exactly as `GroupBy` already does for a non-key tag it
  turns into data (`group_by.py` docstring). An operator cannot know which source a tag value
  came from; after a `Join` it came from several.
* The operator reads tags, which operators may do, and never reads data. It synthesizes no
  value; it copies one.

The prototype in exp03 (about 50 lines, defined in the script, not in `src/`) shows:

* `keys()`, `output_schema()`, `as_table()` and `iter_data()` show the copy as ordinary data,
  and the schema prediction matches `static_process(...).output_schema()`.
* The pod reads `session_start`, and the `Join` with `session` on the shared keys gives the
  correct 5 rows.
* The per-row input data hashes equal the W2 recipe's exactly, so result caches agree with
  the hand-made recipe. Source info does not enter the data hash.
* Two rows with equal data and different tags get different data hashes once the tag is
  copied. That is what makes caching correct, where D would fail.
* A nullable tag gives a nullable copy (`datetime | None`): all EDI keys are nullable in the
  DB schema.
* After the copy, `GroupBy` turns the copy into a list like any data column. `MapData` on the
  copy leaves the tag alone. Equality is a snapshot at copy time; after that the copy is just
  data.
* The `pipeline_hash` of the input stream is unchanged. Only the new node has a new hash.

The cost to the user is one call, and the pod parameter carries the copy's name
(`session_start`), not the key's. That can be read as a feature: the code shows which value is
the label and which is the input.

**Variant C-source:** an option on sources (`data_copies=`) could give the copy a real
source-info token (`experiment::row_0::session_start_datetime`). It would touch every source
class and `to_config`, and the operator already works on any stream, mid-pipeline included.
Not recommended now.

**Inverse (C'), `CopyDataToTags`:** promote a data column into the tags while keeping it as
data. It would cover the foreign-key join after a pod. It is a bigger step for the principle:
the data values would then drive `Join` matching, which is what operators deliberately avoid.
The source-level recipe (`tag_columns=[*pks, "probe_id"]`) covers the EDI tables for the
workshop. Defer.

### 4.5 Design D: pods read declared tags

`@function_pod(..., tag_inputs=["session_start_datetime"])` would pass named tag values as
read-only arguments. No column model changes. But:

* It is the exception the principle forbids: "function pods never inspect tags".
* Caching breaks. `ResultCache.lookup` matches on `input_data.content_hash()`, so two rows
  with equal data and different session starts share one cached result (exp03 #4). Fixing
  that means putting the declared tag values into the cache key and into the input identity.
  That is design C done inside the pod, with a new cache-key layout to version under ITL-938.
* After `Batch`, tags are lists, so the parameter type depends on the upstream.

Not recommended.

## 5. Identity, storage and interactions for the recommended design

* **Hashes:** no existing `content_hash`, `pipeline_hash`, schema hash, system-tag column name
  or golden value changes. A pipeline that uses the operator gets a new node, with
  `identity_structure = ("CopyTagsToData", sorted(name_map.items()))`, and downstream nodes
  chain from it as usual.
* **Record ids:** unchanged. The record-id preimage is system tags plus the input data hash
  (`function_node.py:94`). The copy enters the data hash, which is the intended behaviour: the
  value is now an input.
* **Stored schemas:** none change. A downstream function node stores the copy like any data
  column. No new prefix, path component or version label, so nothing for ITL-938.
* **ITL-929:** independent. The copy happens after the source, and record identity from
  primary keys is a source matter. If ITL-929 lands, nothing here changes.
* **Sync and async:** `async_execute` copies per row from `tag.as_table()`, giving the same
  Arrow type, nullability and column order as the sync path. Add the operator to
  `test_async_sync_equivalence.py`.
* **Save and load:** `to_config()` / `from_config()` carry `name_map`. The DB source stays a
  DB source, unlike the W2 recipe on a DB source.
* **Validation (at graph-build time):** every key must be a user tag column of the input, not
  a system tag. Every target name must be unused in both groups. Otherwise raise
  `InputValidationError` naming the column. Copying a list-valued tag (after `Batch`) gives a
  list-valued data column.

## 6. Recommendation

### 6.1 For the workshop: `CopyTagsToData`

API (stream method plus operator, like the other operators):

```python
from orcapod.operators import CopyTagsToData

stream.copy_tags_to_data({"session_start_datetime": "session_start"})
CopyTagsToData({"session_start_datetime": "session_start"})(stream)
```

Before (today, EDI `experiment` from SpiralDB, correct but leaves the DB source):

```python
raw = SpiralDBTableSource(PROJECT, "experiment", dataset="edi_sample_v1").as_table()
raw = raw.append_column("session_start", raw["session_start_datetime"])
experiment = ArrowTableSource(raw, tag_columns=EXPERIMENT_KEYS, source_id="experiment")
```

After:

```python
experiment = SpiralDBTableSource(PROJECT, "experiment", dataset="edi_sample_v1")

@function_pod(output_keys="onset_s")
def onset_s(session_start: datetime | None, experiment_start_timestamp: int | None) -> float | None:
    ...

onsets = onset_s.pod(
    experiment.copy_tags_to_data({"session_start_datetime": "session_start"})
    .select_data_columns(["session_start", "experiment_start_timestamp"])
)
onsets.join(session)  # keyed on subject_id, session_start_datetime as before
```

Scope: one operator file, a stream method, `__all__`, sync and async paths, `to_config`,
tests (public interface; schema prediction against `static_process`; async/sync equivalence;
`Join` downstream; `GroupBy` after the copy; validation errors), a short docs section, and a
CHANGELOG entry under `[0.3.0] — unreleased`. No hash, golden value or stored schema changes.

### 6.2 If the developer prefers to wait: the recipe

For in-memory sources (`DictSource`, `ArrowTableSource`, CSV), copy the column before building
the source. It is one line, and it is what exp02 W2 shows is correct:

```python
t = t.append_column("session_start", t["session_start_datetime"])
```

For `SpiralDBTableSource`, use the three-line "before" block above. Pass `source_id`
explicitly, and accept that the pipeline cannot be reloaded from its config. **Never**
recommend W1 or W3. Both give wrong joins without any error.

### 6.3 What waits

The inverse operator (C'), the source-level variant, and designs A, B and D.

### 6.4 Risks to the tag/data principle

Low. C keeps the label in the tags and passes an explicit, named copy to the pod. The pod
still never sees a tag. The risk is user confusion: two columns hold the same value at first
and can diverge later. The docs should say the copy is a snapshot.

### 6.5 Open decisions for the developer

1. **Ship `CopyTagsToData` before the workshop, or use the recipe and ship it after?**
   Recommendation: ship it now. It is additive, about a day, and changes no hash.
2. **Name and shape of the API.** `CopyTagsToData(name_map)` plus
   `stream.copy_tags_to_data(name_map)`, with an explicit map and no default naming.
   Recommendation: yes. A default suffix would invent names the user never wrote.
3. **Source info of the copy.** Null, as `GroupBy` does for tags it moves into data, or a
   token derived from the system tags. Recommendation: null.
4. **The inverse direction (data to tag, keeping the data).** Recommendation: defer. Use
   `tag_columns=[*primary_keys, fk]` at the source for the workshop. File an issue only if a
   post-pod foreign-key join turns up.
5. **Reject designs A, B and D** and record why (sections 4.2, 4.3, 4.5). Recommendation: reject.
6. **Fix the `Join`/`MergeJoin` tag-vs-data collision** so it raises `InputValidationError` at
   graph-build time, naming the column and which input has it as a tag and which as data,
   instead of a raw `KeyError` at materialisation. Recommendation: yes, as a separate issue
   before the workshop, because `Join(penetration, probe)` hits it.
7. **Reject a pod whose output key equals a tag name** when the pod is applied (today
   `keys()` accepts it, `as_table()` fails and `iter_data()` yields diverging values).
   Recommendation: yes, a separate issue, after the workshop.

## 7. Related defects found (not fixed here)

* **J1.** `Join` and `MergeJoin` check data-vs-data overlap but not a name that is a tag in one
  input and data in another (`join.py:103-118`). `keys()`/`output_schema()` predict the name
  in both groups. `as_table()` and `iter_data()` then raise
  `KeyError: Field "x" exists 2 times in schema` (exp01 #6, #7, #10b; exp04 F1).
* **P1.** A function pod whose output key equals an input tag name is accepted. `as_table()`
  raises `Duplicate column name`, and `iter_data()` yields a tag and a data value with the same
  name and different values (exp01 #5, #10).
