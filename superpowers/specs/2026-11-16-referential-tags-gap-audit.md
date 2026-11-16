# Referential tags (v0.4): gap audit of the workshop slice

Read-only audit for the developer's review on Monday, November 16, 2026. It covers the
project "Orcapod Python: referential tags (v0.4)" against the workshop slice. It changes no
library code, test or golden file.

- **Slice audited:** `origin/eywalker/itl-1023-workshop-slice-referential-tags-working` at
  `634ad40a` (PR #342, open, CI green on that head: 18 checks passed, 2 skipped). The slice
  is 26 commits ahead of `origin/main` and 33 behind it (`main` at `4dd015a4`, which has
  ITL-946's golden layout `data/v1/` and ITL-1081's `StreamView`). Another worker is
  refreshing the slice tonight.
- **Read:** the specification attached to ITL-1004 ("Orcapod v0.4 — Referential Tags",
  sections cited as "spec §N"); PR #342's description; the slice's code, tests and
  `docs/concepts/referential-tags.md`; ITL-1004, 1009 to 1023, 1025 and 1142, descriptions
  and comments; ITL-1154 (and its branch at `10f1c4a8`), 1157, 1158 and 946.
- **Checked by running:** `tests/test_core/referential_tags` (106 passed). Six probe scripts
  on the slice head, quoted in Appendix A. Every claim marked "(probe)" was run.
- **Sizes:** S is half a worker-day or less, M about one worker-day, and L more than 1.5
  worker-days. Each size includes the review rounds the PR readiness pipeline needs.

## Summary

The slice does what ITL-1023 asked for. Sources default to referential tags. Function pods
read referenced columns, and `pod(stream)` lets them fall through. `Join`, `GroupBy`,
select and drop, renames, filters, `DropNulls`, `SemiJoin`, `Pick` and `Index` handle
references. Function results are reused across retagging (probe: a referential run after a
plain run on the same store makes zero calls). Every v0.3 golden value is unchanged.

The gaps that matter for a release are persistence, `Batch`, the identity pins, and
alignment with ITL-1154:

- **Persistence (ITL-1019):** `job.save()` raises. `node.as_source()` raises. An operator
  node cannot store a referential stream. Nothing stored records which tags are
  referential.
- **`Batch`:** raises for any source built with the default, so existing `src.batch(...)`
  code breaks.
- **Identity pins:** no golden entry pins a referential identity.
- **ITL-1154 alignment:** the merged source-info token grammar does not yet follow the v1
  hash definition.

A v0.4.0 by Wednesday is possible only if:

- the release ships with an explicit "not supported yet" list (`MergeJoin`, operator-node
  storage, possibly `DerivedSource`);
- four or five workers start on Monday;
- ITL-1154 lands on Monday.

---

## 1. Coverage

Legend: **done** on the slice; **partly**; **missing**; **raises** ("not supported with
referential tags yet"); **decided-other** (a later developer decision replaced the
criterion).

### 1.1 By issue

#### ITL-1009 Schema model

| Criterion | Status | Evidence |
|---|---|---|
| C1 binding kind visible in declared schema | done (by representation) | The binding is the name intersection of the two schema halves: `referential_tags.py:74-101` (`reference_names`, `stream_references`). There is no `Schema` marker (PR decision D3 (a)). |
| C2 one shared validator | partly | `check_namespaces` (`referential_tags.py:104-135`) is called only by `Join.output_schema` (`join.py:194`). Elsewhere the rules hold by construction: a name in both halves *is* a reference, so "plain tag and data of one name" and "reference without data" cannot be expressed. |
| C3 type and nullability equal | done | There is one physical column. `check_namespaces` checks type equality for joins. `DropNulls` narrows both roles (probe: `k: int` in both). |
| C4 spec 15.1 tests | partly | `test_model_and_sources.py:29-123`. "Reference to a differently named target" cannot be expressed, so it is not tested. |
| C5 no-reference schema unchanged | done | All golden entries pass (PR test plan). The identity tuple grows only when references exist (`arrow_table_stream.py:187`). |

#### ITL-1010 Streams and datagrams

| Criterion | Status | Evidence |
|---|---|---|
| C1 stream with a referential tag, column once | done | `arrow_table_stream.py:122-140`. Tests: `test_model_and_sources.py:37`, docs sample (`referential-tags.md:42-45`). |
| C2 Tag and Data agree per row | done | Rows are rebuilt from one column: `static_output_pod.py` `_rows_to_table` (drops the data copy), `function_node.py` (`shared` drop, line 645), `side_effects.py`. The EDI test asserts `tag["unit"] == data["unit"]`. |
| C3 rebuilt tables neither duplicate nor drop | done | Same places. The EDI test checks `column_names.count("unit") == 1`. |
| C4 "at least one data column" rule settled | partly | A source whose only data are referenced tags is accepted (probe: `keys() == (('a',), ('a',))`). The plain form still raises. No test pins it and no decision is recorded. |
| C5 no-reference streams byte-identical | done | Golden entries are unchanged. |
| Display marks `~name` (developer, Nov 13, on ITL-1010) | missing | `StreamView` (`core/streams/view.py:151`, `COLUMN_ROLE_MARKS`) is on `main` only. The slice predates it, so the referential role needs one entry after the refresh. |

#### ITL-1011 Identity and hashing

| Criterion | Status | Evidence |
|---|---|---|
| C1 no golden entry moves | done | The 15 fixture sites in `cases.py` pass `tag_only_columns=True`. No data file is touched. |
| C2 binding in identity only when referential | done, with one exception | `arrow_table_stream.py:183-188`. The fall-through pod is built only when references exist (`function_pod.py:774`). **Exception:** `MapTags(tag_only=True)` adds `("tag_only", True)` to its identity even on a stream with no reference. Probe: on a plain stream the pipeline hashes differ and the tables are equal. |
| C3 result identity independent of tagging | done | `test_function_pods.py:171`. Probe: a plain run, then a referential run on one store, makes 0 calls. |
| C4 new golden entries for referential cases | **missing** | No referential case in `tests/test_hashing/golden/cases.py`. See section 4c. |
| C5 approved statement: no data context version; how to reproduce v0.3 hashes | **missing** | The PR body asserts "no context or data context change". No statement has been approved. |

#### ITL-1012 Sources

| Criterion | Status | Evidence |
|---|---|---|
| C1 every source referential by default | done for Arrow, CSV, dict, data frame, Delta, SQLite, PostgreSQL, DB, SpiralDB; **decided-other** for `ListSource` (stays plain, PR decision D6 (a)); `PollingSource` stays plain (gap) | `source_reference_tags` `referential_tags.py:187-221`; each source passes it to the builder (e.g. `dict_source.py`, `spiraldb_table_source.py:325`). |
| C2 explicit plain option identical to v0.3 | done | `tag_only_columns`. Probe: equal default `source_id` and equal source-info tokens. |
| C3 changelog upgrade note | done | `changelog.d/ITL-1023.md`, "Breaking changes and migration". |
| C4 old golden values prove the plain mode; new entries pin the default | half done | Old values pass. New entries are missing (ITL-1011 C4). |
| C5 option name and `ValueError` | done | `referential_tags.py:217`, `test_model_and_sources.py:58`. |
| C6 all-tag source valid | done | probe |
| C7 referential identities pinned | **missing** | section 4c |
| C8 `tags_as_data` unchanged | done | No change to its code path. A referential origin raises (`derived_source.py:236`). |

#### ITL-1013 Function pods

| Criterion | Status | Evidence |
|---|---|---|
| C1 referenced column read by name | done | `test_function_pods.py:37` |
| C2 `process` exact, references plain at the boundary | done | `test_function_pods.py:46`. Note: `process` also **rejects** a referenced column the function does not read (PR decision D2 (a)). The message does not mention referential tags (probe: "column 'unit' is not a parameter of the function"). |
| C3, C4 fall-through variant, per row, no join | done | `ReferenceFallThroughPod`, `function_pod.py:1774-1960` |
| C5 equals the composition, duplicate tags included; hash-equal without references | done | `test_function_pods.py:77`, `:110` |
| C6 result identity and store shared | done | `result_pod`, `function_input` (`function_pod.py` end); `function_node.py` (CachedFunctionPod over `result_pod`) |
| C7 output named like a tag raises at build | done | `_validate_output_keys` `function_pod.py:257`; `test_function_pods.py:130` |
| C8 plain data replacement unchanged | done | `test_function_pods.py:139` |
| C9 automatic join and select | done | `function_pod.py:734-778`. The automatic select is built with the diagnostic off. That is the coordinator's call and the developer has not confirmed it (ITL-1004 comment, Nov 13). |
| C10 sync and async | done | EDI test, sync and async (`test_edi_end_to_end.py:139`) |
| Open questions: list-valued key falls through? size limit? source info after the pod? | largely moot on head | Since `634ad40a` a referential non-key loses its tag role in `GroupBy` (`group_by.py:215`), so the 28k-element `gaze_time` list no longer falls through. A list-valued key can still come from a list-typed source tag. |

#### ITL-1014 Row-preserving operators

| Criterion | Status | Evidence |
|---|---|---|
| C1 bindings preserved, sync and async | done | `REFERENTIAL_TAGS_ISSUE = None` on `PolarsFilter`, `SemiJoin`, `DropNulls`, `Pick`, `Index`; `test_operators.py:270`, `:278`; referential cases in `test_async_sync_equivalence.py` |
| C2 predicted schema equals `static_process` | partly | Tested for `GroupBy` (`test_operators.py:184`), not for each of the five. |
| C3 one shared invariant helper | **missing** | No helper in `tests/`. |
| C4 plain output byte-identical | done | Golden and existing suites pass. |

#### ITL-1015 Select and drop

| Criterion | Status | Evidence |
|---|---|---|
| C1 removing referenced data leaves a plain tag, exact values | done | `column_selection.py`; `test_operators.py:198`, `:208` |
| C2 removing a referential tag keeps the data | done | `test_operators.py:233` |
| C3 transition visible at build time, in identity | done (schema-level) | The output schema shows the plain tag. No inspectable record of the transition exists beyond the warning (spec §7.3, §13.5). |
| C4 `reference_materialization` warn/silent, once per operation | done | A module-level setting (`referential_tags.py:37`), reported once per operator object (`:138-164`); `test_operators.py:215` |
| C5 spec 15.5 sync and async | partly | The "materialization represented in provenance" item is only the warning. |

#### ITL-1016 Join

| Criterion | Status | Evidence |
|---|---|---|
| C1 plain with plain unchanged | done | `test_operators.py:66`, goldens |
| C2 plain with referential, both orders | done | `test_operators.py:77` |
| C3 referential with referential: one column | done | `test_operators.py:91`; `join.py:198-222` (`_merged_references`) |
| C4 merge only for equated references, strict types, nothing else | done | `join.py:170-194`; `test_operators.py:112`, `:122`, `:129` |
| C5 provenance of both inputs | done | One merged token (ITL-1017 decision): `join.py:42-80`, `_merge_source_columns`, `_fold_source_column`. System tags keep both inputs. |
| C6 automatic join same rules | done | It is the same `Join`. |
| C7 N-ary, many-to-many, sync = async | done | `test_operators.py:105`, `:135`; async folds once (`join.py`, `final=is_last`). The async `TypeError` on a list-valued shared tag is pre-existing (ITL-1044). |

#### ITL-1017 GroupBy and Batch

| Criterion | Status | Evidence |
|---|---|---|
| C1 referential key scalar in both roles | done | `group_by.py:300`; `test_operators.py:158` |
| C2 referential non-key: same list in both roles, stays referential | **decided-other** | ITL-1070 (GroupBy drops every non-key tag) and the developer's Nov 13 decision behind `634ad40a`: the tag role is dropped, and the data column stays as a list ordered by its values (`group_by.py:204-216`, `test_group_by_referential_non_key.py`). The developer's Nov 12 comment on ITL-1004 ("GroupBy stays as in section 8.2") and ITL-1017 C2 are not updated. |
| C3 `Batch` rule stated and agreed | **raises** | `batch.py:31` (`REFERENTIAL_TAGS_ISSUE = "ITL-1017"`). No rule is agreed. |
| C4 system tags fold; source info per member | done, as amended | Key: one merged token (decision Nov 13, 16:56). Non-key: one token per member. C4's text is not updated. |
| C5 prediction, zero rows, sync = async | partly | Prediction: `test_operators.py:184`. Zero-row referential `GroupBy` is not tested. |

#### ITL-1018 MergeJoin and renames

| Criterion | Status | Evidence |
|---|---|---|
| C1 MergeJoin rules written and implemented | **raises** | `merge_join.py:159` |
| C2a `MapTags` renames both roles | done | `mappers.py` (MapTags), `test_operators.py:327` |
| C2b `tag_only=True` | done | `test_operators.py:339`. The option name is still the coordinator's proposal. |
| C2c `MapData` data-only | done | `test_operators.py:352`, `:363` |
| C2d plain tags unchanged | done | `test_operators.py:375` |
| C3 rename never makes tag/data collide | done (by construction) | |
| C4 predictions, sync = async, invariant helper | partly | Async equivalence covers renames. The helper is missing (ITL-1014 C3). |

#### ITL-1019 Nodes, stores, saved jobs

| Criterion | Status | Evidence |
|---|---|---|
| C1 stored stream read back keeps bindings | partly | **Function node:** done (in-memory and Delta, sync and async; `function_node.py:3008`, `reference_tags=fall_through_references(self._function_pod)`; EDI test). **Operator node:** raises (`operator_node.py:1043`, `:1270`, `:1394`). |
| C2 what is stored, stated; v0.3 stores read | partly | A v0.3 job (`main` at `f51ce67c`) loads with unchanged hashes (PR test plan). Nothing stored records the binding: see question Q11 in section 3. |
| C3 `DerivedSource` gives back bindings | **raises** | `derived_source.py:236` (probe: `node.as_source()` raises) |
| C4 saved and loaded job keeps bindings and hashes | **raises** | `pipeline/graph.py:166`, `function_pod.py:1821`, `:1830` (probe: `job.save` raises) |
| C5 retagging computes nothing new | partly | Plain to referential retag computes nothing (probe, `test_function_pods.py:171`). Adding a materialized tag is not tested. The re-creation of pipeline records is not asserted. |
| C6 `head()` shows the column once | done | EDI test `node.head()` |
| Developer decision (Nov 12): saved jobs pin behaviour by explicit version info, not by a key's presence | **deviates** | `from_config` reads `config.get("tag_only_columns", True)` (`csv_source.py:91`, `delta_table_source.py:102`, `sqlite_table_source.py:159`, `postgresql_table_source.py:129`, `spiraldb_table_source.py:806`). `to_config` writes the key only when some tag is referential (`sources/base.py:136-147`). The format version (`serialization.py:37`, `:1202`, now `0.3.0`) is written but not used to decide meaning. |
| Open: does v0.3.0 write enough version info? | answered: yes | `main` writes format `0.2.0` plus `orcapod_version` (ITL-1026), so v0.4 can recognise a v0.3 file by version. Nothing has to be added to v0.3.0. |

#### ITL-1020 `tags_as_data`

| Criterion | Status | Evidence |
|---|---|---|
| C1 keep, deprecate or remove: decided | missing | no decision |
| C2 hashes of what stays | done | unchanged code path |
| C3 docs: which to use when | missing | `referential-tags.md` does not mention `tags_as_data`. |
| C4 explicit migration path | missing | |

#### ITL-1021 `ediop` integration

Partly. The notebook ran with 0 errors on `4473e2ef` (ITL-1021, Nov 13). The worker
recommended no-go on that pin: the store was 350 MB against 10 MB, and the notebook took
724 s against 239 s.

The slice head has since changed both causes of the bloat:

- a group key now carries one merged token (ITL-1017);
- a non-key timestamp no longer falls through as a tag (`634ad40a`).

The notebook has **not been re-measured on the head**. C3 needs a recorded run on the
release commit. C4 (an extra contextual tag computes nothing) passed on `4473e2ef`.

#### ITL-1022 Docs and operator audit

| Criterion | Status | Evidence |
|---|---|---|
| C1 operator table, plus a registry test | partly | Table: `referential-tags.md:79-88`. The guard is a class attribute that defaults to raising (`static_output_pod.py:63`), but no test walks the registry against the table. |
| C2 concept pages updated | partly | A new page. Existing pages pin `tag_only_columns=True`, including the first example on `docs/index.md` and `docs/getting-started.md`. |
| C3 `orcapod-design.md`, `CLAUDE.md`, `.zed/rules` | **missing** | not touched by the slice |
| C4 definition of done (spec §20), item by item | missing | |
| C5 changelog | done | `changelog.d/ITL-1023.md` |

#### ITL-1023 Workshop slice

C1 to C5 and C7 done. C6 done, with two differences: renames were implemented rather than
made to raise, and the list of operations that still raise is in the PR. C8 done (CI green
on `634ad40a`). C9: the report is in the PR body. The go/no-go moved to Tuesday,
November 17 (developer comment on ITL-1142, Nov 15).

#### ITL-1025 Full append

Deferred by the developer (Nov 12). Nothing on the slice. Not needed for v0.4.0.

#### ITL-1142 Binding

Nothing written. The slice behaves strictly: a source with a referential tag bound into a
plain slot is refused (probe: `SourceSpecMismatchError ... unexpected data columns:
['unit']`). The reverse, a plain source into a referential slot, cannot arise yet because
a referential job cannot be saved. C1 to C5 are missing.

### 1.2 By specification section

| Spec | Status | Note |
|---|---|---|
| §3, §4.1 to §4.5 model and validity rules | done | By construction: one physical column, roles by name (`referential_tags.py:1-18`). |
| §5, §5.1 operator/pod boundary; no implicit dependency | done | `function_input` drops unread references before the result identity; probe: 0 calls. |
| §6.1 tags pass through pods | done on `pod(...)`; on `process` the tags become plain (decision) | |
| §6.2, §6.4 output/tag collision at build time | done | |
| §6.3 plain data replacement | done | |
| §6.5 projection is separate | done | The automatic select is a real `SelectDataColumns` step. |
| §7.1, §7.2 automatic materialization | done | |
| §7.3 transition recorded and inspectable | partly | A warning and the output schema only. No graph record. |
| §7.4 diagnostics | done | Module-level, not a config entry. |
| §7.5 no override of pod errors | done | |
| §8.1 referential key | done | |
| §8.2 referential non-key | decided-other | The tag role is dropped (ITL-1070). |
| §8.3 ordering | done | Members are ordered by the non-key's values. |
| §8.4 general rule | partly | The non-key is not referential. |
| §9.1 to §9.5 join rules, provenance, collisions | done | |
| §9.6 subset keys | n/a | `Join` joins on every shared tag. |
| §9.7 outer joins | n/a for `Join`; **raises** for `MergeJoin` | |
| §10 operator table | done except `Batch` and `MergeJoin` (raise) | |
| §10.1 renames | done (developer rules) | |
| §10.2 `Batch` | raises | |
| §11.1 to §11.4 sources | done | Default referential (developer); the option is the inverse, `tag_only_columns`. |
| §11.5 `tags_as_data` | unchanged; decision missing | |
| §12.1, §12.2 storage separation, retagging | done for function nodes | |
| §12.3 binding in identity | done | through schemas plus `("reference_tags", ...)` |
| §12.4 result identity | done | |
| §12.6 hash compatibility | partly | Plain hashes are unchanged; referential hashes are not pinned. |
| §13.1 `TagBindingKind` | decided-other | name intersection, no enum |
| §13.2 separate logical namespaces | done | |
| §13.3 physical storage | done | One table with role lists. This is closer to option B than to A, at A's cost. |
| §13.4 centralized validation | partly | `check_namespaces` is used by `Join` only; elsewhere the rules hold by construction. |
| §13.5 explicit transitions | partly | |
| §14 phases 1 to 4 | done | |
| §14 phase 5 sources and persistence | sources done; **persistence missing** | |
| §14 phase 6 audit and docs | partly | |
| §15.1 to §15.5 tests | mostly done | Gaps: §15.3 serialization of list-valued bindings; §15.5 provenance. |
| §15.6 provenance and cache tests | partly | "Reloading stored streams preserves reference bindings" is done for function nodes in a live graph. Reloading from a saved job is missing. |
| §15.7 real pipeline | done in-repo (EDI-shaped test); `ediop` on an old pin | |
| §16.1 legacy streams plain | done | through key absence, not version (see ITL-1019) |
| §16.2 explicit migration | missing | |
| §16.3 result reuse | done | |
| §16.4 versioned hashes and storage | partly | No layout version. Binding persistence is undecided (Q11). |
| §20 definition of done | 7 of 11 items met | Met: invariants; referenced data reachable without copies; pods preserve and reject; `GroupBy` coherence (as amended by ITL-1070); joins; retagging reuses results; EDI-shaped pipeline without copies (in-repo; `ediop` not re-run on the head). Partly: automatic materialization is done but the transitions are not inspectable; operator semantics are defined except `Batch` and `MergeJoin`, which raise; legacy compatibility works but by key absence, not by version. Missing: bindings surviving persistence and reconstruction. |

---

## 2. What a shippable v0.4.0 needs beyond the slice

### Must-have (correctness, data loss, wrong results, saved jobs, persistence)

| # | Item | Size | Depends on |
|---|---|---|---|
| M1 | **Slice refreshed onto `main`, then merged.** The refresh takes in `StreamView`, the golden `data/v1/` layout, and later ITL-1154 (the fold labels `group_v1` and `batch_v1`, `<kind>:` default ids, and the hash-site trace test, which will fail on the new site `fold_source_info_tokens` until the site is listed). | M (in flight) | v0.3.0 tag, ITL-1154 |
| M2 | **Persistence of the binding and saved jobs (ITL-1019 C2 and C4).** Save and load a pipeline or job with referential tags. Pin meaning by format version, not by key absence (developer, Nov 12): `0.1.0` and `0.2.0` mean all plain; from `0.3.0` on, every source writes `tag_only_columns` explicitly. Make the fall-through pod serializable. Decide where the binding lives in stores (Q11). | L | Q11, Q8 |
| M3 | **`Batch` on referential tags (ITL-1017 C3).** With the default, every existing `src.batch(...)` raises (probe). | M | Q5 |
| M4 | **Golden pins of referential identities**, added with `--add` into a new `data/v1` file (section 4c). Plus the ITL-1011 C5 statement, approved. | M | M1 (after the ITL-1154 regeneration), Q4, Q12 |
| M5 | **ITL-1154 alignment:** the merged-token grammar (Q4); list `fold_source_info_tokens` in the hash-site inventory; mark design issue RT2 resolved (the developer decided it on Nov 13). | S | ITL-1154, Q4 |
| M6 | **`ediop` notebook re-measured on the refreshed head** (ITL-1021 C3): store size and run time against plain, sync and async. This is the input to the Tuesday go/no-go. | S (about 15 min of runs plus the report) | M1 |
| M7 | **Readers without the live graph** see a referential column as plain. Example: a function node read with `_function_pod is None` (CACHE_ONLY or deserialized) gets `reference_tags=()`, so `keys()` disagrees with the node's predicted schema (`function_node.py:2934-2944` and `:3008`). Fixed by M2's choice of where the binding is persisted. | in M2 | Q11 |

### Should-have (usability and workshop quality; the release is correct without them)

| # | Item | Size | Depends on |
|---|---|---|---|
| S1 | `DerivedSource` / `node.as_source()` over a referential stream (ITL-1019 C3) | M | Q7, Q11 |
| S2 | Operator-node `LOG`/`REPLAY` storage of referential streams (ITL-1019) | M | Q10, Q11 |
| S3 | Error messages that name the remedy: `process` with an unread referenced key; bind mismatch (`unexpected data columns: ['unit']`); the "not supported yet" errors already name `tag_only_columns` | S | — |
| S4 | Docs: the first examples (`index.md`, `getting-started.md`, `concepts/sources.md`) show the default, not the opt-out. A warning about joining two time series that share `timestamp`. `tags_as_data` versus a referential tag (ITL-1020 C3). | M | Q8, Q2 |
| S5 | `StreamView` mark `~name` for a referential tag (ITL-1010; keys shown twice was an `ediop` complaint) | S | HTML style confirmation |
| S6 | `CLAUDE.md`, `.zed/rules`, `orcapod-design.md` binding rules; operator-registry test (ITL-1022 C1, C3) | S | — |
| S7 | Shared reference-invariant test helper; zero-row referential `GroupBy`; per-operator prediction tests (ITL-1014 C2, C3; ITL-1017 C5) | S | — |
| S8 | `MapTags(tag_only=True)` identity on a plain stream (Q1b) | S | Q1 |

### Can wait (v0.4.x or later)

| Item | Size |
|---|---|
| `MergeJoin` with referential tags (ITL-1018 C1) | M |
| ITL-1142 full role matrix and its implementation | L |
| ITL-1020 deprecation or migration of `tags_as_data` | M |
| ITL-1025 full append | L |
| Inspectable materialization record (spec §7.3, §13.5) | M |
| `PollingSource` referential tags, together with ITL-1158 row identity | M |
| Key-column row identity (section 4b) | L |
| Async `Join` on a list-valued shared tag (ITL-1044, pre-existing) | S |
| Dead `MapTags` class in `column_selection.py:585` (not exported; it would raise on a reference) | S |

### What can NOT be finished by Wednesday, November 18

- ITL-1142 (matrix, decision, implementation).
- `MergeJoin` semantics decided and built, alongside everything else.
- ITL-1020 migration, ITL-1025, and the full ITL-1022 audit (definition of done item by
  item).
- Key-based row identity (section 4b). This is a source-wide design review and a hash
  change.
- M2 **and** S1 **and** S2 together. M2 is L on its own; S1 and S2 share its design (Q11)
  and would land Tuesday night at the earliest. Realistically v0.4.0 ships with
  `DerivedSource` and operator-node storage still raising.
- Pinning row-level referential identities (record ids, source-info tokens) if Q12 is not
  decided by Tuesday. Pin the schema-level ones only; row-level pins can be added later,
  because additions are allowed in any release (ITL-946).

---

## 3. Open decisions for the developer

Each is a question with options and a recommendation (**rec**).

**Q1. ITL-1018: `MergeJoin` and renames.**

- (a) `MergeJoin`, three options:
  - keep raising in v0.4.0;
  - treat a referential tag as `Join` does and merge plain data collisions into lists as
    today, refusing a referential data column that collides as a non-key;
  - define unmatched rows.

  **Rec:** keep raising in v0.4.0 and decide the rule in v0.4.x. `MergeJoin` is not on the
  workshop path.
- (b) Renames are decided (Nov 12) and implemented. Still open:
  - the spelling `tag_only=True` (the coordinator's proposal). **Rec:** confirm it.
  - `MapTags(tag_only=True)` on a stream with no reference: today a different hash for the
    same table (probe). Options: keep it (the option is operator config); or drop
    `("tag_only", True)` from the identity when no renamed tag is referential. That second
    option is impossible, because an operator's identity cannot depend on its input.
    **Rec:** keep it and document it, or refuse `tag_only=True` when nothing it renames is
    referential ("when in doubt, refuse").

**Q2. ITL-1020: `tags_as_data`.** Options:

- (a) keep it unchanged and document "a referential tag for a column needed in both roles
  under its own name; `tags_as_data` for a differently named independent copy";
- (b) deprecate it;
- (c) remove it before 1.0.

**Rec:** (a) for v0.4.0. Its golden values stay, and `ediop` moves off it anyway. Also
decide whether `DerivedSource(origin, tags_as_data=...)` accepts a referential origin
(today it raises). **Rec:** accept it, and refuse a mapping whose source tag is
referential, since that column is already data.

**Q3. ITL-1142: bind with a referential tag on one side.**

- Today: strict. A referential source into a plain slot is refused (probe).
- Developer's intuition: narrowing a referential tag to a plain tag or to data is fine;
  widening into a referential slot is refused.

**Rec:** ship v0.4.0 strict (refuse every role difference) with a message that names
`tag_only_columns`. Do the matrix after the workshop. Strict now can be relaxed later
without moving any identity; the reverse cannot.

**Q4. ITL-1017-RT2 and the merged-token grammar.**

- RT2 itself (a nested merged token enters the next fold labelled or bare) was **decided
  by the developer on Nov 13, 17:28** ("folded whole, label included"). ITL-1154 rule 1 ("a
  hash always carries its label") agrees. `design_issues/ITL-1017-RT2.md` still says
  `open`; it should be marked resolved.
- What is open is the **text** of the token: `merged:source_fold_v1:<hex>` today
  (`arrow_utils.py:1407-1440`). ITL-1154 (developer, Nov 15) says a dedicated algorithm
  gives `<algorithm>_v<N>:<hex>`, like `group_v1:<hex>` and `batch_v1:<hex>`, and "only
  source ids have a kind prefix".
- Options:
  - keep `merged:` (the token is recognisable as an aggregate, which the ITL-1017 coordinator
    note asked for);
  - make it `source_fold_v1:<hex>` (follows ITL-1154; the label already says it is a fold).

**Rec:** `source_fold_v1:<hex>`. Decide it before the merged token is pinned (M4).

**Q5. `Batch`.** Options:

- (a) keep raising;
- (b) `Batch` keeps every tag, list-valued (developer, Nov 13). A referential tag becomes
  the same list in both roles and **stays referential** (spec §10.2, coherent by
  construction). Its source info is one token per member, because the values are listed,
  not merged.
- (c) drop the tag role, as `GroupBy` does for a non-key.

**Rec:** (b). It follows `Batch`'s own "keep every tag" rule and the reference invariant.
It is size M with `GroupBy` as the model. Without it, the default breaks every existing
`batch` call.

**Q6. `PollingSource` tags.** Options:

- (a) stay plain in v0.4.0, documented;
- (b) referential like every other source.

**Rec:** (a) now. Do (b) together with ITL-1158, which must change its row identity anyway
(rows of different batches share record ids).

**Q7. `DerivedSource` over a referential stream.** Options:

- (a) keep raising;
- (b) carry the origin's bindings: the derived source presents the referential tags as
  referential, column stored once;
- (c) present them plain.

**Rec:** (b), as ITL-1019 C3 asks. It needs Q11. If Q11 is not built by Wednesday, ship
(a).

**Q8. The default: every declared source tag referential.**

Decided by the developer on Nov 12 as "a choice of changing default behavior". Its
measured effect on existing code (probes):

- every hash of a source built without `tag_only_columns` changes. This was accepted.
- `pod.process(src)` raises when the function does not read the keys. `pod(src)` works and
  lets them fall through.
- `src.batch(...)` and `MergeJoin` raise (until M3 and Q1).
- `job.save()` raises; `node.as_source()` raises (until M2 and S1).
- A default source bound into a slot of a v0.3-saved job is refused.
- About 631 test call sites in about 98 files pass `tag_only_columns=True`. They pin the
  v0.3 shapes and are the regression suite of the plain mode. The default mode itself is
  covered by about 106 tests.

Options:

- (a) keep the default and ship v0.4.0 when M2 and M3 are in;
- (b) ship v0.4.0 with the default plain (opt in with `reference_tags=`). This reverses the
  developer's decision.
- (c) keep the default, but have the 631 sites run a parametrized subset in both modes.

**Rec:** (a). Add a small parametrized pass in both modes over the operator, pipeline and
async-equivalence suites (S, in S7). Leave the golden fixtures pinned plain. If M2 or M3
cannot land by Wednesday, the honest alternative is (b) for v0.4.0, with the default
flipped in v0.4.1, rather than shipping a default that breaks `save` and `batch`.

**Q9. Materialization diagnostic at a pod boundary.** The slice is silent for the pod
call's automatic select. This was the coordinator's call (ITL-1004, Nov 13), not confirmed
by the developer. **Rec:** confirm silent.

**Q10. Does source info enter `OperatorNode._record_hash`?** It does today
(`operator_node.py:111`, the operator cache's dedup key; ITL-1019 comment, Nov 13). Under
ITL-1154 rule 1 a stored value fed into a hash carries its label, which the merged token
does. Options:

- (a) keep source info in the key: a change of group membership is a different stored row;
- (b) exclude it: rows with equal data and different provenance dedup to one.

**Rec:** (a). Provenance is part of what a stored row is, and the merged token is
deterministic. This unblocks S2.

**Q11. Where is "which tags are referential" persisted?** (Developer question via the
coordinator.)

*What the slice stores today:* see Appendix A, probe 4 (Delta store, plain against
referential run).

- The binding is not stored anywhere. It is a schema property (name in both
  halves, `referential_tags.py:91-101`). For a function node it lives only in the live
  `ReferenceFallThroughPod` (`references`, `function_pod.py:1774-1800`). Its pipeline
  identity carries it (`("fall_through_references", refs)`, `function_pod.py:1833-1836`).
- A node's read rebuilds the binding from the live pod:
  `reference_tags=fall_through_references(self._function_pod)` (`function_node.py:3008`).
- **Pipeline (tag) table (`pdb_v2`):** the referenced column is stored **once, as a user
  tag column** (`unit`, `time`), with no metadata, **plus its source-info column**
  (`_source_unit`, `_source_time`; the plain table has only `_source_amp`) (probe 4).
  Nothing else differs except the system-tag column names, which carry the source's
  pipeline hash.
  - The table's path is `pod.uri + ("schema:<node pipeline hash>",)`
    (`function_node.py:533`). The node pipeline hash includes the source's schema halves
    and the fall-through pod's `("fall_through_references", ...)`, so **the plain and the
    referential node write different pipeline tables**. Probe 4: `schema:a17770bc...`
    against `schema:ff350285...`. The binding is therefore constant within one table, and a
    table's path implies it, but nothing inside the table states it.
  - A reader recovers the data role from the live pod. That is why a bare read sees plain
    tags (M7).
- **Result table (`rdb_v2`):** one table, **shared** by the plain and the referential run.
  Probe 4: one `_result/f/.../rdb_v2` path with 2 rows, and the referential run computed
  nothing. Its columns are `__record_id`, `__input_data_hash`, the `__pf_*` columns, `y`,
  `_source_y`, `__pod_ts` and `__data_context`. It holds no tag, no fall-through column and
  nothing about referential status. It is written by `CachedFunctionPod` over the plain pod
  (`result_pod`) on `function_input(...)`, which removes references the function does not
  read. This already meets the developer's requirement that the result table never cares
  about referential status.

*Options for persisting the binding (pipeline side only):*

- (a) Arrow field metadata on the stored tag column (for example `{"orcapod.role":
  "reference"}`).
  - Read without the graph: yes.
  - Hashes: none. Probe 6: the data context's `arrow_hasher.hash_table` gives the same hash
    with and without `{"orcapod.role": "reference"}` on a column.
  - Stores: probe 6 shows the metadata survives a first write and read on
    `DeltaTableDatabase` and `InMemoryArrowDatabase`. Appends, schema merges and the
    database sources are not checked. ITL-846 found that not every operator path keeps
    field metadata.
  - Layout: metadata on an existing column is not a bookkeeping-column change, so `pdb_v2`
    can stay.
- (b) Table-level schema metadata (`orcapod.reference_tags = [...]`).
  - Read without the graph: yes.
  - It rides on table-creation metadata. Delta keeps table configuration, but appends
    from another writer can drop it.
  - Hashes: none.
  - Layout: arguably a bookkeeping change, so it would bump to `pdb_v3`.
- (c) Only in the saved job or pipeline config (the fall-through pod's `references` plus
  each source's `tag_only_columns`).
  - Read without the graph: **no**. A store read by itself, `as_source()` after a load
    without the pod, or `CachedSource` see plain tags.
  - No store change.
- (d) (c) plus (a): the config is the authority for a loaded job; the field metadata lets a
  bare store read recover the role and is checked against the config on load (refuse a
  disagreement).

*Readers that need it:*

- a loaded job (config is enough);
- `as_source()` / `DerivedSource` (needs the store, or the node's config);
- an operator node's stored output (needs the store, since an operator has no "fall-through
  pod" to ask);
- a read without the graph (needs the store).

**Rec:** (d). The binding is part of the node's pipeline identity, so the config must carry
it. The pipeline table carries it as field metadata on the referenced tag column, which
needs no new bookkeeping column and so no layout bump. Test the round trip on the
in-memory and Delta stores. The result table stays untouched.

**To decide before v0.4.0:**

- (c) or (d);
- if (d): the table hash ignores field metadata (probe 6). The stored-path hash is built
  from the node pipeline hash, not from the stored table, so neither moves. What still
  needs a test is appends and schema merges on Delta;
- whether an operator-node table uses the same mechanism (yes, for S2).

**Q12. Row identity of referential sources (from 4b).** Options:

- (a) keep today's row identifiers (positional `row_<n>`, `record_id_column`, SpiralDB's
  key hash) for referential sources too;
- (b) default the row identifier of a source with referential key columns to those columns.

**Rec:** (a) for v0.4.0, and pin no row-level referential identity until the
source-id/record-id review (ITL-1157, ITL-1158) decides. See 4b.

**Q13. Install path for the workshop.** Options:

- v0.4.0 on PyPI (the memory note: "ideally v0.4.0 with referential tags");
- a pinned commit in SkyPilot (developer, Nov 12, for the slice).

**Rec:** decide on Tuesday with the go/no-go. If v0.4.0 is not tagged by Wednesday, use the
pinned commit of the refreshed slice, with `ediop` pinned to the same commit.

**Q14.** Confirm two behaviours that only the slice decided:

- a source whose only data are referenced tags is valid (ITL-1010 C4);
- a list-valued referential key may fall through a pod with no size limit (ITL-1013).

**Rec:** confirm both. The second is rare on the head.

---

## 4. Interaction with work in flight on `main`

### 4a. The v1 hash definition (ITL-1154)

Rule (developer, Nov 15): "A hash always carries its label, except where it is written into
a name, whose grammar fixes which algorithm produced it." Source ids:

- `<algorithm>_v<N>:<hex>` for a dedicated algorithm;
- `<kind>:<label>:<hex>` for a general algorithm applied by an operation;
- "only source ids have a kind prefix" (branch rule 7; D3 final).

| Structure or fold the slice adds | Where | Verdict |
|---|---|---|
| `("reference_tags", refs)` | `arrow_table_stream.py:187` | No hash inside, only names. Compliant. The table digest next to it is a nested hash that the structure hasher embeds, so under rule 1 it gets its label with no slice change. |
| `(plain_pod, ("fall_through_references", refs))` | `function_pod.py:1833`, `:1836` | The plain pod enters as an object, not as `to_hex()` or `to_string()`. The structure hasher embeds its `ContentHash` (pipeline hash in the pipeline identity, through the resolver), so under rule 1 it carries its label. Compliant, no change needed. |
| `merged:source_fold_v1:<hex>` | `arrow_utils.py:1407-1440` | It is a stored value that also feeds a hash: the next fold, and `_record_hash` once S2 lands. It carries its label, which rule 1 requires. Built through `ContentHash(SOURCE_FOLD_LABEL, ...).to_string()`, with the label constant beside the algorithm. Compliant. **Grammar not compliant:** the `merged:` kind prefix on a non-source-id contradicts "only source ids have a kind prefix". The dedicated form is `source_fold_v1:<hex>` (Q4). |
| A nested merged token inside the next fold | same | Enters whole, label included. This is the developer's decision (Nov 13) and matches rule 1. |
| Member tokens `<source_id>::<row>::<col>` inside the fold preimage | same | These are names. Their source ids already carry `<kind>:<label>:<hex>`, and after ITL-1154 every default id carries its kind. Compliant, but **every merged token value moves with the ITL-1154 regeneration**. So pin merged tokens only after it (M4). |
| `GroupBy`'s folded system tag of a referential stream | `group_by.py` (existing fold) | ITL-1154 relabels it `group_v1:<hex>`. The slice's `group_by.py` changes must merge with that. |
| The hash-site trace test (ITL-1154 deliverable) | — | `fold_source_info_tokens` is a new `hashlib.sha256` site. It must be added to the inventory table as "stored value: carries its label", or the trace test fails after the merge. |

### 4b. Source ids, record ids and key columns

What the slice does: referential tags **do not change row identity**.

- A referential source has the same default `source_id`, the same record ids and the same
  source-info tokens as its plain twin (probe 2: equal `dict:arrow_v1:...` ids, tokens
  `<id>::row_0::unit`).
- The only new provenance is a source-info column for each referential key, plus the merged
  tokens built from those.

What referential tags imply:

- A referential tag *is* a key column kept as data, so a source with referential tags names
  its key columns explicitly. That makes key-based row identity (`record_id_column`
  defaulting to the referential tags, or SpiralDB's `arrow_v1` of the primary key for every
  database source) natural to express.
- But tags need not be unique (spec §18: "require human-readable tags to uniquely identify
  observations" is a non-goal). A key-based identifier would need a uniqueness check, or a
  refusal ("when in doubt, refuse"), per source.
- `DictSource`, `DataFrameSource` and `ListSource` do not accept `record_id_column` at all
  (ITL-1158).

What would be awkward to change after v0.4.0:

1. **Any referential identity pinned in a released golden file.** Released files never
   change (ITL-946); changing a pinned value needs a new data context. This includes:
   source content and pipeline hashes, record ids, source-info tokens of key columns,
   merged token values, the fall-through pod identity, and the function-node pipeline
   table paths.
2. **The record-id recipe of referential sources.** A function node's pipeline records are
   keyed by record ids derived from system-tag values. Moving to key-based ids re-creates
   every pipeline record; results are reused (spec §12.2), and associations are rebuilt.
3. **The merged token grammar and preimage**, once stored in user stores.
4. **The persisted binding (Q11).** If v0.4.0 stores nothing and v0.4.x adds field
   metadata, old tables read back as plain. A layout bump is avoidable only if (a) is
   chosen now.

Recommendation: keep today's identifiers for v0.4.0 (Q12 (a)). Pin only schema-level
referential identities in v0.4.0. Add row-level pins once the record-id review decides.
Because referential identities are new, a key-based recipe for referential sources could
still be introduced additively later, but only while no row-level referential value is
pinned.

### 4c. Golden pins (ITL-946)

No referential identity is pinned today. To be added with `--add` into a new
`tests/test_hashing/golden/data/v1/<group>-NN.json` after ITL-1154's regeneration
(no data context change; the plain fixtures stay `tag_only_columns=True`):

1. Referential `DictSource`, `ArrowTableSource`, `CSVSource`, `DataFrameSource` and
   `DeltaTableSource`: content hash, pipeline hash, output schema hash. The default
   `source_id` too: equal to the plain one, which is worth pinning as an equality.
2. A database source (SQLite fixture) and the SpiralDB schema path: the same.
3. `ArrowTableStream(..., reference_tags=...)` content hash (identity tuple with
   `("reference_tags", ...)`).
4. `Join` in the three cases (plain+plain is already pinned): output pipeline hash, content
   hash, system tag column names.
5. `GroupBy` on a referential key and with a referential non-key: output pipeline hash and
   predicted schema.
6. `SelectDataColumns` / `DropDataColumns` materializing a reference: output pipeline hash.
7. `MapTags` both roles, `MapTags(tag_only=True)`, `MapData` on a referential column:
   operator identity and output pipeline hash.
8. `ReferenceFallThroughPod`: content and pipeline hash. The function node's pipeline table
   path. Equality of the result identity (`__input_data_hash`) with the plain pod's, for a
   function that does not read the reference (§12.4 as a golden equality, not only a test).
9. `fold_source_info_tokens` on fixed member lists: one member, several, a missing member,
   a nested merged token (the algorithm pin; after Q4).
10. Row level, **after Q12**: record ids of a referential source; source-info tokens of a
    key column; merged token values of a `GroupBy` and of a `Join`; the tag and data
    datagram hashes of a referential row.

**Does "the binding enters an identity only when a reference exists" still hold
everywhere?**

Yes for sources (through the schema halves), `ArrowTableStream`, `Join`, `GroupBy`, select
and drop, and the pod call (the fall-through pod is built only when references exist,
`function_pod.py:774`). Plain identities are unchanged (all golden entries pass).

There is **one exception**: `MapTags(tag_only=True)` puts `("tag_only", True)` in its
identity whether or not the stream has a reference (probe 2: same table, different pipeline
hash). See Q1b.

---

## 5. Workshop risks if v0.4.0 ships with the slice's gaps

What an attendee following the docs or the tutorial would hit:

1. **First examples teach the opt-out.** `docs/index.md` and `docs/getting-started.md` open
   with `tag_only_columns=True`. The default is explained only on
   `concepts/referential-tags.md`.
2. **v0.3 habits fail** (probes):
   - `pod.process(src)` → `ValueError ... column 'unit' is not a parameter of the
     function`, with no mention of referential tags or of `pod(src)`;
   - `src.batch(n)` and `MergeJoin` → `NotImplementedError`;
   - `job.save(path)` → `NotImplementedError`;
   - `node.as_source()` → `NotImplementedError`.
3. **Re-binding a v0.3 saved job** with a default source → `SourceSpecMismatchError: ...
   unexpected data columns: ['unit']`. The message does not say `tag_only_columns`.
4. **Silent empty join.** Two time series that both have a referential `timestamp` are
   joined on it and give no rows unless one is renamed (ITL-1021). The in-repo EDI test
   renames with `map_tags`. The docs have no warning.
5. **Performance and store size.** On `4473e2ef` the store was 350 MB against 10 MB, and
   the notebook took 724 s against 239 s. The head should remove both causes, but **it has
   not been re-measured** (M6). This is the largest unknown for go/no-go.
6. **Warning noise.** `ReferenceMaterializationWarning` is on by default. The in-repo EDI
   test silences all warnings, so how loud a notebook run is has not been checked.
7. **Keys printed twice** in `keys()` and schemas. The `StreamView` marks are not in the
   slice (S5).
8. **Throwaway stores.** Referential identities are not frozen, and ITL-1154 moves every
   hash. Stores written during the workshop are not reusable by the final v0.4.0. Say so in
   the tutorial.
9. Pre-existing on `main`, not caused by the slice: the async `Join` `TypeError` on a
   list-valued shared tag (ITL-1044); `PollingSource` record-id collisions (ITL-1158).

---

## 6. Proposed plan

Order of gates:

1. v0.3.0 tag (Monday);
2. ITL-1154 merged (Monday);
3. slice refreshed (W0, in flight) and PR #342 merged to `main` after its review rounds;
4. W1 to W6 PRs against `main`;
5. go/no-go on Tuesday evening;
6. release PR on Wednesday, with the tag and PyPI only on the developer's go.

Workers start from the refreshed slice head on Monday and rebase onto `main` once #342 is
merged.

| Owner | Work | Size | Parallel with | Waits for |
|---|---|---|---|---|
| W0 (in flight) | Refresh the slice onto `main` (and ITL-1154 when merged); add `fold_source_info_tokens` to the hash-site inventory; mark RT2 resolved | M | — | v0.3.0 tag, ITL-1154 |
| W1 | ITL-1019 persistence: save/load with referential tags, meaning pinned by format version, fall-through pod config, binding in the pipeline table per Q11 | L | W2 to W5 | **Q11**, Q8 |
| W2 | ITL-1017 `Batch` per Q5 (b), sync and async, async-equivalence entry | M | W1, W3 to W5 | **Q5** |
| W3 | `DerivedSource` over a referential stream (Q7) and operator-node storage (Q10); stretch: may slip past v0.4.0 | M | W1, W2, W4, W5; shares Q11 with W1 (same mechanism, coordinate) | **Q7, Q10, Q11** |
| W4 | Docs and UX: first examples on the default; time-series join warning; error messages name the remedy (S3); `StreamView` `~name` (S5); `CLAUDE.md`, `.zed/rules`, `orcapod-design.md`; operator-registry test; tests in both modes (S6, S7) | M | all | Q8, Q2, HTML style |
| W5 | `ediop` re-measurement on the refreshed head (M6): store size, run time, warnings; Spiral scratch tables of the tutorial project only | S | all | W0 |
| W6 | Golden pins (4c items 1 to 9, and 10 if Q12 is decided); the ITL-1011 C5 statement; merged token grammar (Q4) | M | W1 to W5 | ITL-1154 merged, W0, **Q4**, Q12 |

**Decisions needed Monday morning to start on time:** Q5 (`Batch`), Q11 (binding
persistence), Q4 (token grammar), Q8 (keep the default). Q7 and Q10 can follow on Monday
for W3. Q1, Q2, Q3, Q6, Q9, Q13 and Q14 can be confirmed by Tuesday.

**Go/no-go inputs (Tuesday evening):**

- W5's measurements;
- whether W1 and W2 are merged;
- the remaining "not supported yet" list, expected to be `MergeJoin`, plus `DerivedSource`
  and operator-node storage if W3 slips.

Fallback: the pinned slice commit for the workshop (Q13), with v0.4.0 after the workshop.

---

## Appendix A. Probes (slice head `634ad40a`, Python 3.12, `uv run`)

Probe 1 (behaviour on default sources):

```
pipeline_hash differs ref vs plain: True; content_hash differs: True
scale.pod.process(ref): ValueError: ... column 'unit' is not a parameter of the function
scale.pod(ref): (('unit','time'), ('unit','time','scaled'))
ref.batch(1): NotImplementedError: Batch: not supported with referential tags yet (ITL-1017)
MergeJoin()(ref, ref): NotImplementedError (ITL-1018)
plain run then ref run on one store: plain 2 calls, ref 0 calls
job.save (ref): NotImplementedError: Saving a pipeline ... (ITL-1019)
node.as_source() (ref): NotImplementedError: DerivedSource ... (ITL-1019)
group_by on a referential key: _source_unit = ['merged:source_fold_v1:<hex>', ...]
```

Probe 2 (identities):

```
default source_id ref == plain: True (dict:arrow_v1:<hex>); token <id>::row_0::unit
MapTags(tag_only=True) on a plain stream: pipeline hash equal? False; tables equal? True
join ref+ref commutative content hash: True; _source_unit = merged tokens
all-tag referential source: keys (('a',), ('a',)); all-tag plain: ValueError (no data column)
DropNulls on a nullable referential tag: (Schema({'k': int}), Schema({'k': int, 'v': float}))
```

Probe 3 (bind): a plain job saved and loaded; binding the plain source works; binding the
referential twin raises `SourceSpecMismatchError ... unexpected data columns: ['unit']`.

Probe 4 (stored tables: Delta store, `f(amp)` run through a job on the plain source, then
on its referential twin, on the same store):

```
plain node keys (('unit','time'), ('y',));  ref node keys (('unit','time'), ('unit','time','y'))
pipeline/_result/f/<uri hash>/v0/python.function.v0/rdb_v2            rows 2 (shared)
  cols: __record_id, __input_data_hash, __pf_var_* (4), __pf_var_git_hash, __pf_exec_* (4),
        y, _source_y, __pod_ts, __data_context
pipeline/f/<uri hash>/v0/python.function.v0/schema:a17770bc...  (plain)   rows 2
  cols: __record_id, unit, time, _tag_source_id::5df6..., _tag_record_id::5df6...,
        _source_amp, __data_id, __node_content_hash, __input_data_hash, __output_data_hash,
        __data_context, __computed, __is_ephemeral, __pipeline_base_entry_id,
        __pipeline_recomputation_index
pipeline/f/<uri hash>/v0/python.function.v0/schema:ff350285...  (referential)   rows 2
  cols: as plain, with _tag_*::b81f... and _source_unit, _source_time added; no field metadata
```

Probe 6 (field metadata): `arrow_hasher.hash_table` is equal with and without
`{"orcapod.role": "reference"}` on a column; that metadata reads back unchanged from
`DeltaTableDatabase` and `InMemoryArrowDatabase` after a first write.

The result table was written once (the referential run computed nothing). The two pipeline
tables differ by path, and in content only by the source-info columns of the referenced
keys. Separately, plain and referential Delta jobs both exit cleanly (each about 1 s).
