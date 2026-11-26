# ITL-1470: a stream built before a job gets other system-tag column names

## What was reproduced

On `main` at 5eb327ad. `units` is a `DictSource` with tags `s`, `u` and data `x`, `y`
(`source_id="units"`); `add` is a function pod over `x`, `y`.

| Chain before the pod | Built | Job nodes | System-tag columns of `add` | Content hash | Pipeline hash |
|---|---|---|---|---|---|
| `select_tag_columns(["s"])` | before the job | `SelectTagColumns:<hash>`, `add` | `...::5c54...` | `0050...` | `bc73...` |
| `select_tag_columns(["s"])` | inside the job | `SelectTagColumns`, `add`, `units` | `...::567d...` | `0050...` | `0c0f...` |
| `map_tags({"s": "site"})` | before | `MapTags:<hash>`, `add` | `...::ea17...` | `99c6...` | `db3d...` |
| `map_tags({"s": "site"})` | inside | `MapTags`, `add`, `units` | `...::567d...` | `99c6...` | `64c0...` |
| select then map | before | `MapTags:<hash>`, `add` | `...::b99c...` | `4c36...` | `d4fc...` |
| select then map | inside | `MapTags`, `SelectTagColumns`, `add`, `units` | `...::567d...` | `4c36...` | `6747...` |
| `join` with a second source | before | refused (`SourceSpecMismatchError`, ITL-1126) | | | |
| `join` with a second source | inside | `Join`, `SelectDataColumns`, `add`, both sources | two pairs | `2464...` | `4463...` |
| none (the source itself) | before or inside | `add`, `units` | `...::567d...` | `8f5e...` | `7817...` |

The plain stream outside any job always equals the "inside" row.

The content hash is equal in both forms, the pipeline hash is not. So the two forms write
their pipeline table at two paths (`.../schema:<pipeline hash>/pdb_v1`), each with its own
column names; they share the result table (same path), which holds no system-tag column.
No single table gets two sets of column names, but one node identity (content hash) has two
pipeline tables with different column names.

## Cause

`Pipeline.compile` makes every stream that has no recorded producer in the job a leaf:
`SourceJobNode.from_stream(stream)`. A stream built before the `with` block has a producer
(the select), but that step was not recorded, so the stream itself becomes a source slot
named `<label>:<content hash>`.

A slot then does what a slot must (ITL-1112, `_DeclaredForm`): it declares the schema of the
stream it was made from (tags `s`; data `x`, `y`) and presents its bound source's rows under
the system-tag column NAMES of that declared schema's hash (`5c54...`). The stream's own
names are those of the source's schema (`567d...`, tags `s`, `u`), kept by the select
(rule 1, name-preserving). The slot's content identity delegates to the bound stream, so the
content hash is unchanged, while its pipeline identity is the declared schema alone, so the
pipeline hash moves.

Neither rule is wrong. The slot rule is right for a real slot. What is wrong is that a
stream with a producer was made a slot without the user asking.

## Which name is right

`...::567d...`, the one built inside the job and the one of the plain stream: a
single-stream operator keeps the system-tag column name of its input (rule 1), and the
pipeline written step by step is the reference. `...::5c54...` is right only for an
explicit slot or an explicit `as_source()`, where the user declares a new root.

## Relation to PR 500 (ITL-1387)

PR 500 (open, head edb0e6d8, CI green, design approved 2026-11-22) removes exactly this
cause: a stream given to a step in a `with` block brings its pod and every upstream step
down to its sources; the implicit `<label>:<hash>` slot is gone, and `from_stream` of a pod
stream needs an explicit name.

Checked by running the table above on PR 500's head: every "before" row equals its "inside"
row in node names, system-tag columns, content hash and pipeline hash, and the joined
pre-built stream works. `generate_golden_hashes.py --check` matches there. PR 500 merges
with current `main` without conflict (`git merge-tree`); `main` is five commits ahead of it.

So PR 500 fixes ITL-1470. A second, separate fix on this branch would rewrite the same walk
in `pipeline/base.py` and conflict with it.

## Proposed fix

Land PR 500 as the fix. This branch adds only what the issue asks beyond it:

1. A regression test, `tests/test_pipeline/test_stream_built_before_job.py`: a source with
   two tags and two data columns; chains select, map, select then map, join, and join then
   select then map, each built before the job and inside it; sync and async; compares node names, system-tag column
   names, rows, content and pipeline hashes, and stored table paths. It fails on `main` and
   passes on PR 500.
2. `changelog.d/ITL-1470.md` (Fixed).
3. No library code and no docs page of its own: PR 500 already adds "Streams built before
   the job" to `docs/concepts/pipeline-job.md`.

Decided (coordinator, 2026-11-26): PR 500 is the fix, with no second library change. This
branch has PR 500's branch merged in, and its pull request is stacked on PR 500 (its base is
PR 500's branch, so its diff is the test alone) until PR 500 lands; then it goes against
`main`. No design-issues entry.

## What moves

Only the affected case moves: a job in which a step inside a `with` block takes a stream
that a pod produced before the block. For such a job:

- job nodes: the slot `<label>:<hash>` is gone; the upstream steps and the real sources are
  nodes (and their results are stored on `run()`);
- stored column names: the system-tag columns of every node downstream of the former slot
  take the source's names (`...::5c54...` becomes `...::567d...` in the example);
- pipeline hash of those nodes, and so their pipeline-table path (`schema:bc73...` becomes
  `schema:0c0f...`);
- unchanged: content hashes, the function result-table path and its rows, record ids'
  values, source ids.

Nothing else moves: a job built in one block, a source used directly, an explicit slot, a
bind, `as_source()`. No golden value moves (`--check` matches on PR 500's head). No user has
a store written in the affected form.

## Not in scope

- Row-id code (`function_node.py`, `side_effects.py`, `operator_node.py`): ITL-1471.
- Edge registration: ITL-1472.
