# ITL-939 design spike plan: a tag and a data column sharing a name

> **For agentic workers:** this is a design spike. No library code changes. Execute inline,
> task by task; tick the boxes and commit after each task so a successor can continue.

**Goal:** a design document (`superpowers/specs/2026-11-11-itl-939-tag-and-data-same-name-design.md`)
that establishes, by running code, what Orcapod does today when a value is wanted both as a tag
and as data, compares four designs, and recommends the smallest safe feature.

**Architecture:** throwaway experiment scripts under `superpowers/specs/itl-939-experiments/`
drive only the public API (sources, stream methods, operators, `function_pod`) on a small
EDI-shaped table (`session`, `experiment`, keys `subject_id`, `session_start_datetime`,
`experiment_index`). Each script prints what it observes; the outputs are pasted into the doc.

**Tech Stack:** orcapod (this repo), pyarrow, `uv run`.

---

### Task 1: Experiment 01, what happens today with a shared name

**Files:** Create `superpowers/specs/itl-939-experiments/exp01_same_name_today.py`

- [ ] Build an Arrow table with duplicate field names (`session_start_datetime` twice) and pass
      it to `ArrowTableSource` with the name in `tag_columns`; record the error or behaviour.
- [ ] `MapData({"x": "<tag name>"})` and `MapTags({"<tag>": "<data name>"})` collisions.
- [ ] `Join` of two streams where one stream's tag name equals the other's data name.
- [ ] `function_pod` whose output key equals a tag name.
- [ ] Run with `uv run python ...`; save stdout to `exp01_same_name_today.out.txt`.

### Task 2: Experiment 02, today's workarounds on the EDI-shaped table

**Files:** Create `superpowers/specs/itl-939-experiments/exp02_workarounds_today.py`

- [ ] W1 choose `tag_columns` so the column stays data.
- [ ] W2 duplicate the column under another name before building the source.
- [ ] W3 read the source twice (once with the column as data), join.
- [ ] W4 anything else found (e.g. `MapTags` cannot copy; `PolarsFilter` sees both).
- [ ] For each: compute elapsed time (`experiment_start - session_start_datetime`) with a pod,
      then Join with a second table keyed on the same columns; record lines, renames,
      `pipeline_hash` / `content_hash` changes and downstream Join behaviour.
- [ ] Save stdout to `exp02_workarounds_today.out.txt`.

### Task 3: Experiment 03, prototype of the smallest feature (design C), out of tree

**Files:** Create `superpowers/specs/itl-939-experiments/exp03_copy_tag_prototype.py`

- [ ] A throwaway subclass of `UnaryOperator` defined in the script (not in `src/`) that copies
      named tag columns into the data group under chosen names, with source-info for the copy.
- [ ] Show `keys()`, `output_schema()`, `as_table()`, `iter_data()`, a pod reading the copy,
      a downstream Join keyed on the original tags, and that streams not using it keep their
      hashes.
- [ ] Save stdout to `exp03_copy_tag_prototype.out.txt`.

### Task 4: Need analysis from the EDI schemas

- [ ] List key columns that are plausibly computation inputs; 3-5 workshop computations.

### Task 5: Design document

- [ ] Sections: today (1), need (2), designs A-D table + prose (3), recommendation and numbered
      decisions (4). Commit and push WIP.

### Task 6: Report and PR

- [ ] `ask` the coordinator with the summary; wait for the reply.
- [ ] Append the DESIGN_ISSUES.md entry for ITL-939 at the end.
- [ ] Merge origin/main, push, open docs-only PR ("Part of ITL-939"), attach to Linear,
      `orca worktree set --pr`. CodeRabbit rounds. `worker_done`.

## Progress notes

(successor: read this section first)
