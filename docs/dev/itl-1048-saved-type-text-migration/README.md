# Saved type text migration (for the migration tool, ITL-1048)

This branch keeps, as PR 454 (ITL-640) had it at commit `73652317`, the code that read
saved pipeline and job files of format 0.1.0 to 0.3.0. PR 454 then removed it from the
library: Orcapod reads only the current format (0.4.0) and refuses older files at load,
because load and save had not been used in production (developer, 2026-11-21). The code
is kept here for the separate migration tool (ITL-1048), which takes stores and saved
files of the v0.1.4 era to the current format. This branch has no pull request and is
not to be merged.

## What the code does

Format 0.4.0 writes a nested field name (a list item, a struct child, a map key or item)
as it is when it is plain (`[A-Za-z0-9_.\-]+`) and as a JSON string otherwise
(`"a:b"`, `""`). Formats 0.1.0 to 0.3.0 wrote every nested name unquoted, so a quote
there is part of the name. The migration rewrites the saved type texts of such a file
into the 0.4.0 grammar, by text, each name as the earlier reader read it, so the 0.4.0
reader never has to guess which grammar a text uses. The format version of the file
decides; the content of a text never does.

It rewrites only the known places, along the known edges, and never a value the file
format does not own (an implementation's own `impl_config`, any other user key):

- every node's `output_schema` (`tag`, `data`);
- a source slot's `source_config` (`tag_schema`, `data_schema`);
- each built-in source kind's own schema field (`SOURCE_CONFIG_SCHEMA_TEXTS`: a
  `CSVSource`'s `schema.columns`);
- source configs nested along `NESTED_SOURCE_CONFIG_KEYS` (`inner_source` of a cached
  source, `origin` of a derived source);
- a job file's bound source configs (`bindings.sources`); the embedded pipeline is
  rewritten with its own version.

## Where it is

`src/orcapod/pipeline/serialization.py`:

- `UNQUOTED_NAME_FORMAT_VERSIONS`: the formats with unquoted nested names.
- `type_text_at_version(text, version)`: one type text in the 0.4.0 grammar.
- `_texts_at_version`, `source_config_type_texts_at_version`,
  `pipeline_type_texts_at_version`, `job_type_texts_at_version`: the known places.
- `SOURCE_CONFIG_SCHEMA_TEXTS`, `NESTED_SOURCE_CONFIG_KEYS`.
- The context variable `orcapod_saved_text_quoted_names` that turns the quote-aware
  reading off while an old text is read.

Called from `Pipeline.load` (`src/orcapod/pipeline/graph.py`) and `PipelineJob.load`
(`src/orcapod/pipeline/job.py`). The plain-tag upgrade of 0.1.0 and 0.2.0 files
(`PLAIN_TAG_FORMAT_VERSIONS`, `source_config_at_version`; ITL-1019, ITL-1159) is in the
same module.

Tests, in `tests/test_hashing/test_logical_type_forms.py`:
`test_an_earlier_files_unquoted_names_still_read`,
`test_a_bare_map_text_of_an_earlier_file_still_reads`,
`test_older_type_text_reads_as_the_earlier_reader_read_it`,
`test_the_format_version_decides_how_a_text_is_read`,
`test_only_the_known_places_of_an_older_file_are_rewritten` and
`test_an_older_pipeline_file_loads_its_quoted_names_exactly`. The plain-tag upgrade is
tested in `tests/test_pipeline/test_saved_job_before_data_tags.py` and
`tests/test_pipeline/test_saved_job_with_data_tags.py`.

The Codex check of this code (1,479 comparisons of 493 old schemas under three old
versions, exact; 1,246 new-format round trips) is recorded on PR 454.

## Files without the nullability and run content hash records

PR 454 also removed (after this branch's first commit) the lossy load of files that lack the
records Orcapod writes since ITL-1043 and ITL-1094: which columns may be null, and the
content hash of each node's run. The library now refuses such a file and names the missing
record. The code below, as it is on this branch (commit `73652317`), loaded such files
lossily. It shows the migration tool what a file of that era lacks, and how the loader
treated each gap.

- Missing nullability, `src/orcapod/pipeline/graph.py`, `Pipeline.load`: the
  `unrecorded_output_nullability` and `unrecorded_source_nullability` sets (a node
  restored with every column non-null), the source config falling back to the output
  schema's record, and `Pipeline.save` leaving the record out again for those nodes. The
  `None` nullability of a slot's saved texts is in `src/orcapod/core/nodes/source_node.py`
  (`_keep_saved`, `_saved_type_texts`).
- Missing run content hash, `src/orcapod/pipeline/job.py`, `PipelineJob.load` and `save`:
  a node without an entry in `run.node_job_content_hashes` is loaded with its descriptor's
  (blueprint) hash, and `save` leaves an operator node's entry out.
  `src/orcapod/core/nodes/operator_node.py`: `_LoadedCacheUnreadableError` and
  `_read_run_records`, where a loaded operator node without its run hash refuses an empty
  answer, and `_run_content_hash` returning `None`.
  `src/orcapod/pipeline/loaded_side_effects.py`: `LoadedSideEffectJobNode` falling back to
  the descriptor's hash.
- Tests: `tests/test_pipeline/loaded_job/test_loaded_operator_cache.py`
  (`test_without_saved_nullability_the_operator_still_reads_its_runs_table`,
  `test_without_the_run_content_hash_*`,
  `test_saving_a_lossy_loaded_job_again_records_nothing_it_does_not_know`,
  `test_saving_again_drops_only_the_records_its_file_lacked`,
  `test_a_source_node_without_its_own_record_takes_its_output_schemas`, the real format
  0.1.0 fixture test) and `tests/test_pipeline/loaded_job/test_loaded_side_effect_nodes.py`
  (`test_a_file_without_recorded_nullability_restores_side_effect_nodes`,
  `test_a_job_file_without_the_recorded_hash_restores_the_descriptor_hash`). The fixtures
  `tests/test_pipeline/fixtures/job_format_0_1_0.json` and `saved_job_37716c76/` (with
  their maker scripts) are real files of formats 0.1.0 and 0.2.0, written by earlier
  Orcapod commits.

The generic `DataFunctionProxy` fallback for a function type id this Orcapod does not know
(such as `python.function.v0`) and the detection of stored tables of other layouts stay in
the library. Loading any saved file whose function cannot be imported reaches the first,
and a live node reading its database reaches the second.
