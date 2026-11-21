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
