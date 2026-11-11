# EDI tutorial dataset (`edi-tutorial-v1`)

The Orcapod workshop reads EDI tables from SpiralDB. This directory holds the tool that builds
its Spiral project: a small subset of the V1 EDI sample (`edi-sample-v1-855632`, dataset
`default`), with every column declared non-nullable where the
[EDI schema specification](https://github.com/enigma-brain/enigma-data-interface)
(`edi_schema_spec.md`, version 0.3.0) requires a value.

**Status: draft (ITL-933).** The project has not been created yet; its ID goes here once it is.

## What is in it

One subject, one session, one recording, one probe and one FreeViewing experiment, as in the
sample, cut down to:

* **one window of 143.9 s**: µs `[5226928854, 5370814965)` relative to
  `session_start_datetime`, from the start of trial 215 to the end of trial 225 (11
  consecutive valid trials). Every `timestamp` in EDI uses that one clock, so the same window
  applies to every time-keyed table;
* **32 units**: the units with the most spikes in the window, among those whose
  `labels.bombcell_label` is not `noise`;
* the trials that overlap the window, plus any trial a kept row references;
* every other table whole, except `sample_info`, which describes the whole sample
  (`n_units` 100, its unit filter and sources). `copy` records the source's `sample_info`
  row in the provenance metadata of `meta.edi_tutorial_build` instead, with the source
  project, window, units, trials and row counts.

Values are copied unchanged (developer, November 11, 2026). Columns that summarize the
whole sample therefore still describe the whole sample, not the subset:

* `experiment`: `total_trials` (873), `correct_trials` (619), `incorrect_trials`,
  `experiment_start_timestamp`, `experiment_stop_timestamp`;
* `recording`: `recording_start_timestamp`, `recording_end_timestamp`;
* `session`: `session_end_datetime`;
* `lfp_acquisition`: `lfp_n_samples` (6,866,378), `recording_start_timestamp`;
* `unit`: every `quality_metrics` field (for example `num_spikes`, `firing_rate_hz`,
  `presence_ratio`), the `rolling_quality_metrics` series over the whole session, and the
  `waveform_metrics`;
* `sample_unit`: `firing_rate_hz`, `presence_ratio`, `fano_factor_detrended`.

About 205,000 rows and 361 MB uncompressed. The largest tables are `lfp_timeseries` (86,331
rows, 154 MB) and `experiment_screen_capture_frame_270p` (4,312 JPEG frames, 177 MB).
`lfp_timeseries`, `experiment_display_params`, `experiment_freeviewing_params` and
`experiment_screen_capture_event` hold `fixed_size_list` columns, which Orcapod sources
cannot hash until starfix supports the type (ITL-927).

## Nullability

`nullability_map.json` records, for every column, the source Arrow type, the
specification's type and the nullability declared in the project. Key columns are always
required. Types are the sample's, exactly; only nullability changes. Three decisions
(developer, November 11, 2026) cover what the specification leaves open; they live in
`decisions.json`, which `derive-map` applies:

* Tables the specification does not define (`experiment_screen_capture_frame_270p`,
  `sample_info`, `sample_unit`): key columns required, other columns nullable, as in the
  source.
* The four column groups of `unit` (`labels`, `quality_metrics`, `rolling_quality_metrics`,
  `waveform_metrics`) are required structs (SpiralDB refuses a null struct anyway); their
  fields stay nullable, as the specification says.
* One reference in the source does not resolve and is copied unchanged: 58 of the 383
  `lfp_location` rows (channels spread over 0–383) have `atlas_label_region_short` =
  `atlas_label_region_full` = `White Matter` for atlas `AFNI_D99v2`. `atlas_label` holds 198
  regions for that atlas (index -1 `Skull`, then 2–224) and none for white matter. `plan`
  and `verify` report this reference as an accepted exception instead of failing. The
  developer has raised it with the EDI owners.

Every required column holds no nulls in the source, key columns included. In the window,
`lfp_voltage_uv` has one null element per row (a channel removed in preprocessing), which
the specification allows (`fixed_size_list(f32?)[384]`).

Print the full per-column table with:

```bash
uv run python workshop/edi_tutorial_dataset/edi_tutorial_dataset.py render-map
```

| Table | Selection | Rows | Key columns | Required values | Nullable values |
| --- | --- | ---: | ---: | --- | ---: |
| `atlas` | whole | 1 | 1 | `atlas_name` | 3 |
| `atlas_label` | whole | 198 | 2 | — | 3 |
| `chamber` | whole | 2 | 2 | — | 2 |
| `experiment` | whole | 1 | 4 | `task_id`, `task_version` | 5 |
| `experiment_display_params` | whole | 1 | 4 | — | 9 |
| `experiment_freeviewing_event` | window | 4,202 | 6 | `trial_index` | 0 |
| `experiment_freeviewing_params` | whole | 1 | 4 | — | 15 |
| `experiment_freeviewing_trial` | trials | 11 | 5 | `trial_start_timestamp` | 37 |
| `experiment_gaze_online_timeseries` | window | 28,780 | 5 | — | 8 |
| `experiment_screen_capture_event` | window | 17,249 | 6 | — | 15 |
| `experiment_screen_capture_frame_270p` | window | 4,312 | 7 | — | 1 (`frame_jpeg`) |
| `lfp_acquisition` | whole | 1 | 5 | `lfp_channel_ids`, `recording_start_timestamp` | 3 |
| `lfp_location` | whole | 383 | 9 | — | 5 |
| `lfp_timeseries` | window | 86,331 | 6 | `lfp_voltage_uv` | 0 |
| `penetration` | whole | 1 | 4 | `probe_id` | 11 |
| `probe` | whole | 22 | 1 | — | 3 |
| `probe_location` | whole | 1 | 6 | — | 4 |
| `probe_map` | whole | 1 | 4 | — | 6 |
| `recording` | whole | 1 | 3 | — | 5 |
| `sample_unit` | units | 32 | 6 | — | 4 |
| `session` | whole | 1 | 2 | `booth`, `session_end_datetime` | 3 |
| `spike_event` | window + units | 63,349 | 7 | `spike_amplitude_uv` | 0 |
| `subject` | whole | 1 | 1 | — | 3 |
| `task` | whole | 1 | 2 | — | 3 |
| `unit` | units | 32 | 6 | 4 column groups (structs) | 56 fields in the groups |
| `unit_location` | units | 32 | 9 | — | 5 |

## Building the project

Every command needs `pyspiral` 0.16.33 or newer (older versions do not use the `spiral login`
credentials) and a `spiral login`. Run them from the repository root. `plan` and `verify` only
read; `create`, `probe` and `copy` write, and only to a project `create` made.

```bash
TOOL="uv run --with pyspiral==0.16.33 python workshop/edi_tutorial_dataset/edi_tutorial_dataset.py"

$TOOL plan                                  # read-only dry run: rows, bytes, nulls, foreign keys
$TOOL create --id-prefix edi-tutorial-v1 --allow-create edi-tutorial-v1 \
    --volume vol_01m6s4jsjeedya88dc8t2ns96q \
    --description "edi-tutorial-v1 · edi-sample-tutorial · ..."   # prints the new project ID
$TOOL probe --target-project ID --allow-writes ID   # how SpiralDB treats non-null columns
$TOOL copy --target-project ID --allow-writes ID --dry-run
$TOOL copy --target-project ID --allow-writes ID
$TOOL verify --target-project ID
$TOOL orcapod-check --target-project ID     # Orcapod sources: required columns are T, not T | None
```

The project uses the Volume every `edi-sample-*` project uses
(`vol_01m6s4jsjeedya88dc8t2ns96q`, the `edi-sample-datasets` bucket); `edi-sample-tutorial`
is named in its description.

Safety rules:

* The tool reads only from `--source-project` (default `edi-sample-v1-855632`).
* It writes only to `--target-project`, which must be repeated as `--allow-writes`, must
  differ from the source and must not start with `edi-sample-v`.
* The target must hold the marker table `meta.edi_tutorial_build`, which only `create`
  writes, into the project it has just made. Every table the tool writes carries the same
  marker in its metadata, and a target holding any table without it is refused.
* A null in a column declared non-nullable stops the copy of that table (its transaction is
  aborted). Nothing is coerced or cast.
* `copy` refuses to run while the map has pending decisions.
* `create` refuses an ID prefix starting with `edi-sample-v`.
* Re-running `copy` skips tables already complete for the same map and subset, and rewrites
  an incomplete one. A complete table built from a different subset is refused.

`verify` checks that the project holds exactly the subset's tables, that each table has the
source's types and the map's nullability, that its key columns are the source's (in order,
non-null), that every foreign key resolves, and that every row equals the source.

To rebuild the map from the specification (reads the source's schemas):

```bash
$TOOL derive-map --spec path/to/edi_schema_spec.md --spec-version 0.3.0 \
    --output workshop/edi_tutorial_dataset/nullability_map.json
```

The tests (`tests/test_workshop/`) run the tool against an in-memory fake of pyspiral.
