# Orcapod on starfix 0.6 (Rust) with float normalization: plan

Issues: ITL-1179 (move to the Rust starfix), ITL-1251 stage 2 (hash -0.0 as 0.0 and every NaN
as the ordinary NaN). Branch `eywalker/itl-1179-orcapod-040-migrate-from-starfix-python-to`.

The developer's three requirements (2026-11-19): use the latest starfix; test -0.0 against 0.0
and NaN specifically; every other hash stays exactly what it was under starfix 0.5.0.

Three stages, one commit (or a small group of commits) each; the coordinator reviews stage 1
before stage 2 starts.

## Stage 1: switch the implementation, float options off, prove nothing moved

1. `pyproject.toml`: `starfix>=0.6.0,<0.7`; `uv lock --upgrade-package starfix` (only starfix
   changes in `uv.lock`); `contexts/data/v1.json` `libraries.starfix` names 0.6.0.
2. Call sites (`hashing/arrow_hashers.py`, `logical_types/polars_type.py`,
   `core/sources/stream_builder.py` comment): the 0.6.0 package keeps starfix-python's
   interface (`ArrowDigester.hash_schema`, `hash_table` with the same keywords), so only
   docstrings and comments that name the implementation change.
3. Proof A: `generate_golden_hashes.py --check` (every group) reports nothing moved, and the
   full `--stdout` dump (every computed entry, pinned or not) is identical under 0.5.0 and
   0.6.0; the test files that pin digests outside the golden folder pass.
4. Proof B: `docs/dev/scripts/starfix_differential_corpus.py` builds a broad deterministic
   corpus of Arrow tables and schemas and hashes each through starfix directly (every option
   combination Orcapod uses) and through Orcapod's `StarfixArrowHasher`; it writes
   `name -> digest or error class`. Run once with starfix 0.5.0, once with 0.6.0, and diff.
   The only acceptable differences are the ones starfix 0.6.0 documents against 0.5.0
   (`python/tests/_harness.py` `EXPECTED_DIFFERENCES`: ITL-1223 and two invalid or
   bufferless dictionary inputs).
5. Proof C: refusals keep their class and the messages Orcapod matches ("Ambiguous node
   path", null-typed data, `list_view`, map); `hash_table` timing on a 1e6-row table under
   each version.
6. Proof D: the 0.6.0 wheel list against Orcapod's matrix.
7. Report to the coordinator and wait.

## Stage 2: float normalization on

1. `StarfixArrowHasher`: `normalize_negative_zero=True, normalize_nan=True` on every starfix
   call that digests data (`hash_table`, `hash_table_ordered`); the Polars value digest too.
   Both are unconditional (not ignore-list options): they make the hash agree with Orcapod's
   value equality (Polars matching), which no data context may turn off.
2. Semantic hasher: a Python `float` -0.0 hashes as 0.0, every NaN as the ordinary NaN, at
   every depth (containers, dataclasses, signature defaults). Library value hashes (numpy,
   torch, pandas, polars): each one's route is stated (starfix or its own bytes) and made to
   agree.
3. Tests (several items, every float width, every nesting, tags and data, sync and async):
   equal hashes for 0.0 / -0.0 and for every NaN bit pattern; stored bits kept; row, datagram,
   table, value hash and default source id; hash(data) == hash(normalized data); matching keys
   have equal hashes.
4. Golden: `--check` reports nothing moved (the golden cases hold no -0.0 and no other NaN);
   then golden cases pinning the normalized values are added.

## Stage 3: docs and wrap-up

Hashing reference (the float rule with byte patterns, the manifest line), `CLAUDE.md` and
`.zed/rules`, design issue entries, `changelog.d/ITL-1179.md`, CI, review, `worker_done`.
