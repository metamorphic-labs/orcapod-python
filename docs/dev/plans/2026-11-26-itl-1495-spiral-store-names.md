# ITL-1495: SpiralDB store, stages 0 and 1

Stages 0 and 1 of `docs/dev/specs/itl-1446-round2-spiral-sdk.md` (PR 588), approved by the
coordinator on the developer's behalf (decisions 1, 2, 3, 4 and 6). One pull request.

## Stage 0: spiral-sdk 0.16.78

1. `pyproject.toml`: `spiral-sdk==0.16.78`; `uv lock --upgrade-package spiral-sdk`. The version
   in `RELEASING.md`'s migration commands follows.
2. `tests/test_databases/fake_spiral_sdk.py`: `get_metadata` / `set_metadata` on the fake keyed
   table, as measured live with 0.16.78 (rule `table-metadata`): an upsert, never a deletion,
   not versioned, readable before any row, a key over 255 UTF-8 bytes, or an entry whose key
   and value together are over 8,192 bytes, refused with `status 400` and nothing written.
3. Tests first: a conformance test of table metadata in
   `tests/test_databases/test_spiraldb_connector_integration.py`, run on the fake always and
   live opt-in, with fixed keys so a live table does not grow. The live conformance suite is
   rerun with 0.16.78 (`itl-1186-switch.md` section 7: a bump reruns it).

## Stage 1: store table names

`src/orcapod/databases/spiral_store_names.py`, pure, imports no client:

- `SPIRAL_TABLE_NAME_LABEL = "spiral_table_name_v1"`, beside `store_table_digest(path)`: SHA-256
  of the canonical JSON array of the full path components, bare hex (a hash in a name).
- `table_name_hint(path)`, `store_table_name(path)` (`o1_<hint>_<digest>`), and
  `store_table_name_prefix(prefix)`, the name prefix every table under a path prefix has.
- `store_table_metadata(path, data_context_key)`: the four entries of spec 2.2.
- `classify_store_table(metadata, path)`: this path's table, or an unmarked one (2.5); any other
  path, format or a partial record raises `SpiralStoreTableCollisionError`.

Then: the hash site inventory row in `docs/reference/hashing.md` ("in a name"); a golden group
`spiral_table_name` in `tests/test_hashing/golden/cases.py`, written by the generator into a new
file (the diff adds only that file and its history entry). Tests: determinism, the length bound,
the alphabet, prefix property of the hint, injectivity on adversarial paths, refusals.

Nothing calls the module yet; no other hash, stored path or golden value moves.
