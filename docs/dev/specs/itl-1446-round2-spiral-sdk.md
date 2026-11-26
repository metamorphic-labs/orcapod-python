# ITL-1446, round 2: a SpiralDB pipeline store on spiral-sdk

Status: **proposal, nothing implemented.** It updates the first design
(`docs/dev/specs/itl-1446-spiral-columns.md` on the branch of PR 542, written for pyspiral) for
spiral-sdk, which Orcapod has used since ITL-1186 (PR 550). Where the two disagree, this one holds.

Developer's direction (2026-11-26): use the latest spiral-sdk, which reads and writes table
metadata; never put a store table's full path into its SpiralDB table name, which is too long:
use an abbreviated name.

Sources: **Read** (the installed SDK's source, and the service's published HTTP reference),
**Live** (a probe on throwaway tables of the conformance dataset, with the agent workload; the
numbers are in the coordinator's report, not here).

## 1. What changed in spiral-sdk

The newest release is 0.16.78; main pins 0.16.74. Between the two, the Python surface changed in
one place (Read: a diff of the two installed packages):

- `KeyedTable.get_metadata() -> dict[str, bytes]` and `KeyedTable.set_metadata(dict[str, bytes])`.

What the probe established (Live):

| Question | Answer |
|---|---|
| metadata of a new table | empty; readable before any row is written |
| `set_metadata` | an upsert of the given keys; other keys are kept. An empty value is stored as empty, not deleted: **no key can be deleted** |
| versions | metadata is **not** versioned: every snapshot, also one opened with `asof`, sees the latest. A write of metadata does not make a new snapshot |
| by name, by id | both see it |
| limits | a key up to 255 bytes, a value up to just under 8 KiB; many entries per table (no total limit was reached well above what this design writes). Reading gets slower as the metadata grows |
| concurrency | two clients upserting different keys at once: every key kept |
| conditional write | none (no compare-and-set) |
| table names | up to 255 characters accepted, 256 refused. `.`, `/`, `:` and spaces are now accepted (pyspiral refused `.`) |
| `create_keyed_table` | an existing name is refused (409), also with `exist_ok=True` and another key schema |
| zero-row append | returns, publishes no snapshot (no panic) |
| dropping a keyed table | **the SDK has no call for it**; `delete_keys` removes rows only |
| listing a dataset | no SDK call; the HTTP listing PR 550 uses is marked **deprecated** in the service's reference. Its items hold id, dataset, name, key schema and root: **no metadata** |
| metadata of a listed table | one open by id and one metadata read per table |

## 2. Table names

### 2.1 The name

A store table's SpiralDB name is computed from its full record path (the store's base path plus
the record path, as `ConnectorArrowDatabase` joins them) and from nothing else:

```
o1_<hint>_<digest>
```

- `o1_` marks a store table and versions this grammar. Every store table name starts with it; a
  later grammar is `o2_`.
- `<digest>`: the SHA-256 of the UTF-8 canonical JSON array of the path components
  (`json.dumps(components, ensure_ascii=False, separators=(",", ":"))`), as 64 lower-case hex
  digits. A JSON array keeps the components apart, so no join character is trusted.
- `<hint>`: the components joined with `/`, each character mapped to itself if it is in `[a-z0-9]`,
  an upper-case ASCII letter to its lower case, and every other character to `_` (one character to
  one), cut to its first 96 characters. It is for people, and for narrowing a listing (2.4).
- At most 3 + 96 + 1 + 64 = 164 characters, all in `[a-z0-9_]`: under the service's limit, and a
  plain identifier in SpiralDB's SQL.

Because the hint maps character by character, the hint of a path starts with the hint of each of
its prefixes (cut at 96). Nothing ever parses a name: the digest is never read back, the hint never
decoded.

**Hash rules.** The digest is in a name, so it is a bare hex digest (the hash label rule). The
algorithm is defined once, beside the function that builds the name, with its label constant
`SPIRAL_TABLE_NAME_LABEL = "spiral_table_name_v1"`; the label is not written into the name, whose
`o1_` grammar fixes the algorithm. The site is added to the hash site inventory of
`docs/reference/hashing.md`, class **in a name**, as "SpiralDB store table name | the full record
path, canonical JSON | SHA-256 hex, `spiral_table_name_v1` | a SpiralDB table name (a name)".
Changing the algorithm means `o2_`, and an `o1_` store is no longer found: so its values are
pinned as golden values (decision 2).

### 2.2 The metadata

Written by the creator right after `create_keyed_table`, and never changed:

| Key | Value |
|---|---|
| `orcapod.store` | the store format, `orcapod.spiral_store.v1` |
| `orcapod.path` | the canonical JSON array of the full path components (the digest's input) |
| `orcapod.layout` | the table's layout, the path's last component (`pdb_v1`, `rdb_v1`, ...) |
| `orcapod.data_context` | the data context key of the writer that created the table |

A path whose JSON does not fit one value (under 8,000 bytes, with margin) is refused before
anything is created. The rows keep their own `__data_context` as today; the table's entry records
its creator only.

### 2.3 Finding a table

A store **computes** the name and opens it (`keyed_table(project, dataset, name)`); it never lists
to find a table. Then:

| Found | Action |
|---|---|
| no table | absent |
| a table whose `orcapod.store` and `orcapod.path` are this store's and this path | the table; its layout and data context are checked as for any stored table |
| a table with another path or format under this name (a digest collision, another program, a later grammar) | **refused**, naming the table and both paths; never read, written or adopted |
| a table with no Orcapod metadata | a create that stopped before its metadata (2.5) |

The metadata of an opened table is kept for the store's life (by table id); it never changes.

### 2.4 Listing: `list_table_paths`

1. List the dataset (one paged call).
2. Keep the names starting with `o1_` + the hint of the prefix (cut at 96). This is a superset of
   the tables under the prefix; tables of other programs and other prefixes mostly drop out here.
3. For each remaining table not already known, open it by id and read its metadata.
4. Return the paths from `orcapod.path` that start with the prefix, relative to the store's base.

Cost: one listing, plus one metadata read per candidate table not yet known in this store. A
store whose tables share a long hint (one function with many result tables) reads all of them
once.

**Stale layouts (ITL-954).** The rule is: find tables of another layout by name, never by a
computed hash, because the hashes in a path differ between builds. With abbreviated names, "by
name" becomes "by the recorded path": `find_stale_tables(..., name_path=...)` calls
`list_table_paths(name_path)` as today, and `tables_of_another_layout` judges the returned paths
by their components as today. No hash is computed to find them: the digest in the name is never
compared with anything, and the hint narrows the listing only. `CURRENT_LAYOUTS` and the revision
rule apply unchanged.

### 2.5 Creating

Under the store's lease (section 4), in this order:

1. `create_keyed_table(dataset, name, key_schema)` with `exist_ok=False`. A 409: open the name and
   judge it by 2.3.
2. `set_metadata` with the four keys of 2.2.
3. The schema row of the dataset's schema table (`__orcapod_schemas__`, as on main).
4. The first rows.

A crash after 1 leaves a table without Orcapod metadata. The next writer, holding the lease,
**adopts** it when it has no rows and the expected key schema, and writes the metadata; with rows,
or another key schema, it is refused (decision 3). A crash after 2 or 3 leaves a table that is
found and has no rows. Stage 3 treats such a table as new, as `itl-1186-switch.md` section 2
proposes (main still refuses a key-only table in `ConnectorArrowDatabase`). No catalog is needed: the first design's catalog, publication
rows and per-attempt names (its sections 5.1 to 5.4, decision 5) are **dropped**, because the
name is computed again and the metadata says which path a table holds.

Deleting a store table: no SDK call drops one. `delete_table` removes its rows (`delete_keys`) and
its schema row; the empty, marked table remains and is reused if the path is written again.

## 3. The codec (PR 545) on spiral-sdk

The codec is pure and imports no client. It carries over, with these changes:

- **Where the manifest lives.** The first design wrote it into the table's key-value metadata in
  chunks, because pyspiral's single Arrow-metadata entry failed above 8 KiB. On main the dataset's
  schema table holds a table's schema as Arrow IPC in a cell with no such limit. Recommended: the
  logical schema and the stored names go into the schema row; table metadata holds only the
  identity of 2.2 (decision 4). The chunking, the chunk digest and its inventory entry go.
- **Measured inputs to redo with spiral-sdk** (they were measured through pyspiral): which column
  names the service refuses or alters (`__`, `.`, `$`, NUL), which types it keeps bit for bit
  (`_NATIVE`), the temporal and decimal substitutes, struct field order, and the service's value
  limits. Each is one constant or table in the module; stage 2 re-measures them live and the fake
  models the result.
- Unchanged: the injective name encoding, reading by the manifest's stored names, nullability
  checks, refusals before anything is written, no row order (S3).

## 4. The one-writer lease (PR 547) on spiral-sdk

PR 547 took the lease by creating `orcapod_lease_v1` exclusively and released it by dropping the
table by id. Exclusive create still holds (409), but **spiral-sdk cannot drop a keyed table**, and
metadata has no conditional write. So the lease must change. Options:

| | How | Cost |
|---|---|---|
| (a) generations | a claim creates `orcapod_lease_v1_g<n>` exclusively, `n` one past the newest; the owner is in its metadata; release writes `released` into it. The newest generation is found by opening names (doubling, then bisecting) | one empty table per run, never removed; about 2 log2(runs) opens to find the newest |
| (b) wait for SpiralDB | ask for a keyed-table drop, or a conditional metadata write; until then one writer by convention, refused only within a process | no protection between processes now |
| (c) the deprecated drop endpoint | — | not documented for keyed tables; not pursued |

Recommended: (a) now, and ask SpiralDB for (b) (decision 5). A generation's metadata holds the
claim token, owner text and start time; the claim and clearing rules of PR 547 (one claim, settle a
409 by reading the name, clear by a person for the exact generation, nothing expires) carry over
with "drop by id" replaced by "mark released". A release that cannot be confirmed keeps the run
failing, as before. Generations are never reused; a ghost create of an old generation is harmless,
because only `n` one past the newest is ever claimed. The session part of PR 547 (admission, drain,
capability, suspension around user code) carries over unchanged.

## 5. What happens to the earlier PRs

| PR | Recommendation |
|---|---|
| 542, the first design | close once this is approved; this spec supersedes its sections 4.1, 5 and 6.1 and decision 5, and points to it for the rest (sections 3, 4.2 to 4.4, 6.2 to 6.4, 7, 8) |
| 545, the codec | revive: merge main in, resolve, move the manifest per decision 4, re-measure on spiral-sdk (stage 2) |
| 547, the lease | rewrite as a new PR from main (it merges the closed 544): keep `write_session` and the capability, replace take and release by generations (stage 4); close 547 |
| 541, conformance tests | keep open; rework onto `fake_spiral_sdk` and the store when stage 3 lands (stage 5) |
| 544 (closed) | its three findings, ITL-1476: names over 255 refused (still true), dotted names now accepted (changed), zero-row append publishes nothing (changed: no panic). The key-table null row is not re-checked: the SDK reads by key ranges now; stage 3 measures absent keys |

## 6. Stages

Each stage is one PR, inert until stage 5 switches the store on.

| # | PR | Size | Needs |
|---|---|---|---|
| 0 | spiral-sdk 0.16.74 to 0.16.78 in `pyproject.toml` and `uv.lock`; the fake gains `get_metadata`/`set_metadata` (upsert, unversioned, key and value limits) and refuses a 409 as measured | S | decision 1 |
| 1 | `databases/spiral_store_names.py`: name, hint, digest, metadata record, the collision rule as a pure function; inventory entry; golden pins | S | decisions 2, 6 |
| 2 | the codec (545) revived on spiral-sdk; manifest in the schema row; names and types re-measured live | M | stage 0, decision 4 |
| 3 | store mode of the connector: create (2.5), find (2.3), `list_table_paths` (2.4), upsert writes, keyed reads with the absent-key rule; ITL-1476 checks | L | stages 1, 2, decision 3 |
| 4 | lease and session on generations (547 rewritten) | L | decision 5 |
| 5 | job integration, conformance (541) on the fake and live, docs, switch on | L | stages 3, 4 |

Stages 0 and 1 can start at once; 2 after 0; 3 after 1 and 2; 4 in parallel with 2 and 3.

## 7. Decisions

| # | Decision | Recommended |
|---|---|---|
| 1 | Move to spiral-sdk 0.16.78 (dependency change) | yes: it is the only version with table metadata |
| 2 | Pin table names as golden values (a new group of path to name), so the algorithm cannot move silently | yes |
| 3 | A table found without Orcapod metadata: adopt it under the lease when it has no rows and the expected key; refuse otherwise | yes |
| 4 | The schema manifest in the dataset's schema row; table metadata only for identity | yes: no chunking, one place for schemas |
| 5 | The lease by generation tables now; ask SpiralDB for a keyed-table drop or a conditional metadata write | yes |
| 6 | Grammar: `o1_`, hint of 96 characters in `[a-z0-9_]`, full 64-digit digest | yes |
| 7 | Rely on the deprecated listing endpoint for `list_table_paths` and stale-layout checks; ask SpiralDB for a supported listing | yes; a store that cannot list warns, as `TableListingNotSupportedError` does today |
| 8 | Supersede PR 542's design with this one, close 542 and 547, revive 545, keep 541 | yes |
