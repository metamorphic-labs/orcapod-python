# Golden Hashes

Within one data context version, a golden hash value that has shipped in a release never
changes and is never removed. New golden values can be added at any time. Everything on this
page exists to keep that promise and to make CI fail when something breaks it (ITL-626,
ITL-946).

The promise matters because Orcapod hashes are addresses, not just checksums. They name
stored table paths, record ids, memoization keys, default source ids and system-tag column
names (see the [Hashing Reference](hashing.md)). A hash that moves without notice strands
data: after an upgrade, a pipeline no longer finds its stored results and computes them
again, or writes a second copy next to the first.

A **golden entry** is one named case and the exact value Orcapod computes for it, for
example the `semantic` entry `value.uuid.nil`, whose value is `semantic_v1:...`. The golden
test computes every entry again from the code on every CI run. A **data context version**
(today `v1`, key `std:v1:default`) names one generation of hashing. A hash change that is
wanted on purpose starts a new data context version. It never edits a released value.

!!! note "v1 is open until 0.4.0"
    Orcapod 0.3.0 first shipped `v1` and was withdrawn on 2026-11-17, before anyone used it
    (ITL-1255). A withdrawn release freezes nothing, so no `v1` value is released today: until
    0.4.0, a change to a `v1` value is made with the developer's approval of the exact list of
    entries that move, each explained. 0.4.0 freezes `v1`. See
    [Withdrawing a release](#withdrawing-a-release).

!!! note "Provisional operators, sources and library value types are outside the freeze"
    A provisional operator (`orcapod.operators.PROVISIONAL_OPERATORS`; ITL-1266, ITL-1279)
    has an identity that no release freezes yet, and so have a provisional source
    (`orcapod.sources.PROVISIONAL_SOURCES`: `DerivedSource` in both forms, plain from
    `node.as_source()` and with `tags_as_data`, ITL-1331; `PollingSource`, ITL-1314) and a
    provisional library value type (`provisional` of `contexts/logical_types.json`: NumPy
    arrays, torch tensors, pandas and Polars frames and series; ITL-1324). The golden values
    of every case that depends on one, its own hashes and those of every step downstream of
    it, are in the provisional files, `data/v1/provisional/<group>.json` (a `PollingSource`
    has none). They are computed and checked on every run like every other value, but no
    release lists them: they can change in a later release without a new data context, with
    the developer's approval of the exact list. So the promise on this page holds for a
    pipeline only when it contains none of them. See [Provisional files](#provisional-files).

## The pieces at a glance

The mechanism has five layers. Each one is checked against the layer below it, and the last
one is a set of records outside the repository that no pull request can change. Read an arrow
as "is checked against". The numbers refer to the scenarios in the
[threat model](#threat-model-who-catches-what).

```text
Orcapod code         computes every hash: stored paths, record ids, cache keys, source ids
   |  golden recompute: stops behaviour drift (1); no-skip rule (9)
   v
Golden files         data/v1/<group>-NN.json: entry name -> pinned value
   |  file hashes: stops edited, deleted and unlisted files (2-7); duplicate names (8)
   v
Chain                golden.history in v1.json: revisions with the files they added (12)
   |  release pins: stops a recomputed chain (13)
   v
Release pins         released_manifests.json: one revision digest per release, manifest pin (10, 11)
   |  tag check in CI: stops a rewrite of files, chain and pins (14, 15)
   v
Outside the tree     a protected git tag per release; the PyPI wheel and sdist (16)
```

The first three checks stop accidental drift. A deliberate rewrite of the whole tree stops
only at the tags and on PyPI.

## Layout and formats

Three places hold the mechanism: the golden folder of the current data context, the
`golden` section of its manifest, and the release pins. The examples below show the state
after a hypothetical patch release 0.4.1 that added one file; digests are shortened.

### Golden files: `tests/test_hashing/golden/data/v1/`

```text
tests/test_hashing/golden/data/
  v1/
    arrow-01.json      # the files of 0.4.0
    datagram-01.json
    pipeline-01.json
    schema-01.json
    semantic-01.json
    torch-01.json
    semantic-02.json   # added after 0.4.0: new semantic entries only
    provisional/       # never frozen: see "Provisional files"
      pipeline.json
      semantic.json
```

- A file is named after the group of cases it pins plus a two-digit counter:
  `<group>-NN.json`, where the group is one of `cases.GROUPS` (`semantic`, `schema`,
  `arrow`, `datagram`, `pipeline`, `torch`). Names carry no date, issue or release; that
  information lives in the manifest. The folder holds nothing else but the `provisional`
  subfolder ([Provisional files](#provisional-files)).
- A file maps entry names to values. An entry name appears in exactly one file of the
  folder, whatever its group.
- A file listed by a released revision is never edited. New values go into the group's next
  file.
- The folder holds only the current data context. The golden folders of older data
  contexts live in `orcapod-sanctuary`.

### The chain: `golden` in `src/orcapod/contexts/data/v1.json`

```json
"golden": {
  "folder": "v1",
  "history": [
    {"date": "2026-11-15", "release": null,
     "added": {"arrow-01.json": "d62d...", "datagram-01.json": "1151...",
               "pipeline-01.json": "e4e4...", "schema-01.json": "315a...",
               "semantic-01.json": "187c...", "torch-01.json": "99af..."},
     "previous": null, "digest": "2bc7..."},
    {"date": "2026-11-30", "release": "0.4.0", "added": {},
     "previous": "2bc7...", "digest": "d002..."},
    {"date": "2026-12-02", "release": null,
     "added": {"semantic-02.json": "e810..."},
     "previous": "d002...", "digest": "d003..."},
    {"date": "2026-12-09", "release": "0.4.1", "added": {},
     "previous": "d003...", "digest": "d004..."}
  ],
  "provisional": {"pipeline.json": "c86b...", "semantic.json": "2f1d..."}
}
```

- `folder` names the golden folder, under `tests/test_hashing/golden/data/`.
- A revision either adds files or marks a release, never both. A revision that adds files
  lists each with the sha256 of its bytes. A release revision has `added` empty and names the
  library version in `release`.
- Revisions after the last release revision are open: they may still change, and their
  digests are then computed again. The revisions up to the last release revision, and every
  file they list, are fixed.
- `date` is the date the revision was last changed. It is informational: nothing can verify
  it.
- `provisional` lists the provisional files, each with the sha256 of its bytes, outside the
  history: no revision lists them, so no release freezes them.

### The pins: `src/orcapod/contexts/released_manifests.json`

```json
{"v1": {"manifest": "b249...",
        "releases": {"0.4.0": "d002...", "0.4.1": "d004..."},
        "withdrawn": {"0.3.0": {"date": "2026-11-17", "reason": "Withdrawn before ...",
                                "manifest": "18e7...", "revision": "6cdd..."}}}}
```

- `manifest` pins the rest of `v1.json` (the hashers, `definitions`, `libraries`,
  `previous`, `released_in`). It is `null` while the data context is open and is set once,
  at the data context's first release.
- `releases` gains one line per library release: the digest of that release's revision.
- `withdrawn` (optional) holds one record per withdrawn release: the date, the reason, and
  the `manifest` pin and release revision digest (`revision`) that release shipped. A
  withdrawn release has no `releases` line and no release revision, so it freezes nothing.

### Provisional files

`tests/test_hashing/golden/data/v1/provisional/<group>.json` holds the values of the cases
that depend on a provisional operator (ITL-1266, ITL-1279), a provisional source (ITL-1331) or
a provisional library value type (ITL-1324), one file per group and no counter: such a file is never frozen, and the generator
rewrites it in place.

- **Which entries.** A case declares the provisional features it depends on, operators and
  sources by class name and library value types by extension name:
  `@case("pipeline", "group_by", provisional=("GroupBy",))`,
  `@case("schema", "derived_source_tags_as_data", provisional=("DerivedSource",))`,
  `@case("datagram", "ndarray", provisional=("orcapod.numpy.ndarray",))`. The generator runs
  every case while Orcapod records which provisional features it uses
  (`orcapod.utils.provisional.recording_provisional_uses`), and refuses a case that uses
  one without declaring it, or that declares a name that is not provisional. The golden
  test checks that every entry is in the kind of file its case declares. An operator is
  recorded when it is built, when its own content or pipeline hash or identity structure is
  read, when it is applied (`process`, its call, a stream method such as `stream.group_by`,
  `static_process`), and when a stream it produced is hashed, read or previewed (`head`); a
  source when it is built and when its source id, content or pipeline hash or identity
  structure is read (a `DerivedSource` also when its rows are read or previewed, and when a
  saved one is loaded, also over a job node it cannot rebuild); a library value type when it
  is resolved. So an operator or source built before the case, at module level, is still
  recorded when the case reads its identity or applies it, and a step built in the case
  downstream of it reaches it while its own hash is computed.
- **What the guard sees, and the convention that makes it enough.** The guard sees a
  provisional object built or read inside the case, not one hidden behind a hash computed
  before the case: a stream, source or job node downstream of a provisional operator or
  source that was built and hashed, or computed, before the case answers from its cached
  hash or kept result and never reaches it, so nothing is recorded. Recording that would
  need the dependency carried through every hash cache, which Orcapod does not do. So every
  case builds its sources, operators, pods' inputs and jobs inside the case function, and
  `test_golden_store.py` (`test_cases_hold_no_prebuilt_pipeline_objects`) checks that the
  golden modules hold no module-level or cached stream, source, operator or job.
- **A dependence the record cannot see.** A user class with a field of a provisional library
  type (the fixture `GoldenWithArray`) embeds that type's identifier in its own identity, but
  the identity is built once, when the class is registered, and cached: a later use of the
  class resolves no type and records nothing. So a case whose items mix such values with
  others is split by a predicate on the items' types (`cases.provisional_types_of`, which
  reads field types, generic arguments and container elements), the fixtures are registered
  before any case records its uses, and `test_golden_provisional_types.py` checks the
  predicate against every fixture class's identity record.
- **Split cases.** A case whose items are partly provisional is registered twice under its
  one name, the provisional part declared, so every entry keeps its name: `semantic:value`,
  `semantic:type`, `schema:single_field`, `arrow:extension_no_values`,
  `pipeline:join_key_nullability` (its `merge_join.*` entries),
  `schema:key_policy_identity` (its `derived_tags_as_data.*` entries) and
  `pipeline:default_source_ids` (its `as_source[...]` entries).
- **No `PollingSource` values.** No case builds a `PollingSource` (ITL-1314): its pins were
  removed rather than kept provisional, and the tool refuses a case that builds one, declared
  or not. Of the provisional sources only `DerivedSource` is declarable
  (`cases.DECLARABLE_PROVISIONAL_SOURCES`).
- **Today:** 475 entries. The `PolarsFilter` cases (`polars_filter`, `polars_filter_expr`,
  `polars_filter_expr_job` and the `semantic` case `polars_expr`, 86 entries) moved out of
  the frozen files with ITL-1266; the cases of the other provisional operators (229 entries)
  and of the library value types (128 entries, with the 12 of `GoldenWithArray`) moved with
  ITL-1279 and ITL-1324, and those of `DerivedSource` (32 entries) with ITL-1331. Every move was values unchanged, before `v1` was frozen.
- **What holds.** Every provisional value is computed and compared on every CI run, so a
  change nobody intended fails. An intended change needs the developer's approval of the
  exact list of entries that move, each explained (`--remove`, then `--add`), but no new
  data context, before or after a release. `--release` neither lists nor pins a provisional
  file, the manifest pin leaves out `golden`, and the tag check does not compare them.
- **Leaving the group.** When an operator (or a source) stops being provisional, at a
  release, it leaves `PROVISIONAL_OPERATORS` (or `PROVISIONAL_SOURCES`), its `PROVISIONAL` marker and its cases' declarations go, and
  `generate_golden_hashes.py --relocate` moves its entries, values unchanged, into the open
  frozen file of their group. The `--release` step then freezes them.
- **Joining the group.** Only an operator or source whose values no release lists can become
  provisional: `--relocate` refuses to move an entry out of a file a release lists.

### Digest definitions

| Value | Computed as |
| --- | --- |
| File hash in `added` | sha256 of the file's bytes, hex |
| Revision `digest` | sha256 of the canonical JSON (sorted keys, no spaces, `json.dumps(..., sort_keys=True, separators=(",", ":"))`) of `{date, release, added, previous}` |
| `manifest` pin | sha256 of the canonical JSON of the manifest without its `golden` key |
| `releases` pin | the `digest` of that release's revision |

Each revision's digest covers the previous revision's digest, so pinning a release digest
also fixes every revision before it.

## Lifecycle

Three events touch the golden files: a pull request adds values, a release freezes them, and
a new data context version starts a new folder and chain. Nothing else writes to them. All
writes go through `tests/test_hashing/golden/generate_golden_hashes.py`.

### Adding golden values (any pull request)

1. Add the case to `tests/test_hashing/golden/cases.py`. Never move or rename an existing
   fixture: function pods, dataclasses and pydantic models hash with their module path.
2. Run `uv run python tests/test_hashing/golden/generate_golden_hashes.py` (`--add` is the
   default). It computes every case and writes only the entries that no golden file pins
   yet. A group's new entries go into its newest file when no release lists that file yet,
   and otherwise into a new file with the next counter (`semantic-02.json`). The file is
   recorded in the open revision, dated today, and the open revisions' digests are computed
   again; after a release revision, a new open revision is appended. It never changes an
   existing value, and it writes nothing when the tree already breaks a rule.
3. The diff shows only added entries or a new file, and the edit to the open revision.
4. `--check` computes every case and reports moved, unpinned and no longer produced entries
   without writing anything, and entries pinned in the wrong kind of file (a provisional
   case in a frozen file, or the other way round).

An entry of a case that declares a provisional operator goes to the provisional file of its
group instead ([Provisional files](#provisional-files)). `--relocate` moves pinned entries
between the frozen and the provisional files, values unchanged, as their cases declare.

Before a release, a value that no release lists can change only with the developer's
approval and a written migration assessment. `--remove <entry>` (repeatable) takes the entry
out of its file and records the file's new sha256 in the open revision; it refuses an entry
in a file a release lists and writes nothing unless every entry can be removed. `--add` then
writes the new value. Editing the file by hand does not work: the file hashes report the
edit and the generator refuses to write over it.

### Releasing (the release pull request)

1. Precondition, in place since 2026-11-15 (developer decision): the repository ruleset
   `protect-release-tags` on `metamorphic-labs/orcapod-python`, where releases are cut, targets
   `refs/tags/v*` with the rules deletion, update and non-fast-forward and no bypass actors. A `v*` tag can be created (the Release workflow
   pushes it) but never moved or deleted. Agents never change repository settings.
2. Run `TZ=UTC uv run python tests/test_hashing/golden/generate_golden_hashes.py --release X.Y.Z`
   (release dates are UTC days, and the revision's date is read from the local clock).
   It computes no hash. It appends a revision with `added: {}` and `release: "X.Y.Z"`, writes
   its digest into `released_manifests.json` under `releases`, and at the data context's first
   release sets `released_in` and the `manifest` pin. It refuses a version already released,
   a development or local version, and a tree that breaks a rule. Try it first on a copy with
   `--root <dir>` (`store.copy_tree` writes one). The same step pins the open data function
   type snapshots (`scripts/data_function_types.py --release`, writing their sha256 and the
   version into `src/orcapod/data_function_types/released.json`; see
   [Data function types](data-function-types.md)).
3. The Release workflow tags the commit it builds, which contains the release revision. From
   then on every file listed up to that revision is frozen.
4. The tag and the PyPI wheel and sdist, which carry `v1.json` and `released_manifests.json`,
   are records of what shipped, outside the repository.

An operator that stops being provisional in this release has its entries moved into a frozen
file (`--relocate`) before the release step, so that the release freezes them.

Every published version needs the release step, a pre-release included: the release gate
refuses to tag a version without it. So the first version of a data context published on
PyPI freezes it; for `v1`, publishing `0.4.0rc1` would freeze `v1` at the release candidate.

### Withdrawing a release

A release is withdrawn only by the developer's decision, when nobody has used it. One
release has been: 0.3.0, withdrawn on 2026-11-17 (ITL-1255), so that `v1` is frozen at 0.4.0
instead, after the open questions that can move a golden value are settled. The developer
withdraws the version from PyPI (yanked or deleted) and deletes its tag and GitHub Release; in
the repository:

1. The release revision is taken out of the golden history and the open revisions after it
   are chained again (`store.withdraw_release`, which only takes the last release). The
   golden files are not touched.
2. Its pins move to a `withdrawn` record in `released_manifests.json`, with the date and the
   reason. Without a release left, `released_in` and the `manifest` pin become `null`: the
   data context is open again, and every file it listed can change by the approved-change
   procedure (`--remove`, then `--add`).
3. The version is never released again: `--release` and the release gate refuse it, and PyPI
   never takes a version twice.
4. The tag check compares the release's tag, while it exists, with the record instead of with
   HEAD, so it passes before and after the tag is deleted.

### Starting a new data context version

Within `v1` a change to a released golden value is refused, with no exemption: also not for
a logical type's own identifier change. When such a change is wanted on purpose, it ships as
a new data context version, only in a minor release (before 1.0) or a major one, never in a
patch:

1. The new manifest `v2.json` starts with
   `"golden": {"folder": "v2", "history": [], "provisional": {}}`, an empty folder
   `data/v2/`, and the pin `"v2": {"manifest": null, "releases": {}}`. The generator then
   writes `v2/<group>-01.json` from the new code, and `v2/provisional/<group>.json` for the
   cases that depend on a provisional operator.
2. The `v1` code, manifest, golden folder and pins move to `orcapod-sanctuary`, which keeps
   them computable.
3. This repository then carries only the `v2` folder and chain.

## Threat model: who catches what

Every drift scenario known to us fails at least one check, as long as git history and the
release tags are protected. Accidental drift is caught inside the tree; a deliberate rewrite
of the whole tree is caught only by records outside it, the tags and PyPI.

| # | Scenario | Kind | Caught by | What fails |
| --- | --- | --- | --- | --- |
| 1 | A code change or a dependency bump in `uv.lock` moves a hash | accidental | Golden recompute | the entry's test names the case and both values |
| 2 | Someone regenerates or edits a released file to make the test pass | accidental | File hashes | the file's sha256 no longer matches its revision |
| 3 | An editor or formatter reformats a golden file | accidental | File hashes | as in 2, though no value moved |
| 4 | A Windows checkout converts line endings | accidental | `.gitattributes` (prevents it) | nothing: the bytes stay as committed |
| 5 | An entry is deleted from a released file | accidental | File hashes | the file's sha256 no longer matches |
| 6 | A released file is deleted | accidental | File hashes | a listed file is missing |
| 7 | A golden file is added but not listed | accidental | File hashes | an unlisted file is in the folder |
| 8 | A new file repeats a released entry name with another value | either | Duplicate-name check | the name is reported with both files |
| 9 | A golden entry is marked `skip` or `xfail` to silence it | either | No-skip rule | the golden test reports the mark |
| 10 | A hasher definition or library in a released `v1.json` is edited | accidental | Manifest pin | the manifest without `golden` no longer matches `manifest` |
| 11 | A release is cut without the release step | accidental | Release gate, final-release check | the Release workflow stops before tagging; a final version has no `releases` line |
| 12 | A released file is edited and its hash in `added` updated to match | deliberate | Chain | that revision's digest no longer computes, or the next revision's `previous` no longer matches |
| 13 | As 12, with every later digest computed again | deliberate | Release pins | the pinned release digest is no longer in the chain |
| 14 | As 13, with the pins rewritten too | deliberate | Tag check, PyPI | the file and the history differ from the release tag; the published `v1.json` shows the original chain |
| 15 | A release line and its revision are removed | deliberate | Tag check | the pins at the tag are missing in HEAD |
| 16 | A release tag is moved or deleted | deliberate | Tag protection, PyPI | GitHub refuses it; PyPI never accepts a second upload of a version |
| 17 | The checks themselves are edited to pass | deliberate | Review only | nothing automatic: see the residual risks |
| 18 | `main` is force-pushed with rewritten history | deliberate | Branch protection | outside this mechanism |
| 19 | A release is recorded as withdrawn to free its files | deliberate | Tag check, review, PyPI | a record that does not name exactly the pins its tag shipped fails the tag check; a record that does is a visible decision in review, and the version stays on PyPI unless it was withdrawn there |

## Each component in detail

The recompute protects behaviour against the files, the file hashes protect the files against
the manifest, the chain and the pins protect the manifest against itself, and the tags and
PyPI protect everything against a rewrite of the tree.

### Golden recompute

`tests/test_hashing/golden/test_golden_hashes.py`

- **Checks:** every entry of every golden file of the current folder is computed again from
  the code and compared with its value, one test per entry; computed entries that no file
  pins fail too. The values are also computed in subprocesses with different
  `PYTHONHASHSEED` values.
- **Protects against:** behaviour drift: a refactor, an interpreter difference (3.11 to
  3.13), or a library bump in `uv.lock` that moves a pinned hash (1).
- **Cannot catch:** a golden file edited to match the new behaviour. The file hashes catch
  that.

### File hashes (the `added` map)

`tests/test_contexts/test_data_context_v1.py`, rules in `tests/test_hashing/golden/store.py`

- **Checks:** every listed file exists with exactly the recorded sha256, and every file in
  the folder is listed by exactly one revision.
- **Protects against:** a change to a golden file's bytes, a deleted released file, an
  unlisted file (2, 3, 5, 6, 7). "The test passes again after regenerating" becomes a
  failure.
- **Cannot catch:** an edit that also updates the hash in `added` (12).

### Chain (`previous` and `digest`)

`tests/test_contexts/test_data_context_v1.py`

- **Checks:** every revision has exactly its five fields; a release revision adds no file
  and any other revision adds at least one; each digest computes from the revision's fields;
  each `previous` is the digest of the revision before; no version is released twice.
- **Protects against:** an edit to one revision without touching the rest (12). It also gives
  every change a date and a place in order.
- **Cannot catch:** a chain computed again from the edit onwards (13).

### Release pins (`releases`)

`tests/test_contexts/test_data_context_v1.py`

- **Checks:** each pinned digest is the digest of the release revision of that version, and
  each release revision is pinned.
- **Protects against:** a chain computed again (13). A rewrite has to touch a second file,
  visible in review.
- **Cannot catch:** a rewrite that edits the pins as well (14).

### Manifest pin (`manifest`)

`tests/test_contexts/test_data_context_v1.py`

- **Checks:** the manifest without its `golden` section has the pinned digest;
  `released_in` is set exactly when the manifest is pinned, which is exactly when the chain
  has a release, and names the first release.
- **Protects against:** an edit to the hashers, `definitions`, `libraries` or `previous` of a
  released data context (10). It is computed over parsed JSON, so a whitespace edit does not
  trip it.

### Final-release check

`tests/test_contexts/test_data_context_v1.py`

- **Checks:** a final version of the library (not a development, pre-release or local
  version; the version comes from the git tag) has its own `releases` line and a pinned
  manifest.
- **Protects against:** a release cut without the release step (11).

### Release gate

`scripts/check_golden_tags.py --release-version X.Y.Z`, a step of the Release workflow's
build job before the tag is made; tested by `tests/test_check_golden_tags.py`

- **Checks:** `X.Y.Z` is not a withdrawn release; HEAD's `released_manifests.json` pins
  release `X.Y.Z` of the current data context, its `manifest` pin is set, and the pinned digest is the release revision of
  `X.Y.Z`, which is the last revision of HEAD's golden history: a golden file added after
  the release step would ship unreleased, so such a commit is not tagged. It fails without
  git.
- **Protects against:** a release cut without the release step (11). The final-release
  check cannot catch it in the Release workflow, whose tests run before the tag exists,
  under a development version.

### Tag check

`scripts/check_golden_tags.py`, a step of the `Run Tests` workflow and of the Release
workflow's build job before the tag is made (so a release dispatched from a branch that
rewrote an earlier release is refused); tested by `tests/test_check_golden_tags.py`

- **Checks:** for each tag `vX.Y.Z` whose `released_manifests.json` pins release `X.Y.Z` of
  the current data context (the one manifest at HEAD): every golden file listed by the
  revisions up to that release exists in HEAD with the bytes it has at the tag; those
  revisions are unchanged in HEAD's history; and the pins of the data context at the tag (the
  manifest pin and every `releases` line) are unchanged in HEAD. For a tag whose release
  HEAD records as withdrawn, it checks instead that the record's `revision` and `manifest`
  are the release digest and manifest pin the tag shipped; once the tag is deleted there is
  nothing to compare.
- **Protects against:** a full rewrite of files, chain and pins inside the tree (14, 15), and
  a withdrawn record that does not describe the release it names (19). This is the anchor
  outside the tree.
- **Relies on:** protected tags and history. In CI (`CI=true`) it fails, never skips, when
  git, the history (a shallow clone) or the tags are missing; outside CI it skips with a
  message. With no release tag of the current data context it passes with nothing to check.

### PyPI artifacts

A record, not a check: the wheel and sdist of each release carry `v1.json` and
`released_manifests.json`, and PyPI never accepts a second upload of a version (16).

### `.gitattributes`

`tests/test_hashing/golden/data/** -text` keeps git from converting line endings on checkout,
so the bytes that are hashed are the bytes that were committed (4).

### No-skip rule

`tests/test_hashing/golden/test_golden_hashes.py`

- **Checks:** no golden entry carries a `skip`, `skipif` or `xfail` mark, from its
  parameters or added by any plugin, except the optional-package rule (ITL-871): the `torch`
  group is skipped where torch is not installed, and an entry the installed torch cannot
  compute (a newer dtype) is skipped. CI installs every extra.
- **Protects against:** a golden entry silenced instead of fixed (9).

### Duplicate-name check

`tests/test_hashing/golden/test_golden_hashes.py` and the generator

- **Checks:** the folder holds only `<group>-NN.json` files of known groups and the
  `provisional` subfolder of `<group>.json` files, and an entry name appears in only one of
  all of them (8).

### Placement check

`tests/test_hashing/golden/cases.py` (`compute_group_kinds`) and
`tests/test_hashing/golden/test_golden_hashes.py`

- **Checks:** a case that builds a provisional operator declares it; every entry of a case
  that declares one is in a provisional file, and every other entry in a frozen file.
- **Protects against:** a value that depends on a provisional operator frozen by a release,
  and a frozen value moved out of the freeze without a declaration.

## Residual risks and assumptions

| Risk or assumption | Why it matters | What covers it |
| --- | --- | --- |
| Git history and `v*` tags are protected on both repositories | The tag check trusts the tags | The `protect-release-tags` ruleset on both repositories (in place since 2026-11-15) and branch protection; the one exception is the tag of the withdrawn 0.3.0, deleted by the developer |
| The checks themselves can be edited (17) | A weakened check passes | Review: changes to these tests and scripts get explicit review |
| Golden values are computed only with the `uv.lock` versions | A user's newer pyarrow, polars or numpy could hash differently unnoticed | ITL-1151: CI against the lowest and highest allowed dependency versions |
| A hash with no golden entry is not protected | Coverage is only as wide as `cases.py` | Add cases whenever a new stored or embedded hash appears |
| Values on `main` after the last release are open | An unreleased commit has no guarantee | Use a released version for anything that must stay stable |
| Optional groups skip without their package | `torch` values are unchecked where torch is missing | CI installs every extra |
| The `date` field cannot be verified | Informational | Order and integrity come from the chain and the tags |
| Provisional values are outside the freeze | A pipeline with a provisional operator or source (`DerivedSource`, `PollingSource`) may compute its results again after an upgrade | `ProvisionalHashWarning` when the operator or source is built or a saved job holding it is loaded; the lists in `PROVISIONAL_OPERATORS` and `PROVISIONAL_SOURCES`, on the operators and sources pages |

A deliberate attacker with merge rights who also rewrites history and tags is beyond what any
check inside the repository can stop; the PyPI artifacts remain the record of what shipped.

## Rules

- A released golden file never changes, and is never removed or renamed.
- A value of a case that depends on a provisional operator is pinned in a provisional file,
  never in a frozen one; no release freezes a provisional file.
- `v1` is open until 0.4.0 (0.3.0 was withdrawn on 2026-11-17): until then, a change to a
  `v1` value needs the developer's approval of the exact list of entries that move, each
  explained. Never edit or regenerate a golden file to make a test pass.
- A withdrawn release freezes nothing; its record stays under `withdrawn`, and its version is
  never released again.
- New golden values can be added in any release, a patch included, with
  `generate_golden_hashes.py`.
- No exemption: any change to a released golden value, including one caused by a logical
  type's own identifier change, needs a new data context version.
- A new data context version ships only in a minor release (before 1.0) or a major one, never
  in a patch.
- The release pull request runs `generate_golden_hashes.py --release X.Y.Z`, and the release
  is tagged on a commit that contains it.
- When a new data context starts, the old context's code and golden files move to
  `orcapod-sanctuary`.
