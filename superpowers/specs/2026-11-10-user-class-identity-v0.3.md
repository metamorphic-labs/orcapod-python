# Identity of a user class in v0.3 (ITL-881)

**Status:** implementation spec, approved by the coordinator November 10, 2026 (P1 decided by the developer; P2 names and the identity-string format provisional).
**Issue:** ITL-881 (v0.3, a public release; tag target November 16, 2026).
**Builds on:** the findings in PR #288 (`superpowers/specs/2026-11-10-user-class-identity.md`,
experiments E01–E08). "W1"–"W15" are the workshop scenarios from §3.4 of that document.

Each decision is marked **CONFIRMED** (the developer decided it, November 10, 2026) or
**PENDING** (the default this spec plans with; each pending item is a small, local change).

| ID | Decision | Status |
|---|---|---|
| D-name | Nominal identity with a declared name; `__main__` classes default to the name only | CONFIRMED: developer decision 1 |
| D-fields | Field names and field types are part of the identity; field order is not | CONFIRMED: developer decision 2 |
| D-computed | Computed fields are not part of the identity in v0.3 | CONFIRMED as deferred: developer decision 2 defers it; v0.3 keeps today's stored fields |
| D-kind | Dataclass and pydantic are distinct kinds | CONFIRMED: developer decision 3 |
| D-rebind | Same identity re-registered: rebind silently | CONFIRMED: developer answer to S1 |
| D-raise | Stored shape disagreeing with registered shape raises, in v0.3 | CONFIRMED: developer answer to S2 |
| D-doc | A page documenting identity | CONFIRMED: developer answer to S4 |
| P1 | A field typed by another user class carries that class's identity as that class was registered | CONFIRMED: developer decision, November 10, 2026 |
| D-cycle | A self-referential or cyclic class is rejected with a clear `TypeError` | CONFIRMED by the coordinator from this proposal: Arrow cannot store a recursive struct |
| P2 | Public API names (`register_type`, `set_type_naming`) and the identity-string format | PENDING with the developer; implemented provisionally, each a single-point change |
| D-hash | The 25 golden entries in §e move; nothing else | CONFIRMED by the coordinator: follows from decision 2; the PR lists every moved entry |
| D-legacy | Reading a pre-0.3 user-class column directly raises a clear error | CONFIRMED by the coordinator: a consequence of the identity check; nothing extra is built |
| D-notice | One-time notice when a `__main__` class is first registered (S3) | CONFIRMED in scope by the coordinator; last item before the cuts |

## a. The identity definition

A user class's identity is an **identity record** with three components:

```json
{"fields": {"x": "int", "y": "float"}, "kind": "dataclass", "name": "Point"}
```

### Name component — CONFIRMED (decision 1)

The name is a string chosen once per class, by the first rule that applies:

1. **Declared name.** The user declared one (§b). Used verbatim.
2. **`__main__` class** (`cls.__module__` is `"__main__"` or `"__mp_main__"`, the name a
   `multiprocessing` spawn child gives the main module, W14): `cls.__qualname__`
   (`"Point"`, or `"Outer.Point"` for a nested class), under the default setting
   `main="name"`. Setting `main="qualified"` gives `"__main__.Point"` (today's string).
3. **Module class:** `f"{module}.{qualname}"` (`"pkg.models.Point"`), under the default
   setting `module="qualified"`. Setting `module="name"` gives `"Point"`. The module is
   passed through `canonical_module_name`, which leaves user modules unchanged.

A name must be non-empty, must not contain `@` or whitespace, and must not start with
`orcapod.` (reserved for built-in types such as `orcapod.path`). A class whose
`__qualname__` contains `<locals>` stays rejected in v0.3, even with a declared name
(scope, §g).

### Kind component — CONFIRMED (decision 3)

`"dataclass"` (`dataclasses.is_dataclass`, which includes `pydantic.dataclasses.dataclass`)
or `"pydantic"` (`pydantic.BaseModel` subclass). A dataclass and a pydantic model with the
same name and fields have different identity records, so they are different types.

### Field component — CONFIRMED (decision 2; nested classes: P1)

`fields` maps each **stored field's name** to its **type token**. It is a JSON object
serialized with sorted keys, so field order never enters. The stored fields are exactly
the ones stored today: dataclass fields with `init=True`, pydantic `model_fields`.

Type token of an annotation (from `typing.get_type_hints`, which strips `Annotated`
extras):

| Annotation | Token | Example |
|---|---|---|
| builtin scalar | its name | `int`, `float`, `str`, `bool`, `bytes` |
| other class without a logical type (stdlib) | `canonical_type_name(cls)` | `datetime.datetime`, `decimal.Decimal` |
| type with a built-in logical type | its logical type name | `orcapod.path`, `orcapod.uuid`, `orcapod.file` |
| generic container | `origin[arg, …]`, origin by its canonical name, args in order | `list[int]`, `dict[str, float]`, `tuple[int, str]` (`typing.List[int]` gives `list[int]`) |
| `Optional` / union | member tokens sorted, joined by ` \| ` | `int \| None` for `Optional[int]`, `int \| None` and `None \| int` |
| nested dataclass / pydantic model (P1) | that class's identity string (below), as that class is registered | `Inner@3c1d…`, `geo.Inner@…` |

**Nested classes (P1, CONFIRMED).** The token of a field typed by another user class is
that class's own registered identity string: its name component however *that* class was
registered (declared name, name only, or `module.qualname`), its kind, and its field
component, recursively through the fingerprint. It is never re-derived from
`module.qualname` and never reduced to the storage struct. So renaming the inner class,
declaring it under another name, or changing its fields changes the outer identity.

**Registration order does not matter.** `register_type` only records the declared name
and checks it (§b); the logical type, and so the identity, is built at first use, and the
outer class's build registers the inner class first (implicitly, by the setting, if it
was never declared). Declaring every class before first use therefore gives the same
identities in any order: outer decorated before inner, inner declared later in the same
cell, inner registered implicitly by the outer's first use. Declaring a class after it
was used under a different name raises (§c). An identical notebook re-run of the inner
class rebinds it (§c) and leaves the outer identity unchanged.

**Cycles (D-cycle).** A class that refers to itself, directly (`next: Node | None`) or
through other classes, is rejected at first use with a `TypeError` naming the cycle
(`Node.next -> Node`). Today's converter already raises `TypeError: Circular type
dependency detected…`; v0.3 keeps the rejection and names the path. Encoding the
back-reference by name would give a well-defined identity, but Arrow cannot store a
recursive struct, so there is nothing to identify.

Deliberately **not** part of the identity: defaults and default factories, methods and
validators, properties and pydantic `computed_field`s, `init=False` dataclass fields,
`ClassVar`s, private attributes, field order, docstrings, pydantic field aliases and
constraints (`Field(gt=0)`, `Annotated` metadata), `frozen`/`slots`/config flags, base
classes (only the resulting fields count). Computed fields are deferred (D-computed); a
later decision to include them adds a key that is absent when empty, so it would move
only classes that have computed fields.

The token is built from the Python annotation, not the Arrow type, because the developer
named field *types* and because the Arrow type cannot tell `str` from `orcapod.path`
(both `large_string`).

### Identity string

```
identity string = f"{name}@{fingerprint}"
fingerprint     = sha256(canonical_json(record)).hexdigest()[:16]
canonical_json  = json.dumps(record, sort_keys=True, separators=(",", ":"))
```

`Point(x: int, y: float)` as a dataclass gives `Point@9117beaaf7ebe3ab`; with `y: int`,
`Point@c0a75f0c39b86873`; as a pydantic model with `y: float`, `Point@703bff8af8d6002c`.
SHA-256 of a documented JSON string, not the Orcapod semantic hasher, so the identity can
be reproduced by hand and does not depend on a hasher version. 16 hex digits (64 bits)
only have to separate classes with the same name and kind.

### Where each component enters

The identity string replaces today's `f"{module}.{qualname}"` as the class's logical
type name. Every place that used the old string now uses the new one; there is no
second mechanism.

| Place | What it carries | How |
|---|---|---|
| Logical type name | identity string | `DataclassLogicalType.logical_type_name` / pydantic equivalent |
| Arrow extension name (`ARROW:extension:name`) | identity string | same string; one Arrow/Polars extension registration per identity |
| Arrow extension metadata | category plus the full record: `{"category":"orcapod.dataclass","identity":{…}}`, canonical JSON | read path uses it to find the class and to print the stored shape |
| Arrow storage type | struct of the stored fields | unchanged (derived from the annotations) |
| Schema hash, source `pipeline_hash`, system-tag column names | identity string | `TypeObjectHandler` hashes a registered class as `type:<logical type name>` (unchanged code) |
| Pod signature hash, node `pipeline_hash`, DB table paths | identity string | via the schema/signature hashes |
| Per-row data hash, `content_hash` | identity string + record + values | the extension column is hashed with its extension name and metadata (unchanged code) |
| `list[Point]`, `Point \| None`, nested fields | identity string | container logical names embed the element's logical name; nested fields via the token (P1) |

**Same name, different fields → different pipeline identity.** `Point(x: int, y: int)`
and `Point(x: int, y: float)` have different fingerprints, so different logical type
names, different schema hashes, different pod and node pipeline hashes, and different
table paths. They never share a table. That removes the "same table, different shape"
failure (W6, W10: today 0 rows plus log warnings): the changed class computes into its own
tables, and the old tables stay as they were. In one process the two shapes have two
extension names, so Arrow's and Polars' global extension registries (keyed by name) hold
both without conflict; that is why the fields must be in the extension *name*, not only
in its metadata.

## b. Public API — PENDING (P2: names), behaviour proposed

```python
import orcapod as op

@op.register_type                     # name by the setting: __main__ → "Point", module → "pkg.models.Point"
@dataclass
class Point: ...

@op.register_type(name="Point")       # declared name, same everywhere it is defined
@dataclass
class Point: ...

op.register_type(Point, name="Point") # call form, for classes you do not own

op.set_type_naming(main="name", module="qualified")   # process-wide defaults (shown)
```

- `register_type` records the name for the class object (a process-wide weak-keyed table,
  so the class is not modified), checks the kind, the name and that the class is not
  local, and returns the class unchanged. The logical type is built at first use, as
  today, so declarations may come in any order (§a). Classes that are never declared are
  registered on first use with the name from the setting.
- Provisional (P2): the public names live in one place (`orcapod/user_types.py`,
  re-exported from `orcapod`) and the identity-string format in one function
  (`identity_string(record)`), so a rename or a format change is a single-point edit.
- `set_type_naming` changes the default for classes registered after the call. Values:
  `"name"` or `"qualified"`. Defaults: `main="name"`, `module="qualified"`. No TOML key
  in v0.3 (scope, §g).
- Registering a class object under a second, different name after it is registered (for
  example `register_type(Point, name="Geo")` after `Point` was used) raises `ValueError`
  naming both names: one class object has one identity per process.

**Name collisions (same kind, same name, two different class objects):**

| Case | What happens | What the user sees |
|---|---|---|
| Same fields, same `module.qualname` (cell re-run, `%autoreload`, W1/W3/W4) | Same identity: the identity is rebound to the new class object (§c) | nothing |
| Same fields, different `module.qualname` (two modules declare `"Point"`, or `module="name"`) | Same identity by definition: they are the same type; values read back are built as the most recently registered class | one `UserWarning` per rebinding naming both classes. Warranted: values come back as a different class object than may be expected |
| Different fields (W2 with changed fields, or two modules) | Different identities: two types that happen to share a display name; no clash | nothing at registration. Reading picks the class whose identity matches (§d) |

## c. Rebinding rule (S1) — CONFIRMED (developer answer to S1)

- **Same identity string, new class object → rebind silently.** The registry rebinds the
  identity (logical name and extension name) to a logical type built for the new class.
  Earlier class objects keep their mapping, so instances created before the re-run still
  convert. Arrow's global registration for that extension name is reused (same name,
  storage type and metadata by construction). This fixes W1, W3, W4.
- **Different identity under the same name → a different type.** Nothing is rebound;
  the new class gets its own logical type and extension name (W2: the next run computes
  into new tables).
- **Still raises:**
  1. One class object registered under two names (§b), including a declaration after
     the class was already used under its default name.
  5. A self-referential or cyclic class (D-cycle).
  2. An invalid declared name (§a).
  3. A local class (`<locals>`), as today.
  4. A shape disagreement on read (§d, S2).

## d. Read path — CONFIRMED behaviour (S2), lookup mechanism proposed

A stored column carries the extension name `Point@9117beaaf7ebe3ab` and the record in its
metadata. To build values, the reader resolves a class:

1. **Registered identity:** the extension name is registered in the reading context → use it.
2. **Declared or registered by name:** classes recorded under `(kind, name)` in this
   process (by `register_type` or by first use); take the one whose identity string
   matches.
3. **`__main__` lookup:** walk the name as an attribute path from
   `sys.modules["__main__"]` (`"Point"`, `"Outer.Point"`), accepted if its identity matches.
   This makes W12 (read notebook results from a script that defines the same `Point`)
   work without a declaration.
4. **Import** (dotted name): import by the longest module prefix, as today, accepted if
   its identity matches.

Outcomes:

- **No candidate found:** `ImportError` (kept), naming the declared name, the kind, the
  stored fields, and how to fix it: "define the class and register it with
  `op.register_type(Point, name='Point')` before reading".
- **Candidates found, none with a matching identity (S2):** `TypeIdentityError`
  (new, a `ValueError` subclass in `orcapod.errors`) naming the column where the read
  path knows it, the class name and kind, the stored fields and the registered fields,
  for example "column 'p': stored Point (dataclass) has fields {x: int, y: int}; the
  registered Point has {x: int, y: float}".
- **Same identity, different Arrow storage** (possible only if two data contexts map one
  annotation to different Arrow types): `TypeIdentityError` naming the column, the class
  and both struct types. The existing check in the synthesized
  `__arrow_ext_deserialize__` raises it.
- These errors propagate. `DeltaTableDatabase` today catches every read error and logs
  "Error reading Delta table"; it re-raises `TypeIdentityError`, so a run can no longer
  report success with 0 rows. Other read errors keep their current handling (out of
  scope). The misleading "data may have been deleted externally" message is untouched
  unless the path is reached for this reason (§g lists it as out of scope).
- **Pre-0.3 column** (D-legacy, PENDING): an extension name without `@` and metadata
  without `identity`, read directly (pipelines never reach these tables, §e): raise
  `TypeIdentityError` saying the column was written by Orcapod before 0.3 under
  `pkg.models.Point`, that v0.3 identifies the class by name and fields, and that the
  pipeline recomputes into new tables. Not converted, not dropped.

## e. Intended hash changes — PENDING (developer approval of the list)

Every pinned value that involves a dataclass or pydantic type moves, because the identity
string changes for every user class (fields are now in it, for everyone). Measured on
`origin/main` (c5ab1e05) with E08 (`--ref origin/main`): changing the identity string of the
golden fixtures `GoldenPoint` and `GoldenModel` moves exactly these 25 entries, and the
`unchanged` control moves 0. Every identity-string change moves the same set (E08's
`__main__` and module-less variants list identical entries), so the v0.3 string moves
these and only these. The implementation re-runs the golden test and the PR lists the
measured diff; any entry outside this list is a bug.

| File | Entries |
|---|---|
| `semantic.json` (2) | `type.dataclass`, `type.pydantic` |
| `schema.json` (5) | `multi_field.logical_mix`, `single_field.dataclass.arrow_type`, `single_field.dataclass.schema_hash`, `single_field.pydantic.arrow_type`, `single_field.pydantic.schema_hash` |
| `datagram.json` (10 change) | `{dataclass,pydantic}.content_hash`, `.pipeline_hash`, `.system_tag_columns`, `.row[{"k":"r1"}].data_hash`, `.row[{"k":"r2"}].data_hash` |
| `datagram.json` (8 replaced) | `{dataclass,pydantic}.row[{"k":"r1"/"r2"}]._tag_record_id::<pipeline hash>` and `._tag_source_id::<pipeline hash>`: the keys embed the moved pipeline hash, so these 8 keys disappear and 8 keys with the new hash appear |

`arrow.json` and `pipeline.json` have no user-class entries and do not move. Golden
additions (new keys only): a declared-name dataclass, a nested dataclass (P1), an
`Optional` field, a container field, and permuted-field-order twins that must pin the
same value.

**For the CHANGELOG (stated plainly):** "Orcapod 0.3 identifies a dataclass or pydantic
model by its name, its kind and its fields. Every pipeline whose data includes such a
class gets new pipeline hashes and table paths, so the first run under 0.3 recomputes
those pod and operator results into new tables. Tables written by earlier versions are
left untouched and are not read by 0.3 pipelines. There is no migration tool. Classes
defined in a notebook or script (`__main__`) are now identified by their name only, so
results survive moving the class into a module if it keeps its name and is registered
with `op.register_type(name=...)`." Results that do not involve user classes are
unaffected.

## f. Interactions with PR #286 and PR #289

- **PR #286** (polars types) touches `builtin_handlers.py`, `logical_types/__init__.py`,
  `contexts/data/v0.1.json`, `docs/concepts/function-pods.md`, the golden `cases.py` and
  `semantic.json`/`schema.json`/`datagram.json`. This work does not touch
  `builtin_handlers.py` or `v0.1.json` (the identity reaches hashing through the
  logical type name), adds a new docs page instead of editing `function-pods.md` beyond a
  link, and edits the golden files only by regeneration.
- **PR #289** (`orcapod.ndarray`) adds `array_format.py` and tests; no file in common.
- **Preferred order:** #286 and #289 merge first. This branch then merges `origin/main`,
  re-runs E08 against the new main to confirm the moved set, and regenerates the golden
  files; the PR's golden diff must then show only the §e entries plus the additions. If
  this PR merges first, #286 regenerates its own additive entries after merging main;
  its user-class-free entries do not move.

## g. Scope cuts, in order, if time runs short

1. TOML/config-file setting for the naming defaults (`set_type_naming` stays).
2. The one-time `__main__` notice (S3, D-notice).
3. The collision `UserWarning` for same identity from a different `module.qualname`
   (behaviour stays: rebind).
4. The `__main__` lookup on read (§d step 3); a declaration is then required to read
   `__main__`-named data in another process.
5. `set_type_naming` itself (defaults fixed: `main="name"`, `module="qualified"`).

Not cut: the identity record and string, rebinding, `register_type`, the S2 raise, the
read lookup by declared name, the docs page and the CHANGELOG entry.

Out of scope for v0.3: computed fields; local classes with a declared name; Enum,
NamedTuple and TypedDict support (D6 of the findings); the `DeltaTableDatabase` row-count
and shutdown-hang findings (§9 of the findings).

## Workshop scenarios after v0.3 (predicted, become the Step 2 tests)

| # | Scenario | v0.3 |
|---|---|---|
| W1/W3 | Re-run the cell, same fields (dataclass / pydantic) | silent rebind; cache hit |
| W2 | Re-run with changed fields | new type; recompute into new tables; old tables untouched |
| W4 | `%autoreload` of a module class | silent rebind |
| W5/W9 | Restart / second notebook, identical class | cache hit |
| W6/W10 | Restart / second notebook, changed fields | clean recompute, new tables; no 0-row success |
| W7 | Script as `__main__` twice | cache hit |
| W8 | `python -m pkg.pipeline` then `import pkg.pipeline` | miss unless declared (`"Point"` vs `"pkg.pipeline.Point"`); hit with `@op.register_type(name="Point")` |
| W11 | Notebook → module | hit if the module class declares the same name (`"Point"`) |
| W12 | Read notebook results from a script | works if the script defines a matching `Point` (top level or declared); otherwise the `ImportError` of §d |
| W14 | spawn child (`__mp_main__`) | same identity as `__main__` |
| W15 | Local class | `ValueError`, as today |
