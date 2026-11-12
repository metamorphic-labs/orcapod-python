# Two kinds of user class: exact and structural (ITL-972)

**Status:** PROPOSED, awaiting approval. Nothing here is implemented.
**Issue:** ITL-972 (v0.3; tag target November 16, 2026; workshop November 17).
**Supersedes:** the identity model of PR #292 (ITL-881,
`superpowers/specs/2026-11-10-user-class-identity-v0.3.md`). PR #292 stays open and is not
merged; its branch is merged into this one and reworked.
**Base:** tip of `eywalker/itl-967-v03-hash-step-combine-the-hash-changing` (c4b1e4a7) with
`eywalker/itl-881-user-class-identity-impl` (c6837a15) merged in.

## Decided by the developer (November 12, 2026) — not reopened

| ID | Decision |
|---|---|
| D1 | An ordinary dataclass is named by its full module path plus class name. |
| D2 | An ordinary dataclass is specific to the class object. Only an Orcapod dataclass is independent of the class object. A notebook re-run of an ordinary class makes a new class that earlier pods refuse. |
| D3 | An Orcapod dataclass is identified by its name plus its fields and their types. The decorator optionally takes a name; the default is the class name. The logical type metadata holds name, fields and types, enough to rebuild the class. |
| D4 | The constructor of a structural class accepts an instance of any Orcapod dataclass or ordinary dataclass and yields a proper instance of the structural class. |
| D5 | Dataclasses only. Pydantic models are completely class based. A structural pydantic model is ITL-973. |
| D-order | Field order is part of identity (developer, November 11, 2026, for PR #292). Kept: I found no reason to change it. |

## Open points, with a recommendation each

| ID | Question | Recommendation |
|---|---|---|
| O1 | Do an ordinary class's fields enter its identity? (asked of the developer) | **Yes** (variant IN, §a.2). |
| O2 | Is a subclass column accepted at a parameter annotated with its ordinary base class? | **No**: exact class means `is` (§a.4). Today `is_subhint` accepts it. |
| O3 | Call form of the conversion (D4) | `P(other)` plus `P.from_instance(other)`; `P(other)` never changes the normal field constructor (§c.1). |
| O4 | What the other instance must have | Exactly the same field names; nothing dropped, nothing defaulted (§c.2). |
| O5 | Is an ordinary dataclass column accepted by the schema check at a structural parameter? | **No**: explicit construction only (§c.4). |
| O6 | `op.dataclass` sets attributes on the class it defines (PR #292's rule was "`register_type` never modifies the class") | **Accept**: it is the defining decorator, like `dataclasses.dataclass`; there is no call form for a class you do not own (§b.1). |
| O7 | Do instances of two structural classes with one identity compare equal (`==`)? | **No in v0.3**: stdlib dataclass equality (same class only). Convert first: `P(a) == b` (§b.6). |

## Terms

- **Ordinary class:** a class made by `dataclasses.dataclass` (or
  `pydantic.dataclasses.dataclass`) and not by `orcapod.dataclass`; kind `dataclass`.
- **Pydantic model:** a `pydantic.BaseModel` subclass; kind `pydantic`.
- **Structural class:** a class made by `orcapod.dataclass`; kind `structural`.
- **Exact class:** the first two. Orcapod treats two exact classes as one type only when they
  are the same class object.

## Shared mechanics (all three kinds)

Carried over from PR #292, changed where marked.

**Identity record.** Canonical JSON (`sort_keys`, no whitespace, UTF-8, arrays keep order):

```json
{"fields": [["x", "int"], ["y", "float"]], "kind": "structural", "name": "Point"}
```

**Identifier (ITL-935 rule: extension name plus extension metadata).**

- Extension name and logical type name: `f"{name}@{sha256(canonical_json(record)).hexdigest()}"`.
  The digest stays in the name (developer, November 10, 2026, D-format of PR #292) for a
  technical reason that still holds: Arrow's and Polars' extension registries are keyed by
  name only, so two shapes of one name in one process (a class edited and its cell re-run)
  need two names. The digest is plain hex, no hash label.
- Extension metadata: `{"category": <category>, "identity": <record>}`, canonical JSON.
  Categories: `orcapod.dataclass` (ordinary), `orcapod.pydantic`, and new
  `orcapod.structural`.
- The whole identifier enters the type hash through `logical_type_identifier(lt).text`
  (ITL-967 code, unchanged). No user-class identifier has a `"version"` key: the record is
  the version. A change to the record format is a change of every user-class identifier.
- `readable_extension_metadata()` is the type's own metadata only.

**Type tokens (CHANGED from PR #292: structured, so a class can be rebuilt from them).**
A field's token is JSON, built from the annotation after `typing.get_type_hints`:

| Annotation | Token |
|---|---|
| `int`, `float`, `str`, `bool`, `bytes`, `None` | `"int"`, … `"None"` |
| stdlib class without a logical type (`datetime.datetime`) | its `canonical_type_name`, a string |
| type with a registered logical type (`Path`, `np.ndarray`, a third-party type) | `{"logical": "<LogicalTypeIdentifier.text>"}`: name **and** metadata, per the ITL-935 rule (PR #292 used the bare name) |
| `list[T]`, `set[T]`, `tuple[...]`, `dict[K, V]` | `{"generic": "list", "args": [<token>, …]}`, origin by canonical name |
| `Optional[T]`, unions | `{"union": [<token>, …]}`, members sorted by their canonical JSON, `"None"` last |
| `Literal[...]` | `{"literal": [<repr>, …]}`, sorted |
| another user class | `{"class": <that class's full identity record>}` |

A nested class contributes its **whole record**, not its identity string, so the outer
metadata describes every level without any class being importable.

**Stored fields:** dataclass fields with `init=True`; pydantic `model_fields`. Storage is a
struct of them in declared order. Not part of identity: defaults, methods, validators,
`init=False` fields, `ClassVar`s, dataclass options, base classes.

**Carried over unchanged from PR #292:** a subclass has its own identity and keeps its
fields (the registry never matches a user class by `issubclass`; audit finding R); field
order is identity; `TypeIdentityError` is not a `StoreReadError` and fails the run under
every `error_policy`; a nested class stored as `None` reads back; storage that differs only
in large versus non-large layout at any depth is accepted (Delta compaction);
`RecursiveTypeError` for a class that refers to itself; `tests/test_user_types`.

## a. Ordinary dataclass (exact class)

### a.1 Name

`f"{module}.{qualname}"`, the module through `canonical_module_name` (D1).

- A class in `__main__` is named `__main__.Point`. `__mp_main__` (a `multiprocessing` spawn
  child) is written `__main__`, so a worker process finds the same type. Consequence: a
  notebook class and the same class moved to `pkg/models.py` are different types
  (`__main__.Point` versus `pkg.models.Point`); two scripts that each define
  `__main__.Point` with the same fields share the type, as in v0.2.0.
- A local class (`<locals>` in the qualified name) is rejected, as today: it cannot be
  looked up by module path.

### a.2 Fields in or out (O1)

**Variant IN (recommended).** Record `{"kind": "dataclass", "name": "pkg.models.Point",
"fields": [...]}`.

- Hash: a field added, removed, retyped or reordered changes the identifier, so the schema
  hash, pipeline hash and table paths. The edited class computes into new tables; the old
  tables stay untouched and are no longer read by the pipeline.
- Storage: extension name `pkg.models.Point@<digest>`; metadata holds the record.
- It is what the ITL-935 rule requires: "change a type's identifier when, and only when, an
  existing value would be stored or hashed differently". A changed field changes the stored
  struct.
- One mechanism for all three kinds; the read check "stored fields equal the class's fields"
  is a comparison of two identifiers.

**Variant OUT (v0.2.0 behaviour).** Record `{"kind", "name"}` only; extension name
`pkg.models.Point`, metadata `{"category":"orcapod.dataclass"}`, both exactly as on the
ITL-967 branch.

- Hash: the identifier does not move when fields change, so pipeline hash and table path
  stay. An edited class meets its old rows in the same table with another struct. Orcapod
  must then raise on write (Delta schema mismatch) and on read (`TypeIdentityError` naming
  the column) and the user must clear or move the store by hand. In v0.2.0 this case ended
  as a run that reported success with 0 rows (W6, W10 of the ITL-881 findings).
- Storage: one extension name for every shape of the class. Within one process (edit a
  field, re-run the cell) Arrow's registry holds the first shape; the second needs
  `pa.unregister_extension_type` and a replacement, after which tables of the first shape
  that pass through Polars or IPC are decoded with the wrong storage type. This is new,
  untested code.
- Golden: no existing dataclass or pydantic entry moves relative to the ITL-967 branch.
- It contradicts the ITL-935 rule quoted above.

The code keeps the choice in one function (which keys an exact class's record has), so
either answer is a small change until the golden files are regenerated.

### a.3 Rebinding and declared names: what remains of PR #292

**Removed, explicitly:**

- `op.register_type`, `orcapod/user_types.py`, declared names, `qualified_name=`.
- Name-only naming of `__main__` classes and the one-time `__main__` notice.
- Equivalence of two ordinary classes with the same identity in the schema check
  (`equivalent_hints` no longer applies to exact classes).
- The read lookup among classes "declared or seen under a name", and the `UserWarning`
  for the same identity from another module path.

**Kept, reduced to one rule:** when a class object with the same module path and name as an
earlier one is used (a cell re-run, `importlib.reload`, `%autoreload`), the registry binds
the identifier to the **newest** class, and the old class object keeps its own binding so
its instances still convert. Without this, re-running a cell raises "already bound" (the
W1 failure of v0.2.0). It gives no equivalence: the two classes remain different types for
every check, and with variant IN an edited class has another identifier anyway.

### a.4 Enforcing the exact class

| Place | Rule |
|---|---|
| Schema compatibility (`schema_utils._accepts`, used at every pod input) | For a dataclass or pydantic class on either side: accepted only if `incoming is receiving`. Inside generics and unions the same rule applies per member. `is_subhint` no longer decides for user classes, so a subclass column is refused at a base-class parameter (O2). The error names the column and both classes with their module paths; when both print the same path it adds that the class was defined again (re-run cell) and the pod must be rebuilt. |
| Pod input, in memory | The column's type is the annotated class (schema check), and the value is the object the upstream produced. No conversion. |
| Writing a value (`python_to_storage`) | `type(value) is python_type`, else `TypeError` naming both classes. Today any object with the right attributes is stored, and a subclass instance loses its extra fields silently. |
| Reading stored values | The class is found by the stored name: the class bound in the reading context, else `sys.modules["__main__"]` for a `__main__.` name, else import by the longest module prefix. Its identifier, computed now, must equal the stored one. Not found: `ImportError`. Found with other fields (variant IN: another identifier; variant OUT: another struct): `TypeIdentityError` with the stored and the current fields. The database layer adds the column name (`register_discovered_logical_types` knows the column; the factory does not receive it). Never a fallback to the struct. |
| Subclass | Its own name, so its own identity, fields and tables. Never stored as its base. |
| Nested class | The outer record embeds the inner record (`{"class": …}`), so a change to the inner class changes the outer identifier. An ordinary class may have fields of any kind. |

The measured case (QOld, QNew; pod A `-> QOld`, pod B `p: QNew`): as ordinary classes they
are two types, B refuses A's output at graph build, on both paths.

## b. `orcapod.dataclass` (structural)

### b.1 Public API

```python
import orcapod as op

@op.dataclass
class Point:
    x: int
    y: float

@op.dataclass(name="geo.Point", frozen=True)
class Point: ...
```

- Decorator only, bare or with keywords. It calls `dataclasses.dataclass` with the given
  options and returns a **real dataclass**: `dataclasses.is_dataclass`, `fields`, `asdict`,
  `replace`, pattern matching all work. It is declared with `typing.dataclass_transform`,
  so type checkers and editors see a dataclass.
- Options: every `dataclasses.dataclass` keyword is passed through (`repr`, `eq`, `order`,
  `unsafe_hash`, `frozen`, `match_args`, `kw_only`, `slots`, `weakref_slot`). `init=False`
  is rejected (`TypeError`): Orcapod builds instances through `__init__`.
- `name=`: optional. Default `cls.__name__`. Rules as in PR #292: non-empty, no `@`, `;` or
  whitespace, not starting with `orcapod.`, NFC-normalized.
- It raises `TypeError` for a class that is already a dataclass or a pydantic model: the
  kind is fixed where the class is defined, and there is no call form for classes you do
  not own (wrap them: define a structural class with the same fields and construct it).
- A local class is allowed: nothing is looked up by module path.
- What it adds to the class (O6): a marker attribute with the name, `from_instance`, and an
  `__init__` that wraps the generated one (§c.1; `inspect.signature` is preserved).
- Field types allowed: scalars, types with a logical type, containers and unions of these,
  and other **structural** classes. A field typed by an ordinary dataclass or a pydantic
  model raises `TypeError` at first use, naming the field: a structural class must be
  rebuildable from its metadata, and an exact class is not. (The reverse is allowed.)

### b.2 Identifier

Record `{"kind": "structural", "name": <name>, "fields": [[field, token], …]}`, fields in
declared order (D-order). Extension name `Point@<digest>`, category `orcapod.structural`.
The module path is not part of it.

- Two structural classes with the same name, fields, types and order: **one type**. Same
  hashes, same tables, accepted for each other at every check.
- Different name, same fields: different types (D3: the name is part of it).
- Same name, different fields or order: different types that share a display name. They
  coexist in one process (two extension names).
- A structural class and an ordinary class with the same name and fields: different types
  (kind differs).

### b.3 Metadata layout and reconstruction

```json
{"category": "orcapod.structural",
 "identity": {"kind": "structural", "name": "Sample",
   "fields": [["id", "str"],
              ["where", {"class": {"kind": "structural", "name": "Point",
                                   "fields": [["x", "int"], ["y", "float"]]}}],
              ["tags", {"generic": "list", "args": ["str"]}],
              ["note", {"union": ["str", "None"]}],
              ["file", {"logical": "orcapod.path;7b2276657273696f6e223a317d"}]]}}
```

Rebuilding a Python annotation from a token: scalars by a fixed table; a stdlib canonical
name by import; `logical` through the reading context's registry (the extension name must
be registered and the metadata readable, else `LogicalTypeIdentifierError`); `generic`,
`union`, `literal` recursively; `class` with kind `structural` by the rule of §b.4. The
rebuilt class's record must equal the stored record, checked before it is used.

### b.4 Reading

1. The identifier is bound in the reading context: use the bound class.
2. A structural class with that name was defined in this process (a weak table filled by
   the decorator) and its record equals the stored one: use the most recently defined one.
3. Otherwise **synthesize** it: `dataclasses.make_dataclass` from the record, passed through
   `op.dataclass(name=<stored name>)`, module `orcapod.synthesized`. It is a normal
   structural class: attribute access, `asdict`, equality, and it converts to and from any
   class of the same shape. Nothing is imported and nothing can fail for a missing module.

If the user defines a matching class later, it becomes the bound class when first used
(same rebinding rule as §a.3). Which class a *read* returns therefore depends on what is
defined; what a *pod parameter* receives does not (§c.3).

### b.5 Schema compatibility

Two structural classes are accepted for each other when their records are equal, at the top
level and inside generics and unions. Everything else about the check is as for any type.

### b.6 Instance equality (O7)

Unchanged stdlib behaviour: `==` is true only for the same class. Not changing it keeps the
decorator from replacing a user's or the stdlib's `__eq__`; values that reach a pod are
already instances of the annotated class.

## c. The conversion rule (D4)

### c.1 Call form (O3)

- `P.from_instance(other)`: the definition, a classmethod.
- `P(other)`: the same conversion, taken when the call has exactly one positional argument
  and no keywords, the argument is a dataclass instance, **and** `P`'s first field is not
  annotated with a dataclass type (nor `Any`/`object`). Otherwise the call is the normal
  field constructor, exactly as generated by `dataclasses`. The choice depends on the class
  definition and the argument's kind, never on values. For a class whose first field is a
  dataclass, use `P.from_instance(other)`.

### c.2 What "accepts" requires (O4)

- `other` is an instance of a dataclass: ordinary or structural. A pydantic model, a dict
  or anything else raises `TypeError` (D5).
- `other`'s stored field names equal `P`'s exactly. A missing or an extra field raises
  `TypeError` naming the fields: dropping a field would lose data silently, and filling a
  default would invent it. Order is not compared (fields are copied by name).
- Declared field types must agree: equal annotations, or, for a field `P` types with a
  structural class, a dataclass instance that this rule converts recursively (also inside
  `list`, `set`, `tuple`, `dict` values and `Optional`). Anything else raises `TypeError`
  naming the field and both types. Values are not otherwise validated or coerced.
- The result is a new instance built by `P`'s own `__init__` (so `__post_init__` runs);
  `other` is not modified. `P(p)` for `p` already a `P` returns a copy built the same way.

### c.3 Where Orcapod applies it automatically

One place: where a Python data function's keyword arguments are assembled from the row
(`PythonDataFunction`, sync and async, all four call sites through one helper). For each
parameter annotated with a structural class `P` (also inside `list`/`set`/`tuple`/`dict`/
`Optional`), a value whose class is not `P` is replaced by `P.from_instance(value)`.

- The schema check has already established that the column's type has `P`'s identity, so
  this is a change of class object, not of type: no field is added, dropped or retyped.
- In memory the value is the upstream's object (possibly of another class of the same
  identity); from a store it is an instance of whichever class the context has bound. Both
  go through the same helper, so **the parameter always receives an instance of `P`**.
- The measured case as structural classes: B receives `QNew` on both paths; `describe()`
  works and `__post_init__` of `QNew` runs.
- Nothing else converts: operators, sources and stores pass values through. Writing a value
  to a structural column requires an instance of a structural class with the column's
  identity (else `TypeError` saying to construct it).

### c.4 Ordinary instance at a structural parameter (O5)

Not accepted by the schema check: the column's type is the ordinary class, which is another
type with another identifier. The user converts explicitly in a pod
(`def to_point(p: OldPoint) -> Point: return Point(p)`). This keeps "no implicit conversion
between types", keeps every column's identifier equal to what its schema says, and leaves
D4 as what the developer stated: a property of the constructor.

## d. Pydantic

Exact class, the rules of §a with kind `pydantic` and category `orcapod.pydantic`: name is
module path plus class name; the O1 answer applies to it as well; `is` in the schema check;
`type(value) is cls` on write; read by import and identifier comparison; a subclass is its
own type. No structural kind, no `op.register_type`. A pydantic instance is not accepted by
a structural constructor, and a structural class cannot have a pydantic field.

Kept from PR #292: the shared `UserClassLogicalType` base and factory, `model_fields` as
the stored fields in declared order, building values with the model's constructor (so
validators run on read), `None` for a null nested model, the `TypeIdentityError` handling.
`pydantic.dataclasses.dataclass` classes are ordinary dataclasses.

## e. What moves

Stored layouts (`pdb_v2`, `rdb_v2`, `tdb_v2`, `odb_v1`, `sdb_v1`), the data context key and
the released-manifest pin do not change: this is a change of logical type identifiers only.
The new category `orcapod.structural` is registered in `contexts/logical_types.json`.

**Relative to the ITL-967 branch** (dataclass and pydantic identifiers there: extension name
`module.qualname`, metadata `{"category": "orcapod.dataclass"}`):

| Cause | What moves |
|---|---|
| Variant IN: the record (fields) enters every exact class's identifier | Every pinned value that involves `GoldenPoint` or `GoldenModel`: in `semantic.json` `type.dataclass`, `type.pydantic`; in `schema.json` `multi_field.logical_mix` and `single_field.{dataclass,pydantic}.{arrow_type,schema_hash}`; in `datagram.json` `{dataclass,pydantic}.{content_hash,pipeline_hash,system_tag_columns}`, the two rows' `data_hash`, and the keys that embed the moved pipeline hash. That was 25 entries when measured for PR #292 on `main`; I re-measure against the ITL-967 tip before regenerating and list each entry in the PR. For users: every pipeline with a dataclass or pydantic column gets new pipeline hashes and table paths. |
| Variant OUT instead | No existing entry moves. |
| New fixtures | Added keys only: structural classes (default name, explicit name, nested, container and optional fields, field-order twins with different values, two classes of one identity with equal values), an ordinary subclass, an ordinary class with a structural field. |
| Stored extension names and metadata | Exact classes (variant IN): `module.qualname` becomes `module.qualname@<digest>`, metadata gains `identity`. Structural: new. A column written by v0.2.0 or by the ITL-967 branch read directly raises `TypeIdentityError` (no identity record), as in PR #292. |

**Relative to PR #292** (never merged, so no stored data exists): every user-class
identifier moves, for three causes: `__main__` classes are named `__main__.X` instead of
`X`; tokens are structured, a nested class embeds its record, and a logical-type field uses
the whole identifier instead of the bare name; declared names no longer exist. PR #292's
golden fixtures that use `register_type` (`golden.Declared`, `golden.Inner`/`Outer`,
`golden.WithArray`, `golden.Twin`) are replaced by structural fixtures; none of PR #292's
golden values survives.

## Merge conflicts (ITL-967 tip + PR #292 branch)

Six files. Proposed resolutions:

| File | Conflict | Resolution |
|---|---|---|
| `databases/delta_lake_databases.py` | import list | union of both (obvious) |
| `logical_types/database_hooks.py` | ITL-967 walks per column and adds the column to `LogicalTypeIdentifierError`; PR #292 walks the whole schema and searches the columns afterwards for `TypeIdentityError`/`ImportError` | ITL-967's per-column loop, catching all three errors and adding the column there; PR #292's column search is dropped |
| `logical_types/registry.py` (`_deserialize`) | metadata mismatch: ITL-967 raises `LogicalTypeIdentifierError` against `readable_metadata`; PR #292 raises `TypeIdentityError` on inequality | ITL-967's check. PR #292's other change in that function (large/non-large tolerance at any depth; storage mismatch as `TypeIdentityError`) merged without conflict and is kept |
| `golden/data/{datagram,schema,semantic}.json` | both sides regenerated | take the ITL-967 side, regenerate once after implementation |

## f. Risks, and the order of cuts

Risks:

1. Four days to the tag for a new public API and a hash change. The identity mechanics are
   PR #292's tested code; the new parts are the tokens, the synthesized class, the
   constructor wrapper and the pod-input helper.
2. Operators that compare Python types across streams (`MergeJoin`, `Join` on shared keys)
   must use the same structural equivalence as the schema check, or two classes of one
   identity are refused there. I audit every Python-type comparison PR #292 touched.
3. `slots=True` makes `dataclasses` build a new class; the wrapper is applied after it. Tested.
4. Exact `is` matching is stricter than v0.2.0 in two places (subclass column at a base
   parameter; duck-typed values on write). Both are intended by D2 but can surprise.
5. A synthesized class has no methods. Reading without the class defined gives data, not
   behaviour; documented.

Cuts, first cut first (each leaves a working, documented state):

1. `P(other)` sugar: keep only `P.from_instance(other)`.
2. Automatic conversion inside containers at pod inputs (`list[P]`, `dict[str, P]`):
   top-level and `Optional[P]` parameters only; a container of another class of the same
   identity raises.
3. `Literal` tokens in structural classes (rejected with a clear error).
4. Recursive conversion of nested structural fields in `from_instance` (the nested value
   must already be of the declared class).
5. The same-path hint in the schema-check error message.

Not cut: the two kinds, exact matching, the structural identifier and metadata, reading
without the class (synthesis), conversion at a top-level pod parameter, the regression test
for the measured case, docs, changelog, golden list by cause.

Out of scope: structural pydantic (ITL-973); migration of v0.2 data; computed fields;
Enum, NamedTuple, TypedDict.
