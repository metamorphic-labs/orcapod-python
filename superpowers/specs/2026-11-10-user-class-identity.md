# What makes two user classes "the same"? (ITL-881)

**Status:** findings and options for a developer decision. Nothing is implemented.
**Issue:** ITL-881 (v0.3, workshop tag target November 16, 2026).
**Evidence:** `superpowers/specs/user-class-identity-experiments/` (scripts E01–E08).
Every claim below cites a script (E01 = `e01_where_identity_enters.py`, and so on) or
a source line, or is marked **predicted** (reasoned from the verified facts, not run)
or **not verified**.

## 1. Summary

- A user class enters Orcapod's identity through one string, its **logical type name**.
  Today that string is `f"{cls.__module__}.{cls.__qualname__}"`. It becomes the Arrow
  extension name, and from there every schema hash, pipeline hash, pod hash,
  system-tag column name, DB table path and data hash (E01).
- The class's **fields are not part of the pipeline identity**. Two classes with the
  same name and different fields share pod hashes and DB tables. Only the per-row data
  hash sees the fields (E01). When the fields differ, the stored table can no longer be
  read, and the run returns zero rows with only log warnings (E03 S5/S6, E04).
- The worst workshop problem is not `__main__` itself. **Re-running a notebook cell that
  defines a dataclass or pydantic model breaks every later pod definition and run with a
  `ValueError` until the kernel restarts**, even when the fields are identical (E02).
  `%autoreload` of a module does the same.
- Moving a class from a notebook into a module, or running a script by import instead
  of as `__main__`, recomputes everything under new tables. Old results stay on disk
  under the old tables and are found again if the old name comes back (E03 S3, S7, S8).
- **A declared name already works end to end.** Registering the class under a chosen
  name (internal API today) makes every hash, column name and table path identical
  whether the class lives in `__main__` or in a module (E07). Only a public entry point
  and a read-side lookup are missing.
- Any change to the identity string of an existing class moves **25 entries** pinned by
  PR #284 (E08). Options that keep today's string for classes that do not opt in move none.

## 2. The question

The developer's position (November 10, 2026), which this document takes as given:

> Technically, a dataclass defined in one module is not the same class as one defined
> in another module, even when everything about the definition is shared. That is why it
> is technically necessary to register a dataclass with Orcapod for it to be treated
> robustly. At the same time, dataclasses are often considered the same when they
> consist of the same properties.

Two things must be decided separately, because the experiments show they fail
separately:

1. **Name:** which string identifies the class (module + qualified name, a declared name,
   or a structural key).
2. **Shape:** whether the field structure is part of the identity, and what happens when
   the same name arrives with a different shape (a redefinition in the same kernel, or a
   changed class in a new process).

## 3. Findings

### 3.1 Where the class's identity enters

Same `Point(x: int, y: int)` defined in different places; each in a fresh process (E01).
"name" = changes when the class name or module changes. "fields" = changes when the
fields change under the same name.

| Component | name | fields | Evidence |
|---|---|---|---|
| Arrow extension name (stored in the column's field metadata) | yes (`__main__.Point`, `models_a.Point`) | no | E01 |
| Arrow extension metadata | no: always `{"category": "orcapod.dataclass"}` | no | E01 |
| Arrow storage type | no | yes | E01 |
| Python schema `repr` | no (`Schema({'p': Point})`) | no | E01 |
| Source `pipeline_hash` (schema hash) | yes | **no** | E01 |
| System-tag column names (`_tag_source_id::<pipeline_hash>`) | yes | **no** | E01 |
| Row data `content_hash` | yes | yes | E01 |
| Pod `content_hash` / `pipeline_hash` (signature) | yes | **no** | E01 |
| Node `pipeline_hash`, node identity path | yes | **no** | E01 |
| DB table paths (`<job>/<pod>/<pod hash>/…/schema:<node hash>/…`) | yes | **no** | E01 |
| The pod function's own module | does not enter the pod hash | — | E01 control |
| Ray executor | same pod hash and table as a local run | — | E05 B |

Mechanism. The factories build the name from the raw `__module__`
(`dataclass_logical_type_factory.py:253`, `pydantic_logical_type_factory.py:261`). A
class object inside a schema or signature hashes as `type:<logical_type_name>` once it is
registered (`builtin_handlers.py:114`). `canonical_type_names.py` does not touch user
classes: `canonical_module_name` returns `__module__` unchanged for anything outside the
standard library (`canonical_type_names.py:66`), and the factories do not call it.

Nested and container types carry the name too: `list[Point]` is stored as
`list[__main__.Point]`, and a dataclass field of type `Point` makes the outer type's
hashes depend on it (E06).

### 3.2 Registration today

| Question | Answer | Evidence |
|---|---|---|
| When is a class registered? | Implicitly, on first use (`DictSource`, `@function_pod`), or by `type_converter.register_python_class(cls)`. | E07 A |
| Under what name? | `f"{__module__}.{__qualname__}"`, always. | E07 A, `dataclass_logical_type_factory.py:253` |
| Can a user choose the name? | Not through public API. `register_python_class(annotation)` takes no name. Possible with internal classes (`DataclassLogicalType` + `register_logical_type`). | E07 A, B |
| Does a chosen name flow through? | Yes: extension name, all hashes, column names, table paths identical for `__main__` and module definitions. | E07 B |
| Classes inside a function (`<locals>`)? | Rejected at `DictSource` and `@function_pod` with a clear `ValueError` ("Define the dataclass at module level"). | E05 A |
| Same name, new class object (notebook re-run, reload)? | `ValueError: Cannot register logical type '__main__.Point': logical_type_name '__main__.Point' is already bound to '__main__.Point'.` | E02, `registry.py:308` |
| Precedent for stable names | Built-in types use declared names (`orcapod.path`, `orcapod.uuid`, `orcapod.file`). | `orcapod/__init__.py` |

### 3.3 Reading stored values back

Stored from `pkg.models.Point`, then read in a new process after a change (E04).
"aware read" = through `ExtensionAwareDatabase`, as Orcapod loads tables.

| Change before reading | Raw table | Aware read | Re-run the pipeline |
|---|---|---|---|
| none | struct | `Point(x=2, y=2)` | 0 calls (cache hit) |
| module deleted | struct | `ImportError: Cannot import from FQCN 'pkg.models.Point'` | not possible |
| class renamed | struct | same `ImportError` | 2 calls, new tables |
| moved to `pkg.shapes` | struct | same `ImportError` | 2 calls, new tables |
| `y` retyped to `float`, read first | struct | `Point(x=2, y=2)`: **ints in a float field**, no error | 0 calls: hits old results |
| `y` retyped to `float`, re-run first | **`None`** (read fails, logged) | `None` | **0 calls, 0 rows, no exception** |
| field `z: int = 0` added | struct | `KeyError: 'z'` | `KeyError: 'z'` |
| field `y` removed | struct | `Point(x=2)`: stored `y` silently dropped | 2 calls |
| stored from `__main__`, read in an analysis script | struct | `ImportError: … '__main__.Point'` | not possible |
| stored under declared name `ws.Point`, reader does not declare it | — | `ImportError: … 'ws.Point'` | — |
| stored under `ws.Point`, reader declares it (class from another module) | — | `Point(x=1, y=2)` | — |

(The last two rows are E07 C.)

Facts behind the table:

- The stored data is never lost: the raw Delta read always returns the struct values,
  except when a different shape is already registered under the same name in the
  reading process (E04 "re-run first").
- Within one process, **whichever shape registers a name first wins**. The read path
  takes the storage type from the stored table without comparing it to the class's
  fields; the write path takes it from the class. Arrow then rejects the other shape
  (`registry.py:132`). So the same code gives different results depending on call order
  (E04, two "y retyped" rows).
- The reading process must be able to import the class by its stored name. There is no
  fallback to plain dicts.

### 3.4 Workshop scenarios as they behave today

| # | Scenario | What the attendee sees | Evidence |
|---|---|---|---|
| W1 | Re-run the cell that defines `Point` (same fields) | Next pod cell or run cell: `ValueError … already bound to '__main__.Point'`. Persists until kernel restart. "Run All" fails the same way. | E02 1–3 |
| W2 | Edit `Point`'s fields and re-run the cell | Same `ValueError`. | E02 4–5 |
| W3 | Same with a pydantic model | Same `ValueError`. | E02 6 |
| W4 | `%autoreload` reloads a module defining `Point` | Same `ValueError` for `<module>.Point`. | E02 7 |
| W5 | Restart kernel, run all again (identical class) | Cache hit (name is `__main__.Point` both times). | E03 S1, S4 |
| W6 | Restart kernel after changing `Point`'s fields | Run "succeeds" with **0 rows**; stderr shows warnings ("Error reading Delta table: … expected storage_type …", "data may have been deleted externally. These inputs will be recomputed") but nothing is recomputed. | E03 S5, S6 (stderr printed) |
| W7 | Script `python pkg/pipeline.py`, then `python -m pkg.pipeline` | Cache hit: both are `__main__.Point`. | E03 S2 |
| W8 | `python -m pkg.pipeline`, then `import pkg.pipeline; main()` | Full recompute; a second set of tables (`pkg.pipeline.Point`). | E03 S3 |
| W9 | Two notebooks on one DB, identical `Point` | Second notebook hits the first's cache. | E03 S4 |
| W10 | Two notebooks on one DB, `Point` with different fields | Second notebook: 0 rows plus warnings (as W6). | E03 S5, S6 |
| W11 | Prototype in notebook, then move `Point` to `pkg/models.py` | Full recompute under new tables; going back to the notebook hits the old ones. | E03 S7, S8 |
| W12 | Read notebook results from a separate analysis script | `ImportError` for `__main__.Point`. | E04 last row |
| W13 | Ray executor with a `__main__` class | Works; same hashes and tables as local. | E05 B |
| W14 | `multiprocessing` spawn child | The class is `__mp_main__.Point` there. No Orcapod executor converts values in a spawn child today, so no effect was observed; a future process-pool executor would see a different name. **Effect on Orcapod not verified.** | E05 C |
| W15 | Class defined inside a function | Clear `ValueError` at first use. | E05 A |

### 3.5 Other class kinds

| Kind | Supported? | Name used | Evidence |
|---|---|---|---|
| dataclass | yes | `module.qualname` | E06 |
| pydantic `BaseModel` | yes | `module.qualname` | E06 |
| nested dataclass, dataclass with `Path` field | yes | outer `module.qualname`; inner fields stored as plain structs | E06 |
| `list[Point]`, `Point \| None` | yes | `list[module.Point]`, `module.Point` | E06 |
| `Enum`, `str` Enum | no: `TypeError: No LogicalType or LogicalTypeFactory registered` | — | E06 |
| `NamedTuple` | no: same `TypeError` | — | E06 |
| `TypedDict` | no: a pod annotation gives the same `TypeError`; a dict value with mixed field types is rejected by union rules | — | E06 |

Only dataclasses and pydantic models are in scope for the decision. Enum, NamedTuple and
TypedDict would need a decision of their own when they gain support.

## 4. Options

"Same" column: which two definitions are treated as the same class.
"Pinned moved" = entries of PR #284's golden files that change. E08 measures what
happens when the identity string of the golden fixtures (importable, undeclared classes)
changes: 25 entries move (2 semantic type hashes, 5 schema entries, 18 datagram entries
of which 8 disappear because their keys embed a moved pipeline hash). An option moves
those 25 if it changes the string for an importable, undeclared class, and none
otherwise. The mapping is **predicted**; the 25 are measured.

| Option | Same when | Pinned moved | Existing stored data |
|---|---|---|---|
| **(a) Nominal, as today** + warnings/errors and docs | same module + qualname | 0 | unchanged |
| **(b) Nominal with a declared name** (opt-in decorator or `register(cls, name=...)`; encouraged or required for `__main__`) | same declared name; undeclared classes keep module + qualname | 0 (fixtures do not declare) | unchanged unless a user declares a name for an already-stored class (then that class recomputes) |
| **(c1) Structural**: field names + types only | same fields, any name or module | 25 | every stored user-class result is stranded under old tables (still on disk; recomputed on next run) |
| **(c2) Structural + class name** (no module) | same name and fields, any module | 25 | as (c1) |
| **(d) Hybrid**: nominal for importable classes, (c2) for `__main__` | importable: module + qualname; `__main__`: name + fields | 0 | importable unchanged; `__main__`-stored results stranded |
| **(e) Nominal + shape**: today's name plus a structural fingerprint in the pipeline identity | same module + qualname **and** same fields | 25 | all stranded (as c1) |

Behaviour per scenario. Everything in this table is **predicted** from §3 unless an
E-reference is given; nothing here is implemented.

| Scenario | (a) | (b) declared | (c1)/(c2) | (d) | (e) |
|---|---|---|---|---|---|
| W1 re-run cell, same fields | needs the re-registration fix (§5 D3) | same | works if registry keys by structure | works for `__main__` | needs D3 |
| W2/W6/W10 same name, new fields | today: fails (E02, E03); D4 decides | same as (a) | new identity: clean recompute, new tables | `__main__`: clean recompute | clean recompute, new tables |
| W8 script vs import | miss (today, E03 S3) | hit if declared | hit | miss (one side importable) | miss |
| W11 notebook → module | miss (today, E03 S7) | hit if the same name is declared in both places (E07 B) | hit | miss (switches regime) | miss |
| W12 read from analysis script | `ImportError` (E04) | needs the declaration (or a fallback, D5) (E07 C) | needs some class to build; which one is ambiguous | as (a) for importable | as (a) |
| Different classes, same fields (`Celsius(v)`, `Meters(v)`) | distinct | distinct | (c1): **same identity; one class's cached results serve the other**; (c2): distinct by name | `__main__`: as (c2) | distinct |
| Two modules, same class name and fields | distinct | distinct unless same declared name | (c2): **same** | importable: distinct | distinct |

Read-back under each option:

- (a), (d) importable, (e): unchanged. The class must be importable under the stored name.
- (b): the reader must declare the same name before reading (E07 C shows the
  `ImportError` without it and a correct read with it).
- (c1), (c2): the stored key no longer names a class. Reading needs either a stored class
  hint plus a registry, or a return of plain dicts. Design not started.

Migration and shipping:

| Option | Code needed | Migration | Ready by Nov 16? |
|---|---|---|---|
| (a) | Messages and docs only (see §6) | none | yes |
| (b) | Public registration entry point; read-side name→class lookup; docs. Hashing already follows the name (E07 B). | none for existing data; a declared name on an already-stored class needs an alias (old name → new) to keep old results, **predicted** | entry point possibly; read-side lookup and aliasing unlikely to be review-ready |
| (c1)/(c2) | New identity key, read-path redesign, hash-change approval | all stored user-class data; golden regeneration with approval | no |
| (d) | Two code paths; `__main__` detection | `__main__`-stored data only | no |
| (e) | Fingerprint in the type hash | all stored user-class data | no |

## 5. Decisions the experiments expose (independent of the name choice)

| ID | Decision | Today | Hash impact |
|---|---|---|---|
| D1 | The identity string (a)–(e) | (a) | see §4 |
| D2 | Is a declared name allowed, encouraged, or required for `__main__` classes? | not possible | none if optional |
| D3 | A class object arrives under a name already bound to another object (cell re-run, reload). Options: reject (today); rebind when the stored shape is identical and reject with a clear message otherwise; always rebind. | reject with a confusing message (E02) | none: the hash uses only name + storage type, both unchanged (**predicted** from E01, E03 S4) |
| D4 | Same name, different fields (in one kernel or across processes). Options: clear error naming the class and both shapes; or make shape part of the identity (= option e). | silent 0 rows + warnings (E03 S5/S6); order-dependent reads (E04) | none for the error; 25 pinned moved for (e) |
| D5 | Reading when the class cannot be imported. Options: `ImportError` (today); fall back to plain dicts with a warning. | `ImportError` (E04) | none |
| D6 | Enum / NamedTuple / TypedDict support | unsupported (E06) | none now |

## 6. Smallest safe step for the workshop (changes no hash)

Each item changes behaviour or messages only. None changes a stored value, a hash, a
column name or a table path. "Predicted" because none is implemented.

| Step | Fixes | Hash change | Size |
|---|---|---|---|
| S1. Rebind on re-registration when the shape is identical (D3, middle option). Different shape: raise naming the class, old and new fields, and "restart the kernel or rename the class". | W1, W3, W4 (most common notebook failure); W2 gets a clear message | none | small, in `LogicalTypeRegistry.register_logical_type` / the converter's lookup |
| S2. Raise instead of logging when stored and registered shapes disagree (D4, error option), naming the column, the class and both struct types. | W6, W10 and the order-dependent read in E04 stop looking like "0 rows, success" | none | small, but touches the per-row failure isolation in `function_node.py` (~1299) and the DB read path; needs care so legitimate per-row failures still isolate |
| S3. Warn once when a `__main__` class is first registered: results are keyed to `__main__.Point`; moving it to a module or importing the script recomputes. | sets expectations for W8, W11, W12 | none | tiny |
| S4. Docs: a "Your own data types" section in `docs/concepts/function-pods.md` and `identity.md`: what the identity is, the table of §3.4 in user words, and "define classes in a module for anything you keep". | all | none | small; doc samples run in CI |

Not in the safe step: anything in §4 that changes an identity string.

## 7. Recommendation

The developer decides; this is the worker's reading of the evidence.

1. **For the workshop:** ship S1, S3 and S4; S2 if review time allows. They stop the
   breakage attendees will actually hit (W1), keep every hash, and keep the door open
   for every option in §4.
2. **Long term:** option (b), nominal identity with an optional declared name. It
   matches the developer's position that a class is identified by registration, it is
   the mechanism the built-in types already use (`orcapod.path`), the hashing pipeline
   already honours it (E07 B), and it moves no pinned value. A declared name is what
   makes "notebook → module" (W11) and "script vs import" (W8) keep their cache, which a
   module-qualified name cannot do.
3. **Keep shape out of the identity, but check it** (D4 error option). Structural
   identity (c1) would merge distinct classes that happen to share fields, and both (c)
   and (e) move 25 pinned values and strand all stored user-class results.

## 8. Decisions for the developer

1. D1: the identity string. Options (a)–(e), §4.
2. D2: may, should, or must a `__main__` class declare a name?
3. D3: what a re-registration under a bound name does (blocks the workshop today).
4. D4: same name with different fields: error, or part of the identity?
5. D5: reading without the class: `ImportError` or plain dicts?
6. Which of S1–S4 ship by November 16.

## 9. Out-of-scope findings (not fixed, not filed)

- `DeltaTableDatabase`: after the first run, `node.get_all_records()` returned 1 row while
  the node had 2, for a plain `int -> int` pod. `InMemoryArrowDatabase` returned 2. A
  second run returned 2. Seen in every E03 first run; reproduced without any user class.
- A process that has read Delta tables sometimes hangs at interpreter shutdown in Arrow's
  `ThreadPool` destructor (native stack captured with `sample`). Seen twice in ~40 child
  processes; a minimal reproduction exited normally. The experiment harness exits with
  `os._exit(0)` to avoid it.
- Misleading log text on a shape mismatch: "data may have been deleted externally. These
  inputs will be recomputed" while nothing is recomputed (E03 S5; `function_node.py:1964`).
- Silent data changes on read: stored `int` values come back in a `float` field, and a
  removed field's stored values are dropped, without a warning (E04).

## 10. Not verified

- Python 3.11 and 3.13 (all runs on 3.12.15, macOS). The identity string uses the raw
  `__module__`, which does not vary for user classes, so no difference is expected.
- Real Jupyter kernels (E02 uses IPython's `InteractiveShell`, which runs cells the same
  way for class definition; kernel transport was not exercised).
- Every row marked "predicted" in §4–§6.
