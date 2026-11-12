# Changelog

## [0.3.0] — unreleased

### Breaking changes and migration

#### Upgrading from 0.2.0: what a v0.2 user does

**v0.3 reads no table written by v0.2, and it moves every stored hash.** Nothing is migrated,
repaired or deleted: v0.2 tables stay as v0.2 left them and stay readable with v0.2. The first
v0.3 run computes every input again into new tables. A migration tool is planned (ITL-889,
ITL-913, ITL-914). The sections below give the detail; this table says what to do.

| Change | What you do |
|---|---|
| Every table has a new path and a `__data_context` column; data context `std:v1:default`; layouts `pdb_v2`, `rdb_v2`, `tdb_v2`, `odb_v1`, `sdb_v1` (ITL-944) | Nothing to run. Expect a full recompute. Drop the old tables by hand when you no longer need them with v0.2. Remove `ignore_schema`, `orcapod.migrations` and `orcapod migrate` from your code; they are gone. |
| A node with side effects (side-effect pod, or a function pod taking the invocation context) finds a v0.2 log | It raises `SideEffectLogDataContextError` before delivering anything. Keep using v0.2 for that node, or accept that a fresh store repeats every side effect. |
| Every Arrow-derived hash changed: starfix hash format 2 (ITL-910), wrong hashes fixed (ITL-942), dict and set values hash in any order (ITL-905), function signatures and defaults hash as structures (ITL-953) | Nothing: stored v0.2 results are not looked up. Code that compares or stores hashes of v0.2 must recompute them. |
| Hash labels are `<algorithm>_v1` (`semantic_v1`, `arrow_v1`, `directory_v1`, `ndarray_v1`, `polars_expr_v1`), and a hash inside a name is a bare hex digest (ITL-945) | Change anything that parses a stored hash column or a path component. |
| Logical type identifiers carry versioned metadata (ITL-935); arrays are stored in the new `orcapod.ndarray` format (ITL-888) | Re-run pods that handle arrays. Anything that read the old `.npy` storage must read the new format. |
| Default source ids are `<kind>:<label>:<hex>`, and a Spiral source's id is `<project id>/<dataset>/<table>` (ITL-957, ITL-926); `tags_as_data` has its own id kind (ITL-939) | System tags and source-info values differ from v0.2. Pass an explicit `source_id` where a stable name matters. |
| Your own classes are exact (dataclass, pydantic) or structural (`orcapod.dataclass`) (ITL-972, ITL-881) | Two different exact classes are never the same type. Use `orcapod.dataclass` where two classes should be compatible by name and fields. |
| A function output must be an instance of the declared type or an exact numeric equivalent (ITL-895, ITL-901) | A pod that returned `float` for `int`, or a set for a list, raises `OutputTypeError`. Fix the annotation or convert explicitly. |
| `GroupBy` keeps non-key tags as list-valued tags (ITL-955); a Delta append with a changed column type is refused (ITL-884); a failed store write fails the run (ITL-878) | Read the sections below; downstream code that took tag values from data columns must change. |
| Source-info tokens stored by 0.2.0 before the ITL-845 fix stay wrong in v0.2 tables | None: v0.3 does not read those tables, and the recompute writes correct rows. |

The general policy for stored data that a later fix leaves wrong or out of date is not decided
yet (ITL-853); v0.3 never silently hides pre-computed results: it reports old tables, or
refuses, as described under ITL-944.

#### NumPy arrays: new storage format and new hashes, no automatic migration (ITL-888, ITL-883)

**The array format and every array hash changed in v0.3.** A store written by v0.2 that holds
`numpy.ndarray` columns is **not migrated**. Every node that handles arrays gets new tables,
so its v0.2 results are no longer found; and since every Arrow-derived hash changed too
(ITL-910, below), no v0.2 result is reused at all: v0.3 ignores the rows a v0.2 store holds and
computes every input again. Nothing written by v0.2 is changed or deleted, and those tables stay
readable with v0.2. A migration is planned after v0.3 (ITL-889).

- **Storage.** An array column is now the Arrow extension type `orcapod.ndarray` (metadata
  `{"version":1}`) over `struct<dtype: large_string, shape: large_list<int64>,
  data: large_binary>`: an Orcapod dtype tag, the shape, and the elements in C order,
  little-endian, packed. It replaces the `.npy` file under `numpy.ndarray`; that name is no
  longer registered or written.
- **Identity.** An array is its dtype, shape and element bits. A structured dtype's field
  names, types and order are part of the dtype, at every nesting depth. Memory layout, byte
  order, structured offsets and padding, and subclass are not part of it. Arrays read back
  canonical: C order, little-endian (`>f4` reads back `<f4`), structured fields packed in
  their declared order, as plain `np.ndarray`.
- **Hash.** `NumpyArrayHandler` returns `ContentHash("ndarray_v1", digest)`. The digest is
  SHA-256 over a length-prefixed encoding of the stored row (`docs/reference/hashing.md`,
  section 8), and the label replaces the bare `sha256`. Because the logical type name is now
  `orcapod.ndarray`, the type, schema and pipeline hashes of anything with an array column
  change too, and so do the table paths of those nodes. Pipeline hashes are chained, so the
  pipeline and operator tables of every node downstream of an array column move as well,
  even when it has no array column itself. A pod whose inputs and outputs hold no array
  keeps its function result table, and nodes with no array column upstream keep their tables;
  but an array-free pod does **not** reuse its v0.2 results: every Arrow-derived hash changed
  as well (ITL-910, below) and every table moved to a new layout (ITL-944, below), so the
  v0.2 tables are not read and the inputs are computed again.
- **Refused at write time** with `orcapod.errors.UnsupportedValueError` (a `ValueError`), naming
  the column and dtype: masked arrays (the mask used to be dropped silently), object dtypes,
  NumPy 2 `StringDType` (used to fail with a misleading pickle error), `longdouble` and
  `clongdouble`, datetimes and timedeltas without a unit, and structured dtypes with titles
  (whether to accept these last three kinds later is tracked in ITL-911).
- **Function pods.** A pod declared `-> np.ndarray` that returns something other than an array,
  such as a NumPy scalar, now raises `TypeError` naming the output.
- **MergeJoin** orders merged arrays by their stored `(dtype tag, shape, data)`, and lists of
  arrays (an array column after `GroupBy` or `Batch`) by their elements' keys, in order.
- **Pick** on a column whose logical type has no keyed access (an array, a dataclass, a
  pydantic model) raises `InputValidationError` naming the column, instead of
  `NotImplementedError` (ITL-883).
- `LogicalTypeRegistry.get_by_python_type` now picks the most specific registered class along
  the MRO, so a registered `np.ndarray` subclass wins over `np.ndarray` for itself and its
  subclasses, whatever the registration order.

#### A changed column type is refused when appending to a Delta table (ITL-884)

Before writing to an existing `DeltaTableDatabase` table, the flush now compares the new rows'
column types with the stored ones. If a column's type differs, the flush raises
`InputValidationError` naming the table, the column and both types, and writes nothing to that
table (other tables' pending rows in the same flush may still be committed). Before,
Delta's merge silently cast the new rows to the stored type (`int32` or `float64` rows written
into an `int64` column, `large_binary` into `large_string`), or, when an extension (logical) type
was involved, accepted rows the table could then no longer read. The comparison treats types the
way Delta stores them: `string` and `large_string` (and the other large and non-large layouts)
are the same type, types the Delta writer converts on the way in are compared as converted
(unsigned integers as signed, every timestamp in microseconds), an extension type is compared by
its storage type, and nullability is not compared. Struct fields are compared by name at any depth, including inside lists and maps.
New and missing columns, and new and missing struct fields, pass the check; whether Delta's
merge adds a new one follows the database's `allow_schema_evolution` setting, as before.

`DeltaTableDatabase.flush()` (and `add_records(..., flush=True)`) now raises a
`StoreReadError` or this `InputValidationError` as itself. Before, it wrapped every failure in a
`RuntimeError`. When several tables fail in one flush, the `StoreReadError` is raised, or else the
first `InputValidationError`, with each other failure attached as a note and logged. Failed rows
stay queued for the next flush. A flush that fails only for other reasons still raises
`RuntimeError`, with the first failure as its cause.

- Saved pipeline/job schema text now includes the full logical type identifier as
  `extension<name;hex-metadata>`, including nested logical types. Loading validates it
  against the registered type; incompatible or malformed identifiers raise
  `LogicalTypeIdentifierError` naming the node and column. Name-only extension text from
  older saved files is rejected with an earlier-version error. Read those files with the
  version that wrote them, and save a new job from the original pipeline under this version.
  Stored table layouts, value decoding and hashes are unchanged. Unregistered logical types
  are reported by node and column; affected operators load as read-only stubs. (ITL-904)

#### A `dict` or `set` value hashes the same whatever order it was built in (ITL-905)

A `dict[K, V]` value is stored as `large_list<struct<key, value>>` and a plain `set[T]`
(element type without a logical type) as `large_list<T>`. Both were stored in Python
iteration order, so `{"a": 1, "b": 2}` and `{"b": 2, "a": 1}` had different row hashes,
and the same `set[str]` had a different row hash in every process (`PYTHONHASHSEED`).
Orcapod now stores dict entries sorted by key and set elements sorted, wherever a Python
value becomes Arrow; see the hashing reference, section 1e, for the order. A NaN key or
element, values that cannot be compared, and two keys that convert to the same stored
value raise `CanonicalOrderError` (a `ValueError`) naming the column. A datagram built from
Python values (and values set with `update` or `with_columns`) holds its dicts in the same
order, so a newly computed pod output and one served from the cache iterate alike. Dicts
nested in dataclass or pydantic instances are the exception: a fresh instance keeps insertion
order, while a read-back instance uses key order. An Arrow table is still hashed exactly as
stored: a key/value list that arrives in Arrow form is never re-sorted.

Set values passed to `update` after Arrow materialization, or to `with_columns` with an
explicit Arrow list field, are also stored in canonical order. The existing Arrow field
(width, nullability and metadata) is preserved; actual list values keep their supplied order.

Which hashes move because of this change, on top of the v0.3 hash step (ITL-910, ITL-945),
which moves every stored hash: only those of values holding a dict whose insertion order was
not key order, or a `set[T]` of two or more elements whose iteration order was not sorted
(for `str` and `bytes` that order changed with every process, so it had no stable value).
For such a row, its data hash moves, and with it the function pod cache key, the pipeline
record and entry ids, and the default `source_id` (with its per-row record ids) of a
source built without an explicit `source_id`. Schema hashes, `pipeline_hash` values and
explicit `source_id`s do not move for this reason, and a dict already built in key order is
stored byte-for-byte as before if every nested unordered container is also canonical.
In the golden hash suite exactly two entries moved:
`pipeline:index_pick.index.row[{"subject":"s1"}].data_hash` and `...s2...`, whose rows hold
`meta={"k": ..., "j": ...}`.

What a `0.2.0` store sees (checked against a Delta store written by `0.2.0`): nothing
changes because of this fix. v0.3 reads no table `0.2.0` wrote (every table is at a new path,
see "Data context `std:v1:default`", ITL-944), so every input is computed again whether its
dict was in key order or not, and no row stored in `0.2.0`'s insertion order is ever served.
The `0.2.0` tables stay where they are, untouched. Every dict and plain set a v0.3 store
holds from a pod output or a source built from Python values is in canonical order.

#### Arrow hashing moves to starfix hash format 2: every Arrow-derived hash changes (ITL-910)

**Orcapod now requires `starfix>=0.5.0,<0.6`, and the Arrow hash label moves from `arrow_v0.1`
to `arrow_v1`.** starfix 0.4 (hash format 1) lost information for some inputs, so different
data could hash the same (ITL-848): the lengths of nested lists (`[[[1]], [[2], [3]]]` and
`[[[1], [2]], [[3]]]`), a null struct and a struct whose fields are all null, and Boolean
columns, whose hash also depended on how the table was split into chunks. starfix 0.5 (hash
format 2, prefix `00 00 02`) is lossless; this is a fix, so the old values are not kept.

- **What moves.** Every hash computed from an Arrow table: per-row data hashes, default
  `source_id`s and the `record_id`s and source content hashes built from them, function
  input-data hashes, pipeline base and entry ids, operator and cached-source record hashes,
  invocation hashes, and the hashes of `pa.Table`, pandas `DataFrame` and `Series` values. The
  hash label in front of each (`arrow_v1:000002...`) shows the new format. Pipeline hashes,
  schema hashes, system-tag column names and table paths under the default
  `table_scope="pipeline_hash"` do not move for this reason; they move in v0.3 because of
  the hash label rule (ITL-945, below). Paths that embed a content hash
  (`table_scope="content_hash"`, `CachedSource` tables) move with it. Where a `GroupBy` or
  `Batch` orders members by `record_id`, the order of members can change too.
- **Results stored by 0.2.0 are not reused.** Every table v0.3 writes is at a new path and
  every row names its data context; see "Data context `std:v1:default`" below (ITL-944) for
  what a v0.2 store looks like to v0.3.
- **Now hashable.** `fixed_size_list` columns, at any depth (ITL-927): a source or operator
  carrying one works, and its type is kept. A `fixed_size_list<T>[n]` hashes differently from a
  `large_list<T>` with the same values. pyarrow's canonical extension types (`arrow.uuid`,
  `arrow.fixed_shape_tensor`) hash with their extension name and metadata, so an `arrow.uuid`
  column never hashes like the `fixed_size_binary(16)` column holding the same bytes, and
  `StarfixArrowHasher.hash_schema` keeps the identity of extension columns (ITL-846).
- **Now refused** with `ValueError` naming the column path and type, where starfix 0.4 hashed
  silently: a non-nullable field holding a null; a zero-row table with a `map` column; two
  schema nodes sharing a path (duplicate column names, for example in a `pa.Table` value, or a
  column `"a/b"` next to a struct column `a` with a child `b`). Data of `null`, `map`,
  interval, `string_view` and `binary_view` columns, union and run-end-encoded types are
  still refused, now with `ValueError` instead of `NotImplementedError`. A source built from
  a table with such columns raises `ValueError` naming every refused column and its type.
- Column order and struct field order are still not part of an Arrow hash.

#### A Spiral source's default `source_id` is `<project id>/<dataset>/<table>` (ITL-926)

`SpiralDBTableSource` used the bare table name as its default `source_id`. Tables with the
same name in different projects (the EDI sample projects all share their table names) then
had the same `content_hash`, record IDs and source-info tokens. In one store, a job on one
project read back the other project's pipeline records along with its own (cached results
were never mixed up: they are keyed on the row's data). The default is now the project id,
the dataset and the table name, for example `edi-sample-v1-855632/default/session`.

For a Spiral source built without `source_id`, the `content_hash`, every record ID and every
source-info token change, so pipeline records stored by 0.2 for such a source are not found
again. The rows are recorded once more and no result is recomputed, because function results
are keyed on the data. For example, table `subject` (key `subject_id`, one row) in project
`edi-sample-v1-855632`:

- `source_id`: `subject` becomes `edi-sample-v1-855632/default/subject`.
- `content_hash`: `semantic_v0.1:0380725acb90...e7f4f6` becomes
  `semantic_v0.1:3a659ed610bb...29a93`.
- Row 0's record ID: `18d1efa53fbc52b6a40f406eca35ca81` becomes
  `096c4e22981c52daa2b088bffe729ee7`.

`pipeline_hash` does not change (it depends only on the schema), nor does anything for a
source given an explicit `source_id`. To keep 0.2 identities, pass the old value:
`SpiralDBTableSource(project, "subject", source_id="subject")`. Jobs saved by 0.2 store
their `source_id` explicitly and keep it when loaded.

#### Logical type identifiers: every built-in type writes one, and all of it is hashed (ITL-935)

A logical type is now identified by its Arrow extension name **and** its Arrow extension
metadata, and the whole identifier is part of the column's data type
(`docs/concepts/logical-type-identifiers.md`). The identifier belongs to whoever implements
the type and changes only when an existing value would be stored or hashed differently.

- **Built-in types write an identifier.** `orcapod.path`, `orcapod.upath`, `orcapod.uuid`,
  `orcapod.file`, `orcapod.directory`, `pandas.dataframe` and `pandas.series` now write the
  extension metadata `{"version":1}` (they wrote none); `orcapod.ndarray` already did.
  `list[T]` and `set[T]` of a logical type (the native containers) have no version of their
  own, by design: their metadata carries the element's name and new metadata, so their
  identifier changes exactly when the element's does.
- **The whole identifier is hashed.** A type hash, a schema hash and a function signature
  used to contain the logical type's name only. They now contain
  `<extension name>;<hex of the metadata>` (`LogicalTypeIdentifier.text`). The hashed cell
  of a value-hashed type (UUID, file, directory, array, pandas) starts with the same text,
  where it used to start with the name alone, written with `:` for `.`.
- **Fixed: `[]` and `[None]` hashed the same in a list or set of a value-hashed type.** The
  hashed cell of a `list[T]` / `set[T]` column (T a UUID, file, directory, array or pandas
  type) joined its element hashes with a separator and wrote nothing for a null element, so
  an empty list, `[None]` and `[None, None]` shared one hash. The cell now holds the element
  count and, per element, a null marker or a length-prefixed element hash.
- **What moves, once, in v0.3.** For every node whose schema or function signature has a
  column of a logical type (including dataclass and pydantic columns): its schema, function
  and pipeline hashes, hence the paths of its stored tables, and the hashes of the rows that
  hold such values. Together with the other v0.3 hash changes this means no result stored by
  v0.2 is reused; nothing stored is changed or deleted. **Nodes with only plain types are
  not affected by this change.**
- **Reading.** A stored column is read only if its stored metadata is one the registered
  type declares readable (`readable_extension_metadata()`, new on `LogicalTypeProtocol`,
  with a default in `BaseLogicalType`). Otherwise `LogicalTypeIdentifierError` (a
  `ValueError`) names the column, the stored identifier and what this installation reads.
  Before, a column read as plain storage was wrapped as the registered type whatever its
  stored metadata said.
- **Data written before v0.3 stays readable.** The seven types above read columns stored
  with empty metadata as version 1 (the layout is the same), for example a Delta table
  written by v0.2 and opened as a source.
- **For authors of logical types.** `make_arrow_extension_type` takes `also_reads`: older
  metadata values the type still decodes. An extension name may not contain `;`
  (`register_logical_type` raises `ValueError`). A custom logical type that does not derive
  from `BaseLogicalType` must implement `readable_extension_metadata()`.

#### Wrong hashes fixed: values that collided or depended on data, process or spelling (ITL-942)

A hash-site audit (ITL-938) found hashes that were **wrong**: two different things shared a
hash, or one thing hashed differently depending on its data, the process or how it was
written. A wrong hash is a bug that overrides hash stability, so these values move. Each fix
changes only the values that were wrong.

| Fix | Before | Now | What moves |
|---|---|---|---|
| Batch / GroupBy source-id fold | members concatenated with no separator: `["ab", "c"]` folded like `["a", "bc"]`, and a `None` member like `""` | SHA-256 of the canonical JSON list of members, in order | the `_tag_source_id` value of every row after a `Batch` or `GroupBy`, and the entry ids downstream of it. Column names and paths do not move. Source-info columns are unchanged |
| Arrow hashing of nested types | the hashed type was rebuilt from the first non-null value, so nested nullability counted for an empty or all-null column and not for a column with data | the hashed type is the declared type: struct and list fields keep their nullability, at every depth, with or without data | row hashes of struct or list columns with non-nullable nested fields, and of all-null or zero-row `Path`, `File`, `Directory` and `UUID` columns. Key values only |
| Set order in structure hashes | items sorted by `str()`, so `{1, "1"}` hashed differently per process (`PYTHONHASHSEED`) | items sorted by their canonical JSON text | structure hashes of sets whose `str()` order differs from their JSON order (mixed types, sets of tuples) |
| Dict keys in structure hashes | keys turned into `str()`: `{1: "x"}` equalled `{"1": "x"}`, and `{1: "a", "1": "b"}` lost a key | a dict with only `str` keys is unchanged; any other dict hashes as a sorted list of `[key, value]` pairs | structure hashes of dicts with non-`str` keys |
| Dicts that spell out an encoding | `{"__type__": "tuple", "items": [1, 2]}` hashed like `(1, 2)` (also set, namedtuple, generic alias, union) | only the hasher and type handlers write `"__type__"` objects; a dict with a `"__type__"` key hashes as a pair list | structure hashes of dicts with a `"__type__"` key. Handlers that returned a plain `{"__type__": ...}` dict must return `orcapod.hashing.semantic_hashing.Encoded(tag, fields)` |
| Namedtuple field order | fields sorted by name: `Point(x=1, y=2)` hashed like a `Point` declared `(y, x)` holding `y=2, x=1`, although the tuples differ | fields hashed in declared order | structure hashes of namedtuples |
| Column selection operators | `SelectTagColumns`, `SelectDataColumns`, `DropTagColumns`, `DropDataColumns` hashed `["x"]`, `("x",)`, `{"x"}`, `["x", "x"]` differently, and `["y", "x"]` unlike `["x", "y"]`, although the output is the same | identity is the set of names, hashed as a sorted list | pipelines that spelled the columns as a tuple, a set, or an unsorted or repeating list: their hashes **and table paths** move. Sorted lists (the usual spelling) keep their hashes |
| Record-id system tag type | `fixed_size_binary[16]` out of a source, `large_binary` after a `Tag`, so one row had two operator-cache keys (`cache_mode` LOG/REPLAY, sync run versus async orchestrator) | `large_binary` everywhere | operator-cache and cached-source row keys, and the content hash of a raw stream holding source system tags. On Delta Lake the stored column type does not change: Delta has a single binary type |
| Default function version | `v0.0` through `@function_pod`, `v1.0` through `FunctionPod.from_fn` and `side_effect_function_pod` | `v0.0` for every entry point | **table paths** of pods built with `FunctionPod.from_fn` or `side_effect_function_pod` without an explicit version: from `.../v1/...` to `.../v0/...` |
| Value-hashed logical types with no value (follow-up to the nested-type fix, found on the combined v0.3 branch, ITL-967) | a column of a value-hashed logical type (UUID, `File`, `Directory`, array, `DataFrame`, `Series`, and lists or sets of them) that had zero rows or held only nulls hashed like a plain `large_binary` column and like every other such type | the hashed field keeps the logical type's identifier (extension name and metadata), with or without values | the Arrow hash of every table with such a column, with data too: row data hashes, default `source_id`s and what derives from them |

**Migration.** A pod built with `FunctionPod.from_fn` or `side_effect_function_pod` that relied
on the old default keeps the major-version component of its paths (`.../v1/...`) by passing
`version="v1.0"`. That alone does not make v0.3 find tables written by v0.2: every v0.3 path
also differs in its hashes and its layout component (ITL-945, ITL-944). A pipeline that spelled
selected columns as a tuple or a set gets new tables for that operator and everything
downstream; no stored row is changed or deleted. Every other fix changes values only, never a
path or a stored schema.

Reviewed and kept by decision: `MergeJoin(m1, m1)` (a stream joined with itself) has a
different pipeline hash from `MergeJoin(m1, m2)` with the same schema, because a pipeline hash
covers the pipeline's structure; two inputs with the same source identity count as one.
Function default values in the function-value hash are tracked in ITL-953. See
`docs/reference/hashing.md`.

#### Data context `std:v1:default`: a `__data_context` column on every table, and every table at a new path (ITL-944)

**v0.3 writes every stored table at a new path, and records on every row which generation of
hashing wrote it.** A store written by v0.2 is not migrated and not modified: v0.3 does not
read its tables as results, and computes every input again into its own tables.

- **One data context key for the whole of Orcapod's hashing.** The default data context is
  now `std:v1:default` (manifest `orcapod/contexts/data/v1.json`). The version increments
  when, and only when, an existing hash value changes for a reason other than a logical
  type's own change. Everything stored before v0.3 (rows that carry `std:v0.1:default`, and
  tables without the new column) is the earlier generation. A manifest is frozen at the
  release of the library version that contains it, and may be edited in place only while
  that release is prepared: `orcapod/contexts/released_manifests.json` pins the sha256 of
  each released one, and a test fails when a pinned file is edited. `v1.json` is pinned
  when v0.3.0 is tagged.
- **Manifest contents.** Besides the wiring of the hashers, a manifest now has
  `released_in` (the library version that froze it; `null` while open), `previous` (the data
  context it follows), `definitions` (a name for each definition that lives in code),
  `libraries` (the libraries the core hashes depend on, with versions), `golden` (a digest
  of the golden hash files, set when the manifest is frozen) and `changes` (what differs
  from `previous`).
- **Logical types moved out of the manifest.** The registered logical types and the value
  handlers that hash their Python values are now in
  `orcapod/contexts/logical_types.json`, which is not frozen: a logical type is versioned by
  its own identifier, not by the data context. What the default context registers is
  unchanged. `JSONDataContextRegistry` and `contexts.create_registry` take
  `logical_types_file` to use another registration file; a manifest that lists its logical
  types inline is still built as written.
- **New column `__data_context`** (`large_string`) on every row of the function pipeline
  table, the function result table, the operator-cache table, the cached-source table and
  the side-effect invocation log. It **replaces** `__input_data_context_key` in the pipeline
  table and `_context_key` in the result table. In memory nothing is renamed:
  `ColumnConfig(context=True)` still shows `_context_key`.
- **New column `__pf_var_function_version`** in the function result table: the full version
  string the function was declared with (`"v3.7"`). Only the major part is in the table
  path, and until now no stored column held the version.
- **New table layouts, so new paths.** The last component of every table path moved:

  | Table | v0.2 | v0.3 |
  |---|---|---|
  | Function pipeline table | `…/schema:<pipeline hash>/pdb_v1` | `…/schema:<pipeline hash>/pdb_v2` |
  | Function result table | `…/_result/<function uri>/rdb_v1` | `…/_result/<function uri>/rdb_v2` |
  | Side-effect invocation log | `…/schema:<pipeline hash>/tdb_v1` | `…/schema:<pipeline hash>/tdb_v2` |
  | Operator-cache table | `<operator uri>/schema:<pipeline hash>` | `<operator uri>/schema:<pipeline hash>/odb_v1` |
  | Cached-source table | `source/node:<content hash>` | `source/node:<content hash>/sdb_v1` |

  The layout component is always the last one: `CachedSource(..., cache_path=("my", "cache"))`
  now stores at `("my", "cache", "sdb_v1")`, and `CachedSource.cache_path` returns that full
  path. Pass the path without the layout component as `cache_path`.
- **Reads and lookups use rows of the current data context only.** A result lookup matches
  on the input hash and the data context; every read of a whole table (a node's `as_table()`,
  `iter_data()`, `get_all_records()`, `as_source()`, `get_all_cached_outputs()` of a cached
  function, a stored operator node, a `CachedSource`) leaves rows of another data context
  out, leaves them unchanged, and logs one warning per table with their number (logger
  `orcapod.contexts.stored`).
- **What a v0.2 store looks like to v0.3.**
  - Its tables are at the old layout. v0.3 never reads them as results and never writes to
    them; they stay byte for byte as v0.2 left them and stay readable with v0.2.
  - The first v0.3 run **computes every input again**, also for functions and data that did
    not change, and stores the rows in the new tables. Before that run, reading a node
    returns no rows.
  - When v0.3 finds a table at an old layout, it logs one warning with the number of rows:
    `Found 3 rows in stored table 'my_job/.../pdb_v1' written by an earlier version of
    Orcapod (older table layout, data context std:v0.1:default). They are not used and are
    left unchanged; they can be migrated later.`
  - **Side-effecting nodes fail instead of delivering again.** A side effect is an external
    action, and a completion stored by v0.2 cannot be matched with current inputs. Both
    kinds of side-effecting node are covered: a side-effect pod (`@side_effect_pod`,
    `@sink_pod`, `@tap_pod`), whose completions are in its invocation log, and a function
    pod that takes the invocation context (`@function_pod(ctx_arg=...)`,
    `@side_effect_function_pod`), whose completions are in its function tables. Such a
    node that finds its tables at an old layout (or completions of another data context in
    its current tables) raises `orcapod.errors.SideEffectLogDataContextError` (a
    `StoreError`, so it fails the run whatever the `error_policy`) before it delivers or
    writes anything. The error names the tables. Other function, operator and source nodes
    of the same job are not refused. Keep using v0.2 for the node. A fresh store is not
    a safe way around the error: it holds no completions, so on it the node performs every
    side effect again; use one only when a repeated delivery is acceptable. The tables of
    an earlier version are found by listing the store (ITL-954): on a store that cannot
    list its tables the node logs one warning that it could not check, and runs, so the
    refusal cannot be relied on there.
  - `orcapod.contexts.stored_data_contexts(store, record_path)` counts the rows of a table
    by data context, for example `{"std:v1:default": 3}`; a table without the column reports
    its rows under `std:v0.1:default`.
- **`std:v0.1:default` is no longer shipped.** `orcapod/contexts/data/v0.1.json` is removed
  (it will be kept in `orcapod-sanctuary`), so `resolve_context("std:v0.1:default")` and
  `resolve_context("v0.1")` raise `ContextResolutionError` naming it as the data context of
  Orcapod before v0.3. A pipeline or job file saved by v0.2 still loads: each node that
  names `std:v0.1:default` is loaded under the current data context with a warning, and
  shows no rows until v0.3 has run (its v0.2 tables are at the old layout).
- **Removed: the v0 to v1 migration machinery.** `orcapod.errors.SchemaVersionError`,
  `NodeConfig(ignore_schema=...)` (and `NodeConfig.ignore_schema`), the package
  `orcapod.migrations` (`migrate_pipeline_db`, `migrate_result_db`, `MigrationResult`), the
  command `orcapod migrate` (`pipeline-db`, `result-db`), `ResultCache.base_record_path`,
  `ResultCache.set_ignore_schema` and `CachedFunctionPod.set_ignore_schema`. They converted
  tables without a layout component to `pdb_v1` / `rdb_v1`, which v0.3 does not read. A
  table without a layout component is now counted and reported like any other old layout,
  where it used to raise `SchemaVersionError`. Remove `ignore_schema` from your code; the
  migration to the v0.3 layouts is a later tool (ITL-913, ITL-914).
- **An input of another data context is refused.** A node, cache or cached source that
  would store a row for an input whose data context differs from its own raises
  `InputValidationError` naming both keys, before anything is computed, delivered or
  written. For a function node this is checked for the whole input, so it fails the run
  whatever the `error_policy`. Only one data context is shipped, so this needs a custom
  context registry to happen. It applies to nodes, caches and cached sources used
  directly. Inside a `PipelineJob` it is not reached: a job wraps every input in a source
  node of the default data context, so an input of another data context is not seen as
  such there (a known gap, ITL-951). `ResultCache.store` refuses an output of another
  data context in the same way.
- **A full data context key must match its manifest.** `resolve_context` selects a manifest
  by the version part of a key, and used to return it whatever the rest of the key said
  (`"custom:v1:production"` gave `std:v1:default`). A full key that is not the manifest's
  key now raises `ContextResolutionError`.
- **Hash values.** The content hash of a stream that has no producer (a bare
  `ArrowTableStream`) covers its `_context_key` column, so it changes with the key. No other
  hash value changes because of this entry.
- **Removed (introduced earlier in this unreleased cycle, never released):**
  `orcapod.hashing.stored_hash_labels` and `orcapod.errors.SideEffectLogHashLabelError`.
  Rows are told apart by `__data_context`, not by the label inside a hash.
- **Internal API.** `ResultCache.lookup`, `ResultCache.store` and
  `ResultCache.get_all_records` take the caller's `data_context_key`.

  **Migration:** none is needed to run the function, operator and source nodes of a v0.2
  store under v0.3, at the price of computing everything again; side-effect nodes fail
  on such a store, unless the store cannot list its tables (then the node warns and
  runs), and on a fresh store they perform every side effect again. A tool that converts old-layout tables is planned (ITL-913, ITL-914); until
  then they are kept, untouched, for it.

#### Hash label rule: labels renamed, out of hash inputs, bare digests inside names (ITL-945)

**Every structure hash, every table path and every key moves in v0.3.** One rule now holds
for every hash label (developer, November 11, 2026; `docs/reference/hashing.md`, "The hash
label rule"):

- **Labels are renamed to `<algorithm>_v1`.** `semantic_v0.1` becomes `semantic_v1`, the Arrow
  label is `arrow_v1` (it was `arrow_v0.1` in v0.2), `merkle_sha256` becomes `directory_v1`;
  `ndarray_v1` and `polars_expr_v1` keep their names; the file content hash keeps `sha256`.
  A label names the algorithm that produced a digest and changes only when that algorithm
  would give an existing input a different digest.
- **A label never enters a hash computation.** A nested hash now contributes its digest only:
  as `{"__type__": "hash", "digest": "<hex>"}` inside a structure hash, as
  `b"<extension name>;<hex metadata>::" + digest` for an extension-typed cell inside an Arrow
  hash (the name and metadata are the logical type identifier of ITL-935), and as the
  raw digest in the entry-id and operator-node record-hash preimages and in the run snapshot
  hash. Renaming a label later changes no digest. Until now `"<label>:<hex>"` was hashed, so
  this moves every `content_hash()` and `pipeline_hash()` whose structure holds a nested hash
  (types, schemas, pods, streams), every schema hash and so every system-tag column name,
  every Arrow hash over a logical-type cell, every pipeline base and entry id, and the run URI.
- **Hashes inside names are bare hex digests.** The function `uri` (its second component), the
  path components `schema:`, `instance:` and `node:`, and the slot name of a `SourceNode`
  built from a stream no longer hold a label. A source id is the exception: it is the
  source's name, so the default `source_id` of a `DerivedSource` (`derived:<label>:<hex>`)
  and of `node.as_source()` keeps the hash's label, now `semantic_v1`. A v0.3 function table is at
  `<job>/<function>/<64 hex>/v<major>/python.function.v0/schema:<64 hex>/pdb_v2`.
  `data_function.uri[1]` and `output_data_schema_hash` return the bare digest.
- **Hashes stored as values carry their label**, now including the record id (`_record_hash`)
  of operator-node and cached-source tables: `b"arrow_v1:" + digest` instead of hex text.
- **A producer-less `ArrowTableStream`** no longer hashes its derived `_content_hash` column
  into its content hash (the column's values carry the Arrow label).
- **`BasicDirectoryHasher(algorithm=...)`** raises `ValueError` for any algorithm but
  `"sha256"`: `directory_v1` names the SHA-256 procedure.
- **Each label is defined once, in code**: `SEMANTIC_HASH_LABEL`, `ARROW_HASH_LABEL`,
  `DIRECTORY_HASH_LABEL`, `NumpyArrayHandler.HASH_LABEL`, `POLARS_EXPR_HASH_LABEL`. The data
  context file no longer carries `hasher_id`; `hasher_id` remains a constructor argument of
  `SemanticAwarePythonHasher` and `StarfixArrowHasher` and defaults to the label.
- **Unchanged:** `ContentHash` and all its methods (`to_string`, `to_hex`,
  `to_prefixed_digest`, `from_string`, `from_prefixed_digest`); the digests of plain Arrow
  tables and schemas, of flat structures of primitives, and of ndarray, Polars expression,
  file and directory values (their label text changes, their digest does not);
  `InvocationContext.invocation_hash`, which keeps its `<label>:<hex>::<label>:<hex>` shape
  with the new label names; record ids.
- **Every v0.3 table path differs from v0.2's.** For the same node, v0.2 and v0.3 agree on
  the job name, the function or operator name, the major version and the wrapper kind, and
  differ in the table layout component (ITL-944) and in every hash inside the path: the
  output-schema hash of a
  function (`semantic_v0.1:<hex>` became another, bare, digest), `schema:<pipeline hash>`,
  `instance:<content hash>` and `node:<content hash>`. Nothing in v0.3 that looks for older
  data at its own table path can find a table written by v0.2. Tables written before v0.3 are
  therefore found by name, with a scan of the store under the job and function or pod name
  (ITL-954), and a node that delivers side effects raises `SideEffectLogDataContextError`
  when one is found (ITL-944).
- **Migration.** None is needed beyond the v0.3 step already described: a store written by
  v0.2 is an older generation whose rows v0.3 does not use. Function, operator and
  cached-source tables are at new paths, so their v0.2 tables are left untouched. Code that
  compared `uri[1]` or a path component with `content_hash().to_string()` must compare with
  `to_hex()`. A job file saved by v0.2 holds the old labels and paths; loaded by
  v0.3, its nodes point at no v0.3 table and show no rows.

#### Function hashes: structured signature, default values through the semantic hasher (ITL-953)

A function's signature is now hashed from one structured description: each parameter's name,
kind, annotation and default value, in declared order. Before, the parameters were one line of
text in which a default appeared as `repr(value)`.

| | Before | Now |
|---|---|---|
| Default values | `repr(value)`: a `frozenset` or `dict` default hashed differently per process (`PYTHONHASHSEED`) or per insertion order, an `object()` default embedded a memory address, and two values with one `repr()` shared a hash | the semantic hasher's hash of the value, like every other hashed value |
| Parameter kind | not hashed: `f(a, *, b)`, `f(a, /, b)` and `f(a, b)` shared a hash | hashed |
| Stored `function_signature_hash` | hash of a separate signature text | the hash of the function as a value: one description of a signature |

**What moves.** The hash of every function used as a value, and every newly written value of
the stored column `__pf_var_function_signature_hash`. Nothing reads that column, and stored
rows are not changed. A function pod's identity does not use this hash: no `uri`,
`pipeline_hash()`, `content_hash()` or table path moves.

**A default value the hasher cannot hash is now an error.** A function pod whose function has
a default of a type with no hash rule fails when it is created (`@function_pod`,
`FunctionPod.from_fn`, `PythonDataFunction`), with a `TypeError` naming the parameter and the
function. There is no fallback to `repr()` or to the type name, which would let two different
defaults share a hash. Hashable defaults: `None`, `bool`, `int`, `float`, `str`, `bytes`,
`uuid.UUID`, lists, tuples, sets and dicts of hashable values, functions, types,
`numpy.ndarray`, `pandas` objects, `File`, `Directory`. Not hashable today: a plain
`enum.Enum` member, a dataclass or pydantic instance, `pathlib.Path`, `datetime`, `date`,
`Decimal`, numpy scalars other than `numpy.float64`. Giving these a hash rule (a pod
parameter's default hashed as the column value it would be) is the follow-up ITL-971.

**Migration.** Use a default the hasher can hash (for example a `str` or `None`, building the
object inside the function), remove the default and supply the value as an input column, or
register a `PythonTypeHandlerProtocol` for the type. A custom `FunctionInfoExtractorProtocol`
that wants its defaults hashed returns `parameters` entries whose `default` is a
`ParameterDefault`.

- **What a v0.2 store looks like to v0.3: old tables are found by name, not by hash.** v0.3
  computes other hashes than v0.2, so a v0.2 table is at a path v0.3 does not compute. Each
  function and side-effect node lists the tables stored under its job and function or pod
  name (`<job>/<function>/`, `<job>/_result/<function>/`,
  `<job>/side_effect_function/<function>/`, `<job>/side_effects/<pod>/`) and treats
  every table that is not at a current layout as written by an earlier version. Function
  nodes report them in one warning per node (no number of rows: the tables are not
  read); side-effecting nodes raise `SideEffectLogDataContextError`. A job run adds one
  total of the old tables that none of its nodes reports: operator caches, cached sources
  stored under the job, and nodes that left the job. Because tables are found by name, an
  old table of another function or pod with the same name in the same job, or of another
  node of the same function, counts too; the warning and the error name the tables.
  (ITL-954)
- A store that cannot list its tables (a Delta store on `gs://` or `az://` without the
  matching `fsspec` package, a third-party database without `list_table_paths`) is not
  searched by name. A side-effecting node then logs one warning that it could not check
  for completions logged before v0.3, and runs. (ITL-954)

#### `GroupBy` keeps non-key tags as tags (ITL-955)

`GroupBy` used to turn every tag that was not a grouping key into a list-valued **data** column,
with null source info, where a function pod could read it. That broke the rule that separates an
operator from a function pod: no data element may come from a tag value. Non-key tags now stay
in the tag group as list-valued tags, in the input's tag order, the way `Batch` keeps its tags.

| `GroupBy(by=["subject"])` on tags `subject, session, probe`, data `path` | Before | Now |
|---|---|---|
| Tag columns | `subject` | `subject`, `session: list[int]`, `probe: list[str]` |
| Data columns | `session: list[int]`, `probe: list[str]`, `path: list[str]` | `path: list[str]` |

- **What to change.** A function that took a moved tag as a parameter
  (`def f(probe: list[int], path: list[str])`) now fails input-schema validation; drop the
  parameter. A function cannot read a tag: if it needs the value, make it a data column of the
  source. A `select_data_columns` that was only there to drop the moved tags can go. To remove
  the list-valued tags from the output, use `drop_tag_columns`.
- **Unchanged.** `by` must still name scalar tag columns; members are still ordered by their
  non-key tag values, then by record id; groups are emitted in key order. The Arrow types of the
  list columns, the folded system tags and the data columns' list-valued source info are the
  same as before. The two all-null `_source_<tag>` columns are gone.
- **New.** A grouped stream can be grouped again on a coarser key; its list-valued tags nest
  (`list[list[T]]`).
- **Hashes and stored results.** No pipeline hash and no table path moves: the `GroupBy`
  stream's `pipeline_hash` and `content_hash`, and the `pipeline_hash` of everything downstream,
  are the same. Each grouped row's data hash changes, because the row's data no longer includes
  the moved tags. A function whose input was already only the real data columns
  (`GroupBy` then `select_data_columns`) receives the same data as before and reuses its stored
  results without computing again; its output now also carries the list-valued tags. A function
  whose signature has to change is a different function and computes once.

#### Default source ids built from a hash say what kind of source they name (ITL-957)

A source id is the source's name, so it should make the source easy to identify (developer,
November 11, 2026). A default id built from a hash now has the form
`<source kind>:<algorithm label>:<hex>`. The one string is used unchanged as the `source_id`
system tag value and at the start of every `_source_<column>` value.

- **In-memory sources with no `source_id`.** `ArrowTableSource`, `DictSource`, `ListSource`
  and `DataFrameSource` were named by the bare hex of the table hash (`000002fe…`). They are
  now named `arrow_table:arrow_v1:<hex>`, `dict:arrow_v1:<hex>`, `list:arrow_v1:<hex>` and
  `data_frame:arrow_v1:<hex>`. The kind is the class's config type key and the label is the
  one the hash carries. `hashing.path_n_char`, when set, still shortens the hex, and only
  the hex.
- **`node.as_source()`.** `FunctionJobNode.as_source()` and `OperatorJobNode.as_source()` no
  longer build `<node path>:<label>:<hex>`. They take the `DerivedSource` default,
  `derived:semantic_v1:<hex>` of the node's content hash, the same id as
  `DerivedSource(node)`.
- **What moves** for an in-memory source with no `source_id`: the `source_id`, the values of
  its `_tag_source_id::…` column, its record ids, its `_source_<column>` values, the name of
  the source in a job, and the content hash of the source (not for `ListSource`, whose
  content hash does not include the source id) and of everything downstream of it. No table
  path holds a source id; paths that hold a content hash (a `table_scope="content_hash"`
  node, the default path of a cached source) move with that hash. Pipeline hashes, schema
  hashes, column names, data hashes and the paths of function tables do not move, and a
  function's stored result for the same input data is still found.
- **What moves for `as_source()`:** the `source_id` and the name of the source in a job, and
  nothing else: its rows are the node's stored records and its content hash is the node's.
- **Not changed:** a `source_id` you give is used exactly as given. Sources whose default is
  not a hash (CSV, Delta, database tables, SpiralDB, cached and polling sources) keep their
  defaults.
- **Migration:** to keep a name stable across releases, pass an explicit `source_id`. As
  rule 5a of the hash label rule states, a default id holds the hash's label, so a later
  rename of a label would move default ids and what derives from them.

#### Your own classes come in two kinds: exact and structural (ITL-972)

Orcapod 0.3 identifies a class you define by its kind, its name and its fields' names and
types, in declared order. The kind is chosen where the class is defined.

- **An ordinary `@dataclass` or a pydantic model is identified by its exact class.** Its
  name is the module path and class name (`pkg.models.Point`; `__main__.Point` for a class
  defined in a notebook or script). Two different classes are never the same type, even
  with the same name and fields: a pod built for a class refuses the class defined again
  (a re-run notebook cell), and the error names the column. A column of a subclass is
  accepted where its base class is expected, as before.
- **A class made with the new `@op.dataclass` is structural** (see "Added"): identified by
  its name and fields, whatever class object or module it is.
- **Hashes change for every pipeline that uses a dataclass or pydantic model.** The
  class's logical type name and Arrow extension name become
  `<name>@<SHA-256 of kind, name and fields>`, and the extension metadata holds that
  description; the stored struct lists fields in declared order, as before. Every schema
  hash, pipeline hash, table path, system-tag column name and data hash that involves such
  a value moves. The first run under 0.3 computes those pod and operator results again,
  into new tables. Tables written by earlier versions are left untouched and are not read
  by 0.3 pipelines; reading such a column directly raises `TypeIdentityError`. There is no
  migration tool. Results that involve no such value are unaffected.
- **Changing a class's fields makes a new type.** Adding, removing, renaming, retyping or
  reordering fields all count. Defaults, methods and pydantic computed fields are not part
  of the identity. The new type's pods get new tables; the results of the old definition
  stay where they are. Before, a changed class shared the old tables and a run returned 0
  rows with only log warnings.
- **A value is stored only as its own class.** Storing a value in a column of an ordinary
  dataclass or pydantic model requires an instance of exactly that class; a subclass
  instance or an unrelated object with the same attributes raises `TypeError`. Before,
  any object with the right attributes was stored, and a subclass instance lost its own
  fields. This holds for a pod's output read in memory as well: its columns are
  converted as the pod's declared output types, where the types were taken from the
  values before.
- **A stored shape that disagrees with the class raises.** Reading a stored column of an
  ordinary dataclass or pydantic model finds the class at its module path. If the class
  there has other fields, `DeltaTableDatabase` raises the new
  `orcapod.errors.TypeIdentityError` naming the column and both sets of fields. It is not
  a `StoreReadError`: the table is intact and the class differs. Like a store error, it
  fails the run under every `error_policy`, with both orchestrators: a stored row that
  cannot be decoded is never skipped as a failed row. If no class is found, reading raises
  `ImportError` naming the column, the class and its stored fields.
- **A subclass is its own type.** A dataclass or pydantic subclass was stored as its
  parent when the parent was registered first: its extra fields were silently dropped from
  stored data, and the pod's hash and table path depended on the registration order. It
  now has its own identity and keeps all its fields, in any registration order.
- **Recursive classes** raise `RecursiveTypeError` (a `TypeError`) naming the field path,
  for example `Node.next -> Node`.

### Added

- **`orcapod.errors.StoreWriteError`** — raised when a node's results or records cannot be
  written to the store. It names the node, the row's tag and the table, and chains the
  database's own exception as its cause. (ITL-878 #287)
- **`orcapod.errors.StoreReadError`** — raised when a stored table exists but cannot be read.
  It names the table, and the node and row being processed when they are known, and chains
  the backend's exception as its cause. (ITL-884 #290)
- **`orcapod.errors.StoreError`** — the base of `StoreReadError` and `StoreWriteError`;
  `except StoreError` handles both. (ITL-884 #290)
- `orcapod.errors.OutputTypeError` identifies function-output mismatches by function,
  output column, declared type and returned type, with an explicit-conversion hint.
- **`PipelineJob.run(commit_every_rows=100)`** — how many rows a function node processes
  between commits of its results and pipeline records (an `int >= 1`). Nodes also commit when
  they finish, before a failed run raises, and at the end of the job; `1` commits after every
  row. A hard crash on a durable store loses at most the rows the running node processed
  since its last commit, and the rerun computes only those. (ITL-919)
- **SpiralDB nullability record** (ITL-936). SpiralDB reports every top-level column (key
  columns included) and every struct field as nullable, whatever was written; only list items
  keep a non-null flag. `SpiralDBConnector` now keeps a *nullability record* of each table's
  non-nullable columns and nested fields: an entry in the table's key-value metadata, not a
  data row (key `__orcapod_nullability__`, format `orcapod.nullability` version 1, chunked
  under SpiralDB's 8 KiB entry limit). It writes the nullability record on a table's first
  write, and `get_column_info` and `iter_batches` restore it, so a `SpiralDBTableSource` over
  such a table gives `T`, not `T | None`, and a function pod can take `x: float`. Every batch
  of such a table is checked: a null in a listed column raises
  `orcapod.errors.NullInNonNullableColumnError` naming the table and column. A write holding a
  null in a non-nullable column is refused before anything is written (SpiralDB stores a null
  written to a non-nullable struct field as `0`), and a write whose non-nullable columns differ
  from the nullability record raises `NullabilityRecordMismatchError`. An unreadable
  nullability record raises `NullabilityRecordError`.
  `orcapod.databases.write_nullability_record` and `read_nullability_record` let other writers
  give a table its nullability record. Tables without one read exactly as before (same schema
  and hashes); a table that gains one has a different schema, `pipeline_hash` and
  `content_hash`, so write it before building pipelines on the table. A
  `ConnectorArrowDatabase` on SpiralDB gives new result tables a nullability record, so results
  read back with their written nullability; existing result tables are not changed.
- **SpiralDB null structs are refused** (ITL-936). SpiralDB cannot store a null struct column:
  pyspiral refuses one, and with `push_down_nulls=True` it reads back as a struct of null fields
  (live, pyspiral 0.16.33, top-level struct columns). `SpiralDBConnector.upsert_records` now refuses a null struct at any depth
  with `orcapod.errors.UnsupportedValueError` naming the table and column, before anything is
  written; it never passes `push_down_nulls`.
- **`DerivedSource(origin, tags_as_data={tag: data_name})`** — a source derived from
  another source or from a job node that keeps the origin's tags and also offers chosen tag
  values as data columns, so a key (for example a session start time) can be an input to a
  function pod. Also available as `node.as_source(tags_as_data=...)`. The copy has the tag's
  exact Arrow type and nullability. The result is a new root source with its own provenance;
  its default `source_id` is `tags_as_data:<label>:<hex>`, a hash of the origin's identity and the
  mapping. A saved job
  reloads it whenever its origin can be reloaded. `DerivedSource` without `tags_as_data` is
  unchanged, except that giving it a source (rather than a job node) now raises `TypeError`
  when it is built instead of `AttributeError` when it is first read. (ITL-939)
- **`DropNulls` operator and `stream.drop_nulls(columns)`** — keeps the rows where
  every named column is non-null and narrows each named column from `T | None` to `T`, in
  `output_schema()` and in the Arrow table, so a pod whose parameter is a plain `T` can
  consume a column that is nullable by specification. The names may be tag columns, data
  columns or both; a name that is not a column of the stream raises `InputValidationError`,
  and a column that is already non-nullable is left as it is. With no argument
  (`DropNulls()`, `stream.drop_nulls()`) the columns are every tag and data column of the
  input stream: a row with a null in any of them is dropped and all of them become
  non-nullable. System-tag, source-info, context and meta columns are never included. On a
  source whose columns are all nullable (SpiralDB stores every column nullable) this form can
  drop most rows; naming the columns is the precise form. An empty collection is not "no
  argument" and raises `ValueError`. Only the top-level value is
  tested (a null list or struct; not list items, struct fields or NaN). Other columns, system
  tags, source info and row order are unchanged. The operator's identity is the set of names;
  the no-argument form has one fixed identity, different from that of any list of names.
  A surviving row's data hash (or tag hash, for a tag column) changes when the named column
  was nullable, because nullability is part of the row's Arrow schema; it becomes the hash
  the row has in a stream that declared the column non-nullable. No existing hash changes.
  (ITL-943)
- **`orcapod.errors.SideEffectLogDataContextError`** — a `StoreError` raised by a
  side-effecting node (a side-effect pod, or a function pod that takes the invocation
  context) whose stored completions are at an older table layout or belong to another data
  context, before anything is delivered. It has `record_paths` (every table found),
  `record_path` (the first), `found_context`, `current_context` and `node_label`. (ITL-944)
- **`orcapod.contexts.stored_data_contexts(database, record_path)`** — counts the rows of a
  stored table by the data context that wrote them. (ITL-944)
- **`list_table_paths(prefix=())` on every database** — a new method of
  `ArrowDatabaseProtocol`: the sorted paths of the tables stored under a path prefix,
  nested tables included, without opening a table. `InMemoryArrowDatabase` and
  `DeltaTableDatabase` on a local directory list exactly; `DeltaTableDatabase` on `s3://`
  lists through `s3fs` (one list request per directory), on `gs://` and `az://` only when
  the matching `fsspec` package is installed; `ConnectorArrowDatabase` (SQLite,
  PostgreSQL, SpiralDB) lists by table name, so path components come back as the backend
  spells them (`:` and `.` as `_`); `NoOpArrowDatabase` lists nothing. A database of your
  own that implements `ArrowDatabaseProtocol` needs this method to keep satisfying the
  protocol; without it Orcapod still runs and treats the store as one that cannot list
  its tables. (ITL-954)
- **`orcapod.errors.TableListingNotSupportedError`** — a `NotImplementedError` raised by
  `list_table_paths` when a store cannot enumerate its tables. (ITL-954)

#### `op.dataclass` — a dataclass identified by its name and fields (ITL-972)

`@op.dataclass` is used like `dataclasses.dataclass` (bare, or with `name=` and the
dataclass options except `init=False`) and returns a real dataclass. Orcapod identifies it
by its name (default: the class name) and its fields, not by the class object or its
module:

- Two such classes with the same name and fields are one type. A class cell run again in
  a notebook, or a class moved from a notebook into a module, keeps its pods and its
  stored results.
- A pod annotated with one class accepts a column of another class of the same type and
  receives the value as it is. Orcapod never converts a value from one class to another:
  a structural class promises its fields, not its methods.
- Instances of two classes of the same type compare equal when their field values are
  equal. Equality and hashing always cover all stored fields: `field(compare=False)` and
  `field(hash=...)` are refused with `TypeError` where the class is defined.
- Stored values can be read in a process that does not define the class: Orcapod rebuilds
  a dataclass from the description stored with the column.
- `P.from_instance(other)` builds an instance from an instance of any other dataclass with
  exactly the same field names and types. It is the only conversion, and it is explicit.
- Every class inside an `op.dataclass` must be an `op.dataclass`.

See [Your Own Data Types](docs/concepts/your-own-data-types.md) for the two kinds and when
to use each.

Re-running a notebook cell that defines a dataclass or pydantic model, or reloading its
module, no longer raises `ValueError: … already bound`.

### Changed

- When persistent and ephemeral results exist for the same input, reads select the
  greatest recomputation index across both stores; persistent rows retain precedence
  for equal indices. Loaded REPLAY operators report an absent cache table explicitly,
  loaded functions reject `run()`, and loaded jobs report `is_runnable()` as false.
  Source schema restoration uses the saved data context's type converter. (ITL-904)
- Each in-memory source class and `DerivedSource` declares its kind once as `SOURCE_KIND`
  (`arrow_table`, `dict`, `list`, `data_frame`, `derived`), used for the default source id,
  for `to_config()["source_type"]` and for the source registry.
  `orcapod.core.sources.source_ids.hash_source_id` is the single definition of the form.

### Fixed

#### A Delta table that cannot be read is no longer reported as having no rows (ITL-884)

When a `DeltaTableDatabase` table existed but could not be read (a missing or damaged data
file, a log the backend cannot load, data files whose column type no longer matches the
table), `get_all_records` and `get_records_by_ids` returned `None`, the same answer as for a
table that does not exist. A `PipelineJob` then warned that the stored results "may have been
deleted externally", swallowed the per-row read errors under `error_policy="continue"`, finished
without raising, and its nodes showed no records. Every read now raises
`orcapod.errors.StoreReadError`, which names the table and chains the backend's error, and a
store read failure fails the run under both orchestrators whatever the `error_policy`, also
when it happens while a row's result is being stored. A table that does not exist still reads as
empty.

A flush that could not load an existing table no longer treats it as absent. Before this fix a
`DeltaProtocolError` while loading the table made the flush write the batch with
`mode="overwrite"`, replacing the stored rows. The flush now raises and writes nothing.

A write that fails because the stored table cannot be read raises `StoreReadError`, not
`StoreWriteError`. When read and write failures happen in the same run, both orchestrators
raise the error of the node that comes first in the pipeline (ITL-884 #290).

#### A failed store write fails the run (ITL-878)

A failed store write fails the run. `PipelineJob.run()` used to return normally when a node's
result or record could not be written (the row was skipped like a crashed function under the
default `error_policy="continue"`), so the node read back fewer rows than it computed. It now
raises `StoreWriteError` at the first failed write, whatever `error_policy` is, on both
orchestrators (the async one no longer raises an exception group). Rows stored before the
failure are committed and kept; re-running does not recompute stored function results. A failed
flush of pending writes raises `StoreWriteError` with `flush=True` and names the store, not a
row. A function output that cannot be turned into a record (a naive `datetime`, a value not
matching the declared type) is still a row failure governed by `error_policy`, like a function
that raised. See *When the store cannot be written* in the function-pods guide. (ITL-878 #287)

#### Newcomer papercuts (ITL-821)

- `orcapod.enable_file_hash_caching` is exported at the top level (ITL-821).
- A source built from data with no rows and no schema (e.g. `DictSource(data=[])`) raises
  `ValueError: Table is empty` instead of `tag_columns not found in table` (ITL-821).
- Applying a function pod to a stream whose data schema does not fit no longer prints a stray
  `Key ... not found in parameter types.` line, and the error names the offending column,
  e.g. `Incoming data schema ... is not compatible with expected input schema ...: column
  'age' is not a parameter of the function` (it used to say `Incoming data data type`)
  (ITL-821).
- `examples/save_and_load_pipelines.py` runs again, rewritten around `PipelineJob`; the test
  suite now runs every example (ITL-821).
- `polars_filter` with a Polars expression predicate (`stream.polars_filter(pl.col("x") > 0)`)
  can be hashed, so it works upstream of a join and inside a `PipelineJob` (ITL-828). An
  expression hashes under the new label `polars_expr_v1`, through Orcapod's own canonical
  encoding of the expression tree rather than Polars' serialization, which is not stable
  across Polars versions. Column references, literals, comparisons, arithmetic, boolean
  operators, `is_null`/`is_not_null`/`is_nan`/`is_not_nan`, `is_in` (a Python list),
  `is_between`, `all_horizontal`/`any_horizontal`, `str.contains`/`starts_with`/`ends_with`
  and `cast` are supported. Anything else, notably Python functions (`map_elements`,
  `map_batches`) and selectors over several columns, raises `ValueError` naming the
  expression when the filter's identity is needed. Keyword-constraint filters hash as before.
- Inside a `PipelineJob`, a `PolarsFilter` (keyword constraints too) upstream of a `Join` no
  longer raises `InputValidationError` (`str` vs `str | None`): the filter now keeps every
  column's input nullability, instead of returning every column nullable (ITL-828). A filtered
  row's data hash now equals its source row's, so the data hashes of filtered rows change from
  `0.2.0`, and a function pod downstream of a `polars_filter` recomputes its results once
  against a `0.2.0` store.
- `PolarsFilter(predicates=[...])` and `stream.polars_filter(...)` with the same predicates now
  have the same hashes (`predicates` is stored as a tuple).
- **`SemiJoin` accepts nested columns and `string` against `large_string` keys.** A `SemiJoin`
  whose input had a list or struct column, as a key or not, failed with `ArrowInvalid: Data type
  list<...> is not supported in join non-key field`. It now keeps every column of the left
  input with that input's Arrow type, nullability and logical type, and its keys may be
  structs, lists or `Path`s. A key that is `string` on one side and `large_string` on the other
  matches (a `CSVSource` against a `DictSource`); keys whose types still differ after large
  normalization, such as `int8` against `int16`, raise `InputValidationError` naming the column
  and both types, on the sync and async paths. A null key never matches on either path: the
  async path used to match `None` to `None`. Results for inputs that already worked are
  unchanged. A non-nullable key against a nullable one still fails in input validation
  (ITL-829, second case). The known limitation "`SemiJoin` fails when an input has a list or
  struct data column" in the 0.2.0 notes no longer applies. (ITL-847, ITL-829 #291)

#### Delta reads return every row the database's handles wrote (ITL-864)

Right after a first `PipelineJob` run on a `DeltaTableDatabase`, `node.get_all_records()` could
return only some of the rows (1 of 2 or 3) on both orchestrators. Handles made with `at()` share
their pending writes, and any handle's flush writes all of them, but each handle kept its own
loaded copy of the table, so a sibling kept reading the version it had loaded before the flush.
The loaded tables and the stored-id sets are now shared by every handle of one root. Every read
method (`get_all_records`, `get_record_by_id`, `get_records_by_ids`,
`get_records_with_column_value`) returns what a flush would leave: the stored rows plus the
pending rows whose record id is not stored, or `None` when there are none. Before,
`get_records_by_ids` ignored pending rows, `get_records_with_column_value` ignored stored rows
while a pending batch existed, and a pending row whose id was already stored could be returned
twice or instead of the stored row. Writes made by another process or another root on the same
path are still seen only after the table is reloaded. (ITL-864 #293)

#### A script that read a local Delta store no longer hangs at exit (ITL-892)

A process that had read a Delta table through `DeltaTableDatabase` or `DeltaTableSource` could
finish its work and then never exit: it hung in Arrow's thread-pool destructor, or, on Linux with
Python 3.11 or 3.12, aborted with `terminate called without an active exception`. It happened in
up to three runs in four on macOS and about one in ten on Linux, on every supported pyarrow and
deltalake version. The cause is upstream (apache/arrow#50482, apache/arrow#54536). deltalake's
default reader goes through a filesystem written in Python, and Arrow can release one of its
buffers on a worker thread while the interpreter is shutting down.

Orcapod now reads local Delta tables through a native pyarrow filesystem, which returns the same
schema and values. Tables on cloud storage (`s3://`, `gs://`, `az://`) still use deltalake's
reader and stay exposed until pyarrow ships the fix; see
[Troubleshooting](docs/troubleshooting.md).

- Function outputs are validated before iteration, hashing or storage on standalone,
  cached, sync and async paths (ITL-895, ITL-901). Float-for-int truncation and unordered
  set-for-list conversion are rejected. Nested values follow the same checks; exact and
  structural user classes reuse their logical type compatibility, and ndarray mismatches
  use the same error. Diagnostics do not expose returned mapping keys or converter values.
- Same-kind numeric values are accepted silently only when represented exactly, including
  NumPy scalars for Python numbers and in-range Python integers for NumPy integer types.
  Numeric narrowing, non-subclass kind changes and iterable-to-list require explicit
  conversion. Subclasses are accepted unchanged in memory; declared base storage omits
  subclass-only fields and their contribution to row hashes. This includes bool for int
  and datetime for date, whose stored form is the declared base type. Accepted numeric
  exceptions are normalized to the declared type immediately, so downstream pods see the
  value that is stored. A subclass value is the exception: downstream pods in memory
  receive the object as returned (a `datetime` declared as `date`, or a user-class
  subclass with its extra fields), while storage holds only the declared base form.
  Conforming-data hashes are unchanged.
- A declared `dict` accepts only a `dict` instance (other mappings such as
  `MappingProxyType` need `dict(value)`), and a declared `Literal` accepts only its
  members, matched by type and value with no numeric exception.

#### An operator node returns its rows after an async run (ITL-896)

After a job ran on `AsyncPipelineOrchestrator`, an operator node (`job.nodes[label]` for
`Join`, `MergeJoin`, `SemiJoin`, `Batch`, `GroupBy`, `MapTags`, `MapData`, `PolarsFilter` and
the column-selection operators) returned no rows from `as_table()` and `iter_data()`, without
an error. It now returns the same rows as after a sync run, whatever `materialize_results`
is, and a later run replaces those rows even when it produces none. With the default
`cache_mode=OFF` (the only mode a job sets) an operator node keeps its output in memory and
stores nothing, so `get_all_records()` returns `None` for it on both orchestrators.
(ITL-896)

#### A loaded job can read its results (ITL-904)

A job loaded with `PipelineJob.load` can read its results. A function node's `as_table()` and
`iter_data()` return the rows the saved run stored, with their stored types (they used to raise
"no function pod and no database attached" while `get_all_records()` worked). Without a store,
reading says to pass `PipelineJob.load(path, store=...)` or call `job.bind(store=...)`. `save()`
now records whether the store held records of each function node (`run.node_has_stored_records`,
an optional field; the format version is unchanged). A node saved with records raises, naming
the store, when the attached store holds none of them, instead of returning no rows. When an
entry has several records (it was recomputed), a function node now reads the one with the
greatest recomputation index, on a live node too; it used to keep whichever record the store
returned last. Loading a job in which an operator consumes a function node no longer crashes
with `InputValidationError`, and a loaded node's `output_schema()` reports Python types; a saved
type it cannot restore raises an error naming the column. Registered logical types such as
`Path` restore normally, so downstream operators remain available. Saved
`timestamp`, `duration`, `time` and `decimal` types are now read back. Reading a loaded operator
node without a readable cache raises instead of returning an empty table; operators can store
outputs when caching is enabled. In-memory sources (`DictSource`, `ListSource`,
`ArrowTableSource`, `DataFrameSource`) are
logged at INFO with the remedy instead of a "skipping" warning. A loaded job stays read-only and
cannot be run again. See *Saving and loading a job* in the function-pods guide. (ITL-904)

Stored-record expectations count only rows of the node's saved data context. Operator
descriptors without a reconstructed operator still lack the saved runtime identity needed
to find cached rows; the runtime-descriptor change is deferred to ITL-977. Direct execution
of loaded operator nodes is rejected. A newer unavailable ephemeral result yields an
`EmptyData` placeholder instead of exposing an older persistent result.

#### A `PipelineJob` on an in-memory store runs in time linear in its rows (ITL-919)

A `PipelineJob` on an `InMemoryArrowDatabase` runs in time linear in its rows. Every row a
function node processed used to filter, copy or rebuild the whole stored table (the
result lookup, the recomputation-index lookup, the duplicate check and the flush, each
over a table that gained one chunk per row), so 1,000 rows took about a minute. Lookups
now use hash indexes over the stored rows and a flush appends without copying them;
stored rows, ids and hashes are unchanged.

#### A `PipelineJob` on a Delta store runs in time linear in its rows (ITL-919)

A `PipelineJob` on a `DeltaTableDatabase` committed every row of every function node as
its own Delta version (a MERGE that reads the whole table) and re-read the stored table for
each per-row lookup, so 1,000 rows took about eight minutes. Function nodes inside
`job.run()` now commit every `commit_every_rows` rows (default 100, see *Added*); a batch
whose record ids are all new is appended instead of merged; and lookups are answered from
an indexed in-memory copy of the committed rows, read once and extended with each commit.
Stored rows, record ids, hashes, paths and schemas are unchanged. Behaviour changes: a store
failure that only a commit reveals (an unreadable table, a failed commit) surfaces at the
node's next commit point and names the node but no row, and the rows computed since the
last commit are computed again on the next run; `commit_every_rows=1` restores per-row
surfacing. A `DeltaTableDatabase` object keeps answering from the table version it loaded
until its own next commit (as before; it no longer re-reads the table per lookup).

#### An asynchronous run whose node fails raises instead of hanging (ITL-932)

On the asynchronous orchestrator, a node failure while more rows were in flight than the
channel buffers hold (about 16 to 64 rows) hung the run forever: the failure cancelled the
readers, and closing a full channel waited for room that never came. Closing a channel no
longer blocks; readers still receive every item sent before the close.

#### An asynchronous rerun of an ephemeral-result node recomputes its rows (ITL-923)

Rerunning a node with `NodeConfig(is_result_ephemeral=True)` on the asynchronous
orchestrator, after its ephemeral results were gone, neither recomputed nor recorded the
rows, and reading the node afterwards raised `EmptyDataAccessError`. It now recomputes them,
as the synchronous orchestrator does.

#### A failed side-effect completion lookup names the row

A `StoreReadError` from a side-effect pod's check of earlier deliveries now names the pod
and the row, as a failed write of the delivery record does.

#### SpiralDB table source (ITL-926)

- The `spiraldb` extra requires `pyspiral>=0.16.33` (was `>=0.14.0`). pyspiral 0.14.9 does not
  use the login saved by a current `spiral login`; 0.16.33 does, and keeps every call
  `SpiralDBConnector` makes.
- `SpiralDBConnector.get_column_info` and `iter_batches` normalize every column to its large
  Arrow variants at every nesting level (`list<string>` becomes `large_list<large_string>`),
  so both report the same types (ITL-40). Before, only top-level `string` and `binary`
  columns were converted. Widths, nullability and nested field names are unchanged, and so
  are all hashes.
- Building any source converts only its `record_id_column` to Python, not the whole table:
  about half the peak memory (200,000 rows: +210 MB down to +115 MB). Tokens, record IDs and
  hashes are byte-identical.
- SpiralDB keeps no non-null flag except on list items, so a Spiral column arrives as `T`
  only when the table carries Orcapod's nullability record (ITL-936, under Added); otherwise
  every column is `T | None`. The pyspiral fake now behaves like the server in this respect.
- A source whose table has a column the Arrow hasher refuses (for example `map`) raises
  `ValueError` naming every such column and its Arrow type, chained to the hasher's error.
  Before, it raised the hasher's bare `Unsupported leaf type: ...`, which named no column.
  `fixed_size_list` columns are hashed since starfix 0.5 (ITL-910).
- `SpiralDBTableSource` errors name the project and dataset searched. Before, a wrong
  project, dataset or table all gave `Table 'x' not found in database.`
- The docstrings give the real location of the saved `spiral login`
  (`~/Library/Application Support/pyspiral/` on macOS). `docs/concepts/sources.md` has a
  SpiralDB section: setup, discovery, nullable columns, and current limits.
- `get_function_signature(include_defaults=False)` no longer cuts a parameter at the first
  `=`, which truncated an annotation such as `Literal["a=b"]` (ITL-953).

#### A nested class stored as `None` reads back (ITL-881)

A dataclass or pydantic model with an optional nested class (`child: Inner | None`) that
stored `None` raised `TypeError: 'NoneType' object is not subscriptable` when the value
was read back. It now reads back as `None`.

#### Stored dataclass and pydantic values stay readable after Delta compaction (ITL-881)

Delta compaction (`optimize.compact()`) rewrites a `large_string` struct field as
`string`. Reading a stored dataclass or pydantic column with a `str` or `bytes` field then
failed, and a cached run returned 0 rows. Extension columns now accept large and non-large
layouts of the same type at any nesting depth, as they already did at the top level
(ITL-602). Known limitation: such a non-large column read over Arrow IPC or the C stream
interface (not Parquet or Delta) gives invalid arrays, at any depth; Orcapod's own writers
never produce one (ITL-894).

- `arrow_type_to_python_type` and `arrow_schema_to_python_schema` no longer name a logical
  type's Python class for a column (or a member of a struct column) whose extension metadata
  that type does not read: they raise `LogicalTypeIdentifierError`, as reading the values
  already did. The check, and the one made before values are decoded
  (`arrow_table_to_python_dicts`), also holds for a type nested in a struct, list, map or
  union when an equal-looking type with readable metadata was converted before (ITL-975).
- `CachedDataFunction.call` and `async_call` refuse an input of another data context before
  the function is called, also with `skip_cache_lookup=True` (ITL-975).
- A side-effecting function node checks an ephemeral result store attached with
  `set_ephemeral_store` for completions of another data context, also when the store is
  attached after the node first read its stores (ITL-975).

### Known limitations

- Previously stored rows remain unchanged and are not automatically recomputed or repaired.
  Python-value inputs (sources and datagrams) remain outside output validation (ITL-895).
- The declared type's logical type decides what is stored. A function that reads fields its
  declared parameter type does not have (the extra fields of a subclass returned for a base
  type, or the time of a `datetime` declared as `date`) gets results that can depend on the
  execution path and can be reused across values that differ only in those fields
  (ITL-978).

## [0.2.0] — 2026-11-10

**Distribution:** Orcapod is MIT licensed starting with `0.2.0` (it was proprietary on
`main` between #277 and #283; no release carried that license). `0.2.0` is a tag only;
`0.3.0` is the first release published to PyPI from this repository. Until then,
`pip install orcapod` and `uv add orcapod` install `0.1.4`, the latest release on PyPI;
`0.2.0` is the tag `v0.2.0` of the repository.

Everything merged to `main` since `v0.1.2`. `0.1.3` and `0.1.4` were patch releases cut
from a separate branch; their one fix is also in this release (see
*Fixed*), so upgrading from `0.1.4` loses nothing.

**Read *Breaking changes and migration* before upgrading an existing pipeline database.**
A pipeline or result database written by any `0.1.x` release cannot be read by `0.2.0`
until it is migrated or explicitly ignored.

### Breaking changes and migration

#### Upgrading a store written by `0.1.4`: what is still found

Checked on 2026-11-10 by writing a Delta Lake store with the released `orcapod==0.1.4` and
opening it with `0.2.0`. Sources whose columns are plain types (`int`, `str`, …) keep the
same schema, content and pipeline hashes and system-tag column names as on `0.1.4`.

| Change | What a `0.1.4` user sees | Are stored results still found? |
|---|---|---|
| Table paths gain `pdb_v1` / `rdb_v1` (ITL-535 #232) | `SchemaVersionError` the first time a node touches a `0.1.x` table | Result tables: yes, after `orcapod migrate result-db` or `migrate_result_v0_to_v1`. In the check, the migrated result tables served every row and no function ran again. |
| Pipeline-DB record IDs (ITL-533 #236) | Every `record_id` differs | Pipeline tables: no. Their migration fails on a `0.1.4` store (ITL-863), so set `ignore_schema=("v0",)` and let the node refill its pipeline table. With migrated result tables this re-runs no function, although the warning says all results will be recomputed. |
| Type-object hashes of `Path`, `UUID`, `DataFrame`, `list[Path]` and `set[UUID]` (ITL-638 #262) | A source with a column of such a type has a new schema hash, `pipeline_hash` and `content_hash`, and new system-tag column names (`_tag_record_id::<pipeline hash>`). Record-ID values and row data hashes are unchanged | Pipeline tables of function nodes downstream of such a source: no. The node's table path changes, so its `0.1.4` table is not seen at all: no `SchemaVersionError`, and `migrate_node` finds 0 rows. Result tables: yes, after migration, unless the function itself is annotated with one of these types (its signature hash changes; see below) |
| Stored Arrow type of `list[Path]` / `set[UUID]` (ITL-173 #251) | These columns are logical list types (`extension<list[orcapod.path]>`) | Nothing to find: `0.1.4` rejected such columns as not yet supported |
| `Batch` system tags folded to scalars (NPIPE-204 #250) | `Batch` output system-tag columns are scalars instead of lists, and their names gain a further `::<pipeline hash>` | Nothing to find for a function downstream of `Batch`: in `0.1.4` such a node failed inside a `PipelineJob` with `ArrowTypeError`. An operator-node table has no migration (see *Known limitations*) |

#### Stored hash columns are now `large_binary`; pipeline and result databases are versioned (ITL-535 #232, ITL-539 #241)

Every orcapod-produced hash column in the pipeline database (pdb) and result database
(rdb) is now stored as `large_binary` (the prefixed digest) instead of `large_string`:
`__node_content_hash`, `__input_data_hash`, `__output_data_hash`, the function variation
hashes `function_signature_hash` / `function_content_hash`, the operator node's
`__node_content_hash`, and the side-effect invocation log's `record_id_hash`.

Storage paths now carry a schema version: pipeline tables live under
`<node identity path>/pdb_v1`, result tables under `<pod uri>/rdb_v1`, and side-effect
tracking tables under a `tdb_v1` suffix. A table written by `0.1.x` (no suffix) is a
"v0" table.

**What you will see.** The first time a function node or result cache touches a v0 table
it raises `orcapod.errors.SchemaVersionError`. Nothing is read or overwritten.

**Migration — choose one per node:**

1. *Migrate in place* (keeps cached results). For Delta Lake databases, from the shell:

   ```bash
   orcapod migrate result-db   <result_db_path> <record_path> [<record_path> ...]
   orcapod migrate pipeline-db <pipeline_db_path> <result_db_path> <node_path> [<node_path> ...]
   ```

   Both accept `--dry-run` (count without writing) and `--json-summary`. For any other
   database backend, or from Python:

   ```python
   from orcapod.migrations import migrate_node, migrate_result_v0_to_v1

   migrate_result_v0_to_v1(result_db, result_path)   # the pod's result table
   migrate_node(function_job_node)                    # the node's pipeline table
   ```

   Migration copies rows to the versioned path and leaves the v0 table untouched; it is
   idempotent, so it can be re-run.

   **Known issue (ITL-863):** migrating the pipeline table of a function node written by a
   released `0.1.x` (reported on `0.1.4`) fails with `ArrowInvalid: Could not convert b'…'
   with type bytes: was not a utf8 string`, because the migration casts the binary
   system-tag record-ID column to a string. Migrating the result table succeeds. Until
   ITL-863 is fixed, use option 2 for such a node.

2. *Ignore and recompute.* Opt a node out of the check and let it recompute from scratch:

   ```python
   from orcapod.types import NodeConfig
   node.node_config = node.node_config.merge(NodeConfig(ignore_schema=("v0",)))
   ```

   `merge` keeps the node's other settings; assigning a bare `NodeConfig(...)` would reset
   `is_result_ephemeral` to its default.

**Not covered by the migration tool:** tables written by an *operator* node
(`OperatorJobNode`). Those have no version check and no migration; an existing table
fails with an Arrow schema mismatch on the next write and must be dropped and recreated.
See *Known limitations*.

`ArrowDatabaseProtocol` gains a required `table_exists(path) -> bool` method. Custom
database backends must implement it.

#### `record_id` no longer includes the node content hash (ITL-533 #236)

The preimage of a pipeline record's `record_id` is now the system tag columns plus
`__input_data_hash` only. `__node_content_hash` is no longer part of it (it is still
stored on each row, and still used to isolate nodes that share a table).

**Effect:** every `record_id` and `__pipeline_base_entry_id` value differs from the one
`0.1.x` computed for the same input. `orcapod migrate pipeline-db` /
`migrate_pipeline_v0_to_v1` recompute both, so a migrated database is consistent. Any
`record_id` you stored *outside* orcapod (logs, external tables, filenames) no longer
matches.

#### Function-pod signature hashes changed for orcapod logical types (ITL-638 #262)

A function pod's signature hash used to embed the full import path of an annotated
orcapod type (`orcapod.logical_types.file_type.File`). It now uses the type's stable
registered name (`orcapod.file`), so moving a module inside orcapod can no longer
invalidate caches.

The same change moved the semantic hash of the type objects `Path`, `UUID`, `DataFrame`,
`list[Path]` and `set[UUID]` (commit `7dbd0b66`). A `Schema` that contains one of them
hashes differently, so a source with such a column has a new `pipeline_hash`,
`content_hash` and system-tag column names; see the table above for what that does to
stored tables.

**Effect — one-time cache invalidation:** any function pod whose parameters or return
annotation mention `op.File`, `op.Directory`, `op.Path` or `op.UUID` (directly or inside
`list[...]`, `X | None`, etc.) gets a new signature hash. Its previously cached results
are not found and are recomputed on the next run. Migrating the database does not avoid
this. Functions annotated only with builtins and user-defined types are unaffected — their
hashes are byte-identical (enforced by golden-value tests).

#### `Schema` objects are hashed by `SchemaHandler` (ITL-639 #263)

`Schema` values now reach their dedicated handler instead of being hashed as a plain
mapping. **No hash value changes** — the eight golden schema hashes are byte-identical.
The one behavioural difference: hashing a `Schema` that contains a Python type with no
Arrow translation now raises a descriptive error at hash time instead of producing a
hash for a schema that could never be stored.

#### `op.Directory(ignore=...)` matches relative POSIX paths (ITL-603 #247)

Glob patterns are matched against the path relative to the directory root with
`PurePosixPath.match()`, not against the bare entry name. Bare patterns such as `"*.pyc"`
behave as before. Patterns containing `/` (for example `"build/*.o"`), which previously
never matched anything, now exclude files — which changes the directory's content hash.
A callable passed as `ignore` now receives a relative `PurePosixPath` rather than an
absolute `UPath`; callables that only read `.name` are unaffected.

#### `SideEffectFunctionPod` removed; context injection moved into `FunctionPod` (ITL-532 #231)

Only relevant if you tracked `main` between releases — none of this was in a published
version. The `orcapod.core.side_effect_function` module, `SideEffectFunctionNodeProtocol`,
`SideEffectFunctionInvocation` and `record_side_effect_function_pod_invocation` are gone.
Use `@function_pod(ctx_arg=...)` / `FunctionPod.from_fn(..., ctx_arg_name=...)`.
`@side_effect_function_pod` remains as a thin alias.

#### Operator outputs keep their inputs' Arrow types, normalized to large variants (ITL-813, ITL-832 #278)

`MergeJoin` no longer infers Arrow types from Python values, and it no longer fails on a
zero-row input: a result with a zero-row input has the same column types as a non-empty
one. Every output column of `MergeJoin` and `PolarsFilter` takes the large-normalized form
of its input Arrow type: `string` becomes `large_string`, `binary` becomes `large_binary`
and `list` becomes `large_list`, recursively. Width, nullability and logical types are
never changed. List columns built by `MergeJoin` (merged columns), `Batch` and `GroupBy`
are `large_list` of the normalized element, or, for a logical element such as `Path`, that
element's logical list type (`list[orcapod.path]`). Merged source-info columns are
`large_list<large_string>`.

| Column | 0.1.x | 0.2.0 |
|---|---|---|
| `MergeJoin` merged column, `int8` input | `list<int64>` | `large_list<int8>` |
| `MergeJoin` merged column, `float32` input | `list<double>` | `large_list<float>` |
| `MergeJoin` merged column, `timestamp[ms]` input | `list<timestamp[us]>` | `large_list<timestamp[ms]>` |
| `MergeJoin` merged column, `decimal128(5, 2)` input | precision taken from the data (e.g. `decimal128(3, 2)`) | `large_list<decimal128(5, 2)>` |
| `MergeJoin` merged column, `string` input | `list<string>` | `large_list<large_string>` |
| `MergeJoin` result with a zero-row input | raised `ValueError` | same types as a non-empty result |
| `Batch` / `GroupBy` list column, `int8` input | `list<int8>` | `large_list<int8>` |
| `MergeJoin` / `PolarsFilter` system-tag record ID | `large_binary` | `fixed_size_binary[16]`, as the input has |

**Inputs that disagree:** when the two inputs of `MergeJoin` give a colliding data column
or a shared tag column different Arrow types, both are normalized first. If they are then
the same (for example `string` and `large_string`), the join proceeds. Otherwise (for
example `int8` and `int64`, two timestamp units, or two decimal precisions) it raises
`InputValidationError` naming the column and both types; cast one input explicitly. This
used to produce a type inferred from the values, or a Polars `SchemaError`. The check runs
when the join executes: `output_schema()` reports Python types, so such a disagreement is
not visible when the graph is built.

**Hashes:** large and non-large variants hash identically, so normalization and `list` →
`large_list` change no hash. The change is confined to `MergeJoin` merged columns whose
inputs used to be re-inferred to a different type: integers and floats narrower than
`int64` / `double` (which used to widen), timestamps with a unit other than microseconds
(which became `timestamp[us]`), and decimals (whose precision came from the data). The data
content hash of each row containing such a merged column changes, and so does the stored
record of any operator node or function node that consumes it; a function pod downstream
misses its cache once and recomputes. Tag hashes, stream `content_hash` and `pipeline_hash`
are unchanged.

**Stored schemas:** an existing table written with `list<...>` aggregates (from `Batch`,
`GroupBy` or `MergeJoin`) will receive `large_list<...>` batches. Appending them is not yet
tested for every database backend; see *Known limitations* (ITL-851).

**Nested nullability:** `normalize_to_large_types` now keeps every nested field's
nullability and metadata. A column such as `list<item: int64 not null>` keeps its non-null
item flag through `Data`, operators and storage instead of losing it. `Data`, `Tag`, stream
and source hashes are unchanged; only the stored Arrow schema of such columns changes.

**Struct Python types:** `struct<b: string>` and `struct<b: large_string>` now map to one
synthesized Python class. Hashes are unchanged.

**Logical list elements:** an aggregating operator (`Batch`, `GroupBy`, `MergeJoin`) over a
column whose extension type has no logical list type in the type converter (for example
PyArrow's canonical `arrow.uuid`) raises a `ValueError` naming the column. It used to fail
with `ArrowNotImplementedError: extension`.

#### `Index` and `Pick` keep their input Arrow types (ITL-834 #279)

`Index` and `Pick` rebuilt every data column from Python values, so columns they only pass
through widened (`int8` → `int64`, `float32` → `double`, `list<int8>` →
`large_list<int64>`). They now keep the input Arrow types of every tag and data column, and
the extracted column takes the element's type (`Index` on `list<int8>` gives `int8`; `Pick`
gives the dict's value type). Outputs are large-normalized as above. An in-place `Index` /
`Pick` keeps the column in its position, and a result in which every row was skipped has
the input's Arrow schema. Extracting a `None` element where the predicted output type is
not optional raises `ValueError` naming the operator, the column and the row's tag.

**Hashes:** rows that pass through `Index` or `Pick` with columns narrower than `int64` /
`double` (or lists and structs of such types) get different data content hashes than on
`0.1.x`, and so does every function pod downstream of them: such pipelines miss their
cache once and recompute. Inputs that are already `int64`, `double` or strings give the
same types and hashes as before. Stream `content_hash` and `pipeline_hash` are unchanged.

The underlying datagram fixes:

- `Datagram.with_columns` and `update` keep the Arrow table whenever it is loaded: existing
  columns keep their exact Arrow fields, and `update` keeps each updated column's field.
  `with_columns` (and `Data.with_columns` / `DatagramProtocol.with_columns`) accepts a
  `pa.DataType` or a `pa.Field` in `column_types`; Python types behave as before.
- Meta (`__`-prefixed) columns keep their Arrow types through every mutator.
- `arrow_schema()` is no longer stale after a mutation of a datagram built from a dict.
  `FunctionPodStream.as_table()` could drop such a row's added tag column silently.
- `Index` and `Pick` use the input stream's data context to type the extracted column.

#### `Join` and `MergeJoin` column order follows argument symmetry (ITL-817 #272)

The order of tag and data columns in a `Join` or `MergeJoin` output now follows the
operator's declared argument symmetry. Both are commutative, so they take their inputs in
canonical `pipeline_hash` order and list columns by first appearance across them:
`Join()(a, b)` and `Join()(b, a)` give identical `keys()`, `output_schema()`, `as_table()`
columns and per-row `Tag` / `Data` keys, in every process. Ordered operators such as
`SemiJoin` keep operand order. Code that reads these columns by position may see a
different order than on `0.1.x`; address columns by name. No hash, system tag, record ID
or stored value changes, because hashing does not depend on column order.

#### `Join` gives the same rows on its async path as on its sync path (ITL-837 #280)

`Join`'s async path (`async_execute`, used by async pipelines) rebuilt every merged row from
Python values, so each column's Arrow type was inferred again: an `int16` tag or an `int8`
data column became `int64`, and a `dict` column could not be built at all. It now builds the
merged row from the input rows' Arrow tables, for two inputs and for three or more. Types,
nullability, column order, system tags and content hashes now equal those of the sync path.

**Hashes:** the `Tag` and `Data` content hashes of rows produced by an async `Join` over
columns narrower than `int64`/`double` change (they now equal the sync values), along with the
stored input hashes of any function node downstream of such a join. Sync hashes, stream content
hashes and pipeline hashes are unchanged.

**Behaviour change:** when a shared tag key has different Arrow types in two `Join` inputs
(for example `int16` and `int32`), `Join` now raises `InputValidationError` naming the column
and both types, on both paths. The sync path used to keep the right-hand input's type silently.
Types that differ only in layout (`string` and `large_string`) still join.

**Also fixed:** `Datagram.copy()` gave a copy of an Arrow-backed datagram an empty Python
`schema()` when the schema had not been computed yet; async `Index` and `Pick` returned such
rows (ITL-859).

#### `Join` no longer carries its inputs' meta columns (ITL-854, ITL-857 #280)

`Join` now drops every input's `Data` meta (`__`-prefixed) columns, on the sync and async
paths, for two inputs or more: for example a cached function pod's `__computed` flag, the
result cache's `__pf_*` and `__pod_ts` columns, and the `__` columns of a raw
`ArrowTableStream`, along with their `_source___*` source-info columns. Previously the sync
path kept them, renaming colliding names with an index suffix (`__computed_1`), while the
async path dropped them. That rename could also give two columns the same name and fail with
a Polars `DuplicateError` (ITL-857). Nothing downstream of a `Join` reads these columns, and
none of them is hashed: content hashes, pipeline hashes, record IDs and stored tables are
unchanged. `Join.output_schema()` no longer lists the `__` columns a raw `ArrowTableStream`
reports as data, matching the joined table. Two inputs that share only a meta column name now
join instead of failing validation.

#### Materialized rows keep their own source info (ITL-845 #282)

When rows were collected and turned back into a stream, every row got the source-info
(`_source_*`) tokens of the first row, so rows after the first were attributed to row 0. This
affected the sync orchestrator, which does this between every pair of nodes (so every
function-node tag table written by a sync `PipelineJob` recorded the wrong input provenance),
the async path of every operator that collects its input (`MergeJoin`, `Batch`, `GroupBy`,
`PolarsFilter` and the other barrier operators), sync `Index` and `Pick`, and operator nodes
(`OperatorJobNode.execute` and `async_execute`). After `Batch`, a batch could even list more
tokens than it had members. Each row now keeps its own tokens.

Only `_source_*` values change. Data values, Arrow types and stored schemas, content hashes,
pipeline hashes and function-node record IDs are unchanged. The one stored ID that covers
`_source_*` values is an operator node's per-row `_record_hash`: it changes for every row
whose token was wrong, and stays the same for rows whose token was right.

**Stored data is not repaired.** Rows stored before this fix (function-node tag tables, and
operator-node tables written with `cache_mode=LOG`) keep the wrong source-info token for every
row after the first; nothing else in them is wrong. Orcapod does not rewrite, repair or
recompute them: a re-run serves function-node rows from the cache unchanged, but stores
corrected operator-node rows next to the old ones, so that table then holds both versions of
those rows. To get correct stored values, drop the pipeline tables (the function-node tag
tables, whose paths end in `pdb_v1`, and any operator-node tables) and re-run the job. The
function result tables (under `_result/`, ending in `rdb_v1`) can be kept: the re-run then
serves every result from them and runs no function again. A table's path is the job name
followed by the node's `node_identity_path` (plus `pdb_v1` for a function node); in a
`DeltaTableDatabase` it is that directory under the root, and the SQL connectors name the
table by joining the path components with `__`, each with non-alphanumeric characters
replaced by `_`. Correcting stored values is tracked in ITL-853.

#### Shipped in 0.1.2 but never recorded here

These two breaking changes were released in `0.1.2` without a changelog entry. They are
listed for anyone upgrading from `0.1.1` or earlier.

**`NodeConfig` split into `PodConfig` and `NodeConfig` (ITL-512 #221).**

| Before | After |
|---|---|
| `NodeConfig(max_concurrency=N)` | `PodConfig(max_concurrency=N)` |
| `FunctionPod(pf, node_config=NodeConfig(...))` | `FunctionPod(pf, pod_config=PodConfig(...))` |
| `pod.node_config` | `pod.pod_config` |
| `resolve_concurrency(node_config, pipeline_config)` | `resolve_concurrency(pod_config, pipeline_config)` |
| `NodeConfig.is_result_ephemeral: bool` | `NodeConfig.is_result_ephemeral: bool \| None` |

`is_result_ephemeral` can no longer be set when constructing a `FunctionPod`. Set it on
the node (`node.node_config = node.node_config.merge(NodeConfig(is_result_ephemeral=True))`)
or for a whole job
(`job.apply_node_config(NodeConfig(is_result_ephemeral=True))`).

**SpikeInterface support moved to a separate package (ITL-473 #220).** Core `orcapod` no
longer contains or depends on any SpikeInterface code: `LogicalSIRecording`, the
`BaseSorting`, `Motion` and `SortingAnalyzer` logical types, and the `spikeinterface`
optional dependency are gone. They live in
[`orcapod-extension-spikeinterface`](https://github.com/nauticalab/orcapod-extension-spikeinterface),
which registers itself through the new `op.register_extension()` hook. That package is
not on PyPI at the time of writing; install it from its repository.

### Added

- **`GroupBy` operator** — the first many→one operator. `GroupBy(by=[...])` (also
  `stream.group_by(...)`) collapses rows sharing a tag tuple into one row; group keys stay
  scalar tags and every other column becomes list-valued. Output is deterministic across
  runs and input orderings. (NPIPE-204 #250)
- **`SideEffectPod`** with `track_completion` and `drop_on_failure`, plus the `@sink_pod`,
  `@tap_pod` and `@side_effect_pod` decorators. The user function receives an
  `InvocationContext` on every call. (ITL-525 #228)
- **Context injection for function pods** — `@function_pod(ctx_arg=...)` /
  `FunctionPod.from_fn(..., ctx_arg_name=...)` passes an `InvocationContext` to the
  function. (ITL-532 #231, #234)
- **Post-run hooks** — `FunctionPod.add_post_run_hook()` and
  `@function_pod(post_run_hooks=[...])` fire after every invocation (computed, cache hit,
  or error). New public types: `InvocationStatus`, `RunStats`, `PodContext`,
  `PostRunPayload`, `HookConfig`, `PostRunHook`. (ITL-523 #226)
- `PostRunPayload.invocation_context`; `InvocationContext` and `InvocationHashConfig` are
  exported from `orcapod`. (ITL-531 #237)
- **`EmptyData`** — a payload-free token emitted when an upstream ephemeral result is
  missing, so downstream nodes can still be served from their own cache. New errors
  `EmptyDataAccessError`, `EmptyDataHashMissingError`, `EphemeralResultMissingError`.
  Pipeline rows now persist `__output_data_hash`. (ITL-534 #229)
- **Schema versioning and migration** — `orcapod.migrations` package
  (`migrate_node`, `migrate_pipeline_v0_to_v1`, `migrate_result_v0_to_v1`,
  `MigrationResult`), the `orcapod migrate pipeline-db` / `result-db` CLI commands,
  `SchemaVersionError`, `NodeConfig.ignore_schema`, `ContentHash.from_prefixed_digest()`,
  and `ArrowDatabaseProtocol.table_exists()`. (ITL-535 #232)
- **`ListLogicalType`** — `list[T]` and `set[T]` columns whose element is a logical type
  (`list[uuid.UUID]`, `list[File]`, `list[MyDataclass]`, nested lists) now round-trip
  through Parquet and Delta Lake with the element type preserved. (ITL-173 #251)
- `match_mtime` flag on the file-hash cache backends and
  `enable_file_hash_caching(match_mtime=...)`; with `False`, lookups match on path and
  size only, for filesystems with unreliable mtimes. (ITL-522 #225)
- Hash computation site reference in the documentation (ITL-529 #238) and the
  side-effect pods design spec (ITL-524 #227).

### Changed

- All hash columns stored in pipeline, result and tracking tables are `large_binary`. See
  *Breaking changes and migration*. (ITL-535 #232, ITL-539 #241)
- `record_id` preimage no longer includes `__node_content_hash`. See *Breaking changes
  and migration*. (ITL-533 #236)
- `op.Directory(ignore=...)` matches relative POSIX paths. See *Breaking changes and
  migration*. (ITL-603 #247)
- Release workflow accepts an optional `branch` input (defaults to `main`). (#239)
- CI: the Python 3.11 release test job no longer destroys its own virtualenv; every
  GitHub Action is pinned to a commit SHA and upgraded to its Node 24 major.
  (ITL-618, ITL-619 #254; ITL-621 #256)
- `pygraphviz` removed from the runtime dependencies. It was declared but never
  imported, and it is built from source against the Graphviz C headers, so installing
  `orcapod` failed on systems without `libgraphviz-dev` (or with a Graphviz newer than
  `pygraphviz` supports). Installing `orcapod` no longer needs a C toolchain or Graphviz
  headers. Rendering pipeline graphs still uses the pure-Python `graphviz` package and
  therefore the optional Graphviz `dot` binary at render time. (ITL-134 #267)
- **Python 3.13 is supported.** Canonical type names, and the hashes derived from them,
  are identical on Python 3.11, 3.12 and 3.13, and unchanged on 3.11 and 3.12.
  (ITL-825 #275, merged through #276)
- The repository meets the organization's required CI: `ruff` lint and format,
  `basedpyright` with a baseline, and tests on Python 3.11–3.13. Six tests that had been
  hidden by duplicated test-class names now run. (ITL-826 #276)
- `MergeJoin`, `PolarsFilter`, `Batch` and `GroupBy` keep their inputs' Arrow types,
  normalized to large variants, and `MergeJoin` accepts zero-row inputs. See *Breaking
  changes and migration*. (ITL-813, ITL-832 #278)
- `Index` and `Pick` keep their input Arrow types, and `Datagram.with_columns` accepts Arrow
  types. See *Breaking changes and migration*. (ITL-834 #279)
- `Join` and `MergeJoin` column order follows argument symmetry. See *Breaking changes and
  migration*. (ITL-817 #272)
- `Join` raises `InputValidationError` when a shared tag key has different Arrow types in two
  inputs (layout-only differences such as `string` and `large_string` still join), its async
  path keeps the inputs' Arrow types, and it drops its inputs' meta (`__`) columns on both
  paths. See *Breaking changes and migration*. (ITL-837, ITL-854, ITL-857 #280)
- The license is MIT (`license = "MIT"`, the MIT text in `LICENSE`). (ITL-875 #283)
- **Hash values are pinned by a golden regression suite.** `tests/test_hashing/golden/`
  pins 608 hash values that Orcapod stores, uses as a cache or database key, or embeds in a
  column name or table path: semantic hashes of Python values and type objects, `Schema`
  hashes and stored Arrow types, Arrow table and schema hashes, datagram hashes, and the
  content hashes, pipeline hashes, system-tag column names, record IDs, row data hashes and
  database table paths of pipelines through every operator and a `PipelineJob`. Each value
  has its own test, so a failure names the value that moved, and the values are identical
  on Python 3.11, 3.12 and 3.13. A failing golden entry means a hash changed: it is treated
  as a bug in the change, and the files are regenerated only to add entries or for a hash
  change the developer has approved with a migration assessment, never to make a test pass.
  The values that differ from `0.1.4` are listed in the table at the top of *Breaking changes
  and migration*. (ITL-626 #284)
- Packaging: the `LICENSE` file ships in the wheel and the sdist; `pymongo` and `pyyaml` are removed from
  the runtime dependencies; `basedpyright` and `typing_extensions` move to the `dev` group;
  the sdist contains only the package source, `README.md`, `LICENSE` and the package
  metadata; `orcapod.__version__` is added. (ITL-827 #277)
- Documentation: every Python sample in `README.md`, `docs/index.md`,
  `docs/getting-started.md`, `docs/concepts/` and `docs/reference/` is executed by the test
  suite, and `mkdocs build --strict` runs in the tests. (PLT-4816 #271) A runbook for
  coordinating issue-specific agents is added for contributors (`AGENT_COORDINATION.md`).
  (ITL-818 #266)

### Fixed

- `RayExecutor` no longer fails when it has no remote options: `ray.remote` is applied
  directly instead of being called with an empty option set. This is the fix released as
  `0.1.3` / `0.1.4`. (#233, ITL-544 #235)
- Function-pod signature hashes no longer depend on orcapod's internal module layout.
  (ITL-638 #262)
- `Schema` hashing is routed through `SchemaHandler`; the handler was unreachable and was
  missing from the `v0.1` context spec. (ITL-639 #263)
- Empty Arrow tables keep the declared field nullability. Previously a function that
  failed under `error_policy="continue"` produced an all-nullable empty table, and a
  downstream `Join` aborted the whole pipeline. (ITL-563 #242)
- Extension-typed columns can be read back after Delta Lake compaction, which rewrites
  `large_string` storage as `string`. (ITL-602 #246)
- List-backed logical types (`list[File]`, `set[File]`, …): the Polars round-trip in
  `Join` / `MergeJoin` no longer raises, content hashing hashes file contents rather than
  path strings, and `MergeJoin` keeps the extension type on merged columns. (ITL-627 #257)
- `Data` can represent list-valued source-info provenance; `MergeJoin` inside a pipeline
  job no longer fails with `ArrowTypeError`. `Batch` and `GroupBy` can aggregate
  logical-typed columns such as `Path`. (NPIPE-204 #250)
- `PollingSource`:
  - runs its full asynchronous polling loop inside a `PipelineJob` instead of a single
    synchronous snapshot (ITL-615 #255);
  - no longer leaks a synthetic `_content_hash` column into its data schema, which raised
    `SchemaInconsistencyError` on the third fetch (ITL-616 #259);
  - no longer loses rows when `iter_data()` / `as_table()` are called while the async loop
    is running (ITL-617 #260);
  - no longer crashes on a zero-row batch; the Arrow schema is established once, from the
    declared schema or the first non-empty batch (ENG-952 #261).
- `ArrowTableStream.iter_data()` no longer drops rows or pairs a tag with another row's
  data when the tag and data columns have different chunk layouts (which result-cache
  hits could produce), and rows after the first record batch get their own source info.
  Public API, schemas, hashes and record ids are unchanged. (ITL-816 #270)
- `MergeJoin` no longer raises on a zero-row input. (ITL-813 #278)
- A function node whose output mixed cached and freshly computed rows could fail in the
  orchestrator with `ArrowInvalid: Schema at index N was different`, because cached rows
  came back with a different column order. Buffer materialization now aligns rows by
  column name. (ITL-817 #272)
- Rows collected and turned back into a stream (the sync orchestrator between nodes, the
  async path of barrier operators, sync `Index` / `Pick`, operator nodes) keep their own
  source-info tokens instead of all taking row 0's. Stored rows are not repaired; see
  *Breaking changes and migration*. (ITL-845 #282)
- `GroupBy` or `Batch` after `MergeJoin`, `Batch` or `GroupBy` no longer raises
  `ArrowTypeError: Expected bytes, got a 'list' object` when its rows are materialized. A
  list column becomes a list of its incoming list type (`large_list<large_list<int8>>`,
  element types unchanged, `Path` as `list[list[Path]]`), and its source info is a list of
  the members' source info. Hashes of pipelines that already worked are unchanged.
  (ITL-849 #281)
- `Join`'s async path no longer infers Arrow types from Python values (`int16` / `int8`
  became `int64`, and `dict` columns failed); a `Join` of three or more inputs no longer
  fails with a Polars `DuplicateError` when two inputs share a meta column name. See
  *Breaking changes and migration*. (ITL-837, ITL-857 #280)
- `Datagram.copy()` of an Arrow-backed datagram whose schema had not been computed yet no
  longer reports an empty Python `schema()`; async `Index` and `Pick` returned such rows.
  (ITL-859 #280)

### Known limitations

- **Migrating a function node's pipeline table written by a released `0.1.x` fails.**
  `orcapod migrate pipeline-db` and `migrate_pipeline_v0_to_v1` raise `ArrowInvalid` on a
  tag table written by `0.1.4`, because the binary system-tag record-ID column is cast to a
  string. Workaround: set `ignore_schema=("v0",)` on the node (the `merge` form under
  *Breaking changes and migration*) and let it recompute. (ITL-863)
- **Operator-node tables have no migration path.** `OperatorJobNode` stores
  `__node_content_hash` as `large_binary` but has neither a schema-version check nor a
  migration utility. An operator-node table written by `0.1.x` fails with an Arrow schema
  mismatch on the next write; drop and recreate it. (ITL-539; `DESIGN_ISSUES.md` ON1)
- **The migration CLI only opens Delta Lake databases.** Other backends must use the
  Python API in `orcapod.migrations`.
- **Migration does not rescue caches invalidated by ITL-638.** Pods annotated with
  orcapod logical types recompute once regardless.
- **Some hash values are not pinned by the golden suite.** Values that are new on every run
  by design (function result record IDs and the per-row token in a function output's source
  info, both UUIDv7, and `__pod_ts`) are left out, and so are the types the Arrow hasher
  cannot hash yet (ITL-848). (ITL-626 #284)
- **`dict[str, list[T]]` with a logical-type `T`** (a list nested inside a dict value)
  does not round-trip; two tests are marked expected-failure. (PLT-1732)
- **`set[T]` round-trips only for logical-type elements**, not primitives such as
  `set[int]`. (ITL-611, open)
- **Rendering a pipeline graph needs the Graphviz `dot` executable** on `PATH`. Everything
  else works without Graphviz.
- **Delta Lake on S3 is not covered by automated tests.** The S3 integration tests do not
  run in CI; they skip in every environment because the MinIO image they start no longer
  exists. (ITL-819)
- **Arrow types are not part of `Schema`, and the rule for inputs whose Arrow types disagree
  is not settled for every operator.** `output_schema()` reports Python types, so a
  disagreement such as `int8` vs `int64` or `string` vs `large_string` is not visible when
  the graph is built. `MergeJoin` (colliding data columns and shared tags) and `Join`
  (shared tag keys, sync and async) normalize both inputs and raise `InputValidationError`
  if they still differ (see *Breaking changes and migration*). `SemiJoin` does not apply that
  rule yet: disagreeing key types fail with Arrow's own `ArrowInvalid`. (ITL-835)
- **Large-variant normalization is not applied everywhere.** `MergeJoin`, `PolarsFilter`,
  `Index`, `Pick`, `Batch` and `GroupBy` emit large variants; `Join`, `SemiJoin`, `MapTags`,
  `MapData`, the column-selection operators and sources keep whatever layout their input
  has (`string` stays `string`). This is layout only and changes no hash. (ITL-841)
- **`MergeJoin` cannot merge non-empty struct columns**: a colliding data column holding
  struct values raises `TypeError`. Zero-row inputs work. (ITL-839)
- **`SemiJoin` fails when an input has a list or struct data column**, even one it only
  passes through: the join raises `ArrowInvalid: Data type list<...> is not supported in
  join non-key field`. (ITL-847)
- **Appending v0.2-layout rows to a table written before v0.2 is untested.** `list<...>`
  aggregates that are now `large_list<...>`, nested non-null flags that are now kept, and
  narrow columns that no longer widen have not been tested against each database backend
  when appended to an existing table. (ITL-851)
- **Stored source-info tokens written before the ITL-845 fix stay wrong.** Function-node tag
  tables and operator-node `LOG` tables keep row 0's `_source_*` token on every later row
  until those pipeline tables are dropped and refilled; result tables can be kept. Orcapod
  has no repair, and no policy yet for stored data that a fix leaves wrong. (ITL-853)
- **Meta (`__`) columns have no written rule.** Which meta columns each element writes and
  exposes, and what operators do with them, is not specified. `Join` now drops them on both
  paths, but other operators do not yet give the same meta columns on their sync and async
  paths: the async path of `MergeJoin`, `Batch`, `GroupBy` and `PolarsFilter` drops them,
  and the sync path of `SelectDataColumns`, `Index` and `Pick` drops some the async path
  keeps (expected-failure tests, ITL-846). A raw `ArrowTableStream` over a table with `__`
  columns treats them as data. (ITL-867)
- **The async path does not handle zero-row inputs like the sync path.** An async barrier
  operator (for example `PolarsFilter`) whose input yields no rows raises `ValueError:
  Cannot materialize an empty list of rows into a stream`, where the sync path returns an
  empty stream
  (ITL-865). An async `Join` whose input yields no rows cannot check its shared-key types,
  so disagreeing types give an empty result instead of the sync path's
  `InputValidationError` (ITL-856).
- **The Arrow hasher cannot hash some types**: `null`-typed columns, canonical PyArrow
  extension types such as `arrow.uuid`, and map and fixed-size-list columns raise
  `NotImplementedError`. (ITL-848)
- **The `SchemaVersionError` hint suggests `node.node_config = NodeConfig(ignore_schema=("v0",))`**,
  which resets the node's other settings. Use the `merge` form shown under *Breaking
  changes and migration*. (ITL-852)
- **`Index` and `Pick` validation carries masked type errors**, and `Pick`'s branch for
  logical-type columns cannot run, because no logical type implements `pick_field` yet.
  (ITL-850)
- **`Data.select` and `Data.drop` leave the removed column's source-info column**
  (`_source_<column>`) behind, visible with `all_info=True`. (ITL-838)
- **Arrow schema and data preservation is not yet audited across every operation**, sync and
  async. Each operator fixed in this release was checked by hand; a test harness covering
  every operator, pod, source, node and execution path is deferred (ITL-846), including its
  nullability part (ITL-842).
- **There is no written rule for which data context an element uses.** Operators use their
  input stream's context for data, but an operator's own context still decides its
  `content_hash` and `pipeline_hash`, and `MergeJoin` uses its left input's context without
  checking the right one. (ITL-843, ITL-844)
- **Column order cannot be chosen.** `Join` and `MergeJoin` list columns in their canonical
  order, which may differ from the order of the call; address columns by name. (ITL-840)
- **A source's default record ID is positional.** Without `record_id_column`, a row's
  record ID comes from its `source_id` and its position. A row that moves to another
  position under the same `source_id` is treated as a new entry: a function node records it
  again (same tag values, a different record ID) and a `sink_pod` delivers it again. A
  function's stored result for that data is reused, not recomputed, and no result is
  paired with the wrong row. When row order can vary between runs, pass a stable
  `source_id` and set `record_id_column` to a column unique per row. (ITL-817,
  `DESIGN_ISSUES.md` RS2)
- **CI tests only the locked dependency versions.** Users get the newest versions the
  `pyproject.toml` ranges allow, which CI does not exercise (ITL-823), and the policy that
  every install uses `uv.lock` is not yet enforced everywhere (ITL-822).
- **The SpiralDB backend is not verified by this release.** Its integration tests have
  been failing on `main` since at least 2026-08-26 with `403 Permission table:view
  denied` from the SpiralDB test project — a credentials problem on the test project, not
  a code change — so the `spiraldb` extra ships untested against a live service.
- **Tested on Python 3.11, 3.12 and 3.13 only.** `requires-python` is `>=3.11` with no
  upper bound; newer interpreters are not exercised in CI.
- **`ray` is pinned to `2.48.0`** in the `ray` extra.
- The SpikeInterface extension is not published on PyPI.

## [0.1.4] — 2026-07-17

Patch release cut from a maintenance branch, not from `main`.

### Fixed

- `RayExecutor`: build the Ray remote function inside the cache-miss branch so a new
  `ray.remote` decorator is not created on every call.

## [0.1.3] — 2026-07-17

Patch release cut from a maintenance branch, not from `main`.

### Fixed

- `RayExecutor`: `ray.remote` cannot be pre-configured with an empty option set; apply it
  directly when there are no remote options.

## [0.1.2] — 2026-07-10

Not itemised at the time. Its two breaking changes — the `NodeConfig` → `PodConfig` /
`NodeConfig` split (ITL-512) and the move of SpikeInterface support to a separate package
(ITL-473) — are described under *Breaking changes and migration* in `0.2.0`. For the full
list of changes see the
[v0.1.1...v0.1.2 comparison](https://github.com/metamorphic-labs/orcapod-python/compare/v0.1.1...v0.1.2).

## [0.1.1] — 2026-06-29

### New

#### `orcapod.File` — content-identified file type (ITL-450)

`orcapod.File` is a new first-class type for materialized files. It subclasses
`upath.extensions.ProxyUPath`, giving it full UPath-like behaviour across all
backends (local, S3, GCS, etc.) without polluting the global protocol registry.

**Construction validates eagerly:** `File(path)` raises `FileNotFoundError` if the path
does not exist, `IsADirectoryError` if it is a directory, and `ValueError` if it is not
a regular file. This means a `File` instance always refers to a file that existed at
construction time.

**Symlink handling:** By default (`follow_symlinks=True`) symlinks are followed. Pass
`follow_symlinks=False` to raise `ValueError` instead of following a symlink.

**Content hashing:** `orcapod.File` is the only type whose hash is derived from the
file's *content*. `pathlib.Path` and `upath.UPath` now hash from the path string only
(see Breaking Changes below).

**Arrow extension type:** `LogicalFile` stores `File` values as `large_string` (the path
string) with extension name `"orcapod.file"`. Re-reading a stored `File` column
re-validates existence, so missing files surface at read time.

```python
from orcapod import File

f = File("/data/results.csv")        # validates existence
f2 = File("s3://my-bucket/out.parquet")  # works with any UPath backend
```

**Exports:**
- `orcapod.File` — the `File` class (top-level package)
- `orcapod.extension_types.LogicalFile` — the Arrow extension type
- `orcapod.hashing.semantic_hashing.FileHandler` — the semantic hasher

### Changed

#### starfix v0.3.0 adoption + schema-metadata cleaner (PLT-1737)

Bumped `starfix` dependency to `~=0.3.0` and introduced a schema-metadata
cleaning step before all Arrow schema and table hashes.

**What changed:** `StarfixArrowHasher.hash_schema` and `hash_table` now strip
every metadata key that does not start with `ARROW:extension:` before passing
the schema to starfix. Only identity-bearing extension metadata (e.g.
`ARROW:extension:name`) is included in the hash. Unrelated keys (comments,
vendor annotations, source-file provenance, etc.) are ignored.

**Stability invariant:** Schemas with no `ARROW:extension:*` metadata anywhere
continue to produce byte-for-byte identical hashes to pre-v0.3.0 Orcapod.

**One-time hash invalidation:** Pipelines whose schemas contained
`ARROW:extension:*` keys *alongside* unrelated field metadata (e.g. a
`comment` key on the same field as `ARROW:extension:name`) will see a changed
hash. On-disk caches for those pipelines should be treated as stale and
recomputed.

**New utility:** `orcapod.hashing.schema_cleaner.clean_schema_for_hashing` and
`has_extension_metadata` are available as semi-public utilities for other
Orcapod code that needs to inspect or clean Arrow schema metadata.

### Breaking Changes

#### `pathlib.Path` and `upath.UPath` no longer hash file content (ITL-450)

`pathlib.Path` and `upath.UPath` values now hash from the **path string** only —
no file is read. This matches the semantics of all other string-like types in orcapod.

**Migration:** Replace any `Path`-typed pipeline column that requires content-based
identity with `orcapod.File`. `File` validates existence at construction and hashes
from file content, exactly as `Path` did before this change.

```python
# Before (Path hashed file content — now hashes path string instead)
{"input": Path("/data/results.csv")}

# After (File hashes file content, as Path previously did)
from orcapod import File
{"input": File("/data/results.csv")}
```

**Hash invalidation:** Any on-disk cache entry produced with a `Path`-typed data column
will no longer match. Treat those entries as stale and allow recomputation.

**Removed:** `PathHandler` and `UPathHandler` have been removed from
`orcapod.hashing.semantic_hashing`. Use `FileHandler` for content-based hashing.

#### `packets` → `data` rename (hard break)

All identifiers containing `packet`/`packets`/`Packet` have been renamed to
`data`/`Data`. No deprecation aliases. Pre-v0.1 artifacts will not load.

| Old name | New name |
|---|---|
| `Packet` | `Data` |
| `PacketProtocol` | `DataProtocol` |
| `PacketFunction` | `DataFunction` |
| `PacketFunctionBase` | `DataFunctionBase` |
| `PacketFunctionProtocol` | `DataFunctionProtocol` |
| `PacketFunctionProxy` | `DataFunctionProxy` |
| `PythonPacketFunction` | `PythonDataFunction` |
| `CachedPacketFunction` | `CachedDataFunction` |
| `PacketFunctionWrapper` | `DataFunctionWrapper` |
| `PacketFunctionExecutorProtocol` | `DataFunctionExecutorProtocol` |
| `PacketExecutionLoggerProtocol` | `DataExecutionLoggerProtocol` |
| `PacketLogger` | `DataLogger` |
| `SelectPacketColumns` | `SelectDataColumns` |
| `DropPacketColumns` | `DropDataColumns` |
| `MapPackets` | `MapData` |
| `iter_packets()` | `iter_data()` |
| `process_packet()` | `process_data()` |
| `async_process_packet()` | `async_process_data()` |
| `execute_packet()` | `execute_data()` |
| `map_packets()` | `map_data()` |
| `select_packet_columns()` | `select_data_columns()` |
| `drop_packet_columns()` | `drop_data_columns()` |
| `on_packet_start()` | `on_data_start()` |
| `on_packet_end()` | `on_data_end()` |
| `on_packet_crash()` | `on_data_crash()` |
| `INPUT_PACKET_HASH_COL` | `INPUT_DATA_HASH_COL` |
| `PACKET_RECORD_ID` | `DATA_RECORD_ID` |
