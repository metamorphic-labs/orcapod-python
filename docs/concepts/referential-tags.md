# Referential Tags

!!! warning "Integration branch (ITL-1023)"
    Referential tags are the first slice of Orcapod v0.4, on an integration branch. The
    identities (content and pipeline hashes) of streams with referential tags are **not
    frozen yet** and may change before the release. Streams whose tags are all plain
    (`tag_only_columns`) keep their v0.3 identities exactly.

A tag is either **plain** (it carries its own value, as every tag did in v0.3) or
**referential**: a handle to the data column of the **same name**. A referential tag is
one column seen through both roles, so a key column can be joined and grouped on as a tag
and read by a function as data, without a copy.

## Sources: declared tags are referential by default

Every tag column a source declares is referential unless you keep it plain with
`tag_only_columns`:

```python
import orcapod as op

rows = [{"unit": 1, "time": 10, "amp": 0.5}, {"unit": 2, "time": 20, "amp": 0.25}]

spikes = op.sources.DictSource(rows, tag_columns=["unit", "time"], source_id="spikes")
print(spikes.keys())
# (('unit', 'time'), ('unit', 'time', 'amp'))

plain = op.sources.DictSource(
    rows, tag_columns=["unit", "time"], tag_only_columns=True, source_id="spikes"
)
print(plain.keys())
# (('unit', 'time'), ('amp',))
```

`tag_only_columns` takes the names to keep plain, or `True` for all of them. A name that is
not a declared tag is an error. A source built with all its tags plain is exactly the
source v0.3 built: same columns, same hashes.

The column is stored once: `as_table()` shows it once, and every row's tag and data hold
the same value.

```python
print(spikes.as_table().column_names)
# ['unit', 'time', 'amp']
```

## Functions read a referenced column by name

```python
@op.function_pod(output_keys=["scaled"])
def scale(time: int, amp: float) -> float:
    return time * amp


scaled = scale.pod(spikes)
print(scaled.keys())
# (('unit', 'time'), ('unit', 'time', 'scaled'))
```

Calling the pod, `pod(stream)`, lets the input's referential tags **fall through**: each
output row has the function's outputs and the row's own referenced columns, so the tags
stay referential. The function, its result identity and the database of its results are
those of the plain pod; only the referenced columns fall through, other input data
columns do not. `pod.process(stream)` is the exact form: there the output data is the
function's outputs only, and every tag of the output is plain, with the value it had.

```python
plain = scale.pod.process(spikes.select_data_columns(["time", "amp"]))
print(plain.keys())
# (('unit', 'time'), ('scaled',))
```

A function output named like a tag is an error when the pipeline is built. A function
that does not read a referenced column keeps its result identity: retagging a source
does not make it compute again.

## Making a data column a tag: `tag_by`

A source declares its tags, but a value a later step should join, group or sort by is
often computed on the way: a session label, a trial index, a phase. `tag_by` gives
existing data columns the tag role: each becomes a referential tag, one column that is
a tag and the data column it was, with its values and source info unchanged.

```python
@op.function_pod(output_keys=["phase"])
def phase_of(time: int) -> str:
    return "early" if time < 15 else "late"


phased = phase_of.pod(spikes).tag_by("phase")
print(phased.keys())
# (('unit', 'time', 'phase'), ('unit', 'time', 'phase'))

by_phase = phased.group_by(["phase"])
print(by_phase.keys())
# (('phase',), ('unit', 'time', 'phase'))
```

- `s.tag_by("x", "y")` equals `s.tag_by(["x", "y"])`. The new tags follow the stream's
  tags in the order given, and that order is part of the operator (`TagBy`): the names
  are a list, never a set.
- Nothing else changes: no row is added, removed or reordered, the data columns keep
  their names, types and values, every data column keeps its own source-info token,
  and the system tags keep their names and values.
- No data element comes from a tag value: the operator adds a tag role to a column
  that is already data, and creates or moves no value. Offering a tag's value as data
  stays with `DerivedSource(tags_as_data=...)`.
- A computed column may become a tag, and then key a join or a group, as `phase` does
  above.

`tag_by` refuses, naming the column: a name that is not a data column of the stream; a
name that is already a tag (plain or referential); a reserved name (the `__` prefix, a
system column, a name holding `::`); and a column whose type cannot be a tag. A tag is
one value per row of type `int`, `float`, `bool`, `str`, `bytes`, `datetime` or `date`.
A nullable column (`T | None`) is refused: drop the rows with a null first,
`s.drop_nulls("x").tag_by("x")`, which narrows it to `T`. Lists, sets, dicts, structs,
your own classes and logical types (`Path`, `UUID`, arrays) are refused.

`tag_by` adds the tag role; the drop operators remove a role. For a referential tag `x`,
dropping the tag (`drop_tag_columns(["x"])`) leaves `x` plain data, dropping the data
(`drop_data_columns(["x"])`) leaves `x` a plain tag, and dropping the column
(`drop_columns(["x"])`) removes it from both roles.

## Operators

| Operator | Referential tag `x` |
|---|---|
| `TagBy` / `tag_by` | Makes the data column `x` a referential tag (above); everything else is unchanged. |
| `Join` | Plain with plain: as before. Plain with referential, or referential with referential: a referential tag and **one** data column `x`. When `x` is referential on several inputs, its source info is one merged token (below). A key that is `T` in one input and `T \| None` in another is `T` in both roles, as for a plain key. Every other data-name collision is still an error. |
| `GroupBy` | A group key stays scalar in both roles, and its source info is one merged token (below). A non-key loses its tag role, as every non-key tag does, but keeps its data column: a list like every other data column, one source-info token per member. The members are ordered by the non-key tags, so that list is in value order and the other data lists follow it. |
| `SelectDataColumns`, `DropDataColumns` | Removing the data column leaves `x` a plain tag with the same values, with a `ReferenceMaterializationWarning`. |
| `SelectTagColumns`, `DropTagColumns` | Removing the tag leaves `x` a data column. |
| `SelectColumns`, `DropColumns` | `x` is one column, so naming it acts on both roles: selecting it keeps `x` referential, dropping it (or not selecting it) removes the column from both roles. The roles are never split, so nothing is materialized. |
| `MapTags` / `map_tags` | Renames the column in both roles; `tag_only=True` renames the tag only and leaves a plain tag. |
| `MapData` / `map_data` | Always a data-only move: the data column is renamed and `x` stays a plain tag. |
| Filters, `DropNulls`, `SemiJoin`, `Pick`, `Index` | Keep the reference. |
| `Batch` | Keeps every tag, list-valued, as for plain tags: `x` becomes the same list in both roles and stays referential. Its source info is one token per member, like any data column: nothing is merged. |
| `MergeJoin` | Not supported with referential tags yet: it raises. |

```python
units = op.sources.DictSource(
    [{"unit": 1, "area": "V1"}, {"unit": 2, "area": "V2"}],
    tag_columns=["unit"],
    source_id="units",
)
joined = spikes.join(units)
print(joined.keys())
# (('unit', 'time'), ('unit', 'area', 'time', 'amp'))
```

```python
print(spikes.select_columns(["time", "amp"]).keys())
# (('time',), ('time', 'amp'))
print(spikes.drop_columns(["time"]).keys())
# (('unit',), ('unit', 'amp'))
```

`Batch` lists every column, a referential tag included, so the tag and its data stay one
column:

```python
batched = spikes.batch()
print(batched.keys())
# (('unit', 'time'), ('unit', 'time', 'amp'))
for tag, data in batched.iter_data():
    print(tag["time"], data["time"])
# [10, 20] [10, 20]
```

### After `GroupBy`: rename a non-key column with `map_data`

`GroupBy` keeps only its key tags, and drops every other tag itself: there is nothing left
to drop with `drop_tag_columns` afterwards (that raises, since the tag is gone). A
referential non-key column loses its tag role there, but keeps its data column, a list
like every other data column. So after the grouping it is data only, and is renamed with
`map_data`; `map_tags` refuses it, since it is no longer a tag, and the error says to use
`map_data`.

```python
by_unit = spikes.group_by(["unit"])
print(by_unit.keys())
# (('unit',), ('unit', 'time', 'amp'))

renamed = by_unit.map_data({"time": "times"})
print(renamed.keys())
# (('unit',), ('unit', 'times', 'amp'))
```

### Source info of a merged value

A source-info column belongs to its data column and has its shape: a scalar value has one
token, a list one token per element. When a value is the merge of several equal elements
that are different entries (the key of a `GroupBy`, or the key a `Join` merges from
several referential inputs), no single member's token is true for it. Its source info is
then one **merged token**, the fold of the members' tokens by the operation that merged
them (the SHA-256 of their canonical JSON list, after the operation's label), as the
system tags of a group are folded:

- `GroupBy`: `group_v1:<hex>` over the members' tokens in member order (the order of the
  group's data lists, which does not depend on row order);
- `Join`: `join_v1:<hex>` over the members' tokens sorted, so it does not depend on the
  order of the inputs.

It is a merged token for a group of one member too, and a merged token can itself be a
member of a later merge. A merged token holds no `::`, so it never reads as a row's
`<source id>::<record id>::<column>`. No content hash, pipeline hash, record id or
function result identity reads source info, so these tokens move no stored result.

```python
sessions = op.sources.DictSource(
    [{"subject": "m1", "t": 1, "v": 0.5}, {"subject": "m1", "t": 2, "v": 0.7}],
    tag_columns=["subject", "t"],
    source_id="sessions",
)
grouped = sessions.group_by(["subject"]).as_table(columns={"source": True})
print(grouped.column("_source_subject").to_pylist())
# ['group_v1:...']
print(grouped.column("_source_v").to_pylist())
# [['sessions::row_0::v', 'sessions::row_1::v']]
```

The materialization warning can be turned off; it never changes a result or a hash:

```python
from orcapod.core import referential_tags

referential_tags.reference_materialization = "silent"
# ... build the pipeline ...
referential_tags.reference_materialization = "warn"  # the default
```

## Stored tables and saved jobs

A stored table states which of its tags are referential. A function node stores each
row's tags in its pipeline table, and a referential tag is stored there once, as a tag
column that carries Arrow field metadata: the key `orcapod:tag_kind` with the value
`referential` (`referential_tags.REFERENTIAL_TAG_METADATA_KEY`). A plain tag carries no
marker. The function's result table never records referential status, so plain and
referential runs of the same function share their results. The marker moves no hash, and
the stores keep it on every write (first write, append, merge, schema evolution; in
memory and on Delta). A database states whether it keeps
field metadata with `keeps_field_metadata`
(`orcapod.protocols.database_protocols.FieldMetadataKeepingDatabaseProtocol`): the
in-memory, Delta and SpiralDB databases do; the SQLite and PostgreSQL databases keep no
Arrow metadata at all. A node that would store a referential tag in a database that does
not state it (a database without the member included) is refused when its database is
attached, before anything is written. Use a database that keeps field metadata, or keep
those tags plain with `tag_only_columns`.

```python
from orcapod.core.referential_tags import stored_references
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import PipelineJob

database = InMemoryArrowDatabase()
job = PipelineJob(database=database)
with job:
    scale.pod(spikes, label="scale")
job.run()
pipeline_table = next(
    database.get_all_records(path)
    for path in database.list_table_paths()
    if "_result" not in path
)
print(stored_references(pipeline_table.schema))
# ('unit', 'time')
```

A node reads the binding back from its table, and refuses a table whose marker disagrees
with what the node declares (its pod's fall-through tags, or for a loaded job the saved
file's).

An operator node that stores its output (`LOG`) marks its table the same way: the
referential tags of the operator's own output carry the marker, written from the
operator's binding, never from a marker that came in from upstream. A `REPLAY` read, and
the stored view `get_all_records()`, check the marker against the node (its operator's
prediction, or for a loaded job the saved output schema, where a referential tag is in
both halves). A database that keeps no field metadata is refused when it is attached in
`LOG` or `REPLAY` mode. Source info stays part of each stored row's record hash; the
marker never enters it.

`node.as_source()` gives the stored rows as a source with the bindings the stored table
states, in the plain form and with `tags_as_data` (a copy of a referential tag is a new,
plain data column). Nothing is added to the derived source's identity for the binding: its
plain form's content hash is the node's, which carries the binding already, and its
pipeline hash is its output schema, where a referential tag is in both halves.

```python
derived = job.nodes["scale"].as_source()
print(derived.keys())
# (('unit', 'time'), ('unit', 'time', 'scaled'))
print(derived.as_table().column_names)
# ['unit', 'time', 'scaled']
```

In a stream the binding is the shared name, so the marker stays with the stored table:
the rows of a replay or of a derived source carry none.

A saved job or pipeline (format `0.3.0`) states every binding: each source config writes
`tag_only_columns` (`True` when every tag is plain), and each function node writes
`fall_through_references` (empty for a plain pod). The format version, never a missing
key, decides what a file means: a file at `0.1.0` or `0.2.0` holds only plain tags, and a
`0.3.0` file without these keys is refused. A loaded job keeps its hashes and bindings, and
the file is checked against the stored tables when a node reads them.

## Not supported yet

These raise "not supported with referential tags yet" when a referential tag reaches them:
`MergeJoin` and `MapColumns` (`map_columns`; rename with `map_tags` and `map_data`).
`PollingSource` keeps its tags plain. Each error names its issue and the way out: build the
source with `tag_only_columns=True` to keep every tag plain, as in v0.3. A plain tag is not
data, so a function that reads a tag's value (as `scale` above reads `time`) can no longer
take it from that column: as in v0.3, the value must also be a data column of its own, for
example a copy in the source's data under another name, or
`DerivedSource(origin, tags_as_data={...})`.

## Errors that name a referential tag

- `pod.process(stream)` and `pod(stream, strict=True)` are exact: they hand every data
  column to the function, so a referential tag the function does not read is refused like
  any extra column. The error names the referential tags and the two ways out: call the
  pod, `pod(stream)`, which for a plain `FunctionPod` lets them pass through as
  referential tags (a wrapper pod such as `CachedFunctionPod` has no fall-through: its
  call keeps them as plain tags), or keep them plain with `tag_only_columns`. It offers
  only a way out that can work: when no data column of the input is a parameter, a
  wrapper pod's call and `tag_only_columns` would leave the function nothing to read,
  and the error says so instead.
- Binding a source to a job's input slot is strict: a column that is a referential tag on
  one side and a plain tag or a data column on the other is refused (it is listed as a
  missing or unexpected column). The error says which side holds it as a referential tag,
  and how to make the two agree, in either direction. A referential source tag for a
  plain slot tag: keep it plain with `tag_only_columns`, or declare the slot with the
  column in both roles. A plain source tag (or a data-only source column) for a
  referential slot tag: make it a declared tag that `tag_only_columns` does not name, or
  declare the slot with it as the source has it (a plain tag, or a data column only). `tag_only_columns` lists every plain tag of the source, so the error
  names the exact list.
