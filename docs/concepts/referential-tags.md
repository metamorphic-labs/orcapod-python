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

## Operators

| Operator | Referential tag `x` |
|---|---|
| `Join` | Plain with plain: as before. Plain with referential, or referential with referential: a referential tag and **one** data column `x`. When `x` is referential on several inputs, its source info is one merged token (below). Every other data-name collision is still an error. |
| `GroupBy` | A group key stays scalar in both roles, and its source info is one merged token (below). A non-key loses its tag role, as every non-key tag does, but keeps its data column: a list like every other data column, one source-info token per member. The members are ordered by the non-key tags, so that list is in value order and the other data lists follow it. |
| `SelectDataColumns`, `DropDataColumns` | Removing the data column leaves `x` a plain tag with the same values, with a `ReferenceMaterializationWarning`. |
| `SelectTagColumns`, `DropTagColumns` | Removing the tag leaves `x` a data column. |
| `MapTags` / `map_tags` | Renames the column in both roles; `tag_only=True` renames the tag only and leaves a plain tag. |
| `MapData` / `map_data` | Always a data-only move: the data column is renamed and `x` stays a plain tag. |
| Filters, `DropNulls`, `SemiJoin`, `Pick`, `Index` | Keep the reference. |
| `Batch`, `MergeJoin` | Not supported with referential tags yet: they raise. |

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

### Source info of a merged value

A source-info column belongs to its data column and has its shape: a scalar value has one
token, a list one token per element. When a value is the merge of several equal elements
that are different entries (the key of a `GroupBy`, or the key a `Join` merges from
several referential inputs), no single member's token is true for it. Its source info is
then one **merged token**, `merged:source_fold_v1:<hex>`: the SHA-256 digest of the
members' tokens, sorted, so it does not depend on row or input order. It is a merged
token for a group of one member too, and a merged token can itself be a member of a
later merge. No content hash, pipeline hash, record id or function result identity reads
source info, so these tokens move no stored result.

```python
sessions = op.sources.DictSource(
    [{"subject": "m1", "t": 1, "v": 0.5}, {"subject": "m1", "t": 2, "v": 0.7}],
    tag_columns=["subject", "t"],
    source_id="sessions",
)
grouped = sessions.group_by(["subject"]).as_table(columns={"source": True})
print(grouped.column("_source_subject").to_pylist())
# ['merged:source_fold_v1:...']
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

## Not supported yet

These raise "not supported with referential tags yet" when a referential tag reaches them:
`Batch`, `MergeJoin`, storing an operator node's results (`LOG` and `REPLAY` cache modes),
`DerivedSource` over a stream with referential tags, and saving a pipeline or job.
`PollingSource` keeps its tags plain.
