# Identity & Hashing

Every pipeline element in Orcapod -- [sources](sources.md), [streams](streams.md),
[operators](operators.md), and [function pods](function-pods.md) -- carries two parallel
identity hashes. These hashes enable Orcapod to deduplicate computations, scope database
storage, and detect when a pipeline's structure or data has changed.

## Two identity chains

### `content_hash()` -- recursive source-inclusive identity

The content hash captures schema, topology, and the **identity of the sources** feeding the
pipeline. It is computed recursively: each element's content hash depends on its own identity
plus the content hashes of all its upstream elements, all the way back to the sources.

A source's identity is its class, its schema, its `source_id` and its key columns (the
`record_id_columns` that name its rows, or none when rows are named by position). What the
`source_id` is depends on how the source was created:

- If you do not pass `source_id`, an in-memory source such as `DictSource` or
  `ArrowTableSource` derives it from the data, so two sources with different rows have
  different content hashes, and two sources with identical rows have the same one.
- File- and table-backed sources default to a name derived from their location (the file
  path for `CSVSource`, the table folder's path or URI for `DeltaTableSource`), not from the
  rows.
- If you pass `source_id` yourself, you are naming the source: two sources with the same
  class, schema, `source_id` and key columns have the same content hash even when their rows
  differ.

Two sources that differ only in their key columns name the same rows differently, so they
are two sources: different content hashes (each gets its own cache), the same pipeline hash
and the same `source_id`.

Rows are still identified individually. Each row carries its own content hash, and that
per-row hash is what caching uses.

**Used for:** deduplication and memoization. When a function runs inside a
[`PipelineJob`](function-pods.md#pipelinejob-database-backed-cached-execution), each input
row's content hash is checked against the database. If the hash already exists, the cached
result is returned without recomputation.

### `pipeline_hash()` -- schema and topology only

The pipeline hash captures the pipeline's **structure** -- schemas, function identities, and
how elements are connected -- but deliberately ignores source identity. Two pipeline elements
with identical Arrow schemas and the same computational graph have the same pipeline hash,
even if they are fed by completely different sources.

**Used for:** database scoping. Pipeline hash determines the database table path where results
are stored. This means that two pipeline nodes with the same function, the same input schema
and the same upstream steps share the same database table -- regardless of which source
produced the data. "The same input schema" means the same Arrow schema: a source whose `x`
column is `int32` and one whose `x` is `int64` both read as `int` in `output_schema()`, but
they are two schemas, with two pipeline hashes and two tables.
This is a powerful feature: it means that running the same function on new data automatically
benefits from results already cached for previous data with the same schema.

## How it works in practice

Consider two sources with the same schema but different data:

```python
from orcapod.sources import DictSource

source_a = DictSource(data=[{"x": 1, "y": 2}], tag_columns=["x"])
source_b = DictSource(data=[{"x": 10, "y": 20}], tag_columns=["x"])

print(source_a.content_hash() == source_b.content_hash())
# False
print(source_a.pipeline_hash() == source_b.pipeline_hash())
# True
```

- The content hashes differ -- different source identity
- The pipeline hashes are equal -- same schema and structure

If `source_a` and `source_b` feed into the same function in a `PipelineJob`, the nodes share
a database table (same pipeline hash), but each row is stored and retrieved by its content
hash.

The schema in a hash is the Arrow schema, not the Python one. Two sources whose column holds
the same numbers as `int32` in one and `int64` in the other have different pipeline hashes:

```python
import pyarrow as pa

from orcapod.sources import ArrowTableSource


def source_of(int_type):
    table = pa.table(
        {
            "subject": pa.array(["s1", "s2"], pa.large_string()),
            "session": pa.array([1, 2], int_type),
            "x": pa.array([0.5, 1.5]),
            "y": pa.array([2, 3], int_type),
        }
    )
    return ArrowTableSource(
        table, tag_columns=["subject", "session"], tag_only_columns=True
    )


narrow, wide = source_of(pa.int32()), source_of(pa.int64())
print(narrow.output_schema() == wide.output_schema())
# True
print(narrow.pipeline_hash() == wide.pipeline_hash())
# False
```

What the schema hash ignores (column order, large and non-large variants, a list's item
field name, dictionary encoding, field metadata) and what it tells apart is listed in the
[hashing reference](../reference/hashing.md#the-arrow-schema-hash).

## The Merkle chain

Pipeline hashes form a **Merkle chain** -- each element's pipeline hash commits to its own
identity plus the pipeline hashes of all its upstream elements.

### Base case: sources

A `RootSource`'s pipeline identity is the hash of its Arrow tag schema and Arrow data schema:
the types its rows have. Sources with the same column names and Arrow types have the same
pipeline hash, regardless of their data; an input slot declared with Python types stands for
one Arrow type per Python type (`int` is `int64`, `datetime` is `timestamp[us, tz=UTC]`), and
a slot can also be declared with Arrow schemas.

### Recursive case: downstream elements

Each downstream element (operator, function pod, or node) computes its pipeline hash from:

1. Its own identity (e.g., the function's name, major version, and output schema for a
   function pod; the operator class name for an operator)
2. The pipeline hashes of all its upstream streams

This creates a chain: changing any element's structure (renaming a column, renaming a
function or its output, adding an operator) changes the pipeline hash of that element and all downstream
elements, while leaving upstream hashes unchanged.

## The resolver pattern

Orcapod uses a resolver pattern to determine which hash method to call on different objects:

- Objects implementing `PipelineElementProtocol` (sources, operators, function pods, streams)
  route through `pipeline_hash()`
- Other `ContentIdentifiable` objects (like raw data values) route through `content_hash()`

This distinction matters when computing pipeline identity structures: a function pod's
pipeline hash depends on its upstream stream's pipeline hash (structural), not its content
hash (data-inclusive).

## Identity in practice

### Memoization

When a function node runs inside a `PipelineJob`, each input row is handled in one of two
ways:

1. **Cached:** if a result for that input's content hash is already in the shared database
   table, it is read back and the function is not called.
2. **Computed:** otherwise the function runs, and the result is stored and returned.

This means that adding new data to a source only triggers computation for the new rows --
previously computed results are served from the cache. The node's own stream
(`iter_data()`, `as_table()`) contains the results for its current inputs only;
`get_all_records()` also returns rows stored by earlier runs of the same node -- same job
name, same source -- over inputs that are no longer present. It does not return rows that
other nodes wrote to the same shared table, such as the same function run over a source with
a different `source_id`.

### DB path scoping

The pipeline hash determines the database table path. Two nodes that apply the same function
to sources with the same Arrow schema, through the same upstream steps, will write to and read
from the same table; sources that differ in any Arrow type write to two tables. This is intentional: it maximizes cache reuse across pipeline runs. A
schema-preserving step added upstream (a filter, say) changes the pipeline hash and so the
table. This is the default `table_scope="pipeline_hash"` of a node; a node built with
`table_scope="content_hash"` also adds its content hash to the path, giving each source its
own table.

### Change detection

A function's identity is its **name**, its **output schema** and its **major version**
(`version="v1.0"` has major version 1; the default is `"v0.0"`). Changing any of these
changes the pipeline hash of its node and of all downstream nodes, which creates new database
table paths, so results stored under the old identity are not returned for the new one.

The function's source code is **not** part of that identity. Editing the body of a function,
or bumping only the minor version (`"v0.0"` to `"v0.1"`), leaves the pipeline hash unchanged,
and previously stored results keep being returned:

```python
from orcapod import function_pod

source = DictSource(data=[{"subject_id": "mouse_01", "age": 12}], tag_columns=["subject_id"])

@function_pod(output_keys="next_age")
def step(age: int) -> int:
    return age + 1

original = step.pod(source).pipeline_hash()

@function_pod(output_keys="next_age")
def step(age: int) -> int:
    return age + 2  # body edited, same name and version

print(step.pod(source).pipeline_hash() == original)
# True

@function_pod(output_keys="next_age", version="v1.0")
def step(age: int) -> int:
    return age + 2

print(step.pod(source).pipeline_hash() == original)
# False
```

When you change what a function computes, bump its major version (or rename it) so that
stale results are not reused.

The output schema is a set of named, typed outputs: each output key with its type, in no
order, like every other schema. `output_keys` also says which position of the returned
tuple each name takes, and that mapping belongs with the body. So reordering the output keys
changes the identity only when a key's type changes. Swapping two keys of the same type
keeps the identity, as a body edit does. When a name now receives another value (the keys
swapped without the returned values, or the reverse), what the function computes changed:
bump the major version. Until stored results are checked against the current function on a
cache hit (ITL-1253), a run serves the earlier results without a warning. Swapping the keys
and the returned values together changes nothing: each name receives the value it had. The
function's input parameters (their names and their order) are not part of the identity
either.

```python
@function_pod(output_keys=["total", "doubled"])
def pair(age: int) -> tuple[int, int]:
    return age + 1, age * 2

original = pair.pod(source).pipeline_hash()

@function_pod(output_keys=["doubled", "total"])  # same types: same identity
def pair(age: int) -> tuple[int, int]:
    return age + 1, age * 2

print(pair.pod(source).pipeline_hash() == original)
# True

@function_pod(output_keys=["label", "next_age"])
def describe(age: int) -> tuple[str, int]:
    return f"age {age}", age + 1

original = describe.pod(source).pipeline_hash()

@function_pod(output_keys=["next_age", "label"])  # next_age is now a str: new identity
def describe(age: int) -> tuple[str, int]:
    return f"age {age}", age + 1

print(describe.pod(source).pipeline_hash() == original)
# False
```

## Order never counts, except where you give it

The rule: no hash, identity, stored value or row depends on the order in which you give
Orcapod a set of things. If an order matters, it is an explicit argument. The rule does not
hold everywhere yet: a few cases still break it, each owned by an open issue, and they are
listed under [What does not hold yet](#what-does-not-hold-yet) below. They are defects to be
fixed, not exceptions. The places where an order is meant to count are the table of
exceptions further down. By the rule, none of these changes a content hash, a pipeline
hash, a system-tag column name, a stored table path, a stored row, a record id or a row's
hashes:

- the order of a source's `tag_columns` (plain tags and data-tags alike) and the order of
  the columns in its table;
- the order of the names, mapping entries or keywords of an argument that names several:
  the columns of `select_columns`, `drop_columns`, `drop_nulls` and the other selections,
  the entries of `map_tags`, `map_data` and `map_columns`, the constraints of
  `polars_filter`, `tag_only_columns`, `tags_as_data`;
- the order of the inputs of an operator that declares itself commutative (`Join`), and of
  the inputs of a function pod (for the ordered operators, see the table of orders that
  count below);
- the path the rows take: the sync and the async run give the same rows.

What follows your order is where the columns are placed: `keys()`, `as_table()` and a
stored table place them after your declared `tag_columns` and your table's columns
(data-tags first among the data columns). That placement is presentation only.

```python
from orcapod.sources import DictSource

visits = [
    {"subject": "m1", "session": 1, "score": 0.5},
    {"subject": "m1", "session": 2, "score": None},
    {"subject": "m2", "session": 1, "score": 0.75},
]
by_subject = DictSource(
    data=visits, tag_columns=["subject", "session"], tag_only_columns=True, source_id="visits"
)
by_session = DictSource(
    data=visits, tag_columns=["session", "subject"], tag_only_columns=True, source_id="visits"
)
print(by_subject.content_hash() == by_session.content_hash())
# True
print(by_subject.keys()[0], by_session.keys()[0])
# ('subject', 'session') ('session', 'subject')
kept = by_subject.drop_nulls(["score", "subject"])
same = by_session.drop_nulls(["subject", "score"])
print(kept.content_hash() == same.content_hash(), kept.as_table().num_rows)
# True 2
```

The order counts only where it is the point of the argument:

| Order that counts | Why |
|---|---|
| `record_id_columns` of every source (by default a database table's primary key) | A record id is the canonical JSON of the key columns in key order, so the key order is part of every record id and of the source's identity. It is the explicit argument that states the order. |
| `member_order` of `GroupBy` (ITL-1226; not in a release yet) | The explicit order of the members inside each group's lists, the argument that replaces the input's tag order there. |
| the inputs of an operator whose `argument_symmetry` is a tuple (`SemiJoin`, `MergeJoin`) | The inputs have roles: `SemiJoin(left, right)` keeps the rows of `left`; `MergeJoin(a, b)` lists `a`'s value first in every merged list (ITL-1281). |
| the order of the ROWS given to a source without key columns | Its record ids are the row positions, so a reorder is a change (by decision). Row order, never column order. |
| the order of the ROWS given to `Batch` | `Batch` groups consecutive rows: row order, never column order. |
| the fields of a user class (`@dataclass`, `orcapod.dataclass`, pydantic) | A class is identified with its fields in declared order: reordering them declares another type. |
| the declared `tag_columns` order and a table's data column order | Presentation only: they place the columns of `keys()`, `as_table()` and stored tables (data-tags first, ITL-1205), and enter no hash, identity or stored value. That this placement is consistent across layouts and paths is ITL-1249. |

A record id names a row by its key columns in key order, so the key order is part of the
source:

```python
keyed = DictSource(
    data=visits,
    tag_columns=["subject", "session"],
    tag_only_columns=True,
    source_id="visits",
    record_id_columns=["subject", "session"],
)
rekeyed = DictSource(
    data=visits,
    tag_columns=["subject", "session"],
    tag_only_columns=True,
    source_id="visits",
    record_id_columns=["session", "subject"],
)
print(keyed.content_hash() == rekeyed.content_hash())
# False
```

### What does not hold yet

`tests/test_core/test_order_invariance.py` checks the rule for every exported operator,
source and stream kind, the function pod forms and a job. Today these cases still break it
(the test marks each as an expected failure naming its issue); they are being fixed before
data context v1 is frozen:

- the order of `polars_filter`'s predicates is in its identity (ITL-1247, undecided);
- a function pod over data-tags: its content and pipeline hash and its node's stored paths
  depend on the input's tag order and table layout (ITL-1246);
- a stream built directly from a table (`ArrowTableStream`): its content hash depends on
  the order of its tags (ITL-1227);
- which rows `head()` shows, sorted by tags, follows the tag order (ITL-1270);
- where the columns are placed is not always the declared order: an operator places its
  output tags in its input table's column order (ITL-1249, presentation only).

## Your own classes in the identity

A class enters both chains through its **identity**: its kind, its name and the names and
types of its fields, in declared order. The identity is part of every schema hash, so it
reaches the pipeline hash of each source, pod and node that uses the class, and with it
the database table paths. Each stored value is hashed together with it, so it also
reaches the content hash.

The name depends on the kind of class:

- A class made with `@op.dataclass` is named by its class name (or the `name=` you give).
  Two such classes with the same name and fields share results, wherever each is
  defined.
- An ordinary `@dataclass` or a pydantic model is named by its module path and class
  name. Moving it to another module makes a new type.

In both kinds a class whose fields change or are reordered is a new type: its pods get
new pipeline hashes and new tables, and the results stored with the old definition stay
where they are.

[Your Own Data Types](your-own-data-types.md) describes the two kinds and when to use
each.

## How it connects to other concepts

- [Sources](sources.md) form the base case of the Merkle chain (schema-only for pipeline hash,
  source identity for content hash)
- [Streams](streams.md) carry both hashes and propagate them through the pipeline
- [Operators](operators.md) contribute their own identity to the chain
- [Function Pods](function-pods.md) run inside a `PipelineJob` use content hashes for
  memoization and pipeline hashes for DB scoping
- [Your Own Data Types](your-own-data-types.md) explains the identity of dataclasses and
  pydantic models
