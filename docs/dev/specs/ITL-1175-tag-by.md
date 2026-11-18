# ITL-1175: mark existing data columns as referential tags (`tag_by`)

Status: approved by the coordinator on 2026-11-16, with two developer decisions (the
name `tag_by` / `TagBy`; a computed column may become a tag). Stacked on the referential
tags slice (ITL-1023, PR #342). The sections below are the design as sent; "Decisions"
records what changed.

## Decisions

- Name: `tag_by` (developer); the class follows the operator naming: `TagBy`.
- A computed column may become a tag and key a join or a group (developer); no lineage
  check.
- Type rule: the slice has no type rule for a referential tag declared at a source (a
  source accepts any type), so `TagBy` uses its own rule, in one function
  (`referential_tags.require_taggable_type`) that the source path could adopt: `int`,
  `float`, `bool`, `str`, `bytes`, `datetime`, `date`. `Decimal`, time of day and
  duration are not in it: the type converter reads them as no Python type (`Any`), and
  `Any` is refused. The source path is not changed here; the inconsistency is reported
  in the PR.
- Removing a role afterwards (developer, November 16, 2026, ITL-1195): no new operators.
  For a referential tag, dropping the tag leaves plain data, dropping the data leaves a
  plain tag, dropping the column removes both roles. So `tag_by("x")` followed by
  `drop_data_columns(["x"])` leaving a plain tag is intended behaviour. The docs
  describe the three moves by role in one paragraph; final wording comes with ITL-1195.

## Public API

- Operator `orcapod.core.operators.TagBy(columns)`; stream method
  `stream.tag_by(*columns, label=None)`. `s.tag_by("x", "y")` equals
  `s.tag_by(["x", "y"])` (the convention of `select_columns`).
- The name is a choice for the developer. It lives in one place per surface: the class
  name, the stream method, the serialization registry entry, the docs. Not
  `reference_tags`: `ArrowTableStream.reference_tags` is already a property.
  Alternatives: `as_tags` / `AsTags`, `mark_tags` / `MarkTags`.
- No `strict` flag: a name that cannot be marked is always an error.

## Order rule and identity

- The names are ordered: the output's user tags are the input's user tags, in their
  order, then the named columns in the order given. So the order is part of the
  operator, and its identity is `("TagBy", [names in the order given])`; its pipeline
  identity is the same. A `set` / `frozenset` is refused (`TypeError`, unordered, as for
  `record_id_columns`), as is an empty list and a repeated name (`ValueError`).

## Refusals

At construction (`ValueError` / `TypeError`, naming the column): no name; a non-string;
a repeated name; a set; the meta prefix (`require_names_not_meta`); a system column name
(`require_names_not_system`); a name holding `::` (`require_names_without_separator`).

At validation (`InputValidationError`, naming the column, raised when the pipeline is
built, sync and async alike):

- not a data column of the input (lists the input's data columns);
- already a tag: a referential tag ("already a referential tag"); a plain tag is never a
  data column, so it is reported as a tag, not a data column;
- a type that cannot be a tag. Recommended rule: the column's Python type must be one of
  `int`, `float`, `bool`, `str`, `bytes`, `datetime`, `date` (see Decisions). Refused: `T | None` (the error says to apply
  `drop_nulls("x")` first, which narrows it to `T`), lists, sets, dicts, tuples, structs,
  user classes and every logical type (`Path`, `UUID`, arrays...). Sources and function
  outputs are non-nullable by default, so this costs nothing in the common case.
  Conservative variant: also refuse `float` (equality on floats as a key).

## What the output is

- Tags: input user tags, then the new names; system tags unchanged (rule 1,
  name-preserving: same column names, same values; record ids unchanged).
- Data: unchanged: same names, order, types, nullability, values; each column keeps its
  own source-info token. No token is created or merged.
- Table layout: the column is stored once and stays where it is, as a source leaves a
  referential tag at its place in the table: the data order is the input's (the
  predicted one). (The first version moved it into the user tag group, which put the
  new tags first among the data columns, unlike the prediction: CodeRabbit on PR #420.)
- Schema prediction (`unary_output_schema`): the tag schema gains the named columns, with
  their data types, after the user tags and before the system tags; the data schema is
  unchanged. Verified against `static_process(...).output_schema(...)`.
- Sync: the input table re-wrapped as an `ArrowTableStream` with the extra tags in
  `tag_columns` and `reference_tags`: no Polars round trip, no cast.
- Async: no own `async_execute`: the base barrier path collects the rows and applies the
  sync rule, so the rows are identical by construction. Added to
  `test_async_sync_equivalence.py`. A streaming version can come later.

## Why "no data element comes from a tag value" holds

The operator adds a tag role to a data column; it creates no value and moves none. Every
output data column is the input data column of that name, with the same values and
source info, and no input tag becomes data. The column stays one physical column, as a
source's referential tag is. The direction the rule forbids (a tag value offered as data)
stays with `DerivedSource(tags_as_data=...)`.

## Downstream

- `Join` with a stream where `x` is a plain tag or a referential tag: the slice's rules
  (one referential tag, one data column, merged `join_v1` token when referential on
  several inputs). With a stream where `x` is data only: the existing tag/data error (its
  remedy text gains "or mark it with `tag_by`").
- `GroupBy(["x"])`, `Batch`, `MapTags`, select/drop: the slice's rules for any
  referential tag.
- Removing the data role afterwards (`tag_by("x").drop_data_columns("x")`) leaves a plain
  tag holding the data value, with the `ReferenceMaterializationWarning`, as for any
  referential tag. That is the data-to-tag direction (the rule holds), but it makes the
  issue's out-of-scope "plain tag from a data column" reachable in two steps.
  Recommendation: allow (existing slice behaviour, warned). Conservative variant: none
  that is small; it would need tracking where a tag came from.
- Jobs: an operator node runs it in every cache mode; in `LOG` mode it stores its output
  with the new tag marked (operator storage of referential tags came with #407, ITL-1019;
  tested). A function node downstream stores the marker (ITL-1019). Saved jobs: `to_config` / `from_config`, and
  the class is registered in `pipeline/serialization.py`.

## Open choice: may a computed column key a join?

Recommendation: yes. It is the issue's own typical use (a function computes a session
label that later steps join, group or sort by), and nothing in hashing or record
identity depends on where a tag's value came from. Conservative variant: `TagBy` refuses
a column a function pod produced. That needs a lineage walk upstream (through renames and
joins) in one helper called from validation; it is not small, and I recommend against it.

## Not in scope

Golden pins (identities provisional in 0.4.0rc1); a plain-tag variant; the reverse
direction; `MergeJoin` (ITL-1194).
