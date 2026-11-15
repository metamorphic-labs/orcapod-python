# ITL-1144: guard against a None inside a container column that a read cannot restore

Status: measurement done (main fe433eab, PR #393 head 8b75ccea); proposal sent to the
coordinator; implementation waits for the reply.

## Measurement

Scripts: the scratchpad `measure/` directory of this session (`cases.py`, `measure.py`,
`probe2.py`, `probe3.py`, `probe4.py`); results summarized here.

Paths: A = function output in a job, read back by `node.as_source()`; B = `DictSource`
built from Python values, read by `iter_data()`; C = the saved job loaded in a fresh
process, `as_source()`; D = a downstream function in the job annotated with the type.
"Row hash" = `Data.content_hash()` of a one-row `DictSource` holding the value read,
compared with the same for the value written.

Stored Arrow always holds the null correctly (write side faithful), and the in-memory
row hash equals the stored row hash in every case. The loss is only on the read to Python.

| element | container | main (A, B, C, D agree) | PR #393 head |
|---|---|---|---|
| int, str, bytes, datetime | `list[T \| None]` | OK | OK |
| same | `set[T \| None]` | None kept; set reads back as list (ITL-611), hash equal | OK |
| same | `list[list[T \| None]]` | OK | OK |
| same | `list[set[T] \| None]` | LOST `[None,{1}]` -> `[[],[1]]`, hash differs | LOST `[set(),{1}]`, hash differs |
| same | `set[T] \| None` (top-level None) | OK | OK |
| same | `dict[str, T \| None]` | OK | OK |
| same | dataclass field `x: T \| None` | OK | OK |
| same | dataclass field `xs: list[T \| None]` | OK | OK |
| UUID, Path | `list[T \| None]`, `set[T \| None]`, `list[list[T \| None]]`, field `x: T \| None`, field `xs: list[T \| None]` | CRASH `TypeError` on read | same |
| UUID, Path | `list[set[T] \| None]` | LOST `[set(), {u}]`, hash differs | same |
| UUID, Path | `set[T] \| None` | OK | OK |
| UUID, Path, user classes | `dict[str, T \| None]` | refused at definition already (ET1, PLT-1732) | same |
| dataclass, orcapod.dataclass | `list[T \| None]`, `list[list[T \| None]]`, `set[T] \| None`, field `x`, field `xs` | OK | OK |
| dataclass, orcapod.dataclass | `set[T \| None]`, `list[set[T] \| None]` | n/a (instances unhashable) | n/a |

Extra probes (path B):

- LOST (None reads as `[]`, `{}` or `set()`, hash differs): `list[list[int] | None]`,
  `list[dict[str, int] | None]`, `dict[str, list[int] | None]`, `dict[str, set[int] | None]`,
  `dict[str, dict[str, int] | None]`, `list[list[UUID] | None]`, `list[list[Pt] | None]`,
  `list[list[SPt] | None]`, dataclass fields `x: list[int] | None`, `set[int] | None`,
  `dict[str, int] | None`, `list[UUID] | None`.
- CRASH on read: `list[UPath | None]`, `list[np.ndarray | None]`, `list[torch.Tensor | None]`,
  `list[pd.DataFrame | None]`, `list[pl.DataFrame | None]`, dataclass field
  `x: np.ndarray | None`, pydantic field `x: UUID | None`.
- OK: `list[Pt | None]`, `list[PydanticModel | None]`, dataclass field
  `x: PydanticModel | None`, a top-level None of any type.
- Not reached by a declaration: `DictSource` declared `list[list[int]]` given `[None, [1]]`
  accepts it and reads `[[], [1]]`; `ArrowTableSource` of `large_list<large_list<int64>>`
  holding `[None, [1]]` types as `list[list[int]] | None` (ITL-1136) and reads `[[], [1]]`.
  A function output refuses a None in a non-optional position (`OutputTypeError`).
- An in-memory pod -> pod chain (no job, no storage, no source built from the values)
  keeps the None for every refused type.

Operator-built lists (coordinator follow-up; `ops.py`): source tags `g`, `k` (`k` null in
one row for Batch), one nullable data column per job; GroupBy(by=g), Batch(2), MergeJoin.
Read via the plain stream, the operator job node (`iter_data`), its LOG cache
`as_source()`, a downstream echo function (what it receives, its `as_source()`), and the
echo node of the job loaded in a fresh process.

| operator | column | predicted | every path |
|---|---|---|---|
| GroupBy, Batch | `m: int \| None` | `list[int \| None]` | OK `[None, 2]` (loaded job too); Batch list tag `k` `[None, 8]` OK |
| GroupBy, Batch | `l: list[int] \| None`, `t: set[int] \| None` | `list[list[int] \| None]` | LOST `[[], [1]]`; the echo function receives and STORES `[[], [1]]` |
| GroupBy, Batch | `p: Path \| None`, `u: UUID \| None` | `list[Path \| None]`, `list[UUID \| None]` | CRASH `TypeError` on read; job.run() counts the downstream row as failed |
| MergeJoin | any colliding column with a None (even int) | `list[T \| None]` | CRASH in MergeJoin itself: `'<' not supported between NoneType` (separate bug) |

Out-of-scope findings met on the way:
- MergeJoin cannot merge a colliding column holding a None (sort of None), any type.
- A function after GroupBy/Batch whose output key equals an input data column name: its
  `as_source()` raises `ArrowNotImplementedError: Unsupported cast from
  large_list<item: large_string> to large_utf8` (source-info column list vs scalar).
- A job with a `list[Path]` output loaded in a fresh process before anything registers
  `list[Path]`: "Cannot restore the output schema ... list[orcapod.path]" and
  `as_source()` returns `['/tmp/a']` (str, not Path): a wrong value, no None involved.

Rule found: a None at a NESTED position (list or set element, dict value, user-class
field) is restored only when the position's type is a primitive (int, float, str, bool,
bytes, datetime, date, Literal) or a user class (dataclass, orcapod.dataclass, pydantic).
A container (list, set, dict, and on PR #393 the set logical type) reads as empty, silently,
with a different hash; any other logical type (UUID, Path, UPath, numpy, torch, pandas,
polars) crashes. A top-level None is always restored.

## Proposal (sent to the coordinator)

- (a) Refuse at type level in `register_python_class` (every declared type: function
  inputs and outputs, `DictSource` data schema, user-class fields): `X | None` at a nested
  position where X is a container or a logical type other than a user class. The caller
  that knows the column or field adds its name.
- (c) Read backstop in the nested decoders (`UniversalTypeConverter.storage_to_python`,
  and the list element, dict value and struct field loops of
  `_create_arrow_to_python_converter`): a null at a container or non-user-class logical
  position raises instead of returning an empty value or a `TypeError`.
- Not (b): writes are faithful.

## Plan

(after the coordinator's reply)
