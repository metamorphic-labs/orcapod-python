# ITL-1374: `Filter`, a general row filter that is part of the pipeline

Status: approved: decisions 1 to 13 and 15, developer, 2026-11-22 (1 and 15 confirmed as
built the same day).

**Changed in revision 2 (2026-11-22), folding in findings F07-F13:**

- 2.1: `Filter` takes tag and data columns alike, stated explicitly (F10); how a user finds
  a struct's field names (F09).
- 2.2 (new 2.6): the literal rule. A literal is checked against the column's type and
  accepted when it is exactly representable (`7` against `uint32`); no conversion between
  Python kinds (`7` against a `double` column is still refused). What enters the identity
  is the literal as written (F11). New decision 15.
- 4: `Filter` is eligible for the job-speed table transport (F13).
- 5: conditions are checked when built, so nothing is refused later by the job (F07); the
  node-naming gap of streams built before a job is out of scope. `Filter` and
  `RestrictedAggregationWarning` (F12). Pushdown rewritten (F08): what a version 1 without
  pushdown costs on a large table, the smallest safe pushdown, and `explain()`, which
  reports what the source applies and what is applied after reading.
- 10: decision 9 rewritten (pushdown and `explain()`); decision 15 added.

## In one paragraph

`stream.filter(...)` keeps the rows that satisfy a condition. It takes exactly what
`source.where(...)` takes (column equalities as keywords or a mapping), plus a small set of
condition objects for everything else: `col("t") >= 5`, `col("unit").is_in([3, 7])`,
`col("t").between(1.0, 2.5)`, `col("rate").is_null()`, combined with `&`, `|` and `~`.
The difference from `where` is the one the developer named: `filter` is a step of the
pipeline. It is an operator with its own identity, applies to any stream, and enters every
hash downstream; `where` chooses which rows a source reads and enters no hash. Conditions are
one class, `orcapod.types.Condition`, used by both. `Filter` computes an Arrow mask and takes
the rows (`Table.filter`), so every value passes through bit for bit, with no Polars round
trip. Its identity is a canonical, order-free form of the condition, with typed values and no
expression text. `PolarsFilter` stays as the escape hatch for anything `Filter` cannot say.

## 1. The user's view, by example

A small synthetic stream, used in every example below:

```python
import pyarrow as pa
import orcapod as op
from orcapod import col          # proposed; see decision 2

table = pa.table({
    "session": ["s1", "s1", "s1", "s2", "s2", "s3"],             # tag
    "unit":    [3, 7, 9, 3, 7, 3],                                # tag (int64)
    "t":       [0.5, 1.2, 2.0, 0.7, float("nan"), 3.1],           # data (double)
    "rate":    [4.0, None, 2.5, 6.1, 0.0, None],                  # data (double | None)
    "probe":   ["imec0", "imec1", "imec0", "imec1", "imec0", "imec1"],  # data
})
units = op.sources.ArrowTableSource(table, tag_columns=["session", "unit"],
                                    tag_only_columns=True)
```

| What | Proposed spelling | Rows kept (session, unit) |
|---|---|---|
| one equality | `units.filter(session="s1")` | (s1,3) (s1,7) (s1,9) |
| several equalities (AND) | `units.filter(session="s1", probe="imec0")` | (s1,3) (s1,9) |
| mapping form | `units.filter({"session": "s1", "probe": "imec0"})` | same |
| membership | `units.filter(col("unit").is_in([3, 7]))` | every unit 3 or 7 |
| comparison | `units.filter(col("t") >= 1.0)` | (s1,7) (s1,9) (s2,7: NaN, see 2.4) (s3,3) |
| range | `units.filter(col("t").between(0.5, 2.0))` | (s1,3) (s1,7) (s1,9) (s2,3) |
| null test | `units.filter(col("rate").is_null())` | (s1,7) (s3,3) |
| not null | `units.filter(col("rate").is_not_null())` | the other four |
| negation | `units.filter(~col("unit").is_in([3, 7]))` | (s1,9) |
| or | `units.filter((col("session") == "s3") \| (col("t") < 0.6))` | (s1,3) (s3,3) |
| mixed | `units.filter(col("t") >= 1.0, session="s1")` | (s1,7) (s1,9) |
| struct field (F02) | `units.filter(col("info").field("kind") != "noise")` | rows whose `info.kind` is not `"noise"` (and not null) |
| column against column | `units.filter(col("t_start") < col("t_stop"))` | rows where the two same-typed columns compare so |
| string match (tier 2) | `units.filter(col("probe").str.starts_with("imec"))` | every row above |

(The last three rows use columns the small table does not have: a struct column `info`
with a field `kind`, two `double` columns `t_start` and `t_stop`.)

Rules of the spelling:

- Keywords and the mapping are **equality only**, exactly as `where` takes them today. Every
  positional condition object and every keyword must hold (AND). A condition object built
  beforehand is a value like any other: `knockouts = col("genotype") == "knockout"`, then
  `units.filter(knockouts)` and, later, `source.where(knockouts)`.
- Anything other than equality is a condition object built from `col(name)`. Its operators
  are those of Polars' `pl.col`, by design, so the forms read as users already write them and
  a later translation from Polars (section 7) maps one to one.
- `filter` and `where` take the same arguments: `filter(*conditions, **columns)` where a
  positional argument is a `Condition`, a condition object, or a mapping (equalities).
  `stream.filter` also takes `label=` as every stream method does, so a column named `label`
  is written `filter({"label": "x"})` or `filter(col("label") == "x")`.

Findings from exploring a real dataset (ITL-1373) that shaped this section:

- F01: `where` takes scalar equality only, and only the SpiralDB source has it; everything
  else is done with `polars_filter` after loading. Users want one condition language for
  `where` on every source and for `filter`. This design gives that language (`Condition`),
  used by `filter` everywhere in version 1; `where` on more sources and with more terms is
  the follow-up (ITL-1219).
- F02: `polars_filter` accepts more outside a job than inside one (a struct-field condition,
  an aggregate), so a selection made while exploring cannot move into the pipeline. `Filter`
  has one accepted set: a condition is checked and given its canonical form when it is
  built, so whatever builds also runs inside a job. Struct-field access is in version 1
  (below), because "a field of a struct column differs from a label" is the selection met
  in practice. Aggregates (`col("t") > col("t").mean()`) are not row conditions: a row's fate
  would depend on other rows. They are refused, never approximated.
- F04: `polars_filter([True, False, ...])`, a positional boolean mask, is accepted inside a
  job and keeps rows by position, although row order is not a result. `Filter` takes no mask:
  a list, array or bool given as a condition is refused (`TypeError`). The `PolarsFilter`
  behaviour is a pre-existing bug, to be filed on its own.

Why this spelling, and not the others weighed:

- **Keywords for equality**: already the `where` and `polars_filter` spelling, and the
  commonest exploration step. Kept.
- **`unit=[3, 7]` for membership**: rejected. A list value is ambiguous the day a list or set
  column takes an equality condition (`set[int]` columns exist today), and it silently turns
  a typo (`unit=[3]`) into membership. "When in doubt, refuse": a list value is refused with
  a message showing `col("unit").is_in([3, 7])`.
- **Keyword helpers (`t=ge(5)`, `unit=one_of(3, 7)`)**: rejected. They cannot say OR across
  columns, so a second spelling would be needed anyway.
- **Tuples (`("t", ">=", 5)`)**: rejected. Easy to mistype, no checking until applied, and
  unlike anything users know. (It is, however, the saved config form; section 3.)
- **A condition object (`col("t") >= 5`)**: chosen. One spelling covers comparisons,
  membership, null tests and `& | ~`; it is checked when built; Polars users read it at once.

`where` and `filter`, side by side:

```python
# exploration: the source reads only these rows; no step is added, no hash moves
s1 = spiral_source.where(session="s1")
late = spiral_source.where(col("t") >= 1.0)        # version 1: refused by where (below)

# pipeline: the same selections as steps of the pipeline
s1 = source.filter(session="s1")
late = source.filter(col("t") >= 1.0)
```

What `where` needs to accept the new forms (a follow-up, not version 1): `Condition` grows
the new terms in version 1 (section 3), so `where(col("t") >= 1.0)` builds without error; the
SpiralDB source then refuses any term other than equality with `TypeError: where takes
equality conditions only today; filter the stream instead: source.filter(col("t") >= 1.0)`.
Pushing comparisons, membership and null tests into the SpiralDB scan is a follow-up issue
(it needs SpiralDB's semantics for NaN, signed zero and nulls checked against section 2.4,
since `where` uses the table system's equality).

## 2. Version 1 scope

### 2.1 What version 1 has

| Condition | Spelling | Column types |
|---|---|---|
| equality, inequality | `col(c) == v`, `col(c) != v`, keyword `c=v` | bool, int, float, str, bytes, date, datetime |
| membership | `col(c).is_in(values)` | same as equality |
| comparisons | `<`, `<=`, `>`, `>=` | int, float, str, bytes, date, datetime |
| range | `col(c).between(lo, hi)` (both ends included) | same as comparisons |
| null tests | `col(c).is_null()`, `col(c).is_not_null()` | every column type |
| combinators | `a & b`, `a \| b`, `~a`; several arguments are AND | |
| struct field | `col(c).field("f")`, `col(c).field("f").field("g")`, then any condition above | a struct column, or a user class column (its stored fields) |
| column against column | `col(a) == col(b)`, `<`, `<=`, `>`, `>=`, `!=` | two columns (or fields) of the same Arrow type, after large normalization |

Tier 2, in version 1 if time allows (decision 12), otherwise the first follow-up:
`col(c).str.starts_with(s)`, `.str.ends_with(s)`, `.str.contains(s)` (a literal substring,
never a regular expression: a regex dialect would enter the identity), on string columns.

A struct field is a path: `col("info").field("kind")` names the child `kind` of the struct
column `info`. A null struct gives a null field, so the condition is unknown (2.3) and the
row is dropped. The field's type is read from the column's Arrow type when the filter is
applied (a user class column through its storage struct, whose fields are its declared
fields). There is no dotted-string shortcut (`"info.kind"`): a column name may contain a dot.

Why struct fields are in version 1 (F05): keeping the rows whose struct column has a field
equal to a value is impossible inside a job by every route today (`polars_filter` refuses
the struct expression in a job; `pick` refuses a plain Arrow struct; a function pod cannot
declare a parameter for a plain struct column). Real data keeps labels and metrics in struct
columns, so this is the selection people need first. Its cost is small:

- identity: the path is data in the term (`("field", ("col", "info"), "kind")`, section 3.2);
- execution: `pc.struct_field` on the Arrow column (by child name, resolved to its index
  from the column's type), then the same term kernel; no Python values;
- nulls: a null struct, or a null field, makes the condition unknown, and the row is dropped;
  `col("info").field("kind").is_null()` keeps both cases;
- reach: a struct inside a struct, to any depth, yes; list elements and map entries, no
  (version 2: `col(c).list...`); a field holding a logical type follows the logical-type rules
  of 2.2 (null tests only).

Two related gaps F05 found are not `Filter`'s job, and are noted for separate issues: `Pick`
on a plain Arrow struct column, and a function-pod annotation that accepts a plain struct
column.

Columns: the stream's user tag and data columns (data-tags included), **alike** (F10).
Whether a column is a tag or data decides what `Join`, `SemiJoin` and `GroupBy` can do with
it; it decides nothing for `Filter`: every condition kind works on a tag, a data column and a
data-tag, with the same semantics. System-tag, source-info, context and meta columns are not
columns a condition names.

Finding a struct's field names (F09): `output_schema()` shows a struct column as a generated
class name, which hides its fields. Version 1 makes `col(c).field(f)` refuse an unknown
field with a message listing the fields the column has (with their types), and the docs
show `stream.as_table().schema` (or the preview's column types) as the way to see them.
Showing struct fields in `output_schema()` and the preview is a separate issue.

The equality column types are those `where` supports today (`_condition_value_type` in
`spiraldb_table_source.py`); that function moves to one shared module so `where` and
`filter` check values by one rule (section 9, piece A).

### 2.2 What version 1 refuses, and how

Checked when the condition is built (`TypeError` / `ValueError`, as `Condition.of` does now):

- a value of no supported Python type, exactly (a NumPy scalar, a `Decimal`, a subclass):
  `TypeError`, "convert it explicitly (a NumPy scalar with .item())";
- `None` as a value: `ValueError`, pointing to `col(c).is_null()`;
- a list, tuple or set as a keyword value: `TypeError`, pointing to `col(c).is_in([...])`;
- `is_in` with no value, or with `None` among them: `ValueError`;
- `between(lo, hi)` with `lo` and `hi` of different types, or `lo > hi`: `TypeError` /
  `ValueError`;
- one column named twice with different values within one call (a mapping and a keyword,
  or two mappings: `filter({"x": 1}, x=5)`): `ValueError` saying the column is named twice
  in one call, as `where` does. That is an error in the arguments, not a contradiction;
- a contradiction between conditions is NOT refused (developer, 2026-11-22: "how is it
  that you could figure out if the condition is contradictory? If there is no
  straightforward way to achieve, then the contradictory case should just be taken as is
  and result in 0 rows."). There is no general way to tell one (`x == 1` with `x > 5`,
  disjoint memberships, empty ranges, null tests), so `(col("x") == 1) & (col("x") == 5)`,
  the same in several positional conditions, and the same in a saved tree, build and load
  as written, keep both terms in their canonical form, and a filter over them keeps zero
  rows. Equal duplicate terms still collapse (`a & a` is `a`). A source's `where` still
  refuses two equalities on one column: such a condition is not equalities only, and a
  restriction reads one value per column;
- `filter()` with no condition: `ValueError` (a filter that keeps everything is not a step);
- a list, NumPy array, Polars series or `bool` given as a positional condition (a mask by
  position, F04): `TypeError`;
- a Polars or Arrow expression given as a condition: `TypeError`, pointing to `col(...)` and
  to `polars_filter` (until stretch 1 translates them);
- an aggregate or anything that reads other rows: not expressible (`col` has no such method).
- Python's `and` between two conditions cannot be refused (a condition that requires
  something is true, so `a and b` is `b`): combine with `&`.
- a column reference used as a truth value (`col("t") and col("u")`): `TypeError`,
  pointing to `&` (Polars does the same). Implementation note (piece A): a `Condition`'s
  own truth value stays "requires anything" (`Condition()` is false), as `where` and
  existing code rely on it, so `(col("t") > 1) and (col("t") < 2)` cannot be refused: it
  evaluates to the second condition. The docs must show `&`.

Checked when the filter is applied to a stream (`InputValidationError`, naming the column):

- a column the stream does not have (listing the user columns it has);
- a value not exactly of the column's Python type: `1` for a `float` column, `True` for an
  `int` column, `"2026-01-01"` for a `date` column. No conversion, as `where` today;
- an integer outside the column's Arrow range (`300` for `int8`); a finite float beyond a
  `float32` column's range; a naive `datetime` for a zoned timestamp column or the reverse;
- `field("f")` on a column that is not a struct (or a user class column), or a field the
  struct does not have;
- two columns compared whose Arrow types differ (`int32` against `int64`, `double` against
  `int64`): no promotion, as everywhere in Orcapod;
- a comparison on a type with no order in version 1 (bool, list, struct, map, logical type);
  equality or membership on a type equality does not support (list, struct, map, decimal,
  logical types). Null tests are accepted on every type.

The Python-type checks run at graph build, from `output_schema()`; the Arrow-level checks
(range, zone) run when the filter is applied, from the input table's Arrow schema, so
validation never derives a zero-row table (the rule `Join` follows).

### 2.3 Nulls: SQL three-valued logic (Kleene)

A comparison with a null is unknown; `&`, `|`, `~` follow Kleene logic
(`null & False = False`, `null | True = True`, `~null = null`); a row whose condition is
unknown is dropped. This is what SQL, Polars' `filter` and Arrow's `and_kleene` /
`or_kleene` with `filter(null_selection_behavior="drop")` all do, so one rule holds
everywhere. Consequences users must be shown in the docs:

- `col("rate") != 4.0` drops the rows where `rate` is null;
- `~(col("rate") > 1.0)` also drops them;
- to keep them, say so: `(col("rate") <= 1.0) | col("rate").is_null()`.

`where` today: a null never equals a value, and `None` is refused as a value. `filter`
behaves the same for equality.

### 2.4 Floats: Orcapod's matching rule (Polars-like), not IEEE

Recommendation: equality and membership match as Orcapod matches everywhere (ITL-1251):
`0.0` equals `-0.0`, NaN equals NaN. Comparisons use Polars' total order: NaN is greater than
every number (including `inf`) and equal to itself, the order `GroupBy` already uses. So
`col("t") >= 1.0` keeps the NaN row, `col("t") == float("nan")` selects NaN rows, and
`col("t") < 1.0` never keeps a NaN.

Alternative: IEEE (every comparison with NaN false, `!=` true). Simpler to state, but it
makes `filter(t=float("nan"))` select nothing, differs from `Join`, `GroupBy` and Polars,
and `polars_filter` users would see different rows for the same expression.

`where` today uses the table system's equality (documented): for a NaN or signed zero the
same condition may select different rows on `where` and on `filter`. This is the one place
the two differ in result, and the reason pushdown is not free (section 5).

### 2.5 Strings, bytes, dates and times

- Strings and bytes compare by byte order of their UTF-8 / raw bytes (Arrow's order; for
  ASCII, ISO dates and zero-padded ids this is the expected order). No collation.
- A `datetime` value against a zoned column compares instants; zones of the value do not
  matter. A naive value is accepted only for a naive column. `date` and `datetime` are
  distinct types (a `datetime` is refused for a `date` column even though it is a subclass).

### 2.6 Literals: checked against the column, never converted between kinds (F11)

A Python literal has no Arrow type of its own; the column gives it one. The rule:

- The literal's Python type must be exactly the column's Python type (`int` for every
  integer width, signed or not; `float` for `float16`/`float32`/`double`; `str`, `bytes`,
  `bool`, `date`, `datetime`). `7` against a `double` column, `True` against an `int`
  column and `"7"` against an `int` column are refused. This is `where`'s rule today.
- Within that kind, the literal is accepted when the column's Arrow type holds it exactly:
  `7` against `uint32` or `int8` is accepted, `-1` against `uint32` and `300` against `int8`
  are refused (`InputValidationError`, naming the column, its type and the value). So
  `is_in([3, 7])` on a `uint32` column works, which `semi_join` against an `int64` key
  column does not (there two columns meet, and columns never convert).
- A float literal against a `float32` column is rounded to the column's precision, as a
  float column stores any value and as `where` does today; a finite literal beyond the
  column's range is refused. Comparisons are made in the column's type, after that rounding,
  and the docs say so (`col("x32") <= 0.1` keeps a stored `0.1f`).
- Timestamps: the literal is compared at the column's unit; a literal with a part below the
  column's unit (a microsecond against a `timestamp[s]` column) is refused, never truncated.

What enters the identity is the literal as written, with its Python type token (section
3.2), never the column's Arrow type: the operator's identity does not depend on its input,
whose schema is already in the upstream's pipeline hash. So `col("unit") == 7` is one
operator whether `unit` is `uint32` or `int64`.

## 3. Identity

### 3.1 One class: `Condition` grows a tree

`Condition` stays the one class (ITL-1218 decided "never add a second restriction class").
Its interface for today's equality use does not change (`of`, `combined`, `equalities`,
`to_config`, equality and hash). Internally it becomes a small tree:

- a term `(column, op, value)`, `op` in `==`, `!=`, `<`, `<=`, `>`, `>=`, `is_in`,
  `is_null`, `is_not_null`;
- `All(children)`, `Any(children)`, `Not(child)`; the top level of a condition is `All`.

`col(name)` returns a column reference whose operators build `Condition`s, so
`col("t") >= 1.0` is a `Condition` and `&`, `|`, `~` are methods of `Condition`.
`equalities` stays (the equality terms of a pure-equality condition), and a new
`only_equalities` property lets `where` refuse the rest.

### 3.2 The canonical form

The operator's identity is `("Filter", <canonical condition>)`, hashed by `semantic_v1`
inside the operator's identity structure like any other operator's arguments. The canonical
form is plain tagged data (tuples of strings), never Polars or Arrow expression text:

- a term: `("term", ref, op, value)`; `is_null` / `is_not_null` carry no value;
- a reference: `("col", name)`, a struct field `("field", <reference>, field name)`, so
  `col("info").field("kind")` is `("field", ("col", "info"), "kind")`;
- column against column: the value is the other reference, `("ref", <reference>)`;
  `col(a) < col(b)` and `col(b) > col(a)` are two identities (no reordering of operands);
- a value carries its type: `("int", "5")`, `("float", "<float.hex()>")`, `("bool", "true")`,
  `("str", s)`, `("bytes", "<hex>")`, `("date", "2026-11-25")`,
  `("datetime_aware", "<UTC instant in microseconds>")`, `("datetime_naive", "<ISO wall time>")`.
  So `1`, `1.0` and `True` are three values, as `Condition` equality already says;
- floats are normalized by the float rule before encoding: `-0.0` as `0.0`, every NaN as one
  NaN, because they select the same rows (section 2.4);
- `is_in(values)`: the values deduplicated (by the same equality) and sorted by their
  encoded form: `("term", c, "is_in", (v1, v2, ...))`;
- `between(lo, hi)` is written as the two terms `>= lo` and `<= hi` (no separate form);
- `All` / `Any`: children flattened (`a & (b & c)` is `All(a, b, c)`), deduplicated, sorted by
  their encoded form: `("all", (...))`, `("any", (...))`; `Not` is `("not", child)`;
- keywords and mapping entries are equality terms in the top-level `All`.

So keyword order, argument order, the order of `&` / `|` operands and the order of an
`is_in` list never change the identity (the order rule, ITL-1271). Nothing else is
canonicalized: `col("t") != 1.0` and `~(col("t") == 1.0)` select the same rows but are two
identities, as two differently written `PolarsFilter`s are. Fewest special cases; the
identity is the condition as written, in a canonical form.

No hash label of its own: the condition is not hashed apart from the operator and fed in as
a digest; it is plain structure inside the operator's identity, which `semantic_v1` labels.
(ITL-1265 asked for "its own hash label"; under the hash label rule a label names an
algorithm that produces a digest, and this design produces none. Decision 6.)

The saved config is the same tree in JSON with values written as `Condition.to_config`
writes them today (`{"date": ...}`, `{"float": "nan"}`), so a saved job reloads the filter
exactly. A pure-equality condition keeps today's saved `where` mapping.

### 3.3 Why it can be frozen, and why ship it provisional anyway

The form above has no dependency on any library's text or version, every value is typed,
and every order is removed by construction: it is as stable as `DropNulls`' sorted names.
It could be frozen in 0.4.0.

Recommendation: ship `Filter` **provisional** in the release the workshop pins (listed in
`PROVISIONAL_OPERATORS`, `PROVISIONAL = True`, golden entries in the provisional files),
and freeze it in the release after the workshop. Reason: the workshop is its first use, and
the spelling may change what the canonical form must hold (for example, if users need
`is_in` on a list column or `between` with open ends). The cost: every process that builds a
`Filter` sees one `ProvisionalHashWarning`, which the workshop notebook would show (it can
silence it with `warnings.filterwarnings`, or the docs can say why it is there).

Freezing at 0.4.0 means: its golden entries go into a frozen file and never change; any later
change to the canonical form (a new op is fine; a new encoding of an existing one is not)
needs a new data context version.

## 4. Execution

- **One kernel**, `Filter._mask(table) -> pa.BooleanArray`, used by every path: each term
  is an Arrow compute call on its column with a typed scalar (`pa.scalar(v, type=column
  type)`, after the checks of 2.2, so nothing is converted); NaN-aware equality and order
  (2.4) are two extra `is_nan` terms per float comparison; `All`/`Any`/`Not` are `and_kleene`,
  `or_kleene`, `invert`. Then `table.filter(mask, null_selection_behavior="drop")`.
- Every column passes through as it is: values bit for bit (a filter is a `take`), types,
  nullability, system tags (names and values: evolution rule 1, name-preserving), source
  info, row order. Meta (`__`) columns are dropped by the operator base classes, as for every
  operator (ITL-1080).
- **Schema**: the input's. `OUTPUT_SCHEMA_IS_INPUT_SCHEMA = True`, so `head()` and the preview
  never apply it to zero rows. `is_not_null` does not narrow `T | None` to `T` (decision 5):
  narrowing is `DropNulls`' job and stays there, and narrowing under `|` or `~` would need
  rules of its own. Docs say: to narrow, `drop_nulls("rate")`.
- **Zero rows**: an empty input or no matching row gives a typed zero-row table with the
  input's columns; never an error.
- **Sync and async**: the async path applies the same kernel to each row's one-row table
  (built from the datagrams' Arrow tables, as `DropNulls` does with `WithoutMetaWriter`); on
  `eywalker/job-speed`, `ROW_BY_ROW = True`, so it is applied per record batch. The filter is
  row-independent, so both give the rows of the sync path in the same order. Added to
  `test_async_sync_equivalence.py`.
- **Job transport (F13)**: on the released version a `polars_filter` step roughly doubles
  `job.run()`, warm runs included (measured on 60k rows: 6.6 s to 14.6 s), because a job node
  rebuilds its rows one by one. The job table transport on `eywalker/job-speed` removes that
  cost for operator nodes whose result is a plain Arrow table. `Filter` must be eligible for
  that fast path: its result is an `ArrowTableStream` built by `Table.filter`, and it
  declares `ROW_BY_ROW = True`. A test on that branch asserts it takes the fast path.
- **Cost**: one pass per term over its column, O(rows × terms), no copy of untouched columns
  beyond Arrow's `filter`. Far cheaper than `PolarsFilter`, which converts the whole table to
  Polars and back and then restores types.

## 5. Part of the pipeline

- `source.filter(...)` is an operator step: in a pipeline it becomes an `OperatorNode` like
  `DropNulls`. Its pipeline hash commits to `("Filter", <canonical condition>)` and its
  upstream's pipeline hash; its content hash to the same plus the upstream's content hash.
  Changing a condition moves every hash downstream, as it should: other rows flow.
- In `LOG` mode the node stores its output rows in the operator cache (`odb_v1`) under its
  own path, as any operator node. The existing refusal applies unchanged: an operator node
  that caches (`LOG` or `REPLAY`) over a restricted source is refused
  (`refuse_restricted_operator_cache`); with `CacheMode.OFF` it runs.
- **Checked when built (F07)**: a `Condition` is checked and given its canonical form when it
  is built, and the operator's schema checks run at graph build; nothing a `Filter` accepts
  outside a job is refused when a job records it. (F07's other part, that a stream built
  before a job gets nodes named `<label>:<hash>` and its source is not listed in
  `job.nodes`, is a job-capture gap outside `Filter`'s scope, for its own issue.)
- **`RestrictedAggregationWarning` (F12)**: a `where`-restricted source feeding `GroupBy`,
  `Batch` or `MergeJoin` warns at every run, because the restriction is in no identity: the
  aggregate's hash does not say which rows were aggregated. A `Filter` step does not
  trigger it by itself: its condition is in the identity of every node downstream, so a
  filter over an unrestricted source lets the aggregate's hash say exactly which rows it
  saw. A filter does not clear it either: when a `where`-restricted source stays upstream
  of the `Filter`, the restriction is still in no identity (two restrictions to different
  sessions, filtered alike, give one downstream identity over different rows), so the
  warning still applies. This is a reason to move a selection from `where` into `filter`
  once it becomes part of what a pipeline computes. A pushdown (below) is internal to the
  `Filter` and must not trigger the warning: the source it reads is not restricted by the
  user, and the `Filter` still evaluates its whole condition.
- **`filter` directly over a source versus `where`**: the same rows (except NaN and signed
  zero on float columns, 2.4), different identities: `where` leaves the source's identity
  unchanged and adds no node; `filter` adds a node. A pipeline that started from
  `source.where(session="s1")` and moves to `source.filter(session="s1")` gets new hashes for
  every node downstream. That is the intended difference.
- **`head()` and the preview** (F03): today `head()` of any operator over rows it cannot
  show reads "not computed", so a filtered stream shows nothing while `source.head(5)` shows
  rows. A `Filter` holds no user code: it is a declarative condition evaluated by Arrow, with
  no effect and no write. Proposal (decision 13): `head(n)` of a `Filter` may evaluate the
  condition over the rows its upstream's `head` would show (what is held or stored, or the
  bounded read a source's `head` already does) and show the matches, labelled as "matching
  rows among the first N read"; it may show fewer than `n`, or none. It never computes the
  upstream (a function pod upstream still reads "not computed"), never polls, never
  writes. Pushing the condition into a source's bounded read (so the first `n` matching
  rows of a SpiralDB table are shown) is the pushdown below, and comes with it.
- **Pushdown (F08)**. Today nothing tells a user whether a condition runs in the table
  system or after a full read: `polars_filter(pl.col("unit") == 7)` on a large unrestricted
  table reads every row (and can hit a SpiralDB source's `max_rows` cap), where
  `where(unit=7)` reads few.

  *Cost of version 1 without pushdown*: a `filter` over a SpiralDB source reads the whole
  table into memory, then filters. Time and memory grow with the table, not with the
  result; a table above the source's `max_rows` cannot be filtered at all. The workaround,
  `where` on the source plus the same `filter` in the pipeline, is correct but makes the
  source restricted, with its rules (`RestrictedAggregationWarning`, no caching operator node
  over it).

  *The smallest safe pushdown* rests on one invariant: **the source may return more rows
  than match, never fewer, and the `Filter` always evaluates its whole condition on what is
  read.** Then the result is the same rows whatever the table system's semantics, and no
  hash moves (the `Filter`'s identity is unchanged, and a source's restriction is in no
  identity). Under that invariant, version 1 can push:
  - the top-level equality terms (`==`, keywords) of a `Filter` placed directly on a
    `SpiralDBTableSource` (the only source with a scan condition today),
  - on columns whose equality the table system shares with Orcapod: integer, string, bytes,
    bool, date and timestamp columns; never a float column (NaN, signed zero), a struct
    field, or a logical type;
  - only when the source has no `limit` (a limit after a pushed condition chooses other
    rows), and never into a `CachedSource` or a source over another.
  `is_in` and comparisons follow when SpiralDB's scan takes them (piece E). Record ids are
  unchanged by a restriction (a positional id is never renumbered), so the rows read carry
  the ids a full read gives them.

  *`explain()`*: `stream.explain()` (and one line of the preview of a `Filter`) reports the
  split, for example `applied by the source: unit == 7; after reading: t >= 1.0` or
  `applied by the source: nothing; after reading: unit == 7, t >= 1.0 (the whole table is
  read)`. It is in version 1 even without pushdown, so the cost is visible.

  Recommendation (decision 9): `explain()` in version 1; the equality pushdown above in
  version 1 if piece E is done by Tuesday, otherwise the first follow-up.

## 6. Relation to `PolarsFilter` and `DropNulls`

- `DropNulls` stays: it is the operator that narrows types. `filter(col(c).is_not_null())`
  keeps the same rows but does not narrow.
- `PolarsFilter` stays, provisional, as the documented escape hatch for what `Filter`
  cannot say (arithmetic across columns, string functions, a list element). Its keyword form
  (`polars_filter(genotype="knockout")`) is replaced in docs and tutorials by `filter`.
- Docs that change (every sample tested): `docs/tutorial/03-operators.md` (the filtering
  section moves to `filter`; `polars_filter` keeps one example as the escape hatch),
  `docs/concepts/operators.md` (a `Filter` section beside `DropNulls`; the `PolarsFilter`
  section says when to use it), `docs/concepts/sources.md` (`where` and `filter` side by
  side), `docs/tutorial/04`, `07`, `08`, `10`, `README.md` and `docs/troubleshooting.md` where
  they use `polars_filter` for equality, `docs/api/operators.md` (`::: Filter`, `::: col`),
  `docs/reference/hashing.md` (hash site inventory, if the canonical form is a new site),
  `docs/concepts/identity.md` (order rules).
- `Filter` does not add sorting, counting, distinct values or summaries (F06: no stream
  method does). The documented route for exploration summaries stays `as_polars_df()` /
  `as_lazy_frame()`; the docs' exploration page should say so beside `filter`.
- Whether `polars_filter` should later be built on `Filter` (translate what it can, section 7)
  is decision 8.

## 7. Stretch goals and their cost

1. **Translate a Polars expression into a `Condition`** (recommended first stretch):
   `units.filter(pl.col("t") >= 1.0)` walks the expression tree (`expr.meta` and its JSON
   serialization), maps `col`, literals, the comparisons, `is_in`, `is_null`, `&`, `|`, `~`
   to the condition, and refuses anything else with a pointer to `polars_filter`. The
   identity is the canonical condition, so `pl.col("t") >= 1.0` and `col("t") >= 1.0` are one
   operator. Cost: about 2 days, plus upkeep: Polars' serialized form changes across versions,
   so the translator needs tests pinned to each supported Polars version. Literal types need
   care (`pl.lit(1)` is `i32` in Polars; the translator must keep the Python value's type).
2. **An Arrow compute `Expression`** (`pc.field("t") >= 1.0`): the same approach, translating
   a subset to a `Condition`. Hashing the expression itself would need a stable serialization
   (Substrait); `str()` is not stable. Cost: about 1–2 days, same upkeep concern.
3. **A Polars expression as the condition itself** (no translation): this is `PolarsFilter`;
   its identity rests on `polars_expr_v1` and Polars' expression text, which is why it is
   provisional. Not recommended as part of `Filter`.
4. Version-2 candidates from use: open-ended `between` (`closed=`), the tier-2 string
   matches if not in version 1, `is_nan`, comparisons on decimals and times, list
   membership (`col(c).list.contains(v)`), `is_in` against another stream (that is
   `SemiJoin`), arithmetic in a condition (`col("a") - col("b") > 5`, which reads values
   but synthesizes none; needs a typing rule for the arithmetic).
5. **`where` on every source** (F01): the in-memory, file and database sources accept a
   `Condition` as a restriction (in-memory: the same kernel as `Filter`, applied at read;
   databases: translated to SQL). ITL-1219 territory; not before the workshop.

## 8. Test plan

By CLAUDE.md's conventions, on the shared family `tests/test_core/operators/several_items.py`
(two or more tags and data columns, several arrangements):

- **Several items**: two and more keywords, two and more positional conditions, `is_in` with
  several values; every condition kind on a tag, a data column and a data-tag; each in more
  than one order (keywords, arguments, `&`/`|` operands, `is_in` values), asserting equal
  rows, content hash, pipeline hash and system-tag column names.
- **Semantics**: Kleene null logic for each combinator; NaN and signed zero for `==`, `!=`,
  `is_in`, each comparison; strings and bytes order; aware and naive datetimes; dates;
  integer widths (`int8` range refusal); every refusal in 2.2 with its error type and message.
- **Bit for bit**: values after filtering equal the input's (`float32` signaling NaN bits,
  `time64[ns]`, a `decimal256` data column passing through, a logical-type data column passing
  through); types and nullability unchanged; no meta columns.
- **Struct fields and column operands**: a field, a field of a field, a null struct, a null
  field, a user class column's field, a missing field, a non-struct column; two columns of
  equal and of different Arrow types; the same condition inside and outside a job (F02).
- **Literals** (2.6): `7` on `uint8`, `uint32`, `int64`; `-1` on `uint32` and `300` on `int8`
  refused; `7` on `double` refused; a float on `float32`; identity equal across widths.
- **Pushdown and `explain()`**: the `explain()` text for each case; with pushdown, equal rows,
  record ids and hashes against a full read, a float column and a `limit` never pushed, no
  `RestrictedAggregationWarning`.
- **No masks**: a list, array, series or bool as a condition is refused (F04).
- **head()** (if decision 13 is taken): a `Filter` over a source shows matches among the
  bounded read; over a function pod it still reads "not computed"; nothing is written.
- **Schema**: prediction equals `unary_static_process(stream).output_schema(...)`; zero-row
  input and zero matching rows.
- **Identity**: the canonical form for each kind (unit tests of the encoder); `1`, `1.0`,
  `True` differ; `-0.0` and `0.0` agree; config round trip through a saved job.
- **Order invariance**: a `Filter` case in `tests/test_core/test_order_invariance.py`.
- **Sync / async**: `test_async_sync_equivalence.py` entries, several conditions.
- **Provisional**: the operator in `PROVISIONAL_OPERATORS` and the consistency tests; golden
  cases declared `provisional=("Filter",)` in the provisional files (developer approves the
  new entries; adding entries is allowed, no value moves).
- **Condition / where**: today's `Condition` tests unchanged; `where` refuses non-equality
  terms; `Condition` equality and hash on trees.
- **Docs**: every new sample runs in `test_doc_samples.py`.
- **Changelog**: `changelog.d/ITL-1374.md`, `### Added`.

## 9. Implementation plan

Four pieces, three of them in parallel once piece A's interface (section 3.1) is fixed by
this document:

| Piece | Work | Needs | Estimate |
|---|---|---|---|
| A. `Condition` tree and `col` | The term kinds, references (column, struct field path, column operand), `All`/`Any`/`Not`, `col(name)` with its operators, construction checks, equality and hash on trees, the canonical encoder, config form; the shared value-type check moved out of the SpiralDB source; `where` refuses non-equality terms. `src/orcapod/types/condition.py`. | nothing | 1 day |
| B. `Filter` operator | The class, validation (Python types at build, Arrow at apply), the mask kernel with NaN-aware terms, struct-field paths and column-against-column terms, sync and async (and `ROW_BY_ROW` on job-speed), provisional registration, `stream.filter`, the protocol method, exports. | A's interface (can start on stubs) | 1–1.5 days |
| C. Tests | The matrix of section 8, order invariance, async-sync, golden provisional cases. | A, B | 1 day, overlapping B |
| D. Docs | Tutorial 03 and the others, concepts, API, changelog fragment. | B's spelling (frozen by this document) | 0.5–1 day |
| E. Pushdown and `explain()` | `explain()` and the preview line (version 1, with B); the equality pushdown into a SpiralDB scan under the invariant of section 5, with tests that rows, record ids and every hash equal those of a full read, and that no restriction rule or warning fires. | A, B | `explain()` 0.5 day; pushdown 1 day |

Honest total: about 3 working days of wall time with three workers, 4–5 for one. For
Wednesday's workshop: A and B by Monday evening, C and D on Tuesday, a release candidate
Tuesday night. A smaller workshop cut, if time runs short: equality, `is_in`, the
comparisons, `between`, `is_null`/`is_not_null` and AND only (no `|`, `~`), which removes
the tree from piece A.

## 10. Decisions for the developer

1. **Spelling.** CONFIRMED as built (developer, 2026-11-22). Keywords and mapping for equality (as `where`), `col(...)` condition objects
   for everything else. Alternative: keyword helpers (`t=ge(5)`), which cannot say OR.
2. **Name of the column reference.** `orcapod.col`, mirroring `pl.col`. Alternative:
   `orcapod.column` or `C`, to avoid confusion with Polars in one notebook (passing a
   `pl.col` expression to `filter` is refused with a pointer to `polars_filter` until
   stretch 1 exists).
3. **Membership.** `col(c).is_in([...])` only; a list value as a keyword is refused.
   Alternative: `unit=[3, 7]` as membership (convenient, ambiguous for list and set columns).
4. **Nulls and floats.** Kleene three-valued logic; floats by Orcapod's rule (`0.0 == -0.0`,
   NaN equals NaN, NaN greater than every number). Alternative: IEEE comparisons.
5. **`is_not_null` does not narrow `T | None`.** `DropNulls` narrows. Alternative: narrow for
   a top-level `is_not_null` term (a special case; loses `OUTPUT_SCHEMA_IS_INPUT_SCHEMA` for
   those filters).
6. **Identity.** `("Filter", canonical condition)` hashed by `semantic_v1`, typed values,
   sets sorted, no separate label. Alternative: hash the condition apart under its own label
   (`condition_v1`) and embed the `ContentHash` (one more label to define, no benefit today).
7. **Provisional.** Ship provisional in the release the workshop pins; freeze in the next one.
   Alternative: freeze at 0.4.0 (no warning in the workshop; no room to change the form).
8. **`PolarsFilter`.** Keep as the escape hatch, provisional; docs move equality filtering to
   `filter`. Alternative: rebuild `polars_filter` on stretch 1 (translate when possible), or
   deprecate it later.
9. **Pushdown and `explain()`** (F08). `explain()` reports what the source applies and what
   is applied after reading, in version 1. Push the top-level equality terms on integer,
   string, bytes, bool, date and timestamp columns of a `Filter` directly on a SpiralDB
   source with no `limit` into its scan, the `Filter` still evaluating everything (rows and
   hashes unchanged), in version 1 if time allows (piece E). Without it, a `filter` over a
   large table reads the whole table and fails above `max_rows`. Alternative: no pushdown
   in version 1, users combine `where` and `filter` on large tables.
10. **`where` and the new terms.** `Condition` holds them in version 1; `where` refuses
    non-equality terms with a pointer to `filter`; pushing them into SpiralDB is a follow-up
    issue. Alternative: leave `Condition` equality-only and give `Filter` its own condition
    class (contradicts "never add a second restriction class").
11. **Workshop cut.** If short on time, drop `|` and `~` from version 1 (AND only), and
    column-against-column. Keep struct fields (F02: the selection met in practice).
    Alternative: hold the full set and accept a later release candidate.
12. **Struct fields, column against column, string matches.** Struct fields and
    column-against-column in version 1; literal `starts_with` / `ends_with` / `contains` in
    version 1 if time allows. Alternative: all three as follow-ups.
13. **`head()` of a `Filter`.** Evaluate the declarative condition over what the upstream's
    `head` would show (bounded), labelled as such; amends the "head never computes" rule for
    this one declarative operator. Alternative: keep today's "not computed" (users call
    `run()` or `where` on the source to look).
14. **`PolarsFilter` positional masks** (F04): file a bug; refuse a positional mask inside
    a job (or everywhere). `Filter` never takes one.
15. **Literals** (F11, section 2.6). CONFIRMED as built (developer, 2026-11-22). A literal must be of the column's Python kind and is
    accepted when the column's Arrow type holds it exactly (`7` against `uint32`), a float
    rounded to a `float32` column as `where` does; the identity holds the literal as
    written. Alternative: refuse a literal unless it is of the column's exact Arrow width
    (the `semi_join` behaviour; strict, but Python has no `uint32` literal, so `is_in` on
    such a column would need a NumPy or Arrow value, which is refused).
