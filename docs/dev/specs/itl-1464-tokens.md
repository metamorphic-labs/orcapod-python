# Source-info tokens carried as a recipe (ITL-1464, track TOKENS)

Status: direction approved November 25, 2026. Step 1 (sources and row-wise operators)
implemented with this document; step 2 (joins) is a separate pull request.

## The cost

A source row's token is `<source id>::<record id>::<column>`, one per row and data
column: about 300 bytes each on the measured synthetic tables, 35 times the data
(`itl-1464-memory-diagnosis.md`). Everything in it is in the row's two system tags and
the column's name at the source.

## The representation

A `_source_x` column is **derived** while no one needs its text: a column of
`DerivedTokenType` (`src/orcapod/utils/source_tokens.py`), an internal Arrow extension
type. Its storage is a struct with no field, so a row has no byte, whatever the number of
rows. Its parameter is a recipe, `TokenRecipe(source_id, record_id, column)`: two
system-tag column names of the same table, and the column's name at the source.

- `filter`, `take`, `slice`, `concat_tables`, `select` and `rename_columns` carry the
  column with no special code, since it is one column like any other. The rows of the
  table and their system tags move together, so the recipe stays right.
- Two columns have one Arrow type only with one recipe (`__eq__`), so Arrow itself refuses
  to concatenate two that would read different columns.
- The system-tag names of a join change (`append_to_system_tags`): the recipes follow
  there (`rename_read_columns`). The values of the system tags move between columns only
  in `sort_system_tag_values` (inputs of one pipeline hash): the tokens that read them
  become text first (`materialize_reading`).
- Text is made in one function, `materialize`, at most 65,536 rows at a time.

A derived column is loud where it does not belong: reading a value, casting to text,
hashing, pickling, storing and writing Parquet each raise.

## Where a derived table can go

`core/streams/carried.py` holds the internal read, `carried_table(stream, columns)`: the
table of `as_table` with derived columns left as they are. It is a private library function,
no part of a stream's interface. Both ends are exact:

- the stream: its own class declares `_carried_table` (the library's `ArrowTableStream`,
  `DynamicPodStream` and source kinds), and the object reads as that class
  (`reads_as_its_class`). A subclass, a replaced `as_table` and a user's stream are read
  through `as_table`, and give text;
- the reader: an operator whose own class declares `CARRIES_TOKENS`, through
  `StaticOutputOperatorPod._input_table`. A user's operator, also a subclass of a library
  one, reads text.

`as_table` with any columns, `iter_data`, `head`, `run`, the data frame methods and the
identity table give text, as before, and so does a read without the system tags (the recipe
would have nothing to read). The Arrow hashers (schemas, and tables without rows too) and
every database's `add_records` refuse a derived column by name (`refuse_derived`).

## Producers of `_source_` columns

| Producer | Form |
|---|---|
| `SourceStreamBuilder.attach_provenance` (every source's rows) | derived |
| `ArrowTableStream(source_info=...)` and a raw table's own `_source_` columns | as given (text) |
| `prepare_prefixed_columns` for a data column with no token | null text |
| `CachedSource`, `DerivedSource` over stored records | text, read from storage |
| `DerivedSource(tags_as_data=)` | derived (a new root, through the builder) |
| `PollingSource` | derived per batch; its combined stream is text (its batches are combined through `as_table`) |
| A function pod's outputs (`data_function.py`) | text: an output's token names the computation, not a source row |
| `Pick` (`with_source_info`) | text: the token is extended |
| `Index` | per row, text (the element's token) |
| `Batch`, `GroupBy` | text lists, one element per member |
| `Join`, `MergeJoin` merged or colliding columns | text |

## Consumers

| Consumer | What it needs | Step 1 |
|---|---|---|
| Column selections, `MapData`, `MapTags`, `MapColumns`, `TagBy`, `AddTags`, `DropNulls`, `Filter` | none of the values | derived in, derived out |
| `PolarsFilter` | none, unless a predicate reads a token | the columns stay out of the Polars frame and are put back for the kept rows; a predicate that could read one (a selector, a regular expression, a non-expression, a name that is no column) gets text, as before |
| `without_input_meta` | names only | derived |
| `SemiJoin` (left input) | none | step 2 |
| `Join`, `Rejoin`, `MergeJoin`: columns of one input | none | step 2 |
| `Join`: a shared data-tag | both tokens, folded (`join_v1`) | step 2: a fold recipe; until then text |
| `MergeJoin`: a colliding column | both tokens, in a list | text of that column |
| `GroupBy`, `Batch` | the members' tokens, in a list | text (below) |
| `iter_data` (rows: function pods, side effects, async rows) | one string per column of each row | built for 8,192 rows at a time |
| `F` (`_row_built_form`, `row_built_provenance`) | types and null flags | passes a derived column through (a token is one string and never null) |
| Job nodes (`core/nodes/*`) | the stored text; the kept table | text until the transport calls `carried_table` |
| Function node pipeline table, result table, operator and source caches | stored text | unchanged: they read rows or `as_table` |
| Record-batch channels, streaming joins | types, values | unchanged (text) |
| Provenance trace (`pipeline/provenance.py`) | text | unchanged |
| Identity of a raw stream (`_identity_table`), record hashes | text | unchanged |

## After each operator

| Operator | Derivable after it? |
|---|---|
| filter, drop nulls, select, drop, rename, add tags, tag by | yes, all columns |
| semi join | yes, all left columns |
| join, rejoin | yes: each input's columns through its renamed system tags; a shared data-tag through a fold of two recipes. Not for inputs of one pipeline hash in `Join`, whose system-tag values are sorted per row |
| merge join | yes for columns only one input has; a colliding column is a list of two tokens |
| group by, batch | no: the system tags are folded to one digest, the members' ids are gone. The lists must be text (or carry the members' ids, a later step) |
| function pod | an output: no. Inputs' tokens are not in its output |

## Aggregations

`GroupBy` and `Batch` keep one token per member and column, so their output needs the
members' record ids: text lists are about 300 bytes per member and column. A derived list
(the members' record ids once, in one hidden list column, and recipes over it) is possible
but adds a column operators would have to carry; it is not done here.

## Measured

A synthetic time series table, about 2.2 million rows, 13 columns, about 250 MiB of Arrow data, read through
`SpiralDBTableSource` (`measure.py` of the diagnosis, one process per step).

| Step | Peak RSS before | after | Arrow pool peak before | after |
|---|---|---|---|---|
| `as_table()`, `run()` | 10.8 GiB | 2.0 GiB | 9.3 GiB | 0.75 GiB |
| `map_data` | 10.8 GiB | 2.0 GiB | 9.3 GiB | 0.75 GiB |
| `drop_nulls` | 19.7 GiB | 2.4 GiB | 18.6 GiB | 1.7 GiB |
| `polars_filter` | 16.6 GiB | 2.5 GiB | 9.3 GiB | 0.75 GiB |
| `join` (8 and 14 columns) | above 24 GiB | above 24 GiB (step 2) | | |
| `group_by` | 20.9 GiB | 20.7 GiB (its lists are text) | 15.9 GiB | 15.9 GiB |
