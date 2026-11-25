# Source-info tokens carried as a recipe (ITL-1464, track TOKENS)

Status: direction approved November 25, 2026. Step 1 (sources and row-wise operators)
implemented with this document; step 2 (joins) and step 3 (aggregations) are stacked on it.

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
| `Batch`, `GroupBy` | derived lists, one element per member (step 3) |
| `Join`, `MergeJoin` merged or colliding columns | text |

## Consumers

| Consumer | What it needs | Step 1 |
|---|---|---|
| Column selections, `MapData`, `MapTags`, `MapColumns`, `TagBy`, `AddTags`, `DropNulls`, `Filter` | none of the values | derived in, derived out |
| `PolarsFilter` | none, unless a predicate reads a token | the columns stay out of the Polars frame and are put back for the kept rows; a predicate that could read one (a selector, a regular expression, a non-expression, a name that is no column) gets text, as before |
| `without_input_meta` | names only | derived |
| `SemiJoin` (left input) | none | derived (step 2) |
| `Join`, `Rejoin`, `MergeJoin`: columns of one input | none | derived (step 2) |
| `Rejoin`: a shared data-tag | equality of the tokens | one recipe in every input: equal wherever the system tags are, so not compared; otherwise text (step 2) |
| `Join`: a shared data-tag | both tokens, folded (`join_v1`) | a fold recipe (`FoldRecipe`), the same fold run when the text is made; text when a member is text (step 2) |
| `MergeJoin`: a colliding column | both tokens, in a list | text of that column |
| `GroupBy`, `Batch` | the members' tokens, in a list | derived lists (step 3, below) |
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
| group by, batch | not from the system tags, which are folded to one digest: a list column carries its members' ids |
| function pod | an output: no. Inputs' tokens are not in its output |

## Aggregations (step 3)

`GroupBy` and `Batch` fold the members' system tags into one digest, so a list of tokens
cannot be derived from the table's own system tags. A list column of `MemberTokenType`
holds its members' ids itself: its storage is `large_list<struct<record id, source id,
guard>>`, the two ids as dictionaries over the input's columns (a member is an index; the
ids' text is held once, the few source ids once each), the guard a struct with no field,
so Parquet refuses it. Every token column of one aggregation that reads the same system
tags shares one array of ids, so a further column costs nothing.

- The text is made by the same `materialize`, 65,536 members at a time, and has the type
  the aggregation gave it (F retypes it as it does text).
- A data-tag key's fold, and the system tags' folds, read the members' text a part at a
  time, and the fold digests its canonical JSON a part at a time (the same bytes).
- A list of tokens that is aggregated again, and a token that is text, are aggregated as
  text. Matching operators carry a list column of one input; a shared one becomes text.
- `iter_data` builds at most 8,192 rows and, beyond one row, 65,536 members at once.

On a spill (`RowSpool`, `source_tokens.Shelf`): a column of one token a row is not written
(its type says it all), and lists whose members' ids are equal share one ids column in a
file, whose dictionaries hold only the ids that file's rows read. Rows a spool keeps in
memory are trimmed the same way, so a slice never pins the ids of the whole table. Read
back, the columns share the mapped ids again. `kept_table` cuts by bytes, what is shared
counted once. Measured with 200,000 members in 300 groups and 9 token columns: 16.1 MiB in
one file and 16.4 MiB in 150 files of 2 groups (0.11 MiB a file; it was 96 MiB), against
125.5 MiB as text.

Refusing to leave the process: the storage of a recipe column is a list of no element of
structs nested deeper than Arrow IPC and the C data interface write (the depth costs
nothing per row, since no element exists), and a list's storage holds the same as a guard.
IPC files, streams and messages, Feather, the C stream and pandas refuse a table with rows.
A table with no rows can be written: it has no value. The hash and store guards look inside
structs, lists, dictionaries and extension storages (`refuse_derived`).

## Grouping millions of rows

`GroupBy` reads its keys and system tags in one chunk each (a `take` from many chunks puts
them together every time), and sorts by the record ids, the largest columns, only when rows
tie on every tag before them. Text with few distinct values is sorted by its rank (four bytes
a row). Measured outside a job with a synthetic event table of about
8 million rows, 8 columns, 100 groups of about 80,000 members (footprint: the memory the
system charges the process; the source's own read is 3.5 GiB of every line):

| Step | Seconds before | after | Peak footprint before | after |
|---|---|---|---|---|
| read, recipes carried | | 64 | | 3.5 GiB |
| `group_by` | 411 | 143 | 10.5 GiB | 4.9 GiB |
| `group_by`, a function per group | | 155 | | 5.3 GiB |

The values of the grouped table are 0.09 GiB: they do not dominate. What it holds is 1.7
GiB, the members' record ids (about 190 bytes each, the canonical JSON of the key). Of the
143 seconds, about 70 are the folds of the eight system-tag and key columns, whose digests
read every member's text, as they always did.

In a job, on the integration branch's head with this: 4.4 GiB on sync and 5.8 GiB on async
for `group_by` (3.0 GiB on async at half the rows); with a function per group 10.9 GiB on sync and 6.1 GiB on async. The
difference on sync is the function node, which holds every input row as objects before it
computes: 100 rows of 80,000 members, values and token text.

## Measured

A synthetic time series table, about 2.2 million rows, 13 columns, about 250 MiB of Arrow data, read through
`SpiralDBTableSource` (`measure.py` of the diagnosis, one process per step).

| Step | Peak RSS before | after | Arrow pool peak before | after |
|---|---|---|---|---|
| `as_table()`, `run()` | 10.8 GiB | 2.0 GiB | 9.3 GiB | 0.75 GiB |
| `map_data` | 10.8 GiB | 2.0 GiB | 9.3 GiB | 0.75 GiB |
| `drop_nulls` | 19.7 GiB | 2.4 GiB | 18.6 GiB | 1.7 GiB |
| `polars_filter` | 16.6 GiB | 2.5 GiB | 9.3 GiB | 0.75 GiB |
| `join` (8 and 14 columns) | above 24 GiB | 3.6 GiB (step 2) | | 2.6 GiB |
| `group_by` | 20.9 GiB | 3.3 GiB (step 3) | 15.9 GiB | 1.6 GiB |
| `drop_nulls`, `group_by`, `map_data` | above 24 GiB | 3.9 GiB (step 3) | | 1.8 GiB |
| the same, then a function pod | above 24 GiB | 5.5 GiB (step 3) | | 2.2 GiB |
