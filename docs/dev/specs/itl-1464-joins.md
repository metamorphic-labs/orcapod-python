# Joins that hold keys, and certificates of unique tags (ITL-1464)

Status: implemented with this document, November 25, 2026; design decisions approved by the
coordinator under the developer's delegation. It covers `Join`, `MergeJoin` and
`SemiJoin` over record-batch channels (`async_execute_batches`). No job node calls that
form yet: the transport work of ITL-1464 wires it, and until then a job's memory is as
before. The sync path is unchanged, and no hash, identity, stored row or golden value moves.

## What an async join holds

Each side of a binary join step keeps, per row that can still match, its prepared join keys
and its row number (a Polars frame, `streaming_joins._Side`); the rows are in a row spool
(`core/row_spool.py`). Matching is the sync path's (`join._matched_pairs` on
`arrow_keys.polars_key_frame`), one vectorized join per arriving batch, and every value is
taken from the rows by number, so it is the input's, bit for bit.

| Case | Held | Let go |
|---|---|---|
| both sides unique on the shared tags (1:1) | keys of the rows not matched yet | both rows of a pair, when they match |
| one side unique (a lookup) | keys of the unique side; of the other side, rows that arrived and have not matched | a row of the other side, when it matches |
| neither (many to many) | keys of both sides | nothing while both inputs are open |
| any | | a side's rows, when the other side ends |
| `SemiJoin` | distinct right keys; undecided left rows' keys | a left row, when a right row matches it |

One rule covers the first three: **a row is let go once it has matched if and only if the
other side is unique on the join keys.** A join of three or more inputs is a chain of such
steps in canonical order; each step's left side has the certificate of the rows joined so
far (`unique_keys.joined_keys`).

Memory, per held row: the prepared key (16 bytes per string, plus its length when above 12
bytes; 8 per integer, timestamp or float; 1 for the constant key), 8 for the row number, 1
for the removal mask. Beside it: at most 64 MiB of rows per side before they go to disk, the
batches in flight, and one output piece of at most 8,192 pairs. Worst case, with no
certificate and neither input ended, the keys of both inputs. A many-to-many output is never
held: the pairs are counted first, then made in pieces; one arriving row with more pairs
than a piece has its pairs listed (16 bytes each, at most the held rows) and is sent in
pieces.

Every input is read at once and to its end, each until 65,536 rows wait for the join (the
producer's last batch is taken whole, whatever its size). Nothing
waits for one input to end, because both inputs may read one upstream (a diamond).

Not covered: `Rejoin` stays a barrier on this path, because its kernel can differ from the
sync path for repeated origins with three or more inputs. A subclass of an operator runs its
own sync call over its whole inputs.

## The row spool

Rows past the memory limit become a file each: an uncompressed Arrow IPC file of one record
batch, read memory-mapped, under a directory of this process in the system's temporary
directory. A file is deleted when its last row is discarded; the rest at a normal end, a
failure, a cancellation, collection and exit. A killed process leaves its directory; the
next process to spill there removes it (it holds no lock any more). A spool belongs to one
process, whose threads may all use it, one call at a time; a forked child's copy refuses
every read and write.

## Certificates

`StreamProtocol.unique_keys` is a set of tag sets on which no two rows agree, under the
joins' equality. It is derived when asked and stored nowhere: in no hash, identity, config,
stored row or equality. A wrong certificate would lose rows, so only what is certain is
certified: a rule is read from the exact class, and only while no member of the instance is
replaced (`reads_as_its_class`), for the source, the pod, the stream and the node alike. A
subclass or a customized instance certifies nothing, and a customized join runs its own sync
call over its whole inputs.

| Stream | Certificate |
|---|---|
| `ArrowTableSource`, `DictSource`, `ListSource`, `DataFrameSource`, SQLite and PostgreSQL tables | the key columns, when all are tags |
| every other source; a raw `ArrowTableStream`, also with a producer | none |
| `DropNulls`, `Filter`, `PolarsFilter`, `AddTags`, `TagBy`, `Pick`, `Index`, `MapData`, the data column selections, function pods (`FunctionPod`, `CachedFunctionPod`), `SideEffectPod` | the input's |
| the tag selections and drops | the input's keys whose tags all stay |
| `MapTags`, `MapColumns` | the input's keys renamed, without those that lose a tag |
| `GroupBy` | its keys |
| `Batch` | none |
| `SemiJoin` | the left input's |
| `Join`, `MergeJoin` on shared tags S | unions of one key of each side; a side's own keys when the other side is unique on S |
| `Rejoin` | every input's (a row is in at most one result row) |
| a source slot | its bound source's |
| an operator job node in `REPLAY` mode | none: it replays every logged run |

Why a source's key columns are certain: those source kinds build every row through
`SourceStreamBuilder`, which names a row by its key columns and refuses a null or repeated
key, on every read. The record id is a function of the key values and a key is a string, an
integer, a boolean, a timestamp or a date (never a float), so two rows whose keys are equal
to a join have one record id and are refused.

That direction is the safe one for a database: a store whose equality is coarser than the
join's (a case-insensitive collation, padded `CHAR`) never holds two keys the join takes as
equal; one whose equality is finer (a float key, a timestamp read at a coarser unit) could.
No store is trusted here: the SQL sources are checked as read. A later step may trust a
store's key instead of checking it, only under those conditions and only for a key without
nulls.

CSV and Delta (a running check is a later step), `CachedSource` (rows of earlier reads),
`DerivedSource` and `PollingSource` certify nothing.

## Measured

`docs/dev/specs/itl-1464-joins/measure.py`, each case in a fresh process, synthetic rows
with their source-info tokens, sent and read through bounded
channels. Peak RSS includes 143 MiB of imports and the mapped pages a join reads. "Barrier"
is the form before this change. It is the operator alone, not a job.

| Case | Rows | Form | Output rows | Peak RSS | Arrow pool peak | Time |
|---|---|---|---|---|---|---|
| `lookup` | 1,000,000 | barrier | 1,000,000 | 1,793 MiB | 1,323 MiB | 2.3 s |
| `lookup` | 1,000,000 | keys | 1,000,000 | 259 MiB | 26 MiB | 2.4 s |
| `lookup` | 8,000,000 | keys | 8,000,000 | 257 MiB | 26 MiB | 19.9 s |
| `lookup_late` | 1,000,000 | keys | 1,000,000 | 756 MiB | 169 MiB | 5.4 s |
| `lookup_late` | 8,000,000 | keys | 8,000,000 | 2,733 MiB | 392 MiB | 61.7 s |
| `one_to_one` | 1,000,000 | keys | 1,000,000 | 317 MiB | 64 MiB | 4.7 s |
| `one_to_one` | 8,000,000 | keys | 8,000,000 | 341 MiB | 64 MiB | 39.3 s |
| `one_to_one_late` | 1,000,000 | barrier | 1,000,000 | 2,574 MiB | 1,854 MiB | 4.4 s |
| `one_to_one_late` | 1,000,000 | keys | 1,000,000 | 741 MiB | 176 MiB | 5.1 s |
| `one_to_one_late` | 8,000,000 | keys | 8,000,000 | 2,301 MiB | 453 MiB | 60.0 s |
| `many` (5 per row) | 1,000,000 | barrier | 5,000,000 | 4,835 MiB | 4,111 MiB | 2.4 s |
| `many` | 1,000,000 | keys | 5,000,000 | 445 MiB | 144 MiB | 3.2 s |
| `many` | 8,000,000 | keys | 40,000,000 | 453 MiB | 146 MiB | 26.0 s |
| `many_open` (5 per row) | 1,000,000 | barrier | 5,000,000 | 6,068 MiB | 5,002 MiB | 4.2 s |
| `many_open` | 1,000,000 | keys | 5,000,000 | 992 MiB | 384 MiB | 5.6 s |
| `many_open` | 4,000,000 | keys | 20,000,000 | 1,691 MiB | 633 MiB | 23.5 s |
| `many_open` | 8,000,000 | keys | 40,000,000 | 2,629 MiB | 941 MiB | 57.3 s |
| `semi` | 8,000,000 | keys | 8,000,000 | 252 MiB | 21 MiB | 19.6 s |

`lookup`, `one_to_one` in step, `many` and `semi` do not grow with the rows. The late
cases grow with the keys of the input that arrived first, as the bound says. `many` ends its small
side first, so it measures the bounded output; `many_open` is the worst case, two open inputs
with no certificate, and grows with the keys of both.

## Batches with derived source-info tokens

A channel and the kernels take a batch whose source-info columns are derived
(`utils/source_tokens.py`): such a column has no bytes, so it costs a held or spooled row
nothing. The form is the input's first batch's, per column. Text first: a later derived
batch is built as text. Derived first: a later batch in another form is refused, because the
rows held have no text; a producer sends each column in one form. Rows sent through an
output channel may mix forms: `concatenate_batches` builds the text of a column that does.

Text is built where values are needed: a source-info column both inputs of a `MergeJoin`
have, a merged data-tag one side of which is text, and the tokens that read system tags whose
values a join of one pipeline hash sorts. `Rejoin`'s kernel, not wired, builds all of it.

| Case (8,000,000 rows) | Text: peak RSS, Arrow pool, time | Derived |
|---|---|---|
| `lookup` | 257 MiB, 26 MiB, 19.9 s | 224 MiB, 10 MiB, 20.7 s |
| `lookup_late` | 2,733 MiB, 392 MiB, 61.7 s | 2,722 MiB, 376 MiB, 45.1 s |
| `one_to_one_late` | 2,301 MiB, 453 MiB, 60.0 s | 2,292 MiB, 435 MiB, 61.4 s |
| `many_open` | 2,629 MiB, 941 MiB, 57.3 s | 2,484 MiB, 855 MiB, 50.8 s |

Peak memory hardly moves, because the rows were on disk already: what is left in the late and
open cases is the keys. The times were taken with other work on the machine.
