# Joins that hold keys, and certificates of unique tags (ITL-1464)

Status: implemented with this document, November 25, 2026; design decisions approved by the
coordinator under the developer's delegation. It covers `Join`, `MergeJoin`, `SemiJoin` and
two-input `Rejoin` over record-batch channels (`async_execute_batches`). No job node calls
that form yet: the transport work of ITL-1464 wires it, and until then a job's memory is as
before. The sync path is unchanged, and no hash, identity, stored row or golden value moves.

## What an async join holds

A binary join step keeps, per row of **one** side that can still match, its prepared join
keys and its row number (a Polars frame, `streaming_joins._Side`); rows are in a row spool
(`core/row_spool.py`). Matching is the sync path's (`join._matched_pairs` on
`arrow_keys.polars_key_frame`), one vectorized join per arriving batch, and every value is
taken from the rows by number, so it is the input's, bit for bit.

| Step | Held | When rows are emitted |
|---|---|---|
| both sides certified unique on the shared tags (1:1) | keys of the rows not matched yet; both rows of a pair go when they match | as they match |
| any other | keys of the smaller side; a row of it goes when it matches, if the other side is certified unique | once the smaller side is known |
| `SemiJoin` | the distinct keys of the right input | once the right input has ended |
| `Rejoin` of two inputs | shared columns of the rows not matched yet | as they match |

**Which side is indexed.** While both sides are open, the rows of both are spooled without
keys. When a side ends with at most 65,536 rows it is indexed. When it ends with more, the
other side is spooled on until it has as many rows (the ended side is indexed) or ends with
fewer (it is indexed itself). The other side's spooled rows are then matched, a block at a
time, and let go; rows still to arrive are matched at once and not held.

**Cost.** *Latency*: such a step gives no row before a side has ended and, when that side is
large, before the other has delivered as many rows or ended. An endless second input is
joined as it arrives from then on. *Disk*: each side's rows are written at most once, so at
most both inputs' rows; a chain's middle result counts as an input of its next step. A
replayed file is deleted when its rows are matched.

**Nothing is held back.** Every input is read at once and to its end, each until 65,536 rows
wait for the join (the producer's last batch is taken whole). Draining one side while the
other waits is not safe, not even for inputs with no common upstream: of two joins of the
same two producers, one may hold back the one the other waits for.

A join of three or more inputs is a chain of such steps in canonical order; each step's left
side has the certificate of the rows joined so far (`unique_keys.joined_keys`).

Memory, per indexed row: the prepared key (16 bytes per string, plus its length when above
12 bytes; 8 per integer, timestamp or float; 1 for the constant key), 8 for the row number,
9 for its place and whether it is held. Beside it: at most 64 MiB of rows per side before
they go to disk, the batches in flight, a block of 65,536 rows being replayed, and one
output piece of at most 8,192 pairs. A many-to-many output is never held: the pairs are
counted first, then made in pieces; one arriving row with more pairs than a piece has its
pairs listed (16 bytes each, at most the indexed rows) and is sent in pieces.

`Rejoin` of three or more inputs stays a barrier, because with repeated origin rows which
repeats are matched follows the whole inputs. A subclass or a customized instance of an
operator runs its own sync call over its whole inputs. A stream must implement the whole
`StreamProtocol` (`unique_keys` included): protocols are internal, and every stream class
of the library and everything built on `StreamBase` does.

## What changes for users

Once job nodes run joins this way (async runs only; the sync path is unchanged):

- **Memory**: an async join holds the join keys of its smaller input (of both inputs' rows not
  matched yet, when both are certified unique on the shared tags) and no rows beyond 64 MiB
  per input, whatever its inputs and output are.
- **Latency**: a join that is not one to one gives its first row only once one input has
  ended and, when that input has more than 65,536 rows, once the other has delivered as many
  or ended. A semi-join gives its first row once its right input has ended. One-to-one joins
  and two-input rejoins give rows as they match.
- **Local disk**: at most both inputs' rows (a chain's middle results count as inputs), in
  the system's temporary directory, removed when the join ends, however it ends. A full disk
  fails the run with the operating system's error.
- **Row order** of an async result still follows arrival and is not a result.

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

| Case | Rows | Form | Output rows | Peak RSS | Arrow pool peak | Disk peak | Time |
|---|---|---|---|---|---|---|---|
| `lookup` (300 units first) | 1,000,000 | barrier | 1,000,000 | 1,793 MiB | 1,323 MiB | | 2.3 s |
| `lookup` | 8,000,000 | keys | 8,000,000 | 221 MiB | 10 MiB | 0 | 21.3 s |
| `lookup_late` | 1,000,000 | keys | 1,000,000 | 455 MiB | 131 MiB | 128 MiB | 2.7 s |
| `lookup_late` | 8,000,000 | keys | 8,000,000 | 481 MiB | 132 MiB | 1,480 MiB | 21.0 s |
| `one_to_one` (in step) | 8,000,000 | keys | 8,000,000 | 262 MiB | 27 MiB | 0 | 39.2 s |
| `one_to_one_late` | 1,000,000 | barrier | 1,000,000 | 2,574 MiB | 1,854 MiB | | 4.4 s |
| `one_to_one_late` | 1,000,000 | keys | 1,000,000 | 754 MiB | 157 MiB | 128 MiB | 5.5 s |
| `one_to_one_late` | 8,000,000 | keys | 8,000,000 | 2,456 MiB | 435 MiB | 1,480 MiB | 64.6 s |
| `many` (5 per row) | 1,000,000 | barrier | 5,000,000 | 4,835 MiB | 4,111 MiB | | 2.4 s |
| `many` | 8,000,000 | keys | 40,000,000 | 330 MiB | 57 MiB | 0 | 29.8 s |
| `many_open` (5 per row) | 1,000,000 | barrier | 5,000,000 | 6,068 MiB | 5,002 MiB | | 4.2 s |
| `many_open` | 1,000,000 | keys | 5,000,000 | 684 MiB | 194 MiB | 259 MiB | 6.9 s |
| `many_open` | 8,000,000 | keys | 40,000,000 | 1,793 MiB | 423 MiB | 2,528 MiB | 62.4 s |
| `semi` | 8,000,000 | keys | 8,000,000 | 214 MiB | 8 MiB | 0 | 20.1 s |
| `semi_late` | 8,000,000 | keys | 8,000,000 | 477 MiB | 132 MiB | 1,480 MiB | 19.6 s |
| `rejoin` (two branches in step) | 8,000,000 | keys | 8,000,000 | 320 MiB | 51 MiB | 0 | 51.9 s |

The "keys" rows are with derived tokens, the barrier rows (measured before) with text. The
disk peak is sampled every 0.2 s. What still grows with the rows: `many_open`, two inputs of
equal size with no certificate, by the keys of one of them; and `one_to_one_late`, where one
certified input arrives whole before the other, by its keys. Times were taken with other
work on the machine.

## Batches with derived source-info tokens

A channel and the kernels take a batch whose source-info columns are derived
(`utils/source_tokens.py`): such a column has no bytes, so it costs a held or spooled row
nothing. Derived columns travel between the library's operators only
(`RecordBatchInput.carried`): every public read of a channel (`receive`, iteration,
`collect`) gives text, and a stream is read through its own `as_table` unless its class is a
library class that declares the carried read. Batches of one input may hold a column as text
in one and derived in another, however they arrive; rows fetched from both are text.

Text is built where values are needed: a source-info column both inputs of a `MergeJoin`
have, a merged data-tag one side of which is text, and the tokens that read system tags whose
values a join of one pipeline hash sorts. `Rejoin`'s kernel builds all of it.

## The bytes of a held key row

Measured on 4,000,000 rows with the keys of the benchmark (two strings of 24 and 10
characters and one integer), as the growth of the process's physical footprint:

| Form held | Bytes per row | Probe of 65,536 rows |
|---|---|---|
| the prepared key frame (what a join holds) | 95 | 26 ms, and about 100 more bytes per row while it runs |
| the same, strings as Polars categories | 38 | 15 ms, about 115 more |
| a 64-bit hash of the prepared key and a row number | 23 | 5 ms, 4 more |

Of the 95 bytes, 42 are the key's data and 9 the row number and the constant key; the rest is
the string views and buffers Polars keeps. The join's own 9 bytes per row (its place and
whether it is held) are beside every form. These are measurements of the frames, not of a
whole join.

### A hash index (not built; for the developer)

The index would hold, for a key without a nested column, the 64-bit `hash_rows` of the
prepared key and the row number. An arriving batch's hashes pick candidate pairs; the rows of
the candidates are fetched, as they are for the output anyway, and each candidate pair is
matched by `join._matched_pairs` on its real keys (with the pair's number as one more key).
So there is one equality rule, and two keys of one hash cost a fetch, never a wrong row.

Gain, estimated and not measured: the 1:1 case with one input entirely first (2,456 MiB at
8,000,000 rows) would fall well below 1 GiB, and a probe would take a fifth of the time.

Risk: it rests on keys that a join takes as equal having equal hashes, across frames, chunks
and slices. Polars documents no such promise for `hash_rows`. If it failed, a pair would be
missing from the output and nothing would say so. The coordinator deferred it for that
reason (November 25, 2026): the memory of today's form fits the target.

Checked so far, by hand and not as a test: signed zeros and NaNs of several payloads at
64 bits, signed zeros and NaNs at 32 bits, floats in lists and structs, strings across chunks
and slices.

Test plan, if it is built:

1. For every key type a join allows (`arrow_keys.part_without_rule`), every pair of values of
   the types' edge pools: the join's equality implies equal hashes.
2. The same with the two keys in different frames, chunk layouts, slices and frames of other
   columns beside them, and with strings on both sides of the 12-byte view limit.
3. The certificate graphs (`test_certificate_graphs.py`) and every kernel test, run with both
   indexes: equal rows.
4. A run with every hash forced equal (all candidates), and with a hash of few bits: equal
   rows, bounded memory of the candidates.
5. A check at the first batch of a run that Polars' hashes of fixed vectors (signed zeros,
   NaN payloads, a sliced string) agree with the join; if not, the frame index is used.
