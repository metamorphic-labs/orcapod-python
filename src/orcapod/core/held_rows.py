"""Rows a job node holds, as Arrow data on local disk when they are many (ITL-1464).

A function job node holds its results and a side-effect job node the rows it passed on,
each as a ``(Tag, Data)`` pair: about 20 KiB per row for a few numbers. ``HeldPairs`` (a
list of pairs) and ``HeldRows`` (pairs by entry id) hold them in chunks: a full chunk's
rows become one Arrow table in a ``RowSpool`` (in memory up to its limit, then a private
temporary file read back memory-mapped), and a pair is built again when it is read.

A row is kept that way only when the pair built again is the pair that was held, in every
view of it: both are plain ``Tag`` and ``Data``, its tables have the chunk's schema, and
each Python value it already holds (a function's own return value, its meta values) is of
a kind that the Arrow value gives back exactly (``_same``). Any other row (a user class
instance, an array, a missing result) stays the pair it is, in its place.

This module imports no other part of Orcapod but the spool, so that the node modules can
import it without a cycle: the classes of a row, ``Tag`` and ``Data``, are given to it.

The spool's files go when the rows do: ``clear`` and ``close``, which a node calls when
its run fails, is cancelled or is cleared.
"""

from __future__ import annotations

import bisect
import datetime as dt
import decimal
import functools
import itertools
import pathlib
import struct
import uuid
from collections.abc import Iterable, Iterator, Mapping, MutableMapping, Sequence
from typing import Any, NamedTuple, TypeVar, cast, overload

import pyarrow as pa
from typing_extensions import override

from orcapod.core.row_spool import SPOOL_MEMORY_BYTES, RowSpool
from orcapod.system_constants import constants

_T = TypeVar("_T")

Pair = tuple[Any, Any]
"""A tag and its data; the data is ``None`` for an input without output."""


HELD_CHUNK_ROWS = 8_192
"""Rows held as pairs before they become a chunk."""

HELD_MEMORY_BYTES = SPOOL_MEMORY_BYTES
"""Bytes of chunks that stay in memory; beyond them they are on local disk."""

_TAG = "t:"
_DATA = "d:"
_TAG_ID = "tag_id"
_DATA_ID = "data_id"
_TAG_COLUMNS = {"system_tags": True, "meta": True}
_DATA_COLUMNS = {"source": True, "meta": True}


_EXACT_KINDS = frozenset(
    (
        type(None),
        bool,
        int,
        str,
        bytes,
        dt.date,
        dt.timedelta,
        uuid.UUID,
        pathlib.PosixPath,
        pathlib.WindowsPath,
    )
)
"""Kinds whose values, when equal, are the same."""


def _same(given: object, read: object) -> bool:
    """Whether ``read``, a value read from Arrow, is ``given`` in every respect.

    Only for kinds whose equality and type say so; any other kind is not the same.
    """
    kind = type(given)
    if kind is not type(read):
        return False
    if kind is float:
        # Bit for bit: -0.0 is not 0.0, and a NaN is itself.
        return struct.pack("<d", given) == struct.pack("<d", read)
    if kind is dt.datetime:
        a, b = cast(dt.datetime, given), cast(dt.datetime, read)
        return (
            a.replace(tzinfo=None) == b.replace(tzinfo=None)
            and a.tzinfo is b.tzinfo
            and a.fold == b.fold
        )
    if kind is decimal.Decimal:
        return (
            cast(decimal.Decimal, given).as_tuple()
            == cast(decimal.Decimal, read).as_tuple()
        )
    if kind is list:
        a_list, b_list = cast("list[object]", given), cast("list[object]", read)
        return len(a_list) == len(b_list) and all(map(_same, a_list, b_list))
    if kind is dict:
        a_dict = cast("dict[object, object]", given)
        b_dict = cast("dict[object, object]", read)
        return _same(list(a_dict), list(b_dict)) and all(
            map(_same, a_dict.values(), b_dict.values())
        )
    return kind in _EXACT_KINDS and given == read


def _may_be_same(value: object) -> bool:
    """Whether ``value`` is of a kind ``_same`` compares; nothing is converted."""
    kind = type(value)
    if kind is list:
        return all(map(_may_be_same, cast("list[object]", value)))
    if kind is dict:
        items = cast("dict[object, object]", value)
        return all(map(_may_be_same, items)) and all(map(_may_be_same, items.values()))
    return kind in _EXACT_KINDS or kind in (float, dt.datetime, decimal.Decimal)


def _same_values(given: Mapping[str, object], read: Mapping[str, object]) -> bool:
    """Whether two rows' values have the same names, in order, and are the same."""
    return list(given) == list(read) and all(
        _same(value, read[name]) for name, value in given.items()
    )


class _Origin(NamedTuple):
    """What every row of a chunk shares besides its columns."""

    tag_context: Any
    data_context: Any
    tag_config: Any
    data_config: Any


def _origin(tag: Any, data: Any) -> _Origin:
    return _Origin(
        tag.data_context, data.data_context, tag.orcapod_config, data.orcapod_config
    )


class _Candidate(NamedTuple):
    """A row whose tables can be part of a chunk."""

    place: int
    tag: Any
    data: Any
    tag_table: pa.Table
    data_table: pa.Table


class RowTypes(NamedTuple):
    """The classes of the rows that become chunks (``Tag`` and ``Data``), exactly."""

    tag: type[Any]
    data: type[Any]


def _candidate(types: RowTypes, place: int, pair: Pair) -> _Candidate | None:
    """The row as tables, or ``None`` for a row that stays the pair it is."""
    tag, data = pair
    if type(tag) is not types.tag or type(data) is not types.data:
        return None
    try:
        if not tag.keys() or set(data._source_info) != set(
            data.keys()
        ):  # a token dropped or filled in would differ
            return None
        for datagram in (tag, data):
            # Before its table is built: a row held as it is gets no second form.
            values = datagram._data_dict  # ``None``: every value comes from Arrow
            if values is not None and not all(map(_may_be_same, values.values())):
                return None
        tag_table = tag.as_table(columns=_TAG_COLUMNS)
        data_table = data.as_table(columns=_DATA_COLUMNS)
    except (MemoryError, OSError):
        raise
    except Exception:  # noqa: BLE001  # a row that makes no table is held as it is
        return None
    if not data_table.num_columns:
        return None
    return _Candidate(place, tag, data, tag_table, data_table)


def _prefixed(table: pa.Table, prefix: str) -> pa.Table:
    return table.rename_columns([prefix + name for name in table.column_names])


def _unprefixed(table: pa.Table, prefix: str) -> pa.Table:
    names = [name for name in table.column_names if name.startswith(prefix)]
    return table.select(names).rename_columns([n.removeprefix(prefix) for n in names])


def _pairs_of(types: RowTypes, table: pa.Table, origin: _Origin) -> Iterator[Pair]:
    """Build the pairs of a chunk's rows again."""
    tags = types.tag.rows_of(_unprefixed(table, _TAG), origin.tag_context)
    datas = types.data.rows_of(_unprefixed(table, _DATA), origin.data_context)
    tag_ids = table.column(_TAG_ID).to_pylist()
    data_ids = table.column(_DATA_ID).to_pylist()
    for tag, data, tag_id, data_id in zip(tags, datas, tag_ids, data_ids, strict=True):
        tag._datagram_uuid = uuid.UUID(
            bytes=tag_id
        )  # the row's own id, as constructing it with one sets it
        data._datagram_uuid = uuid.UUID(bytes=data_id)  # as above
        tag._orcapod_config = origin.tag_config  # as constructing it with one sets it
        data._orcapod_config = origin.data_config  # as above
        yield tag, data


class _PythonSide:
    """What the rows of a chunk's tag, or data, columns read as in Python."""

    def __init__(self, table: pa.Table, context: Any) -> None:
        not_values = (
            constants.META_PREFIX,
            constants.SYSTEM_TAG_PREFIX,
            constants.SOURCE_PREFIX,
        )
        names = table.column_names
        self._own = table.select([n for n in names if not n.startswith(not_values)])
        self._meta = table.select(
            [n for n in names if n.startswith(constants.META_PREFIX)]
        )
        self._converter = context.type_converter

    def _rows(self, table: pa.Table) -> list[dict[str, Any]]:
        if not table.num_columns:
            return [{}] * table.num_rows
        return cast(
            "list[dict[str, Any]]",
            cast(object, self._converter.arrow_table_to_python_dicts(table)),
        )

    def _schema(self, table: pa.Table) -> Any:
        return self._converter.arrow_schema_to_python_schema(table.schema)

    # Each read once for the whole chunk, and only when a row asks for it.
    @functools.cached_property
    def values(self) -> list[dict[str, Any]]:
        return self._rows(self._own)

    @functools.cached_property
    def schema(self) -> Any:
        return self._schema(self._own)

    @functools.cached_property
    def meta(self) -> list[dict[str, Any]]:
        return self._rows(self._meta)

    @functools.cached_property
    def meta_schema(self) -> Any:
        return self._schema(self._meta)

    def gives_back(self, row: int, datagram: Any) -> bool:
        """Whether row ``row`` reads as everything ``datagram`` holds in Python."""
        held = datagram._data_dict  # ``None``: every value comes from Arrow already
        if held is not None and not _same_values(held, self.values[row]):
            return False
        schema = datagram._data_python_schema  # ``None``: from Arrow already
        if schema is not None and not _same_schema(schema, self.schema):
            return False
        meta = datagram.get_meta_info()
        return (
            not meta
            or (
                _same_values(meta, self.meta[row])
                and _same_schema(
                    datagram._meta_python_schema,  # the types its meta columns are built with
                    self.meta_schema,
                )
            )
        )


def _same_schema(given: Any, read: Any) -> bool:
    return list(given.items()) == list(read.items()) and (
        given.optional_fields == read.optional_fields
    )


class _Chunk(NamedTuple):
    """Consecutive rows: those in the spool by number, the others as they are."""

    first: int
    """The spool's number of the chunk's first spooled row."""
    stored: int
    """How many of its rows are in the spool."""
    kept: dict[int, Pair]
    """The rows that are not, by place in the chunk."""
    origin: _Origin | None


class HeldPairs(Sequence[Pair]):
    """Pairs in the order appended, full chunks of them in a ``RowSpool``.

    Appending never builds a chunk: ``spill`` does, where the caller can let a full disk
    end the run.
    """

    def __init__(self, types: RowTypes) -> None:
        self._types: RowTypes = types
        self._start()

    def _start(self) -> None:
        self._spool: RowSpool | None = None
        self._schema: pa.Schema | None = None
        self._chunks: list[_Chunk] = []
        self._chunk_rows: int = 0
        self._starts: list[int] = []
        self._pending: list[Pair] = []
        # The last chunk read, so reading neighbours reads its rows from the spool once.
        self._last: tuple[int, pa.Table] | None = None
        # Every spooled row, read through mappings: a forked child reads it too.
        self._table: pa.Table | None = None

    def append(self, pair: Pair) -> None:
        """Hold ``pair`` after every pair held."""
        self._pending.append(pair)

    def spill(self) -> None:
        """Make chunks of the pairs appended, as far as they fill one.

        Raises:
            OSError: If a chunk cannot be written to the local disk. Every pair is then
                still held.
        """
        while len(self._pending) >= HELD_CHUNK_ROWS:
            rows = self._pending[:HELD_CHUNK_ROWS]
            chunk = self._chunk_of(rows)
            if self._spool is not None:
                self._table = self._spool.table()
            # From here on nothing can fail: the chunk replaces its pairs.
            self._starts.append(self._chunk_rows)
            self._chunks.append(chunk)
            self._chunk_rows += len(rows)
            self._pending = self._pending[HELD_CHUNK_ROWS:]
            self._last = None

    def _chunk_of(self, rows: list[Pair]) -> _Chunk:
        """Put the rows that can be into the spool."""
        kept = dict(enumerate(rows))
        candidates = [
            c
            for place, pair in enumerate(rows)
            if (c := _candidate(self._types, place, pair))
        ]
        origin: _Origin | None = None
        table: pa.Table | None = None
        if candidates:
            first = candidates[0]
            origin = _origin(first.tag, first.data)
            alike = [
                c
                for c in candidates
                if all(
                    a is b for a, b in zip(_origin(c.tag, c.data), origin, strict=True)
                )
                and c.tag_table.schema.equals(first.tag_table.schema, True)
                and c.data_table.schema.equals(first.data_table.schema, True)
            ]
            table = self._table_of(alike)
            if self._schema is not None and not table.schema.equals(self._schema, True):
                table = None
            else:
                tags = _PythonSide(_unprefixed(table, _TAG), origin.tag_context)
                datas = _PythonSide(_unprefixed(table, _DATA), origin.data_context)
                good = [
                    i
                    for i, c in enumerate(alike)
                    if tags.gives_back(i, c.tag) and datas.gives_back(i, c.data)
                ]
                if len(good) < len(alike):
                    table = table.take(pa.array(good, pa.int64())) if good else None
                if table is not None:
                    for i in good:
                        del kept[alike[i].place]
        if table is None:
            return _Chunk(0, 0, dict(enumerate(rows)), None)
        if self._spool is None:
            self._spool = RowSpool(memory_bytes=HELD_MEMORY_BYTES)
        first_number = self._spool.size
        self._spool.append(table)
        self._schema = table.schema
        return _Chunk(first_number, table.num_rows, kept, origin)

    @staticmethod
    def _table_of(rows: list[_Candidate]) -> pa.Table:
        ids = pa.schema(
            [
                pa.field(_TAG_ID, pa.large_binary(), nullable=False),
                pa.field(_DATA_ID, pa.large_binary(), nullable=False),
            ]
        )
        parts = (
            _prefixed(pa.concat_tables([c.tag_table for c in rows]), _TAG),
            _prefixed(pa.concat_tables([c.data_table for c in rows]), _DATA),
            pa.Table.from_arrays(
                [
                    pa.array([c.tag.datagram_uuid.bytes for c in rows], ids.types[0]),
                    pa.array([c.data.datagram_uuid.bytes for c in rows], ids.types[1]),
                ],
                schema=ids,
            ),
        )
        return pa.Table.from_arrays(
            [column for part in parts for column in part.columns],
            schema=pa.schema([field for part in parts for field in part.schema]),
        ).combine_chunks()

    def _stored_table(self, index: int) -> pa.Table:
        """The spooled rows of chunk ``index``."""
        if self._last is None or self._last[0] != index:
            chunk = self._chunks[index]
            assert self._table is not None
            self._last = (index, self._table.slice(chunk.first, chunk.stored))
        return self._last[1]

    def has_data(self, place: int) -> bool:
        """Whether the pair at ``place`` has data; nothing is read or built."""
        if place >= self._chunk_rows:
            return self._pending[place - self._chunk_rows][1] is not None
        index = bisect.bisect_right(self._starts, place) - 1
        kept = self._chunks[index].kept.get(place - self._starts[index])
        return kept is None or kept[1] is not None

    def is_kept(self, place: int) -> bool:
        """Whether the pair at ``place`` is held as it is; nothing is read or built."""
        if place >= self._chunk_rows:
            return True
        index = bisect.bisect_right(self._starts, place) - 1
        return place - self._starts[index] in self._chunks[index].kept

    @override
    def __len__(self) -> int:
        return self._chunk_rows + len(self._pending)

    @overload
    def __getitem__(self, index: int) -> Pair: ...
    @overload
    def __getitem__(self, index: slice) -> Sequence[Pair]: ...
    @override
    def __getitem__(self, index: int | slice) -> Pair | Sequence[Pair]:
        if isinstance(index, slice):
            return [self[i] for i in range(*index.indices(len(self)))]
        place = index + len(self) if index < 0 else index
        if not 0 <= place < len(self):
            raise IndexError(index)
        if place >= self._chunk_rows:
            return self._pending[place - self._chunk_rows]
        number = bisect.bisect_right(self._starts, place) - 1
        chunk, row = self._chunks[number], place - self._starts[number]
        if row in chunk.kept:
            return chunk.kept[row]
        assert chunk.origin is not None
        before = sum(1 for other in chunk.kept if other < row)
        table = self._stored_table(number).slice(row - before, 1)
        return next(_pairs_of(self._types, table, chunk.origin))

    @override
    def __iter__(self) -> Iterator[Pair]:
        """The pairs held now, in order, also if they are let go while reading."""
        return self._read(
            self._table, list(self._chunks), list(self._starts), self._pending
        )

    def _read(
        self,
        table: pa.Table | None,
        chunks: list[_Chunk],
        starts: list[int],
        pending: list[Pair],
    ) -> Iterator[Pair]:
        pending = list(pending)
        ends = [*starts[1:], sum(c.stored + len(c.kept) for c in chunks)]
        for chunk, start, end in zip(chunks, starts, ends, strict=False):
            stored: Iterator[Pair] = iter(())
            if chunk.origin is not None and table is not None:
                stored = _pairs_of(
                    self._types, table.slice(chunk.first, chunk.stored), chunk.origin
                )
            for row in range(end - start):
                yield chunk.kept[row] if row in chunk.kept else next(stored)
        yield from pending

    def close(self) -> None:
        """Let every pair go, and delete the files; rows being read stay readable."""
        if self._spool is not None:
            self._spool.close()
        self._start()


class HeldRows(MutableMapping[bytes, Pair]):
    """Pairs by entry id, in the order first given, held in ``HeldPairs``."""

    def __init__(self, types: RowTypes) -> None:
        self._pairs: HeldPairs = HeldPairs(types)
        self._places: dict[bytes, int] = {}

    def spill(self) -> None:
        """Make full chunks of the pairs held (``HeldPairs.spill``)."""
        self._pairs.spill()

    def has_data(self, key: bytes) -> bool:
        """Whether ``key`` is held with data; nothing is read or built."""
        place = self._places.get(key)
        return place is not None and self._pairs.has_data(place)

    def is_kept(self, key: bytes) -> bool:
        """Whether ``key`` is held as the pair it is; nothing is read or built.

        A pair that is not is always a ``Tag`` and a ``Data`` with values.
        """
        place = self._places.get(key)
        return place is not None and self._pairs.is_kept(place)

    @override
    def __getitem__(self, key: bytes) -> Pair:
        return self._pairs[self._places[key]]

    @override
    def __setitem__(self, key: bytes, pair: Pair) -> None:
        self._pairs.append(pair)
        self._places[key] = len(self._pairs) - 1

    @override
    def __delitem__(self, key: bytes) -> None:
        del self._places[key]

    @override
    def __iter__(self) -> Iterator[bytes]:
        return iter(self._places)

    @override
    def __len__(self) -> int:
        return len(self._places)

    @override
    def __contains__(self, key: object) -> bool:
        return key in self._places

    @override
    def clear(self) -> None:
        """Let every pair go, and delete the files."""
        self._pairs.close()
        self._places.clear()


def batched(pairs: Iterable[_T], size: int) -> Iterator[list[_T]]:
    """``pairs`` in lists of ``size``, the last one shorter."""
    iterator = iter(pairs)
    while batch := list(itertools.islice(iterator, size)):
        yield batch
