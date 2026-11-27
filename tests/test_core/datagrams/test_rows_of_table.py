"""The rows of a table as ``Tag`` and ``Data``, prepared once per table (ITL-1348).

``Tag.rows_of(table)`` and ``Data.rows_of(table)`` yield, for each row, the datagram the
constructor builds from the one-row table ``table.slice(i, 1)``. What the constructor does
to every row alike (the normalization to large types, the split of the columns into
groups) is done once per table, so building the rows of a 60,000-row stream no longer
normalizes 180,000 one-row tables. Nothing a row holds or reports may differ: these
tests compare every row with the constructor's, in what it holds and in every view.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

import pyarrow as pa
import pytest

from orcapod import contexts
from orcapod.core.datagrams import Data, Tag
from orcapod.core.datagrams.datagram import Datagram
from orcapod.core.streams import ArrowTableStream
from orcapod.sources import ArrowTableSource, DictSource
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig
from orcapod.utils import arrow_utils

SYS = constants.SYSTEM_TAG_PREFIX
SRC = constants.SOURCE_PREFIX
META = constants.META_PREFIX
CONTEXT = constants.CONTEXT_KEY
EVERYTHING = ColumnConfig.all()

# --- Comparing two datagrams ------------------------------------------------------------


def _same_table(left: pa.Table | None, right: pa.Table | None) -> bool:
    """Whether two tables have one schema (metadata included) and the same values."""
    if left is None or right is None:
        return left is right
    return left.schema.equals(right.schema, check_metadata=True) and (
        left.to_pylist() == right.to_pylist()
    )


def _assert_same_datagram(built: Datagram, expected: Datagram) -> None:
    """Assert ``built`` holds and reports exactly what ``expected`` does."""
    assert type(built) is type(expected)
    # What it holds: the same attributes, each with the same value.
    held, wanted = vars(built), vars(expected)
    assert sorted(held) == sorted(wanted)
    for name, value in wanted.items():
        if isinstance(value, pa.Table) or isinstance(held[name], pa.Table):
            assert _same_table(held[name], value), name
        elif name == "_data_context":
            assert held[name] is value
        elif name == "_token_layout":
            # What ``token_layout()`` answered: the rows of one table are given
            # the answer once; the constructor's row reads it when asked (below).
            assert value is None
        else:
            assert held[name] == value, name
    # What it reports, through every view.
    assert built.data_context_key == expected.data_context_key
    assert built.keys(columns=EVERYTHING) == expected.keys(columns=EVERYTHING)
    assert built.schema(columns=EVERYTHING) == expected.schema(columns=EVERYTHING)
    assert built.arrow_schema(columns=EVERYTHING).equals(
        expected.arrow_schema(columns=EVERYTHING), check_metadata=True
    )
    assert built.as_dict(columns=EVERYTHING) == expected.as_dict(columns=EVERYTHING)
    assert _same_table(
        built.as_table(columns=EVERYTHING), expected.as_table(columns=EVERYTHING)
    )
    assert _same_table(built.as_table(), expected.as_table())
    assert built.get_meta_info() == expected.get_meta_info()
    assert built.content_hash() == expected.content_hash()
    if isinstance(expected, Tag):
        assert isinstance(built, Tag)
        assert built.system_tags() == expected.system_tags()
    if isinstance(expected, Data):
        assert isinstance(built, Data)
        assert built.source_info() == expected.source_info()
        assert built.token_layout() == expected.token_layout()


def _assert_rows_match_the_constructor(
    kind: type[Tag] | type[Data], table: pa.Table, data_context: Any = None
) -> None:
    """Assert ``kind.rows_of(table)`` yields the constructor's datagram of every row."""
    rows = list(kind.rows_of(table, data_context))
    assert len(rows) == table.num_rows
    for i, row in enumerate(rows):
        _assert_same_datagram(row, kind(table.slice(i, 1), data_context=data_context))


# --- Tables of tags ---------------------------------------------------------------------


def _user_and_system_tags() -> pa.Table:
    """Two user tags and two system tags, small types, a null, one non-null field."""
    return pa.table(
        {
            "subject": pa.array(["m1", "m1", "m2"], pa.string()),
            "trial": pa.array([1, 2, None], pa.int16()),
            f"{SYS}source:abc": pa.array(["events", "events", "events"], pa.string()),
            f"{SYS}record_id:abc": pa.array(["row_0", "row_1", "row_2"]),
        },
        schema=pa.schema(
            [
                pa.field("subject", pa.string(), nullable=False),
                pa.field("trial", pa.int16(), metadata={"unit": "count"}),
                pa.field(f"{SYS}source:abc", pa.string()),
                pa.field(f"{SYS}record_id:abc", pa.string()),
            ],
            metadata={"origin": "a hand-built table"},
        ),
    )


def _reordered(table: pa.Table) -> pa.Table:
    """The same table with its columns in the reverse order."""
    return table.select(list(reversed(table.column_names)))


def _in_two_chunks(table: pa.Table) -> pa.Table:
    """The same rows, held in two chunks."""
    return pa.concat_tables([table.slice(0, 1), table.slice(1)])


def _one_chunk_per_row(table: pa.Table) -> pa.Table:
    """The same rows, one chunk each, as a table built from rows holds them."""
    return pa.concat_tables([table.slice(i, 1) for i in range(table.num_rows)])


def _system_tags_only() -> pa.Table:
    """No user tag: every column is a system tag."""
    return pa.table(
        {
            f"{SYS}source:abc": pa.array(["a", "b"], pa.large_string()),
            f"{SYS}record_id:abc": pa.array(["row_0", None], pa.large_string()),
        }
    )


def _user_tags_only() -> pa.Table:
    """No system tag; a list tag, a binary tag and a tag with no values at all."""
    return pa.table(
        {
            "session": pa.array([["a", "b"], [], None], pa.list_(pa.string())),
            "blob": pa.array([b"x", b"", None], pa.binary()),
            "unknown": pa.array([None, None, None], pa.null()),
            "day": pa.array([1, 2, 3], pa.date32()),
        }
    )


def _tags_with_meta_and_context() -> pa.Table:
    """Tags beside a meta column and each row's own data context."""
    key = contexts.get_default_context_key()
    return pa.table(
        {
            "subject": pa.array(["m1", "m2"], pa.large_string()),
            "trial": pa.array([1, 2], pa.int64()),
            f"{META}note": pa.array(["first", None], pa.string()),
            f"{SYS}source:abc": pa.array(["events", "events"], pa.large_string()),
            CONTEXT: pa.array([key, key], pa.large_string()),
        }
    )


TAG_TABLES: dict[str, Callable[[], pa.Table]] = {
    "user_and_system_tags": _user_and_system_tags,
    "user_and_system_tags_reordered": lambda: _reordered(_user_and_system_tags()),
    "user_and_system_tags_in_two_chunks": lambda: _in_two_chunks(
        _user_and_system_tags()
    ),
    "user_and_system_tags_one_chunk_per_row": lambda: _one_chunk_per_row(
        _user_and_system_tags()
    ),
    "system_tags_only": _system_tags_only,
    "system_tags_only_one_chunk_per_row": lambda: _one_chunk_per_row(
        _system_tags_only()
    ),
    "user_tags_only": _user_tags_only,
    "user_tags_only_reordered": lambda: _reordered(_user_tags_only()),
    "tags_with_meta_and_context": _tags_with_meta_and_context,
    "no_rows": lambda: _user_and_system_tags().slice(0, 0),
}


@pytest.mark.parametrize("name", list(TAG_TABLES))
@pytest.mark.parametrize("given_context", [False, True])
def test_tag_rows_are_the_constructors(name: str, given_context: bool) -> None:
    """Each tag of ``Tag.rows_of`` is ``Tag(row)``, in every attribute and view."""
    context = contexts.resolve_context(None) if given_context else None

    _assert_rows_match_the_constructor(Tag, TAG_TABLES[name](), context)


# --- Tables of data ---------------------------------------------------------------------


def _data_with_some_source_info() -> pa.Table:
    """Three data columns: one with a token, one with a list of tokens, one with none."""
    return pa.table(
        {
            "amplitude": pa.array([0.5, None, 2.5], pa.float32()),
            "note": pa.array(["a", "b", None], pa.string()),
            "trace": pa.array([[1.0], [], None], pa.list_(pa.float64())),
            f"{SRC}amplitude": pa.array(
                ["s::row_0::amplitude", "s::row_1::amplitude", None], pa.string()
            ),
            f"{SRC}trace": pa.array(
                [["s::row_0::trace", "s::row_9::trace"], [], None],
                pa.list_(pa.string()),
            ),
        },
        schema=pa.schema(
            [
                pa.field("amplitude", pa.float32(), metadata={"unit": "mV"}),
                pa.field("note", pa.string()),
                pa.field("trace", pa.list_(pa.float64())),
                pa.field(f"{SRC}amplitude", pa.string()),
                pa.field(f"{SRC}trace", pa.list_(pa.string())),
            ],
            metadata={"origin": "a hand-built table"},
        ),
    )


def _data_with_meta_and_context() -> pa.Table:
    """Data beside two meta columns, a source-info column and the data context."""
    key = contexts.get_default_context_key()
    return pa.table(
        {
            "count": pa.array([1, 2], pa.int8()),
            "label": pa.array(["x", "y"], pa.large_string()),
            f"{META}pod_version": pa.array(["v1", "v1"], pa.string()),
            f"{META}tries": pa.array([1, None], pa.int32()),
            f"{SRC}count": pa.array(["s::row_0::count", "s::row_1::count"]),
            f"{SRC}orphan": pa.array(["no such data column", "x"]),
            CONTEXT: pa.array([key, key], pa.large_string()),
        }
    )


def _data_of_types_normalization_changes() -> pa.Table:
    """Columns a datagram stores in another type: ``null``, small strings, a struct."""
    return pa.table(
        {
            "unknown": pa.array([None, None], pa.null()),
            "lists_of_unknown": pa.array([[None], []], pa.list_(pa.null())),
            "pair": pa.array(
                [{"name": "a", "size": 1}, None],
                pa.struct(
                    [pa.field("name", pa.string()), pa.field("size", pa.int16())]
                ),
            ),
            "raw": pa.array([b"\x00", b""], pa.binary()),
            f"{SRC}unknown": pa.array([None, None], pa.null()),
        },
        schema=pa.schema(
            [
                pa.field("unknown", pa.null()),
                pa.field("lists_of_unknown", pa.list_(pa.null())),
                pa.field(
                    "pair",
                    pa.struct(
                        [pa.field("name", pa.string()), pa.field("size", pa.int16())]
                    ),
                ),
                pa.field("raw", pa.binary(), nullable=False),
                pa.field(f"{SRC}unknown", pa.null()),
            ]
        ),
    )


DATA_TABLES: dict[str, Callable[[], pa.Table]] = {
    "some_source_info": _data_with_some_source_info,
    "some_source_info_reordered": lambda: _reordered(_data_with_some_source_info()),
    "some_source_info_in_two_chunks": lambda: _in_two_chunks(
        _data_with_some_source_info()
    ),
    "some_source_info_one_chunk_per_row": lambda: _one_chunk_per_row(
        _data_with_some_source_info()
    ),
    "meta_and_context": _data_with_meta_and_context,
    "meta_and_context_one_chunk_per_row": lambda: _one_chunk_per_row(
        _data_with_meta_and_context()
    ),
    "meta_and_context_reordered": lambda: _reordered(_data_with_meta_and_context()),
    "types_normalization_changes": _data_of_types_normalization_changes,
    "no_rows": lambda: _data_with_some_source_info().slice(0, 0),
}


@pytest.mark.parametrize("name", list(DATA_TABLES))
@pytest.mark.parametrize("given_context", [False, True])
def test_data_rows_are_the_constructors(name: str, given_context: bool) -> None:
    """Each data of ``Data.rows_of`` is ``Data(row)``, in every attribute and view."""
    context = contexts.resolve_context(None) if given_context else None

    _assert_rows_match_the_constructor(Data, DATA_TABLES[name](), context)


def test_rows_are_separate_objects() -> None:
    """No two rows share a datagram or the mappings it holds."""
    tags = list(Tag.rows_of(_user_and_system_tags()))
    datas = list(Data.rows_of(_data_with_meta_and_context()))

    for rows in (tags, datas):
        assert len({id(row) for row in rows}) == len(rows)
    assert tags[0]._system_tags is not tags[1]._system_tags
    assert datas[0]._source_info is not datas[1]._source_info
    assert datas[0]._meta is not datas[1]._meta
    assert datas[0]._content_hash_cache is not datas[1]._content_hash_cache


def test_rows_are_built_as_they_are_asked_for() -> None:
    """``rows_of`` is lazy: taking the first row builds no other."""
    with mock.patch.object(
        Datagram, "_init_from_row", autospec=True, side_effect=Datagram._init_from_row
    ) as built:
        first = next(Tag.rows_of(_user_and_system_tags()))

    assert built.call_count == 1
    assert first["subject"] == "m1"


@pytest.mark.parametrize("kind", [Tag, Data])
def test_a_table_with_rows_and_no_column_of_its_own_is_refused(
    kind: type[Tag] | type[Data],
) -> None:
    """Only meta or source-info columns: refused, as the constructor refuses the row."""
    table = pa.table(
        {f"{META}note": ["a", "b"]}
        if kind is Tag
        else {f"{SRC}x": ["s::row_0::x", "s::row_1::x"]}
    )

    with pytest.raises(ValueError):
        _ = kind(table.slice(0, 1))
    with pytest.raises(ValueError):
        _ = list(kind.rows_of(table))


# --- A stream's rows --------------------------------------------------------------------


def _source_with_data_tags() -> Any:
    """A source with two tags, one of them a data-tag, and three data columns."""
    return ArrowTableSource(
        pa.table(
            {
                "subject": pa.array(["m1", "m1", "m2", "m2"], pa.string()),
                "trial": pa.array([1, 2, 1, 2], pa.int16()),
                "amplitude": pa.array([0.5, 1.5, None, 3.5], pa.float32()),
                "note": pa.array(["a", None, "c", "d"], pa.string()),
                "trace": pa.array(
                    [[1.0], [], None, [2.0, 3.0]], pa.list_(pa.float64())
                ),
            }
        ),
        tag_columns=["subject", "trial"],
        source_id="events",
    )


def _source_in_another_arrangement() -> Any:
    """The same kind of source, tags last and in the other order, tag-only."""
    return ArrowTableSource(
        pa.table(
            {
                "note": pa.array(["a", None, "c"], pa.large_string()),
                "amplitude": pa.array([0.5, 1.5, None], pa.float64()),
                "trial": pa.array([1, 2, 1], pa.int64()),
                "subject": pa.array(["m1", "m1", "m2"], pa.large_string()),
            }
        ),
        tag_columns=["trial", "subject"],
        tag_only_columns=True,
        record_id_columns=["subject", "trial"],
        source_id="events",
    )


def _joined() -> Any:
    """Two sources joined: two system-tag pairs, source info from both."""
    left = DictSource(
        data=[{"k": i, "g": i % 2, "x": 10 * i} for i in range(4)],
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id="left",
    )
    right = DictSource(
        data=[{"k": i, "y": f"y{i}", "z": [i, i + 1]} for i in range(4)],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="right",
    )
    return left.join(right)


def _grouped() -> Any:
    """A grouped stream: list-valued data and source info, folded system tags."""
    return _joined().group_by(["g"])


def _raw_stream_without_tags() -> Any:
    """A raw stream with no tag column, a meta column and a column with no values."""
    return ArrowTableStream(
        pa.table(
            {
                "x": pa.array([1, 2, 3], pa.int32()),
                "label": pa.array(["a", "b", None], pa.string()),
                "unknown": pa.array([None, None, None], pa.null()),
                f"{META}note": pa.array(["n1", None, "n3"], pa.string()),
            }
        )
    )


def _raw_stream_in_two_chunks() -> Any:
    """A raw stream whose table is held in two chunks, with given source info."""
    table = pa.concat_tables(
        [
            pa.table({"k": ["a", "b"], "t": [1, 2], "x": [1.0, 2.0], "y": ["p", "q"]}),
            pa.table({"k": ["c"], "t": [3], "x": [3.0], "y": ["r"]}),
        ]
    )
    return ArrowTableStream(
        table, tag_columns=["k", "t"], source_info={"x": "given::token::x"}
    )


STREAMS: dict[str, Callable[[], Any]] = {
    "source_with_data_tags": _source_with_data_tags,
    "source_in_another_arrangement": _source_in_another_arrangement,
    "joined": _joined,
    "grouped": _grouped,
    "raw_stream_without_tags": _raw_stream_without_tags,
    "raw_stream_in_two_chunks": _raw_stream_in_two_chunks,
}


def _rows_through_the_constructors(stream: Any) -> list[tuple[Tag, Data]]:
    """The rows of ``stream`` as ``iter_data`` built them before ITL-1348.

    Each row's tag and data are built by the public constructors from one-row tables
    of the stream's own table: its tags and system tags, and its data columns with
    their source info.
    """
    table = stream.as_table(columns={"system_tags": True, "source": True, "meta": True})
    tag_keys, data_keys = stream.keys(columns={"system_tags": True, "meta": True})
    source_keys = [c for c in table.column_names if c.startswith(SRC)]
    context = stream.data_context
    rows: list[tuple[Tag, Data]] = []
    for i in range(table.num_rows):
        row = table.slice(i, 1)
        tag = (
            Tag(row.select(list(tag_keys)), data_context=context)
            if tag_keys
            else Tag({})
        )
        data = Data(row.select([*data_keys, *source_keys]), data_context=context)
        rows.append((tag, data))
    return rows


@pytest.mark.parametrize("name", list(STREAMS))
def test_a_streams_rows_are_the_constructors(name: str) -> None:
    """``iter_data`` yields, for every row, the tag and data the constructors build."""
    stream = STREAMS[name]()
    expected = _rows_through_the_constructors(stream)

    rows = list(stream.iter_data())

    assert len(rows) == len(expected) > 0
    for (tag, data), (expected_tag, expected_data) in zip(rows, expected):
        if stream.keys(columns={"system_tags": True})[0]:
            _assert_same_datagram(tag, expected_tag)
        else:
            # No tag column: the empty tag, which has no table to compare.
            assert tag.as_dict(columns=EVERYTHING) == expected_tag.as_dict(
                columns=EVERYTHING
            )
            assert tag.system_tags() == {}
        _assert_same_datagram(data, expected_data)


@pytest.mark.parametrize(
    "name", [name for name in STREAMS if name != "raw_stream_without_tags"]
)
def test_a_streams_rows_rebuild_its_table(name: str) -> None:
    """The rows' own tables, put side by side, are the stream's table.

    Not for the raw stream with a meta column: a row's data holds that column as meta,
    the stream as a column like any other.
    """
    stream = STREAMS[name]()
    columns = {"system_tags": True, "source": True}
    table = stream.as_table(columns=columns)

    rebuilt = pa.concat_tables(
        [
            arrow_utils.hstack_tables(
                tag.as_table(columns=columns),
                data.as_table(columns=columns).drop_columns(
                    [k for k in data.keys() if k in tag.keys()]
                ),
            )
            for tag, data in stream.iter_data()
        ]
    )

    assert sorted(rebuilt.column_names) == sorted(table.column_names)
    assert rebuilt.select(table.column_names).to_pylist() == table.to_pylist()


# --- The cost: nothing is prepared per row ---------------------------------------------


def _many_rows(n: int, one_chunk_per_row: bool) -> Any:
    """A stream of ``n`` rows over a table of one chunk, or of one chunk per row."""
    table = pa.table(
        {
            "subject": [f"m{i % 3}" for i in range(n)],
            "trial": list(range(n)),
            "amplitude": [0.5 * i for i in range(n)],
            "note": [f"n{i}" for i in range(n)],
        }
    )
    return ArrowTableSource(
        _one_chunk_per_row(table) if one_chunk_per_row else table,
        tag_columns=["subject", "trial"],
        tag_only_columns=True,
        source_id="events",
    )


@pytest.mark.parametrize("one_chunk_per_row", [False, True])
@pytest.mark.parametrize(
    "prepared_per_table",
    ["normalize_schema_to_large_types", "prepare_prefixed_columns"],
)
def test_iterating_rows_prepares_nothing_per_row(
    prepared_per_table: str, one_chunk_per_row: bool
) -> None:
    """Ten times the rows cost not one more call of what is done once per table.

    Also for a table of one chunk per row, which is what a table built from rows (a
    job node's output) is.
    """

    def calls(n: int) -> int:
        stream = _many_rows(n, one_chunk_per_row)
        _ = stream.as_table()
        with mock.patch.object(
            arrow_utils,
            prepared_per_table,
            side_effect=getattr(arrow_utils, prepared_per_table),
        ) as counted:
            assert len(list(stream.iter_data())) == n
        return counted.call_count

    assert calls(50) == calls(5)


def test_rows_of_a_table_built_from_rows_are_the_constructors() -> None:
    """A stream rebuilt from its own rows (one chunk per row) yields the same rows."""
    stream = _joined()
    table = pa.concat_tables(
        [
            arrow_utils.hstack_tables(
                tag.as_table(columns={"system_tags": True}),
                data.as_table(columns={"source": True}),
            )
            for tag, data in stream.iter_data()
        ]
    )
    assert table.column("x").num_chunks == table.num_rows > 1
    rebuilt: Any = ArrowTableStream(table, tag_columns=["k", "g"])

    rows = list(rebuilt.iter_data())

    for (tag, data), (expected_tag, expected_data) in zip(
        rows, _rows_through_the_constructors(rebuilt), strict=True
    ):
        _assert_same_datagram(tag, expected_tag)
        _assert_same_datagram(data, expected_data)


def test_iterating_rows_twice_gives_the_same_objects(tmp_path: Path) -> None:
    """A stream keeps the rows it built: a second pass yields those objects."""
    stream = _source_with_data_tags()

    first = list(stream.iter_data())
    second = list(stream.iter_data())

    assert all(a[0] is b[0] and a[1] is b[1] for a, b in zip(first, second))
