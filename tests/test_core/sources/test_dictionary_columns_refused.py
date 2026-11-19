"""A dictionary-encoded column is refused when a source or stream is built (ITL-1264, ITL-1277).

Developer decisions: dictionary-encoded tags are not currently supported (November 18,
2026), and neither are dictionary-encoded data columns, for now (November 19, 2026, on
ITL-1277: "safe default, though could be inconvenient, is to reject working with it. It'd
be useful if rejection error can suggest how to make it work if staying dict encoding
isn't critical for the user"). Every column of a stream whose Arrow type is, or contains,
a dictionary (top level, list, list-view, fixed-size list, struct child, map key or item,
extension storage) is refused with ``InputValidationError``. The message names each such
column with its role (tag, data-tag, data, or one of Orcapod's own column kinds), its
type and the type it decodes to, then how to decode it. The rule is
``schema_utils.dictionary_columns`` and ``dictionary_columns_error``, applied by
``ArrowTableStream`` (every source and operator result becomes one) and first by the
source builder, so a source reports the dictionary before its key columns and its hash.

A library value in a cell (a pandas or polars data frame holding a categorical column)
is an extension column stored as bytes, not a dictionary: it is accepted and hashes as it
did before.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from orcapod.core.sources import (
    ArrowTableSource,
    DataFrameSource,
    DictSource,
    PollingSource,
)
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import (
    InputValidationError,
    RowIdentifierError,
    SourceCheckFailedWarning,
)
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.types import Cursor, PollingConfig, Schema
from orcapod.contexts import get_default_context
from orcapod.utils import schema_utils
from orcapod.utils.arrow_utils import decode_dictionaries

NOT_SUPPORTED = "dictionary-encoded columns are not currently supported in Orcapod"

# The values of each column, by kind: tags ``a`` and ``b``, data ``v`` and ``w``. Every
# kind has two distinct values per tag column so the rows stay distinct whichever
# columns are encoded.
VALUES: dict[str, tuple[pa.DataType, dict[str, list[Any]]]] = {
    "string": (
        pa.string(),
        {
            "a": ["x", "y", "x"],
            "b": ["p", "p", "q"],
            "v": ["r", "s", "r"],
            "w": ["m", "n", "n"],
        },
    ),
    "int": (
        pa.int64(),
        {"a": [1, 2, 1], "b": [7, 7, 8], "v": [3, 4, 3], "w": [5, 5, 6]},
    ),
    "float": (
        pa.float64(),
        {
            "a": [1.5, 2.5, 1.5],
            "b": [0.25, 0.25, 0.75],
            "v": [3.5, 4.5, 3.5],
            "w": [5.5, 5.5, 6.5],
        },
    ),
}
TAG_ORDERS = [("a", "b"), ("b", "a")]
# Tags, data, both, one of each, all.
ENCODED = [("a",), ("a", "b"), ("v",), ("v", "w"), ("b", "w"), ("a", "b", "v", "w")]
DATA = ("v", "w")
ALL: tuple[str, ...] = ("a", "b", "v", "w")


def _table(kind: str, encoded: tuple[str, ...], data_first: bool = False) -> pa.Table:
    """Tags ``a``, ``b`` and data ``v``, ``w`` (those in ``encoded`` dictionary-encoded),
    in one of two layouts."""
    value_type, values = VALUES[kind]
    columns: dict[str, pa.Array[Any]] = {}
    for name in ALL:
        array = pa.array(values[name], value_type)
        columns[name] = array.dictionary_encode() if name in encoded else array
    order = ["v", "w", "a", "b"] if data_first else ["a", "b", "v", "w"]
    return pa.table([columns[name] for name in order], names=order)


def _expected(
    tags: tuple[str, ...], encoded: tuple[str, ...], data_tags: tuple[str, ...]
) -> list[tuple[str, str]]:
    """The ``(role, name)`` the message names, in its order: tags in tag order, then data
    columns in table order (``v`` before ``w`` in both layouts)."""
    out = [("data-tag" if t in data_tags else "tag", t) for t in tags if t in encoded]
    out += [("data", d) for d in DATA if d in encoded]
    return out


def _assert_refused(
    exc: BaseException, expected: list[tuple[str, str]], all_names: tuple[str, ...]
) -> None:
    """The message says dictionary columns are not supported, names each encoded column
    with its role and type in the expected order, names no other column, and says how
    to decode."""
    assert isinstance(exc, InputValidationError)
    assert not isinstance(exc, RowIdentifierError)
    message = str(exc)
    assert message.startswith(NOT_SUPPORTED)
    positions = [
        message.index(f"- {role} column {name!r} has type dictionary<")
        for role, name in expected
    ]
    assert positions == sorted(positions)
    named = {name for _, name in expected}
    for name in all_names:
        if name not in named:
            assert f"column {name!r}" not in message
    assert (
        "table.set_column(i, field.with_type(decoded), column.cast(decoded))" in message
    )
    assert "chunk.dictionary_decode()" in message
    assert "pyarrow.compute.run_end_decode" in message
    assert "df = df.with_columns(pl.col(name).cast(pl.String))" in message


# ── a raw ArrowTableStream ──────────────────────────────────────────────────


@pytest.mark.parametrize("data_first", [False, True], ids=["tags-first", "data-first"])
@pytest.mark.parametrize(
    "data_tags", [(), ("a",), ("a", "b")], ids=["plain", "a", "ab"]
)
@pytest.mark.parametrize("encoded", ENCODED, ids="-".join)
@pytest.mark.parametrize("tags", TAG_ORDERS, ids="-".join)
@pytest.mark.parametrize("kind", sorted(VALUES))
def test_a_raw_stream_refuses_dictionary_columns(
    kind: str,
    tags: tuple[str, ...],
    encoded: tuple[str, ...],
    data_tags: tuple[str, ...],
    data_first: bool,
) -> None:
    with pytest.raises(InputValidationError) as info:
        ArrowTableStream(
            _table(kind, encoded, data_first), tag_columns=tags, data_tags=data_tags
        )
    _assert_refused(info.value, _expected(tags, encoded, data_tags), ALL)


# ── ArrowTableSource ────────────────────────────────────────────────────────


TAG_ONLY: dict[str, tuple[Any, tuple[str, ...]]] = {
    "tag-only": (True, ()),
    "data-tags": (False, ("a", "b")),
    "a-plain": (["a"], ("b",)),
    "b-plain": (["b"], ("a",)),
}


@pytest.mark.parametrize("roles", sorted(TAG_ONLY))
@pytest.mark.parametrize("encoded", ENCODED, ids="-".join)
@pytest.mark.parametrize("tags", TAG_ORDERS, ids="-".join)
@pytest.mark.parametrize("kind", sorted(VALUES))
def test_an_arrow_table_source_refuses_dictionary_columns(
    kind: str, tags: tuple[str, ...], encoded: tuple[str, ...], roles: str
) -> None:
    tag_only_columns, data_tags = TAG_ONLY[roles]
    with pytest.raises(InputValidationError) as info:
        ArrowTableSource(
            _table(kind, encoded),
            tag_columns=list(tags),
            tag_only_columns=tag_only_columns,
        )
    _assert_refused(info.value, _expected(tags, encoded, data_tags), ALL)


@pytest.mark.parametrize("record_id_columns", [["a"], ["b", "a"], ["a", "b"]])
@pytest.mark.parametrize("kind", ["string", "int"])
def test_a_dictionary_key_column_is_reported_as_a_dictionary_column(
    kind: str, record_id_columns: list[str]
) -> None:
    """The builder checks first: not the key-column error, which would say a key must
    hold strings or integers although the dictionary's values are."""
    with pytest.raises(InputValidationError) as info:
        ArrowTableSource(
            _table(kind, ("a",)),
            tag_columns=["a", "b"],
            tag_only_columns=True,
            record_id_columns=record_id_columns,
        )
    _assert_refused(info.value, [("tag", "a")], ALL)


@pytest.mark.parametrize("encoded", [("b",), ("w",)], ids=["tag", "data"])
def test_a_dictionary_column_is_refused_before_anything_is_hashed(
    monkeypatch: pytest.MonkeyPatch, encoded: tuple[str, ...]
) -> None:
    from orcapod.core.sources import stream_builder

    def _never(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("the source hashed before checking its columns")

    monkeypatch.setattr(stream_builder, "row_identifiers", _never)
    # No table or column hash either (the default source id, a key column check).
    monkeypatch.setattr(get_default_context().arrow_hasher, "hash_table", _never)
    with pytest.raises(InputValidationError, match=NOT_SUPPORTED):
        ArrowTableSource(
            _table("string", encoded), tag_columns=["a", "b"], tag_only_columns=True
        )


def test_the_message_gives_each_role_type_and_decoded_type() -> None:
    table = pa.table(
        {
            "a": pa.array(["x", "y"]).dictionary_encode(),
            "b": pa.array([1.5, 2.5])
            .dictionary_encode()
            .cast(pa.dictionary(pa.int8(), pa.float64())),
            "v": pa.array([1, 2]).dictionary_encode(),
            "w": [1, 2],
        }
    )
    with pytest.raises(InputValidationError) as info:
        ArrowTableSource(table, tag_columns=["b", "a"], tag_only_columns=["b"])
    message = str(info.value)
    assert message.splitlines()[:4] == [
        f"{NOT_SUPPORTED}:",
        "  - tag column 'b' has type dictionary<values=double, indices=int8, "
        + "ordered=0> (decoded: double)",
        "  - data-tag column 'a' has type dictionary<values=string, indices=int32, "
        + "ordered=0> (decoded: string)",
        "  - data column 'v' has type dictionary<values=int64, indices=int32, "
        + "ordered=0> (decoded: int64)",
    ]
    assert "column 'w'" not in message


# ── Orcapod's own column kinds in a hand-built table ─────────────────────────


@pytest.mark.parametrize(
    ("column", "role", "kwargs"),
    [
        ("rid", "system-tag", {"system_tag_columns": ["rid"]}),
        ("_tag_source_id::abc", "system-tag", {}),
        ("_source_v", "source-info", {}),
        ("__note", "meta", {}),
        ("_context_key", "data context", {}),
    ],
    ids=["named-system-tag", "prefixed-system-tag", "source-info", "meta", "context"],
)
def test_a_raw_stream_refuses_a_dictionary_in_orcapods_own_columns(
    column: str, role: str, kwargs: dict[str, Any]
) -> None:
    """Orcapod writes these as plain strings; only a hand-built table can hold a
    dictionary there, and it is refused the same way, with its role."""
    table = pa.table(
        {
            "a": ["x", "y"],
            "b": ["p", "q"],
            "v": [1.0, 2.0],
            "w": [3, 4],
            column: pa.array(["c1", "c2"]).dictionary_encode(),
        }
    )
    with pytest.raises(InputValidationError) as info:
        ArrowTableStream(table, tag_columns=["a", "b"], **kwargs)
    assert f"- {role} column {column!r} has type dictionary<" in str(info.value)


OWN_COLUMNS = {
    "__note": "meta",
    "_source_v": "source-info",
    "_tag_source_id::abc": "system-tag",
}


@pytest.mark.parametrize(
    "own", [("__note",), ("_source_v",), ("_tag_source_id::abc",), tuple(OWN_COLUMNS)]
)
@pytest.mark.parametrize("with_user_column", [False, True], ids=["alone", "beside-v"])
@pytest.mark.parametrize("own_first", [False, True], ids=["user-first", "own-first"])
def test_a_source_refuses_a_dictionary_in_hand_built_system_columns(
    own: tuple[str, ...], with_user_column: bool, own_first: bool
) -> None:
    """The builder checks before it drops system columns (CodeRabbit on PR 455): a
    dictionary-free system column is still stripped, a dictionary one is refused."""
    user: dict[str, pa.Array[Any]] = {
        "a": pa.array(["x", "y"]),
        "b": pa.array(["p", "q"]),
        "v": pa.array(["r", "s"]).dictionary_encode()
        if with_user_column
        else pa.array(["r", "s"]),
        "w": pa.array([3, 4]),
    }
    system = {name: pa.array(["c1", "c2"]).dictionary_encode() for name in own}
    columns = {**system, **user} if own_first else {**user, **system}
    table = pa.table(list(columns.values()), names=list(columns))
    with pytest.raises(InputValidationError) as info:
        ArrowTableSource(table, tag_columns=["a", "b"], tag_only_columns=True)
    message = str(info.value)
    for name in own:
        assert f"- {OWN_COLUMNS[name]} column {name!r} has type dictionary<" in message
    assert ("- data column 'v'" in message) == with_user_column


def test_a_dictionary_free_system_column_is_still_stripped() -> None:
    table = pa.table(
        {
            "a": ["x", "y"],
            "__note": ["n1", "n2"],
            "_source_v": ["s1", "s2"],
            "v": [1, 2],
        }
    )
    source = ArrowTableSource(table, tag_columns=["a"], tag_only_columns=True)
    assert source.keys() == (("a",), ("v",))


def test_an_opaque_type_over_a_dictionary_is_refused() -> None:
    """``pa.opaque`` is a built-in extension type, not a Python ``pa.ExtensionType``."""
    opaque = pa.opaque(pa.dictionary(pa.int32(), pa.string()), "label", "vendor")
    storage = pa.array(["x", "y"]).dictionary_encode()
    column = pa.ExtensionArray.from_storage(opaque, storage)
    table = pa.table({"a": ["p", "q"], "o": column, "v": [1, 2]})
    with pytest.raises(InputValidationError, match="- data column 'o' has type"):
        ArrowTableStream(table, tag_columns=["a"])


# ── data frames ─────────────────────────────────────────────────────────────


FRAME_COLUMNS: dict[str, list[Any]] = {
    "a": ["x", "y", "x"],
    "b": ["p", "p", "q"],
    "v": ["r", "s", "r"],
    "w": [1, 2, 3],
}
# String columns only: a pandas Categorical of integers reaches the source as plain
# integers (the frame goes through polars, which has no integer Categorical).
FRAME_ENCODED = [("a",), ("v",), ("a", "b"), ("b", "v")]


def _pandas(encoded: tuple[str, ...]) -> pd.DataFrame:
    frame = pd.DataFrame(FRAME_COLUMNS)
    for name in encoded:
        frame[name] = frame[name].astype("category")
    return frame


def _polars(encoded: tuple[str, ...], dtype: str) -> pl.DataFrame:
    frame = pl.DataFrame(FRAME_COLUMNS)
    target = (
        pl.Categorical()
        if dtype == "Categorical"
        else pl.Enum(["p", "q", "r", "s", "x", "y"])
    )
    return frame.with_columns([pl.col(name).cast(target) for name in encoded])


@pytest.mark.parametrize(
    "tag_only_columns", [True, False], ids=["tag-only", "data-tags"]
)
@pytest.mark.parametrize("encoded", FRAME_ENCODED, ids="-".join)
@pytest.mark.parametrize("tags", TAG_ORDERS, ids="-".join)
@pytest.mark.parametrize("frame_kind", ["pandas", "polars-Categorical", "polars-Enum"])
def test_a_data_frame_source_refuses_categorical_columns(
    frame_kind: str,
    tags: tuple[str, ...],
    encoded: tuple[str, ...],
    tag_only_columns: bool,
) -> None:
    """A pandas Categorical and a polars Categorical or Enum arrive dictionary-encoded."""
    frame: Any = (
        _pandas(encoded)
        if frame_kind == "pandas"
        else _polars(encoded, frame_kind.split("-")[1])
    )
    with pytest.raises(InputValidationError) as info:
        DataFrameSource(
            frame, tag_columns=list(tags), tag_only_columns=tag_only_columns
        )
    data_tags = () if tag_only_columns else tags
    _assert_refused(info.value, _expected(tags, encoded, data_tags), ALL)


# ── a polling source's batches, read sync and async ─────────────────────────


class _CategoricalImpl:
    """Fetches one batch whose column ``column`` is a polars Categorical."""

    def __init__(self, column: str) -> None:
        self.column = column

    def identity(self) -> str:
        return f"categorical-impl-{self.column}"

    def to_config(self) -> None:
        return None

    @classmethod
    def from_config(cls, config: Any) -> _CategoricalImpl:
        return cls("a")

    def schema(self) -> Schema:
        # Declared, so building a pipeline over the source reads no batch: the job
        # run reads the first one.
        return Schema({"a": str, "b": str, "v": float, "s": str})

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        return cursor is None

    async def fetch(self, cursor: Cursor[int] | None = None) -> tuple[Cursor[int], Any]:
        if cursor is not None:
            return Cursor(value=1), {}
        frame = pl.DataFrame(
            {"a": ["x", "y"], "b": ["p", "q"], "v": [1.0, 2.0], "s": ["m", "n"]}
        ).with_columns(pl.col(self.column).cast(pl.Categorical()))
        return Cursor(value=1), frame

    async def close(self) -> None:
        return None


def _polling_source(column: str) -> PollingSource[int]:
    return PollingSource(
        _CategoricalImpl(column),
        tag_columns=["b", "a"],
        record_id_columns="b",
        polling_config=PollingConfig(duration=0.01),
    )


# A polling source keeps its tags plain (ITL-1023).
POLLED = pytest.mark.parametrize(
    ("column", "role"), [("a", "tag"), ("s", "data")], ids=["tag", "data"]
)


@POLLED
def test_a_polling_source_refuses_a_categorical_column_on_a_sync_read(
    column: str, role: str
) -> None:
    with pytest.raises(InputValidationError) as info:
        _polling_source(column).as_table()
    _assert_refused(info.value, [(role, column)], ("a", "b", "v", "s"))


@POLLED
@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_a_job_over_a_polling_source_refuses_a_categorical_column(
    orchestrator: type, column: str, role: str
) -> None:
    job = PipelineJob(name="itl1264", database=InMemoryArrowDatabase())
    with job:
        _polling_source(column).select_data_columns("v", label="selected")
    # The run's start-of-run check warns and goes on; the read of the batch raises.
    with (
        pytest.warns(SourceCheckFailedWarning, match=NOT_SUPPORTED),
        pytest.raises(InputValidationError, match=f"{role} column '{column}'"),
    ):
        if orchestrator is AsyncPipelineOrchestrator:
            asyncio.run(job.run_async())
        else:
            job.run(orchestrator=orchestrator())


# ── a column whose type contains a dictionary ───────────────────────────────


def _nested(kind: str) -> pa.Array[Any]:
    encoded = pa.array(["x", "y", "x"]).dictionary_encode()
    if kind == "list":
        return pa.LargeListArray.from_arrays(
            pa.array([0, 1, 2, 3], pa.int64()), encoded
        )
    if kind == "struct":
        return pa.StructArray.from_arrays(
            [encoded, pa.array([1, 2, 3])], names=["k", "n"]
        )
    if kind == "list_view":
        return pa.ListViewArray.from_arrays(  # pyright: ignore[reportCallIssue, reportUnknownVariableType]  # the pyarrow stubs omit from_arrays' sizes argument
            pa.array([0, 1, 2], pa.int32()), pa.array([1, 1, 1], pa.int32()), encoded
        )
    if kind == "run_end_encoded":
        return pa.RunEndEncodedArray.from_arrays(
            pa.array([1, 2, 3], pa.int32()), encoded
        )
    if kind == "large_list_view":
        return pa.LargeListViewArray.from_arrays(  # pyright: ignore[reportCallIssue, reportUnknownVariableType]  # the pyarrow stubs omit from_arrays' sizes argument
            pa.array([0, 1, 2], pa.int64()), pa.array([1, 1, 1], pa.int64()), encoded
        )
    raise ValueError(kind)


DECODED = {
    "list": "large_list<item: string>",
    "struct": "struct<k: string, n: int64>",
    "list_view": "list_view<item: string>",
    "large_list_view": "large_list_view<item: string>",
    "run_end_encoded": "run_end_encoded<run_ends: int32, values: string>",
}
NESTED_KINDS = pytest.mark.parametrize("kind", sorted(DECODED))

# Several items (CodeRabbit on PR 455): two tags and two data columns, both tag orders,
# two table layouts, and the three roles of the tags.
NESTED_TAG_ORDERS = [("a", "b"), ("b", "a")]
NESTED_LAYOUTS = {
    "tags_first": ["a", "b", "v", "w"],
    "interleaved": ["v", "a", "w", "b"],
}
NESTED_ROLES = ["plain", "data_tags", "mixed"]
NESTED_ARRANGEMENTS = pytest.mark.parametrize(
    ("tag_order", "layout", "roles"),
    [
        (order, layout, roles)
        for order in NESTED_TAG_ORDERS
        for layout in NESTED_LAYOUTS
        for roles in NESTED_ROLES
    ],
    ids=lambda value: "-".join(value) if isinstance(value, tuple) else str(value),
)


def _nested_table(nested: pa.Array[Any], column: str, layout: str) -> pa.Table:
    """Tags ``a``, ``b`` and data ``v``, ``w`` in ``layout``; ``column`` holds ``nested``."""
    columns: dict[str, pa.Array[Any]] = {
        "a": pa.array(["x", "y", "z"], pa.large_string()),
        "b": pa.array([1, 2, 3], pa.int64()),
        "v": pa.array([1.0, 2.0, 3.0], pa.float64()),
        "w": pa.array(["p", "q", "r"], pa.large_string()),
    }
    columns[column] = nested
    names = NESTED_LAYOUTS[layout]
    return pa.table([columns[name] for name in names], names=names)


def _build_nested(
    build: str, table: pa.Table, tag_order: tuple[str, str], roles: str
) -> None:
    """Build ``table`` as a source or a raw stream with its tags in ``roles``.

    ``mixed`` keeps ``b`` plain and makes ``a`` a data-tag.
    """
    if build == "source":
        tag_only: bool | list[str] = {
            "plain": True,
            "data_tags": False,
            "mixed": ["b"],
        }[roles]
        ArrowTableSource(table, tag_columns=list(tag_order), tag_only_columns=tag_only)
    else:
        data_tags = {"plain": (), "data_tags": tag_order, "mixed": ("a",)}[roles]
        ArrowTableStream(table, tag_columns=list(tag_order), data_tags=data_tags)


@NESTED_ARRANGEMENTS
@pytest.mark.parametrize("column", ["a", "w"], ids=["in-tag", "in-data"])
@pytest.mark.parametrize("build", ["source", "stream"])
@NESTED_KINDS
def test_a_column_containing_a_dictionary_is_refused(
    kind: str,
    build: str,
    column: str,
    tag_order: tuple[str, str],
    layout: str,
    roles: str,
) -> None:
    table = _nested_table(_nested(kind), column, layout)
    with pytest.raises(InputValidationError) as info:
        _build_nested(build, table, tag_order, roles)
    message = str(info.value)
    assert NOT_SUPPORTED in message
    assert re.search(rf"column '{column}' has type \S.*dictionary<", message)
    if column == "w":
        assert "- data column 'w'" in message
    if build == "source":
        assert f"(decoded: {DECODED[kind]})" in message
    for other in set(ALL) - {column}:
        assert f"column {other!r}" not in message


def _plain_list_view(kind: str) -> pa.Array[Any]:
    plain = pa.array(["x", "y", "x"])
    offsets, sizes = (
        (pa.array([0, 1, 2], pa.int32()), pa.array([1, 1, 1], pa.int32()))
        if kind == "list_view"
        else (pa.array([0, 1, 2], pa.int64()), pa.array([1, 1, 1], pa.int64()))
    )
    return (
        pa.ListViewArray if kind == "list_view" else pa.LargeListViewArray
    ).from_arrays(offsets, sizes, plain)  # pyright: ignore[reportCallIssue, reportUnknownVariableType]  # the pyarrow stubs omit from_arrays' sizes argument


@NESTED_ARRANGEMENTS
@pytest.mark.parametrize("column", ["a", "w"], ids=["in-tag", "in-data"])
@pytest.mark.parametrize("kind", ["list_view", "large_list_view"])
def test_a_list_view_without_a_dictionary_is_handled_as_before(
    kind: str, column: str, tag_order: tuple[str, str], layout: str, roles: str
) -> None:
    """Only a dictionary inside a list view is refused here (CodeRabbit on PR 455).

    A list view of plain values keeps what it had: a raw stream holds it as given,
    and a source refuses it because the Arrow hasher cannot hash it (ITL-1129).
    """
    table = _nested_table(_plain_list_view(kind), column, layout)
    _build_nested("stream", table, tag_order, roles)
    with pytest.raises(ValueError, match="the Arrow hasher refused") as info:
        _build_nested("source", table, tag_order, roles)
    assert NOT_SUPPORTED not in str(info.value)


# ── the decoding the message suggests works ─────────────────────────────────


@pytest.mark.parametrize("kind", sorted(VALUES))
def test_a_table_decoded_with_pyarrow_builds(kind: str) -> None:
    table = _table(kind, ALL)
    for name in ("b", "w", "a", "v"):
        index = table.schema.get_field_index(name)
        table = table.set_column(
            index, name, table[name].cast(table[name].type.value_type)
        )
    source = ArrowTableSource(table, tag_columns=["a", "b"], tag_only_columns=True)
    assert source.as_table(all_info=True).num_rows == 3


def _decoded_type(message: str, name: str) -> str:
    """The decoded type the error message gives for column ``name``."""
    line = next(
        text for text in message.splitlines() if f"column {name!r} has type" in text
    )
    return line.rsplit("(decoded: ", 1)[1].rstrip(")")


@pytest.mark.parametrize("kind", ["list", "struct"])
def test_a_nested_dictionary_decoded_as_the_message_says_builds(kind: str) -> None:
    """The recipe for a column holding a dictionary inside it: cast it to the decoded type
    the message gives, keeping the field (nullability and metadata).

    Lists and structs: pyarrow cannot cast a list view or a run-end-encoded column, and a
    source refuses those types whatever they hold (the Arrow hasher cannot hash them).
    """
    nested = _nested(kind)
    table = _nested_table(nested, "w", "tags_first")
    schema = table.schema.set(
        table.schema.get_field_index("w"),
        table.schema.field("w").with_nullable(False).with_metadata({"note": "kept"}),
    )
    table = pa.Table.from_arrays(table.columns, schema=schema)
    with pytest.raises(InputValidationError) as info:
        ArrowTableSource(table, tag_columns=["a", "b"], tag_only_columns=True)
    assert _decoded_type(str(info.value), "w") == DECODED[kind]
    decoded = decode_dictionaries(table.schema.field("w").type)
    assert str(decoded) == DECODED[kind]
    field = table.schema.field("w")
    table = table.set_column(
        table.schema.get_field_index("w"),
        field.with_type(decoded),
        table["w"].cast(decoded),
    )
    stream = ArrowTableStream(table, tag_columns=["a", "b"])
    kept = stream.as_table().schema.field("w")
    assert not kept.nullable
    assert (kept.metadata or {}).get(b"note") == b"kept"


def _replace(table: pa.Table, name: str, column: Any) -> pa.Table:
    """Replace a column keeping its field: name, nullability and metadata."""
    field = table.schema.field(name)
    return table.set_column(
        table.schema.get_field_index(name), field.with_type(column.type), column
    )


def _recover(kind: str, column: Any) -> Any:
    """The recipe the message and the troubleshooting page give, per kind of column."""
    import pyarrow.compute as pc

    if kind == "dictionary of a dictionary":
        # The values hold a dictionary: decode the top level first, then cast.
        return pa.chunked_array(
            [chunk.dictionary_decode() for chunk in column.chunks]
        ).cast(decode_dictionaries(column.type))
    if kind == "list_view":
        # A source refuses list views; a large list of the decoded items it accepts.
        return column.cast(pa.large_list(decode_dictionaries(column.type.value_type)))
    if kind == "run_end_encoded":
        # Decode the values, then expand the runs.
        return pa.chunked_array(
            [
                pc.run_end_decode(  # pyright: ignore[reportAttributeAccessIssue]  # a generated compute function the stubs omit
                    pa.RunEndEncodedArray.from_arrays(
                        chunk.run_ends,
                        chunk.values.cast(decode_dictionaries(chunk.values.type)),
                    )
                )
                for chunk in column.chunks
            ]
        )
    return column.cast(decode_dictionaries(column.type))


def _special(kind: str) -> pa.Array[Any]:
    encoded = pa.array(["x", "y"]).dictionary_encode()
    if kind == "dictionary of a dictionary":
        values = pa.StructArray.from_arrays([encoded], names=["k"])
        return pa.DictionaryArray.from_arrays(pa.array([0, 1, 0], pa.int32()), values)
    if kind == "list_view":
        return pa.ListViewArray.from_arrays(  # pyright: ignore[reportCallIssue, reportUnknownVariableType]  # the pyarrow stubs omit from_arrays' sizes argument
            pa.array([0, 1, 0], pa.int32()), pa.array([1, 1, 2], pa.int32()), encoded
        )
    if kind == "run_end_encoded":
        return pa.RunEndEncodedArray.from_arrays(pa.array([1, 3], pa.int32()), encoded)
    raise ValueError(kind)


@pytest.mark.parametrize(
    "kind", ["dictionary of a dictionary", "list_view", "run_end_encoded"]
)
def test_the_columns_a_plain_cast_cannot_decode_have_a_recipe_that_works(
    kind: str,
) -> None:
    """pyarrow cannot cast these to their decoded type; the stated steps decode them."""
    table = pa.table(
        {
            "a": ["p", "q", "r"],
            "b": [1, 2, 3],
            "c": _special(kind),
            "w": [0.5, 1.5, 2.5],
        }
    )
    with pytest.raises(InputValidationError):
        ArrowTableSource(table, tag_columns=["a", "b"], tag_only_columns=True)
    with pytest.raises(pa.ArrowNotImplementedError):
        table["c"].cast(decode_dictionaries(table.schema.field("c").type))
    decoded = _replace(table, "c", _recover(kind, table["c"]))
    assert not schema_utils.dictionary_columns(decoded.schema)
    source = ArrowTableSource(decoded, tag_columns=["a", "b"], tag_only_columns=True)
    assert source.as_table().num_rows == 3


@pytest.mark.parametrize("dtype", ["Categorical", "Enum"])
def test_a_frame_decoded_with_polars_builds(dtype: str) -> None:
    frame = _polars(("a", "b", "v"), dtype).with_columns(
        pl.col("a", "b", "v").cast(pl.String)
    )
    source = DataFrameSource(frame, tag_columns=["b", "a"], tag_only_columns=True)
    assert source.as_table(all_info=True).num_rows == 3


def test_a_frame_decoded_with_pandas_builds() -> None:
    frame = _pandas(("a", "b", "v"))
    for name in ("a", "b", "v"):
        frame[name] = frame[name].astype(frame[name].cat.categories.dtype)
    source = DataFrameSource(frame, tag_columns=["a", "b"], tag_only_columns=True)
    assert source.as_table(all_info=True).num_rows == 3


# ── no operator makes a dictionary column ───────────────────────────────────


def test_tag_by_never_sees_a_dictionary_data_column() -> None:
    """The source holding one is refused, so ``TagBy`` cannot be applied to it."""
    with pytest.raises(InputValidationError, match="- data column 'v'"):
        ArrowTableSource(
            _table("float", ("v",)), tag_columns=["a", "b"], tag_only_columns=True
        )


# ── a library value in a cell is not a dictionary column ─────────────────────

# The data hash of each row, computed on main (84cc52cd) before this change.
CELL_HASHES = {
    "pandas": "arrow_v1:00000244fe7fa574e19c2f88db92ca42d2391aa3892d3f2f27a338d758531bae57cf46",
    "polars": "arrow_v1:00000202d6fc73547e8eb457800df1c8d39b5cee3e0168b779ceb02a70e7112f7523fb",
}


def _frames() -> dict[str, Any]:
    return {
        "pandas": pd.DataFrame(
            {
                "c": pd.Categorical(["x", "y", "x"], categories=["y", "x", "z"]),
                "n": [1, 2, 3],
            }
        ),
        "polars": pl.DataFrame(
            {
                "c": pl.Series(["p", None, "q"], dtype=pl.Categorical),
                "e": pl.Series(["x", "y", None], dtype=pl.Enum(["y", "x", "z"])),
            }
        ),
    }


@pytest.mark.parametrize("library", sorted(CELL_HASHES))
def test_a_data_frame_value_with_categorical_columns_is_accepted_and_hashes_as_before(
    library: str,
) -> None:
    """The boundary: a data frame in a cell is a library value, stored as bytes in an
    extension column, so its categorical columns are not a dictionary column."""
    source = DictSource(
        [{"k": "r1", "f": _frames()[library]}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="cells",
    )
    assert not pa.types.is_dictionary(source.as_table().schema.field("f").type)
    ((_, data),) = list(source.iter_data())
    assert data.content_hash().to_string() == CELL_HASHES[library]
