"""A dictionary-encoded tag is refused where a stream gets its tags (ITL-1264).

Developer decision, November 18, 2026: dictionary-encoded tags are not currently
supported in Orcapod. A tag column whose Arrow type is, or contains, a dictionary is
refused with ``InputValidationError`` when a source or a stream is built, naming every
such column with its type and its decoded type, instead of failing far from the cause
(a sort, a nullability restore, ``as_table(all_info=True)``, ``Batch``, ``GroupBy``).
The rule is one function, ``schema_utils.require_tags_not_dictionary``, called by
``ArrowTableStream`` (every source and operator result becomes one) and first by the
source builder, so a source reports the dictionary before its key columns and its hash.

Dictionary-encoded DATA columns are not covered by the decision and are accepted as
before; a data-tag is a tag, so it is refused.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from orcapod.core.sources import ArrowTableSource, DataFrameSource, PollingSource
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

NOT_SUPPORTED = "dictionary-encoded tags are not currently supported in Orcapod"

# The values of each tag column, by kind; every kind has two distinct values per
# column so the rows stay distinct whichever columns are encoded.
VALUES: dict[str, tuple[pa.DataType, list[Any], list[Any]]] = {
    "string": (pa.string(), ["x", "y", "x"], ["p", "p", "q"]),
    "int": (pa.int64(), [1, 2, 1], [7, 7, 8]),
    "float": (pa.float64(), [1.5, 2.5, 1.5], [0.25, 0.25, 0.75]),
}
TAG_ORDERS = [("a", "b"), ("b", "a")]
ENCODED = [("a",), ("b",), ("a", "b")]


def _table(kind: str, encoded: tuple[str, ...], data_first: bool = False) -> pa.Table:
    """Two tag columns ``a`` and ``b`` (those in ``encoded`` dictionary-encoded) and two
    data columns ``v`` and ``w``, in one of two layouts."""
    value_type, a_values, b_values = VALUES[kind]
    columns: dict[str, pa.Array[Any]] = {}
    for name, values in (("a", a_values), ("b", b_values)):
        array = pa.array(values, value_type)
        columns[name] = array.dictionary_encode() if name in encoded else array
    data = {
        "v": pa.array([1.0, 2.0, 3.0], pa.float64()),
        "w": pa.array(["r", "s", "t"], pa.large_string()),
    }
    ordered = {**data, **columns} if data_first else {**columns, **data}
    return pa.table(ordered)


def _assert_refused(
    exc: BaseException, encoded_in_order: tuple[str, ...], plain: tuple[str, ...]
) -> None:
    """The message says dictionary tags are not supported, names each encoded column
    in tag order with its type, and names no plain tag."""
    assert isinstance(exc, InputValidationError)
    assert not isinstance(exc, RowIdentifierError)
    message = str(exc)
    assert NOT_SUPPORTED in message
    positions = [
        message.index(f"tag column {name!r} has type dictionary<")
        for name in encoded_in_order
    ]
    assert positions == sorted(positions)
    for name in plain:
        assert f"tag column {name!r}" not in message


def _in_tag_order(tags: tuple[str, ...], encoded: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(t for t in tags if t in encoded)


# ── a raw ArrowTableStream ──────────────────────────────────────────────────


@pytest.mark.parametrize("data_first", [False, True])
@pytest.mark.parametrize("data_tags", [(), ("a",), ("a", "b")])
@pytest.mark.parametrize("encoded", ENCODED)
@pytest.mark.parametrize("tags", TAG_ORDERS)
@pytest.mark.parametrize("kind", sorted(VALUES))
def test_a_raw_stream_refuses_dictionary_tags(
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
    plain = tuple(t for t in tags if t not in encoded)
    _assert_refused(info.value, _in_tag_order(tags, encoded), plain)


# ── ArrowTableSource ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "tag_only_columns",
    [True, False, ["a"], ["b"]],
    ids=["tag-only", "data-tags", "a-plain", "b-plain"],
)
@pytest.mark.parametrize("encoded", ENCODED)
@pytest.mark.parametrize("tags", TAG_ORDERS)
@pytest.mark.parametrize("kind", sorted(VALUES))
def test_an_arrow_table_source_refuses_dictionary_tags(
    kind: str, tags: tuple[str, ...], encoded: tuple[str, ...], tag_only_columns: Any
) -> None:
    """Plain tags and data-tags alike: a data-tag is a tag."""
    with pytest.raises(InputValidationError) as info:
        ArrowTableSource(
            _table(kind, encoded),
            tag_columns=list(tags),
            tag_only_columns=tag_only_columns,
        )
    plain = tuple(t for t in tags if t not in encoded)
    _assert_refused(info.value, _in_tag_order(tags, encoded), plain)


@pytest.mark.parametrize("record_id_columns", [["a"], ["b", "a"], ["a", "b"]])
@pytest.mark.parametrize("kind", ["string", "int"])
def test_a_dictionary_key_column_is_reported_as_a_dictionary_tag(
    kind: str, record_id_columns: list[str]
) -> None:
    """The builder checks the tags first: not the key-column error, which would say a
    key must hold strings or integers although the dictionary's values are."""
    with pytest.raises(InputValidationError) as info:
        ArrowTableSource(
            _table(kind, ("a",)),
            tag_columns=["a", "b"],
            tag_only_columns=True,
            record_id_columns=record_id_columns,
        )
    _assert_refused(info.value, ("a",), ("b",))


def test_a_dictionary_tag_is_refused_before_anything_is_hashed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from orcapod.core.sources import stream_builder

    def _never(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("the source hashed before checking its tags")

    monkeypatch.setattr(stream_builder, "row_identifiers", _never)
    with pytest.raises(InputValidationError, match=NOT_SUPPORTED):
        ArrowTableSource(
            _table("string", ("b",)), tag_columns=["a", "b"], tag_only_columns=True
        )


def test_the_message_gives_the_type_and_its_decoded_type() -> None:
    table = pa.table(
        {
            "a": pa.array(["x", "y"]).dictionary_encode(),
            "b": pa.array([1.5, 2.5])
            .dictionary_encode()
            .cast(pa.dictionary(pa.int8(), pa.float64())),
            "v": [1, 2],
        }
    )
    with pytest.raises(InputValidationError) as info:
        ArrowTableSource(table, tag_columns=["a", "b"], tag_only_columns=True)
    message = str(info.value)
    assert (
        "tag column 'a' has type dictionary<values=string, indices=int32, ordered=0> "
        "(decoded: string)" in message
    )
    assert (
        "tag column 'b' has type dictionary<values=double, indices=int8, ordered=0> "
        "(decoded: double)" in message
    )


# ── data frames ─────────────────────────────────────────────────────────────


def _pandas(encoded: tuple[str, ...]) -> pd.DataFrame:
    # String categories: a pandas Categorical of integers reaches the source as its
    # integers (the frame goes through polars, which has no integer Categorical).
    frame = pd.DataFrame(
        {
            "a": ["x", "y", "x"],
            "b": ["p", "p", "q"],
            "v": [1.0, 2.0, 3.0],
            "w": [1, 2, 3],
        }
    )
    for name in encoded:
        frame[name] = frame[name].astype("category")
    return frame


def _polars(encoded: tuple[str, ...], dtype: str) -> pl.DataFrame:
    frame = pl.DataFrame(
        {
            "a": ["x", "y", "x"],
            "b": ["p", "p", "q"],
            "v": [1.0, 2.0, 3.0],
            "w": [1, 2, 3],
        }
    )
    target = (
        pl.Categorical() if dtype == "Categorical" else pl.Enum(["p", "q", "x", "y"])
    )
    return frame.with_columns([pl.col(name).cast(target) for name in encoded])


@pytest.mark.parametrize(
    "tag_only_columns", [True, False], ids=["tag-only", "data-tags"]
)
@pytest.mark.parametrize("encoded", ENCODED)
@pytest.mark.parametrize("tags", TAG_ORDERS)
@pytest.mark.parametrize("frame_kind", ["pandas", "polars-Categorical", "polars-Enum"])
def test_a_data_frame_source_refuses_categorical_tags(
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
    plain = tuple(t for t in tags if t not in encoded)
    _assert_refused(info.value, _in_tag_order(tags, encoded), plain)


# ── a polling source's batches, read sync and async ─────────────────────────


class _CategoricalImpl:
    """Fetches one batch whose tag ``a`` is a polars Categorical."""

    def identity(self) -> str:
        return "categorical-impl"

    def to_config(self) -> None:
        return None

    @classmethod
    def from_config(cls, config: Any) -> _CategoricalImpl:
        return cls()

    def schema(self) -> Schema:
        # Declared, so building a pipeline over the source reads no batch: the job
        # run reads the first one.
        return Schema({"a": str, "b": str, "v": float, "w": int})

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        return cursor is None

    async def fetch(self, cursor: Cursor[int] | None = None) -> tuple[Cursor[int], Any]:
        if cursor is not None:
            return Cursor(value=1), {}
        frame = pl.DataFrame(
            {"a": ["x", "y"], "b": ["p", "q"], "v": [1.0, 2.0], "w": [1, 2]}
        ).with_columns(pl.col("a").cast(pl.Categorical()))
        return Cursor(value=1), frame

    async def close(self) -> None:
        return None


def _polling_source() -> PollingSource[int]:
    return PollingSource(
        _CategoricalImpl(),
        tag_columns=["b", "a"],
        record_id_columns="b",
        polling_config=PollingConfig(duration=0.01),
    )


def test_a_polling_source_refuses_a_categorical_tag_on_a_sync_read() -> None:
    with pytest.raises(InputValidationError) as info:
        _polling_source().as_table()
    _assert_refused(info.value, ("a",), ("b",))


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
def test_a_job_over_a_polling_source_refuses_a_categorical_tag(
    orchestrator: type,
) -> None:
    job = PipelineJob(name="itl1264", database=InMemoryArrowDatabase())
    with job:
        _polling_source().select_data_columns("v", label="selected")
    # The run's start-of-run check warns and goes on; the read of the batch raises.
    with (
        pytest.warns(SourceCheckFailedWarning, match=NOT_SUPPORTED),
        pytest.raises(InputValidationError, match=NOT_SUPPORTED),
    ):
        if orchestrator is AsyncPipelineOrchestrator:
            asyncio.run(job.run_async())
        else:
            job.run(orchestrator=orchestrator())


# ── a tag whose type contains a dictionary ──────────────────────────────────


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
    raise ValueError(kind)


DECODED = {
    "list": "large_list<item: string>",
    "struct": "struct<k: string, n: int64>",
}


@pytest.mark.parametrize("build", ["source", "stream"])
@pytest.mark.parametrize("kind", ["list", "struct"])
def test_a_tag_containing_a_dictionary_is_refused(kind: str, build: str) -> None:
    table = pa.table({"b": [1, 2, 3], "a": _nested(kind), "v": [1.0, 2.0, 3.0]})
    with pytest.raises(InputValidationError) as info:
        if build == "source":
            ArrowTableSource(table, tag_columns=["a", "b"], tag_only_columns=True)
        else:
            ArrowTableStream(table, tag_columns=["a", "b"])
    message = str(info.value)
    assert NOT_SUPPORTED in message
    assert re.search(r"tag column 'a' has type \S.*dictionary<", message)
    if build == "source":
        assert f"(decoded: {DECODED[kind]})" in message
    assert "tag column 'b'" not in message


# ── the decoding the message suggests works ─────────────────────────────────


@pytest.mark.parametrize("kind", sorted(VALUES))
def test_a_table_decoded_with_pyarrow_builds(kind: str) -> None:
    table = _table(kind, ("a", "b"))
    for name in ("b", "a"):
        index = table.schema.get_field_index(name)
        table = table.set_column(
            index, name, table[name].cast(table[name].type.value_type)
        )
    source = ArrowTableSource(table, tag_columns=["a", "b"], tag_only_columns=True)
    assert source.as_table(all_info=True).num_rows == 3


@pytest.mark.parametrize("dtype", ["Categorical", "Enum"])
def test_a_frame_decoded_with_polars_builds(dtype: str) -> None:
    frame = _polars(("a", "b"), dtype).with_columns(pl.col("a", "b").cast(pl.String))
    source = DataFrameSource(frame, tag_columns=["b", "a"], tag_only_columns=True)
    assert source.as_table(all_info=True).num_rows == 3


def test_a_frame_decoded_with_pandas_builds() -> None:
    frame = _pandas(("a", "b"))
    for name in ("a", "b"):
        frame[name] = frame[name].astype(frame[name].cat.categories.dtype)
    source = DataFrameSource(frame, tag_columns=["a", "b"], tag_only_columns=True)
    assert source.as_table(all_info=True).num_rows == 3


# ── no operator makes a dictionary tag ──────────────────────────────────────


def _data_source() -> ArrowTableSource:
    """Two plain tags; data columns ``v`` (dictionary-encoded float) and ``u``
    (dictionary-encoded string) and ``w`` plain."""
    return ArrowTableSource(
        pa.table(
            {
                "a": ["x", "y", "x"],
                "v": pa.array([1.5, 2.5, 1.5]).dictionary_encode(),
                "b": [7, 7, 8],
                "u": pa.array(["r", "s", "r"]).dictionary_encode(),
                "w": [1, 2, 3],
            }
        ),
        tag_columns=["a", "b"],
        tag_only_columns=True,
    )


@pytest.mark.parametrize("columns", [["v"], ["u", "v"], ["w", "u"]])
def test_tag_by_refuses_a_dictionary_data_column(columns: list[str]) -> None:
    """``TagBy`` refuses it before any stream is built (its Python type is ``Any``)."""
    with pytest.raises(InputValidationError, match="cannot be a tag"):
        _data_source().tag_by(columns)


# ── dictionary-encoded DATA columns are accepted as before ───────────────────


@pytest.mark.parametrize("tags", [["a", "b"], ["b", "a"]])
def test_dictionary_data_columns_are_accepted_unchanged(tags: list[str]) -> None:
    """Not covered by the decision: the source keeps them dictionary-encoded."""
    source = ArrowTableSource(
        _data_source().as_table(), tag_columns=tags, tag_only_columns=True
    )
    tag_keys, data_keys = source.keys()
    assert tag_keys == tuple(tags)
    assert data_keys == ("v", "u", "w")
    table = source.as_table()
    assert pa.types.is_dictionary(table.schema.field("v").type)
    assert pa.types.is_dictionary(table.schema.field("u").type)
    stream = ArrowTableStream(_data_source().as_table(), tag_columns=tags)
    assert pa.types.is_dictionary(stream.as_table().schema.field("u").type)
