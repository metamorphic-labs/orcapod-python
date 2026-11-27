"""Findings of the fourth independent review of PR #601 (ITL-1491), each as a test.

The check of the rows a run holds (the async orchestrator sends them on without
building a stream) keeps its verdict per row layout. A layout holds Arrow types,
and the extension type Orcapod builds for a list of a logical element
(``list[Path]``) cannot be hashed, so the verdict is kept under a description of
the layout that holds no Arrow type.
"""

from __future__ import annotations

import asyncio
import uuid
import warnings
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from upath import UPath

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.contexts import create_registry
from orcapod.core.operators import Batch, GroupBy, Index, MergeJoin
from orcapod.core.operators.static_output_pod import rows_on_read
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.logical_types.directory_type import Directory
from orcapod.logical_types.file_type import File
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.async_orchestrator import (
    _send_held,  # pyright: ignore[reportPrivateUsage]  # the held-row check is the subject
)
from orcapod.sources import DictSource
from orcapod.utils import schema_utils
from tests.test_pipeline.test_token_types_review_2 import (
    _keyed,  # pyright: ignore[reportPrivateUsage]  # the shared stream fixtures
)

STR = pa.large_string()
TOKENS = pa.large_list(STR)
NESTED = pa.large_list(TOKENS)
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}

pytestmark = pytest.mark.filterwarnings("ignore::orcapod.errors.ProvisionalHashWarning")


def _sent_on(rows: list[Any]) -> list[Any]:
    """Send held rows as the async orchestrator does; return what was sent."""

    async def run() -> list[Any]:
        channel: Channel[Any] = Channel(buffer_size=len(rows) + 1)
        await _send_held(rows, channel.writer)
        return await channel.reader.collect()

    return asyncio.run(run())


# ---------------------------------------------------------------------------
# 1. Held rows over a logical list: the replay of an ordinary pipeline
# ---------------------------------------------------------------------------


def _paths(source_id: str) -> DictSource:
    return DictSource(
        [
            {"g": "m", "k": 1, "p": Path("/a"), "n": 1},
            {"g": "m", "k": 2, "p": Path("/b"), "n": 2},
            {"g": "n", "k": 1, "p": Path("/c"), "n": 3},
        ],
        tag_columns=["g", "k"],
        tag_only_columns=True,
        source_id=source_id,
    )


@function_pod(output_keys="total")
def count_paths(p: list[Path], n: list[int]) -> int:
    return len(p) + sum(n)


@pytest.mark.parametrize("second", list(ORCHESTRATORS))
@pytest.mark.parametrize("first", list(ORCHESTRATORS))
def test_a_replay_over_a_batch_of_paths_reuses_its_rows(
    first: str, second: str
) -> None:
    """A source with a ``Path`` column, ``Batch(2)``, a function over the lists.

    After a run, the function's cache alone is cleared and the job runs again:
    the batch node is held, and its rows (a ``list[Path]`` column, an extension
    type, with list tokens) feed the function, which finds both rows stored. The
    async orchestrator's check of held rows hashed the rows' Arrow types and raised
    ``TypeError: unhashable type`` (fourth review of PR #601, finding 1). Every
    order of the two routes over one store reuses both rows.
    """
    job = PipelineJob(name="logical_held", database=InMemoryArrowDatabase())
    with job:
        batched = Batch(batch_size=2)(_paths("a"), label="batch")
        count_paths.pod(batched, label="f")
    ran = job.run(orchestrator=ORCHESTRATORS[first]())
    assert ran.nodes["f"].computed == 2
    cast("Any", job.nodes["f"]).clear_cache()
    again = job.run(orchestrator=ORCHESTRATORS[second]())
    assert (again.nodes["f"].reused, again.nodes["f"].computed) == (2, 0)


# ---------------------------------------------------------------------------
# Every registered logical type, through every operator that builds lists
# ---------------------------------------------------------------------------

REGISTERED: tuple[str, ...] = (
    create_registry().get_context().type_converter.registered_logical_type_names()
)


def _file(folder: Path, i: int) -> File:
    path = folder / f"f{i}.txt"
    _ = path.write_text(str(i))
    return File(path)


def _directory(folder: Path, i: int) -> Directory:
    path = folder / f"d{i}"
    path.mkdir(exist_ok=True)
    _ = (path / "a.txt").write_text(str(i))
    return Directory(path)


def _tensor(_: Path, i: int) -> Any:
    torch = pytest.importorskip("torch")
    return torch.arange(i + 1)


# A value of each registered logical type, by its extension name.
VALUES: dict[str, Any] = {
    "orcapod.path": lambda _, i: Path(f"/p{i}"),
    "orcapod.upath": lambda _, i: UPath(f"memory://p{i}"),
    "orcapod.uuid": lambda _, i: uuid.UUID(int=i),
    "orcapod.file": _file,
    "orcapod.directory": _directory,
    "orcapod.numpy.ndarray": lambda _, i: np.arange(i + 1),
    "orcapod.pandas.series": lambda _, i: pd.Series([i, i + 1]),
    "orcapod.pandas.dataframe": lambda _, i: pd.DataFrame({"a": [i]}),
    "orcapod.polars.series": lambda _, i: pl.Series("s", [i]),
    "orcapod.polars.dataframe": lambda _, i: pl.DataFrame({"a": [i]}),
    "orcapod.torch.tensor": _tensor,
}


def test_every_registered_logical_type_has_a_value_here() -> None:
    """A newly registered logical type fails here until it is added to ``VALUES``,
    and so to the held-row tests below."""
    assert sorted(VALUES) == sorted(REGISTERED)


def _source(name: str, folder: Path, source_id: str, tags: tuple[str, ...]) -> Any:
    """Tags ``g`` and ``k``; data ``p`` (the logical type ``name``) and ``n``."""
    value = VALUES[name]
    rows = [
        {"g": g, "k": k, "p": value(folder, i), "n": i}
        for i, (g, k) in enumerate((("m", 1), ("m", 2), ("n", 1)), start=1)
    ]
    return DictSource(
        rows, tag_columns=list(tags), tag_only_columns=True, source_id=source_id
    )


LIST_BUILDERS: dict[str, Any] = {
    "batch": lambda make: Batch(batch_size=2)(make("a")),
    "group_by": lambda make: GroupBy(by=["g"])(make("a")),
    "merge_join": lambda make: MergeJoin()(make("a"), make("b")),
    "group_by_then_batch": lambda make: Batch(batch_size=2)(
        GroupBy(by=["g"])(make("a"))
    ),
}


@pytest.mark.parametrize("tags", [("g", "k"), ("k", "g")], ids=["g_k", "k_g"])
@pytest.mark.parametrize("builder", list(LIST_BUILDERS))
@pytest.mark.parametrize("name", list(VALUES))
def test_held_rows_of_every_logical_list_pass_the_async_check(
    name: str, builder: str, tags: tuple[str, ...], tmp_path: Path
) -> None:
    """The rows of ``Batch``, ``GroupBy``, ``MergeJoin`` and ``GroupBy`` then
    ``Batch`` over a column of each registered logical type, as a run holds them.

    The data column is the logical list type of the element (an Arrow extension
    type, which need not be hashable) and its token a list of strings. The async
    check passes them, as the sync orchestrator's stream of them is built.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        stream = LIST_BUILDERS[builder](
            lambda source_id: _source(name, tmp_path, source_id, tags)
        )
        rows: list[Any] = list(stream.iter_data())
    assert rows
    layout = rows[0][1].token_layout()
    assert [entry[:2] for entry in layout] == [("_source_p", "p"), ("_source_n", "n")]
    assert isinstance(layout[0][3], pa.ExtensionType)
    assert len(_sent_on(rows)) == len(rows)
    built = cast("Any", rows_on_read(rows, stream)).built()
    assert built.as_table().num_rows == len(rows)


# ---------------------------------------------------------------------------
# The key a verdict is kept under holds no Arrow type
# ---------------------------------------------------------------------------


class _UnhashableList(pa.ExtensionType):
    """An extension type stored as a list whose objects cannot be hashed."""

    __hash__ = None  # pyright: ignore[reportAssignmentType]  # the case under test

    def __init__(self) -> None:
        super().__init__(pa.large_list(pa.int64()), "review.unhashable_list")

    def __arrow_ext_serialize__(self) -> bytes:
        return b""

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> "_UnhashableList":
        return cls()


def _plain(value: Any) -> bool:
    """Whether ``value`` is made of tuples, strings, integers and ``None`` only."""
    if isinstance(value, tuple):
        return all(_plain(item) for item in cast("tuple[Any, ...]", value))
    return value is None or type(value) in (str, int)


def _logical_list_types() -> list[pa.DataType]:
    """The logical list type of every available registered logical type, one and
    two list levels deep."""
    from orcapod.contexts import get_default_context
    from orcapod.utils import arrow_utils

    converter = get_default_context().type_converter
    found: list[pa.DataType] = []
    for name in REGISTERED:
        try:
            logical_type = converter.get_logical_type_by_arrow_extension_name(name)
        except ImportError:  # an optional package that is not installed
            continue
        assert logical_type is not None, name
        one = arrow_utils.aggregated_list_type(
            logical_type.get_arrow_extension_type(), converter, column="p"
        )
        found += [one, arrow_utils.aggregated_list_type(one, converter, column="p")]
    return found


def test_a_layout_key_holds_no_arrow_type() -> None:
    """Every part of the key a verdict is kept under is a tuple, a string, an
    integer or ``None``: nothing whose hash an Arrow type decides. So a type that
    cannot be hashed fails here, not in a user's replay."""
    data_types: list[pa.DataType] = [
        pa.int64(),
        pa.large_list(pa.int64()),
        pa.list_(pa.int64(), 2),
        pa.large_list(pa.list_(pa.large_list(pa.int64()), 3)),
        pa.struct([("a", pa.int64())]),
        pa.map_(pa.large_string(), pa.int64()),
        _UnhashableList(),
        *_logical_list_types(),
    ]
    token_types: list[pa.DataType] = [
        pa.null(),
        pa.string(),
        TOKENS,
        NESTED,
        pa.list_(STR, 2),
        pa.large_list(pa.list_(STR, 3)),
        pa.large_list(pa.int64()),
        pa.struct([("a", STR)]),
        _UnhashableList(),
    ]
    assert len(data_types) > 2 * len(REGISTERED) - 2
    keys = set()
    for token_type in token_types:
        for data_type in data_types:
            layout = (("_source_x", "x", token_type, data_type),)
            key = schema_utils.token_layout_key(layout)
            assert _plain(key), (token_type, data_type, key)
            keys.add(key)
    # Layouts the rule judges differently never share a key.
    for token_type in token_types:
        for data_type in data_types:
            for other in data_types:
                same_key = schema_utils.token_layout_key(
                    (("_source_x", "x", token_type, data_type),)
                ) == schema_utils.token_layout_key(
                    (("_source_x", "x", token_type, other),)
                )
                if same_key:
                    assert (
                        schema_utils.token_shape_fault(token_type, data_type) is None
                    ) == (schema_utils.token_shape_fault(token_type, other) is None)
    assert len(keys) > len(token_types)


def test_a_token_that_does_not_fit_a_logical_list_is_refused_alike() -> None:
    """Two list levels of tokens over a one-level ``list[Path]``: both routes
    refuse with the same message, which names the logical list type."""
    stream = Batch(batch_size=2)(_paths("a"))
    rows: list[Any] = [
        (tag, data.with_source_info(p=pa.scalar([["a"], ["b"]], type=NESTED)))
        for tag, data in stream.iter_data()
    ]
    with pytest.raises(InputValidationError) as on_async:
        _ = _sent_on(rows)
    with pytest.raises(InputValidationError) as on_sync:
        _ = cast("Any", rows_on_read(rows, stream)).built()
    assert str(on_async.value) == str(on_sync.value)
    assert "'_source_p'" in str(on_async.value) and "list[orcapod.path]" in str(
        on_async.value
    )


# ---------------------------------------------------------------------------
# 2. The sync call sites of the guards against a null inner list
# ---------------------------------------------------------------------------

KEY_OF_LISTS = pa.large_list(pa.large_list(pa.int64()))
KEYS = [[[1], [2]], [[3], [4]]]
WHOLE = [[["p"], ["a"]], [["q"], ["b"]]]
HOLED = [None, ["a"]]


class _Tampered(ArrowTableStream):
    """A stream that hands out a token no stream can hold: its table's
    ``_source_k`` is replaced, after construction, by a null inner list."""

    def as_table(self, *args: Any, **kwargs: Any) -> pa.Table:
        table = super().as_table(*args, **kwargs)
        if "_source_k" not in table.column_names or not table.num_rows:
            return table
        index = table.schema.get_field_index("_source_k")
        return table.set_column(
            index,
            table.schema.field(index),
            pa.array([HOLED] * table.num_rows, NESTED),
        )


def _tampered(stream: ArrowTableStream) -> ArrowTableStream:
    cast("Any", stream).__class__ = _Tampered
    return stream


@pytest.mark.parametrize("side", [0, 1], ids=["first", "second"])
@pytest.mark.parametrize("tags", [("k", "g"), ("g", "k")], ids=["k_g", "g_k"])
def test_merge_join_s_sync_call_guards_against_a_null_inner_list(
    side: int, tags: tuple[str, ...]
) -> None:
    """``MergeJoin`` called on streams: no stream can hold a null inner list, so
    one input is made to hand out such a token. The sync merge refuses, naming
    the operator and the token column (fourth review of PR #601, finding 2)."""
    streams = [
        _keyed(("x", "w"), KEYS, WHOLE, NESTED, tags, key_type=KEY_OF_LISTS),
        _keyed(("y", "v"), KEYS, WHOLE, NESTED, tags, key_type=KEY_OF_LISTS),
    ]
    streams[side] = _tampered(streams[side])
    with pytest.raises(InputValidationError) as refused:
        _ = MergeJoin()(*streams).as_table()
    message = str(refused.value)
    assert message.startswith("MergeJoin") and "'_source_k'" in message


@pytest.mark.parametrize("layout", ["tags_first", "reversed"])
def test_index_s_sync_call_guards_against_a_null_element(layout: str) -> None:
    """``Index`` called on a stream whose row hands out a null inner list (no
    stream holds one): taking that element would give a null token whose type is
    a list, and the sync path refuses, naming the operator and the column. (With
    ``out`` the row keeps ``x`` and its token, and the result stream is refused
    where it is built.)"""
    data_type = pa.large_list(pa.large_list(pa.int64()))
    columns: dict[str, Any] = {
        "g": pa.array(["m", "n"], STR),
        "k": pa.array([1, 2], pa.int64()),
        "x": pa.array([[[1], [2]], [[3], [4]]], data_type),
        "w": pa.array([10, 20], pa.int64()),
        "_source_x": pa.array(WHOLE, NESTED),
    }
    names = list(columns) if layout == "tags_first" else list(reversed(columns))
    table = pa.table({name: columns[name] for name in names})
    table = table.cast(pa.schema([f.with_nullable(False) for f in table.schema]))

    class Handing(ArrowTableStream):
        def iter_data(self) -> Any:
            for tag, data in super().iter_data():
                yield tag, data.with_source_info(x=pa.scalar(HOLED, type=NESTED))

    stream = Handing(table, tag_columns=["g", "k"])
    with pytest.raises(InputValidationError) as refused:
        _ = Index("x", 0)(stream).as_table()
    message = str(refused.value)
    assert message.startswith("Index") and "'_source_x'" in message
    taken = Index("x", 1)(stream).as_table(columns={"source": True})
    assert taken.column("_source_x").to_pylist() == [["a"], ["a"]]
