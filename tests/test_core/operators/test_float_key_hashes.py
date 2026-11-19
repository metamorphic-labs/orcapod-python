"""Float keys: rows that match have equal key hashes, and only they (ITL-1251, ITL-1179).

The operators match float keys as Polars does (``0.0`` equals ``-0.0``, every NaN equals
every NaN; ITL-1234 for ``GroupBy``), and since ITL-1179 the hashes of Orcapod's tables
follow the same rule. So for ``Join``, ``SemiJoin``, ``MergeJoin`` and ``GroupBy``: two
rows match exactly when their key hashes are equal. Each is checked on the sync and the
async path, on inputs with two tags and two data columns, with the tags in both orders and
the float key both a plain tag and a data-tag.
"""

from __future__ import annotations

import itertools
import struct
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core.operators import GroupBy, Join, MergeJoin, SemiJoin
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.sources import ArrowTableSource
from tests.test_core.operators.several_items import Row, async_rows

BITS64 = {
    "0": 0x0000000000000000,
    "-0": 0x8000000000000000,
    "nan": 0x7FF8000000000000,
    "-nan": 0xFFF8000000000000,
    "payload_nan": 0x7FF8000000000001,
    "signaling_nan": 0x7FF0000000000001,
    "1": 0x3FF0000000000000,
    "2": 0x4000000000000000,
}
BITS32 = {
    "0": 0x00000000,
    "-0": 0x80000000,
    "nan": 0x7FC00000,
    "-nan": 0xFFC00000,
    "payload_nan": 0x7FC00001,
    "signaling_nan": 0x7F800001,
    "1": 0x3F800000,
    "2": 0x40000000,
}

LEFT_KEYS = ["-0", "nan", "-nan", "1", "0", "signaling_nan"]
RIGHT_KEYS = ["0", "payload_nan", "1", "2", "-0"]


def _f64(names: list[str]) -> pa.Array[Any]:
    return pa.array([BITS64[n] for n in names], pa.uint64()).view(pa.float64())


def _f32(names: list[str]) -> pa.Array[Any]:
    return pa.array([BITS32[n] for n in names], pa.uint32()).view(pa.float32())


def _source(
    source_id: str,
    keys: list[str],
    data: dict[str, pa.Array[Any]],
    tag_order: tuple[str, ...],
    data_tag: bool,
) -> ArrowTableSource:
    """Two tags, ``g`` (text) and ``k`` (the float key), and two data columns."""
    n = len(keys)
    columns: dict[str, pa.Array[Any]] = {
        "g": pa.array(["p"] * n, pa.large_string()),
        "k": _f64(keys),
        **data,
    }
    return ArrowTableSource(
        pa.table({name: columns[name] for name in (*tag_order, *data)}),
        tag_columns=list(tag_order),
        tag_only_columns=["g"] if data_tag else True,
        source_id=source_id,
    )


def _left(tag_order: tuple[str, ...], data_tag: bool) -> ArrowTableSource:
    n = len(LEFT_KEYS)
    return _source(
        "left",
        LEFT_KEYS,
        {
            "a": pa.array(range(n), pa.int64()),
            "b": _f32(list(reversed(LEFT_KEYS))),
        },
        tag_order,
        data_tag,
    )


def _right(tag_order: tuple[str, ...], data_tag: bool) -> ArrowTableSource:
    n = len(RIGHT_KEYS)
    return _source(
        "right",
        RIGHT_KEYS,
        {
            "c": pa.array(range(100, 100 + n), pa.int64()),
            "d": _f32(RIGHT_KEYS),
        },
        tag_order,
        data_tag,
    )


TAG_ORDERS = [("g", "k"), ("k", "g")]
ARRANGEMENTS = [
    pytest.param(
        order, data_tag, id=f"{'-'.join(order)}-{'data_tag' if data_tag else 'tag'}"
    )
    for order, data_tag in itertools.product(TAG_ORDERS, [False, True])
]
PATHS = ["sync", "async"]


def _rows(
    op: StaticOutputOperatorPod, path: str, *sources: ArrowTableSource
) -> list[Row]:
    if path == "sync":
        return list(op(*sources).iter_data())
    return async_rows(op, *sources)


def _key_hash(row_tag: Any, keys: tuple[str, ...]) -> str:
    """The hash of a row's key columns, as Orcapod hashes its own tables."""
    table = row_tag.as_table().select(list(keys))
    return get_default_context().arrow_hasher.hash_table(table).to_string()


def _input(source: ArrowTableSource, id_column: str) -> dict[int, Any]:
    return {data.as_dict()[id_column]: tag for tag, data in source.iter_data()}


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize(("tag_order", "data_tag"), ARRANGEMENTS)
@pytest.mark.parametrize("op_class", [Join, MergeJoin], ids=["Join", "MergeJoin"])
def test_joined_pairs_are_the_pairs_with_equal_key_hashes(
    op_class: type[StaticOutputOperatorPod],
    tag_order: tuple[str, ...],
    data_tag: bool,
    path: str,
) -> None:
    left, right = _left(tag_order, data_tag), _right(tag_order, data_tag)
    left_tags, right_tags = _input(left, "a"), _input(right, "c")
    keys = ("g", "k")
    equal_hashes = {
        (a, c)
        for (a, lt), (c, rt) in itertools.product(left_tags.items(), right_tags.items())
        if _key_hash(lt, keys) == _key_hash(rt, keys)
    }
    joined = {
        (data.as_dict()["a"], data.as_dict()["c"])
        for _, data in _rows(op_class(), path, left, right)
    }
    assert joined == equal_hashes
    # 0.0 / -0.0 pairs, NaN pairs and the 1.0 pair, at least.
    assert len(joined) >= 3


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize(("tag_order", "data_tag"), ARRANGEMENTS)
def test_semijoin_keeps_the_rows_with_a_partner_of_equal_key_hash(
    tag_order: tuple[str, ...], data_tag: bool, path: str
) -> None:
    left, right = _left(tag_order, data_tag), _right(tag_order, data_tag)
    keys = ("g", "k")
    right_hashes = {_key_hash(rt, keys) for rt in _input(right, "c").values()}
    expected = {
        a for a, lt in _input(left, "a").items() if _key_hash(lt, keys) in right_hashes
    }
    kept = {data.as_dict()["a"] for _, data in _rows(SemiJoin(), path, left, right)}
    assert kept == expected


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize(("tag_order", "data_tag"), ARRANGEMENTS)
def test_group_members_are_the_rows_with_equal_key_hashes(
    tag_order: tuple[str, ...], data_tag: bool, path: str
) -> None:
    left = _left(tag_order, data_tag)
    keys = ("k",)
    by_hash: dict[str, set[int]] = {}
    for a, tag in _input(left, "a").items():
        by_hash.setdefault(_key_hash(tag, keys), set()).add(a)
    rows = _rows(GroupBy(by=["k"]), path, left)
    groups = {frozenset(cast("list[int]", data.as_dict()["a"])) for _, data in rows}
    assert groups == {frozenset(members) for members in by_hash.values()}
    # Each group's emitted key has the members' key hash.
    for tag, data in rows:
        member_hashes = {
            _key_hash(t, keys)
            for a, t in _input(left, "a").items()
            if a in cast("list[int]", data.as_dict()["a"])
        }
        assert member_hashes == {_key_hash(tag, keys)}


def test_the_float_bits_of_matched_rows_are_kept() -> None:
    """Matching and hashing never rewrite a value: the joined data keep their bits."""
    left, right = _left(("g", "k"), False), _right(("g", "k"), False)
    out = Join()(left, right).as_table()
    by_a = {cast(int, a): i for i, a in enumerate(out.column("a").to_pylist())}
    b_bits = out.column("b").combine_chunks().view(pa.uint32()).to_pylist()
    expected = dict(
        zip(range(len(LEFT_KEYS)), (BITS32[n] for n in reversed(LEFT_KEYS)))
    )
    for a, i in by_a.items():
        assert b_bits[i] == expected[a]


def test_bit_patterns_used_are_distinct() -> None:
    """Sanity: the named patterns are the bits they claim (no two names alias)."""
    assert len(set(BITS64.values())) == len(BITS64)
    assert struct.unpack("<d", struct.pack("<Q", BITS64["-0"]))[0] == 0.0


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_a_job_stores_and_reads_back_every_float_bit(orchestrator: str) -> None:
    """A job node's stored rows keep every bit of -0.0 and of each NaN, sync and async."""
    from orcapod import PipelineJob
    from orcapod.databases import InMemoryArrowDatabase
    from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

    names = ["-0", "nan", "-nan", "payload_nan", "signaling_nan", "0"]
    n = len(names)
    source = ArrowTableSource(
        pa.table(
            {
                "g": pa.array(["p", "q"] * (n // 2), pa.large_string()),
                "i": pa.array(range(n), pa.int64()),
                "x": _f64(names),
                "y": _f64(list(reversed(names))),
            }
        ),
        tag_columns=["g", "i"],
        tag_only_columns=True,
        source_id="bits",
    )
    job = PipelineJob(name="float_bits", database=InMemoryArrowDatabase())
    with job:
        _ = source.select_data_columns(["y", "x"], label="kept")
    run = (
        SyncPipelineOrchestrator
        if orchestrator == "sync"
        else AsyncPipelineOrchestrator
    )
    job.run(orchestrator=run())
    node = job.nodes["kept"]
    for table in (node.as_table(), node.as_source().as_table()):
        order = table.column("i").to_pylist()
        for name, expected in (("x", names), ("y", list(reversed(names)))):
            bits = table.column(name).combine_chunks().view(pa.uint64()).to_pylist()
            assert bits == [BITS64[expected[i]] for i in order], name
