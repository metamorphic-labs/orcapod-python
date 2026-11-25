"""Uniqueness certificates: what each stream and step certifies (``core/unique_keys``)."""

from __future__ import annotations

import warnings
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from orcapod import FunctionPod, function_pod
from orcapod.core.nodes import OperatorJobNode, SourceJobNode
from orcapod.core.operators import DropNulls, Join, SemiJoin
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.core.unique_keys import (
    MAX_UNIQUE_KEYS,
    certificate,
    joined_keys,
    renamed_keys,
    unique_on,
)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.types import CacheMode

pytestmark = pytest.mark.filterwarnings("ignore")


def _keys(*keys: tuple[str, ...]) -> frozenset[frozenset[str]]:
    return frozenset(frozenset(key) for key in keys)


def _table() -> pa.Table:
    return pa.table(
        {
            "subject": ["a", "a", "b"],
            "trial": [1, 2, 1],
            "side": ["l", "r", "l"],
            "x": [1.0, None, 3.0],
            "y": [10, 20, 30],
        }
    )


def _source(
    record_id_columns: Any = ("subject", "trial"),
    tags: tuple[str, ...] = ("side", "trial", "subject"),
    table: pa.Table | None = None,
    **kwargs: Any,
) -> ArrowTableSource:
    return ArrowTableSource(
        _table() if table is None else table,
        tag_columns=tags,
        record_id_columns=record_id_columns,
        tag_only_columns=True,
        **kwargs,
    )


KEY = ("subject", "trial")


def test_a_source_certifies_key_columns_that_are_all_tags() -> None:
    assert _source().unique_keys == _keys(KEY)
    assert _source(("trial", "subject")).unique_keys == _keys(KEY)  # order is no matter
    assert _source(tags=("subject", "side")).unique_keys == frozenset()  # one is data
    assert _source(None).unique_keys == frozenset()
    assert _source(()).unique_keys == frozenset()
    with_data_tags = ArrowTableSource(
        _table(),
        tag_columns=["subject", "trial"],
        record_id_columns=["subject", "trial"],
        tag_only_columns=False,
    )
    assert with_data_tags.unique_keys == _keys(KEY)


def test_a_subclass_of_a_source_certifies_nothing() -> None:
    class Repeating(ArrowTableSource):
        pass

    source = Repeating(
        _table(),
        tag_columns=["subject", "trial"],
        record_id_columns=["subject", "trial"],
    )
    assert source.unique_keys == frozenset()


def test_a_raw_stream_certifies_nothing_also_with_a_producer() -> None:
    raw = ArrowTableStream(_table(), tag_columns=["subject", "trial"])
    assert raw.unique_keys == frozenset()
    made = ArrowTableStream(
        _table(),
        tag_columns=["subject", "trial"],
        producer=DropNulls(),
        upstreams=(_source(),),
    )
    assert made.unique_keys == frozenset()


STEPS: list[tuple[str, Callable[[Any], StreamProtocol], frozenset[frozenset[str]]]] = [
    ("drop_nulls", lambda s: s.drop_nulls("x"), _keys(KEY)),
    ("filter", lambda s: s.filter(side="l"), _keys(KEY)),
    ("polars_filter", lambda s: s.polars_filter(pl.col("y") > 5), _keys(KEY)),
    ("add_tags", lambda s: s.add_tags(run=1, cohort="A"), _keys(KEY)),
    ("tag_by", lambda s: s.drop_nulls().tag_by("y"), _keys(KEY)),
    ("select_data", lambda s: s.select_data_columns(["x"]), _keys(KEY)),
    ("drop_data", lambda s: s.drop_data_columns(["y"]), _keys(KEY)),
    (
        "select_tags_kept",
        lambda s: s.select_tag_columns(["trial", "subject"]),
        _keys(KEY),
    ),
    (
        "select_tags_lost",
        lambda s: s.select_tag_columns(["side", "trial"]),
        frozenset(),
    ),
    ("drop_tag_kept", lambda s: s.drop_tag_columns(["side"]), _keys(KEY)),
    ("drop_tag_lost", lambda s: s.drop_tag_columns(["trial"]), frozenset()),
    ("select_kept", lambda s: s.select_columns(["subject", "trial", "x"]), _keys(KEY)),
    ("select_lost", lambda s: s.select_columns(["subject", "x"]), frozenset()),
    ("drop_kept", lambda s: s.drop_columns(["side", "x"]), _keys(KEY)),
    ("drop_lost", lambda s: s.drop_columns(["subject"]), frozenset()),
    ("map_data", lambda s: s.map_data(x="z", y="w"), _keys(KEY)),
    ("map_tags", lambda s: s.map_tags(trial="t", side="s"), _keys(("subject", "t"))),
    (
        "map_tags_swapped",
        lambda s: s.map_tags({"trial": "side", "side": "trial"}),
        _keys(("subject", "side")),
    ),
    (
        "map_tags_drops_a_key",
        lambda s: s.map_tags({"side": "s", "subject": "subject2"}, drop_unmapped=True),
        frozenset(),
    ),
    ("map_columns", lambda s: s.map_columns(trial="t", x="z"), _keys(("subject", "t"))),
    ("group_by", lambda s: s.group_by(["side", "trial"]), _keys(("side", "trial"))),
    ("batch", lambda s: s.batch(2), frozenset()),
    (
        "chain",
        lambda s: s.drop_nulls().map_tags(subject="u").add_tags(k=1),
        _keys(("u", "trial")),
    ),
]


def _tag_rows(stream: StreamProtocol, names: frozenset[str]) -> list[tuple[Any, ...]]:
    table = stream.as_table()
    return list(zip(*(table.column(name).to_pylist() for name in sorted(names))))


@pytest.mark.parametrize("label,step,expected", STEPS, ids=[s[0] for s in STEPS])
def test_each_step_derives_its_certificate(
    label: str,
    step: Callable[[Any], StreamProtocol],
    expected: frozenset[frozenset[str]],
) -> None:
    result = step(_source())
    assert result.unique_keys == expected
    for key in result.unique_keys:  # and it holds
        rows = _tag_rows(result, key)
        assert len(rows) == len(set(rows))


def _other(key: tuple[str, ...] | None, tags: tuple[str, ...]) -> ArrowTableSource:
    table = pa.table({"trial": [1, 2], "depth": [5, 6], "z": [1, 2], "w": [3, 4]})
    return _source(key, tags, table, source_id="other")


def test_joins_derive_their_certificate() -> None:
    left = _source()
    lookup = _other(("trial",), ("depth", "trial"))
    both = _other(("trial", "depth"), ("depth", "trial"))
    none = _other(None, ("depth", "trial"))
    # A lookup matches each left row at most once: the left key stays.
    assert left.join(lookup).unique_keys == _keys(KEY)
    assert lookup.join(left).unique_keys == _keys(KEY)
    assert left.join(both).unique_keys == _keys((*KEY, "depth"))
    assert left.join(none).unique_keys == frozenset()
    assert Join()(left, lookup, _wide()).unique_keys == _keys(KEY)
    assert left.semi_join(none).unique_keys == _keys(KEY)
    assert none.semi_join(left).unique_keys == frozenset()
    renamed = left.select_data_columns(["x"])
    assert renamed.rejoin(left.select_data_columns(["y"])).unique_keys == _keys(KEY)


def _wide() -> ArrowTableSource:
    table = pa.table({"subject": ["a", "b"], "q": [1, 2]})
    return _source(("subject",), ("subject",), table, source_id="wide")


def test_a_one_to_one_join_keeps_both_keys() -> None:
    a = _source(("trial",), ("trial",), pa.table({"trial": [1, 2], "x": [1, 2]}))
    b = _source(
        ("trial",), ("trial",), pa.table({"trial": [2, 1], "y": [1, 2]}), source_id="b"
    )
    assert a.join(b).unique_keys == _keys(("trial",))


def test_a_function_pod_and_a_side_effect_keep_the_certificate() -> None:
    @function_pod(output_keys=["total", "half"])
    def add(x: float | None, y: int | None) -> tuple[float, float]:
        return (x or 0.0) + (y or 0), (y or 0) / 2

    assert add.pod(_source()).unique_keys == _keys(KEY)
    assert isinstance(add.pod, FunctionPod)


def test_an_operator_subclass_certifies_nothing() -> None:
    class Doubling(DropNulls):
        pass

    class MyJoin(Join):
        pass

    assert Doubling("x")(_source()).unique_keys == frozenset()
    assert MyJoin()(_source(), _wide()).unique_keys == frozenset()


def test_a_source_slot_answers_as_its_bound_source() -> None:
    source = _source()
    node = SourceJobNode.from_stream(source)
    assert node.unique_keys == _keys(KEY)
    node.bound_source = None
    assert node.unique_keys == frozenset()


@pytest.mark.parametrize("mode", list(CacheMode))
def test_a_replaying_operator_node_certifies_nothing(mode: CacheMode) -> None:
    """``REPLAY`` gives every logged run's rows, so a tag tuple can repeat."""
    node = OperatorJobNode(DropNulls("x"), (SourceJobNode.from_stream(_source()),))
    node.attach_databases(pipeline_database=InMemoryArrowDatabase(), cache_mode=mode)
    expected = frozenset() if mode == CacheMode.REPLAY else _keys(KEY)
    assert node.unique_keys == expected


def test_a_certificate_follows_a_changed_upstream() -> None:
    node = SourceJobNode.from_stream(_source())
    operator = OperatorJobNode(DropNulls("x"), (node,))
    assert operator.unique_keys == _keys(KEY)
    node.bound_source = _source(None)
    assert operator.unique_keys == frozenset()


def test_a_certificate_enters_no_identity() -> None:
    table = _table()
    keyed = _source(source_id="s", table=table)
    plain = _source(None, source_id="s", table=table)
    assert keyed.unique_keys != plain.unique_keys
    assert keyed.pipeline_hash() == plain.pipeline_hash()
    derived = keyed.drop_nulls("x")
    before = derived.content_hash()
    _ = derived.unique_keys
    assert derived.content_hash() == before
    assert "unique_keys" not in keyed.to_config()


def test_a_tall_stack_of_diamonds_is_derived_once_per_stream() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        stream: Any = _source()
        for _ in range(40):  # 2**40 ways down without the memo
            stream = stream.semi_join(stream)
        assert stream.unique_keys == _keys(KEY)
        assert isinstance(stream.producer, SemiJoin)


def test_key_values_equal_under_a_join_are_refused_by_the_source() -> None:
    at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    repeated = pa.table(
        {"t": pa.array([at, at], pa.timestamp("ns", "UTC")), "x": [1, 2]}
    )
    with pytest.raises(Exception, match="must be unique"):
        _ = _source(("t",), ("t",), repeated)
    with pytest.raises(Exception, match="cannot identify"):
        _ = _source(("t",), ("t",), pa.table({"t": [0.0, -0.0], "x": [1, 2]}))
    assert DictSource(
        [{"k": 1, "x": 1}], tag_columns=["k"], record_id_columns="k"
    ).unique_keys == _keys(("k",))


def test_the_algebra() -> None:
    assert certificate([("a", "b"), ("a",), ("b", "a")]) == _keys(("a",))
    many = certificate((f"k{i}",) for i in range(50))
    assert len(many) == MAX_UNIQUE_KEYS
    assert unique_on(_keys(("a",)), ["b", "a"])
    assert not unique_on(_keys(("a", "b")), ["a"])
    assert not unique_on(frozenset(), ["a"])
    assert renamed_keys(_keys(("a", "b")), {"a": "b", "b": "a"}) == _keys(("a", "b"))
    assert renamed_keys(_keys(("a",), ("b",)), {"b": "a"}, ["a"]) == _keys(("a",))
    a, b = _keys(("a",)), _keys(("a", "b"))
    assert joined_keys(a, b, ["a"]) == _keys(("a", "b"))
    assert joined_keys(a, _keys(("a",)), ["a"]) == _keys(("a",))
    assert joined_keys(a, b, []) == _keys(("a", "b"))
    assert joined_keys(a, frozenset(), ["a"]) == frozenset()


# ---------------------------------------------------------------------------
# A customized instance certifies nothing: its rows may be other rows
# ---------------------------------------------------------------------------


def _replaced(obj: Any, name: str) -> Any:
    """``obj`` with a method replaced on the instance (by an equal one)."""
    original = getattr(obj, name)
    setattr(obj, name, lambda *args, **kwargs: original(*args, **kwargs))
    return obj


@pytest.mark.parametrize("name", ["as_table", "iter_data", "open_channel", "keys"])
def test_a_source_with_a_replaced_reader_certifies_nothing(name: str) -> None:
    source = _replaced(_source(), name)
    assert source.unique_keys == frozenset()
    assert source.drop_nulls("x").unique_keys == frozenset()
    assert SourceJobNode.from_stream(source).unique_keys == frozenset()


OPERATORS: list[Callable[[Any], Any]] = [
    lambda s: DropNulls("x"),
    lambda s: s.filter(side="l").producer,
    lambda s: s.group_by(["side"]).producer,
    lambda s: s.map_tags(trial="t").producer,
    lambda s: s.add_tags(k=1).producer,
    lambda s: s.select_data_columns(["x"]).producer,
]


@pytest.mark.parametrize("make", OPERATORS)
@pytest.mark.parametrize("name", ["unary_static_process", "static_process"])
def test_an_operator_with_a_replaced_body_certifies_nothing(
    make: Callable[[Any], Any], name: str
) -> None:
    assert make(_source())(_source()).unique_keys != frozenset()
    assert _replaced(make(_source()), name)(_source()).unique_keys == frozenset()


def test_a_stream_or_node_with_a_replaced_reader_certifies_nothing() -> None:
    assert _replaced(_source().drop_nulls("x"), "as_table").unique_keys == frozenset()
    node = OperatorJobNode(DropNulls("x"), (SourceJobNode.from_stream(_source()),))
    assert node.unique_keys == _keys(KEY)
    assert _replaced(node, "iter_data").unique_keys == frozenset()
    slot = SourceJobNode.from_stream(_source())
    assert _replaced(slot, "as_table").unique_keys == frozenset()

    class MyNode(OperatorJobNode):
        pass

    own = MyNode(DropNulls("x"), (SourceJobNode.from_stream(_source()),))
    assert own.unique_keys == frozenset()


def test_a_function_pod_with_a_replaced_call_certifies_nothing() -> None:
    @function_pod(output_keys="total")
    def add(x: float | None, y: int | None) -> float:
        return (x or 0.0) + (y or 0)

    assert add.pod(_source()).unique_keys == _keys(KEY)
    assert _replaced(add.pod, "process_data")(_source()).unique_keys == frozenset()
