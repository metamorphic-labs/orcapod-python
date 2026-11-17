"""The source info of a value merged from several equal elements is one token (ITL-1017).

Developer decision, November 13, 2026: when a data value is the merge of several equal
elements, its source-info value is one scalar token, a deterministic digest of the member
tokens: the scalar key of a ``GroupBy`` on a referential tag, and the shared key of a
``Join`` that is referential on more than one input. Provenance belongs to the data column
and keeps its shape: a scalar data column has a scalar token, a list column a list of
tokens, nested as deep as the data. The token is the operation's fold (ITL-1154):
``group_v1:<hex>`` over the members' tokens in member order for a ``GroupBy`` key, and
``join_v1:<hex>`` over the members' tokens sorted for a ``Join`` key.

Every operator is driven through its public call and read through the stream's public
methods.
"""

from __future__ import annotations

import itertools
import os
import re
import subprocess
import sys
import textwrap
import types
import typing
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.operators import (
    DropNulls,
    GroupBy,
    Index,
    Join,
    MapData,
    MapTags,
    PolarsFilter,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.operators.group_by import GROUP_BY_FOLD_LABEL
from orcapod.core.operators.join import JOIN_FOLD_LABEL
from orcapod.core.referential_tags import merge_source_info
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.utils import arrow_utils

GROUP_TOKEN = re.compile(rf"^{GROUP_BY_FOLD_LABEL}:[0-9a-f]{{64}}$")
JOIN_TOKEN = re.compile(rf"^{JOIN_FOLD_LABEL}:[0-9a-f]{{64}}$")


def group_fold(tokens: Sequence[str | None]) -> str:
    """The token of a ``GroupBy`` key: its members' tokens folded in member order."""
    return arrow_utils.fold_system_tag_values(tokens, label=GROUP_BY_FOLD_LABEL)


def join_fold(tokens: Sequence[str | None]) -> str:
    """The token of a ``Join`` key: its members' tokens sorted, then folded."""
    ordered = sorted(tokens, key=lambda token: (token is None, token or ""))
    return arrow_utils.fold_system_tag_values(ordered, label=JOIN_FOLD_LABEL)


def _ref(rows: Sequence[Mapping[str, Any]], tags: list[str], sid: str, **kw: Any):
    return op.sources.DictSource(rows, tag_columns=tags, source_id=sid, **kw)


GROUP_ROWS = [
    {"s": "s1", "t": 1, "x": 20, "y": 5},
    {"s": "s1", "t": 2, "x": 10, "y": 4},
    {"s": "s2", "t": 1, "x": 5, "y": 1},
]


def _grouped(rows: Sequence[Mapping[str, Any]] = GROUP_ROWS) -> StreamProtocol:
    return GroupBy(["s"])(_ref(rows, ["s", "t", "x"], "G"))


def _source_column(stream: StreamProtocol, name: str) -> list[Any]:
    return (
        stream.as_table(columns={"source": True}).column(f"_source_{name}").to_pylist()
    )


def _by(stream: StreamProtocol, key: str, name: str) -> dict[Any, Any]:
    """The source info of ``name``, by the row's value of tag ``key``."""
    table = stream.as_table(columns={"source": True})
    return dict(
        zip(table.column(key).to_pylist(), table.column(f"_source_{name}").to_pylist())
    )


# ── the shape rule ──────────────────────────────────────────────────────────


def _list_depth(python_type: Any) -> int:
    """How many ``list[...]`` levels a Python data type has (``T | None`` unwrapped)."""
    if isinstance(python_type, types.UnionType) or typing.get_origin(python_type) is (
        typing.Union
    ):
        (python_type,) = [
            a for a in typing.get_args(python_type) if a is not type(None)
        ]
    if typing.get_origin(python_type) is list:
        return 1 + _list_depth(typing.get_args(python_type)[0])
    return 0


def _arrow_list_depth(arrow_type: Any) -> int:
    depth = 0
    while hasattr(arrow_type, "value_type"):
        depth += 1
        arrow_type = arrow_type.value_type
    return depth


def assert_provenance_has_the_shape_of_its_data(stream: StreamProtocol) -> None:
    """Every ``_source_<c>`` has the list depth of data column ``c``, in type and value."""
    _, data_schema = stream.output_schema()
    table = stream.as_table(columns={"source": True})
    checked = 0
    for name, data_type in data_schema.items():
        column = f"_source_{name}"
        if column not in table.column_names:
            continue
        depth = _list_depth(data_type)
        assert _arrow_list_depth(table.schema.field(column).type) == depth, column
        for value, token in zip(
            table.column(name).to_pylist(), table.column(column).to_pylist()
        ):
            _assert_same_shape(value, token, depth, column)
        checked += 1
    assert checked, "no source-info column was checked"


def _assert_same_shape(value: Any, token: Any, depth: int, column: str) -> None:
    """``token`` nests as ``value`` does, ``depth`` lists deep, at every member."""
    if depth == 0:
        if value is not None:
            assert token is None or isinstance(token, str), column
        return
    if value is None:
        return
    assert isinstance(token, list) and len(token) == len(value), column
    for member, member_token in zip(value, token):
        _assert_same_shape(member, member_token, depth - 1, column)


def test_the_shape_check_looks_at_every_member() -> None:
    """A nested member without its tokens is caught, not only the first one."""
    _assert_same_shape([[20], [10]], [["a"], ["b"]], 2, "c")
    with pytest.raises(AssertionError):
        _assert_same_shape([[20], [10]], [["a"], []], 2, "c")


def test_a_referential_group_key_schema_prediction_matches_the_result() -> None:
    """The key stays scalar in both roles; the prediction agrees with the result."""
    source = _ref(
        [{"s": "a", "t": 1, "v": 0.5}, {"s": "a", "t": 2, "v": 0.7}], ["s", "t"], "S"
    )
    grouped = GroupBy(["s"])
    result = grouped.static_process(source)
    for columns in ({}, {"source": True}):
        predicted = grouped.output_schema(source, columns=columns)
        assert predicted == result.output_schema(columns=columns)
        assert predicted[1]["s"] is str
    predicted = grouped.output_schema(source, all_info=True)
    assert predicted == result.output_schema(all_info=True)


def _a(**kw: Any):
    return _ref([{"x": 1, "a": 0.5}, {"x": 2, "a": 0.6}], ["x"], "A", **kw)


def _b(**kw: Any):
    return _ref([{"x": 1, "b": 10}, {"x": 3, "b": 30}], ["x"], "B", **kw)


def _c(**kw: Any):
    return _ref([{"x": 1, "c": 7}], ["x"], "C", **kw)


SHAPE_CASES: dict[str, Callable[[], StreamProtocol]] = {
    "group_by_key": lambda: _grouped(),
    "group_by_two_keys": lambda: GroupBy(["s", "t"])(
        _ref(GROUP_ROWS, ["s", "t", "x"], "G")
    ),
    "group_by_twice": lambda: GroupBy(["s"])(
        GroupBy(["s", "t"])(_ref(GROUP_ROWS, ["s", "t", "x"], "G"))
    ),
    "join_ref_ref": lambda: Join()(_a(), _b()),
    "join_plain_ref": lambda: Join()(_a(tag_only_columns=True), _b()),
    "join_three": lambda: Join()(_a(), _b(), _c()),
    "join_after_group_by": lambda: Join()(
        _grouped(), _ref([{"s": "s1", "z": 1}], ["s"], "Z")
    ),
    "semijoin": lambda: SemiJoin()(_a(), _b()),
    "select_tags": lambda: SelectTagColumns(["s"])(_grouped()),
    "map_tags": lambda: MapTags({"s": "subject"})(_grouped()),
    "map_data": lambda: MapData({"y": "w"})(_grouped()),
    "filter": lambda: PolarsFilter(constraints={"s": "s1"})(_grouped()),
    "drop_nulls": lambda: DropNulls()(_grouped()),
    "index": lambda: Index("y", 0, out="first_y")(_grouped()),
}


@pytest.mark.parametrize("make", SHAPE_CASES.values(), ids=SHAPE_CASES.keys())
def test_source_info_has_the_shape_of_its_data_column(
    make: Callable[[], StreamProtocol],
) -> None:
    assert_provenance_has_the_shape_of_its_data(make())


# ── GroupBy: the key's token ────────────────────────────────────────────────


def test_group_by_key_has_one_merged_token() -> None:
    grouped = _grouped()
    tokens = _by(grouped, "s", "s")
    assert all(GROUP_TOKEN.match(t) for t in tokens.values())
    # The digest of the members' own tokens; the non-key keeps every member's token.
    members = _by(GroupBy(["s"])(_ref(GROUP_ROWS, ["s", "t", "x"], "G")), "s", "y")
    assert tokens["s1"] == group_fold([t.replace("::y", "::s") for t in members["s1"]])
    assert len(members["s1"]) == 2


def test_a_group_of_one_member_still_has_a_merged_token() -> None:
    tokens = _by(_grouped(), "s", "s")
    assert tokens["s2"] == group_fold(["G::row_2::s"])
    assert tokens["s2"] != "G::row_2::s"


def test_grouping_a_grouped_stream_folds_the_digests() -> None:
    source = _ref(GROUP_ROWS, ["s", "t", "x"], "G")
    once = GroupBy(["s", "t"])(source)
    twice = GroupBy(["s"])(once)
    first = [token for (s, _), token in _pairs(once, ("s", "t"), "s") if s == "s1"]
    assert len(first) == 2 and all(GROUP_TOKEN.match(t) for t in first)
    assert _by(twice, "s", "s")["s1"] == group_fold(first)


def _pairs(
    stream: StreamProtocol, keys: tuple[str, ...], name: str
) -> list[tuple[tuple[Any, ...], Any]]:
    table = stream.as_table(columns={"source": True})
    rows = table.to_pylist()
    return [(tuple(r[k] for k in keys), r[f"_source_{name}"]) for r in rows]


def test_the_group_token_does_not_depend_on_row_order() -> None:
    rows = [{**r, "rid": f"r{i}"} for i, r in enumerate(GROUP_ROWS)]

    def token(ordered: Sequence[Mapping[str, Any]]) -> str:
        # A record-id column keeps each row's token when the rows are reordered.
        source = op.sources.ArrowTableSource(
            pa.Table.from_pylist(list(ordered)),
            tag_columns=["s", "t", "x"],
            source_id="G",
            record_id_columns="rid",
        )
        return _by(GroupBy(["s"])(source), "s", "s")["s1"]

    expected = token(rows)
    for permutation in itertools.permutations(rows):
        assert token(list(permutation)) == expected


def test_the_group_token_is_the_same_in_another_process() -> None:
    script = textwrap.dedent(
        """
        import orcapod as op
        from orcapod.core.operators import GroupBy
        rows = [
            {"s": "s1", "t": 1, "x": 20, "y": 5},
            {"s": "s1", "t": 2, "x": 10, "y": 4},
            {"s": "s2", "t": 1, "x": 5, "y": 1},
        ]
        source = op.sources.DictSource(rows, tag_columns=["s", "t", "x"], source_id="G")
        table = GroupBy(["s"])(source).as_table(columns={"source": True})
        print(table.column("_source_s").to_pylist()[0])
        """
    )
    runs = {
        subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "PYTHONHASHSEED": seed},
        ).stdout.strip()
        for seed in ("1", "2")
    }
    assert runs == {_by(_grouped(), "s", "s")["s1"]}


# ── Join: the shared key's token ────────────────────────────────────────────


def test_join_of_two_referential_inputs_merges_their_tokens() -> None:
    (token,) = _source_column(Join()(_a(), _b()), "x")
    assert JOIN_TOKEN.match(token)
    assert token == join_fold(["A::row_0::x", "B::row_0::x"])


def test_join_with_a_plain_input_keeps_the_referential_token() -> None:
    assert _source_column(Join()(_a(tag_only_columns=True), _b()), "x") == [
        "B::row_0::x"
    ]


def test_three_way_join_folds_all_inputs_once() -> None:
    (token,) = _source_column(Join()(_a(), _b(), _c()), "x")
    assert token == join_fold(["A::row_0::x", "B::row_0::x", "C::row_0::x"])


def _tied(sid: str):
    """A key-only referential input: every such input has one pipeline hash (a tie)."""
    return _ref([{"x": 1}], ["x"], sid)


def test_join_token_does_not_depend_on_input_order() -> None:
    assert _tied("P").pipeline_hash() == _tied("Q").pipeline_hash()  # the tie
    inputs = {"P": _tied, "Q": _tied, "A": lambda _: _a()}
    tokens = {
        _source_column(Join()(*(inputs[k](k) for k in order)), "x")[0]
        for order in itertools.permutations(inputs)
    }
    assert len(tokens) == 1
    assert tokens.pop() == join_fold(["Q::row_0::x", "A::row_0::x", "P::row_0::x"])


def test_join_after_group_by_folds_the_group_digest() -> None:
    other = _ref([{"s": "s1", "z": 1}], ["s"], "Z")
    group_token = _by(_grouped(), "s", "s")["s1"]
    (token,) = _source_column(Join()(_grouped(), other), "s")
    assert token == join_fold([group_token, "Z::row_0::s"])


# ── the helpers ─────────────────────────────────────────────────────────────


def test_a_merged_token_never_reads_as_a_row_token() -> None:
    # A row's token is <source id>::<record id>::<column>; a source id never holds
    # "::" (ITL-1154), and a folded token holds none, so the two forms never meet.
    for token in (group_fold(["G::row_0::s"]), join_fold(["A::row_0::x"])):
        assert "::" not in token


def test_the_two_folds_keep_their_own_member_rules() -> None:
    # GroupBy folds in member order; Join sorts first. Both keep nulls apart.
    assert group_fold(["b", "a"]) != group_fold(["a", "b"])
    assert join_fold(["b", "a"]) == join_fold(["a", "b"])
    for fold in (group_fold, join_fold):
        assert fold(["a"]) != fold(["a", "a"])
        assert fold([None]) != fold([""])
        assert fold(["a,b"]) != fold(["a", "b"])
    assert join_fold(["a", None]) == join_fold([None, "a"])


def test_merge_source_info_keeps_the_shape() -> None:
    fold = join_fold
    assert merge_source_info(["a", "b"], fold) == fold(["a", "b"])
    assert merge_source_info([["a", "c"], ["b", "d"]], fold) == [
        fold(["a", "b"]),
        fold(["c", "d"]),
    ]
    assert merge_source_info([[["a"]], [["b"]]], fold) == [[fold(["a", "b"])]]


@pytest.mark.parametrize(
    "values",
    [[["a"], ["b", "c"]], ["a", ["b"]]],
    ids=["unequal_lengths", "scalar_and_list"],
)
def test_merge_source_info_rejects_unequal_shapes(values: list[Any]) -> None:
    with pytest.raises(ValueError):
        merge_source_info(values, join_fold)


# ── plain tags are unchanged ────────────────────────────────────────────────


def test_plain_group_key_has_no_source_info() -> None:
    grouped = GroupBy(["s"])(
        _ref(GROUP_ROWS, ["s", "t", "x"], "G", tag_only_columns=True)
    )
    names = grouped.as_table(columns={"source": True}).column_names
    assert "_source_s" not in names and "_source_y" in names
