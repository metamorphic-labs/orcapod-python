"""``MergeJoin`` on referential tags (ITL-1194).

``MergeJoin`` follows ``Join`` (developer-approved design, November 17, 2026):

1. A tag ``x`` referential in both inputs is a join key, so its two values are equal:
   the output has ONE scalar referential column ``x``, and its source info is one
   token, the ``join_v1`` fold of the two inputs' tokens (sorted).
2. ``x`` referential in one input and a plain tag in the other stays referential,
   with its own token.
3. ``x`` referential in one input and absent from the other stays referential, with
   its own token.
4. ``x`` referential in one input and a data-only column in the other is refused (the
   tag and data role check).

Every other colliding data column is merged into a sorted list, as for plain inputs.
A shared key that is ``T`` in one input and ``T | None`` in the other is ``T`` in
both roles (ITL-829). Every operator is driven through its public call and read
through the stream's public methods; a schema prediction is checked against
``static_process``.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.operators import Join, MapData, MergeJoin
from orcapod.core.operators.join import JOIN_FOLD_LABEL
from orcapod.core.referential_tags import stream_references
from orcapod.core.sources import ArrowTableSource
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils

JOIN_TOKEN = re.compile(rf"^{JOIN_FOLD_LABEL}:[0-9a-f]{{64}}$")


def join_fold(tokens: Sequence[str | None]) -> str:
    """The token of a key referential in both inputs: the tokens sorted, then folded."""
    ordered = sorted(tokens, key=lambda token: (token is None, token or ""))
    return arrow_utils.fold_system_tag_values(ordered, label=JOIN_FOLD_LABEL)


def _source(
    sid: str, values: Sequence[float], tags: Sequence[str] = ("k", "x"), **kw: Any
) -> op.sources.DictSource:
    """Tags ``k`` and ``x`` (referential unless ``tag_only_columns=True``); data ``v``."""
    rows = [
        {"k": k, "x": 10 * k, "v": v} for k, v in zip(range(1, len(values) + 1), values)
    ]
    return op.sources.DictSource(rows, tag_columns=list(tags), source_id=sid, **kw)


def _without_x(sid: str, values: Sequence[float]) -> op.sources.DictSource:
    """Plain tag ``k``, data ``v``: no column ``x`` at all."""
    rows = [{"k": k, "v": v} for k, v in zip(range(1, len(values) + 1), values)]
    return op.sources.DictSource(
        rows, tag_columns=["k"], source_id=sid, tag_only_columns=True
    )


def _a() -> op.sources.DictSource:
    return _source("A", [1.0, 2.0])


def _b() -> op.sources.DictSource:
    return _source("B", [3.0, 0.5])


def _by_k(stream: StreamProtocol) -> dict[int, dict[str, Any]]:
    table = stream.as_table(all_info=True)
    return {row["k"]: row for row in table.to_pylist()}


# ── case 1: referential in both inputs ──────────────────────────────────────


def test_a_tag_referential_in_both_is_one_scalar_referential_column() -> None:
    merged = MergeJoin()(_a(), _b())
    assert merged.keys() == (("k", "x"), ("k", "x", "v"))
    assert stream_references(merged) == ("k", "x")
    rows = _by_k(merged)
    # The key is one value, not a two-element list; the other collision is a list.
    assert {k: row["x"] for k, row in rows.items()} == {1: 10, 2: 20}
    assert {k: row["v"] for k, row in rows.items()} == {1: [1.0, 3.0], 2: [0.5, 2.0]}
    for tag, data in merged.iter_data():
        assert tag["x"] == data["x"]


def test_its_source_info_is_one_join_token_of_both_tokens() -> None:
    rows = _by_k(MergeJoin()(_a(), _b()))
    source_x = f"{constants.SOURCE_PREFIX}x"
    for k, row in rows.items():
        assert JOIN_TOKEN.match(row[source_x])
        assert row[source_x] == join_fold([f"A::row_{k - 1}::x", f"B::row_{k - 1}::x"])
    # A merged list keeps one token per member, in the order of its values.
    assert rows[2][f"{constants.SOURCE_PREFIX}v"] == ["B::row_1::v", "A::row_1::v"]


def test_the_token_is_the_one_join_gives() -> None:
    """One fold, ``Join``'s: the same inputs joined by ``Join`` give the same token."""
    source_x = f"{constants.SOURCE_PREFIX}x"
    merged = _by_k(MergeJoin()(_a(), _b()))
    joined = _by_k(Join()(_a(), MapData({"v": "w"})(_b())))
    assert {k: row[source_x] for k, row in merged.items()} == {
        k: row[source_x] for k, row in joined.items()
    }


def test_the_inputs_order_does_not_matter() -> None:
    forward = MergeJoin()(_a(), _b()).as_table(all_info=True)
    backward = MergeJoin()(_b(), _a()).as_table(all_info=True)
    assert forward.equals(backward)


# ── cases 2 and 3: referential in one input only ────────────────────────────


@pytest.mark.parametrize(
    "other",
    [
        lambda: _source("B", [3.0, 0.5], tag_only_columns=True),
        lambda: _without_x("B", [3.0, 0.5]),
    ],
    ids=["plain_tag", "absent"],
)
def test_a_tag_referential_in_one_input_keeps_its_own_token(other: Any) -> None:
    for left, right in ((_a(), other()), (other(), _a())):
        merged = MergeJoin()(left, right)
        assert merged.keys() == (("k", "x"), ("k", "x", "v"))
        assert stream_references(merged) == ("k", "x")
        rows = _by_k(merged)
        for k, row in rows.items():
            assert row["x"] == 10 * k
            assert row[f"{constants.SOURCE_PREFIX}x"] == f"A::row_{k - 1}::x"


# ── case 4: referential against data-only ───────────────────────────────────


def test_a_referential_tag_against_a_data_only_column_is_refused() -> None:
    data_only = _source("D", [3.0, 0.5], tags=("k",), tag_only_columns=True)
    for left, right in ((_a(), data_only), (data_only, _a())):
        with pytest.raises(InputValidationError, match="column 'x' is a tag"):
            _ = MergeJoin()(left, right)


def test_a_referential_tag_of_two_types_is_refused() -> None:
    other = op.sources.DictSource(
        [{"k": 1, "x": "ten", "v": 3.0}], tag_columns=["k", "x"], source_id="S"
    )
    # Refused where the tag schemas meet, as for a plain tag of two types.
    with pytest.raises(InputValidationError, match="not compatible"):
        _ = MergeJoin()(_a(), other)


# ── nullability (ITL-829) ───────────────────────────────────────────────────


def _arrow(sid: str, x_nullable: bool, xs: list[int | None]) -> ArrowTableSource:
    schema = pa.schema(
        [
            pa.field("x", pa.int64(), nullable=x_nullable),
            pa.field("v", pa.float64(), nullable=False),
        ]
    )
    table = pa.table({"x": xs, "v": [float(i) for i in range(len(xs))]}, schema=schema)
    return ArrowTableSource(table, tag_columns=["x"], source_id=sid)


@pytest.mark.parametrize(
    ("left_nullable", "right_nullable", "expected"),
    [(False, True, int), (True, False, int), (True, True, int | None)],
)
def test_a_merged_key_takes_the_join_nullability_in_both_roles(
    left_nullable: bool, right_nullable: bool, expected: Any
) -> None:
    left = _arrow("L", left_nullable, [1, 2])
    right = _arrow("R", right_nullable, [1, 2] if not right_nullable else [1, None])
    merged = MergeJoin()(left, right)
    tag_schema, data_schema = merged.output_schema()
    assert tag_schema["x"] == expected
    assert data_schema["x"] == expected
    computed = MergeJoin().static_process(left, right)
    assert computed.output_schema() == merged.output_schema()
    assert computed.as_table().schema.field("x").nullable == (expected is not int)


# ── schema prediction, zero rows ────────────────────────────────────────────


@pytest.mark.parametrize("all_info", [False, True])
@pytest.mark.parametrize(
    "inputs",
    [
        lambda: (_a(), _b()),
        lambda: (_a(), _source("B", [3.0, 0.5], tag_only_columns=True)),
        lambda: (_without_x("B", [3.0, 0.5]), _a()),
    ],
    ids=["both", "plain_tag", "absent"],
)
def test_the_schema_is_predicted(inputs: Any, all_info: bool) -> None:
    left, right = inputs()
    merge_join = MergeJoin()
    predicted = merge_join.output_schema(left, right, all_info=all_info)
    computed = merge_join.static_process(left, right)
    assert predicted == computed.output_schema(all_info=all_info)
    # The order too: the referential columns lead the data, in tag order.
    assert [list(schema) for schema in predicted] == [
        list(schema) for schema in computed.output_schema(all_info=all_info)
    ]
    assert computed.keys() == merge_join(left, right).keys()


def test_zero_rows_have_the_types_of_rows() -> None:
    full = MergeJoin()(_a(), _b()).as_table(all_info=True)
    empty = MergeJoin()(_a().polars_filter(k=99), _b()).as_table(all_info=True)
    assert empty.num_rows == 0

    # The filter is a step of its own, so only the system-tag NAMES differ.
    def fields(schema: pa.Schema) -> list[Any]:
        return [
            ("system tag", f.type, f.nullable)
            if f.name.startswith(constants.SYSTEM_TAG_PREFIX)
            else f
            for f in schema
        ]

    assert fields(empty.schema) == fields(full.schema)


# ── plain inputs unchanged ──────────────────────────────────────────────────


def test_plain_inputs_merge_as_before() -> None:
    merged = MergeJoin()(
        _source("A", [1.0, 2.0], tag_only_columns=True),
        _source("B", [3.0, 0.5], tag_only_columns=True),
    )
    assert merged.keys() == (("k", "x"), ("v",))
    assert stream_references(merged) == ()
    assert {k: row["v"] for k, row in _by_k(merged).items()} == {
        1: [1.0, 3.0],
        2: [0.5, 2.0],
    }
