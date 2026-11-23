"""An operator's description names its system-tag columns as its rows do (ITL-1347).

``arrow_schema()`` is typed from the inputs' zero-row tables, without reading a row. An
input with no producer (a raw ``ArrowTableStream``) states its own pipeline hash, and the
operators that rename system tags (joins, folds) name them after it. A raw stream's meta
(``__``) columns enter that hash while its zero-row table has none, so the description
must take the input's own pipeline hash, never one re-derived from the zero-row table:
its system-tag names then equal those of the operator's sync table, and of its async
result, whose record batches are checked against the description.

Inputs come from the shared several-items family (``several_items``): two or more tags
and two or more data columns, in every tag order and table layout, with plain, data-tag
and mixed roles, with and without a meta column.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence

import pyarrow as pa
import pytest

from orcapod.core.data_tags import stream_data_tags
from orcapod.core.operators import Batch, GroupBy, Join, MergeJoin, SemiJoin
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.record_batch_channels import OutputChannel
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.base import typed_zero_row_table
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from tests.test_core.operators.several_items import (
    MAIN,
    P,
    Q,
    Arrangement,
    Spec,
    arrangements,
    make_source,
)

pytestmark = pytest.mark.filterwarnings(
    "ignore::orcapod.errors.ProvisionalHashWarning",
    "ignore::orcapod.errors.SemiJoinIgnoredColumnWarning",
)

SYSTEM_TAGS = {"system_tags": True}


def _raw(spec: Spec, arrangement: Arrangement, *, meta: bool) -> ArrowTableStream:
    """``spec`` in ``arrangement`` as a raw stream over its source's table (system tags
    and source info), with a meta column when ``meta``."""
    source = make_source(spec, arrangement)
    table = source.as_table(columns={"system_tags": True, "source": True})
    if meta:
        table = table.append_column(
            "__note",
            pa.array([f"n{i}" for i in range(table.num_rows)], pa.large_string()),
        )
    return ArrowTableStream(
        table,
        tag_columns=list(arrangement.tag_order),
        data_tags=stream_data_tags(source),
    )


def _paired(arrangement: Arrangement) -> Arrangement:
    """The arrangement of Q matching ``arrangement`` of P: the same layout and roles,
    Q's tags in the same direction (declared or reversed)."""
    declared = arrangement.tag_order == P.tags
    order = Q.tags if declared else tuple(reversed(Q.tags))
    return Arrangement(order, arrangement.layout, arrangement.roles)


def _id(arrangement: Arrangement) -> str:
    return f"{'-'.join(arrangement.tag_order)}:{arrangement.layout}:{arrangement.roles}"


# One operator over raw inputs built in one arrangement.
Case = tuple[
    Callable[[], StaticOutputOperatorPod],
    Callable[[Arrangement, bool], Sequence[StreamProtocol]],
]


def _binary(arrangement: Arrangement, meta: bool) -> Sequence[StreamProtocol]:
    return (
        _raw(P, arrangement, meta=meta),
        _raw(Q, _paired(arrangement), meta=meta),
    )


def _unary(arrangement: Arrangement, meta: bool) -> Sequence[StreamProtocol]:
    return (_raw(MAIN, arrangement, meta=meta),)


BINARY: dict[str, Callable[[], StaticOutputOperatorPod]] = {
    "join": Join,
    "merge_join": MergeJoin,
    "semi_join": SemiJoin,
}
UNARY: dict[str, Callable[[], StaticOutputOperatorPod]] = {
    "batch": lambda: Batch(batch_size=2),
    "group_by_t1_t2": lambda: GroupBy(["t1", "t2"]),
    "group_by_t2_t1": lambda: GroupBy(["t2", "t1"]),
}

CASES: list[tuple[str, Case, Arrangement]] = [
    (name, (make_op, _binary), arrangement)
    for name, make_op in BINARY.items()
    for arrangement in arrangements(P)
] + [
    (name, (make_op, _unary), arrangement)
    for name, make_op in UNARY.items()
    for arrangement in arrangements(MAIN)
]

PARAMS = [
    pytest.param(case, arrangement, id=f"{name}[{_id(arrangement)}]")
    for name, case, arrangement in CASES
]


def _system_tags(names: Sequence[str]) -> list[str]:
    return sorted(name for name in names if name.startswith("_tag"))


def _sync_names(stream: StreamProtocol) -> list[str] | None:
    """The sync table's system-tag names, or ``None`` when the sync path refuses."""
    try:
        return _system_tags(stream.as_table(columns=SYSTEM_TAGS).column_names)
    except InputValidationError:
        return None


@pytest.mark.parametrize("meta", [True, False], ids=["with_meta", "without_meta"])
@pytest.mark.parametrize(("case", "arrangement"), PARAMS)
def test_the_description_names_system_tags_as_the_sync_rows_do(
    case: Case, arrangement: Arrangement, meta: bool
) -> None:
    """``arrow_schema()`` and the typed zero-row table (a head or a run with no rows)
    name the system-tag columns as the sync table does; where the sync path refuses
    the inputs, so does the description."""
    make_op, make_inputs = case
    stream = make_op()(*make_inputs(arrangement, meta))
    actual = _sync_names(stream)
    if actual is None:
        with pytest.raises(InputValidationError):
            _ = stream.arrow_schema()
        return
    assert actual
    assert _system_tags(stream.arrow_schema().names) == actual
    zero_rows = typed_zero_row_table(stream)
    assert zero_rows is not None
    assert _system_tags(zero_rows.column_names) == actual


@pytest.mark.parametrize(("case", "arrangement"), PARAMS)
def test_the_async_result_names_system_tags_as_the_description_does(
    case: Case, arrangement: Arrangement
) -> None:
    """The async path, where the description is used: the operator's record batches
    pass the output channel's check against ``arrow_schema()``, and the result names
    the system-tag columns as the description and the sync table do (inputs with a
    meta column)."""
    make_op, make_inputs = case
    inputs = make_inputs(arrangement, True)
    stream = make_op()(*inputs)
    expected = _sync_names(stream)
    if expected is None:
        pytest.skip("the sync path refuses these inputs (checked above)")
    op = make_op()

    async def run() -> StreamProtocol:
        output = OutputChannel(op(*inputs))
        await op.async_execute_batches(
            [stream_input.open_channel() for stream_input in inputs], output
        )
        return output.result()

    result = asyncio.run(run())
    names = _system_tags(result.as_table(columns=SYSTEM_TAGS).column_names)
    assert names == expected
    assert _system_tags(stream.arrow_schema().names) == names
