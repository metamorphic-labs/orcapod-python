"""An operator's description names its system-tag columns as its rows do (ITL-1347).

``arrow_schema()`` is typed from the inputs' zero-row tables, without reading a row. An
input with no producer (a raw ``ArrowTableStream``) states its own pipeline hash, and the
operators that rename system tags (joins, folds) name them after it. A raw stream's meta
(``__``) columns enter that hash while its zero-row table has none, so the description
must take the input's own pipeline hash, never one re-derived from the zero-row table:
its system-tag names then equal those of the operator's sync table.
"""

from __future__ import annotations

from collections.abc import Callable

import pyarrow as pa
import pytest

from orcapod.core.operators import Batch, GroupBy, Join, MergeJoin, SemiJoin
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.base import typed_zero_row_table
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.sources import ArrowTableSource

pytestmark = pytest.mark.filterwarnings(
    "ignore::orcapod.errors.ProvisionalHashWarning",
    "ignore::orcapod.errors.SemiJoinIgnoredColumnWarning",
)


def _raw(source_id: str, value: str, *, meta: bool) -> ArrowTableStream:
    """A raw stream over a source's table (its system tags and source info), with a
    meta column when ``meta``: tags ``k`` and ``g``, data ``value``."""
    source = ArrowTableSource(
        pa.table(
            {
                "k": pa.array([1, 2, 3], pa.int64()),
                "g": pa.array(["a", "a", "b"], pa.large_string()),
                value: pa.array([0.5, 1.5, 2.5], pa.float64()),
            }
        ),
        tag_columns=["k", "g"],
        tag_only_columns=True,
        source_id=source_id,
    )
    table = source.as_table(columns={"system_tags": True, "source": True})
    if meta:
        table = table.append_column(
            "__note", pa.array(["n1", "n2", "n3"], pa.large_string())
        )
    return ArrowTableStream(table, tag_columns=["k", "g"])


CASES: dict[
    str,
    tuple[
        Callable[[], StaticOutputOperatorPod],
        Callable[[bool], tuple[StreamProtocol, ...]],
    ],
] = {
    "join": (Join, lambda meta: (_raw("a", "x", meta=meta), _raw("b", "y", meta=meta))),
    "merge_join": (
        MergeJoin,
        lambda meta: (_raw("a", "x", meta=meta), _raw("b", "y", meta=meta)),
    ),
    "semi_join": (
        SemiJoin,
        lambda meta: (_raw("a", "x", meta=meta), _raw("b", "y", meta=meta)),
    ),
    "batch": (lambda: Batch(batch_size=2), lambda meta: (_raw("a", "x", meta=meta),)),
    "group_by": (lambda: GroupBy(["g"]), lambda meta: (_raw("a", "x", meta=meta),)),
}


def _system_tags(names: list[str]) -> list[str]:
    return sorted(name for name in names if name.startswith("_tag"))


@pytest.mark.parametrize("meta", [True, False], ids=["with_meta", "without_meta"])
@pytest.mark.parametrize("case", list(CASES))
def test_the_description_names_system_tags_as_the_rows_do(
    case: str, meta: bool
) -> None:
    """``arrow_schema()``'s system-tag names equal those of the sync table, over raw
    streams with and without a meta column."""
    make_op, make_inputs = CASES[case]
    stream = make_op()(*make_inputs(meta))
    described = _system_tags(stream.arrow_schema().names)
    actual = _system_tags(stream.as_table(columns={"system_tags": True}).column_names)
    assert described
    assert described == actual


@pytest.mark.parametrize("case", list(CASES))
def test_the_zero_row_table_names_system_tags_as_the_rows_do(case: str) -> None:
    """The typed zero-row table (what a head and a run with no rows show) has the sync
    table's system-tag columns too, over inputs with a meta column."""
    make_op, make_inputs = CASES[case]
    stream = make_op()(*make_inputs(True))
    zero_rows = typed_zero_row_table(stream)
    assert zero_rows is not None
    actual = stream.as_table(columns={"system_tags": True}).column_names
    assert _system_tags(zero_rows.column_names) == _system_tags(actual)
