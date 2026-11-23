"""A stream lists its tags in one order: ``keys()`` and ``output_schema()`` agree (ITL-1365).

``ArrowTableStream`` built its tag schema in the order of its table's columns while its
``keys()`` (and its data schema) follow the declared tag order. Every operator's
predicted schema starts from its input's ``output_schema()``, so for a table not laid
out in tag order a prediction listed the tags in another order than the computed
result: ``DropTagColumns(["a"])`` over tags ``c, b, a`` predicted ``("b", "c")`` and
computed ``("c", "b")``. The tag schema now follows the tag order; no row, table,
stored column order or hash changes (``docs/dev/scripts/itl1365_tag_schema_order.py``
compares a job before and after).

The sweep runs every select, drop and map operator over a source with three tags and
three data columns, in every tag order and table layout, with every set of data-tags,
and compares the predicted ``keys()`` and ``output_schema()`` order with
``static_process(...)`` (CLAUDE.md's standing exception), and the tag order of the sync
and the async rows with the prediction.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.operators import (
    DropColumns,
    DropDataColumns,
    DropTagColumns,
    MapColumns,
    MapData,
    MapTags,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
)
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import ProvisionalHashWarning

from .operators.several_items import MAIN, LAYOUTS, _layout, async_rows
from .test_order_invariance import STREAM_CASES, StreamCase

pytestmark = [
    pytest.mark.filterwarnings(
        f"ignore::{ProvisionalHashWarning.__module__}.{ProvisionalHashWarning.__name__}"
    ),
    pytest.mark.filterwarnings("ignore::UserWarning"),
]

CONFIGS: tuple[dict[str, Any], ...] = (
    {},
    {"columns": {"system_tags": True}},
    {"columns": {"source": True, "meta": True, "context": True}},
    {"all_info": True},
)


# ── every source and stream kind ─────────────────────────────────────────────


@pytest.mark.parametrize("case", STREAM_CASES, ids=lambda c: c.label)
def test_a_stream_lists_its_tags_in_keys_order(
    case: StreamCase, tmp_path: Path
) -> None:
    """``output_schema()[0]`` lists the tags in ``keys()[0]`` order, in every layout."""
    for n, (role, order, layout) in enumerate(
        itertools.product(case.roles, range(3), LAYOUTS)
    ):
        folder = tmp_path / str(n)
        folder.mkdir()
        stream = case.build(role, order, layout, folder)
        for config in CONFIGS:
            label = f"{role}/{order}/{layout} {config}"
            tag_keys = stream.keys(**config)[0]
            assert tuple(stream.output_schema(**config)[0]) == tuple(tag_keys), label


def test_the_reported_case() -> None:
    """Tags ``c, b, a`` over a table laid out ``a, b, c, x``; ``a`` and ``b`` data-tags."""
    table = pa.table({"a": [1, 2], "b": [3, 4], "c": [7, 8], "x": [5.0, 6.0]})
    stream = ArrowTableStream(table, tag_columns=["c", "b", "a"], data_tags=["a", "b"])
    assert tuple(stream.output_schema()[0]) == stream.keys()[0] == ("c", "b", "a")
    op = DropTagColumns(["a"])
    assert op(stream).keys() == op.static_process(stream).keys()
    assert op(stream).keys()[0] == ("c", "b")


# ── select, drop and map operators ───────────────────────────────────────────


def _source(order: tuple[str, ...], layout: str, data_tags: tuple[str, ...]) -> Any:
    names = _layout(MAIN, order, layout)
    table = pa.table({name: MAIN.columns[name] for name in names})
    return ArrowTableSource(
        table,
        tag_columns=list(order),
        source_id="s",
        tag_only_columns=[t for t in order if t not in data_tags],
    )


OPERATORS: dict[str, Callable[[], StaticOutputOperatorPod]] = {
    "select_tag_columns": lambda: SelectTagColumns(["t3", "t1"]),
    "drop_tag_columns_one": lambda: DropTagColumns(["t1"]),
    "drop_tag_columns_two": lambda: DropTagColumns(["t3", "t1"]),
    "select_data_columns": lambda: SelectDataColumns(["d3", "d1"]),
    "drop_data_columns": lambda: DropDataColumns(["d2"]),
    "select_columns": lambda: SelectColumns(["t3", "d3", "t1", "d1"]),
    "drop_columns": lambda: DropColumns(["d2", "t2"]),
    "map_tags": lambda: MapTags({"t3": "u3", "t1": "u1"}),
    "map_tags_drop": lambda: MapTags({"t3": "u3", "t1": "u1"}, drop_unmapped=True),
    "map_tags_tag_only": lambda: MapTags({"t3": "u3", "t1": "u1"}, tag_only=True),
    "map_data": lambda: MapData({"d3": "e3", "d1": "e1"}),
    "map_data_drop": lambda: MapData({"d3": "e3", "d1": "e1"}, drop_unmapped=True),
    "map_columns": lambda: MapColumns({"t3": "u3", "d1": "e1"}),
    "map_columns_drop": lambda: MapColumns(
        {"t3": "u3", "t1": "u1", "d1": "e1"}, drop_unmapped=True
    ),
}
# MapColumns is refused with data-tags (ITL-1022).
PLAIN_ONLY = {"map_columns", "map_columns_drop"}

DATA_TAG_SETS = [
    tuple(c)
    for n in range(len(MAIN.tags) + 1)
    for c in itertools.combinations(MAIN.tags, n)
]


def _demoted(stream: Any, result: Any) -> set[str]:
    """Data-tags of ``stream`` that are data, but no tag, of ``result``."""
    tags, data = stream.keys()
    out_tags, out_data = result.keys()
    return {c for c in tags if c in data and c not in out_tags and c in out_data}


@pytest.mark.parametrize(
    "data_tags", DATA_TAG_SETS, ids=lambda d: "+".join(d) or "none"
)
@pytest.mark.parametrize("name", list(OPERATORS))
def test_the_prediction_lists_the_columns_in_the_result_order(
    name: str, data_tags: tuple[str, ...]
) -> None:
    if name in PLAIN_ONLY and data_tags:
        pytest.skip("MapColumns is refused with data-tags (ITL-1022)")
    for order in MAIN.tag_orders():
        for layout in LAYOUTS:
            source = _source(order, layout, data_tags)
            op = OPERATORS[name]()
            predicted = op(source)
            computed = op.static_process(source)
            label = f"{'-'.join(order)}/{layout}"
            for config in CONFIGS:
                p_tags, p_data = predicted.keys(**config)
                c_tags, c_data = computed.keys(**config)
                assert p_tags == c_tags, f"{label} {config}"
                p_schema = predicted.output_schema(**config)
                assert tuple(p_schema[0]) == c_tags, f"{label} {config}"
                if _demoted(source, computed):
                    # A data-tag that loses its tag role keeps its data column at its
                    # table position in the result and right after the data-tags in
                    # the prediction (ITL-1249): the names agree here.
                    assert sorted(p_data) == sorted(c_data), f"{label} {config}"
                else:
                    assert p_data == c_data, f"{label} {config}"
                    assert tuple(p_schema[1]) == c_data, f"{label} {config}"
            # The rows, sync and async, list their tags in the predicted order.
            tags = predicted.keys()[0]
            for tag, _ in computed.iter_data():
                assert tuple(tag.keys()) == tags, label
            for tag, _ in async_rows(op, source):
                assert tuple(tag.keys()) == tags, label
