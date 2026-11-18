"""Streams with several tag and several data columns, in several arrangements (ITL-1236).

An argument that takes several items (column names, a mapping, input streams) must be
tested with two or more items, in more than one order, in each role a column can have
(plain tag, data, data-tag), on inputs with two or more tags AND two or more data columns,
on the sync and the async path. A stream with one tag and one data column cannot show a
result that depends on column order or role: ``GroupBy``'s did (ITL-1226).

This module is the shared family of such inputs. ``main_source`` has three tags
(``t1``, ``t2``, ``t3``) and three data columns (``d1``, ``d2``, ``d3``); the join
partners ``P``, ``Q`` and ``R`` share some of those tags and add data columns of their
own. Each source can be built in any ``Arrangement``:

* the order of its tag columns (``tag_order``);
* the physical order of its table's columns (``layout``: tags first, data first and
  reversed, or interleaved);
* the role of its tags (``roles``): all plain (``tag_only_columns=True``), all data-tags
  (the default of a source), or mixed (one plain tag, the others data-tags).

``fingerprint`` is what must not depend on an arrangement or on the order of an argument
that names a set: the rows (values, system tags, row hashes, source info), the stream's
content and pipeline hashes, and the column names of each group. ``presentation`` is the
column order, which follows the input as for every operator and enters no hash.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, TypeAlias

import pyarrow as pa

from orcapod.channels import Channel
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.sources import ArrowTableSource
from orcapod.protocols.core_protocols import (
    DataProtocol,
    StreamProtocol,
    TagProtocol,
)

Row: TypeAlias = tuple[TagProtocol, DataProtocol]

# Roles of a source's tags.
PLAIN = "plain"
DATA_TAGS = "data_tags"
MIXED = "mixed"
ROLES = (PLAIN, DATA_TAGS, MIXED)

# Layouts of a table: where the tag columns sit among the data columns.
TAGS_FIRST = "tags_first"
DATA_FIRST_REVERSED = "data_first_reversed"
INTERLEAVED = "interleaved"
LAYOUTS = (TAGS_FIRST, DATA_FIRST_REVERSED, INTERLEAVED)


@dataclass(frozen=True)
class Spec:
    """The columns of one source: tags, data, values and the plain tag of ``MIXED``."""

    source_id: str
    tags: tuple[str, ...]
    data: tuple[str, ...]
    columns: dict[str, "pa.Array[Any]"]
    mixed_plain: str

    def tag_orders(self) -> tuple[tuple[str, ...], ...]:
        """The declared order, its reverse, and (with three tags) a rotation."""
        orders = [self.tags, tuple(reversed(self.tags))]
        if len(self.tags) > 2:
            orders.append(self.tags[1:] + self.tags[:1])
        return tuple(orders)


def _array(values: list[Any], arrow_type: pa.DataType) -> "pa.Array[Any]":
    return pa.array(values, arrow_type)


# Six rows, each tag tuple once. ``t1`` and ``t2`` repeat on their own; ``d2`` holds a
# null (nullable); the other data columns hold none and are non-nullable.
MAIN = Spec(
    source_id="s",
    tags=("t1", "t2", "t3"),
    data=("d1", "d2", "d3"),
    columns={
        "t1": _array(["b", "a", "a", "b", "a", "b"], pa.large_string()),
        "t2": _array([2, 1, 2, 1, 1, 2], pa.int64()),
        "t3": _array([1, 2, 1, 2, 1, 2], pa.int64()),
        "d1": _array([10, 20, 30, 40, 50, 60], pa.int64()),
        "d2": _array([0.5, None, 2.5, 3.5, 4.5, None], pa.float64()),
        "d3": _array(["w", "x", "y", "z", "v", "u"], pa.large_string()),
    },
    mixed_plain="t2",
)

# Join partners. Each shares tags with ``MAIN`` and with each other, never a data name.
P = Spec(
    source_id="p",
    tags=("t1", "t2"),
    data=("p1", "p2"),
    columns={
        "t1": _array(["a", "b", "a", "b"], pa.large_string()),
        "t2": _array([2, 1, 1, 2], pa.int64()),
        "p1": _array([7, 8, 9, 6], pa.int64()),
        "p2": _array(["m", "n", "o", "q"], pa.large_string()),
    },
    mixed_plain="t1",
)
Q = Spec(
    source_id="q",
    tags=("t2", "t3"),
    data=("q1", "q2"),
    columns={
        "t2": _array([1, 2, 2, 1], pa.int64()),
        "t3": _array([2, 1, 2, 1], pa.int64()),
        "q1": _array([0.25, 0.75, 1.25, 1.75], pa.float64()),
        "q2": _array([100, 200, 300, 400], pa.int64()),
    },
    mixed_plain="t3",
)
R = Spec(
    source_id="r",
    tags=("t1",),
    data=("r1", "r2"),
    columns={
        "t1": _array(["b", "a"], pa.large_string()),
        "r1": _array([-1, -2], pa.int64()),
        "r2": _array(["rb", "ra"], pa.large_string()),
    },
    mixed_plain="t1",
)


@dataclass(frozen=True)
class Arrangement:
    """How a source presents its columns: tag order, table layout and tag roles."""

    tag_order: tuple[str, ...]
    layout: str
    roles: str

    @property
    def id(self) -> str:
        return f"{'-'.join(self.tag_order)}/{self.layout}/{self.roles}"


def arrangements(spec: Spec, roles: Sequence[str] = ROLES) -> list[Arrangement]:
    """Every tag order of ``spec.tag_orders()`` in every layout, for each role."""
    return [
        Arrangement(order, layout, role)
        for role in roles
        for order in spec.tag_orders()
        for layout in LAYOUTS
    ]


def _layout(spec: Spec, tag_order: tuple[str, ...], layout: str) -> list[str]:
    """The physical column order of the table."""
    if layout == TAGS_FIRST:
        return [*tag_order, *spec.data]
    if layout == DATA_FIRST_REVERSED:
        return [*reversed(spec.data), *reversed(tag_order)]
    if layout == INTERLEAVED:
        names: list[str] = []
        for i in range(max(len(tag_order), len(spec.data))):
            if i < len(spec.data):
                names.append(spec.data[i])
            if i < len(tag_order):
                names.append(tag_order[i])
        return names
    raise ValueError(f"unknown layout {layout!r}")


def make_source(
    spec: Spec,
    arrangement: Arrangement | None = None,
    *,
    source_id: str | None = None,
    rows: Sequence[int] | None = None,
) -> ArrowTableSource:
    """Build ``spec`` as an ``ArrowTableSource`` in ``arrangement``.

    Args:
        spec: The columns and values.
        arrangement: Tag order, layout and roles; default: declared order, tags
            first, plain tags.
        source_id: The source id; default ``spec.source_id``.
        rows: The row positions to keep, in this order; default every row.

    Returns:
        The source. A column is nullable exactly when it holds a null.
    """
    arrangement = arrangement or Arrangement(spec.tags, TAGS_FIRST, PLAIN)
    names = _layout(spec, arrangement.tag_order, arrangement.layout)
    columns = {name: spec.columns[name] for name in names}
    if rows is not None:
        columns = {name: column.take(list(rows)) for name, column in columns.items()}
    schema = pa.schema(
        pa.field(name, column.type, nullable=column.null_count > 0)
        for name, column in columns.items()
    )
    table = pa.table(list(columns.values()), schema=schema)
    if arrangement.roles == PLAIN:
        tag_only: bool | list[str] = True
    elif arrangement.roles == DATA_TAGS:
        tag_only = []
    elif arrangement.roles == MIXED:
        tag_only = [spec.mixed_plain]
    else:
        raise ValueError(f"unknown roles {arrangement.roles!r}")
    return ArrowTableSource(
        table,
        tag_columns=list(arrangement.tag_order),
        source_id=source_id or spec.source_id,
        tag_only_columns=tag_only,
    )


def row_fingerprint(tag: TagProtocol, data: DataProtocol) -> tuple[str, ...]:
    """What identifies one row regardless of column order: values, system tags, hashes."""
    return (
        repr(sorted(tag.as_dict().items())),
        repr(sorted(data.as_dict().items())),
        repr(sorted(tag.system_tags().items())),
        tag.content_hash().to_string(),
        data.content_hash().to_string(),
        repr(sorted(data.source_info().items())),
    )


def rows_fingerprint(rows: Sequence[Row]) -> list[tuple[str, ...]]:
    """The rows' fingerprints, sorted: row order is not part of it."""
    return sorted(row_fingerprint(tag, data) for tag, data in rows)


def fingerprint(stream: StreamProtocol) -> dict[str, Any]:
    """What must not depend on the input's arrangement or a set argument's order.

    Args:
        stream: An operator's result.

    Returns:
        The rows (``rows_fingerprint``), the content and pipeline hashes, and the
        sorted column names of the tag group (system tags included) and the data group.
    """
    tag_keys, data_keys = stream.keys(columns={"system_tags": True})
    return {
        "rows": rows_fingerprint(list(stream.iter_data())),
        "content_hash": stream.content_hash().to_string(),
        "pipeline_hash": stream.pipeline_hash().to_string(),
        "tag_names": sorted(tag_keys),
        "data_names": sorted(data_keys),
    }


def presentation(stream: StreamProtocol) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The column order of each group: follows the input, enters no hash."""
    tag_keys, data_keys = stream.keys()
    return tuple(tag_keys), tuple(data_keys)


async def _run_async(
    op: StaticOutputOperatorPod, sources: Sequence[StreamProtocol]
) -> list[Row]:
    channels: list[Channel[Row]] = [Channel(buffer_size=64) for _ in sources]
    output: Channel[Row] = Channel(buffer_size=64)
    for stream, channel in zip(sources, channels):
        for row in stream.iter_data():
            await channel.writer.send(row)
        await channel.writer.close()
    await op.async_execute(
        [channel.reader for channel in channels],
        output.writer,
        input_pipeline_hashes=[s.pipeline_hash() for s in sources],
    )
    return await output.reader.collect()


def async_rows(op: StaticOutputOperatorPod, *sources: StreamProtocol) -> list[Row]:
    """The rows of the async path, ``op.async_execute`` over channels."""
    return asyncio.run(_run_async(op, sources))
