"""``Filter`` passes every value it keeps bit for bit (ITL-1296).

``PolarsFilter`` carries every column through Polars and changes some values (a
``date64`` off midnight comes back at midnight). ``Filter`` chooses rows with an Arrow
mask and takes them from the input table, so every column, read by the condition or
not, must come out as it went in. This is the probe of ITL-1296 kept as a test: columns
of the types another representation changes (``date64`` off midnight, ``time64[ns]``
below a microsecond, a float32 signaling NaN, ``-0.0``, ``decimal128`` and
``decimal256`` at full precision, logical types, strings and binaries of both widths,
lists and structs of them), each as a plain tag, as data and as a data-tag, in two
arrangements of the table, on the sync path, the row channel path and in a job run by
either orchestrator. On every path the rows handed out one by one are compared too, the
tag half and the data half each on its own: a data-tag is a column of both. The last
tests change the filter (one column carried through Polars) and require the comparison
of every path to fail.

A dictionary-encoded column is not here: no stream holds one (ITL-1264).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import decimal
import struct
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeAlias, cast

from typing_extensions import override

import polars as pl

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod import PipelineJob, col
from orcapod.channels import Channel
from orcapod.contexts import get_default_type_converter
from orcapod.core.datagrams import Data, Tag
from orcapod.core.operators import Filter
from orcapod.core.operators import filter as filter_module
from orcapod.core.operators.filter_mask import filter_table
from orcapod.core.operators.meta_columns import WithoutMetaWriter
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.types import Condition
from orcapod.utils import arrow_utils

Row: TypeAlias = tuple[TagProtocol, DataProtocol]
Values: TypeAlias = "pa.Array[Any] | pa.ChunkedArray[Any]"

N_ROWS = 4
KEPT = [0, 2, 3]
"""The rows ``n in (10, 30, 40)`` keeps."""

# Every column group of a table: source info, system tags and the data context.
EVERYTHING = {"source": True, "system_tags": True, "context": True}

# A float32 signaling NaN, as its bytes: a Python float would quiet it.
_SIGNALING_NAN = bytes.fromhex("0100807f")


def _logical(values: list[Any], python_type: Any) -> pa.Array[Any]:
    """A column of ``values`` in Orcapod's logical Arrow type for ``python_type``."""
    return (
        get_default_type_converter()
        .python_dicts_to_arrow_table(
            [{"v": v} for v in values], python_schema={"v": python_type}
        )
        .column("v")
        .combine_chunks()
    )


def _full_precision_decimals(precision: int, scale: int) -> list[decimal.Decimal]:
    """One decimal per row with exactly ``precision`` significant digits, ``scale`` of
    them after the point, the second one negative.

    Each is written as a string: arithmetic on a ``Decimal`` (``scaleb``, a product)
    rounds to the context's 28 digits before Arrow sees the value.
    """
    values: list[decimal.Decimal] = []
    for n in range(N_ROWS):
        digits = "".join(str((n + 1 + i * 7) % 9 + 1) for i in range(precision))
        sign = "-" if n == 1 else ""
        whole, fraction = digits[: precision - scale], digits[precision - scale :]
        values.append(decimal.Decimal(f"{sign}{whole}.{fraction}"))
    return values


def _probed_columns(prefix: str) -> dict[str, pa.Array[Any]]:
    """One column of every probed type, named ``<prefix><kind>``."""
    dates = pa.array([1, 86_400_001, 2, 172_800_003], pa.int64()).cast(pa.date64())
    floats32 = pa.Array.from_buffers(
        pa.float32(),
        N_ROWS,
        [None, pa.py_buffer(_SIGNALING_NAN + struct.pack("<fff", -0.0, 0.0, 2.5))],  # pyright: ignore[reportArgumentType]  # no validity buffer: no nulls
    )
    offsets = list(range(N_ROWS + 1))
    return {
        f"{prefix}date64": dates,
        f"{prefix}time_ns": pa.array([1, 1_001, 123_456_789, 999], pa.int64()).cast(
            pa.time64("ns")
        ),
        f"{prefix}f32": floats32,
        f"{prefix}f64": pa.array([-0.0, 0.0, float("nan"), 1.5], pa.float64()),
        f"{prefix}dec128": pa.array(
            _full_precision_decimals(38, 20), pa.decimal128(38, 20)
        ),
        f"{prefix}dec256": pa.array(
            _full_precision_decimals(76, 40), pa.decimal256(76, 40)
        ),
        f"{prefix}path": _logical([Path(f"/x/{n}") for n in range(N_ROWS)], Path),
        f"{prefix}uuid": _logical(
            [uuid.UUID(int=n + 1) for n in range(N_ROWS)], uuid.UUID
        ),
        f"{prefix}str": pa.array(["a", "", "ü", "d"], pa.string()),
        f"{prefix}large_str": pa.array(["a", "", "ü", "d"], pa.large_string()),
        f"{prefix}bin": pa.array([b"\x00", b"", b"\xff", b"d"], pa.binary()),
        f"{prefix}large_bin": pa.array(
            [b"\x00", b"", b"\xff", b"d"], pa.large_binary()
        ),
        f"{prefix}list": pa.LargeListArray.from_arrays(
            pa.array(offsets, pa.int64()), floats32
        ),
        f"{prefix}list_date": pa.ListArray.from_arrays(
            pa.array(offsets, pa.int32()), dates
        ),
        f"{prefix}struct": pa.StructArray.from_arrays(
            [dates, floats32], names=["a", "b"]
        ),
    }


def _table(arrangement: str) -> tuple[pa.Table, list[str]]:
    """The probed table and its tag columns, laid out in one of two arrangements.

    Sixteen tag columns (``n`` and one of every probed type) and sixteen data columns
    (``m`` and one of every probed type). ``"reversed"`` reverses the table's columns,
    puts the data before the tags and declares the tags in the opposite order.
    """
    tags = _probed_columns("t_")
    data = _probed_columns("d_")
    n = pa.array([10, 20, 30, 40], pa.int64())
    m = pa.array([1, 2, 3, 4], pa.int32())
    if arrangement == "given":
        return pa.table({"n": n, **tags, "m": m, **data}), ["n", *tags]
    columns = {
        **dict(reversed(data.items())),
        "m": m,
        **dict(reversed(tags.items())),
        "n": n,
    }
    return pa.table(columns), [*reversed(tags), "n"]


def _source(arrangement: str, role: str) -> tuple[ArrowTableSource, pa.Table]:
    """A source over the probed table; its tags plain (``"tag"``) or data-tags."""
    table, tags = _table(arrangement)
    source = ArrowTableSource(
        table,
        tag_columns=tags,
        tag_only_columns=(role == "tag"),
        source_id="probe",
    )
    return source, table


def _filters(table: pa.Table) -> dict[str, Filter]:
    """A filter that reads none of the probed columns, and one that reads all of them."""
    keep = Condition.of(col("n").is_in([10, 30, 40]))
    reads_all = keep
    for name in table.column_names:
        if name not in ("n", "m"):
            reads_all = reads_all.combined(Condition.of(col(name).is_not_null()))
    return {
        "reads_another_column": Filter(keep),
        "reads_every_column": Filter(reads_all),
    }


# ── bit-for-bit comparison ──────────────────────────────────────────────────


def _differences(actual: Values, expected: Values, where: str) -> list[str]:
    """What differs between two columns, floats and temporal values by their bits."""
    a = actual.combine_chunks() if isinstance(actual, pa.ChunkedArray) else actual
    b = expected.combine_chunks() if isinstance(expected, pa.ChunkedArray) else expected
    if a.type != b.type:
        return [f"{where}: type {a.type} is not {b.type}"]
    if len(a) != len(b):
        return [f"{where}: {len(a)} rows, expected {len(b)}"]
    if a.is_valid().to_pylist() != b.is_valid().to_pylist():
        return [f"{where}: validity differs"]
    arrow_type = a.type
    if isinstance(arrow_type, pa.ExtensionType):
        return _differences(a.storage, b.storage, f"{where}<storage>")  # pyright: ignore[reportAttributeAccessIssue]  # an extension array
    if pa.types.is_floating(arrow_type):
        bits = {2: pa.uint16(), 4: pa.uint32(), 8: pa.uint64()}[arrow_type.byte_width]
        # ``take`` copies, so the view reads this array's own values at offset 0.
        every = pa.array(range(len(a)), pa.int64())
        a_bits = a.take(every).view(bits).to_pylist()
        b_bits = b.take(every).view(bits).to_pylist()
        return [] if a_bits == b_bits else [f"{where}: bits {a_bits} are not {b_bits}"]
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        if pc.list_value_length(a).to_pylist() != pc.list_value_length(b).to_pylist():
            return [f"{where}: list lengths differ"]
        return _differences(a.flatten(), b.flatten(), f"{where}[]")  # pyright: ignore[reportAttributeAccessIssue]  # a list array
    if pa.types.is_struct(arrow_type):
        found: list[str] = []
        for index in range(arrow_type.num_fields):
            found += _differences(
                a.field(index),  # pyright: ignore[reportAttributeAccessIssue]  # a struct array
                b.field(index),  # pyright: ignore[reportAttributeAccessIssue]  # a struct array
                f"{where}.{arrow_type.field(index).name}",
            )
        return found
    if pa.types.is_temporal(arrow_type):
        counts = pa.int64() if arrow_type.bit_width == 64 else pa.int32()
        a_counts, b_counts = a.cast(counts).to_pylist(), b.cast(counts).to_pylist()
        return (
            [] if a_counts == b_counts else [f"{where}: {a_counts} are not {b_counts}"]
        )
    return [] if a.equals(b) else [f"{where}: values differ"]


def _table_differences(
    actual: pa.Table, expected: pa.Table, given: pa.Table, kept: list[int]
) -> list[str]:
    """What differs between a filter's output and the kept rows of its input.

    ``expected`` is the input stream's own table (every column group); ``given`` is the
    table the source was built from, whose user columns the output must equal too (in
    their large variants, the one change a stream makes).
    """
    found: list[str] = []
    if actual.column_names != expected.column_names:
        found.append(f"columns {actual.column_names} are not {expected.column_names}")
    if actual.schema != expected.schema:
        found.append("schemas differ")
    for name in expected.column_names:
        if name in actual.column_names:
            found += _differences(
                actual.column(name), expected.column(name).take(kept), name
            )
    for name in given.column_names:
        column = given.column(name).combine_chunks()
        large = column.cast(
            arrow_utils.normalize_to_large_types(column.type, keep_null=True)
        )
        found += _differences(actual.column(name), large.take(kept), f"given {name}")
    return found


# ── the paths ───────────────────────────────────────────────────────────────


def _row_tables(rows: list[Row]) -> tuple[pa.Table, pa.Table]:
    """Rows as two tables, as a channel carries them: the tags with their system tags,
    and the data with its source info.

    The two are kept apart: a data-tag is a column of both, and its data half is a
    value of its own, which a table holding the column once would not show.
    """
    tags = pa.concat_tables(
        [tag.as_table(columns={"system_tags": True}) for tag, _ in rows]
    )
    data = pa.concat_tables(
        [data.as_table(columns={"source": True}) for _, data in rows]
    )
    return tags, data


def _half_differences(
    actual: pa.Table, expected: pa.Table, given: pa.Table, kept: list[int], half: str
) -> list[str]:
    """What differs between one half (tags or data) of the rows a filter passes on and
    the kept rows of its input: against the input's own rows, and against the table
    the source was given (in the large variants)."""
    found: list[str] = []
    if actual.column_names != expected.column_names:
        found.append(
            f"{half} columns {actual.column_names} are not {expected.column_names}"
        )
    if actual.schema != expected.schema:
        found.append(f"{half} schemas differ")
    for name in expected.column_names:
        if name in actual.column_names:
            found += _differences(
                actual.column(name), expected.column(name).take(kept), f"{half} {name}"
            )
    for name in given.column_names:
        if name not in actual.column_names:
            continue
        column = given.column(name).combine_chunks()
        large = column.cast(
            arrow_utils.normalize_to_large_types(column.type, keep_null=True)
        )
        found += _differences(
            actual.column(name), large.take(kept), f"{half} given {name}"
        )
    return found


def _row_differences(
    rows: list[Row], input_rows: list[Row], given: pa.Table, kept: list[int]
) -> list[str]:
    """What differs between the rows a filter passes on and the kept rows of its input,
    the tag half and the data half each on its own (both halves of a data-tag)."""
    if len(rows) != len(kept):
        return [f"{len(rows)} rows, expected {len(kept)}"]
    tags, data = _row_tables(rows)
    input_tags, input_data = _row_tables(input_rows)
    return _half_differences(tags, input_tags, given, kept, "tag") + _half_differences(
        data, input_data, given, kept, "data"
    )


async def _through_channels(op: Filter, source: StreamProtocol) -> list[Row]:
    upstream: Channel[Row] = Channel(buffer_size=64)
    output: Channel[Row] = Channel(buffer_size=64)
    for row in source.iter_data():
        await upstream.writer.send(row)
    await upstream.writer.close()
    await op.async_execute(
        [upstream.reader],
        output.writer,
        input_pipeline_hashes=[source.pipeline_hash()],
    )
    return await output.reader.collect()


ARRANGEMENTS = ("given", "reversed")
ROLES = ("tag", "data_tag")
FILTERS = ("reads_another_column", "reads_every_column")


def _sync_differences(arrangement: str, role: str, which: str) -> list[str]:
    """What the sync path changes: its table, and the rows it hands out one by one."""
    source, given = _source(arrangement, role)
    stream = _filters(given)[which](source)
    out = stream.as_table(columns=EVERYTHING)
    expected = source.as_table(columns=EVERYTHING)
    return _table_differences(out, expected, given, KEPT) + _row_differences(
        list(stream.iter_data()), list(source.iter_data()), given, KEPT
    )


def _channel_differences(arrangement: str, role: str, which: str) -> list[str]:
    """What the row channel path (``async_execute``) changes, in either half of a row."""
    source, given = _source(arrangement, role)
    rows = asyncio.run(_through_channels(_filters(given)[which], source))
    return _row_differences(rows, list(source.iter_data()), given, KEPT)


def _job_differences(
    arrangement: str, role: str, which: str, orchestrator: type | None
) -> list[str]:
    """What a job run changes: the node's table, and the rows it hands out."""
    source, given = _source(arrangement, role)
    job = PipelineJob(name="exact", database=InMemoryArrowDatabase())
    with job:
        _ = _filters(given)[which](source, label="kept")
    _ = job.run(orchestrator=orchestrator() if orchestrator else None)
    node = job.nodes["kept"]
    out = node.as_table(columns=EVERYTHING)
    expected = source.as_table(columns=EVERYTHING)
    # A job node lays its columns out in row order (tags, system tags, data, source
    # info), not the source table's: the same columns, compared by name.
    if sorted(out.column_names) != sorted(expected.column_names):
        return [f"columns {out.column_names} are not {expected.column_names}"]
    out = out.select(expected.column_names)
    return _table_differences(out, expected, given, KEPT) + _row_differences(
        list(node.iter_data()), list(source.iter_data()), given, KEPT
    )


PATHS: dict[str, Callable[[str, str, str], list[str]]] = {
    "sync": _sync_differences,
    "channel": _channel_differences,
    "job_sync": lambda a, r, w: _job_differences(a, r, w, None),
    "job_async": lambda a, r, w: _job_differences(a, r, w, AsyncPipelineOrchestrator),
}


@pytest.mark.parametrize("which", FILTERS)
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("arrangement", ARRANGEMENTS)
@pytest.mark.parametrize("path", list(PATHS))
def test_every_path_keeps_every_value(
    path: str, arrangement: str, role: str, which: str
) -> None:
    """On every path the rows are the input's rows: the table, and each half (tags,
    data) of the rows handed out one by one, hold the values of the table the source
    was given."""
    assert PATHS[path](arrangement, role, which) == []


# ── the comparison catches what it is for ───────────────────────────────────


def _through_polars(table: pa.Table, name: str) -> pa.Table:
    """``table`` with the column ``name`` carried through Polars and cast back, as
    ``PolarsFilter`` carries every column: a ``date64`` off midnight moves to midnight."""
    field = table.schema.field(name)
    converted = pl.from_arrow(table.select([name])).to_arrow().column(name)  # pyright: ignore[reportAttributeAccessIssue]  # a frame: a table was given
    return table.set_column(
        table.column_names.index(name), field, converted.cast(field.type)
    )


class _DataHalfThroughPolars(WithoutMetaWriter):
    """The channel writer of ``Filter``, changed to carry the data half of the data-tag
    ``t_date64`` through Polars; the tag half is passed on as it is."""

    @override
    async def send(self, item: Row) -> None:
        tag, data = item
        table = data.as_table(columns={"source": True})
        if "t_date64" in table.column_names:
            data = Data(_through_polars(table, "t_date64"))
        await super().send((tag, data))


@pytest.mark.parametrize("which", FILTERS)
@pytest.mark.parametrize("arrangement", ARRANGEMENTS)
def test_a_change_of_only_the_data_half_of_a_data_tag_is_caught(
    monkeypatch: pytest.MonkeyPatch, arrangement: str, which: str
) -> None:
    """A filter whose channel path changed only the data half of a data-tag (the tag
    half intact) fails the channel comparison (review of PR 625, P2)."""
    monkeypatch.setattr(filter_module, "WithoutMetaWriter", _DataHalfThroughPolars)
    found = _channel_differences(arrangement, "data_tag", which)
    assert found
    assert all(line.startswith("data ") and "t_date64" in line for line in found)


@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("arrangement", ARRANGEMENTS)
@pytest.mark.parametrize("path", list(PATHS))
def test_a_column_carried_through_polars_is_caught_on_every_path(
    monkeypatch: pytest.MonkeyPatch, path: str, arrangement: str, role: str
) -> None:
    """A filter that carried one column through Polars, on its table path and on its
    channel path, fails the comparison of every path."""

    def table_through_polars(table: pa.Table, condition: Condition) -> pa.Table:
        return _through_polars(filter_table(table, condition), "t_date64")

    class TagHalfThroughPolars(WithoutMetaWriter):
        @override
        async def send(self, item: Row) -> None:
            tag, data = item
            changed = _through_polars(
                tag.as_table(columns={"system_tags": True}), "t_date64"
            )
            await super().send((Tag(changed), data))

    monkeypatch.setattr(filter_module, "filter_table", table_through_polars)
    monkeypatch.setattr(filter_module, "WithoutMetaWriter", TagHalfThroughPolars)
    found = PATHS[path](arrangement, role, "reads_another_column")
    assert any("t_date64" in line for line in found)


def test_the_decimal_fixtures_fill_their_precision() -> None:
    """Every decimal of the fixture has as many significant digits as its type
    declares (38 and 76), and as many after the point as its scale (20 and 40)."""
    columns = _probed_columns("")
    for name, precision, scale in (("dec128", 38, 20), ("dec256", 76, 40)):
        values = cast("list[decimal.Decimal]", columns[name].to_pylist())
        assert len(set(values)) == N_ROWS
        assert any(value < 0 for value in values)
        for value in values:
            _, digits, exponent = value.as_tuple()
            assert len(digits) == precision and digits[0] != 0 and digits[-1] != 0
            assert exponent == -scale


@pytest.mark.parametrize(
    ("condition", "kept"),
    [
        (col("d_f64") == 0.0, [0, 1]),
        (col("d_f32") == 0.0, [1, 2]),
        (col("t_f64") >= 0.0, [0, 1, 2, 3]),
        (col("t_f32").is_in([0.0, 2.5]), [1, 2, 3]),
        (col("d_date64") >= dt.date(1970, 1, 2), [1, 3]),
        (col("t_large_str") != "a", [1, 2, 3]),
        (col("d_struct").field("a").is_not_null(), [0, 1, 2, 3]),
    ],
    ids=str,
)
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("arrangement", ARRANGEMENTS)
def test_a_condition_on_a_value_keeps_every_value(
    arrangement: str, role: str, condition: Condition, kept: list[int]
) -> None:
    """A condition that compares a probed column's values (a ``-0.0``, a signaling NaN,
    a ``date64`` off midnight) changes none of them, on the sync and the channel path."""
    source, given = _source(arrangement, role)
    stream = Filter(condition)(source)
    out = stream.as_table(columns=EVERYTHING)
    expected = source.as_table(columns=EVERYTHING)
    assert _table_differences(out, expected, given, kept) == []
    input_rows = list(source.iter_data())
    assert _row_differences(list(stream.iter_data()), input_rows, given, kept) == []
    through = asyncio.run(_through_channels(Filter(condition), source))
    assert _row_differences(through, input_rows, given, kept) == []
