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
either orchestrator.

A dictionary-encoded column is not here: no stream holds one (ITL-1264).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import decimal
import struct
import uuid
from pathlib import Path
from typing import Any, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod import PipelineJob, col
from orcapod.channels import Channel
from orcapod.contexts import get_default_type_converter
from orcapod.core.operators import Filter
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
            [decimal.Decimal(f"{n}{n + 1:037d}").scaleb(-20) for n in range(N_ROWS)],
            pa.decimal128(38, 20),
        ),
        f"{prefix}dec256": pa.array(
            [decimal.Decimal(f"{n}.{n + 1:040d}") for n in range(N_ROWS)],
            pa.decimal256(76, 40),
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


def _rows_table(rows: list[Row]) -> pa.Table:
    """Rows of the channel path as one table: tags with system tags, then data with
    source info (a data-tag once, from the tag)."""
    tables: list[pa.Table] = []
    for tag, data in rows:
        tag_table = tag.as_table(columns={"system_tags": True})
        data_table = data.as_table(columns={"source": True})
        data_table = data_table.drop_columns(
            [n for n in data_table.column_names if n in tag_table.column_names]
        )
        tables.append(
            pa.Table.from_arrays(
                [*tag_table.columns, *data_table.columns],
                schema=pa.schema([*tag_table.schema, *data_table.schema]),
            )
        )
    return pa.concat_tables(tables)


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


@pytest.mark.parametrize("which", FILTERS)
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("arrangement", ARRANGEMENTS)
def test_sync_path_keeps_every_value(arrangement: str, role: str, which: str) -> None:
    source, given = _source(arrangement, role)
    out = _filters(given)[which](source).as_table(columns=EVERYTHING)
    assert out.num_rows == len(KEPT)
    expected = source.as_table(columns=EVERYTHING)
    assert _table_differences(out, expected, given, KEPT) == []


@pytest.mark.parametrize("which", FILTERS)
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("arrangement", ARRANGEMENTS)
def test_channel_path_keeps_every_value(
    arrangement: str, role: str, which: str
) -> None:
    """The rows ``async_execute`` passes on are the input's rows, in the form the
    channel carries them, and hold the values of the table the source was given."""
    source, given = _source(arrangement, role)
    rows = asyncio.run(_through_channels(_filters(given)[which], source))
    assert len(rows) == len(KEPT)
    out = _rows_table(rows)
    expected = _rows_table(list(source.iter_data()))
    assert _table_differences(out, expected, given, KEPT) == []


@pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
@pytest.mark.parametrize("which", FILTERS)
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("arrangement", ARRANGEMENTS)
def test_job_keeps_every_value(
    arrangement: str, role: str, which: str, orchestrator: type | None
) -> None:
    source, given = _source(arrangement, role)
    job = PipelineJob(name="exact", database=InMemoryArrowDatabase())
    with job:
        _ = _filters(given)[which](source, label="kept")
    _ = job.run(orchestrator=orchestrator() if orchestrator else None)
    out = job.nodes["kept"].as_table(columns=EVERYTHING)
    assert out.num_rows == len(KEPT)
    expected = source.as_table(columns=EVERYTHING)
    # A job node lays its columns out in row order (tags, system tags, data, source
    # info), not the source table's: the same columns, compared by name.
    assert sorted(out.column_names) == sorted(expected.column_names)
    out = out.select(expected.column_names)
    assert _table_differences(out, expected, given, KEPT) == []


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
@pytest.mark.parametrize("arrangement", ARRANGEMENTS)
def test_a_condition_on_a_value_keeps_every_value(
    arrangement: str, condition: Condition, kept: list[int]
) -> None:
    """A condition that compares a probed column's values (a ``-0.0``, a signaling NaN,
    a ``date64`` off midnight) changes none of them."""
    source, given = _source(arrangement, "tag")
    out = Filter(condition)(source).as_table(columns=EVERYTHING)
    expected = source.as_table(columns=EVERYTHING)
    assert _table_differences(out, expected, given, kept) == []
