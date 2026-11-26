"""A SpiralDB ``where`` refuses a float equality to ``-0.0`` or NaN (ITL-998, ITL-1480).

Orcapod counts ``-0.0`` equal to ``0.0`` and NaN equal to NaN, so ``Condition.of`` and
``combined`` keep one of two such values. SpiralDB matches a float by its bits (the
live probe, November 26, 2026): ``-0.0`` reads only ``-0.0`` and a NaN only its own bit
pattern. Which rows a source read would then depend on which equal value was kept
(independent review of d4115197): ``where(col("id") >= 0, {"v": -0.0}, v=0.0)`` read
one row and the signs reversed another, under identical conditions, canonical forms
and saved JSON. Such a value is refused, pointing to ``filter``. Other float equality
values must fit the column exactly. Runs on the fake only.
"""

from __future__ import annotations

import asyncio
import math
import struct
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.logical_types.canonical_form import (
    EXTENSION_METADATA_KEY,
    EXTENSION_NAME_KEY,
)
from orcapod.sources import SpiralDBTableSource
from orcapod.types import Condition, col
from tests.test_databases import fake_spiral_sdk

_PROJECT = "workshop-sample"
_NEGATIVE_NAN = struct.unpack("<d", struct.pack("<Q", 0xFFF8000000000000))[0]
_KEYS = pa.schema([pa.field("id", pa.int64()), pa.field("lab", pa.large_string())])


def _table() -> pa.Table:
    """Return rows with both zeros, NaNs, infinities, and ordinary values."""
    return pa.table(
        {
            "id": pa.array([0, 1, 2, 3, 4, 5], pa.int64()),
            "lab": pa.array(["a", "a", "b", "b", "c", "c"], pa.large_string()),
            "v": pa.array(
                [-0.0, 0.0, float(2**53 + 2), 0.1, 1.5, math.inf], pa.float64()
            ),
            "g": pa.array(
                [-0.0, 0.0, float(2**100), 0.5, 1.5, -math.inf], pa.float32()
            ),
            "h": pa.array([-0.0, 0.0, math.nan, 0.5, 1.5, 2.0], pa.float16()),
            "n": pa.array([10, 11, 12, 13, 14, 15], pa.int32()),
        }
    )


@pytest.fixture()
def project(tmp_path: Path):
    """Provide a fake project holding the ``zeros`` table."""
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        project = fake_spiral_sdk.ExternalProject(_PROJECT)
        project.create_table("default.zeros", key_schema=_KEYS).write(_table())
        fake_spiral_sdk.scan_log.clear()
        yield project
        fake_spiral_sdk.scan_log.clear()


def _source(**kwargs: Any) -> SpiralDBTableSource:
    """Return a source over ``zeros``, tags kept tag-only."""
    return SpiralDBTableSource(_PROJECT, "zeros", tag_only_columns=True, **kwargs)


def _ids(source: SpiralDBTableSource) -> list[Any]:
    """Return the ``id`` of every row ``source`` reads (sync)."""
    return source.as_table()["id"].to_pylist()


def _async_ids(source: SpiralDBTableSource) -> list[Any]:
    """Return the ``id`` of every row ``source`` reads (async)."""

    async def collect() -> list[Any]:
        return [tag.as_dict()["id"] async for tag, _ in source.async_iter_data()]

    return asyncio.run(asyncio.wait_for(collect(), timeout=60))


def _forms(
    refused: float, other: float, column: str
) -> dict[str, Callable[[], SpiralDBTableSource]]:
    """Return every way of giving ``column == refused``, alone or beside ``other``."""
    rng = col("id") >= 0
    return {
        "keyword": lambda: _source().where(**{column: refused}),
        "mapping": lambda: _source().where({column: refused}),
        "col": lambda: _source().where(col(column) == refused),
        "Condition.of": lambda: _source().where(Condition.of({column: refused})),
        "range, mapping refused, keyword other": lambda: _source().where(
            rng, {column: refused}, **{column: other}
        ),
        "range, mapping other, keyword refused": lambda: _source().where(
            rng, {column: other}, **{column: refused}
        ),
        "keyword refused, range": lambda: _source().where(
            {column: refused}, rng, **{column: other}
        ),
        "two mappings, refused first": lambda: _source().where(
            {column: refused}, {column: other}
        ),
        "two mappings, refused last": lambda: _source().where(
            {column: other}, {column: refused}
        ),
        "col other, mapping refused": lambda: _source().where(
            col(column) == other, {column: refused}
        ),
        "mapping refused, col other": lambda: _source().where(
            {column: refused}, col(column) == other
        ),
        "with another column": lambda: _source().where(n=11, **{column: refused}),
        "chained, refused first": lambda: (
            _source().where(**{column: refused}).where(**{column: other})
        ),
        "chained, refused last": lambda: (
            _source().where(**{column: other}).where(**{column: refused})
        ),
        "where= mapping": lambda: _source(where={column: refused, "n": 11}),
        "where= condition": lambda: _source(where=Condition.of(rng, {column: refused})),
    }


_CASES = [
    (column, refused, other, form)
    for column in ("v", "g")
    for refused, other in ((-0.0, 0.0), (math.nan, math.nan), (_NEGATIVE_NAN, math.nan))
    for form in _forms(refused, other, column)
]


class TestRefused:
    @pytest.mark.parametrize(
        ("column", "refused", "other", "form"),
        _CASES,
        ids=[
            f"{c}-{'-0.0' if r == 0 else ('-nan' if math.copysign(1, r) < 0 else 'nan')}"
            f"-{f}"
            for c, r, _, f in _CASES
        ],
    )
    def test_an_equality_to_negative_zero_or_nan_is_refused(
        self, project, column, refused, other, form
    ):
        """Every form, both orders, keywords and mappings: refused before any scan."""
        with pytest.raises(TypeError, match=r"ITL-1480.*filter") as raised:
            _forms(refused, other, column)[form]()

        assert repr(column) in str(raised.value)
        assert fake_spiral_sdk.scan_log == []

    @pytest.mark.parametrize("signs", [(-0.0, 0.0), (0.0, -0.0)], ids=["-+", "+-"])
    def test_the_reviewed_case_no_longer_depends_on_the_sign_kept(self, project, signs):
        """The finding: both orders used to read different rows under one condition."""
        first, second = signs
        with pytest.raises(TypeError, match="ITL-1480"):
            _source().where(col("id") >= 0, {"v": first}, v=second)


class TestEveryOtherFloatEqualityAsBefore:
    @pytest.mark.parametrize(
        ("where", "expected"),
        [
            ({"v": 0.0}, [1]),
            ({"v": 1.5}, [4]),
            ({"v": math.inf}, [5]),
            ({"g": 0.0}, [1]),
            ({"g": 1.5}, [4]),
            ({"g": -math.inf}, [5]),
            ({"v": 0.0, "n": 11}, [1]),
            ({"n": 14, "v": 1.5}, [4]),
        ],
    )
    def test_pushed_down_with_the_services_equality(self, project, where, expected):
        """``0.0`` reads only ``+0.0`` (the service's equality, documented); sync and
        async read the same rows, keywords and a mapping alike."""
        for source in (_source().where(where), _source().where(**where)):
            assert _ids(source) == expected
            assert _async_ids(source) == expected
            pushed = dict(fake_spiral_sdk.scan_log[-1].kwargs["where"].equalities)
            assert set(pushed) == set(where)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("h", 0.1),
        ("h", 2.0**-25),
        ("h", 2049),
        ("g", 0.1),
        ("g", -0.1),
        ("g", 2.0**-150),
        ("g", 1e300),
        ("g", 2**24 + 1),
        ("v", 2**53 + 1),
    ],
)
def test_inexact_float_column_equality_is_refused(
    project: fake_spiral_sdk.ExternalProject, column: str, value: int | float
) -> None:
    """Require exact values before scanning, regardless of equality order or form."""
    source = _source()
    terms = [(column, value), ("n", 13)]
    message = rf"column {column!r} holds .* cannot hold .* exactly"
    for equalities in (dict(terms), dict(reversed(terms))):
        with pytest.raises(TypeError, match=message):
            source.where(**equalities)
        with pytest.raises(TypeError, match=message):
            _source(where=equalities)
    with pytest.raises(TypeError, match=message):
        source.where(col(column) == value)
    assert fake_spiral_sdk.scan_log == []


@pytest.mark.parametrize(
    ("column", "value", "row"),
    [
        ("h", 0.5, 3),
        ("h", 2, 5),
        ("g", 0.5, 3),
        ("g", 2**100, 2),
        ("v", 0.1, 3),
        ("v", 2**53 + 2, 2),
    ],
)
def test_exact_float_column_equalities_select_rows(
    project: fake_spiral_sdk.ExternalProject, column: str, value: int | float, row: int
) -> None:
    """An exact Python float or int selects the matching row at each width."""
    source = _source()
    terms = [(column, value), ("n", 10 + row)]
    for equalities in (dict(terms), dict(reversed(terms))):
        restricted = source.where(equalities)
        assert _ids(restricted) == [row]
        assert _async_ids(restricted) == [row]


@pytest.fixture()
def logical_float_source(
    project: fake_spiral_sdk.ExternalProject,
) -> SpiralDBTableSource:
    """Read a logical column whose float storage carries its extension identity."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "v",
                pa.float32(),
                metadata={
                    EXTENSION_NAME_KEY: b"test.measurement",
                    EXTENSION_METADATA_KEY: b"v1",
                },
            ),
        ]
    )
    table = project.create_table("default.logical", key_schema=[("id", pa.int64())])
    table.write(pa.table({"id": [0, 1], "v": [0.1, 1.0]}, schema=schema))
    table.record(schema)
    return SpiralDBTableSource(_PROJECT, "logical", tag_only_columns=True)


def test_logical_float_equality_refuses_integer(
    logical_float_source: SpiralDBTableSource,
):
    """The logical column retains its existing Python float requirement."""
    with pytest.raises(TypeError, match=r"column 'v'.*value 1 is a int"):
        logical_float_source.where(v=1)
    assert fake_spiral_sdk.scan_log == []


def test_logical_float_equality_accepts_exact_float(
    logical_float_source: SpiralDBTableSource,
):
    """An exact Python float can still select a row by its stored value."""
    assert _ids(logical_float_source.where(v=1.0)) == [1]


def test_logical_float_equality_refuses_inexact_float(
    logical_float_source: SpiralDBTableSource,
):
    """A logical float column also requires exact representation."""
    with pytest.raises(TypeError, match=r"column 'v'.*cannot hold 0\.1 exactly"):
        logical_float_source.where(v=0.1)
    assert fake_spiral_sdk.scan_log == []


def test_huge_integer_equality_is_refused(project: fake_spiral_sdk.ExternalProject):
    """A value outside the float range reports a column error even with a digit limit."""
    source = _source()
    original_limit = sys.get_int_max_str_digits()
    try:
        sys.set_int_max_str_digits(4300)
        with pytest.raises(TypeError, match=r"column 'v'.*cannot hold .* exactly"):
            source.where(v=10**5000)
    finally:
        sys.set_int_max_str_digits(original_limit)
    assert fake_spiral_sdk.scan_log == []


@pytest.mark.parametrize(
    ("arrow_type", "bit_type", "bits"),
    [
        (pa.float32(), pa.uint32(), [0x7FC00000, 0x7FC00001, 0xFFC00000, 0x7F800001]),
        (
            pa.float64(),
            pa.uint64(),
            [
                0x7FF8000000000000,
                0x7FF8000000000001,
                0xFFF8000000000000,
                0x7FF0000000000001,
            ],
        ),
    ],
    ids=["float32", "float64"],
)
def test_fake_nan_equality_requires_matching_bits(
    project: fake_spiral_sdk.ExternalProject,
    arrow_type: pa.DataType,
    bit_type: pa.DataType,
    bits: list[int],
) -> None:
    """The fake's scan matches a NaN only when both sign and payload match."""
    values = pa.array([*bits, None, 0], bit_type).view(arrow_type)
    project.create_table("default.nans", key_schema=[("id", pa.int64())]).write(
        pa.table({"id": list(range(len(values))), "v": values})
    )
    table = fake_spiral_sdk.FakeClient().keyed_table(_PROJECT, "default", "nans")
    for row, value in enumerate(values.slice(0, 3).cast(pa.float64()).to_pylist()):
        predicate = fake_spiral_sdk.eq(
            fake_spiral_sdk.column("v"), fake_spiral_sdk.literal(value, arrow_type)
        )
        assert table.scan(filter=predicate).to_table()["id"].to_pylist() == [row]
