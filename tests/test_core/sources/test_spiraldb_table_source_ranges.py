"""``SpiralDBTableSource.where()`` takes ranges of a column (ITL-998).

A range is a restriction like an equality (ITL-1218): SpiralDB applies it in the scan, and
it changes no identity, schema, system tag or record id. Every expected row set here is
what pyarrow selects from the same table. Runs against the disk-backed pyspiral fake.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import functools
import itertools
import json
import operator
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import InconsistentSourceError, SourceTooLargeError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import SpiralDBTableSource
from orcapod.types import Condition, col
from tests.test_databases import fake_spiral_sdk
from tests.test_docs._doc_samples import extract_blocks

_PROJECT = "workshop-sample"
_UTC = dt.timezone.utc
_T0 = dt.datetime(2026, 11, 1, tzinfo=_UTC)
_WALL = dt.datetime(2026, 11, 1, 6, 30)
_DAY = dt.date(2026, 11, 1)
_MS_PER_DAY = 86_400_000
_Key = tuple[Any, Any]
"""A row's ``(unit, at)`` key."""


def _table() -> pa.Table:
    """Return the ``ranges`` rows: two key columns and data of every range type.

    ``v`` and ``d`` hold nulls; ``w`` holds nanoseconds between two microseconds;
    ``d64`` holds values that are not midnight. The other columns are of the types a
    range refuses.
    """
    n = 12
    w0 = int(dt.datetime(2026, 11, 1, 6, 30, tzinfo=_UTC).timestamp()) * 10**9
    return pa.table(
        {
            "unit": pa.array([1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4], pa.int32()),
            "at": pa.array(
                [_T0 + dt.timedelta(minutes=90 * i) for i in range(n)],
                pa.timestamp("us", tz="UTC"),
            ),
            "v": pa.array(
                [5, None, -3, 7, 7, 0, None, 12, -8, 3, 100, -100], pa.int64()
            ),
            "n": pa.array([0, 255, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], pa.uint8()),
            "d": pa.array(
                [
                    _DAY + dt.timedelta(days=i % 5 - 2) if i != 4 else None
                    for i in range(n)
                ],
                pa.date32(),
            ),
            "w": pa.array(
                [
                    w0 + delta
                    for delta in (
                        0,
                        1,
                        999,
                        1000,
                        1001,
                        -1,
                        -1000,
                        0,
                        5,
                        2000,
                        10**9,
                        -(10**9),
                    )
                ],
                pa.int64(),
            ).view(pa.timestamp("ns")),
            "d64": pa.array(
                [
                    (i % 4) * _MS_PER_DAY + (i % 3) * 3_600_000 + 20_000 * _MS_PER_DAY
                    for i in range(n)
                ],
                pa.int64(),
            ).view(pa.date64()),
            "sec": pa.array(
                [int(_T0.timestamp()) + 60 * i for i in range(n)], pa.int64()
            ).view(pa.timestamp("s")),
            "x": pa.array([float(i) - 5.5 for i in range(n)], pa.float64()),
            "g": pa.array([float(i) for i in range(n)], pa.float32()),
            "h": pa.array(np.arange(n, dtype=np.float16), pa.float16()),
            "s": [f"s{i:02d}" for i in range(n)],
            "b": [bytes([i]) for i in range(n)],
            "f": [i % 2 == 0 for i in range(n)],
            "dec": pa.array([Decimal(i) for i in range(n)], pa.decimal128(5, 0)),
        }
    )


_KEYS = pa.schema(
    [pa.field("unit", pa.int32()), pa.field("at", pa.timestamp("us", tz="UTC"))]
)


@pytest.fixture()
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Provide a fake project holding the 12-row ``ranges`` table, 4 rows per batch."""
    monkeypatch.setattr(fake_spiral_sdk, "scan_batch_rows", 4)
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        project = fake_spiral_sdk.ExternalProject(_PROJECT)
        project.create_table("default.ranges", key_schema=_KEYS).write(_table())
        fake_spiral_sdk.scan_log.clear()
        yield project
        fake_spiral_sdk.scan_log.clear()


def _source(**kwargs: Any) -> SpiralDBTableSource:
    """Return a source over ``ranges``, tags kept tag-only unless ``kwargs`` says."""
    kwargs.setdefault("tag_only_columns", True)
    return SpiralDBTableSource(_PROJECT, "ranges", **kwargs)


def _keys(source: SpiralDBTableSource) -> list[_Key]:
    """Return the ``(unit, at)`` key of every row ``source`` reads, in order."""
    table = source.as_table()
    return list(zip(table["unit"].to_pylist(), table["at"].to_pylist()))


def _expected(*masks: Any) -> list[_Key]:
    """Return the keys of the rows where every pyarrow mask holds (a null holds not)."""
    table = _table()
    keep = functools.reduce(pc.and_, [pc.fill_null(m, False) for m in masks])
    kept = table.filter(keep)
    return list(zip(kept["unit"].to_pylist(), kept["at"].to_pylist()))


def _scalar(column: str, value: Any) -> pa.Scalar[Any]:
    """Return ``value`` as a pyarrow scalar of ``column``'s type."""
    return pa.scalar(value, _table().schema.field(column).type)


_OPERATORS: dict[str, tuple[Callable[[Any, Any], Any], Any]] = {
    "<": (operator.lt, pc.less),
    "<=": (operator.le, pc.less_equal),
    ">": (operator.gt, pc.greater),
    ">=": (operator.ge, pc.greater_equal),
}

# Bounds per range column: a stored value or one between, at and beyond both ends.
_BOUNDS: dict[str, list[Any]] = {
    "unit": [0, 1, 2, 4, 5],
    "at": [
        _T0,
        _T0 + dt.timedelta(minutes=100),
        _T0 + dt.timedelta(hours=16, minutes=30),
    ],
    "v": [-100, -3, 0, 7, 100],
    "n": [0, 5, 255],
    "d": [_DAY - dt.timedelta(days=2), _DAY, _DAY + dt.timedelta(days=2)],
    "w": [
        _WALL - dt.timedelta(microseconds=1),
        _WALL,
        _WALL + dt.timedelta(microseconds=1),
    ],
    "d64": [
        dt.date(1970, 1, 1) + dt.timedelta(days=20_000),
        dt.date(1970, 1, 1) + dt.timedelta(days=20_001),
        dt.date(1970, 1, 1) + dt.timedelta(days=20_003),
    ],
    "sec": [_T0, _T0 + dt.timedelta(minutes=5)],
}


def _pyarrow_bound(column: str, value: Any) -> pa.Scalar[Any]:
    """Return a bound as the column's scalar (``w`` and ``sec`` are naive in UTC)."""
    if column == "sec" and isinstance(value, dt.datetime):
        return _scalar(column, value.replace(tzinfo=None))
    return _scalar(column, value)


def _bound(column: str, value: Any) -> Any:
    """Return a bound as ``where`` takes it (``sec`` is naive)."""
    if column == "sec":
        return value.replace(tzinfo=None)
    return value


_CASES = [
    (column, op, value)
    for column, values in _BOUNDS.items()
    for op in _OPERATORS
    for value in values
]


class TestRows:
    """Each operator and ``between``, on every range type, reads pyarrow's rows."""

    @pytest.mark.parametrize(("column", "op", "value"), _CASES)
    def test_each_operator(self, project, column, op, value):
        python_op, arrow_op = _OPERATORS[op]
        condition = python_op(col(column), _bound(column, value))
        expected = _expected(arrow_op(_table()[column], _pyarrow_bound(column, value)))

        assert _keys(_source().where(condition)) == expected

    @pytest.mark.parametrize("column", list(_BOUNDS))
    def test_between_includes_both_ends(self, project, column):
        values = _BOUNDS[column]
        for low, high in itertools.combinations_with_replacement(values, 2):
            expected = _expected(
                pc.greater_equal(_table()[column], _pyarrow_bound(column, low)),
                pc.less_equal(_table()[column], _pyarrow_bound(column, high)),
            )
            restricted = _source().where(
                col(column).between(_bound(column, low), _bound(column, high))
            )
            assert _keys(restricted) == expected

    def test_a_half_open_window_and_its_neighbour_share_no_row(self, project):
        """``>= a`` and ``< b``: adjacent windows neither overlap nor leave a gap."""
        cut = _T0 + dt.timedelta(hours=6)
        before = _keys(_source().where(col("at") < cut))
        after = _keys(_source().where(col("at") >= cut))

        assert not set(before) & set(after)
        assert sorted(before + after) == _keys(_source())

    def test_documented_windows_over_integer_microseconds(
        self, project: fake_spiral_sdk.ExternalProject
    ) -> None:
        """The documented windows split the count column at the hour boundary."""
        page = Path(__file__).parents[3] / "docs" / "concepts" / "sources.md"
        (sample,) = (
            block.source
            for block in extract_blocks(page.read_text(encoding="utf-8"))
            if "first_hour = events.where(" in block.source
        )
        hour = 3_600_000_000
        project.create_table(
            "default.events", key_schema=[("timestamp", pa.uint64())]
        ).write(
            pa.table(
                {
                    "timestamp": pa.array([0, hour - 1, hour, 2 * hour], pa.uint64()),
                    "unit_id": [106, 107, 106, 106],
                }
            )
        )
        scope: dict[str, Any] = {
            "events": SpiralDBTableSource(_PROJECT, "events", tag_only_columns=True)
        }
        exec(sample, scope)

        assert scope["first_hour"].as_table()["timestamp"].to_pylist() == [0, hour - 1]
        assert scope["second_hour"].as_table()["timestamp"].to_pylist() == [hour]
        assert scope["one_unit"].as_table()["timestamp"].to_pylist() == [0]

    def test_both_ends_of_an_integer_type(self, project):
        assert _keys(_source().where(col("n") >= 0, col("n") <= 255)) == _keys(
            _source()
        )
        assert _keys(_source().where(col("n") > 255)) == []
        assert _keys(_source().where(col("v") >= -(2**63), col("v") < 2**63 - 1)) == (
            _expected(pc.is_valid(_table()["v"]))
        )

    def test_a_null_meets_no_comparison(self, project):
        """A null ``v`` is in no range, whatever the operator."""
        nulls = {
            key
            for key, v in zip(_keys(_source()), _table()["v"].to_pylist())
            if v is None
        }
        for op in _OPERATORS.values():
            assert not nulls & set(_keys(_source().where(op[0](col("v"), 0))))

    @pytest.mark.parametrize(
        "build",
        [
            lambda: (col("unit") >= 3, col("unit") < 2),
            lambda: (col("unit") >= 3, col("unit") < 3),
            lambda: (col("v") > 100,),
            lambda: (Condition.of(unit=1), col("unit") >= 3),
        ],
        ids=["reversed", "start equals stop", "beyond", "equality outside"],
    )
    def test_a_window_that_holds_no_row_reads_none(self, project, build):
        """Kept, not refused (D3): zero rows, with the full schema."""
        restricted = _source().where(*build())

        table = restricted.as_table(all_info=True)
        assert table.num_rows == 0
        assert table.schema == _source().as_table(all_info=True).schema

    def test_the_bounds_reach_the_scan_with_the_column_type(self, project):
        """SpiralDB compares only a literal of the column's own type."""
        _source().where(
            col("n") >= 2, col("at") < _T0 + dt.timedelta(hours=9)
        ).as_table()

        terms = fake_spiral_sdk.scan_log[-1].kwargs["where"].terms
        assert {(name, op, lit.type) for name, lit, op in terms} == {
            ("n", ">=", pa.uint8()),
            ("at", "<", pa.timestamp("us", tz="UTC")),
        }


def _parts() -> list[Condition]:
    """Four parts of one restriction: ranges of two columns, a between, an equality."""
    return [
        col("unit") >= 2,
        col("at") < _T0 + dt.timedelta(hours=14),
        col("v").between(-5, 50),
        Condition.of(f=False),
    ]


def _expected_parts() -> list[_Key]:
    """Return the rows ``_parts()`` selects, as pyarrow selects them."""
    return _expected(
        pc.greater_equal(_table()["unit"], _scalar("unit", 2)),
        pc.less(_table()["at"], _scalar("at", _T0 + dt.timedelta(hours=14))),
        pc.greater_equal(_table()["v"], _scalar("v", -5)),
        pc.less_equal(_table()["v"], _scalar("v", 50)),
        pc.equal(_table()["f"], pa.scalar(False)),
    )


_FORMS: dict[str, Callable[[list[Condition]], SpiralDBTableSource]] = {
    "arguments": lambda parts: _source().where(*parts),
    "&": lambda parts: _source().where(functools.reduce(operator.and_, parts)),
    "combined": lambda parts: _source().where(
        functools.reduce(lambda a, b: a.combined(b), parts)
    ),
    "chained": lambda parts: functools.reduce(
        lambda source, part: source.where(part), parts, _source()
    ),
    "constructor then where": lambda parts: _source(where=parts[0]).where(*parts[1:]),
}


class TestSeveralItemsInEveryOrder:
    """Every order of the parts, in every form, is one restriction (D7, D8)."""

    @pytest.mark.parametrize("form", list(_FORMS))
    def test_every_order_reads_the_same_rows_and_saves_the_same_bytes(
        self, project, form
    ):
        reference = _source().where(*_parts())
        expected_rows = _expected_parts()
        saved = json.dumps(reference.to_config()["where"])
        for order in itertools.permutations(range(4)):
            parts = [_parts()[i] for i in order]
            restricted = _FORMS[form](parts)

            assert _keys(restricted) == expected_rows, order
            assert restricted.condition == reference.condition
            assert hash(restricted.condition) == hash(reference.condition)
            assert (
                restricted.condition.canonical_form()
                == reference.condition.canonical_form()
            )
            assert json.dumps(restricted.to_config()["where"]) == saved
            loaded = SpiralDBTableSource.from_config(
                json.loads(json.dumps(restricted.to_config()))
            )
            assert loaded.condition == reference.condition

    def test_the_text_keeps_the_order_given(self, project):
        """``text()`` is display, in the order given; the saved form is canonical."""
        forward = _source().where(*_parts())
        backward = _source().where(*reversed(_parts()))

        assert forward.condition.text().startswith("unit >= 2")
        assert backward.condition.text().startswith("f=False")
        assert forward.to_config() == backward.to_config()

    @pytest.mark.parametrize(
        "build",
        [
            lambda: _source().where({"f": False}, *_parts()[:3]),
            lambda: _source().where(*_parts()[:3], f=False),
            lambda: _source().where(_parts()[2], {"f": False}, *_parts()[:2]),
            lambda: _source(where={"f": False}).where(*_parts()[:3]),
        ],
        ids=["mapping first", "keyword last", "mapping between", "constructor mapping"],
    )
    def test_mappings_and_keywords_are_the_same_equality(self, project, build):
        restricted = build()

        assert restricted.condition == _source().where(*_parts()).condition
        assert _keys(restricted) == _expected_parts()

    def test_two_ranges_of_one_column_add_up(self, project):
        restricted = _source().where(col("v") >= -5, col("v") >= 0, col("v") < 12)

        assert _keys(restricted) == _expected(
            pc.greater_equal(_table()["v"], _scalar("v", 0)),
            pc.less(_table()["v"], _scalar("v", 12)),
        )

    def test_redundant_bounds_are_kept(self, project):
        """No simplification (D7): ``v >= 0`` and ``v > -1`` are two restrictions."""
        assert _source().where(col("v") >= 0).condition != (
            _source().where(col("v") > -1).condition
        )
        both = _source().where(col("v") >= 0, col("v") > -1).condition
        assert len(list(both.terms())) == 2


class TestRoles:
    """A range of each kind of column, on the sync and the async path."""

    @staticmethod
    def _both_paths(source: SpiralDBTableSource) -> tuple[list[Any], list[Any]]:
        """Return the rows (every column) the sync and the async read yield."""

        async def collect() -> list[dict[str, Any]]:
            return [
                {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}
                async for tag, data in source.async_iter_data()
            ]

        sync = [
            {**tag.as_dict(all_info=True), **data.as_dict(all_info=True)}
            for tag, data in source.iter_data()
        ]
        return sync, asyncio.run(collect())

    @pytest.mark.parametrize(
        ("build", "masks"),
        [
            (
                lambda: col("unit") >= 3,
                lambda t: [pc.greater_equal(t["unit"], pa.scalar(3, pa.int32()))],
            ),
            (
                lambda: col("at") < _T0 + dt.timedelta(hours=6),
                lambda t: [
                    pc.less(t["at"], _scalar("at", _T0 + dt.timedelta(hours=6)))
                ],
            ),
            (
                lambda: col("v") <= 5,
                lambda t: [pc.less_equal(t["v"], pa.scalar(5, pa.int64()))],
            ),
        ],
        ids=["first key", "last key", "data with nulls"],
    )
    @pytest.mark.parametrize("tag_only", [True, False], ids=["tags", "data-tags"])
    def test_sync_and_async_read_the_same_rows(self, project, build, masks, tag_only):
        source = _source(tag_only_columns=tag_only).where(build())

        sync, asynchronous = self._both_paths(source)

        assert asynchronous == sync
        assert [(r["unit"], r["at"]) for r in sync] == _expected(*masks(_table()))

    def test_a_key_offered_as_data_is_restricted_by_its_table_name(self, project):
        """With ``tags_as_data``, the range names the table's column, not the copy."""
        source = _source(tags_as_data={"at": "at_copy"}, source_id="ranges")
        restricted = source.where(col("at") < _T0 + dt.timedelta(hours=3))

        assert restricted.as_table()["at_copy"].to_pylist() == [
            _T0,
            _T0 + dt.timedelta(minutes=90),
        ]
        with pytest.raises(ValueError, match="at_copy"):
            source.where(col("at_copy") < _T0)


class TestRefusals:
    """Everything a range does not take is refused before any scan."""

    @pytest.mark.parametrize(
        ("column", "value"),
        [
            ("x", 0.5),
            ("g", 1.0),
            ("h", 1.0),
            ("s", "s03"),
            ("b", b"\x03"),
            ("f", True),
        ],
        ids=["float64", "float32", "float16", "string", "binary", "bool"],
    )
    @pytest.mark.parametrize("op", list(_OPERATORS))
    def test_a_range_of_another_type_is_refused(self, project, column, value, op):
        source = _source()
        fake_spiral_sdk.scan_log.clear()

        with pytest.raises(
            TypeError, match=rf"column {column!r}.*integer, timestamp or date.*filter"
        ):
            source.where(_OPERATORS[op][0](col(column), value))
        assert fake_spiral_sdk.scan_log == []

    def test_a_between_of_another_type_is_refused(self, project):
        with pytest.raises(TypeError, match="integer, timestamp or date"):
            _source().where(col("x").between(0.0, 1.0))

    def test_a_chained_comparison_is_refused(self, project):
        """``start <= col(c) < stop`` would keep one bound silently (ITL-1465)."""
        start, stop = _T0, _T0 + dt.timedelta(hours=1)
        source = _source()
        fake_spiral_sdk.scan_log.clear()

        with pytest.raises(
            TypeError,
            match=r"Condition has no truth value\. .*For a range, use "
            r"\.between\(a, b\) \(both ends included\)\.",
        ):
            source.where(start <= col("at") < stop)
        assert source.condition.is_empty
        assert fake_spiral_sdk.scan_log == []

    @pytest.mark.parametrize(
        ("column", "value"),
        [("x", 0.5), ("g", 1.0), ("s", "s03"), ("f", True)],
        ids=["float64", "float32", "string", "bool"],
    )
    def test_an_equality_of_those_types_still_works(self, project, column, value):
        expected = _expected(pc.equal(_table()[column], _scalar(column, value)))

        assert _keys(_source().where({column: value})) == expected

    def test_a_decimal_column_takes_no_condition(self, project):
        with pytest.raises(TypeError, match="dec"):
            _source().where(col("dec") >= 3)

    @pytest.mark.parametrize(
        ("column", "value", "match"),
        [
            ("n", -1, "cannot hold -1"),
            ("n", 256, "cannot hold 256"),
            ("n", 2.0, "is a float"),
            ("n", True, "is a bool"),
            ("n", "2", "is a str"),
            ("n", np.uint8(2), "is a uint8"),
            ("unit", 2**31, "cannot hold"),
            ("v", 2**63, "cannot hold"),
            ("at", _WALL, "aware"),
            ("w", _T0, "naive"),
            ("sec", _WALL.replace(microsecond=1), "cannot hold"),
            ("d", _T0, "is a datetime"),
            ("d64", _T0, "is a datetime"),
            ("d", "2026-11-01", "is a str"),
        ],
        ids=[
            "negative unsigned",
            "beyond uint8",
            "float for int",
            "bool for int",
            "str for int",
            "numpy scalar",
            "beyond int32",
            "beyond int64",
            "naive for zoned",
            "aware for naive",
            "finer than the unit",
            "datetime for date32",
            "datetime for date64",
            "str for date",
        ],
    )
    @pytest.mark.parametrize("op", ["<", ">="])
    def test_a_bound_the_column_cannot_hold_is_refused(
        self, project, column, value, match, op
    ):
        """Nothing is converted, rounded or clamped (D6); nothing is read."""
        source = _source()
        fake_spiral_sdk.scan_log.clear()

        with pytest.raises(TypeError, match=match) as caught:
            source.where(_OPERATORS[op][0](col(column), value))
        assert f"column {column!r}" in str(caught.value)
        assert fake_spiral_sdk.scan_log == []

    def test_the_constructor_refuses_what_where_refuses(self, project):
        with pytest.raises(TypeError, match="integer, timestamp or date"):
            _source(where=col("x") > 0.0)
        with pytest.raises(TypeError, match="cannot hold"):
            _source(where=col("n") < 256)

    def test_an_unknown_column_is_refused(self, project):
        with pytest.raises(ValueError, match="no_such"):
            _source().where(col("no_such") >= 1)

    def test_a_refused_range_leaves_the_source_unchanged(self, project):
        source = _source().where(col("unit") >= 2)

        with pytest.raises(TypeError):
            source.where(col("x") > 0.0)
        assert source.condition == Condition.of(col("unit") >= 2)


class TestIdentity:
    """A range changes no identity, schema, system tag or record id."""

    def test_hashes_source_id_schema_and_rows_are_the_unrestricted_ones(self, project):
        source = _source()
        restricted = source.where(
            col("unit") >= 2, col("at") < _T0 + dt.timedelta(hours=12)
        )

        assert restricted.content_hash() == source.content_hash()
        assert restricted.pipeline_hash() == source.pipeline_hash()
        assert restricted.source_id == source.source_id
        assert restricted.output_schema() == source.output_schema()
        assert restricted.output_schema(all_info=True) == source.output_schema(
            all_info=True
        )
        every = {
            (r["unit"], r["at"]): r for r in source.as_table(all_info=True).to_pylist()
        }
        rows = restricted.as_table(all_info=True).to_pylist()
        assert rows
        assert rows == [every[(r["unit"], r["at"])] for r in rows]

    def test_the_original_source_is_unchanged(self, project):
        source = _source()
        _ = source.where(col("unit") >= 4)

        assert source.condition.is_empty
        assert len(_keys(source)) == 12

    def test_the_constructor_and_where_are_one_mechanism(self, project):
        window = (col("unit") >= 2) & (col("unit") < 4)

        built = _source(where=window)
        added = _source().where(window)

        assert built.condition == added.condition
        assert built.to_config() == added.to_config()
        assert _keys(built) == _keys(added)


class TestReads:
    """The cap, the limit, ``head`` and new data, with a range."""

    def test_a_window_under_the_cap_is_read_without_counting_the_table(self, project):
        source = _source(max_rows=4)
        fake_spiral_sdk.scan_log.clear()

        assert len(_keys(source.where(col("unit") >= 4))) == 3
        assert len(fake_spiral_sdk.scan_log) == 1
        assert fake_spiral_sdk.scan_log[0].kwargs["where"] is not None

    def test_a_window_over_the_cap_names_the_window(self, project):
        with pytest.raises(SourceTooLargeError, match=r"unit >= 2.*max_rows"):
            _source(max_rows=4).where(col("unit") >= 2).as_table()

    def test_the_limit_counts_matching_rows(self, project):
        restricted = _source(limit=2).where(col("v") > 0)

        assert (
            _keys(restricted)
            == _expected(pc.greater(_table()["v"], pa.scalar(0, pa.int64())))[:2]
        )
        assert fake_spiral_sdk.scan_log[-1].kwargs["limit"] == 2

    def test_head_reads_the_first_matching_rows_and_names_the_window(self, project):
        view = _source().where(col("unit") >= 3, col("unit") < 4).head(2)

        assert view.as_table()["unit"].to_pylist() == [3, 3]
        assert len(fake_spiral_sdk.scan_log) == 1
        assert view.description is not None
        assert "unit >= 3, unit < 4" in view.description

    def test_new_rows_outside_the_window_move_the_version_only(self, project):
        restricted = _source().where(col("unit") <= 2)
        before = _keys(restricted)
        extra = _table().slice(0, 1).set_column(0, "unit", pa.array([9], pa.int32()))
        project.table("default.ranges").write(extra)

        assert restricted.check_for_new_data() is True
        assert _keys(restricted) == before


class TestJobs:
    """A job over a window: summary, refusals, reuse of stored rows, saving."""

    @staticmethod
    def _job(store: Path, source: Any, calls: list[int]) -> PipelineJob:
        """Return a job over ``source`` that counts its calls."""

        @function_pod(output_keys="half")
        def half(n: int | None) -> float | None:
            """Return half of ``n``; counts the call."""
            calls.append(-1 if n is None else n)
            return None if n is None else n / 2

        job = PipelineJob(name="windows", database=DeltaTableDatabase(store))
        with job:
            half.pod(source.select_data_columns(["n"]), label="half")
        return job

    @pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
    def test_the_summary_records_the_window(self, project, tmp_path, orchestrator):
        window = (col("at") >= _T0) & (col("at") < _T0 + dt.timedelta(hours=6))
        source = _source(source_id="events").where(window)
        job = self._job(tmp_path / "store", source, [])

        summary = job.run(orchestrator=orchestrator() if orchestrator else None)

        assert summary.conditions == {"events": window}
        assert sorted(job.nodes["half"].as_table()["half"].to_pylist()) == [
            0.0,
            0.5,
            1.0,
            127.5,
        ]

    def test_overlapping_windows_compute_each_row_once(self, project, tmp_path):
        """Record ids do not depend on the window, so stored rows are reused."""
        calls: list[int] = []
        events = _source(source_id="events")
        job = self._job(tmp_path / "store", events.where(col("unit") < 3), calls)
        job.run()
        assert sorted(calls) == [0, 1, 2, 3, 4, 255]

        calls.clear()
        job.bind(sources={"events": events.where(col("unit") >= 2)})
        assert job.nodes["half"].is_stale
        job.run()

        assert sorted(calls) == [5, 6, 7, 8, 9, 10]
        assert len(job.nodes["half"].as_table()) == 9

    def test_one_table_under_two_windows_in_one_job_is_refused(self, project, tmp_path):
        events = _source(source_id="events")

        @function_pod(output_keys="m")
        def same(n: int | None) -> int | None:
            return n

        job = PipelineJob(name="two", database=DeltaTableDatabase(tmp_path / "s"))
        with job:
            same.pod(
                events.where(col("unit") < 2).select_data_columns(["n"]), label="a"
            )
            with pytest.raises(
                InconsistentSourceError, match="distinct source objects"
            ):
                events.where(col("unit") >= 2).select_data_columns(["n"], label="b")
        assert {"a", "b"} & job.nodes.keys() == {"a"}

    def test_a_saved_job_keeps_the_window_in_canonical_order(self, project, tmp_path):
        store = tmp_path / "store"
        source = _source(source_id="events").where(
            col("at") < _T0 + dt.timedelta(hours=9), f=True, unit=2
        )
        job = self._job(store, source, [])
        job.run()
        path = tmp_path / "job.json"
        job.save(path)

        saved = json.loads(path.read_text())["bindings"]["sources"]["events"]["where"]
        assert saved == source.condition.to_config()
        terms = saved[":condition"]["all"]
        assert [t["column"] for t in terms] == sorted(t["column"] for t in terms)
        loaded = PipelineJob.load(path, database=DeltaTableDatabase(store))
        loaded_source = loaded.nodes["events"].bound_source
        assert isinstance(loaded_source, SpiralDBTableSource)
        assert loaded_source.condition == source.condition
        assert _keys(loaded_source) == _keys(source)


class TestSavedEqualities:
    """An equality-only ``where`` is saved in canonical order (D8)."""

    def test_two_equalities_given_in_either_order_save_the_same_bytes(self, project):
        forward = _source().where(unit=2, f=True)
        backward = _source().where(f=True, unit=2)

        assert json.dumps(forward.to_config()["where"]) == '{"f": true, "unit": 2}'
        assert json.dumps(backward.to_config()["where"]) == '{"f": true, "unit": 2}'

    def test_a_config_saved_in_the_order_given_still_loads(self, project):
        """A file written before this change (keys in the order given) loads equal."""
        config = _source().where(unit=2, f=True).to_config()
        config["where"] = {"unit": 2, "f": True}

        loaded = SpiralDBTableSource.from_config(config)

        assert loaded.condition == Condition.of(f=True, unit=2)
        assert _keys(loaded) == _expected(
            pc.equal(_table()["unit"], pa.scalar(2, pa.int32())),
            pc.equal(_table()["f"], pa.scalar(True)),
        )

    def test_one_equality_is_saved_as_before(self, project):
        assert _source().where(unit=2).to_config()["where"] == {"unit": 2}
