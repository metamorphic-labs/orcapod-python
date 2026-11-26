"""Conformance of reading: the same tests on the fake and, opt-in, on SpiralDB (ITL-1186).

The fake's runs are always on. The live runs need ``SPIRAL_INTEGRATION_TESTS=1``,
``SPIRAL_PROJECT_ID`` and a sign-in spiral-sdk finds in the environment (CI:
``SPIRAL_CLIENT_ID`` and ``SPIRAL_PRIVATE_KEY``, see ``RELEASING.md``); with the first two
set, a missing sign-in fails the tests rather than skipping them. ``SPIRAL_SERVER_URL``
names another address.

spiral-sdk cannot drop a keyed table, so a live run creates nothing anew: it uses fixed
tables in the dataset ``orcapod_conformance`` (``ORCAPOD_SPIRAL_CONFORMANCE_DATASET`` names
another), made on the first run and written the same rows at each. It writes through
spiral-sdk itself, never through Orcapod, which only reads.
"""

from __future__ import annotations

import datetime as dt
import math
import operator
import os
import struct
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.databases import SpiralDBConnector
from orcapod.databases import spiraldb_connector as connector_module
from orcapod.databases import spiraldb_schema_table as st
from orcapod.databases.spiraldb_connector import SpiralDBTableNotFoundError
from orcapod.sources import SpiralDBTableSource
from orcapod.types import Condition, col
from tests.test_databases import fake_spiral_sdk as fake

_LIVE = os.environ.get("SPIRAL_INTEGRATION_TESTS") == "1"
_PROJECT = os.environ.get("SPIRAL_PROJECT_ID", "")
_URL = os.environ.get("SPIRAL_SERVER_URL", "https://api.spiraldb.dev")
_DATASET = os.environ.get("ORCAPOD_SPIRAL_CONFORMANCE_DATASET", "orcapod_conformance")

_INNER = pa.struct([pa.field("q", pa.int8()), pa.field("b", pa.large_string(), False)])
_UNITS = pa.schema(
    [
        pa.field("zz", pa.int64(), False, {"unit": "mV"}),
        pa.field(
            "g",
            pa.struct(
                [
                    pa.field("z", pa.int8(), False),
                    pa.field("a", _INNER, False),
                    pa.field("m", pa.large_string()),
                ]
            ),
            False,
        ),
        pa.field("k1", pa.large_string(), False),
        pa.field("aa", pa.float64()),
        pa.field(
            "ls",
            pa.large_list(pa.struct([("z", pa.int8()), ("a", pa.int8())])),
        ),
        pa.field("k2", pa.int32(), False),
        pa.field("fx", pa.list_(pa.field("item", pa.float32(), False), 2)),
    ],
    metadata={"who": "conformance"},
)
_ROWS = pa.Table.from_pylist(
    [
        {
            "zz": i,
            "g": {"z": i, "a": {"q": None, "b": "x"}, "m": None},
            "k1": key,
            "aa": None if i == 1 else i / 2,
            "ls": [{"z": 1, "a": 2}],
            "k2": i % 2,
            "fx": [1.0, 2.0],
        }
        for i, key in enumerate(["b", "a", "c"])
    ],
    schema=_UNITS,
)
_TYPED = pa.table(
    {
        "k": pa.array([1, 2, 3], pa.int64()),
        "h": pa.array([0.5, -0.0, float("nan")], pa.float64()).cast(pa.float16()),
        "f": pa.array([0.1, -0.0, float("nan")], pa.float32()),
        "d": pa.array([0.1, -0.0, float("nan")]),
        "s": pa.array(["x", "é", None], pa.large_string()),
        "b": pa.array([b"x", b"", None], pa.large_binary()),
        "u": pa.array([2**64 - 1, 0, 1], pa.uint64()),
        "sec": pa.array([1, 2, 3], pa.timestamp("s")),
        "utc": pa.array([1, 2, 3], pa.timestamp("us", tz="UTC")),
        "d32": pa.array([0, 1, None], pa.date32()),
    }
)


def _ranges() -> pa.Table:
    """Return the ``ranges`` rows: every type a range takes, and a float column (ITL-998).

    Integers of every width at both ends; timestamps of every unit, naive, in UTC and in
    a daylight-saving zone, before and after 1970, with values one unit apart; dates, and
    ``date64`` values that are not midnight; a null in every column but the key. ``x``
    holds the floats whose order decided that a float range is refused.
    """
    columns: dict[str, Any] = {"k": pa.array(range(10), pa.int64())}
    for name in (
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "int8",
        "int16",
        "int32",
        "int64",
    ):
        arrow_type = getattr(pa, name)()
        signed = name.startswith("int")
        high = 2 ** (arrow_type.bit_width - signed) - 1
        low = -high - 1 if signed else 0
        values = [low, low + 1, 0, 7, 7, None, 3, high - 1, high, 5]
        columns[name] = pa.array(values, arrow_type)
    timestamp: Any = pa.timestamp  # its stubs take literal units only
    seconds = [-315_619_200, 0, 1_793_511_000, 1_793_514_600, 1_793_514_600]
    seconds += [None, 4_102_444_800, 1_793_514_600, 1, -1]
    for unit, per_second in (("s", 1), ("ms", 10**3), ("us", 10**6), ("ns", 10**9)):
        counts = [None if s is None else s * per_second for s in seconds]
        counts[7] = 1_793_514_600 * per_second + 1  # one unit after a stored value
        for zone in (None, "UTC", "America/New_York"):
            name = f"ts_{unit}_{zone or 'naive'}".replace("America/New_York", "ny")
            arrow_type = timestamp(unit, zone)
            columns[name] = pa.array(counts, pa.int64()).view(arrow_type)
    days = [-400, 0, 1, 20_000, 20_000, None, 30_000, -719_162, 2, 5]
    columns["d32"] = pa.array(days, pa.int32()).view(pa.date32())
    day = 86_400_000
    columns["d64"] = pa.array(
        [None if d is None else d * day for d in days], pa.int64()
    ).view(pa.date64())
    off_midnight = [None if d is None else d * day + (d % 3) * 3_600_000 for d in days]
    columns["d64_off"] = pa.array(off_midnight, pa.int64()).view(pa.date64())
    nan, negative_nan = struct.unpack(
        "<2d", struct.pack("<2Q", 0x7FF8 << 48, 0xFFF8 << 48)
    )
    floats = [nan, negative_nan, -0.0, 0.0, math.inf, -math.inf, 1.5, None, 2.5, -3.0]
    columns["x"] = pa.array(floats, pa.float64())
    return pa.table(columns)


_RANGES = _ranges()


@dataclass(frozen=True)
class _Place:
    """Where the tables are."""

    project: str
    dataset: str
    overrides: dict[str, str] | None

    def connector(self) -> SpiralDBConnector:
        """Return a new connector."""
        return SpiralDBConnector(self.project, self.dataset, self.overrides)

    def source(self, name: str, **options: Any) -> SpiralDBTableSource:
        """Return a source on a table."""
        return SpiralDBTableSource(
            self.project,
            name,
            dataset=self.dataset,
            overrides=self.overrides,
            tag_only_columns=True,
            **options,
        )


def _seed(client: Any, place: _Place) -> None:
    """Write the tables and, once, their schema rows, through the SDK."""
    project = client.project(place.project)
    made = {}
    for name, keys, rows in (
        ("units", ["k2", "k1"], _ROWS),
        ("typed", ["k"], _TYPED),
        ("ranges", ["k"], _RANGES),
    ):
        key_schema = pa.schema([rows.schema.field(k) for k in keys])
        table = project.create_keyed_table(
            place.dataset, name, key_schema, exist_ok=True
        )
        made[name] = (table.append(rows), rows.schema)
    schemas = project.create_keyed_table(
        place.dataset, st.SCHEMA_TABLE, st.KEY_SCHEMA, exist_ok=True
    )
    if st.require_schema_table(schemas):
        return
    rows = []
    for name, (table, written) in made.items():
        presented = pa.schema(
            [_in_order(f, written.field(f.name)) for f in table.schema()],
            metadata=written.metadata,
        )
        rows.append(
            st.SchemaRow(
                table.resource_id,
                name,
                tuple(table.key_columns),
                presented.serialize().to_pybytes(),
                st.FROM_RECORD,
                int(table.version_id),
            )
        )
    scope = st.Scope(place.project, place.dataset, "0" * 64)
    for values in (*(r.stored() for r in rows), st.marker_values(scope, rows)):
        schemas.append(
            pa.Table.from_pylist(
                [{"written_at": "", "written_by": "", **values}], schema=st.ROW_SCHEMA
            )
        )


def _in_order(found: Any, written: Any) -> Any:
    """Return the written field with its struct fields in SpiralDB's order."""
    if not pa.types.is_struct(written.type):
        return written
    return written.with_type(
        pa.struct([_in_order(f, written.type.field(f.name)) for f in found.type])
    )


@pytest.fixture(
    scope="module",
    params=[
        "fake",
        pytest.param(
            "live",
            marks=pytest.mark.skipif(
                not (_LIVE and _PROJECT),
                reason="Set SPIRAL_INTEGRATION_TESTS=1 and SPIRAL_PROJECT_ID",
            ),
        ),
    ],
)
def place(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[_Place]:
    """The seeded tables, on the fake or on SpiralDB."""
    if request.param == "fake":
        root: Path = tmp_path_factory.mktemp("spiral")
        with fake.install(root):
            made = _Place("p", _DATASET, None)
            _seed(fake.FakeClient(), made)
            yield made
        return
    made = _Place(_PROJECT, _DATASET, {"server.url": _URL})
    with made.connector() as connector:
        _seed(connector._client, made)  # pyright: ignore[reportPrivateUsage]  # the SDK's client, for the test's own writes
    yield made


def test_the_listing_holds_the_tables_and_not_the_schema_table(place: _Place) -> None:
    names = place.connector().get_table_names()
    assert {"units", "typed"} <= set(names)
    assert st.SCHEMA_TABLE not in names
    assert names == sorted(names)


def test_the_presented_schema(place: _Place) -> None:
    connector = place.connector()
    schema = connector.get_arrow_schema("units")
    assert connector.get_pk_columns("units") == ["k2", "k1"]
    assert schema.names == ["k2", "k1", "aa", "fx", "ls", "zz", "g"]
    group = schema.field("g").type
    assert [f.name for f in group] == ["m", "z", "a"]
    assert [f.name for f in group.field("a").type] == ["b", "q"]
    assert [f.name for f in schema.field("ls").type.value_type] == ["z", "a"]
    assert schema.metadata == {b"who": b"conformance"}
    assert schema.field("zz").metadata == {b"unit": b"mV"}
    assert [f.name for f in schema if not f.nullable] == ["k2", "k1", "zz", "g"]
    assert not group.field("z").nullable
    assert group.field("m").nullable
    assert not group.field("a").type.field("b").nullable
    for batch in connector.scan_batches("units"):
        assert batch.schema.equals(schema, check_metadata=True)


def test_rows_in_key_order_with_their_values(place: _Place) -> None:
    table = pa.Table.from_batches(place.connector().scan_batches("units"))
    keys = list(zip(table["k2"].to_pylist(), table["k1"].to_pylist()))
    assert keys == [(0, "b"), (0, "c"), (1, "a")]
    by_key = {(r["k2"], r["k1"]): r for r in _ROWS.to_pylist()}
    assert [by_key[k] for k in keys] == table.to_pylist()


def test_source_identity_and_rows(place: _Place) -> None:
    source = place.source("units")
    assert source.source_id == (
        f"spiraldb_table_v1:{place.project}.{place.dataset}.units@"
        f"{place.connector().get_spiral_table_id('units')}"
    )
    tags, data = source.output_schema()
    assert dict(tags) == {"k2": int, "k1": str}
    assert data["zz"] is int
    rows = [(t.as_dict(), d.as_dict()) for t, d in source.iter_data()]
    assert [t["k1"] for t, _ in rows] == ["b", "c", "a"]
    again = place.source("units")
    assert again.content_hash() == source.content_hash()
    assert again.pipeline_hash() == source.pipeline_hash()


@pytest.mark.parametrize("limit", [1, 2, 3, 9])
def test_limit_reads_the_first_keys(place: _Place, limit: int) -> None:
    table = pa.Table.from_batches(place.connector().scan_batches("units", limit=limit))
    assert table["k1"].to_pylist() == ["b", "c", "a"][:limit]


@pytest.mark.parametrize(
    ("where", "expected"),
    [
        ({"h": 0.5}, [1]),
        ({"h": -0.0}, [2]),
        ({"h": math.nan}, [3]),
        ({"f": 0.1}, []),
        ({"f": -0.0}, [2]),
        ({"f": math.nan}, [3]),
        ({"f": 1e300}, []),
        ({"d": 0.1}, [1]),
        ({"d": 0.0}, []),
        ({"s": "é"}, [2]),
        ({"b": b""}, [2]),
        ({"u": 2**64 - 1}, [1]),
        ({"u": 0}, [2]),
        ({"sec": dt.datetime(1970, 1, 1, 0, 0, 1)}, [1]),
        ({"utc": dt.datetime(1970, 1, 1, 0, 0, 0, 2, dt.timezone.utc)}, [2]),
        ({"d32": dt.date(1970, 1, 2)}, [2]),
        ({"s": "x", "k": 1}, [1]),
        ({"k": 1, "s": "é"}, []),
        ({"h": 0.5, "s": "x"}, [1]),
        ({"s": "x", "h": 0.5}, [1]),
        ({"k": 1, "s": "x"}, [1]),
        ({"s": "é", "k": 1}, []),
    ],
)
def test_where(place: _Place, where: dict[str, Any], expected: list[int]) -> None:
    batches = place.connector().scan_batches("typed", where=Condition.of(where))
    assert [k for b in batches for k in b.column("k").to_pylist()] == expected


def test_counts(place: _Place) -> None:
    table = place.connector().lookup_table("units")
    assert table.count_rows(at_most=2) == 2
    assert table.row_count_estimate() == 3


def test_versions(place: _Place) -> None:
    connector = place.connector()
    version = connector.get_table_version("units")
    assert version > 0
    pinned = pa.Table.from_batches(connector.scan_batches("units", asof=version))
    latest = pa.Table.from_batches(connector.scan_batches("units"))
    assert pinned.equals(latest)


def test_a_missing_table_and_dataset(place: _Place) -> None:
    with pytest.raises(SpiralDBTableNotFoundError):
        place.connector().lookup_table("absent")
    missing = SpiralDBConnector(place.project, "absent_dataset", place.overrides)
    with pytest.raises(SpiralDBTableNotFoundError):
        missing.lookup_table("absent")
    assert missing.get_table_names() == []


def test_an_early_close(place: _Place) -> None:
    batches: Any = place.connector().scan_batches("units")
    assert next(batches).num_rows > 0
    batches.close()


# ---------------------------------------------------------------------------- ranges
# A dataset seeded before ``ranges`` existed records no schema row for it (rows are
# written once): its columns then read as nullable, with a warning; the keys compared
# here are unaffected.

_RANGE_OPERATORS = {
    "<": (operator.lt, pc.less),
    "<=": (operator.le, pc.less_equal),
    ">": (operator.gt, pc.greater),
    ">=": (operator.ge, pc.greater_equal),
}


def _range_bounds(name: str) -> tuple[Any, Any, Any]:
    """Return a low, a middle and a high bound for a ``ranges`` column, as ``where``
    takes them (a stored value where the column holds one exactly)."""
    arrow_type = _RANGES.schema.field(name).type
    if pa.types.is_integer(arrow_type):
        return (_RANGES[name][1].as_py(), 7, _RANGES[name][7].as_py())
    if pa.types.is_date(arrow_type):
        epoch = dt.date(1970, 1, 1)
        return (
            epoch,
            epoch + dt.timedelta(days=20_000),
            epoch + dt.timedelta(days=30_000),
        )
    zone = dt.timezone.utc if arrow_type.tz is not None else None
    moments = [
        dt.datetime(1970, 1, 1),
        dt.datetime(2026, 11, 1, 6, 30),
        dt.datetime(2100, 1, 1),
    ]
    low, middle, high = (m.replace(tzinfo=zone) for m in moments)
    return low, middle, high


_RANGE_COLUMNS = [n for n in _RANGES.column_names if n not in ("k", "x")]


def _keys_where(place: _Place, condition: Condition) -> list[Any]:
    """Return the keys the connector reads under ``condition``, in order."""
    batches = place.connector().scan_batches("ranges", where=condition)
    return [k for b in batches for k in b.column("k").to_pylist()]


def _pyarrow_keys(*masks: Any) -> list[Any]:
    """Return the keys of the rows where every mask holds (a null holds not)."""
    keep = pc.fill_null(masks[0], False)
    for mask in masks[1:]:
        keep = pc.and_(keep, pc.fill_null(mask, False))
    return _RANGES.filter(keep)["k"].to_pylist()


@pytest.mark.filterwarnings("ignore::orcapod.errors.UnrecordedTableSchemaWarning")
@pytest.mark.parametrize("name", _RANGE_COLUMNS)
def test_a_range_of_every_type_reads_pyarrows_rows(place: _Place, name: str) -> None:
    """Each operator with a bound, and ``between``, on every type a range takes."""
    column = _RANGES[name]
    arrow_type = column.type
    low, middle, high = _range_bounds(name)
    for python_op, arrow_op in _RANGE_OPERATORS.values():
        expected = _pyarrow_keys(arrow_op(column, pa.scalar(middle, arrow_type)))
        assert _keys_where(place, python_op(col(name), middle)) == expected, name
    expected = _pyarrow_keys(
        pc.greater_equal(column, pa.scalar(low, arrow_type)),
        pc.less_equal(column, pa.scalar(high, arrow_type)),
    )
    assert _keys_where(place, col(name).between(low, high)) == expected


def test_the_service_orders_floats_by_total_order(place: _Place) -> None:
    """Why a float range is refused (ITL-998, D14; ITL-1477): through the SDK alone, a
    positive NaN is above every number, the negative NaN below, and -0.0 below 0.0."""
    sp: Any = connector_module.sp
    connector = place.connector()
    with connector:
        table = connector.lookup_table("ranges")._at(None)  # pyright: ignore[reportPrivateUsage]  # the SDK's handle, for an SDK-only scan
        zero = sp.literal(0.0, pa.float64())

        def keys(predicate: Any) -> list[int]:
            return table.scan(filter=predicate).to_table()["k"].to_pylist()

        assert keys(sp.gt(sp.column("x"), zero)) == [0, 4, 6, 8]  # NaN, inf, 1.5, 2.5
        assert keys(sp.lt(sp.column("x"), zero)) == [1, 2, 5, 9]  # -NaN, -0.0, -inf, -3
        assert keys(sp.eq(sp.column("x"), zero)) == [3]  # not -0.0


@pytest.mark.filterwarnings("ignore::orcapod.errors.UnrecordedTableSchemaWarning")
def test_a_source_reads_a_window_with_an_equality_and_a_limit(place: _Place) -> None:
    """Through the source: a window and an equality, with and without a limit; a float
    range is refused before any read."""
    window = (col("ts_us_UTC") >= dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)) & (
        col("int64") < 7
    )
    expected = _pyarrow_keys(
        pc.greater_equal(_RANGES["ts_us_UTC"], pa.scalar(0, pa.timestamp("us", "UTC"))),
        pc.less(_RANGES["int64"], pa.scalar(7, pa.int64())),
    )
    source = place.source("ranges").where(window)
    assert source.as_table()["k"].to_pylist() == expected
    limited = place.source("ranges", limit=2).where(window)
    assert limited.as_table()["k"].to_pylist() == expected[:2]
    with pytest.raises(TypeError, match="integer, timestamp or date"):
        place.source("ranges").where(col("x") > 0.0)
