"""Orcapod's float rule: in the Arrow tables Orcapod builds, -0.0 hashes as 0.0 and every
NaN as the ordinary NaN (ITL-1251, ITL-1179).

Matching follows Polars (0.0 equals -0.0, NaN equals NaN), and so do the hashes of the
tables Orcapod builds (streams, rows, tag and data datagrams, preimages, the tables behind
default source ids): ``StarfixArrowHasher.hash_table`` runs starfix with
``normalize_negative_zero`` and ``normalize_nan`` explicitly on. Out of scope, strict as
before (every bit counts): library values (numpy, torch, pandas, Polars, a pyarrow table
given as a value) and the semantic hasher's Python floats. Stored values are never
rewritten.

The rule restated at Orcapod's level, and tested here as a property: the hash Orcapod
gives a table it built equals the hash it gives the same table with every float
normalized (``-0.0`` to ``0.0``, any NaN to ``0x7e00`` / ``0x7fc00000`` /
``0x7ff8000000000000``).
"""

from __future__ import annotations

import struct
from collections.abc import Sequence
from typing import Any

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.sources import ArrowTableSource
from orcapod.types import Schema

WIDTHS = (16, 32, 64)
FLOAT = {16: pa.float16(), 32: pa.float32(), 64: pa.float64()}
UNSIGNED = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}
NP_UNSIGNED = {16: np.uint16, 32: np.uint32, 64: np.uint64}

# Bit patterns by name and width. Every NaN below hashes as "nan", "-0" as "0".
BITS: dict[str, dict[int, int]] = {
    "0": {16: 0x0000, 32: 0x00000000, 64: 0x0000000000000000},
    "-0": {16: 0x8000, 32: 0x80000000, 64: 0x8000000000000000},
    "nan": {16: 0x7E00, 32: 0x7FC00000, 64: 0x7FF8000000000000},
    "-nan": {16: 0xFE00, 32: 0xFFC00000, 64: 0xFFF8000000000000},
    "payload_nan": {16: 0x7E01, 32: 0x7FC00001, 64: 0x7FF8000000000001},
    "-payload_nan": {16: 0xFE3F, 32: 0xFFFFFFFF, 64: 0xFFFFFFFFFFFFFFFF},
    "signaling_nan": {16: 0x7C01, 32: 0x7F800001, 64: 0x7FF0000000000001},
    "-signaling_nan": {16: 0xFC01, 32: 0xFF800001, 64: 0xFFF0000000000001},
    "max_payload_signaling_nan": {16: 0x7DFF, 32: 0x7FBFFFFF, 64: 0x7FF7FFFFFFFFFFFF},
    "1": {16: 0x3C00, 32: 0x3F800000, 64: 0x3FF0000000000000},
    "inf": {16: 0x7C00, 32: 0x7F800000, 64: 0x7FF0000000000000},
    "-inf": {16: 0xFC00, 32: 0xFF800000, 64: 0xFFF0000000000000},
    "subnormal": {16: 0x0001, 32: 0x00000001, 64: 0x0000000000000001},
    "-subnormal": {16: 0x8001, 32: 0x80000001, 64: 0x8000000000000001},
}
NANS = [name for name in BITS if "nan" in name]
NORMAL_FORM = {name: ("nan" if "nan" in name else name) for name in BITS}
NORMAL_FORM["-0"] = "0"


def _floats(names: Sequence[str | None], width: int) -> pa.Array[Any]:
    """A float array holding exactly the named bit patterns (``None`` is a null)."""
    bits = [None if name is None else BITS[name][width] for name in names]
    return pa.array(bits, UNSIGNED[width]).view(FLOAT[width])


def _normalized(names: Sequence[str | None]) -> list[str | None]:
    return [None if name is None else NORMAL_FORM[name] for name in names]


def _f64(name: str) -> float:
    value: float = struct.unpack("<d", struct.pack("<Q", BITS[name][64]))[0]
    return value


def _hasher() -> StarfixArrowHasher:
    hasher = get_default_context().arrow_hasher
    assert isinstance(hasher, StarfixArrowHasher)
    return hasher


def _own(table: pa.Table) -> str:
    """The hash of a table Orcapod built (``hash_table``)."""
    return _hasher().hash_table(table).to_string()


def _value_hashes(table: pa.Table) -> tuple[str, str, str]:
    """The strict hashes of a table as a library value: as a value, ordered, ordered
    without struct order."""
    hasher = _hasher()
    return (
        hasher.hash_table_strict(table).to_string(),
        hasher.hash_table_ordered(table).to_string(),
        hasher.hash_table_ordered(table, struct_field_order=False).to_string(),
    )


def _ipc_bytes(table: pa.Table) -> bytes:
    """The table's Arrow IPC stream: equal exactly when every bit is equal."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _bits_of(
    array: pa.Array[Any] | pa.ChunkedArray[Any], width: int
) -> list[int | None]:
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    return array.view(UNSIGNED[width]).to_pylist()


# --- the hash of a table Orcapod built ------------------------------------------------------


class TestEveryPatternHashesAsItsNormalForm:
    @pytest.mark.parametrize("width", WIDTHS)
    @pytest.mark.parametrize("name", list(BITS))
    def test_one_value(self, name: str, width: int) -> None:
        """A one-cell column hashes as the cell's normal form, at every width; as a
        library value the same table keeps its bits."""
        raw = pa.table({"x": _floats([name], width)})
        normal = pa.table({"x": _floats([NORMAL_FORM[name]], width)})
        assert _own(raw) == _own(normal)
        if name != NORMAL_FORM[name]:
            for strict_raw, strict_normal in zip(
                _value_hashes(raw), _value_hashes(normal), strict=True
            ):
                assert strict_raw != strict_normal

    @pytest.mark.parametrize("width", WIDTHS)
    def test_every_nan_is_one_value(self, width: int) -> None:
        """Every NaN pattern (sign, payload, quiet or signaling) hashes alike."""
        assert len({_own(pa.table({"x": _floats([n], width)})) for n in NANS}) == 1

    @pytest.mark.parametrize("width", WIDTHS)
    def test_distinct_values_stay_distinct(self, width: int) -> None:
        """The rule merges only -0.0 with 0.0 and the NaNs: every other value is its own."""
        names = ["0", "nan", "1", "inf", "-inf", "subnormal", "-subnormal"]
        assert len({_own(pa.table({"x": _floats([n], width)})) for n in names}) == len(
            names
        )

    @pytest.mark.parametrize("width", WIDTHS)
    def test_null_is_not_nan_or_zero(self, width: int) -> None:
        """A null is a null: it hashes apart from 0.0, -0.0 and every NaN."""
        null = _own(pa.table({"x": _floats([None], width)}))
        for name in ("0", "-0", *NANS):
            assert _own(pa.table({"x": _floats([name], width)})) != null


def _column_layouts(names: list[str | None], width: int) -> dict[str, pa.Array[Any]]:
    """The same floats as a column, in a list, large list, nested list, fixed-size list,
    struct, nested struct, list of structs, dictionary and extension storage."""
    flat = _floats(names, width)
    n = len(names)
    layouts: dict[str, pa.Array[Any]] = {
        "scalar": flat,
        "list": pa.ListArray.from_arrays(pa.array([0, n // 2, n], pa.int32()), flat),
        "large_list": pa.LargeListArray.from_arrays(
            pa.array([0, 1, n], pa.int64()), flat
        ),
        "nested_list": pa.ListArray.from_arrays(
            pa.array([0, 1, 2], pa.int32()),
            pa.ListArray.from_arrays(pa.array([0, n - 1, n], pa.int32()), flat),
        ),
        "fixed_size_list": pa.FixedSizeListArray.from_arrays(flat, 2),
        "struct": pa.StructArray.from_arrays(
            [flat, pa.array(range(n), pa.int64())], names=["f", "i"]
        ),
        "nested_struct": pa.StructArray.from_arrays(
            [
                pa.StructArray.from_arrays([flat], names=["f"]),
                pa.array(range(n), pa.int64()),
            ],
            names=["s", "i"],
        ),
        "list_of_struct": pa.ListArray.from_arrays(
            pa.array([0, n], pa.int32()),
            pa.StructArray.from_arrays([flat], names=["f"]),
        ),
        "dictionary": pa.DictionaryArray.from_arrays(
            pa.array(list(reversed(range(n))), pa.int32()), flat
        ),
    }
    if width != 16:  # pyarrow's tensor extension type takes no float16 value type
        layouts["fixed_shape_tensor"] = pa.ExtensionArray.from_storage(
            pa.fixed_shape_tensor(FLOAT[width], [2]),
            pa.FixedSizeListArray.from_arrays(flat, 2),
        )
    return layouts


VALUES = ["-0", "nan", "1", "-nan", "payload_nan", None, "signaling_nan", "0"]
LAYOUTS = list(_column_layouts(VALUES, 64))


class TestTableEqualsNormalizedTable:
    """``hash(table) == hash(table with every float normalized)`` in every layout."""

    @pytest.mark.parametrize("width", WIDTHS)
    @pytest.mark.parametrize("layout", LAYOUTS)
    def test_layout(self, layout: str, width: int) -> None:
        raw_layouts = _column_layouts(VALUES, width)
        if layout not in raw_layouts:
            pytest.skip(f"{layout} has no {width}-bit form in pyarrow")
        raw = pa.table({"x": raw_layouts[layout]})
        normal = pa.table({"x": _column_layouts(_normalized(VALUES), width)[layout]})
        assert _own(raw) == _own(normal)
        assert _value_hashes(raw)[0] != _value_hashes(normal)[0]
        # Hashing leaves the stored values as they were, bit for bit.
        assert _ipc_bytes(raw) == _ipc_bytes(
            pa.table({"x": _column_layouts(VALUES, width)[layout]})
        )

    def test_several_columns_of_every_width_in_both_orders(self) -> None:
        """Several float columns (three widths) beside other columns, in two orders."""
        reverse = list(reversed(VALUES))
        columns = {
            "a": _floats(VALUES, 16),
            "id": pa.array(range(len(VALUES)), pa.int64()),
            "b": _floats(reverse, 32),
            "c": _floats(VALUES, 64),
            "s": pa.array([str(v) for v in VALUES], pa.large_string()),
        }
        normal = {
            "a": _floats(_normalized(VALUES), 16),
            "id": columns["id"],
            "b": _floats(_normalized(reverse), 32),
            "c": _floats(_normalized(VALUES), 64),
            "s": columns["s"],
        }
        hashes = set()
        for order in (list(columns), list(reversed(columns))):
            raw_table = pa.table({k: columns[k] for k in order})
            normal_table = pa.table({k: normal[k] for k in order})
            assert _own(raw_table) == _own(normal_table)
            hashes.add(_own(raw_table))
        assert len(hashes) == 1  # column order is not part of an Orcapod table's hash

    def test_chunked_column(self) -> None:
        """A column in several chunks hashes as its normalized single chunk."""
        chunks = [_floats(VALUES[:3], 64), _floats(VALUES[3:], 64)]
        raw = pa.Table.from_arrays([pa.chunked_array(chunks)], names=["x"])
        normal = pa.table({"x": _floats(_normalized(VALUES), 64)})
        assert _own(raw) == _own(normal)


def _reference_normalize(
    bits: np.ndarray[Any, Any], width: int
) -> np.ndarray[Any, Any]:
    """The float rule on raw bits, written independently of starfix and Orcapod."""
    dtype = NP_UNSIGNED[width]
    exponent = dtype({16: 0x7C00, 32: 0x7F800000, 64: 0x7FF0000000000000}[width])
    mantissa = dtype({16: 0x03FF, 32: 0x007FFFFF, 64: 0x000FFFFFFFFFFFFF}[width])
    sign = dtype({16: 0x8000, 32: 0x80000000, 64: 0x8000000000000000}[width])
    out = bits.copy()
    out[((bits & exponent) == exponent) & ((bits & mantissa) != 0)] = dtype(
        BITS["nan"][width]
    )
    out[bits == sign] = dtype(0)
    return out


class TestPropertyOverGeneratedTables:
    """The invariant over generated tables, against a reference normalization."""

    @pytest.mark.parametrize("width", WIDTHS)
    @pytest.mark.parametrize("seed", range(8))
    def test_hash_equals_hash_of_reference_normalized(
        self, width: int, seed: int
    ) -> None:
        rng = np.random.default_rng(seed)
        dtype = NP_UNSIGNED[width]
        n = int(rng.integers(1, 40))
        specials = np.array([v[width] for v in BITS.values()], dtype=dtype)
        bits = np.where(
            rng.random(n) < 0.7,
            rng.choice(specials, n),
            rng.integers(0, np.iinfo(dtype).max, n, dtype=dtype, endpoint=True),
        ).astype(dtype)
        mask = rng.random(n) < 0.15
        # n list rows of varying lengths over the same n values.
        cuts = np.sort(rng.integers(0, n + 1, n - 1))
        offsets = pa.array([0, *cuts.tolist(), n], pa.int32())

        def table(raw_bits: np.ndarray[Any, Any]) -> pa.Table:
            column = pa.array(raw_bits, UNSIGNED[width], mask=mask).view(FLOAT[width])
            return pa.table(
                {"x": column, "l": pa.ListArray.from_arrays(offsets, column)}
            )

        assert _own(table(bits)) == _own(table(_reference_normalize(bits, width)))


# --- stored values, rows and source ids -----------------------------------------------------


class TestStoredValuesKeepTheirBits:
    @pytest.mark.parametrize("width", WIDTHS)
    def test_source_rows_keep_their_bits(self, width: int) -> None:
        """A source's table and its rows keep every bit; only hashes are normalized."""
        names = ["-0", "nan", "-nan", "payload_nan", "signaling_nan", "0"]
        table = pa.table(
            {
                "k": pa.array(["a", "b", "c", "d", "e", "f"], pa.large_string()),
                "j": pa.array([1, 2, 1, 2, 1, 2], pa.int64()),
                "x": _floats(names, width),
                "y": _floats(list(reversed(names)), width),
            }
        )
        source = ArrowTableSource(
            table, tag_columns=["k", "j"], source_id="s", tag_only_columns=True
        )
        out = source.as_table()
        assert _bits_of(out["x"], width) == [BITS[n][width] for n in names]
        assert _bits_of(out["y"], width) == [BITS[n][width] for n in reversed(names)]
        for i, (_, data) in enumerate(source.iter_data()):
            assert _bits_of(data.as_table()["x"], width) == [BITS[names[i]][width]]


class TestRowAndSourceIdentity:
    """Row hashes and default source ids of rows that differ only in -0.0 or NaN bits."""

    @staticmethod
    def _source(
        x: list[str], k: list[str], *, data_tag: bool, source_id: str | None, width: int
    ) -> ArrowTableSource:
        table = pa.table(
            {
                "g": pa.array(["p", "q", "p"], pa.large_string()),
                "k": _floats(list(k), width),
                "x": _floats(list(x), width),
                "y": pa.array([1.5, 2.5, 3.5], pa.float64()),
            }
        )
        return ArrowTableSource(
            table,
            tag_columns=["g", "k"],
            tag_only_columns=["g"] if data_tag else True,
            source_id=source_id,
        )

    @pytest.mark.parametrize("width", WIDTHS)
    @pytest.mark.parametrize("data_tag", [False, True], ids=["plain_tag", "data_tag"])
    def test_tag_and_data_hashes(self, data_tag: bool, width: int) -> None:
        """Two tags and two data columns: tag and data hashes follow the rule."""
        raw = self._source(
            ["-0", "-nan", "1"],
            ["signaling_nan", "-0", "payload_nan"],
            data_tag=data_tag,
            source_id="s",
            width=width,
        )
        normal = self._source(
            ["0", "nan", "1"],
            ["nan", "0", "nan"],
            data_tag=data_tag,
            source_id="s",
            width=width,
        )
        for (t1, d1), (t2, d2) in zip(raw.iter_data(), normal.iter_data(), strict=True):
            assert t1.content_hash() == t2.content_hash()
            assert d1.content_hash() == d2.content_hash()

    @pytest.mark.parametrize("width", WIDTHS)
    def test_default_source_id(self, width: int) -> None:
        """A default source id (the hash of the table) follows the rule."""
        kwargs: dict[str, Any] = {"data_tag": False, "source_id": None, "width": width}
        raw = self._source(["-0", "-nan", "1"], ["0", "1", "payload_nan"], **kwargs)
        normal = self._source(["0", "nan", "1"], ["0", "1", "nan"], **kwargs)
        other = self._source(["0", "nan", "inf"], ["0", "1", "nan"], **kwargs)
        assert raw.source_id == normal.source_id
        assert raw.source_id != other.source_id


# --- the boundary: library values keep their bits -------------------------------------------


class TestLibraryValuesAreOutOfScope:
    def test_a_pyarrow_value_is_strict_while_the_same_floats_in_a_row_are_not(
        self,
    ) -> None:
        """A pyarrow table given as a value hashes apart from its normalized twin; the same
        floats in an Orcapod row hash equal."""
        semantic = get_default_context().semantic_hasher
        names = ["-0", "payload_nan", "-signaling_nan", "1"]
        reverse = list(reversed(names))
        raw = pa.table({"x": _floats(names, 64), "y": _floats(reverse, 32)})
        normal = pa.table(
            {
                "x": _floats(_normalized(names), 64),
                "y": _floats(_normalized(reverse), 32),
            }
        )
        assert semantic.hash_object(raw) != semantic.hash_object(normal)
        assert semantic.hash_object(raw.to_batches()[0]) != semantic.hash_object(
            normal.to_batches()[0]
        )
        keys = pa.array(["a", "b", "c", "d"], pa.large_string())
        rows = [
            [
                (tag.content_hash(), data.content_hash())
                for tag, data in ArrowTableSource(
                    table.append_column("k", keys),
                    tag_columns=["k"],
                    source_id="s",
                    tag_only_columns=True,
                ).iter_data()
            ]
            for table in (raw, normal)
        ]
        assert rows[0] == rows[1]

    def test_semantic_hasher_python_floats_are_strict(self) -> None:
        """The semantic hasher's Python floats (defaults, arguments, fields) are out of
        scope (developer, November 19, 2026): -0.0 and 0.0 hash apart, as on main."""
        semantic = get_default_context().semantic_hasher
        assert semantic.hash_object(-0.0) != semantic.hash_object(0.0)
        assert semantic.hash_object([-0.0, 1.0]) != semantic.hash_object([0.0, 1.0])


_ROW_SCHEMA = Schema(
    {
        "k": str,
        "g": int,
        "x": float,
        "w": float,
        "arr": np.ndarray,
        "frame": pd.DataFrame,
        "pl_frame": pl.DataFrame,
    }
)


def _row_table(variant: str) -> pa.Table:
    """One row: two plain float columns and numpy, pandas and Polars cells, all holding
    the float named ``variant``."""
    value = _f64(variant)
    row = {
        "k": "a",
        "g": 1,
        "x": value,
        "w": value,
        "arr": np.array([value, 2.0]),
        "frame": pd.DataFrame({"v": [value, 2.0]}),
        "pl_frame": pl.DataFrame({"v": [value, 2.0]}),
    }
    converter = get_default_context().type_converter
    return converter.python_dicts_to_arrow_table([row], python_schema=_ROW_SCHEMA)


VARIANTS = [("-0", "0"), ("-nan", "nan"), ("payload_nan", "nan")]


class TestRowWithValueCells:
    """A row with plain float columns beside numpy, pandas and Polars cells: each cell is
    replaced by its strict value digest before the row is hashed, so the cells keep their
    bits while the plain floats follow the float rule."""

    @staticmethod
    def _hashes(table: pa.Table, columns: list[str]) -> tuple[str, str]:
        source = ArrowTableSource(
            table.select(["k", "g", *columns]),
            tag_columns=["k", "g"],
            source_id="s",
            tag_only_columns=True,
        )
        ((tag, data),) = list(source.iter_data())
        return tag.content_hash().to_string(), data.content_hash().to_string()

    @pytest.mark.parametrize(("variant", "normal"), VARIANTS)
    def test_plain_floats_hash_equal(self, variant: str, normal: str) -> None:
        raw, norm = _row_table(variant), _row_table(normal)
        assert self._hashes(raw, ["x", "w"]) == self._hashes(norm, ["x", "w"])
        assert self._hashes(raw, ["w", "x"]) == self._hashes(norm, ["x", "w"])

    @pytest.mark.parametrize("cell", ["arr", "frame", "pl_frame"])
    @pytest.mark.parametrize(("variant", "normal"), VARIANTS)
    def test_value_cells_hash_differently(
        self, cell: str, variant: str, normal: str
    ) -> None:
        raw, norm = _row_table(variant), _row_table(normal)
        if cell == "frame" and variant != "-0":
            # pandas holds a missing float as NaN and Orcapod's pandas hash reads it as a
            # null, whatever its bits: a NaN payload is not part of a pandas value.
            assert self._hashes(raw, ["x", cell]) == self._hashes(norm, ["x", cell])
            return
        assert self._hashes(raw, ["x", cell]) != self._hashes(norm, ["x", cell])
        everything = ["x", "w", "arr", "frame", "pl_frame"]
        assert self._hashes(raw, everything) != self._hashes(norm, everything)
