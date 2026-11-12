"""Content hashing of ``pl.DataFrame`` and ``pl.Series`` values (ITL-578).

The hash of a frame-valued data column is a cache key. It depends on the frame's logical
content, everything a pod can observe through the public Polars API: its values (a null
struct and a struct of nulls included), row count, column order, and each column's name and
dtype in Orcapod's own dtype grammar (see ``orcapod.logical_types.polars_type``), struct field
order and a ``Categorical``'s ``Categories`` included. It does not depend on how the frame
happens to be laid out in memory (chunking, slice offsets, the bytes under null slots), and it
is the same in every process.

Hashes are taken through the public path a cache lookup uses: the ``content_hash()`` of the
``Data`` a frame-valued source yields. The pinned values themselves live in the shared
golden suite (``tests/test_hashing/golden``, the ``polars_*`` cases).
"""

from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from starfix import ArrowDigester

from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase
from orcapod.logical_types.identifier import (
    LogicalTypeIdentifier,
    arrow_type_identifier,
)
from orcapod.logical_types.polars_type import (
    POLARS_DATAFRAME_HASH_LABEL,
    POLARS_SERIES_HASH_LABEL,
    LogicalPolarsDataFrame,
    LogicalPolarsSeries,
    polars_dtype_descriptor,
)
from orcapod.sources import DictSource
from orcapod.types import ContentHash


def _data_hash(value: pl.DataFrame | pl.Series) -> str:
    """Content hash of the ``Data`` holding ``value`` in its only data column."""
    rows: list[dict[str, Any]] = [{"k": "a", "v": value}]
    source = DictSource(
        rows, tag_columns=["k"], data_schema={"k": str, "v": type(value)}
    )
    ((_, data),) = list(source.iter_data())
    return data.content_hash().to_hex()


def _all_distinct(values: list[pl.DataFrame] | list[pl.Series]) -> bool:
    hashes = [_data_hash(value) for value in values]
    return len(set(hashes)) == len(hashes)


_BASE = pl.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
_XSTRUCT = pl.Struct({"x": pl.Int8})


class TestEqualFramesHashEqual:
    def test_same_frame_twice(self) -> None:
        assert _data_hash(_BASE) == _data_hash(_BASE.clone())

    def test_chunk_layout_does_not_matter(self) -> None:
        chunked = pl.concat([_BASE.slice(0, 1), _BASE.slice(1, 2)], rechunk=False)
        assert chunked.n_chunks() == 2
        assert _data_hash(chunked) == _data_hash(chunked.rechunk())
        assert _data_hash(chunked) == _data_hash(_BASE)

    def test_bytes_under_null_slots_do_not_matter(self) -> None:
        """``[1, None, 3]`` built directly and by nulling a ``2`` are the same frame."""
        direct = pl.DataFrame({"x": [1, None, 3]})
        nulled = pl.DataFrame({"x": [1, 2, 3]}).with_columns(
            pl.when(pl.col("x") == 2).then(None).otherwise(pl.col("x")).alias("x")
        )
        assert _data_hash(direct) == _data_hash(nulled)

    def test_slice_offset_does_not_matter(self) -> None:
        wide = pl.DataFrame({"s": ["z", "a", "b"], "l": [[0], [1], [2, 3]]})
        assert _data_hash(wide.slice(1)) == _data_hash(
            pl.DataFrame({"s": ["a", "b"], "l": [[1], [2, 3]]})
        )

    def test_default_categoricals_hash_equal(self) -> None:
        values = ["b", "a"]
        assert _data_hash(
            pl.DataFrame({"c": pl.Series(values, dtype=pl.Categorical)})
        ) == _data_hash(pl.DataFrame({"c": pl.Series(values, dtype=pl.Categorical())}))

    def test_value_read_back_from_a_database_hashes_the_same(
        self, tmp_path: Path
    ) -> None:
        expected = pl.DataFrame(
            {
                "e": pl.Series(["a", None], dtype=pl.Enum(["b", "a"])),
                "a": pl.Series([[1, 2], None], dtype=pl.Array(pl.Int16, 2)),
                "n": pl.Series([None, None], dtype=pl.Null),
            }
        )

        @function_pod(output_keys="v")
        def make(n: int) -> pl.DataFrame:
            return expected.head(n)

        job = PipelineJob(name="h", store=DeltaTableDatabase(tmp_path / "store"))
        with job:
            make.pod(DictSource([{"k": "a", "n": 2}], tag_columns=["k"]), label="m")
        job.run()
        ((_, data),) = list(job.nodes["m"].iter_data())
        assert data.content_hash().to_hex() == _data_hash(expected)


class TestDifferentFramesHashDifferent:
    def test_value(self) -> None:
        changed = _BASE.with_columns(pl.Series("x", [1, 2, 4]))
        assert _data_hash(changed) != _data_hash(_BASE)

    def test_column_name(self) -> None:
        assert _data_hash(_BASE.rename({"x": "z"})) != _data_hash(_BASE)

    def test_row_order(self) -> None:
        assert _data_hash(_BASE.reverse()) != _data_hash(_BASE)

    def test_column_order(self) -> None:
        """A pod can observe column order (``df.columns[0]``), so it is identity."""
        assert _data_hash(_BASE.select("y", "x")) != _data_hash(_BASE)

    @pytest.mark.parametrize(
        "wrap",
        [
            pytest.param(lambda dtype: dtype, id="top-level"),
            pytest.param(pl.List, id="in-list"),
            pytest.param(lambda dtype: pl.Array(dtype, 1), id="in-array"),
            pytest.param(lambda dtype: pl.Struct({"outer": dtype}), id="in-struct"),
        ],
    )
    def test_struct_field_order(self, wrap: Any) -> None:
        """Struct fields are ordered and visible to pod code, at every nesting depth."""
        ab = pl.Struct({"a": pl.Int64, "b": pl.String})
        ba = pl.Struct({"b": pl.String, "a": pl.Int64})
        frames = [
            pl.DataFrame({"s": pl.Series([None], dtype=wrap(dtype))})
            for dtype in (ab, ba)
        ]
        assert _all_distinct(frames)

    def test_struct_field_order_in_series(self) -> None:
        ab = pl.Series("s", [{"a": 1, "b": "x"}])
        assert _data_hash(ab) != _data_hash(
            ab.struct.unnest().select("b", "a").to_struct("s")
        )

    @pytest.mark.parametrize(
        ("wrap", "dtype"),
        [
            pytest.param(lambda v: v, _XSTRUCT, id="top-level"),
            pytest.param(lambda v: [v], pl.List(_XSTRUCT), id="in-list"),
            pytest.param(
                lambda v: {"outer": v}, pl.Struct({"outer": _XSTRUCT}), id="in-struct"
            ),
        ],
    )
    def test_null_struct_and_struct_of_nulls(self, wrap: Any, dtype: Any) -> None:
        """``is_null()`` tells a null struct from a struct whose fields are all null."""
        null_struct = pl.DataFrame(
            {"s": pl.Series([wrap(None), wrap({"x": 1})], dtype=dtype)}
        )
        struct_of_nulls = pl.DataFrame(
            {"s": pl.Series([wrap({"x": None}), wrap({"x": 1})], dtype=dtype)}
        )
        assert _data_hash(null_struct) != _data_hash(struct_of_nulls)

    @pytest.mark.parametrize(
        ("wrap", "dtype"),
        [
            pytest.param(lambda v: v, pl.Struct({}), id="top-level"),
            pytest.param(lambda v: [v], pl.List(pl.Struct({})), id="in-list"),
            pytest.param(
                lambda v: {"outer": v},
                pl.Struct({"outer": pl.Struct({})}),
                id="in-struct",
            ),
        ],
    )
    def test_zero_field_struct(self, wrap: Any, dtype: Any) -> None:
        """A struct with no fields hashes, and its nulls are still identity."""
        frames = [
            pl.DataFrame({"s": pl.Series([wrap({}), wrap({})], dtype=dtype)}),
            pl.DataFrame({"s": pl.Series([wrap(None), wrap({})], dtype=dtype)}),
            pl.DataFrame({"s": pl.Series([wrap({})], dtype=dtype)}),
        ]
        assert _all_distinct(frames)
        series = [frame.to_series().alias("s") for frame in frames]
        assert _all_distinct(series)

    def test_categorical_categories_are_identity(self) -> None:
        """A ``Categorical``'s ``Categories`` name, namespace and physical width."""
        values = ["a", "b"]
        frames = [
            pl.DataFrame({"c": pl.Series(values, dtype=pl.Categorical(categories))})
            for categories in (
                pl.Categories(),
                pl.Categories("units"),
                pl.Categories("units", "lab"),
                pl.Categories("units", "lab", pl.UInt8),
            )
        ]
        assert _all_distinct(frames)

    def test_integer_width_and_signedness(self) -> None:
        frames = [
            _BASE.with_columns(pl.col("x").cast(dtype))
            for dtype in (pl.Int64, pl.Int32, pl.Int8, pl.UInt8, pl.UInt64)
        ]
        assert _all_distinct(frames)

    def test_float_width(self) -> None:
        values = [1.5, 2.5]
        frames = [
            pl.DataFrame({"f": pl.Series(values, dtype=dtype)})
            for dtype in (pl.Float16, pl.Float32, pl.Float64)
        ]
        assert _all_distinct(frames)

    def test_enum_category_order_is_identity(self) -> None:
        values = ["a", "b"]
        ab = pl.DataFrame({"e": pl.Series(values, dtype=pl.Enum(["a", "b"]))})
        ba = pl.DataFrame({"e": pl.Series(values, dtype=pl.Enum(["b", "a"]))})
        assert _data_hash(ab) != _data_hash(ba)

    def test_enum_unused_categories_are_identity(self) -> None:
        """No row holds ``z``, yet ``Enum(a, b)`` and ``Enum(a, b, z)`` are different types."""
        values = ["a", "b"]
        ab = pl.DataFrame({"e": pl.Series(values, dtype=pl.Enum(["a", "b"]))})
        abz = pl.DataFrame({"e": pl.Series(values, dtype=pl.Enum(["a", "b", "z"]))})
        assert _data_hash(ab) != _data_hash(abz)

    def test_enum_series_categories_are_identity(self) -> None:
        values = ["a"]
        series = [
            pl.Series("e", values, dtype=pl.Enum(["a", "b"])),
            pl.Series("e", values, dtype=pl.Enum(["b", "a"])),
            pl.Series("e", values, dtype=pl.Enum(["a", "b", "z"])),
        ]
        assert _all_distinct(series)

    def test_string_categorical_and_enums_all_differ(self) -> None:
        values = ["a", "b"]
        frames = [
            pl.DataFrame({"c": pl.Series(values, dtype=dtype)})
            for dtype in (
                pl.String,
                pl.Categorical,
                pl.Enum(["a", "b"]),
                pl.Enum(["b", "a"]),
                pl.Enum(["a", "b", "c"]),
            )
        ]
        assert _all_distinct(frames)

    def test_string_and_binary(self) -> None:
        assert _data_hash(pl.DataFrame({"s": ["a"]})) != _data_hash(
            pl.DataFrame({"s": [b"a"]})
        )

    def test_decimal_precision_and_scale(self) -> None:
        values = [decimal.Decimal("1.25")]
        frames = [
            pl.DataFrame({"d": pl.Series(values, dtype=pl.Decimal(p, s))})
            for p, s in ((10, 2), (12, 2), (10, 3))
        ]
        assert _all_distinct(frames)

    def test_datetime_unit_and_time_zone(self) -> None:
        when = [dt.datetime(2020, 1, 1)]
        frames = [
            pl.DataFrame({"t": pl.Series(when, dtype=dtype)})
            for dtype in (
                pl.Datetime("us"),
                pl.Datetime("ms"),
                pl.Datetime("us", "UTC"),
                pl.Datetime("us", "Asia/Tokyo"),
            )
        ]
        assert _all_distinct(frames)

    def test_duration_unit(self) -> None:
        delta = [dt.timedelta(seconds=1)]
        frames = [
            pl.DataFrame({"d": pl.Series(delta, dtype=pl.Duration(unit))})
            for unit in ("ms", "us", "ns")
        ]
        assert _all_distinct(frames)

    def test_array_list_and_array_shape(self) -> None:
        frames = [
            pl.DataFrame({"v": pl.Series([[1, 2]], dtype=pl.List(pl.Int16))}),
            pl.DataFrame({"v": pl.Series([[1, 2]], dtype=pl.Array(pl.Int16, 2))}),
            pl.DataFrame(
                {"v": pl.Series([[[1, 2]]], dtype=pl.Array(pl.Int16, (1, 2)))}
            ),
            pl.DataFrame(
                {"v": pl.Series([[[1], [2]]], dtype=pl.Array(pl.Int16, (2, 1)))}
            ),
        ]
        assert _all_distinct(frames)

    def test_struct_field_dtype(self) -> None:
        frames = [
            pl.DataFrame({"s": pl.Series([{"a": 1}], dtype=pl.Struct({"a": inner}))})
            for inner in (pl.Int64, pl.Int32, pl.UInt8)
        ]
        assert _all_distinct(frames)

    def test_null_column_height_and_name(self) -> None:
        frames = [
            pl.DataFrame({"n": pl.Series([None, None], dtype=pl.Null)}),
            pl.DataFrame({"n": pl.Series([None, None, None], dtype=pl.Null)}),
            pl.DataFrame({"m": pl.Series([None, None], dtype=pl.Null)}),
        ]
        assert _all_distinct(frames)

    def test_null_and_all_null_columns(self) -> None:
        frames = [
            pl.DataFrame({"n": pl.Series([None], dtype=pl.Null)}),
            pl.DataFrame({"n": pl.Series([None], dtype=pl.Boolean)}),
            pl.DataFrame({"n": pl.Series([None], dtype=pl.String)}),
        ]
        assert _all_distinct(frames)

    def test_nested_null_list_lengths(self) -> None:
        frames = [
            pl.DataFrame({"l": pl.Series([[None]], dtype=pl.List(pl.Null))}),
            pl.DataFrame({"l": pl.Series([[None, None]], dtype=pl.List(pl.Null))}),
            pl.DataFrame({"l": pl.Series([[None]], dtype=pl.List(pl.Boolean))}),
        ]
        assert _all_distinct(frames)

    def test_empty_frames_with_different_schemas(self) -> None:
        frames = [
            pl.DataFrame(schema={"x": pl.Int64}),
            pl.DataFrame(schema={"x": pl.Int32}),
            pl.DataFrame(schema={"y": pl.Int64}),
            pl.DataFrame(),
        ]
        assert _all_distinct(frames)


class TestSeriesHashing:
    def test_equal_series_hash_equal(self) -> None:
        s = pl.Series("m", [1, None, 3], dtype=pl.Int16)
        chunked = pl.concat([s.slice(0, 1), s.slice(1, 2)], rechunk=False)
        assert _data_hash(chunked) == _data_hash(s)

    def test_name_dtype_and_values_all_matter(self) -> None:
        series = [
            pl.Series("m", [1, 2], dtype=pl.Int16),
            pl.Series("n", [1, 2], dtype=pl.Int16),
            pl.Series("m", [1, 2], dtype=pl.Int32),
            pl.Series("m", [1, 3], dtype=pl.Int16),
            pl.Series("", [1, 2], dtype=pl.Int16),
        ]
        assert _all_distinct(series)

    def test_series_and_one_column_frame_differ(self) -> None:
        s = pl.Series("m", [1, 2])
        assert _data_hash(s) != _data_hash(s.to_frame())


class TestHashLabel:
    """Each label names its algorithm and is defined once, beside it (the hash label rule)."""

    def test_dataframe_hash_label(self) -> None:
        hasher = get_default_context().semantic_hasher
        frame_hash = hasher.hash_object(pl.DataFrame({"a": [1]}))
        assert frame_hash.method == POLARS_DATAFRAME_HASH_LABEL == "polars_dataframe_v1"

    def test_series_hash_label(self) -> None:
        hasher = get_default_context().semantic_hasher
        series_hash = hasher.hash_object(pl.Series("a", [1]))
        assert series_hash.method == POLARS_SERIES_HASH_LABEL == "polars_series_v1"


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


class TestHashDefinition:
    """The hash is ``sha256(descriptor + NUL + starfix ordered digest of the values)``.

    Recomputed here from the documented definition for values whose Arrow export needs no
    projection (no ``Null``, ``Array`` or dictionary column). starfix runs in its ordered
    mode (column order and struct field order, ITL-898).
    """

    _STRUCT = pl.Struct({"u": pl.Int8, "v": pl.String})

    def _frame(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "x": pl.Series([1, 2, 3], dtype=pl.Int64),
                "s": pl.Series(
                    [{"u": 1, "v": "a"}, None, {"u": None, "v": None}],
                    dtype=self._STRUCT,
                ),
            }
        )

    @staticmethod
    def _ordered_digest(frame: pl.DataFrame) -> bytes:
        return ArrowDigester.hash_table(
            frame.to_arrow(compat_level=pl.CompatLevel.oldest()),
            ordered_columns=True,
            ordered_struct_fields=True,
        )

    def test_frame(self) -> None:
        frame = self._frame()
        descriptor = _canonical_json(
            {
                "kind": "polars.dataframe",
                "height": 3,
                "columns": [
                    ["x", "int64"],
                    ["s", {"struct": [["u", "int8"], ["v", "string"]]}],
                ],
            }
        )
        expected = hashlib.sha256(
            descriptor + b"\x00" + self._ordered_digest(frame)
        ).digest()
        got = get_default_context().semantic_hasher.hash_object(frame)
        assert got == ContentHash(POLARS_DATAFRAME_HASH_LABEL, expected)

    def test_series(self) -> None:
        series = self._frame()["s"]
        descriptor = _canonical_json(
            {
                "kind": "polars.series",
                "name": "s",
                "height": 3,
                "dtype": {"struct": [["u", "int8"], ["v", "string"]]},
            }
        )
        expected = hashlib.sha256(
            descriptor + b"\x00" + self._ordered_digest(series.to_frame())
        ).digest()
        got = get_default_context().semantic_hasher.hash_object(series)
        assert got == ContentHash(POLARS_SERIES_HASH_LABEL, expected)


class TestLogicalTypeIdentifier:
    """The identifier is the extension name plus ``{"version":1}`` (ITL-935)."""

    @pytest.mark.parametrize(
        ("python_type", "name"),
        [(pl.DataFrame, "polars.dataframe"), (pl.Series, "polars.series")],
    )
    def test_identifier(self, python_type: type, name: str) -> None:
        converter = get_default_context().type_converter
        arrow_type = converter.python_type_to_arrow_type(python_type)
        assert isinstance(arrow_type, pa.ExtensionType)
        assert arrow_type_identifier(arrow_type) == LogicalTypeIdentifier(
            name, b'{"version":1}'
        )

    @pytest.mark.parametrize(
        "logical_type", [LogicalPolarsDataFrame(), LogicalPolarsSeries()]
    )
    def test_polars_extension_carries_the_same_metadata(
        self, logical_type: LogicalPolarsDataFrame | LogicalPolarsSeries
    ) -> None:
        polars_ext = logical_type.get_polars_extension_type()
        assert polars_ext.ext_metadata() == '{"version":1}'

    @pytest.mark.parametrize("rows", [0, 2], ids=["zero_rows", "all_null"])
    def test_column_without_values_hashes_as_its_type(self, rows: int) -> None:
        """A column holding no value still hashes as its logical type (ITL-967).

        Such a column has no cell token, so the type enters through the hashed field.
        """
        context = get_default_context()

        def column_hash(arrow_type: pa.DataType) -> str:
            storage_type = (
                arrow_type.storage_type
                if isinstance(arrow_type, pa.ExtensionType)
                else arrow_type
            )
            column: pa.Array[Any] = pa.array([None] * rows, storage_type)
            if isinstance(arrow_type, pa.ExtensionType):
                column = pa.ExtensionArray.from_storage(arrow_type, column)
            return context.arrow_hasher.hash_table(pa.table({"c": column})).to_string()

        converter = context.type_converter
        hashes = [
            column_hash(converter.python_type_to_arrow_type(python_type))
            for python_type in (pl.DataFrame, pl.Series, pd.DataFrame, pd.Series)
        ]
        hashes.append(column_hash(pa.large_binary()))
        assert len(set(hashes)) == len(hashes)


class TestHashTimeRejection:
    def test_object_column_is_rejected_naming_the_column(self) -> None:
        df = pl.DataFrame(
            [pl.Series("ok", [1]), pl.Series("obj", [object()], dtype=pl.Object)]
        )
        hasher = get_default_context().semantic_hasher
        with pytest.raises(ValueError, match=r"'obj'.*Object"):
            hasher.hash_object(df)

    def test_series_outside_the_grammar_is_rejected_naming_it(self) -> None:
        series = pl.Series("big", [[1]], dtype=pl.List(pl.Int128))
        hasher = get_default_context().semantic_hasher
        with pytest.raises(ValueError, match=r"'big'.*Int128"):
            hasher.hash_object(series)


_CROSS_PROCESS_SCRIPT = textwrap.dedent(
    """
    import datetime as dt, json
    import polars as pl
    from orcapod.sources import DictSource

    df = pl.DataFrame({
        "i": pl.Series([1, None, 3], dtype=pl.Int8),
        "e": pl.Series(["a", "b", None], dtype=pl.Enum(["b", "a"])),
        "t": pl.Series([dt.datetime(2020, 1, 1)] * 3, dtype=pl.Datetime("ms", "UTC")),
        "c": pl.Series(["y", None, "x"], dtype=pl.Categorical),
    })
    source = DictSource([{"k": "a", "v": df}], tag_columns=["k"],
                        data_schema={"k": str, "v": pl.DataFrame})
    ((_, data),) = list(source.iter_data())
    print(json.dumps({"hash": data.content_hash().to_hex()}))
    """
)


@pytest.mark.parametrize("seed", ["0", "12345"])
def test_hash_is_identical_across_processes(seed: str) -> None:
    env = {**os.environ, "PYTHONHASHSEED": seed}
    runs = [
        subprocess.run(
            [sys.executable, "-c", _CROSS_PROCESS_SCRIPT],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
        for _ in range(2)
    ]
    for run in runs:
        assert run.returncode == 0, run.stderr
    first, second = (json.loads(run.stdout.strip().splitlines()[-1]) for run in runs)
    assert first == second

    in_process = _data_hash(
        pl.DataFrame(
            {
                "i": pl.Series([1, None, 3], dtype=pl.Int8),
                "e": pl.Series(["a", "b", None], dtype=pl.Enum(["b", "a"])),
                "t": pl.Series(
                    [dt.datetime(2020, 1, 1)] * 3, dtype=pl.Datetime("ms", "UTC")
                ),
                "c": pl.Series(["y", None, "x"], dtype=pl.Categorical),
            }
        )
    )
    assert first["hash"] == in_process


class TestDescriptorGrammar:
    """The descriptor is Orcapod's own grammar; pin it so it cannot drift silently."""

    def test_nested_dtype_encoding(self) -> None:
        dtype = pl.Struct(
            {
                "e": pl.Enum(["y", "x"]),
                "a": pl.Array(pl.Int16, (2, 3)),
                "t": pl.List(pl.Datetime("ms", "UTC")),
                "m": pl.Decimal(10, 2),
                "d": pl.Duration("ns"),
                "c": pl.Categorical,
                "n": pl.Null,
            }
        )
        assert polars_dtype_descriptor(dtype) == {
            "struct": [
                ["e", {"enum": ["y", "x"]}],
                ["a", {"array": [{"array": ["int16", 3]}, 2]}],
                ["t", {"list": {"datetime": ["ms", "UTC"]}}],
                ["m", {"decimal": [10, 2]}],
                ["d", {"duration": "ns"}],
                ["c", {"categorical": ["", "", "uint32"]}],
                ["n", "null"],
            ]
        }

    def test_categorical_categories_encoding(self) -> None:
        dtype = pl.Categorical(pl.Categories("units", "lab", pl.UInt8))
        assert polars_dtype_descriptor(dtype) == {
            "categorical": ["units", "lab", "uint8"]
        }

    def test_dtype_outside_the_grammar_names_the_dtype(self) -> None:
        with pytest.raises(ValueError, match=r"Int128"):
            polars_dtype_descriptor(pl.List(pl.Int128))
