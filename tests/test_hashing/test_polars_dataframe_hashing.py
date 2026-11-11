"""Content hashing of ``pl.DataFrame`` and ``pl.Series`` values (ITL-578).

The hash of a frame-valued data column is a cache key. It depends on the frame's logical
content: its values, row count, and each column's name and dtype (in Orcapod's own dtype
grammar, see ``orcapod.logical_types.polars_type``). Like every Orcapod table hash it does not
depend on column order or struct field order. It does not depend on how the frame happens to
be laid out in memory (chunking, slice offsets, the bytes under null slots), and it is the same
in every process.

Hashes are taken through the public path a cache lookup uses: the ``content_hash()`` of the
``Data`` a frame-valued source yields.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase
from orcapod.logical_types.polars_type import polars_dtype_descriptor
from orcapod.sources import DictSource


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

    def test_column_order_does_not_matter(self) -> None:
        """Column order is not identity, as for every Orcapod table hash (starfix)."""
        assert _data_hash(_BASE.select("y", "x")) == _data_hash(_BASE)

    def test_struct_field_order_does_not_matter(self) -> None:
        ab = pl.DataFrame(
            {
                "s": pl.Series(
                    [{"a": 1, "b": "x"}],
                    dtype=pl.Struct({"a": pl.Int64, "b": pl.String}),
                )
            }
        )
        ba = pl.DataFrame(
            {
                "s": pl.Series(
                    [{"b": "x", "a": 1}],
                    dtype=pl.Struct({"b": pl.String, "a": pl.Int64}),
                )
            }
        )
        assert _data_hash(ab) == _data_hash(ba)

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


class TestHashTimeRejection:
    def test_object_column_is_rejected_naming_the_column(self) -> None:
        df = pl.DataFrame(
            [pl.Series("ok", [1]), pl.Series("obj", [object()], dtype=pl.Object)]
        )
        hasher = get_default_context().semantic_hasher
        with pytest.raises(ValueError, match=r"'obj'.*Object"):
            hasher.hash_object(df)


_CROSS_PROCESS_SCRIPT = textwrap.dedent(
    """
    import datetime as dt, json
    import polars as pl
    from orcapod.sources import DictSource

    df = pl.DataFrame({
        "i": pl.Series([1, None, 3], dtype=pl.Int8),
        "e": pl.Series(["a", "b", None], dtype=pl.Enum(["b", "a"])),
        "t": pl.Series([dt.datetime(2020, 1, 1)] * 3, dtype=pl.Datetime("ms", "UTC")),
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
        assert polars_dtype_descriptor(dtype, "col") == {
            "struct": {
                "e": {"enum": ["y", "x"]},
                "a": {"array": [{"array": ["int16", 3]}, 2]},
                "t": {"list": {"datetime": ["ms", "UTC"]}},
                "m": {"decimal": [10, 2]},
                "d": {"duration": "ns"},
                "c": "categorical",
                "n": "null",
            }
        }

    def test_dtype_outside_the_grammar_names_the_column(self) -> None:
        with pytest.raises(ValueError, match=r"'col'.*Int128"):
            polars_dtype_descriptor(pl.List(pl.Int128), "col")


def _golden_values() -> dict[str, pl.DataFrame | pl.Series]:
    return {
        "simple": pl.DataFrame(
            {"x": pl.Series([1, 2, 3], dtype=pl.Int64), "y": ["a", "b", "c"]}
        ),
        "nulls_and_temporal": pl.DataFrame(
            {
                "i": pl.Series([1, None, -3], dtype=pl.Int8),
                "t": pl.Series(
                    [dt.datetime(2020, 1, 1, 12), None, dt.datetime(1999, 12, 31)],
                    dtype=pl.Datetime("ms", "America/New_York"),
                ),
                "d": pl.Series(
                    [dt.timedelta(seconds=1), dt.timedelta(0), None],
                    dtype=pl.Duration("us"),
                ),
                "m": pl.Series(
                    [decimal.Decimal("1.25"), None, decimal.Decimal("-0.50")],
                    dtype=pl.Decimal(10, 2),
                ),
            }
        ),
        "nested_and_categorical": pl.DataFrame(
            {
                "e": pl.Series(["x", "y", None], dtype=pl.Enum(["y", "x", "z"])),
                "c": pl.Series(["p", None, "q"], dtype=pl.Categorical),
                "a": pl.Series([[1, 2], [3, 4], None], dtype=pl.Array(pl.Int16, 2)),
                "s": pl.Series(
                    [{"u": 1, "l": [1.5]}, None, {"u": None, "l": []}],
                    dtype=pl.Struct({"u": pl.UInt16, "l": pl.List(pl.Float32)}),
                ),
                "n": pl.Series([None, None, None], dtype=pl.Null),
            }
        ),
        "empty_with_schema": pl.DataFrame(schema={"x": pl.Int32, "b": pl.Binary}),
        "series": pl.Series("metric", [1.5, None, 3.0], dtype=pl.Float32),
    }


# Pinned on November 10, 2026 (polars 1.41.2, pyarrow 23.0.1, starfix 0.4.0). These are cache
# keys: a change here means every stored polars value misses the cache. Only change them on
# purpose, together with a bump of POLARS_HASH_GRAMMAR_VERSION or an announced hasher change.
# "value" is the semantic handler's hash of the object; "data" is the content hash of the
# ``Data`` holding it in a column named "v". Kept in this module because the shared golden
# suite (tests/test_hashing/golden/, ITL-626) is not merged yet.
_GOLDEN = {
    "simple": {
        "value": "306917e01124956b9202e9ca4b79aa55b015232916ea9914e9e98917d4660f94",
        "data": "000001acd418e856020bb0c09b22b362b2603236b6322e0b0bc836a054fdc615117547",
    },
    "nulls_and_temporal": {
        "value": "a9c8bae9d486c9963fdbd10a3e5d9cb01d9e6090da681061ea43a158fab7ee93",
        "data": "000001057fd462f8c4269cd6619489fe760d8f103640eba3571754b63aab5e4feb58d6",
    },
    "nested_and_categorical": {
        "value": "21b78b7721071a3abf265525be7cd93034eb223cc431d11c58671eca4af7c091",
        "data": "00000162d3242f6fc32246dc58dddf75a8343ac3e359cc496cbb926efd6e0b7145a80a",
    },
    "empty_with_schema": {
        "value": "a25fc4db020721a983e893d64a212d7077e7b3d7396db5fcd10e9a9af6abfff3",
        "data": "0000019b9251c5bf4d2fc6d232adb6290498aa4ca4a23f761cafc55c42d2cf59352375",
    },
    "series": {
        "value": "7e4547f5f8bdf21350763f0bb2ba461f9b53a603af95df7fc9c4d2c7a2fd6018",
        "data": "000001a6b6e2ced3ebca541ac63f50c8d75154075ff8bbe3e49370b48d9a9f2f5686cc",
    },
}


@pytest.mark.parametrize("name", list(_GOLDEN))
def test_golden_hashes(name: str) -> None:
    value = _golden_values()[name]
    hasher = get_default_context().semantic_hasher
    assert hasher.hash_object(value).to_hex() == _GOLDEN[name]["value"]
    assert _data_hash(value) == _GOLDEN[name]["data"]
