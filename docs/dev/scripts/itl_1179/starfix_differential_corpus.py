"""Differential corpus: every starfix call Orcapod makes, over a broad set of Arrow inputs.

ITL-1179 stage 1, proof B. Run it once with starfix 0.5.0 (starfix-python) and once with
starfix 0.6.0 (the Rust crate's binding), then diff the two outputs with
``compare_corpus_runs.py``. The corpus is deterministic: no randomness, no clock, no temporary
path (the files a ``File`` / ``Directory`` cell names live under ``corpus_files/`` beside this
script).

Each entry is ``<case>|<call>`` mapped to the hex digest, or to
``raise:<exception class>:<message>`` when the call raises. The calls:

* ``sf.hash_table[m,c,s]``: ``starfix.ArrowDigester.hash_table`` with ``include_metadata``,
  ``ordered_columns`` and ``ordered_struct_fields`` (all eight combinations; Orcapod uses
  ``m`` both ways, ``c,s`` = ``0,0``, ``1,1`` and ``1,0``);
* ``sf.hash_schema[m]``: ``ArrowDigester.hash_schema`` with ``include_metadata``;
* ``op.hash_table``, ``op.hash_table_ordered``, ``op.hash_table_ordered_nostruct``,
  ``op.hash_schema``: Orcapod's ``StarfixArrowHasher`` (the semantic preprocessing, extension
  identity and the starfix call as Orcapod makes it);
* ``op.value``: the semantic hasher's value hash of a library value (numpy, pandas, polars,
  torch), which goes through starfix for pandas and polars.

Usage (one process at a time)::

    uv run python docs/dev/scripts/itl_1179/starfix_differential_corpus.py OUT.json
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import decimal
import importlib.metadata
import json
import math
import struct
import sys
import uuid
import warnings
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
from starfix import ArrowDigester

import orcapod

HERE = Path(__file__).resolve().parent
FILES = HERE / "corpus_files"

warnings.filterwarnings("ignore")


# --- float values: no -0.0 and only the ordinary NaN (stage 1) -----------------------------


def _f64_from_bits(bits: int) -> float:
    return struct.unpack("<d", struct.pack("<Q", bits))[0]


F64 = [
    0.0,
    1.0,
    -1.0,
    1.5,
    math.pi,
    -math.e,
    1e308,
    -1e308,
    2.2250738585072014e-308,  # smallest normal
    _f64_from_bits(1),  # smallest subnormal
    _f64_from_bits(0x000FFFFFFFFFFFFF),  # largest subnormal
    math.inf,
    -math.inf,
    math.nan,  # 0x7ff8000000000000, the ordinary NaN
]
F32 = [
    0.0,
    1.0,
    -1.0,
    0.5,
    3.4028234663852886e38,
    1.401298464324817e-45,
    math.inf,
    -math.inf,
    math.nan,
]
F16 = [0.0, 1.0, -2.0, 65504.0, 5.960464477539063e-08, math.inf, -math.inf, math.nan]


def _f16_array(values: list[float | None]) -> pa.Array[Any]:
    # pyarrow builds float16 from numpy float16 values (np.float16 NaN is 0x7e00).
    arr = np.array([0.0 if v is None else v for v in values], dtype=np.float16)
    mask = np.array([v is None for v in values])
    return pa.array(arr, type=pa.float16(), mask=mask)


def _tbl(columns: Mapping[str, Any]) -> pa.Table:
    """A table of the given columns (arrays or chunked arrays), in the given order."""
    return pa.Table.from_arrays(list(columns.values()), names=list(columns))


# --- leaf arrays --------------------------------------------------------------------------


def leaf_arrays() -> dict[str, pa.Array[Any]]:
    """One array per leaf type, nulls included where the type allows."""
    out: dict[str, pa.Array[Any]] = {}
    out["bool"] = pa.array([True, False, None, True], pa.bool_())
    for bits in (8, 16, 32, 64):
        lo, hi = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1
        out[f"int{bits}"] = pa.array([0, lo, hi, None, -1], getattr(pa, f"int{bits}")())
        out[f"uint{bits}"] = pa.array(
            [0, 2**bits - 1, None, 1], getattr(pa, f"uint{bits}")()
        )
    out["float64"] = pa.array([*F64, None], pa.float64())
    out["float32"] = pa.array([*F32, None], pa.float32())
    out["float16"] = _f16_array([*F16, None])
    out["decimal128_5_2"] = pa.array(
        [decimal.Decimal("123.45"), decimal.Decimal("-0.01"), None], pa.decimal128(5, 2)
    )
    out["decimal128_38_10"] = pa.array(
        [decimal.Decimal("1234567890123456789012345678.0123456789"), None],
        pa.decimal128(38, 10),
    )
    out["decimal256_60_5"] = pa.array(
        [
            decimal.Decimal(
                "-12345678901234567890123456789012345678901234567890.12345"
            ),
            None,
        ],
        pa.decimal256(60, 5),
    )
    out["date32"] = pa.array(
        [dt.date(1970, 1, 1), dt.date(2026, 11, 19), None, dt.date(1, 1, 1)],
        pa.date32(),
    )
    out["date64"] = pa.array(
        [dt.date(1970, 1, 1), dt.date(2026, 11, 19), None], pa.date64()
    )
    out["time32_s"] = pa.array([0, 86399, None], pa.time32("s"))
    out["time32_ms"] = pa.array([0, 86399999, None], pa.time32("ms"))
    out["time64_us"] = pa.array([0, 86399999999, None], pa.time64("us"))
    out["time64_ns"] = pa.array([0, 86399999999999, None], pa.time64("ns"))
    for unit in ("s", "ms", "us", "ns"):
        for tz in (None, "UTC", "America/New_York", "+05:30"):
            name = (
                f"timestamp_{unit}_{tz or 'naive'}".replace("/", "_")
                .replace("+", "p")
                .replace(":", "")
            )
            out[name] = pa.array(
                [0, 1_700_000_000, None, -5],
                pa.timestamp(unit, tz) if tz is not None else pa.timestamp(unit),
            )
        out[f"duration_{unit}"] = pa.array([0, -3, 1_000_000, None], pa.duration(unit))
    out["string"] = pa.array(["", "a", "héllo", None, "a/b", "x\x00y"], pa.string())
    out["large_string"] = pa.array(["", "a", "héllo", None, "a/b"], pa.large_string())
    out["binary"] = pa.array([b"", b"\x00\xff", None, b"abc"], pa.binary())
    out["large_binary"] = pa.array([b"", b"\x00\xff", None, b"abc"], pa.large_binary())
    out["fixed_size_binary_4"] = pa.array(
        [b"abcd", None, b"\x00\x00\x00\x00"], pa.binary(4)
    )
    out["fixed_size_binary_0"] = pa.array([b"", None, b""], pa.binary(0))
    return out


def nested_arrays() -> dict[str, pa.Array[Any]]:
    out: dict[str, pa.Array[Any]] = {}
    out["list_int64"] = pa.array([[1, 2], [], None, [None, 3]], pa.list_(pa.int64()))
    out["large_list_string"] = pa.array(
        [["a"], [], None, [None, "b"]], pa.large_list(pa.string())
    )
    out["list_list_float64"] = pa.array(
        [[[1.0, math.nan], []], None, [None, [math.inf]]],
        pa.list_(pa.list_(pa.float64())),
    )
    out["large_list_large_list_int8"] = pa.array(
        [[[1], [2, 3]], [], None], pa.large_list(pa.large_list(pa.int8()))
    )
    out["list_nonnull_items"] = pa.array(
        [[1, 2], [], None], pa.list_(pa.field("item", pa.int32(), nullable=False))
    )
    out["fsl_int64_3"] = pa.array(
        [[1, 2, 3], None, [4, None, 6]], pa.list_(pa.int64(), 3)
    )
    out["fsl_0"] = pa.array([[], None, []], pa.list_(pa.int64(), 0))
    out["fsl_fsl_float32"] = pa.array(
        [[[1.0, 2.0], [3.0, 4.0]], None, [[math.nan, 0.5], None]],
        pa.list_(pa.list_(pa.float32(), 2), 2),
    )
    out["list_fsl_float16"] = pa.ListArray.from_arrays(
        pa.array([0, 1, 1], pa.int32()),
        pa.FixedSizeListArray.from_arrays(_f16_array([1.0, 2.0]), 2),
    )
    struct_t = pa.struct([("a", pa.int64()), ("b", pa.string())])
    out["struct_ab"] = pa.array(
        [{"a": 1, "b": "x"}, None, {"a": None, "b": None}], struct_t
    )
    struct_ba = pa.struct([("b", pa.string()), ("a", pa.int64())])
    out["struct_ba"] = pa.array(
        [{"b": "x", "a": 1}, None, {"b": None, "a": None}], struct_ba
    )
    nested_struct = pa.struct(
        [
            ("outer", pa.struct([("y", pa.float64()), ("x", pa.list_(pa.int16()))])),
            ("tag", pa.large_string()),
        ]
    )
    out["struct_nested"] = pa.array(
        [
            {"outer": {"y": 1.5, "x": [1, 2]}, "tag": "t"},
            {"outer": None, "tag": None},
            None,
            {"outer": {"y": None, "x": None}, "tag": "u"},
        ],
        nested_struct,
    )
    out["struct_nonnull_child"] = pa.array(
        [{"a": 1}, None], pa.struct([pa.field("a", pa.int64(), nullable=False)])
    )
    out["struct_empty"] = pa.StructArray.from_arrays(
        [], fields=[], mask=pa.array([False, True, False])
    )
    out["list_struct"] = pa.array(
        [[{"a": 1, "b": "x"}, None], [], None], pa.list_(struct_t)
    )
    out["struct_list_struct"] = pa.array(
        [{"l": [{"p": 1.0}, {"p": None}]}, {"l": None}],
        pa.struct([("l", pa.list_(pa.struct([("p", pa.float32())])))]),
    )
    return out


def dictionary_arrays() -> dict[str, pa.Array[Any]]:
    out: dict[str, pa.Array[Any]] = {}
    values: dict[str, pa.Array[Any]] = {
        "string": pa.array(["a", "b", "c"]),
        "large_string": pa.array(["a", "b"], pa.large_string()),
        "int64": pa.array([10, 20, 30], pa.int64()),
        "float64": pa.array([1.5, math.nan, math.inf]),
        "float32": pa.array([0.5, -1.0], pa.float32()),
        "binary": pa.array([b"x", b"y"], pa.binary()),
        "date32": pa.array([dt.date(2020, 1, 1), dt.date(2021, 1, 1)], pa.date32()),
        "decimal": pa.array(
            [decimal.Decimal("1.10"), decimal.Decimal("2.20")], pa.decimal128(4, 2)
        ),
        "list_int": pa.array([[1, 2], [], None], pa.list_(pa.int64())),
        "struct": pa.array([{"a": 1}, {"a": None}], pa.struct([("a", pa.int64())])),
        "fsl": pa.array([[1, 2], [3, 4]], pa.list_(pa.int64(), 2)),
        "fsl_fsl": pa.array(
            [[[1], [2]], [[3], [4]]], pa.list_(pa.list_(pa.int64(), 1), 2)
        ),
    }
    for index_type in (pa.int8(), pa.int16(), pa.int32(), pa.uint8()):
        for vname, varr in values.items():
            n = len(varr)
            keys: Any = pa.array([0, n - 1, None, 0], index_type)
            out[f"dict_{index_type}_{vname}"] = pa.DictionaryArray.from_arrays(
                keys, varr
            )
    # A dictionary of dictionaries.
    inner = pa.DictionaryArray.from_arrays(
        pa.array([1, 0], pa.int8()), pa.array(["p", "q"])
    )
    out["dict_of_dict"] = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 1], pa.int16()), inner
    )
    # The same logical values, dictionary-encoded and plain, must hash alike.
    out["dict_vs_plain_dict"] = pa.array(["a", "b", "a", None]).dictionary_encode()
    out["dict_vs_plain_plain"] = pa.array(["a", "b", "a", None])
    # Sliced dictionary (offset on the keys).
    out["dict_sliced"] = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 2, 1, 0], pa.int32()), pa.array(["a", "b", "c"])
    ).slice(1, 3)
    # Dictionary in a list and in a struct.
    d = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 0], pa.int32()), pa.array(["u", "v"])
    )
    out["list_of_dict"] = pa.ListArray.from_arrays(pa.array([0, 2, 3], pa.int32()), d)
    out["struct_of_dict"] = pa.StructArray.from_arrays([d], names=["d"])
    return out


def sliced_arrays() -> dict[str, pa.Array[Any]]:
    out: dict[str, pa.Array[Any]] = {}
    base_int = pa.array([1, None, 3, 4, 5, None, 7], pa.int64())
    out["int_slice_2_4"] = base_int.slice(2, 4)
    out["bool_slice_3"] = pa.array(
        [True, False, None, True, True, False, None, True, False]
    ).slice(3)
    out["string_slice_1_2"] = pa.array(["aa", "b", None, "ccc"]).slice(1, 2)
    out["list_slice"] = pa.array(
        [[1], [2, 3], None, [4, 5, 6], []], pa.list_(pa.int64())
    ).slice(1, 3)
    out["large_list_slice"] = pa.array(
        [[1], [2, 3], None, [4]], pa.large_list(pa.int8())
    ).slice(2)
    out["fsl_slice"] = pa.array(
        [[1, 2], [3, 4], None, [5, 6]], pa.list_(pa.int64(), 2)
    ).slice(1, 2)
    out["struct_slice"] = pa.array(
        [{"a": 1, "b": "x"}, {"a": 2, "b": None}, None, {"a": 4, "b": "z"}],
        pa.struct([("a", pa.int64()), ("b", pa.string())]),
    ).slice(1, 3)
    out["struct_in_sliced_list"] = pa.array(
        [[{"a": 1}], [{"a": 2}, None], [{"a": 3}]],
        pa.list_(pa.struct([("a", pa.int64())])),
    ).slice(1)
    out["float_slice"] = pa.array([*F64], pa.float64()).slice(3, 7)
    return out


# --- tables ---------------------------------------------------------------------------------


def _table(columns: dict[str, Any], metadata: dict[str, str] | None = None) -> pa.Table:
    return _tbl(columns).replace_schema_metadata(metadata)


def tables() -> dict[str, pa.Table]:
    out: dict[str, pa.Table] = {}
    groups = {
        "leaf": leaf_arrays(),
        "nested": nested_arrays(),
        "dict": dictionary_arrays(),
        "sliced": sliced_arrays(),
    }
    for gname, arrays in groups.items():
        for name, arr in arrays.items():
            out[f"{gname}.{name}"] = _tbl({"c": arr})
            out[f"{gname}.{name}.with_id"] = _tbl(
                {"id": pa.array(range(len(arr)), pa.int32()), "c": arr}
            )
    # Several columns, both column orders; schema-level and field-level metadata.
    leaf = leaf_arrays()
    a, b = leaf["int32"].slice(0, 3), leaf["string"].slice(0, 3)
    out["multi.ab"] = _tbl({"a": a, "b": b})
    out["multi.ba"] = _tbl({"b": b, "a": a})
    out["multi.ab.schema_meta"] = _table({"a": a, "b": b}, {"k": "v", "z": "1"})
    out["multi.ab.field_meta"] = pa.Table.from_arrays(
        [a, b],
        schema=pa.schema(
            [pa.field("a", a.type, metadata={"unit": "s"}), pa.field("b", b.type)]
        ),
    )
    out["multi.nonnull_field"] = pa.Table.from_arrays(
        [pa.array([1, 2], pa.int64())],
        schema=pa.schema([pa.field("x", pa.int64(), nullable=False)]),
    )
    # Zero rows and zero columns.
    for name in ("int64", "float32", "string", "timestamp_us_UTC"):
        arr = leaf[name]
        out[f"zero_rows.{name}"] = _tbl({"c": arr.slice(0, 0)})
    out["zero_rows.nested"] = _tbl({"c": nested_arrays()["struct_nested"].slice(0, 0)})
    out["zero_rows.dict"] = _tbl(
        {"c": dictionary_arrays()["dict_int32_string"].slice(0, 0)}
    )
    out["zero_columns.3_rows"] = _tbl({"c": pa.array([1, 2, 3])}).drop_columns(["c"])
    out["zero_columns.0_rows"] = _tbl({})
    # Chunkings of one column.
    values = pa.array([1.5, None, math.inf, 2.5, math.nan, -3.0, 7.0], pa.float64())
    for chunking in ((7,), (1, 6), (3, 4), (2, 2, 3), (0, 7), (1, 1, 1, 1, 1, 1, 1)):
        chunks, pos = [], 0
        for size in chunking:
            chunks.append(values.slice(pos, size))
            pos += size
        out[f"chunked.{'-'.join(map(str, chunking))}"] = _tbl(
            {"c": pa.chunked_array(chunks, pa.float64())}
        )
    strs = pa.array(["a", None, "bc", "", "d"])
    out["chunked.strings.1"] = _tbl({"s": pa.chunked_array([strs])})
    out["chunked.strings.2_3"] = _tbl(
        {"s": pa.chunked_array([strs.slice(0, 2), strs.slice(2)])}
    )
    lists = pa.array([[1], None, [2, 3], []], pa.list_(pa.int64()))
    out["chunked.lists.1"] = _tbl({"l": pa.chunked_array([lists])})
    out["chunked.lists.1_1_2"] = _tbl(
        {"l": pa.chunked_array([lists.slice(0, 1), lists.slice(1, 1), lists.slice(2)])}
    )
    dicts = dictionary_arrays()["dict_int32_string"]
    out["chunked.dict.split"] = _tbl(
        {"d": pa.chunked_array([dicts.slice(0, 2), dicts.slice(2)])}
    )
    out["chunked.dict.whole"] = _tbl({"d": pa.chunked_array([dicts])})
    # pyarrow canonical extension types.
    out["ext.arrow_uuid"] = _tbl(
        {
            "c": pa.ExtensionArray.from_storage(
                pa.uuid(), pa.array([b"0123456789abcdef", None], pa.binary(16))
            )
        }
    )
    out["ext.fixed_shape_tensor"] = _tbl(
        {
            "c": pa.ExtensionArray.from_storage(
                pa.fixed_shape_tensor(pa.float64(), [2, 2]),
                pa.array([[1.0, 2.0, 3.0, math.nan], None], pa.list_(pa.float64(), 4)),
            )
        }
    )
    out["ext.bool8"] = _tbl(
        {
            "c": pa.ExtensionArray.from_storage(
                pa.bool8(), pa.array([1, 0, None], pa.int8())
            )
        }
    )
    out["ext.json"] = _tbl(
        {
            "c": pa.ExtensionArray.from_storage(
                pa.json_(), pa.array(['{"a":1}', None], pa.string())
            )
        }
    )
    return out


def refused_tables() -> dict[str, Callable[[], pa.Table]]:
    """Inputs starfix refuses; the class and message are compared."""
    return {
        "refused.null_type": lambda: _tbl({"c": pa.array([None, None], pa.null())}),
        "refused.null_type_zero_rows": lambda: _tbl({"c": pa.array([], pa.null())}),
        "refused.list_of_null": lambda: _tbl(
            {"c": pa.array([[None]], pa.list_(pa.null()))}
        ),
        "refused.map": lambda: _tbl(
            {"c": pa.array([[("a", 1)]], pa.map_(pa.string(), pa.int64()))}
        ),
        "refused.list_view": lambda: _tbl(
            {"c": pa.array([[1, 2]], pa.list_view(pa.int64()))}
        ),
        "refused.large_list_view": lambda: _tbl(
            {"c": pa.array([[1]], pa.large_list_view(pa.int64()))}
        ),
        "refused.string_view": lambda: _tbl({"c": pa.array(["a"], pa.string_view())}),
        "refused.binary_view": lambda: _tbl({"c": pa.array([b"a"], pa.binary_view())}),
        "refused.interval_mdn": lambda: _tbl(
            {"c": pa.array([(1, 2, 3)], pa.month_day_nano_interval())}
        ),
        "refused.dense_union": lambda: _tbl(
            {
                "c": pa.UnionArray.from_dense(
                    pa.array([0, 1], pa.int8()),
                    pa.array([0, 0], pa.int32()),
                    [pa.array([1]), pa.array(["a"])],
                )
            }
        ),
        "refused.run_end_encoded": lambda: _tbl(
            {
                "c": pa.RunEndEncodedArray.from_arrays(
                    pa.array([2, 3], pa.int32()), pa.array([1, 2])
                )
            }
        ),
        "refused.duplicate_names": lambda: pa.Table.from_arrays(
            [pa.array([1]), pa.array([2])], names=["x", "x"]
        ),
        "refused.ambiguous_slash": lambda: _tbl(
            {
                "a/b": pa.array([1]),
                "a": pa.array([{"b": 2}], pa.struct([("b", pa.int64())])),
            }
        ),
        "refused.ambiguous_slash_nested": lambda: _tbl(
            {
                "s": pa.array(
                    [{"a/b": 1, "a": {"b": 2}}],
                    pa.struct(
                        [("a/b", pa.int64()), ("a", pa.struct([("b", pa.int64())]))]
                    ),
                )
            }
        ),
        "refused.nonnull_with_null": lambda: pa.Table.from_arrays(
            [pa.array([1, None], pa.int64())],
            schema=pa.schema([pa.field("x", pa.int64(), nullable=False)]),
        ),
        "refused.dict_of_map": lambda: _tbl(
            {
                "c": pa.DictionaryArray.from_arrays(
                    pa.array([0], pa.int8()),
                    pa.array([[("a", 1)]], pa.map_(pa.string(), pa.int64())),
                )
            }
        ),
        "refused.fsl_of_null": lambda: _tbl(
            {"c": pa.array([[None, None]], pa.list_(pa.null(), 2))}
        ),
        "accepted.slash_without_alias": lambda: _tbl(
            {"a/b": pa.array([1]), "c": pa.array([2])}
        ),
        "refused.nonnull_struct_child_with_null": lambda: _tbl(
            {
                "s": pa.StructArray.from_arrays(
                    [pa.array([1, None], pa.int64())],
                    fields=[pa.field("a", pa.int64(), nullable=False)],
                )
            }
        ),
        "refused.nonnull_list_item_with_null": lambda: _tbl(
            {
                "l": pa.ListArray.from_arrays(
                    pa.array([0, 2], pa.int32()),
                    pa.array([1, None], pa.int64()),
                    type=pa.list_(pa.field("item", pa.int64(), nullable=False)),
                )
            }
        ),
    }


def known_defect_tables() -> dict[str, Callable[[], pa.Table]]:
    """The inputs starfix 0.6.0 documents as hashing differently from 0.5.0
    (``python/tests/_harness.py`` ``EXPECTED_DIFFERENCES`` of the 0.6.0 sdist)."""

    def sliced_nested_fsl_dictionary() -> pa.Table:
        # ITL-1223: 0.5.0 decodes the dictionary with pyarrow take(), which returns wrong
        # values for a sliced fixed_size_list<fixed_size_list<...>> on pyarrow >= 17.
        nested = pa.list_(pa.list_(pa.int64(), 1), 2)
        values = pa.array([[[1], [2]], [[3], [4]], [[5], [6]]], nested).slice(1)
        return _tbl(
            {"d": pa.DictionaryArray.from_arrays(pa.array([0], pa.int8()), values)}
        )

    def bufferless_empty_dictionary() -> pa.Table:
        # A dictionary whose empty string dictionary has no offsets buffer (valid Arrow; every
        # key null): 0.5.0 raises TypeError, 0.6.0 hashes it as the allocated equivalent.
        buffers: list[Any] = [None, None, pa.py_buffer(b"")]
        dictionary = pa.Array.from_buffers(pa.string(), 0, buffers)
        column = pa.DictionaryArray.from_arrays(
            pa.array([None, None], pa.int32()), dictionary
        )
        return _tbl({"d": column})

    return {
        "known_defect.itl1223_sliced_nested_fsl_dictionary": sliced_nested_fsl_dictionary,
        "known_defect.bufferless_empty_dictionary": bufferless_empty_dictionary,
    }


# --- Orcapod logical-type tables (extension types) -----------------------------------------


@orcapod.dataclass
class Point:
    """A structural class (identified by name and fields)."""

    x: float
    y: int


@dataclasses.dataclass
class Exact:
    """An exact class (identified by module path and qualified name)."""

    name: str
    weights: list[float]


def orcapod_tables() -> dict[str, pa.Table]:
    import pandas as pd
    import polars as pl

    from orcapod.contexts import get_default_context
    from orcapod.types import Schema

    tc = get_default_context().type_converter

    FILES.mkdir(exist_ok=True)
    (FILES / "a.txt").write_text("alpha\n")
    (FILES / "b.txt").write_bytes(b"\x00\x01beta")
    (FILES / "dir").mkdir(exist_ok=True)
    (FILES / "dir" / "inner.txt").write_text("inner\n")
    rel = Path("docs/dev/scripts/itl_1179/corpus_files")

    rows_types: dict[str, tuple[list[Any], Any]] = {
        "path": ([Path("/x/a.txt"), Path("/y/b")], Path),
        "uuid": ([uuid.UUID(int=1), uuid.UUID(int=2**127)], uuid.UUID),
        "numpy_f64": (
            [
                np.array([1.0, math.nan, math.inf]),
                np.arange(6, dtype=np.float64).reshape(2, 3),
            ],
            np.ndarray,
        ),
        "numpy_f32_fortran": (
            [np.asfortranarray(np.arange(6, dtype=np.float32).reshape(2, 3))],
            np.ndarray,
        ),
        "numpy_i16": ([np.array([1, -2, 3], dtype=np.int16)], np.ndarray),
        "pandas_df": (
            [pd.DataFrame({"a": [1.0, math.nan], "b": ["x", "y"]})],
            pd.DataFrame,
        ),
        "pandas_series": ([pd.Series([1.5, 2.5], name="s")], pd.Series),
        "polars_df": (
            [
                pl.DataFrame(
                    {
                        "a": [1.0, None, math.nan],
                        "b": [[1], [], None],
                        "c": [{"p": 1}, None, {"p": 2}],
                    }
                )
            ],
            pl.DataFrame,
        ),
        "polars_series": (
            [pl.Series("s", [1.0, 2.0, math.inf], dtype=pl.Float32)],
            pl.Series,
        ),
        "structural_point": ([Point(1.5, 2), Point(math.nan, -1)], Point),
        "exact_dataclass": ([Exact("a", [1.0, 2.0]), Exact("b", [])], Exact),
        "set_int": ([{3, 1, 2}, set()], set[int]),
        "list_path": ([[Path("/a"), Path("/b")], []], list[Path]),
        "float_list": ([[1.0, math.nan], [math.inf]], list[float]),
    }
    try:
        import torch

        rows_types["torch_f32"] = ([torch.tensor([1.0, 2.0, math.nan])], torch.Tensor)
        rows_types["torch_bf16"] = (
            [torch.tensor([1.0, -2.0], dtype=torch.bfloat16)],
            torch.Tensor,
        )
    except ImportError:  # pragma: no cover - torch is optional
        pass
    try:
        from orcapod import File as FileT

        rows_types["file"] = ([FileT(rel / "a.txt"), FileT(rel / "b.txt")], FileT)
    except Exception:  # noqa: BLE001 - a missing File type is reported by the absence
        pass
    try:
        from orcapod import Directory as DirT

        rows_types["directory"] = ([DirT(rel / "dir")], DirT)
    except Exception:  # noqa: BLE001
        pass

    out: dict[str, pa.Table] = {}
    for name, (values, py_type) in rows_types.items():
        try:
            table = tc.python_dicts_to_arrow_table(
                [{"c": v, "k": i} for i, v in enumerate(values)],
                python_schema=Schema({"c": py_type, "k": int}),
            )
        except Exception as exc:  # noqa: BLE001 - recorded, compared across runs
            print(f"orcapod table {name}: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
        out[f"orcapod.{name}"] = table
        out[f"orcapod.{name}.zero_rows"] = table.slice(0, 0)
    return out


def library_values() -> dict[str, Any]:
    import pandas as pd
    import polars as pl

    values: dict[str, Any] = {
        "numpy.f64": np.array([1.0, math.nan, -math.inf, 5e-324]),
        "numpy.f16_2d": np.arange(4, dtype=np.float16).reshape(2, 2),
        "numpy.structured": np.array([(1, 2.0)], dtype=[("a", "<i4"), ("b", "<f8")]),
        "pandas.df_mixed": pd.DataFrame(
            {
                "a": [1, 2],
                "b": [1.5, math.nan],
                "c": ["x", None],
                "d": pd.to_datetime(["2020-01-01", None]),
            }
        ),
        "pandas.df_reordered": pd.DataFrame({"b": [1.5, math.nan], "a": [1, 2]}),
        "pandas.series_cat": pd.Series(["a", "b", "a"], dtype="category"),
        "pandas.series_f32": pd.Series([1.0, math.inf], dtype="float32", name="f"),
        "polars.df_nested": pl.DataFrame(
            {
                "f": [1.0, math.nan, None],
                "l": [[1.0], [], None],
                "s": [{"x": 1, "y": "a"}, None, {"x": 3, "y": None}],
                "d": pl.Series(["a", "b", "a"], dtype=pl.Categorical),
                "e": pl.Series(["lo", "hi", "lo"], dtype=pl.Enum(["lo", "hi"])),
                "t": pl.Series(
                    [dt.datetime(2020, 1, 1), None, dt.datetime(1999, 12, 31)]
                ).dt.replace_time_zone("UTC"),
                "dur": pl.Series(
                    [dt.timedelta(seconds=1), None, dt.timedelta(days=-1)]
                ),
                "dec": pl.Series(
                    [decimal.Decimal("1.5"), None, decimal.Decimal("-2.25")],
                    dtype=pl.Decimal(10, 2),
                ),
                "arr": pl.Series([[1, 2], [3, 4], None], dtype=pl.Array(pl.Int64, 2)),
            }
        ),
        "polars.df_null_col": pl.DataFrame({"n": [None, None], "v": [1, 2]}),
        "polars.df_empty": pl.DataFrame({"a": pl.Series([], dtype=pl.Float64)}),
        "polars.series_f16ish": pl.Series("x", [0.5, 1.5], dtype=pl.Float32),
        "polars.series_str": pl.Series("x", ["a", None]),
    }
    try:
        import torch

        values["torch.f64"] = torch.tensor(
            [1.0, math.nan, math.inf], dtype=torch.float64
        )
        values["torch.f16_strided"] = (
            torch.arange(6, dtype=torch.float16).reshape(2, 3).t()
        )
    except ImportError:  # pragma: no cover
        pass
    return values


# --- running ---------------------------------------------------------------------------------


def _outcome(fn: Callable[[], Any]) -> str:
    try:
        result = fn()
    except Exception as exc:  # noqa: BLE001 - the class and message are the record
        message = str(exc).splitlines()[0] if str(exc) else ""
        return f"raise:{type(exc).__name__}:{message}"
    if isinstance(result, bytes):
        return result.hex()
    return str(result)


def run() -> dict[str, str]:
    from orcapod.contexts import get_default_context
    from orcapod.hashing.arrow_hashers import StarfixArrowHasher

    ctx = get_default_context()
    hasher = cast(StarfixArrowHasher, ctx.arrow_hasher)
    semantic = ctx.semantic_hasher
    out: dict[str, str] = {}

    def starfix_calls(name: str, table_fn: Callable[[], pa.Table]) -> None:
        for m in (False, True):
            for c in (False, True):
                for s in (False, True):
                    out[f"{name}|sf.hash_table[{int(m)},{int(c)},{int(s)}]"] = _outcome(
                        lambda m=m, c=c, s=s: ArrowDigester.hash_table(
                            table_fn(),
                            include_metadata=m,
                            ordered_columns=c,
                            ordered_struct_fields=s,
                        )
                    )
            out[f"{name}|sf.hash_schema[{int(m)}]"] = _outcome(
                lambda m=m: ArrowDigester.hash_schema(
                    table_fn().schema, include_metadata=m
                )
            )

    def orcapod_calls(name: str, table_fn: Callable[[], pa.Table]) -> None:
        out[f"{name}|op.hash_table"] = _outcome(
            lambda: hasher.hash_table(table_fn()).to_string()
        )
        out[f"{name}|op.hash_table_ordered"] = _outcome(
            lambda: hasher.hash_table_ordered(table_fn()).to_string()
        )
        out[f"{name}|op.hash_table_ordered_nostruct"] = _outcome(
            lambda: hasher.hash_table_ordered(
                table_fn(), struct_field_order=False
            ).to_string()
        )
        out[f"{name}|op.hash_schema"] = _outcome(
            lambda: hasher.hash_schema(table_fn().schema).to_string()
        )

    for name, table in tables().items():
        starfix_calls(name, lambda t=table: t)
        orcapod_calls(name, lambda t=table: t)
    for name, table_fn in refused_tables().items():
        starfix_calls(name, table_fn)
        orcapod_calls(name, table_fn)
    for name, table_fn in known_defect_tables().items():
        starfix_calls(name, table_fn)
        orcapod_calls(name, table_fn)
    for name, table in orcapod_tables().items():
        orcapod_calls(name, lambda t=table: t)
    for name, value in library_values().items():
        out[f"value.{name}|op.value"] = _outcome(
            lambda v=value: semantic.hash_object(v).to_string()
        )
    return out


def main() -> None:
    target = Path(sys.argv[1])
    result = {
        "starfix_version": importlib.metadata.version("starfix"),
        "pyarrow_version": pa.__version__,
        "entries": run(),
    }
    target.write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
    print(
        f"{len(result['entries'])} entries, starfix {result['starfix_version']} -> {target}"
    )


if __name__ == "__main__":
    main()
