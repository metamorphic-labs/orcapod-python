"""A struct's Python type does not depend on its children's layouts (ITL-832).

The type converter synthesizes one ``TypedDict`` class per Arrow struct type.
It keys that class by the struct normalized to its large variants
(``arrow_utils.normalize_to_large_types``), so ``struct<b: string>`` and
``struct<b: large_string>`` (hash-identical under the Arrow hasher) map to the
same Python class, and that class maps back to the large-normalized struct.
Without this, a stream with a ``struct<b: string>`` column reported a
different Python type in ``output_schema()`` than in its own ``Data`` rows
(which normalize to large), and operators that normalize their output could
not match their predicted schema.

The hash pins were computed on ``main`` (365a5f74) before the change: keying
the Python class differently must not move any content or pipeline hash.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_type_converter
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.streams import ArrowTableStream

SMALL = pa.struct([("a", pa.int8()), ("b", pa.string())])
LARGE = pa.struct([("a", pa.int8()), ("b", pa.large_string())])
NESTED = pa.struct([("inner", SMALL), ("c", pa.list_(pa.string()))])


def _tables() -> dict[str, pa.Table]:
    """Build the tables whose hashes are pinned below."""
    keys = pa.array([1, 2], pa.int64())
    flat = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    nested = [
        {"inner": {"a": 1, "b": "x"}, "c": ["p"]},
        {"inner": {"a": 2, "b": "y"}, "c": []},
    ]
    return {
        "small": pa.table({"k": keys, "x": pa.array(flat, type=SMALL)}),
        "large": pa.table({"k": keys, "x": pa.array(flat, type=LARGE)}),
        "nested": pa.table({"k": keys, "x": pa.array(nested, type=NESTED)}),
        "list_of_struct": pa.table(
            {
                "k": keys,
                "x": pa.array([[flat[0]], []], type=pa.list_(SMALL)),
            }
        ),
    }


_FLAT_HASHES: dict[str, object] = {
    "stream_content": "e34f7e47f388b93db0403fe3e882ea4c2c7ce30cdcf7937eb928868e6aeaf93a",
    "stream_pipeline": "747ee6e5014ea0a4a4da0f444f4279aa2019e47e519885a40e04b96faff3d74e",
    "source_content": "63c596013c190818f771def09771ab2dcf923fd7536be3812bfa7368a7a53e7b",
    "source_pipeline": "747ee6e5014ea0a4a4da0f444f4279aa2019e47e519885a40e04b96faff3d74e",
    "data_rows": [
        "000002a3b465dfc79ea3f32b39f55417f4c2372d7a3cc7ad63aef00aa9108b21b76b9c",
        "000002e540dc3b59a01d14edc26eebff96453fb1de9018c351676cef8d1f67ee3346b2",
    ],
}

# Hashes on main (365a5f74), before the converter change; the stream content and per-row
# data hashes re-recorded in starfix hash format 2 (arrow_v1, ITL-910). The pipeline
# hashes and the source content hashes re-recorded for ITL-640 (a schema enters an
# identity as its Arrow schema); the stream content and data row hashes did not move.
MAIN_HASHES: dict[str, dict[str, object]] = {
    "small": _FLAT_HASHES,
    "large": _FLAT_HASHES,
    "nested": {
        "stream_content": "413fa479269c7957fef99caca81336f0d94c0cb240160c831e4b7b719c0a3ff1",
        "stream_pipeline": "89426b05815ca2e3ec349814707b2651e9f7e1aa7157ce4775e8111323511f19",
        "source_content": "ebf71afb9ea2a9525437d4c6a9a8355347dd0340c539c333786b2d6ae8ad1cb8",
        "source_pipeline": "89426b05815ca2e3ec349814707b2651e9f7e1aa7157ce4775e8111323511f19",
        "data_rows": [
            "000002b734209953036fae7ee935c232f7949f4874dc22dca2af4ecfa2882e5ddaf457",
            "0000024d11adcf6077042679df69776a748d71f2818fc47042913d83fa1fbebca67ad9",
        ],
    },
    "list_of_struct": {
        "stream_content": "145c0f2bdd30c6ded9e29067f7d6fa7649b18712ee75cb9a6c5eb6582500075e",
        "stream_pipeline": "9a7003a144808be6f9809ae7fe15411197bf88b634c14248ff5dee1bdce0134a",
        "source_content": "19c822dd6e78260958ea025254e50d2090e5be7fc0156c401e201ea3f9051f42",
        "source_pipeline": "9a7003a144808be6f9809ae7fe15411197bf88b634c14248ff5dee1bdce0134a",
        "data_rows": [
            "000002c262cfe3995378fe521abbc19b8cd14f10f626b5e5b1259a20f6d4277677fb87",
            "000002188277589a87f65b49271c1a2cf461ebed48ddd2fd3cb943198f420fced6862a",
        ],
    },
}


class TestStructPythonType:
    """Both layouts of a struct map to one Python class."""

    def test_layouts_share_one_class(self) -> None:
        """``struct<b: string>`` and ``struct<b: large_string>`` give the same class."""
        converter = get_default_type_converter()
        assert converter.arrow_type_to_python_type(
            SMALL
        ) is converter.arrow_type_to_python_type(LARGE)

    def test_nested_layouts_share_one_class(self) -> None:
        """Layout differences inside nested structs and lists do not matter either."""
        converter = get_default_type_converter()
        nested_large = pa.struct(
            [("inner", LARGE), ("c", pa.large_list(pa.large_string()))]
        )
        assert converter.arrow_type_to_python_type(
            NESTED
        ) is converter.arrow_type_to_python_type(nested_large)

    def test_width_still_distinguishes_classes(self) -> None:
        """Only layout is ignored: an ``int64`` field is a different struct."""
        converter = get_default_type_converter()
        wide = pa.struct([("a", pa.int64()), ("b", pa.string())])
        assert converter.arrow_type_to_python_type(
            wide
        ) is not converter.arrow_type_to_python_type(SMALL)

    @pytest.mark.parametrize("struct_type", [SMALL, LARGE], ids=str)
    def test_class_maps_back_to_the_large_struct(
        self, struct_type: pa.DataType
    ) -> None:
        """The class converts back to the large-normalized struct, whichever came first."""
        converter = get_default_type_converter()
        python_type = converter.arrow_type_to_python_type(struct_type)
        assert converter.python_type_to_arrow_type(python_type) == LARGE

    def test_stream_schema_matches_its_data_rows(self) -> None:
        """A stream reports the same struct class as the ``Data`` rows it yields.

        ``Data`` normalizes its table to large types, so before the change the
        row-level class differed from the stream-level one on ``main``.
        """
        stream = ArrowTableStream(_tables()["small"], tag_columns=["k"])
        _, data = next(iter(stream.iter_data()))
        assert data.schema()["x"] == stream.output_schema()[1]["x"]


class TestStructHashesUnchanged:
    """Content and pipeline hashes of struct columns are the same as on ``main``."""

    @pytest.mark.parametrize("name", sorted(MAIN_HASHES))
    def test_hashes_match_main(self, name: str) -> None:
        """Stream, source and per-row data hashes equal the values pinned on main."""
        table = _tables()[name]
        stream = ArrowTableStream(table, tag_columns=["k"])
        source = ArrowTableSource(
            table, tag_columns=["k"], tag_only_columns=True, source_id="fixed"
        )
        expected = MAIN_HASHES[name]
        assert stream.content_hash().to_hex() == expected["stream_content"]
        assert stream.pipeline_hash().to_hex() == expected["stream_pipeline"]
        assert source.content_hash().to_hex() == expected["source_content"]
        assert source.pipeline_hash().to_hex() == expected["source_pipeline"]
        assert [
            data.content_hash().to_hex() for _, data in stream.iter_data()
        ] == expected["data_rows"]


class TestNullFields:
    """Only layouts are normalized: a ``null`` field stays ``null``."""

    def test_null_field_round_trips(self) -> None:
        """``struct<a: null, b: string>`` maps back to ``struct<a: null, b: large_string>``.

        ``normalize_to_large_types`` turns ``null`` into ``large_string`` by
        default, which is a type change, not a layout change; the converter
        must not apply it.
        """
        converter = get_default_type_converter()
        struct_type = pa.struct([("a", pa.null()), ("b", pa.string())])
        python_type = converter.arrow_type_to_python_type(struct_type)
        assert python_type.__annotations__["b"] is str
        assert python_type.__annotations__["a"] is not str
        assert converter.python_type_to_arrow_type(python_type) == pa.struct(
            [("a", pa.null()), ("b", pa.large_string())]
        )
