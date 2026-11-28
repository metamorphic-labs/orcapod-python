"""The three nested row ids and their preimages (ITL-1471).

``core/row_ids.py`` is the one place the entry id, the instance id and the record id
are built. These tests state each preimage column by column and hash it by hand, so a
change of a column name, a type or the nesting fails here.

The rows have three tags (two user tags in two orders, with two system tags) and three
data columns in two orders, each with its own source-info token.
"""

from __future__ import annotations

import uuid
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core import row_ids
from orcapod.core.data_function import output_source_info
from orcapod.core.datagrams import Data, Tag
from orcapod.core.datagrams.tag_data import EmptyData
from orcapod.system_constants import constants
from orcapod.utils import arrow_utils

HASHER = get_default_context().arrow_hasher

SYSTEM_TAGS = {
    f"{constants.SYSTEM_TAG_PREFIX}source_id::abc": "dict_v1:mice",
    f"{constants.SYSTEM_TAG_PREFIX}record_id::abc": "row_0",
}
TAG_ORDERS = [("subject", "session"), ("session", "subject")]
DATA_ORDERS = [("weight", "length", "note"), ("note", "weight", "length")]
TAG_VALUES: dict[str, Any] = {"subject": "m1", "session": 3}
DATA_VALUES: dict[str, Any] = {"weight": 21.5, "length": 9, "note": "ok"}
TOKENS = {name: f"dict_v1:mice::row_0::{name}" for name in DATA_VALUES}


def _tag(order: tuple[str, ...] = TAG_ORDERS[0]) -> Tag:
    return Tag({name: TAG_VALUES[name] for name in order}, system_tags=SYSTEM_TAGS)


def _data(
    order: tuple[str, ...] = DATA_ORDERS[0], tokens: dict[str, str] | None = None
) -> Data:
    tokens = TOKENS if tokens is None else tokens
    return Data(
        {name: DATA_VALUES[name] for name in order},
        source_info={name: tokens[name] for name in order},
    )


def _binary(value: bytes) -> "pa.Array[Any]":
    return pa.array([value], type=pa.large_binary())


@pytest.mark.parametrize("scalar_last", [False, True])
def test_with_source_info_uses_the_later_alias_in_every_view(scalar_last: bool) -> None:
    """Prefixed and bare spellings update the same token in keyword order."""
    data = Data(
        pa.table({"x": [1], "y": [2], "_source_x": ["old"], "_source_y": ["kept"]})
    )
    scalar = pa.scalar(["arrow"], type=pa.large_list(pa.large_string()))
    if scalar_last:
        updated = data.with_source_info(x="python", _source_x=scalar)
        expected = ["arrow"]
        expected_type = scalar.type
    else:
        updated = data.with_source_info(x=scalar, _source_x="python")
        expected = "python"
        expected_type = pa.large_string()

    assert updated.source_info() == {"x": expected, "y": "kept"}
    entry = row_ids.entry_id(HASHER, _tag(), updated)
    for table in (
        updated.as_table(columns={"source": True}),
        updated.source_info_table(),
        row_ids.instance_id_preimage(entry, updated),
    ):
        assert table.column("_source_x").to_pylist() == [expected]
        assert table.schema.field("_source_x").type.equals(
            expected_type, check_metadata=True
        )
        assert table.column("_source_y").to_pylist() == ["kept"]


def test_with_source_info_keeps_a_scalar_type_its_value_cannot_show() -> None:
    """Replacing a typed column never infers the Arrow scalar's type."""
    data = Data(
        pa.table({"x": [1], "y": [2], "_source_x": ["old"], "_source_y": ["kept"]})
    )
    token_type = pa.large_list(pa.large_list(pa.large_list(pa.large_string())))
    token = [[], [[]]]
    updated = data.with_source_info(x=pa.scalar(token, type=token_type))

    assert updated.source_info() == {"x": token, "y": "kept"}
    entry = row_ids.entry_id(HASHER, _tag(), updated)
    for table in (
        updated.as_table(columns={"source": True}),
        updated.source_info_table(),
        row_ids.instance_id_preimage(entry, updated),
    ):
        assert table.column("_source_x").to_pylist() == [token]
        assert table.schema.field("_source_x").type.equals(
            token_type, check_metadata=True
        )
        assert table.column("_source_y").to_pylist() == ["kept"]


class TestEntryId:
    def test_the_preimage_is_the_tags_the_system_tags_and_the_labelled_data_hash(
        self,
    ) -> None:
        tag, data = _tag(), _data()
        preimage = row_ids.entry_id_preimage(tag, data)
        assert preimage.num_rows == 1
        assert preimage.column_names == [
            "subject",
            "session",
            *SYSTEM_TAGS,
            "__input_data_hash",
        ]
        assert preimage.schema.field("__input_data_hash").type == pa.large_binary()
        assert preimage.column("__input_data_hash").to_pylist() == [
            data.content_hash().to_prefixed_digest()
        ]
        # No token and no index column.
        assert not [n for n in preimage.column_names if n.startswith("_source_")]
        assert "__pipeline_recomputation_index" not in preimage.column_names

    def test_the_id_is_the_arrow_hash_of_the_preimage(self) -> None:
        tag, data = _tag(), _data()
        by_hand = tag.as_table(columns={"system_tags": True}).append_column(
            "__input_data_hash", _binary(data.content_hash().to_prefixed_digest())
        )
        assert row_ids.entry_id(HASHER, tag, data) == HASHER.hash_table(by_hand)

    @pytest.mark.parametrize("tag_order", TAG_ORDERS)
    @pytest.mark.parametrize("data_order", DATA_ORDERS)
    def test_the_order_of_tags_and_data_columns_does_not_count(
        self, tag_order: tuple[str, ...], data_order: tuple[str, ...]
    ) -> None:
        assert row_ids.entry_id(
            HASHER, _tag(tag_order), _data(data_order)
        ) == row_ids.entry_id(HASHER, _tag(), _data())

    def test_a_token_is_no_part_of_it(self) -> None:
        other = {name: f"dict_v1:other::row_9::{name}" for name in DATA_VALUES}
        assert row_ids.entry_id(
            HASHER, _tag(), _data(tokens=other)
        ) == row_ids.entry_id(HASHER, _tag(), _data())

    def test_tags_system_tags_and_data_each_count(self) -> None:
        base = row_ids.entry_id(HASHER, _tag(), _data())
        other_tag = Tag({"subject": "m2", "session": 3}, system_tags=SYSTEM_TAGS)
        other_system = Tag(
            TAG_VALUES,
            system_tags={**SYSTEM_TAGS, next(reversed(SYSTEM_TAGS)): "row_1"},
        )
        other_data = Data({**DATA_VALUES, "length": 10}, source_info=TOKENS)
        ids = {
            base,
            row_ids.entry_id(HASHER, other_tag, _data()),
            row_ids.entry_id(HASHER, other_system, _data()),
            row_ids.entry_id(HASHER, _tag(), other_data),
        }
        assert len(ids) == 4


class TestInstanceId:
    def test_the_preimage_is_the_labelled_entry_id_then_the_tokens_in_name_order(
        self,
    ) -> None:
        tag, data = _tag(), _data()
        entry = row_ids.entry_id(HASHER, tag, data)
        preimage = row_ids.instance_id_preimage(entry, data)
        assert preimage.column_names == [
            "__pipeline_entry_id",
            "_source_length",
            "_source_note",
            "_source_weight",
        ]
        assert preimage.schema.field("__pipeline_entry_id").type == pa.large_binary()
        assert preimage.to_pylist() == [
            {
                "__pipeline_entry_id": entry.to_prefixed_digest(),
                "_source_length": TOKENS["length"],
                "_source_note": TOKENS["note"],
                "_source_weight": TOKENS["weight"],
            }
        ]

    def test_the_id_is_the_arrow_hash_of_the_preimage(self) -> None:
        tag, data = _tag(), _data()
        entry = row_ids.entry_id(HASHER, tag, data)
        by_hand = pa.table(
            {
                "__pipeline_entry_id": _binary(entry.to_prefixed_digest()),
                **{
                    f"_source_{name}": pa.array([TOKENS[name]], pa.large_string())
                    for name in sorted(TOKENS)
                },
            }
        )
        assert row_ids.instance_id(HASHER, tag, data) == HASHER.hash_table(by_hand)

    @pytest.mark.parametrize("tag_order", TAG_ORDERS)
    @pytest.mark.parametrize("data_order", DATA_ORDERS)
    def test_the_order_of_the_token_columns_does_not_count(
        self, tag_order: tuple[str, ...], data_order: tuple[str, ...]
    ) -> None:
        assert row_ids.instance_id(
            HASHER, _tag(tag_order), _data(data_order)
        ) == row_ids.instance_id(HASHER, _tag(), _data())

    @pytest.mark.parametrize("changed", sorted(TOKENS))
    def test_a_change_of_any_one_token_is_another_instance_of_the_same_entry(
        self, changed: str
    ) -> None:
        other = {**TOKENS, changed: f"dict_v1:mice::row_7::{changed}"}
        assert row_ids.entry_id(
            HASHER, _tag(), _data(tokens=other)
        ) == row_ids.entry_id(HASHER, _tag(), _data())
        assert row_ids.instance_id(
            HASHER, _tag(), _data(tokens=other)
        ) != row_ids.instance_id(HASHER, _tag(), _data())

    def test_it_differs_from_the_entry_id(self) -> None:
        tag, data = _tag(), _data()
        assert row_ids.instance_id(HASHER, tag, data) != row_ids.entry_id(
            HASHER, tag, data
        )


class TestRecordId:
    NODE = HASHER.hash_table(pa.table({"node": pa.array(["first"])}))
    OTHER_NODE = HASHER.hash_table(pa.table({"node": pa.array(["second"])}))

    def test_the_preimage_is_the_labelled_node_hash_and_instance_id_and_the_result_id(
        self,
    ) -> None:
        instance = row_ids.instance_id(HASHER, _tag(), _data())
        result_id = uuid.uuid4()
        preimage = row_ids.record_id_preimage(self.NODE, instance, result_id)
        assert preimage.column_names == [
            "__node_content_hash",
            "__pipeline_instance_id",
            "__data_id",
        ]
        assert set(preimage.schema.types) == {pa.large_binary()}
        assert preimage.to_pylist() == [
            {
                "__node_content_hash": self.NODE.to_prefixed_digest(),
                "__pipeline_instance_id": instance.to_prefixed_digest(),
                "__data_id": result_id.bytes,
            }
        ]
        assert row_ids.record_id(
            HASHER, self.NODE, instance, result_id
        ) == HASHER.hash_table(preimage)

    def test_another_result_of_one_instance_is_another_record(self) -> None:
        instance = row_ids.instance_id(HASHER, _tag(), _data())
        first, second = uuid.uuid4(), uuid.uuid4()
        assert row_ids.record_id(
            HASHER, self.NODE, instance, first
        ) != row_ids.record_id(HASHER, self.NODE, instance, second)
        assert row_ids.record_id(
            HASHER, self.NODE, instance, first
        ) == row_ids.record_id(HASHER, self.NODE, instance, first)

    def test_one_result_of_two_instances_is_two_records(self) -> None:
        other = {name: f"dict_v1:other::row_9::{name}" for name in DATA_VALUES}
        result_id = uuid.uuid4()
        assert row_ids.record_id(
            HASHER, self.NODE, row_ids.instance_id(HASHER, _tag(), _data()), result_id
        ) != row_ids.record_id(
            HASHER,
            self.NODE,
            row_ids.instance_id(HASHER, _tag(), _data(tokens=other)),
            result_id,
        )

    def test_one_instance_and_result_recorded_by_two_nodes_is_two_records(self) -> None:
        instance = row_ids.instance_id(HASHER, _tag(), _data())
        result_id = uuid.uuid4()
        assert row_ids.record_id(
            HASHER, self.NODE, instance, result_id
        ) != row_ids.record_id(HASHER, self.OTHER_NODE, instance, result_id)

    def test_the_three_ids_carry_the_arrow_label(self) -> None:
        tag, data = _tag(), _data()
        instance = row_ids.instance_id(HASHER, tag, data)
        for value in (
            row_ids.entry_id(HASHER, tag, data),
            instance,
            row_ids.record_id(HASHER, instance, instance, uuid.uuid4()),
        ):
            assert value.to_prefixed_digest().startswith(b"arrow_v1:")


class TestPlaceholder:
    """The instance id of an ``EmptyData`` placeholder, with and without tokens."""

    URI = ("inc", "0" * 64, "v0", "python.function.v1")

    def _placeholder(self, columns: tuple[str, ...]) -> EmptyData:
        return EmptyData(
            cached_content_hash=_data().content_hash(),
            source_info=output_source_info(
                self.URI, row_ids.EPHEMERAL_RESULT_GONE, columns
            ),
        )

    def test_the_entry_id_reads_only_the_hash_the_placeholder_keeps(self) -> None:
        assert row_ids.entry_id(
            HASHER, _tag(), self._placeholder(("y", "z"))
        ) == row_ids.entry_id(HASHER, _tag(), _data())

    def test_a_placeholder_with_no_token_gives_the_hash_of_the_entry_id_alone(
        self,
    ) -> None:
        placeholder = EmptyData(cached_content_hash=_data().content_hash())
        entry = row_ids.entry_id(HASHER, _tag(), placeholder)
        preimage = row_ids.instance_id_preimage(entry, placeholder)
        assert preimage.column_names == ["__pipeline_entry_id"]
        assert row_ids.instance_id(HASHER, _tag(), placeholder) == HASHER.hash_table(
            pa.table({"__pipeline_entry_id": _binary(entry.to_prefixed_digest())})
        )

    @pytest.mark.parametrize("columns", [("y", "z"), ("z", "y")])
    def test_a_placeholder_with_tokens_is_hashed_as_any_row_with_those_tokens(
        self, columns: tuple[str, ...]
    ) -> None:
        placeholder = self._placeholder(columns)
        entry = row_ids.entry_id(HASHER, _tag(), placeholder)
        source = ":".join(self.URI)
        by_hand = pa.table(
            {
                "__pipeline_entry_id": _binary(entry.to_prefixed_digest()),
                "_source_y": pa.array(
                    [f"{source}::EPHEMERAL_RESULT_GONE::y"], pa.large_string()
                ),
                "_source_z": pa.array(
                    [f"{source}::EPHEMERAL_RESULT_GONE::z"], pa.large_string()
                ),
            }
        )
        assert row_ids.instance_id_preimage(entry, placeholder).equals(by_hand)
        assert row_ids.instance_id(HASHER, _tag(), placeholder) == HASHER.hash_table(
            by_hand
        )

    def test_its_instance_id_is_not_the_one_of_the_result_that_is_gone(self) -> None:
        real = Data(
            DATA_VALUES,
            source_info=output_source_info(self.URI, uuid.uuid4().hex, DATA_VALUES),
        )
        gone = EmptyData(
            cached_content_hash=real.content_hash(),
            source_info=output_source_info(
                self.URI, row_ids.EPHEMERAL_RESULT_GONE, DATA_VALUES
            ),
        )
        assert row_ids.entry_id(HASHER, _tag(), gone) == row_ids.entry_id(
            HASHER, _tag(), real
        )
        assert row_ids.instance_id(HASHER, _tag(), gone) != row_ids.instance_id(
            HASHER, _tag(), real
        )


class TestSentinel:
    """``EPHEMERAL_RESULT_GONE`` names no result and keeps the token grammar."""

    def test_no_result_id_can_equal_it(self) -> None:
        sentinel = row_ids.EPHEMERAL_RESULT_GONE
        # A result id in a token is the hex of a UUID: 32 characters of 0-9a-f.
        for result_id in (uuid.uuid4(), uuid.UUID(int=0), uuid.UUID(int=2**128 - 1)):
            assert len(result_id.hex) == 32
            assert set(result_id.hex) <= set("0123456789abcdef")
        assert len(sentinel) != 32
        assert not set(sentinel) <= set("0123456789abcdefABCDEF")
        with pytest.raises(ValueError):
            uuid.UUID(hex=sentinel)
        with pytest.raises(ValueError):
            uuid.UUID(sentinel)

    def test_it_holds_no_separator(self) -> None:
        assert constants.BLOCK_SEPARATOR == "::"
        assert "::" not in row_ids.EPHEMERAL_RESULT_GONE
        assert ":" not in row_ids.EPHEMERAL_RESULT_GONE

    @pytest.mark.parametrize("column", ["y", "weight_g", "a:b"])
    def test_a_token_with_it_splits_at_its_first_and_last_separator(
        self, column: str
    ) -> None:
        uri = ("inc", "ab" * 32, "v0", "python.function.v1")
        token = output_source_info(uri, row_ids.EPHEMERAL_RESULT_GONE, [column])[column]
        source, rest = token.split("::", 1)
        record, last = rest.rsplit("::", 1)
        assert (source, record, last) == (
            ":".join(uri),
            row_ids.EPHEMERAL_RESULT_GONE,
            column,
        )
        assert token.count("::") == 2

    def test_a_real_output_token_has_the_same_form_with_the_result_id(self) -> None:
        uri = ("inc", "ab" * 32, "v0", "python.function.v1")
        result_id = uuid.uuid4()
        real = output_source_info(uri, result_id.hex, ["y", "z"])
        gone = output_source_info(uri, row_ids.EPHEMERAL_RESULT_GONE, ["y", "z"])
        assert list(real) == list(gone) == ["y", "z"]
        for column in real:
            assert real[column].replace(result_id.hex, "X") == gone[column].replace(
                row_ids.EPHEMERAL_RESULT_GONE, "X"
            )


class TestTokensAreReadAsTheRowCarriesThem:
    """The instance id reads a stream row's typed token columns (review of PR #583).

    A row read from an Arrow table keeps that table's source-info columns. The
    instance id is made from them, with a row's own nullable fields, and a token's
    type is never worked out again from its value.
    """

    NESTED = pa.large_list(pa.large_list(pa.large_list(pa.large_string())))

    def _stream(self, nested: bool) -> Any:
        from orcapod.core.streams import ArrowTableStream

        tokens_a = (
            pa.array([[[], [[]]], [[], [[]]], [[["t"]]]], self.NESTED)
            if nested
            else pa.array(
                ["s::row_0::a", "s::row_1::a", "s::row_2::a"], pa.large_string()
            )
        )
        # ``a`` has its token's list layers (the token-shape rule, ITL-1491).
        from tests.token_shapes import data_for, data_type_for

        a_type = data_type_for(tokens_a.type)
        data_a = pa.array(
            [
                data_for(t, v, a_type)
                for t, v in zip(cast("Any", tokens_a).to_pylist(), (10, 20, 30))
            ],
            a_type,
        )
        table = pa.Table.from_arrays(
            [
                pa.array(["m1", "m1", "m2"], pa.large_string()),
                pa.array([1, 2, 1], pa.int64()),
                data_a,
                pa.array([0.5, 1.5, 2.5], pa.float64()),
                tokens_a,
                pa.array(["s::row_0::b", "s::row_1::b", None], pa.large_string()),
            ],
            schema=pa.schema(
                [
                    pa.field("subject", pa.large_string(), nullable=False),
                    pa.field("session", pa.int64(), nullable=False),
                    pa.field("a", a_type, nullable=False),
                    pa.field("b", pa.float64(), nullable=False),
                    pa.field("_source_a", tokens_a.type, nullable=nested),
                    pa.field("_source_b", pa.large_string()),
                ]
            ),
        )
        return ArrowTableStream(table, tag_columns=["subject", "session"])

    def test_an_ordinary_rows_columns_are_those_its_own_table_gives(self) -> None:
        rows = list(self._stream(nested=False).iter_data())
        assert len(rows) == 3
        for _, data in rows:
            typed = data.source_info_table()
            own = data.as_table(columns={"source": True}).select(
                ["_source_a", "_source_b"]
            )
            assert typed.equals(own)
            assert typed.schema.equals(own.schema)
            assert all(field.nullable for field in typed.schema)

    def test_an_ordinary_rows_instance_id_is_the_one_its_own_table_gives(self) -> None:
        ids = set()
        for tag, data in self._stream(nested=False).iter_data():
            entry = row_ids.entry_id(HASHER, tag, data)
            own = data.as_table(columns={"source": True})
            by_hand = pa.table(
                {"__pipeline_entry_id": _binary(entry.to_prefixed_digest())}
            )
            for name in ("_source_a", "_source_b"):
                by_hand = by_hand.append_column(
                    own.schema.field(name), own.column(name)
                )
            assert row_ids.instance_id(HASHER, tag, data) == HASHER.hash_table(by_hand)
            ids.add(row_ids.instance_id(HASHER, tag, data))
        assert len(ids) == 3

    def test_a_nested_token_column_is_read_with_its_type_and_never_typed_again(
        self,
    ) -> None:
        rows = list(self._stream(nested=True).iter_data())
        ids = set()
        for index, (tag, data) in enumerate(rows):
            typed = data.source_info_table()
            assert typed.schema.field("_source_a").type == self.NESTED
            assert typed.schema.field("_source_b").type == pa.large_string()
            preimage = row_ids.instance_id_preimage(
                row_ids.entry_id(HASHER, tag, data), data
            )
            assert preimage.column_names == [
                "__pipeline_entry_id",
                "_source_a",
                "_source_b",
            ]
            assert preimage.schema.field("_source_a").type == self.NESTED
            ids.add(row_ids.instance_id(HASHER, tag, data))
            # The row's table gives the same typed column, never one typed again
            # from the token's value, which fails for the first two rows (ITL-1491).
            shown = data.as_table(columns={"source": True})
            assert shown.schema.field("_source_a").type == self.NESTED
            assert shown.column("_source_a").equals(typed.column("_source_a"))
            if index < 2:
                with pytest.raises(pa.ArrowTypeError):
                    pa.array(
                        shown.column("_source_a").to_pylist(),
                        arrow_utils.source_info_arrow_type(
                            shown.column("_source_a").to_pylist()[0]
                        ),
                    )
        assert len(ids) == 3

    def test_a_changed_row_gives_the_tokens_it_now_has(self) -> None:
        _, data = next(iter(self._stream(nested=False).iter_data()))
        assert data.copy().source_info_table().equals(data.source_info_table())
        selected = data.select("b")
        assert selected.source_info_table().column_names == ["_source_b"]
        renamed = data.rename({"a": "c"})
        assert renamed.source_info_table().column_names == ["_source_c", "_source_b"]
        retold = data.with_source_info(a="other::row_9::a")
        assert retold.source_info_table().to_pylist() == [
            {"_source_a": "other::row_9::a", "_source_b": "s::row_0::b"}
        ]
        assert row_ids.instance_id(HASHER, _tag(), retold) != row_ids.instance_id(
            HASHER, _tag(), data
        )


class TestTokensOfBuiltAndChangedRows:
    """A missing token is a null token, and a kept token keeps its type (review of PR #583).

    A row built from Python values and the same row read from an Arrow table give
    the same token columns, so one instance id. A change of the row (a selection,
    a drop, a rename, added columns, one replaced token) keeps the typed column of
    every token it does not replace.
    """

    NESTED = TestTokensAreReadAsTheRowCarriesThem.NESTED

    @pytest.mark.parametrize("order", DATA_ORDERS)
    @pytest.mark.parametrize("told", [(), ("length",), ("note", "weight")])
    def test_a_built_row_and_the_row_read_from_a_table_have_one_instance_id(
        self, order: tuple[str, ...], told: tuple[str, ...]
    ) -> None:
        tokens = {name: TOKENS[name] for name in told}
        built = Data({name: DATA_VALUES[name] for name in order}, source_info=tokens)
        # The data fields as a row built from values has them (not null), so the
        # two rows have one data hash and differ only in how they were built.
        values = Data({name: DATA_VALUES[name] for name in order}).as_table()
        table = values
        for name in order:
            table = table.append_column(
                pa.field(f"_source_{name}", pa.large_string()),
                pa.array([tokens.get(name)], pa.large_string()),
            )
        read = Data(table)
        (streamed,) = Data.rows_of(table, get_default_context())
        expected = [f"_source_{name}" for name in order]
        ids = set()
        for data in (built, read, streamed):
            tokens_table = data.source_info_table()
            assert tokens_table.column_names == expected
            assert tokens_table.num_rows == 1
            assert all(
                field.type == pa.large_string() and field.nullable
                for field in tokens_table.schema
            )
            assert tokens_table.to_pylist() == [
                {f"_source_{name}": tokens.get(name) for name in order}
            ]
            ids.add(row_ids.instance_id(HASHER, _tag(), data))
        assert len(ids) == 1
        # The order of the data columns does not count either.
        other = Data(
            {name: DATA_VALUES[name] for name in reversed(order)}, source_info=tokens
        )
        assert row_ids.instance_id(HASHER, _tag(), other) in ids

    def _nested_rows(self) -> list[Any]:
        stream = TestTokensAreReadAsTheRowCarriesThem()._stream(nested=True)
        return [data for _, data in stream.iter_data()]

    @pytest.mark.parametrize(
        ("change", "columns"),
        [
            (lambda d: d.select("a"), {"a": "a"}),
            (lambda d: d.select("b", "a"), {"a": "a", "b": "b"}),
            (lambda d: d.select("a", "b"), {"a": "a", "b": "b"}),
            (lambda d: d.drop("b"), {"a": "a"}),
            (lambda d: d.rename({"a": "c"}), {"c": "a", "b": "b"}),
            (lambda d: d.rename({"b": "e", "a": "c"}), {"c": "a", "e": "b"}),
            (lambda d: d.with_columns(extra=1, more="m"), {"a": "a", "b": "b"}),
            (lambda d: d.with_source_info(b="other::row_9::b"), {"a": "a"}),
            (lambda d: d.select("a").copy(), {"a": "a"}),
        ],
    )
    def test_a_change_keeps_the_typed_column_of_every_token_it_keeps(
        self, change: Any, columns: dict[str, str]
    ) -> None:
        ids = set()
        for data in self._nested_rows():
            before = data.source_info_table()
            changed = change(data)
            after = changed.source_info_table()
            assert after.column_names == [f"_source_{name}" for name in changed.keys()]
            for new, old in columns.items():
                assert after.column(f"_source_{new}").equals(
                    before.column(f"_source_{old}")
                )
            if "a" in columns.values():
                kept = next(new for new, old in columns.items() if old == "a")
                if before.column("_source_a").null_count == 0:
                    assert after.schema.field(f"_source_{kept}").type == self.NESTED
            for name in set(changed.keys()) - set(columns):
                token = changed.source_info()[name]
                assert after.column(f"_source_{name}").to_pylist() == [token]
            ids.add(row_ids.instance_id(HASHER, _tag(), changed))
        assert len(ids) == 3

    def test_columns_taken_from_another_row_keep_their_tokens_types(
        self,
    ) -> None:
        """``with_columns_of`` (a data-tag falling through a function) keeps the type.

        The taken columns' tokens keep the typed columns the other row carries
        (developer, November 27, 2026, ITL-1491: the pipeline determines a token
        column's type; it is never taken from values): a fixed-size list token
        stays a fixed-size list, and a nested token that cannot be typed from its
        value is taken as it is. The row's own columns (a function's output) keep
        their string tokens.
        """
        fixed = pa.list_(pa.large_string(), 2)
        table = pa.Table.from_arrays(
            [
                pa.array([1, 2], pa.int64()),
                pa.array([0.5, 1.5], pa.float64()),
                pa.array([["p", "q"], ["r", "s"]], fixed),
                pa.array(["s::row_0::b", None], pa.large_string()),
            ],
            schema=pa.schema(
                [
                    pa.field("a", pa.int64(), nullable=False),
                    pa.field("b", pa.float64(), nullable=False),
                    pa.field("_source_a", fixed),
                    pa.field("_source_b", pa.large_string()),
                ]
            ),
        )
        ids = set()
        for other in Data.rows_of(table, get_default_context()):
            assert other.source_info_table().schema.field("_source_a").type == fixed
            for names in (["b", "a"], ["a", "b"]):
                own = Data(
                    {"y": 1, "z": "t"}, source_info={"y": "f::r::y", "z": "f::r::z"}
                )
                joined = own.with_columns_of(other, names)
                tokens = joined.source_info_table()
                assert tokens.column_names == [
                    *(f"_source_{name}" for name in names),
                    "_source_y",
                    "_source_z",
                ]
                assert tokens.schema.field("_source_a").type == fixed
                assert tokens.column("_source_a").equals(
                    other.source_info_table().column("_source_a")
                )
                assert tokens.schema.field("_source_b").type == pa.large_string()
                # Every view gives the same tokens, and the table view the same types.
                as_table = joined.as_table(columns={"source": True})
                for name in ("a", "b", "y", "z"):
                    column = f"_source_{name}"
                    assert tokens.column(column).to_pylist() == [
                        joined.source_info()[name]
                    ]
                    if joined.source_info()[name] is not None:
                        assert as_table.schema.field(column).type == (
                            tokens.schema.field(column).type
                        )
                ids.add(row_ids.instance_id(HASHER, _tag(), joined))
        # Two rows, each with one id whatever the order the columns were taken in.
        assert len(ids) == 2
        for other in self._nested_rows():
            own = Data({"y": 1}, source_info={"y": "f::r::y"})
            joined = own.with_columns_of(other, ["a"])
            for view in (
                joined.source_info_table(),
                joined.as_table(columns={"source": True}),
            ):
                assert view.schema.field("_source_a").type == self.NESTED
                assert view.column("_source_a").equals(
                    other.source_info_table().column("_source_a")
                )
                assert view.schema.field("_source_y").type == pa.large_string()


class TestARenameMovesTheToken:
    """A rename moves a column's token with it, in every view (review of PR #583).

    A token given earlier for the destination name, while the row had no such
    column, is dropped: it never wins over the renamed column's own token.
    """

    def _rows(self) -> list[Data]:
        values = {"weight": 21.5, "length": 9, "note": "ok"}
        built = Data(values, source_info=TOKENS)
        table = built.as_table(columns={"source": True})
        (streamed,) = Data.rows_of(table, get_default_context())
        return [built, Data(table), streamed]

    @pytest.mark.parametrize("stale_first", [True, False])
    @pytest.mark.parametrize(
        "mapping",
        [
            {"weight": "mass"},
            {"note": "mass", "weight": "size"},
            {"weight": "size", "note": "mass"},
        ],
    )
    def test_every_view_gives_the_renamed_columns_own_token(
        self, mapping: dict[str, str], stale_first: bool
    ) -> None:
        stale = {new: f"stale::row_9::{new}" for new in mapping.values()}
        ids = set()
        for data in self._rows():
            if stale_first:
                told = data.with_source_info(**stale)
            else:
                # The stale tokens after the row's own, in the order they are held.
                told = data.with_source_info(**stale).with_source_info(**TOKENS)
            renamed = told.rename(mapping)
            expected = {mapping.get(name, name): TOKENS[name] for name in data.keys()}
            assert renamed.source_info() == expected
            as_dict = renamed.as_dict(columns={"source": True})
            assert {name: as_dict[f"_source_{name}"] for name in expected} == expected
            table = renamed.as_table(columns={"source": True})
            tokens_table = renamed.source_info_table()
            for name, token in expected.items():
                assert table.column(f"_source_{name}").to_pylist() == [token]
                assert tokens_table.column(f"_source_{name}").to_pylist() == [token]
            assert tokens_table.column_names == [f"_source_{n}" for n in renamed.keys()]
            # The id is the one of the row renamed without any stale token.
            clean = data.rename(mapping)
            assert row_ids.instance_id(HASHER, _tag(), renamed) == row_ids.instance_id(
                HASHER, _tag(), clean
            )
            preimage = row_ids.instance_id_preimage(
                row_ids.entry_id(HASHER, _tag(), renamed), renamed
            )
            for name, token in expected.items():
                assert preimage.column(f"_source_{name}").to_pylist() == [token]
            ids.add(row_ids.instance_id(HASHER, _tag(), renamed))
            # Renaming back gives the first row's tokens again.
            back = renamed.rename({new: old for old, new in mapping.items()})
            assert back.source_info() == data.source_info()
        assert len(ids) == 1


class TestARenameOfAPlaceholderKeepsItsTokens:
    """A placeholder's sentinel tokens survive a rename (review of PR #583).

    A placeholder holds no payload column: its tokens are all it has of a row.
    An empty mapping changes nothing and returns the row itself; a mapping that
    names none of its tokens keeps every token; a mapping that names one moves it.
    """

    URI = ("combine", "abc", "v0", "python.function.v1")

    def _placeholder(self, columns: tuple[str, ...]) -> EmptyData:
        return EmptyData(
            cached_content_hash=_data().content_hash(),
            source_info=output_source_info(
                self.URI, row_ids.EPHEMERAL_RESULT_GONE, list(columns)
            ),
        )

    @pytest.mark.parametrize("columns", [("y", "z", "w"), ("w", "z", "y")])
    @pytest.mark.parametrize(
        "mapping", [{}, {"absent": "other"}, {"absent": "other", "gone": "more"}]
    )
    def test_a_rename_that_names_no_token_changes_nothing(
        self, columns: tuple[str, ...], mapping: dict[str, str]
    ) -> None:
        placeholder = self._placeholder(columns)
        tokens = dict(placeholder.source_info())
        table = placeholder.source_info_table()
        instance = row_ids.instance_id(HASHER, _tag(), placeholder)
        assert len(tokens) == 3

        renamed = placeholder.rename(mapping)
        if not mapping:
            assert renamed is placeholder
        assert isinstance(renamed, EmptyData)
        for row in (renamed, placeholder):
            assert row.source_info() == tokens
            assert list(row.source_info()) == list(columns)
            assert row.source_info_table().equals(table)
            assert row.source_info_table().schema.equals(table.schema)
            assert row_ids.instance_id(HASHER, _tag(), row) == instance
            assert row.content_hash() == _data().content_hash()

    @pytest.mark.parametrize("columns", [("y", "z", "w"), ("w", "z", "y")])
    def test_a_rename_that_names_tokens_moves_them(
        self, columns: tuple[str, ...]
    ) -> None:
        placeholder = self._placeholder(columns)
        tokens = dict(placeholder.source_info())
        instance = row_ids.instance_id(HASHER, _tag(), placeholder)
        mapping = {"y": "q", "w": "r", "absent": "other"}

        renamed = placeholder.rename(mapping)
        assert renamed.source_info() == {
            mapping.get(name, name): token for name, token in tokens.items()
        }
        assert sorted(renamed.source_info_table().column_names) == [
            "_source_q",
            "_source_r",
            "_source_z",
        ]
        assert row_ids.instance_id(HASHER, _tag(), renamed) != instance
        # The first row is as it was.
        assert placeholder.source_info() == tokens
        assert row_ids.instance_id(HASHER, _tag(), placeholder) == instance
