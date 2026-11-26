"""The three nested row ids and their preimages (ITL-1471).

``core/row_ids.py`` is the one place the entry id, the instance id and the record id
are built. These tests state each preimage column by column and hash it by hand, so a
change of a column name, a type or the nesting fails here.

The rows have three tags (two user tags in two orders, with two system tags) and three
data columns in two orders, each with its own source-info token.
"""

from __future__ import annotations

import uuid
from typing import Any

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core import row_ids
from orcapod.core.data_function import output_source_info
from orcapod.core.datagrams import Data, Tag
from orcapod.core.datagrams.tag_data import EmptyData
from orcapod.system_constants import constants

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
    def test_the_preimage_is_the_labelled_instance_id_and_the_result_id_bytes(
        self,
    ) -> None:
        instance = row_ids.instance_id(HASHER, _tag(), _data())
        result_id = uuid.uuid4()
        preimage = row_ids.record_id_preimage(instance, result_id)
        assert preimage.column_names == ["__pipeline_instance_id", "__data_id"]
        assert set(preimage.schema.types) == {pa.large_binary()}
        assert preimage.to_pylist() == [
            {
                "__pipeline_instance_id": instance.to_prefixed_digest(),
                "__data_id": result_id.bytes,
            }
        ]
        assert row_ids.record_id(HASHER, instance, result_id) == HASHER.hash_table(
            preimage
        )

    def test_another_result_of_one_instance_is_another_record(self) -> None:
        instance = row_ids.instance_id(HASHER, _tag(), _data())
        first, second = uuid.uuid4(), uuid.uuid4()
        assert row_ids.record_id(HASHER, instance, first) != row_ids.record_id(
            HASHER, instance, second
        )
        assert row_ids.record_id(HASHER, instance, first) == row_ids.record_id(
            HASHER, instance, first
        )

    def test_one_result_of_two_instances_is_two_records(self) -> None:
        other = {name: f"dict_v1:other::row_9::{name}" for name in DATA_VALUES}
        result_id = uuid.uuid4()
        assert row_ids.record_id(
            HASHER, row_ids.instance_id(HASHER, _tag(), _data()), result_id
        ) != row_ids.record_id(
            HASHER, row_ids.instance_id(HASHER, _tag(), _data(tokens=other)), result_id
        )

    def test_the_three_ids_carry_the_arrow_label(self) -> None:
        tag, data = _tag(), _data()
        instance = row_ids.instance_id(HASHER, tag, data)
        for value in (
            row_ids.entry_id(HASHER, tag, data),
            instance,
            row_ids.record_id(HASHER, instance, uuid.uuid4()),
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
            pa.array([[None, [[]]], [None, [[]]], [[["t"]]]], self.NESTED)
            if nested
            else pa.array(
                ["s::row_0::a", "s::row_1::a", "s::row_2::a"], pa.large_string()
            )
        )
        table = pa.Table.from_arrays(
            [
                pa.array(["m1", "m1", "m2"], pa.large_string()),
                pa.array([1, 2, 1], pa.int64()),
                pa.array([10, 20, 30], pa.int64()),
                pa.array([0.5, 1.5, 2.5], pa.float64()),
                tokens_a,
                pa.array(["s::row_0::b", "s::row_1::b", None], pa.large_string()),
            ],
            schema=pa.schema(
                [
                    pa.field("subject", pa.large_string(), nullable=False),
                    pa.field("session", pa.int64(), nullable=False),
                    pa.field("a", pa.int64(), nullable=False),
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
            if index < 2:
                # Typing this token again from its value is what fails.
                with pytest.raises(pa.ArrowTypeError):
                    data.as_table(columns={"source": True})
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
