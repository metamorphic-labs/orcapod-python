"""Regression tests for the wrong-hash findings of the hash-site audit (ITL-942).

A wrong hash is one where two different things share a hash, or one thing hashes
differently depending on data, process or spelling. Each class reproduces one finding
(letters as in Appendix A.9 of the ITL-938 design) through the public interface.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import uuid
from typing import Any

import pyarrow as pa
import pytest

from orcapod import FunctionPod, function_pod, side_effect_function_pod
from orcapod.contexts import resolve_context
from orcapod.operators import (
    Batch,
    DropDataColumns,
    DropTagColumns,
    GroupBy,
    MergeJoin,
    SelectDataColumns,
    SelectTagColumns,
)
from orcapod.sources import DictSource
from orcapod.streams import ArrowTableStream

SOURCE_ID_COL = "_tag_source_id::abc"
RECORD_ID_COL = "_tag_record_id::abc"

# Findings whose fix awaits the developer's decision (ITL-942): the tests document the
# wrong hash and must fail until the fix lands (strict xfail).
_PENDING_DICT_KEYS = pytest.mark.xfail(
    strict=True, reason="ITL-942 dict keys: fix awaiting decision"
)
_PENDING_E = pytest.mark.xfail(strict=True, reason="ITL-942 E: fix awaiting decision")


def _semantic_hasher() -> Any:
    return resolve_context(None).semantic_hasher


def _arrow_hasher() -> Any:
    return resolve_context(None).arrow_hasher


def _run_with_seed(script: str, seed: str) -> str:
    """Run ``script`` in a fresh interpreter under ``PYTHONHASHSEED=seed``; return stdout."""
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = seed
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


# ---------------------------------------------------------------------------
# H. Batch / GroupBy fold of system-tag values
# ---------------------------------------------------------------------------


def _stream_with_source_ids(source_ids: list[str | None]) -> ArrowTableStream:
    """A two-row stream in one group whose rows carry the given source ids."""
    table = pa.table(
        {
            "g": pa.array([1] * len(source_ids), type=pa.int64()),
            "k": pa.array(list(range(len(source_ids))), type=pa.int64()),
            "x": pa.array(list(range(len(source_ids))), type=pa.int64()),
            SOURCE_ID_COL: pa.array(source_ids, type=pa.large_string()),
            RECORD_ID_COL: pa.array(
                [uuid.UUID(int=i + 1).bytes for i in range(len(source_ids))],
                type=pa.large_binary(),
            ),
        }
    )
    return ArrowTableStream(table, tag_columns=["g", "k"])


def _folded_source_id(stream: Any) -> str:
    table = stream.as_table(columns={"system_tags": True})
    [col] = [c for c in table.column_names if c.startswith("_tag_source_id")]
    [value] = table.column(col).to_pylist()
    return value


class TestFoldSeparatesMembers:
    @pytest.mark.parametrize(
        "make_op", [lambda: Batch(batch_size=2), lambda: GroupBy(by=["g"])]
    )
    def test_member_boundaries_are_identity(self, make_op: Any) -> None:
        left = _folded_source_id(make_op()(_stream_with_source_ids(["ab", "c"])))
        right = _folded_source_id(make_op()(_stream_with_source_ids(["a", "bc"])))
        assert left != right

    def test_null_member_differs_from_empty_member(self) -> None:
        left = _folded_source_id(
            Batch(batch_size=2)(_stream_with_source_ids([None, "x"]))
        )
        right = _folded_source_id(
            Batch(batch_size=2)(_stream_with_source_ids(["", "x"]))
        )
        assert left != right

    def test_source_info_stays_the_list_of_member_tokens(self) -> None:
        # Only the scalar system-tag fold changed: source-info columns still list
        # each member's own provenance token, in member order.
        source = DictSource(
            data=[{"g": 1, "k": 1, "x": 10}, {"g": 1, "k": 2, "x": 20}],
            tag_columns=["g", "k"],
            source_id="members",
        )
        member_tokens = (
            source.as_table(columns={"source": True}).column("_source_x").to_pylist()
        )
        for op in (Batch(batch_size=2), GroupBy(by=["g"])):
            table = op(source).as_table(columns={"source": True})
            assert table.column("_source_x").to_pylist() == [member_tokens]
            assert table.column("x").to_pylist() == [[10, 20]]

    def test_member_order_is_identity(self) -> None:
        left = _folded_source_id(
            Batch(batch_size=2)(_stream_with_source_ids(["a", "b"]))
        )
        right = _folded_source_id(
            Batch(batch_size=2)(_stream_with_source_ids(["b", "a"]))
        )
        assert left != right


# ---------------------------------------------------------------------------
# I. Arrow hashing of nested types does not depend on the data
# ---------------------------------------------------------------------------

_NON_NULL_STRUCT = pa.struct([pa.field("a", pa.int64(), nullable=False)])
_NULLABLE_STRUCT = pa.struct([pa.field("a", pa.int64(), nullable=True)])
_NON_NULL_LIST = pa.large_list(pa.field("item", pa.int64(), nullable=False))
_NULLABLE_LIST = pa.large_list(pa.field("item", pa.int64(), nullable=True))


class TestNestedNullabilityIsIdentity:
    @pytest.mark.parametrize(
        ("non_null", "nullable", "value"),
        [
            (_NON_NULL_STRUCT, _NULLABLE_STRUCT, {"a": 1}),
            (_NON_NULL_LIST, _NULLABLE_LIST, [1]),
            (
                pa.large_list(_NON_NULL_STRUCT),
                pa.large_list(_NULLABLE_STRUCT),
                [{"a": 1}],
            ),
        ],
        ids=["struct", "list", "list_of_struct"],
    )
    @pytest.mark.parametrize("rows", [0, 1], ids=["zero_rows", "with_data"])
    def test_nested_nullability_hashes_with_and_without_data(
        self, non_null: pa.DataType, nullable: pa.DataType, value: Any, rows: int
    ) -> None:
        hasher = _arrow_hasher()
        a = pa.table({"c": pa.array([value] * rows, type=non_null)})
        b = pa.table({"c": pa.array([value] * rows, type=nullable)})
        assert hasher.hash_table(a) != hasher.hash_table(b)

    def test_nested_type_hash_does_not_depend_on_first_row(self) -> None:
        # The same column type, once with a null first row: the type that is hashed must
        # not change with the data.
        hasher = _arrow_hasher()
        first = pa.table({"c": pa.array([{"a": 1}], type=_NON_NULL_STRUCT)})
        nulls = pa.table({"c": pa.array([None], type=_NON_NULL_STRUCT)})
        mixed = pa.table({"c": pa.array([None, {"a": 1}], type=_NON_NULL_STRUCT)})
        # Hash of the schema each table is hashed under: compare against tables that only
        # differ in nested nullability; with data or without, the flag must count.
        nulls_nullable = pa.table({"c": pa.array([None], type=_NULLABLE_STRUCT)})
        mixed_nullable = pa.table(
            {"c": pa.array([None, {"a": 1}], type=_NULLABLE_STRUCT)}
        )
        first_nullable = pa.table({"c": pa.array([{"a": 1}], type=_NULLABLE_STRUCT)})
        assert hasher.hash_table(nulls) != hasher.hash_table(nulls_nullable)
        assert hasher.hash_table(mixed) != hasher.hash_table(mixed_nullable)
        assert hasher.hash_table(first) != hasher.hash_table(first_nullable)

    def test_list_item_name_is_not_identity(self) -> None:
        # starfix ignores list item names (pod code cannot observe them); keep it so.
        hasher = _arrow_hasher()
        item = pa.large_list(pa.field("item", pa.int64()))
        element = pa.large_list(pa.field("element", pa.int64()))
        for rows in (0, 1):
            a = pa.table({"c": pa.array([[1]] * rows, type=item)})
            b = pa.table({"c": pa.array([[1]] * rows, type=element)})
            assert hasher.hash_table(a) == hasher.hash_table(b)


# ---------------------------------------------------------------------------
# L. Structure hasher: set order and function defaults are process independent
# ---------------------------------------------------------------------------

_SEEDS = ("1", "2", "3", "4", "5", "6")


class TestStructureHasherProcessIndependence:
    def test_set_with_str_equal_items(self) -> None:
        script = """
            from orcapod.contexts import resolve_context
            print(resolve_context(None).semantic_hasher.hash_object({1, "1", 2, "2"}).to_string())
        """
        assert len({_run_with_seed(script, seed) for seed in _SEEDS}) == 1

    def test_set_with_str_equal_items_differs_from_set_without(self) -> None:
        hasher = _semantic_hasher()
        assert hasher.hash_object({1, "1"}) != hasher.hash_object({1})
        assert hasher.hash_object({1, "1"}) != hasher.hash_object({"1"})


# ---------------------------------------------------------------------------
# Dict keys keep their type
# ---------------------------------------------------------------------------


class TestDictKeysKeepTheirType:
    @_PENDING_DICT_KEYS
    def test_int_key_differs_from_str_key(self) -> None:
        hasher = _semantic_hasher()
        assert hasher.hash_object({1: "x"}) != hasher.hash_object({"1": "x"})

    @_PENDING_DICT_KEYS
    def test_keys_with_equal_str_do_not_collapse(self) -> None:
        hasher = _semantic_hasher()
        both = {1: "a", "1": "b"}
        assert hasher.hash_object(both) != hasher.hash_object({"1": "b"})
        assert hasher.hash_object(both) != hasher.hash_object({1: "a"})

    @_PENDING_DICT_KEYS
    def test_tuple_differs_from_its_tagged_dict_spelling(self) -> None:
        hasher = _semantic_hasher()
        assert hasher.hash_object((1, 2)) != hasher.hash_object(
            {"__type__": "tuple", "items": [1, 2]}
        )

    def test_str_keyed_dict_insertion_order_still_ignored(self) -> None:
        hasher = _semantic_hasher()
        assert hasher.hash_object({"a": 1, "b": 2}) == hasher.hash_object(
            {"b": 2, "a": 1}
        )
        assert hasher.hash_object({1: "a", 2: "b"}) == hasher.hash_object(
            {2: "b", 1: "a"}
        )


# ---------------------------------------------------------------------------
# F. Column selection identity is the set of names
# ---------------------------------------------------------------------------


def _xy_source() -> DictSource:
    return DictSource(
        data=[{"k": 1, "j": 2, "x": 1, "y": 2}], tag_columns=["k", "j"], source_id="s"
    )


class TestColumnSelectionIdentity:
    @pytest.mark.parametrize("op_cls", [SelectDataColumns, DropDataColumns])
    def test_data_column_spellings_share_identity(self, op_cls: Any) -> None:
        source = _xy_source()
        spellings = [["x", "y"], ("x", "y"), {"x", "y"}, ["y", "x"], ["x", "y", "x"]]
        streams = [op_cls(columns=c)(source) for c in spellings]
        assert len({s.pipeline_hash() for s in streams}) == 1
        assert len({s.content_hash() for s in streams}) == 1

    @pytest.mark.parametrize("op_cls", [SelectTagColumns, DropTagColumns])
    def test_tag_column_spellings_share_identity(self, op_cls: Any) -> None:
        source = _xy_source()
        spellings = [["k"], ("k",), {"k"}, "k", ["k", "k"]]
        streams = [op_cls(columns=c)(source) for c in spellings]
        assert len({s.pipeline_hash() for s in streams}) == 1

    def test_argument_order_is_not_observable(self) -> None:
        source = _xy_source()
        a = SelectDataColumns(columns=["x", "y"])(source).as_table()
        b = SelectDataColumns(columns=["y", "x"])(source).as_table()
        assert a.column_names == b.column_names

    def test_caller_list_is_not_aliased(self) -> None:
        source = _xy_source()
        columns = ["x"]
        stream = SelectDataColumns(columns=columns)(source)
        before = stream.pipeline_hash()
        columns.append("y")
        assert SelectDataColumns(columns=["x"])(source).pipeline_hash() == before


# ---------------------------------------------------------------------------
# E. Commutative operators keep input multiplicity
# ---------------------------------------------------------------------------


def _weights(source_id: str, weight: float) -> DictSource:
    return DictSource(
        data=[{"k": 1, "w": weight}], tag_columns=["k"], source_id=source_id
    )


class TestCommutativeInputsAreAMultiset:
    @_PENDING_E
    def test_self_merge_join_pipeline_hash_is_schema_and_topology(self) -> None:
        m1 = _weights("m", 1.0)
        m2 = _weights("m2", 3.0)
        assert (
            MergeJoin()(m1, m1).pipeline_hash() == MergeJoin()(m1, m2).pipeline_hash()
        )

    def test_input_order_is_still_not_identity(self) -> None:
        m1 = _weights("m", 1.0)
        m2 = _weights("m2", 3.0)
        assert MergeJoin()(m1, m2).content_hash() == MergeJoin()(m2, m1).content_hash()
        assert (
            MergeJoin()(m1, m2).pipeline_hash() == MergeJoin()(m2, m1).pipeline_hash()
        )

    def test_multiplicity_is_identity(self) -> None:
        m1 = _weights("m", 1.0)
        m2 = _weights("m2", 3.0)
        assert MergeJoin()(m1, m1).content_hash() != MergeJoin()(m1, m2).content_hash()


# ---------------------------------------------------------------------------
# G. The record-id system tag keeps one Arrow type
# ---------------------------------------------------------------------------


def _record_id_types(table: pa.Table) -> set[pa.DataType]:
    return {f.type for f in table.schema if f.name.startswith("_tag_record_id")}


class TestRecordIdType:
    def test_source_stream_and_tag_agree(self) -> None:
        source = DictSource(
            data=[{"k": 1, "x": 1}, {"k": 2, "x": 2}], tag_columns=["k"], source_id="s"
        )
        stream_types = _record_id_types(source.as_table(columns={"system_tags": True}))
        tag, _ = next(iter(source.iter_data()))
        tag_types = _record_id_types(tag.as_table(columns={"system_tags": True}))
        assert stream_types == tag_types == {pa.large_binary()}

    def test_batch_keeps_the_type(self) -> None:
        source = DictSource(
            data=[{"k": 1, "x": 1}, {"k": 2, "x": 2}], tag_columns=["k"], source_id="s"
        )
        batched = Batch(batch_size=2)(source)
        assert _record_id_types(batched.as_table(columns={"system_tags": True})) == {
            pa.large_binary()
        }

    def test_operator_cache_keys_match_on_sync_and_async_paths(self) -> None:
        # OperatorJobNode is driven directly: the test is about its two execution
        # paths (sync run and async_execute), which the cache key must not tell apart.
        import asyncio

        from orcapod.channels import Channel
        from orcapod.core.nodes.operator_node import OperatorJobNode
        from orcapod.databases import InMemoryArrowDatabase
        from orcapod.types import CacheMode

        def source() -> DictSource:
            return DictSource(
                data=[{"k": i, "x": i, "y": i} for i in range(2)],
                tag_columns=["k"],
                source_id="s",
            )

        def stored_keys(db: Any, node: Any) -> list[bytes]:
            table = db.get_all_records(
                node.node_identity_path, record_id_column="_record_hash"
            )
            return sorted(table.column("_record_hash").to_pylist())

        sync_db = InMemoryArrowDatabase()
        sync_node = OperatorJobNode(
            SelectDataColumns(["x"]),
            [source()],
            pipeline_database=sync_db,
            cache_mode=CacheMode.LOG,
        )
        sync_node.run()

        async def run_async() -> tuple[Any, Any]:
            db = InMemoryArrowDatabase()
            node = OperatorJobNode(
                SelectDataColumns(["x"]),
                [source()],
                pipeline_database=db,
                cache_mode=CacheMode.LOG,
            )
            inputs, outputs = Channel(buffer_size=16), Channel(buffer_size=16)
            for item in source().iter_data():
                await inputs.writer.send(item)
            await inputs.writer.close()
            await node.async_execute([inputs.reader], outputs.writer)
            await outputs.reader.collect()
            return db, node

        async_db, async_node = asyncio.run(run_async())
        assert stored_keys(sync_db, sync_node) == stored_keys(async_db, async_node)


# ---------------------------------------------------------------------------
# N. One default function version, whatever the entry point
# ---------------------------------------------------------------------------


def _inc(x: int) -> int:
    return x + 1


class TestDefaultFunctionVersion:
    def test_decorator_and_from_fn_agree(self) -> None:
        decorated = function_pod(output_keys="y", function_name="_inc")(_inc)
        built = FunctionPod.from_fn(_inc, output_keys="y", name="_inc")
        assert decorated.pod.uri == built.uri

    def test_side_effect_decorator_agrees(self) -> None:
        def _inc_ctx(x: int, ctx: Any) -> int:
            return x + 1

        pod = side_effect_function_pod(_inc_ctx, output_keys="y")
        decorated = function_pod(
            output_keys="y", function_name="_inc_ctx", ctx_arg="ctx"
        )(_inc_ctx)
        assert pod.uri == decorated.pod.uri
