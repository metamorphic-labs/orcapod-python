"""A dict's identity does not depend on its key order (ITL-905).

A ``dict[K, V]`` column is stored as ``large_list<struct<key, value>>``. Equal Python
dicts must give equal row hashes whatever order their entries were inserted in, on every
path that turns a Python dict into Arrow: sources, nested dicts, dataclass fields,
function pod outputs (sync and async), and cached results read back from a store. An
Arrow table that already holds a key/value list is hashed exactly as stored.
"""

from __future__ import annotations

import tarfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.sources import ArrowTableSource, DictSource

V020_STORE = Path(__file__).parent / "data" / "v0_2_0_dict_store.tar.gz"

KV_TYPE = pa.large_list(
    pa.struct([pa.field("key", pa.large_string()), pa.field("value", pa.int64())])
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _source(value: Any, value_type: Any = None) -> DictSource:
    schema = None if value_type is None else {"k": str, "m": value_type}
    return DictSource(
        data=[{"k": "r", "m": value}], tag_columns=["k"], data_schema=schema
    )


def _data(value: Any, value_type: Any = None) -> Any:
    _, data = next(iter(_source(value, value_type).iter_data()))
    return data


def _row_hash(value: Any, value_type: Any = None) -> str:
    return _data(value, value_type).content_hash().to_string()


def _arrow_source(entries: list[dict[str, Any]]) -> ArrowTableSource:
    # Non-nullable like a DictSource column declared ``dict[str, int]``.
    table = pa.table(
        {
            "k": pa.array(["r"], pa.large_string()),
            "m": pa.array([entries], KV_TYPE),
        },
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("m", KV_TYPE, nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["k"], source_id="itl905")


def _arrow_row_hash(entries: list[dict[str, Any]]) -> str:
    _, data = next(iter(_arrow_source(entries).iter_data()))
    return data.content_hash().to_string()


def total(m: dict[str, int]) -> int:
    return sum(m.values())


def echo(m: dict[str, int]) -> dict[str, int]:
    return m


def reorder(m: dict[str, int]) -> dict[str, int]:
    """Return an equal dict in reverse insertion order."""
    return dict(reversed(list(m.items())))


def _pod(fn: Any, output_key: str) -> FunctionPod:
    return FunctionPod(PythonDataFunction(fn, output_keys=output_key))


@dataclass
class Holder:
    """A dataclass with a dict field: dicts reach Arrow through the logical type."""

    name: str
    counts: dict[str, int]


# ---------------------------------------------------------------------------
# Python values through the converter
# ---------------------------------------------------------------------------


class TestPythonDictValues:
    def test_inferred_schema_reordered_dicts_hash_equal(self):
        assert _row_hash({"a": 1, "b": 2}) == _row_hash({"b": 2, "a": 1})

    def test_declared_schema_reordered_dicts_hash_equal(self):
        t = dict[str, int]
        assert _row_hash({"a": 1, "b": 2}, t) == _row_hash({"b": 2, "a": 1}, t)

    def test_stored_entries_are_in_key_order(self):
        table = _source({"b": 2, "a": 1}).as_table()
        assert [e["key"] for e in table.column("m").to_pylist()[0]] == ["a", "b"]

    def test_str_keys_order_by_code_point_not_locale(self):
        table = _source({"é": 1, "z": 2, "A": 3}).as_table()
        assert [e["key"] for e in table.column("m").to_pylist()[0]] == ["A", "z", "é"]

    def test_different_values_still_hash_differently(self):
        assert _row_hash({"a": 1, "b": 2}) != _row_hash({"a": 2, "b": 1})

    @pytest.mark.parametrize(
        ("value_type", "first", "second"),
        [
            (dict[int, str], {10: "a", 9: "b", -1: "c"}, {-1: "c", 9: "b", 10: "a"}),
            (dict[bool, int], {True: 1, False: 0}, {False: 0, True: 1}),
            (dict[bytes, int], {b"\xff": 1, b"\x00": 2}, {b"\x00": 2, b"\xff": 1}),
            (dict[float, int], {1.5: 1, -0.0: 2, -3.0: 3}, {-3.0: 3, -0.0: 2, 1.5: 1}),
            (
                dict[date, int],
                {date(2026, 1, 2): 1, date(2025, 5, 1): 2},
                {date(2025, 5, 1): 2, date(2026, 1, 2): 1},
            ),
            (
                dict[datetime, int],
                {
                    datetime(2026, 1, 2, tzinfo=timezone.utc): 1,
                    datetime(2025, 5, 1, tzinfo=timezone.utc): 2,
                },
                {
                    datetime(2025, 5, 1, tzinfo=timezone.utc): 2,
                    datetime(2026, 1, 2, tzinfo=timezone.utc): 1,
                },
            ),
        ],
    )
    def test_key_types_reordered_hash_equal(self, value_type, first, second):
        assert _row_hash(first, value_type) == _row_hash(second, value_type)

    def test_nan_key_is_rejected_naming_the_column(self):
        with pytest.raises(ValueError, match="'m'.*NaN|NaN.*'m'"):
            _row_hash({float("nan"): 1, 1.0: 2}, dict[float, int])


class TestNestedDicts:
    def test_dict_of_dicts_inner_and_outer_reordered(self):
        t = dict[str, dict[str, int]]
        first = {"x": {"a": 1, "b": 2}, "y": {"c": 3}}
        second = {"y": {"c": 3}, "x": {"b": 2, "a": 1}}
        assert _row_hash(first, t) == _row_hash(second, t)

    def test_list_of_dicts_reordered_inside(self):
        t = list[dict[str, int]]
        assert _row_hash([{"a": 1, "b": 2}, {"c": 3}], t) == _row_hash(
            [{"b": 2, "a": 1}, {"c": 3}], t
        )

    def test_list_of_dicts_keeps_list_order(self):
        t = list[dict[str, int]]
        assert _row_hash([{"a": 1}, {"c": 3}], t) != _row_hash([{"c": 3}, {"a": 1}], t)

    def test_dict_of_list_of_dicts_reordered(self):
        t = dict[str, list[dict[str, int]]]
        first = {"x": [{"a": 1, "b": 2}], "w": []}
        second = {"w": [], "x": [{"b": 2, "a": 1}]}
        assert _row_hash(first, t) == _row_hash(second, t)

    def test_dict_inside_dataclass_field_reordered(self):
        first = Holder("h", {"a": 1, "b": 2})
        second = Holder("h", {"b": 2, "a": 1})
        assert _row_hash(first, Holder) == _row_hash(second, Holder)


# ---------------------------------------------------------------------------
# Arrow tables that already hold key/value lists
# ---------------------------------------------------------------------------


class TestArrowNativeKeyValueLists:
    """An Arrow table is hashed exactly as stored (developer, November 11, 2026).

    A ``list<struct<key, value>>`` that arrives already in Arrow form is an ordered list
    at the Arrow level; Python reads it as a dict, but its stored order is part of its
    hash. Canonical order is established only where a Python dict becomes Arrow.
    """

    def test_unsorted_entries_hash_as_stored(self):
        sorted_entries = [{"key": "a", "value": 1}, {"key": "b", "value": 2}]
        unsorted_entries = [{"key": "b", "value": 2}, {"key": "a", "value": 1}]
        assert _arrow_row_hash(sorted_entries) != _arrow_row_hash(unsorted_entries)

    def test_key_sorted_entries_match_python_dicts_in_any_order(self):
        sorted_entries = [{"key": "a", "value": 1}, {"key": "b", "value": 2}]
        t = dict[str, int]
        assert _arrow_row_hash(sorted_entries) == _row_hash({"b": 2, "a": 1}, t)
        assert _arrow_row_hash(sorted_entries) == _row_hash({"a": 1, "b": 2}, t)

    def test_user_table_is_not_rewritten(self):
        unsorted_entries = [{"key": "b", "value": 2}, {"key": "a", "value": 1}]
        source = _arrow_source(unsorted_entries)
        _, data = next(iter(source.iter_data()))
        data.content_hash()
        assert source.as_table().column("m").to_pylist()[0] == unsorted_entries

    def test_ordered_struct_pairs_are_not_sorted(self):
        pair_type = pa.large_list(
            pa.struct([pa.field("f0", pa.large_string()), pa.field("f1", pa.int64())])
        )

        def pair_hash(entries: list[dict[str, Any]]) -> str:
            table = pa.table(
                {
                    "k": pa.array(["r"], pa.large_string()),
                    "m": pa.array([entries], pair_type),
                }
            )
            src = ArrowTableSource(table, tag_columns=["k"], source_id="pairs")
            _, data = next(iter(src.iter_data()))
            return data.content_hash().to_string()

        assert pair_hash([{"f0": "a", "f1": 1}, {"f0": "b", "f1": 2}]) != pair_hash(
            [{"f0": "b", "f1": 2}, {"f0": "a", "f1": 1}]
        )


# ---------------------------------------------------------------------------
# Function pods: outputs, async, caches
# ---------------------------------------------------------------------------


class TestFunctionPodDictOutputs:
    def test_pod_output_reordered_dicts_hash_equal(self):
        source = _source({"a": 1, "b": 2}, dict[str, int])
        [(_, plain)] = list(_pod(echo, "out").process(source).iter_data())
        [(_, flipped)] = list(_pod(reorder, "out").process(source).iter_data())
        assert plain.content_hash() == flipped.content_hash()

    def test_fresh_pod_output_iterates_in_key_order(self):
        source = _source({"b": 2, "a": 1}, dict[str, int])
        [(_, out)] = list(_pod(echo, "out").process(source).iter_data())
        assert list(out.as_dict()["out"]) == ["a", "b"]

    def test_pod_does_not_mutate_the_returned_dict(self):
        returned: list[dict[str, int]] = []

        def keep(m: dict[str, int]) -> dict[str, int]:
            result = {"b": m["b"], "a": m["a"]}
            returned.append(result)
            return result

        source = _source({"a": 1, "b": 2}, dict[str, int])
        list(_pod(keep, "out").process(source).iter_data())
        assert list(returned[0]) == ["b", "a"]

    @pytest.mark.asyncio
    async def test_async_output_matches_sync_and_is_order_independent(self):
        source = _source({"a": 1, "b": 2}, dict[str, int])
        tag, data = next(iter(source.iter_data()))
        _, sync_out = _pod(echo, "out").process_data(tag, data)
        _, async_plain = await _pod(echo, "out").async_process_data(tag, data)
        _, async_flipped = await _pod(reorder, "out").async_process_data(tag, data)
        assert sync_out is not None
        assert async_plain is not None
        assert async_flipped is not None
        assert sync_out.content_hash() == async_plain.content_hash()
        assert async_plain.content_hash() == async_flipped.content_hash()
        assert list(async_flipped.as_dict()["out"]) == ["a", "b"]


def _delta_db(tmp_path: Path) -> DeltaTableDatabase:
    return DeltaTableDatabase(tmp_path / "store")


@pytest.fixture(params=["in_memory", "delta"])
def result_db(request, tmp_path):
    if request.param == "in_memory":
        return InMemoryArrowDatabase()
    return _delta_db(tmp_path)


class TestCachedPodHit:
    def test_reordered_equal_dict_is_a_cache_hit(self, result_db):
        pod = CachedFunctionPod(_pod(total, "total"), result_database=result_db)
        first = _source({"b": 2, "a": 1}, dict[str, int])
        second = _source({"a": 1, "b": 2}, dict[str, int])

        [(_, out1)] = list(pod.process(first).iter_data())
        result_db.flush()
        [(_, out2)] = list(pod.process(second).iter_data())

        assert out1.get_meta_value(pod.RESULT_COMPUTED_FLAG) is True
        assert out2.get_meta_value(pod.RESULT_COMPUTED_FLAG) is False
        records = result_db.get_all_records(pod.record_path)
        assert records is not None
        assert records.num_rows == 1

    @pytest.mark.asyncio
    async def test_reordered_equal_dict_is_an_async_cache_hit(self, result_db):
        pod = CachedFunctionPod(_pod(total, "total"), result_database=result_db)
        tag1, data1 = next(iter(_source({"b": 2, "a": 1}, dict[str, int]).iter_data()))
        tag2, data2 = next(iter(_source({"a": 1, "b": 2}, dict[str, int]).iter_data()))

        _, out1 = await pod.async_process_data(tag1, data1)
        result_db.flush()
        _, out2 = await pod.async_process_data(tag2, data2)

        assert out1 is not None
        assert out2 is not None
        assert out1.get_meta_value(pod.RESULT_COMPUTED_FLAG) is True
        assert out2.get_meta_value(pod.RESULT_COMPUTED_FLAG) is False

    def test_fresh_and_cached_dict_outputs_iterate_identically(self, result_db):
        pod = CachedFunctionPod(_pod(reorder, "out"), result_database=result_db)
        source = _source({"a": 1, "b": 2, "c": 3}, dict[str, int])

        [(_, fresh)] = list(pod.process(source).iter_data())
        result_db.flush()
        [(_, cached)] = list(pod.process(source).iter_data())

        assert cached.get_meta_value(pod.RESULT_COMPUTED_FLAG) is False
        assert list(fresh.as_dict()["out"]) == list(cached.as_dict()["out"])
        assert list(cached.as_dict()["out"]) == ["a", "b", "c"]
        assert fresh.content_hash() == cached.content_hash()


class TestV020Store:
    """A Delta result store written by the released v0.2.0 code.

    ``data/v0_2_0_dict_store.tar.gz`` was produced by v0.2.0 running ``total`` over two
    inputs: ``k=unsorted`` holding ``{"b": 2, "a": 1}`` (stored in that order, so its
    v0.2.0 input hash is order dependent) and ``k=sorted`` holding ``{"a": 1, "c": 3}``
    (already in key order, so its hash does not move). ``data/generate_v0_2_0_dict_store.py``
    regenerates it from a checkout of the ``v0.2.0`` tag.
    """

    @pytest.fixture
    def store(self, tmp_path):
        # The store is shipped as a tarball: its directory names contain ":".
        target = tmp_path / "v020_store"
        with tarfile.open(V020_STORE) as archive:
            archive.extractall(target, filter="data")
        return DeltaTableDatabase(target)

    def _inputs(self) -> DictSource:
        return DictSource(
            data=[
                {"k": "unsorted", "m": {"b": 2, "a": 1}},
                {"k": "sorted", "m": {"a": 1, "c": 3}},
            ],
            tag_columns=["k"],
            data_schema={"k": str, "m": dict[str, int]},
            source_id="itl905_v020",
        )

    def test_unsorted_row_recomputes_once_and_old_rows_stay(self, store):
        pod = CachedFunctionPod(_pod(total, "total"), result_database=store)
        before = store.get_all_records(pod.record_path)
        assert before is not None
        assert before.num_rows == 2

        results = {
            tag.as_dict()["k"]: out
            for tag, out in pod.process(self._inputs()).iter_data()
        }
        store.flush()

        assert results["sorted"].get_meta_value(pod.RESULT_COMPUTED_FLAG) is False
        assert results["unsorted"].get_meta_value(pod.RESULT_COMPUTED_FLAG) is True
        assert results["unsorted"].as_dict()["total"] == 3
        after = store.get_all_records(pod.record_path)
        assert after is not None
        assert after.num_rows == 3  # both old rows kept, one new row

        again = {
            tag.as_dict()["k"]: out
            for tag, out in pod.process(self._inputs()).iter_data()
        }
        assert all(
            out.get_meta_value(pod.RESULT_COMPUTED_FLAG) is False
            for out in again.values()
        )
