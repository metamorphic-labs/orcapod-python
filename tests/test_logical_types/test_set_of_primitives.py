"""A ``set[T]`` of a primitive element type keeps its set identity (ITL-611).

``set[T]`` is stored as the native set container, an Arrow extension type named
``set[<element Arrow type>]``, whatever ``T`` is; ``list[T]`` of a primitive stays a plain
``large_list``. So a set reads back as a set everywhere: from the node, in a downstream
pod, from ``as_source()`` and from a job loaded in a fresh process; and a set row never
hashes like the list row with the same elements.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.sources import DictSource

UTC = dt.timezone.utc

# element type -> (one set value, the large-normalized Arrow type text of the element)
PRIMITIVE_SETS: dict[str, tuple[Any, Any, str]] = {
    "int": (int, {3, 1, 2, 10}, "int64"),
    "str": (str, {"b", "a", "10", "2"}, "large_string"),
    "float": (float, {1.5, -2.5, 10.0}, "double"),
    "bool": (bool, {True, False}, "bool"),
    "bytes": (bytes, {b"y", b"x", b"\xff"}, "large_binary"),
    "datetime": (
        dt.datetime,
        {dt.datetime(2026, 1, 2, tzinfo=UTC), dt.datetime(2025, 1, 2, tzinfo=UTC)},
        "timestamp[us, tz=UTC]",
    ),
    "date": (dt.date, {dt.date(2026, 1, 2), dt.date(2025, 1, 2)}, "date32[day]"),
}


def _producer(declared: Any, value: Any) -> Any:
    """Return a function pod with one input ``i`` that returns ``value`` as ``out``."""

    def produce(i: int) -> Any:
        return value

    produce.__annotations__["return"] = declared
    return op.function_pod(output_keys="out")(produce)


def _consumer(declared: Any) -> Any:
    """Return a function pod that reports the Python type of its ``out`` input."""

    def consume(out: Any) -> str:
        if isinstance(out, list):
            return "list[" + ",".join(type(x).__name__ for x in out) + "]"
        return type(out).__name__

    consume.__annotations__["out"] = declared
    return op.function_pod(output_keys="got")(consume)


def _source() -> DictSource:
    return DictSource([{"k": 0, "i": 0}], tag_columns=["k"], source_id="one_row")


def _run_job(database: Any, declared: Any, value: Any, *, use_async: bool) -> Any:
    """Run a job ``produce -> consume`` and return it."""
    job = op.PipelineJob(name="sets", database=database)
    with job:
        produced = _producer(declared, value).pod(_source(), label="produce")
        _consumer(declared).pod(produced, label="consume")
    if use_async:
        job.run_async()
    else:
        job.run()
    return job


def _out_values(stream: Any) -> list[Any]:
    return [data.as_dict()["out"] for _, data in stream.iter_data()]


def _database(kind: str, tmp_path: Path) -> Any:
    if kind == "delta":
        return DeltaTableDatabase(tmp_path / "store")
    return InMemoryArrowDatabase()


@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("database_kind", ["memory", "delta"])
@pytest.mark.parametrize("element", list(PRIMITIVE_SETS))
class TestJobRoundTrip:
    """A set of a primitive reads back as a set from every reader in the process."""

    def test_node_downstream_pod_and_as_source_read_a_set(
        self, element: str, database_kind: str, use_async: bool, tmp_path: Path
    ) -> None:
        element_type, value, _ = PRIMITIVE_SETS[element]
        declared = set[element_type]
        job = _run_job(
            _database(database_kind, tmp_path), declared, value, use_async=use_async
        )
        node = job.nodes["produce"]
        assert _out_values(node) == [value]
        assert all(isinstance(v, set) for v in _out_values(node))

        got = job.nodes["consume"].as_table().column("got").to_pylist()
        assert got == ["set"]

        stored = node.as_source()
        assert stored.output_schema()[1]["out"] == declared
        assert _out_values(stored) == [value]
        assert all(isinstance(v, set) for v in _out_values(stored))


@pytest.mark.parametrize("element", list(PRIMITIVE_SETS))
def test_stored_arrow_type_is_the_set_container(element: str) -> None:
    """The stored column is ``extension<set[<element Arrow type>]>`` over ``large_list``."""
    element_type, value, element_text = PRIMITIVE_SETS[element]
    job = _run_job(InMemoryArrowDatabase(), set[element_type], value, use_async=False)
    arrow_type = job.nodes["produce"].as_table().schema.field("out").type
    assert isinstance(arrow_type, pa.ExtensionType)
    assert arrow_type.extension_name == f"set[{element_text}]"
    assert json.loads(arrow_type.__arrow_ext_serialize__()) == {
        "category": "set",
        "element_arrow_type": element_text,
    }
    assert pa.types.is_large_list(arrow_type.storage_type)


@pytest.mark.parametrize(
    ("declared", "value", "name", "metadata"),
    [
        (
            set[int],
            {2, 1},
            "set[int64]",
            b'{"category": "set", "element_arrow_type": "int64"}',
        ),
        (
            set[str],
            {"x"},
            "set[large_string]",
            b'{"category": "set", "element_arrow_type": "large_string"}',
        ),
        (
            set[bytes],
            {b"x"},
            "set[large_binary]",
            b'{"category": "set", "element_arrow_type": "large_binary"}',
        ),
    ],
    ids=["int", "str", "bytes"],
)
def test_identifier_bytes(
    declared: Any, value: Any, name: str, metadata: bytes
) -> None:
    """The identifier of a set of a primitive: exact name and metadata, no version key."""
    source = DictSource(
        [{"k": 0, "s": value}],
        tag_columns=["k"],
        data_schema={"k": int, "s": declared},
    )
    arrow_type = source.as_table().schema.field("s").type
    assert arrow_type.extension_name == name
    assert arrow_type.__arrow_ext_serialize__() == metadata


@pytest.mark.parametrize(
    ("declared", "value", "arrow_type"),
    [
        (list[int], [3, 1, 2], pa.large_list(pa.int64())),
        (list[str], ["b", "a"], pa.large_list(pa.large_string())),
        (list[bytes], [b"y", b"x"], pa.large_list(pa.large_binary())),
    ],
    ids=["int", "str", "bytes"],
)
def test_list_of_a_primitive_is_unchanged(
    declared: Any, value: Any, arrow_type: pa.DataType
) -> None:
    """``list[T]`` of a primitive stays a plain ``large_list`` and reads back as a list."""
    job = _run_job(InMemoryArrowDatabase(), declared, value, use_async=False)
    node = job.nodes["produce"]
    assert node.as_table().schema.field("out").type == arrow_type
    assert _out_values(node) == [value]
    assert _out_values(node.as_source()) == [value]
    assert job.nodes["consume"].as_table().column("got").to_pylist() == [
        "list[" + ",".join(type(x).__name__ for x in value) + "]"
    ]


def _row_hash(declared: Any, value: Any) -> Any:
    source = DictSource(
        [{"k": 0, "s": value}],
        tag_columns=["k"],
        data_schema={"k": int, "s": declared},
        source_id="row_hash",
    )
    _, data = next(iter(source.iter_data()))
    return data.content_hash()


def test_a_set_row_does_not_hash_like_the_list_row() -> None:
    """``{1, 2, 3}`` as ``set[int]`` and ``[1, 2, 3]`` as ``list[int]`` are two rows."""
    assert _row_hash(set[int], {3, 1, 2}) != _row_hash(list[int], [1, 2, 3])


def test_equal_sets_hash_equal() -> None:
    """The stored order is canonical, so equal sets give one row hash."""
    assert _row_hash(set[str], {"a", "b", "c"}) == _row_hash(set[str], {"c", "a", "b"})


@pytest.mark.parametrize(
    ("declared", "value", "expected"),
    [
        (list[set[int]], [{3, 1}, {2}], "list[set,set]"),
        (set[int] | None, {3, 1, 2}, "set"),
    ],
    ids=["list_of_sets", "optional_set"],
)
def test_nested_and_optional_sets_reach_a_downstream_pod_as_sets(
    declared: Any, value: Any, expected: str
) -> None:
    """A set inside a list, and an optional set, reach a downstream pod as sets."""
    job = _run_job(InMemoryArrowDatabase(), declared, value, use_async=False)
    assert job.nodes["consume"].as_table().column("got").to_pylist() == [expected]
    assert _out_values(job.nodes["produce"].as_source()) == [value]


def test_a_set_inside_a_dict_value_is_still_stored() -> None:
    """``dict[str, set[int]]`` keeps working (plain storage inside the dict value)."""
    job = _run_job(
        InMemoryArrowDatabase(), dict[str, set[int]], {"a": {2, 1}}, use_async=False
    )
    assert len(job.nodes["produce"].as_table()) == 1


def test_frozenset_stays_refused() -> None:
    """``frozenset[T]`` is not a supported annotation."""
    with pytest.raises(ValueError, match="frozenset"):
        _run_job(
            InMemoryArrowDatabase(), frozenset[int], frozenset({1}), use_async=False
        )


_FRESH_READ = textwrap.dedent(
    """
    import json, sys
    from orcapod import PipelineJob
    from orcapod.databases import DeltaTableDatabase

    job = PipelineJob.load(sys.argv[1], database=DeltaTableDatabase(sys.argv[2]))
    stored = job.nodes["produce"].as_source()
    _, data = next(iter(stored.iter_data()))
    value = data.as_dict()["out"]
    print(json.dumps({
        "schema": str(stored.output_schema()[1]["out"]),
        "python_type": type(value).__name__,
        "elements": sorted(repr(v) for v in value),
        "row_hash": data.content_hash().to_string(),
    }))
    """
)

FRESH_CASES: dict[str, tuple[Any, Any]] = {
    "int": (set[int], {3, 1, 2}),
    "str": (set[str], {"b", "a"}),
    "bytes": (set[bytes], {b"y", b"x"}),
    "path": (set[pathlib.Path], {pathlib.Path("/a/b"), pathlib.Path("/a/c")}),
}


@pytest.mark.parametrize("case", list(FRESH_CASES))
def test_a_loaded_job_in_a_fresh_process_reads_a_set(case: str, tmp_path: Path) -> None:
    """A job loaded in another process restores the set type, values and row hash."""
    declared, value = FRESH_CASES[case]
    store = tmp_path / "store"
    job = _run_job(DeltaTableDatabase(store), declared, value, use_async=False)
    saved = tmp_path / "job.json"
    job.save(saved)
    _, data = next(iter(job.nodes["produce"].as_source().iter_data()))
    in_process_hash = data.content_hash().to_string()

    result = subprocess.run(
        [sys.executable, "-c", _FRESH_READ, str(saved), str(store)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    read = json.loads(result.stdout.strip().splitlines()[-1])
    assert read["schema"] == str(declared)
    assert read["python_type"] == "set"
    assert read["elements"] == sorted(repr(v) for v in value)
    assert read["row_hash"] == in_process_hash
