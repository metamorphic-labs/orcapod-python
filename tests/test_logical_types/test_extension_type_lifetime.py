"""Arrow's worker threads never destroy a logical type's extension type (ITL-1422).

The C++ destructor of an extension type written in Python takes the GIL. A worker that runs
it while the interpreter is finalizing aborts the process on Linux (Python 3.11 and 3.12)
or leaves it hanging. A type that has one instance, which is never destroyed, cannot be
destroyed by a worker. The race itself is rare, so these tests check the property.
"""

from __future__ import annotations

import gc
import subprocess
import sys
import textwrap
import weakref
from collections.abc import Callable, Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
import pytest

import orcapod.contexts as contexts
from orcapod.logical_types import make_arrow_extension_type
from orcapod.logical_types.ignore_list import _ExtensionFamily
from orcapod.semantic_types.universal_converter import UniversalTypeConverter


def _registered_types() -> dict[str, pa.ExtensionType]:
    """The extension type of every logical type of the default data context."""
    converter = contexts.get_default_context().type_converter
    assert isinstance(converter, UniversalTypeConverter)
    registry = converter._logical_type_registry
    assert registry is not None
    # The container types are registered when first used.
    for hint in (set[bytes], set[int], list[Path], set[Path]):
        converter.python_type_to_arrow_type(hint)
    return {
        name: logical_type.get_arrow_extension_type()
        for name, logical_type in registry._by_logical_name.items()
    }


def test_a_type_is_one_instance_however_it_is_made() -> None:
    """Calling and deserializing give the one instance, with its own storage."""
    extension_class = make_arrow_extension_type(
        "test.lifetime.one", pa.large_string(), b"1", also_reads=(b"0",)
    )
    one = extension_class()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert extension_class() is one  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    for stored, metadata in [
        (pa.large_string(), b"1"),
        (pa.string(), b"1"),
        (pa.large_string(), b"0"),
    ]:
        assert extension_class.__arrow_ext_deserialize__(stored, metadata) is one
    assert one.storage_type == pa.large_string()
    assert one.extension_name == "test.lifetime.one"
    assert one.__arrow_ext_serialize__() == b"1"


def test_two_classes_do_not_share_an_instance() -> None:
    """Each made class has its own instance, also under one extension name."""
    first = make_arrow_extension_type("test.lifetime.two", pa.int8(), b"1")()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    second = make_arrow_extension_type("test.lifetime.two", pa.int64(), b"2")()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert first is not second
    assert (first.storage_type, second.storage_type) == (pa.int8(), pa.int64())
    assert (first.__arrow_ext_serialize__(), second.__arrow_ext_serialize__()) == (
        b"1",
        b"2",
    )


class _TaggedScalar(pa.ExtensionScalar):
    """A scalar that reads as the name of its type's class and its stored value."""

    def as_py(self, **kwargs: object) -> str | None:
        stored = self.value
        if stored is None:
            return None
        return f"{type(self.type).__name__}:{stored.as_py()}"


class _TaggedArray(pa.ExtensionArray):
    """An array class of its own, to tell a subclass's array hook apart."""


def _read_back(
    extension: pa.ExtensionType, values: list, path: Path
) -> pa.ChunkedArray:
    """Write a column of ``extension`` to a Parquet file and scan it with threads."""
    storage = pa.array(values, extension.storage_type)
    table = pa.table({"column": pa.ExtensionArray.from_storage(extension, storage)})
    pq.write_table(table, path)
    return pa_ds.dataset(path).to_table(use_threads=True).column("column")


@pytest.fixture
def registered() -> Iterator[Callable[[pa.ExtensionType], pa.ExtensionType]]:
    """Register extension types with Arrow for one test."""
    names: list[str] = []

    def register(extension: pa.ExtensionType) -> pa.ExtensionType:
        pa.register_extension_type(extension)
        names.append(extension.extension_name)
        return extension

    yield register
    for name in names:
        pa.unregister_extension_type(name)


def _made(name: str) -> type[pa.ExtensionType]:
    return make_arrow_extension_type(name, pa.int64(), b"1")


def test_a_subclass_keeps_its_class_and_its_hooks(
    tmp_path: Path,
    registered: Callable[[pa.ExtensionType], pa.ExtensionType],
) -> None:
    """A subclass of a made class is its own type: its scalar and array classes are used."""
    made = _made("test.lifetime.subclass")

    class Custom(made):
        def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
            return _TaggedScalar

        def __arrow_ext_class__(self) -> type[pa.ExtensionArray]:
            return _TaggedArray

    custom = Custom()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    base = made()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert type(custom) is Custom
    assert type(base) is made
    assert custom is not base
    # Each class has one instance, also when deserialized.
    assert Custom() is custom  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert Custom.__arrow_ext_deserialize__(pa.int64(), b"1") is custom
    assert made.__arrow_ext_deserialize__(pa.int64(), b"1") is base
    # The identifier is the made class's.
    assert custom.extension_name == base.extension_name == "test.lifetime.subclass"
    assert custom.__arrow_ext_serialize__() == base.__arrow_ext_serialize__() == b"1"
    assert custom.storage_type == pa.int64()

    in_memory = pa.ExtensionArray.from_storage(custom, pa.array([1, 2, None]))
    assert type(in_memory) is _TaggedArray
    assert in_memory.to_pylist() == ["Custom:1", "Custom:2", None]
    assert pa.ExtensionArray.from_storage(base, pa.array([1, 2, None])).to_pylist() == [
        1,
        2,
        None,
    ]

    registered(custom)
    read = _read_back(custom, [1, 2, None], tmp_path / "custom.parquet")
    assert read.type is custom
    assert type(read.chunk(0)) is _TaggedArray
    assert read.to_pylist() == ["Custom:1", "Custom:2", None]


def test_two_subclasses_of_one_made_class_are_two_types() -> None:
    """Two subclasses do not share an instance with each other or with their base."""
    made = _made("test.lifetime.two_subclasses")

    class First(made):
        def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
            return _TaggedScalar

    class Second(made):
        def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
            return _TaggedScalar

    # Made in more than one order: the first instance made does not decide the others.
    second = Second()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    base = made()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    first = First()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert [type(first), type(second), type(base)] == [First, Second, made]
    assert len({id(first), id(second), id(base)}) == 3
    assert First() is first  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert Second() is second  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    storage = pa.array([7, 8])
    assert pa.ExtensionArray.from_storage(first, storage).to_pylist() == [
        "First:7",
        "First:8",
    ]
    assert pa.ExtensionArray.from_storage(second, storage).to_pylist() == [
        "Second:7",
        "Second:8",
    ]
    assert pa.ExtensionArray.from_storage(base, storage).to_pylist() == [7, 8]


def test_a_subclass_of_a_subclass_is_its_own_type(
    tmp_path: Path,
    registered: Callable[[pa.ExtensionType], pa.ExtensionType],
) -> None:
    """A second level of subclassing keeps the inherited hook and its own identity."""
    made = _made("test.lifetime.grandchild")

    class Child(made):
        def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
            return _TaggedScalar

    class Grandchild(Child):
        def __arrow_ext_class__(self) -> type[pa.ExtensionArray]:
            return _TaggedArray

    child = Child()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    grandchild = Grandchild()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert [type(child), type(grandchild)] == [Child, Grandchild]
    assert Grandchild() is grandchild  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert Grandchild.__arrow_ext_deserialize__(pa.int64(), b"1") is grandchild
    assert Child.__arrow_ext_deserialize__(pa.int64(), b"1") is child

    of_child = pa.ExtensionArray.from_storage(child, pa.array([3, 4]))
    assert type(of_child) is not _TaggedArray
    assert of_child.to_pylist() == ["Child:3", "Child:4"]

    registered(grandchild)
    read = _read_back(grandchild, [3, 4], tmp_path / "grandchild.parquet")
    assert read.type is grandchild
    assert type(read.chunk(0)) is _TaggedArray
    assert read.to_pylist() == ["Grandchild:3", "Grandchild:4"]


def test_a_subclass_made_with_arguments_has_one_instance_for_each(
    tmp_path: Path,
    registered: Callable[[pa.ExtensionType], pa.ExtensionType],
) -> None:
    """Different construction arguments give different types, equal ones the same one."""
    made = _made("test.lifetime.arguments")
    built: list[tuple[str, int]] = []

    class Scaled(made):
        def __init__(self, unit: str, factor: int = 1) -> None:
            built.append((unit, factor))
            self.unit = unit
            self.factor = factor
            pa.ExtensionType.__init__(self, pa.int64(), "test.lifetime.arguments")

        def __arrow_ext_serialize__(self) -> bytes:
            return f"{self.unit}*{self.factor}".encode()

        @classmethod
        def __arrow_ext_deserialize__(
            cls, storage_type: pa.DataType, serialized: bytes
        ) -> pa.ExtensionType:
            unit, factor = serialized.decode().split("*")
            return cls(unit, factor=int(factor))

        def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
            return _TaggedScalar

    metres = Scaled("m", factor=2)
    seconds = Scaled("s", factor=2)
    metres_by_three = Scaled("m", factor=3)
    assert len({id(metres), id(seconds), id(metres_by_three)}) == 3
    assert [(t.unit, t.factor) for t in (metres, seconds, metres_by_three)] == [
        ("m", 2),
        ("s", 2),
        ("m", 3),
    ]
    # Equal arguments give the instance already made, and build nothing again.
    assert Scaled("m", factor=2) is metres
    assert Scaled("s", factor=2) is seconds
    assert Scaled.__arrow_ext_deserialize__(pa.int64(), b"m*3") is metres_by_three
    assert sorted(built) == [("m", 2), ("m", 3), ("s", 2)]
    # Arguments that compare equal but are of another type are other arguments.
    assert Scaled("m", factor=True) is not Scaled("m", factor=1)
    assert Scaled("m", factor=True).factor is True
    # The base class's instance is none of them.
    assert type(made()) is made  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments

    registered(metres)
    read = _read_back(metres_by_three, [5, 6], tmp_path / "scaled.parquet")
    assert read.type is metres_by_three
    assert read.to_pylist() == ["Scaled:5", "Scaled:6"]

    alive = weakref.ref(Scaled("kg", factor=4))
    gc.collect()
    kept = alive()
    assert kept is not None and Scaled("kg", factor=4) is kept


def test_arguments_that_cannot_name_an_instance_are_refused() -> None:
    """An unhashable argument cannot say which instance is meant."""
    made = _made("test.lifetime.unhashable")

    class Listed(made):
        def __init__(self, names: list[str]) -> None:
            pa.ExtensionType.__init__(self, pa.int64(), "test.lifetime.unhashable")

    with pytest.raises(TypeError, match="hashable"):
        Listed(["a", "b"])


def test_a_subclass_instance_outlives_every_python_reference() -> None:
    """A subclass's instance is kept for the process like its base's."""
    made = _made("test.lifetime.subclass_kept")

    class Custom(made):
        pass

    alive = weakref.ref(Custom())  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    del Custom
    gc.collect()
    assert alive() is not None


@pytest.mark.parametrize("kind", ["made", "ignore_list"])
def test_a_type_outlives_every_python_reference(kind: str) -> None:
    """Dropping every reference Python holds does not destroy the type."""
    if kind == "made":
        extension = make_arrow_extension_type(
            "test.lifetime.kept", pa.large_string(), b"1"
        )()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    else:
        # The family is made apart from the process-wide one, so it can be dropped.
        family = _ExtensionFamily(
            "test.lifetime.family", pa.large_string(), 1, frozenset({"label"})
        )
        extension = family.arrow_type(("label",))
        del family
    alive = weakref.ref(extension)
    del extension
    gc.collect()
    assert alive() is not None


def test_a_scan_reads_the_shared_instances(tmp_path: Path) -> None:
    """A threaded scan of every registered type gives the instances already held."""
    types = {
        name: t
        for name, t in _registered_types().items()
        # Parquet has no form for a struct without fields, a null or a dictionary.
        if not pa.types.is_null(t.storage_type)
    }
    arrays = [
        pa.ExtensionArray.from_storage(t, pa.array([None, None], t.storage_type))
        for t in types.values()
    ]
    table = pa.table(arrays, names=list(types))
    pq.write_table(table, tmp_path / "all.parquet")
    read = pa_ds.dataset(tmp_path / "all.parquet").to_table(use_threads=True)
    for name, extension in types.items():
        assert read.schema.field(name).type is extension, name
    assert read.num_rows == 2


_CHILD = """
    import asyncio
    import sys
    from dataclasses import dataclass

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DictSource

    @dataclass(frozen=True)
    class Reading:
        label: str
        level: int

    @function_pod(output_keys=["members", "reading"])
    def produce(i: int, name: str) -> tuple[set[bytes], Reading]:
        return {name.encode(), str(i).encode()}, Reading(label=name, level=i)

    ROWS = [
        {"subject": "s2", "session": 1, "i": 3, "name": "c"},
        {"subject": "s1", "session": 2, "i": 2, "name": "b"},
        {"subject": "s1", "session": 1, "i": 1, "name": "a"},
        {"subject": "s2", "session": 2, "i": 4, "name": "d"},
    ]
    EXPECTED = sorted(
        (
            row["subject"],
            row["session"],
            sorted({row["name"].encode(), str(row["i"]).encode()}),
            (row["name"], row["i"]),
        )
        for row in ROWS
    )

    def source():
        return DictSource(
            ROWS,
            tag_columns=["subject", "session"],
            tag_only_columns=True,
            source_id="readings",
        )

    def run(job, **options):
        if path == "async":
            return asyncio.run(job.run_async(**options))
        return job.run(**options)

    def rows_of(stream):
        return sorted(
            (
                tag.as_dict()["subject"],
                tag.as_dict()["session"],
                sorted(data.as_dict()["members"]),
                (data.as_dict()["reading"].label, data.as_dict()["reading"].level),
            )
            for tag, data in stream.iter_data()
        )

    work, mode, path = sys.argv[1:]
    database = DeltaTableDatabase(work + "/store")
    if mode == "save":
        job = PipelineJob(name="exit", database=database)
        with job:
            produce.pod(source(), label="produce")
        run(job)
        job.save(work + "/job.json")
    else:
        # A loaded job does not hold an in-memory source's rows: bind them again,
        # so that the run loads the four stored results on the path under test.
        job = PipelineJob.load(work + "/job.json", database=database)
        job.bind({"readings": source()})
        summary = run(job, load_only=True)
        produced = summary.nodes["produce"]
        assert (produced.reused, produced.computed, produced.failed) == (4, 0, 0), summary
    node = job.nodes["produce"]
    assert rows_of(node) == EXPECTED, rows_of(node)
    assert rows_of(node.as_source()) == EXPECTED, rows_of(node.as_source())
    print("read", len(EXPECTED), flush=True)
"""


@pytest.mark.parametrize("path", ["sync", "async"])
def test_a_process_that_read_logical_types_exits_cleanly(
    tmp_path: Path, path: str
) -> None:
    """Processes that run, save, load and read a job's rows exit with code 0.

    Four rows, two tags and two outputs of two logical types (``set[bytes]`` and a
    dataclass), on the sync and the async job path: the first process computes and
    stores them, each later one loads the saved job and reads them from the store.
    """
    for mode in ["save", *["load"] * 5]:
        result = subprocess.run(
            [sys.executable, "-c", textwrap.dedent(_CHILD), str(tmp_path), mode, path],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert result.returncode == 0, result.stderr[-3000:]
        assert result.stdout.splitlines()[-1] == "read 4"
