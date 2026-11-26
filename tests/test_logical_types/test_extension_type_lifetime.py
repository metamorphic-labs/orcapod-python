"""Arrow's worker threads never destroy a logical type's extension type (ITL-1422).

The C++ destructor of an extension type written in Python takes the GIL. A worker that runs
it while the interpreter is finalizing aborts the process on Linux (Python 3.11 and 3.12)
or leaves it hanging. A type that has one instance, which is never destroyed, cannot be
destroyed by a worker. The race itself is rare, so these tests check the property.
"""

from __future__ import annotations

import copy
import gc
import os
import struct
import subprocess
import sys
import textwrap
import threading
import weakref
from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from ctypes import py_object
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.dataset as pa_ds
import pyarrow.parquet as pq
import pytest

import orcapod.contexts as contexts
from orcapod.logical_types import arrow_lifetime, make_arrow_extension_type
from orcapod.logical_types.arrow_lifetime import KeptInstances
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


class _TaggedArray(pa.ExtensionArray):  # pyright: ignore[reportMissingTypeArgument]  # not generic at run time
    """An array class of its own, to tell a subclass's array hook apart."""


def _array(extension: pa.ExtensionType, values: list[Any]) -> Any:
    """An array of ``extension`` over ``values``, of the class its hook names."""
    storage = pa.array(values, extension.storage_type)
    return pa.ExtensionArray.from_storage(extension, storage)


def _read_back(extension: pa.ExtensionType, values: list[Any], path: Path) -> Any:
    """Write a column of ``extension`` to a Parquet file and scan it with threads."""
    pq.write_table(pa.table({"column": _array(extension, values)}), path)
    return pa_ds.dataset(path).to_table(use_threads=True).column("column")


@pytest.fixture
def registered() -> Iterator[Callable[[Any], Any]]:
    """Register extension types with Arrow for one test."""
    names: list[str] = []

    def register(extension: Any) -> Any:
        """Register a type and record its name for fixture teardown."""
        pa.register_extension_type(extension)
        names.append(extension.extension_name)
        return extension

    yield register
    for name in names:
        pa.unregister_extension_type(name)


def _made(name: str) -> type[pa.ExtensionType]:
    """Create a generated Arrow type over int64 storage."""
    return make_arrow_extension_type(name, pa.int64(), b"1")


def test_a_subclass_keeps_its_class_and_its_hooks(
    tmp_path: Path,
    registered: Callable[[Any], Any],
) -> None:
    """A subclass of a made class is its own type: its scalar and array classes are used."""
    made = _made("test.lifetime.subclass")

    class Custom(made):
        def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
            return _TaggedScalar

        def __arrow_ext_class__(self) -> type[pa.ExtensionArray[Any]]:
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

    in_memory = _array(custom, [1, 2, None])
    assert in_memory.to_pylist() == ["Custom:1", "Custom:2", None]
    assert type(in_memory) is _TaggedArray
    assert _array(base, [1, 2, None]).to_pylist() == [
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
    assert _array(first, [7, 8]).to_pylist() == [
        "First:7",
        "First:8",
    ]
    assert _array(second, [7, 8]).to_pylist() == [
        "Second:7",
        "Second:8",
    ]
    assert _array(base, [7, 8]).to_pylist() == [7, 8]


def test_a_subclass_of_a_subclass_is_its_own_type(
    tmp_path: Path,
    registered: Callable[[Any], Any],
) -> None:
    """A second level of subclassing keeps the inherited hook and its own identity."""
    made = _made("test.lifetime.grandchild")

    class Child(made):
        def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
            return _TaggedScalar

    class Grandchild(Child):
        def __arrow_ext_class__(self) -> type[pa.ExtensionArray[Any]]:
            return _TaggedArray

    child = Child()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    grandchild = Grandchild()  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert [type(child), type(grandchild)] == [Child, Grandchild]
    assert Grandchild() is grandchild  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert Grandchild.__arrow_ext_deserialize__(pa.int64(), b"1") is grandchild
    assert Child.__arrow_ext_deserialize__(pa.int64(), b"1") is child

    of_child = _array(child, [3, 4])
    assert type(of_child) is not _TaggedArray
    assert of_child.to_pylist() == ["Child:3", "Child:4"]

    registered(grandchild)
    read = _read_back(grandchild, [3, 4], tmp_path / "grandchild.parquet")
    assert read.type is grandchild
    assert type(read.chunk(0)) is _TaggedArray
    assert read.to_pylist() == ["Grandchild:3", "Grandchild:4"]


def test_a_subclass_made_with_arguments_has_one_instance_for_each(
    tmp_path: Path,
    registered: Callable[[Any], Any],
) -> None:
    """Different construction arguments give different types, equal ones the same one."""
    made = _made("test.lifetime.arguments")

    class Scaled(made):
        def __init__(self, unit: str, factor: int = 1) -> None:
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
    # Equal arguments give the instance already made.
    assert Scaled("m", factor=2) is metres
    assert Scaled("s", factor=2) is seconds
    assert Scaled.__arrow_ext_deserialize__(pa.int64(), b"m*3") is metres_by_three
    # Arguments that compare equal and give other metadata are other arguments.
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


def _describing(name: str, describe: Callable[[Any], str]) -> type[Any]:
    """A subclass of a made class whose metadata is ``describe`` of its one argument."""
    made = _made(name)

    class Describing(made):
        def __init__(self, value: Any) -> None:
            self.value = value
            pa.ExtensionType.__init__(self, pa.int64(), name)

        def __arrow_ext_serialize__(self) -> bytes:
            return describe(self.value).encode()

        def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
            return _TaggedScalar

    return Describing


class _TaggedTuple(tuple):  # pyright: ignore[reportMissingTypeArgument]  # any items
    """A tuple with a tag its equality does not read."""

    tag: str

    def __new__(cls, items: Iterable[Any], tag: str) -> "_TaggedTuple":
        self = super().__new__(cls, items)
        self.tag = tag
        return self


class _UnhashableTuple(_TaggedTuple):
    """A tagged tuple that cannot be hashed."""

    __hash__ = None  # pyright: ignore[reportAssignmentType]  # unhashable on purpose


class _Loose:
    """A value whose equality ignores the note its metadata holds."""

    def __init__(self, note: str) -> None:
        self.note = note

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Loose)

    def __hash__(self) -> int:
        return 0


class _Config:
    """A value with the default equality: equal only to itself."""

    def __init__(self, payload: str) -> None:
        self.payload = payload


def _same_bit_nans() -> tuple[float, float]:
    """Two float objects holding the same NaN, bit for bit."""
    first = struct.unpack(">d", bytes.fromhex("7ff8000000000000"))[0]
    second = struct.unpack(">d", bytes.fromhex("7ff8000000000000"))[0]
    assert first is not second
    return first, second


# Arguments a comparison of argument values gets wrong, with the metadata each gives:
# name -> (how the metadata is made, the arguments).
_METADATA_DECIDES: dict[str, tuple[Callable[[Any], str], Callable[[], list[Any]]]] = {
    # Two NaN objects with identical bits are two members of a frozenset.
    "nan_frozensets": (
        lambda value: f"{len(value):02d}",
        lambda: [frozenset(_same_bit_nans()[:1]), frozenset(_same_bit_nans())],
    ),
    "nan_tuples": (
        lambda value: f"{len(value):02d}",
        lambda: [_same_bit_nans()[:1], _same_bit_nans(), _same_bit_nans() * 2],
    ),
    # A tuple subclass's own state is not its contents.
    "tagged_tuples": (
        lambda value: value.tag,
        lambda: [
            _TaggedTuple([1, 2], "first"),
            _TaggedTuple([1, 2], "second"),
            _TaggedTuple([1, 2], "third"),
        ],
    ),
    "unhashable_tuples": (
        lambda value: value.tag,
        lambda: [_UnhashableTuple([1, 2], "first"), _UnhashableTuple([1, 2], "second")],
    ),
    "decimals": (str, lambda: [Decimal("1.0"), Decimal("1.00"), Decimal("1")]),
    "loose_equality": (
        lambda value: value.note,
        lambda: [_Loose("first"), _Loose("second"), _Loose("third")],
    ),
    "unhashable_lists": (
        lambda value: "+".join(value),
        lambda: [["a", "b"], ["b", "a"], ["a"]],
    ),
}


@pytest.mark.parametrize("case", sorted(_METADATA_DECIDES))
@pytest.mark.parametrize("order", ["as_listed", "reversed"])
def test_arguments_of_different_metadata_are_different_types(
    tmp_path: Path, registered: Callable[[Any], Any], case: str, order: str
) -> None:
    """Whatever the arguments' own equality says, each metadata has its own instance.

    The order in which the instances are first made never decides which metadata a
    column is written with.
    """
    describe, arguments = _METADATA_DECIDES[case]
    values = arguments()
    if order == "reversed":
        values = values[::-1]
    describing = _describing(f"test.lifetime.metadata.{case}.{order}", describe)
    types = [describing(value) for value in values]
    assert [t.__arrow_ext_serialize__().decode() for t in types] == [
        describe(value) for value in values
    ]
    assert len({id(t) for t in types}) == len(values)
    # The same metadata again, from freshly made arguments, is the instance made first.
    for extension, value in zip(
        types, arguments()[:: -1 if order == "reversed" else 1]
    ):
        assert describing(value) is extension

    registered(types[0])
    # Read back, the metadata alone names the instance.
    by_metadata = {t.__arrow_ext_serialize__(): t for t in types}
    describing.__arrow_ext_deserialize__ = classmethod(
        lambda cls, storage_type, serialized: by_metadata[serialized]
    )
    table = pa.table(
        {f"column_{i}": _array(t, [10 + i, 20 + i, None]) for i, t in enumerate(types)}
    )
    pq.write_table(table, tmp_path / "metadata.parquet")
    read = pa_ds.dataset(tmp_path / "metadata.parquet").to_table(use_threads=True)
    for i, extension in enumerate(types):
        column = read.column(f"column_{i}")
        assert column.type is extension, describe(values[i])
        assert column.to_pylist() == [
            f"Describing:{10 + i}",
            f"Describing:{20 + i}",
            None,
        ]


def test_reads_of_one_metadata_keep_one_instance(
    tmp_path: Path, registered: Callable[[Any], Any]
) -> None:
    """A deserializer that makes a fresh argument on every read keeps one type.

    The kept instances are counted by metadata, never by reads: an argument equal only
    to itself (the default equality) does not make an instance of its own.
    """
    made = _made("test.lifetime.fresh")

    class Configured(made):
        def __init__(self, config: _Config) -> None:
            self.config = config
            pa.ExtensionType.__init__(self, pa.int64(), "test.lifetime.fresh")

        def __arrow_ext_serialize__(self) -> bytes:
            return self.config.payload.encode()

        @classmethod
        def __arrow_ext_deserialize__(
            cls, storage_type: pa.DataType, serialized: bytes
        ) -> pa.ExtensionType:
            return cls(_Config(serialized.decode()))

    payloads = ["alpha", "beta", "gamma"]
    first = {payload: Configured(_Config(payload)) for payload in payloads}
    assert len({id(t) for t in first.values()}) == 3
    made_by_reads = [
        Configured.__arrow_ext_deserialize__(pa.int64(), payload.encode())
        for _ in range(500)
        for payload in payloads[::-1]
    ]
    assert {id(t) for t in made_by_reads} == {id(t) for t in first.values()}
    for extension in made_by_reads[:3]:
        assert first[extension.__arrow_ext_serialize__().decode()] is extension

    registered(first["alpha"])
    table = pa.table(
        {payload: _array(first[payload], [1, 2, None]) for payload in payloads}
    )
    pq.write_table(table, tmp_path / "fresh.parquet")
    for _ in range(3):
        read = pa_ds.dataset(tmp_path / "fresh.parquet").to_table(use_threads=True)
        for payload in payloads:
            assert read.column(payload).type is first[payload]
            assert read.column(payload).to_pylist() == [1, 2, None]


@pytest.mark.parametrize("order", [("a", "b", "c"), ("c", "a", "b")])
def test_an_argument_the_metadata_does_not_hold_makes_no_type(
    tmp_path: Path, registered: Callable[[Any], Any], order: tuple[str, ...]
) -> None:
    """Instances of one class, name, storage and metadata are one type: the first made.

    Arrow cannot tell them apart on read, so an argument that only a hook reads, and
    the metadata does not hold, could not survive a write and a read anyway.
    """
    name = f"test.lifetime.unserialized.{'_'.join(order)}"
    made = _made(name)

    class Labelled(made):
        def __init__(self, label: str, unit: str = "m") -> None:
            self.label = label
            self.unit = unit
            pa.ExtensionType.__init__(self, pa.int64(), name)

        def __arrow_ext_serialize__(self) -> bytes:
            return self.unit.encode()

        @classmethod
        def __arrow_ext_deserialize__(
            cls, storage_type: pa.DataType, serialized: bytes
        ) -> pa.ExtensionType:
            return cls("from a read", unit=serialized.decode())

    types = [Labelled(label) for label in order]
    assert all(t is types[0] for t in types)
    assert types[0].label == order[0]
    # The metadata does hold the unit: another unit is another type.
    seconds = [Labelled(label, unit="s") for label in order[::-1]]
    assert all(t is seconds[0] for t in seconds)
    assert seconds[0] is not types[0]
    assert (seconds[0].label, seconds[0].unit) == (order[-1], "s")

    registered(types[0])
    for extension in (types[0], seconds[0]):
        read = _read_back(extension, [1, 2, None], tmp_path / "labelled.parquet")
        assert read.type is extension
        assert read.type.label == extension.label
        assert read.to_pylist() == [1, 2, None]


def _echoing(name: str) -> type[Any]:
    """A subclass of a made class whose metadata is the text of its one argument."""
    made = _made(name)

    class Echoing(made):
        def __init__(self, value: Any) -> None:
            self.value = value
            pa.ExtensionType.__init__(self, pa.int64(), name)

        def __arrow_ext_serialize__(self) -> bytes:
            return _exact_text(self.value).encode()

        @classmethod
        def __arrow_ext_deserialize__(
            cls, storage_type: pa.DataType, serialized: bytes
        ) -> pa.ExtensionType:
            return cls(_VALUES_BY_TEXT[serialized.decode()])

        def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
            return _TaggedScalar

    return Echoing


def _exact_text(value: Any) -> str:
    """Text that tells apart every value the instances must tell apart."""
    if isinstance(value, float):
        return f"float:{struct.pack('>d', value).hex()}"
    if isinstance(value, (tuple, frozenset)):
        parts = [_exact_text(item) for item in value]
        if isinstance(value, frozenset):
            parts.sort()
        return f"{type(value).__name__}({','.join(parts)})"
    return f"{type(value).__name__}:{value!r}"


_OTHER_NAN = struct.unpack(">d", bytes.fromhex("7ff8000000000001"))[0]
_NEGATIVE_NAN = struct.unpack(">d", bytes.fromhex("fff8000000000000"))[0]

# Groups of arguments that compare equal in Python (or, for the NaNs, are one value to
# Arrow's matching) and are still different arguments: each has its own metadata.
_EQUAL_BUT_DISTINCT: list[list[Any]] = [
    [0.0, -0.0, 0, False],
    [1, 1.0, True],
    [float("nan"), _OTHER_NAN, _NEGATIVE_NAN],
    [(0.0,), (-0.0,), (0,), (False,)],
    [(1, (2.0, True)), (1, (2, True)), (1, (2.0, 1)), (True, (2.0, True))],
    [frozenset({0.0}), frozenset({-0.0}), frozenset({False})],
    [("a", frozenset({(1,)})), ("a", frozenset({(True,)})), ("a", frozenset({(1.0,)}))],
]
_VALUES_BY_TEXT: dict[str, Any] = {
    _exact_text(value): value for group in _EQUAL_BUT_DISTINCT for value in group
}


@pytest.mark.parametrize("group", range(len(_EQUAL_BUT_DISTINCT)))
@pytest.mark.parametrize("order", ["as_listed", "reversed"])
def test_equal_but_distinct_arguments_are_distinct_types(
    tmp_path: Path,
    registered: Callable[[Any], Any],
    group: int,
    order: str,
) -> None:
    """``0.0`` and ``-0.0``, ``1`` and ``True``: each has its own instance and metadata.

    The order in which they are first constructed never decides which metadata a
    column is written with.
    """
    values = _EQUAL_BUT_DISTINCT[group]
    if order == "reversed":
        values = values[::-1]
    echoing = _echoing(f"test.lifetime.distinct.{group}.{order}")
    types = [echoing(value) for value in values]
    assert len({id(t) for t in types}) == len(values)
    assert [t.__arrow_ext_serialize__().decode() for t in types] == [
        _exact_text(value) for value in values
    ]
    for value, extension in zip(values, types):
        # An equal argument made again (another object) gives the instance already made.
        again = _VALUES_BY_TEXT[_exact_text(value)]
        assert echoing(again) is extension
        assert echoing(copy.deepcopy(value)) is extension

    registered(types[0])
    table = pa.table(
        {f"column_{i}": _array(t, [10 + i, 20 + i, None]) for i, t in enumerate(types)}
    )
    pq.write_table(table, tmp_path / "distinct.parquet")
    read = pa_ds.dataset(tmp_path / "distinct.parquet").to_table(use_threads=True)
    for i, extension in enumerate(types):
        column = read.column(f"column_{i}")
        assert column.type is extension, values[i]
        assert column.to_pylist() == [f"Echoing:{10 + i}", f"Echoing:{20 + i}", None]


def test_a_constructor_may_wait_for_another_type_made_on_another_thread() -> None:
    """No process-wide lock is held while a constructor runs."""
    inner_made = _made("test.lifetime.threads.inner")
    outer_made = _made("test.lifetime.threads.outer")

    class Inner(inner_made):
        pass

    class Outer(outer_made):
        def __init__(self) -> None:
            made: list[Any] = []
            thread = threading.Thread(target=lambda: made.append(Inner()), daemon=True)  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
            thread.start()
            thread.join(30)
            self.inner = made[0] if made else None
            pa.ExtensionType.__init__(self, pa.int64(), "test.lifetime.threads.outer")

    outer = Outer()
    assert outer.inner is not None, "the other thread could not construct its type"
    assert type(outer.inner) is Inner
    assert Inner() is outer.inner  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert Outer() is outer


def test_threads_constructing_cold_subclasses_at_once_get_one_instance_each() -> None:
    """Different cold subclasses are made at once; each class still has one instance."""
    made = _made("test.lifetime.threads.cold")
    classes = 4
    callers = 6
    inside = threading.Barrier(classes, timeout=30)
    first_pass = threading.Event()
    first_pass.set()
    start = threading.Barrier(classes * callers, timeout=30)

    def subclass(index: int) -> type[Any]:
        """Make a subclass that joins the barrier on its first construction."""

        class Cold(made):
            def __init__(self, unit: str) -> None:
                """Wait for one cold constructor per class during the first phase."""
                self.unit = unit
                self.index = index
                if unit == "together" and first_pass.is_set():
                    # Every class has a constructor running at this point.
                    inside.wait()
                pa.ExtensionType.__init__(
                    self, pa.int64(), "test.lifetime.threads.cold"
                )

            def __arrow_ext_serialize__(self) -> bytes:
                """Encode the unit so the two phases use distinct keys."""
                return self.unit.encode()

            def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
                """Use a scalar that exposes which subclass supplied it."""
                return _TaggedScalar

        Cold.__name__ = f"Cold{index}"
        return Cold

    cold = [subclass(index) for index in range(classes)]
    # First, one constructor of each class runs at the same time as the others.
    with ThreadPoolExecutor(classes) as pool:
        together = list(pool.map(lambda cls: cls("together"), cold))
    first_pass.clear()
    assert [type(t) for t in together] == cold
    assert all(
        cls("together") is kept for cls, kept in zip(cold, together, strict=True)
    )

    # Then many threads race to make the same cold instance of each class.
    def construct(cls: type[Any]) -> Any:
        """Start all callers together before they race to publish each key."""
        start.wait()
        return cls("raced")

    with ThreadPoolExecutor(classes * callers) as pool:
        raced = list(pool.map(construct, cold * callers))
    mine: list[Any] = []
    for index, cls in enumerate(cold):
        mine = raced[index::classes]
        assert len(mine) == callers
        assert all(t is mine[0] for t in mine)
        assert type(mine[0]) is cls and mine[0].index == index
        assert cls("raced") is mine[0]
        assert _array(mine[0], [index, None]).to_pylist() == [
            f"Cold{index}:{index}",
            None,
        ]
    alive = [weakref.ref(t) for t in raced[:classes]]
    del raced, mine, together
    gc.collect()
    assert all(ref() is not None for ref in alive)


def _measured(name: str) -> type[Any]:
    """A subclass whose unit is held only by its storage type's nested field metadata."""
    made = _made(name)

    class Measured(made):
        def __init__(self, unit: str) -> None:
            self.unit = unit
            reading = pa.field("reading", pa.int64(), metadata={"unit": unit})
            inner = pa.field("inner", pa.struct([reading]))
            pa.ExtensionType.__init__(self, pa.struct([inner]), name)

        def __arrow_ext_serialize__(self) -> bytes:
            return b"measured"

        @classmethod
        def __arrow_ext_deserialize__(
            cls, storage_type: pa.DataType, serialized: bytes
        ) -> pa.ExtensionType:
            metadata = storage_type.field(0).type.field(0).metadata
            return cls(metadata[b"unit"].decode())

    return Measured


@pytest.mark.parametrize("order", [("m", "s", "kg"), ("kg", "s", "m")])
@pytest.mark.parametrize("file_format", ["ipc", "parquet"])
def test_storage_types_differing_in_nested_field_metadata_are_different_types(
    tmp_path: Path,
    registered: Callable[[Any], Any],
    order: tuple[str, ...],
    file_format: str,
) -> None:
    """Arrow hands a read the storage type with its nested field metadata.

    Two storage types Arrow's own equality calls equal, and that differ in the metadata
    of a nested field, are two types: a column is read as the type it was written as,
    whichever was made first.
    """
    measured = _measured(f"test.lifetime.nested.{file_format}.{'_'.join(order)}")
    types = {unit: measured(unit) for unit in order}
    assert len({id(t) for t in types.values()}) == 3
    assert [t.unit for t in types.values()] == list(order)
    # pyarrow's equality of the storage types does not see the difference.
    assert types["m"].storage_type == types["s"].storage_type
    for unit in order[::-1]:
        assert measured(unit) is types[unit]

    registered(types[order[0]])
    rows = [{"inner": {"reading": 1}}, {"inner": {"reading": 2}}, None]
    table = pa.table({unit: _array(types[unit], rows) for unit in order})
    path = tmp_path / f"nested.{file_format}"
    if file_format == "ipc":
        with pa.OSFile(str(path), "wb") as sink:
            with pa.ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)
        read = pa_ds.dataset(path, format="ipc").to_table(use_threads=True)
    else:
        pq.write_table(table, path)
        read = pa_ds.dataset(path).to_table(use_threads=True)
    for unit in order:
        column = read.column(unit)
        assert column.type is types[unit], unit
        assert column.type.unit == unit
        assert column.to_pylist() == rows


def test_gc_during_pin_preparation_can_wait_for_another_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A GC callback waiting for another thread can finish during pin preparation."""
    outer_type = _made("test.lifetime.pin_gc.outer")
    # The generated classes have zero-argument constructors.
    make_outer = cast(Callable[[], pa.ExtensionType], outer_type)
    make_inner = cast(
        Callable[[], pa.ExtensionType], _made("test.lifetime.pin_gc.inner")
    )
    reference_to = arrow_lifetime._REFERENCE_TO
    threads: list[threading.Thread] = []
    made: list[pa.ExtensionType] = []
    finished = threading.Event()
    finished_during_gc: list[bool] = []

    def construct_inner() -> None:
        """Construct the other type and record completion for the GC callback."""
        made.append(make_inner())
        finished.set()

    def on_gc(phase: str, _info: dict[str, int]) -> None:
        """On the first collection, wait for a constructor on another thread."""
        if phase == "start" and not threads:
            thread = threading.Thread(target=construct_inner, daemon=True)
            threads.append(thread)
            thread.start()
            finished_during_gc.append(finished.wait(timeout=5))

    def collect_before_referencing(value: object) -> py_object:
        """Collect while the outer type's owning reference is prepared."""
        if type(value) is outer_type:
            # Collect here deterministically instead of relying on the GC threshold.
            gc.callbacks.append(on_gc)
            try:
                _ = gc.collect()
            finally:
                gc.callbacks.remove(on_gc)
        return reference_to(value)

    monkeypatch.setattr(arrow_lifetime, "_REFERENCE_TO", collect_before_referencing)
    _ = make_outer()
    for thread in threads:
        thread.join(timeout=5)
    assert finished_during_gc == [True], "GC could not wait for the other constructor"
    assert make_inner() is made[0]


def test_reentry_during_failed_pin_preparation_keeps_one_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An inner call can publish a winner even if the outer preparation fails."""
    extension_type = _made("test.lifetime.pin_reentry")
    make = cast(Callable[[], pa.ExtensionType], extension_type)
    reference_to = arrow_lifetime._REFERENCE_TO
    inner: list[pa.ExtensionType] = []
    interrupted = False

    def reenter_before_referencing(value: object) -> py_object:
        """Construct the same type once before interrupting outer preparation."""
        nonlocal interrupted
        if type(value) is extension_type and not interrupted:
            interrupted = True
            inner.append(make())
            raise MemoryError("pin preparation interrupted")
        return reference_to(value)

    monkeypatch.setattr(arrow_lifetime, "_REFERENCE_TO", reenter_before_referencing)
    with pytest.raises(MemoryError, match="pin preparation interrupted"):
        _ = make()
    winner = inner.pop()
    assert all(make() is winner for _ in range(3))


def _loose_metaclass() -> type[KeptInstances]:
    class LooseMeta(KeptInstances):
        """Classes that all compare equal and hash alike."""

        def __eq__(cls, other: object) -> bool:
            return isinstance(other, LooseMeta)

        def __hash__(cls) -> int:
            return 0

    return LooseMeta


def _raising_metaclass() -> type[KeptInstances]:
    class RaisingMeta(KeptInstances):
        """Classes that cannot be hashed or compared."""

        def __eq__(cls, other: object) -> bool:
            raise RuntimeError("this class cannot be compared")

        def __hash__(cls) -> int:
            raise RuntimeError("this class cannot be hashed")

    return RaisingMeta


@pytest.mark.parametrize("metaclass", ["loose", "raising"])
@pytest.mark.parametrize("order", [(0, 1, 2), (2, 0, 1)])
def test_a_class_is_told_apart_by_what_it_is_not_by_its_equality(
    metaclass: str, order: tuple[int, ...]
) -> None:
    """Classes that compare equal, or cannot be hashed, each keep their own instance."""
    name = f"test.lifetime.class_identity.{metaclass}.{'_'.join(map(str, order))}"
    made = _made(name)
    meta = _loose_metaclass() if metaclass == "loose" else _raising_metaclass()
    refs: list[weakref.ref[Any]] = []

    def subclass(index: int) -> type[Any]:
        class Sibling(made, metaclass=meta):
            def __init__(self) -> None:
                refs.append(weakref.ref(self))
                self.index = index
                pa.ExtensionType.__init__(self, pa.int64(), name)

            def __arrow_ext_scalar_class__(self) -> type[pa.ExtensionScalar]:
                return _TaggedScalar

        Sibling.__name__ = f"Sibling{index}"
        return Sibling

    classes = [subclass(index) for index in range(3)]
    types = {index: classes[index]() for index in order}
    for index in order[::-1]:
        assert type(types[index]) is classes[index]
        assert types[index].index == index
        assert classes[index]() is types[index]
        assert _array(types[index], [index, None]).to_pylist() == [
            f"Sibling{index}:{index}",
            None,
        ]
    assert len({id(t) for t in types.values()}) == 3
    # Nothing but the three types is kept: every other instance made is gone.
    del types
    gc.collect()
    assert sum(ref() is not None for ref in refs) == 3


@pytest.mark.parametrize("order", ["winner_first", "returning_class_first"])
def test_a_new_that_returns_the_kept_instance_adds_no_reference(order: str) -> None:
    """A thousand calls that give the instance already kept pin it no further."""
    name = f"test.lifetime.returning.{order}"
    made = _made(name)
    winner: list[Any] = []

    class Returning(made):
        def __new__(cls) -> Any:
            if winner:
                return winner[0]
            return super().__new__(cls)

        def __init__(self) -> None:
            if not winner:
                pa.ExtensionType.__init__(self, pa.int64(), name)

    class Plain(made):
        pass

    classes = [Plain, Returning] if order == "winner_first" else [Returning, Plain]
    first = [cls() for cls in classes]  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    winner.append(first[classes.index(Returning)])
    kept = winner[0]
    before = sys.getrefcount(kept)
    for _ in range(1000):
        assert Returning() is kept
        assert Plain() is first[classes.index(Plain)]  # pyright: ignore[reportCallIssue]  # the synthesized class takes no arguments
    assert sys.getrefcount(kept) - before == 0


def test_a_reused_instance_runs_its_initializer_again() -> None:
    """Identity sharing does not freeze a reused instance's hook-only state."""
    made = _made("test.lifetime.reinitialized")
    created: list[Reused] = []

    class Reused(made):
        def __new__(cls, label: str) -> "Reused":
            """Return the original object when it already exists."""
            return created[0] if created else super().__new__(cls)

        def __init__(self, label: str) -> None:
            """Set a label that the Arrow descriptor never stores."""
            if not created:
                super().__init__()  # pyright: ignore[reportCallIssue]  # generated initializer
                created.append(self)
            self.label: str = label

    first = Reused("first")
    assert first.label == "first"
    assert Reused("second") is first
    assert first.label == "second"


@pytest.mark.parametrize("parent_first", [True, False])
def test_a_constructor_returning_a_subclass_shares_the_subclass_instance(
    parent_first: bool,
) -> None:
    """The class of the candidate defines the key, even via another constructor."""
    made = _made("test.lifetime.subclass_candidate")

    class Parent(made):
        def __new__(cls) -> "Parent":
            """Allocate the concrete child from either entry point."""
            return pa.ExtensionType.__new__(Child)

        def __init__(self) -> None:
            """Initialize the synthesized type without extra arguments."""
            super().__init__()  # pyright: ignore[reportCallIssue]  # generated initializer

    class Child(Parent):
        """The concrete Arrow type allocated by both constructors."""

    first, second = (Parent, Child) if parent_first else (Child, Parent)
    winner = first()
    assert type(winner) is Child
    assert second() is winner
    assert first() is winner


def test_a_custom_metaclass_can_join_the_generated_metaclass() -> None:
    """Composing metaclasses preserves both abstract methods and sharing."""
    made = _made("test.lifetime.abstract")
    generated_meta = cast(type[KeptInstances], type(made))

    class CombinedMeta(generated_meta, ABCMeta):
        """Combine the generated type's metaclass with ABCMeta."""

    class Abstract(made, metaclass=CombinedMeta):  # pyright: ignore[reportImplicitAbstractClass]  # CombinedMeta includes ABCMeta
        """An extension type that requires a label hook."""

        def __init__(self) -> None:
            """Initialize the synthesized type without extra arguments."""
            super().__init__()  # pyright: ignore[reportCallIssue]  # generated initializer

        @abstractmethod
        def label(self) -> str:
            """Return the hook label."""

    class Concrete(Abstract):
        """An instantiable extension type with a custom hook."""

        def label(self) -> str:
            """Return the concrete hook label."""
            return "concrete"

    with pytest.raises(TypeError, match="abstract"):
        _ = Abstract()  # pyright: ignore[reportAbstractUsage]  # exercise ABCMeta
    kept = Concrete()
    assert Concrete() is kept
    assert kept.label() == "concrete"


_FORK_WITH_LOCK_HELD = '''
    import gc
    import os
    import signal
    import sys
    import threading

    import pyarrow as pa

    callback_kind = sys.argv[1]
    made_in_callback = []

    def on_collection(phase, info):
        """Construct a type during the collection started by the child hook."""
        if phase == "start":
            made_in_callback.append(callback_type())

    def early_child_callback():
        """Construct before the Orcapod child hook has reset its inherited lock."""
        signal.alarm(3)
        if callback_kind == "gc":
            gc.callbacks.append(on_collection)
            try:
                gc.collect()
            finally:
                gc.callbacks.remove(on_collection)
        else:
            made_in_callback.append(callback_type())

    # Child hooks run in registration order, so this hook runs before Orcapod's.
    if callback_kind != "none":
        os.register_at_fork(after_in_child=early_child_callback)

    from orcapod.logical_types import arrow_lifetime, make_arrow_extension_type

    extension_type = make_arrow_extension_type("test.lifetime.fork", pa.int64())
    winner = extension_type()
    callback_type = make_arrow_extension_type("test.lifetime.fork_callback", pa.int64())
    locked = threading.Event()
    starting_fork = threading.Event()
    release = threading.Event()

    def hold_publication_lock():
        """Keep the lock held by a thread that will not exist in the child."""
        with arrow_lifetime._publication_lock:
            locked.set()
            assert starting_fork.wait(timeout=5)
            # Begin fork with the lock held and let a waiting before-fork hook proceed.
            release.wait(timeout=1)

    thread = threading.Thread(target=hold_publication_lock)
    thread.start()
    try:
        assert locked.wait(timeout=5)
        starting_fork.set()
        child = os.fork()
        if child == 0:
            signal.alarm(3)
            assert extension_type() is winner
            if callback_kind != "none":
                assert len(made_in_callback) == 1
                assert callback_type() is made_in_callback[0]
            os._exit(0)
        _, status = os.waitpid(child, 0)
        assert os.waitstatus_to_exitcode(status) == 0, status
    finally:
        release.set()
        thread.join(timeout=5)

    parent_result = []
    parent_reader = threading.Thread(
        target=lambda: parent_result.append(extension_type()), daemon=True
    )
    parent_reader.start()
    parent_reader.join(timeout=3)
    assert len(parent_result) == 1 and parent_result[0] is winner
'''


@pytest.mark.skipif(not hasattr(os, "fork"), reason="fork requires POSIX")
@pytest.mark.parametrize("callback_kind", ["none", "at_fork", "gc"])
def test_a_child_can_construct_a_type_after_fork_with_the_lock_held(
    callback_kind: str,
) -> None:
    """The child can publish even in a callback preceding Orcapod's fork hook."""
    process = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(_FORK_WITH_LOCK_HELD), callback_kind],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert process.returncode == 0, process.stderr[-3000:]


class _FailingOnce(dict):  # pyright: ignore[reportMissingTypeArgument]  # the table's own types
    """A table whose next insertion fails for lack of memory."""

    failures = 1

    def _fail(self) -> None:
        if self.failures:
            self.failures -= 1
            raise MemoryError("no memory for the table entry")

    def setdefault(self, key: Any, default: Any = None) -> Any:
        if key not in self:
            self._fail()
        return super().setdefault(key, default)


@pytest.mark.parametrize("units", [("m", "s"), ("s", "m")])
def test_a_failed_insertion_keeps_nothing(
    monkeypatch: pytest.MonkeyPatch, units: tuple[str, str]
) -> None:
    """An instance that could not be stored is not pinned, and the next call works."""
    name = f"test.lifetime.failed_insertion.{'_'.join(units)}"
    made = _made(name)
    refs: list[weakref.ref[Any]] = []

    class Unit(made):
        def __init__(self, unit: str) -> None:
            refs.append(weakref.ref(self))
            self.unit = unit
            pa.ExtensionType.__init__(self, pa.int64(), name)

        def __arrow_ext_serialize__(self) -> bytes:
            return self.unit.encode()

    table = _FailingOnce(KeptInstances._kept)
    monkeypatch.setattr(KeptInstances, "_kept", table)
    with pytest.raises(MemoryError, match="no memory for the table entry"):
        Unit(units[0])
    gc.collect()
    assert [ref() is not None for ref in refs] == [False]

    types = [Unit(unit) for unit in units]
    assert [t.unit for t in types] == list(units)
    assert [Unit(unit) for unit in units[::-1]] == types[::-1]
    assert Unit(units[0]) is types[0] and Unit(units[1]) is types[1]
    del types
    gc.collect()
    assert sum(ref() is not None for ref in refs) == 2


_NO_SYMBOL_LOOKUP = """
    import gc
    import sys
    import weakref

    import pyarrow as pa

    from orcapod.logical_types import make_arrow_extension_type

    def deny(event, args):
        if event.startswith("ctypes.dlsym"):
            raise RuntimeError("symbol lookup denied: " + repr(args[1:]))

    sys.addaudithook(deny)

    name = "test.lifetime.audit"
    made = make_arrow_extension_type(name, pa.int64(), b"1")
    constructed = []
    inner = []

    class Nested(made):
        def __init__(self, unit):
            constructed.append(weakref.ref(self))
            self.unit = unit
            pa.ExtensionType.__init__(self, pa.int64(), name)
            if len(constructed) % 2:
                # The same key, stored before this instance is.
                inner.append(Nested(unit))

        def __arrow_ext_serialize__(self):
            return self.unit.encode()

    for unit in sys.argv[1:]:
        outer = Nested(unit)
        assert inner.pop() is outer and outer.unit == unit
        assert Nested(unit) is outer
        del outer
    assert type(made()) is made
    gc.collect()
    alive = [ref() is not None for ref in constructed]
    # Per unit: the outer instance lost, the inner one is kept, later ones are hits.
    assert alive == [False, True, False, False] * len(sys.argv[1:]), alive
    print("kept", sum(alive), flush=True)
"""


@pytest.mark.parametrize("units", [("m", "s", "kg"), ("kg", "m", "s")])
def test_keeping_a_type_looks_up_no_symbol(units: tuple[str, ...]) -> None:
    """What the pin needs is bound when Orcapod is imported.

    A process that refuses symbol lookups afterwards (an audit hook) still makes, keeps
    and drops types: nothing is left pinned by a failed clean-up.
    """
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(_NO_SYMBOL_LOOKUP), *units],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    assert result.stdout.splitlines()[-1] == "kept 3"


@pytest.mark.parametrize("depth", [1, 3])
def test_an_instance_that_is_not_the_first_of_its_key_is_not_kept(depth: int) -> None:
    """A constructor that makes its own type again: the first stored is the one kept.

    Every call returns that instance, and the others, which no caller gets, do not
    stay alive.
    """
    name = f"test.lifetime.loser.{depth}"
    made = _made(name)
    constructed: list[weakref.ref[Any]] = []
    inner: list[Any] = []

    class Nested(made):
        def __init__(self) -> None:
            constructed.append(weakref.ref(self))
            level = len(constructed)
            pa.ExtensionType.__init__(self, pa.int64(), name)
            if level <= depth:
                # The same key, made and stored before this instance is.
                inner.append(Nested())

    outer = Nested()
    assert len(constructed) == depth + 1
    assert all(t is outer for t in inner)
    # The innermost call stored first: its instance is the type.
    assert constructed[-1]() is outer
    again = Nested()
    assert again is outer
    del again, inner[:]
    gc.collect()
    alive = [ref() is not None for ref in constructed]
    assert alive[: depth + 1] == [False] * depth + [True]
    assert not any(alive[depth + 1 :])
    del outer
    gc.collect()
    assert constructed[depth]() is not None


def test_the_metadata_is_serialized_once_for_each_construction() -> None:
    """Finding an instance's key does not call the serializer again."""
    name = "test.lifetime.serialized_once"
    made = _made(name)
    calls: list[str] = []

    class Counted(made):
        def __init__(self, unit: str) -> None:
            self.unit = unit
            pa.ExtensionType.__init__(self, pa.int64(), name)

        def __arrow_ext_serialize__(self) -> bytes:
            calls.append(self.unit)
            return self.unit.encode()

    units = ["m", "s", "m", "kg", "s", "m"]
    types = [Counted(unit) for unit in units]
    assert calls == units
    assert [t.unit for t in types] == units
    assert len({id(t) for t in types}) == 3
    assert all(t is types[units.index(u)] for t, u in zip(types, units))


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


_CHILD = '''
    import asyncio
    import sys
    from dataclasses import dataclass

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import DictSource

    @dataclass(frozen=True)
    class Reading:
        """Two fields stored together as a logical type."""

        label: str
        level: int

    work, mode, path, arrangement, output_order = sys.argv[1:]
    if output_order == "members_first":
        OUTPUT_KEYS = ("members", "reading")

        @function_pod(output_keys=OUTPUT_KEYS)
        def produce(i: int, name: str) -> tuple[set[bytes], Reading]:
            """Return the set before the reading."""
            return {name.encode(), str(i).encode()}, Reading(label=name, level=i)
    else:
        OUTPUT_KEYS = ("reading", "members")

        @function_pod(output_keys=OUTPUT_KEYS)
        def produce(i: int, name: str) -> tuple[Reading, set[bytes]]:
            """Return the reading before the set."""
            return Reading(label=name, level=i), {name.encode(), str(i).encode()}

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

    # The declared tag order and the layout of the rows' columns.
    TAGS, COLUMNS = {
        "tags_first": (["subject", "session"], ["subject", "session", "i", "name"]),
        "data_first": (["session", "subject"], ["name", "i", "session", "subject"]),
        "mixed": (["session", "subject"], ["subject", "name", "session", "i"]),
    }[arrangement]

    def source():
        """Build the source with the selected tag and input-column order."""
        return DictSource(
            [{column: row[column] for column in COLUMNS} for row in ROWS],
            tag_columns=TAGS,
            tag_only_columns=True,
            source_id="readings",
        )

    def run(job, **options):
        """Run the job through the selected synchronous or asynchronous path."""
        if path == "async":
            return asyncio.run(job.run_async(**options))
        return job.run(**options)

    def rows_of(stream):
        """Compare rows by tag values and set members in sorted order."""
        return sorted(
            (
                tag.as_dict()["subject"],
                tag.as_dict()["session"],
                sorted(data.as_dict()["members"]),
                (data.as_dict()["reading"].label, data.as_dict()["reading"].level),
            )
            for tag, data in stream.iter_data()
        )

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
    assert node.keys()[1] == OUTPUT_KEYS, node.keys()
    columns = node.as_table().column_names
    assert tuple(column for column in columns if column in OUTPUT_KEYS) == OUTPUT_KEYS, columns
    assert rows_of(node) == EXPECTED, rows_of(node)
    assert rows_of(node.as_source()) == EXPECTED, rows_of(node.as_source())
    print("read", len(EXPECTED), flush=True)
'''


@pytest.mark.parametrize("path", ["sync", "async"])
@pytest.mark.parametrize("arrangement", ["tags_first", "data_first", "mixed"])
@pytest.mark.parametrize("output_order", ["members_first", "reading_first"])
def test_a_process_that_read_logical_types_exits_cleanly(
    tmp_path: Path, path: str, arrangement: str, output_order: str
) -> None:
    """Processes that run, save, load and read a job's rows exit with code 0.

    Four rows, two tags and two outputs of two logical types (``set[bytes]`` and a
    dataclass), on the sync and the async job path, with both tag and output orders
    and three input-column layouts. The first process stores the rows and each later
    one loads the saved job and reads them from the store.
    """
    for mode in ["save", *["load"] * 5]:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent(_CHILD),
                str(tmp_path),
                mode,
                path,
                arrangement,
                output_order,
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert result.returncode == 0, result.stderr[-3000:]
        assert result.stdout.splitlines()[-1] == "read 4"
