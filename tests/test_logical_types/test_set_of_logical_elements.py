"""The stored order of a ``set[T]`` whose element ``T`` is a logical type (ITL-922).

A set has no order, so the order it is stored in, and with it the row hash, must come
from its elements' values alone. A set of a logical type whose storage value is a scalar
(text, bytes, a number) is put in the canonical order of
``canonical_order.sort_set_elements``, as a set of a plain Arrow type is: a NaN, two
elements with one storage value and elements that cannot be ordered are refused.

Struct storage is compared by declared field order, recursively through lists and
nested structs.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from random import Random
from typing import Any, Never, cast

import numpy as np
import polars as pl
import pyarrow as pa
import pydantic
import pytest
from typing_extensions import override
from upath import UPath

import orcapod as op
from orcapod.contexts import create_registry, get_default_context
from orcapod.errors import CanonicalOrderError, RowFailuresWarning
from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.identifier import version_metadata
from orcapod.logical_types.protocols import TypeConverterProtocol
from orcapod.logical_types.registry import (
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.semantic_types.canonical_order import (
    ordered_set_storage,
    sort_set_elements,
)
from orcapod.semantic_types.universal_converter import UniversalTypeConverter
from orcapod.sources import DictSource

# ── user-registered logical types ────────────────────────────────────────────


class Reading(float):
    """A value of a user-registered logical type stored as a float."""


class LogicalReading(BaseLogicalType):
    """A user-registered logical type whose storage is ``double``."""

    logical_type_name: str = "example.itl922.reading"
    python_type: type[Reading] = Reading
    _arrow_ext_class = make_arrow_extension_type(
        "example.itl922.reading", pa.float64(), version_metadata(1)
    )

    @override
    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the reading extension type."""
        return self._arrow_ext_class()  # pyright: ignore[reportCallIssue]  # factory fixes the extension name and storage type

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the equivalent Polars type."""
        polars_type = cast(
            "Callable[[], pl.BaseExtension]",
            make_polars_extension_type(
                self.logical_type_name, pa.float64(), version_metadata(1).decode()
            ),
        )
        return polars_type()

    def python_to_storage(
        self, value: Reading, converter: TypeConverterProtocol | None = None
    ) -> float:
        """Store the reading as a plain float."""
        return float(value)

    def storage_to_python(
        self, storage_value: float, converter: TypeConverterProtocol | None = None
    ) -> Reading:
        """Rebuild a reading from its float."""
        return Reading(storage_value)


@dataclasses.dataclass(frozen=True)
class Word:
    """A word that is told apart by its spelling but stored in lower case."""

    text: str


class LogicalWord(BaseLogicalType):
    """A user-registered logical type that stores two different values as one."""

    logical_type_name: str = "example.itl922.word"
    python_type: type[Word] = Word
    _arrow_ext_class = make_arrow_extension_type(
        "example.itl922.word", pa.large_string(), version_metadata(1)
    )

    @override
    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the word extension type."""
        return self._arrow_ext_class()  # pyright: ignore[reportCallIssue]  # factory fixes the extension name and storage type

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the equivalent Polars type."""
        polars_type = cast(
            "Callable[[], pl.BaseExtension]",
            make_polars_extension_type(
                self.logical_type_name, pa.large_string(), version_metadata(1).decode()
            ),
        )
        return polars_type()

    def python_to_storage(
        self, value: Word, converter: TypeConverterProtocol | None = None
    ) -> str:
        """Store the word in lower case."""
        return value.text.lower()

    def storage_to_python(
        self, storage_value: str, converter: TypeConverterProtocol | None = None
    ) -> Word:
        """Rebuild a word from its stored text."""
        return Word(storage_value)


@pytest.fixture
def registered_types(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register the two user logical types with an isolated converter."""
    converter = create_registry().get_context().type_converter
    assert isinstance(converter, UniversalTypeConverter)
    converter.register_logical_type(LogicalReading())
    converter.register_logical_type(LogicalWord())
    monkeypatch.setattr(get_default_context(), "type_converter", converter)


# ── sources with several tag and data columns, in two arrangements ───────────

ARRANGEMENTS = ("tags_first", "data_first_reversed")
"""Two layouts of one row: ``site, day, s, n`` and ``n, s, day, site``."""


def _source(
    sets: list[set[Any]], element: Any, arrangement: str = "tags_first"
) -> DictSource:
    """Return a source of one row per set, with two tag and two data columns.

    Args:
        sets: The value of the set column ``s`` of each row.
        element: The element type of the set column.
        arrangement: One of ``ARRANGEMENTS``: the order of the row's columns and of
            the declared tag columns.

    Returns:
        The source; its data columns are ``s`` (the set) and ``n`` (an integer).
    """
    schema: dict[str, Any] = {"site": str, "day": int, "s": set[element], "n": int}
    tags = ["site", "day"]
    names = list(schema)
    if arrangement == "data_first_reversed":
        names.reverse()
        tags.reverse()
    rows = [
        {"site": f"site{i}", "day": i, "s": value, "n": 10 * i}
        for i, value in enumerate(sets)
    ]
    return DictSource(
        data=[{name: row[name] for name in names} for row in rows],
        tag_columns=tags,
        tag_only_columns=True,
        data_schema={name: schema[name] for name in names},
        source_id="itl922",
    )


def _stored(source: DictSource) -> dict[str, list[Any]]:
    """Return the stored elements of the set column ``s``, by the ``site`` tag."""
    table = source.as_table()
    column = table.column("s").combine_chunks()
    if isinstance(column, pa.ExtensionArray):
        column = column.storage
    sites = cast(list[str], table.column("site").to_pylist())
    values = cast(list[list[Any]], column.to_pylist())
    return dict(zip(sites, values))


def _row_hashes(source: DictSource) -> dict[str, str]:
    """Return each row's data content hash, by the ``site`` tag."""
    return {
        cast(str, tag["site"]): data.content_hash().to_string()
        for tag, data in source.iter_data()
    }


# ── a float-stored logical type (the fault of ITL-922) ───────────────────────


@pytest.mark.usefixtures("registered_types")
class TestSetOfFloatStoredLogicalType:
    """A set of a logical type stored as a float is in canonical order or refused."""

    @pytest.mark.parametrize("arrangement", ARRANGEMENTS)
    def test_nan_element_is_refused(self, arrangement: str) -> None:
        """A NaN has no order: the set is refused, never stored in iteration order."""
        with pytest.raises(CanonicalOrderError, match="NaN"):
            _source(
                [{Reading(1.0)}, {Reading("nan"), Reading(1.0), Reading(-2.0)}],
                Reading,
                arrangement,
            ).as_table()

    def test_a_nan_alone_is_refused(self) -> None:
        """A one-element set holding a NaN is refused too."""
        with pytest.raises(CanonicalOrderError, match="NaN"):
            _source([{Reading("nan")}], Reading).as_table()

    @pytest.mark.parametrize("arrangement", ARRANGEMENTS)
    def test_elements_are_stored_in_numeric_order(self, arrangement: str) -> None:
        """The stored order is numeric, whatever order the elements were given in."""
        values = [2.0, -1.5, 10.0, 0.5]
        forward = {Reading(v) for v in values}
        backward = {Reading(v) for v in reversed(values)}
        stored = _stored(_source([forward, backward], Reading, arrangement))
        assert stored == {
            "site0": [-1.5, 0.5, 2.0, 10.0],
            "site1": [-1.5, 0.5, 2.0, 10.0],
        }

    def test_arrangement_changes_no_row_hash(self) -> None:
        """The two arrangements of the columns give the same row hashes."""
        sets = [{Reading(2.0), Reading(-1.5)}, {Reading(10.0), Reading(0.5)}]
        first, second = (_row_hashes(_source(sets, Reading, a)) for a in ARRANGEMENTS)
        assert first == second
        assert first["site0"] != first["site1"]

    def test_set_is_read_back_as_given(self) -> None:
        """The set a row gives back holds the elements it was given."""
        source = _source([{Reading(2.0), Reading(-1.5)}], Reading)
        _, data = next(iter(source.iter_data()))
        value = data.as_dict()["s"]
        assert value == {Reading(2.0), Reading(-1.5)}
        assert all(isinstance(element, Reading) for element in cast(set[Any], value))


@pytest.mark.usefixtures("registered_types")
class TestSetOfTextStoredLogicalType:
    """A set of a user-registered logical type stored as text."""

    @pytest.mark.parametrize("arrangement", ARRANGEMENTS)
    def test_two_elements_with_one_storage_value_are_refused(
        self, arrangement: str
    ) -> None:
        """Two different elements stored as one value would be stored twice."""
        with pytest.raises(CanonicalOrderError, match="duplicate"):
            _source(
                [{Word("b")}, {Word("A"), Word("a"), Word("c")}], Word, arrangement
            ).as_table()

    @pytest.mark.parametrize("arrangement", ARRANGEMENTS)
    def test_elements_are_stored_in_code_point_order(self, arrangement: str) -> None:
        """Text storage values are ordered by code point."""
        stored = _stored(
            _source(
                [{Word("b"), Word("é"), Word("a"), Word("z")}, {Word("c")}],
                Word,
                arrangement,
            )
        )
        assert stored == {"site0": ["a", "b", "z", "é"], "site1": ["c"]}


# ── a function that returns such a set, sync and async ───────────────────────


def spread(low: float, high: float) -> tuple[set[Reading], int]:
    """Return the readings of one row; one is a NaN when ``high`` is."""
    return {Reading(high), Reading(low), Reading((low + high) / 2)}, 3


def _inputs(rows: list[tuple[float, float]], arrangement: str) -> DictSource:
    """Return the input of ``spread``: two tags and the two parameters."""
    names = ["site", "day", "low", "high"]
    tags = ["site", "day"]
    if arrangement == "data_first_reversed":
        names.reverse()
        tags.reverse()
    full = [
        {"site": f"site{i}", "day": i, "low": low, "high": high}
        for i, (low, high) in enumerate(rows)
    ]
    return DictSource(
        data=[{name: row[name] for name in names} for row in full],
        tag_columns=tags,
        tag_only_columns=True,
        source_id="itl922_inputs",
    )


def _run(job: op.PipelineJob, mode: str) -> None:
    """Run the job on the sync or the async path."""
    if mode == "sync":
        job.run()
    else:
        asyncio.run(job.run_async())


def _job(rows: list[tuple[float, float]], arrangement: str) -> op.PipelineJob:
    """Return a job applying ``spread`` to the rows."""
    job = op.PipelineJob(name="itl922", database=op.databases.InMemoryArrowDatabase())
    # The pod is built here, after the fixture registered the element's logical type.
    pod = op.function_pod(output_keys=("readings", "count"))(spread).pod
    with job:
        pod(_inputs(rows, arrangement), label="spread")
    return job


@pytest.mark.usefixtures("registered_types")
@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("arrangement", ARRANGEMENTS)
class TestFunctionReturningASet:
    """A function output ``set[Reading]`` follows the same rule on both paths."""

    def test_returned_set_is_stored_in_numeric_order(
        self, mode: str, arrangement: str
    ) -> None:
        """The stored output is in numeric order on the sync and the async path."""
        job = _job([(4.0, -2.0), (0.0, 8.0)], arrangement)
        _run(job, mode)
        table = job.nodes["spread"].as_table()
        column = table.column("readings").combine_chunks()
        assert isinstance(column, pa.ExtensionArray)
        stored = dict(zip(table.column("site").to_pylist(), column.storage.to_pylist()))
        assert stored == {"site0": [-2.0, 1.0, 4.0], "site1": [0.0, 4.0, 8.0]}
        assert sorted(table.column("count").to_pylist()) == [3, 3]

    def test_returned_nan_is_refused(self, mode: str, arrangement: str) -> None:
        """A returned set holding a NaN fails its row; the other row is stored."""
        job = _job([(4.0, -2.0), (0.0, float("nan"))], arrangement)
        with pytest.warns(RowFailuresWarning, match="CanonicalOrderError"):
            _run(job, mode)
        table = job.nodes["spread"].as_table()
        assert table.column("site").to_pylist() == ["site0"]
        column = table.column("readings").combine_chunks()
        assert isinstance(column, pa.ExtensionArray)
        assert column.storage.to_pylist() == [[-2.0, 1.0, 4.0]]


# ── built-in scalar-stored logical types: the stored order is unchanged ──────


class TestBuiltInScalarStoredElements:
    """The stored order of a set of a built-in logical type is as it was (ITL-922)."""

    @pytest.mark.parametrize("arrangement", ARRANGEMENTS)
    def test_path_elements(self, arrangement: str) -> None:
        """Paths are stored as text, in code point order."""
        paths = {Path("/b"), Path("/a/z"), Path("/é"), Path("/B"), Path("/a")}
        stored = _stored(_source([paths, {Path("/x")}], Path, arrangement))
        assert stored == {"site0": ["/B", "/a", "/a/z", "/b", "/é"], "site1": ["/x"]}

    @pytest.mark.parametrize("arrangement", ARRANGEMENTS)
    def test_upath_elements(self, arrangement: str) -> None:
        """Universal paths are stored as text, in code point order."""
        paths = {UPath("s3://b/x"), UPath("s3://a/y"), UPath("/local")}
        stored = _stored(_source([paths], UPath, arrangement))
        assert stored == {"site0": ["/local", "s3://a/y", "s3://b/x"]}

    @pytest.mark.parametrize("arrangement", ARRANGEMENTS)
    def test_uuid_elements(self, arrangement: str) -> None:
        """UUIDs are stored as their 16 bytes, in unsigned byte order."""
        numbers = [2**127, 5, 2**64, 255, 256]
        ids = {uuid.UUID(int=n) for n in numbers}
        stored = _stored(_source([ids], uuid.UUID, arrangement))
        assert stored == {"site0": [uuid.UUID(int=n).bytes for n in sorted(numbers)]}

    def test_file_and_directory_elements(self, tmp_path: Path) -> None:
        """Files and directories are stored in the natural order of their storage."""
        for name in ("b.txt", "a.txt", "c.txt"):
            (tmp_path / name).write_text(name)
        for name in ("y", "x"):
            (tmp_path / name).mkdir()
            (tmp_path / name / "f").write_text(name)
        files = {op.File(tmp_path / name) for name in ("b.txt", "a.txt", "c.txt")}
        stored_files = _stored(_source([files], op.File))["site0"]
        assert len(stored_files) == 3
        assert stored_files == sorted(stored_files)
        directories = {op.Directory(tmp_path / name) for name in ("y", "x")}
        stored_directories = _stored(_source([directories], op.Directory))["site0"]
        assert len(stored_directories) == 2
        assert stored_directories == sorted(stored_directories)


# ── struct-stored elements ──────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class ExactPoint:
    """An exact user class."""

    a: int
    b: str


@op.dataclass(name="itl922.StructuralPoint", frozen=True)
class StructuralPoint:
    """A structural user class."""

    a: int
    b: str


class ModelPoint(pydantic.BaseModel):
    """A pydantic user class."""

    model_config = pydantic.ConfigDict(frozen=True)

    a: int
    b: str


@pytest.mark.parametrize("point", [ExactPoint, StructuralPoint, ModelPoint])
@pytest.mark.parametrize("arrangement", ARRANGEMENTS)
def test_user_class_elements_are_stored_in_field_order(
    point: Any, arrangement: str
) -> None:
    """A set of a user class is stored in numeric and then text order."""
    points = [
        point(a=2, b="z"),
        point(a=-10, b="b"),
        point(a=10, b="c"),
        point(a=2, b="a"),
    ]
    stored = _stored(_source([set(points), set(reversed(points))], point, arrangement))
    expected = [
        {"a": -10, "b": "b"},
        {"a": 2, "b": "a"},
        {"a": 2, "b": "z"},
        {"a": 10, "b": "c"},
    ]
    assert stored == {"site0": expected, "site1": expected}


@dataclasses.dataclass(frozen=True)
class PointSubclass(ExactPoint):
    """A distinct value that stores only the base class's fields."""

    extra: int


@dataclasses.dataclass(frozen=True)
class Measurement:
    """A nested struct with an optional numeric field and a sequence."""

    z: float | None
    a: list[float | None]

    @override
    def __hash__(self) -> int:
        """Hash the sequence's items so a measurement can belong to a set."""
        return hash((self.z, tuple(self.a)))


@dataclasses.dataclass(frozen=True)
class Sample:
    """A class whose fields are declared out of alphabetical order."""

    z: Measurement
    a: str

    def __lt__(self, other: Sample) -> bool:
        """Fail if storage ever asks the user class to compare instances."""
        raise AssertionError("the user comparator must not run")


def _samples(*, integral_values: bool = True) -> set[Sample]:
    """Return input values that exercise every level of the sort key."""
    whole = int if integral_values else float
    return {
        Sample(Measurement(whole(10), []), "a"),
        Sample(Measurement(2.5, [whole(10)]), "a"),
        Sample(Measurement(2.5, [whole(2)]), "z"),
        Sample(Measurement(2.5, [whole(2)]), "b"),
        Sample(Measurement(2.5, [None]), "z"),
        Sample(Measurement(2.5, []), "z"),
        Sample(Measurement(whole(-2), []), "z"),
        Sample(Measurement(None, []), "z"),
    }


_SAMPLE_STORAGE = [
    {"z": {"z": None, "a": []}, "a": "z"},
    {"z": {"z": -2.0, "a": []}, "a": "z"},
    {"z": {"z": 2.5, "a": []}, "a": "z"},
    {"z": {"z": 2.5, "a": [None]}, "a": "z"},
    {"z": {"z": 2.5, "a": [2.0]}, "a": "b"},
    {"z": {"z": 2.5, "a": [2.0]}, "a": "z"},
    {"z": {"z": 2.5, "a": [10.0]}, "a": "a"},
    {"z": {"z": 10.0, "a": []}, "a": "a"},
]


def test_nested_structs_and_lists_sort_by_storage_value() -> None:
    """Use declared field order, list order and scalar order, including nulls."""
    assert _stored(_source([_samples()], Sample)) == {"site0": _SAMPLE_STORAGE}


def test_subclass_with_equal_stored_fields_is_refused() -> None:
    """The declared base's storage is identical for two distinct set elements."""
    points = {ExactPoint(2, "a"), PointSubclass(2, "a", 9)}
    assert len(points) == 2
    with pytest.raises(CanonicalOrderError, match=r"Column 's': .*duplicate"):
        _source([points], ExactPoint).as_table()


def test_incomparable_struct_field_values_are_refused() -> None:
    """Report the column when corresponding storage fields cannot be compared."""
    points = {ExactPoint(2, "a"), ExactPoint(cast(Any, "2"), "a")}
    with pytest.raises(CanonicalOrderError, match=r"Column 's': .*cannot be ordered"):
        _source([points], ExactPoint).as_table()


def test_nan_in_nested_list_is_refused() -> None:
    """A NaN deep in a single element is refused even when no comparison is needed."""
    values = {Sample(Measurement(1, [2, float("nan")]), "a")}
    with pytest.raises(CanonicalOrderError, match=r"Column 's': .*NaN"):
        _source([values], Sample).as_table()


def _produce_samples(low: float) -> set[Sample]:
    """Produce the test set, or a set with a nested NaN for the failing row."""
    if low < 0:
        return {Sample(Measurement(1.0, [low, float("nan")]), "a")}
    return _samples(integral_values=False)


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_function_returns_canonically_stored_structs(mode: str) -> None:
    """Both paths store ordered structs and diagnose a nested NaN by column."""
    job = op.PipelineJob(
        name="itl922_struct", database=op.databases.InMemoryArrowDatabase()
    )
    pod = op.function_pod(output_keys="samples")(_produce_samples).pod
    with job:
        pod(_inputs([(0.0, 1.0), (-2.0, 1.0)], "tags_first"), label="produce")
    with pytest.warns(
        RowFailuresWarning, match=r"CanonicalOrderError.*Column 'samples': .*NaN"
    ):
        _run(job, mode)
    table = job.nodes["produce"].as_table()
    assert table.column("site").to_pylist() == ["site0"]
    column = table.column("samples").combine_chunks()
    assert isinstance(column, pa.ExtensionArray)
    assert column.storage.to_pylist() == [_SAMPLE_STORAGE]


def test_struct_storage_order_and_hash_are_independent_of_hash_seed() -> None:
    """Two interpreters serialize and hash the same class set identically."""
    script = """
import json
from tests.test_logical_types.test_set_of_logical_elements import Sample, _row_hashes, _samples, _source, _stored

source = _source([_samples()], Sample)
print(json.dumps([_stored(source), _row_hashes(source)]))
"""
    results = [
        json.loads(
            subprocess.check_output(
                [sys.executable, "-c", script],
                env={**os.environ, "PYTHONHASHSEED": seed},
                text=True,
            )
        )
        for seed in ("1", "2")
    ]
    assert results[0] == results[1]
    assert results[0][0] == {"site0": _SAMPLE_STORAGE}


@dataclasses.dataclass(frozen=True)
class StoredDay:
    """Datetime inputs to this class lose their time of day on write."""

    z: date
    a: str


@dataclasses.dataclass(frozen=True)
class StoredSingle:
    """Different Python floats can round to the same float32."""

    z: np.float32
    a: str


@pytest.mark.parametrize(
    "element, first, second",
    [
        (StoredDay, datetime(2026, 1, 1, 1), datetime(2026, 1, 1, 2)),
        (StoredSingle, float(2**24), float(2**24 + 1)),
    ],
)
def test_duplicates_after_arrow_coercion_are_refused(
    element: Any, first: Any, second: Any
) -> None:
    """Refuse values that only become equal once written into the declared type."""
    values = {element(first, "a"), element(second, "a")}
    assert len(values) == 2
    with pytest.raises(CanonicalOrderError, match=r"Column 's': .*duplicate"):
        _source([values], element).as_table()


def test_ties_after_arrow_coercion_use_the_next_field_and_keep_the_hash() -> None:
    """Re-encoding the stored float32 values preserves both order and content hash."""
    values = {
        StoredSingle(cast(Any, float(2**24)), "z"),
        StoredSingle(cast(Any, float(2**24 + 1)), "a"),
    }
    source = _source([values], StoredSingle)
    _, row = next(iter(source.iter_data()))
    assert row.update(s=row["s"]).content_hash() == row.content_hash()
    assert [item["a"] for item in _stored(source)["site0"]] == ["a", "z"]


def test_stored_string_order_ignores_a_subclass_comparator() -> None:
    """Compare the stored text in code point order even if the inputs disagree."""

    comparisons = 0

    class ReverseText(str):
        @override
        def __lt__(self, other: str) -> bool:
            nonlocal comparisons
            comparisons += 1
            return str.__gt__(self, other)

    original = _source(
        [{ExactPoint(1, ReverseText(s)) for s in ("a", "z")}], ExactPoint
    )
    plain = _source([{ExactPoint(1, s) for s in ("a", "z")}], ExactPoint)
    assert _stored(original) == _stored(plain)
    assert _row_hashes(original) == _row_hashes(plain)
    assert comparisons == 0


def test_builtin_scalar_sets_skip_generic_ordering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The five built-ins keep the single native sort of their storage values."""

    def unexpected(*_args: object, **_kwargs: object) -> Never:
        pytest.fail("a built-in scalar set entered generic canonical ordering")

    monkeypatch.setattr(
        "orcapod.semantic_types.canonical_order.sort_set_elements", unexpected
    )
    for name in ("b", "a"):
        (tmp_path / name).write_text(name)
        (tmp_path / f"{name}-dir").mkdir()
    for element, values in (
        (Path, {Path("/b"), Path("/a")}),
        (UPath, {UPath("s3://b/x"), UPath("s3://a/y")}),
        (uuid.UUID, {uuid.UUID(int=2), uuid.UUID(int=1)}),
        (op.File, {op.File(tmp_path / name) for name in ("a", "b")}),
        (op.Directory, {op.Directory(tmp_path / name) for name in ("a-dir", "b-dir")}),
    ):
        stored = _stored(_source([values], element))["site0"]
        assert len(stored) == 2
        assert stored == sorted(stored)


@pytest.mark.parametrize(
    "storage_type, kind",
    [
        (pa.bool_(), "bool"),
        *((type_(), "signed") for type_ in (pa.int8, pa.int16, pa.int32, pa.int64)),
        *(
            (type_(), "unsigned")
            for type_ in (pa.uint8, pa.uint16, pa.uint32, pa.uint64)
        ),
        *((type_(), "float") for type_ in (pa.float16, pa.float32, pa.float64)),
        *(
            (type_(8, 2), "decimal")
            for type_ in (pa.decimal32, pa.decimal64, pa.decimal128, pa.decimal256)
        ),
        *((type_(), "text") for type_ in (pa.string, pa.large_string, pa.string_view)),
        *(
            (type_(), "binary")
            for type_ in (pa.binary, pa.large_binary, pa.binary_view)
        ),
        (pa.binary(4), "binary"),
        (pa.date32(), "date"),
        (pa.date64(), "date"),
        (pa.time32("ms"), "time"),
        (pa.time64("ns"), "time"),
        (pa.timestamp("ms"), "timestamp"),
        (pa.timestamp("ns", "America/New_York"), "zoned timestamp"),
        (pa.duration("us"), "duration"),
        (pa.duration("ns"), "duration"),
        (pa.null(), "null"),
    ],
    ids=lambda value: str(value),
)
def test_arrow_scalar_storage_order_matches_primitive_order(
    storage_type: pa.DataType, kind: str
) -> None:
    """Arrow's sort on each scalar storage kind follows the primitive order."""
    random = Random(922)
    numbers = random.sample(range(1, 120), 40)
    if kind == "bool":
        values = [False, True]
    elif kind == "signed":
        values = [n - 60 for n in numbers]
    elif kind == "unsigned":
        values = numbers
    elif kind == "float":
        # Half-floats need NumPy scalars on the input path.
        values = [np.float16((n - 60) / 7) for n in numbers]
        values.extend([np.float16(-np.inf), np.float16(np.inf)])
    elif kind == "decimal":
        values = [Decimal(n - 60).scaleb(-2) for n in numbers]
    elif kind == "text":
        values = [random.choice("éÅ𝄞𐀀aZ\x00") + chr(n) for n in numbers]
        values.extend(["", "a", "a\x00", "e\u0301", "é"])
    elif kind == "binary":
        values = [n.to_bytes(2, "big") + random.randbytes(2) for n in numbers]
        values.extend([b"\x00\x00\x00\x00", b"\xff\xff\xff\xff"])
    elif kind == "date":
        values = [date(2026, 1, 1) + timedelta(days=n - 60) for n in numbers]
    elif kind == "time":
        values = [time(n % 24, n % 60, microsecond=n * 1000) for n in numbers]
    elif kind == "timestamp" or kind == "zoned timestamp":
        start = datetime(2026, 11, 1, 4)
        if kind == "zoned timestamp":
            start = start.replace(tzinfo=timezone.utc)
        # Span a repeated local hour in the zoned Arrow storage.
        values = [start + timedelta(minutes=n * 3) for n in numbers]
    elif kind == "duration":
        values = [timedelta(microseconds=n - 60) for n in numbers]
    else:
        assert kind == "null"
        values = []
    shuffled: list[Any] = [*values, None]
    random.shuffle(shuffled)
    array = cast("pa.Array[Any]", pa.array(shuffled, type=storage_type))
    stored = array.to_pylist()
    expected = sort_set_elements(stored)
    assert ordered_set_storage(shuffled, storage_type).to_pylist() == expected


def test_arrow_struct_null_is_distinct_from_a_struct_of_nulls() -> None:
    """Flattening a struct preserves the validity of the struct itself."""
    shape = pa.struct([("z", pa.struct([("n", pa.int64())])), ("a", pa.string())])
    values = [
        {"z": {"n": 0}, "a": "a"},
        {"z": {"n": None}, "a": "a"},
        {"z": None, "a": "z"},
        None,
    ]
    assert ordered_set_storage(values, shape).to_pylist() == values[::-1]


def test_nested_list_order_preserves_arrow_temporal_precision() -> None:
    """List comparison keys use stored nanoseconds even below Python's precision."""
    shape = pa.struct([("z", pa.list_(pa.time64("ns"))), ("a", pa.string())])
    values = [{"z": [3], "a": "a"}, {"z": [2], "a": "z"}]
    assert ordered_set_storage(values, shape).equals(pa.array(values[::-1], type=shape))


def test_arrow_set_order_ties_signed_zero_in_nested_lists() -> None:
    """Zeros with opposite signs are equal storage keys even inside lists."""
    shape = pa.struct([("z", pa.list_(pa.float64()))])
    with pytest.raises(CanonicalOrderError, match="duplicate"):
        ordered_set_storage([{"z": [-0.0]}, {"z": [0.0]}], shape)


def test_encoded_struct_set_column_keeps_null_and_empty_rows() -> None:
    """Assemble typed set storage directly while keeping row offsets and validity."""
    converter = get_default_context().type_converter
    values = [
        None,
        {ExactPoint(10, "a"), ExactPoint(2, "z")},
        set(),
        {ExactPoint(3, "b")},
    ]
    table = converter.python_dicts_to_arrow_table(
        [{"s": value, "i": i} for i, value in enumerate(values)],
        python_schema={"s": set[ExactPoint] | None, "i": int},
    )
    array = table["s"].combine_chunks()
    assert isinstance(array, pa.ExtensionArray)
    assert array.storage.to_pylist() == [
        None,
        [{"a": 2, "b": "z"}, {"a": 10, "b": "a"}],
        [],
        [{"a": 3, "b": "b"}],
    ]
    assert table["i"].to_pylist() == [0, 1, 2, 3]
