"""The stored order of a ``set[T]`` whose element ``T`` is a logical type (ITL-922).

A set has no order, so the order it is stored in, and with it the row hash, must come
from its elements' values alone. A set of a logical type whose storage value is a scalar
(text, bytes, a number) is put in the canonical order of
``canonical_order.sort_set_elements``, as a set of a plain Arrow type is: a NaN, two
elements with one storage value and elements that cannot be ordered are refused.

A set of a logical type stored as a struct (a user class: a dataclass, a structural
class, a pydantic model) is left as it was, ordered by the ``repr`` of the storage
values, pending the developer's decision in ITL-922. The tests at the end pin that order,
so a later change of it is visible.
"""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

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
        return self._arrow_ext_class()

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
        return self._arrow_ext_class()

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
    return dict(zip(table.column("site").to_pylist(), column.to_pylist()))


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


# ── struct-stored elements (user classes): today's order, pinned ─────────────
#
# Pending the developer's decision in ITL-922. A user class is stored as a struct, which
# has no canonical order yet, so its sets keep the order they had: by the ``repr`` of the
# storage values (text order: 10 before 2). These tests pin that order so that a change
# of it, which moves the row hash of every such set, is visible.


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
def test_user_class_elements_keep_their_order(point: Any, arrangement: str) -> None:
    """A set of a user class is stored in the ``repr`` order of its structs (ITL-922)."""
    points = [point(a=2, b="a"), point(a=1, b="b"), point(a=10, b="c")]
    stored = _stored(_source([set(points), set(reversed(points))], point, arrangement))
    expected = [{"a": 1, "b": "b"}, {"a": 10, "b": "c"}, {"a": 2, "b": "a"}]
    assert stored == {"site0": expected, "site1": expected}
