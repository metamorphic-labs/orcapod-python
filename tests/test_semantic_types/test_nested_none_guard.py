"""A None at a nested position that a read cannot restore is refused (ITL-1144).

A None inside a container (a list or set element, a dict value, a user-class field) is
stored faithfully, but reading the stored column back to Python restored it only for a
primitive or a user class: a container read as empty and any other logical type crashed.
Until the read restores them (ITL-1144 follow-up), such declarations are refused when the
type is declared, and a stored column holding such a None raises when it is read.
"""

import dataclasses
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pytest

import orcapod as op
from orcapod.operators import Batch, GroupBy
from orcapod.sources import ArrowTableSource, DictSource

U = uuid.UUID(int=1)
P = Path("/tmp/itl-1144")


@dataclasses.dataclass
class Point:
    x: int


@op.dataclass
class StructuralPoint:
    x: int


@dataclasses.dataclass
class OptionalList:
    x: list[int] | None


@dataclasses.dataclass
class OptionalUUID:
    x: uuid.UUID | None


@dataclasses.dataclass
class OptionalPath:
    x: Path | None


@dataclasses.dataclass
class OptionalScalars:
    x: int | None
    xs: list[int | None]
    p: Point | None


REFUSED_TYPES = [
    list[set[int] | None],
    list[list[int] | None],
    list[dict[str, int] | None],
    dict[str, list[int] | None],
    list[uuid.UUID | None],
    set[uuid.UUID | None],
    list[Path | None],
    list[list[uuid.UUID] | None],
    list[np.ndarray | None],
]

REFUSED_CLASSES = [OptionalList, OptionalUUID, OptionalPath]


def _source(value: Any, annotation: Any) -> DictSource:
    """A one-row source holding ``value`` in column ``v`` declared as ``annotation``."""
    return DictSource(
        [{"id": 1, "v": value}],
        tag_columns=["id"],
        data_schema={"id": int, "v": annotation},
        source_id="v",
    )


def _read_v(source: Any) -> tuple[Any, str]:
    """The value of column ``v`` read back, and its row's data hash."""
    [(_, data)] = list(source.iter_data())
    return data.as_dict()["v"], data.content_hash().to_string()


def _rows(stream: Any) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """Every row of ``stream`` as Python values."""
    return [(tag.as_dict(), data.as_dict()) for tag, data in stream.iter_data()]


class TestRefusedAtDefinition:
    """A declared type with a None the read cannot restore is refused, naming the column."""

    @pytest.mark.parametrize("annotation", REFUSED_TYPES, ids=str)
    def test_a_source_schema_is_refused(self, annotation: Any) -> None:
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1144"):
            _source(None, annotation)

    @pytest.mark.parametrize("annotation", REFUSED_TYPES, ids=str)
    def test_a_function_output_is_refused(self, annotation: Any) -> None:
        def produce(n: int) -> Any:
            return None

        produce.__annotations__["return"] = annotation
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1144"):
            op.function_pod(output_keys="v")(produce)

    @pytest.mark.parametrize("annotation", REFUSED_TYPES, ids=str)
    def test_a_function_input_is_refused(self, annotation: Any) -> None:
        def consume(v: Any) -> int:
            return 0

        consume.__annotations__["v"] = annotation
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1144"):
            op.function_pod(output_keys="n")(consume)

    @pytest.mark.parametrize("cls", REFUSED_CLASSES, ids=lambda c: c.__name__)
    def test_a_class_field_is_refused_naming_the_field(self, cls: type) -> None:
        with pytest.raises(
            ValueError, match=rf"(?s)Column 'v'.*{cls.__qualname__}\.x.*ITL-1144"
        ):
            _source(cls(None), cls)

    def test_the_message_says_what_to_do(self) -> None:
        with pytest.raises(ValueError, match=r"list\[int\] \| None") as info:
            _source([None], list[list[int] | None])
        assert "without '| None'" in str(info.value)


ACCEPTED = [
    (list[int | None], [None, 1]),
    (list[str | None], [None, "a"]),
    (list[list[int | None]], [[None, 1]]),
    (dict[str, int | None], {"j": 1, "k": None}),
    (set[int] | None, None),
    (list[uuid.UUID] | None, None),
    (uuid.UUID | None, None),
    (list[Point | None], [None, Point(1)]),
    (list[StructuralPoint | None], [None, StructuralPoint(1)]),
    (OptionalScalars, OptionalScalars(None, [None, 1], None)),
]


class TestAccepted:
    """A None that a read restores stays accepted: same value, same row hash."""

    @pytest.mark.parametrize(("annotation", "value"), ACCEPTED, ids=str)
    def test_round_trips(self, annotation: Any, value: Any) -> None:
        read, written_hash = _read_v(_source(value, annotation))
        assert read == value
        _, read_hash = _read_v(_source(read, annotation))
        assert read_hash == written_hash


class TestRaisedAtRead:
    """A stored None the read cannot restore raises instead of a wrong value."""

    def test_an_arrow_table_with_a_null_inner_list(self) -> None:
        table = pa.table(
            {
                "id": pa.array([1], pa.int64()),
                "v": pa.array([[None, [1]]], pa.large_list(pa.large_list(pa.int64()))),
            }
        )
        source = ArrowTableSource(table, tag_columns=["id"], source_id="v")
        with pytest.raises(ValueError, match=r"(?s)Column 'v'.*ITL-1144"):
            _read_v(source)


def _members(annotation: Any, first: Any, second: Any) -> DictSource:
    """Two rows of group ``g=1``: column ``m`` holds ``first`` then ``second``."""
    return DictSource(
        [{"g": 1, "k": 1, "m": first}, {"g": 1, "k": 2, "m": second}],
        tag_columns=["g", "k"],
        data_schema={"g": int, "k": int, "m": annotation},
        source_id="m",
    )


OPERATORS = {
    "group_by": lambda s: GroupBy(by=["g"])(s),
    "batch": lambda s: Batch(2)(s),
}


class TestReducingOperators:
    """GroupBy and Batch list a nullable column's values: a None stays where restorable."""

    @pytest.mark.parametrize("operator", OPERATORS)
    def test_a_nullable_int_column_round_trips(self, operator: str) -> None:
        out = OPERATORS[operator](_members(int | None, None, 2))
        [(_, data)] = _rows(out)
        assert data["m"] == [None, 2]

    @pytest.mark.parametrize("operator", OPERATORS)
    @pytest.mark.parametrize(
        ("annotation", "second"),
        [(list[int] | None, [1]), (set[int] | None, {1}), (Path | None, P), (uuid.UUID | None, U)],
        ids=["list", "set", "Path", "UUID"],
    )
    def test_a_nullable_container_or_logical_column_raises_at_read(
        self, operator: str, annotation: Any, second: Any
    ) -> None:
        out = OPERATORS[operator](_members(annotation, None, second))
        with pytest.raises(ValueError, match=r"(?s)Column 'm'.*ITL-1144"):
            _rows(out)

    def test_a_function_declared_for_such_a_list_is_refused(self) -> None:
        def consume(m: list[list[int] | None]) -> int:
            return 0

        with pytest.raises(ValueError, match=r"(?s)Column 'm'.*ITL-1144"):
            op.function_pod(output_keys="n")(consume)
