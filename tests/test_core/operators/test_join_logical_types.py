"""Join compatibility checks Arrow extension identifiers at every depth."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import polars as pl
import pyarrow as pa
import pytest
from typing_extensions import override

from orcapod.contexts import (
    create_registry,
    get_default_context,
    get_default_type_converter,
)
from orcapod.core.operators import Join, MergeJoin, SemiJoin
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.identifier import arrow_type_identifier
from orcapod.logical_types.protocols import TypeConverterProtocol
from orcapod.logical_types.registry import make_polars_extension_type
from orcapod.semantic_types.universal_converter import UniversalTypeConverter

from .several_items import (
    DATA_FIRST_REVERSED,
    DATA_TAGS,
    MAIN,
    MIXED,
    P,
    PLAIN,
    Q,
    TAGS_FIRST,
    Arrangement,
    Spec,
    async_rows,
    make_source,
    make_table,
)


class PeriodType(pa.ExtensionType):
    """An Arrow type with one Python class and a frequency for each instance."""

    def __init__(self, frequency: bytes) -> None:
        """Store the frequency in the extension metadata."""
        self.frequency: bytes = frequency
        super().__init__(pa.int64(), "example.itl1380.period")

    @override
    def __arrow_ext_serialize__(self) -> bytes:
        """Return the frequency bytes."""
        return self.frequency

    @classmethod
    @override
    def __arrow_ext_deserialize__(
        cls, storage_type: pa.DataType, serialized: bytes
    ) -> PeriodType:
        """Reconstruct the period with its stored frequency."""
        return cls(serialized)


class Period(int):
    """Python value type for the registered period extension."""


class LogicalPeriod(BaseLogicalType):
    """Register the daily period and the list of periods used by MergeJoin."""

    logical_type_name: str = "example.itl1380.period"
    python_type: type[Period] = Period

    @override
    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the canonical period type."""
        return PeriodType(b"D")

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the equivalent Polars type."""
        polars_type = cast(
            "Callable[[], pl.BaseExtension]",
            make_polars_extension_type(self.logical_type_name, pa.int64(), "D"),
        )
        return polars_type()

    def python_to_storage(
        self, value: Period, converter: TypeConverterProtocol | None = None
    ) -> int:
        """Store the period ordinal."""
        return int(value)

    def storage_to_python(
        self, storage_value: int, converter: TypeConverterProtocol | None = None
    ) -> Period:
        """Reconstruct a period from its ordinal."""
        return Period(storage_value)


@pytest.fixture
def registered_period(monkeypatch: pytest.MonkeyPatch) -> None:
    """Supply an isolated converter for aggregating otherwise unregistered periods."""
    converter = create_registry().get_context().type_converter
    assert isinstance(converter, UniversalTypeConverter)
    converter.register_logical_type(LogicalPeriod())
    monkeypatch.setattr(get_default_context(), "type_converter", converter)


def _wrap(values: pa.ExtensionArray[pa.Array[Any]], nesting: str) -> pa.Array[Any]:
    """Place an extension at the requested position without converting its values."""
    if nesting == "extension":
        return values
    if nesting == "struct":
        return pa.StructArray.from_arrays(
            [values, values.storage], names=["period", "ordinal"]
        )
    offsets = pa.array(range(len(values) + 1), type=pa.int64())
    if nesting == "list":
        return pa.LargeListArray.from_arrays(offsets, values)
    if nesting == "map-key":
        return pa.MapArray.from_arrays(offsets, values, values.storage)
    assert nesting == "map-value"
    return pa.MapArray.from_arrays(offsets, values.storage, values)


def _with_period(
    spec: Spec, column: str, original: str, frequency: bytes, nesting: str
) -> Spec:
    """Replace the selected shared tag or rename a data column to create a collision."""
    columns = dict(spec.columns)
    values = pa.ExtensionArray.from_storage(
        PeriodType(frequency), columns.pop(original)
    )
    columns[column] = _wrap(values, nesting)
    return replace(
        spec,
        data=tuple(column if name == original else name for name in spec.data),
        columns=columns,
    )


def _stream(spec: Spec, reverse: bool, role: str) -> ArrowTableStream:
    """Wrap the fixture table directly so source hashing does not rebuild its nested arrays."""
    arrangement = Arrangement(
        tuple(reversed(spec.tags)) if reverse else spec.tags,
        DATA_FIRST_REVERSED if reverse else TAGS_FIRST,
        role,
    )
    # Give each common tag the same role in every input.
    data_tags = {
        PLAIN: (),
        DATA_TAGS: arrangement.tag_order,
        MIXED: tuple(name for name in arrangement.tag_order if name != "t2"),
    }[role]
    return ArrowTableStream(
        make_table(spec, arrangement),
        tag_columns=arrangement.tag_order,
        data_tags=data_tags,
    )


def _assert_period_mismatch(error: BaseException, column: str) -> None:
    """Check the validation error, including through async task groups."""
    if isinstance(error, BaseExceptionGroup):
        for child in error.exceptions:
            _assert_period_mismatch(child, column)
        return
    assert isinstance(error, InputValidationError)
    message = str(error)
    assert f"column {column!r}" in message
    assert arrow_type_identifier(PeriodType(b"D")).text in message
    assert arrow_type_identifier(PeriodType(b"M")).text in message


@pytest.mark.parametrize(
    "operator, arity, column, nesting",
    [
        pytest.param(operator, arity, column, nesting, id=f"{case}-{nesting}")
        for operator, arity, column, case in (
            (Join, 2, "t2", "join-two"),
            (Join, 3, "t2", "join-three"),
            (MergeJoin, 2, "t2", "merge-key"),
            (SemiJoin, 2, "t2", "semijoin"),
            (MergeJoin, 2, "value", "merge-data"),
        )
        for nesting in ("extension", "list", "struct")
    ]
    + [
        pytest.param(MergeJoin, 2, "value", nesting, id=f"merge-data-{nesting}")
        for nesting in ("map-key", "map-value")
    ],
)
@pytest.mark.parametrize("path", ["sync", "async"])
@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
@pytest.mark.parametrize("same", [False, True], ids=["different", "same"])
@pytest.mark.parametrize("role", [PLAIN, DATA_TAGS, MIXED])
def test_join_period_parameters(
    operator: type[Join] | type[MergeJoin] | type[SemiJoin],
    arity: int,
    column: str,
    nesting: str,
    path: str,
    reverse: bool,
    same: bool,
    role: str,
    request: pytest.FixtureRequest,
) -> None:
    """All shared periods must agree, including a key present in three inputs."""
    if column == "value" and nesting == "extension" and same:
        _ = request.getfixturevalue("registered_period")
    frequency = b"D" if same else b"M"
    specs = (
        [
            _with_period(MAIN, column, "d1", b"D", nesting),
            _with_period(P, column, "p1", frequency, nesting),
        ]
        if column == "value"
        else [
            _with_period(
                spec, column, column, frequency if i == arity - 1 else b"D", nesting
            )
            for i, spec in enumerate((MAIN, P, Q)[:arity])
        ]
    )
    streams = [_stream(spec, reverse, role) for spec in specs]
    if reverse:
        streams.reverse()
    op = operator()

    def result_count() -> int:
        """Run the requested public execution path and materialize its result."""
        return (
            op(*streams).as_table().num_rows
            if path == "sync"
            else len(async_rows(op, *streams))
        )

    if same:
        assert result_count() == (4 if operator is SemiJoin and reverse else 6)
    else:
        with pytest.raises((InputValidationError, ExceptionGroup)) as error:
            _ = result_count()
        _assert_period_mismatch(error.value, column)


@pytest.mark.parametrize("path", ["sync", "async"])
def test_join_builtin_logical_key(path: str) -> None:
    """The same built-in path identifier remains a matchable shared key."""
    path_type = get_default_type_converter().python_type_to_arrow_type(Path)
    assert isinstance(path_type, pa.ExtensionType)
    streams = [
        make_source(
            replace(
                spec,
                columns={
                    **spec.columns,
                    "t1": pa.ExtensionArray.from_storage(path_type, spec.columns["t1"]),
                },
            )
        )
        for spec in (MAIN, P)
    ]
    if path == "sync":
        table = Join()(*streams).as_table()
        assert table.num_rows == 6
        field_type = table.schema.field("t1").type
        assert isinstance(field_type, pa.ExtensionType)
        assert arrow_type_identifier(field_type) == arrow_type_identifier(path_type)
    else:
        rows = async_rows(Join(), *streams)
        assert len(rows) == 6
        for tag, _ in rows:
            assert isinstance(tag.as_dict()["t1"], Path)
