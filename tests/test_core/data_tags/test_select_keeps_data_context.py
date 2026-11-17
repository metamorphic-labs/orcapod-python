"""The select and drop operators keep their input's data context (CodeRabbit on PR #342).

Their sync results rebuild the stream from the input's table: they must keep the
input's data context and each row's context column, as ``without_input_meta`` and the
combined ``SelectColumns`` / ``DropColumns`` do. One data context ships, so a copy of
it under another key stands in for a second one.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

import orcapod.contexts as contexts
from orcapod.core.operators import (
    DropColumns,
    DropDataColumns,
    DropTagColumns,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
)
from orcapod.core.streams import ArrowTableStream
from orcapod.system_constants import constants

OTHER = "std:v1:test-other"


@pytest.fixture
def other_context(monkeypatch: pytest.MonkeyPatch) -> contexts.DataContext:
    """A copy of the default data context, registered under another key."""
    other = dataclasses.replace(contexts.get_default_context(), context_key=OTHER)
    registry = contexts._get_registry()  # pyright: ignore[reportPrivateUsage]  # the test registers a second context
    original = registry.get_context

    def get_context(key: Any = None) -> contexts.DataContext:  # pyright: ignore[reportExplicitAny]  # the registry's own argument
        return other if key == OTHER else original(key)

    monkeypatch.setattr(registry, "get_context", get_context)
    return other


def _stream(context: contexts.DataContext) -> ArrowTableStream:
    table = pa.table(
        {
            "k": pa.array([1, 2], pa.int64()),
            "u": pa.array([3, 4], pa.int64()),
            "a": pa.array([0.5, 0.25], pa.float64()),
            "b": pa.array([1.0, 2.0], pa.float64()),
            constants.CONTEXT_KEY: pa.array([OTHER, OTHER], pa.large_string()),
        }
    )
    return ArrowTableStream(
        table, tag_columns=["k", "u"], data_tags=["k"], data_context=context
    )


OPERATORS: list[tuple[str, Callable[[], Any]]] = [  # pyright: ignore[reportExplicitAny]  # operator factories
    ("select_tag_columns", lambda: SelectTagColumns(["k"])),
    ("drop_tag_columns", lambda: DropTagColumns(["u"])),
    ("select_data_columns", lambda: SelectDataColumns(["k", "a"])),
    ("drop_data_columns", lambda: DropDataColumns(["b"])),
    ("select_columns", lambda: SelectColumns(["k", "u", "a"])),
    ("drop_columns", lambda: DropColumns(["b"])),
]


@pytest.mark.parametrize(("label", "make_op"), OPERATORS, ids=[o[0] for o in OPERATORS])
def test_the_result_keeps_the_input_data_context(
    other_context: contexts.DataContext,
    label: str,
    make_op: Callable[[], Any],  # pyright: ignore[reportExplicitAny]  # operator factory
) -> None:
    stream = _stream(other_context)
    assert stream.data_context_key == OTHER
    result = make_op().static_process(stream)
    assert result.data_context_key == OTHER
    table = result.as_table(columns={"context": True})
    assert table.column(constants.CONTEXT_KEY).to_pylist() == [OTHER, OTHER]
    assert [data.data_context_key for _, data in result.iter_data()] == [OTHER, OTHER]
