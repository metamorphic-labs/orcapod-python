"""Python truth tests cannot build row conditions."""

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, col
from orcapod.sources import ArrowTableSource, SpiralDBTableSource
from orcapod.types import ColumnRef, Condition
from tests.test_databases import fake_spiral_sdk

pytestmark = pytest.mark.filterwarnings(
    "ignore::orcapod.errors.UnrecordedTableSchemaWarning"
)

_TRUTH_ERROR = "has no truth value"
_ApplyCondition = Callable[[Condition], object]


@pytest.fixture
def table() -> pa.Table:
    return pa.table(
        {
            "session": ["s1", "s1", "s1", "s2", "s2"],
            "t": [0, 1, 2, 3, 4],
            "u": [1, 3, 2, 0, 5],
            "v": [2, 2, 3, 1, 6],
        }
    )


@pytest.fixture
def stream(table: pa.Table) -> ArrowTableSource:
    return ArrowTableSource(table, tag_columns=["session", "t"], tag_only_columns=True)


@pytest.fixture
def spiral(table: pa.Table, tmp_path: Path) -> Iterator[None]:
    with fake_spiral_sdk.install(tmp_path / "spiral"):
        project = fake_spiral_sdk.ExternalProject("truth-tests")
        keys = pa.schema([pa.field("session", pa.string()), pa.field("t", pa.int64())])
        _ = project.create_table("default.samples", key_schema=keys).write(table)
        yield


@pytest.fixture(params=["stream", "job", "where", "restriction"])
def apply_condition(
    request: pytest.FixtureRequest, stream: ArrowTableSource
) -> Iterator[_ApplyCondition]:
    target = cast(str, request.param)
    if target == "stream":
        yield stream.filter
    elif target == "job":
        with PipelineJob(name="truth_tests"):
            yield stream.filter
    else:
        request.getfixturevalue("spiral")
        if target == "where":
            yield SpiralDBTableSource("truth-tests", "samples").where
        else:

            def restriction(condition: Condition) -> SpiralDBTableSource:
                return SpiralDBTableSource("truth-tests", "samples", where=condition)

            yield restriction


def _if(left: object, right: object) -> object:
    if left:
        return right
    return left


# Exercise runtime truth tests that static typing can catch when it knows the operands.
_TRUTH_TESTS: dict[str, Callable[[object, object], object]] = {
    "and": lambda left, right: left and right,
    "or": lambda left, right: left or right,
    "not": lambda left, _: not left,
    "if": _if,
    "bool": lambda left, _: bool(left),
}


@pytest.mark.parametrize("two_columns", [False, True], ids=["literal", "two-columns"])
@pytest.mark.parametrize("operation", _TRUTH_TESTS)
def test_python_truth_tests_raise(
    apply_condition: _ApplyCondition, operation: str, two_columns: bool
) -> None:
    left, right = (
        (col("t") < col("u"), col("u") < col("v"))
        if two_columns
        else (col("session") == "s1", col("t") == 2)
    )
    with pytest.raises(TypeError, match=_TRUTH_ERROR):
        # A mistaken truth test can produce a bool on older versions. Pass it on
        # so the test distinguishes the truth error from argument validation.
        _ = apply_condition(cast(Condition, _TRUTH_TESTS[operation](left, right)))


@pytest.mark.parametrize("two_columns", [False, True], ids=["literal", "two-columns"])
def test_chained_comparison_raises(
    apply_condition: _ApplyCondition, two_columns: bool
) -> None:
    with pytest.raises(TypeError, match=_TRUTH_ERROR):
        if two_columns:
            _ = apply_condition(col("t") < col("u") < col("v"))
        else:
            _ = apply_condition(0 < col("t") < 3)


@pytest.mark.parametrize(
    "predicate",
    [Condition(), col("t") < 3, col("t") < col("u"), col("t")],
    ids=["empty", "literal", "two-columns", "column-ref"],
)
def test_truth_error_explains_how_to_write_the_condition(
    predicate: Condition | ColumnRef,
) -> None:
    with pytest.raises(TypeError, match=_TRUTH_ERROR) as error:
        _ = bool(predicate)

    message = str(error.value)
    assert "separate arguments to filter() or where()" in message
    assert "& (and), | (or), and ~ (not)" in message
    assert "each comparison in parentheses" in message
    assert ".between(a, b)" in message
    assert "both ends included" in message


def test_filter_composition_selects_rows_and_chaining_is_refused(
    stream: ArrowTableSource,
) -> None:
    t, u, v = col("t"), col("u"), col("v")
    assert stream.filter(t > 0, t <= 3).as_table()["t"].to_pylist() == [1, 2, 3]
    assert stream.filter(t.between(1, 3)).as_table()["t"].to_pylist() == [1, 2, 3]
    assert stream.filter((t > 0) & (t <= 3)).as_table()["t"].to_pylist() == [1, 2, 3]
    assert stream.filter((t < 1) | (t > 3)).as_table()["t"].to_pylist() == [0, 4]
    assert stream.filter(~(t <= 3)).as_table()["t"].to_pylist() == [4]
    assert stream.filter(t < u, u < v).as_table()["t"].to_pylist() == [0, 4]
    assert stream.filter((t < u) & (u < v)).as_table()["t"].to_pylist() == [0, 4]
    assert stream.filter((t < u) | (u < v)).as_table()["t"].to_pylist() == [
        0,
        1,
        2,
        3,
        4,
    ]
    assert stream.filter(~(t < u)).as_table()["t"].to_pylist() == [2, 3]

    with pytest.raises(TypeError, match=_TRUTH_ERROR):
        _ = stream.filter(1 <= t <= 3)
