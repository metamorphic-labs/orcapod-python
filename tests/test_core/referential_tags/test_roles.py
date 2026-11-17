"""The select and drop operators act on a referential tag by role (ITL-1195).

Developer decision, November 16, 2026: for a referential tag ``x`` (one column in two
roles), dropping the tag removes the tag role only and leaves ``x`` plain data; dropping
the data removes the data role only and leaves ``x`` a plain tag, with a
``ReferenceMaterializationWarning``; dropping the column removes it from both roles. The select operators follow the same rule: selecting tags
or data drops the others by role. Plain tags and plain data columns behave as before.

Every operator is driven through its public call and read through the stream's public
methods; a schema prediction is checked against ``static_process``.
"""

from __future__ import annotations

from collections.abc import Callable
import warnings
from typing import Any

import pytest

import orcapod as op
from orcapod.core.operators import (
    DropColumns,
    DropDataColumns,
    DropTagColumns,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    TagBy,
)
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.core.referential_tags import (
    ReferenceMaterializationWarning,
    stream_references,
)
from orcapod.errors import InputValidationError
from orcapod.system_constants import constants

ROWS = [{"k": 1, "x": 10, "y": 0.5}, {"k": 2, "x": 20, "y": 1.5}]


def _source() -> op.sources.DictSource:
    """Referential tags ``k`` and ``x``; data ``y``."""
    return op.sources.DictSource(ROWS, tag_columns=["k", "x"], source_id="R")


def _system_tags(stream: Any) -> list[dict[str, Any]]:  # pyright: ignore[reportExplicitAny]  # any stream
    table = stream.as_table(columns={"system_tags": True})
    names = [c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)]
    return sorted(table.select(names).to_pylist(), key=repr)


def _source_info(stream: Any) -> dict[str, list[Any]]:  # pyright: ignore[reportExplicitAny]  # any stream
    table = stream.as_table(columns={"source": True})
    return {
        name.removeprefix(constants.SOURCE_PREFIX): table.column(name).to_pylist()
        for name in table.column_names
        if name.startswith(constants.SOURCE_PREFIX)
    }


# (operator, expected keys, expected referential tags): ``x`` is the one moved.
ROLE_CASES: list[tuple[str, Callable[[], StaticOutputOperatorPod], Any, Any]] = [
    # Dropping or not selecting the tag: the tag role goes, x is plain data.
    ("drop_tag", lambda: DropTagColumns(["x"]), (("k",), ("k", "x", "y")), ("k",)),
    ("select_tag", lambda: SelectTagColumns(["k"]), (("k",), ("k", "x", "y")), ("k",)),
    # Dropping or not selecting the data: the data role goes, x is a plain tag.
    ("drop_data", lambda: DropDataColumns(["x"]), (("k", "x"), ("k", "y")), ("k",)),
    (
        "select_data",
        lambda: SelectDataColumns(["k", "y"]),
        (("k", "x"), ("k", "y")),
        ("k",),
    ),
    # Dropping or not selecting the column: both roles go.
    ("drop_column", lambda: DropColumns(["x"]), (("k",), ("k", "y")), ("k",)),
    ("select_column", lambda: SelectColumns(["k", "y"]), (("k",), ("k", "y")), ("k",)),
]
ROLE_IDS = [case[0] for case in ROLE_CASES]


@pytest.mark.parametrize(
    ("label", "make_op", "keys", "references"), ROLE_CASES, ids=ROLE_IDS
)
def test_each_operator_acts_by_role(
    label: str,
    make_op: Callable[[], StaticOutputOperatorPod],
    keys: Any,  # pyright: ignore[reportExplicitAny]  # expected keys
    references: tuple[str, ...],
) -> None:
    source = _source()
    result = make_op()(source)
    assert result.keys() == keys
    assert stream_references(result) == references
    # No value changes: every remaining column holds the source's values.
    for row in result.as_table().to_pylist():
        assert row == {name: _by_k(row["k"])[name] for name in row}
    # System tags and record ids never change.
    assert _system_tags(result) == _system_tags(source)
    # Source info belongs to the data role.
    info = _source_info(result)
    _, data = keys
    assert set(info) == set(data)
    if "x" in data:
        assert info["x"] == _source_info(source)["x"]
    _ = label


@pytest.mark.parametrize(
    ("label", "make_op", "keys", "references"), ROLE_CASES, ids=ROLE_IDS
)
def test_only_removing_the_data_role_warns(
    label: str,
    make_op: Callable[[], StaticOutputOperatorPod],
    keys: Any,  # pyright: ignore[reportExplicitAny]  # unused case field
    references: Any,  # pyright: ignore[reportExplicitAny]  # unused case field
) -> None:
    """Dropping or selecting away the data of ``x`` leaves a plain tag and warns
    (``ReferenceMaterializationWarning``); removing the tag role or the whole column
    is silent (developer, November 17, 2026: keep the warning for now)."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _ = make_op()(_source()).keys()
    warned = any(w.category is ReferenceMaterializationWarning for w in caught)
    assert warned == (label in ("drop_data", "select_data"))


def _by_k(k: int) -> dict[str, Any]:  # pyright: ignore[reportExplicitAny]  # row values
    return next(row for row in ROWS if row["k"] == k)


@pytest.mark.parametrize(
    ("label", "make_op", "keys", "references"), ROLE_CASES, ids=ROLE_IDS
)
@pytest.mark.parametrize("all_info", [False, True])
def test_the_schema_is_predicted_by_role(
    label: str,
    make_op: Callable[[], StaticOutputOperatorPod],
    keys: Any,  # pyright: ignore[reportExplicitAny]  # expected keys
    references: tuple[str, ...],
    all_info: bool,
) -> None:
    source = _source()
    operator = make_op()
    predicted = operator.output_schema(source, all_info=all_info)
    assert predicted == operator.static_process(source).output_schema(all_info=all_info)
    _ = (label, keys, references)


def test_dropping_the_data_role_of_the_last_data_column_is_refused() -> None:
    only = op.sources.DictSource(
        [{"k": 1, "x": 10}],
        tag_columns=["k", "x"],
        tag_only_columns=["k"],
        source_id="O",
    )
    assert only.keys() == (("k", "x"), ("x",))
    with pytest.raises(InputValidationError, match="no data column would remain"):
        _ = DropDataColumns(["x"])(only)


def test_a_role_that_is_not_there_is_a_missing_name() -> None:
    plain = op.sources.DictSource(
        ROWS, tag_columns=["k", "x"], tag_only_columns=["x"], source_id="P"
    )
    # A plain tag has no data role; a data-only column has no tag role.
    with pytest.raises(InputValidationError, match="Missing data columns"):
        _ = DropDataColumns(["x"])(plain)
    with pytest.raises(InputValidationError, match="Missing tag columns"):
        _ = DropTagColumns(["y"])(plain)


def test_plain_tags_and_data_are_dropped_as_before() -> None:
    plain = op.sources.DictSource(
        ROWS, tag_columns=["k", "x"], tag_only_columns=True, source_id="P"
    )
    assert DropTagColumns(["x"])(plain).keys() == (("k",), ("y",))
    assert DropColumns(["x"])(plain).keys() == (("k",), ("y",))
    assert SelectTagColumns(["k"])(plain).keys() == (("k",), ("y",))


def test_tag_by_then_drop_tag_gives_back_the_rows() -> None:
    plain = op.sources.DictSource(
        ROWS, tag_columns=["k"], tag_only_columns=True, source_id="P"
    )
    back = DropTagColumns(["x"])(TagBy("x")(plain))
    assert back.keys() == plain.keys()
    assert sorted(back.as_table().to_pylist(), key=repr) == sorted(
        plain.as_table().to_pylist(), key=repr
    )
    assert _source_info(back) == _source_info(plain)
