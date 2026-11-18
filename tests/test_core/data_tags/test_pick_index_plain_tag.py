"""``Pick`` / ``Index`` on a plain tag give the missing-data error (Copilot on PR #342).

Only a data-tag (a name in both the tag and the data schema) is refused for an
in-place replacement with the advice to give ``out=``. A plain tag is not a data
column at all, so ``out=`` would not help: it gets the missing-data diagnostic.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

import orcapod as op
from orcapod.core.operators import Index, Pick
from orcapod.errors import InputValidationError


def _source(tag_only: object) -> op.sources.ArrowTableSource:
    table = pa.table(
        {
            "s": pa.array([{"x": 1}, {"x": 2}], pa.struct([("x", pa.int64())])),
            "l": pa.array([[1, 2], [3]], pa.list_(pa.int64())),
            "v": pa.array([0.5, 0.25], pa.float64()),
        }
    )
    return op.sources.ArrowTableSource(
        table,
        tag_columns=["s", "l"],
        tag_only_columns=tag_only,  # pyright: ignore[reportArgumentType]  # the test varies it
        source_id="nested",
    )


CASES: list[tuple[str, Callable[[str | None], Any]]] = [  # pyright: ignore[reportExplicitAny]  # operator factories
    ("pick", lambda out: Pick("s", "x", out=out)),
    ("index", lambda out: Index("l", 0, out=out)),
]


@pytest.mark.parametrize(("label", "make_op"), CASES, ids=[c[0] for c in CASES])
@pytest.mark.parametrize("out", [None, "first"], ids=["in_place", "out"])
def test_a_plain_tag_is_not_a_data_column(
    label: str,
    make_op: Callable[[str | None], Any],  # pyright: ignore[reportExplicitAny]  # operator factory
    out: str | None,
) -> None:
    with pytest.raises(InputValidationError, match="not found in data schema"):
        make_op(out)(_source(True))


@pytest.mark.parametrize(("label", "make_op"), CASES, ids=[c[0] for c in CASES])
def test_a_data_tag_in_place_still_asks_for_out(
    label: str,
    make_op: Callable[[str | None], Any],  # pyright: ignore[reportExplicitAny]  # operator factory
) -> None:
    with pytest.raises(InputValidationError, match="is a data-tag"):
        make_op(None)(_source(()))
