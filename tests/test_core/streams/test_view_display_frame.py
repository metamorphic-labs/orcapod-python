"""The display of a long view reads the rows it shows only, and renders as Polars renders
the whole table (ITL-1464)."""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any
from unittest import mock

import polars as pl
import pyarrow as pa
import pytest

from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams import view as view_module


def _table(rows: int, wide: bool) -> pa.Table:
    """Values whose width differs along the rows, a middle row the widest."""
    columns: dict[str, Any] = {
        "i": [10**9 if i == rows // 2 else i for i in range(rows)],
        "f": [i / 7 if i % 3 else float(i) for i in range(rows)],
        "t": ["x" * (70 if i == rows // 2 else i % 9) for i in range(rows)],
        "n": [None if i % 4 else i for i in range(rows)],
        "b": [i % 2 == 0 for i in range(rows)],
        "d": [dt.datetime(2026, 1, 1) + dt.timedelta(hours=i) for i in range(rows)],
        "l": [[i] * (i % 3) for i in range(rows)],
        "s": [{"a": i} for i in range(rows)],
    }
    if wide:
        columns |= {f"c{k}": list(range(rows)) for k in range(12)}
    return pa.table(columns)


@pytest.mark.parametrize("wide", [False, True])
@pytest.mark.parametrize("rows", [1, 10, 11, 12, 25, 1_000, 1_234])
def test_a_view_renders_as_polars_renders_the_whole(rows: int, wide: bool) -> None:
    """Text and HTML: the shape line, the widths, the ellipsis and every shown cell."""
    stream = ArrowTableStream(_table(rows, wide), tag_columns=[])
    table = stream.as_table()
    limit = 10 if rows > 10 else -1
    with pl.Config(fmt_str_lengths=80, tbl_rows=limit):
        whole = pl.DataFrame(table)
        text, html = str(whole), whole._repr_html_()
    assert str(stream.run()).endswith(text)
    with mock.patch.object(view_module, "_mark_columns", lambda frame, *_: frame):
        assert stream.run()._repr_html_().endswith(html)


def test_a_summarised_column_renders_its_shown_cells() -> None:
    """Long text, bytes, lists and a logical type, at the ends; the middle is not read."""
    rows = 200
    table = pa.table(
        {
            "text": [f"{i:03d}" + "a" * 200 for i in range(rows)],
            "raw": [bytes(i % 9) for i in range(rows)],
            "many": [list(range(20)) for _ in range(rows)],
            "id": pa.ExtensionArray.from_storage(
                pa.uuid(),
                pa.array([uuid.UUID(int=i).bytes for i in range(rows)], pa.binary(16)),
            ),
        }
    )
    view = ArrowTableStream(table, tag_columns=[]).run()
    text = str(view)
    assert "shape: (200, 4)" in text
    for shown in (0, 4, 195, 199):
        assert f"{shown:03d}aaa" in text
    for left_out in (5, 100, 194):
        assert f"{left_out:03d}aaa" not in text
    assert "(200, 4)" in view._repr_html_()


def test_a_long_view_gives_polars_the_rows_it_shows() -> None:
    """One row more than the display: never the table."""
    view = ArrowTableStream(_table(5_000, False), tag_columns=[]).run()
    assert view._display_frame().height == 11
    assert view.head(25)._display_frame().height == 25
