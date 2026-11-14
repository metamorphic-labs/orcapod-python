"""A ``StreamView``'s own methods choose what is shown (ITL-1081).

``run()`` returns a view of every computed row; ``view.head(n)``, ``view.tail(n)`` and
``view.sort_by_tags()`` return a new view of its rows, with a header that says which rows
they are. They never read or compute anything (developer, November 13, 2026).
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.streams import ViewStatus

ROWS = 25


def _source(rows: int = ROWS) -> ArrowTableSource:
    """An ``ArrowTableSource`` of ``id`` and ``x``, labelled ``readings``."""
    table = pa.table(
        {"id": list(range(rows)), "x": [10 * i for i in range(rows)]},
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id"], source_id="r", label="readings")


def _unordered() -> DictSource:
    """Three rows whose tags are not in order."""
    return DictSource(
        [{"id": i, "x": 10 * i} for i in (3, 1, 2)], tag_columns=["id"], label="u"
    )


@function_pod(output_keys="y")
def triple(x: int) -> int:
    """Triple ``x``."""
    return 3 * x


def _ids(view: Any) -> list[int]:
    return view.table.column("id").to_pylist()


def test_head_and_tail_of_a_run_view_name_their_rows() -> None:
    """``head`` and ``tail`` cut the view and say which rows are shown."""
    view = _source().run()
    assert view.header() == "ArrowTableSource[readings] — all 25 rows"
    assert view.head(10).header() == "ArrowTableSource[readings] — first 10 of 25 rows"
    assert _ids(view.head(3)) == [0, 1, 2]
    assert view.tail(10).header() == "ArrowTableSource[readings] — last 10 of 25 rows"
    assert _ids(view.tail(3)) == [22, 23, 24]
    middle = view.head(20).tail(10)
    assert middle.header() == "ArrowTableSource[readings] — rows 11–20 of 25 rows"
    assert _ids(middle) == list(range(10, 20))
    assert view.head(100).header() == "ArrowTableSource[readings] — all 25 rows"


def test_computed_rows_are_named_computed() -> None:
    """A computed stream's view says its rows are computed."""
    view = triple.pod(_source(), label="triple").run()
    assert view.header() == "FunctionPodStream[triple] — all 25 computed rows"
    assert view.head(5).header() == (
        "FunctionPodStream[triple] — first 5 of 25 computed rows"
    )


def test_a_view_head_is_the_stream_head_after_run() -> None:
    """After ``run()``, the view's ``head(n)`` and the stream's ``head(n)`` agree."""
    stream = triple.pod(_source())
    view = stream.run()
    assert view.head(4).table.equals(stream.head(4).table)
    assert view.head(4).header() == stream.head(4).header()


def test_sort_by_tags_of_every_row_is_the_sorted_table() -> None:
    """Sorting a view of every row gives the rows of a sorted ``as_table``."""
    source = _unordered()
    view = source.run().sort_by_tags()
    assert view.header() == "DictSource[u] — all 3 rows, sorted by tags"
    assert view.table.equals(source.as_table(columns={"sort_by_tags": True}))
    assert view.head(2).header() == "DictSource[u] — first 2 of 3 rows, sorted by tags"
    assert _ids(view.head(2)) == [1, 2]
    assert view.sort_by_tags() is view


def test_sorting_a_cut_view_says_the_sort_came_after_the_cut() -> None:
    """The first rows, then sorted, are not the first rows in tag order."""
    view = _unordered().run().head(2).sort_by_tags()
    assert _ids(view) == [1, 3]
    assert view.header() == "DictSource[u] — first 2 of 3 rows, then sorted by tags"
    assert view.tail(1).header() == (
        "DictSource[u] — last 1 of first 2 of 3 rows, then sorted by tags"
    )


def test_sorting_a_view_without_tags_or_rows_returns_it() -> None:
    """A view with no tag column, or no rows, has nothing to sort."""
    untagged = ArrowTableStream(pa.table({"x": [3, 1, 2]})).run()
    assert untagged.sort_by_tags() is untagged
    empty = triple.pod(_source()).head()
    assert empty.status is ViewStatus.NOT_COMPUTED
    assert empty.sort_by_tags() is empty
    assert empty.head(3).header() == empty.header()


def test_a_partial_view_is_cut_within_the_rows_computed_so_far() -> None:
    """A view of a partial computation is cut among the rows it has, and says so."""
    stream = triple.pod(_source())
    rows = iter(stream.iter_data())
    for _ in range(4):
        next(rows)
    partial = stream.head()
    assert partial.status is ViewStatus.PARTIAL
    assert partial.tail(2).header() == (
        f"{partial.kind}[{partial.label}] — last 2 of "
        f"{partial.header().split(' — ', 1)[1]}"
    )
    assert len(partial.tail(2)) == 2


def test_view_methods_check_the_number_of_rows() -> None:
    """``head`` and ``tail`` take a non-negative ``int``."""
    view = _source().run()
    with pytest.raises(TypeError):
        view.head(2.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        view.tail(-1)


def test_a_long_uncut_view_displays_its_first_and_last_rows() -> None:
    """The display of a long ``run()`` view shows its ends; the header gives the count."""
    text = str(_source(1000).run())
    lines = text.split("\n")
    assert lines[0] == "ArrowTableSource[readings] — all 1,000 rows"
    assert "…" in text
    assert "999" in text and "500" not in text
    assert "<td>&hellip;</td>" in _source(1000).run()._repr_html_()


def test_a_cut_or_short_view_displays_every_row() -> None:
    """A view cut by ``head`` or ``tail``, or of at most ten rows, shows every row."""
    view = _source(1000).run()
    assert "…" not in str(view.head(25)).split("\n", 2)[2]
    assert "…" not in str(view.tail(25)).split("\n", 2)[2]
    assert "…" not in str(_source(10).run()).split("\n", 2)[2]


def test_large_cells_of_a_long_view_are_summarised_where_shown() -> None:
    """Only the shown rows are summarised; the table keeps the full values."""
    source = DictSource(
        [{"id": i, "text": f"{i:04d}" + "a" * 200} for i in range(50)],
        tag_columns=["id"],
        label="long",
    )
    view = source.run()
    text = str(view)
    assert "0000aaa" in text and "0049aaa" in text
    assert "a" * 200 not in text
    assert view.table.equals(source.as_table())
