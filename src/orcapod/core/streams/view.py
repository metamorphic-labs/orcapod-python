"""A view of a stream's rows and its notebook preview (ITL-991, ITL-1081).

``head(n)`` on a stream, source or node returns a ``StreamView`` of at most ``n`` rows the
object already holds, has cached from an earlier computation, has stored, or can read from
its table with a bounded read. It never computes: no row is computed, read, polled or
delivered, no user code (a function, a predicate, a side effect) is called and nothing is
written. With no rows to show, the column types come from applying operators to zero-row
tables, which processes no data (ITL-912; see ``StreamBase.head``). The
notebook preview of a stream is its ``head()`` rendered by ``StreamView._repr_html_``.

``run()`` on a stream computes it (the work ``as_table()`` does) and returns a
``StreamView`` of all the computed rows (developer, November 13, 2026, ITL-1081). The
view's own methods (``head``, ``tail``, ``sort_by_tags``) choose what is shown; each
returns a new view and never touches the stream.
"""

from __future__ import annotations

import ast
import dataclasses
import html
import importlib
from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import Enum
from html.parser import HTMLParser
from typing import Any, cast

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
from typing_extensions import override

from orcapod.errors import BoundedViewError
from orcapod.system_constants import constants


class ViewStatus(str, Enum):
    """Where the rows of a ``StreamView`` came from, or why it has none.

    Attributes:
        ROWS: The rows are the first rows of the stream's complete content. Zero rows
            means the content is empty.
        PARTIAL: A computation stopped part way; the rows are those computed so far.
        LOADED_ONLY: A job node filled by a load-only run (``run(load_only=True)``):
            the rows are the stored results of its inputs; inputs without one were
            skipped, so the rows may be fewer than a full run gives (ITL-1095).
        NOT_COMPUTED: Nothing is computed, or what was computed is out of date.
        NOTHING_STORED: A stored view (``node.as_source()``) whose database holds no rows
            for its node yet.
        UNAVAILABLE: No rows can exist here: a pipeline blueprint, an input slot with no
            source bound, or a source that is not available.
    """

    ROWS = "rows"
    PARTIAL = "partial"
    LOADED_ONLY = "loaded_only"
    NOT_COMPUTED = "not_computed"
    NOTHING_STORED = "nothing_stored"
    UNAVAILABLE = "unavailable"


NOT_COMPUTED_NOTE = "not computed; run() computes it"
STALE_NOTE = (
    "not computed: inputs changed since the last computation; run() recomputes it"
)
NOTHING_STORED_NOTE = "nothing stored; run the job"
# A job node shows the rows of its current run only; before one it shows none (ITL-1095).
JOB_NODE_NOT_RUN_NOTE = (
    "not run; run() fills it; node.as_source() shows every stored result"
)
BLUEPRINT_NOTE = "pipeline blueprint; run it in a PipelineJob"
SIDE_EFFECTS_NOTE = (
    "not run; run() delivers its side effects; displaying never delivers side effects"
)

# An operator node whose output came from a load-only run: its input may lack rows that a
# full run computes (ITL-1095).
LOADED_ONLY_OPERATOR_NOTE = (
    "loaded only: the output of a load-only run, whose inputs may lack rows"
)

# A side-effect node whose rows came from a load-only run: it passed them on and delivered
# nothing (ITL-1095).
LOADED_ONLY_SIDE_EFFECT_NOTE = (
    "loaded only: a load-only run passed these rows on and delivered nothing"
)

_DEFAULT_NOTES = {
    ViewStatus.NOT_COMPUTED: NOT_COMPUTED_NOTE,
    ViewStatus.NOTHING_STORED: NOTHING_STORED_NOTE,
    ViewStatus.UNAVAILABLE: "no rows available",
}

SCHEMA_NEEDS_POLL_NOTE = "columns not known until its source is polled"


def default_note(status: ViewStatus) -> str:
    """Return the note shown for a head with ``status`` and no rows of its own."""
    return _DEFAULT_NOTES[status]


# Words naming the rows of a ``ROWS`` head, by where they came from.
_ORIGIN_NOUNS = {
    "held": "rows",
    "read": "rows",
    "computed": "computed rows",
    "stored": "stored rows",
    "delivered": "delivered rows",
}

# Display limits of the preview.
# A view nobody cut to n rows (the view ``run()`` returns, or a sort of it) shows at most
# this many rows: the first and the last half, with Polars' ellipsis row between.
_DISPLAY_ROWS = 10
# A ``run()`` that reads its rows batch by batch keeps every row of a result of at most
# ``RUN_VIEW_COMPLETE_ROWS`` rows; of a larger one, the view has the first and the last
# ``RUN_VIEW_WINDOW_ROWS`` rows (ITL-1464). Counted in rows, so equal data is always
# treated alike, however its batches were cut.
RUN_VIEW_COMPLETE_ROWS = 50_000
RUN_VIEW_WINDOW_ROWS = 1_000
_MAX_TEXT = 80
_TEXT_START = 56
_TEXT_END = 20
_MAX_LIST_ITEMS = 5


@dataclass(frozen=True)
class ColumnRoleMark:
    """How the display marks the header of a column with one role.

    Only the display is marked: never ``StreamView.table``, ``as_table()``,
    ``as_polars_df()`` or a schema.

    Attributes:
        text_prefix: Put before the column's name in the text display.
        html_style: Inline CSS of the column's header cell in the HTML display.
        text_legend: The legend line of the text display naming this mark, or empty.
        html_legend: The legend line of the HTML display naming this mark, or empty.
        group: The group of columns the role is shown in: the HTML display draws its
            rule between two groups, never between two roles of one group.
    """

    text_prefix: str = ""
    html_style: str = ""
    text_legend: str = ""
    html_legend: str = ""
    group: str = ""


# The one place that says how the display marks each column role (developer, November 13,
# 2026, ITL-1081; the data-tag, ITL-1304). Roles: "tag" (a plain tag), "data" (a data
# column of the stream), "data-tag" (a tag that is also the data column of the same name,
# shown once, among the tags) and "other" (a column shown that is neither a tag nor a
# data column of the stream: a source-info, context, content-hash or meta column, left
# unmarked; a system-tag column shown is a tag column of the view, marked as a tag). The renderers read
# only this table. In the HTML display the marks compose (developer, November 21, 2026):
# bold in the accent colour means a tag, an underline means data, and a data-tag has both.
# A data header is set to normal weight, since a header cell is bold by default. The accent
# colour has a contrast of about 4.3:1 on white and on #111; bold and the underline carry
# the meaning without the colour. The legend names the marks shown, in this table's order.
COLUMN_ROLE_MARKS: dict[str, ColumnRoleMark] = {
    "tag": ColumnRoleMark(
        text_prefix="*",
        html_style="font-weight:bold;color:#2a7fb8",
        text_legend="* = tag",
        html_legend="bold = tag",
        group="tags",
    ),
    "data": ColumnRoleMark(
        html_style="font-weight:normal;text-decoration:underline",
        html_legend="underlined = data",
        group="data",
    ),
    "data-tag": ColumnRoleMark(
        text_prefix="+",
        html_style="font-weight:bold;text-decoration:underline;color:#2a7fb8",
        text_legend="+ = data-tag (tag and data)",
        html_legend="bold underlined = data-tag (a tag that is also data)",
        group="tags",
    ),
    "other": ColumnRoleMark(group="data"),
}

# The HTML display draws this rule after the last column of each group of roles, on every
# row, so the table reads "tags | data". It takes the text colour, so it reads on any
# theme.
ROLE_BOUNDARY_STYLE = "border-right:2px solid currentColor"


@dataclass(frozen=True)
class StreamView:
    """Rows of a stream, and how they were obtained.

    ``head(n)`` on a stream returns a view of at most ``n`` readily available rows;
    ``run()`` returns a view of all the computed rows (a ``BoundedStreamView``, which holds
    only the ends, of a large result read in batches). The view's ``head``, ``tail`` and
    ``sort_by_tags`` return a new view of its own rows, with a header that says what they
    are; they never read, compute or change the stream. A view holds the columns it was
    made with: for other column groups (system tags, source info, context, content
    hashes) ask the stream, ``stream.as_table(columns=...)`` or
    ``stream.head(columns=...)``. After ``run()`` they read the rows a stream kept (a
    pod's or operator's output stream, a job node, a job's side-effect node included, an
    in-memory source); a stream that keeps none does its ``as_table()`` work again (a
    side-effect stream outside a job delivers again, a polling source polls again).

    Attributes:
        table: The rows, with the exact Arrow types the stream gives them. When the
            stream has no rows to show, a zero-row table with its columns.
        status: Where the rows came from, or why there are none.
        kind: Class name of the stream, source or node shown.
        label: The name shown for it: its label, or for a source given no label its
            source id (``display_name``). Never used as an identity.
        tag_columns: The columns of ``table`` that are tags.
        data_tag_columns: The columns of ``tag_columns`` that are data-tags: each is also
            a data column of the stream (``data_roles_of``). Only the display marks them.
        data_columns: The other columns of ``table`` that are data columns of the stream
            (``data_roles_of``); any other column (a source-info, context,
            content-hash or meta column) is shown unmarked.
        total_rows: The stream's row count when it is known without computing, else
            ``None``. For ``PARTIAL``, the number of rows computed so far.
        total_is_estimate: ``True`` when ``total_rows`` is a recorded estimate.
        origin: What the rows are: ``"held"`` (in memory), ``"read"`` (a bounded read
            of a table), ``"computed"`` (cached from an earlier computation),
            ``"stored"`` (a job node's database) or ``"delivered"`` (the rows a side-effect
            stream passed on while delivering).
        note: For a view without rows, the explanation shown instead of the default;
            for a view with rows, a remark shown after the row count.
        description: When given, the whole text after the name in the header, written
            by a stream that words its own rows (a table-backed source naming its
            ``where``); otherwise the header is built from the fields above.
        sorted_by_tags: ``True`` when the rows were asked for in tag order (the notebook
            preview, ``sort_by_tags``): the header then says so, and for a bounded read
            or a partial computation says they are the rows read or computed so far, in
            tag order. A ``description`` words its own sorting.
        start: The position of the first row of ``table`` among the rows it was cut
            from (``tail`` of a view moves it), counted from 0.
        pool: When set, the rows were cut from or sorted within a set of rows that is
            not the stream's whole sequence (a few rows read, a view already cut), and
            this is the header text naming that set; ``pool_rows`` is its size.
        pool_rows: The number of rows of ``pool``.
        display_all_rows: ``True`` when the display shows every row (a view cut to
            ``n`` rows by ``head`` or ``tail``); ``False`` for a view nobody cut (the
            view ``run()`` returns, or a sort of it), whose display shows the first and
            the last rows of a long view, as Polars does. The header gives the count.
    """

    table: pa.Table
    status: ViewStatus
    kind: str
    label: str
    tag_columns: tuple[str, ...] = ()
    data_tag_columns: tuple[str, ...] = ()
    data_columns: tuple[str, ...] = ()
    total_rows: int | None = None
    total_is_estimate: bool = False
    origin: str = "held"
    note: str | None = None
    description: str | None = None
    sorted_by_tags: bool = False
    start: int = 0
    pool: str | None = None
    pool_rows: int = 0
    display_all_rows: bool = True

    @classmethod
    def of_rows(
        cls,
        stream: Any,
        table: pa.Table,
        *,
        tag_columns: tuple[str, ...],
        total_rows: int | None,
        status: ViewStatus = ViewStatus.ROWS,
        total_is_estimate: bool = False,
        origin: str = "held",
        note: str | None = None,
        description: str | None = None,
        display_all_rows: bool = True,
    ) -> StreamView:
        """Return a view with rows of ``stream``; its data-tags and data columns are
        ``data_roles_of``."""
        tags = tuple(c for c in tag_columns if c in table.column_names)
        data_tags, data = data_roles_of(stream, tags, table.column_names)
        return cls(
            table=table,
            status=status,
            kind=type(stream).__name__,
            label=display_name(stream),
            tag_columns=tags,
            data_tag_columns=data_tags,
            data_columns=data,
            total_rows=total_rows,
            total_is_estimate=total_is_estimate,
            origin=origin,
            note=note,
            description=description,
            display_all_rows=display_all_rows,
        )

    @classmethod
    def without_rows(
        cls,
        stream: Any,
        status: ViewStatus,
        table: pa.Table,
        *,
        tag_columns: tuple[str, ...],
        note: str | None = None,
    ) -> StreamView:
        """Return a head of ``stream`` with no rows: ``table`` holds its columns only;
        its data-tags and data columns are ``data_roles_of``."""
        tags = tuple(c for c in tag_columns if c in table.column_names)
        data_tags, data = data_roles_of(stream, tags, table.column_names)
        return cls(
            table=table.slice(0, 0),
            status=status,
            kind=type(stream).__name__,
            label=display_name(stream),
            tag_columns=tags,
            data_tag_columns=data_tags,
            data_columns=data,
            note=note,
        )

    def for_stream(
        self, stream: Any, *, origin: str | None = None, note: str | None = None
    ) -> StreamView:
        """Return the same rows shown as ``stream`` (a source over its inner stream, a node
        over its cached output), optionally with another origin or note."""
        return dataclasses.replace(
            self,
            kind=type(stream).__name__,
            label=display_name(stream),
            origin=self.origin if origin is None else origin,
            note=self.note if note is None else note,
        )

    # ------------------------------------------------------------------
    # Data access
    # ------------------------------------------------------------------

    def as_table(self) -> pa.Table:
        """Return the rows as a PyArrow table."""
        return self.table

    def as_polars_df(self) -> pl.DataFrame:
        """Return the rows as a Polars DataFrame."""
        return pl.DataFrame(self.table)

    def __len__(self) -> int:
        """Return the number of rows in ``table``."""
        return self.table.num_rows

    # ------------------------------------------------------------------
    # Choosing what is shown
    # ------------------------------------------------------------------

    def head(self, n: int = 10) -> StreamView:
        """Return a view of the first ``n`` rows of this view.

        Never reads or computes anything: the rows are this view's. The header says
        which rows they are ("first 10 of 1,234 computed rows").

        Args:
            n: The largest number of rows to keep.

        Raises:
            TypeError: If ``n`` is not an ``int``.
            ValueError: If ``n`` is negative.
        """
        _check_row_count(n)
        view = self._cuttable()
        return dataclasses.replace(
            view, table=view.table.slice(0, n), display_all_rows=True
        )

    def tail(self, n: int = 10) -> StreamView:
        """Return a view of the last ``n`` rows of this view.

        Never reads or computes anything: the rows are this view's. The header says
        which rows they are ("last 10 of 1,234 computed rows", "rows 11–20 of 1,234
        computed rows").

        Args:
            n: The largest number of rows to keep.

        Raises:
            TypeError: If ``n`` is not an ``int``.
            ValueError: If ``n`` is negative.
        """
        _check_row_count(n)
        view = self._cuttable()
        dropped = max(view.table.num_rows - n, 0)
        return dataclasses.replace(
            view,
            table=view.table.slice(dropped),
            start=view.start + dropped,
            display_all_rows=True,
        )

    def sort_by_tags(self) -> StreamView:
        """Return a view of this view's rows in tag order.

        The rows are ordered as ``as_table(columns={"sort_by_tags": True})`` orders them
        (``arrow_utils.sort_by_tag_columns``: ascending by the tag columns, nulls last).
        Rows with equal tags are then ordered by the system tag columns the view holds;
        the view ``run()`` returns holds none, so such rows keep their order in the view,
        where ``as_table`` with ``sort_by_tags`` orders them by their system tags. A view
        of all of a stream's rows sorts them all ("all 1,234 computed rows, sorted by
        tags"); a view of some of them sorts those, and its header says so ("first 10 of
        1,234 computed rows, then sorted by tags"). A view with no tag column or no rows
        is returned as it is. Never reads or computes anything.
        """
        if not self.tag_columns or self.table.num_rows == 0:
            return self
        if self._is_window() and self.pool is None and self.sorted_by_tags:
            return self
        # arrow_utils is read when a view is sorted, not imported: it sits above this
        # module in the import graph (types, protocols, side effects), as the array
        # logical type does for the display.
        arrow_utils = importlib.import_module("orcapod.utils.arrow_utils")
        table: pa.Table = arrow_utils.sort_by_tag_columns(self.table, self.tag_columns)
        if self._is_window() and self.pool is None and self._holds_every_row():
            return dataclasses.replace(self, table=table, sorted_by_tags=True)
        view = self._as_pool()
        return dataclasses.replace(
            view, table=table, pool=f"{view.pool}, then sorted by tags"
        )

    def _is_window(self) -> bool:
        """Whether the rows are a run of the stream's own row sequence, so that a cut
        can be named by position: a ``ROWS`` view with no ``description``, unsorted or
        sorted among all of the stream's rows."""
        return (
            self.status in (ViewStatus.ROWS, ViewStatus.LOADED_ONLY)
            and self.description is None
            and (not self.sorted_by_tags or self._total_is_exact())
        )

    def _total_is_exact(self) -> bool:
        """Whether the stream's row count is known exactly."""
        return self.total_rows is not None and not self.total_is_estimate

    def _holds_every_row(self) -> bool:
        """Whether ``table`` holds every row of the stream."""
        return (
            self._total_is_exact()
            and self.start == 0
            and self.table.num_rows == self.total_rows
        )

    def _cuttable(self) -> StreamView:
        """Return this view in a form whose rows can be cut by position: itself when its
        rows are a run of the stream's rows (or of a pool), else a pool of its rows."""
        if self.status not in (
            ViewStatus.ROWS,
            ViewStatus.PARTIAL,
            ViewStatus.LOADED_ONLY,
        ):
            return self
        if self.pool is not None or self._is_window():
            return self
        return self._as_pool()

    def _as_pool(self) -> StreamView:
        """Return this view as a pool: its rows, named by its current header text."""
        return dataclasses.replace(
            self,
            pool=self.description or self._describe(),
            pool_rows=self.table.num_rows,
            start=0,
            description=None,
            note=None,
            sorted_by_tags=False,
        )

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def header(self) -> str:
        """Return the one-line description shown above the rows."""
        return f"{self.kind}[{self.label}] — {self.description or self._describe()}"

    def _describe(self) -> str:
        """Return the header text after the name, for this status."""
        shown = self.table.num_rows
        if self.pool is not None:
            return _window_text(self.pool, shown, self.start, self.pool_rows)
        if self.status in (ViewStatus.ROWS, ViewStatus.LOADED_ONLY):
            text = self._describe_rows(shown)
        elif self.status is ViewStatus.PARTIAL:
            total = self.total_rows if self.total_rows is not None else shown
            suffix = ", sorted by tags" if self.sorted_by_tags else ""
            if shown == total:
                text = f"{total:,} {_rows(total)} computed so far{suffix}"
            else:
                text = f"first {shown:,} of {total:,} rows computed so far{suffix}"
            text += " (computation incomplete)"
        else:
            return self.note or _DEFAULT_NOTES[self.status]
        if self.note:
            text += f"; {self.note}"
        return text

    def _describe_rows(self, shown: int) -> str:
        """Return the header text for a ``ROWS`` head showing ``shown`` rows."""
        total = self.total_rows
        noun = _ORIGIN_NOUNS.get(self.origin, "rows")
        if shown == 0 and (total is None or total == 0):
            return "computed: no rows" if self.origin == "computed" else f"no {noun}"
        suffix = ", sorted by tags" if self.sorted_by_tags else ""
        if self.start and shown:
            # A cut past the first row (``tail``): name the rows by position.
            first, last = self.start + 1, self.start + shown
            if total is None:
                return f"rows {first:,}–{last:,}{suffix}"
            if self.total_is_estimate:
                return f"rows {first:,}–{last:,} of about {total:,}"
            if last == total:
                return f"last {shown:,} of {total:,} {noun}{suffix}"
            return f"rows {first:,}–{last:,} of {total:,} {noun}{suffix}"
        if total is None:
            return f"first {shown:,} {_rows(shown)}{suffix}"
        if self.total_is_estimate:
            if self.sorted_by_tags:
                # A bounded read: the rows read, in tag order, not the first in tag order.
                return (
                    f"{shown:,} {_rows(shown)} read, sorted by tags, of about {total:,}"
                )
            return f"first {shown:,} {_rows(shown)} of about {total:,}"
        if shown == total:
            return f"all {total:,} {noun if total != 1 else noun[:-1]}{suffix}"
        return f"first {shown:,} of {total:,} {noun}{suffix}"

    def _display_columns(self) -> list[tuple[str, str]]:
        """Return the shown columns in display order, each with its role: the tags first,
        in tag order, each a ``"data-tag"`` or a plain ``"tag"``, then the other columns
        in table order, each ``"data"`` or ``"other"``. A data-tag is one column and is
        shown once, among the tags."""
        tags = [c for c in self.tag_columns if c in self.table.column_names]
        return [
            (c, "data-tag" if c in self.data_tag_columns else "tag") for c in tags
        ] + [
            (c, "data" if c in self.data_columns else "other")
            for c in self.table.column_names
            if c not in self.tag_columns
        ]

    def _legend(self, text: bool, roles: Iterable[str]) -> str:
        """Return the legend line of the marks of ``roles``, the roles of the columns a
        display rendered, in the order of ``COLUMN_ROLE_MARKS``, or empty.

        Args:
            text: Whether the legend is the text display's (else the HTML display's).
            roles: The role of each rendered column.
        """
        shown = set(roles)
        legends = [
            mark.text_legend if text else mark.html_legend
            for role, mark in COLUMN_ROLE_MARKS.items()
            if role in shown
        ]
        return "; ".join(legend for legend in legends if legend)

    def _display_frame(self, text: bool = False) -> pl.DataFrame:
        """Return a Polars frame of the rows for display: tag columns first, large cells
        summarised; for the text display, each header carries its role's prefix (a name
        that would then clash with another column's stays unmarked). ``table`` itself is
        never changed."""
        columns_shown = self._display_columns()
        table = self.table.select([name for name, _ in columns_shown])
        rows = table.num_rows
        half = _DISPLAY_ROWS // 2
        # Only the rows the display shows are summarised; the others are left null,
        # because Polars shows the first and last ``half`` of a long frame only.
        shown = (
            table
            if self._displays_every_row()
            else pa.concat_tables([table.slice(0, half), table.slice(rows - half)])
        )
        columns = []
        for name in table.column_names:
            summary = _summarise(shown.column(name))
            if summary is None:
                columns.append(table.column(name))
            elif shown is table:
                columns.append(pa.chunked_array([summary], type=pa.large_string()))
            else:
                columns.append(
                    pa.chunked_array(
                        [
                            pa.array(summary[:half], type=pa.large_string()),
                            pa.nulls(rows - 2 * half, type=pa.large_string()),
                            pa.array(summary[half:], type=pa.large_string()),
                        ]
                    )
                )
        names = list(table.column_names)
        if text:
            marked = [
                COLUMN_ROLE_MARKS[role].text_prefix + n for n, role in columns_shown
            ]
            taken = set(names)
            names = [
                m if m == n or m not in taken else n
                for m, n in zip(marked, names, strict=True)
            ]
            if len(set(names)) != len(names):
                names = list(table.column_names)
        return pl.DataFrame(pa.table(columns, names=names))

    def _with_row_count(self, rendered: str) -> str:
        """Return Polars' rendering of the display frame, which names its row count."""
        return rendered

    def _displays_every_row(self) -> bool:
        """Whether the display shows every row: a view cut to ``n`` rows, or a short one."""
        return self.display_all_rows or self.table.num_rows <= _DISPLAY_ROWS

    def _display_rows(self) -> int:
        """Return Polars' ``tbl_rows`` for this view: all rows, or ``_DISPLAY_ROWS``."""
        return -1 if self._displays_every_row() else _DISPLAY_ROWS

    def _repr_html_(self) -> str:
        """Return the notebook preview: header, legend and the rows (large cells
        summarised), each rendered column's header styled by its role
        (``COLUMN_ROLE_MARKS``). The legend names the marks of the columns Polars
        rendered (``_rendered_columns``): none when the header cannot be matched, since
        no cell is then marked, and none when Polars renders no column."""
        parts = [
            f"<div><strong>{html.escape(self.header(), quote=False)}</strong></div>"
        ]
        table_html = ""
        roles: list[str] = []
        if self.table.num_columns:
            # Polars shortens strings to its own default length; show up to _MAX_TEXT.
            with pl.Config(fmt_str_lengths=_MAX_TEXT, tbl_rows=self._display_rows()):
                frame_html = self._with_row_count(self._display_frame()._repr_html_())
            columns = self._display_columns()
            names = _HeaderNames()
            names.feed(frame_html)
            names.close()
            rendered = _rendered_columns(names.cells, [name for name, _ in columns])
            table_html = _mark_columns(frame_html, columns, rendered)
            if rendered is not None:
                roles = [columns[i][1] for i in rendered if i is not None]
        legend = self._legend(text=False, roles=roles)
        if legend:
            parts.append(
                f"<div><small>{html.escape(legend, quote=False)}</small></div>"
            )
        if table_html:
            parts.append(table_html)
        return "\n".join(parts)

    def __str__(self) -> str:
        """Return the preview as text: header, legend and the rows, each column's
        header prefixed by its role's mark (``*`` for a plain tag, ``+`` for a
        data-tag). The legend names the marks of the columns Polars rendered
        (``_rendered_columns``); when its header cannot be read, of every column."""
        lines = [self.header()]
        frame_text = ""
        roles: list[str] = []
        if self.table.num_columns:
            with pl.Config(fmt_str_lengths=_MAX_TEXT, tbl_rows=self._display_rows()):
                frame = self._display_frame(text=True)
                frame_text = self._with_row_count(str(frame))
            columns = self._display_columns()
            rendered = _rendered_columns(_text_header_cells(frame_text), frame.columns)
            if rendered is not None:
                roles = [columns[i][1] for i in rendered if i is not None]
            else:
                # A header that cannot be read (a name Polars wraps over two lines,
                # another table style): the marks are part of the names, so every
                # column's role is named.
                roles = [role for _, role in columns]
        legend = self._legend(text=True, roles=roles)
        if legend:
            lines.append(legend)
        if frame_text:
            lines.append(frame_text)
        return "\n".join(lines)

    def __repr__(self) -> str:
        """Return a short description: kind, label, status and row count."""
        return (
            f"StreamView({self.kind}[{self.label}], status={self.status.value}, "
            f"rows={self.table.num_rows})"
        )


@dataclass(frozen=True, repr=False)
class _EndsView(StreamView):
    """The first and last rows of a longer view, displayed as that view is.

    ``table`` holds one row more than a long view shows, so Polars renders its ellipsis.

    Attributes:
        shown_total: The row count of the whole view, named by the display.
    """

    shown_total: int = 0

    @override
    def _with_row_count(self, rendered: str) -> str:
        # Polars names the frame's shape before anything else it renders.
        return rendered.replace(
            f"shape: ({self.table.num_rows:_}, ", f"shape: ({self.shown_total:_}, ", 1
        )


class BoundedStreamView(StreamView):
    """The view of a ``run()`` whose rows are not held (ITL-1464).

    It has the first and the last ``window_rows`` rows, the row count and the columns.
    It is displayed as a view holding every row is. ``head(n)`` and ``tail(n)`` within
    the window give the same views such a view gives. What needs rows it does not hold
    (``table``, ``as_table()``, ``as_polars_df()``, ``sort_by_tags()``, a longer
    ``head`` or ``tail``) raises ``BoundedViewError``, which names the stream's own
    method: a part is never given as the whole, and a view never reads or computes.

    Attributes:
        window_rows: The rows held at each end.
    """

    def __init__(  # pyright: ignore[reportMissingSuperCall]  # the parent's would set ``table``
        self, first: StreamView, last: pa.Table, total_rows: int
    ) -> None:
        """Make the view.

        Args:
            first: A view of the first rows, named as the stream's view is named.
            last: The last rows, as many as ``first`` has.
            total_rows: The row count of the whole.
        """
        # ``table`` is deliberately not set: reading it reaches ``__getattr__``.
        for field in dataclasses.fields(StreamView):
            if field.name != "table":
                object.__setattr__(self, field.name, getattr(first, field.name))
        object.__setattr__(self, "total_rows", total_rows)
        object.__setattr__(
            self, "_first", dataclasses.replace(first, total_rows=total_rows)
        )
        object.__setattr__(self, "_last", last)

    def __getattr__(self, name: str) -> Any:
        if name == "table":
            raise self._refused("its table")
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")

    def _refused(self, what: str) -> BoundedViewError:
        """Return the error for ``what``, which needs rows the view does not hold."""
        return BoundedViewError(
            f"{what} needs every row, and this view of {self._total():,} rows holds only "
            f"the first and the last {self.window_rows:,}: run() does not keep a result "
            f"of more than {RUN_VIEW_COMPLETE_ROWS:,} rows read in batches. Ask the "
            "stream for its rows: stream.as_table(), stream.as_polars_df() or iteration "
            "(stream.as_table(columns={'sort_by_tags': True}) sorts them)."
        )

    def _total(self) -> int:
        return cast(int, self.total_rows)

    def _held(self) -> tuple[StreamView, pa.Table]:
        values = vars(self)
        return cast("StreamView", values["_first"]), cast(pa.Table, values["_last"])

    @property
    def window_rows(self) -> int:
        """The rows held at each end."""
        return self._held()[1].num_rows

    @override
    def __len__(self) -> int:
        return self._total()

    @override
    def __eq__(self, other: object) -> bool:
        return self is other

    @override
    def __hash__(self) -> int:
        return id(self)

    @override
    def as_table(self) -> pa.Table:
        raise self._refused("as_table()")

    @override
    def as_polars_df(self) -> pl.DataFrame:
        raise self._refused("as_polars_df()")

    @override
    def sort_by_tags(self) -> StreamView:
        # A view with no tag column is returned as it is, as every view is.
        if not self.tag_columns:
            return self
        raise self._refused("sort_by_tags()")

    @override
    def head(self, n: int = 10) -> StreamView:
        _check_row_count(n)
        if n > self.window_rows:
            raise self._refused(f"head({n})")
        return dataclasses.replace(self._held()[0].head(n), display_all_rows=True)

    @override
    def tail(self, n: int = 10) -> StreamView:
        _check_row_count(n)
        if n > self.window_rows:
            raise self._refused(f"tail({n})")
        first, last = self._held()
        whole = dataclasses.replace(
            first, table=last, start=self._total() - last.num_rows
        )
        return whole.tail(n)

    @override
    def for_stream(
        self, stream: Any, *, origin: str | None = None, note: str | None = None
    ) -> StreamView:
        first, last = self._held()
        return BoundedStreamView(
            first.for_stream(stream, origin=origin, note=note), last, self._total()
        )

    def _ends(self) -> _EndsView:
        """The rows the display shows, worded and counted as the whole."""
        first, last = self._held()
        half = _DISPLAY_ROWS // 2
        values = {
            f.name: getattr(first, f.name) for f in dataclasses.fields(StreamView)
        }
        values["table"] = pa.concat_tables(
            [first.table.slice(0, half + 1), last.slice(last.num_rows - half)]
        )
        values["description"] = first._describe_rows(self._total())
        values["display_all_rows"] = False
        return _EndsView(**values, shown_total=self._total())

    @override
    def header(self) -> str:
        return self._ends().header()

    @override
    def _repr_html_(self) -> str:
        return self._ends()._repr_html_()

    @override
    def __str__(self) -> str:
        return str(self._ends())

    @override
    def __repr__(self) -> str:
        return (
            f"StreamView({self.kind}[{self.label}], status={self.status.value}, "
            f"rows={self._total()})"
        )


class WindowOfRows:
    """Collects the tables of a stream's rows, batch by batch: all rows of a result of
    at most ``RUN_VIEW_COMPLETE_ROWS`` rows, else the first and the last
    ``RUN_VIEW_WINDOW_ROWS``. The window is copied, so no batch stays alive behind it.
    """

    def __init__(self) -> None:
        self.total: int = 0
        self._all: list[pa.Table] | None = []
        self._first: list[pa.Table] = []
        self._last: deque[pa.Table] = deque()

    def add(self, table: pa.Table) -> None:
        """Take the next rows.

        Args:
            table: The rows; every table of a stream has the same schema.
        """
        missing = RUN_VIEW_WINDOW_ROWS - min(self.total, RUN_VIEW_WINDOW_ROWS)
        if missing and table.num_rows:
            self._first.append(_copied(table.slice(0, missing)))
        if table.num_rows:
            end = max(table.num_rows - RUN_VIEW_WINDOW_ROWS, 0)
            self._last.append(_copied(table.slice(end)))
            held = sum(part.num_rows for part in self._last)
            while held - self._last[0].num_rows >= RUN_VIEW_WINDOW_ROWS:
                held -= self._last.popleft().num_rows
        self.total += table.num_rows
        if self._all is not None:
            self._all.append(table)
            if self.total > RUN_VIEW_COMPLETE_ROWS:
                self._all = None

    @property
    def complete(self) -> pa.Table | None:
        """Every row, or ``None`` when there are too many (or nothing was taken)."""
        return pa.concat_tables(self._all) if self._all else None

    @property
    def first(self) -> pa.Table:
        """The first rows."""
        return pa.concat_tables(self._first)

    @property
    def last(self) -> pa.Table:
        """The last rows."""
        rows = pa.concat_tables(list(self._last))
        return rows.slice(max(rows.num_rows - RUN_VIEW_WINDOW_ROWS, 0))


def _copied(table: pa.Table) -> pa.Table:
    """Return ``table``'s rows in buffers of their own, every value bit for bit."""
    return table.take(pa.array(range(table.num_rows), pa.int64()))


def _mark_columns(
    frame_html: str,
    columns: Sequence[tuple[str, str]],
    rendered: Sequence[int | None] | None,
) -> str:
    """Return Polars' HTML table ``frame_html`` with its columns styled by role.

    Polars may leave columns out of a wide table, showing the first and the last ones
    around an ellipsis cell, so each rendered header cell is styled by the column
    ``rendered`` matched it to by name (``_rendered_columns``), never by its position
    among all the columns. The header cell of a column gets its role's ``html_style``;
    every cell of the last column of a group of roles followed by another group gets
    ``ROLE_BOUNDARY_STYLE`` (the ellipsis cell's when the boundary is among the columns
    left out). The ellipsis cell gets no mark, and a header that could not be matched
    leaves every cell unmarked: no mark is better than a wrong one. Everything else is
    passed on unchanged.

    Args:
        frame_html: The HTML of the display frame, one column per entry of ``columns``.
        columns: The name and the role of each column, in display order.
        rendered: For each rendered header cell, the index of its column in
            ``columns``, or ``None`` for the ellipsis cell; ``None`` when the cells
            could not be matched (``_rendered_columns``).
    """
    cells = [] if rendered is None else list(rendered)
    roles = [role for _, role in columns]
    header = [
        "" if column is None else COLUMN_ROLE_MARKS[roles[column]].html_style
        for column in cells
    ]
    boundary = [""] * len(cells)
    groups = [COLUMN_ROLE_MARKS[role].group for role in roles]
    if cells and any(column is not None for column in cells):
        for last in range(len(groups) - 1):
            if groups[last] != groups[last + 1]:
                # The cell showing the last column of the group, or the ellipsis cell
                # when that column is left out.
                shown = [i for i, column in enumerate(cells) if column == last]
                if not shown:
                    shown = [i for i, column in enumerate(cells) if column is None]
                for i in shown:
                    boundary[i] = ROLE_BOUNDARY_STYLE
    marker = _ColumnMarker(header, boundary)
    marker.feed(frame_html)
    marker.close()
    return "".join(marker.out)


_ELLIPSIS = "…"


def _rendered_columns(
    cells: Sequence[str], names: Sequence[str]
) -> list[int | None] | None:
    """Return, for each rendered header cell, the index of its column in ``names``, or
    ``None`` for the ellipsis cell; ``None`` instead of the list when the cells cannot be
    matched to the columns.

    Polars renders either every column, or a first run and a last run of them around one
    ellipsis cell; the match is checked by name on both sides of the ellipsis. The one
    reading of which columns a display rendered: the HTML display's header marks and
    legend, and the text display's legend, all use it.

    Args:
        cells: The text of each cell of the first header row, as rendered.
        names: The names of the columns of the display frame, in order.
    """
    if list(cells) == list(names):
        return list(range(len(names)))
    left_out = len(names) - (len(cells) - 1)
    if left_out > 0:
        for i, cell in enumerate(cells):
            if (
                cell == _ELLIPSIS
                and list(cells[:i]) == list(names[:i])
                and list(cells[i + 1 :]) == list(names[i + left_out :])
            ):
                return [*range(i), None, *range(i + left_out, len(names))]
    return None


def _text_header_cells(frame_text: str) -> list[str]:
    """Return the text of each cell of the first header line of Polars' text table, or
    no cell when the table has no such line."""
    for line in frame_text.splitlines():
        if line.startswith("│"):
            return [cell.strip() for cell in line.strip().strip("│").split("┆")]
    return []


class _HeaderNames(HTMLParser):
    """Collects the text of each cell of the first header row of an HTML table."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.cells: list[str] = []
        self._rows: int = 0
        self._in_thead: bool = False
        self._cell: list[str] | None = None

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "thead":
            self._in_thead = True
        elif tag == "tr" and self._in_thead:
            self._rows += 1
        elif tag in ("th", "td") and self._in_thead and self._rows == 1:
            self._cell = []

    @override
    def handle_endtag(self, tag: str) -> None:
        if tag == "thead":
            self._in_thead = False
        elif tag in ("th", "td") and self._cell is not None:
            self.cells.append("".join(self._cell))
            self._cell = None

    @override
    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)


class _ColumnMarker(HTMLParser):
    """Copies Polars' HTML table, adding inline styles to cells by column position."""

    def __init__(self, header: list[str], boundary: list[str]) -> None:
        super().__init__(convert_charrefs=False)
        self.header: list[str] = header
        self.boundary: list[str] = boundary
        self.out: list[str] = []
        self.in_thead: bool = False
        self.rows_in_thead: int = 0
        self.column: int = 0

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        raw = self.get_starttag_text() or f"<{tag}>"
        if tag == "thead":
            self.in_thead = True
        elif tag == "tr":
            self.column = 0
            if self.in_thead:
                self.rows_in_thead += 1
        elif tag in ("th", "td") and not attrs:
            index = self.column
            self.column += 1
            styles: list[str] = []
            if index < len(self.header):
                if self.in_thead and self.rows_in_thead == 1:
                    styles.append(self.header[index])
                styles.append(self.boundary[index])
            style = ";".join(part for part in styles if part)
            if style:
                raw = f'<{tag} style="{html.escape(style)}">'
        elif tag in ("th", "td"):
            self.column += 1
        self.out.append(raw)

    @override
    def handle_endtag(self, tag: str) -> None:
        if tag == "thead":
            self.in_thead = False
        self.out.append(f"</{tag}>")

    @override
    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.out.append(self.get_starttag_text() or f"<{tag}/>")

    @override
    def handle_data(self, data: str) -> None:
        self.out.append(data)

    @override
    def handle_entityref(self, name: str) -> None:
        self.out.append(f"&{name};")

    @override
    def handle_charref(self, name: str) -> None:
        self.out.append(f"&#{name};")

    @override
    def handle_comment(self, data: str) -> None:
        self.out.append(f"<!--{data}-->")

    @override
    def handle_decl(self, decl: str) -> None:
        self.out.append(f"<!{decl}>")


def _check_row_count(n: int) -> None:
    """Check a number of rows given to a view's ``head`` or ``tail``.

    Raises:
        TypeError: If ``n`` is not an ``int``.
        ValueError: If ``n`` is negative.
    """
    if isinstance(n, bool) or not isinstance(n, int):
        raise TypeError(f"the number of rows must be an int, got {type(n).__name__}")
    if n < 0:
        raise ValueError(f"the number of rows must not be negative, got {n}")


def _window_text(pool: str, shown: int, start: int, pool_rows: int) -> str:
    """Return the header text for ``shown`` rows from ``start`` of a pool of
    ``pool_rows`` rows named ``pool``."""
    if shown == pool_rows:
        return pool
    if start == 0:
        return f"first {shown:,} of {pool}"
    if start + shown == pool_rows:
        return f"last {shown:,} of {pool}"
    return f"rows {start + 1:,}–{start + shown:,} of {pool}"


_MAX_NAME = 48


def display_name(stream: Any) -> str:
    """Return the name the header shows for ``stream``: never its label changed.

    A stream that says (``_display_name``) gives its own, e.g. a source with no label
    of its own is shown by its source id; otherwise its label. A long name keeps its
    start and end around an ellipsis.
    """
    name_of = getattr(stream, "_display_name", None)
    name = str(name_of() if callable(name_of) else stream.label)
    if len(name) > _MAX_NAME:
        name = f"{name[:30]}…{name[-12:]}"
    return name


def data_roles_of(
    stream: Any, tag_columns: Sequence[str], column_names: Sequence[str]
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return which shown columns of ``stream`` are data-tags and which are data columns.

    The roles are read from the stream's public ``keys()``: a data-tag is one column in
    both roles, a name among both its tag and its data keys; a data column is a name
    among its data keys that is not a tag and not a meta column (``META_PREFIX``): a raw
    ``ArrowTableStream`` lists its meta columns among its data keys, and a meta column
    is shown unmarked. A view that shows columns has its stream's
    schema already, so reading the keys computes, reads and polls nothing; a view with no
    column (one whose schema would need a poll, ``schema_requires_io``) does not read
    them.

    Args:
        stream: The stream, source or node whose view is built.
        tag_columns: The tag columns the view shows, in display order.
        column_names: Every column the view shows, in table order.

    Returns:
        The data-tags, in the order of ``tag_columns``, and the data columns, in the
        order of ``column_names``.
    """
    if not column_names:
        return (), ()
    tags, data = stream.keys()
    data_names = set(data)
    data_tags = tuple(c for c in tag_columns if c in data_names and c in set(tags))
    data_columns = tuple(
        c
        for c in column_names
        if c in data_names
        and c not in tag_columns
        and not c.startswith(constants.META_PREFIX)
    )
    return data_tags, data_columns


def preview_html(stream: Any) -> str:
    """Return the notebook preview of ``stream``: its head with the rows in tag order.

    The preview asks ``head`` for ``sort_by_tags`` (developer, November 12, 2026), so the
    rows shown do not depend on the order they were computed in. For a bounded read or a
    partial computation they are the rows read or computed so far, in tag order. A stream
    with no tag column shows its rows in stream order. It never computes anything more
    than ``head`` does.
    """
    try:
        head = stream.head(columns={"sort_by_tags": True})
    except Exception as error:  # noqa: BLE001 - a display hook must not raise
        return render_error_html(stream, error)
    if head.tag_columns and head.table.num_rows:
        head = dataclasses.replace(head, sorted_by_tags=True)
    return head._repr_html_()


def head_of(
    head: Any, stream: Any, *, origin: str | None = None, note: str | None = None
) -> StreamView:
    """Return ``head`` (another stream's) shown as ``stream``.

    A ``StreamView`` keeps every field. Any other result of ``head`` (a
    ``StreamViewProtocol``) is adapted from what that protocol gives (its rows, status
    and total), with the columns' roles read from ``stream``'s ``keys()``, as every other
    view gets them (``data_roles_of``).

    Args:
        head: What another stream's ``head`` returned.
        stream: The stream, source or node that shows those rows as its own.
        origin: Another origin for the rows, if given.
        note: Another note, if given.
    """
    if not isinstance(head, StreamView):
        table: pa.Table = head.table
        names = table.column_names
        # A table with no column needs no keys (``data_roles_of``).
        tags = tuple(c for c in stream.keys()[0] if c in names) if names else ()
        data_tags, data = data_roles_of(stream, tags, names)
        head = StreamView(
            table=table,
            status=ViewStatus(head.status),
            kind=type(stream).__name__,
            label=display_name(stream),
            tag_columns=tags,
            data_tag_columns=data_tags,
            data_columns=data,
            total_rows=head.total_rows,
        )
    return head.for_stream(stream, origin=origin, note=note)


def render_error_html(stream: Any, error: BaseException) -> str:
    """Return the preview of a stream whose ``head`` raised: its name and the error."""
    title = f"{type(stream).__name__}[{display_name(stream)}] — could not read: {error}"
    return f"<div><strong>{html.escape(title, quote=False)}</strong></div>"


def _rows(count: int) -> str:
    """Return ``row`` or ``rows`` for ``count``."""
    return "row" if count == 1 else "rows"


def _size(count: int) -> str:
    """Return a byte count as text: ``300 B``, ``1.2 KB``, ``3.4 MB``."""
    value = float(count)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1000 or unit == "GB":
            return f"{count} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1000
    return f"{count} B"  # pragma: no cover


def _summarise(column: pa.ChunkedArray[Any]) -> list[str | None] | None:
    """Return display strings for a column whose cells are too large to show as they are,
    or ``None`` to show the column as it is."""
    arrow_type = column.type
    if isinstance(arrow_type, pa.ExtensionType):
        return _summarise_extension(column, arrow_type)
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return _byte_summaries([column], "bytes")
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        # Lengths come from Arrow; only the kept parts become Python strings. A long
        # value keeps its start and its end (hash keys differ at the end), with an
        # ellipsis between.
        lengths: list[int | None] = pc.utf8_length(column).to_pylist()
        if all(n is None or n <= _MAX_TEXT for n in lengths):
            return None
        shorts: list[str | None] = pc.utf8_slice_codeunits(
            column, 0, _MAX_TEXT
        ).to_pylist()
        starts: list[str | None] = pc.utf8_slice_codeunits(
            column, 0, _TEXT_START
        ).to_pylist()
        ends = [
            None if n is None or n <= _MAX_TEXT else _text_end(column[i])
            for i, n in enumerate(lengths)
        ]
        return [
            None if n is None else f"{a}…{b}" if n > _MAX_TEXT else short
            for n, short, a, b in zip(lengths, shorts, starts, ends)
        ]
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return _summarise_list(column, arrow_type.value_type)
    return None


def _summarise_extension(
    column: pa.ChunkedArray[Any], arrow_type: pa.ExtensionType
) -> list[str | None] | None:
    """Return display strings for a logical-type column, or ``None`` to let Polars show it.

    An array column is recognised by its logical type's extension name, never by the
    bytes of a cell: it shows as ``ndarray float64 (2, 3)``, read from its stored form
    without decoding the data. Another logical type over binary storage shows its size,
    over a struct its name; any other storage is shown by Polars.
    """
    # The array logical type's module is read when a column is shown, not imported:
    # it sits above this module in the import graph (types, protocols, side effects),
    # and it loads numpy, which only an array column needs.
    numpy_type = importlib.import_module("orcapod.logical_types.numpy_type")
    array_extension_name: str = numpy_type.NDARRAY_EXTENSION_NAME

    storage_type = arrow_type.storage_type
    short_name = arrow_type.extension_name.rsplit(".", 1)[-1]
    storages = [
        cast("pa.ExtensionArray[Any]", array).storage for array in column.chunks
    ]
    is_binary = pa.types.is_binary(storage_type) or pa.types.is_large_binary(
        storage_type
    )
    if arrow_type.extension_name == array_extension_name:
        if pa.types.is_struct(storage_type):
            return [
                None if v is None else f"ndarray {v['dtype']} {_shape_text(v['shape'])}"
                for storage in storages
                for v in _struct_fields(
                    cast(pa.StructArray, storage), ["dtype", "shape"]
                )
            ]
        if is_binary:
            return [
                _npy_summary(storage[i])
                for storage in storages
                for i in range(len(storage))
            ]
    if is_binary:
        return _byte_summaries(storages, short_name)
    if pa.types.is_struct(storage_type):
        return [
            f"<{short_name}>" if valid else None
            for storage in storages
            for valid in storage.is_valid().to_pylist()
        ]
    return None


def _npy_summary(cell: Any) -> str | None:
    """Return ``ndarray <dtype> <shape>`` for one ``.npy`` cell, reading its header only.

    The ``.npy`` header is a magic string, a version, the header length and a Python
    dict literal with ``descr``, ``fortran_order`` and ``shape``. The buffer is sliced
    without copying, and the array data is never read.
    """
    if not cell.is_valid:
        return None
    buffer = cell.as_buffer()
    size = f"ndarray ({_size(buffer.size)})"
    try:
        prefix = buffer[:12].to_pybytes()
        if not prefix.startswith(b"\x93NUMPY"):
            return size
        if prefix[6] == 1:
            start, length = 10, int.from_bytes(prefix[8:10], "little")
        else:
            start, length = 12, int.from_bytes(prefix[8:12], "little")
        text = buffer[start : start + length].to_pybytes().decode("utf-8")
        header = ast.literal_eval(text)
        import numpy as np

        dtype = np.dtype(header["descr"])
        return f"ndarray {dtype.name} {_shape_text(header['shape'])}"
    except (ValueError, SyntaxError, KeyError, TypeError, IndexError):
        return size


def _byte_summaries(arrays: Sequence[Any], name: str) -> list[str | None]:
    """Return ``name (size)`` for each binary cell of ``arrays``, in order.

    Sizes come from Arrow (``binary_length``); the bytes are never read.
    """
    return [
        None if n is None else f"{name} ({_size(n)})"
        for array in arrays
        for n in pc.binary_length(array).to_pylist()
    ]


def _struct_fields(
    array: pa.StructArray, names: list[str]
) -> list[dict[str, Any] | None]:
    """Return each row of ``array`` as a dict of the named fields (``None`` for a null row)."""
    fields = {name: array.field(name).to_pylist() for name in names}
    valid = array.is_valid().to_pylist()
    return [
        {name: fields[name][i] for name in names} if valid[i] else None
        for i in range(len(array))
    ]


def _shape_text(shape: Any) -> str:
    """Return an array shape as Python writes a tuple: ``(40,)``, ``(2, 3)``."""
    dims = list(shape or [])
    if len(dims) == 1:
        return f"({dims[0]},)"
    return "(" + ", ".join(str(d) for d in dims) + ")"


def _text_end(cell: Any) -> str:
    """Return the last ``_TEXT_END`` characters of a string cell, reading only its end.

    A character is at most 4 bytes in UTF-8, so the last ``4 * _TEXT_END`` bytes hold
    them; a character cut at the start of that window is dropped when decoding.
    """
    buffer = cell.as_buffer()
    window = buffer[max(0, buffer.size - 4 * _TEXT_END) :].to_pybytes()
    return window.decode("utf-8", errors="ignore")[-_TEXT_END:]


def _summarise_list(
    column: pa.ChunkedArray[Any], value_type: pa.DataType
) -> list[str | None] | None:
    """Return display strings for a list column: short lists of scalars as written, others by length."""
    simple_items = not (
        isinstance(value_type, pa.ExtensionType)
        or pa.types.is_nested(value_type)
        or pa.types.is_binary(value_type)
        or pa.types.is_large_binary(value_type)
    )
    lengths: list[int | None] = pc.list_value_length(column).to_pylist()
    if simple_items and all(n is None or n <= _MAX_LIST_ITEMS for n in lengths):
        return None
    item = "list" if pa.types.is_nested(value_type) else None
    out: list[str | None] = []
    for index, length in enumerate(lengths):
        if length is None:
            out.append(None)
        elif simple_items and length <= _MAX_LIST_ITEMS:
            # Only a short list is converted to Python, to show its items.
            out.append(repr(column[index].as_py()))
        else:
            out.append(f"list (len {length})" + (f" of {item}" if item else ""))
    return out
