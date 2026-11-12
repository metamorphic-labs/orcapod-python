"""The first rows of a stream that are readily available, and their notebook preview (ITL-991).

``head(n)`` on a stream, source or node returns a ``StreamHead``: at most ``n`` rows the
object already holds, has cached from an earlier computation, has stored, or can read from
its table with a bounded read. It never computes: no user function is called, no operator
is run, no side effect is delivered, no source is polled and nothing is written. The
notebook preview of a stream is its ``head()`` rendered by ``StreamHead._repr_html_``.
"""

from __future__ import annotations

import dataclasses
import html
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from orcapod.types import ColumnConfig
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pl = LazyModule("polars")


class HeadStatus(str, Enum):
    """Where the rows of a ``StreamHead`` came from, or why it has none.

    Attributes:
        ROWS: The rows are the first rows of the stream's complete content. Zero rows
            means the content is empty.
        PARTIAL: A computation stopped part way; the rows are those computed so far.
        NOT_COMPUTED: Nothing is computed, or what was computed is out of date.
        NOTHING_STORED: A job node whose store holds no rows for it yet.
        UNAVAILABLE: No rows can exist here: a pipeline blueprint, an input slot with no
            source bound, or a source that is not available.
    """

    ROWS = "rows"
    PARTIAL = "partial"
    NOT_COMPUTED = "not_computed"
    NOTHING_STORED = "nothing_stored"
    UNAVAILABLE = "unavailable"


NOT_COMPUTED_NOTE = "not computed; as_table(), iter_data() or a job run computes it"
STALE_NOTE = "not computed: inputs changed since the last computation"
NOTHING_STORED_NOTE = "nothing stored; run the job"
SIDE_EFFECTS_NOTE = (
    "not run; iterating it or a job run delivers its side effects; "
    "displaying never delivers side effects"
)

_DEFAULT_NOTES = {
    HeadStatus.NOT_COMPUTED: NOT_COMPUTED_NOTE,
    HeadStatus.NOTHING_STORED: NOTHING_STORED_NOTE,
    HeadStatus.UNAVAILABLE: "no rows available",
}

# Words naming the rows of a ``ROWS`` head, by where they came from.
_ORIGIN_NOUNS = {
    "held": "rows",
    "read": "rows",
    "computed": "computed rows",
    "stored": "stored rows",
}

# Display limits of the preview.
_MAX_TEXT = 80
_MAX_LIST_ITEMS = 5


def check_head_size(n: int) -> None:
    """Check the row count given to ``head``.

    Raises:
        TypeError: If ``n`` is not an ``int``.
        ValueError: If ``n`` is negative.
    """
    if isinstance(n, bool) or not isinstance(n, int):
        raise TypeError(f"head() takes an int number of rows, got {type(n).__name__}")
    if n < 0:
        raise ValueError(f"head() takes a non-negative number of rows n, got {n}")


def head_column_config(
    columns: ColumnConfig | dict[str, Any] | None, all_info: bool
) -> ColumnConfig:
    """Return the column configuration ``head`` uses for ``columns`` and ``all_info``.

    The column groups mean what they mean for ``as_table``. ``head`` returns rows in
    stream order, so ``all_info`` does not sort, and asking for ``sort_by_tags`` raises.

    Raises:
        ValueError: If ``columns`` asks for ``sort_by_tags``.
    """
    if isinstance(columns, ColumnConfig):
        asked_to_sort = bool(columns.sort_by_tags)
    elif isinstance(columns, Mapping):
        asked_to_sort = bool(columns.get("sort_by_tags", False))
    else:
        asked_to_sort = False
    if asked_to_sort:
        raise ValueError(
            "head() returns the first rows in stream order and cannot sort them by "
            "tags; sort the result of as_table() instead"
        )
    config = ColumnConfig.handle_config(columns, all_info=all_info)
    return dataclasses.replace(config, sort_by_tags=False)


@dataclass(frozen=True)
class StreamHead:
    """At most ``n`` readily available rows of a stream, and how they were obtained.

    Attributes:
        table: The rows, with the exact Arrow types the stream gives them. When the
            stream has no rows to show, a zero-row table with its columns.
        status: Where the rows came from, or why there are none.
        kind: Class name of the stream, source or node shown.
        label: Its label.
        tag_columns: The columns of ``table`` that are tags.
        total_rows: The stream's row count when it is known without computing, else
            ``None``. For ``PARTIAL``, the number of rows computed so far.
        total_is_estimate: ``True`` when ``total_rows`` is a recorded estimate.
        origin: What the rows are: ``"held"`` (in memory), ``"read"`` (a bounded read
            of a table), ``"computed"`` (cached from an earlier computation) or
            ``"stored"`` (a job node's store).
        note: For a head without rows, the explanation shown instead of the default;
            for a head with rows, a remark shown after the row count.
    """

    table: pa.Table
    status: HeadStatus
    kind: str
    label: str
    tag_columns: tuple[str, ...] = ()
    total_rows: int | None = None
    total_is_estimate: bool = False
    origin: str = "held"
    note: str | None = None

    @classmethod
    def of_rows(
        cls,
        stream: Any,
        table: pa.Table,
        *,
        tag_columns: tuple[str, ...],
        total_rows: int | None,
        status: HeadStatus = HeadStatus.ROWS,
        total_is_estimate: bool = False,
        origin: str = "held",
        note: str | None = None,
    ) -> StreamHead:
        """Return a head with rows of ``stream``."""
        return cls(
            table=table,
            status=status,
            kind=type(stream).__name__,
            label=str(stream.label),
            tag_columns=tuple(c for c in tag_columns if c in table.column_names),
            total_rows=total_rows,
            total_is_estimate=total_is_estimate,
            origin=origin,
            note=note,
        )

    @classmethod
    def without_rows(
        cls,
        stream: Any,
        status: HeadStatus,
        table: pa.Table,
        *,
        tag_columns: tuple[str, ...],
        note: str | None = None,
    ) -> StreamHead:
        """Return a head of ``stream`` with no rows: ``table`` holds its columns only."""
        return cls(
            table=table.slice(0, 0),
            status=status,
            kind=type(stream).__name__,
            label=str(stream.label),
            tag_columns=tuple(c for c in tag_columns if c in table.column_names),
            note=note,
        )

    def for_stream(
        self, stream: Any, *, origin: str | None = None, note: str | None = None
    ) -> StreamHead:
        """Return the same rows shown as ``stream`` (a source over its inner stream, a node
        over its cached output), optionally with another origin or note."""
        return dataclasses.replace(
            self,
            kind=type(stream).__name__,
            label=str(stream.label),
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
        return self.table.num_rows

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def header(self) -> str:
        """Return the one-line description shown above the rows."""
        return f"{self.kind}[{self.label}] — {self._describe()}"

    def _describe(self) -> str:
        shown = self.table.num_rows
        if self.status is HeadStatus.ROWS:
            text = self._describe_rows(shown)
        elif self.status is HeadStatus.PARTIAL:
            total = self.total_rows if self.total_rows is not None else shown
            if shown == total:
                text = f"{total:,} {_rows(total)} computed so far"
            else:
                text = f"first {shown:,} of {total:,} rows computed so far"
            text += " (computation incomplete)"
        else:
            return self.note or _DEFAULT_NOTES[self.status]
        if self.note:
            text += f"; {self.note}"
        return text

    def _describe_rows(self, shown: int) -> str:
        total = self.total_rows
        noun = _ORIGIN_NOUNS.get(self.origin, "rows")
        if shown == 0 and (total is None or total == 0):
            return "computed: no rows" if self.origin == "computed" else f"no {noun}"
        if total is None:
            return f"first {shown:,} {_rows(shown)}"
        if self.total_is_estimate:
            return f"first {shown:,} {_rows(shown)} of about {total:,}"
        if shown == total:
            return f"all {total:,} {noun if total != 1 else noun[:-1]}"
        return f"first {shown:,} of {total:,} {noun}"

    def _display_frame(self) -> pl.DataFrame:
        """Return a Polars frame of the rows for display: tag columns first, large cells
        summarised. ``table`` itself is never changed."""
        order = [c for c in self.tag_columns if c in self.table.column_names] + [
            c for c in self.table.column_names if c not in self.tag_columns
        ]
        table = self.table.select(order)
        columns = []
        for name in table.column_names:
            column = table.column(name)
            summary = _summarise(column)
            if summary is None:
                columns.append(column)
            else:
                columns.append(pa.chunked_array([summary], type=pa.large_string()))
        return pl.DataFrame(pa.table(columns, names=table.column_names))

    def _tags_line(self) -> str:
        if not self.tag_columns:
            return ""
        return "tags: " + ", ".join(self.tag_columns)

    def _repr_html_(self) -> str:
        parts = [f"<div><strong>{html.escape(self.header())}</strong></div>"]
        tags = self._tags_line()
        if tags:
            parts.append(f"<div><small>{html.escape(tags)}</small></div>")
        if self.table.num_columns:
            parts.append(self._display_frame()._repr_html_())
        return "\n".join(parts)

    def __str__(self) -> str:
        lines = [self.header()]
        tags = self._tags_line()
        if tags:
            lines.append(tags)
        if self.table.num_columns:
            lines.append(str(self._display_frame()))
        return "\n".join(lines)

    def __repr__(self) -> str:
        return (
            f"StreamHead({self.kind}[{self.label}], status={self.status.value}, "
            f"rows={self.table.num_rows})"
        )


def render_error_html(stream: Any, error: BaseException) -> str:
    """Return the preview of a stream whose ``head`` raised: its name and the error."""
    title = f"{type(stream).__name__}[{stream.label}] — could not read: {error}"
    return f"<div><strong>{html.escape(title)}</strong></div>"


def _rows(count: int) -> str:
    return "row" if count == 1 else "rows"


def _size(count: int) -> str:
    """Return a byte count as text: ``300 B``, ``1.2 KB``, ``3.4 MB``."""
    value = float(count)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1000 or unit == "GB":
            return f"{count} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1000
    return f"{count} B"  # pragma: no cover


def _summarise(column: pa.ChunkedArray) -> list[str | None] | None:
    """Return display strings for a column whose cells are too large to show as they are,
    or ``None`` to show the column as it is."""
    arrow_type = column.type
    if isinstance(arrow_type, pa.ExtensionType):
        return _summarise_extension(column, arrow_type)
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return [
            None if v is None else f"bytes ({_size(len(v))})"
            for v in column.to_pylist()
        ]
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        values = column.to_pylist()
        if all(v is None or len(v) <= _MAX_TEXT for v in values):
            return None
        return [
            v if v is None or len(v) <= _MAX_TEXT else v[: _MAX_TEXT - 3] + "…"
            for v in values
        ]
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return _summarise_list(column, arrow_type.value_type)
    return None


def _summarise_extension(
    column: pa.ChunkedArray, arrow_type: pa.ExtensionType
) -> list[str | None] | None:
    storage_type = arrow_type.storage_type
    short_name = arrow_type.extension_name.rsplit(".", 1)[-1]
    if pa.types.is_struct(storage_type):
        names = {storage_type.field(i).name for i in range(storage_type.num_fields)}
        # Only the small fields are read; an ndarray's ``data`` is never decoded.
        wanted = [n for n in ("dtype", "shape") if n in names]
        values = [
            v for array in column.chunks for v in _struct_fields(array.storage, wanted)
        ]
        if {"dtype", "shape"} <= names:
            return [
                None
                if v is None
                else f"{short_name} {v['dtype']} {_shape_text(v['shape'])}"
                for v in values
            ]
        return [None if v is None else f"<{short_name}>" for v in values]
    if pa.types.is_binary(storage_type) or pa.types.is_large_binary(storage_type):
        return [
            None if v is None else f"{short_name} ({_size(len(v))})"
            for array in column.chunks
            for v in array.storage.to_pylist()
        ]
    return None


def _struct_fields(array: pa.StructArray, names: list[str]) -> list[dict | None]:
    """Return each row of ``array`` as a dict of the named fields (``None`` for a null row)."""
    fields = {name: array.field(name).to_pylist() for name in names}
    valid = array.is_valid().to_pylist()
    return [
        {name: fields[name][i] for name in names} if valid[i] else None
        for i in range(len(array))
    ]


def _shape_text(shape: Any) -> str:
    dims = list(shape or [])
    if len(dims) == 1:
        return f"({dims[0]},)"
    return "(" + ", ".join(str(d) for d in dims) + ")"


def _summarise_list(
    column: pa.ChunkedArray, value_type: pa.DataType
) -> list[str | None] | None:
    simple_items = not (
        isinstance(value_type, pa.ExtensionType)
        or pa.types.is_nested(value_type)
        or pa.types.is_binary(value_type)
        or pa.types.is_large_binary(value_type)
    )
    lengths: list[int | None] = pc.list_value_length(column).to_pylist()
    if simple_items and all(n is None or n <= _MAX_LIST_ITEMS for n in lengths):
        return None
    values = column.to_pylist() if simple_items else [None] * len(lengths)
    item = "list" if pa.types.is_nested(value_type) else None
    out: list[str | None] = []
    for value, length in zip(values, lengths):
        if length is None:
            out.append(None)
        elif simple_items and length <= _MAX_LIST_ITEMS:
            out.append(repr(value))
        else:
            out.append(f"list (len {length})" + (f" of {item}" if item else ""))
    return out
