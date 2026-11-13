"""Node-owned storage views, retained only for the most recent input buffer."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, TypeVar

TagT = TypeVar("TagT")
DataT = TypeVar("DataT")


class RetainedRowsMixin:
    """Own reusable row views and expose explicit release to job orchestration."""

    _sync_row_views: dict[int, tuple[Any, Any]] | None = None

    def clear_retained_rows(self) -> None:
        """Release row views without removing any persisted results."""
        self._sync_row_views = {}

    def retain_storage_rows(
        self,
        rows: Sequence[tuple[TagT, DataT]],
        *,
        tag_view: Callable[[TagT], TagT],
        data_view: Callable[[DataT], DataT],
    ) -> list[tuple[TagT, DataT]]:
        """Return canonical stored rows, reusing views for identical live rows."""
        previous = self._sync_row_views or {}
        retained: dict[int, tuple[Any, Any]] = {}

        def view(row: Any, convert: Any) -> Any:
            cached = previous.get(id(row))
            value = (
                cached[1] if cached is not None and cached[0] is row else convert(row)
            )
            retained[id(row)] = (row, value)
            return value

        result = [(view(tag, tag_view), view(data, data_view)) for tag, data in rows]
        self._sync_row_views = retained
        return result
