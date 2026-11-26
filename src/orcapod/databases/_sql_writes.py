"""Row isolation for a SQL upsert that rolled back after a data refusal."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Protocol, runtime_checkable

import pyarrow as pa


@runtime_checkable
class IsolatesRefusedRows(Protocol):
    def isolate_refused_rows(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool,
        error: Exception,
    ) -> dict[int, str] | None:
        """After a refusal, commit other rows and return the rejected positions.

        Return None for an error that is not a data refusal. A failure in isolation
        itself must roll back all writes in the isolation transaction.
        """
        ...


class _Execute(Protocol):
    def __call__(self, sql: str, params: tuple[object, ...] = (), /) -> object: ...


def write_with_savepoints(
    execute: _Execute,
    sql: str,
    rows: Iterable[tuple[object, ...]],
    is_refusal: Callable[[Exception], bool],
) -> dict[int, str]:
    """Write rows in the caller's transaction, rolling back each refused statement."""
    refused: dict[int, str] = {}
    for index, row in enumerate(rows):
        execute("SAVEPOINT orcapod_row")
        try:
            execute(sql, row)
        except Exception as error:
            if not is_refusal(error):
                raise
            execute("ROLLBACK TO SAVEPOINT orcapod_row")
            refused[index] = str(error)
        execute("RELEASE SAVEPOINT orcapod_row")
    return refused
