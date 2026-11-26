"""Row isolation for a SQL upsert that rolled back after a data refusal."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

import pyarrow as pa


@dataclass
class RowIsolation:
    """Positions that became durable or were definitively refused."""

    settled: set[int] = field(default_factory=set)
    refused: dict[int, str] = field(default_factory=dict)


@runtime_checkable
class IsolatesRefusedRows(Protocol):
    def isolate_refused_rows(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool,
        error: Exception,
        progress: RowIsolation,
    ) -> bool:
        """After a refusal, commit other rows and record each settled position.

        Return False for an error that is not a data refusal. If isolation is
        interrupted, retain any positions already settled.
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
