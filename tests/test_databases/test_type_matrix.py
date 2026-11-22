"""The supported-data-types matrix: every type kept or refused by every database (ITL-1334).

``type_matrix.measure_all`` writes and reads every type of the matrix through every
database. No cell may be silent (another type, other values, an error that does not name
the column), and the committed page ``docs/reference/database-types.md`` must equal the
page rendered from the measurements (``scripts/database_type_matrix.py`` writes it).
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path

import pytest

from tests.test_databases.type_matrix import (
    DATABASES,
    KEPT,
    KEPT_BY_RULE,
    PAGE,
    REFUSED,
    ROWS,
    SILENT,
    Cell,
    measure_all,
    render,
)


@pytest.fixture(scope="module")
def cells(tmp_path_factory: pytest.TempPathFactory) -> dict[tuple[str, str], Cell]:
    """Every cell, measured once for the module (its warnings and logs silenced)."""
    previous = logging.root.manager.disable
    logging.disable(logging.WARNING)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return measure_all(tmp_path_factory.mktemp("matrix"))
    finally:
        logging.disable(previous)


def test_no_database_changes_a_type_silently(
    cells: dict[tuple[str, str], Cell],
) -> None:
    silent = {key: cell.detail for key, cell in cells.items() if cell.kind == SILENT}
    assert not silent, "\n".join(
        f"{row} / {db}: {d}" for (row, db), d in silent.items()
    )


def test_every_row_has_every_database(cells: dict[tuple[str, str], Cell]) -> None:
    assert set(cells) == {(row.label, db) for row in ROWS for db in DATABASES}
    assert len({row.label for row in ROWS}) == len(ROWS)


def test_the_in_memory_database_keeps_every_type(
    cells: dict[tuple[str, str], Cell],
) -> None:
    assert {cells[(row.label, "in-memory")].kind for row in ROWS} == {KEPT}


def test_the_postgresql_column_is_the_connectors_rule(
    cells: dict[tuple[str, str], Cell],
) -> None:
    """No server is used: a PostgreSQL cell is the rule's verdict, never a read."""
    assert {cells[(row.label, "PostgreSQL")].kind for row in ROWS} <= {
        KEPT_BY_RULE,
        REFUSED,
    }


def test_the_committed_page_matches_the_measurements(
    cells: dict[tuple[str, str], Cell],
) -> None:
    """``uv run python scripts/database_type_matrix.py`` rewrites the page."""
    assert Path(PAGE).read_text(encoding="utf-8") == render(cells)
