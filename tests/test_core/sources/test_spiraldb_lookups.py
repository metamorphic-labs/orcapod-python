"""A SpiralDB source looks its table up once to build and once per check (ITL-1350).

Every lookup of a table by name is a round trip to SpiralDB (0.2-0.9 s each in a
workshop-scale profile). A source used to look the table up for each item it needs: four
times to build (key, id, schema, version), three times per new-data check (id, schema,
version). It now takes them from one lookup (``SpiralDBConnector.lookup_table``). The
calls are counted on the repository's pyspiral fake; what the source decides is unchanged.
"""

from __future__ import annotations

import collections
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod.databases import SpiralDBConnector, write_nullability_record
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_pyspiral

_PROJECT = "lookups"
_KEYS = pa.schema([pa.field("k", pa.int64(), nullable=False)])
_SCHEMA = pa.schema(
    [
        pa.field("k", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
        pa.field("y", pa.large_string(), nullable=True),
    ]
)
COUNTED = {
    (fake_pyspiral.FakeProject, "table"): "lookup",
    (fake_pyspiral.FakeProject, "list_tables"): "list_tables",
    (fake_pyspiral.FakeTable, "schema"): "schema",
    (fake_pyspiral.FakeTable, "last_modified_at"): "version",
    (fake_pyspiral.FakeTable, "get_metadata"): "metadata",
}


def _rows(keys: list[int]) -> pa.Table:
    return pa.table(
        {"k": keys, "x": [10 * k for k in keys], "y": [f"v{k}" for k in keys]},
        schema=_SCHEMA,
    )


@pytest.fixture()
def calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[collections.Counter[str]]:
    """A fake project with table ``default.t``; counts the pyspiral calls made on it."""
    counts: collections.Counter[str] = collections.Counter()
    for (cls, name), label in COUNTED.items():
        original: Callable[..., Any] = getattr(cls, name)  # pyright: ignore[reportExplicitAny]  # the fake's method

        def wrapper(
            *args: Any,  # pyright: ignore[reportExplicitAny]  # passed through
            _original: Callable[..., Any] = original,  # pyright: ignore[reportExplicitAny]
            _label: str = label,
            **kwargs: Any,  # pyright: ignore[reportExplicitAny]
        ) -> Any:  # pyright: ignore[reportExplicitAny]
            counts[_label] += 1
            return _original(*args, **kwargs)

        monkeypatch.setattr(cls, name, wrapper)
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        table = project.create_table("default.t", key_schema=_KEYS)
        table.write(_rows([0, 1, 2]))
        write_nullability_record(table, _SCHEMA)
        counts.clear()
        yield counts


def _source(**kwargs: Any) -> SpiralDBTableSource:  # pyright: ignore[reportExplicitAny]  # options
    return SpiralDBTableSource(
        project_id=_PROJECT, table_name="t", tag_only_columns=True, **kwargs
    )


def test_a_source_looks_its_table_up_once_to_build(
    calls: collections.Counter[str],
) -> None:
    source = _source()
    # Before ITL-1350: 4 lookups (key, id, schema, version).
    assert calls == {
        "list_tables": 1,
        "lookup": 1,
        "schema": 1,
        "metadata": 1,
        "version": 1,
    }
    assert source.as_table().num_rows == 3


def test_a_source_with_a_given_version_reads_no_version(
    calls: collections.Counter[str],
) -> None:
    _source(asof=1)
    # Before ITL-1350: 3 lookups (key, id, schema).
    assert calls == {"list_tables": 1, "lookup": 1, "schema": 1, "metadata": 1}


def test_each_check_looks_the_table_up_once(calls: collections.Counter[str]) -> None:
    source = _source()
    for _ in range(2):
        calls.clear()
        assert source.check_for_new_data() is False
        # Before ITL-1350: 3 lookups (id, schema, version).
        assert calls == {"lookup": 1, "schema": 1, "metadata": 1, "version": 1}


def test_a_check_still_finds_a_table_created_again(
    calls: collections.Counter[str],
) -> None:
    source = _source()
    project = fake_pyspiral.FakeSpiral().project(_PROJECT)
    project.drop_table("default.t")
    again = project.create_table("default.t", key_schema=_KEYS)
    again.write(_rows([0, 1, 2, 3]))
    write_nullability_record(again, _SCHEMA)
    calls.clear()
    with pytest.raises(ValueError, match="dropped and created again"):
        source.check_for_new_data()
    # Refused on the id, before the new table's schema or version is read.
    assert calls == {"lookup": 1}


def test_the_connector_getters_answer_as_the_handle_does(
    calls: collections.Counter[str],
) -> None:
    with SpiralDBConnector(project_id=_PROJECT) as connector:
        table = connector.lookup_table("t")
        assert connector.get_pk_columns("t") == table.key_columns == ["k"]
        assert connector.get_spiral_table_id("t") == table.table_id
        assert connector.get_arrow_schema("t").equals(
            table.arrow_schema(), check_metadata=True
        )
        assert connector.get_table_version("t") == table.version()
