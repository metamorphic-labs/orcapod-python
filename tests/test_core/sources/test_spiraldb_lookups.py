"""A SpiralDB source looks its table up once to build and once per check (ITL-1350).

Every lookup of a table by name is a round trip to SpiralDB (0.2-0.9 s each in a
workshop-scale profile). A source used to look the table up for each item it needs: four
times to build (key, id, schema, version), three times per new-data check (id, schema,
version). It now takes them from one lookup (``SpiralDBConnector.lookup_table``). The
calls are counted on the repository's pyspiral fake; what the source decides is unchanged.
"""

from __future__ import annotations

import asyncio
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
# Two key columns (tags) and two data columns, in two arrangements: the key order of the
# table and the column layout of its rows (CodeRabbit on #479, the several-items rule).
ARRANGEMENTS: dict[str, tuple[list[str], list[str]]] = {
    # name: (key columns in key order, column layout)
    "keys_first": (["subject", "k"], ["subject", "k", "x", "y"]),
    "reversed": (["k", "subject"], ["y", "x", "k", "subject"]),
}
# The source's tag roles: every key a plain tag, or every key a data-tag (the default).
ROLES: dict[str, bool | tuple[str, ...]] = {"plain": True, "data_tags": ()}
_FIELDS = {
    "subject": pa.field("subject", pa.large_string(), nullable=False),
    "k": pa.field("k", pa.int64(), nullable=False),
    "x": pa.field("x", pa.int64(), nullable=False),
    "y": pa.field("y", pa.large_string(), nullable=True),
}
COUNTED = {
    (fake_pyspiral.FakeProject, "table"): "lookup",
    (fake_pyspiral.FakeProject, "list_tables"): "list_tables",
    (fake_pyspiral.FakeTable, "schema"): "schema",
    (fake_pyspiral.FakeTable, "last_modified_at"): "version",
    (fake_pyspiral.FakeTable, "get_metadata"): "metadata",
}


def _schema(layout: list[str]) -> pa.Schema:
    return pa.schema([_FIELDS[name] for name in layout])


def _keys(keys: list[str]) -> pa.Schema:
    return pa.schema([_FIELDS[name] for name in keys])


def _rows(layout: list[str], ks: list[int]) -> pa.Table:
    values = {
        "subject": [f"s{k % 2}" for k in ks],
        "k": ks,
        "x": [10 * k for k in ks],
        "y": [f"v{k}" for k in ks],
    }
    return pa.table({name: values[name] for name in layout}, schema=_schema(layout))


@pytest.fixture(params=sorted(ARRANGEMENTS))
def calls(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[collections.Counter[str]]:
    """A fake project with table ``default.t`` in one arrangement; counts the pyspiral
    calls made on it."""
    keys, layout = ARRANGEMENTS[request.param]
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
        table = project.create_table("default.t", key_schema=_keys(keys))
        table.write(_rows(layout, [0, 1, 2]))
        write_nullability_record(table, _schema(layout))
        _ARRANGED["keys"], _ARRANGED["layout"] = keys, layout
        counts.clear()
        yield counts


# The arrangement the current ``calls`` fixture built (for the recreation test).
_ARRANGED: dict[str, list[str]] = {}


def _source(role: str, **kwargs: Any) -> SpiralDBTableSource:  # pyright: ignore[reportExplicitAny]  # options
    return SpiralDBTableSource(
        project_id=_PROJECT, table_name="t", tag_only_columns=ROLES[role], **kwargs
    )


def _sync_and_async_rows(source: SpiralDBTableSource) -> tuple[list[Any], list[Any]]:  # pyright: ignore[reportExplicitAny]  # rows
    """Every row through the public sync and async reads, tags and data merged."""

    def merged(tag: Any, data: Any) -> tuple[tuple[str, Any], ...]:  # pyright: ignore[reportExplicitAny]
        return tuple(sorted({**tag.as_dict(), **data.as_dict()}.items()))

    async def collect() -> list[Any]:  # pyright: ignore[reportExplicitAny]
        return [merged(t, d) async for t, d in source.async_iter_data()]

    sync = [merged(t, d) for t, d in source.iter_data()]
    return sync, asyncio.run(collect())


def _expected_rows(ks: list[int]) -> list[Any]:  # pyright: ignore[reportExplicitAny]  # rows
    """The rows of ``_rows(..., ks)``, written out independently, tags and data merged."""
    return sorted(
        tuple(
            sorted({"subject": f"s{k % 2}", "k": k, "x": 10 * k, "y": f"v{k}"}.items())
        )
        for k in ks
    )


@pytest.mark.parametrize("role", sorted(ROLES))
def test_a_source_looks_its_table_up_once_to_build(
    calls: collections.Counter[str], role: str
) -> None:
    source = _source(role)
    # Before ITL-1350: 4 lookups (key, id, schema, version).
    assert calls == {
        "list_tables": 1,
        "lookup": 1,
        "schema": 1,
        "metadata": 1,
        "version": 1,
    }
    # Its reads are those of every source: the key order, layout and role it has.
    tags, data = source.keys()
    assert set(tags) == {"subject", "k"}
    assert {"x", "y"} <= set(data)
    assert (set(tags) <= set(data)) == (role == "data_tags")
    sync, async_ = _sync_and_async_rows(source)
    assert sorted(sync) == sorted(async_) == _expected_rows([0, 1, 2])


@pytest.mark.parametrize("role", sorted(ROLES))
def test_a_source_with_a_given_version_reads_no_version(
    calls: collections.Counter[str], role: str
) -> None:
    _source(role, asof=1)
    # Before ITL-1350: 3 lookups (key, id, schema).
    assert calls == {"list_tables": 1, "lookup": 1, "schema": 1, "metadata": 1}


@pytest.mark.parametrize("role", sorted(ROLES))
def test_each_check_looks_the_table_up_once(
    calls: collections.Counter[str], role: str
) -> None:
    source = _source(role)
    for _ in range(2):
        calls.clear()
        assert source.check_for_new_data() is False
        # Before ITL-1350: 3 lookups (id, schema, version).
        assert calls == {"lookup": 1, "schema": 1, "metadata": 1, "version": 1}
    sync, async_ = _sync_and_async_rows(source)
    assert sorted(sync) == sorted(async_) == _expected_rows([0, 1, 2])


@pytest.mark.parametrize("role", sorted(ROLES))
def test_a_check_still_finds_a_table_created_again(
    calls: collections.Counter[str], role: str
) -> None:
    source = _source(role)
    keys, layout = _ARRANGED["keys"], _ARRANGED["layout"]
    project = fake_pyspiral.FakeSpiral().project(_PROJECT)
    project.drop_table("default.t")
    again = project.create_table("default.t", key_schema=_keys(keys))
    again.write(_rows(layout, [0, 1, 2, 3]))
    write_nullability_record(again, _schema(layout))
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
        # The key columns in the table's key order.
        assert connector.get_pk_columns("t") == table.key_columns == _ARRANGED["keys"]
        assert connector.get_spiral_table_id("t") == table.table_id
        assert connector.get_arrow_schema("t").equals(
            table.arrow_schema(), check_metadata=True
        )
        assert table.arrow_schema().names == _ARRANGED["layout"]
        assert connector.get_table_version("t") == table.version()


@pytest.mark.parametrize("role", sorted(ROLES))
def test_the_arrangements_give_the_same_rows_and_pipeline_hash(
    tmp_path: Path, role: str
) -> None:
    """Two tables holding the same rows in the two arrangements: the same rows and the
    same pipeline hash (a schema, in no column order); different content hashes, since
    they are two tables (two table ids) and the key order is part of a row's record id."""
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        sources = {}
        for name, (keys, layout) in ARRANGEMENTS.items():
            table = project.create_table(f"default.{name}", key_schema=_keys(keys))
            table.write(_rows(layout, [0, 1, 2]))
            write_nullability_record(table, _schema(layout))
            sources[name] = SpiralDBTableSource(
                project_id=_PROJECT, table_name=name, tag_only_columns=ROLES[role]
            )
        first, second = sources.values()
        for source in (first, second):
            sync, async_ = _sync_and_async_rows(source)
            assert sorted(sync) == sorted(async_) == _expected_rows([0, 1, 2])
        assert first.pipeline_hash() == second.pipeline_hash()
        assert first.content_hash() != second.content_hash()
