"""What a SpiralDB source reads again without fetching it again (ITL-1457), on the fake.

A source keeps the opened snapshot of its pinned version. The rows, refusals, warnings and
what is looked at in which order are those of a source that keeps nothing.
"""

from __future__ import annotations

import asyncio
import collections
import os
import threading
import types
import warnings
import weakref
from collections.abc import Callable, Generator, Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.databases import InMemoryArrowDatabase, SpiralDBConnector
from orcapod.databases import spiraldb_connector as connector_module
from orcapod.databases import spiraldb_schema_table as st
from orcapod.errors import (
    NullInNonNullableColumnError,
    SourceCheckFailedWarning,
    TableSchemaError,
)
from orcapod.operators import SelectColumns
from orcapod.pipeline import node_run
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_spiral_sdk as fake

_SCHEMA = pa.schema(
    [
        pa.field("k", pa.int64(), False),
        pa.field("j", pa.large_string(), False),
        pa.field("x", pa.float64(), False),
        pa.field("y", pa.large_string()),
    ]
)
_TABLES = ("a", "b", "c")


def _rows(offset: int) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {"k": i + offset, "j": f"j{i % 2}", "x": 0.5 * i, "y": None}
            for i in range(4)
        ],
        schema=_SCHEMA,
    )


def _make(project: str, dataset: str) -> None:
    made = fake.ExternalProject(project)
    for offset, name in enumerate(_TABLES):
        made.create_table(
            f"{dataset}.{name}", key_schema=[_SCHEMA.field("k"), _SCHEMA.field("j")]
        ).write(_rows(10 * offset))
    fake.migrate(project, dict.fromkeys(_TABLES, _SCHEMA), dataset)


@pytest.fixture()
def root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Two projects with two datasets of three tables each, and nothing shared yet."""
    monkeypatch.setattr(connector_module, "_warned_unrecorded", set())
    monkeypatch.setenv("SPIRAL_AUTH_FILE", str(tmp_path / "auth.json"))
    with fake.install(tmp_path / "spiral"):
        for project in ("p", "q"):
            for dataset in ("default", "other"):
                _make(project, dataset)
        yield tmp_path


class _Calls:
    """The SDK calls made, by kind, in order."""

    def __init__(self) -> None:
        """Start with none."""
        self.log: list[str] = []
        self.clients: list[weakref.ReferenceType[Any]] = []
        self.lock = threading.Lock()

    def add(self, what: str) -> None:
        """Log a call."""
        with self.lock:
            self.log.append(what)

    def take(self) -> dict[str, int]:
        """Return the counts since the last time, and start again."""
        with self.lock:
            counts = dict(collections.Counter(self.log))
            self.log.clear()
        return counts


@pytest.fixture()
def calls(root: Path, monkeypatch: pytest.MonkeyPatch) -> _Calls:
    """Every SDK call, logged."""
    made = _Calls()
    init, opened = fake.FakeClient.__init__, fake.FakeClient.keyed_table
    by_id = fake.FakeClient.keyed_table_by_id
    schema, scan = fake.FakeKeyedTable.schema, fake.FakeKeyedTable.scan

    def is_schemas(table: Any) -> bool:
        return table._table["name"] == st.SCHEMA_TABLE

    def new_init(self: Any, **given: Any) -> None:
        made.add("client")
        made.clients.append(weakref.ref(self))
        init(self, **given)

    def new_opened(self: Any, project: str, dataset: str, name: str, **given: Any):
        made.add("open schemas" if name == st.SCHEMA_TABLE else "open")
        return opened(self, project, dataset, name, **given)

    def new_by_id(self: Any, *args: Any, **given: Any):
        made.add("open by id")
        return by_id(self, *args, **given)

    def new_schema(self: Any):
        if not is_schemas(self):
            made.add("schema")
        return schema(self)

    def new_scan(self: Any, *args: Any, **given: Any):
        made.add("read schemas" if is_schemas(self) else "scan")
        return scan(self, *args, **given)

    monkeypatch.setattr(fake.FakeClient, "__init__", new_init)
    monkeypatch.setattr(fake.FakeClient, "keyed_table", new_opened)
    monkeypatch.setattr(fake.FakeClient, "keyed_table_by_id", new_by_id)
    monkeypatch.setattr(fake.FakeKeyedTable, "schema", new_schema)
    monkeypatch.setattr(fake.FakeKeyedTable, "scan", new_scan)
    return made


def _source(
    name: str = "a", project: str = "p", dataset: str = "default", **given: Any
):
    return SpiralDBTableSource(
        project, name, dataset=dataset, tag_only_columns=True, **given
    )


def _ks(source: SpiralDBTableSource) -> list[Any]:
    return source.as_table()["k"].to_pylist()


# ------------------------------------------------------------ the schema table's state


def _damage(**change: Any) -> None:
    table = fake.FakeClient().keyed_table("p", "default", st.SCHEMA_TABLE)
    (marker,) = [
        r for r in table.scan().to_table().to_pylist() if r["table_id"] == st.MARKER_KEY
    ]
    table.append(pa.Table.from_pylist([{**marker, **change}], schema=st.ROW_SCHEMA))


def _moved_on(source: SpiralDBTableSource) -> None:
    """Write the source's table, and read once: no kept snapshot is its current one."""
    _write("a", 60)
    _ = source.head()


def _write(name: str, k: int) -> None:
    fake.ExternalProject("p").table(f"default.{name}").write(_rows(k).slice(0, 1))


def _job(sources: list[Any]) -> PipelineJob:
    job = PipelineJob(name="checks", database=InMemoryArrowDatabase())
    with job:
        for index, source in enumerate(sources):
            SelectColumns(["k", "j", "x"])(source, label=f"n{index}")
    return job


def _refuse_kinds() -> dict[str, Callable[[], None]]:
    def incomplete() -> None:
        fake.ExternalProject("p").drop_table(f"default.{st.SCHEMA_TABLE}")
        fake.migrate("p", dict.fromkeys(_TABLES, _SCHEMA), complete=False)

    return {
        "withdrawn": lambda: _damage(rows_digest="withdrawn"),
        "incomplete": incomplete,
        "damaged": lambda: _damage(table_count=100),
        "other scope": lambda: _damage(dataset="other"),
    }


@pytest.mark.parametrize("kind", sorted(_refuse_kinds()))
@pytest.mark.parametrize("during", ["open", "schema"])
def test_a_schema_table_changed_during_the_call_before_it_is_refused_by_that_read(
    root: Path, monkeypatch: pytest.MonkeyPatch, kind: str, during: str
) -> None:
    """Review of #553, finding 1: the schema table is looked at last."""
    source = _source("a")
    if during == "schema":
        _moved_on(source)
    changed: list[bool] = []

    def change() -> None:
        if not changed:
            changed.append(True)
            _refuse_kinds()[kind]()

    opened, schema = fake.FakeClient.keyed_table, fake.FakeKeyedTable.schema

    def new_opened(self: Any, project: str, dataset: str, name: str, **given: Any):
        table = opened(self, project, dataset, name, **given)
        if name == "a" and during == "open":
            change()
        return table

    def new_schema(self: Any):
        found = schema(self)
        if self._table["name"] == "a" and during == "schema":
            change()
        return found

    monkeypatch.setattr(fake.FakeClient, "keyed_table", new_opened)
    monkeypatch.setattr(fake.FakeKeyedTable, "schema", new_schema)
    with pytest.raises(TableSchemaError):
        source.as_table()
    assert changed


def test_each_source_has_its_own_sign_in(
    calls: _Calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review of #553, finding 2: a client is never taken for another account's."""
    init = fake.FakeClient.__init__

    def bind(self: Any, **given: Any) -> None:
        init(self, **given)
        self.account = os.environ["SPIRAL_TOKEN"]
        if (
            len(calls.clients) == 1
        ):  # another account while the first was made, and back
            self.account = "b"

    monkeypatch.setattr(fake.FakeClient, "__init__", bind)
    monkeypatch.setenv("SPIRAL_TOKEN", "a")
    made = [SpiralDBConnector("p") for _ in range(3)]
    assert [c._client.account for c in made] == ["b", "a", "a"]  # pyright: ignore[reportPrivateUsage]  # which each signed in as


def test_checks_look_at_one_source_after_another(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review of #553, finding 3: a row written during one check is in the next."""
    a, b = _source("a"), _source("b")
    _write("a", 60)  # so that a's check fetches its schema
    schema = fake.FakeKeyedTable.schema
    wrote: list[bool] = []

    def new_schema(self: Any):
        if self._table["name"] == "a" and not wrote:
            wrote.append(True)
            _write("b", 99)
        return schema(self)

    monkeypatch.setattr(fake.FakeKeyedTable, "schema", new_schema)
    assert node_run.check_sources([a, b]) == ()
    assert wrote and 99 in _ks(b)


def test_a_failing_schema_is_raised_without_looking_at_the_schema_table(
    calls: _Calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review of #553, finding 4."""
    source = _source("a")
    _moved_on(source)
    calls.take()

    def failing(self: Any):
        raise fake.SpiralError("status 503")

    monkeypatch.setattr(fake.FakeKeyedTable, "schema", failing)
    with pytest.raises(fake.SpiralError, match="503"):
        source.as_table()
    assert "open schemas" not in calls.take()


def test_a_failing_open_is_raised_without_looking_at_the_schema_table(
    calls: _Calls,
) -> None:
    source = _source("a")
    fake.ExternalProject("p").drop_table("default.a")
    calls.take()
    with pytest.raises(ValueError, match="not found"):
        source.as_table()
    assert calls.take() == {"open": 1}


def test_a_subclass_looks_its_table_up_in_the_calling_thread(root: Path) -> None:
    """Review of #553, finding 5."""
    here = threading.get_ident()

    class ThreadBound(SpiralDBTableSource):
        """Its lookup works in one thread only."""

        def _lookup_table(self) -> Any:
            assert threading.get_ident() == here
            return super()._lookup_table()

    bound = ThreadBound("p", "a", tag_only_columns=True)
    assert node_run.check_sources([bound, _source("b")]) == ()
    assert _ks(bound) == [0, 1, 2, 3]


def test_failing_checks_warn_in_the_sources_order(root: Path) -> None:
    sources = [_source(n) for n in _TABLES]
    for name in ("a", "c"):
        fake.ExternalProject("p").drop_table(f"default.{name}")
    _write("b", 77)
    with pytest.warns(SourceCheckFailedWarning) as seen:
        assert len(node_run.check_sources(sources)) == 2
    assert ["default.a" in str(w.message) for w in seen] == [True, False]
    assert 77 in _ks(sources[1])


# ------------------------------------------------------------------------------ budgets


def test_the_calls_each_action_makes(calls: _Calls) -> None:
    """A call more is a call repeated."""
    read = {"open": 1, "open schemas": 1, "scan": 1}
    sources = [_source(n) for n in _TABLES]
    assert calls.take() == {
        "client": 3,
        "open": 3,
        "schema": 3,
        "open schemas": 3,
        "read schemas": 3,
    }
    first = sources[0]
    actions: dict[str, tuple[Callable[[], Any], dict[str, int]]] = {
        "first preview": (lambda: first.head(2), read | {"scan": 2}),
        "preview": (lambda: first.head(2), read),
        "run, the cap's count": (first.run, read),
        "run": (first.run, read),
        "where": (lambda: first.where(j="j0"), {}),
        "restricted read": (lambda: first.where(j="j0").as_table(), read),
        "count": (lambda: first.row_count_estimate, {"open": 1, "open schemas": 1}),
    }
    for name, (action, budget) in actions.items():
        action()
        assert calls.take() == budget, name
    job = _job(sources)
    for runner, budget in (
        (None, {"open": 6, "open schemas": 6, "scan": 5}),
        (None, {"open": 3, "open schemas": 3}),
        (AsyncPipelineOrchestrator(), {"open": 3, "open schemas": 3}),
    ):
        job.run(orchestrator=runner)
        assert calls.take() == budget


# ------------------------------------------------------------------- the kept snapshot


def _reads(source: SpiralDBTableSource) -> dict[str, Callable[[], Any]]:
    async def collect() -> list[Any]:
        return [t.as_dict()["k"] async for t, _ in source.async_iter_data()]

    return {
        "table": lambda: source.as_table()["k"].to_pylist(),
        "iter": lambda: [t.as_dict()["k"] for t, _ in source.iter_data()],
        "async": lambda: sorted(asyncio.run(collect())),
        "head": lambda: source.head(9).table["k"].to_pylist(),
        "run": lambda: source.run().table["k"].to_pylist(),
        "count": lambda: list(range(source.row_count_estimate)),
    }


_READS = ["table", "iter", "async", "head", "run", "count"]
_CHANGES: dict[str, Callable[[], Any]] = {
    "dropped": lambda: fake.ExternalProject("p").drop_table("default.a"),
    "recreated": lambda: (
        fake.ExternalProject("p").drop_table("default.a"),
        fake.ExternalProject("p")
        .create_table("default.a", key_schema=[_SCHEMA.field("k"), _SCHEMA.field("j")])
        .write(_rows(0)),
    ),
    "withdrawn": lambda: _damage(rows_digest="withdrawn"),
    "incomplete": lambda: _damage(table_count=9),
    "re-recorded": lambda: fake.migrate("p", {"a": _other_flags()}),
    "new rows": lambda: _write("a", 60),
    "new column": lambda: (
        fake.ExternalProject("p")
        .table("default.a")
        .write(_rows(60).append_column("w", pa.array([1] * 4)))
    ),
    "nothing": lambda: None,
}


def _other_flags() -> pa.Schema:
    return pa.schema([f.with_nullable(True) if f.name == "x" else f for f in _SCHEMA])


def _outcome(read: Callable[[], Any]) -> tuple[str, str]:
    """What a read gives or raises, without the parts that name this run's ids."""
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        try:
            result = repr(read())
        except Exception as exc:
            result = f"{type(exc).__name__}: {exc} {getattr(exc, '__notes__', [])}"
    return result, repr([str(w.message) for w in seen])


@pytest.mark.parametrize("read", _READS)
@pytest.mark.parametrize("change", sorted(_CHANGES))
@pytest.mark.parametrize("restricted", [False, True])
def test_a_kept_snapshot_changes_no_outcome(
    calls: _Calls, change: str, read: str, restricted: bool
) -> None:
    """Two sources on one table, one of which keeps nothing: equal rows and refusals,
    before and after a check."""
    keeping, plain = _source("a"), _source("a")
    if restricted:
        keeping, plain = keeping.where(j="j0"), plain.where(j="j0")

    def forget() -> None:
        plain._kept = None  # pyright: ignore[reportPrivateUsage]  # keeps nothing

    for _ in range(2):
        forget()
        assert _outcome(_reads(keeping)[read]) == _outcome(_reads(plain)[read])
    _CHANGES[change]()
    steps: list[Callable[[SpiralDBTableSource], Any]] = [
        lambda source: _reads(source)[read](),
        lambda source: source.check_for_new_data(),
        lambda source: _reads(source)[read](),
        lambda source: source.where(k=1).as_table().to_pydict(),
    ]
    for step in steps:
        forget()
        expected = _outcome(lambda step=step: step(plain))
        calls.take()
        assert _outcome(lambda step=step: step(keeping)) == expected
        if change in ("dropped", "recreated", "withdrawn", "incomplete"):
            assert "scan" not in calls.take()


def test_a_kept_snapshot_is_read_only_after_both_confirm(calls: _Calls) -> None:
    source = _source("a", max_rows=None)
    for read in _READS[:-1]:
        calls.take()
        _reads(source)[read]()
        assert calls.log[:2] == ["open", "open schemas"]
        assert set(calls.log[2:]) == {"scan"}


@pytest.mark.parametrize("read", _READS)
@pytest.mark.parametrize("when", ["plan", "first batch"])
def test_a_kept_snapshot_that_fails_is_read_afresh(
    calls: _Calls, monkeypatch: pytest.MonkeyPatch, read: str, when: str
) -> None:
    """A grant or credential that a handle holds may expire."""
    source, kept = _source("a"), []
    expected = _reads(_source("a"))[read]()
    kept.append(source._kept.handle)  # pyright: ignore[reportPrivateUsage, reportOptionalMemberAccess]  # the handle that fails
    scan = fake.FakeKeyedTable.scan

    class Expired:
        """A scan whose reader fails at its first batch."""

        def to_reader(self) -> Any:
            """Return itself."""
            return self

        def __iter__(self) -> Any:
            return self

        def __next__(self) -> Any:
            raise fake.SpiralError("grant expired")

        def close(self) -> None:
            """Nothing to close."""

    def new_scan(self: Any, *args: Any, **given: Any) -> Any:
        if self is not kept[0]:
            return scan(self, *args, **given)
        if when == "plan":
            raise fake.SpiralError("grant expired")
        return Expired()

    monkeypatch.setattr(fake.FakeKeyedTable, "scan", new_scan)
    assert _reads(source)[read]() == expected
    # The snapshot opened afresh is the one that is kept now.
    assert source._kept is not None and source._kept.handle is not kept[0]  # pyright: ignore[reportPrivateUsage]  # the subject
    calls.take()
    assert _reads(source)[read]() == expected
    assert "schema" not in calls.take()


def test_a_kept_snapshot_that_fails_later_fails_the_read(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No row may be given twice, or of two reads."""
    source = _source("a", max_rows=None)
    scan = fake.FakeKeyedTable.scan

    def new_scan(self: Any, *args: Any, **given: Any) -> Any:
        rows = scan(self, *args, **given).to_table()

        def batches() -> Iterator[Any]:
            yield rows.slice(0, 2).to_batches()[0]
            raise fake.SpiralError("status 500")

        reader = batches()
        return types.SimpleNamespace(to_reader=lambda: reader)

    monkeypatch.setattr(fake.FakeKeyedTable, "scan", new_scan)
    rows = source.iter_data()
    assert [next(rows)[0].as_dict()["k"] for _ in range(2)] == [0, 1]
    with pytest.raises(fake.SpiralError, match="500") as raised:
        next(rows)
    assert raised.value.__notes__ == ["SpiralDB: could not read p/default/a"]


def test_a_null_in_a_kept_snapshot_is_refused_and_not_read_afresh(
    calls: _Calls,
) -> None:
    source = _source("a", max_rows=None)
    bad = _rows(0).to_pylist()
    bad[0]["x"] = None
    fake.ExternalProject("p").table("default.a").write(
        pa.Table.from_pylist(
            bad, schema=pa.schema([f.with_nullable(True) for f in _SCHEMA])
        )
    )
    assert source.check_for_new_data()
    for _ in range(2):
        calls.take()
        with pytest.raises(NullInNonNullableColumnError, match="p/default/a"):
            source.as_table()
        assert calls.take()["scan"] == 1


def test_a_check_keeps_the_version_it_pins(calls: _Calls) -> None:
    source = _source("a")
    copy = source.where(j="j0")
    _write("a", 60)
    assert source.check_for_new_data()
    calls.take()
    assert _ks(source) == [0, 1, 2, 3, 60]
    assert "schema" not in calls.take()
    # A copy pins its own version still.
    assert _ks(copy) == [0, 2]


def test_a_closed_source_keeps_nothing(root: Path) -> None:
    source = _source("a")
    rows = source.iter_data()
    _ = next(rows)
    source.close()
    assert source._kept is None  # pyright: ignore[reportPrivateUsage]  # the subject
    assert len(list(rows)) == 3
    assert source._kept is None  # pyright: ignore[reportPrivateUsage]  # the subject


def test_threads_read_one_kept_snapshot(root: Path) -> None:
    source = _source("a")
    reads = [
        _reads(source.where(j="j0") if i % 2 else source)["table"] for i in range(32)
    ]
    with ThreadPoolExecutor(8) as pool:
        found = list(pool.map(lambda read: read(), reads))
    assert found == [[0, 1, 2, 3], [0, 2]] * 16


def test_an_early_stop_closes_the_reader(root: Path) -> None:
    table = SpiralDBConnector("p").lookup_table("a")
    _ = table.arrow_schema()
    again = SpiralDBConnector("p").lookup_table("a", table.opened_snapshot())
    batches = again.scan_batches()
    _ = next(batches)
    assert isinstance(batches, Generator)
    batches.close()
