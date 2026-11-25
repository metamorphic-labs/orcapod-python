"""``SpiralDBTableSource(connector=...)`` (ITL-1457), on the fake.

Sources given one connector use one client and read the schema table once per version.
Everything else is compared with sources that make their own: rows, order, ids, hashes,
errors, warnings, and the SDK calls in their order. The findings of the reviews of #553 and
#554, which shared or reused more than this, are cases here.
"""

from __future__ import annotations

import asyncio
import collections
import gc
import os
import threading
import types
import warnings
import weakref
from collections.abc import Callable, Iterator
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
    UnrecordedTableSchemaWarning,
)
from orcapod.operators import SelectColumns
from orcapod.pipeline import node_run
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_spiral_sdk as fake

_FIELDS = {
    "k": pa.field("k", pa.int64(), False),
    "j": pa.field("j", pa.large_string(), False),
    "x": pa.field("x", pa.float64(), False),
    "y": pa.field("y", pa.large_string()),
}
# Key order and column layout, in two arrangements.
_ARRANGEMENTS = {
    "keys first": (["k", "j"], ["k", "j", "x", "y"]),
    "reversed": (["j", "k"], ["y", "x", "j", "k"]),
}
_TABLES = ("a", "b", "c")
_LAYOUT: list[str] = []


def _schema() -> pa.Schema:
    return pa.schema([_FIELDS[n] for n in _LAYOUT])


def _rows(offset: int, count: int = 4) -> pa.Table:
    values = [
        {"k": i + offset, "j": f"j{i % 2}", "x": 0.5 * i, "y": None}
        for i in range(count)
    ]
    return pa.Table.from_pylist(values, schema=_schema())


def _make(project: str, dataset: str, keys: list[str]) -> None:
    made = fake.ExternalProject(project)
    for index, name in enumerate(_TABLES):
        made.create_table(
            f"{dataset}.{name}", key_schema=[_FIELDS[k] for k in keys]
        ).write(_rows(10 * index))
    fake.migrate(project, dict.fromkeys(_TABLES, _schema()), dataset)


@pytest.fixture(params=sorted(_ARRANGEMENTS))
def root(
    request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Path]:
    """Three tables in a dataset, in one arrangement, also in another dataset and project."""
    monkeypatch.setattr(connector_module, "_warned_unrecorded", set())
    keys, _LAYOUT[:] = _ARRANGEMENTS[request.param]
    with fake.install(tmp_path / "spiral"):
        for project, dataset in (("p", "default"), ("p", "other"), ("q", "default")):
            _make(project, dataset, keys)
        yield tmp_path


class _Calls:
    """The SDK calls made, in order, and the clients that were made."""

    def __init__(self) -> None:
        """Start with none."""
        self.log: list[str] = []
        self.clients: list[weakref.ReferenceType[Any]] = []

    def take(self) -> list[str]:
        """Return the calls since the last time."""
        taken, self.log = self.log, []
        return taken


@pytest.fixture()
def calls(root: Path, monkeypatch: pytest.MonkeyPatch) -> _Calls:
    """Every SDK call, logged. A call on the schema table is marked."""
    made = _Calls()

    def wrap(cls: type, name: str, label: Callable[..., str]) -> None:
        original = getattr(cls, name)

        def logged(self: Any, *args: Any, **given: Any) -> Any:
            made.log.append(label(self, *args))
            if name == "__init__":
                made.clients.append(weakref.ref(self))
            return original(self, *args, **given)

        monkeypatch.setattr(cls, name, logged)

    def on(what: str) -> Callable[..., str]:
        return lambda self, *_a: (
            f"{what} schemas" if self._table["name"] == st.SCHEMA_TABLE else what
        )

    wrap(fake.FakeClient, "__init__", lambda self, *_a: "client")
    wrap(
        fake.FakeClient,
        "keyed_table",
        lambda self, p, d, name: "open schemas" if name == st.SCHEMA_TABLE else "open",
    )
    wrap(fake.FakeClient, "keyed_table_by_id", lambda self, *_a: "open by id")
    wrap(fake.FakeKeyedTable, "schema", on("schema"))
    wrap(fake.FakeKeyedTable, "scan", on("scan"))
    return made


def _source(name: str = "a", **given: Any) -> SpiralDBTableSource:
    options: dict[str, Any] = {"tag_only_columns": True, **given}
    return SpiralDBTableSource("p", name, **options)


def _ks(source: SpiralDBTableSource) -> list[Any]:
    return source.as_table()["k"].to_pylist()


def _damage(**change: Any) -> None:
    table = fake.FakeClient().keyed_table("p", "default", st.SCHEMA_TABLE)
    (marker,) = [
        r for r in table.scan().to_table().to_pylist() if r["table_id"] == st.MARKER_KEY
    ]
    table.append(pa.Table.from_pylist([{**marker, **change}], schema=st.ROW_SCHEMA))


def _incomplete() -> None:
    fake.ExternalProject("p").drop_table(f"default.{st.SCHEMA_TABLE}")
    fake.migrate("p", dict.fromkeys(_TABLES, _schema()), complete=False)


def _write(name: str, k: int, count: int = 1) -> None:
    fake.ExternalProject("p").table(f"default.{name}").write(_rows(k, count))


def _recreate() -> None:
    made = fake.ExternalProject("p")
    made.drop_table("default.a")
    made.create_table("default.a", key_schema=[_FIELDS["k"], _FIELDS["j"]]).write(
        _rows(0)
    )


def _re_record() -> None:
    fake.migrate(
        "p",
        {
            "a": pa.schema(
                [f.with_nullable(True) if f.name == "x" else f for f in _schema()]
            )
        },
    )


_REFUSED: dict[str, Callable[[], Any]] = {
    "withdrawn": lambda: _damage(rows_digest="withdrawn"),
    "incomplete": _incomplete,
    "damaged": lambda: _damage(table_count=100),
    "another dataset's": lambda: _damage(dataset="other"),
}
_CHANGES: dict[str, Callable[[], Any]] = {
    **_REFUSED,
    "nothing": lambda: None,
    "dropped": lambda: fake.ExternalProject("p").drop_table("default.a"),
    "recreated": _recreate,
    "re-recorded": _re_record,
    "schema table rewritten": lambda: fake.migrate("p", {}),
    "schema table dropped": lambda: fake.ExternalProject("p").drop_table(
        f"default.{st.SCHEMA_TABLE}"
    ),
    "new rows": lambda: _write("a", 60),
    "new column": lambda: (
        fake.ExternalProject("p")
        .table("default.a")
        .write(_rows(60).append_column("w", pa.array([1] * 4)))
    ),
}


async def _async_rows(source: SpiralDBTableSource) -> list[str]:
    rows = [repr((t.as_dict(), d.as_dict())) async for t, d in source.async_iter_data()]
    return sorted(rows)


_READS: dict[str, Callable[[SpiralDBTableSource], Any]] = {
    "table": lambda s: s.as_table(all_info=True).to_pylist(),
    "iter": lambda s: [(t.as_dict(), d.as_dict()) for t, d in s.iter_data()],
    "async": lambda s: asyncio.run(_async_rows(s)),
    "head": lambda s: (s.head(2).table.to_pylist(), s.head(2).total_rows),
    "preview": lambda s: s._repr_html_(),
    "run": lambda s: s.run().table.to_pylist(),
    "count": lambda s: s.row_count_estimate,
    "check": lambda s: s.check_for_new_data(),
}
_KINDS: dict[str, Callable[[SpiralDBTableSource], SpiralDBTableSource]] = {
    "plain": lambda s: s,
    "where": lambda s: s.where(j="j0"),
    "where twice": lambda s: s.where(j="j0").where(k=2),
}
_OPTIONS: dict[str, dict[str, Any]] = {
    "none": {},
    "limit": {"limit": 3},
    "data-tags": {"tag_only_columns": ()},
    "no cap": {"max_rows": None, "batch_rows": 1},
    "cap": {"max_rows": 2},
}


def _outcome(call: Callable[[], Any]) -> tuple[str, list[str]]:
    """What a call gives or raises, notes included, and its warnings."""
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        connector_module._warned_unrecorded.clear()  # pyright: ignore[reportPrivateUsage]  # warned once per process
        try:
            result = repr(call())
        except Exception as exc:
            notes = getattr(exc, "__notes__", [])
            result = f"{type(exc).__name__}: {exc} {notes}"
    return result, [f"{w.category.__name__}: {w.message} {w.filename}" for w in seen]


def _pair(
    kind: str = "plain", options: dict[str, Any] | None = None
) -> tuple[SpiralDBTableSource, SpiralDBTableSource]:
    """A source given a connector that others use too, and one that makes its own."""
    connector = SpiralDBConnector("p")
    for other in ("b", "c"):
        _ = _source(other, connector=connector).as_table()
    given = _source(connector=connector, **(options or {}))
    connector.close()
    return _KINDS[kind](given), _KINDS[kind](_source(**(options or {})))


def _same(
    given: SpiralDBTableSource,
    own: SpiralDBTableSource,
    read: Callable[[SpiralDBTableSource], Any],
    calls: _Calls,
) -> tuple[str, list[str]]:
    """Read both, require equal outcomes and equal SDK calls, and return the outcome."""
    calls.take()
    expected = _outcome(lambda: read(own))
    expected_calls = calls.take()
    assert _outcome(lambda: read(given)) == expected
    assert calls.take() == expected_calls
    return expected


# ----------------------------------------------------------------------- the differential


@pytest.mark.parametrize("kind", sorted(_KINDS))
@pytest.mark.parametrize("read", sorted(_READS))
@pytest.mark.parametrize("change", sorted(_CHANGES))
def test_a_table_changed_between_reads(
    calls: _Calls, change: str, read: str, kind: str
) -> None:
    """Equal outcomes and equal calls in equal order: before, after, after a check."""
    given, own = _pair(kind)
    for _ in range(2):
        before = _same(given, own, _READS[read], calls)
        assert "Error" not in before[0] and "could not read" not in before[0]
    _CHANGES[change]()
    changed = _same(given, own, _READS[read], calls)
    if change in _REFUSED:
        assert (
            "TableSchemaError" in changed[0]
            or "completion marker" in changed[0]
            or ("was written for dataset" in changed[0])
        )
    _ = _same(given, own, _READS["check"], calls)
    _ = _same(given, own, _READS[read], calls)
    _ = _same(given, own, lambda s: s.where(k=1).as_table().to_pylist(), calls)


@pytest.mark.parametrize("options", sorted(_OPTIONS))
@pytest.mark.parametrize("read", sorted(_READS))
@pytest.mark.parametrize("change", ["nothing", "new rows", "withdrawn"])
def test_the_options(calls: _Calls, change: str, read: str, options: str) -> None:
    given, own = _pair("plain", _OPTIONS[options])
    _ = _same(given, own, _READS[read], calls)
    _CHANGES[change]()
    for step in (read, "check", read):
        _ = _same(given, own, _READS[step], calls)


@pytest.mark.parametrize("read", sorted(_READS))
def test_an_older_version(calls: _Calls, read: str) -> None:
    """Finding 554-2 too: a moved table is opened by id as often as without."""
    asof = SpiralDBConnector("p").get_table_version("a")
    _write("a", 60)
    given, own = _pair("plain", {"asof": asof})
    for change in ("nothing", "new rows", "withdrawn"):
        _CHANGES[change]()
        _ = _same(given, own, _READS[read], calls)


_FAILURES = ("open", "schema", "open schemas", "plan", "first batch", "later batch")


def _fail(monkeypatch: pytest.MonkeyPatch, at: str, table_name: str) -> None:
    """Let one kind of call on one table fail, whatever client makes it."""
    opened, schema = fake.FakeClient.keyed_table, fake.FakeKeyedTable.schema
    scan = fake.FakeKeyedTable.scan

    def new_opened(self: Any, p: str, d: str, name: str, **given: Any) -> Any:
        if (at == "open" and name == table_name) or (
            at == "open schemas" and name == st.SCHEMA_TABLE
        ):
            raise fake.SpiralError("status 401 expired")
        return opened(self, p, d, name, **given)

    def new_schema(self: Any) -> Any:
        if at == "schema" and self._table["name"] == table_name:
            raise fake.SpiralError("status 401 expired")
        return schema(self)

    def new_scan(self: Any, *args: Any, **given: Any) -> Any:
        if self._table["name"] != table_name or at not in _FAILURES[3:]:
            return scan(self, *args, **given)
        if at == "plan":
            raise fake.SpiralError("status 401 expired")
        found = scan(self, *args, **given).to_table()

        def batches() -> Iterator[Any]:
            if at == "later batch":
                yield found.slice(0, 1).to_batches()[0]
            raise fake.SpiralError("status 401 expired")

        reader = batches()
        return types.SimpleNamespace(to_reader=lambda: reader)

    monkeypatch.setattr(fake.FakeClient, "keyed_table", new_opened)
    monkeypatch.setattr(fake.FakeKeyedTable, "schema", new_schema)
    monkeypatch.setattr(fake.FakeKeyedTable, "scan", new_scan)


@pytest.mark.parametrize("withdrawn", [False, True])
@pytest.mark.parametrize("read", sorted(_READS))
@pytest.mark.parametrize("at", _FAILURES)
def test_a_failing_call(
    calls: _Calls, monkeypatch: pytest.MonkeyPatch, at: str, read: str, withdrawn: bool
) -> None:
    """Findings 553-4, 554-1 and 554-4: the same error, first, after the same calls; also
    where the schema table would be refused later. An expired sign-in fails like this."""
    given, own = _pair()
    if withdrawn:
        _damage(rows_digest="withdrawn")
    with monkeypatch.context() as failing:
        _fail(failing, at, "a")
        outcome = _same(given, own, _READS[read], calls)
    if at in ("open", "schema"):
        assert "expired" in outcome[0]
        assert "withdrawn" not in outcome[0] and "marker" not in outcome[0]
    if not withdrawn:
        # The client recovers, as one that is not shared does.
        recovered = _same(given, own, _READS[read], calls)
        assert "expired" not in recovered[0]


def test_rows_given_before_a_failing_batch_are_the_same(
    calls: _Calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Finding 554-4: every read uses a handle opened for it."""
    given, own = _pair("plain", {"max_rows": None})
    _fail(monkeypatch, "later batch", "a")

    def until_it_fails(source: SpiralDBTableSource) -> Any:
        got: list[Any] = []
        try:
            got.extend(t.as_dict() for t, _ in source.iter_data())
        except fake.SpiralError as exc:
            got.append((str(exc), exc.__notes__))
        return got

    result, _ = _same(given, own, until_it_fails, calls)
    assert "expired" in result


@pytest.mark.parametrize("during", ["open", "schema"])
@pytest.mark.parametrize("kind", sorted(_REFUSED))
def test_a_schema_table_changed_during_the_call_before_it_is_refused_by_that_read(
    root: Path, monkeypatch: pytest.MonkeyPatch, kind: str, during: str
) -> None:
    """Finding 553-1: the schema table is looked at after the table's schema."""
    given, _ = _pair()
    changed: list[bool] = []

    def change() -> None:
        if not changed:
            changed.append(True)
            _REFUSED[kind]()

    opened, schema = fake.FakeClient.keyed_table, fake.FakeKeyedTable.schema

    def new_opened(self: Any, p: str, d: str, name: str, **options: Any) -> Any:
        table = opened(self, p, d, name, **options)
        if name == "a" and during == "open":
            change()
        return table

    def new_schema(self: Any) -> Any:
        found = schema(self)
        if self._table["name"] == "a" and during == "schema":
            change()
        return found

    monkeypatch.setattr(fake.FakeClient, "keyed_table", new_opened)
    monkeypatch.setattr(fake.FakeKeyedTable, "schema", new_schema)
    with pytest.raises(TableSchemaError):
        given.as_table()
    assert changed


def test_a_read_that_races_a_check_reads_its_own_version(
    calls: _Calls,
) -> None:
    """Finding 554-2: one open by id, and the rows of the version the read began with."""
    entered, resume = threading.Event(), threading.Event()
    reader: list[int] = []

    class Gated(SpiralDBTableSource):
        """Waits between taking its version and looking its table up."""

        def _lookup_table(self) -> Any:
            if threading.get_ident() in reader:
                entered.set()
                assert resume.wait(10)
            return super()._lookup_table()

    source = Gated(
        "p", "a", tag_only_columns=True, max_rows=None, connector=SpiralDBConnector("p")
    )
    _write("a", 60)

    def read() -> list[Any]:
        reader.append(threading.get_ident())
        return _ks(source)

    with ThreadPoolExecutor(1) as pool:
        pending = pool.submit(read)
        assert entered.wait(10)
        assert source.check_for_new_data()
        calls.take()
        resume.set()
        assert sorted(pending.result(10)) == [0, 1, 2, 3]
    assert calls.take().count("open by id") == 1
    assert 60 in _ks(source)


# -------------------------------------------------------------------- who reads, and where


def test_the_identity_is_that_of_a_source_that_makes_its_own(root: Path) -> None:
    """Order, ids, hashes, schemas, config: nothing holds the connector."""
    connector = SpiralDBConnector("p")
    for options in _OPTIONS.values():
        for name in _TABLES:
            given, own = (
                _source(name, connector=connector, **options),
                _source(name, **options),
            )
            assert given.source_id == own.source_id
            assert given.content_hash() == own.content_hash()
            assert given.pipeline_hash() == own.pipeline_hash()
            assert given.output_schema(all_info=True) == own.output_schema(
                all_info=True
            )
            assert given.keys() == own.keys()
            assert given.to_config() == own.to_config()


@pytest.mark.parametrize("order", [("a", "b", "c"), ("c", "a", "b")])
def test_the_order_sources_are_made_in_changes_nothing(
    calls: _Calls, order: tuple[str, ...]
) -> None:
    connector = SpiralDBConnector("p")
    made = {name: _source(name, connector=connector) for name in order}
    log = calls.take()
    assert collections.Counter(log) == {
        "client": 1,
        "open": 3,
        "schema": 3,
        "open schemas": 3,  # looked at for each source
        "schema schemas": 1,
        "scan schemas": 1,
    }
    for index, name in enumerate(_TABLES):
        assert sorted(_ks(made[name])) == [10 * index + i for i in range(4)]


def test_each_read_looks_at_everything_again(calls: _Calls) -> None:
    given, _ = _pair("plain", {"max_rows": None})
    for _ in range(2):
        calls.take()
        _ = given.as_table()
        assert calls.take() == ["open", "schema", "open schemas", "scan"]


def test_a_new_version_of_the_schema_table_is_read_once(calls: _Calls) -> None:
    connector = SpiralDBConnector("p")
    sources = [_source(n, connector=connector, max_rows=None) for n in _TABLES]
    fake.migrate("p", {})
    calls.take()
    for _ in range(2):
        for source in sources:
            _ = source.as_table()
    counts = collections.Counter(calls.take())
    assert counts["open schemas"] == 6 and counts["scan schemas"] == 1


def test_the_one_call_that_is_not_made_again(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The rows of a schema table's version are read once for the sources given one
    connector, so that a failure of reading them again cannot be seen by the second. Its
    current version is looked at all the same."""
    connector = SpiralDBConnector("p")
    _ = _source("b", connector=connector)
    scan = fake.FakeKeyedTable.scan

    def new_scan(self: Any, *args: Any, **given: Any) -> Any:
        if self._table["name"] == st.SCHEMA_TABLE:
            raise fake.SpiralError("status 500")
        return scan(self, *args, **given)

    monkeypatch.setattr(fake.FakeKeyedTable, "scan", new_scan)
    with pytest.raises(fake.SpiralError, match="500"):
        _source("a")
    assert len(_ks(_source("a", connector=connector))) == 4


def test_a_count_is_each_sources_own(calls: _Calls) -> None:
    """What one counted is not taken as counted by another, as without a connector."""
    connector = SpiralDBConnector("p")
    first, second = (_source(connector=connector) for _ in range(2))
    _ = first.row_count_estimate
    calls.take()
    _ = second.row_count_estimate
    assert calls.take() == ["open", "schema", "open schemas", "scan"]
    _ = second.where(j="j0").row_count_estimate
    assert calls.take() == ["open", "schema", "open schemas"]


def test_closing_one_breaks_no_other_and_the_last_releases_the_client(
    calls: _Calls,
) -> None:
    connector = SpiralDBConnector("p")
    first, second = _source("a", connector=connector), _source("b", connector=connector)
    copy = first.where(j="j0")
    rows = second.iter_data()
    _ = next(rows)
    connector.close()
    first.close()
    assert len(list(rows)) == 3
    assert sorted(_ks(copy)) == [0, 2] and len(_ks(second)) == 4
    with pytest.raises(RuntimeError, match="closed"):
        first.as_table()
    with pytest.raises(RuntimeError, match="closed"):
        _source("c", connector=connector)
    copy.close()
    second.close()
    gc.collect()
    assert [c() for c in calls.clients] == [None]


def test_a_source_leaves_the_connector_open(root: Path) -> None:
    connector = SpiralDBConnector("p")
    _source(connector=connector).close()
    with pytest.raises(ValueError, match="not found"):
        _source("absent", connector=connector)
    with pytest.raises(ValueError, match="no_such"):
        _source(connector=connector, where={"no_such": 1})
    assert connector.get_table_names() == list(_TABLES)


def test_a_connector_of_another_place_is_refused(calls: _Calls) -> None:
    connector = SpiralDBConnector("p")
    calls.take()
    for project, dataset in (("q", "default"), ("p", "other")):
        with pytest.raises(
            ValueError, match=f"project '{project}', dataset '{dataset}'"
        ):
            SpiralDBTableSource(project, "a", dataset=dataset, connector=connector)
    with pytest.raises(ValueError, match="overrides"):
        _source(connector=connector, overrides={"server.url": "https://one.example"})
    not_one: Any = object()
    with pytest.raises(TypeError, match="SpiralDBConnector"):
        _source(connector=not_one)
    assert calls.take() == []


def test_the_service_address_is_the_connectors(calls: _Calls) -> None:
    url = {"server.url": "https://one.example"}
    connector = SpiralDBConnector("p", overrides=url)
    for overrides in (None, dict(url)):
        source = _source(connector=connector, overrides=overrides)
        assert source.to_config()["overrides"] == url
    with pytest.raises(ValueError, match="overrides"):
        _source(connector=connector, overrides={})


def test_a_dataset_and_its_connector(root: Path) -> None:
    sources = [
        SpiralDBTableSource(p, "a", dataset=d, connector=SpiralDBConnector(p, d))
        for p, d in (("p", "default"), ("p", "other"), ("q", "default"))
    ]
    _damage(rows_digest="withdrawn")
    with pytest.raises(TableSchemaError):
        sources[0].as_table()
    assert [len(_ks(s)) for s in sources[1:]] == [4, 4]


def test_a_saved_source_makes_its_own_connector(calls: _Calls) -> None:
    given, _ = _pair("where")
    calls.take()
    loaded = SpiralDBTableSource.from_config(given.to_config())
    assert calls.take().count("client") == 1
    assert loaded.content_hash() == given.content_hash() and _ks(loaded) == _ks(given)


def test_a_warning_names_the_callers_line(root: Path) -> None:
    fake.ExternalProject("p").create_table(
        "default.raw", key_schema=[_FIELDS["k"]]
    ).write(_rows(0).drop_columns(["j"]))
    connector = SpiralDBConnector("p")
    with pytest.warns(UnrecordedTableSchemaWarning, match="raw has no row") as seen:
        _source("raw", connector=connector)
    assert [w.filename for w in seen] == [__file__]
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _source("raw", connector=connector)  # once per process, as before


# ------------------------------------------------------------------------- two accounts


def _accounts(monkeypatch: pytest.MonkeyPatch) -> None:
    init = fake.FakeClient.__init__

    def bound(self: Any, **given: Any) -> None:
        init(self, **given)
        self.account = os.environ["SPIRAL_TOKEN"]

    monkeypatch.setattr(fake.FakeClient, "__init__", bound)


def test_an_account_is_never_guessed(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Finding 553-2: the client is the one given; without one, each source's own."""
    _accounts(monkeypatch)
    monkeypatch.setenv("SPIRAL_TOKEN", "one")
    first = SpiralDBConnector("p")
    monkeypatch.setenv("SPIRAL_TOKEN", "two")
    second = SpiralDBConnector("p")
    monkeypatch.setenv("SPIRAL_TOKEN", "one")  # and back
    sources = [
        _source(),
        _source(connector=second),
        _source(connector=first),
        _source(connector=second).where(j="j0"),
        _source(),
    ]
    accounts = [s._connector()._client.account for s in sources]  # pyright: ignore[reportPrivateUsage]  # who each signed in as
    assert accounts == ["one", "two", "one", "two", "one"]
    assert len({id(s._connector()._client) for s in sources}) == 4  # pyright: ignore[reportPrivateUsage]


@pytest.mark.parametrize(
    "denied", ["open", "schema", "scan", "open schemas", "scan schemas"]
)
def test_an_account_reads_nothing_through_another(
    root: Path, monkeypatch: pytest.MonkeyPatch, denied: str
) -> None:
    """Finding 554-3: whatever one account may not call, it is refused, the other not,
    also after the other read the same version of everything."""
    _accounts(monkeypatch)
    monkeypatch.setenv("SPIRAL_TOKEN", "allowed")
    allowed = SpiralDBConnector("p")
    monkeypatch.setenv("SPIRAL_TOKEN", "denied")
    refused = SpiralDBConnector("p")
    reading = _source(connector=allowed)
    assert len(_ks(reading)) == 4
    opened, schema = fake.FakeClient.keyed_table, fake.FakeKeyedTable.schema
    scan = fake.FakeKeyedTable.scan
    of_denied: list[Any] = []

    def check(table: Any, what: str) -> None:
        name = f"{what} schemas" if table._table["name"] == st.SCHEMA_TABLE else what
        if name == denied and any(table is t for t in of_denied):
            raise fake.SpiralError("status 403")

    def new_opened(self: Any, p: str, d: str, name: str, **given: Any) -> Any:
        table = opened(self, p, d, name, **given)
        if self.account == "denied":
            of_denied.append(table)
            check(table, "open")
        return table

    def new_schema(self: Any) -> Any:
        check(self, "schema")
        return schema(self)

    def new_scan(self: Any, *args: Any, **given: Any) -> Any:
        check(self, "scan")
        return scan(self, *args, **given)

    monkeypatch.setattr(fake.FakeClient, "keyed_table", new_opened)
    monkeypatch.setattr(fake.FakeKeyedTable, "schema", new_schema)
    monkeypatch.setattr(fake.FakeKeyedTable, "scan", new_scan)
    with pytest.raises(fake.SpiralError, match="403"):
        _source(connector=refused).as_table()
    assert len(_ks(reading)) == 4
    assert len(_ks(_source(connector=allowed))) == 4


# --------------------------------------------------------------- forks, threads, asyncio


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is POSIX")
@pytest.mark.parametrize("held", [False, True], ids=["lock free", "lock held"])
def test_after_a_fork_it_is_refused_and_never_waited_for(
    root: Path, held: bool
) -> None:
    connector = SpiralDBConnector("p")
    source = _source(connector=connector)
    taken, release = threading.Event(), threading.Event()

    def hold() -> None:
        # Both locks a read could meet.
        with source._connection_lock, connector._schemas._lock:  # pyright: ignore[reportPrivateUsage]  # the inherited locks are the subject
            taken.set()
            _ = release.wait(60)

    holder = threading.Thread(target=hold)
    if held:
        holder.start()
        assert taken.wait(10)
    uses: list[Callable[[], Any]] = [
        lambda: _source("b", connector=connector),
        source.as_table,
        source.head,
        source.check_for_new_data,
        lambda: source.where(j="j0"),
        source.close,
    ]
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # Python warns of a fork with threads
            pid = os.fork()
        if pid == 0:  # the child: never returns to pytest
            code = 1
            try:
                import signal

                signal.alarm(20)
                for use in uses:
                    with pytest.raises(RuntimeError, match="after a fork"):
                        use()
                code = 0
            finally:
                os._exit(code)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
    finally:
        release.set()
        if held:
            holder.join(10)
    assert len(_ks(source)) == 4


def test_threads_make_and_read_sources_of_one_connector(root: Path) -> None:
    connector = SpiralDBConnector("p")
    ready = threading.Barrier(12)

    def use(index: int) -> list[Any]:
        _ = ready.wait(30)
        source = _source(_TABLES[index % 3], connector=connector)
        return sorted(_ks(source.where(j="j0") if index % 2 else source))

    with ThreadPoolExecutor(12) as pool:
        found = list(pool.map(use, range(12)))
    assert found == [
        [10 * (i % 3) + j for j in range(4) if not (i % 2 and j % 2)] for i in range(12)
    ]


def test_threads_that_meet_a_new_version_all_refuse_or_all_read(root: Path) -> None:
    connector = SpiralDBConnector("p")
    sources = [_source(n, connector=connector) for n in _TABLES] * 4
    _damage(rows_digest="withdrawn")
    with ThreadPoolExecutor(12) as pool:
        refused = list(pool.map(lambda s: _outcome_kind(s), sources))
    assert set(refused) == {"TableSchemaError"}
    fake.migrate("p", {})
    with ThreadPoolExecutor(12) as pool:
        assert set(pool.map(_outcome_kind, sources)) == {"4"}


def _outcome_kind(source: SpiralDBTableSource) -> str:
    try:
        return str(source.as_table().num_rows)
    except Exception as exc:
        return type(exc).__name__


def test_asyncio_reads_of_one_connector(root: Path) -> None:
    connector = SpiralDBConnector("p")
    sources = [_source(n, connector=connector) for n in _TABLES]

    async def all_at_once() -> list[list[str]]:
        return await asyncio.gather(*(_async_rows(s) for s in sources * 2))

    found = asyncio.run(all_at_once())
    expected = [asyncio.run(_async_rows(_source(n))) for n in _TABLES]
    assert found == expected * 2


def test_a_cancelled_async_read_leaves_the_others(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connector = SpiralDBConnector("p")
    active, peer = _source("a", connector=connector), _source("b", connector=connector)
    entered, release = threading.Event(), threading.Event()
    schema = fake.FakeKeyedTable.schema

    def slow(self: Any) -> Any:
        if self._table["name"] == "a":
            entered.set()
            assert release.wait(10)
        return schema(self)

    monkeypatch.setattr(fake.FakeKeyedTable, "schema", slow)

    async def run() -> int:
        rows = active.async_iter_data()
        first = asyncio.ensure_future(anext(rows))
        try:
            assert await asyncio.to_thread(entered.wait, 10)
            _ = first.cancel()
            with pytest.raises(asyncio.CancelledError):
                _ = await first
            return len(await _async_rows(peer))
        finally:
            release.set()

    assert asyncio.run(run()) == 4
    assert len(_ks(active)) == 4


# --------------------------------------------------------------------- checks and jobs


def test_checks_look_at_one_source_after_another(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Finding 553-3: a row written during one check is in the next; in this thread."""
    connector = SpiralDBConnector("p")
    here = threading.get_ident()

    class ThreadBound(SpiralDBTableSource):
        """Its lookup works in one thread only (finding 553-5)."""

        def _lookup_table(self) -> Any:
            assert threading.get_ident() == here
            return super()._lookup_table()

    a = ThreadBound("p", "a", tag_only_columns=True, connector=connector)
    b = ThreadBound("p", "b", tag_only_columns=True, connector=connector)
    schema = fake.FakeKeyedTable.schema
    wrote: list[bool] = []

    def new_schema(self: Any) -> Any:
        if self._table["name"] == "a" and not wrote:
            wrote.append(True)
            _write("b", 99)
        return schema(self)

    monkeypatch.setattr(fake.FakeKeyedTable, "schema", new_schema)
    assert node_run.check_sources([a, b]) == ()
    assert wrote and 99 in _ks(b)


def test_failing_checks_warn_in_the_sources_order(root: Path) -> None:
    connector = SpiralDBConnector("p")
    sources = [_source(n, connector=connector) for n in _TABLES]
    for name in ("a", "c"):
        fake.ExternalProject("p").drop_table(f"default.{name}")
    _write("b", 77)
    with pytest.warns(SourceCheckFailedWarning) as seen:
        assert len(node_run.check_sources(sources)) == 2
    assert ["default.a" in str(w.message) for w in seen] == [True, False]
    assert [w.filename for w in seen] == [__file__] * 2
    assert 77 in _ks(sources[1])


def test_a_connector_subclass_keeps_its_own_methods(root: Path) -> None:
    """#554, other findings: an override of today's signature is called as today."""
    looked_up: list[str] = []

    class Counting(SpiralDBConnector):
        """Counts its lookups."""

        def lookup_table(self, table_name: str) -> Any:
            """Count and look up."""
            looked_up.append(table_name)
            return super().lookup_table(table_name)

    source = _source(connector=Counting("p"))
    _ = source.as_table()
    _ = source.check_for_new_data()
    assert looked_up == ["a"] * 3


def test_a_preview_whose_count_fails_leaves_nothing_behind(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#554, other findings: nothing is kept from a read."""
    given, _ = _pair()
    scan = fake.FakeKeyedTable.scan

    def new_scan(self: Any, *args: Any, **options: Any) -> Any:
        if self._table["name"] == "a" and args:
            raise fake.SpiralError("status 500")
        return scan(self, *args, **options)

    with monkeypatch.context() as failing:
        failing.setattr(fake.FakeKeyedTable, "scan", new_scan)
        with pytest.raises(fake.SpiralError, match="500"):
            given.head(1)
    assert given.head(1).total_rows == 4


def test_a_null_in_a_non_nullable_column_is_refused_alike(calls: _Calls) -> None:
    given, own = _pair("plain", {"max_rows": None})
    bad = _rows(0).to_pylist()
    bad[0]["x"] = None
    fake.ExternalProject("p").table("default.a").write(
        pa.Table.from_pylist(
            bad, schema=pa.schema([f.with_nullable(True) for f in _schema()])
        )
    )
    for source in (given, own):
        assert source.check_for_new_data()
    outcome = _same(given, own, _READS["table"], calls)
    assert NullInNonNullableColumnError.__name__ in outcome[0]


@pytest.mark.parametrize("orchestrator", [None, AsyncPipelineOrchestrator])
def test_a_job_is_the_same(
    calls: _Calls, orchestrator: type[AsyncPipelineOrchestrator] | None
) -> None:
    connector = SpiralDBConnector("p")

    def run(sources: list[SpiralDBTableSource]) -> list[Any]:
        job = PipelineJob(name="given", database=InMemoryArrowDatabase())
        with job:
            for index, source in enumerate(sources):
                SelectColumns(["k", "j", "x"])(source, label=f"n{index}")
        facts: list[Any] = []
        for _ in range(2):
            calls.take()
            job.run(orchestrator=orchestrator() if orchestrator else None)
            facts.append(collections.Counter(calls.take()))
            for index in range(len(sources)):
                node = job.nodes[f"n{index}"]
                facts.append(node.content_hash().to_string())
                facts.append(node.pipeline_hash().to_string())
                facts.append(
                    sorted(map(repr, node.as_table(all_info=True).to_pylist()))
                )
        return facts

    given = run([_source(n, connector=connector).where(j="j0") for n in _TABLES])
    own = run([_source(n).where(j="j0") for n in _TABLES])
    assert given == own
