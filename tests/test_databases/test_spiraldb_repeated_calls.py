"""What SpiralDB sources share, and what they may never share (ITL-1457), on the fake.

One client per service address and sign-in, one state of a dataset's schema table per
client, and the checks before a run made at the same time. The results, refusals and
warnings are those of sources that share nothing.
"""

from __future__ import annotations

import collections
import gc
import os
import threading
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
    SourceCheckFailedWarning,
    TableSchemaError,
    UnrecordedTableSchemaWarning,
)
from orcapod.operators import SelectColumns
from orcapod.pipeline import node_run
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import DictSource, SpiralDBTableSource
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


# ---------------------------------------------------------------------- the one client


def test_one_client_and_one_read_of_a_schema_table_serve_every_source(
    calls: _Calls,
) -> None:
    sources = [
        _source(n, p, d) for p in "pq" for d in ("default", "other") for n in "abc"
    ]
    assert calls.take() == {
        "client": 1,
        "open": 12,
        "schema": 12,
        "open schemas": 12,  # checked at each lookup
        "read schemas": 4,  # read once per dataset
    }
    assert [_ks(s) for s in sources] == [
        [10 * i + j for j in range(4)] for _ in range(4) for i in range(3)
    ]


def test_sharing_changes_no_result(root: Path) -> None:
    """Sources that share everything and sources that share nothing are equal."""

    def facts(source: SpiralDBTableSource) -> list[Any]:
        return [
            source.source_id,
            source.content_hash().to_string(),
            source.pipeline_hash().to_string(),
            source.output_schema(),
            source.as_table(all_info=True),
            [t.as_dict() for t, _ in source.iter_data()],
            source.head(2).table,
            source.row_count_estimate,
        ]

    shared = [_source(n, d) for n, d in (("a", "p"), ("b", "p"), ("a", "q"))]
    apart = []
    for name, project in (("a", "p"), ("b", "p"), ("a", "q")):
        connector_module._forget_clients()  # pyright: ignore[reportPrivateUsage]  # so nothing is shared
        apart.append(_source(name, project))
    assert [facts(s) for s in shared] == [facts(s) for s in apart]


def test_closing_a_source_breaks_no_other_and_the_last_releases_the_client(
    calls: _Calls,
) -> None:
    first, second = _source("a"), _source("b")
    copy = first.where(j="j0")
    first.close()
    assert _ks(copy) == [0, 2] and _ks(second) == [10, 11, 12, 13]
    with pytest.raises(RuntimeError, match="closed"):
        first.as_table()
    copy.close()
    assert _ks(second) == [10, 11, 12, 13]
    second.close()
    gc.collect()
    assert [c() for c in calls.clients] == [None]
    assert _ks(_source("a")) == [0, 1, 2, 3]
    assert len(calls.clients) == 2


def test_a_scan_in_progress_survives_the_close_of_another_source(root: Path) -> None:
    first, second = _source("a", batch_rows=1), _source("b")
    rows = first.iter_data()
    next(rows)
    second.close()
    assert len(list(rows)) == 3


def test_a_failure_after_close_is_the_services_error(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A read may go on after ``close``, and its errors are still noted."""
    source = _source("a")
    batches = SpiralDBConnector("p").lookup_table("a")
    connector = batches._connector  # pyright: ignore[reportPrivateUsage]  # closed below
    started = threading.Event()

    def fail(*_args: Any, **_given: Any) -> Any:
        connector.close()
        started.set()
        raise fake.SpiralError("status 500")

    monkeypatch.setattr(fake.FakeKeyedTable, "scan", fail)
    with pytest.raises(fake.SpiralError, match="500") as raised:
        list(batches.scan_batches())
    assert started.is_set()
    assert raised.value.__notes__ == ["SpiralDB: could not read p/default/a"]
    source.close()


@pytest.mark.parametrize(
    "change",
    [
        {"SPIRAL_CLIENT_ID": "work_other"},
        {"SPIRAL_PRIVATE_KEY": "other"},
        {"SPIRAL_TOKEN": "t"},
        {"SPIRAL_WORKLOAD_ID": "w"},
        {"SPIRAL_AUTH_FILE": "elsewhere"},
        {"HOME": "elsewhere"},
        {"ACTIONS_ID_TOKEN_REQUEST_URL": "u"},
        {"HTTPS_PROXY": "http://localhost:1"},
    ],
    ids=lambda c: next(iter(c)),
)
def test_a_changed_environment_is_another_client(
    calls: _Calls, monkeypatch: pytest.MonkeyPatch, change: dict[str, str]
) -> None:
    first = _source("a")
    for name, value in change.items():
        monkeypatch.setenv(name, value)
    second = _source("a")
    assert calls.take()["client"] == 2
    # The second read the schema table with its own sign-in.
    assert _ks(first) == _ks(second)


def test_a_changed_saved_login_is_another_client(calls: _Calls, root: Path) -> None:
    kept = [_source("a")]
    login = root / "auth.json"
    for write in (
        lambda: login.write_text("one"),
        lambda: login.write_text("two"),  # another account, the same size
        lambda: login.replace(root / "away"),
    ):
        write()
        kept.append(_source("a"))
        kept.append(_source("b"))
    # No login, the first, the second; then none again, which is the first client's state.
    assert calls.take()["client"] == 3
    assert {tuple(_ks(s)) for s in kept} == {
        (0, 1, 2, 3),
        (10, 11, 12, 13),
    }


def test_another_address_is_another_client(calls: _Calls) -> None:
    kept = [
        _source("a", overrides=overrides)
        for overrides in (
            None,
            {"server.url": "https://one.example"},
            {"server.url": "https://two.example"},
            {"server.url": "https://one.example"},
            None,
        )
    ]
    assert calls.take()["client"] == 3
    assert {tuple(_ks(s)) for s in kept} == {(0, 1, 2, 3)}


def test_a_sign_in_that_changes_while_connecting_is_not_shared(
    calls: _Calls, root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    init = fake.FakeClient.__init__
    writes = iter("xy")

    def renewing(self: Any, **given: Any) -> None:
        init(self, **given)
        if (text := next(writes, None)) is not None:
            (root / "auth.json").write_text(text * len(calls.clients))

    monkeypatch.setattr(fake.FakeClient, "__init__", renewing)
    kept = [_source("a") for _ in range(4)]
    assert calls.take()["client"] == 3  # two unknown, then one that is shared
    assert {tuple(_ks(s)) for s in kept} == {(0, 1, 2, 3)}


def test_an_unknown_sign_in_is_not_shared(
    calls: _Calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_home(_environ: Any) -> Path:
        raise RuntimeError("no home folder")

    monkeypatch.setattr(connector_module, "saved_login_path", no_home)
    kept = [_source("a"), _source("a")]
    assert calls.take()["client"] == 2
    assert {tuple(_ks(s)) for s in kept} == {(0, 1, 2, 3)}


@pytest.mark.parametrize("failing", ["open", "schema", "scan"])
def test_a_client_whose_call_failed_is_given_to_no_new_source(
    calls: _Calls, monkeypatch: pytest.MonkeyPatch, failing: str
) -> None:
    """An expired or ended sign-in: a new source signs in again, as it always did."""
    source = _source("a")
    cls, name = {
        "open": (fake.FakeClient, "keyed_table"),
        "schema": (fake.FakeKeyedTable, "schema"),
        "scan": (fake.FakeKeyedTable, "scan"),
    }[failing]
    with monkeypatch.context() as broken:

        def denied(*_args: Any, **_given: Any) -> Any:
            raise fake.SpiralError("status 401")

        broken.setattr(cls, name, denied)
        with pytest.raises(fake.SpiralError, match="401"):
            source.as_table()
    again = _source("b")
    assert calls.take()["client"] == 2
    _ = _source("c")
    assert "client" not in calls.take()
    # What was built keeps its own client, and recovers with it.
    assert _ks(source) == [0, 1, 2, 3] and _ks(again) == [10, 11, 12, 13]


def test_a_missing_table_is_no_failure_of_the_client(calls: _Calls) -> None:
    kept = _source("a")
    with pytest.raises(ValueError, match="not found"):
        _source("absent")
    _ = _source("b")
    assert calls.take()["client"] == 1
    kept.close()


def test_a_failed_connection_is_raised_by_each_source(
    calls: _Calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(self: Any, **_given: Any) -> None:
        raise fake.SpiralError("sign-in refused")

    with monkeypatch.context() as broken:
        broken.setattr(fake.FakeClient, "__init__", refuse)
        for _ in range(2):
            with pytest.raises(fake.SpiralError, match="refused"):
                _source("a")
    assert _ks(_source("a")) == [0, 1, 2, 3]


def test_many_threads_on_first_use_end_with_one_client(calls: _Calls) -> None:
    ready = threading.Barrier(16)

    def build(index: int) -> tuple[SpiralDBTableSource, list[Any]]:
        ready.wait(30)
        source = _source("abc"[index % 3])
        return source, _ks(source)

    with ThreadPoolExecutor(16) as pool:
        built = list(pool.map(build, range(16)))
    assert [ks for _, ks in built] == [
        [10 * (i % 3) + j for j in range(4)] for i in range(16)
    ]
    used = {id(s._connection._client) for s, _ in built if s._connection}  # pyright: ignore[reportPrivateUsage]  # which client each holds
    assert len(used) == 1
    calls.take()
    _ = _source("a")
    assert "client" not in calls.take()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is POSIX")
def test_a_forked_child_makes_its_own_client_and_never_waits(
    root: Path,
) -> None:
    """The registry's lock may be held by a thread that the child does not have."""
    parent = _source("a")
    taken, release = threading.Event(), threading.Event()
    lock = connector_module._clients_lock  # pyright: ignore[reportPrivateUsage]  # the inherited lock is the subject

    def hold() -> None:
        with lock:
            taken.set()
            release.wait(30)

    holder = threading.Thread(target=hold)
    holder.start()
    assert taken.wait(10)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # Python warns of a fork with threads
            pid = os.fork()
        if pid == 0:  # the child: never returns to pytest
            code = 3
            try:
                import signal

                signal.alarm(20)
                with pytest.raises(RuntimeError, match="after a fork"):
                    parent.as_table()
                child = SpiralDBConnector("p")
                same = child._client is parent._connection._client  # pyright: ignore[reportPrivateUsage, reportOptionalMemberAccess]  # must differ
                code = int(same)
            finally:
                os._exit(code)
        _, status = os.waitpid(pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0
    finally:
        release.set()
        holder.join(10)
    assert _ks(parent) == [0, 1, 2, 3]


# ------------------------------------------------------------ the schema table's state


def _damage(project: str = "p", dataset: str = "default", **change: Any) -> None:
    table = fake.FakeClient().keyed_table(project, dataset, st.SCHEMA_TABLE)
    (marker,) = [
        r for r in table.scan().to_table().to_pylist() if r["table_id"] == st.MARKER_KEY
    ]
    table.append(pa.Table.from_pylist([{**marker, **change}], schema=st.ROW_SCHEMA))


def test_every_source_refuses_a_schema_table_withdrawn_since(root: Path) -> None:
    """At every read that opens a table, as without sharing; another dataset is read."""
    sources = [_source(n) for n in _TABLES]
    other = [_source("a", "q"), _source("a", "p", "other")]
    asof = _source("a", asof=sources[0]._asof)  # pyright: ignore[reportPrivateUsage]  # its version
    copy = sources[0].where(j="j0")
    marker = {"rows_digest": "withdrawn"}
    kept = fake.FakeClient().keyed_table("p", "default", st.SCHEMA_TABLE)
    kept = kept.scan().to_table()
    _damage(**marker)
    for source in (*sources, asof, copy):
        for read in (
            source.as_table,
            lambda source=source: list(source.iter_data()),
            source.head,
            lambda source=source: source.row_count_estimate,
        ):
            with pytest.raises(TableSchemaError, match="withdrawn"):
                read()
    assert [_ks(s) for s in other] == [[0, 1, 2, 3]] * 2
    with pytest.raises(TableSchemaError):
        _source("a")
    # Restored: every one of them reads again.
    fake.FakeClient().keyed_table("p", "default", st.SCHEMA_TABLE).append(kept)
    assert [_ks(s) for s in sources] == [
        [0, 1, 2, 3],
        [10, 11, 12, 13],
        [20, 21, 22, 23],
    ]
    assert _ks(copy) == [0, 2]


def test_a_changed_schema_table_is_read_again_once(calls: _Calls) -> None:
    sources = [_source(n) for n in _TABLES]
    calls.take()
    made = fake.ExternalProject("p")
    made.create_table("default.d", key_schema=[_SCHEMA.field("k")]).write(
        _rows(0).drop_columns(["j"])
    )
    fake.migrate("p", {})
    calls.take()
    for _ in range(2):
        for source in sources:
            assert len(_ks(source)) == 4
    counts = calls.take()
    assert counts["read schemas"] == 1 and counts["open schemas"] == 6


def test_the_schema_table_is_seen_after_the_open_and_before_any_scan(
    calls: _Calls,
) -> None:
    source = _source("a", max_rows=None)
    for read in (source.as_table, source.head, lambda: source.row_count_estimate):
        calls.take()
        read()
        log = calls.log
        assert log[0] == "open"
        assert set(log[1:3]) == {"schema", "open schemas"}
        assert set(log[3:]) <= {"scan"}


def test_the_schema_table_and_the_tables_schema_are_fetched_at_the_same_time(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    both = threading.Barrier(2)
    opened, schema = fake.FakeClient.keyed_table, fake.FakeKeyedTable.schema

    def new_opened(self: Any, project: str, dataset: str, name: str, **given: Any):
        if name == st.SCHEMA_TABLE:
            both.wait(10)
        return opened(self, project, dataset, name, **given)

    def new_schema(self: Any):
        if self._table["name"] != st.SCHEMA_TABLE:
            both.wait(10)
        return schema(self)

    monkeypatch.setattr(fake.FakeClient, "keyed_table", new_opened)
    monkeypatch.setattr(fake.FakeKeyedTable, "schema", new_schema)
    assert _ks(_source("a")) == [0, 1, 2, 3]


def test_the_tables_own_error_comes_before_the_schema_tables(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source("a")
    _damage(table_count=9)
    schema = fake.FakeKeyedTable.schema

    def new_schema(self: Any):
        if self._table["name"] != st.SCHEMA_TABLE:
            raise fake.SpiralError("status 503")
        return schema(self)

    with monkeypatch.context() as broken:
        broken.setattr(fake.FakeKeyedTable, "schema", new_schema)
        with pytest.raises(fake.SpiralError, match="503") as raised:
            source.as_table()
    assert raised.value.__notes__ == [
        "SpiralDB: could not read the schema of p/default/a"
    ]
    with pytest.raises(TableSchemaError, match="p/default/a"):
        source.as_table()


def test_two_sign_ins_never_share_what_one_may_read(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    allowed = _source("a")
    assert _ks(allowed) == [0, 1, 2, 3]
    original = fake.FakeClient.keyed_table
    trusted = allowed._connection._client  # pyright: ignore[reportPrivateUsage, reportOptionalMemberAccess]  # the first sign-in's

    def opened(self: Any, project: str, dataset: str, name: str, **given: Any):
        if self is not trusted and name == st.SCHEMA_TABLE:
            raise fake.SpiralError("status 403")
        return original(self, project, dataset, name, **given)

    monkeypatch.setattr(fake.FakeClient, "keyed_table", opened)
    monkeypatch.setenv("SPIRAL_CLIENT_ID", "work_second")
    with pytest.raises(fake.SpiralError, match="403"):
        _source("a")
    assert _ks(allowed) == [0, 1, 2, 3]


def test_a_warning_is_issued_by_the_reading_thread(root: Path) -> None:
    """So its filters and the line it names are the caller's."""
    made = fake.ExternalProject("p")
    made.create_table("default.raw", key_schema=[_SCHEMA.field("k")]).write(
        _rows(0).drop_columns(["j"])
    )
    with pytest.warns(UnrecordedTableSchemaWarning, match="raw has no row") as seen:
        _source("raw")
    assert [w.filename for w in seen] == [__file__]


def test_reads_work_where_no_thread_can_be_started(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_thread(*_args: Any, **_given: Any) -> None:
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(threading.Thread, "start", no_thread)
    sources = [_source(n) for n in _TABLES]
    assert _ks(sources[0]) == [0, 1, 2, 3]
    assert node_run.check_sources(sources) == ()


# --------------------------------------------------------------- the checks before a run


def _job(sources: list[Any]) -> PipelineJob:
    job = PipelineJob(name="checks", database=InMemoryArrowDatabase())
    with job:
        for index, source in enumerate(sources):
            SelectColumns(["k", "j", "x"])(source, label=f"n{index}")
    return job


def _write(name: str, k: int) -> None:
    fake.ExternalProject("p").table(f"default.{name}").write(_rows(k).slice(0, 1))


def test_neighbouring_checks_are_made_at_the_same_time(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sources = [_source(n) for n in _TABLES]
    job = _job(sources)
    job.run()
    _write("b", 77)
    together = threading.Barrier(3)
    opened = fake.FakeClient.keyed_table

    def new_opened(self: Any, project: str, dataset: str, name: str, **given: Any):
        if name in _TABLES:
            together.wait(10)
        return opened(self, project, dataset, name, **given)

    modified = [s.last_modified for s in sources]
    with monkeypatch.context() as slow:
        slow.setattr(fake.FakeClient, "keyed_table", new_opened)
        assert node_run.check_sources(sources) == ()
    moved = [s.last_modified != m for s, m in zip(sources, modified, strict=True)]
    assert moved == [False, True, False]
    job.run()
    assert 77 in job.nodes["n1"].as_table()["k"].to_pylist()


def test_failing_checks_warn_in_the_sources_order(root: Path) -> None:
    sources = [_source(n) for n in _TABLES]
    made = fake.ExternalProject("p")
    for name in ("a", "c"):
        made.drop_table(f"default.{name}")
    _write("b", 77)
    with pytest.warns(SourceCheckFailedWarning) as seen:
        unchecked = node_run.check_sources(sources)
    assert [w.filename for w in seen] == [__file__] * 2
    assert "default.a" in str(seen[0].message) and "not found" in str(seen[0].message)
    assert "default.c" in str(seen[1].message)
    assert len(unchecked) == 2
    assert 77 in _ks(sources[1])


def test_a_warning_made_an_error_leaves_the_later_sources_unchecked(
    root: Path,
) -> None:
    sources = [_source(n) for n in _TABLES]
    fake.ExternalProject("p").drop_table("default.b")
    _write("a", 77)
    _write("c", 78)
    with warnings.catch_warnings():
        warnings.simplefilter("error", SourceCheckFailedWarning)
        with pytest.raises(SourceCheckFailedWarning):
            node_run.check_sources(sources)
    assert 77 in _ks(sources[0]) and 78 not in _ks(sources[2])


def test_another_sources_check_keeps_its_place(root: Path) -> None:
    """What a check of another kind does is its own: no fetch moves across it."""

    class Writes(DictSource):
        """Writes to a SpiralDB table when it is checked."""

        def check_for_new_data(self) -> bool:
            """Write a row the next source must find."""
            _write("c", 91)
            return False

    class Counts(SpiralDBTableSource):
        """Replaces the check."""

        checked = 0

        def check_for_new_data(self) -> bool:
            """Count, then check."""
            type(self).checked += 1
            return super().check_for_new_data()

    writes = Writes([{"k": 1, "x": 1.0}], tag_columns=["k"], tag_only_columns=True)
    counts = Counts("p", "b", tag_only_columns=True)
    sources = [_source("a"), _source("b"), counts, writes, _source("c"), _source("a")]
    assert node_run.check_sources(sources) == ()
    assert Counts.checked == 1
    last = sources[4]
    assert isinstance(last, SpiralDBTableSource) and 91 in _ks(last)


def test_a_source_with_a_given_version_makes_no_call(calls: _Calls) -> None:
    sources = [_source("a"), _source("b", asof=1), _source("c")]
    calls.take()
    _write("b", 5)
    calls.take()
    assert node_run.check_sources(sources) == ()
    assert calls.take() == {"open": 2, "schema": 2, "open schemas": 2}


def test_a_closed_source_is_reported_like_any_failing_check(root: Path) -> None:
    sources = [_source("a"), _source("b")]
    sources[0].close()
    with pytest.warns(SourceCheckFailedWarning, match="is closed"):
        assert len(node_run.check_sources(sources)) == 1


# ------------------------------------------------------------------------------ budgets


def test_the_calls_each_action_makes(calls: _Calls) -> None:
    """A call more is a call repeated."""
    read = {"open": 1, "schema": 1, "open schemas": 1, "scan": 1}
    sources = [_source(n) for n in _TABLES]
    assert calls.take() == {
        "client": 1,
        "open": 3,
        "schema": 3,
        "open schemas": 3,
        "read schemas": 1,
    }
    first = sources[0]
    actions: dict[str, tuple[Callable[[], Any], dict[str, int]]] = {
        "first preview": (lambda: first.head(2), read | {"scan": 2}),
        "preview": (lambda: first.head(2), read),
        "run, the cap's count": (first.run, read),
        "run": (first.run, read),
        "where": (lambda: first.where(j="j0"), {}),
        "restricted read": (lambda: first.where(j="j0").as_table(), read),
        "count": (
            lambda: first.row_count_estimate,
            {"open": 1, "schema": 1, "open schemas": 1},
        ),
    }
    for name, (action, budget) in actions.items():
        action()
        assert calls.take() == budget, name
    job = _job(sources)
    for runner, budget in (
        (None, {"open": 6, "schema": 6, "open schemas": 6, "scan": 5}),
        (None, {"open": 3, "schema": 3, "open schemas": 3}),
        (AsyncPipelineOrchestrator(), {"open": 3, "schema": 3, "open schemas": 3}),
    ):
        job.run(orchestrator=runner)
        assert calls.take() == budget
