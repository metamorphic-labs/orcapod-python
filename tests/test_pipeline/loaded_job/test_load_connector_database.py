"""A saved connector database is supplied at load, never rebuilt (ITL-1391).

The connectors cannot hold a function node's tables (entry CA3, ITL-1162), so the jobs
here hold operators only. SQLite can store their tables. For PostgreSQL, which limits
table names to 63 bytes, and read-only SpiralDB, the test inserts the connector's
configuration into a saved file. All three can be saved directly as a result database.
"""

from __future__ import annotations

import asyncio
import json
import logging
import traceback
from collections.abc import Iterator
from contextlib import ExitStack
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from orcapod import PipelineJob
from orcapod.databases import (
    ConnectorArrowDatabase,
    DeltaTableDatabase,
    PostgreSQLConnector,
    SpiralDBConnector,
    SQLiteConnector,
)
from orcapod.pipeline.serialization import SOURCE_REGISTRY
from orcapod.sources import ArrowTableSource
from tests.test_databases import fake_spiral_sdk

KINDS = ("sqlite", "postgresql", "spiraldb")
MARKER = "marker-in-dsn"


class Databases:
    """Connector databases of each kind, each over its own place per name."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def make(self, kind: str, name: str) -> ConnectorArrowDatabase:
        if kind == "sqlite":
            return ConnectorArrowDatabase(
                SQLiteConnector(str(self.root / f"{name}.sqlite"))
            )
        if kind == "postgresql":
            return ConnectorArrowDatabase(
                PostgreSQLConnector(f"postgresql://u:{MARKER}@h/{name}")
            )
        return ConnectorArrowDatabase(SpiralDBConnector(name))


@pytest.fixture
def databases(tmp_path: Path) -> Iterator[Databases]:
    with (
        fake_spiral_sdk.install(tmp_path / "spiral"),
        patch("psycopg.connect") as connect,
    ):
        connect.return_value = MagicMock()
        yield Databases(tmp_path)


def _sources(
    order: tuple[str, str] = ("left", "right"),
    *,
    tag_order: tuple[str, str] = ("a", "b"),
    data_tags: bool = False,
) -> dict[str, Any]:
    left = pa.table(
        {"a": [1, 2, 3], "b": ["x", "y", "z"], "p": [1.0, 2.0, 3.0], "q": [4, 5, 6]}
    )
    right = pa.table({"b": ["z", "x"], "a": [3, 1], "r": [30, 10], "s": ["u", "v"]})
    tables = {"left": left, "right": right}
    return {
        name: ArrowTableSource(
            tables[name],
            tag_columns=list(tag_order),
            source_id=name,
            tag_only_columns=() if data_tags else True,
        )
        for name in order
    }


def _save(
    path: Path,
    *,
    run_async: bool = False,
    input_order: tuple[str, str] = ("left", "right"),
    selected: tuple[str, str] = ("s", "p"),
    tag_order: tuple[str, str] = ("a", "b"),
    data_tags: bool = False,
    **databases: Any,
) -> tuple[PipelineJob, dict[str, Any]]:
    """An operator-only job, run and saved, and its file's bindings."""
    sources = _sources(tag_order=tag_order, data_tags=data_tags)
    first, second = input_order
    job = PipelineJob(name="j", **databases)
    with job:
        joined = sources[first].join(sources[second], label="joined")
        joined.select_data_columns(selected, label="picked")
    if run_async:
        asyncio.run(job.run_async())
    else:
        job.run()
    job.save(path)
    return job, json.loads(path.read_text())["bindings"]


def _save_over(path: Path, databases: Databases, kind: str, **given: Any) -> None:
    """A saved job file whose database is of ``kind`` (see the module docstring)."""
    if kind == "sqlite":
        _save(path, database=databases.make(kind, "a"), **given)
        return
    _save(path, database=databases.make("sqlite", "a"), **given)
    data = json.loads(path.read_text())
    data["bindings"]["database"] = databases.make(kind, "a").to_config()
    path.write_text(json.dumps(data))


def _values(job: PipelineJob) -> list[dict[str, Any]]:
    rows = job.nodes["picked"].as_table().select(["a", "b", "s", "p"]).to_pylist()
    return sorted(rows, key=lambda row: row["a"])


EXPECTED = [
    {"a": 1, "b": "x", "s": "v", "p": 1.0},
    {"a": 3, "b": "z", "s": "u", "p": 3.0},
]


@pytest.mark.parametrize("kind", KINDS)
def test_load_without_the_database_names_the_slot_and_what_to_pass(
    databases: Databases, tmp_path: Path, kind: str
) -> None:
    path = tmp_path / "job.json"
    _save_over(path, databases, kind)
    with pytest.raises(ValueError) as refused:
        PipelineJob.load(path)
    message = str(refused.value)
    assert f"bindings.database is a connector database ({kind})" in message
    assert "pass database=ConnectorArrowDatabase(" in message
    assert "result_database" not in message
    assert "Unknown database type" not in message
    # The saved connection is never shown.
    assert MARKER not in message


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("result_kind", KINDS)
def test_every_missing_slot_is_named(
    databases: Databases, tmp_path: Path, kind: str, result_kind: str
) -> None:
    path = tmp_path / "job.json"
    results = databases.make(result_kind, "b")
    _save_over(path, databases, kind, result_database=results)
    # Any given database fills a slot: one the job's operators can be stored in.
    database = databases.make("sqlite", "a")
    wanted = {
        "database": f"bindings.database is a connector database ({kind})",
        "result_database": (
            f"bindings.result_database is a connector database ({result_kind})"
        ),
    }
    for given in ({}, {"database": database}, {"result_database": results}):
        with pytest.raises(ValueError) as refused:
            PipelineJob.load(path, **given)
        for slot, text in wanted.items():
            assert (text in str(refused.value)) is (slot not in given)
        assert MARKER not in str(refused.value)
    loaded = PipelineJob.load(path, result_database=results, database=database)
    assert loaded.database is database
    assert loaded.result_database is results


@pytest.mark.parametrize("kind", KINDS)
def test_a_connector_result_database_beside_a_delta_database(
    databases: Databases, tmp_path: Path, kind: str
) -> None:
    path = tmp_path / "job.json"
    results = databases.make(kind, "b")
    _save(
        path,
        database=DeltaTableDatabase(tmp_path / "delta"),
        result_database=results,
    )
    with pytest.raises(ValueError, match="pass result_database="):
        PipelineJob.load(path)
    loaded = PipelineJob.load(path, result_database=results)
    assert isinstance(loaded.database, DeltaTableDatabase)


@pytest.mark.parametrize("saved_async", [False, True])
@pytest.mark.parametrize("loaded_async", [False, True])
@pytest.mark.parametrize("bind_order", [("left", "right"), ("right", "left")])
@pytest.mark.parametrize(
    ("input_order", "selected"),
    [(("left", "right"), ("s", "p")), (("right", "left"), ("p", "s"))],
)
@pytest.mark.parametrize("data_tags", [False, True])
@pytest.mark.parametrize("tag_order", [("a", "b"), ("b", "a")])
def test_load_with_the_database_then_load_only(
    databases: Databases,
    tmp_path: Path,
    saved_async: bool,
    loaded_async: bool,
    bind_order: tuple[str, str],
    input_order: tuple[str, str],
    selected: tuple[str, str],
    data_tags: bool,
    tag_order: tuple[str, str],
) -> None:
    path = tmp_path / "job.json"
    job, bindings = _save(
        path,
        run_async=saved_async,
        input_order=input_order,
        selected=selected,
        tag_order=tag_order,
        data_tags=data_tags,
        database=databases.make("sqlite", "a"),
    )
    assert _values(job) == EXPECTED
    assert {
        name: config["tag_columns"] for name, config in bindings["sources"].items()
    } == {"left": list(tag_order), "right": list(tag_order)}
    hashes = {
        label: (node.pipeline_hash(), node.content_hash())
        for label, node in job.nodes.items()
    }

    # A new handle over the same place, as another process would make.
    loaded = PipelineJob.load(path, database=databases.make("sqlite", "a"))
    first_tag, second_tag = tag_order
    loaded.bind(
        sources=_sources(
            bind_order, tag_order=(second_tag, first_tag), data_tags=data_tags
        )
    )
    assert {
        label: (node.pipeline_hash(), node.content_hash())
        for label, node in loaded.nodes.items()
    } == hashes
    if loaded_async:
        asyncio.run(loaded.run_async(load_only=True))
    else:
        loaded.run(load_only=True)
    assert _values(loaded) == EXPECTED
    assert loaded.nodes["picked"].as_table().column_names == (
        job.nodes["picked"].as_table().column_names
    )
    _, joined_data = loaded.nodes["joined"].output_schema()
    assert {"a", "b"}.issubset(joined_data) is data_tags

    again = tmp_path / "again.json"
    loaded.save(again)
    assert (
        json.loads(again.read_text())["bindings"]["database"] == (bindings["database"])
    )
    with pytest.raises(ValueError, match="pass database="):
        PipelineJob.load(again)


@pytest.mark.parametrize("kind", KINDS)
def test_any_given_database_is_used_whatever_connector_was_saved(
    databases: Databases, tmp_path: Path, kind: str
) -> None:
    path = tmp_path / "job.json"
    _save_over(path, databases, kind)
    given = DeltaTableDatabase(tmp_path / "delta")
    assert PipelineJob.load(path, database=given).database is given


@pytest.mark.parametrize("kind", KINDS)
def test_a_resave_writes_the_databases_actually_bound(
    databases: Databases, tmp_path: Path, kind: str
) -> None:
    """Databases given at load replace the recorded ones in a later save, in both slots."""
    path, again = tmp_path / "job.json", tmp_path / "again.json"
    results = databases.make(kind, "b")
    _save_over(path, databases, kind, result_database=results)
    recorded = json.loads(path.read_text())["bindings"]
    moved = databases.make("sqlite", "moved-a")
    moved_results = databases.make(kind, "moved-b")
    loaded = PipelineJob.load(path, database=moved, result_database=moved_results)
    loaded.save(again)
    bindings = json.loads(again.read_text())["bindings"]
    assert bindings["database"] == moved.to_config() != recorded["database"]
    assert bindings["result_database"] == moved_results.to_config()
    assert bindings["result_database"] != recorded["result_database"]


TEXT = "marker-text://x"


def _marked_files(recorded: dict[str, Any], slot: str) -> Iterator[dict[str, Any]]:
    """The saved bindings with ``TEXT`` put in each field of ``slot`` in turn."""
    for field in recorded[slot]:
        for value in (TEXT, [TEXT], {TEXT: TEXT}):
            changed = json.loads(json.dumps(recorded))
            changed[slot][field] = value
            yield changed
    for field in [*recorded[slot]["connector"], "another"]:
        for value in (TEXT, [TEXT], {TEXT: TEXT}):
            changed = json.loads(json.dumps(recorded))
            changed[slot]["connector"][field] = value
            yield changed


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("slot", ["database", "result_database"])
@pytest.mark.filterwarnings("always")
def test_no_saved_text_is_shown_by_a_load(
    databases: Databases,
    tmp_path: Path,
    kind: str,
    slot: str,
    caplog: pytest.LogCaptureFixture,
    recwarn: pytest.WarningsRecorder,
) -> None:
    """Text in any field of a saved connector database reaches no exception (a
    chained one included), log or warning, whether or not the other slot is given."""
    path = tmp_path / "job.json"
    results = databases.make(kind, "b")
    _save_over(path, databases, kind, result_database=results)
    data = json.loads(path.read_text())
    given = {
        "database": databases.make("sqlite", "a"),
        "result_database": results,
    }
    del given[slot]
    caplog.set_level(logging.DEBUG)
    shown: list[str] = []
    for bindings in _marked_files(data["bindings"], slot):
        path.write_text(json.dumps({**data, "bindings": bindings}))
        for databases_given in ({}, given):
            try:
                PipelineJob.load(path, **databases_given)
            except Exception as exc:
                # The whole chain, causes and contexts included.
                shown.append("".join(traceback.format_exception(exc)))
                shown.append(repr(exc))
    assert shown
    text = "\n".join([*shown, caplog.text, *(str(w.message) for w in recwarn)])
    assert "marker-text" not in text
    assert TEXT not in text


def _delta_changes(root: Path) -> dict[str, dict[str, Any]]:
    """Delta configurations that fail to rebuild, with ``TEXT`` in what they hold."""
    blocked = root / "a-file"
    blocked.write_text("not a directory")
    return {
        "a path that cannot be made": {"root_uri": f"{blocked}/{TEXT}"},
        "a URI that is not supported": {"root_uri": f"{TEXT}::x"},
        "a URI that is not text": {"root_uri": {TEXT: TEXT}},
        "options that are not a mapping": {
            "root_uri": f"s3://bucket/{TEXT}",
            "storage_options": TEXT,
        },
        "options of a file": {
            "root_uri": f"{blocked}/x",
            "storage_options": {"AWS_REGION": TEXT, TEXT: TEXT},
        },
        "an unknown kind": {"type": TEXT},
    }


@pytest.mark.parametrize("slot", ["database", "result_database"])
@pytest.mark.parametrize("fails", ["as the database does", "with the whole config"])
def test_a_failed_delta_rebuild_logs_the_slot_kind_and_exception_type_only(
    tmp_path: Path,
    slot: str,
    fails: str,
    caplog: pytest.LogCaptureFixture,
    recwarn: pytest.WarningsRecorder,
) -> None:
    """Review of PR 540: the warning no longer holds the exception text."""
    path = tmp_path / "job.json"
    _save(
        path,
        database=DeltaTableDatabase(tmp_path / "delta"),
        result_database=DeltaTableDatabase(tmp_path / "results"),
    )
    data = json.loads(path.read_text())
    caplog.set_level(logging.DEBUG)
    for name, change in _delta_changes(tmp_path).items():
        changed = json.loads(json.dumps(data))
        changed["bindings"][slot].update(change)
        path.write_text(json.dumps(changed))
        caplog.clear()

        def refuse(config: dict[str, Any]) -> Any:
            raise OSError(f"cannot open {config}")

        with ExitStack() as stack:
            if fails == "with the whole config":
                stack.enter_context(
                    patch.object(DeltaTableDatabase, "from_config", refuse)
                )
            # The other slot is given, so only the changed one is rebuilt.
            other = "result_database" if slot == "database" else "database"
            given: dict[str, Any] = {other: DeltaTableDatabase(tmp_path / "given")}
            shown = ""
            try:
                PipelineJob.load(path, **given)
            except Exception as exc:
                shown = "".join(traceback.format_exception(exc))
        text = "\n".join([shown, caplog.text, *(str(w.message) for w in recwarn)])
        assert "marker-text" not in text, name
        if fails == "with the whole config":
            assert f"bindings.{slot}" in caplog.text
            if name == "an unknown kind":
                assert "(unknown database kind): ValueError." in caplog.text
            else:
                assert "(delta_table): OSError." in caplog.text


@pytest.mark.parametrize(
    "connector",
    [None, {}, {"connector_type": 3}, "text", {"connector_type": ["sqlite"]}],
)
def test_a_connector_the_file_does_not_name_is_refused_too(
    databases: Databases, tmp_path: Path, connector: Any
) -> None:
    path = tmp_path / "job.json"
    _save(path, database=databases.make("sqlite", "a"))
    data = json.loads(path.read_text())
    data["bindings"]["database"]["connector"] = connector
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match=r"\(unknown connector kind\)"):
        PipelineJob.load(path)


@pytest.mark.parametrize("slot", ["database", "result_database"])
def test_saving_a_loaded_job_keeps_a_database_that_could_not_be_rebuilt(
    tmp_path: Path, slot: str
) -> None:
    path, again = tmp_path / "job.json", tmp_path / "again.json"
    _save(path, database=DeltaTableDatabase(tmp_path / "delta"))
    data = json.loads(path.read_text())
    saved = {"type": "no_such_database", "place": ["x", "y"]}
    data["bindings"][slot] = saved
    path.write_text(json.dumps(data))

    loaded = PipelineJob.load(path)
    assert getattr(loaded, slot) is None
    loaded.save(again)
    assert json.loads(again.read_text())["bindings"][slot] == saved

    # Rebinding sources or the other database leaves this slot untouched.
    loaded.bind(sources=_sources())
    other_database = DeltaTableDatabase(tmp_path / "other")
    if slot == "database":
        loaded.bind(result_database=other_database)
    else:
        loaded.bind(database=other_database)
    loaded.save(again)
    assert json.loads(again.read_text())["bindings"][slot] == saved
    given = DeltaTableDatabase(tmp_path / "given")
    if slot == "database":
        loaded.bind(database=given)
    else:
        loaded.bind(result_database=given)
    loaded.save(again)
    assert json.loads(again.read_text())["bindings"][slot] == given.to_config()


SAVED_SOURCE_KINDS = (*sorted(SOURCE_REGISTRY), "db_table", "polling_source", TEXT)


def _raise_chained(config: dict[str, Any], **_: Any) -> Any:
    """A rebuild that fails with ``TEXT`` in the error, its cause and its context."""
    try:
        try:
            raise KeyError(f"first {TEXT}")
        except KeyError:
            raise OSError(f"second {TEXT}")
    except OSError as exc:
        raise RuntimeError(f"third {TEXT} {config}") from exc


@pytest.mark.parametrize("kind", SAVED_SOURCE_KINDS)
@pytest.mark.parametrize("slots", [["left"], ["left", "right"], ["right", "left"]])
def test_a_source_that_cannot_be_rebuilt_logs_the_slot_kind_and_error_type_only(
    tmp_path: Path,
    kind: str,
    slots: list[str],
    caplog: pytest.LogCaptureFixture,
    recwarn: pytest.WarningsRecorder,
) -> None:
    """The warning no longer holds the exception text, for every source kind."""
    path = tmp_path / "job.json"
    _save(path, database=DeltaTableDatabase(tmp_path / "delta"))
    data = json.loads(path.read_text())
    sources = data["bindings"]["sources"]
    data["bindings"]["sources"] = {
        slot: {**sources[slot], "source_type": kind, "entry": TEXT} for slot in slots
    } | {slot: value for slot, value in sources.items() if slot not in slots}
    path.write_text(json.dumps(data))
    caplog.set_level(logging.DEBUG)
    with ExitStack() as stack:
        if kind in SOURCE_REGISTRY:
            stack.enter_context(
                patch.object(SOURCE_REGISTRY[kind], "from_config", _raise_chained)
            )
        loaded = PipelineJob.load(path)
    assert set(slots) <= set(loaded.unbound_sources)
    for slot in slots:
        shown = "unknown source kind" if kind == TEXT else kind
        error = "RuntimeError" if kind in SOURCE_REGISTRY else "ValueError"
        records = [
            r
            for r in caplog.records
            if r.getMessage().startswith(
                f"Could not reconstruct source {slot!r} ({shown}) from its saved "
                f"config: {error}. "
            )
        ]
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert records[0].exc_info is None
    text = "\n".join([caplog.text, *(str(w.message) for w in recwarn)])
    assert "marker-text" not in text


@pytest.mark.parametrize("value", [TEXT, [TEXT], {TEXT: TEXT}])
def test_a_source_kind_of_a_file_is_never_shown(
    tmp_path: Path, value: Any, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / "job.json"
    _save(path, database=DeltaTableDatabase(tmp_path / "delta"))
    data = json.loads(path.read_text())
    data["bindings"]["sources"]["left"]["source_type"] = value
    path.write_text(json.dumps(data))
    caplog.set_level(logging.DEBUG)
    shown = ""
    try:
        PipelineJob.load(path)
    except Exception as exc:
        # A kind that is not text is refused by the file's own checks.
        shown = "".join(traceback.format_exception(exc))
    assert "marker-text" not in shown + caplog.text
