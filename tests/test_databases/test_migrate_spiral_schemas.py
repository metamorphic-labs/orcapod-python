"""The migration tool's write and verify steps, on the fake (ITL-1186).

The dry run needs pyspiral, so its plan is made here from the same functions; all three
steps were run on throwaway tables of the service.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod.databases import SpiralDBConnector
from orcapod.databases import spiraldb_schema_table as st
from orcapod.errors import TableSchemaError
from orcapod.sources import SpiralDBTableSource
from tests.test_databases import fake_spiral_sdk as fake

_SCRIPT = Path(__file__).parents[2] / "scripts" / "migrate_spiral_schemas.py"
_DATASETS = ("one", "two")
_TABLES = ("trials", "units")
_WRITTEN = pa.schema(
    [
        pa.field("k", pa.large_string(), False),
        pa.field("j", pa.int32(), False),
        pa.field("v", pa.int64(), False, {"unit": "mV"}),
        pa.field("w", pa.large_string()),
    ],
    metadata={"who": "test"},
)


def _rows(keys: list[str]) -> pa.Table:
    return pa.table(
        {
            "w": [None] * len(keys),
            "v": list(range(len(keys))),
            "j": pa.array([1] * len(keys), pa.int32()),
            "k": keys,
        },
        schema=pa.schema(
            [
                pa.field("w", pa.string()),
                pa.field("v", pa.int64()),
                pa.field("j", pa.int32()),
                pa.field("k", pa.string()),
            ]
        ),
    )


@pytest.fixture()
def tool(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """The script as a module, its client the fake's."""
    spec = importlib.util.spec_from_file_location("migrate_spiral_schemas", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "sdk_client", fake.FakeClient)
    return module


@pytest.fixture()
def project(tmp_path: Path):
    """Two datasets of two tables, with key columns in two orders."""
    with fake.install(tmp_path / "spiral"):
        made = fake.ExternalProject("p")
        for dataset in _DATASETS:
            for name, keys in zip(_TABLES, (["k", "j"], ["j", "k"])):
                made.create_table(
                    f"{dataset}.{name}",
                    key_schema=[_WRITTEN.field(key) for key in keys],
                ).write(_rows(["a", "b", "c"]))
        yield made


@pytest.fixture()
def args(
    tool: ModuleType, project: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> argparse.Namespace:
    """The arguments of a step, with the plan of ``project`` on disk.

    What each table was read as before is stated by a copy of the project that is
    migrated by the fake.
    """
    described: dict[str, Any] = {}
    root = tmp_path / "before"
    for dataset in _DATASETS:
        tables = []
        for name in _TABLES:
            handle = fake.FakeClient().keyed_table("p", dataset, name)
            found = handle.schema()
            schema = pa.schema(
                [_WRITTEN.field(f.name) for f in found],
                metadata=cast(Any, _WRITTEN.metadata),
            )
            row = st.SchemaRow(
                handle.resource_id,
                name,
                handle.key_columns,
                schema.serialize().to_pybytes(),
                st.FROM_RECORD,
                int(handle.version_id),
            )
            tables.append({"row": row.content()})
        described[dataset] = {"schema_table": None, "tables": tables}
    # The reads the dry run makes, with the schemas in place; then they are taken away.
    for dataset in _DATASETS:
        fake.migrate("p", dict.fromkeys(_TABLES, _WRITTEN), dataset)
        connector = SpiralDBConnector("p", dataset)
        for table in described[dataset]["tables"]:
            name = table["row"]["table_name"]
            table["source"] = tool.source_values("p", dataset, name)
            table["first_rows"] = tool.first_rows_digest(connector, name)
        project.drop_table(f"{dataset}.{st.SCHEMA_TABLE}")
    plan = {
        "format": tool.PLAN_FORMAT,
        "version": tool.PLAN_VERSION,
        "project": "p",
        "datasets": described,
    }
    root.mkdir()
    path = root / "plan.json"
    path.write_text(tool.canonical(plan))
    return argparse.Namespace(
        project="p",
        dataset=list(_DATASETS),
        plan=str(path),
        digest=tool.plan_digest(plan),
        stop_after=None,
        adopt=None,
    )


def test_write_then_verify(tool: ModuleType, args: argparse.Namespace) -> None:
    tool.write(args)
    tool.verify(args)
    for dataset in _DATASETS:
        for name in _TABLES:
            schema = SpiralDBConnector("p", dataset).get_arrow_schema(name)
            assert schema.metadata == {b"who": b"test"}
            assert not schema.field("v").nullable
            assert schema.field("v").metadata == {b"unit": b"mV"}


@pytest.mark.parametrize("stop_after", [1, 2, 3, 4])
def test_an_interrupted_write_is_refused_by_reads_and_resumed(
    tool: ModuleType, args: argparse.Namespace, stop_after: int
) -> None:
    args.stop_after = stop_after
    with pytest.raises(tool.MigrationError, match="stopped as asked"):
        tool.write(args)
    unfinished = _DATASETS[(stop_after - 1) // len(_TABLES)]
    with pytest.raises(TableSchemaError, match="interrupted"):
        SpiralDBTableSource("p", "units", dataset=unfinished)
    args.stop_after = None
    tool.write(args)
    tool.verify(args)
    tool.write(args)  # nothing left to do
    tool.verify(args)


def test_a_wrong_digest_or_scope_writes_nothing(
    tool: ModuleType, args: argparse.Namespace
) -> None:
    for change in (
        {"digest": "0" * 64},
        {"project": "q"},
        {"dataset": ["one"]},
        {"dataset": ["one", "two", "three"]},
    ):
        with pytest.raises(tool.MigrationError):
            tool.write(argparse.Namespace(**{**vars(args), **change}))
    for dataset in _DATASETS:
        with pytest.raises(fake.SpiralError):
            fake.FakeClient().keyed_table("p", dataset, st.SCHEMA_TABLE)


def _write_rows(project: Any) -> None:
    project.table("two.units").write(_rows(["z"]))


def _add_table(project: Any) -> None:
    project.create_table("two.new", key_schema=[("k", pa.string())]).write(
        pa.table({"k": ["a"], "v": [1]})
    )


def _drop_table(project: Any) -> None:
    project.drop_table("two.units")


def _recreate_table(project: Any) -> None:
    project.drop_table("two.units")
    project.create_table(
        "two.units", key_schema=[_WRITTEN.field("j"), _WRITTEN.field("k")]
    ).write(_rows(["a", "b", "c"]))


_DRIFTS: dict[str, Callable[[Any], None]] = {
    "rows written": _write_rows,
    "table added": _add_table,
    "table dropped": _drop_table,
    "table created again": _recreate_table,
}


@pytest.mark.parametrize("drift", sorted(_DRIFTS))
def test_a_change_since_the_dry_run_writes_nothing(
    tool: ModuleType, args: argparse.Namespace, project: Any, drift: str
) -> None:
    """Review findings B10 and 28b9ea8f: the refusal names the table, no KeyError."""
    _DRIFTS[drift](project)
    with pytest.raises(tool.MigrationError, match="two"):
        tool.write(args)
    for dataset in _DATASETS:
        with pytest.raises(fake.SpiralError):
            fake.FakeClient().keyed_table("p", dataset, st.SCHEMA_TABLE)


@pytest.mark.parametrize("drift", sorted(_DRIFTS))
@pytest.mark.parametrize("when", ["before the marker", "after the marker"])
def test_a_change_during_the_write_leaves_no_valid_marker(
    tool: ModuleType,
    args: argparse.Namespace,
    project: Any,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
    when: str,
) -> None:
    """Review finding B10. The change comes as the last row of dataset ``two``, or its
    marker, is written."""
    original = fake.FakeKeyedTable.append
    last = st.MARKER_KEY if when == "after the marker" else None

    def append(self: Any, data: pa.Table) -> Any:
        done = original(self, data)
        (row,) = data.to_pylist()
        is_last = (
            row["table_id"] == st.MARKER_KEY
            if last
            else row["table_name"] == _TABLES[-1]
        )
        if self._table["dataset"] == "two" and is_last:
            monkeypatch.setattr(fake.FakeKeyedTable, "append", original)
            _DRIFTS[drift](project)
        return done

    monkeypatch.setattr(fake.FakeKeyedTable, "append", append)
    with pytest.raises(tool.MigrationError) as raised:
        tool.write(args)
    assert ("withdrawn" in str(raised.value)) == (when == "after the marker")
    with pytest.raises(TableSchemaError):
        SpiralDBTableSource("p", "trials", dataset="two")
    tool_error = (tool.MigrationError, TableSchemaError)
    with pytest.raises(tool_error):
        tool.verify(args)


@pytest.mark.parametrize("error", [RuntimeError("network"), KeyboardInterrupt()])
def test_any_failure_after_the_marker_withdraws_it_and_is_resumed(
    tool: ModuleType,
    args: argparse.Namespace,
    monkeypatch: pytest.MonkeyPatch,
    error: BaseException,
) -> None:
    original = tool.require_unchanged
    calls = []

    def fail_after_marker(*given: Any) -> None:
        calls.append(given)
        # 1 and 2: before anything, per dataset; 3: before the first marker; 4: after.
        if len(calls) == 4:
            raise error
        original(*given)

    monkeypatch.setattr(tool, "require_unchanged", fail_after_marker)
    with pytest.raises(tool.MigrationError, match="withdrawn"):
        tool.write(args)
    with pytest.raises(TableSchemaError, match="does not fit"):
        SpiralDBTableSource("p", "trials", dataset="one")
    monkeypatch.setattr(tool, "require_unchanged", original)
    tool.write(args)
    tool.verify(args)


def test_verify_finds_what_changed_after_it(
    tool: ModuleType, args: argparse.Namespace, project: Any
) -> None:
    tool.write(args)
    _write_rows(project)
    with pytest.raises(tool.MigrationError, match="written since"):
        tool.verify(args)


@pytest.mark.parametrize(
    "part", ["source_id", "content_hash", "pipeline_hash", "tag_schema", "data_schema"]
)
def test_verify_compares_every_identity(
    tool: ModuleType,
    args: argparse.Namespace,
    monkeypatch: pytest.MonkeyPatch,
    part: str,
) -> None:
    tool.write(args)
    original = tool.source_values

    def changed(project: str, dataset: str, name: str) -> dict[str, Any]:
        values = original(project, dataset, name)
        return {**values, part: "another"} if name == "units" else values

    monkeypatch.setattr(tool, "source_values", changed)
    with pytest.raises(tool.MigrationError, match=f"units: {part}") as raised:
        tool.verify(args)
    assert "trials" not in str(raised.value)
    assert "first" not in str(raised.value)


def test_verify_compares_the_first_rows(
    tool: ModuleType, args: argparse.Namespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    tool.write(args)
    monkeypatch.setattr(tool, "first_rows_digest", lambda connector, name: "another")
    with pytest.raises(tool.MigrationError, match="its first 20 rows"):
        tool.verify(args)


@pytest.mark.parametrize("when", ["before the creation", "after it", "at the note"])
def test_a_stop_while_the_schema_table_is_created_is_resumed(
    tool: ModuleType,
    args: argparse.Namespace,
    monkeypatch: pytest.MonkeyPatch,
    when: str,
) -> None:
    create = fake.FakeProject.create_keyed_table
    note = tool.write_note

    def stopping(self: Any, *given: Any, **options: Any) -> Any:
        if when == "before the creation":
            raise RuntimeError("stopped")
        made = create(self, *given, **options)
        if when == "after it":
            raise RuntimeError("stopped")
        return made

    def failing(given: Any, dataset: str, value: str) -> None:
        if when == "at the note" and value != tool.CREATING:
            raise OSError("disk full")
        note(given, dataset, value)

    monkeypatch.setattr(fake.FakeProject, "create_keyed_table", stopping)
    monkeypatch.setattr(tool, "write_note", failing)
    with pytest.raises((RuntimeError, OSError)):
        tool.write(args)
    monkeypatch.setattr(fake.FakeProject, "create_keyed_table", create)
    monkeypatch.setattr(tool, "write_note", note)
    if when != "before the creation":
        # An empty table of that name may be anyone's: a person names it.
        table = fake.FakeClient().keyed_table("p", "one", st.SCHEMA_TABLE)
        for adopt in (None, "table_other"):
            args.adopt = adopt
            with pytest.raises(tool.MigrationError, match=table.resource_id):
                tool.write(args)
        args.adopt = table.resource_id
    tool.write(args)
    tool.verify(args)


def test_a_table_made_after_a_stop_before_the_creation_is_not_adopted(
    tool: ModuleType, args: argparse.Namespace
) -> None:
    tool.write_note(args, "one", tool.CREATING)
    other = fake.FakeClient().project("p")
    made = other.create_keyed_table("one", st.SCHEMA_TABLE, st.KEY_SCHEMA)
    with pytest.raises(tool.MigrationError, match="nobody else"):
        tool.write(args)
    assert fake.FakeClient().keyed_table_by_id(made.resource_id).version_id == "0"


def test_a_table_another_program_creates_first_is_never_adopted(
    tool: ModuleType, args: argparse.Namespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The exclusive create is refused (409): no retry takes the other's empty table,
    which looks exactly like one of ours."""
    create = fake.FakeProject.create_keyed_table
    calls = []

    def second(self: Any, *given: Any, **options: Any) -> Any:
        calls.append(options)
        if len(calls) == 1:
            create(self, *given, exist_ok=True)  # the other program is first
        return create(self, *given, **options)

    monkeypatch.setattr(fake.FakeProject, "create_keyed_table", second)
    with pytest.raises(tool.MigrationError, match="another program created"):
        tool.write(args)
    for _ in range(2):
        with pytest.raises(tool.MigrationError, match="not created by this plan"):
            tool.write(args)
    table = fake.FakeClient().keyed_table("p", "one", st.SCHEMA_TABLE)
    assert table.version_id == "0"  # nothing was written to it
    assert len(calls) == 1


def test_a_noted_creation_does_not_adopt_a_table_that_holds_rows(
    tool: ModuleType, args: argparse.Namespace
) -> None:
    fake.migrate("p", {}, "one")
    tool.write_note(args, "one", tool.CREATING)
    with pytest.raises(tool.MigrationError, match="not created by this plan"):
        tool.write(args)


def test_a_schema_table_of_another_hand_is_refused(
    tool: ModuleType, args: argparse.Namespace, project: Any
) -> None:
    """Review finding B12: only a schema table this plan's write created is continued."""
    fake.migrate("p", {}, "two")
    with pytest.raises(tool.MigrationError, match="not created by this plan"):
        tool.write(args)
    with pytest.raises(fake.SpiralError):
        fake.FakeClient().keyed_table("p", "one", st.SCHEMA_TABLE)


def test_a_schema_table_seen_by_the_dry_run_is_refused(
    tool: ModuleType, args: argparse.Namespace
) -> None:
    plan = json.loads(Path(args.plan).read_text())
    plan["datasets"]["two"]["schema_table"] = "table_x"
    Path(args.plan).write_text(tool.canonical(plan))
    args.digest = tool.plan_digest(plan)
    with pytest.raises(tool.MigrationError, match="existed at the dry run"):
        tool.write(args)


def test_no_sign_in_stops_every_step(
    tool: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name in ("SPIRAL_TOKEN", "SPIRAL_WORKLOAD_ID", "SPIRAL_CLIENT_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SPIRAL_AUTH_FILE", str(tmp_path / "none.json"))
    with pytest.raises(tool.MigrationError, match="sign-in"):
        tool.require_credentials()


def test_the_principal_is_named_and_the_token_is_not(tool: ModuleType) -> None:
    import base64

    body = base64.urlsafe_b64encode(b'{"sub": "work_1"}').decode().rstrip("=")
    assert tool.principal_of(f"head.{body}.secret") == "work_1"
