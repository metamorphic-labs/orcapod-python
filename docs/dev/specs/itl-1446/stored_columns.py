"""ITL-1446: every column of every table a job stores, and what SpiralDB says about it.

A job over one source (two tags, two data columns) runs a function
pod, an operator node that logs (a ``Join``), a cached source, a side-effect pod that tracks
its completions and a status and a logging observer against an in-memory database. Each
``add_records`` call is recorded by table path, so the columns below are those the library
writes, never a list typed by hand. Each column is then given to
``SpiralDBConnector.unstorable_column_reason``, and written alone (beside the record id)
through pyspiral's disk fake, which raises SpiralDB's own refusals.

A second function has outputs of logical types, for the columns that depend on the user's
types. Observers are opt-in (``job.run(observer=...)``); their tables are listed because a
job can be given one over the same store.

Run: ``uv run python docs/dev/specs/itl-1446/stored_columns.py`` (prints Markdown).
"""

from __future__ import annotations

import datetime
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa

import orcapod
from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.cached_source import CachedSource
from orcapod.databases import InMemoryArrowDatabase, SpiralDBConnector
from orcapod.pipeline.composite_observer import CompositeObserver
from orcapod.pipeline.logging_observer import LoggingObserver
from orcapod.pipeline.status_observer import StatusObserver
from orcapod.side_effects import SideEffectPod, SideEffectPodConfig
from orcapod.types import CacheMode
from orcapod.utils.arrow_utils import normalize_schema_to_large_types
from tests.test_databases import fake_pyspiral

SEEN: dict[tuple[str, ...], pa.Table] = {}
_add_records = InMemoryArrowDatabase.add_records


def _recording(
    self: Any, record_path: Any, records: pa.Table, *a: Any, **k: Any
) -> Any:
    given = (records, *a)
    key = k.get("record_id_column") or (a[0] if a else None)
    if key and key != "__record_id":
        records = records.rename_columns(
            ["__record_id" if n == key else n for n in records.column_names]
        )
    SEEN.setdefault(self.base_path + tuple(record_path), records)
    return _add_records(self, record_path, *given, **k)


setattr(InMemoryArrowDatabase, "add_records", _recording)  # noqa: B010


@orcapod.dataclass
class Reading:
    """A structural user class (a struct in storage)."""

    value: float
    unit: str


@function_pod(output_keys=["w_out", "c_out"])
def compute(y: float, b: float) -> tuple[float, float]:
    """Two outputs."""
    return y + b, y - b


@function_pod(output_keys=["at", "path", "reading", "ids", "counts"])
def logical(
    y: float,
) -> tuple[datetime.datetime, Path, Reading, list[uuid.UUID], dict[str, int]]:
    """Outputs of the types a user is likely to declare."""
    return (
        datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
        Path("/a"),
        Reading(y, "mV"),
        [uuid.UUID(int=1)],
        {"a": 1},
    )


def notify(ctx: Any, y: float, b: float) -> None:
    """A side effect."""


def _source(name: str, data: list[str]) -> ArrowTableSource:
    fields = [
        pa.field("z_tag", pa.large_string(), nullable=False),
        pa.field("a_tag", pa.int64(), nullable=False),
        *(pa.field(n, pa.float64(), nullable=False) for n in data),
    ]
    arrays = [
        pa.array(["s1", "s2"], pa.large_string()),
        pa.array([1, 2], pa.int64()),
        *(pa.array([1.0, 2.0], pa.float64()) for _ in data),
    ]
    return ArrowTableSource(
        pa.Table.from_arrays(arrays, schema=pa.schema(fields)),
        tag_columns=["z_tag", "a_tag"],
        tag_only_columns=True,
        source_id=name,
    )


def run_jobs() -> None:
    """Run both jobs, recording every table written."""
    database = InMemoryArrowDatabase()
    job = PipelineJob(name="itl1446", database=database)
    cached = CachedSource(_source("left", ["y", "b"]), cache_database=database)
    with job:
        right = _source("right", ["x", "e"])
        compute.pod(cached, label="fn")
        Join()(cached, right, label="joined")
        SideEffectPod(notify, SideEffectPodConfig(track_completion=True))(
            cached, label="notify"
        )
        logical.pod(_source("plain", ["y"]), label="logical")
    job.nodes["joined"].attach_databases(
        pipeline_database=database, cache_mode=CacheMode.LOG
    )
    observers = database.at("_observers")
    observer: Any = CompositeObserver(
        StatusObserver(observers), LoggingObserver(observers)
    )
    job.run(observer=observer)


def _short(text: str) -> str:
    return re.sub(r"[0-9a-f]{64}", "<64 hex>", text)


def _kind(path: tuple[str, ...]) -> str:
    return {
        "pdb_v1": "function pipeline",
        "rdb_v1": "function result",
        "tdb_v1": "side-effect log",
        "odb_v1": "operator cache",
        "sdb_v1": "cached source",
    }.get(path[-1], f"observer ({path[-1]})")


def _fake_refusal(records: pa.Table, name: str) -> str:
    """What SpiralDB (the fake) raises or silently changes for one column."""
    with tempfile.TemporaryDirectory() as root, fake_pyspiral.install(root):
        project = fake_pyspiral.FakeSpiral().project("p")
        key = pa.schema([pa.field("__record_id", pa.large_binary())])
        table = project.create_table("default.t", key_schema=key)
        rows = records.select(["__record_id"])
        rows = rows.set_column(0, "__record_id", rows.column(0).cast(pa.large_binary()))
        if name == "__record_id":
            return "key column"
        field = records.schema.field(name)
        rows = rows.append_column(field, records.column(name))
        try:
            table.write(rows)
        except ValueError as exc:
            return f"raises: {_short(str(exc))}"
        back = normalize_schema_to_large_types(
            table.schema().to_arrow(), keep_null=True
        ).field(name)
        want = normalize_schema_to_large_types(rows.schema, keep_null=True).field(name)
        changes = []
        if back.type != want.type:
            changes.append(f"type reads {back.type}")
        if (back.metadata or {}) != (want.metadata or {}):
            changes.append("field metadata dropped (connector records it)")
        if back.nullable != want.nullable:
            changes.append("nullability dropped (connector records it)")
        return "; ".join(changes) or "kept"


def main() -> None:
    """Print one Markdown table per stored table."""
    run_jobs()
    with tempfile.TemporaryDirectory() as root, fake_pyspiral.install(root):
        connector = SpiralDBConnector("p")
        total = refused = 0
        for path, records in sorted(SEEN.items(), key=lambda i: (_kind(i[0]), i[0])):
            schema = normalize_schema_to_large_types(records.schema, keep_null=True)
            print(f"\n### {_kind(path)}: `{_short('/'.join(path))}`\n")
            print(
                "| Column | Arrow type | Nullable | Refused today because | Alone, through the fake |"
            )
            print("|---|---|---|---|---|")
            for field in schema:
                reason = connector.unstorable_column_reason(
                    field.name, field.type, primary_key=field.name == "__record_id"
                )
                total += 1
                refused += reason is not None
                text = re.sub(r"\s+", " ", _short(str(field.type)))
                if isinstance(field.type, pa.BaseExtensionType):
                    text = f"{field.type.extension_name} over {field.type.storage_type}"
                    text = re.sub(r"@?[0-9a-f]{64}", "@<64 hex>", text)
                print(
                    f"| `{_short(field.name)}` | `{text}` | "
                    f"{'yes' if field.nullable else 'no'} | {reason or 'stored'} | "
                    f"{_fake_refusal(records, field.name)} |"
                )
        print(f"\n{total} columns, {refused} refused.")


if __name__ == "__main__":
    main()
