"""Shared helpers for the ITL-1421 provenance audit scripts (public API only).

Every script builds tiny in-memory sources with two tag columns AND two data columns,
named so their order is not their name order, and prints what it proves.
"""

from __future__ import annotations

import warnings
from typing import Any

import pyarrow as pa

from orcapod.operators import GroupBy, SelectDataColumns
from orcapod.sources import ArrowTableSource

warnings.filterwarnings("ignore", message=".*provisional.*")
warnings.filterwarnings("ignore", category=Warning, module="orcapod.*")


def units_source(
    *, reverse: bool = False, source_id: str = "units"
) -> ArrowTableSource:
    """Four units: tags (subject, session), data (n_spikes, depth_um)."""
    rows = [
        ("m1", "s1", 10, 200),
        ("m1", "s2", 20, 400),
        ("m2", "s1", 30, 600),
        ("m2", "s2", 40, 800),
    ]
    if reverse:
        rows = rows[::-1]
    schema = pa.schema(
        [
            pa.field("subject", pa.large_string(), nullable=False),
            pa.field("session", pa.large_string(), nullable=False),
            pa.field("n_spikes", pa.int64(), nullable=False),
            pa.field("depth_um", pa.int64(), nullable=False),
        ]
    )
    table = pa.Table.from_pylist(
        [dict(zip(schema.names, r)) for r in rows], schema=schema
    )
    return ArrowTableSource(
        table,
        tag_columns=["subject", "session"],
        tag_only_columns=True,
        source_id=source_id,
        record_id_columns=["subject", "session"],
    )


def sessions_source(*, source_id: str = "sessions") -> ArrowTableSource:
    """Four sessions: tags (session, subject: declared in non-name order), data
    (duration_s, rig)."""
    schema = pa.schema(
        [
            pa.field("session", pa.large_string(), nullable=False),
            pa.field("subject", pa.large_string(), nullable=False),
            pa.field("duration_s", pa.float64(), nullable=False),
            pa.field("rig", pa.large_string(), nullable=False),
        ]
    )
    rows = [
        ("s1", "m1", 60.0, "A"),
        ("s2", "m1", 90.0, "B"),
        ("s1", "m2", 120.0, "A"),
        ("s2", "m2", 60.0, "C"),
    ]
    table = pa.Table.from_pylist(
        [dict(zip(schema.names, r)) for r in rows], schema=schema
    )
    return ArrowTableSource(
        table,
        tag_columns=["session", "subject"],
        tag_only_columns=True,
        source_id=source_id,
    )


def provenance_columns(table: pa.Table) -> list[str]:
    """The source-info (``_source_``) and system-tag (``_tag``) columns of a table."""
    return [c for c in table.column_names if c.startswith(("_source_", "_tag"))]


def show(title: str, stream, *, rows: int = 2) -> pa.Table:
    """Print a stream's provenance columns and their first rows; return the table."""
    table = stream.as_table(all_info=True)
    print(f"--- {title}: {table.num_rows} rows")
    for name in provenance_columns(table):
        values = table.column(name).to_pylist()[:rows]
        print(f"  {short(name)} : {table.schema.field(name).type} = {values}")
    return table


def short(name: str) -> str:
    """Shorten a system-tag column name: keep the hash tails to 8 characters."""
    parts = name.split("::")
    return "::".join(p if len(p) <= 20 else p[:12] + "…" for p in parts)


def workshop_job(
    root, *, database=None, name: str = "workshop", regenerate: bool = True
):
    """The tutorial's pipeline (Part 8) over the synthetic workshop dataset.

    units (CSV) -> select n_spikes ┐
                                    ├ join -> firing_rate [rates] -> group_by(subject_id,
    sessions (CSV) -> select duration_s ┘        session) -> select rate_hz -> mean_rate
                                                 [session_means]

    Returns ``(job, data_dir)``; the job is built but not run.
    """
    import importlib
    import sys
    from pathlib import Path

    # The tutorial's generator is a standalone file, not an installed module.
    sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "tutorial"))
    make_dataset: Any = importlib.import_module("workshop_data").make_dataset

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.sources import CSVSource

    root = Path(root)
    data_dir = (
        make_dataset(root / "workshop_data") if regenerate else root / "workshop_data"
    )
    if database is None:
        database = DeltaTableDatabase(root / "store")

    @function_pod(output_keys="rate_hz")
    def firing_rate(n_spikes: int, duration_s: float) -> float:
        return n_spikes / duration_s

    @function_pod(output_keys="mean_rate_hz")
    def mean_rate(rate_hz: list[float]) -> float:
        return sum(rate_hz) / len(rate_hz)

    job = PipelineJob(name=name, database=database)
    with job:
        units = CSVSource(
            str(data_dir / "units.csv"),
            tag_columns=["subject_id", "session", "probe", "unit"],
            tag_only_columns=True,
            source_id="units",
        )
        sessions = CSVSource(
            str(data_dir / "sessions.csv"),
            tag_columns=["subject_id", "session"],
            tag_only_columns=True,
            source_id="sessions",
        )
        rates = firing_rate.pod(
            units.select_data_columns(["n_spikes"]),
            sessions.select_data_columns(["duration_s"]),
            label="rates",
        )
        grouped = GroupBy(["subject_id", "session"])(rates)
        mean_rate.pod(SelectDataColumns(["rate_hz"])(grouped), label="session_means")
    return job, data_dir
