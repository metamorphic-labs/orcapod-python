"""``node.trace()``, ``node.why_missing()`` and ``diagram(highlight=...)`` (ITL-1432).

Every test drives a ``PipelineJob`` through its public interface. The sources have
several tags and several data columns, declared out of name order; the function pods
have several outputs. The pipeline joins units with sessions, filters the units, runs a
function per unit, groups by subject and session (or batches), and runs a function per
group. Two units of one session have equal inputs, so they share one stored result.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import ProvenanceInconsistencyError
from orcapod.operators import Batch, Filter, GroupBy, Join, SelectDataColumns
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource
from orcapod.types import Highlight, SourceCell, col

pytestmark = pytest.mark.filterwarnings("ignore::Warning")

# (unit, session, subject) tags, out of name order; data (depth_um, n_spikes).
UNITS = [
    ("u0", "s1", "m1", 100, 10),  # row_0
    ("u1", "s1", "m1", 200, 20),  # row_1
    ("u2", "s1", "m1", 300, 10),  # row_2: same inputs as row_0 -> one shared result
    ("u0", "s2", "m1", 100, 30),  # row_3
    ("u0", "s1", "m2", 100, 40),  # row_4
    ("u1", "s1", "m2", 900, 50),  # row_5: removed by the filter (depth_um < 500)
    ("u0", "s1", "m3", 100, 60),  # row_6: no session m3 s1 -> removed by the join
]
# (session, subject) tags; data (rig, duration_s).
SESSIONS = [
    ("s1", "m1", "A", 10.0),  # row_0
    ("s2", "m1", "B", 20.0),  # row_1
    ("s1", "m2", "A", 10.0),  # row_2
]

CALLS: dict[str, int] = {"rates": 0, "means": 0}


def units() -> ArrowTableSource:
    """The units: three tags and two data columns (``UNITS``); ``row_0`` and
    ``row_2`` share one result, ``row_5`` is filtered out, ``row_6`` has no session."""
    schema = pa.schema(
        [
            pa.field("unit", pa.large_string(), nullable=False),
            pa.field("session", pa.large_string(), nullable=False),
            pa.field("subject", pa.large_string(), nullable=False),
            pa.field("depth_um", pa.int64(), nullable=False),
            pa.field("n_spikes", pa.int64(), nullable=False),
        ]
    )
    table = pa.Table.from_pylist(
        [dict(zip(schema.names, r)) for r in UNITS], schema=schema
    )
    return ArrowTableSource(
        table,
        tag_columns=["unit", "session", "subject"],
        tag_only_columns=True,
        source_id="units",
    )


def sessions() -> ArrowTableSource:
    """The sessions: two tags and two data columns (``SESSIONS``); none for ``m3 s1``."""
    schema = pa.schema(
        [
            pa.field("session", pa.large_string(), nullable=False),
            pa.field("subject", pa.large_string(), nullable=False),
            pa.field("rig", pa.large_string(), nullable=False),
            pa.field("duration_s", pa.float64(), nullable=False),
        ]
    )
    table = pa.Table.from_pylist(
        [dict(zip(schema.names, r)) for r in SESSIONS], schema=schema
    )
    return ArrowTableSource(
        table,
        tag_columns=["session", "subject"],
        tag_only_columns=True,
        source_id="sessions",
    )


@function_pod(output_keys=["rate_hz", "per_min"])
def rates(n_spikes: int, duration_s: float) -> tuple[float, float]:
    """Spikes per second and per minute; counts its calls (no call may come from a
    provenance read). Equal inputs (u0 and u2 of m1 s1) share one stored result."""
    CALLS["rates"] += 1
    return n_spikes / duration_s, 60 * n_spikes / duration_s


@function_pod(output_keys=["mean_hz", "n_units"])
def means(rate_hz: list[float]) -> tuple[float, int]:
    """The mean rate and the number of rates of a group; counts its calls."""
    CALLS["means"] += 1
    return sum(rate_hz) / len(rate_hz), len(rate_hz)


def build(
    root: Path,
    *,
    sessions_first: bool = False,
    fold: str = "group",
    name: str = "prov",
) -> PipelineJob:
    """The test pipeline: units filtered (``depth_um < 500``, which removes ``row_5``),
    joined with sessions (``row_6`` has no session), a per-unit function with two outputs
    (``row_0`` and ``row_2`` have equal inputs and share one result), then a ``GroupBy``
    of subject and session (``fold="group"``) or a ``Batch`` of two (``fold="batch"``),
    then a per-group function.

    Args:
        root: The directory of the job's store.
        sessions_first: Give the join its inputs in the other argument order.
        fold: ``"group"`` or ``"batch"``.
        name: The job's name.
    """
    job = PipelineJob(name=name, database=DeltaTableDatabase(root / "store"))
    with job:
        kept = Filter(col("depth_um") < 500)(units())
        left = SelectDataColumns(["n_spikes"])(kept)
        right = SelectDataColumns(["duration_s"])(sessions())
        joined = Join()(right, left) if sessions_first else Join()(left, right)
        unit_rates = rates.pod(joined, label="unit_rates")
        if fold == "group":
            folded = GroupBy(["session", "subject"])(unit_rates)
        else:
            folded = Batch(batch_size=2)(unit_rates)
        means.pod(SelectDataColumns(["rate_hz"])(folded), label="means")
    return job


def load(root: Path) -> PipelineJob:
    """The job saved under ``root``, loaded, with its in-memory sources bound again."""
    loaded = PipelineJob.load(
        root / "job.json", database=DeltaTableDatabase(root / "store")
    )
    loaded.bind(sources={"units": units(), "sessions": sessions()})
    return loaded


def run(job: PipelineJob, mode: str) -> None:
    """Run ``job`` on the sync or the async orchestrator (``mode``)."""
    orchestrator = (
        AsyncPipelineOrchestrator() if mode == "async" else SyncPipelineOrchestrator()
    )
    job.run(orchestrator=orchestrator)


def cells(trace: Any) -> set[tuple[str, str, str, Any]]:
    """A trace's source cells as ``(source id, record id, column, value)``."""
    return {(c.source_id, c.record_id, c.column, c.value) for c in trace.source_cells()}


GROUP_M1_S1 = {
    ("arrow_table_v1:units", "row_0", "n_spikes", 10),
    ("arrow_table_v1:units", "row_1", "n_spikes", 20),
    ("arrow_table_v1:units", "row_2", "n_spikes", 10),
    ("arrow_table_v1:sessions", "row_0", "duration_s", 10.0),
}


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("sessions_first", [False, True])
def test_trace_reaches_every_source_cell(
    tmp_path: Path, mode: str, sessions_first: bool
) -> None:
    job = build(tmp_path, sessions_first=sessions_first)
    run(job, mode)
    trace = job.nodes["means"].trace({"subject": "m1", "session": "s1"}, "mean_hz")
    assert trace.status == "traced"
    assert trace.root.value == pytest.approx((1.0 + 2.0 + 1.0) / 3)
    assert cells(trace) == GROUP_M1_S1
    runs = trace.function_runs()
    assert [label for label, _ in runs] == ["means"] + ["unit_rates"] * 3
    assert {r.function for _, r in runs} == {"means", "rates"}
    assert all(r.version == "v0.0" and r.executor for _, r in runs)
    assert all(r.body_hash.startswith("function_content_v1:") for _, r in runs)
    # Each member of the group fills its position of the folded lists.
    (grouped,) = trace.root.inputs
    members = [step for step in grouped.walk() if step.node == "unit_rates"]
    assert [m.member for m in members] == [0, 1, 2]
    assert [dict(m.tags)["unit"] for m in members] == ["u0", "u1", "u2"]


def test_argument_order_gives_one_trace(tmp_path: Path) -> None:
    traces = []
    for first in (False, True):
        job = build(tmp_path / str(first), sessions_first=first)
        job.run()
        traces.append(
            job.nodes["means"].trace({"session": "s1", "subject": "m2"}, "n_units")
        )
    assert (
        cells(traces[0])
        == cells(traces[1])
        == {
            ("arrow_table_v1:units", "row_4", "n_spikes", 40),
            ("arrow_table_v1:sessions", "row_2", "duration_s", 10.0),
        }
    )


def test_shared_memoized_result_is_traced_exactly(tmp_path: Path) -> None:
    job = build(tmp_path)
    job.run()
    trace = job.nodes["means"].trace({"subject": "m1", "session": "s1"}, "mean_hz")
    unit_runs = {
        dict(step.tags)["unit"]: step.function
        for step in trace.root.walk()
        if step.node == "unit_rates"
    }
    # u0 and u2 have equal inputs: one stored result, two entries, two records.
    assert unit_runs["u0"] is not None and unit_runs["u2"] is not None
    assert unit_runs["u0"].result_id == unit_runs["u2"].result_id
    assert unit_runs["u0"].entry_id != unit_runs["u2"].entry_id
    assert {unit_runs["u0"].computed_here, unit_runs["u2"].computed_here} == {
        True,
        False,
    }
    assert unit_runs["u1"].computed_here is True
    assert {
        c.record_id
        for c in trace.source_cells()
        if c.source_id == "arrow_table_v1:units"
    } == {
        "row_0",
        "row_1",
        "row_2",
    }
    assert "reused a stored result" in trace.text()


def test_trace_on_a_loaded_job(tmp_path: Path) -> None:
    job = build(tmp_path)
    job.run()
    live = job.nodes["means"].trace({"subject": "m1", "session": "s1"}, "mean_hz")
    job.save(tmp_path / "job.json")
    loaded = load(tmp_path)
    before = loaded.nodes["means"].trace({"subject": "m1", "session": "s1"}, "mean_hz")
    assert before.status == "not_computed"
    loaded.run(load_only=True)
    again = loaded.nodes["means"].trace({"subject": "m1", "session": "s1"}, "mean_hz")
    assert again.status == "traced"
    assert cells(again) == cells(live)
    assert [(n, r.result_id, r.computed_here) for n, r in again.function_runs()] == [
        (n, r.result_id, r.computed_here) for n, r in live.function_runs()
    ]


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_trace_through_a_batch(tmp_path: Path, mode: str) -> None:
    """Each batch member is traced to its own unit, in the batch's member order, on
    the sync and the async path (the async path batches in arrival order)."""
    job = build(tmp_path, fold="batch")
    run(job, mode)
    held = job.nodes["means"].as_table().to_pylist()
    # The singleton remainder can appear first in the held results.
    full_batch = next(row for row in held if len(row["unit"]) == 2)
    tags = {name: full_batch[name] for name in ("unit", "session", "subject")}
    trace = job.nodes["means"].trace(tags, "mean_hz")
    members = [s for s in trace.root.walk() if s.node == "unit_rates"]
    assert [m.member for m in members] == [0, 1]
    assert [dict(m.tags)["unit"] for m in members] == full_batch["unit"]
    # The selection feeding means (by structure: numbered labels follow hashes).
    selection = job.nodes["means"].upstreams[0].label
    grouped = [s for s in trace.root.walk() if s.node == selection]
    held_batch = job.nodes[selection].as_table().to_pylist()
    (batch,) = [b for b in held_batch if all(b[name] == v for name, v in tags.items())]
    assert [m.value for m in members] == batch["rate_hz"]
    assert len(grouped) == 1


def test_a_refold_that_disagrees_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = build(tmp_path)
    job.run()
    real = GroupBy.fold_members

    def reversed_members(self: GroupBy, table: Any, tags: Any) -> list[list[int]]:
        return [m[::-1] for m in real(self, table, tags)]

    monkeypatch.setattr(GroupBy, "fold_members", reversed_members)
    with pytest.raises(ProvenanceInconsistencyError, match="fold"):
        job.nodes["means"].trace({"subject": "m1", "session": "s1"}, "mean_hz")


def test_trace_of_an_operator_and_a_source_node(tmp_path: Path) -> None:
    job = build(tmp_path)
    job.run()
    joined = job.nodes["Join"].trace(
        {"unit": "u1", "session": "s1", "subject": "m1"}, "n_spikes"
    )
    assert cells(joined) == {("arrow_table_v1:units", "row_1", "n_spikes", 20)}
    leaf = job.nodes["sessions"].trace({"session": "s2", "subject": "m1"}, "rig")
    assert leaf.root.source == SourceCell(
        "sessions", "arrow_table_v1:sessions", "row_1", "rig", "B"
    )


def test_trace_is_read_only_and_not_computed_before_a_run(tmp_path: Path) -> None:
    job = build(tmp_path)
    before = job.nodes["means"].trace({"subject": "m1", "session": "s1"}, "mean_hz")
    assert before.status == "not_computed"
    assert "not computed" in before.text()
    job.run()

    def store_files() -> dict[str, tuple[int, int]]:
        """Every file of the store with its size and modification time: a write,
        an overwrite in place included, changes this."""
        return {
            str(path.relative_to(tmp_path)): (
                path.stat().st_size,
                path.stat().st_mtime_ns,
            )
            for path in sorted((tmp_path / "store").rglob("*"))
            if path.is_file()
        }

    files = store_files()
    calls = dict(CALLS)
    job.nodes["means"].trace({"subject": "m1", "session": "s1"}, "mean_hz")
    job.nodes["means"].why_missing("arrow_table_v1:units", "row_5")
    assert CALLS == calls  # no function was called
    assert store_files() == files  # nothing was written


def test_row_selection_errors(tmp_path: Path) -> None:
    job = build(tmp_path)
    job.run()
    node = job.nodes["means"]
    with pytest.raises(ValueError, match="not a data column"):
        node.trace({"subject": "m1", "session": "s1"}, "subject")
    with pytest.raises(ValueError, match="no column"):
        node.trace({"probe": "x"}, "mean_hz")
    # A selector matching several rows, or none, is an answer, not an error.
    assert node.trace({"subject": "m1"}, "mean_hz").matched == 2
    none = node.trace({"subject": "m9", "session": "s1"}, "mean_hz")
    assert (none.status, none.matched, none.roots) == ("no_match", 0, ())
    assert none.text() == (
        "trace of mean_hz at node means, for subject='m9', session='s1': "
        "no held row matches"
    )
    with pytest.raises(ValueError, match="read roots"):
        _ = none.root


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_why_missing(tmp_path: Path, mode: str) -> None:
    job = build(tmp_path)
    run(job, mode)
    node = job.nodes["means"]

    present = node.why_missing("arrow_table_v1:units", "row_0")
    assert not present.missing
    assert present.reached_function is True
    assert present.present_in[-1] == "means"

    filtered = node.why_missing("arrow_table_v1:units", "row_5")
    # No stored entry proves nothing (the function might have raised): unknown.
    assert filtered.missing and filtered.reached_function is None
    assert filtered.entry_recorded is False
    assert filtered.function_node == "unit_rates"
    assert filtered.removed_by is not None
    assert filtered.removed_by.operator == "Filter"
    assert filtered.removed_by.text == "depth_um < 500"
    assert filtered.present_in == ("units",)

    joined = node.why_missing("arrow_table_v1:units", "row_6")
    assert joined.missing and joined.reached_function is None
    assert joined.entry_recorded is False
    assert joined.removed_by is not None and joined.removed_by.operator == "Join"
    assert "session='s1', subject='m3'" in joined.because
    assert "Join" in joined.text()

    with pytest.raises(ValueError, match="no source"):
        node.why_missing("probes", "row_0")


def test_why_missing_on_a_loaded_job(tmp_path: Path) -> None:
    job = build(tmp_path)
    job.run()
    job.save(tmp_path / "job.json")
    loaded = load(tmp_path)
    assert (
        loaded.nodes["means"].why_missing("arrow_table_v1:units", "row_5").status
        == "not_computed"
    )
    loaded.run(load_only=True)
    answer = loaded.nodes["means"].why_missing("arrow_table_v1:units", "row_5")
    assert answer.status == "read" and answer.removed_by is not None
    assert answer.removed_by.operator == "Filter"


def test_diagram_highlight(tmp_path: Path) -> None:
    job = build(tmp_path)
    job.run()
    trace = job.nodes["means"].trace({"subject": "m1", "session": "s1"}, "mean_hz")
    diagram = job.diagram(highlight=trace.highlight())
    text = diagram.to_text()
    assert (
        "highlight (*): trace of mean_hz at node means, for subject='m1', "
        "session='s1': 1 row matches"
    ) in text
    assert " * unit_rates" in text
    model = diagram.to_dict()
    lit = {n["label"] for n in model["nodes"] if n["highlighted"]}
    assert lit == set(trace.nodes())
    assert any(e["highlighted"] for e in model["edges"])
    assert "op-dag-highlight" in diagram.to_svg()

    absence = job.nodes["means"].why_missing("arrow_table_v1:units", "row_5")
    shown = job.diagram(highlight=absence.highlight()).to_dict()
    assert {n["label"] for n in shown["nodes"] if n["highlighted"]} == {
        "units",
        "Filter",
    }
    assert 'opacity="0.4' in job.diagram(highlight=absence.highlight()).to_svg()

    with pytest.raises(ValueError, match="not a node"):
        job.diagram(highlight=Highlight(nodes=frozenset({"nowhere"})))
    # Without a highlight the model has no highlight fields.
    assert "note" not in job.diagram().to_dict()


def test_reader_keeps_the_nodes_own_entries(tmp_path: Path) -> None:
    """Two jobs of one name over sources of one schema share the pipeline tables; each
    node reads only its own entries (``__node_content_hash``). The reader is internal:
    this test is about it."""
    job = build(tmp_path)
    job.run()
    other = PipelineJob(name="prov", database=DeltaTableDatabase(tmp_path / "store"))
    renamed = units()
    with other:
        kept = Filter(col("depth_um") < 500)(
            ArrowTableSource(
                renamed.as_table(),
                tag_columns=["unit", "session", "subject"],
                tag_only_columns=True,
                source_id="units_b",
            )
        )
        left = SelectDataColumns(["n_spikes"])(kept)
        right = SelectDataColumns(["duration_s"])(sessions())
        rates.pod(Join()(left, right), label="unit_rates")
    other.run()
    mine, _ = job.nodes["unit_rates"]._stored_provenance()
    theirs, _ = other.nodes["unit_rates"]._stored_provenance()
    assert mine is not None and theirs is not None
    assert mine.num_rows == theirs.num_rows == 5
    assert set(mine.column("__pipeline_entry_id").to_pylist()).isdisjoint(
        theirs.column("__pipeline_entry_id").to_pylist()
    )
    for column in (
        "__pipeline_base_entry_id",
        "__node_content_hash",
        "__computed",
        "__input_data_hash",
    ):
        assert column in mine.column_names
    # The trace of the first job reads only its own entries.
    trace = job.nodes["means"].trace({"subject": "m1", "session": "s1"}, "mean_hz")
    assert {c.source_id for c in trace.source_cells()} == {
        "arrow_table_v1:units",
        "arrow_table_v1:sessions",
    }


def test_reader_of_a_loaded_node_reads_its_ephemeral_results(tmp_path: Path) -> None:
    """A loaded node has no pod to build an ephemeral cached pod from; the provenance
    reader still reads the attached ephemeral database (internal reader, CodeRabbit
    thread on PR 530)."""
    from orcapod.types import NodeConfig

    job = build(tmp_path)
    ephemeral = DeltaTableDatabase(tmp_path / "ephemeral")
    job.set_ephemeral_database(ephemeral)
    job.nodes["unit_rates"].node_config = NodeConfig(is_result_ephemeral=True)
    job.run()
    _, live = job.nodes["unit_rates"]._stored_provenance()
    assert live is not None and live.num_rows == 4  # u0 and u2 share one
    job.save(tmp_path / "job.json")
    loaded = load(tmp_path)
    loaded.set_ephemeral_database(ephemeral)
    _, stored = loaded.nodes["unit_rates"]._stored_provenance()
    assert stored is not None
    assert sorted(stored.column("__data_id").to_pylist()) == sorted(
        live.column("__data_id").to_pylist()
    )


def test_a_loaded_node_refreshes_its_ephemeral_results_before_reading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The provenance read of a loaded node refreshes every table it reads, the
    ephemeral result table it reads without an ephemeral cached pod included (a Delta
    handle could hold an older version). Probe: ``DeltaTableDatabase``'s own
    ``refresh_table`` and ``get_all_records`` calls, by database and path."""
    from orcapod.types import NodeConfig

    job = build(tmp_path)
    ephemeral = DeltaTableDatabase(tmp_path / "ephemeral")
    job.set_ephemeral_database(ephemeral)
    job.nodes["unit_rates"].node_config = NodeConfig(is_result_ephemeral=True)
    job.run()
    job.save(tmp_path / "job.json")
    loaded = load(tmp_path)
    loaded.set_ephemeral_database(ephemeral)
    calls: list[tuple[str, int, tuple[str, ...]]] = []
    for name in ("refresh_table", "get_all_records"):
        real = getattr(DeltaTableDatabase, name)

        def recorded(
            self: Any, path: Any, *args: Any, _r: Any = real, _n: str = name, **kw: Any
        ) -> Any:
            calls.append((_n, id(self), tuple(path)))
            return _r(self, path, *args, **kw)

        monkeypatch.setattr(DeltaTableDatabase, name, recorded)
    _, stored = loaded.nodes["unit_rates"]._stored_provenance()
    assert stored is not None
    reads = [c for c in calls if c[0] == "get_all_records"]
    assert any(c[1] == id(ephemeral) for c in reads)
    for i, (name, db, path) in enumerate(calls):
        if name == "get_all_records":
            assert ("refresh_table", db, path) in calls[:i]
